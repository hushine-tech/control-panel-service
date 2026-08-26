package runtimechannel

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	cpv1 "github.com/hushine-tech/control-panel-service/gen/controlpanelv1"
	"github.com/hushine-tech/control-panel-service/internal/domain"
)

var ErrRuntimeCredentialConnected = errors.New("runtime credential already connected")
var ErrRegistryClosed = errors.New("runtime channel registry is closed")

const (
	defaultOutboundQueueCapacity = 64
	defaultPhysicalSendTimeout   = 5 * time.Second
)

type outboundPumpConfig struct {
	queueCapacity int
	sendTimeout   time.Duration
	after         func(time.Duration) <-chan time.Time
}

type runtimeStream struct {
	Runtime  AuthenticatedRuntime
	openedAt time.Time

	mu          sync.Mutex
	lastFrameAt time.Time
	closed      chan struct{}
	closeOnce   sync.Once
	send        func(*cpv1.RuntimeFrame) error
	inFlight    map[string]chan *cpv1.RuntimeFrame
	dropped     int64

	outboundMu          sync.Mutex
	outboundQueue       *PriorityFrameQueue
	outboundPumping     bool
	physicalSendTimeout time.Duration
	after               func(time.Duration) <-chan time.Time
}

func newRuntimeStream(rt AuthenticatedRuntime, now time.Time) *runtimeStream {
	stream := &runtimeStream{
		Runtime:     cloneAuthenticatedRuntime(rt),
		openedAt:    now,
		lastFrameAt: now,
		closed:      make(chan struct{}),
		inFlight:    map[string]chan *cpv1.RuntimeFrame{},
	}
	stream.configureOutbound(outboundPumpConfig{})
	return stream
}

func (s *runtimeStream) configureOutbound(cfg outboundPumpConfig) {
	if cfg.queueCapacity <= 0 {
		cfg.queueCapacity = defaultOutboundQueueCapacity
	}
	if cfg.sendTimeout <= 0 {
		cfg.sendTimeout = defaultPhysicalSendTimeout
	}
	if cfg.after == nil {
		cfg.after = time.After
	}
	s.outboundMu.Lock()
	s.outboundQueue = NewPriorityFrameQueue(cfg.queueCapacity)
	s.physicalSendTimeout = cfg.sendTimeout
	s.after = cfg.after
	s.outboundMu.Unlock()
}

func (s *runtimeStream) touch(at time.Time) {
	s.mu.Lock()
	s.lastFrameAt = at
	s.mu.Unlock()
}

func (s *runtimeStream) lastFrame() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastFrameAt
}

func (s *runtimeStream) connectionContext(parent context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(parent)
	go func() {
		select {
		case <-s.closed:
			cancel()
		case <-ctx.Done():
		}
	}()
	return ctx, cancel
}

func (s *runtimeStream) close() {
	s.closeOnce.Do(func() {
		close(s.closed)
		s.failInFlight()
		s.outboundMu.Lock()
		queue := s.outboundQueue
		s.outboundMu.Unlock()
		if queue != nil {
			queue.failAll(status.Error(codes.Unavailable, "runtime stream closed"))
		}
	})
}

func (s *runtimeStream) setSender(send func(*cpv1.RuntimeFrame) error) {
	s.mu.Lock()
	s.send = send
	s.mu.Unlock()
}

func (s *runtimeStream) sendFrame(frame *cpv1.RuntimeFrame) error {
	done := make(chan error, 1)
	if err := s.enqueueOutbound(&outboundFrame{frame: frame, done: done}); err != nil {
		return err
	}
	select {
	case err := <-done:
		return err
	case <-s.closed:
		return status.Error(codes.Unavailable, "runtime stream closed")
	}
}

func (s *runtimeStream) enqueueDataFrame(frame *cpv1.RuntimeFrame, onFailed func(error)) error {
	if runtimeFrameIsControl(frame) {
		return status.Error(codes.InvalidArgument, "outbound data enqueue requires a data frame")
	}
	return s.enqueueOutbound(&outboundFrame{frame: frame, onFailed: onFailed})
}

func (s *runtimeStream) enqueueOutbound(item *outboundFrame) error {
	if item == nil || item.frame == nil {
		return status.Error(codes.InvalidArgument, "outbound runtime frame is required")
	}
	select {
	case <-s.closed:
		return status.Error(codes.Unavailable, "runtime stream closed")
	default:
	}
	s.mu.Lock()
	senderReady := s.send != nil
	s.mu.Unlock()
	if !senderReady {
		return status.Error(codes.Unavailable, "runtime stream sender is not ready")
	}
	s.outboundMu.Lock()
	queue := s.outboundQueue
	s.outboundMu.Unlock()
	if queue == nil {
		return status.Error(codes.Unavailable, "runtime stream outbound queue is not ready")
	}
	if err := queue.enqueue(item); err != nil {
		return err
	}
	s.startOutboundPump()
	return nil
}

func (s *runtimeStream) startOutboundPump() {
	s.outboundMu.Lock()
	if s.outboundPumping {
		s.outboundMu.Unlock()
		return
	}
	s.outboundPumping = true
	s.outboundMu.Unlock()
	go s.runOutboundPump()
}

func (s *runtimeStream) runOutboundPump() {
	for {
		s.outboundMu.Lock()
		queue := s.outboundQueue
		s.outboundMu.Unlock()
		item, ok := queue.dequeue()
		if !ok {
			s.outboundMu.Lock()
			item, ok = s.outboundQueue.dequeue()
			if !ok {
				s.outboundPumping = false
				s.outboundMu.Unlock()
				return
			}
			s.outboundMu.Unlock()
		}
		select {
		case <-s.closed:
			completeOutboundFrame(item, status.Error(codes.Unavailable, "runtime stream closed"))
			return
		default:
		}
		err := s.sendPhysicalFrame(item.frame)
		completeOutboundFrame(item, err)
		if err != nil {
			s.close()
			return
		}
	}
}

func (s *runtimeStream) sendPhysicalFrame(frame *cpv1.RuntimeFrame) error {
	s.mu.Lock()
	send := s.send
	s.mu.Unlock()
	if send == nil {
		return status.Error(codes.Unavailable, "runtime stream sender is not ready")
	}
	result := make(chan error, 1)
	go func() {
		result <- send(frame)
	}()
	s.outboundMu.Lock()
	timeout := s.physicalSendTimeout
	after := s.after
	s.outboundMu.Unlock()
	select {
	case err := <-result:
		return err
	case <-s.closed:
		return status.Error(codes.Unavailable, "runtime stream closed")
	case <-after(timeout):
		return status.Error(codes.DeadlineExceeded, "runtime stream physical send deadline exceeded")
	}
}

func (s *runtimeStream) registerCall(correlationID string) chan *cpv1.RuntimeFrame {
	ch := make(chan *cpv1.RuntimeFrame, 16)
	s.mu.Lock()
	s.inFlight[correlationID] = ch
	s.mu.Unlock()
	return ch
}

func (s *runtimeStream) unregisterCall(correlationID string) {
	s.mu.Lock()
	delete(s.inFlight, correlationID)
	s.mu.Unlock()
}

func (s *runtimeStream) deliver(frame *cpv1.RuntimeFrame) bool {
	correlationID := frameCorrelationID(frame)
	if correlationID == "" {
		return false
	}
	s.mu.Lock()
	ch := s.inFlight[correlationID]
	s.mu.Unlock()
	if ch == nil {
		return false
	}
	select {
	case ch <- frame:
		return true
	default:
		s.mu.Lock()
		s.dropped++
		s.mu.Unlock()
		return false
	}
}

func frameCorrelationID(frame *cpv1.RuntimeFrame) string {
	if frame == nil {
		return ""
	}
	if cid := frame.GetCorrelationId(); cid != "" {
		return cid
	}
	switch frame.GetFrameType() {
	case cpv1.FrameType_FRAME_TYPE_COMMAND_ACK:
		if frame.GetCommandAck() != nil {
			return frame.GetCommandAck().GetCommandId()
		}
	case cpv1.FrameType_FRAME_TYPE_COMMAND_RESULT:
		if frame.GetCommandResult() != nil {
			return frame.GetCommandResult().GetCommandId()
		}
	}
	return ""
}

func (s *runtimeStream) failInFlight() {
	s.mu.Lock()
	chans := make([]chan *cpv1.RuntimeFrame, 0, len(s.inFlight))
	for cid, ch := range s.inFlight {
		delete(s.inFlight, cid)
		chans = append(chans, ch)
	}
	s.mu.Unlock()
	for _, ch := range chans {
		close(ch)
	}
}

// Registry owns the active RuntimeChannel double-index.
type Registry struct {
	mu               sync.Mutex
	streamsByRuntime map[string]*runtimeStream
	runtimesByKeyID  map[string]map[string]struct{}
	nextConnectionID uint64
	closed           bool
}

func NewRegistry() *Registry {
	return &Registry{
		streamsByRuntime: map[string]*runtimeStream{},
		runtimesByKeyID:  map[string]map[string]struct{}{},
	}
}

func (r *Registry) Register(rt AuthenticatedRuntime, now time.Time) (*runtimeStream, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, ErrRegistryClosed
	}

	if rt.KeyID != "" {
		if set := r.runtimesByKeyID[rt.KeyID]; len(set) > 0 {
			for runtimeID := range set {
				if runtimeID != rt.RuntimeID {
					return nil, fmt.Errorf("%w: key_id %s is already used by runtime %s", ErrRuntimeCredentialConnected, rt.KeyID, runtimeID)
				}
			}
		}
	}
	if old := r.streamsByRuntime[rt.RuntimeID]; old != nil {
		old.close()
		r.removeLocked(old)
	}
	r.nextConnectionID++
	rt.ConnectionID = fmt.Sprintf("%s/%d", rt.RuntimeID, r.nextConnectionID)
	stream := newRuntimeStream(rt, now)
	r.streamsByRuntime[rt.RuntimeID] = stream
	if rt.KeyID == "" {
		return stream, nil
	}
	set := r.runtimesByKeyID[rt.KeyID]
	if set == nil {
		set = map[string]struct{}{}
		r.runtimesByKeyID[rt.KeyID] = set
	}
	set[rt.RuntimeID] = struct{}{}
	return stream, nil
}

// CloseAll permanently closes the registry and every active RuntimeChannel.
// A service shutdown must reject late reconnects while the gRPC server drains;
// otherwise GracefulStop can wait forever on a newly admitted long-lived stream.
func (r *Registry) CloseAll() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	closed := 0
	for _, stream := range r.streamsByRuntime {
		stream.close()
		r.removeLocked(stream)
		closed++
	}
	return closed
}

// Unregister closes and removes expected only when it is still the active
// stream for runtimeID. A nil current stream is safe for the caller to treat as
// released because shutdown and explicit close paths remove the stream before
// the owning handler's deferred cleanup runs. A different current stream means
// the runtime reconnected and stale cleanup must not disturb its replacement.
func (r *Registry) Unregister(runtimeID string, expected *runtimeStream) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	current := r.streamsByRuntime[runtimeID]
	if current == nil {
		return true
	}
	if expected == nil || current != expected {
		return false
	}
	current.close()
	r.removeLocked(current)
	return true
}

func (r *Registry) CloseByKeyID(keyID string) []*runtimeStream {
	r.mu.Lock()
	defer r.mu.Unlock()

	ids := r.runtimesByKeyID[keyID]
	if len(ids) == 0 {
		return nil
	}
	streams := make([]*runtimeStream, 0, len(ids))
	for runtimeID := range ids {
		if s := r.streamsByRuntime[runtimeID]; s != nil {
			streams = append(streams, s)
			s.close()
			r.removeLocked(s)
		}
	}
	return streams
}

func (r *Registry) CloseByRuntimeID(runtimeID string) *runtimeStream {
	r.mu.Lock()
	defer r.mu.Unlock()

	s := r.streamsByRuntime[runtimeID]
	if s == nil {
		return nil
	}
	s.close()
	r.removeLocked(s)
	return s
}

func (r *Registry) Snapshot() []AuthenticatedRuntime {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]AuthenticatedRuntime, 0, len(r.streamsByRuntime))
	for _, s := range r.streamsByRuntime {
		out = append(out, cloneAuthenticatedRuntime(s.Runtime))
	}
	return out
}

type StreamMetric struct {
	RuntimeID        string
	KeyID            string
	UserID           int64
	Name             string
	Uptime           time.Duration
	LastFrameLatency time.Duration
	InFlightCalls    int
	DroppedCommands  int64
}

func (r *Registry) MetricsSnapshot(now time.Time) []StreamMetric {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]StreamMetric, 0, len(r.streamsByRuntime))
	for _, s := range r.streamsByRuntime {
		s.mu.Lock()
		metric := StreamMetric{
			RuntimeID:        s.Runtime.RuntimeID,
			KeyID:            s.Runtime.KeyID,
			UserID:           s.Runtime.UserID,
			Name:             s.Runtime.Name,
			Uptime:           now.Sub(s.openedAt),
			LastFrameLatency: now.Sub(s.lastFrameAt),
			InFlightCalls:    len(s.inFlight),
			DroppedCommands:  s.dropped,
		}
		s.mu.Unlock()
		out = append(out, metric)
	}
	return out
}

func (r *Registry) FindByRuntimeID(userID int64, runtimeID string) *runtimeStream {
	if runtimeID == "" {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	stream := r.streamsByRuntime[runtimeID]
	if stream == nil {
		return nil
	}
	if stream.Runtime.UserID != userID {
		return nil
	}
	return stream
}

func (r *Registry) IsCurrent(stream *runtimeStream) bool {
	if stream == nil {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.streamsByRuntime[stream.Runtime.RuntimeID] == stream
}

func (r *Registry) withCurrent(stream *runtimeStream, action func()) bool {
	if stream == nil {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.streamsByRuntime[stream.Runtime.RuntimeID] != stream {
		return false
	}
	if action != nil {
		action()
	}
	return true
}

func (r *Registry) removeLocked(s *runtimeStream) {
	delete(r.streamsByRuntime, s.Runtime.RuntimeID)
	if set := r.runtimesByKeyID[s.Runtime.KeyID]; set != nil {
		delete(set, s.Runtime.RuntimeID)
		if len(set) == 0 {
			delete(r.runtimesByKeyID, s.Runtime.KeyID)
		}
	}
}

func streamClosedErr(ch <-chan *cpv1.RuntimeFrame) (*cpv1.RuntimeFrame, error) {
	frame, ok := <-ch
	if !ok {
		return nil, status.Error(codes.Unavailable, "runtime stream disconnected")
	}
	if frame == nil {
		return nil, errors.New("nil runtime frame")
	}
	return frame, nil
}

// CloseStreamsForKey implements credential.RevokeStreamCloser.
func (s *Service) CloseStreamsForKey(ctx context.Context, keyID string) (int, int, error) {
	streams := s.registry.CloseByKeyID(keyID)
	ended := int64(0)
	if s.repo != nil {
		n, err := s.repo.EndRuntimesByCredentialKey(ctx, keyID, domain.RuntimeEndedReasonAuthFailed, s.now().UTC())
		if err != nil {
			return len(streams), int(ended), err
		}
		ended = n
	}
	return len(streams), int(ended), nil
}

func (s *Service) CloseStreamForRuntime(_ context.Context, runtimeID string) (bool, error) {
	return s.registry.CloseByRuntimeID(runtimeID) != nil, nil
}
