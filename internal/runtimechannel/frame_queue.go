package runtimechannel

import (
	"errors"
	"sort"
	"strconv"
	"sync"
	"time"

	cpv1 "github.com/hushine-tech/control-panel-service/gen/controlpanelv1"
)

var ErrRuntimeDataBackpressure = errors.New("runtime data backpressure")

type PriorityFrameQueue struct {
	mu           sync.Mutex
	capacity     int
	control      []*outboundFrame
	data         map[string][]*outboundFrame
	dataSessions []string
	dataCount    int
}

type outboundFrame struct {
	frame    *cpv1.RuntimeFrame
	done     chan error
	onFailed func(error)
}

func NewPriorityFrameQueue(capacity int) *PriorityFrameQueue {
	if capacity <= 0 {
		capacity = 1
	}
	return &PriorityFrameQueue{capacity: capacity, data: map[string][]*outboundFrame{}}
}

func (q *PriorityFrameQueue) Enqueue(frame *cpv1.RuntimeFrame) error {
	if frame == nil {
		return nil
	}
	return q.enqueue(&outboundFrame{frame: frame})
}

func (q *PriorityFrameQueue) enqueue(item *outboundFrame) error {
	if item == nil || item.frame == nil {
		return nil
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if runtimeFrameIsControl(item.frame) {
		if len(q.control) >= q.capacity {
			return ErrRuntimeDataBackpressure
		}
		q.control = append(q.control, item)
	} else {
		if q.dataCount >= q.capacity {
			return ErrRuntimeDataBackpressure
		}
		sessionID := runtimeFrameSessionID(item.frame)
		if len(q.data[sessionID]) == 0 {
			q.dataSessions = append(q.dataSessions, sessionID)
		}
		q.data[sessionID] = append(q.data[sessionID], item)
		q.dataCount++
	}
	return nil
}

func (q *PriorityFrameQueue) Dequeue() (*cpv1.RuntimeFrame, bool) {
	item, ok := q.dequeue()
	if !ok {
		return nil, false
	}
	return item.frame, true
}

func (q *PriorityFrameQueue) dequeue() (*outboundFrame, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.control) > 0 {
		item := q.control[0]
		q.control = q.control[1:]
		return item, true
	}
	if len(q.dataSessions) > 0 {
		sessionID := q.dataSessions[0]
		items := q.data[sessionID]
		item := items[0]
		q.dataCount--
		if len(items) == 1 {
			delete(q.data, sessionID)
			q.dataSessions = q.dataSessions[1:]
		} else {
			q.data[sessionID] = items[1:]
			q.dataSessions = append(q.dataSessions[1:], sessionID)
		}
		return item, true
	}
	return nil, false
}

func (q *PriorityFrameQueue) failAll(err error) {
	q.mu.Lock()
	items := append([]*outboundFrame(nil), q.control...)
	q.control = nil
	for _, sessionID := range q.dataSessions {
		items = append(items, q.data[sessionID]...)
	}
	q.data = map[string][]*outboundFrame{}
	q.dataSessions = nil
	q.dataCount = 0
	q.mu.Unlock()
	for _, item := range items {
		completeOutboundFrame(item, err)
	}
}

func runtimeFrameIsControl(frame *cpv1.RuntimeFrame) bool {
	switch frame.GetFrameType() {
	case cpv1.FrameType_FRAME_TYPE_LIVE_KLINE_BATCH,
		cpv1.FrameType_FRAME_TYPE_DATASET_CHUNK,
		cpv1.FrameType_FRAME_TYPE_ORDER_UPDATE_BATCH,
		cpv1.FrameType_FRAME_TYPE_INCOME_BATCH:
		return false
	default:
		return true
	}
}

func runtimeFrameSessionID(frame *cpv1.RuntimeFrame) string {
	if frame == nil {
		return ""
	}
	if batch := frame.GetIncomeBatch(); batch != nil {
		return batch.GetSessionId()
	}
	if batch := frame.GetLiveKlineBatch(); batch != nil {
		return batch.GetSessionId()
	}
	if batch := frame.GetOrderUpdateBatch(); batch != nil {
		return batch.GetSessionId()
	}
	if chunk := frame.GetDatasetChunk(); chunk != nil {
		return chunk.GetSessionId()
	}
	return ""
}

func completeOutboundFrame(item *outboundFrame, err error) {
	if item == nil {
		return
	}
	if err != nil && item.onFailed != nil {
		item.onFailed(err)
	}
	if item.done != nil {
		item.done <- err
	}
}

type RuntimeDataChunk struct {
	SessionID    string
	StreamKey    string
	Sequence     int64
	AttemptToken string
	Payload      []byte
	SentAt       time.Time
}

type RuntimeDataWindow struct {
	mu            sync.Mutex
	capacity      int
	sessionScoped bool
	nextSeq       map[string]int64
	unacked       map[string]RuntimeDataChunk
	perSession    map[string]int
	orderKeys     []string
}

func NewRuntimeDataWindow(capacity int) *RuntimeDataWindow {
	return newRuntimeDataWindow(capacity, false)
}

func NewSessionRuntimeDataWindow(capacity int) *RuntimeDataWindow {
	return newRuntimeDataWindow(capacity, true)
}

func newRuntimeDataWindow(capacity int, sessionScoped bool) *RuntimeDataWindow {
	if capacity <= 0 {
		capacity = 1
	}
	return &RuntimeDataWindow{
		capacity:      capacity,
		sessionScoped: sessionScoped,
		nextSeq:       map[string]int64{},
		unacked:       map[string]RuntimeDataChunk{},
		perSession:    map[string]int{},
	}
}

func (w *RuntimeDataWindow) Enqueue(sessionID, streamKey string, payload []byte, at time.Time) (RuntimeDataChunk, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.fullLocked(sessionID) {
		return RuntimeDataChunk{}, ErrRuntimeDataBackpressure
	}
	key := dataWindowStreamKey(sessionID, streamKey)
	seq := w.nextSeq[key] + 1
	w.nextSeq[key] = seq
	return w.trackLocked(sessionID, streamKey, seq, "", payload, at), nil
}

func (w *RuntimeDataWindow) Track(sessionID, streamKey string, sequence int64, payload []byte, at time.Time) (RuntimeDataChunk, error) {
	return w.TrackAttempt(sessionID, streamKey, sequence, "", payload, at)
}

func (w *RuntimeDataWindow) TrackAttempt(sessionID, streamKey string, sequence int64, attemptToken string, payload []byte, at time.Time) (RuntimeDataChunk, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	ackKey := dataWindowAckKey(sessionID, streamKey, sequence)
	if existing, ok := w.unacked[ackKey]; ok {
		if existing.AttemptToken == attemptToken {
			return existing, nil
		}
		return RuntimeDataChunk{}, ErrRuntimeDataBackpressure
	}
	if w.fullLocked(sessionID) {
		return RuntimeDataChunk{}, ErrRuntimeDataBackpressure
	}
	key := dataWindowStreamKey(sessionID, streamKey)
	if sequence > w.nextSeq[key] {
		w.nextSeq[key] = sequence
	}
	return w.trackLocked(sessionID, streamKey, sequence, attemptToken, payload, at), nil
}

func (w *RuntimeDataWindow) fullLocked(sessionID string) bool {
	if w.sessionScoped {
		return w.perSession[sessionID] >= w.capacity
	}
	return len(w.unacked) >= w.capacity
}

func (w *RuntimeDataWindow) trackLocked(sessionID, streamKey string, sequence int64, attemptToken string, payload []byte, at time.Time) RuntimeDataChunk {
	chunk := RuntimeDataChunk{
		SessionID:    sessionID,
		StreamKey:    streamKey,
		Sequence:     sequence,
		AttemptToken: attemptToken,
		Payload:      append([]byte(nil), payload...),
		SentAt:       at.UTC(),
	}
	ackKey := dataWindowAckKey(sessionID, streamKey, sequence)
	w.unacked[ackKey] = chunk
	w.perSession[sessionID]++
	w.orderKeys = append(w.orderKeys, ackKey)
	return chunk
}

func (w *RuntimeDataWindow) Ack(sessionID, streamKey string, sequence int64) bool {
	return w.AckAttempt(sessionID, streamKey, sequence, "")
}

func (w *RuntimeDataWindow) AckAttempt(sessionID, streamKey string, sequence int64, attemptToken string) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	key := dataWindowAckKey(sessionID, streamKey, sequence)
	chunk, ok := w.unacked[key]
	if !ok || chunk.AttemptToken != attemptToken {
		return false
	}
	w.removeLocked(key)
	return true
}

func (w *RuntimeDataWindow) Cancel(sessionID, streamKey string, sequence int64) bool {
	return w.CancelAttempt(sessionID, streamKey, sequence, "")
}

func (w *RuntimeDataWindow) CancelAttempt(sessionID, streamKey string, sequence int64, attemptToken string) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	key := dataWindowAckKey(sessionID, streamKey, sequence)
	chunk, ok := w.unacked[key]
	if !ok || chunk.AttemptToken != attemptToken {
		return false
	}
	w.removeLocked(key)
	return true
}

func (w *RuntimeDataWindow) ForgetAttempt(sessionID, streamKey string, sequence int64, attemptToken string) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	key := dataWindowAckKey(sessionID, streamKey, sequence)
	chunk, ok := w.unacked[key]
	if !ok || chunk.AttemptToken != attemptToken {
		return false
	}
	w.removeLocked(key)
	return true
}

func (w *RuntimeDataWindow) ForgetStream(sessionID, streamKey string) int {
	w.mu.Lock()
	defer w.mu.Unlock()
	removed := 0
	for _, key := range append([]string(nil), w.orderKeys...) {
		chunk, ok := w.unacked[key]
		if !ok || chunk.SessionID != sessionID || chunk.StreamKey != streamKey {
			continue
		}
		w.removeLocked(key)
		removed++
	}
	return removed
}

func (w *RuntimeDataWindow) removeLocked(key string) {
	chunk := w.unacked[key]
	delete(w.unacked, key)
	if w.perSession[chunk.SessionID] <= 1 {
		delete(w.perSession, chunk.SessionID)
	} else {
		w.perSession[chunk.SessionID]--
	}
	for i, existing := range w.orderKeys {
		if existing == key {
			w.orderKeys = append(w.orderKeys[:i], w.orderKeys[i+1:]...)
			break
		}
	}
}

func (w *RuntimeDataWindow) Expired(now time.Time, maxAge time.Duration) []RuntimeDataChunk {
	w.mu.Lock()
	defer w.mu.Unlock()
	var expired []RuntimeDataChunk
	for _, key := range w.orderKeys {
		chunk, ok := w.unacked[key]
		if !ok {
			continue
		}
		if now.Sub(chunk.SentAt) >= maxAge {
			expired = append(expired, chunk)
		}
	}
	sort.SliceStable(expired, func(i, j int) bool {
		if expired[i].SessionID != expired[j].SessionID {
			return expired[i].SessionID < expired[j].SessionID
		}
		if expired[i].StreamKey != expired[j].StreamKey {
			return expired[i].StreamKey < expired[j].StreamKey
		}
		return expired[i].Sequence < expired[j].Sequence
	})
	return expired
}

func dataWindowStreamKey(sessionID, streamKey string) string {
	return sessionID + "\x00" + streamKey
}

func dataWindowAckKey(sessionID, streamKey string, sequence int64) string {
	return dataWindowStreamKey(sessionID, streamKey) + "\x00" + strconv.FormatInt(sequence, 10)
}
