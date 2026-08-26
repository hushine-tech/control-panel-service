package runtimechannel

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"

	portfoliov1 "github.com/hushine-tech/core-service/gen/portfoliov1"
)

const (
	defaultIncomeDeliveryPollInterval = time.Second
	defaultIncomeDeliveryPageSize     = int32(100)
	maxIncomeDeliveryPageSize         = int32(500)
	incomeSessionListPageSize         = int32(200)
)

type IncomeDeliverySession struct {
	UserID       int64
	RuntimeID    string
	SessionID    string
	ConnectionID string
	Status       string
	WorkerCursor *int64
}

type IncomeDeliverySessionSource interface {
	ListIncomeDeliverySessions(ctx context.Context) ([]IncomeDeliverySession, error)
}

type IncomeDeliveryClient interface {
	ListVenueIncomeEntries(ctx context.Context, in *portfoliov1.ListVenueIncomeEntriesRequest, opts ...grpc.CallOption) (*portfoliov1.ListVenueIncomeEntriesResponse, error)
}

type IncomeDeliveryStreamResetter interface {
	ResetIncomeDeliveryStream(sessionID, streamKey string)
}

type IncomeDeliveryConfig struct {
	PollInterval time.Duration
	PageSize     int32
	Now          func() time.Time
}

type IncomeDeliveryWorker struct {
	sessions  IncomeDeliverySessionSource
	client    IncomeDeliveryClient
	deliverer IncomeBatchDeliverer
	cfg       IncomeDeliveryConfig

	mu     sync.Mutex
	states map[string]*incomeDeliveryState
}

type incomeDeliveryState struct {
	route             IncomeDeliverySession
	generation        uint64
	cursor            int64
	delivering        bool
	inflightSequence  int64
	inflightCursor    int64
	backpressureUntil time.Time
	cancel            context.CancelFunc
}

func NewIncomeDeliveryWorker(
	sessions IncomeDeliverySessionSource,
	client IncomeDeliveryClient,
	deliverer IncomeBatchDeliverer,
	cfg IncomeDeliveryConfig,
) *IncomeDeliveryWorker {
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = defaultIncomeDeliveryPollInterval
	}
	if cfg.PageSize <= 0 {
		cfg.PageSize = defaultIncomeDeliveryPageSize
	}
	if cfg.PageSize > maxIncomeDeliveryPageSize {
		cfg.PageSize = maxIncomeDeliveryPageSize
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &IncomeDeliveryWorker{
		sessions:  sessions,
		client:    client,
		deliverer: deliverer,
		cfg:       cfg,
		states:    map[string]*incomeDeliveryState{},
	}
}

func (w *IncomeDeliveryWorker) Run(ctx context.Context) error {
	if w == nil || w.sessions == nil || w.client == nil || w.deliverer == nil {
		return nil
	}
	ticker := time.NewTicker(w.cfg.PollInterval)
	defer ticker.Stop()
	for {
		if err := w.SyncOnce(ctx); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			log.Printf("Income RuntimeChannel delivery discovery failed: %v", err)
		}
		select {
		case <-ctx.Done():
			w.cancelAll()
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (w *IncomeDeliveryWorker) SyncOnce(ctx context.Context) error {
	if w == nil || w.sessions == nil || w.client == nil || w.deliverer == nil {
		return nil
	}
	routes, err := w.sessions.ListIncomeDeliverySessions(ctx)
	if err != nil {
		return err
	}
	normalized := make([]IncomeDeliverySession, 0, len(routes))
	routesBySession := make(map[string]IncomeDeliverySession, len(routes))
	for _, route := range routes {
		route.RuntimeID = strings.TrimSpace(route.RuntimeID)
		route.SessionID = strings.TrimSpace(route.SessionID)
		route.ConnectionID = strings.TrimSpace(route.ConnectionID)
		if route.UserID <= 0 || route.RuntimeID == "" || route.SessionID == "" {
			continue
		}
		if route.ConnectionID == "" {
			route.ConnectionID = route.RuntimeID
		}
		if previous, duplicate := routesBySession[route.SessionID]; duplicate {
			if previous.UserID != route.UserID || previous.RuntimeID != route.RuntimeID || previous.ConnectionID != route.ConnectionID {
				return fmt.Errorf("Session %q has conflicting Income delivery routes", route.SessionID)
			}
			continue
		}
		routesBySession[route.SessionID] = route
		normalized = append(normalized, route)
	}
	seen := make(map[string]struct{}, len(normalized))
	for _, route := range normalized {
		seen[route.SessionID] = struct{}{}
		w.startSessionSync(ctx, route)
	}
	w.removeAbsentSessions(seen)
	return nil
}

func (w *IncomeDeliveryWorker) startSessionSync(ctx context.Context, route IncomeDeliverySession) {
	w.mu.Lock()
	var resetter IncomeDeliveryStreamResetter
	state := w.states[route.SessionID]
	if state == nil {
		state = &incomeDeliveryState{
			route:      route,
			generation: 1,
			cursor:     initialIncomeCursor(route.WorkerCursor),
		}
		w.states[route.SessionID] = state
	} else if incomeConnectionChanged(state.route, route) {
		if state.cancel != nil {
			state.cancel()
		}
		state.route = route
		state.generation++
		state.cursor = initialIncomeCursor(route.WorkerCursor)
		state.delivering = false
		state.inflightSequence = 0
		state.inflightCursor = 0
		state.backpressureUntil = time.Time{}
		state.cancel = nil
		resetter, _ = w.deliverer.(IncomeDeliveryStreamResetter)
	} else {
		state.route = route
	}
	if state.delivering || state.inflightSequence != 0 || w.cfg.Now().UTC().Before(state.backpressureUntil) {
		w.mu.Unlock()
		return
	}
	deliveryCtx, cancel := context.WithCancel(ctx)
	state.delivering = true
	state.cancel = cancel
	generation := state.generation
	cursor := state.cursor
	w.mu.Unlock()

	if resetter != nil {
		resetter.ResetIncomeDeliveryStream(route.SessionID, incomeStreamKey(route.SessionID))
	}
	go w.syncSession(deliveryCtx, route, generation, cursor)
}

func (w *IncomeDeliveryWorker) syncSession(ctx context.Context, route IncomeDeliverySession, generation uint64, cursor int64) {
	resp, err := w.client.ListVenueIncomeEntries(ctx, &portfoliov1.ListVenueIncomeEntriesRequest{
		SessionId:          route.SessionID,
		AfterIncomeEntryId: cursor,
		Limit:              w.cfg.PageSize,
		UserId:             route.UserID,
	})
	if err != nil {
		w.finishSessionSync(route.SessionID, generation, 0, err)
		return
	}
	entries, sequence, err := deliverableIncomePage(route.SessionID, cursor, resp.GetEntries())
	if err != nil {
		w.finishSessionSync(route.SessionID, generation, 0, err)
		return
	}
	if len(entries) == 0 {
		w.finishSessionSync(route.SessionID, generation, 0, nil)
		return
	}

	w.mu.Lock()
	state := w.states[route.SessionID]
	if state == nil || state.generation != generation || state.cursor != cursor {
		w.mu.Unlock()
		return
	}
	state.inflightSequence = sequence
	state.inflightCursor = sequence
	w.mu.Unlock()

	err = w.deliverer.DeliverIncomeBatch(ctx, IncomeDeliveryBatch{
		UserID:       route.UserID,
		RuntimeID:    route.RuntimeID,
		ConnectionID: route.ConnectionID,
		SessionID:    route.SessionID,
		StreamKey:    incomeStreamKey(route.SessionID),
		Sequence:     sequence,
		Entries:      entries,
	})
	w.finishSessionSync(route.SessionID, generation, sequence, err)
}

func (w *IncomeDeliveryWorker) finishSessionSync(sessionID string, generation uint64, sequence int64, err error) {
	w.mu.Lock()
	state := w.states[sessionID]
	if state == nil || state.generation != generation {
		w.mu.Unlock()
		return
	}
	if state.cancel != nil {
		state.cancel()
		state.cancel = nil
	}
	state.delivering = false
	if err != nil && sequence != 0 && state.inflightSequence == sequence {
		state.inflightSequence = 0
		state.inflightCursor = 0
	}
	w.mu.Unlock()
	if err != nil && ctxErrorCode(err) == "" {
		log.Printf("Income RuntimeChannel delivery failed session=%s: %v", sessionID, err)
	}
}

func (w *IncomeDeliveryWorker) OwnsIncomeDelivery(connection IncomeDeliveryConnection, sessionID, streamKey string, sequence int64) bool {
	if w == nil || streamKey != incomeStreamKey(sessionID) || sequence <= 0 {
		return false
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	state := w.states[sessionID]
	return incomeConnectionOwnsState(connection, state) && state.inflightSequence == sequence && state.inflightCursor == sequence
}

func (w *IncomeDeliveryWorker) OwnsIncomeStream(connection IncomeDeliveryConnection, sessionID, streamKey string) bool {
	if w == nil || streamKey != incomeStreamKey(sessionID) {
		return false
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return incomeConnectionOwnsState(connection, w.states[sessionID])
}

func (w *IncomeDeliveryWorker) HandleIncomeAck(connection IncomeDeliveryConnection, sessionID, streamKey string, sequence int64) {
	if w == nil || streamKey != incomeStreamKey(sessionID) || sequence <= 0 {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	state := w.states[sessionID]
	if !incomeConnectionOwnsState(connection, state) || state.inflightSequence != sequence || state.inflightCursor != sequence {
		return
	}
	state.cursor = sequence
	state.inflightSequence = 0
	state.inflightCursor = 0
}

func (w *IncomeDeliveryWorker) HandleIncomeBackpressure(connection IncomeDeliveryConnection, sessionID, streamKey string, resumeAfter time.Time) {
	if w == nil || streamKey != incomeStreamKey(sessionID) {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	state := w.states[sessionID]
	if !incomeConnectionOwnsState(connection, state) {
		return
	}
	state.backpressureUntil = resumeAfter.UTC()
	state.inflightSequence = 0
	state.inflightCursor = 0
}

func (w *IncomeDeliveryWorker) removeAbsentSessions(seen map[string]struct{}) {
	w.mu.Lock()
	var removed []string
	for sessionID, state := range w.states {
		if _, ok := seen[sessionID]; ok {
			continue
		}
		if state.cancel != nil {
			state.cancel()
		}
		delete(w.states, sessionID)
		removed = append(removed, sessionID)
	}
	w.mu.Unlock()
	if resetter, ok := w.deliverer.(IncomeDeliveryStreamResetter); ok {
		for _, sessionID := range removed {
			resetter.ResetIncomeDeliveryStream(sessionID, incomeStreamKey(sessionID))
		}
	}
}

func (w *IncomeDeliveryWorker) cancelAll() {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, state := range w.states {
		if state.cancel != nil {
			state.cancel()
			state.cancel = nil
		}
	}
}

func deliverableIncomePage(sessionID string, cursor int64, rows []*portfoliov1.VenueIncomeEntry) ([]*portfoliov1.VenueIncomeEntry, int64, error) {
	entries := make([]*portfoliov1.VenueIncomeEntry, 0, len(rows))
	lastID := cursor
	previousPageID := int64(0)
	for _, entry := range rows {
		if entry == nil {
			return nil, 0, fmt.Errorf("Income page contains a nil entry")
		}
		if entry.GetIncomeEntryId() <= previousPageID {
			return nil, 0, fmt.Errorf("Income page is not strictly ascending after %d", previousPageID)
		}
		previousPageID = entry.GetIncomeEntryId()
		if entry.GetIncomeEntryId() <= cursor {
			continue
		}
		if entry.GetSessionId() != sessionID {
			return nil, 0, fmt.Errorf("Income page entry %d belongs to Session %q, want %q", entry.GetIncomeEntryId(), entry.GetSessionId(), sessionID)
		}
		if entry.GetIncomeEntryId() <= lastID {
			return nil, 0, fmt.Errorf("Income page is not strictly ascending after %d", lastID)
		}
		switch entry.GetStatus() {
		case "confirmed", "calculated":
		default:
			return nil, 0, fmt.Errorf("Income page entry %d is not deliverable: %s", entry.GetIncomeEntryId(), entry.GetStatus())
		}
		entries = append(entries, entry)
		lastID = entry.GetIncomeEntryId()
	}
	return entries, lastID, nil
}

func incomeStreamKey(sessionID string) string {
	return "income/" + sessionID
}

func initialIncomeCursor(cursor *int64) int64 {
	if cursor == nil || *cursor < 0 {
		return 0
	}
	return *cursor
}

func incomeConnectionChanged(previous, next IncomeDeliverySession) bool {
	return previous.UserID != next.UserID || previous.RuntimeID != next.RuntimeID || previous.ConnectionID != next.ConnectionID
}

func incomeConnectionOwnsState(connection IncomeDeliveryConnection, state *incomeDeliveryState) bool {
	return state != nil &&
		connection.UserID == state.route.UserID &&
		connection.RuntimeID == state.route.RuntimeID &&
		connection.ConnectionID == state.route.ConnectionID
}

func incomeDeliveryConnection(rt AuthenticatedRuntime) IncomeDeliveryConnection {
	connectionID := strings.TrimSpace(rt.ConnectionID)
	if connectionID == "" {
		connectionID = rt.RuntimeID + "/" + strconv.FormatInt(rt.AuthenticatedAt.UnixNano(), 10)
	}
	return IncomeDeliveryConnection{
		UserID:       rt.UserID,
		RuntimeID:    rt.RuntimeID,
		ConnectionID: connectionID,
	}
}

func ctxErrorCode(err error) string {
	if errors.Is(err, context.Canceled) {
		return "canceled"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "deadline"
	}
	return ""
}

type IncomeDeliveryRuntimeSource interface {
	RegistrySnapshot() []AuthenticatedRuntime
}

type IncomeSessionLister interface {
	ListSessions(ctx context.Context, in *portfoliov1.ListSessionsRequest, opts ...grpc.CallOption) (*portfoliov1.ListSessionsResponse, error)
}

type PortfolioIncomeDeliverySessionSource struct {
	runtimes IncomeDeliveryRuntimeSource
	client   IncomeSessionLister
}

func NewPortfolioIncomeDeliverySessionSource(runtimes IncomeDeliveryRuntimeSource, client IncomeSessionLister) *PortfolioIncomeDeliverySessionSource {
	return &PortfolioIncomeDeliverySessionSource{runtimes: runtimes, client: client}
}

func (s *PortfolioIncomeDeliverySessionSource) ListIncomeDeliverySessions(ctx context.Context) ([]IncomeDeliverySession, error) {
	if s == nil || s.runtimes == nil || s.client == nil {
		return nil, nil
	}
	var routes []IncomeDeliverySession
	for _, runtime := range s.runtimes.RegistrySnapshot() {
		if runtime.UserID <= 0 || strings.TrimSpace(runtime.RuntimeID) == "" {
			continue
		}
		connectionID := incomeDeliveryConnection(runtime).ConnectionID
		for offset := int32(0); ; offset += incomeSessionListPageSize {
			resp, err := s.client.ListSessions(ctx, &portfoliov1.ListSessionsRequest{
				UserId:    runtime.UserID,
				RuntimeId: runtime.RuntimeID,
				Limit:     incomeSessionListPageSize,
				Offset:    offset,
			})
			if err != nil {
				return nil, err
			}
			for _, session := range resp.GetSessions() {
				if session == nil || session.GetUserId() != runtime.UserID || session.GetRuntimeId() != runtime.RuntimeID {
					return nil, fmt.Errorf("core-service returned a Session outside authenticated runtime ownership")
				}
				routes = append(routes, IncomeDeliverySession{
					UserID:       runtime.UserID,
					RuntimeID:    runtime.RuntimeID,
					SessionID:    session.GetSessionId(),
					ConnectionID: connectionID,
					Status:       session.GetStatus(),
				})
			}
			if !resp.GetHasMore() {
				break
			}
			if len(resp.GetSessions()) == 0 {
				return nil, fmt.Errorf("core-service returned has_more with an empty Session page")
			}
		}
	}
	return routes, nil
}
