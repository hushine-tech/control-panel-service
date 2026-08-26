package runtimechannel

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	portfoliov1 "github.com/hushine-tech/core-service/gen/portfoliov1"
)

func TestIncomeDeliverySendsOneSessionPage(t *testing.T) {
	source := &incomeSessionSourceStub{sessions: []IncomeDeliverySession{{
		UserID: 42, RuntimeID: "runtime-1", SessionID: "session-1", ConnectionID: "connection-1",
	}}}
	client := newIncomeClientStub(map[string][]*portfoliov1.VenueIncomeEntry{
		"session-1": {
			{IncomeEntryId: 10, SessionId: "session-1", Status: "confirmed"},
			{IncomeEntryId: 12, SessionId: "session-1", Status: "calculated"},
		},
	})
	deliverer := newIncomeDelivererStub()
	worker := NewIncomeDeliveryWorker(source, client, deliverer, IncomeDeliveryConfig{})

	if err := worker.SyncOnce(context.Background()); err != nil {
		t.Fatalf("SyncOnce: %v", err)
	}
	waitForIncomeAttempts(t, deliverer, 1)
	batch := deliverer.attemptsSnapshot()[0]
	if batch.UserID != 42 || batch.RuntimeID != "runtime-1" || batch.ConnectionID != "connection-1" || batch.SessionID != "session-1" {
		t.Fatalf("batch route = %+v", batch)
	}
	if batch.StreamKey != "income/session-1" || batch.Sequence != 12 || len(batch.Entries) != 2 {
		t.Fatalf("batch = %+v, want canonical stream, sequence 12, and two entries", batch)
	}
}

func TestIncomeDeliveryTwoSessionsProgressIndependently(t *testing.T) {
	source := &incomeSessionSourceStub{sessions: []IncomeDeliverySession{
		{UserID: 42, RuntimeID: "runtime-1", SessionID: "session-a", ConnectionID: "connection-1"},
		{UserID: 42, RuntimeID: "runtime-1", SessionID: "session-b", ConnectionID: "connection-1"},
	}}
	client := newIncomeClientStub(map[string][]*portfoliov1.VenueIncomeEntry{
		"session-a": {{IncomeEntryId: 10, SessionId: "session-a", Status: "confirmed"}},
		"session-b": {{IncomeEntryId: 20, SessionId: "session-b", Status: "confirmed"}},
	})
	deliverer := newIncomeDelivererStub()
	worker := NewIncomeDeliveryWorker(source, client, deliverer, IncomeDeliveryConfig{})

	if err := worker.SyncOnce(context.Background()); err != nil {
		t.Fatalf("SyncOnce: %v", err)
	}
	waitForIncomeAttempts(t, deliverer, 2)
	seen := map[string]int64{}
	for _, batch := range deliverer.attemptsSnapshot() {
		seen[batch.SessionID] = batch.Sequence
	}
	if seen["session-a"] != 10 || seen["session-b"] != 20 {
		t.Fatalf("delivered sequences = %+v, want independent session-a/session-b progress", seen)
	}
}

func TestIncomeDeliveryCursorAdvancesOnlyAfterMatchingAck(t *testing.T) {
	source := &incomeSessionSourceStub{sessions: []IncomeDeliverySession{{
		UserID: 42, RuntimeID: "runtime-1", SessionID: "session-1", ConnectionID: "connection-1",
	}}}
	client := newIncomeClientStub(map[string][]*portfoliov1.VenueIncomeEntry{
		"session-1": {{IncomeEntryId: 10, SessionId: "session-1", Status: "confirmed"}},
	})
	deliverer := newIncomeDelivererStub()
	worker := NewIncomeDeliveryWorker(source, client, deliverer, IncomeDeliveryConfig{})

	if err := worker.SyncOnce(context.Background()); err != nil {
		t.Fatalf("SyncOnce: %v", err)
	}
	waitForIncomeAttempts(t, deliverer, 1)
	worker.HandleIncomeAck(IncomeDeliveryConnection{UserID: 42, RuntimeID: "runtime-1", ConnectionID: "connection-1"}, "session-other", "income/session-1", 10)
	worker.HandleIncomeAck(IncomeDeliveryConnection{UserID: 42, RuntimeID: "runtime-1", ConnectionID: "connection-1"}, "session-1", "income/session-other", 10)
	worker.HandleIncomeAck(IncomeDeliveryConnection{UserID: 42, RuntimeID: "runtime-1", ConnectionID: "connection-1"}, "session-1", "income/session-1", 9)
	if err := worker.SyncOnce(context.Background()); err != nil {
		t.Fatalf("SyncOnce after stale ACKs: %v", err)
	}
	if got := client.latestAfter("session-1"); got != 0 {
		t.Fatalf("after_income_entry_id = %d after mismatched/stale ACKs, want 0", got)
	}

	worker.HandleIncomeAck(IncomeDeliveryConnection{UserID: 42, RuntimeID: "runtime-1", ConnectionID: "connection-1"}, "session-1", "income/session-1", 10)
	if err := worker.SyncOnce(context.Background()); err != nil {
		t.Fatalf("SyncOnce after matching ACK: %v", err)
	}
	waitForIncomeRequests(t, client, "session-1", 2)
	if got := client.latestAfter("session-1"); got != 10 {
		t.Fatalf("after_income_entry_id = %d after matching ACK, want 10", got)
	}
}

func TestIncomeDeliveryRejectsAckFromAnotherRuntimeConnection(t *testing.T) {
	source := &incomeSessionSourceStub{sessions: []IncomeDeliverySession{{
		UserID: 42, RuntimeID: "runtime-b", SessionID: "session-b", ConnectionID: "connection-b",
	}}}
	client := newIncomeClientStub(map[string][]*portfoliov1.VenueIncomeEntry{
		"session-b": {{IncomeEntryId: 10, SessionId: "session-b", Status: "confirmed"}},
	})
	deliverer := newIncomeDelivererStub()
	worker := NewIncomeDeliveryWorker(source, client, deliverer, IncomeDeliveryConfig{})
	if err := worker.SyncOnce(context.Background()); err != nil {
		t.Fatalf("SyncOnce: %v", err)
	}
	waitForIncomeAttempts(t, deliverer, 1)

	foreign := IncomeDeliveryConnection{UserID: 42, RuntimeID: "runtime-a", ConnectionID: "connection-a"}
	worker.HandleIncomeAck(foreign, "session-b", "income/session-b", 10)
	if err := worker.SyncOnce(context.Background()); err != nil {
		t.Fatalf("SyncOnce after foreign ACK: %v", err)
	}
	if got := client.latestAfter("session-b"); got != 0 {
		t.Fatalf("after_income_entry_id = %d after foreign ACK, want 0", got)
	}

	owner := IncomeDeliveryConnection{UserID: 42, RuntimeID: "runtime-b", ConnectionID: "connection-b"}
	worker.HandleIncomeAck(owner, "session-b", "income/session-b", 10)
	if err := worker.SyncOnce(context.Background()); err != nil {
		t.Fatalf("SyncOnce after owner ACK: %v", err)
	}
	waitForIncomeRequests(t, client, "session-b", 2)
	if got := client.latestAfter("session-b"); got != 10 {
		t.Fatalf("after_income_entry_id = %d after owner ACK, want 10", got)
	}
}

func TestIncomeDeliveryRejectsConflictingSessionRoutes(t *testing.T) {
	source := &incomeSessionSourceStub{sessions: []IncomeDeliverySession{
		{UserID: 42, RuntimeID: "runtime-a", SessionID: "session-1", ConnectionID: "connection-a"},
		{UserID: 42, RuntimeID: "runtime-b", SessionID: "session-1", ConnectionID: "connection-b"},
	}}
	client := newIncomeClientStub(map[string][]*portfoliov1.VenueIncomeEntry{
		"session-1": {{IncomeEntryId: 10, SessionId: "session-1", Status: "confirmed"}},
	})
	worker := NewIncomeDeliveryWorker(source, client, newIncomeDelivererStub(), IncomeDeliveryConfig{})

	if err := worker.SyncOnce(context.Background()); err == nil {
		t.Fatal("conflicting exact-Session routes error = nil")
	}
	if got := client.requestCount("session-1"); got != 0 {
		t.Fatalf("conflicting routes started %d Income requests, want 0", got)
	}
}

func TestIncomeDeliveryIgnoresDuplicatePageAfterAck(t *testing.T) {
	source := &incomeSessionSourceStub{sessions: []IncomeDeliverySession{{
		UserID: 42, RuntimeID: "runtime-1", SessionID: "session-1", ConnectionID: "connection-1",
	}}}
	client := newIncomeClientStub(map[string][]*portfoliov1.VenueIncomeEntry{
		"session-1": {{IncomeEntryId: 10, SessionId: "session-1", Status: "confirmed"}},
	})
	client.returnDuplicatePages = true
	deliverer := newIncomeDelivererStub()
	worker := NewIncomeDeliveryWorker(source, client, deliverer, IncomeDeliveryConfig{})

	if err := worker.SyncOnce(context.Background()); err != nil {
		t.Fatalf("SyncOnce: %v", err)
	}
	waitForIncomeAttempts(t, deliverer, 1)
	worker.HandleIncomeAck(IncomeDeliveryConnection{UserID: 42, RuntimeID: "runtime-1", ConnectionID: "connection-1"}, "session-1", "income/session-1", 10)
	if err := worker.SyncOnce(context.Background()); err != nil {
		t.Fatalf("SyncOnce duplicate page: %v", err)
	}
	waitForIncomeRequests(t, client, "session-1", 2)
	if got := len(deliverer.attemptsSnapshot()); got != 1 {
		t.Fatalf("delivery attempts = %d, want duplicate page suppressed", got)
	}
}

func TestIncomeDeliveryReconnectUsesWorkerCursor(t *testing.T) {
	cursor := int64(10)
	source := &incomeSessionSourceStub{sessions: []IncomeDeliverySession{{
		UserID: 42, RuntimeID: "runtime-1", SessionID: "session-1", ConnectionID: "connection-1",
	}}}
	client := newIncomeClientStub(map[string][]*portfoliov1.VenueIncomeEntry{
		"session-1": {
			{IncomeEntryId: 10, SessionId: "session-1", Status: "confirmed"},
			{IncomeEntryId: 11, SessionId: "session-1", Status: "confirmed"},
		},
	})
	deliverer := newIncomeDelivererStub()
	worker := NewIncomeDeliveryWorker(source, client, deliverer, IncomeDeliveryConfig{})

	if err := worker.SyncOnce(context.Background()); err != nil {
		t.Fatalf("SyncOnce: %v", err)
	}
	waitForIncomeAttempts(t, deliverer, 1)
	source.setSessions([]IncomeDeliverySession{{
		UserID: 42, RuntimeID: "runtime-1", SessionID: "session-1", ConnectionID: "connection-2", WorkerCursor: &cursor,
	}})
	if err := worker.SyncOnce(context.Background()); err != nil {
		t.Fatalf("SyncOnce after reconnect: %v", err)
	}
	waitForIncomeAttempts(t, deliverer, 2)
	if got := deliverer.attemptsSnapshot()[1].Entries[0].GetIncomeEntryId(); got != 11 {
		t.Fatalf("first replayed income_entry_id = %d, want 11 after Worker cursor 10", got)
	}
}

func TestIncomeDeliveryReconnectDiscardsPriorConnectionWindow(t *testing.T) {
	cursor := int64(10)
	source := &incomeSessionSourceStub{sessions: []IncomeDeliverySession{{
		UserID: 42, RuntimeID: "runtime-1", SessionID: "session-1", ConnectionID: "connection-1",
	}}}
	client := newIncomeClientStub(map[string][]*portfoliov1.VenueIncomeEntry{
		"session-1": {{IncomeEntryId: 10, SessionId: "session-1", Status: "confirmed"}},
	})
	deliverer := newIncomeDelivererStub()
	worker := NewIncomeDeliveryWorker(source, client, deliverer, IncomeDeliveryConfig{})
	if err := worker.SyncOnce(context.Background()); err != nil {
		t.Fatalf("SyncOnce: %v", err)
	}
	waitForIncomeAttempts(t, deliverer, 1)

	source.setSessions([]IncomeDeliverySession{{
		UserID: 42, RuntimeID: "runtime-1", SessionID: "session-1", ConnectionID: "connection-2", WorkerCursor: &cursor,
	}})
	if err := worker.SyncOnce(context.Background()); err != nil {
		t.Fatalf("SyncOnce after reconnect: %v", err)
	}
	if got := deliverer.resetCount("session-1"); got != 1 {
		t.Fatalf("stream resets = %d, want one reset on reconnect", got)
	}
}

func TestIncomeDeliveryReconnectWithoutWorkerCursorReplaysFromZero(t *testing.T) {
	source := &incomeSessionSourceStub{sessions: []IncomeDeliverySession{{
		UserID: 42, RuntimeID: "runtime-1", SessionID: "session-1", ConnectionID: "connection-1",
	}}}
	client := newIncomeClientStub(map[string][]*portfoliov1.VenueIncomeEntry{
		"session-1": {{IncomeEntryId: 10, SessionId: "session-1", Status: "confirmed"}},
	})
	deliverer := newIncomeDelivererStub()
	worker := NewIncomeDeliveryWorker(source, client, deliverer, IncomeDeliveryConfig{})

	if err := worker.SyncOnce(context.Background()); err != nil {
		t.Fatalf("SyncOnce: %v", err)
	}
	waitForIncomeAttempts(t, deliverer, 1)
	worker.HandleIncomeAck(IncomeDeliveryConnection{UserID: 42, RuntimeID: "runtime-1", ConnectionID: "connection-1"}, "session-1", "income/session-1", 10)
	source.setSessions([]IncomeDeliverySession{{
		UserID: 42, RuntimeID: "runtime-1", SessionID: "session-1", ConnectionID: "connection-2",
	}})
	if err := worker.SyncOnce(context.Background()); err != nil {
		t.Fatalf("SyncOnce after reconnect: %v", err)
	}
	waitForIncomeAttempts(t, deliverer, 2)
	if got := client.latestAfter("session-1"); got != 0 {
		t.Fatalf("after_income_entry_id = %d, want reconnect without Worker cursor to replay from zero", got)
	}
}

func TestIncomeDeliveryBackpressurePausesOnlyMatchingSession(t *testing.T) {
	now := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	source := &incomeSessionSourceStub{sessions: []IncomeDeliverySession{
		{UserID: 42, RuntimeID: "runtime-1", SessionID: "session-a", ConnectionID: "connection-1"},
		{UserID: 42, RuntimeID: "runtime-1", SessionID: "session-b", ConnectionID: "connection-1"},
	}}
	client := newIncomeClientStub(nil)
	deliverer := newIncomeDelivererStub()
	worker := NewIncomeDeliveryWorker(source, client, deliverer, IncomeDeliveryConfig{Now: func() time.Time { return now }})
	if err := worker.SyncOnce(context.Background()); err != nil {
		t.Fatalf("prime SyncOnce: %v", err)
	}
	waitForIncomeRequests(t, client, "session-a", 1)
	waitForIncomeRequests(t, client, "session-b", 1)
	client.setEntries(map[string][]*portfoliov1.VenueIncomeEntry{
		"session-a": {{IncomeEntryId: 10, SessionId: "session-a", Status: "confirmed"}},
		"session-b": {{IncomeEntryId: 20, SessionId: "session-b", Status: "confirmed"}},
	})
	worker.HandleIncomeBackpressure(IncomeDeliveryConnection{UserID: 42, RuntimeID: "runtime-1", ConnectionID: "connection-1"}, "session-a", "income/session-a", now.Add(10*time.Minute))
	if err := worker.SyncOnce(context.Background()); err != nil {
		t.Fatalf("SyncOnce under backpressure: %v", err)
	}
	waitForIncomeAttempts(t, deliverer, 1)
	if got := deliverer.attemptsSnapshot()[0].SessionID; got != "session-b" {
		t.Fatalf("delivered session = %q, want unblocked session-b", got)
	}
	now = now.Add(10 * time.Minute)
	if err := worker.SyncOnce(context.Background()); err != nil {
		t.Fatalf("SyncOnce after resume time: %v", err)
	}
	waitForIncomeAttempts(t, deliverer, 2)
}

func TestIncomeDeliveryRejectsBackpressureFromAnotherRuntimeConnection(t *testing.T) {
	now := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	source := &incomeSessionSourceStub{sessions: []IncomeDeliverySession{{
		UserID: 42, RuntimeID: "runtime-b", SessionID: "session-b", ConnectionID: "connection-b",
	}}}
	client := newIncomeClientStub(nil)
	deliverer := newIncomeDelivererStub()
	worker := NewIncomeDeliveryWorker(source, client, deliverer, IncomeDeliveryConfig{Now: func() time.Time { return now }})
	if err := worker.SyncOnce(context.Background()); err != nil {
		t.Fatalf("prime SyncOnce: %v", err)
	}
	waitForIncomeRequests(t, client, "session-b", 1)
	client.setEntries(map[string][]*portfoliov1.VenueIncomeEntry{
		"session-b": {{IncomeEntryId: 10, SessionId: "session-b", Status: "confirmed"}},
	})
	foreign := IncomeDeliveryConnection{UserID: 42, RuntimeID: "runtime-a", ConnectionID: "connection-a"}
	worker.HandleIncomeBackpressure(foreign, "session-b", "income/session-b", now.Add(10*time.Minute))
	if err := worker.SyncOnce(context.Background()); err != nil {
		t.Fatalf("SyncOnce after foreign backpressure: %v", err)
	}
	waitForIncomeAttempts(t, deliverer, 1)
}

func TestIncomeDeliveryMissingWorkerTenMinutesDoesNotCallPlatformApplication(t *testing.T) {
	now := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	source := &incomeSessionSourceStub{sessions: []IncomeDeliverySession{{
		UserID: 42, RuntimeID: "runtime-1", SessionID: "session-1", ConnectionID: "connection-1",
	}}}
	client := newIncomeClientStub(map[string][]*portfoliov1.VenueIncomeEntry{
		"session-1": {{IncomeEntryId: 10, SessionId: "session-1", Status: "confirmed"}},
	})
	deliverer := newIncomeDelivererStub()
	deliverer.block("session-1")
	worker := NewIncomeDeliveryWorker(source, client, deliverer, IncomeDeliveryConfig{Now: func() time.Time { return now }})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := worker.SyncOnce(ctx); err != nil {
		t.Fatalf("SyncOnce: %v", err)
	}
	waitForIncomeAttempts(t, deliverer, 1)
	now = now.Add(10 * time.Minute)
	if err := worker.SyncOnce(ctx); err != nil {
		t.Fatalf("SyncOnce after simulated ten minutes: %v", err)
	}
	if got := client.platformApplicationCalls.Load(); got != 0 {
		t.Fatalf("platform application calls from delivery = %d, want 0", got)
	}
	if got := len(deliverer.attemptsSnapshot()); got != 1 {
		t.Fatalf("blocked delivery attempts = %d, want one in-flight attempt", got)
	}
}

func TestIncomeDeliveryBlockedSessionDoesNotStarveAnother(t *testing.T) {
	source := &incomeSessionSourceStub{sessions: []IncomeDeliverySession{
		{UserID: 42, RuntimeID: "runtime-1", SessionID: "session-blocked", ConnectionID: "connection-1"},
		{UserID: 42, RuntimeID: "runtime-1", SessionID: "session-free", ConnectionID: "connection-1"},
	}}
	client := newIncomeClientStub(map[string][]*portfoliov1.VenueIncomeEntry{
		"session-blocked": {{IncomeEntryId: 10, SessionId: "session-blocked", Status: "confirmed"}},
		"session-free":    {{IncomeEntryId: 20, SessionId: "session-free", Status: "confirmed"}},
	})
	deliverer := newIncomeDelivererStub()
	deliverer.block("session-blocked")
	worker := NewIncomeDeliveryWorker(source, client, deliverer, IncomeDeliveryConfig{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := worker.SyncOnce(ctx); err != nil {
		t.Fatalf("SyncOnce: %v", err)
	}
	waitForIncomeAttempts(t, deliverer, 2)
	if !deliverer.completed("session-free") {
		t.Fatal("unblocked session did not complete while another Session was blocked")
	}
}

func TestIncomeDeliveryTerminalSessionReplays(t *testing.T) {
	source := &incomeSessionSourceStub{sessions: []IncomeDeliverySession{{
		UserID: 42, RuntimeID: "runtime-1", SessionID: "session-finished", ConnectionID: "connection-1", Status: "finished",
	}}}
	client := newIncomeClientStub(map[string][]*portfoliov1.VenueIncomeEntry{
		"session-finished": {{IncomeEntryId: 10, SessionId: "session-finished", Status: "confirmed"}},
	})
	deliverer := newIncomeDelivererStub()
	worker := NewIncomeDeliveryWorker(source, client, deliverer, IncomeDeliveryConfig{})

	if err := worker.SyncOnce(context.Background()); err != nil {
		t.Fatalf("SyncOnce: %v", err)
	}
	waitForIncomeAttempts(t, deliverer, 1)
	if got := deliverer.attemptsSnapshot()[0].SessionID; got != "session-finished" {
		t.Fatalf("replayed Session = %q, want terminal session-finished", got)
	}
}

func TestIncomeDeliveryRejectsOutOfOrderCorePage(t *testing.T) {
	_, _, err := deliverableIncomePage("session-1", 10, []*portfoliov1.VenueIncomeEntry{
		{IncomeEntryId: 11, SessionId: "session-1", Status: "confirmed"},
		{IncomeEntryId: 10, SessionId: "session-1", Status: "confirmed"},
		{IncomeEntryId: 12, SessionId: "session-1", Status: "confirmed"},
	})
	if err == nil {
		t.Fatal("out-of-order core page error = nil")
	}
}

func TestPortfolioIncomeDeliverySessionSourceIncludesTerminalPages(t *testing.T) {
	connectedAt := time.Date(2026, 8, 26, 12, 0, 0, 123, time.UTC)
	runtimes := incomeRuntimeSourceStub{runtimes: []AuthenticatedRuntime{{
		UserID: 42, RuntimeID: "runtime-1", AuthenticatedAt: connectedAt,
	}}}
	lister := &incomeSessionListerStub{pages: map[int32]*portfoliov1.ListSessionsResponse{
		0: {
			Sessions: []*portfoliov1.StrategySessionEntry{{
				SessionId: "session-finished", UserId: 42, RuntimeId: "runtime-1", Status: "finished",
			}},
			HasMore: true,
		},
		incomeSessionListPageSize: {
			Sessions: []*portfoliov1.StrategySessionEntry{{
				SessionId: "session-running", UserId: 42, RuntimeId: "runtime-1", Status: "running",
			}},
		},
	}}
	source := NewPortfolioIncomeDeliverySessionSource(runtimes, lister)

	routes, err := source.ListIncomeDeliverySessions(context.Background())
	if err != nil {
		t.Fatalf("ListIncomeDeliverySessions: %v", err)
	}
	if len(routes) != 2 || routes[0].SessionID != "session-finished" || routes[1].SessionID != "session-running" {
		t.Fatalf("routes = %+v, want terminal and running Session pages", routes)
	}
	if routes[0].ConnectionID == "" || routes[0].ConnectionID != routes[1].ConnectionID {
		t.Fatalf("connection IDs = %q/%q, want one non-empty authenticated connection identity", routes[0].ConnectionID, routes[1].ConnectionID)
	}
	if len(lister.requests) != 2 || lister.requests[0].GetUserId() != 42 || lister.requests[0].GetRuntimeId() != "runtime-1" || lister.requests[1].GetOffset() != incomeSessionListPageSize {
		t.Fatalf("ListSessions requests = %+v", lister.requests)
	}
}

func TestPortfolioIncomeDeliverySessionSourceRejectsOwnershipMismatch(t *testing.T) {
	runtimes := incomeRuntimeSourceStub{runtimes: []AuthenticatedRuntime{{
		UserID: 42, RuntimeID: "runtime-1", AuthenticatedAt: time.Now().UTC(),
	}}}
	lister := &incomeSessionListerStub{pages: map[int32]*portfoliov1.ListSessionsResponse{
		0: {Sessions: []*portfoliov1.StrategySessionEntry{{
			SessionId: "session-other", UserId: 42, RuntimeId: "runtime-other", Status: "finished",
		}}},
	}}
	source := NewPortfolioIncomeDeliverySessionSource(runtimes, lister)

	if _, err := source.ListIncomeDeliverySessions(context.Background()); err == nil {
		t.Fatal("ownership-mismatched Session source error = nil")
	}
}

func TestRuntimeDataWindowBackpressureIsIsolatedPerSession(t *testing.T) {
	window := NewSessionRuntimeDataWindow(1)
	now := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	if _, err := window.Track("session-blocked", "income/session-blocked", 10, nil, now); err != nil {
		t.Fatalf("Track blocked Session: %v", err)
	}
	if _, err := window.Track("session-free", "income/session-free", 20, nil, now); err != nil {
		t.Fatalf("Track independent Session: %v", err)
	}
	if _, err := window.Track("session-blocked", "income/session-blocked", 11, nil, now); !errors.Is(err, ErrRuntimeDataBackpressure) {
		t.Fatalf("second blocked-Session Track error = %v, want ErrRuntimeDataBackpressure", err)
	}
}

type incomeSessionSourceStub struct {
	mu       sync.Mutex
	sessions []IncomeDeliverySession
}

type incomeRuntimeSourceStub struct {
	runtimes []AuthenticatedRuntime
}

func (s incomeRuntimeSourceStub) RegistrySnapshot() []AuthenticatedRuntime {
	return append([]AuthenticatedRuntime(nil), s.runtimes...)
}

type incomeSessionListerStub struct {
	pages    map[int32]*portfoliov1.ListSessionsResponse
	requests []*portfoliov1.ListSessionsRequest
}

func (s *incomeSessionListerStub) ListSessions(_ context.Context, req *portfoliov1.ListSessionsRequest, _ ...grpc.CallOption) (*portfoliov1.ListSessionsResponse, error) {
	s.requests = append(s.requests, proto.Clone(req).(*portfoliov1.ListSessionsRequest))
	if page := s.pages[req.GetOffset()]; page != nil {
		return page, nil
	}
	return &portfoliov1.ListSessionsResponse{}, nil
}

func (s *incomeSessionSourceStub) ListIncomeDeliverySessions(context.Context) ([]IncomeDeliverySession, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]IncomeDeliverySession(nil), s.sessions...), nil
}

func (s *incomeSessionSourceStub) setSessions(sessions []IncomeDeliverySession) {
	s.mu.Lock()
	s.sessions = append([]IncomeDeliverySession(nil), sessions...)
	s.mu.Unlock()
}

type incomeClientStub struct {
	mu                       sync.Mutex
	entries                  map[string][]*portfoliov1.VenueIncomeEntry
	requests                 map[string][]*portfoliov1.ListVenueIncomeEntriesRequest
	returnDuplicatePages     bool
	platformApplicationCalls atomic.Int64
}

func newIncomeClientStub(entries map[string][]*portfoliov1.VenueIncomeEntry) *incomeClientStub {
	stub := &incomeClientStub{requests: map[string][]*portfoliov1.ListVenueIncomeEntriesRequest{}}
	stub.setEntries(entries)
	return stub
}

func (s *incomeClientStub) setEntries(entries map[string][]*portfoliov1.VenueIncomeEntry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries = map[string][]*portfoliov1.VenueIncomeEntry{}
	for sessionID, rows := range entries {
		for _, row := range rows {
			s.entries[sessionID] = append(s.entries[sessionID], row)
		}
	}
}

func (s *incomeClientStub) ListVenueIncomeEntries(_ context.Context, req *portfoliov1.ListVenueIncomeEntriesRequest, _ ...grpc.CallOption) (*portfoliov1.ListVenueIncomeEntriesResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requests[req.GetSessionId()] = append(s.requests[req.GetSessionId()], proto.Clone(req).(*portfoliov1.ListVenueIncomeEntriesRequest))
	rows := s.entries[req.GetSessionId()]
	response := &portfoliov1.ListVenueIncomeEntriesResponse{NextAfterIncomeEntryId: req.GetAfterIncomeEntryId()}
	for _, row := range rows {
		if !s.returnDuplicatePages && row.GetIncomeEntryId() <= req.GetAfterIncomeEntryId() {
			continue
		}
		response.Entries = append(response.Entries, row)
		response.NextAfterIncomeEntryId = row.GetIncomeEntryId()
	}
	return response, nil
}

func (s *incomeClientStub) latestAfter(sessionID string) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	requests := s.requests[sessionID]
	if len(requests) == 0 {
		return -1
	}
	return requests[len(requests)-1].GetAfterIncomeEntryId()
}

func (s *incomeClientStub) requestCount(sessionID string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.requests[sessionID])
}

type incomeDelivererStub struct {
	mu                sync.Mutex
	attempts          []IncomeDeliveryBatch
	completedSessions map[string]bool
	blocked           map[string]chan struct{}
	resets            map[string]int
}

func newIncomeDelivererStub() *incomeDelivererStub {
	return &incomeDelivererStub{
		completedSessions: map[string]bool{},
		blocked:           map[string]chan struct{}{},
		resets:            map[string]int{},
	}
}

func (s *incomeDelivererStub) ResetIncomeDeliveryStream(sessionID, _ string) {
	s.mu.Lock()
	s.resets[sessionID]++
	s.mu.Unlock()
}

func (s *incomeDelivererStub) resetCount(sessionID string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.resets[sessionID]
}

func (s *incomeDelivererStub) block(sessionID string) {
	s.mu.Lock()
	s.blocked[sessionID] = make(chan struct{})
	s.mu.Unlock()
}

func (s *incomeDelivererStub) DeliverIncomeBatch(ctx context.Context, batch IncomeDeliveryBatch) error {
	s.mu.Lock()
	s.attempts = append(s.attempts, batch)
	blocked := s.blocked[batch.SessionID]
	s.mu.Unlock()
	if blocked != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-blocked:
		}
	}
	s.mu.Lock()
	s.completedSessions[batch.SessionID] = true
	s.mu.Unlock()
	return nil
}

func (s *incomeDelivererStub) attemptsSnapshot() []IncomeDeliveryBatch {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]IncomeDeliveryBatch(nil), s.attempts...)
}

func (s *incomeDelivererStub) completed(sessionID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.completedSessions[sessionID]
}

func waitForIncomeAttempts(t *testing.T, deliverer *incomeDelivererStub, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if len(deliverer.attemptsSnapshot()) >= want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("delivery attempts = %d, want at least %d", len(deliverer.attemptsSnapshot()), want)
}

func waitForIncomeRequests(t *testing.T, client *incomeClientStub, sessionID string, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if client.requestCount(sessionID) >= want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("%s requests = %d, want at least %d", sessionID, client.requestCount(sessionID), want)
}
