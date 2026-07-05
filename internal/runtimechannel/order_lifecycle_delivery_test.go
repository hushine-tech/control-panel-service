package runtimechannel

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/hushine-tech/control-panel-service/internal/domain"
	orderv1 "github.com/hushine-tech/core-service/gen/orderv1"
)

type orderLifecycleSessionSourceStub struct {
	subs []domain.SessionMarketDataSubscription
}

func (s orderLifecycleSessionSourceStub) ListActiveSessionMarketDataSubscriptions(context.Context) ([]domain.SessionMarketDataSubscription, error) {
	return append([]domain.SessionMarketDataSubscription(nil), s.subs...), nil
}

type orderLifecycleClientStub struct {
	requests []*orderv1.ListOrderLifecycleEventsRequest
	events   map[string][]*orderv1.OrderLifecycleEventEntry
}

func (s *orderLifecycleClientStub) ListOrderLifecycleEvents(_ context.Context, req *orderv1.ListOrderLifecycleEventsRequest, _ ...grpc.CallOption) (*orderv1.ListOrderLifecycleEventsResponse, error) {
	s.requests = append(s.requests, req)
	rows := s.events[req.GetSessionId()]
	out := make([]*orderv1.OrderLifecycleEventEntry, 0, len(rows))
	for _, row := range rows {
		if row.GetEventId() > req.GetAfterEventId() {
			out = append(out, row)
		}
	}
	return &orderv1.ListOrderLifecycleEventsResponse{Events: out}, nil
}

type orderLifecycleDelivererStub struct {
	batches []OrderLifecycleDeliveryBatch
	err     error
}

func (s *orderLifecycleDelivererStub) DeliverOrderLifecycleBatch(_ context.Context, batch OrderLifecycleDeliveryBatch) error {
	if s.err != nil {
		return s.err
	}
	s.batches = append(s.batches, batch)
	return nil
}

func TestOrderLifecycleDeliveryWorkerDeliversAndAdvancesCursor(t *testing.T) {
	client := &orderLifecycleClientStub{events: map[string][]*orderv1.OrderLifecycleEventEntry{
		"sess-1": {
			{EventId: 10, SessionId: "sess-1", PortfolioId: 7, VenueId: 20, EventType: "fill"},
			{EventId: 11, SessionId: "sess-1", PortfolioId: 7, VenueId: 20, EventType: "terminal"},
		},
	}}
	deliverer := &orderLifecycleDelivererStub{}
	worker := NewOrderLifecycleDeliveryWorker(
		orderLifecycleSessionSourceStub{subs: []domain.SessionMarketDataSubscription{{
			UserID: 42, RuntimeID: "rt-1", SessionID: "sess-1",
		}}},
		client,
		deliverer,
		OrderLifecycleDeliveryConfig{PollInterval: time.Hour},
	)

	if err := worker.SyncOnce(context.Background()); err != nil {
		t.Fatalf("SyncOnce: %v", err)
	}
	if len(deliverer.batches) != 1 {
		t.Fatalf("batches = %d, want 1", len(deliverer.batches))
	}
	if deliverer.batches[0].UserID != 42 || deliverer.batches[0].RuntimeID != "rt-1" || deliverer.batches[0].SessionID != "sess-1" {
		t.Fatalf("batch route = %+v", deliverer.batches[0])
	}
	if deliverer.batches[0].Sequence != 11 {
		t.Fatalf("sequence = %d, want 11", deliverer.batches[0].Sequence)
	}

	if err := worker.SyncOnce(context.Background()); err != nil {
		t.Fatalf("second SyncOnce: %v", err)
	}
	if got := client.requests[len(client.requests)-1].GetAfterEventId(); got != 11 {
		t.Fatalf("after_event_id = %d, want 11", got)
	}
}

func TestOrderLifecycleDeliveryWorkerDoesNotAdvanceCursorWhenDeliveryFails(t *testing.T) {
	client := &orderLifecycleClientStub{events: map[string][]*orderv1.OrderLifecycleEventEntry{
		"sess-1": {{EventId: 10, SessionId: "sess-1", PortfolioId: 7, VenueId: 20, EventType: "fill"}},
	}}
	deliverer := &orderLifecycleDelivererStub{err: errors.New("stream unavailable")}
	worker := NewOrderLifecycleDeliveryWorker(
		orderLifecycleSessionSourceStub{subs: []domain.SessionMarketDataSubscription{{
			UserID: 42, RuntimeID: "rt-1", SessionID: "sess-1",
		}}},
		client,
		deliverer,
		OrderLifecycleDeliveryConfig{PollInterval: time.Hour},
	)

	if err := worker.SyncOnce(context.Background()); err == nil {
		t.Fatal("SyncOnce error = nil, want delivery error")
	}
	deliverer.err = nil
	if err := worker.SyncOnce(context.Background()); err != nil {
		t.Fatalf("retry SyncOnce: %v", err)
	}
	if len(deliverer.batches) != 1 {
		t.Fatalf("batches after retry = %d, want 1", len(deliverer.batches))
	}
	if got := client.requests[len(client.requests)-1].GetAfterEventId(); got != 0 {
		t.Fatalf("retry after_event_id = %d, want 0", got)
	}
}

func TestOrderLifecycleDeliveryWorkerDeduplicatesSessionRoutes(t *testing.T) {
	client := &orderLifecycleClientStub{events: map[string][]*orderv1.OrderLifecycleEventEntry{
		"sess-1": {{EventId: 10, SessionId: "sess-1", PortfolioId: 7, VenueId: 20, EventType: "fill"}},
	}}
	deliverer := &orderLifecycleDelivererStub{}
	worker := NewOrderLifecycleDeliveryWorker(
		orderLifecycleSessionSourceStub{subs: []domain.SessionMarketDataSubscription{
			{UserID: 42, RuntimeID: "rt-1", SessionID: "sess-1"},
			{UserID: 42, RuntimeID: "rt-1", SessionID: "sess-1"},
		}},
		client,
		deliverer,
		OrderLifecycleDeliveryConfig{PollInterval: time.Hour},
	)

	if err := worker.SyncOnce(context.Background()); err != nil {
		t.Fatalf("SyncOnce: %v", err)
	}
	if len(client.requests) != 1 {
		t.Fatalf("requests = %d, want 1", len(client.requests))
	}
	if len(deliverer.batches) != 1 {
		t.Fatalf("batches = %d, want 1", len(deliverer.batches))
	}
}

func TestOrderLifecycleDeliveryWorkerRunContinuesAfterTransientSyncError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := &flakyOrderLifecycleClient{
		events: []*orderv1.OrderLifecycleEventEntry{
			{EventId: 10, SessionId: "sess-1", PortfolioId: 7, VenueId: 20, EventType: "fill"},
		},
	}
	deliverer := &cancelOnDeliveryOrderLifecycleDeliverer{cancel: cancel}
	worker := NewOrderLifecycleDeliveryWorker(
		orderLifecycleSessionSourceStub{subs: []domain.SessionMarketDataSubscription{{
			UserID: 42, RuntimeID: "rt-1", SessionID: "sess-1",
		}}},
		client,
		deliverer,
		OrderLifecycleDeliveryConfig{PollInterval: time.Millisecond},
	)

	errCh := make(chan error, 1)
	go func() {
		errCh <- worker.Run(ctx)
	}()

	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		cancel()
		t.Fatal("worker did not continue to a successful delivery after transient error")
	}
	if got := client.calls(); got < 2 {
		t.Fatalf("client calls = %d, want at least 2", got)
	}
	if got := deliverer.count(); got != 1 {
		t.Fatalf("deliveries = %d, want 1", got)
	}
}

type flakyOrderLifecycleClient struct {
	mu     sync.Mutex
	called int
	events []*orderv1.OrderLifecycleEventEntry
}

func (s *flakyOrderLifecycleClient) ListOrderLifecycleEvents(_ context.Context, req *orderv1.ListOrderLifecycleEventsRequest, _ ...grpc.CallOption) (*orderv1.ListOrderLifecycleEventsResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.called++
	if s.called == 1 {
		return nil, errors.New("temporary order.v1 outage")
	}
	return &orderv1.ListOrderLifecycleEventsResponse{Events: s.events}, nil
}

func (s *flakyOrderLifecycleClient) calls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.called
}

type cancelOnDeliveryOrderLifecycleDeliverer struct {
	mu     sync.Mutex
	cancel context.CancelFunc
	n      int
}

func (s *cancelOnDeliveryOrderLifecycleDeliverer) DeliverOrderLifecycleBatch(_ context.Context, _ OrderLifecycleDeliveryBatch) error {
	s.mu.Lock()
	s.n++
	s.mu.Unlock()
	s.cancel()
	return nil
}

func (s *cancelOnDeliveryOrderLifecycleDeliverer) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.n
}
