package runtimechannel

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	cpv1 "github.com/hushine-tech/control-panel-service/gen/controlpanelv1"
	portfoliov1 "github.com/hushine-tech/core-service/gen/portfoliov1"
)

func TestReplacedRuntimeConnectionCannotDispatchQueuedPlatformRequest(t *testing.T) {
	svc := NewWithInstanceID(&stubRepo{}, "cp-1")
	dispatcher := &countingPlatformDispatcher{}
	svc.SetPlatformDispatcher(dispatcher)
	now := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	runtime := AuthenticatedRuntime{UserID: 42, RuntimeID: "runtime-1", KeyID: "key-1"}
	old, err := svc.registry.Register(runtime, now)
	if err != nil {
		t.Fatalf("register old connection: %v", err)
	}
	old.setSender(func(*cpv1.RuntimeFrame) error { return nil })
	if _, err := svc.registry.Register(runtime, now); err != nil {
		t.Fatalf("register replacement connection: %v", err)
	}
	payload, err := anypb.New(&portfoliov1.GetSessionRequest{SessionId: "session-1"})
	if err != nil {
		t.Fatal(err)
	}

	svc.handleRuntimeRequest(context.Background(), old, &cpv1.RuntimeFrame{
		CorrelationId: "request-1",
		FrameType:     cpv1.FrameType_FRAME_TYPE_REQUEST,
		Payload: &cpv1.RuntimeFrame_Request{Request: &cpv1.StrategyRequest{
			Method:  "portfolio.GetSession",
			Request: payload,
		}},
	})

	if got := dispatcher.calls.Load(); got != 0 {
		t.Fatalf("stale connection platform dispatches = %d, want 0", got)
	}
}

func TestReplacingRuntimeConnectionCancelsActivePlatformRequest(t *testing.T) {
	svc := NewWithInstanceID(&stubRepo{}, "cp-1")
	dispatcher := &blockingPlatformDispatcher{
		entered: make(chan struct{}),
		exited:  make(chan struct{}),
	}
	svc.SetPlatformDispatcher(dispatcher)
	now := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	runtime := AuthenticatedRuntime{UserID: 42, RuntimeID: "runtime-1", KeyID: "key-1"}
	old, err := svc.registry.Register(runtime, now)
	if err != nil {
		t.Fatalf("register old connection: %v", err)
	}
	old.setSender(func(*cpv1.RuntimeFrame) error { return nil })
	payload, err := anypb.New(&portfoliov1.GetSessionRequest{SessionId: "session-1"})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		svc.handleRuntimeRequest(context.Background(), old, &cpv1.RuntimeFrame{
			CorrelationId: "request-1",
			FrameType:     cpv1.FrameType_FRAME_TYPE_REQUEST,
			Payload: &cpv1.RuntimeFrame_Request{Request: &cpv1.StrategyRequest{
				Method:  "portfolio.GetSession",
				Request: payload,
			}},
		})
	}()
	receiveSignal(t, dispatcher.entered, "platform request dispatch")
	if _, err := svc.registry.Register(runtime, now); err != nil {
		t.Fatalf("register replacement connection: %v", err)
	}
	receiveSignal(t, dispatcher.exited, "platform request cancellation")
	receiveSignal(t, done, "request handler exit")
}

func TestReplacedRuntimeConnectionCannotAckOrBackpressureReplacementIncomeAttempt(t *testing.T) {
	svc := NewWithInstanceID(&stubRepo{}, "cp-1")
	now := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	runtime := AuthenticatedRuntime{UserID: 42, RuntimeID: "runtime-1", KeyID: "key-1"}
	old, err := svc.registry.Register(runtime, now)
	if err != nil {
		t.Fatalf("register old connection: %v", err)
	}
	current, err := svc.registry.Register(runtime, now)
	if err != nil {
		t.Fatalf("register replacement connection: %v", err)
	}
	observer := &incomeDeliveryObserverStub{
		owner:        incomeDeliveryConnection(current.Runtime),
		attemptToken: "replacement-attempt-1",
		sequence:     11,
	}
	svc.SetIncomeDeliveryObserver(observer)
	if _, err := svc.incomeDataWindow.TrackAttempt("sess-1", "income/sess-1", 11, observer.attemptToken, nil, now); err != nil {
		t.Fatalf("track replacement attempt for ACK: %v", err)
	}
	ack := &cpv1.RuntimeFrame{Payload: &cpv1.RuntimeFrame_DataAck{DataAck: &cpv1.RuntimeDataAck{
		SessionId: "sess-1", StreamKey: "income/sess-1", Sequence: 11,
	}}}
	svc.handleRuntimeDataAck(old, ack)
	if observer.ackCount != 0 {
		t.Fatalf("stale connection ACK count = %d, want 0", observer.ackCount)
	}
	svc.handleRuntimeDataAck(current, ack)
	if observer.ackCount != 1 {
		t.Fatalf("current replacement ACK count = %d, want 1", observer.ackCount)
	}

	observer.attemptToken = "replacement-attempt-2"
	observer.sequence = 12
	if _, err := svc.incomeDataWindow.TrackAttempt("sess-1", "income/sess-1", 12, observer.attemptToken, nil, now); err != nil {
		t.Fatalf("track replacement attempt for backpressure: %v", err)
	}
	backpressure := &cpv1.RuntimeFrame{Payload: &cpv1.RuntimeFrame_DataBackpressure{DataBackpressure: &cpv1.RuntimeDataBackpressure{
		SessionId: "sess-1", StreamKey: "income/sess-1", ResumeAfterUnixMs: now.Add(time.Second).UnixMilli(),
	}}}
	svc.handleRuntimeDataBackpressure(context.Background(), old, backpressure)
	if observer.backpressureCount != 0 {
		t.Fatalf("stale connection backpressure count = %d, want 0", observer.backpressureCount)
	}
	svc.handleRuntimeDataBackpressure(context.Background(), current, backpressure)
	if observer.backpressureCount != 1 {
		t.Fatalf("current replacement backpressure count = %d, want 1", observer.backpressureCount)
	}
}

type countingPlatformDispatcher struct {
	calls atomic.Int64
}

func (d *countingPlatformDispatcher) DispatchRuntimeRequest(context.Context, AuthenticatedRuntime, string, *anypb.Any) (proto.Message, error) {
	d.calls.Add(1)
	return &portfoliov1.GetSessionResponse{}, nil
}

type blockingPlatformDispatcher struct {
	entered chan struct{}
	exited  chan struct{}
}

func (d *blockingPlatformDispatcher) DispatchRuntimeRequest(ctx context.Context, _ AuthenticatedRuntime, _ string, _ *anypb.Any) (proto.Message, error) {
	close(d.entered)
	<-ctx.Done()
	close(d.exited)
	return nil, ctx.Err()
}

func receiveSignal(t *testing.T, ch <-chan struct{}, description string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s", description)
	}
}
