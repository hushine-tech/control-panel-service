package runtimechannel

import (
	"context"
	"testing"
	"time"

	cpv1 "github.com/hushine-tech/control-panel-service/gen/controlpanelv1"
	cpnotify "github.com/hushine-tech/control-panel-service/internal/notification"
)

func TestRuntimeChannelDataBackpressurePublishesSlowConsumerNotification(t *testing.T) {
	pub := &captureBackpressurePublisher{}
	svc := NewWithConfig(nil, Config{NotificationPublisher: pub})
	stream := registerRuntimeStreamForTest(t, svc, AuthenticatedRuntime{
		UserID:    42,
		RuntimeID: "rt-1",
		Name:      "debug-runtime",
	})

	svc.handleRuntimeDataBackpressure(context.Background(), stream, &cpv1.RuntimeFrame{
		FrameType: cpv1.FrameType_FRAME_TYPE_DATA_BACKPRESSURE,
		Payload: &cpv1.RuntimeFrame_DataBackpressure{
			DataBackpressure: &cpv1.RuntimeDataBackpressure{
				SessionId: "sess-1",
				StreamKey: "binance/futures/kline/ETHUSDT/1m",
				Reason:    "slow_consumer: kind=live_kline queue_depth=2 dropped=0",
			},
		},
	})

	if len(pub.events) != 1 {
		t.Fatalf("events = %d, want 1", len(pub.events))
	}
	event := pub.events[0]
	if event.EventType != cpnotify.EventRuntimeDataDelayed || event.Severity != cpnotify.SeverityWarn {
		t.Fatalf("event type/severity = %s/%s", event.EventType, event.Severity)
	}
	if event.UserID != 42 || event.RuntimeID != "rt-1" || event.SessionID != "sess-1" {
		t.Fatalf("event route = %+v", event)
	}
	if event.Metadata["stream_key"] != "binance/futures/kline/ETHUSDT/1m" {
		t.Fatalf("stream metadata = %q", event.Metadata["stream_key"])
	}
}

func TestRuntimeChannelIncomeBackpressureRequiresOwningConnection(t *testing.T) {
	pub := &captureBackpressurePublisher{}
	svc := NewWithConfig(nil, Config{NotificationPublisher: pub})
	owner := AuthenticatedRuntime{
		UserID: 42, RuntimeID: "rt-owner", AuthenticatedAt: time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC),
	}
	ownerStream := registerRuntimeStreamForTest(t, svc, owner)
	observer := &incomeDeliveryObserverStub{owner: incomeDeliveryConnection(ownerStream.Runtime), attemptToken: "attempt-owner", sequence: 11}
	svc.SetIncomeDeliveryObserver(observer)
	if _, err := svc.incomeDataWindow.TrackAttempt("sess-1", "income/sess-1", 11, observer.attemptToken, nil, owner.AuthenticatedAt); err != nil {
		t.Fatalf("track owner Income attempt: %v", err)
	}
	frame := &cpv1.RuntimeFrame{
		FrameType: cpv1.FrameType_FRAME_TYPE_DATA_BACKPRESSURE,
		Payload: &cpv1.RuntimeFrame_DataBackpressure{DataBackpressure: &cpv1.RuntimeDataBackpressure{
			SessionId: "sess-1", StreamKey: "income/sess-1", ResumeAfterUnixMs: 1,
		}},
	}

	foreign := registerRuntimeStreamForTest(t, svc, AuthenticatedRuntime{
		UserID: 42, RuntimeID: "rt-foreign", AuthenticatedAt: owner.AuthenticatedAt,
	})
	svc.handleRuntimeDataBackpressure(context.Background(), foreign, frame)
	if observer.backpressureCount != 0 || len(pub.events) != 0 {
		t.Fatalf("foreign backpressure observer/events = %d/%d, want 0/0", observer.backpressureCount, len(pub.events))
	}

	unregistered := newRuntimeStream(AuthenticatedRuntime{
		UserID: 7, RuntimeID: owner.RuntimeID, AuthenticatedAt: owner.AuthenticatedAt,
	}, owner.AuthenticatedAt)
	svc.handleRuntimeDataBackpressure(context.Background(), unregistered, frame)
	replacement := registerRuntimeStreamForTest(t, svc, owner)
	svc.handleRuntimeDataBackpressure(context.Background(), ownerStream, frame)
	if observer.backpressureCount != 0 || len(pub.events) != 0 {
		t.Fatalf("foreign-user/stale-connection backpressure observer/events = %d/%d, want 0/0", observer.backpressureCount, len(pub.events))
	}

	observer.owner = incomeDeliveryConnection(replacement.Runtime)
	observer.attemptToken = "attempt-replacement"
	svc.incomeDataWindow.ForgetAttempt("sess-1", "income/sess-1", 11, "attempt-owner")
	if _, err := svc.incomeDataWindow.TrackAttempt("sess-1", "income/sess-1", 11, observer.attemptToken, nil, owner.AuthenticatedAt); err != nil {
		t.Fatalf("track replacement Income attempt: %v", err)
	}
	svc.handleRuntimeDataBackpressure(context.Background(), replacement, frame)
	if observer.backpressureCount != 1 || len(pub.events) != 1 {
		t.Fatalf("owner backpressure observer/events = %d/%d, want 1/1", observer.backpressureCount, len(pub.events))
	}
}

func TestRuntimeChannelDataBackpressurePublishesDroppedNotification(t *testing.T) {
	pub := &captureBackpressurePublisher{}
	svc := NewWithConfig(nil, Config{NotificationPublisher: pub})
	stream := registerRuntimeStreamForTest(t, svc, AuthenticatedRuntime{
		UserID:    42,
		RuntimeID: "rt-1",
		Name:      "debug-runtime",
	})

	svc.handleRuntimeDataBackpressure(context.Background(), stream, &cpv1.RuntimeFrame{
		FrameType: cpv1.FrameType_FRAME_TYPE_DATA_BACKPRESSURE,
		Payload: &cpv1.RuntimeFrame_DataBackpressure{
			DataBackpressure: &cpv1.RuntimeDataBackpressure{
				SessionId: "sess-1",
				StreamKey: "order_lifecycle",
				Reason:    "data_dropped: kind=order_update queue_depth=2048 dropped=1",
			},
		},
	})

	if len(pub.events) != 1 {
		t.Fatalf("events = %d, want 1", len(pub.events))
	}
	event := pub.events[0]
	if event.EventType != cpnotify.EventRuntimeDataDropped || event.Severity != cpnotify.SeverityWarn {
		t.Fatalf("event type/severity = %s/%s", event.EventType, event.Severity)
	}
	if event.Metadata["reason"] != "data_dropped: kind=order_update queue_depth=2048 dropped=1" {
		t.Fatalf("reason metadata = %q", event.Metadata["reason"])
	}
}

func registerRuntimeStreamForTest(t *testing.T, svc *Service, runtime AuthenticatedRuntime) *runtimeStream {
	t.Helper()
	stream, err := svc.registry.Register(runtime, runtime.AuthenticatedAt)
	if err != nil {
		t.Fatalf("register runtime %s: %v", runtime.RuntimeID, err)
	}
	return stream
}

type captureBackpressurePublisher struct {
	events []cpnotify.Event
}

func (c *captureBackpressurePublisher) Publish(_ context.Context, event cpnotify.Event) error {
	c.events = append(c.events, event)
	return nil
}
