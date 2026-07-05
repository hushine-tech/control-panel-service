package runtimechannel

import (
	"context"
	"testing"

	cpv1 "github.com/hushine-tech/control-panel-service/gen/controlpanelv1"
	cpnotify "github.com/hushine-tech/control-panel-service/internal/notification"
)

func TestRuntimeChannelDataBackpressurePublishesSlowConsumerNotification(t *testing.T) {
	pub := &captureBackpressurePublisher{}
	svc := NewWithConfig(nil, Config{NotificationPublisher: pub})

	svc.handleRuntimeDataBackpressure(context.Background(), AuthenticatedRuntime{
		UserID:    42,
		RuntimeID: "rt-1",
		Name:      "debug-runtime",
	}, &cpv1.RuntimeFrame{
		FrameType: cpv1.FrameType_FRAME_TYPE_DATA_BACKPRESSURE,
		Payload: &cpv1.RuntimeFrame_DataBackpressure{
			DataBackpressure: &cpv1.RuntimeDataBackpressure{
				SessionId: "sess-1",
				StreamKey: "binance/futures/kline/ETHUSDT/1m",
				Reason: "slow_consumer: kind=live_kline queue_depth=2 dropped=0",
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

func TestRuntimeChannelDataBackpressurePublishesDroppedNotification(t *testing.T) {
	pub := &captureBackpressurePublisher{}
	svc := NewWithConfig(nil, Config{NotificationPublisher: pub})

	svc.handleRuntimeDataBackpressure(context.Background(), AuthenticatedRuntime{
		UserID:    42,
		RuntimeID: "rt-1",
		Name:      "debug-runtime",
	}, &cpv1.RuntimeFrame{
		FrameType: cpv1.FrameType_FRAME_TYPE_DATA_BACKPRESSURE,
		Payload: &cpv1.RuntimeFrame_DataBackpressure{
			DataBackpressure: &cpv1.RuntimeDataBackpressure{
				SessionId: "sess-1",
				StreamKey: "order_lifecycle",
				Reason: "data_dropped: kind=order_update queue_depth=2048 dropped=1",
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

type captureBackpressurePublisher struct {
	events []cpnotify.Event
}

func (c *captureBackpressurePublisher) Publish(_ context.Context, event cpnotify.Event) error {
	c.events = append(c.events, event)
	return nil
}
