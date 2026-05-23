package notification

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/IBM/sarama"
)

type fakeProducer struct {
	msgs []*sarama.ProducerMessage
}

func (f *fakeProducer) SendMessage(msg *sarama.ProducerMessage) (int32, int64, error) {
	f.msgs = append(f.msgs, msg)
	return 0, int64(len(f.msgs)), nil
}

func (f *fakeProducer) Close() error { return nil }

func TestKafkaPublisherWritesSchemaV1JSON(t *testing.T) {
	producer := &fakeProducer{}
	publisher := NewKafkaPublisherWithProducer(producer, "notification.events", "control-panel-service")

	if err := publisher.Publish(context.Background(), Event{
		UserID:    42,
		Category:  CategorySystem,
		EventType: EventRuntimeStarted,
		Severity:  SeverityInfo,
		RuntimeID: "rt-1",
		Message:   "Runtime started",
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if len(producer.msgs) != 1 {
		t.Fatalf("messages = %d, want 1", len(producer.msgs))
	}
	raw, err := producer.msgs[0].Value.Encode()
	if err != nil {
		t.Fatalf("encode value: %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatalf("payload json: %v", err)
	}
	if payload["schema_version"] != float64(1) || payload["source_service"] != "control-panel-service" {
		t.Fatalf("payload = %#v, want schema v1 and source_service", payload)
	}
}

func TestNoopPublisherAcceptsEvent(t *testing.T) {
	if err := (NoopPublisher{}).Publish(context.Background(), Event{UserID: 1}); err != nil {
		t.Fatalf("noop publish: %v", err)
	}
}
