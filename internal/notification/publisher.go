package notification

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/IBM/sarama"
	"github.com/google/uuid"
)

type Producer interface {
	SendMessage(msg *sarama.ProducerMessage) (partition int32, offset int64, err error)
	Close() error
}

type KafkaPublisher struct {
	producer      Producer
	topic         string
	sourceService string
	now           func() time.Time
}

func NewKafkaPublisher(brokers []string, topic string, clientID string) (*KafkaPublisher, error) {
	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true
	if clientID != "" {
		cfg.ClientID = clientID
	}
	producer, err := sarama.NewSyncProducer(brokers, cfg)
	if err != nil {
		return nil, err
	}
	return NewKafkaPublisherWithProducer(producer, topic, "control-panel-service"), nil
}

func NewKafkaPublisherWithProducer(producer Producer, topic string, sourceService string) *KafkaPublisher {
	if topic == "" {
		topic = "notification.events"
	}
	if sourceService == "" {
		sourceService = "control-panel-service"
	}
	return &KafkaPublisher{
		producer:      producer,
		topic:         topic,
		sourceService: sourceService,
		now:           time.Now,
	}
}

func (p *KafkaPublisher) Publish(ctx context.Context, event Event) error {
	if p == nil || p.producer == nil {
		return nil
	}
	if event.SchemaVersion == 0 {
		event.SchemaVersion = SchemaVersion
	}
	if event.EventID == "" {
		event.EventID = uuid.NewString()
	}
	if event.CreatedAt.IsZero() {
		event.CreatedAt = p.now().UTC()
	}
	if event.SourceService == "" {
		event.SourceService = p.sourceService
	}
	raw, err := json.Marshal(event)
	if err != nil {
		return err
	}
	_, _, err = p.producer.SendMessage(&sarama.ProducerMessage{
		Topic: p.topic,
		Key:   sarama.StringEncoder(fmt.Sprintf("%d", event.UserID)),
		Value: sarama.ByteEncoder(raw),
	})
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}

func (p *KafkaPublisher) Close() error {
	if p == nil || p.producer == nil {
		return nil
	}
	return p.producer.Close()
}

type NoopPublisher struct{}

func (NoopPublisher) Publish(context.Context, Event) error { return nil }
