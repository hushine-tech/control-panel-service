package runtimechannel

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/hushine-tech/control-panel-service/internal/domain"
	mdrepo "github.com/hushine-tech/control-panel-service/internal/marketdata/repository"
)

func TestKafkaLiveDeliveryWorkerRoutesMatchingKlineMessages(t *testing.T) {
	repo := &liveDeliveryRepoStub{subs: []domain.SessionMarketDataSubscription{{
		SubscriptionID: 7,
		UserID:         42,
		SessionID:      "sess-1",
		RuntimeID:      "rt-1",
		Key: domain.StreamKey{
			Exchange: "binance",
			Market:   "futures",
			Kind:     "kline",
			Symbol:   "ETHUSDT",
			Interval: "1m",
		},
		Status: "active",
	}}}
	deliverer := &captureLiveDeliverer{}
	worker := NewKafkaLiveDeliveryWorker(repo, deliverer, KafkaLiveDeliveryConfig{
		OwnerInstanceID: "cp-1",
		LeaseTTL:        time.Minute,
	})

	if err := worker.refreshSubscriptions(context.Background()); err != nil {
		t.Fatalf("refreshSubscriptions: %v", err)
	}
	err := worker.handleKlineMessage(
		context.Background(),
		"md.kline.binance.futures.1m",
		[]byte("ETHUSDT"),
		[]byte(`{"symbol":"ethusdt","interval":"1m","open_time":1,"close_time":2,"open":100,"high":101,"low":99,"close":100.5,"volume":12.3}`),
		0,
		11,
	)
	if err != nil {
		t.Fatalf("handleKlineMessage: %v", err)
	}

	if repo.claimed < 1 || repo.lastOwner != "cp-1" || repo.lastSubscriptionID != 7 {
		t.Fatalf("lease claim = id:%d owner:%q count:%d", repo.lastSubscriptionID, repo.lastOwner, repo.claimed)
	}
	if len(deliverer.batches) != 1 {
		t.Fatalf("delivered batches = %d, want 1", len(deliverer.batches))
	}
	if repo.progress != 1 || repo.lastOffset != 11 {
		t.Fatalf("delivery progress = count:%d offset:%d, want 1/11", repo.progress, repo.lastOffset)
	}
	batch := deliverer.batches[0]
	if batch.UserID != 42 || batch.RuntimeID != "rt-1" || batch.SessionID != "sess-1" {
		t.Fatalf("batch route = %+v", batch)
	}
	if batch.StreamKey != "binance/futures/kline/ETHUSDT/1m" {
		t.Fatalf("stream key = %q", batch.StreamKey)
	}
	if len(batch.Klines) != 1 {
		t.Fatalf("klines = %d, want 1", len(batch.Klines))
	}
	var st structpb.Struct
	if err := batch.Klines[0].UnmarshalTo(&st); err != nil {
		t.Fatalf("unmarshal kline: %v", err)
	}
	if got := st.GetFields()["symbol"].GetStringValue(); got != "ETHUSDT" {
		t.Fatalf("packed symbol = %q, want ETHUSDT", got)
	}
}

func TestKafkaLiveDeliveryWorkerClaimsLeaseDuringRefresh(t *testing.T) {
	repo := &liveDeliveryRepoStub{subs: []domain.SessionMarketDataSubscription{{
		SubscriptionID: 7,
		UserID:         42,
		SessionID:      "sess-1",
		RuntimeID:      "rt-1",
		Key:            domain.StreamKey{Exchange: "binance", Market: "futures", Kind: "kline", Symbol: "ETHUSDT", Interval: "1m"},
		Status:         "active",
	}}}
	worker := NewKafkaLiveDeliveryWorker(repo, &captureLiveDeliverer{}, KafkaLiveDeliveryConfig{
		OwnerInstanceID: "cp-1",
		LeaseTTL:        time.Minute,
	})

	if err := worker.refreshSubscriptions(context.Background()); err != nil {
		t.Fatalf("refreshSubscriptions: %v", err)
	}

	if repo.claimed != 1 || repo.lastSubscriptionID != 7 || repo.lastOwner != "cp-1" {
		t.Fatalf("lease claim during refresh = id:%d owner:%q count:%d, want id 7 owner cp-1 count 1", repo.lastSubscriptionID, repo.lastOwner, repo.claimed)
	}
}

func TestKafkaLiveDeliveryWorkerReturnsTopicConsumerStartupError(t *testing.T) {
	consumer := &fakeSaramaConsumer{partitionsErr: errors.New("topic missing")}
	worker := NewKafkaLiveDeliveryWorker(&liveDeliveryRepoStub{}, &captureLiveDeliverer{}, KafkaLiveDeliveryConfig{
		OwnerInstanceID: "cp-1",
		LeaseTTL:        time.Minute,
	})
	worker.consumer = consumer
	worker.subsByRoute = map[klineRoute][]domain.SessionMarketDataSubscription{
		{Exchange: "binance", Market: "futures", Kind: "kline", Symbol: "ETHUSDT", Interval: "1m"}: {{
			SubscriptionID: 7,
			UserID:         42,
			SessionID:      "sess-1",
			RuntimeID:      "rt-1",
			Key:            domain.StreamKey{Exchange: "binance", Market: "futures", Kind: "kline", Symbol: "ETHUSDT", Interval: "1m"},
			Status:         "active",
		}},
	}

	err := worker.ensureTopicConsumers(context.Background())

	if err == nil || !strings.Contains(err.Error(), "topic missing") {
		t.Fatalf("ensureTopicConsumers err = %v, want topic missing", err)
	}
}

func TestKafkaLiveDeliveryWorkerRecordsTopicConsumerStartupError(t *testing.T) {
	repo := &liveDeliveryRepoStub{subs: []domain.SessionMarketDataSubscription{{
		SubscriptionID: 7,
		UserID:         42,
		SessionID:      "sess-1",
		RuntimeID:      "rt-1",
		Key:            domain.StreamKey{Exchange: "binance", Market: "futures", Kind: "kline", Symbol: "ETHUSDT", Interval: "1m"},
		Status:         "active",
	}}}
	consumer := &fakeSaramaConsumer{partitionsErr: errors.New("topic missing")}
	worker := NewKafkaLiveDeliveryWorker(repo, &captureLiveDeliverer{}, KafkaLiveDeliveryConfig{
		OwnerInstanceID: "cp-1",
		LeaseTTL:        time.Minute,
	})
	worker.consumer = consumer
	if err := worker.refreshSubscriptions(context.Background()); err != nil {
		t.Fatalf("refreshSubscriptions: %v", err)
	}

	err := worker.ensureTopicConsumers(context.Background())

	if err == nil {
		t.Fatal("ensureTopicConsumers err is nil, want topic startup error")
	}
	if len(repo.failures) != 1 {
		t.Fatalf("recorded failures = %+v, want one", repo.failures)
	}
	got := repo.failures[0]
	if got.SubscriptionID != 7 || got.Topic != "md.kline.binance.futures.1m" || !strings.Contains(got.Reason, "topic missing") {
		t.Fatalf("recorded failure = %+v, want subscription/topic/reason", got)
	}
}

func TestKafkaLiveDeliveryWorkerSkipsSubscriptionsOwnedByAnotherInstance(t *testing.T) {
	repo := &liveDeliveryRepoStub{
		subs: []domain.SessionMarketDataSubscription{{
			SubscriptionID: 7,
			UserID:         42,
			SessionID:      "sess-1",
			RuntimeID:      "rt-1",
			Key:            domain.StreamKey{Exchange: "binance", Market: "futures", Kind: "kline", Symbol: "ETHUSDT", Interval: "1m"},
			Status:         "active",
		}},
		claimErr: mdrepo.ErrPermissionDenied,
	}
	deliverer := &captureLiveDeliverer{}
	worker := NewKafkaLiveDeliveryWorker(repo, deliverer, KafkaLiveDeliveryConfig{
		OwnerInstanceID: "cp-1",
		LeaseTTL:        time.Minute,
	})

	if err := worker.refreshSubscriptions(context.Background()); err != nil {
		t.Fatalf("refreshSubscriptions: %v", err)
	}
	err := worker.handleKlineMessage(
		context.Background(),
		"md.kline.binance.futures.1m",
		[]byte("ETHUSDT"),
		[]byte(`{"symbol":"ETHUSDT","interval":"1m","close":100.5}`),
		0,
		11,
	)
	if err != nil {
		t.Fatalf("handleKlineMessage: %v", err)
	}
	if len(deliverer.batches) != 0 {
		t.Fatalf("delivered batches = %d, want 0", len(deliverer.batches))
	}
}

func TestKafkaLiveDeliveryWorkerContinuesAfterSubscriptionDeliveryError(t *testing.T) {
	repo := &liveDeliveryRepoStub{subs: []domain.SessionMarketDataSubscription{
		{
			SubscriptionID: 7,
			UserID:         42,
			SessionID:      "old-session",
			RuntimeID:      "rt-stale",
			Key:            domain.StreamKey{Exchange: "binance", Market: "futures", Kind: "kline", Symbol: "ETHUSDT", Interval: "1m"},
			Status:         "active",
		},
		{
			SubscriptionID: 8,
			UserID:         42,
			SessionID:      "live-session",
			RuntimeID:      "rt-live",
			Key:            domain.StreamKey{Exchange: "binance", Market: "futures", Kind: "kline", Symbol: "ETHUSDT", Interval: "1m"},
			Status:         "active",
		},
	}}
	deliverer := &captureLiveDeliverer{errByRuntimeID: map[string]error{
		"rt-stale": errors.New("runtime stream unavailable"),
	}}
	worker := NewKafkaLiveDeliveryWorker(repo, deliverer, KafkaLiveDeliveryConfig{
		OwnerInstanceID: "cp-1",
		LeaseTTL:        time.Minute,
	})
	if err := worker.refreshSubscriptions(context.Background()); err != nil {
		t.Fatalf("refreshSubscriptions: %v", err)
	}

	err := worker.handleKlineMessage(
		context.Background(),
		"md.kline.binance.futures.1m",
		[]byte("ETHUSDT"),
		[]byte(`{"symbol":"ETHUSDT","interval":"1m","close":100.5}`),
		0,
		11,
	)

	if err != nil {
		t.Fatalf("handleKlineMessage: %v", err)
	}
	if len(deliverer.batches) != 2 {
		t.Fatalf("delivered attempts = %d, want 2", len(deliverer.batches))
	}
	if repo.progress != 1 || repo.lastSubscriptionID != 8 || repo.lastOffset != 11 {
		t.Fatalf("progress = count:%d sub:%d offset:%d, want 1/8/11", repo.progress, repo.lastSubscriptionID, repo.lastOffset)
	}
	if len(repo.failures) != 1 {
		t.Fatalf("failures = %+v, want one", repo.failures)
	}
	if repo.failures[0].SubscriptionID != 7 || repo.failures[0].FailureCode != "delivery_error" {
		t.Fatalf("failure = %+v, want delivery_error for stale subscription", repo.failures[0])
	}
}

func TestKafkaLiveDeliveryWorkerRecordsMessageDeliveryError(t *testing.T) {
	repo := &liveDeliveryRepoStub{subs: []domain.SessionMarketDataSubscription{{
		SubscriptionID: 7,
		UserID:         42,
		SessionID:      "sess-1",
		RuntimeID:      "rt-1",
		Key:            domain.StreamKey{Exchange: "binance", Market: "futures", Kind: "kline", Symbol: "ETHUSDT", Interval: "1m"},
		Status:         "active",
	}}}
	deliverer := &captureLiveDeliverer{err: errors.New("runtime stream unavailable")}
	worker := NewKafkaLiveDeliveryWorker(repo, deliverer, KafkaLiveDeliveryConfig{
		OwnerInstanceID: "cp-1",
		LeaseTTL:        time.Minute,
	})
	if err := worker.refreshSubscriptions(context.Background()); err != nil {
		t.Fatalf("refreshSubscriptions: %v", err)
	}
	topic := "md.kline.binance.futures.1m"
	messages := make(chan *sarama.ConsumerMessage, 1)
	messages <- &sarama.ConsumerMessage{
		Topic:     topic,
		Partition: 0,
		Offset:    11,
		Key:       []byte("ETHUSDT"),
		Value:     []byte(`{"symbol":"ETHUSDT","interval":"1m","close":100.5}`),
	}
	close(messages)
	pc := &fakePartitionConsumer{messages: messages, errors: make(chan *sarama.ConsumerError)}
	worker.topicCancels[topic] = &topicConsumer{id: 1, cancel: func() {}}

	worker.consumePartition(context.Background(), 1, topic, 0, pc, func() {})

	if len(repo.failures) != 1 {
		t.Fatalf("recorded failures = %+v, want one", repo.failures)
	}
	got := repo.failures[0]
	if got.SubscriptionID != 7 || got.FailureCode != "delivery_error" || !strings.Contains(got.Reason, "runtime stream unavailable") {
		t.Fatalf("recorded failure = %+v, want delivery_error for subscription", got)
	}
}

func TestKafkaLiveDeliveryWorkerDropsStoppedPartitionConsumerForRestart(t *testing.T) {
	worker := NewKafkaLiveDeliveryWorker(&liveDeliveryRepoStub{}, &captureLiveDeliverer{}, KafkaLiveDeliveryConfig{
		OwnerInstanceID: "cp-1",
		LeaseTTL:        time.Minute,
	})
	topic := "md.kline.binance.futures.1m"
	cancelled := false
	worker.topicCancels[topic] = &topicConsumer{id: 7, cancel: func() { cancelled = true }}
	messages := make(chan *sarama.ConsumerMessage)
	close(messages)
	pc := &fakePartitionConsumer{messages: messages, errors: make(chan *sarama.ConsumerError)}

	worker.consumePartition(context.Background(), 7, topic, 0, pc, func() { cancelled = true })

	if !cancelled {
		t.Fatal("partition consumer cancel was not called")
	}
	if _, ok := worker.topicCancels[topic]; ok {
		t.Fatalf("topic consumer still registered for %s, want dropped for restart", topic)
	}
}

type liveDeliveryRepoStub struct {
	subs               []domain.SessionMarketDataSubscription
	claimErr           error
	claimed            int
	lastOwner          string
	lastSubscriptionID int64
	failures           []domain.StreamDeliveryFailure
	progress           int
	lastOffset         int64
}

func (s *liveDeliveryRepoStub) ListActiveSessionMarketDataSubscriptions(context.Context) ([]domain.SessionMarketDataSubscription, error) {
	return append([]domain.SessionMarketDataSubscription(nil), s.subs...), nil
}

func (s *liveDeliveryRepoStub) CreateOrRenewStreamDeliveryLease(_ context.Context, subscriptionID int64, ownerInstanceID string, ttl time.Duration) (domain.StreamDeliveryLease, error) {
	if s.claimErr != nil {
		return domain.StreamDeliveryLease{}, s.claimErr
	}
	s.claimed++
	s.lastOwner = ownerInstanceID
	s.lastSubscriptionID = subscriptionID
	now := time.Now().UTC()
	return domain.StreamDeliveryLease{
		LeaseID:         "sdl-7",
		SubscriptionID:  subscriptionID,
		OwnerInstanceID: ownerInstanceID,
		Status:          "active",
		AcquiredAt:      now,
		LastHeartbeatAt: now,
		ExpiresAt:       now.Add(ttl),
		CreatedAt:       now,
		UpdatedAt:       now,
	}, nil
}

func (s *liveDeliveryRepoStub) RecordStreamDeliveryFailure(_ context.Context, failure domain.StreamDeliveryFailure) error {
	s.failures = append(s.failures, failure)
	return nil
}

func (s *liveDeliveryRepoStub) RecordStreamDeliveryProgress(_ context.Context, subscriptionID int64, ownerInstanceID string, topic string, partition int32, offset int64, at time.Time) error {
	s.progress++
	s.lastSubscriptionID = subscriptionID
	s.lastOwner = ownerInstanceID
	s.lastOffset = offset
	return nil
}

type captureLiveDeliverer struct {
	batches        []LiveKlineDeliveryBatch
	err            error
	errByRuntimeID map[string]error
}

func (c *captureLiveDeliverer) DeliverLiveKlineBatch(_ context.Context, batch LiveKlineDeliveryBatch) error {
	c.batches = append(c.batches, batch)
	if c.errByRuntimeID != nil && c.errByRuntimeID[batch.RuntimeID] != nil {
		return errors.New(c.errByRuntimeID[batch.RuntimeID].Error())
	}
	if c.err != nil {
		return errors.New(c.err.Error())
	}
	return nil
}

type fakeSaramaConsumer struct {
	partitionsErr error
}

func (f *fakeSaramaConsumer) Topics() ([]string, error) { return nil, nil }

func (f *fakeSaramaConsumer) Partitions(topic string) ([]int32, error) {
	if f.partitionsErr != nil {
		return nil, f.partitionsErr
	}
	return []int32{0}, nil
}

func (f *fakeSaramaConsumer) ConsumePartition(string, int32, int64) (sarama.PartitionConsumer, error) {
	return nil, errors.New("not implemented")
}

func (f *fakeSaramaConsumer) HighWaterMarks() map[string]map[int32]int64 { return nil }

func (f *fakeSaramaConsumer) Close() error { return nil }

type fakePartitionConsumer struct {
	messages    chan *sarama.ConsumerMessage
	errors      chan *sarama.ConsumerError
	asyncClosed bool
	closed      bool
}

func (f *fakePartitionConsumer) AsyncClose() { f.asyncClosed = true }

func (f *fakePartitionConsumer) Close() error {
	f.closed = true
	return nil
}

func (f *fakePartitionConsumer) Messages() <-chan *sarama.ConsumerMessage {
	return f.messages
}

func (f *fakePartitionConsumer) Errors() <-chan *sarama.ConsumerError {
	return f.errors
}

func (f *fakePartitionConsumer) HighWaterMarkOffset() int64 { return 0 }

func (f *fakePartitionConsumer) Pause() {}

func (f *fakePartitionConsumer) Resume() {}

func (f *fakePartitionConsumer) IsPaused() bool { return false }

func (f *fakeSaramaConsumer) Pause(map[string][]int32) {}

func (f *fakeSaramaConsumer) Resume(map[string][]int32) {}

func (f *fakeSaramaConsumer) PauseAll() {}

func (f *fakeSaramaConsumer) ResumeAll() {}
