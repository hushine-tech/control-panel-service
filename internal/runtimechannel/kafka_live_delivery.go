package runtimechannel

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/hushine-tech/control-panel-service/internal/domain"
	mdrepo "github.com/hushine-tech/control-panel-service/internal/marketdata/repository"
)

const (
	klineTopicPrefix           = "md.kline"
	defaultKafkaRefreshEvery   = 5 * time.Second
	defaultStreamDeliveryLease = 30 * time.Second
	demoEnvironment            = 1
	maxDemoKlineDelay          = time.Minute
)

type LiveDeliverySubscriptionRepository interface {
	ListActiveSessionMarketDataSubscriptions(ctx context.Context) ([]domain.SessionMarketDataSubscription, error)
	CreateOrRenewStreamDeliveryLease(ctx context.Context, subscriptionID int64, ownerInstanceID string, ttl time.Duration) (domain.StreamDeliveryLease, error)
	RecordStreamDeliveryProgress(ctx context.Context, subscriptionID int64, ownerInstanceID string, topic string, partition int32, offset int64, at time.Time) error
	RecordStreamDeliveryFailure(ctx context.Context, failure domain.StreamDeliveryFailure) error
}

type LiveKlineDeliverer interface {
	DeliverLiveKlineBatch(ctx context.Context, batch LiveKlineDeliveryBatch) error
}

type KafkaLiveDeliveryConfig struct {
	Brokers         []string
	OwnerInstanceID string
	RefreshInterval time.Duration
	LeaseTTL        time.Duration
}

type KafkaLiveDeliveryWorker struct {
	repo      LiveDeliverySubscriptionRepository
	deliverer LiveKlineDeliverer
	cfg       KafkaLiveDeliveryConfig

	mu          sync.RWMutex
	subsByRoute map[klineRoute][]domain.SessionMarketDataSubscription

	consumer     sarama.Consumer
	topicCancels map[string]*topicConsumer
	nextTopicID  uint64
}

type klineRoute struct {
	Exchange string
	Market   string
	Kind     string
	Symbol   string
	Interval string
}

type topicConsumer struct {
	id     uint64
	cancel context.CancelFunc
}

func NewKafkaLiveDeliveryWorker(repo LiveDeliverySubscriptionRepository, deliverer LiveKlineDeliverer, cfg KafkaLiveDeliveryConfig) *KafkaLiveDeliveryWorker {
	if cfg.RefreshInterval <= 0 {
		cfg.RefreshInterval = defaultKafkaRefreshEvery
	}
	if cfg.LeaseTTL <= 0 {
		cfg.LeaseTTL = defaultStreamDeliveryLease
	}
	return &KafkaLiveDeliveryWorker{
		repo:         repo,
		deliverer:    deliverer,
		cfg:          cfg,
		subsByRoute:  map[klineRoute][]domain.SessionMarketDataSubscription{},
		topicCancels: map[string]*topicConsumer{},
	}
}

func (w *KafkaLiveDeliveryWorker) Run(ctx context.Context) error {
	if w == nil || w.repo == nil || w.deliverer == nil {
		return errors.New("kafka live delivery worker is not configured")
	}
	if len(w.cfg.Brokers) == 0 {
		return errors.New("market-data kafka brokers are not configured")
	}
	if strings.TrimSpace(w.cfg.OwnerInstanceID) == "" {
		return errors.New("owner instance id is required")
	}
	cfg := sarama.NewConfig()
	cfg.Consumer.Return.Errors = true
	cfg.Consumer.Offsets.Initial = sarama.OffsetNewest
	consumer, err := sarama.NewConsumer(w.cfg.Brokers, cfg)
	if err != nil {
		return fmt.Errorf("create market-data kafka consumer: %w", err)
	}
	w.consumer = consumer
	defer consumer.Close() //nolint:errcheck

	if err := w.refreshSubscriptions(ctx); err != nil {
		return err
	}
	if err := w.ensureTopicConsumers(ctx); err != nil {
		return err
	}
	ticker := time.NewTicker(w.cfg.RefreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			w.stopTopicConsumers()
			return ctx.Err()
		case <-ticker.C:
			if err := w.refreshSubscriptions(ctx); err != nil {
				return err
			}
			if err := w.ensureTopicConsumers(ctx); err != nil {
				return err
			}
		}
	}
}

func (w *KafkaLiveDeliveryWorker) refreshSubscriptions(ctx context.Context) error {
	subs, err := w.repo.ListActiveSessionMarketDataSubscriptions(ctx)
	if err != nil {
		return err
	}
	next := map[klineRoute][]domain.SessionMarketDataSubscription{}
	for _, sub := range subs {
		if sub.Status != "" && sub.Status != "active" {
			continue
		}
		route := normalizeRoute(klineRoute{
			Exchange: sub.Key.Exchange,
			Market:   sub.Key.Market,
			Kind:     sub.Key.Kind,
			Symbol:   sub.Key.Symbol,
			Interval: sub.Key.Interval,
		})
		if route.Exchange == "" || route.Market == "" || route.Kind != "kline" || route.Symbol == "" || route.Interval == "" {
			continue
		}
		sub.Key = domain.StreamKey{
			Exchange: route.Exchange,
			Market:   route.Market,
			Kind:     route.Kind,
			Symbol:   route.Symbol,
			Interval: route.Interval,
		}
		if _, err := w.repo.CreateOrRenewStreamDeliveryLease(ctx, sub.SubscriptionID, w.cfg.OwnerInstanceID, w.cfg.LeaseTTL); err != nil {
			if errors.Is(err, mdrepo.ErrPermissionDenied) {
				continue
			}
			_ = w.recordDeliveryFailure(ctx, sub.SubscriptionID, klineTopic(route.Exchange, route.Market, route.Interval), route.streamKey(), "lease_error", err)
			return fmt.Errorf("renew stream delivery lease for subscription %d: %w", sub.SubscriptionID, err)
		}
		next[route] = append(next[route], sub)
	}
	w.mu.Lock()
	w.subsByRoute = next
	w.mu.Unlock()
	return nil
}

func (w *KafkaLiveDeliveryWorker) ensureTopicConsumers(ctx context.Context) error {
	if w.consumer == nil {
		return nil
	}
	topics := w.activeTopics()
	for topic := range topics {
		w.mu.RLock()
		if _, ok := w.topicCancels[topic]; ok {
			w.mu.RUnlock()
			continue
		}
		w.mu.RUnlock()
		// Topic creation is owned by scraper/Kafka. A session may subscribe
		// before the physical topic exists, so keep the worker alive and retry
		// on the next refresh tick.
		if err := w.startTopicConsumer(ctx, topic); err != nil {
			w.recordTopicConsumerFailure(ctx, topic, err)
			return err
		}
	}
	return nil
}

func (w *KafkaLiveDeliveryWorker) activeTopics() map[string]struct{} {
	w.mu.RLock()
	defer w.mu.RUnlock()
	out := map[string]struct{}{}
	for route := range w.subsByRoute {
		out[klineTopic(route.Exchange, route.Market, route.Interval)] = struct{}{}
	}
	return out
}

func (w *KafkaLiveDeliveryWorker) startTopicConsumer(ctx context.Context, topic string) error {
	partitions, err := w.consumer.Partitions(topic)
	if err != nil {
		return fmt.Errorf("list kafka partitions for %s: %w", topic, err)
	}
	childCtx, cancel := context.WithCancel(ctx)
	w.mu.Lock()
	w.nextTopicID++
	topicID := w.nextTopicID
	w.topicCancels[topic] = &topicConsumer{id: topicID, cancel: cancel}
	w.mu.Unlock()
	for _, partition := range partitions {
		pc, err := w.consumer.ConsumePartition(topic, partition, sarama.OffsetNewest)
		if err != nil {
			cancel()
			w.mu.Lock()
			delete(w.topicCancels, topic)
			w.mu.Unlock()
			return fmt.Errorf("consume kafka partition %s/%d: %w", topic, partition, err)
		}
		go w.consumePartition(childCtx, topicID, topic, partition, pc, cancel)
	}
	return nil
}

func (w *KafkaLiveDeliveryWorker) recordTopicConsumerFailure(ctx context.Context, topic string, cause error) {
	log.Printf("market-data live delivery topic consumer failed topic=%s owner=%s error=%v", topic, w.cfg.OwnerInstanceID, cause)
	w.mu.RLock()
	defer w.mu.RUnlock()
	for route, subs := range w.subsByRoute {
		if klineTopic(route.Exchange, route.Market, route.Interval) != topic {
			continue
		}
		for _, sub := range subs {
			_ = w.recordDeliveryFailure(ctx, sub.SubscriptionID, topic, route.streamKey(), "consumer_start_error", cause)
		}
	}
}

func (w *KafkaLiveDeliveryWorker) recordDeliveryFailure(ctx context.Context, subscriptionID int64, topic, streamKey, code string, cause error) error {
	if w.repo == nil || cause == nil {
		return nil
	}
	return w.repo.RecordStreamDeliveryFailure(ctx, domain.StreamDeliveryFailure{
		SubscriptionID:  subscriptionID,
		OwnerInstanceID: w.cfg.OwnerInstanceID,
		Topic:           topic,
		StreamKey:       streamKey,
		FailureCode:     code,
		Reason:          cause.Error(),
		LastSeenAt:      time.Now().UTC(),
		AttemptCount:    1,
	})
}

func (w *KafkaLiveDeliveryWorker) consumePartition(ctx context.Context, topicID uint64, topic string, partition int32, pc sarama.PartitionConsumer, cancel context.CancelFunc) {
	cleanExit := false
	defer pc.Close() //nolint:errcheck
	defer func() {
		if cleanExit || ctx.Err() != nil {
			return
		}
		log.Printf("market-data live delivery partition consumer stopped topic=%s partition=%d owner=%s", topic, partition, w.cfg.OwnerInstanceID)
		cancel()
		w.mu.Lock()
		if current := w.topicCancels[topic]; current != nil && current.id == topicID {
			delete(w.topicCancels, topic)
		}
		w.mu.Unlock()
	}()
	for {
		select {
		case <-ctx.Done():
			pc.AsyncClose()
			cleanExit = true
			return
		case msg, ok := <-pc.Messages():
			if !ok {
				return
			}
			if err := w.handleKlineMessage(ctx, msg.Topic, msg.Key, msg.Value, msg.Partition, msg.Offset); err != nil {
				w.recordMessageDeliveryFailure(ctx, msg.Topic, msg.Key, msg.Value, err)
			}
		case consumerErr, ok := <-pc.Errors():
			if !ok {
				return
			}
			if consumerErr != nil && consumerErr.Err != nil {
				w.recordTopicConsumerFailure(ctx, topic, consumerErr.Err)
			}
		}
	}
}

func (w *KafkaLiveDeliveryWorker) stopTopicConsumers() {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, consumer := range w.topicCancels {
		if consumer != nil && consumer.cancel != nil {
			consumer.cancel()
		}
	}
	w.topicCancels = map[string]*topicConsumer{}
}

func (w *KafkaLiveDeliveryWorker) recordMessageDeliveryFailure(ctx context.Context, topic string, key, value []byte, cause error) {
	route, err := routeFromKlineMessage(topic, key, value)
	if err != nil {
		log.Printf("market-data live delivery message failed topic=%s owner=%s error=%v", topic, w.cfg.OwnerInstanceID, cause)
		return
	}
	w.mu.RLock()
	subs := append([]domain.SessionMarketDataSubscription(nil), w.subsByRoute[route]...)
	w.mu.RUnlock()
	if len(subs) == 0 {
		log.Printf("market-data live delivery message failed topic=%s stream=%s owner=%s error=%v", topic, route.streamKey(), w.cfg.OwnerInstanceID, cause)
		return
	}
	for _, sub := range subs {
		_ = w.recordDeliveryFailure(ctx, sub.SubscriptionID, topic, route.streamKey(), "delivery_error", cause)
	}
}

func (w *KafkaLiveDeliveryWorker) handleKlineMessage(ctx context.Context, topic string, key, value []byte, partition int32, offset int64) error {
	route, err := parseKlineTopic(topic)
	if err != nil {
		return nil
	}
	payload := map[string]any{}
	if err := json.Unmarshal(value, &payload); err != nil {
		return err
	}
	symbol := strings.ToUpper(strings.TrimSpace(stringFromAny(payload["symbol"])))
	if symbol == "" {
		symbol = strings.ToUpper(strings.TrimSpace(string(key)))
	}
	if symbol == "" {
		return nil
	}
	route.Symbol = symbol
	route = normalizeRoute(route)
	payload["symbol"] = route.Symbol
	payload["market"] = route.Market
	payload["interval"] = route.Interval

	w.mu.RLock()
	subs := append([]domain.SessionMarketDataSubscription(nil), w.subsByRoute[route]...)
	w.mu.RUnlock()
	if len(subs) == 0 {
		return nil
	}
	st, err := structpb.NewStruct(payload)
	if err != nil {
		return err
	}
	packed, err := anypb.New(st)
	if err != nil {
		return err
	}
	streamKey := route.streamKey()
	now := time.Now().UTC()
	for _, sub := range subs {
		if _, err := w.repo.CreateOrRenewStreamDeliveryLease(ctx, sub.SubscriptionID, w.cfg.OwnerInstanceID, w.cfg.LeaseTTL); err != nil {
			if errors.Is(err, mdrepo.ErrPermissionDenied) {
				continue
			}
			_ = w.recordDeliveryFailure(ctx, sub.SubscriptionID, topic, streamKey, "lease_error", err)
			continue
		}
		if stale, ageMs, eventTimeMs := isStaleDemoKline(sub.Environment, payload, now); stale {
			log.Printf(
				"dropping stale demo live kline session=%s runtime=%s stream=%s topic=%s offset=%d age_ms=%d event_time_ms=%d max_age_ms=%d",
				sub.SessionID,
				sub.RuntimeID,
				streamKey,
				topic,
				offset,
				ageMs,
				eventTimeMs,
				maxDemoKlineDelay.Milliseconds(),
			)
			if err := w.repo.RecordStreamDeliveryProgress(ctx, sub.SubscriptionID, w.cfg.OwnerInstanceID, topic, partition, offset, now); err != nil {
				_ = w.recordDeliveryFailure(ctx, sub.SubscriptionID, topic, streamKey, "progress_error", err)
			}
			continue
		}
		if err := w.deliverer.DeliverLiveKlineBatch(ctx, LiveKlineDeliveryBatch{
			UserID:    sub.UserID,
			RuntimeID: sub.RuntimeID,
			SessionID: sub.SessionID,
			StreamKey: streamKey,
			Klines:    []*anypb.Any{packed},
		}); err != nil {
			_ = w.recordDeliveryFailure(ctx, sub.SubscriptionID, topic, streamKey, "delivery_error", err)
			continue
		}
		if err := w.repo.RecordStreamDeliveryProgress(ctx, sub.SubscriptionID, w.cfg.OwnerInstanceID, topic, partition, offset, time.Now().UTC()); err != nil {
			_ = w.recordDeliveryFailure(ctx, sub.SubscriptionID, topic, streamKey, "progress_error", err)
		}
	}
	return nil
}

func routeFromKlineMessage(topic string, key, value []byte) (klineRoute, error) {
	route, err := parseKlineTopic(topic)
	if err != nil {
		return klineRoute{}, err
	}
	payload := map[string]any{}
	if err := json.Unmarshal(value, &payload); err != nil {
		return klineRoute{}, err
	}
	symbol := strings.ToUpper(strings.TrimSpace(stringFromAny(payload["symbol"])))
	if symbol == "" {
		symbol = strings.ToUpper(strings.TrimSpace(string(key)))
	}
	if symbol == "" {
		return klineRoute{}, errors.New("kline symbol is required")
	}
	route.Symbol = symbol
	return normalizeRoute(route), nil
}

func isStaleDemoKline(environment int32, payload map[string]any, now time.Time) (bool, int64, int64) {
	if environment != demoEnvironment {
		return false, 0, 0
	}
	eventTimeMs := klineEventTimeMs(payload)
	if eventTimeMs <= 0 {
		return false, 0, 0
	}
	ageMs := now.UnixMilli() - eventTimeMs
	return ageMs > maxDemoKlineDelay.Milliseconds(), ageMs, eventTimeMs
}

func klineEventTimeMs(payload map[string]any) int64 {
	for _, key := range []string{"timestamp", "close_time", "close_time_ms", "event_time"} {
		if value, ok := int64FromAny(payload[key]); ok {
			return value
		}
	}
	return 0
}

func int64FromAny(value any) (int64, bool) {
	switch v := value.(type) {
	case int64:
		return v, true
	case int:
		return int64(v), true
	case int32:
		return int64(v), true
	case float64:
		return int64(v), true
	case float32:
		return int64(v), true
	case json.Number:
		n, err := v.Int64()
		if err == nil {
			return n, true
		}
	case string:
		n, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
		if err == nil {
			return n, true
		}
	}
	return 0, false
}

func parseKlineTopic(topic string) (klineRoute, error) {
	parts := strings.Split(strings.ToLower(strings.TrimSpace(topic)), ".")
	if len(parts) != 5 || parts[0]+"."+parts[1] != klineTopicPrefix {
		return klineRoute{}, fmt.Errorf("not a kline topic: %s", topic)
	}
	return normalizeRoute(klineRoute{
		Exchange: parts[2],
		Market:   parts[3],
		Kind:     "kline",
		Interval: parts[4],
	}), nil
}

func klineTopic(exchange, market, interval string) string {
	route := normalizeRoute(klineRoute{Exchange: exchange, Market: market, Kind: "kline", Interval: interval})
	return fmt.Sprintf("%s.%s.%s.%s", klineTopicPrefix, route.Exchange, route.Market, route.Interval)
}

func normalizeRoute(route klineRoute) klineRoute {
	route.Exchange = strings.ToLower(strings.TrimSpace(route.Exchange))
	route.Market = strings.ToLower(strings.TrimSpace(route.Market))
	route.Kind = strings.ToLower(strings.TrimSpace(route.Kind))
	if route.Kind == "" {
		route.Kind = "kline"
	}
	route.Symbol = strings.ToUpper(strings.TrimSpace(route.Symbol))
	route.Interval = strings.ToLower(strings.TrimSpace(route.Interval))
	return route
}

func (r klineRoute) streamKey() string {
	return strings.Join([]string{r.Exchange, r.Market, r.Kind, r.Symbol, r.Interval}, "/")
}

func stringFromAny(v any) string {
	s, _ := v.(string)
	return s
}
