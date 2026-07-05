package runtimechannel

import (
	"context"
	"log"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/hushine-tech/control-panel-service/internal/domain"
	orderv1 "github.com/hushine-tech/core-service/gen/orderv1"
)

const defaultOrderLifecyclePollInterval = time.Second

type OrderLifecycleSessionSource interface {
	ListActiveSessionMarketDataSubscriptions(ctx context.Context) ([]domain.SessionMarketDataSubscription, error)
}

type OrderLifecycleClient interface {
	ListOrderLifecycleEvents(ctx context.Context, in *orderv1.ListOrderLifecycleEventsRequest, opts ...grpc.CallOption) (*orderv1.ListOrderLifecycleEventsResponse, error)
}

type OrderLifecycleDeliveryConfig struct {
	PollInterval time.Duration
	Limit        int32
}

type OrderLifecycleDeliveryWorker struct {
	sessions  OrderLifecycleSessionSource
	client    OrderLifecycleClient
	deliverer OrderLifecycleDeliverer
	cfg       OrderLifecycleDeliveryConfig
	cursor    map[string]int64
}

type orderLifecycleSessionRoute struct {
	UserID    int64
	RuntimeID string
	SessionID string
}

func NewOrderLifecycleDeliveryWorker(
	sessions OrderLifecycleSessionSource,
	client OrderLifecycleClient,
	deliverer OrderLifecycleDeliverer,
	cfg OrderLifecycleDeliveryConfig,
) *OrderLifecycleDeliveryWorker {
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = defaultOrderLifecyclePollInterval
	}
	if cfg.Limit <= 0 {
		cfg.Limit = 100
	}
	return &OrderLifecycleDeliveryWorker{
		sessions:  sessions,
		client:    client,
		deliverer: deliverer,
		cfg:       cfg,
		cursor:    map[string]int64{},
	}
}

func (w *OrderLifecycleDeliveryWorker) Run(ctx context.Context) error {
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
			log.Printf("order lifecycle RuntimeChannel delivery sync failed: %v", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (w *OrderLifecycleDeliveryWorker) SyncOnce(ctx context.Context) error {
	routes, err := w.activeRoutes(ctx)
	if err != nil {
		return err
	}
	for _, route := range routes {
		after := w.cursor[route.SessionID]
		resp, err := w.client.ListOrderLifecycleEvents(ctx, &orderv1.ListOrderLifecycleEventsRequest{
			SessionId:    route.SessionID,
			AfterEventId: after,
			Limit:        w.cfg.Limit,
		})
		if err != nil {
			return err
		}
		events := resp.GetEvents()
		if len(events) == 0 {
			continue
		}
		packed := make([]*anypb.Any, 0, len(events))
		maxEventID := after
		for _, event := range events {
			item, err := anypb.New(event)
			if err != nil {
				return err
			}
			packed = append(packed, item)
			if event.GetEventId() > maxEventID {
				maxEventID = event.GetEventId()
			}
		}
		if err := w.deliverer.DeliverOrderLifecycleBatch(ctx, OrderLifecycleDeliveryBatch{
			UserID:    route.UserID,
			RuntimeID: route.RuntimeID,
			SessionID: route.SessionID,
			Sequence:  maxEventID,
			Events:    packed,
		}); err != nil {
			return err
		}
		w.cursor[route.SessionID] = maxEventID
	}
	return nil
}

func (w *OrderLifecycleDeliveryWorker) activeRoutes(ctx context.Context) ([]orderLifecycleSessionRoute, error) {
	subs, err := w.sessions.ListActiveSessionMarketDataSubscriptions(ctx)
	if err != nil {
		return nil, err
	}
	seen := map[string]struct{}{}
	out := make([]orderLifecycleSessionRoute, 0, len(subs))
	for _, sub := range subs {
		route := orderLifecycleSessionRoute{
			UserID:    sub.UserID,
			RuntimeID: strings.TrimSpace(sub.RuntimeID),
			SessionID: strings.TrimSpace(sub.SessionID),
		}
		if route.UserID <= 0 || route.RuntimeID == "" || route.SessionID == "" {
			continue
		}
		key := route.RuntimeID + "\x00" + route.SessionID
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, route)
	}
	return out, nil
}
