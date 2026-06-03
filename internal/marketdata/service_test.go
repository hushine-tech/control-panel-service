package marketdata

import (
	"context"
	"testing"
	"time"

	mdv1 "github.com/hushine-tech/control-panel-service/gen/marketdatav1"
	"github.com/hushine-tech/control-panel-service/internal/domain"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func newSvc() *Service { return NewService(newStubRepo()) }

func liveKey() *mdv1.StreamKey {
	return &mdv1.StreamKey{
		Exchange: "binance",
		Market:   "futures",
		Kind:     "kline",
		Symbol:   "BTCUSDT",
		Interval: "1m",
	}
}

// ── CreateMarketDataRequest ────────────────────────────────────────────

func TestCreateMarketDataRequest_LiveHappyPath(t *testing.T) {
	svc := newSvc()
	resp, err := svc.CreateMarketDataRequest(context.Background(), &mdv1.CreateMarketDataRequestRequest{
		UserId:            42,
		Key:               liveKey(),
		NeedsLiveDelivery: true,
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if resp.GetRequest().GetUserId() != 42 {
		t.Errorf("user_id = %d, want 42", resp.GetRequest().GetUserId())
	}
	if resp.GetRequest().GetStatus() != "active" {
		t.Errorf("status = %q, want active", resp.GetRequest().GetStatus())
	}
	if resp.GetStream().GetStreamId() == 0 {
		t.Error("stream not lazily created")
	}
	if resp.GetRequest().GetScope() != "live" {
		t.Errorf("scope = %q, want live", resp.GetRequest().GetScope())
	}
}

func TestCreateMarketDataRequest_RejectsZeroUserID(t *testing.T) {
	svc := newSvc()
	_, err := svc.CreateMarketDataRequest(context.Background(), &mdv1.CreateMarketDataRequestRequest{
		UserId: 0, Key: liveKey(),
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("code = %v, want InvalidArgument", status.Code(err))
	}
}

func TestCreateMarketDataRequest_ValidatesKey(t *testing.T) {
	cases := []struct {
		name string
		key  *mdv1.StreamKey
	}{
		{"missing key", nil},
		{"bad market", &mdv1.StreamKey{Exchange: "binance", Market: "weird", Kind: "kline", Symbol: "BTCUSDT", Interval: "1m"}},
		{"bad kind", &mdv1.StreamKey{Exchange: "binance", Market: "spot", Kind: "trades", Symbol: "BTCUSDT", Interval: "1m"}},
		{"bad symbol", &mdv1.StreamKey{Exchange: "binance", Market: "spot", Kind: "kline", Symbol: "x", Interval: "1m"}},
		{"bad interval", &mdv1.StreamKey{Exchange: "binance", Market: "spot", Kind: "kline", Symbol: "BTCUSDT", Interval: "weird"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			svc := newSvc()
			_, err := svc.CreateMarketDataRequest(context.Background(), &mdv1.CreateMarketDataRequestRequest{
				UserId: 42, Key: c.key,
			})
			if status.Code(err) != codes.InvalidArgument {
				t.Errorf("code = %v, want InvalidArgument", status.Code(err))
			}
		})
	}
}

func TestCreateMarketDataRequest_HistoricalScope(t *testing.T) {
	svc := newSvc()
	start := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(24 * time.Hour)
	resp, err := svc.CreateMarketDataRequest(context.Background(), &mdv1.CreateMarketDataRequestRequest{
		UserId:           42,
		Key:              liveKey(),
		Scope:            "historical",
		RequestedStartAt: timestamppb.New(start),
		RequestedEndAt:   timestamppb.New(end),
	})
	if err != nil {
		t.Fatalf("historical Create: %v", err)
	}
	if resp.GetRequest().GetScope() != "historical" {
		t.Errorf("scope = %q, want historical", resp.GetRequest().GetScope())
	}
	if resp.GetRequest().GetStatus() != "pending" {
		t.Errorf("status = %q, want pending", resp.GetRequest().GetStatus())
	}
}

func TestCreateMarketDataRequest_HistoricalRejectsLiveDelivery(t *testing.T) {
	svc := newSvc()
	start := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(24 * time.Hour)
	_, err := svc.CreateMarketDataRequest(context.Background(), &mdv1.CreateMarketDataRequestRequest{
		UserId: 42, Key: liveKey(),
		Scope:             "historical",
		NeedsLiveDelivery: true,
		RequestedStartAt:  timestamppb.New(start),
		RequestedEndAt:    timestamppb.New(end),
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("code = %v, want InvalidArgument", status.Code(err))
	}
}

func TestCreateMarketDataRequest_HistoricalEndBeforeStart(t *testing.T) {
	svc := newSvc()
	start := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(-1 * time.Hour)
	_, err := svc.CreateMarketDataRequest(context.Background(), &mdv1.CreateMarketDataRequestRequest{
		UserId: 42, Key: liveKey(),
		Scope:            "historical",
		RequestedStartAt: timestamppb.New(start),
		RequestedEndAt:   timestamppb.New(end),
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("code = %v, want InvalidArgument", status.Code(err))
	}
}

// ── CancelMarketDataRequest ────────────────────────────────────────────

func TestCancelMarketDataRequest_Live(t *testing.T) {
	svc := newSvc()
	created, _ := svc.CreateMarketDataRequest(context.Background(), &mdv1.CreateMarketDataRequestRequest{
		UserId: 42, Key: liveKey(),
	})
	_, err := svc.CancelMarketDataRequest(context.Background(), &mdv1.CancelMarketDataRequestRequest{
		UserId: 42, RequestId: created.GetRequest().GetRequestId(),
	})
	if err != nil {
		t.Fatalf("Cancel: %v", err)
	}
}

func TestCancelMarketDataRequest_NotFound(t *testing.T) {
	svc := newSvc()
	_, err := svc.CancelMarketDataRequest(context.Background(), &mdv1.CancelMarketDataRequestRequest{
		UserId: 42, RequestId: 999,
	})
	if status.Code(err) != codes.NotFound {
		t.Errorf("code = %v, want NotFound", status.Code(err))
	}
}

func TestCancelMarketDataRequest_PermissionDeniedAcrossUsers(t *testing.T) {
	svc := newSvc()
	created, _ := svc.CreateMarketDataRequest(context.Background(), &mdv1.CreateMarketDataRequestRequest{
		UserId: 42, Key: liveKey(),
	})
	_, err := svc.CancelMarketDataRequest(context.Background(), &mdv1.CancelMarketDataRequestRequest{
		UserId:    99,
		RequestId: created.GetRequest().GetRequestId(),
	})
	if status.Code(err) != codes.PermissionDenied {
		t.Errorf("code = %v, want PermissionDenied", status.Code(err))
	}
}

// ── ListMarketDataRequests ─────────────────────────────────────────────

func TestListMarketDataRequests_MergesLiveAndHistorical(t *testing.T) {
	svc := newSvc()
	_, _ = svc.CreateMarketDataRequest(context.Background(), &mdv1.CreateMarketDataRequestRequest{
		UserId: 42, Key: liveKey(),
	})
	start := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(24 * time.Hour)
	_, _ = svc.CreateMarketDataRequest(context.Background(), &mdv1.CreateMarketDataRequestRequest{
		UserId: 42, Key: liveKey(),
		Scope: "historical", RequestedStartAt: timestamppb.New(start), RequestedEndAt: timestamppb.New(end),
	})
	resp, err := svc.ListMarketDataRequests(context.Background(), &mdv1.ListMarketDataRequestsRequest{UserId: 42})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(resp.GetEntries()) != 2 {
		t.Errorf("entries = %d, want 2", len(resp.GetEntries()))
	}
}

func TestListMarketDataRequests_ExcludesCancelledLiveRequestAfterRestart(t *testing.T) {
	svc := newSvc()
	first, err := svc.CreateMarketDataRequest(context.Background(), &mdv1.CreateMarketDataRequestRequest{
		UserId:            42,
		Key:               liveKey(),
		NeedsLiveDelivery: true,
	})
	if err != nil {
		t.Fatalf("first Create: %v", err)
	}
	_, err = svc.CancelMarketDataRequest(context.Background(), &mdv1.CancelMarketDataRequestRequest{
		UserId:    42,
		RequestId: first.GetRequest().GetRequestId(),
	})
	if err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	second, err := svc.CreateMarketDataRequest(context.Background(), &mdv1.CreateMarketDataRequestRequest{
		UserId:            42,
		Key:               liveKey(),
		NeedsLiveDelivery: true,
	})
	if err != nil {
		t.Fatalf("second Create: %v", err)
	}

	resp, err := svc.ListMarketDataRequests(context.Background(), &mdv1.ListMarketDataRequestsRequest{UserId: 42})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(resp.GetEntries()) != 1 {
		t.Fatalf("entries = %d, want 1", len(resp.GetEntries()))
	}
	got := resp.GetEntries()[0].GetRequest()
	if got.GetRequestId() != second.GetRequest().GetRequestId() {
		t.Errorf("request_id = %d, want %d", got.GetRequestId(), second.GetRequest().GetRequestId())
	}
	if got.GetStatus() != "active" {
		t.Errorf("status = %q, want active", got.GetStatus())
	}
}

// ── GetMarketDataStreamStatus ──────────────────────────────────────────

func TestGetMarketDataStreamStatus_ByID(t *testing.T) {
	svc := newSvc()
	created, _ := svc.CreateMarketDataRequest(context.Background(), &mdv1.CreateMarketDataRequestRequest{
		UserId: 42, Key: liveKey(),
	})
	resp, err := svc.GetMarketDataStreamStatus(context.Background(), &mdv1.GetMarketDataStreamStatusRequest{
		StreamId: created.GetStream().GetStreamId(),
	})
	if err != nil {
		t.Fatalf("Get by id: %v", err)
	}
	if resp.GetStream().GetKey().GetSymbol() != "BTCUSDT" {
		t.Errorf("symbol = %q, want BTCUSDT", resp.GetStream().GetKey().GetSymbol())
	}
}

func TestGetMarketDataStreamStatus_ByKey(t *testing.T) {
	svc := newSvc()
	_, _ = svc.CreateMarketDataRequest(context.Background(), &mdv1.CreateMarketDataRequestRequest{
		UserId: 42, Key: liveKey(),
	})
	resp, err := svc.GetMarketDataStreamStatus(context.Background(), &mdv1.GetMarketDataStreamStatusRequest{
		Key: liveKey(),
	})
	if err != nil {
		t.Fatalf("Get by key: %v", err)
	}
	if resp.GetStream().GetStreamId() == 0 {
		t.Error("stream_id missing")
	}
}

func TestGetMarketDataStreamStatus_NotFound(t *testing.T) {
	svc := newSvc()
	_, err := svc.GetMarketDataStreamStatus(context.Background(), &mdv1.GetMarketDataStreamStatusRequest{
		StreamId: 999,
	})
	if status.Code(err) != codes.NotFound {
		t.Errorf("code = %v, want NotFound", status.Code(err))
	}
}

// ── ListMarketDataStreams ──────────────────────────────────────────────

func TestListMarketDataStreams(t *testing.T) {
	svc := newSvc()
	_, _ = svc.CreateMarketDataRequest(context.Background(), &mdv1.CreateMarketDataRequestRequest{
		UserId: 42, Key: liveKey(),
	})
	resp, err := svc.ListMarketDataStreams(context.Background(), &mdv1.ListMarketDataStreamsRequest{})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(resp.GetStreams()) != 1 {
		t.Errorf("streams = %d, want 1", len(resp.GetStreams()))
	}
}

// ── ReportMarketDataStreamState ────────────────────────────────────────

func TestReportMarketDataStreamState_HappyPath(t *testing.T) {
	svc := newSvc()
	created, _ := svc.CreateMarketDataRequest(context.Background(), &mdv1.CreateMarketDataRequestRequest{
		UserId: 42, Key: liveKey(),
	})
	now := time.Now()
	resp, err := svc.ReportMarketDataStreamState(context.Background(), &mdv1.ReportMarketDataStreamStateRequest{
		StreamId:    created.GetStream().GetStreamId(),
		ActualState: "running",
		LastDataAt:  timestamppb.New(now),
	})
	if err != nil {
		t.Fatalf("Report: %v", err)
	}
	if resp.GetStream().GetActualState() != "running" {
		t.Errorf("actual_state = %q, want running", resp.GetStream().GetActualState())
	}
}

func TestReportMarketDataStreamState_RejectsBadState(t *testing.T) {
	svc := newSvc()
	created, _ := svc.CreateMarketDataRequest(context.Background(), &mdv1.CreateMarketDataRequestRequest{
		UserId: 42, Key: liveKey(),
	})
	_, err := svc.ReportMarketDataStreamState(context.Background(), &mdv1.ReportMarketDataStreamStateRequest{
		StreamId:    created.GetStream().GetStreamId(),
		ActualState: "weird",
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("code = %v, want InvalidArgument", status.Code(err))
	}
}

func TestReportMarketDataStreamState_NotFound(t *testing.T) {
	svc := newSvc()
	_, err := svc.ReportMarketDataStreamState(context.Background(), &mdv1.ReportMarketDataStreamStateRequest{
		StreamId:    999,
		ActualState: "running",
	})
	if status.Code(err) != codes.NotFound {
		t.Errorf("code = %v, want NotFound", status.Code(err))
	}
}

// ── Lease lifecycle ────────────────────────────────────────────────────

func TestCreateOrRenewLease_HappyPath(t *testing.T) {
	svc := newSvc()
	created, _ := svc.CreateMarketDataRequest(context.Background(), &mdv1.CreateMarketDataRequestRequest{
		UserId: 42, Key: liveKey(),
	})
	resp, err := svc.CreateOrRenewMarketDataLease(context.Background(), &mdv1.CreateOrRenewMarketDataLeaseRequest{
		SessionId:  "sess-1",
		StreamId:   created.GetStream().GetStreamId(),
		TtlSeconds: 60,
	})
	if err != nil {
		t.Fatalf("CreateOrRenew: %v", err)
	}
	if resp.GetLease().GetSessionId() != "sess-1" {
		t.Errorf("session_id = %q, want sess-1", resp.GetLease().GetSessionId())
	}
}

func TestCreateOrRenewLease_TTLClampedToMin(t *testing.T) {
	svc := newSvc()
	created, _ := svc.CreateMarketDataRequest(context.Background(), &mdv1.CreateMarketDataRequestRequest{
		UserId: 42, Key: liveKey(),
	})
	resp, err := svc.CreateOrRenewMarketDataLease(context.Background(), &mdv1.CreateOrRenewMarketDataLeaseRequest{
		SessionId:  "sess-1",
		StreamId:   created.GetStream().GetStreamId(),
		TtlSeconds: 5, // below minLeaseTTL
	})
	if err != nil {
		t.Fatalf("CreateOrRenew: %v", err)
	}
	// We can't check the exact expiry but want at least minLeaseTTL into the future.
	now := time.Now()
	if resp.GetLease().GetExpiresAt().AsTime().Before(now.Add(minLeaseTTL - time.Second)) {
		t.Errorf("expires_at not pushed to at least minLeaseTTL")
	}
}

func TestCreateOrRenewLease_RequiresSessionAndStream(t *testing.T) {
	svc := newSvc()
	cases := []*mdv1.CreateOrRenewMarketDataLeaseRequest{
		{SessionId: "", StreamId: 1},
		{SessionId: "s", StreamId: 0},
	}
	for _, req := range cases {
		_, err := svc.CreateOrRenewMarketDataLease(context.Background(), req)
		if status.Code(err) != codes.InvalidArgument {
			t.Errorf("code = %v, want InvalidArgument", status.Code(err))
		}
	}
}

func TestReleaseLease(t *testing.T) {
	svc := newSvc()
	created, _ := svc.CreateMarketDataRequest(context.Background(), &mdv1.CreateMarketDataRequestRequest{
		UserId: 42, Key: liveKey(),
	})
	_, err := svc.CreateOrRenewMarketDataLease(context.Background(), &mdv1.CreateOrRenewMarketDataLeaseRequest{
		SessionId: "sess-1", StreamId: created.GetStream().GetStreamId(), TtlSeconds: 60,
	})
	if err != nil {
		t.Fatalf("create lease: %v", err)
	}
	_, err = svc.ReleaseMarketDataLease(context.Background(), &mdv1.ReleaseMarketDataLeaseRequest{
		SessionId: "sess-1", StreamId: created.GetStream().GetStreamId(),
	})
	if err != nil {
		t.Fatalf("Release: %v", err)
	}
}

// ── ListMarketDataHistoryRequests ──────────────────────────────────────

func TestListMarketDataHistoryRequests(t *testing.T) {
	svc := newSvc()
	start := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(24 * time.Hour)
	_, _ = svc.CreateMarketDataRequest(context.Background(), &mdv1.CreateMarketDataRequestRequest{
		UserId: 42, Key: liveKey(),
		Scope: "historical", RequestedStartAt: timestamppb.New(start), RequestedEndAt: timestamppb.New(end),
	})
	resp, err := svc.ListMarketDataHistoryRequests(context.Background(), &mdv1.ListMarketDataHistoryRequestsRequest{})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(resp.GetRequests()) != 1 {
		t.Errorf("requests = %d, want 1", len(resp.GetRequests()))
	}
}

// ── ReportMarketDataHistoryRequestState ────────────────────────────────

func TestReportMarketDataHistoryRequestState(t *testing.T) {
	svc := newSvc()
	start := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(24 * time.Hour)
	created, _ := svc.CreateMarketDataRequest(context.Background(), &mdv1.CreateMarketDataRequestRequest{
		UserId: 42, Key: liveKey(),
		Scope: "historical", RequestedStartAt: timestamppb.New(start), RequestedEndAt: timestamppb.New(end),
	})
	resp, err := svc.ReportMarketDataHistoryRequestState(context.Background(), &mdv1.ReportMarketDataHistoryRequestStateRequest{
		RequestId: created.GetRequest().GetRequestId(),
		Status:    "ready",
	})
	if err != nil {
		t.Fatalf("Report: %v", err)
	}
	if resp.GetRequest().GetStatus() != "ready" {
		t.Errorf("status = %q, want ready", resp.GetRequest().GetStatus())
	}
}

func TestReportMarketDataHistoryRequestState_RejectsBadStatus(t *testing.T) {
	svc := newSvc()
	_, err := svc.ReportMarketDataHistoryRequestState(context.Background(), &mdv1.ReportMarketDataHistoryRequestStateRequest{
		RequestId: 1,
		Status:    "weird",
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("code = %v, want InvalidArgument", status.Code(err))
	}
}

// ── RuntimeChannel delivery subscriptions ──────────────────────────────

func TestCreateSessionMarketDataSubscriptions(t *testing.T) {
	svc := newSvc()
	resp, err := svc.CreateSessionMarketDataSubscriptions(context.Background(), &mdv1.CreateSessionMarketDataSubscriptionsRequest{
		UserId:      42,
		SessionId:   "sess-1",
		RuntimeId:   "rt-1",
		Environment: 1,
		Keys:        []*mdv1.StreamKey{liveKey()},
	})
	if err != nil {
		t.Fatalf("CreateSessionMarketDataSubscriptions: %v", err)
	}
	if len(resp.GetSubscriptions()) != 1 {
		t.Fatalf("subscriptions = %d, want 1", len(resp.GetSubscriptions()))
	}
	sub := resp.GetSubscriptions()[0]
	if sub.GetUserId() != 42 || sub.GetSessionId() != "sess-1" || sub.GetRuntimeId() != "rt-1" {
		t.Fatalf("subscription owner fields = %+v", sub)
	}
	if sub.GetKey().GetSymbol() != "BTCUSDT" || sub.GetEnvironment() != 1 || sub.GetStatus() != "active" {
		t.Fatalf("subscription key/environment/status = %+v", sub)
	}
}

func TestCreateSessionMarketDataSubscriptionsRejectsWrongEnvironment(t *testing.T) {
	svc := newSvc()
	_, err := svc.CreateSessionMarketDataSubscriptions(context.Background(), &mdv1.CreateSessionMarketDataSubscriptionsRequest{
		UserId:      42,
		SessionId:   "sess-1",
		RuntimeId:   "rt-1",
		Environment: 0,
		Keys:        []*mdv1.StreamKey{liveKey()},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("code = %v, want InvalidArgument", status.Code(err))
	}
}

func TestCreateOrRenewStreamDeliveryLease(t *testing.T) {
	svc := newSvc()
	subs, err := svc.CreateSessionMarketDataSubscriptions(context.Background(), &mdv1.CreateSessionMarketDataSubscriptionsRequest{
		UserId:      42,
		SessionId:   "sess-1",
		RuntimeId:   "rt-1",
		Environment: 1,
		Keys:        []*mdv1.StreamKey{liveKey()},
	})
	if err != nil {
		t.Fatalf("create subscription: %v", err)
	}
	resp, err := svc.CreateOrRenewStreamDeliveryLease(context.Background(), &mdv1.CreateOrRenewStreamDeliveryLeaseRequest{
		SubscriptionId:  subs.GetSubscriptions()[0].GetSubscriptionId(),
		OwnerInstanceId: "cp-1",
		TtlSeconds:      90,
	})
	if err != nil {
		t.Fatalf("CreateOrRenewStreamDeliveryLease: %v", err)
	}
	if resp.GetLease().GetOwnerInstanceId() != "cp-1" || resp.GetLease().GetStatus() != "active" {
		t.Fatalf("lease = %+v", resp.GetLease())
	}
}

func TestListSessionDeliveryHealthMarksBlockedWithoutProgress(t *testing.T) {
	repo := newStubRepo()
	svc := NewService(repo)
	subs, err := svc.CreateSessionMarketDataSubscriptions(context.Background(), &mdv1.CreateSessionMarketDataSubscriptionsRequest{
		UserId:      42,
		SessionId:   "sess-1",
		RuntimeId:   "rt-1",
		Environment: 1,
		Keys:        []*mdv1.StreamKey{liveKey()},
	})
	if err != nil {
		t.Fatalf("create subscription: %v", err)
	}
	subID := subs.GetSubscriptions()[0].GetSubscriptionId()
	if _, err := svc.CreateOrRenewStreamDeliveryLease(context.Background(), &mdv1.CreateOrRenewStreamDeliveryLeaseRequest{
		SubscriptionId:  subID,
		OwnerInstanceId: "cp-1",
		TtlSeconds:      90,
	}); err != nil {
		t.Fatalf("CreateOrRenewStreamDeliveryLease: %v", err)
	}
	repo.mu.Lock()
	sub := repo.subs[subID]
	sub.CreatedAt = time.Now().Add(-2 * deliveryProgressGrace)
	repo.subs[subID] = sub
	repo.mu.Unlock()

	resp, err := svc.ListSessionDeliveryHealth(context.Background(), &mdv1.ListSessionDeliveryHealthRequest{
		UserId:    42,
		SessionId: "sess-1",
		RuntimeId: "rt-1",
	})
	if err != nil {
		t.Fatalf("ListSessionDeliveryHealth: %v", err)
	}
	if len(resp.GetItems()) != 1 {
		t.Fatalf("items = %d, want 1", len(resp.GetItems()))
	}
	item := resp.GetItems()[0]
	if item.GetHealthStatus() != "delivery_blocked" || item.GetBlockedReason() == "" {
		t.Fatalf("health = %q/%q, want delivery_blocked reason", item.GetHealthStatus(), item.GetBlockedReason())
	}
}

func TestListSessionDeliveryHealthShowsDeliveryProgress(t *testing.T) {
	repo := newStubRepo()
	svc := NewService(repo)
	subs, err := svc.CreateSessionMarketDataSubscriptions(context.Background(), &mdv1.CreateSessionMarketDataSubscriptionsRequest{
		UserId:      42,
		SessionId:   "sess-1",
		RuntimeId:   "rt-1",
		Environment: 1,
		Keys:        []*mdv1.StreamKey{liveKey()},
	})
	if err != nil {
		t.Fatalf("create subscription: %v", err)
	}
	subID := subs.GetSubscriptions()[0].GetSubscriptionId()
	if _, err := svc.CreateOrRenewStreamDeliveryLease(context.Background(), &mdv1.CreateOrRenewStreamDeliveryLeaseRequest{
		SubscriptionId:  subID,
		OwnerInstanceId: "cp-1",
		TtlSeconds:      90,
	}); err != nil {
		t.Fatalf("CreateOrRenewStreamDeliveryLease: %v", err)
	}
	deliveredAt := time.Now().UTC()
	if err := repo.RecordStreamDeliveryProgress(context.Background(), subID, "cp-1", "md.kline.binance.futures.1m", 3, 99, deliveredAt); err != nil {
		t.Fatalf("RecordStreamDeliveryProgress: %v", err)
	}

	resp, err := svc.ListSessionDeliveryHealth(context.Background(), &mdv1.ListSessionDeliveryHealthRequest{
		UserId:    42,
		SessionId: "sess-1",
		RuntimeId: "rt-1",
	})
	if err != nil {
		t.Fatalf("ListSessionDeliveryHealth: %v", err)
	}
	item := resp.GetItems()[0]
	if item.GetHealthStatus() != "delivering" {
		t.Fatalf("health = %q, want delivering", item.GetHealthStatus())
	}
	if item.GetLease().GetLastOffset() != 99 || item.GetLease().GetLastPartition() != 3 {
		t.Fatalf("lease progress = partition %d offset %d, want 3/99", item.GetLease().GetLastPartition(), item.GetLease().GetLastOffset())
	}
}

func TestCreateOrRenewMarketDataWriterLease(t *testing.T) {
	svc := newSvc()
	resp, err := svc.CreateOrRenewMarketDataWriterLease(context.Background(), &mdv1.CreateOrRenewMarketDataWriterLeaseRequest{
		Key: &mdv1.StreamKey{
			Exchange: "binance",
			Market:   "futures",
			Kind:     "open_interest",
			Symbol:   "BTCUSDT",
		},
		Year:              2026,
		OwnerInstanceId:   "scraper-owner-1",
		ScraperInstanceId: "scraper-1",
		CollectorId:       "binance:futures:open_interest:BTCUSDT",
		TtlSeconds:        90,
	})
	if err != nil {
		t.Fatalf("CreateOrRenewMarketDataWriterLease: %v", err)
	}
	lease := resp.GetLease()
	if lease.GetStatus() != "active" || lease.GetYear() != 2026 {
		t.Fatalf("lease status/year = %+v", lease)
	}
	if lease.GetKey().GetKind() != "open_interest" || lease.GetCollectorId() == "" {
		t.Fatalf("lease key/collector = %+v", lease)
	}
}

func TestCreateOrRenewMarketDataWriterLeaseRejectsActiveOtherOwner(t *testing.T) {
	repo := newStubRepo()
	svc := NewService(repo)
	req := &mdv1.CreateOrRenewMarketDataWriterLeaseRequest{
		Key:               liveKey(),
		Year:              2026,
		OwnerInstanceId:   "owner-1",
		ScraperInstanceId: "scraper-1",
		CollectorId:       "binance:futures:kline:BTCUSDT:1m",
		TtlSeconds:        90,
	}
	if _, err := svc.CreateOrRenewMarketDataWriterLease(context.Background(), req); err != nil {
		t.Fatalf("first writer lease: %v", err)
	}
	req.OwnerInstanceId = "owner-2"
	req.ScraperInstanceId = "scraper-2"
	if _, err := svc.CreateOrRenewMarketDataWriterLease(context.Background(), req); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("code = %v, want FailedPrecondition", status.Code(err))
	}
}

func TestCreateOrRenewMarketDataWriterLeaseReacquiresExpiredLease(t *testing.T) {
	repo := newStubRepo()
	svc := NewService(repo)
	req := &mdv1.CreateOrRenewMarketDataWriterLeaseRequest{
		Key:               liveKey(),
		Year:              2026,
		OwnerInstanceId:   "owner-1",
		ScraperInstanceId: "scraper-1",
		CollectorId:       "binance:futures:kline:BTCUSDT:1m",
		TtlSeconds:        90,
	}
	first, err := svc.CreateOrRenewMarketDataWriterLease(context.Background(), req)
	if err != nil {
		t.Fatalf("first writer lease: %v", err)
	}

	repo.mu.Lock()
	key := repo.writerKey(streamKeyToDomainTest(first.GetLease().GetKey()), first.GetLease().GetYear())
	expired := repo.writer[key]
	expired.ExpiresAt = time.Now().Add(-time.Second)
	repo.writer[key] = expired
	repo.mu.Unlock()

	req.OwnerInstanceId = "owner-2"
	req.ScraperInstanceId = "scraper-2"
	second, err := svc.CreateOrRenewMarketDataWriterLease(context.Background(), req)
	if err != nil {
		t.Fatalf("expired writer lease reacquire: %v", err)
	}
	if second.GetLease().GetOwnerInstanceId() != "owner-2" {
		t.Fatalf("owner = %q, want owner-2", second.GetLease().GetOwnerInstanceId())
	}
}

func streamKeyToDomainTest(k *mdv1.StreamKey) domain.StreamKey {
	return domain.StreamKey{
		Exchange: k.GetExchange(),
		Market:   k.GetMarket(),
		Kind:     k.GetKind(),
		Symbol:   k.GetSymbol(),
		Interval: k.GetInterval(),
	}
}
