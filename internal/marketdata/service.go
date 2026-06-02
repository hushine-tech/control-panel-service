// Package marketdata is the Phase D2 owner of the market-data control
// plane (`market_data_requests` / `_streams` / `_leases` /
// `_history_requests`). The 10 RPCs were ported verbatim from
// core-service (the source `grpc_market_data.go` was deleted in the
// same change). This package is a sibling to the runtime control plane
// in `control-panel-service/internal/runtime/`.
package marketdata

import (
	"context"
	"errors"
	"regexp"
	"sort"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	mdv1 "github.com/hushine-tech/control-panel-service/gen/marketdatav1"
	"github.com/hushine-tech/control-panel-service/internal/domain"
	"github.com/hushine-tech/control-panel-service/internal/marketdata/repository"
	"github.com/hushine-tech/control-panel-service/internal/runtimechannel"
)

// Service binds the marketdata gRPC handlers to the Repository. It is
// constructed in main.go alongside the runtime control-plane service
// and registered on the same gRPC server.
type Service struct {
	mdv1.UnimplementedMarketDataControlPlaneServiceServer
	repo       repository.Repository
	klineQuery KlineQuerier
}

type KlineQuerier interface {
	FetchKlines(context.Context, runtimechannel.KlineQuery) ([]runtimechannel.KlineRow, error)
}

type Option func(*Service)

func WithMarketDataQuery(q KlineQuerier) Option {
	return func(s *Service) {
		s.klineQuery = q
	}
}

func NewService(repo repository.Repository, opts ...Option) *Service {
	s := &Service{repo: repo}
	for _, opt := range opts {
		if opt != nil {
			opt(s)
		}
	}
	return s
}

// ── validation ──────────────────────────────────────────────────────────

// v1 constrains the control plane to kline streams; broaden when
// orderbook / funding / oi land in a later change.
var (
	reValidExchange     = regexp.MustCompile(`^(binance|okx)$`)
	reValidMarket       = regexp.MustCompile(`^(spot|futures)$`)
	reValidKind         = regexp.MustCompile(`^kline$`)
	reValidWriterKind   = regexp.MustCompile(`^(kline|funding_rate|open_interest|orderbook)$`)
	reValidSymbolStream = regexp.MustCompile(`^[A-Z0-9]{2,30}$`)
	reValidInterval     = regexp.MustCompile(`^(1s|5s|10s|30s|1m|3m|5m|15m|30m|1h|2h|4h|6h|8h|12h|1d|3d|1w|1M)$`)
)

// minLeaseTTL / maxLeaseTTL bound what strategy-service can request.
// Default design targets heartbeat 30s / TTL 90s. Out-of-band values are
// CLAMPED into [minLeaseTTL, maxLeaseTTL] rather than rejected — keeps
// CreateOrRenewMarketDataLease tolerant of misconfigured callers without
// letting them keep a stream alive indefinitely. ttl_seconds=0 is treated
// as "use default".
const (
	minLeaseTTL     = 30 * time.Second
	maxLeaseTTL     = 5 * time.Minute
	defaultLeaseTTL = 90 * time.Second
)

const deliveryProgressGrace = defaultLeaseTTL

const rawKlineValidationFetchLimit = 5000
const defaultRawKlineQueryLimit = 100
const maxRawKlineQueryLimit = 1000
const rawCoverageSource = "raw_storage"

func requireUserID(userID int64) error {
	if userID <= 0 {
		return status.Error(codes.InvalidArgument, "user_id is required")
	}
	return nil
}

func validateStreamKey(k *mdv1.StreamKey) (domain.StreamKey, error) {
	if k == nil {
		return domain.StreamKey{}, status.Error(codes.InvalidArgument, "key is required")
	}
	exchange := strings.TrimSpace(strings.ToLower(k.GetExchange()))
	market := strings.TrimSpace(strings.ToLower(k.GetMarket()))
	kind := strings.TrimSpace(strings.ToLower(k.GetKind()))
	symbol := strings.TrimSpace(strings.ToUpper(k.GetSymbol()))
	interval := strings.TrimSpace(k.GetInterval())

	if exchange == "" {
		return domain.StreamKey{}, status.Error(codes.InvalidArgument, "key.exchange is required")
	}
	if !reValidExchange.MatchString(exchange) {
		return domain.StreamKey{}, status.Errorf(codes.InvalidArgument, "key.exchange must be 'binance' or 'okx', got %q", exchange)
	}
	if !reValidMarket.MatchString(market) {
		return domain.StreamKey{}, status.Errorf(codes.InvalidArgument, "key.market must be 'spot' or 'futures', got %q", market)
	}
	if !reValidKind.MatchString(kind) {
		return domain.StreamKey{}, status.Errorf(codes.InvalidArgument, "key.kind must be 'kline' (v1 scope), got %q", kind)
	}
	if !reValidSymbolStream.MatchString(symbol) {
		return domain.StreamKey{}, status.Errorf(codes.InvalidArgument, "key.symbol must be 2-30 upper-case alphanum, got %q", symbol)
	}
	if !reValidInterval.MatchString(interval) {
		return domain.StreamKey{}, status.Errorf(codes.InvalidArgument, "key.interval invalid, got %q", interval)
	}
	return domain.StreamKey{
		Exchange: exchange,
		Market:   market,
		Kind:     kind,
		Symbol:   symbol,
		Interval: interval,
	}, nil
}

func validateWriterKey(k *mdv1.StreamKey) (domain.StreamKey, error) {
	if k == nil {
		return domain.StreamKey{}, status.Error(codes.InvalidArgument, "key is required")
	}
	exchange := strings.TrimSpace(strings.ToLower(k.GetExchange()))
	market := strings.TrimSpace(strings.ToLower(k.GetMarket()))
	kind := strings.TrimSpace(strings.ToLower(k.GetKind()))
	symbol := strings.TrimSpace(strings.ToUpper(k.GetSymbol()))
	interval := strings.TrimSpace(k.GetInterval())

	if !reValidExchange.MatchString(exchange) {
		return domain.StreamKey{}, status.Errorf(codes.InvalidArgument, "key.exchange must be 'binance' or 'okx', got %q", exchange)
	}
	if !reValidMarket.MatchString(market) {
		return domain.StreamKey{}, status.Errorf(codes.InvalidArgument, "key.market must be 'spot' or 'futures', got %q", market)
	}
	if !reValidWriterKind.MatchString(kind) {
		return domain.StreamKey{}, status.Errorf(codes.InvalidArgument, "key.kind must be kline/funding_rate/open_interest/orderbook, got %q", kind)
	}
	if !reValidSymbolStream.MatchString(symbol) {
		return domain.StreamKey{}, status.Errorf(codes.InvalidArgument, "key.symbol must be 2-30 upper-case alphanum, got %q", symbol)
	}
	if kind == "kline" {
		if !reValidInterval.MatchString(interval) {
			return domain.StreamKey{}, status.Errorf(codes.InvalidArgument, "key.interval invalid, got %q", interval)
		}
	} else if interval != "" {
		return domain.StreamKey{}, status.Errorf(codes.InvalidArgument, "key.interval must be empty for writer kind %q", kind)
	}
	if (kind == "funding_rate" || kind == "open_interest") && market != "futures" {
		return domain.StreamKey{}, status.Errorf(codes.InvalidArgument, "writer kind %q requires market=futures", kind)
	}
	return domain.StreamKey{
		Exchange: exchange,
		Market:   market,
		Kind:     kind,
		Symbol:   symbol,
		Interval: interval,
	}, nil
}

func normalizeRequestScope(raw string) (domain.MarketDataRequestScope, error) {
	scope := strings.TrimSpace(strings.ToLower(raw))
	if scope == "" {
		return domain.RequestScopeLive, nil
	}
	switch scope {
	case string(domain.RequestScopeLive):
		return domain.RequestScopeLive, nil
	case string(domain.RequestScopeHistorical):
		return domain.RequestScopeHistorical, nil
	default:
		return "", status.Errorf(codes.InvalidArgument, "scope must be 'live' or 'historical', got %q", raw)
	}
}

func validateHistoricalRange(start, end *timestamppb.Timestamp) (time.Time, time.Time, error) {
	if start == nil || end == nil {
		return time.Time{}, time.Time{}, status.Error(codes.InvalidArgument, "historical scope requires requested_start_at and requested_end_at")
	}
	startAt := start.AsTime().UTC()
	endAt := end.AsTime().UTC()
	if startAt.IsZero() || endAt.IsZero() {
		return time.Time{}, time.Time{}, status.Error(codes.InvalidArgument, "historical scope requires non-zero requested_start_at and requested_end_at")
	}
	if !endAt.After(startAt) {
		return time.Time{}, time.Time{}, status.Error(codes.InvalidArgument, "requested_end_at must be after requested_start_at")
	}
	return startAt, endAt, nil
}

func normalizeHistoryStatus(raw string) (domain.MarketDataHistoryRequestStatus, error) {
	statusValue := domain.MarketDataHistoryRequestStatus(strings.TrimSpace(strings.ToLower(raw)))
	switch statusValue {
	case domain.HistoryRequestRunning,
		domain.HistoryRequestVerifying,
		domain.HistoryRequestReady,
		domain.HistoryRequestError:
		return statusValue, nil
	default:
		return "", status.Errorf(codes.InvalidArgument, "history status %q is not recognized", raw)
	}
}

// ── helpers ─────────────────────────────────────────────────────────────

// activeLeaseCount returns the active lease count for a stream, or 0 if
// the count query fails. The count is purely UI/display metadata
// (`MarketDataStream.active_lease_count`) — it is NOT consulted by any
// scheduling decision in scraper or strategy-service. We intentionally
// swallow DB errors here so a transient blip on this single auxiliary
// query does not fail an otherwise-successful CRUD RPC. If the
// underlying DB is actually broken, the primary query before this call
// will already have surfaced an Internal error to the caller.
//
// This matches the `core-service` behaviour pre-D2.
func (s *Service) activeLeaseCount(ctx context.Context, streamID int64) int {
	n, err := s.repo.CountActiveLeasesForStream(ctx, streamID)
	if err != nil {
		return 0
	}
	return n
}

// ── proto converters ────────────────────────────────────────────────────

func toProtoStreamKey(k domain.StreamKey) *mdv1.StreamKey {
	return &mdv1.StreamKey{
		Exchange: k.Exchange,
		Market:   k.Market,
		Kind:     k.Kind,
		Symbol:   k.Symbol,
		Interval: k.Interval,
	}
}

func toProtoStream(s domain.MarketDataStream, activeLeaseCount int) *mdv1.MarketDataStream {
	out := &mdv1.MarketDataStream{
		StreamId:              s.StreamID,
		Key:                   toProtoStreamKey(s.Key),
		DesiredState:          string(s.DesiredState),
		ActualState:           string(s.ActualState),
		EffectiveLiveDelivery: s.EffectiveLiveDelivery,
		LastError:             s.LastError,
		ActiveLeaseCount:      int32(activeLeaseCount),
		CreatedAt:             timestamppb.New(s.CreatedAt),
		UpdatedAt:             timestamppb.New(s.UpdatedAt),
	}
	if s.LastDataAt != nil {
		out.LastDataAt = timestamppb.New(*s.LastDataAt)
	}
	if s.LastReconciledAt != nil {
		out.LastReconciledAt = timestamppb.New(*s.LastReconciledAt)
	}
	return out
}

func toProtoRequest(r domain.MarketDataRequest) *mdv1.MarketDataRequest {
	out := &mdv1.MarketDataRequest{
		RequestId:         r.RequestID,
		UserId:            r.UserID,
		StreamId:          r.StreamID,
		Key:               toProtoStreamKey(r.Key),
		NeedsLiveDelivery: r.NeedsLiveDelivery,
		Status:            string(r.Status),
		Scope:             string(domain.RequestScopeLive),
		CreatedAt:         timestamppb.New(r.CreatedAt),
		UpdatedAt:         timestamppb.New(r.UpdatedAt),
	}
	if r.AccountID != nil {
		out.AccountId = *r.AccountID
	}
	if r.CancelledAt != nil {
		out.CancelledAt = timestamppb.New(*r.CancelledAt)
	}
	return out
}

func toProtoHistoryRequest(r domain.MarketDataHistoryRequest) *mdv1.MarketDataRequest {
	out := &mdv1.MarketDataRequest{
		RequestId:         r.RequestID,
		UserId:            r.UserID,
		AccountId:         0,
		StreamId:          0,
		Key:               toProtoStreamKey(r.Key),
		NeedsLiveDelivery: false,
		Status:            string(r.Status),
		Scope:             string(domain.RequestScopeHistorical),
		RequestedStartAt:  timestamppb.New(r.RequestedStartAt),
		RequestedEndAt:    timestamppb.New(r.RequestedEndAt),
		LastError:         r.LastError,
		Ready:             r.Status == domain.HistoryRequestReady,
		CreatedAt:         timestamppb.New(r.CreatedAt),
		UpdatedAt:         timestamppb.New(r.UpdatedAt),
	}
	if r.AccountID != nil {
		out.AccountId = *r.AccountID
	}
	if r.CoveredStartAt != nil {
		out.CoveredStartAt = timestamppb.New(*r.CoveredStartAt)
	}
	if r.CoveredEndAt != nil {
		out.CoveredEndAt = timestamppb.New(*r.CoveredEndAt)
	}
	if r.CancelledAt != nil {
		out.CancelledAt = timestamppb.New(*r.CancelledAt)
	}
	return out
}

func toProtoLease(l domain.MarketDataLease) *mdv1.MarketDataLease {
	out := &mdv1.MarketDataLease{
		LeaseId:         l.LeaseID,
		SessionId:       l.SessionID,
		StreamId:        l.StreamID,
		ExpiresAt:       timestamppb.New(l.ExpiresAt),
		LastHeartbeatAt: timestamppb.New(l.LastHeartbeatAt),
		CreatedAt:       timestamppb.New(l.CreatedAt),
	}
	if l.StrategyID != nil {
		out.StrategyId = *l.StrategyID
	}
	if l.AccountID != nil {
		out.AccountId = *l.AccountID
	}
	if l.ReleasedAt != nil {
		out.ReleasedAt = timestamppb.New(*l.ReleasedAt)
	}
	return out
}

func toProtoSessionSubscription(s domain.SessionMarketDataSubscription) *mdv1.SessionMarketDataSubscription {
	out := &mdv1.SessionMarketDataSubscription{
		SubscriptionId: s.SubscriptionID,
		UserId:         s.UserID,
		SessionId:      s.SessionID,
		RuntimeId:      s.RuntimeID,
		Key:            toProtoStreamKey(s.Key),
		Environment:    s.Environment,
		Status:         s.Status,
		CreatedAt:      timestamppb.New(s.CreatedAt),
		UpdatedAt:      timestamppb.New(s.UpdatedAt),
	}
	if s.ReleasedAt != nil {
		out.ReleasedAt = timestamppb.New(*s.ReleasedAt)
	}
	return out
}

func toProtoStreamDeliveryLease(l domain.StreamDeliveryLease) *mdv1.StreamDeliveryLease {
	out := &mdv1.StreamDeliveryLease{
		LeaseId:         l.LeaseID,
		SubscriptionId:  l.SubscriptionID,
		OwnerInstanceId: l.OwnerInstanceID,
		Status:          l.Status,
		AcquiredAt:      timestamppb.New(l.AcquiredAt),
		LastHeartbeatAt: timestamppb.New(l.LastHeartbeatAt),
		ExpiresAt:       timestamppb.New(l.ExpiresAt),
		CreatedAt:       timestamppb.New(l.CreatedAt),
		UpdatedAt:       timestamppb.New(l.UpdatedAt),
		LastTopic:       l.LastTopic,
		LastPartition:   l.LastPartition,
		LastOffset:      l.LastOffset,
	}
	if l.LastDeliveryAt != nil {
		out.LastDeliveryAt = timestamppb.New(*l.LastDeliveryAt)
	}
	if l.ReleasedAt != nil {
		out.ReleasedAt = timestamppb.New(*l.ReleasedAt)
	}
	return out
}

func toProtoStreamDeliveryFailure(f domain.StreamDeliveryFailure) *mdv1.StreamDeliveryFailure {
	out := &mdv1.StreamDeliveryFailure{
		FailureId:       f.FailureID,
		SubscriptionId:  f.SubscriptionID,
		OwnerInstanceId: f.OwnerInstanceID,
		Topic:           f.Topic,
		StreamKey:       f.StreamKey,
		FailureCode:     f.FailureCode,
		Reason:          f.Reason,
		AttemptCount:    int32(f.AttemptCount),
	}
	if !f.FirstSeenAt.IsZero() {
		out.FirstSeenAt = timestamppb.New(f.FirstSeenAt)
	}
	if !f.LastSeenAt.IsZero() {
		out.LastSeenAt = timestamppb.New(f.LastSeenAt)
	}
	return out
}

func toProtoSessionDeliveryHealth(h domain.SessionDeliveryHealth) *mdv1.SessionDeliveryHealth {
	out := &mdv1.SessionDeliveryHealth{
		Subscription:  toProtoSessionSubscription(h.Subscription),
		HealthStatus:  h.HealthStatus,
		BlockedReason: h.BlockedReason,
	}
	if h.Lease != nil {
		out.Lease = toProtoStreamDeliveryLease(*h.Lease)
	}
	if h.LatestFailure != nil {
		out.LatestFailure = toProtoStreamDeliveryFailure(*h.LatestFailure)
	}
	if !h.ObservedAt.IsZero() {
		out.ObservedAt = timestamppb.New(h.ObservedAt)
	}
	return out
}

func toProtoMarketDataWriterLease(l domain.MarketDataWriterLease) *mdv1.MarketDataWriterLease {
	out := &mdv1.MarketDataWriterLease{
		LeaseId:           l.LeaseID,
		Key:               toProtoStreamKey(l.Key),
		Year:              l.Year,
		OwnerInstanceId:   l.OwnerInstanceID,
		ScraperInstanceId: l.ScraperInstanceID,
		CollectorId:       l.CollectorID,
		Status:            l.Status,
		AcquiredAt:        timestamppb.New(l.AcquiredAt),
		LastHeartbeatAt:   timestamppb.New(l.LastHeartbeatAt),
		ExpiresAt:         timestamppb.New(l.ExpiresAt),
		CreatedAt:         timestamppb.New(l.CreatedAt),
		UpdatedAt:         timestamppb.New(l.UpdatedAt),
	}
	if l.ReleasedAt != nil {
		out.ReleasedAt = timestamppb.New(*l.ReleasedAt)
	}
	return out
}

func toProtoMarketDataTimeRange(r domain.MarketDataTimeRange) *mdv1.MarketDataTimeRange {
	return &mdv1.MarketDataTimeRange{
		StartAt:       timestamppb.New(r.StartAt),
		EndAt:         timestamppb.New(r.EndAt),
		ExpectedCount: r.ExpectedCount,
	}
}

func toProtoMarketDataCoverageSegment(seg domain.MarketDataCoverageSegment) *mdv1.MarketDataCoverageSegment {
	return &mdv1.MarketDataCoverageSegment{
		Key:      toProtoStreamKey(seg.Key),
		Year:     seg.Year,
		StartAt:  timestamppb.New(seg.StartAt),
		EndAt:    timestamppb.New(seg.EndAt),
		RowCount: seg.RowCount,
		Source:   seg.Source,
	}
}

func toProtoMarketDataKline(row runtimechannel.KlineRow) *mdv1.MarketDataKline {
	return &mdv1.MarketDataKline{
		OpenTime:  timestamppb.New(time.UnixMilli(row.OpenTime).UTC()),
		CloseTime: timestamppb.New(time.UnixMilli(row.CloseTime).UTC()),
		Open:      row.Open,
		High:      row.High,
		Low:       row.Low,
		Close:     row.Close,
		Volume:    row.Volume,
	}
}

func validateCoverageSegment(raw *mdv1.MarketDataCoverageSegment) (domain.MarketDataCoverageSegment, error) {
	if raw == nil {
		return domain.MarketDataCoverageSegment{}, status.Error(codes.InvalidArgument, "coverage segment is required")
	}
	key, err := validateStreamKey(raw.GetKey())
	if err != nil {
		return domain.MarketDataCoverageSegment{}, err
	}
	if raw.GetYear() < 1970 {
		return domain.MarketDataCoverageSegment{}, status.Errorf(codes.InvalidArgument, "coverage segment year %d is invalid", raw.GetYear())
	}
	if raw.GetStartAt() == nil || raw.GetEndAt() == nil {
		return domain.MarketDataCoverageSegment{}, status.Error(codes.InvalidArgument, "coverage segment start_at and end_at are required")
	}
	startAt := raw.GetStartAt().AsTime().UTC()
	endAt := raw.GetEndAt().AsTime().UTC()
	if startAt.IsZero() || endAt.IsZero() {
		return domain.MarketDataCoverageSegment{}, status.Error(codes.InvalidArgument, "coverage segment start_at and end_at must be non-zero")
	}
	if !endAt.After(startAt) {
		return domain.MarketDataCoverageSegment{}, status.Error(codes.InvalidArgument, "coverage segment end_at must be after start_at")
	}
	yearStart := time.Date(int(raw.GetYear()), 1, 1, 0, 0, 0, 0, time.UTC)
	nextYearStart := yearStart.AddDate(1, 0, 0)
	if startAt.Before(yearStart) || !startAt.Before(nextYearStart) {
		return domain.MarketDataCoverageSegment{}, status.Errorf(codes.InvalidArgument, "coverage segment start_at must fall within year %d UTC", raw.GetYear())
	}
	if endAt.After(nextYearStart) {
		return domain.MarketDataCoverageSegment{}, status.Errorf(codes.InvalidArgument, "coverage segment end_at must not exceed %d-01-01 UTC", raw.GetYear()+1)
	}
	if raw.GetRowCount() <= 0 {
		return domain.MarketDataCoverageSegment{}, status.Error(codes.InvalidArgument, "coverage segment row_count must be positive")
	}
	source := strings.TrimSpace(raw.GetSource())
	if source == "" {
		return domain.MarketDataCoverageSegment{}, status.Error(codes.InvalidArgument, "coverage segment source is required")
	}
	n, err := expectedCount(startAt, endAt, key.Interval)
	if err != nil {
		return domain.MarketDataCoverageSegment{}, status.Errorf(codes.InvalidArgument, "coverage segment interval: %v", err)
	}
	if raw.GetRowCount() != n {
		return domain.MarketDataCoverageSegment{}, status.Errorf(codes.InvalidArgument, "coverage segment row_count = %d, want %d for interval %s", raw.GetRowCount(), n, key.Interval)
	}
	return domain.MarketDataCoverageSegment{
		Key:      key,
		Year:     raw.GetYear(),
		StartAt:  startAt,
		EndAt:    endAt,
		RowCount: raw.GetRowCount(),
		Source:   source,
	}, nil
}

// ── User-facing request RPCs ────────────────────────────────────────────

func (s *Service) CreateMarketDataRequest(ctx context.Context, req *mdv1.CreateMarketDataRequestRequest) (*mdv1.CreateMarketDataRequestResponse, error) {
	if err := requireUserID(req.GetUserId()); err != nil {
		return nil, err
	}
	key, err := validateStreamKey(req.GetKey())
	if err != nil {
		return nil, err
	}
	scope, err := normalizeRequestScope(req.GetScope())
	if err != nil {
		return nil, err
	}
	var accountID *int64
	if aid := req.GetAccountId(); aid > 0 {
		accountID = &aid
	}
	if scope == domain.RequestScopeHistorical {
		if req.GetNeedsLiveDelivery() {
			return nil, status.Error(codes.InvalidArgument, "historical scope does not support needs_live_delivery=true")
		}
		startAt, endAt, err := validateHistoricalRange(req.GetRequestedStartAt(), req.GetRequestedEndAt())
		if err != nil {
			return nil, err
		}
		r, err := s.repo.UpsertMarketDataHistoryRequest(ctx, req.GetUserId(), accountID, key, startAt, endAt)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "upsert historical market-data request: %v", err)
		}
		return &mdv1.CreateMarketDataRequestResponse{
			Request: toProtoHistoryRequest(r),
		}, nil
	}

	r, err := s.repo.UpsertMarketDataRequest(ctx, req.GetUserId(), accountID, key, req.GetNeedsLiveDelivery())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "upsert market-data request: %v", err)
	}
	stream, err := s.repo.GetMarketDataStream(ctx, r.StreamID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get stream for new request: %v", err)
	}
	return &mdv1.CreateMarketDataRequestResponse{
		Request: toProtoRequest(r),
		Stream:  toProtoStream(stream, s.activeLeaseCount(ctx, stream.StreamID)),
	}, nil
}

func (s *Service) CancelMarketDataRequest(ctx context.Context, req *mdv1.CancelMarketDataRequestRequest) (*mdv1.CancelMarketDataRequestResponse, error) {
	if err := requireUserID(req.GetUserId()); err != nil {
		return nil, err
	}
	if req.GetRequestId() <= 0 {
		return nil, status.Error(codes.InvalidArgument, "request_id is required")
	}
	err := s.repo.CancelMarketDataRequest(ctx, req.GetRequestId(), req.GetUserId())
	if errors.Is(err, repository.ErrNotFound) {
		err = s.repo.CancelMarketDataHistoryRequest(ctx, req.GetRequestId(), req.GetUserId())
	}
	if errors.Is(err, repository.ErrNotFound) {
		return nil, status.Errorf(codes.NotFound, "market-data request %d not found", req.GetRequestId())
	}
	if errors.Is(err, repository.ErrPermissionDenied) {
		return nil, status.Errorf(codes.PermissionDenied, "request %d does not belong to user %d", req.GetRequestId(), req.GetUserId())
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "cancel request: %v", err)
	}
	return &mdv1.CancelMarketDataRequestResponse{}, nil
}

func (s *Service) ListMarketDataRequests(ctx context.Context, req *mdv1.ListMarketDataRequestsRequest) (*mdv1.ListMarketDataRequestsResponse, error) {
	if err := requireUserID(req.GetUserId()); err != nil {
		return nil, err
	}
	list, err := s.repo.ListMarketDataRequestsByUser(ctx, req.GetUserId())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list requests: %v", err)
	}
	history, err := s.repo.ListMarketDataHistoryRequestsByUser(ctx, req.GetUserId())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list historical requests: %v", err)
	}
	out := make([]*mdv1.MarketDataRequestWithStream, 0, len(list)+len(history))
	for _, r := range list {
		st, err := s.repo.GetMarketDataStream(ctx, r.StreamID)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "get stream %d for request %d: %v", r.StreamID, r.RequestID, err)
		}
		out = append(out, &mdv1.MarketDataRequestWithStream{
			Request: toProtoRequest(r),
			Stream:  toProtoStream(st, s.activeLeaseCount(ctx, st.StreamID)),
		})
	}
	for _, r := range history {
		out = append(out, &mdv1.MarketDataRequestWithStream{
			Request: toProtoHistoryRequest(r),
		})
	}
	sort.SliceStable(out, func(i, j int) bool {
		return out[i].GetRequest().GetRequestId() > out[j].GetRequest().GetRequestId()
	})
	if req.GetLimit() <= 0 && req.GetOffset() <= 0 {
		return &mdv1.ListMarketDataRequestsResponse{Entries: out, Total: int64(len(out))}, nil
	}
	limit := int(req.GetLimit())
	if limit <= 0 {
		limit = 50
	}
	if limit > 200 {
		limit = 200
	}
	offset := int(req.GetOffset())
	if offset < 0 {
		offset = 0
	}
	total := int64(len(out))
	hasMore := offset+limit < len(out)
	if offset >= len(out) {
		out = nil
	} else {
		end := offset + limit
		if end > len(out) {
			end = len(out)
		}
		out = out[offset:end]
	}
	return &mdv1.ListMarketDataRequestsResponse{Entries: out, HasMore: hasMore, Total: total}, nil
}

func (s *Service) GetMarketDataStreamStatus(ctx context.Context, req *mdv1.GetMarketDataStreamStatusRequest) (*mdv1.GetMarketDataStreamStatusResponse, error) {
	var stream domain.MarketDataStream
	var err error
	if req.GetStreamId() > 0 {
		stream, err = s.repo.GetMarketDataStream(ctx, req.GetStreamId())
	} else {
		key, verr := validateStreamKey(req.GetKey())
		if verr != nil {
			return nil, verr
		}
		stream, err = s.repo.GetMarketDataStreamByKey(ctx, key)
	}
	if errors.Is(err, repository.ErrNotFound) {
		return nil, status.Error(codes.NotFound, "stream not found")
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get stream: %v", err)
	}
	return &mdv1.GetMarketDataStreamStatusResponse{
		Stream: toProtoStream(stream, s.activeLeaseCount(ctx, stream.StreamID)),
	}, nil
}

// ── Scraper / strategy-service RPCs ─────────────────────────────────────

func (s *Service) ListMarketDataStreams(ctx context.Context, _ *mdv1.ListMarketDataStreamsRequest) (*mdv1.ListMarketDataStreamsResponse, error) {
	list, err := s.repo.ListMarketDataStreams(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list streams: %v", err)
	}
	leaseCounts, err := s.repo.CountActiveLeasesByStream(ctx)
	if err != nil {
		// Active lease count is display metadata; preserve the stream
		// list even if the count query fails.
		leaseCounts = map[int64]int{}
	}
	out := make([]*mdv1.MarketDataStream, 0, len(list))
	for _, st := range list {
		out = append(out, toProtoStream(st, leaseCounts[st.StreamID]))
	}
	return &mdv1.ListMarketDataStreamsResponse{Streams: out}, nil
}

func (s *Service) ListMarketDataHistoryRequests(ctx context.Context, req *mdv1.ListMarketDataHistoryRequestsRequest) (*mdv1.ListMarketDataHistoryRequestsResponse, error) {
	list, err := s.repo.ListMarketDataHistoryRequests(ctx, req.GetIncludeTerminal())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list historical requests: %v", err)
	}
	out := make([]*mdv1.MarketDataRequest, 0, len(list))
	for _, item := range list {
		out = append(out, toProtoHistoryRequest(item))
	}
	return &mdv1.ListMarketDataHistoryRequestsResponse{Requests: out}, nil
}

func (s *Service) QueryMarketDataCoverage(ctx context.Context, req *mdv1.QueryMarketDataCoverageRequest) (*mdv1.QueryMarketDataCoverageResponse, error) {
	key, err := validateStreamKey(req.GetKey())
	if err != nil {
		return nil, err
	}
	if req.GetStartAt() == nil || req.GetEndAt() == nil {
		return nil, status.Error(codes.InvalidArgument, "start_at and end_at are required")
	}
	startAt := req.GetStartAt().AsTime().UTC()
	endAt := req.GetEndAt().AsTime().UTC()
	if startAt.IsZero() || endAt.IsZero() {
		return nil, status.Error(codes.InvalidArgument, "start_at and end_at must be non-zero")
	}
	if !endAt.After(startAt) {
		return nil, status.Error(codes.InvalidArgument, "end_at must be after start_at")
	}
	totalExpected, err := expectedCount(startAt, endAt, key.Interval)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "coverage interval: %v", err)
	}
	covered, err := s.repo.QueryMarketDataCoverage(ctx, key, startAt, endAt)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "query market-data coverage: %v", err)
	}
	covered, err = s.coverageWithRawFallback(ctx, key, startAt, endAt, covered)
	if err != nil {
		return nil, err
	}
	missing, err := computeMissingSegments(startAt, endAt, key.Interval, covered)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "compute missing coverage: %v", err)
	}

	var missingCount int64
	for _, item := range missing {
		missingCount += item.ExpectedCount
	}
	coveredCount := totalExpected - missingCount
	if coveredCount < 0 {
		coveredCount = 0
	}

	outCovered := make([]*mdv1.MarketDataCoverageSegment, 0, len(covered))
	for _, seg := range covered {
		outCovered = append(outCovered, toProtoMarketDataCoverageSegment(seg))
	}
	outMissing := make([]*mdv1.MarketDataTimeRange, 0, len(missing))
	for _, item := range missing {
		outMissing = append(outMissing, toProtoMarketDataTimeRange(item))
	}
	return &mdv1.QueryMarketDataCoverageResponse{
		Key:                   toProtoStreamKey(key),
		RequestedStartAt:      timestamppb.New(startAt),
		RequestedEndAt:        timestamppb.New(endAt),
		Complete:              len(missing) == 0,
		ExpectedCount:         totalExpected,
		CoveredCount:          coveredCount,
		CoveredSegments:       outCovered,
		MissingSegments:       outMissing,
		NonDownloadableReason: "",
	}, nil
}

func (s *Service) coverageWithRawFallback(
	ctx context.Context,
	key domain.StreamKey,
	startAt, endAt time.Time,
	indexed []domain.MarketDataCoverageSegment,
) ([]domain.MarketDataCoverageSegment, error) {
	covered, err := mergeCoverageForQuery(key, startAt, endAt, indexed)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "merge indexed coverage: %v", err)
	}
	if s.klineQuery == nil {
		return covered, nil
	}
	missing, err := computeMissingSegments(startAt, endAt, key.Interval, covered)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "compute indexed coverage gaps: %v", err)
	}
	if len(missing) == 0 {
		return covered, nil
	}

	all := make([]domain.MarketDataCoverageSegment, 0, len(covered)+len(missing))
	all = append(all, covered...)
	for _, gap := range missing {
		rows, err := s.fetchRawKlinesForCoverage(ctx, key, gap.StartAt, gap.EndAt)
		if err != nil {
			return nil, status.Errorf(codes.Unavailable, "fetch raw klines for coverage: %v", err)
		}
		rawSegments, err := coverageSegmentsFromKlineRows(key, gap.StartAt, gap.EndAt, rows, rawCoverageSource)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "build raw kline coverage: %v", err)
		}
		all = append(all, rawSegments...)
	}
	merged, err := mergeCoverageForQuery(key, startAt, endAt, all)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "merge raw coverage: %v", err)
	}
	return merged, nil
}

func (s *Service) fetchRawKlinesForCoverage(ctx context.Context, key domain.StreamKey, startAt, endAt time.Time) ([]runtimechannel.KlineRow, error) {
	step, err := intervalDuration(key.Interval)
	if err != nil {
		return nil, err
	}
	stepMS := int64(step / time.Millisecond)
	startMS := startAt.UTC().UnixMilli()
	endMS := endAt.UTC().UnixMilli()
	rows := make([]runtimechannel.KlineRow, 0, rawKlineValidationFetchLimit)
	nextStartMS := startMS
	for nextStartMS < endMS {
		chunk, err := s.klineQuery.FetchKlines(ctx, runtimechannel.KlineQuery{
			Exchange:    key.Exchange,
			Market:      key.Market,
			Symbol:      key.Symbol,
			Interval:    key.Interval,
			StartTimeMS: nextStartMS,
			EndTimeMS:   endMS,
			Limit:       rawKlineValidationFetchLimit,
		})
		if err != nil {
			return nil, err
		}
		if len(chunk) == 0 {
			break
		}
		rows = append(rows, chunk...)
		maxOpenMS := chunk[0].OpenTime
		for _, row := range chunk[1:] {
			if row.OpenTime > maxOpenMS {
				maxOpenMS = row.OpenTime
			}
		}
		nextStart := maxOpenMS + stepMS
		if nextStart <= nextStartMS {
			return nil, errors.New("raw kline query did not advance")
		}
		nextStartMS = nextStart
	}
	return rows, nil
}

func (s *Service) ValidateMarketDataCoverage(ctx context.Context, req *mdv1.ValidateMarketDataCoverageRequest) (*mdv1.ValidateMarketDataCoverageResponse, error) {
	key, err := validateStreamKey(req.GetKey())
	if err != nil {
		return nil, err
	}
	if req.GetStartAt() == nil || req.GetEndAt() == nil {
		return nil, status.Error(codes.InvalidArgument, "start_at and end_at are required")
	}
	startAt := req.GetStartAt().AsTime().UTC()
	endAt := req.GetEndAt().AsTime().UTC()
	if startAt.IsZero() || endAt.IsZero() {
		return nil, status.Error(codes.InvalidArgument, "start_at and end_at must be non-zero")
	}
	if !endAt.After(startAt) {
		return nil, status.Error(codes.InvalidArgument, "end_at must be after start_at")
	}
	totalExpected, err := expectedCount(startAt, endAt, key.Interval)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "coverage interval: %v", err)
	}
	if s.klineQuery == nil {
		return nil, status.Error(codes.FailedPrecondition, "market-data raw kline query is not configured")
	}

	step, err := intervalDuration(key.Interval)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "coverage interval: %v", err)
	}
	stepMS := int64(step / time.Millisecond)
	startMS := startAt.UnixMilli()
	endMS := endAt.UnixMilli()
	rows := make([]runtimechannel.KlineRow, 0, rawKlineValidationFetchLimit)
	nextStartMS := startMS
	for nextStartMS < endMS {
		chunk, err := s.klineQuery.FetchKlines(ctx, runtimechannel.KlineQuery{
			Exchange:    key.Exchange,
			Market:      key.Market,
			Symbol:      key.Symbol,
			Interval:    key.Interval,
			StartTimeMS: nextStartMS,
			EndTimeMS:   endMS,
			Limit:       rawKlineValidationFetchLimit,
		})
		if err != nil {
			return nil, status.Errorf(codes.Unavailable, "fetch raw klines: %v", err)
		}
		if len(chunk) == 0 {
			break
		}
		rows = append(rows, chunk...)
		maxOpenMS := chunk[0].OpenTime
		for _, row := range chunk[1:] {
			if row.OpenTime > maxOpenMS {
				maxOpenMS = row.OpenTime
			}
		}
		nextStart := maxOpenMS + stepMS
		if nextStart <= nextStartMS {
			return nil, status.Error(codes.Internal, "raw kline query did not advance")
		}
		nextStartMS = nextStart
	}

	validation := runtimechannel.ValidateKlineRows(key.Interval, startMS, endMS, rows)
	missing := make([]*mdv1.MarketDataTimeRange, 0, len(validation.MissingGaps))
	for _, gap := range validation.MissingGaps {
		missing = append(missing, toProtoMarketDataTimeRange(domain.MarketDataTimeRange{
			StartAt:       time.UnixMilli(gap.StartMS).UTC(),
			EndAt:         time.UnixMilli(gap.EndMS).UTC(),
			ExpectedCount: gap.ExpectedCount,
		}))
	}

	return &mdv1.ValidateMarketDataCoverageResponse{
		Key:              toProtoStreamKey(key),
		RequestedStartAt: timestamppb.New(startAt),
		RequestedEndAt:   timestamppb.New(endAt),
		Ok:               validation.OK,
		ExpectedCount:    totalExpected,
		ActualCount:      validation.ActualCount,
		MissingSegments:  missing,
		Reason:           validation.Reason,
	}, nil
}

func (s *Service) QueryMarketDataKlines(ctx context.Context, req *mdv1.QueryMarketDataKlinesRequest) (*mdv1.QueryMarketDataKlinesResponse, error) {
	key, err := validateStreamKey(req.GetKey())
	if err != nil {
		return nil, err
	}
	if req.GetStartAt() == nil || req.GetEndAt() == nil {
		return nil, status.Error(codes.InvalidArgument, "start_at and end_at are required")
	}
	startAt := req.GetStartAt().AsTime().UTC()
	endAt := req.GetEndAt().AsTime().UTC()
	if startAt.IsZero() || endAt.IsZero() {
		return nil, status.Error(codes.InvalidArgument, "start_at and end_at must be non-zero")
	}
	if !endAt.After(startAt) {
		return nil, status.Error(codes.InvalidArgument, "end_at must be after start_at")
	}
	if _, err := intervalDuration(key.Interval); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "kline interval: %v", err)
	}
	if s.klineQuery == nil {
		return nil, status.Error(codes.FailedPrecondition, "market-data raw kline query is not configured")
	}

	limit := int(req.GetLimit())
	if limit <= 0 {
		limit = defaultRawKlineQueryLimit
	}
	if limit > maxRawKlineQueryLimit {
		limit = maxRawKlineQueryLimit
	}
	rows, err := s.klineQuery.FetchKlines(ctx, runtimechannel.KlineQuery{
		Exchange:    key.Exchange,
		Market:      key.Market,
		Symbol:      key.Symbol,
		Interval:    key.Interval,
		StartTimeMS: startAt.UnixMilli(),
		EndTimeMS:   endAt.UnixMilli(),
		Limit:       limit + 1,
	})
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "fetch raw klines: %v", err)
	}
	truncated := len(rows) > limit
	if truncated {
		rows = rows[:limit]
	}
	outRows := make([]*mdv1.MarketDataKline, 0, len(rows))
	for _, row := range rows {
		outRows = append(outRows, toProtoMarketDataKline(row))
	}
	return &mdv1.QueryMarketDataKlinesResponse{
		Key:              toProtoStreamKey(key),
		RequestedStartAt: timestamppb.New(startAt),
		RequestedEndAt:   timestamppb.New(endAt),
		Rows:             outRows,
		RowCount:         int64(len(outRows)),
		Truncated:        truncated,
		Limit:            int32(limit),
	}, nil
}

func (s *Service) ReportMarketDataCoverageSegments(ctx context.Context, req *mdv1.ReportMarketDataCoverageSegmentsRequest) (*mdv1.ReportMarketDataCoverageSegmentsResponse, error) {
	if len(req.GetSegments()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "at least one coverage segment is required")
	}
	segments := make([]domain.MarketDataCoverageSegment, 0, len(req.GetSegments()))
	for _, raw := range req.GetSegments() {
		seg, err := validateCoverageSegment(raw)
		if err != nil {
			return nil, err
		}
		segments = append(segments, seg)
	}
	merged, err := s.repo.MergeMarketDataCoverageSegments(ctx, segments)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "merge market-data coverage segments: %v", err)
	}
	out := make([]*mdv1.MarketDataCoverageSegment, 0, len(merged))
	for _, seg := range merged {
		out = append(out, toProtoMarketDataCoverageSegment(seg))
	}
	return &mdv1.ReportMarketDataCoverageSegmentsResponse{MergedSegments: out}, nil
}

func (s *Service) ReportMarketDataStreamState(ctx context.Context, req *mdv1.ReportMarketDataStreamStateRequest) (*mdv1.ReportMarketDataStreamStateResponse, error) {
	if req.GetStreamId() <= 0 {
		return nil, status.Error(codes.InvalidArgument, "stream_id is required")
	}
	actual := domain.StreamActualState(strings.TrimSpace(strings.ToLower(req.GetActualState())))
	switch actual {
	case domain.StreamActualPending, domain.StreamActualStarting,
		domain.StreamActualRunning, domain.StreamActualDraining,
		domain.StreamActualStopped, domain.StreamActualError:
		// valid
	default:
		return nil, status.Errorf(codes.InvalidArgument, "actual_state %q is not recognized", actual)
	}

	var lastData *time.Time
	if req.GetLastDataAt() != nil {
		t := req.GetLastDataAt().AsTime()
		lastData = &t
	}

	if err := s.repo.UpdateMarketDataStreamActualState(ctx, req.GetStreamId(), actual, lastData, req.GetLastError()); err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "stream %d not found", req.GetStreamId())
		}
		return nil, status.Errorf(codes.Internal, "update stream state: %v", err)
	}
	stream, err := s.repo.GetMarketDataStream(ctx, req.GetStreamId())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "refetch stream: %v", err)
	}
	return &mdv1.ReportMarketDataStreamStateResponse{
		Stream: toProtoStream(stream, s.activeLeaseCount(ctx, stream.StreamID)),
	}, nil
}

func (s *Service) ReportMarketDataHistoryRequestState(ctx context.Context, req *mdv1.ReportMarketDataHistoryRequestStateRequest) (*mdv1.ReportMarketDataHistoryRequestStateResponse, error) {
	if req.GetRequestId() <= 0 {
		return nil, status.Error(codes.InvalidArgument, "request_id is required")
	}
	statusValue, err := normalizeHistoryStatus(req.GetStatus())
	if err != nil {
		return nil, err
	}
	var coveredStart *time.Time
	if ts := req.GetCoveredStartAt(); ts != nil {
		t := ts.AsTime().UTC()
		coveredStart = &t
	}
	var coveredEnd *time.Time
	if ts := req.GetCoveredEndAt(); ts != nil {
		t := ts.AsTime().UTC()
		coveredEnd = &t
	}
	updated, err := s.repo.UpdateMarketDataHistoryRequestState(
		ctx,
		req.GetRequestId(),
		statusValue,
		coveredStart,
		coveredEnd,
		req.GetLastError(),
	)
	if errors.Is(err, repository.ErrNotFound) {
		return nil, status.Errorf(codes.NotFound, "history request %d not found", req.GetRequestId())
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "update history request state: %v", err)
	}
	return &mdv1.ReportMarketDataHistoryRequestStateResponse{
		Request: toProtoHistoryRequest(updated),
	}, nil
}

func (s *Service) CreateOrRenewMarketDataLease(ctx context.Context, req *mdv1.CreateOrRenewMarketDataLeaseRequest) (*mdv1.CreateOrRenewMarketDataLeaseResponse, error) {
	if strings.TrimSpace(req.GetSessionId()) == "" {
		return nil, status.Error(codes.InvalidArgument, "session_id is required")
	}
	if req.GetStreamId() <= 0 {
		return nil, status.Error(codes.InvalidArgument, "stream_id is required")
	}
	ttl := time.Duration(req.GetTtlSeconds()) * time.Second
	if ttl == 0 {
		ttl = defaultLeaseTTL
	}
	if ttl < minLeaseTTL {
		ttl = minLeaseTTL
	}
	if ttl > maxLeaseTTL {
		ttl = maxLeaseTTL
	}

	var strategyID, accountID *int64
	if v := req.GetStrategyId(); v > 0 {
		strategyID = &v
	}
	if v := req.GetAccountId(); v > 0 {
		accountID = &v
	}

	lease, err := s.repo.CreateOrRenewLease(ctx, req.GetSessionId(), strategyID, accountID, req.GetStreamId(), ttl)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "create/renew lease: %v", err)
	}
	return &mdv1.CreateOrRenewMarketDataLeaseResponse{
		Lease: toProtoLease(lease),
	}, nil
}

func (s *Service) ReleaseMarketDataLease(ctx context.Context, req *mdv1.ReleaseMarketDataLeaseRequest) (*mdv1.ReleaseMarketDataLeaseResponse, error) {
	if strings.TrimSpace(req.GetSessionId()) == "" {
		return nil, status.Error(codes.InvalidArgument, "session_id is required")
	}
	if req.GetStreamId() <= 0 {
		return nil, status.Error(codes.InvalidArgument, "stream_id is required")
	}
	if err := s.repo.ReleaseLease(ctx, req.GetSessionId(), req.GetStreamId()); err != nil {
		return nil, status.Errorf(codes.Internal, "release lease: %v", err)
	}
	return &mdv1.ReleaseMarketDataLeaseResponse{}, nil
}

func (s *Service) CreateSessionMarketDataSubscriptions(ctx context.Context, req *mdv1.CreateSessionMarketDataSubscriptionsRequest) (*mdv1.CreateSessionMarketDataSubscriptionsResponse, error) {
	if err := requireUserID(req.GetUserId()); err != nil {
		return nil, err
	}
	sessionID := strings.TrimSpace(req.GetSessionId())
	if sessionID == "" {
		return nil, status.Error(codes.InvalidArgument, "session_id is required")
	}
	runtimeID := strings.TrimSpace(req.GetRuntimeId())
	if runtimeID == "" {
		return nil, status.Error(codes.InvalidArgument, "runtime_id is required")
	}
	if req.GetEnvironment() != 1 {
		return nil, status.Errorf(codes.InvalidArgument, "session market-data subscriptions are only supported for demo environment, got %d", req.GetEnvironment())
	}
	if len(req.GetKeys()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "at least one stream key is required")
	}
	keys := make([]domain.StreamKey, 0, len(req.GetKeys()))
	for _, raw := range req.GetKeys() {
		key, err := validateStreamKey(raw)
		if err != nil {
			return nil, err
		}
		keys = append(keys, key)
	}
	subs, err := s.repo.UpsertSessionMarketDataSubscriptions(
		ctx,
		req.GetUserId(),
		sessionID,
		runtimeID,
		req.GetEnvironment(),
		keys,
	)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "create session subscriptions: %v", err)
	}
	out := make([]*mdv1.SessionMarketDataSubscription, 0, len(subs))
	for _, sub := range subs {
		out = append(out, toProtoSessionSubscription(sub))
	}
	return &mdv1.CreateSessionMarketDataSubscriptionsResponse{Subscriptions: out}, nil
}

func (s *Service) ReleaseSessionMarketDataSubscriptions(ctx context.Context, req *mdv1.ReleaseSessionMarketDataSubscriptionsRequest) (*mdv1.ReleaseSessionMarketDataSubscriptionsResponse, error) {
	sessionID := strings.TrimSpace(req.GetSessionId())
	if sessionID == "" {
		return nil, status.Error(codes.InvalidArgument, "session_id is required")
	}
	n, err := s.repo.ReleaseSessionMarketDataSubscriptions(ctx, sessionID, strings.TrimSpace(req.GetRuntimeId()))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "release session subscriptions: %v", err)
	}
	return &mdv1.ReleaseSessionMarketDataSubscriptionsResponse{ReleasedCount: n}, nil
}

func (s *Service) CreateOrRenewStreamDeliveryLease(ctx context.Context, req *mdv1.CreateOrRenewStreamDeliveryLeaseRequest) (*mdv1.CreateOrRenewStreamDeliveryLeaseResponse, error) {
	if req.GetSubscriptionId() <= 0 {
		return nil, status.Error(codes.InvalidArgument, "subscription_id is required")
	}
	owner := strings.TrimSpace(req.GetOwnerInstanceId())
	if owner == "" {
		return nil, status.Error(codes.InvalidArgument, "owner_instance_id is required")
	}
	ttl := time.Duration(req.GetTtlSeconds()) * time.Second
	if ttl == 0 {
		ttl = defaultLeaseTTL
	}
	if ttl < minLeaseTTL {
		ttl = minLeaseTTL
	}
	if ttl > maxLeaseTTL {
		ttl = maxLeaseTTL
	}
	lease, err := s.repo.CreateOrRenewStreamDeliveryLease(ctx, req.GetSubscriptionId(), owner, ttl)
	if errors.Is(err, repository.ErrPermissionDenied) {
		return nil, status.Error(codes.FailedPrecondition, "stream delivery lease is owned by another active instance")
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "create/renew stream delivery lease: %v", err)
	}
	return &mdv1.CreateOrRenewStreamDeliveryLeaseResponse{Lease: toProtoStreamDeliveryLease(lease)}, nil
}

func (s *Service) ReleaseStreamDeliveryLease(ctx context.Context, req *mdv1.ReleaseStreamDeliveryLeaseRequest) (*mdv1.ReleaseStreamDeliveryLeaseResponse, error) {
	if strings.TrimSpace(req.GetLeaseId()) == "" {
		return nil, status.Error(codes.InvalidArgument, "lease_id is required")
	}
	if strings.TrimSpace(req.GetOwnerInstanceId()) == "" {
		return nil, status.Error(codes.InvalidArgument, "owner_instance_id is required")
	}
	if err := s.repo.ReleaseStreamDeliveryLease(ctx, req.GetLeaseId(), req.GetOwnerInstanceId()); errors.Is(err, repository.ErrNotFound) {
		return nil, status.Error(codes.NotFound, "stream delivery lease not found")
	} else if err != nil {
		return nil, status.Errorf(codes.Internal, "release stream delivery lease: %v", err)
	}
	return &mdv1.ReleaseStreamDeliveryLeaseResponse{}, nil
}

func (s *Service) ListSessionDeliveryHealth(ctx context.Context, req *mdv1.ListSessionDeliveryHealthRequest) (*mdv1.ListSessionDeliveryHealthResponse, error) {
	if err := requireUserID(req.GetUserId()); err != nil {
		return nil, err
	}
	items, err := s.repo.ListSessionDeliveryHealth(ctx, req.GetUserId(), strings.TrimSpace(req.GetSessionId()), strings.TrimSpace(req.GetRuntimeId()))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list session delivery health: %v", err)
	}
	now := time.Now().UTC()
	out := make([]*mdv1.SessionDeliveryHealth, 0, len(items))
	for _, item := range items {
		item = classifyDeliveryHealth(item, now)
		out = append(out, toProtoSessionDeliveryHealth(item))
	}
	return &mdv1.ListSessionDeliveryHealthResponse{Items: out}, nil
}

func classifyDeliveryHealth(item domain.SessionDeliveryHealth, now time.Time) domain.SessionDeliveryHealth {
	item.ObservedAt = now
	if item.Lease == nil {
		if now.Sub(item.Subscription.CreatedAt) < deliveryProgressGrace {
			item.HealthStatus = "warming_up"
			item.BlockedReason = "waiting for delivery lease"
		} else {
			item.HealthStatus = "delivery_blocked"
			item.BlockedReason = "no active delivery lease"
		}
		return item
	}
	if !item.Lease.ExpiresAt.IsZero() && !item.Lease.ExpiresAt.After(now) {
		item.HealthStatus = "delivery_blocked"
		item.BlockedReason = "delivery lease expired"
		return item
	}
	if item.Lease.LastDeliveryAt == nil {
		if now.Sub(item.Subscription.CreatedAt) < deliveryProgressGrace {
			item.HealthStatus = "warming_up"
			item.BlockedReason = "waiting for first delivered bar"
		} else {
			item.HealthStatus = "delivery_blocked"
			item.BlockedReason = "no delivered bars observed"
		}
		return item
	}
	if now.Sub(*item.Lease.LastDeliveryAt) > 2*deliveryProgressGrace {
		item.HealthStatus = "delivery_blocked"
		item.BlockedReason = "delivery progress is stale"
		return item
	}
	item.HealthStatus = "delivering"
	item.BlockedReason = ""
	return item
}

func (s *Service) CreateOrRenewMarketDataWriterLease(ctx context.Context, req *mdv1.CreateOrRenewMarketDataWriterLeaseRequest) (*mdv1.CreateOrRenewMarketDataWriterLeaseResponse, error) {
	key, err := validateWriterKey(req.GetKey())
	if err != nil {
		return nil, err
	}
	if req.GetYear() < 1970 {
		return nil, status.Errorf(codes.InvalidArgument, "year %d is invalid", req.GetYear())
	}
	owner := strings.TrimSpace(req.GetOwnerInstanceId())
	if owner == "" {
		return nil, status.Error(codes.InvalidArgument, "owner_instance_id is required")
	}
	scraperInstanceID := strings.TrimSpace(req.GetScraperInstanceId())
	if scraperInstanceID == "" {
		return nil, status.Error(codes.InvalidArgument, "scraper_instance_id is required")
	}
	collectorID := strings.TrimSpace(req.GetCollectorId())
	if collectorID == "" {
		return nil, status.Error(codes.InvalidArgument, "collector_id is required")
	}
	ttl := time.Duration(req.GetTtlSeconds()) * time.Second
	if ttl == 0 {
		ttl = defaultLeaseTTL
	}
	if ttl < minLeaseTTL {
		ttl = minLeaseTTL
	}
	if ttl > maxLeaseTTL {
		ttl = maxLeaseTTL
	}
	lease, err := s.repo.CreateOrRenewMarketDataWriterLease(
		ctx,
		key,
		req.GetYear(),
		owner,
		scraperInstanceID,
		collectorID,
		ttl,
	)
	if errors.Is(err, repository.ErrPermissionDenied) {
		return nil, status.Error(codes.FailedPrecondition, "market-data writer lease is owned by another active instance")
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "create/renew market-data writer lease: %v", err)
	}
	return &mdv1.CreateOrRenewMarketDataWriterLeaseResponse{Lease: toProtoMarketDataWriterLease(lease)}, nil
}

func (s *Service) ReleaseMarketDataWriterLease(ctx context.Context, req *mdv1.ReleaseMarketDataWriterLeaseRequest) (*mdv1.ReleaseMarketDataWriterLeaseResponse, error) {
	if strings.TrimSpace(req.GetLeaseId()) == "" {
		return nil, status.Error(codes.InvalidArgument, "lease_id is required")
	}
	if strings.TrimSpace(req.GetOwnerInstanceId()) == "" {
		return nil, status.Error(codes.InvalidArgument, "owner_instance_id is required")
	}
	if err := s.repo.ReleaseMarketDataWriterLease(ctx, req.GetLeaseId(), req.GetOwnerInstanceId()); errors.Is(err, repository.ErrNotFound) {
		return nil, status.Error(codes.NotFound, "market-data writer lease not found")
	} else if err != nil {
		return nil, status.Errorf(codes.Internal, "release market-data writer lease: %v", err)
	}
	return &mdv1.ReleaseMarketDataWriterLeaseResponse{}, nil
}
