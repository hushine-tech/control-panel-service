// Package repository owns the persistence interface for the market-data
// control plane. Phase D2 ported the methods verbatim from
// core-service (the source `market_data_control_plane.go` /
// `market_data_history.go` were deleted in the same change). Behaviour
// is unchanged. The implementation lives in `timescale.go`.
package repository

import (
	"context"
	"errors"
	"time"

	"github.com/hushine-tech/control-panel-service/internal/domain"
)

// Sentinel errors used by the service layer to translate persistence
// outcomes into gRPC status codes.
var (
	// ErrNotFound — Get / lookup miss.
	ErrNotFound = errors.New("not found")

	// ErrPermissionDenied — caller is not allowed to operate on this row
	// (e.g. cancelling a request owned by a different user).
	ErrPermissionDenied = errors.New("permission denied")
)

// Repository is the persistence surface the marketdata service depends
// on. Defining it as an interface lets unit tests substitute an in-
// memory stub.
type Repository interface {
	// ── Streams ─────────────────────────────────────────────────────────

	UpsertMarketDataStream(ctx context.Context, key domain.StreamKey) (domain.MarketDataStream, error)
	GetMarketDataStream(ctx context.Context, streamID int64) (domain.MarketDataStream, error)
	GetMarketDataStreamByKey(ctx context.Context, key domain.StreamKey) (domain.MarketDataStream, error)
	ListMarketDataStreams(ctx context.Context) ([]domain.MarketDataStream, error)
	UpdateMarketDataStreamActualState(
		ctx context.Context,
		streamID int64,
		state domain.StreamActualState,
		lastData *time.Time,
		lastErr string,
	) error

	// ── Requests ────────────────────────────────────────────────────────

	UpsertMarketDataRequest(
		ctx context.Context,
		userID int64,
		accountID *int64,
		key domain.StreamKey,
		needsLive bool,
	) (domain.MarketDataRequest, error)
	CancelMarketDataRequest(ctx context.Context, requestID, userID int64) error
	GetMarketDataRequest(ctx context.Context, requestID, userID int64) (domain.MarketDataRequest, error)
	ListMarketDataRequestsByUser(ctx context.Context, userID int64) ([]domain.MarketDataRequest, error)
	ListMarketDataRequestsByUserPage(ctx context.Context, userID int64, limit, offset int) (items []domain.MarketDataRequest, total int64, hasMore bool, err error)

	// ── Leases ──────────────────────────────────────────────────────────

	CreateOrRenewLease(
		ctx context.Context,
		sessionID string,
		strategyID, accountID *int64,
		streamID int64,
		ttl time.Duration,
	) (domain.MarketDataLease, error)
	ReleaseLease(ctx context.Context, sessionID string, streamID int64) error
	CountActiveLeasesForStream(ctx context.Context, streamID int64) (int, error)
	CountActiveLeasesByStream(ctx context.Context) (map[int64]int, error)
	ExpireStaleLeases(ctx context.Context, now time.Time) (int, error)
	ListActiveLeasesForStream(ctx context.Context, streamID int64) ([]domain.MarketDataLease, error)

	// ── RuntimeChannel delivery subscriptions ──────────────────────────

	UpsertSessionMarketDataSubscriptions(
		ctx context.Context,
		userID int64,
		sessionID string,
		runtimeID string,
		environment int32,
		keys []domain.StreamKey,
	) ([]domain.SessionMarketDataSubscription, error)
	ListActiveSessionMarketDataSubscriptions(ctx context.Context) ([]domain.SessionMarketDataSubscription, error)
	ReleaseSessionMarketDataSubscriptions(ctx context.Context, sessionID, runtimeID string) (int64, error)
	CreateOrRenewStreamDeliveryLease(
		ctx context.Context,
		subscriptionID int64,
		ownerInstanceID string,
		ttl time.Duration,
	) (domain.StreamDeliveryLease, error)
	RecordStreamDeliveryProgress(
		ctx context.Context,
		subscriptionID int64,
		ownerInstanceID string,
		topic string,
		partition int32,
		offset int64,
		at time.Time,
	) error
	RecordStreamDeliveryFailure(ctx context.Context, failure domain.StreamDeliveryFailure) error
	ListSessionDeliveryHealth(ctx context.Context, userID int64, sessionID, runtimeID string) ([]domain.SessionDeliveryHealth, error)
	ReleaseStreamDeliveryLease(ctx context.Context, leaseID, ownerInstanceID string) error
	CreateOrRenewMarketDataWriterLease(
		ctx context.Context,
		key domain.StreamKey,
		year int32,
		ownerInstanceID string,
		scraperInstanceID string,
		collectorID string,
		ttl time.Duration,
	) (domain.MarketDataWriterLease, error)
	ReleaseMarketDataWriterLease(ctx context.Context, leaseID, ownerInstanceID string) error

	// ── History requests ────────────────────────────────────────────────

	UpsertMarketDataHistoryRequest(
		ctx context.Context,
		userID int64,
		accountID *int64,
		key domain.StreamKey,
		startAt, endAt time.Time,
	) (domain.MarketDataHistoryRequest, error)
	CancelMarketDataHistoryRequest(ctx context.Context, requestID, userID int64) error
	ListMarketDataHistoryRequests(ctx context.Context, includeTerminal bool) ([]domain.MarketDataHistoryRequest, error)
	ListMarketDataHistoryRequestsByUser(ctx context.Context, userID int64) ([]domain.MarketDataHistoryRequest, error)
	UpdateMarketDataHistoryRequestState(
		ctx context.Context,
		requestID int64,
		status domain.MarketDataHistoryRequestStatus,
		coveredStart, coveredEnd *time.Time,
		lastErr string,
	) (domain.MarketDataHistoryRequest, error)

	// ── Historical coverage ─────────────────────────────────────────────

	MergeMarketDataCoverageSegments(ctx context.Context, segments []domain.MarketDataCoverageSegment) ([]domain.MarketDataCoverageSegment, error)
	QueryMarketDataCoverage(ctx context.Context, key domain.StreamKey, startAt, endAt time.Time) ([]domain.MarketDataCoverageSegment, error)
}
