package domain

import "time"

// ── Market-data control plane (Phase D2) ────────────────────────────────
//
// These types were ported verbatim from core-service/internal/domain
// when D2 moved control-plane ownership into control-panel-service. The
// underlying tables live in the `control_panel` database
// (migrations 0003–0006). Cross-database FK references to
// `account.users(id)` / `account.accounts(account_id)` were dropped at
// the schema level; UserID and AccountID are validated at the service
// layer via core-service.GetUser when applicable.
//
// v1 scope is `kline` only. The `Kind` field reserves space for future
// `orderbook` / `funding` / `oi` extensions.

// StreamDesiredState is what scraper reconcile is targeting for this
// physical stream.
type StreamDesiredState string

const (
	StreamDesiredRunning StreamDesiredState = "running"
	StreamDesiredStopped StreamDesiredState = "stopped"
)

// StreamActualState is the latest state scraper has reported back.
type StreamActualState string

const (
	StreamActualPending  StreamActualState = "pending"
	StreamActualStarting StreamActualState = "starting"
	StreamActualRunning  StreamActualState = "running"
	StreamActualDraining StreamActualState = "draining"
	StreamActualStopped  StreamActualState = "stopped"
	StreamActualError    StreamActualState = "error"
)

// MarketDataRequestStatus is the user-request lifecycle.
type MarketDataRequestStatus string

const (
	RequestStatusPending   MarketDataRequestStatus = "pending"
	RequestStatusActive    MarketDataRequestStatus = "active"
	RequestStatusCancelled MarketDataRequestStatus = "cancelled"
)

type MarketDataRequestScope string

const (
	RequestScopeLive       MarketDataRequestScope = "live"
	RequestScopeHistorical MarketDataRequestScope = "historical"
)

type MarketDataHistoryRequestStatus string

const (
	HistoryRequestPending   MarketDataHistoryRequestStatus = "pending"
	HistoryRequestRunning   MarketDataHistoryRequestStatus = "running"
	HistoryRequestVerifying MarketDataHistoryRequestStatus = "verifying"
	HistoryRequestReady     MarketDataHistoryRequestStatus = "ready"
	HistoryRequestError     MarketDataHistoryRequestStatus = "error"
	HistoryRequestCancelled MarketDataHistoryRequestStatus = "cancelled"
)

// StreamKey is the canonical identity of a physical market-data stream.
type StreamKey struct {
	Exchange string
	Market   string // "spot" / "futures"
	Kind     string // v1 only "kline"
	Symbol   string // canonical upper-case
	Interval string // "1m" / "5m" / ...
}

// MarketDataStream is the cross-user / cross-session aggregated state
// of one physical stream.
type MarketDataStream struct {
	StreamID              int64
	Key                   StreamKey
	DesiredState          StreamDesiredState
	ActualState           StreamActualState
	EffectiveLiveDelivery bool
	LastDataAt            *time.Time
	LastError             string
	LastReconciledAt      *time.Time
	CreatedAt             time.Time
	UpdatedAt             time.Time
}

// MarketDataRequest = "user X declares a need for stream Y".
type MarketDataRequest struct {
	RequestID         int64
	UserID            int64
	AccountID         *int64
	StreamID          int64
	Key               StreamKey
	NeedsLiveDelivery bool
	Status            MarketDataRequestStatus
	CreatedAt         time.Time
	UpdatedAt         time.Time
	CancelledAt       *time.Time
}

// MarketDataHistoryRequest = "user X declares a need for a finite
// historical window of stream Y".
type MarketDataHistoryRequest struct {
	RequestID        int64
	UserID           int64
	AccountID        *int64
	Key              StreamKey
	Status           MarketDataHistoryRequestStatus
	RequestedStartAt time.Time
	RequestedEndAt   time.Time
	CoveredStartAt   *time.Time
	CoveredEndAt     *time.Time
	LastError        string
	CreatedAt        time.Time
	UpdatedAt        time.Time
	CancelledAt      *time.Time
}

// MarketDataLease = session-scoped TTL claim, refreshed via heartbeat.
type MarketDataLease struct {
	LeaseID         int64
	SessionID       string
	StrategyID      *int64
	AccountID       *int64
	StreamID        int64
	ExpiresAt       time.Time
	LastHeartbeatAt time.Time
	CreatedAt       time.Time
	ReleasedAt      *time.Time
}

// SessionMarketDataSubscription records the exact stream universe a session
// is authorized to receive over RuntimeChannel data frames.
type SessionMarketDataSubscription struct {
	SubscriptionID int64
	UserID         int64
	SessionID      string
	RuntimeID      string
	Key            StreamKey
	Mode           int32
	Status         string
	CreatedAt      time.Time
	UpdatedAt      time.Time
	ReleasedAt     *time.Time
}

// StreamDeliveryLease records which control-panel/delivery instance owns
// delivery work for one session subscription shard.
type StreamDeliveryLease struct {
	LeaseID         string
	SubscriptionID  int64
	OwnerInstanceID string
	Status          string
	AcquiredAt      time.Time
	LastHeartbeatAt time.Time
	ExpiresAt       time.Time
	LastDeliveryAt  *time.Time
	LastTopic       string
	LastPartition   int32
	LastOffset      int64
	ReleasedAt      *time.Time
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

// StreamDeliveryFailure records non-secret diagnostics for delivery worker
// failures such as Kafka topic/consumer startup errors.
type StreamDeliveryFailure struct {
	FailureID       int64
	SubscriptionID  int64
	OwnerInstanceID string
	Topic           string
	StreamKey       string
	FailureCode     string
	Reason          string
	FirstSeenAt     time.Time
	LastSeenAt      time.Time
	AttemptCount    int
}

// SessionDeliveryHealth is the user-visible delivery state for a session
// subscription. It is derived from subscription + delivery lease + latest
// non-secret delivery failure.
type SessionDeliveryHealth struct {
	Subscription  SessionMarketDataSubscription
	Lease         *StreamDeliveryLease
	LatestFailure *StreamDeliveryFailure
	HealthStatus  string
	BlockedReason string
	ObservedAt    time.Time
}

// MarketDataWriterLease records which scraper instance owns writes for one
// physical year-partitioned market-data domain.
type MarketDataWriterLease struct {
	LeaseID           string
	Key               StreamKey
	Year              int32
	OwnerInstanceID   string
	ScraperInstanceID string
	CollectorID       string
	Status            string
	AcquiredAt        time.Time
	LastHeartbeatAt   time.Time
	ExpiresAt         time.Time
	ReleasedAt        *time.Time
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

type MarketDataTimeRange struct {
	StartAt       time.Time
	EndAt         time.Time
	ExpectedCount int64
}

type MarketDataCoverageSegment struct {
	SegmentID int64
	Key       StreamKey
	Year      int32
	StartAt   time.Time
	EndAt     time.Time
	RowCount  int64
	Source    string
	UpdatedAt time.Time
}

type MarketDataCoverageResult struct {
	Key                   StreamKey
	RequestedStartAt      time.Time
	RequestedEndAt        time.Time
	Complete              bool
	ExpectedCount         int64
	CoveredCount          int64
	CoveredSegments       []MarketDataCoverageSegment
	MissingSegments       []MarketDataTimeRange
	NonDownloadableReason string
}

type MarketDataCoverageValidation struct {
	Key              StreamKey
	RequestedStartAt time.Time
	RequestedEndAt   time.Time
	OK               bool
	ExpectedCount    int64
	ActualCount      int64
	MissingSegments  []MarketDataTimeRange
	Reason           string
}
