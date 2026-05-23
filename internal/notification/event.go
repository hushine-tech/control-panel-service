package notification

import (
	"context"
	"time"
)

const (
	SchemaVersion = 1

	CategorySystem   = "system"
	CategoryStrategy = "strategy"
	CategoryCustom   = "custom"

	SeverityInfo  = "info"
	SeverityWarn  = "warn"
	SeverityError = "error"

	EventRuntimeStarted            = "runtime.started"
	EventRuntimeEnded              = "runtime.ended"
	EventRuntimeUnhealthy          = "runtime.unhealthy"
	EventRuntimeRecovered          = "runtime.recovered"
	EventRuntimeHeartbeatLost      = "runtime.heartbeat_lost"
	EventRuntimeHeartbeatRecovered = "runtime.heartbeat_recovered"
	EventSessionStarted            = "session.started"
	EventSessionStopped            = "session.stopped"
	EventSessionFailed             = "session.failed"
	EventStrategyMessage           = "strategy.message"
	EventSystemMessage             = "system.message"
	EventCustomInfo                = "custom.info"
	EventCustomWarn                = "custom.warn"
	EventCustomError               = "custom.error"
)

type Event struct {
	SchemaVersion int               `json:"schema_version"`
	EventID       string            `json:"event_id,omitempty"`
	CreatedAt     time.Time         `json:"created_at,omitempty"`
	UserID        int64             `json:"user_id"`
	Category      string            `json:"category"`
	EventType     string            `json:"event_type"`
	Severity      string            `json:"severity,omitempty"`
	SourceService string            `json:"source_service,omitempty"`
	RuntimeID     string            `json:"runtime_id,omitempty"`
	RuntimeName   string            `json:"runtime_name,omitempty"`
	AccountID     int64             `json:"account_id,omitempty"`
	StrategyID    int64             `json:"strategy_id,omitempty"`
	SessionID     string            `json:"session_id,omitempty"`
	Title         string            `json:"title,omitempty"`
	Message       string            `json:"message"`
	DedupeKey     string            `json:"dedupe_key,omitempty"`
	Metadata      map[string]string `json:"metadata,omitempty"`
}

type Publisher interface {
	Publish(ctx context.Context, event Event) error
}
