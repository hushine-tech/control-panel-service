package domain

import (
	"encoding/json"
	"time"
)

const (
	RuntimeCommandTypeStartSession    = "start_session"
	RuntimeCommandTypeStopSession     = "stop_session"
	RuntimeCommandTypeFinishSession   = "finish_session"
	RuntimeCommandTypeShutdownRuntime = "shutdown_runtime"
	RuntimeCommandTypeStatusPatch     = "status_patch"
)

const (
	RuntimeCommandStatusQueued    = "queued"
	RuntimeCommandStatusSent      = "sent"
	RuntimeCommandStatusAcked     = "acked"
	RuntimeCommandStatusRunning   = "running"
	RuntimeCommandStatusSucceeded = "succeeded"
	RuntimeCommandStatusFailed    = "failed"
	RuntimeCommandStatusTimedOut  = "timed_out"
	RuntimeCommandStatusCancelled = "cancelled"
)

// RuntimeCommand mirrors one runtime_commands row. Payload/Result stay as raw
// JSON so callers can store command-specific data without coupling the storage
// layer to every command type.
type RuntimeCommand struct {
	CommandID      string
	UserID         int64
	RuntimeID      string
	SessionID      string
	IdempotencyKey string
	CommandType    string
	Status         string
	DeadlineAt     time.Time
	SentAt         *time.Time
	AckedAt        *time.Time
	StartedAt      *time.Time
	CompletedAt    *time.Time
	CancelledAt    *time.Time
	AttemptCount   int
	Payload        json.RawMessage
	Result         json.RawMessage
	FailureReason  string
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

func IsRuntimeCommandInFlightStatus(status string) bool {
	switch status {
	case RuntimeCommandStatusSent, RuntimeCommandStatusAcked, RuntimeCommandStatusRunning:
		return true
	default:
		return false
	}
}

func IsRuntimeCommandTerminalStatus(status string) bool {
	switch status {
	case RuntimeCommandStatusSucceeded, RuntimeCommandStatusFailed, RuntimeCommandStatusTimedOut, RuntimeCommandStatusCancelled:
		return true
	default:
		return false
	}
}
