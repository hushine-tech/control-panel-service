package domain

import "time"

// Runtime status values mirror the runtime_registry.status column. Current
// lifecycle is starting -> active <-> unhealthy -> terminal.
const (
	RuntimeStatusStarting       = "starting"
	RuntimeStatusPaired         = "paired" // legacy pre-unification status; treated like starting.
	RuntimeStatusActive         = "active"
	RuntimeStatusUnhealthy      = "unhealthy"
	RuntimeStatusHeartbeatStale = "heartbeat_stale"
	RuntimeStatusEnded          = "ended"
	RuntimeStatusCancelled      = "cancelled"
	RuntimeStatusFailed         = "failed"
)

// Runtime ended reason values mirror runtime_registry.ended_reason.
const (
	RuntimeEndedReasonUserCancelled        = "user_cancelled"
	RuntimeEndedReasonRuntimeExited        = "runtime_exited"
	RuntimeEndedReasonHeartbeatStale       = "heartbeat_stale"
	RuntimeEndedReasonProvisionFailed      = "provision_failed"
	RuntimeEndedReasonAuthFailed           = "auth_failed"
	RuntimeEndedReasonControlPanelShutdown = "control_panel_shutdown"
)

// Runtime cleanup status values mirror runtime_registry.cleanup_status.
const (
	RuntimeCleanupStatusSucceeded = "succeeded"
	RuntimeCleanupStatusFailed    = "failed"
	RuntimeCleanupStatusUserOwned = "user_owned"
)

func IsRuntimeTerminalStatus(status string) bool {
	switch status {
	case RuntimeStatusHeartbeatStale, RuntimeStatusEnded, RuntimeStatusCancelled, RuntimeStatusFailed:
		return true
	default:
		return false
	}
}

func RuntimeTerminalStatusForReason(reason string) string {
	switch reason {
	case RuntimeEndedReasonUserCancelled:
		return RuntimeStatusCancelled
	case RuntimeEndedReasonHeartbeatStale:
		return RuntimeStatusHeartbeatStale
	case RuntimeEndedReasonAuthFailed, RuntimeEndedReasonProvisionFailed:
		return RuntimeStatusFailed
	default:
		return RuntimeStatusEnded
	}
}

// Runtime source values mirror the runtime_registry.source column.
const (
	RuntimeSourceHosted     = "hosted"
	RuntimeSourceSelfHosted = "self_hosted"
)

// Runtime is the canonical Go view of a runtime_registry row. Token-bearing
// fields (token_hash) stay on this struct because the repo needs to write
// them; gRPC layer redacts them in the wire-format Runtime message.
type Runtime struct {
	RuntimeID                  string
	CredentialKeyID            string // self_hosted only; binds registry rows to RuntimeChannel credentials
	UserID                     int64  // 0 = unpaired
	Name                       string
	Source                     string // RuntimeSource*
	Role                       CredentialRole
	EndpointHost               string
	GRPCPort                   int32
	DebugPort                  int32 // 0 = none
	Capabilities               []string
	ResourceProfile            string
	Version                    string
	Status                     string // RuntimeStatus*
	TokenHash                  string
	PairedAt                   *time.Time
	StartedAt                  *time.Time
	EndedAt                    *time.Time
	EndedReason                string
	CleanupStatus              string
	CleanupReason              string
	CleanupAt                  *time.Time
	HeartbeatAt                *time.Time
	ConnectionOwnerInstanceID  string
	ConnectionOwnerAcquiredAt  *time.Time
	ConnectionOwnerHeartbeatAt *time.Time
	DebugWorkspace             DebugWorkspaceState
	DebugDataset               *DebugDatasetState
	CreatedAt                  time.Time
	UpdatedAt                  time.Time
}

type DebugWorkspaceState struct {
	HostPath              string
	ContainerPath         string
	TemplatePath          string
	ArchivedTemplatePath  string
	VSCodeLaunchCreated   bool
	VSCodeLaunchPreserved bool
	PyCharmDocCreated     bool
	PyCharmDocPreserved   bool
	PreparedAt            *time.Time
	LastError             string
}

type DebugDatasetState struct {
	DatasetID      string
	UserID         int64
	AccountID      int64
	RuntimeID      string
	Market         string
	Symbol         string
	Interval       string
	StartAt        time.Time
	EndAt          time.Time
	BarCount       int64
	CoverageStatus string
	LoadedAt       time.Time
	State          string
	LastError      string
}

// RuntimeUsageCounts is the per-user usage snapshot used to compare against
// plan/platform quotas during route resolution and provisioning.
type RuntimeUsageCounts struct {
	Hosted     int64 // count of hosted, non-ended runtimes
	SelfHosted int64 // count of self_hosted, non-ended runtimes
}

// RuntimeChannelLease is a non-persistent-user-visible resume token for the
// currently running runtime process. Only the hash is stored.
type RuntimeChannelLease struct {
	RuntimeID       string
	UserID          int64
	CredentialKeyID string
	LeaseHash       string
	IssuedAt        time.Time
	ExpiresAt       time.Time
	LastUsedAt      *time.Time
	RevokedAt       *time.Time
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

// RuntimeAdmissionFailure is the auditable, non-secret view of a failed
// RuntimeChannel admission attempt.
type RuntimeAdmissionFailure struct {
	AdmissionFailureID int64
	UserID             int64
	CredentialKeyID    string
	RequestedRuntimeID string
	RequestedName      string
	Source             string
	Role               CredentialRole
	FailureCode        string
	Reason             string
	ConsumedRuntimeID  string
	FirstSeenAt        time.Time
	LastSeenAt         time.Time
	AttemptCount       int
}
