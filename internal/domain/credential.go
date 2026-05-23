package domain

import "time"

// CredentialStatus is the lifecycle state of a runtime credential.
// Only two states exist: revocation is irreversible (Decision 8 in
// `phase-d3-self-hosted-runtime/design.md`).
type CredentialStatus string

const (
	CredentialStatusActive     CredentialStatus = "active"
	CredentialStatusDownloaded CredentialStatus = "downloaded"
	CredentialStatusConsumed   CredentialStatus = "consumed"
	CredentialStatusRevoked    CredentialStatus = "revoked"
	CredentialStatusExpired    CredentialStatus = "expired"
)

// CredentialRole is the runtime role authorized by a credential.
type CredentialRole string

const (
	CredentialRoleExecutor CredentialRole = "executor"
	CredentialRoleDebugger CredentialRole = "debugger"
)

// RuntimeCredential is the canonical view of a `runtime_credentials`
// row. Note: the private key is NEVER stored on the platform — it is
// returned exactly once at issue time and discarded server-side. This
// struct therefore has no PrivateKeyPEM field.
type RuntimeCredential struct {
	KeyID             string
	UserID            int64
	Label             string
	Role              CredentialRole
	PublicKeyPEM      string
	Status            CredentialStatus
	CreatedAt         time.Time
	DownloadedAt      *time.Time
	ConsumedAt        *time.Time
	ConsumedRuntimeID string
	ExpiresAt         *time.Time
	LastUsedAt        *time.Time
	RevokedAt         *time.Time
	HostedInternal    bool
}

// IssuedCredential is the one-time bundle returned to the user at
// issue time. It carries the freshly-generated private key so the
// frontend can trigger a download. The private key MUST NOT be
// persisted anywhere on the platform.
type IssuedCredential struct {
	RuntimeCredential
	PrivateKeyPEM string
}
