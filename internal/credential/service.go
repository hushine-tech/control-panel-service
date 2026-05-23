// Package credential is the Phase D3 owner of the runtime-credential
// lifecycle (issue / list / revoke). It is a sibling subdomain to
// `internal/runtime/` (D1 runtime registry) and `internal/marketdata/`
// (D2 market-data control plane) — same `*sql.DB` pool, distinct
// package ownership. The proto RPCs live on `ControlPanelService`
// (see `proto/control_panel_service.proto`); the gRPC adapter in
// `internal/runtime/grpc.go` constructs a credential.Service and
// dispatches the three credential RPCs to it.
package credential

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/hushine-tech/control-panel-service/internal/domain"
	"github.com/hushine-tech/control-panel-service/internal/repository"
)

// Repository is the persistence surface the credential service needs.
// Defining it here as a narrow interface keeps the package focused and
// makes unit testing without a real *sql.DB easy.
type Repository interface {
	CreateRuntimeCredential(ctx context.Context, c domain.RuntimeCredential) error
	GetRuntimeCredential(ctx context.Context, keyID string) (domain.RuntimeCredential, error)
	ListRuntimeCredentialsByUser(ctx context.Context, userID int64, includeInactive bool) ([]domain.RuntimeCredential, error)
	RevokeRuntimeCredential(ctx context.Context, keyID string, userID int64) (domain.RuntimeCredential, error)
}

// RevokeStreamCloser is the hook for closing live RuntimeChannel streams
// on revoke. Phase D3 section 2 implements this; until that lands the
// service accepts a no-op implementation that returns 0/0.
type RevokeStreamCloser interface {
	// CloseStreamsForKey closes any open RuntimeChannel streams keyed by
	// keyID and ends their associated runtime_registry rows. Returns
	// (streamsClosed, runtimesEnded) for the response payload.
	CloseStreamsForKey(ctx context.Context, keyID string) (streamsClosed, runtimesEnded int, err error)
}

// NoopStreamCloser is the default until Phase D3 section 2 wires up the
// real stream registry. It logs intent only — when the registry exists,
// swap this for the real closer.
type NoopStreamCloser struct{}

func (NoopStreamCloser) CloseStreamsForKey(_ context.Context, _ string) (int, int, error) {
	return 0, 0, nil
}

// Service is the credential lifecycle implementation.
type Service struct {
	repo   Repository
	closer RevokeStreamCloser

	// now is overrideable for tests.
	now func() time.Time
}

func New(repo Repository, closer RevokeStreamCloser) *Service {
	if closer == nil {
		closer = NoopStreamCloser{}
	}
	return &Service{
		repo:   repo,
		closer: closer,
		now:    time.Now,
	}
}

// SetClock overrides the time source for tests.
func (s *Service) SetClock(now func() time.Time) { s.now = now }

// ── Errors ──────────────────────────────────────────────────────────────────

var (
	// ErrInvalidArgument — caller passed a malformed input.
	ErrInvalidArgument = errors.New("invalid argument")
	// ErrNotFound — credential does not exist.
	ErrNotFound = errors.New("credential not found")
	// ErrPermissionDenied — credential exists but is owned by another user.
	ErrPermissionDenied = errors.New("permission denied")
)

// ── Issue ───────────────────────────────────────────────────────────────────

type IssueArgs struct {
	UserID int64
	Label  string
	Role   domain.CredentialRole
}

// Issue generates a fresh Ed25519 keypair, persists the public key keyed
// by a server-generated key_id, and returns the IssuedCredential bundle
// (which carries the private key for one-time download). The private
// key is NOT persisted anywhere.
func (s *Service) Issue(ctx context.Context, args IssueArgs) (domain.IssuedCredential, error) {
	return s.issue(ctx, args, false)
}

func (s *Service) IssueHostedInternalRuntimeCredential(ctx context.Context, userID int64, runtimeID, name string) (domain.IssuedCredential, error) {
	label := strings.TrimSpace(name)
	if label == "" {
		label = runtimeID
	}
	return s.issue(ctx, IssueArgs{
		UserID: userID,
		Label:  "hosted:" + label,
		Role:   domain.CredentialRoleExecutor,
	}, true)
}

func (s *Service) issue(ctx context.Context, args IssueArgs, hostedInternal bool) (domain.IssuedCredential, error) {
	if args.UserID <= 0 {
		return domain.IssuedCredential{}, fmt.Errorf("%w: user_id is required", ErrInvalidArgument)
	}
	if args.Role != domain.CredentialRoleExecutor && args.Role != domain.CredentialRoleDebugger {
		return domain.IssuedCredential{}, fmt.Errorf("%w: role must be executor or debugger", ErrInvalidArgument)
	}
	label := strings.TrimSpace(args.Label)

	// Generate the keypair.
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return domain.IssuedCredential{}, fmt.Errorf("generate keypair: %w", err)
	}
	pubPEM, err := encodePublicKeyPEM(pub)
	if err != nil {
		return domain.IssuedCredential{}, fmt.Errorf("encode public key: %w", err)
	}
	privPEM, err := encodePrivateKeyPEM(priv)
	if err != nil {
		return domain.IssuedCredential{}, fmt.Errorf("encode private key: %w", err)
	}

	keyID, err := generateKeyID()
	if err != nil {
		return domain.IssuedCredential{}, fmt.Errorf("generate key_id: %w", err)
	}

	now := s.now().UTC()
	downloadedAt := now
	cred := domain.RuntimeCredential{
		KeyID:          keyID,
		UserID:         args.UserID,
		Label:          label,
		Role:           args.Role,
		PublicKeyPEM:   pubPEM,
		Status:         domain.CredentialStatusDownloaded,
		CreatedAt:      now,
		DownloadedAt:   &downloadedAt,
		HostedInternal: hostedInternal,
	}
	if err := s.repo.CreateRuntimeCredential(ctx, cred); err != nil {
		// A repository.ErrConflict here means the key_id PK collided —
		// astronomically unlikely with 16 random bytes (~2^-128 per
		// call). When it does happen it is a SERVER-side failure, not
		// a caller error: the request was well-formed, the server just
		// failed to mint a unique id. Surface it as a generic persist
		// error so the gRPC adapter maps it to Internal (not
		// InvalidArgument). The user CAN retry but does not need to
		// change the request.
		return domain.IssuedCredential{}, fmt.Errorf("persist credential: %w", err)
	}

	return domain.IssuedCredential{
		RuntimeCredential: cred,
		PrivateKeyPEM:     privPEM,
	}, nil
}

// ── List ────────────────────────────────────────────────────────────────────

// List returns the credentials owned by userID. Consumed credentials are
// visible by default; revoked/expired credentials require includeInactive.
func (s *Service) List(ctx context.Context, userID int64, includeInactive bool) ([]domain.RuntimeCredential, error) {
	if userID <= 0 {
		return nil, fmt.Errorf("%w: user_id is required", ErrInvalidArgument)
	}
	return s.repo.ListRuntimeCredentialsByUser(ctx, userID, includeInactive)
}

// ── Revoke ──────────────────────────────────────────────────────────────────

type RevokeResult struct {
	Credential    domain.RuntimeCredential
	StreamsClosed int
	RuntimesEnded int
}

// Revoke flips the credential to status='revoked', closes any open
// RuntimeChannel streams keyed by it, and ends associated
// runtime_registry rows.
func (s *Service) Revoke(ctx context.Context, userID int64, keyID string) (RevokeResult, error) {
	if userID <= 0 {
		return RevokeResult{}, fmt.Errorf("%w: user_id is required", ErrInvalidArgument)
	}
	if strings.TrimSpace(keyID) == "" {
		return RevokeResult{}, fmt.Errorf("%w: key_id is required", ErrInvalidArgument)
	}
	cred, err := s.repo.RevokeRuntimeCredential(ctx, keyID, userID)
	if errors.Is(err, repository.ErrNotFound) {
		return RevokeResult{}, ErrNotFound
	}
	if errors.Is(err, repository.ErrConflict) {
		// Repository signals "owner mismatch" via ErrConflict; map to
		// PermissionDenied at the service layer.
		return RevokeResult{}, ErrPermissionDenied
	}
	if err != nil {
		return RevokeResult{}, fmt.Errorf("revoke credential: %w", err)
	}

	// Close any live streams + end runtime registry rows. Best-effort:
	// errors here do NOT roll back the revocation — the credential is
	// already revoked in the DB and that is the authoritative state.
	streamsClosed, runtimesEnded, closeErr := s.closer.CloseStreamsForKey(ctx, keyID)
	if closeErr != nil {
		// We deliberately do not propagate; the revocation already
		// succeeded. The next HELLO from a stream signed by this key
		// will be rejected at signature-verification time anyway.
		// TODO: log the closeErr at WARN level when a logger is wired
		// into the credential service.
		streamsClosed = 0
		runtimesEnded = 0
	}

	return RevokeResult{
		Credential:    cred,
		StreamsClosed: streamsClosed,
		RuntimesEnded: runtimesEnded,
	}, nil
}

// ── Lookup (for HELLO verification — Phase D3 section 2) ────────────────────

// Get returns a credential by keyID regardless of status. Used by the
// HELLO verification path; the caller is responsible for rejecting
// HELLOs whose credential is revoked.
func (s *Service) Get(ctx context.Context, keyID string) (domain.RuntimeCredential, error) {
	if strings.TrimSpace(keyID) == "" {
		return domain.RuntimeCredential{}, fmt.Errorf("%w: key_id is required", ErrInvalidArgument)
	}
	cred, err := s.repo.GetRuntimeCredential(ctx, keyID)
	if errors.Is(err, repository.ErrNotFound) {
		return domain.RuntimeCredential{}, ErrNotFound
	}
	return cred, err
}

// ── Helpers ─────────────────────────────────────────────────────────────────

// generateKeyID returns a base64url-encoded random 16-byte id (no
// padding, ~22 chars). Cryptographically random; collision probability
// is astronomically low.
func generateKeyID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b[:]), nil
}

// encodePublicKeyPEM marshals an Ed25519 public key as PKIX/PEM.
func encodePublicKeyPEM(pub ed25519.PublicKey) (string, error) {
	der, err := x509.MarshalPKIXPublicKey(pub)
	if err != nil {
		return "", err
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der})), nil
}

// encodePrivateKeyPEM marshals an Ed25519 private key as PKCS#8/PEM.
func encodePrivateKeyPEM(priv ed25519.PrivateKey) (string, error) {
	der, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return "", err
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})), nil
}
