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
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/hushine-tech/control-panel-service/internal/domain"
	"github.com/hushine-tech/control-panel-service/internal/repository"
	"github.com/hushine-tech/control-panel-service/internal/runtimecert"
)

// Repository is the persistence surface the credential service needs.
// Defining it here as a narrow interface keeps the package focused and
// makes unit testing without a real *sql.DB easy.
type Repository interface {
	CreateRuntimeCredential(ctx context.Context, c domain.RuntimeCredential) error
	GetRuntimeCredential(ctx context.Context, keyID string) (domain.RuntimeCredential, error)
	ListRuntimeCredentialsByUser(ctx context.Context, userID int64, includeInactive bool) ([]domain.RuntimeCredential, error)
	ListRuntimeCredentialsByUserPage(ctx context.Context, userID int64, includeInactive bool, limit, offset int) ([]domain.RuntimeCredential, int64, bool, error)
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

type certSigner interface {
	CAPEM() []byte
	SignRuntimeClientCertificate(runtimecert.SignRequest) ([]byte, *x509.Certificate, error)
}

// Service is the credential lifecycle implementation.
type Service struct {
	repo        Repository
	closer      RevokeStreamCloser
	signer      certSigner
	serverCAPEM []byte

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

func (s *Service) SetCertificateSigner(signer certSigner) {
	s.signer = signer
}

func (s *Service) SetRuntimeServerCAPEM(serverCAPEM []byte) {
	s.serverCAPEM = append([]byte(nil), serverCAPEM...)
}

// ── Errors ──────────────────────────────────────────────────────────────────

var (
	// ErrInvalidArgument — caller passed a malformed input.
	ErrInvalidArgument = errors.New("invalid argument")
	// ErrNotFound — credential does not exist.
	ErrNotFound = errors.New("credential not found")
	// ErrPermissionDenied — credential exists but is owned by another user.
	ErrPermissionDenied = errors.New("permission denied")
	// ErrCertificateSignerUnavailable — runtime mTLS credential issuance is not configured.
	ErrCertificateSignerUnavailable = errors.New("runtime certificate signer unavailable")
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
	return s.issue(ctx, args, false, "")
}

func (s *Service) IssueHostedInternalRuntimeCredential(ctx context.Context, userID int64, runtimeID, name string) (domain.IssuedCredential, error) {
	runtimeID = strings.TrimSpace(runtimeID)
	if runtimeID == "" {
		return domain.IssuedCredential{}, fmt.Errorf("%w: runtime_id is required", ErrInvalidArgument)
	}
	label := strings.TrimSpace(name)
	if label == "" {
		label = runtimeID
	}
	return s.issue(ctx, IssueArgs{
		UserID: userID,
		Label:  "hosted:" + label,
		Role:   domain.CredentialRoleExecutor,
	}, true, runtimeID)
}

func (s *Service) issue(ctx context.Context, args IssueArgs, hostedInternal bool, certificateRuntimeID string) (domain.IssuedCredential, error) {
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
	if s.signer == nil {
		return domain.IssuedCredential{}, fmt.Errorf("%w: runtime certificate signer is not configured", ErrCertificateSignerUnavailable)
	}
	if len(s.serverCAPEM) == 0 {
		return domain.IssuedCredential{}, fmt.Errorf("%w: runtime server ca is not configured", ErrCertificateSignerUnavailable)
	}

	now := s.now().UTC()
	certRuntimeID := "selfhosted-" + keyID
	if hostedInternal {
		certRuntimeID = strings.TrimSpace(certificateRuntimeID)
		if certRuntimeID == "" {
			return domain.IssuedCredential{}, fmt.Errorf("%w: runtime_id is required", ErrInvalidArgument)
		}
	}
	clientKeyPEM, csrPEM, err := generateClientKeyAndCSR(certRuntimeID)
	if err != nil {
		return domain.IssuedCredential{}, fmt.Errorf("generate runtime client key: %w", err)
	}
	issuer := domain.RuntimeCredentialIssuerUser
	source := domain.RuntimeSourceSelfHosted
	if hostedInternal {
		issuer = domain.RuntimeCredentialIssuerHostedInternal
		source = domain.RuntimeSourceHosted
	}
	certPEM, cert, err := s.signer.SignRuntimeClientCertificate(runtimecert.SignRequest{
		CSRPEM:    csrPEM,
		RuntimeID: certRuntimeID,
		UserID:    args.UserID,
		Source:    source,
		Role:      string(args.Role),
		Name:      label,
		TTL:       24 * time.Hour,
		Now:       now,
	})
	if err != nil {
		return domain.IssuedCredential{}, fmt.Errorf("sign runtime client certificate: %w", err)
	}
	clientCertExpiresAt := cert.NotAfter

	downloadedAt := now
	cred := domain.RuntimeCredential{
		KeyID:                 keyID,
		UserID:                args.UserID,
		Label:                 label,
		Role:                  args.Role,
		PublicKeyPEM:          pubPEM,
		Status:                domain.CredentialStatusDownloaded,
		CreatedAt:             now,
		DownloadedAt:          &downloadedAt,
		HostedInternal:        hostedInternal,
		ClientCertPEM:         string(certPEM),
		ClientCertFingerprint: certificateFingerprint(cert),
		ClientCertExpiresAt:   &clientCertExpiresAt,
		Issuer:                issuer,
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
		RuntimeCredential:   cred,
		PrivateKeyPEM:       privPEM,
		ClientCertPEM:       string(certPEM),
		ClientKeyPEM:        string(clientKeyPEM),
		ServerCAPEM:         string(s.serverCAPEM),
		ClientCertExpiresAt: &clientCertExpiresAt,
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

func (s *Service) ListPage(ctx context.Context, userID int64, includeInactive bool, limit, offset int) ([]domain.RuntimeCredential, int64, bool, error) {
	if userID <= 0 {
		return nil, 0, false, fmt.Errorf("%w: user_id is required", ErrInvalidArgument)
	}
	return s.repo.ListRuntimeCredentialsByUserPage(ctx, userID, includeInactive, limit, offset)
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

func generateClientKeyAndCSR(runtimeID string) (keyPEM, csrPEM []byte, err error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, err
	}
	csrDER, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{
		Subject: pkix.Name{CommonName: runtimeID},
	}, key)
	if err != nil {
		return nil, nil, err
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return nil, nil, err
	}
	return pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}),
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: csrDER}),
		nil
}

func certificateFingerprint(cert *x509.Certificate) string {
	if cert == nil {
		return ""
	}
	sum := sha256.Sum256(cert.Raw)
	return hex.EncodeToString(sum[:])
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
