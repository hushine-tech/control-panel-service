package credential

import (
	"context"
	"crypto/ed25519"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"testing"
	"time"

	"github.com/hushine-tech/control-panel-service/internal/domain"
	"github.com/hushine-tech/control-panel-service/internal/repository"
)

// stubRepo is an in-memory Repository for unit tests.
type stubRepo struct {
	creds map[string]domain.RuntimeCredential
}

func newStubRepo() *stubRepo {
	return &stubRepo{creds: map[string]domain.RuntimeCredential{}}
}

func (s *stubRepo) CreateRuntimeCredential(_ context.Context, c domain.RuntimeCredential) error {
	if _, ok := s.creds[c.KeyID]; ok {
		return repository.ErrConflict
	}
	s.creds[c.KeyID] = c
	return nil
}

func (s *stubRepo) GetRuntimeCredential(_ context.Context, keyID string) (domain.RuntimeCredential, error) {
	c, ok := s.creds[keyID]
	if !ok {
		return domain.RuntimeCredential{}, repository.ErrNotFound
	}
	return c, nil
}

func (s *stubRepo) ListRuntimeCredentialsByUser(_ context.Context, userID int64, includeInactive bool) ([]domain.RuntimeCredential, error) {
	var out []domain.RuntimeCredential
	for _, c := range s.creds {
		if c.UserID != userID {
			continue
		}
		if !includeInactive &&
			c.Status != domain.CredentialStatusActive &&
			c.Status != domain.CredentialStatusDownloaded &&
			c.Status != domain.CredentialStatusConsumed {
			continue
		}
		out = append(out, c)
	}
	return out, nil
}

func (s *stubRepo) ListRuntimeCredentialsByUserPage(ctx context.Context, userID int64, includeInactive bool, limit, offset int) ([]domain.RuntimeCredential, int64, bool, error) {
	out, err := s.ListRuntimeCredentialsByUser(ctx, userID, includeInactive)
	if err != nil {
		return nil, 0, false, err
	}
	total := len(out)
	if offset > total {
		offset = total
	}
	end := total
	if limit > 0 && offset+limit < end {
		end = offset + limit
	}
	return out[offset:end], int64(total), end < total, nil
}

func (s *stubRepo) RevokeRuntimeCredential(_ context.Context, keyID string, userID int64) (domain.RuntimeCredential, error) {
	c, ok := s.creds[keyID]
	if !ok {
		return domain.RuntimeCredential{}, repository.ErrNotFound
	}
	if c.UserID != userID {
		return domain.RuntimeCredential{}, repository.ErrConflict
	}
	if c.Status != domain.CredentialStatusRevoked {
		c.Status = domain.CredentialStatusRevoked
		now := time.Now().UTC()
		c.RevokedAt = &now
		s.creds[keyID] = c
	}
	return c, nil
}

// stubCloser records calls; returns canned counts.
type stubCloser struct {
	calls    []string
	streams  int
	runtimes int
}

func (c *stubCloser) CloseStreamsForKey(_ context.Context, keyID string) (int, int, error) {
	c.calls = append(c.calls, keyID)
	return c.streams, c.runtimes, nil
}

// ── Issue ──────────────────────────────────────────────────────────────────

func TestIssue_HappyPath(t *testing.T) {
	svc := New(newStubRepo(), nil)
	res, err := svc.Issue(context.Background(), IssueArgs{UserID: 42, Label: "home VPS", Role: domain.CredentialRoleExecutor})
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	if res.KeyID == "" {
		t.Error("KeyID empty")
	}
	if res.PublicKeyPEM == "" || res.PrivateKeyPEM == "" {
		t.Error("PEM blocks empty")
	}
	if res.UserID != 42 {
		t.Errorf("UserID = %d, want 42", res.UserID)
	}
	if res.Label != "home VPS" {
		t.Errorf("Label = %q, want 'home VPS'", res.Label)
	}
	if res.Status != domain.CredentialStatusDownloaded {
		t.Errorf("Status = %q, want downloaded", res.Status)
	}
	if res.DownloadedAt == nil {
		t.Error("DownloadedAt nil, want timestamp because private key is returned by Issue")
	}
	if res.Role != domain.CredentialRoleExecutor {
		t.Errorf("Role = %q, want executor", res.Role)
	}

	// Round-trip: parse the PEMs we issued.
	pubBlock, _ := pem.Decode([]byte(res.PublicKeyPEM))
	if pubBlock == nil || pubBlock.Type != "PUBLIC KEY" {
		t.Fatalf("public PEM bad: %+v", pubBlock)
	}
	pubAny, err := x509.ParsePKIXPublicKey(pubBlock.Bytes)
	if err != nil {
		t.Fatalf("parse public key: %v", err)
	}
	pub, ok := pubAny.(ed25519.PublicKey)
	if !ok {
		t.Fatalf("public key wrong type: %T", pubAny)
	}

	privBlock, _ := pem.Decode([]byte(res.PrivateKeyPEM))
	if privBlock == nil || privBlock.Type != "PRIVATE KEY" {
		t.Fatalf("private PEM bad: %+v", privBlock)
	}
	privAny, err := x509.ParsePKCS8PrivateKey(privBlock.Bytes)
	if err != nil {
		t.Fatalf("parse private key: %v", err)
	}
	priv, ok := privAny.(ed25519.PrivateKey)
	if !ok {
		t.Fatalf("private key wrong type: %T", privAny)
	}

	// Sign + verify a message to confirm the keypair pairs.
	msg := []byte("hello, runtime")
	sig := ed25519.Sign(priv, msg)
	if !ed25519.Verify(pub, msg, sig) {
		t.Error("signed message did not verify with paired public key")
	}
}

func TestIssue_CanCreateDebuggerCredential(t *testing.T) {
	svc := New(newStubRepo(), nil)
	res, err := svc.Issue(context.Background(), IssueArgs{UserID: 42, Label: "debug laptop", Role: domain.CredentialRoleDebugger})
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	if res.Role != domain.CredentialRoleDebugger {
		t.Errorf("Role = %q, want debugger", res.Role)
	}
}

func TestIssue_RejectsInvalidRole(t *testing.T) {
	svc := New(newStubRepo(), nil)
	_, err := svc.Issue(context.Background(), IssueArgs{UserID: 42, Role: "admin"})
	if !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("err = %v, want ErrInvalidArgument", err)
	}
}

func TestIssue_RejectsZeroUserID(t *testing.T) {
	svc := New(newStubRepo(), nil)
	_, err := svc.Issue(context.Background(), IssueArgs{UserID: 0, Role: domain.CredentialRoleExecutor})
	if !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("err = %v, want ErrInvalidArgument", err)
	}
}

func TestIssue_TrimsLabel(t *testing.T) {
	svc := New(newStubRepo(), nil)
	res, err := svc.Issue(context.Background(), IssueArgs{UserID: 1, Label: "   spaced   ", Role: domain.CredentialRoleExecutor})
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	if res.Label != "spaced" {
		t.Errorf("Label = %q, want trimmed", res.Label)
	}
}

// ── List ───────────────────────────────────────────────────────────────────

func TestList_ExcludesRevokedByDefault(t *testing.T) {
	repo := newStubRepo()
	svc := New(repo, nil)
	a, _ := svc.Issue(context.Background(), IssueArgs{UserID: 7, Role: domain.CredentialRoleExecutor})
	b, _ := svc.Issue(context.Background(), IssueArgs{UserID: 7, Label: "second", Role: domain.CredentialRoleExecutor})
	if _, err := svc.Revoke(context.Background(), 7, b.KeyID); err != nil {
		t.Fatalf("Revoke: %v", err)
	}

	active, err := svc.List(context.Background(), 7, false)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(active) != 1 || active[0].KeyID != a.KeyID {
		t.Errorf("active list = %+v, want only %s", active, a.KeyID)
	}
	if active[0].Status != domain.CredentialStatusDownloaded {
		t.Errorf("active list status = %q, want downloaded credential to remain listable", active[0].Status)
	}

	all, err := svc.List(context.Background(), 7, true)
	if err != nil {
		t.Fatalf("List include_revoked: %v", err)
	}
	if len(all) != 2 {
		t.Errorf("all list len = %d, want 2", len(all))
	}
}

func TestList_IncludesConsumedByDefault(t *testing.T) {
	repo := newStubRepo()
	now := time.Unix(1_700_000_000, 0).UTC()
	repo.creds["consumed"] = domain.RuntimeCredential{
		KeyID:             "consumed",
		UserID:            7,
		Role:              domain.CredentialRoleExecutor,
		Status:            domain.CredentialStatusConsumed,
		CreatedAt:         now,
		ConsumedAt:        &now,
		ConsumedRuntimeID: "runtime-consumer",
	}
	repo.creds["revoked"] = domain.RuntimeCredential{
		KeyID:     "revoked",
		UserID:    7,
		Role:      domain.CredentialRoleExecutor,
		Status:    domain.CredentialStatusRevoked,
		CreatedAt: now,
		RevokedAt: &now,
	}
	svc := New(repo, nil)

	visible, err := svc.List(context.Background(), 7, false)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(visible) != 1 || visible[0].KeyID != "consumed" || visible[0].ConsumedRuntimeID != "runtime-consumer" {
		t.Fatalf("visible = %+v, want consumed credential with runtime binding", visible)
	}
}

func TestList_RejectsZeroUserID(t *testing.T) {
	svc := New(newStubRepo(), nil)
	_, err := svc.List(context.Background(), 0, false)
	if !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("err = %v, want ErrInvalidArgument", err)
	}
}

// ── Revoke ─────────────────────────────────────────────────────────────────

func TestRevoke_HappyPath(t *testing.T) {
	repo := newStubRepo()
	closer := &stubCloser{streams: 2, runtimes: 3}
	svc := New(repo, closer)
	issued, _ := svc.Issue(context.Background(), IssueArgs{UserID: 9, Role: domain.CredentialRoleExecutor})

	res, err := svc.Revoke(context.Background(), 9, issued.KeyID)
	if err != nil {
		t.Fatalf("Revoke: %v", err)
	}
	if res.Credential.Status != domain.CredentialStatusRevoked {
		t.Errorf("Status = %q, want revoked", res.Credential.Status)
	}
	if res.StreamsClosed != 2 || res.RuntimesEnded != 3 {
		t.Errorf("counters = (%d, %d), want (2, 3)", res.StreamsClosed, res.RuntimesEnded)
	}
	if len(closer.calls) != 1 || closer.calls[0] != issued.KeyID {
		t.Errorf("closer called with %v, want [%s]", closer.calls, issued.KeyID)
	}
}

func TestRevoke_NotFound(t *testing.T) {
	svc := New(newStubRepo(), nil)
	_, err := svc.Revoke(context.Background(), 1, "no-such-key")
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("err = %v, want ErrNotFound", err)
	}
}

func TestRevoke_PermissionDenied(t *testing.T) {
	svc := New(newStubRepo(), nil)
	issued, _ := svc.Issue(context.Background(), IssueArgs{UserID: 1, Role: domain.CredentialRoleExecutor})
	_, err := svc.Revoke(context.Background(), 99, issued.KeyID)
	if !errors.Is(err, ErrPermissionDenied) {
		t.Errorf("err = %v, want ErrPermissionDenied", err)
	}
}

func TestRevoke_Idempotent(t *testing.T) {
	svc := New(newStubRepo(), nil)
	issued, _ := svc.Issue(context.Background(), IssueArgs{UserID: 1, Role: domain.CredentialRoleExecutor})
	_, err := svc.Revoke(context.Background(), 1, issued.KeyID)
	if err != nil {
		t.Fatalf("first revoke: %v", err)
	}
	res, err := svc.Revoke(context.Background(), 1, issued.KeyID)
	if err != nil {
		t.Fatalf("second revoke (should be no-op): %v", err)
	}
	if res.Credential.Status != domain.CredentialStatusRevoked {
		t.Errorf("Status = %q, want revoked", res.Credential.Status)
	}
}

// ── Get ────────────────────────────────────────────────────────────────────

func TestGet_HappyPath(t *testing.T) {
	svc := New(newStubRepo(), nil)
	issued, _ := svc.Issue(context.Background(), IssueArgs{UserID: 1, Role: domain.CredentialRoleExecutor})
	got, err := svc.Get(context.Background(), issued.KeyID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.KeyID != issued.KeyID {
		t.Errorf("KeyID = %q, want %q", got.KeyID, issued.KeyID)
	}
}

func TestGet_NotFound(t *testing.T) {
	svc := New(newStubRepo(), nil)
	_, err := svc.Get(context.Background(), "no-such-key")
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("err = %v, want ErrNotFound", err)
	}
}
