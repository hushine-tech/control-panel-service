package runtimechannel

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	cpv1 "github.com/hushine-tech/control-panel-service/gen/controlpanelv1"
	"github.com/hushine-tech/control-panel-service/internal/domain"
	"github.com/hushine-tech/control-panel-service/internal/repository"
)

const (
	maxHelloClockSkew = 5 * time.Minute
	replayTTL         = 30 * time.Minute
)

var (
	ErrInvalidHello     = errors.New("invalid runtime hello")
	ErrPermissionDenied = errors.New("runtime hello permission denied")
	ErrReplay           = errors.New("runtime hello replay")
)

// Repository is the storage surface RuntimeChannel needs for HELLO auth and
// revocation. TimescaleRepository already implements this subset.
type Repository interface {
	GetRuntimeCredential(ctx context.Context, keyID string) (domain.RuntimeCredential, error)
	TouchRuntimeCredentialUsed(ctx context.Context, keyID string, at time.Time) error
	CreateOrReplaceHostedRuntime(ctx context.Context, r domain.Runtime) error
	CreateOrReplaceSelfHostedRuntime(ctx context.Context, r domain.Runtime) error
	GetRuntime(ctx context.Context, runtimeID string) (domain.Runtime, error)
	UpdateRuntimeHeartbeat(ctx context.Context, runtimeID string, at time.Time) error
	RecordRuntimeConnectionOwner(ctx context.Context, runtimeID, instanceID string, at time.Time) error
	ClearRuntimeConnectionOwner(ctx context.Context, runtimeID, instanceID string) error
	EndRuntimesByCredentialKey(ctx context.Context, keyID, reason string, endedAt time.Time) (int64, error)
	CreateRuntimeChannelLease(ctx context.Context, lease domain.RuntimeChannelLease) error
	GetRuntimeChannelLeaseByHash(ctx context.Context, leaseHash string) (domain.RuntimeChannelLease, error)
	TouchRuntimeChannelLease(ctx context.Context, runtimeID, leaseHash string, at time.Time) error
	RotateRuntimeChannelLease(ctx context.Context, runtimeID, oldLeaseHash, newLeaseHash string, expiresAt, at time.Time) error
	RecordRuntimeAdmissionFailure(ctx context.Context, failure domain.RuntimeAdmissionFailure) error
	ListRuntimeAdmissionFailuresByUser(ctx context.Context, userID int64, limit int) ([]domain.RuntimeAdmissionFailure, error)
	ClaimNextRuntimeCommand(ctx context.Context, runtimeID, ownerInstanceID string, at time.Time, inFlightLimit int) (domain.RuntimeCommand, bool, error)
	AcknowledgeRuntimeCommand(ctx context.Context, commandID string, at time.Time) (domain.RuntimeCommand, error)
	MarkRuntimeCommandRunning(ctx context.Context, commandID string, at time.Time) (domain.RuntimeCommand, error)
	CompleteRuntimeCommand(ctx context.Context, commandID, status string, result []byte, failureReason string, at time.Time) (domain.RuntimeCommand, error)
	RuntimeCommandCircuitOpen(ctx context.Context, runtimeID string, since time.Time, threshold int64) (bool, int64, error)
}

type AuthenticatedRuntime struct {
	KeyID           string
	UserID          int64
	RuntimeID       string
	Name            string
	Source          string
	Role            domain.CredentialRole
	EndpointHost    string
	GRPCPort        int32
	DebugPort       int32
	Capabilities    []string
	ResourceProfile string
	Version         string
	AuthenticatedAt time.Time
}

type helloPayload struct {
	Capabilities    []string `json:"capabilities"`
	DebugPort       int32    `json:"debug_port"`
	EndpointHost    string   `json:"endpoint_host"`
	GRPCPort        int32    `json:"grpc_port"`
	IssuedAtUnixMS  int64    `json:"issued_at_unix_ms"`
	KeyID           string   `json:"key_id"`
	Nonce           string   `json:"nonce"`
	ResourceProfile string   `json:"resource_profile"`
	RuntimeID       string   `json:"runtime_id"`
	Name            string   `json:"name"`
	Version         string   `json:"version"`
}

var runtimeChannelNameRe = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{1,46}[a-z0-9]$`)

// CanonicalHelloPayload is the exact byte payload signed by runtime and
// verified by control-panel-service. Keep this in sync with
// strategy_service.runtime_channel.canonical_hello_payload.
func CanonicalHelloPayload(h *cpv1.RuntimeHello) ([]byte, error) {
	if h == nil {
		return nil, fmt.Errorf("%w: hello is required", ErrInvalidHello)
	}
	p := helloPayload{
		Capabilities:    append([]string(nil), h.GetCapabilities()...),
		DebugPort:       h.GetDebugPort(),
		EndpointHost:    h.GetEndpointHost(),
		GRPCPort:        h.GetGrpcPort(),
		IssuedAtUnixMS:  h.GetIssuedAtUnixMs(),
		KeyID:           h.GetKeyId(),
		Name:            h.GetName(),
		Nonce:           h.GetNonce(),
		ResourceProfile: h.GetResourceProfile(),
		RuntimeID:       h.GetRuntimeId(),
		Version:         h.GetVersion(),
	}
	return json.Marshal(p)
}

func verifyHello(ctx context.Context, repo Repository, cache *ReplayCache, now func() time.Time, h *cpv1.RuntimeHello) (AuthenticatedRuntime, error) {
	if repo == nil {
		return AuthenticatedRuntime{}, fmt.Errorf("%w: repository is not configured", ErrInvalidHello)
	}
	if now == nil {
		now = time.Now
	}
	if h == nil {
		return AuthenticatedRuntime{}, fmt.Errorf("%w: hello is required", ErrInvalidHello)
	}
	if h.GetKeyId() == "" {
		return AuthenticatedRuntime{}, fmt.Errorf("%w: key_id is required", ErrInvalidHello)
	}
	if h.GetNonce() == "" {
		return AuthenticatedRuntime{}, fmt.Errorf("%w: nonce is required", ErrInvalidHello)
	}
	if h.GetSignature() == "" {
		return AuthenticatedRuntime{}, fmt.Errorf("%w: signature is required", ErrInvalidHello)
	}
	nonce, err := base64.RawURLEncoding.DecodeString(h.GetNonce())
	if err != nil || len(nonce) != 16 {
		return AuthenticatedRuntime{}, fmt.Errorf("%w: nonce must be base64url 16 bytes", ErrInvalidHello)
	}
	issued := time.UnixMilli(h.GetIssuedAtUnixMs()).UTC()
	at := now().UTC()
	if issued.IsZero() || at.Sub(issued) > maxHelloClockSkew || issued.Sub(at) > maxHelloClockSkew {
		return AuthenticatedRuntime{}, fmt.Errorf("%w: issued_at outside allowed clock skew", ErrPermissionDenied)
	}

	cred, err := repo.GetRuntimeCredential(ctx, h.GetKeyId())
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			return AuthenticatedRuntime{}, fmt.Errorf("%w: credential not found", ErrPermissionDenied)
		}
		return AuthenticatedRuntime{}, fmt.Errorf("lookup credential: %w", err)
	}
	if err := validateCredentialUsableForHello(cred, h, at); err != nil {
		return AuthenticatedRuntime{}, err
	}
	pub, err := parseEd25519PublicKeyPEM(cred.PublicKeyPEM)
	if err != nil {
		return AuthenticatedRuntime{}, fmt.Errorf("%w: malformed credential public key", ErrPermissionDenied)
	}
	sig, err := base64.RawURLEncoding.DecodeString(h.GetSignature())
	if err != nil {
		return AuthenticatedRuntime{}, fmt.Errorf("%w: signature must be base64url", ErrInvalidHello)
	}
	payload, err := CanonicalHelloPayload(h)
	if err != nil {
		return AuthenticatedRuntime{}, err
	}
	if !ed25519.Verify(pub, payload, sig) {
		return AuthenticatedRuntime{}, fmt.Errorf("%w: signature mismatch", ErrPermissionDenied)
	}
	if cache != nil && !cache.CheckAndStore(h.GetKeyId(), h.GetNonce(), at) {
		return AuthenticatedRuntime{}, fmt.Errorf("%w: nonce already seen", ErrReplay)
	}
	_ = repo.TouchRuntimeCredentialUsed(ctx, h.GetKeyId(), at)

	runtimeID := h.GetRuntimeId()
	if runtimeID == "" {
		runtimeID = "selfhosted-" + h.GetKeyId()
	}
	name := strings.TrimSpace(h.GetName())
	if name == "" {
		name = generateCustomRuntimeName()
	}
	if !validRuntimeChannelName(name) {
		return AuthenticatedRuntime{}, fmt.Errorf("%w: name must match %s", ErrInvalidHello, runtimeChannelNameRe.String())
	}
	role := cred.Role
	if role == "" {
		role = domain.CredentialRoleExecutor
	}
	if role != domain.CredentialRoleExecutor && role != domain.CredentialRoleDebugger {
		return AuthenticatedRuntime{}, fmt.Errorf("%w: credential role %q is not usable", ErrPermissionDenied, role)
	}
	source := domain.RuntimeSourceSelfHosted
	if cred.HostedInternal {
		source = domain.RuntimeSourceHosted
	}
	return AuthenticatedRuntime{
		KeyID:           h.GetKeyId(),
		UserID:          cred.UserID,
		RuntimeID:       runtimeID,
		Name:            name,
		Source:          source,
		Role:            role,
		EndpointHost:    h.GetEndpointHost(),
		GRPCPort:        h.GetGrpcPort(),
		DebugPort:       h.GetDebugPort(),
		Capabilities:    append([]string(nil), h.GetCapabilities()...),
		ResourceProfile: h.GetResourceProfile(),
		Version:         h.GetVersion(),
		AuthenticatedAt: at,
	}, nil
}

func validateCredentialUsableForHello(cred domain.RuntimeCredential, h *cpv1.RuntimeHello, at time.Time) error {
	if cred.ExpiresAt != nil && !cred.ExpiresAt.After(at) &&
		(cred.Status == domain.CredentialStatusActive || cred.Status == domain.CredentialStatusDownloaded) {
		return fmt.Errorf("%w: credential expired; stop retrying with this credential", ErrPermissionDenied)
	}
	switch cred.Status {
	case domain.CredentialStatusActive, domain.CredentialStatusDownloaded:
		return nil
	case domain.CredentialStatusConsumed:
		if cred.ConsumedRuntimeID != "" {
			return fmt.Errorf("%w: credential consumed by runtime %s; stop retrying with this credential", ErrPermissionDenied, cred.ConsumedRuntimeID)
		}
		return fmt.Errorf("%w: credential consumed; stop retrying with this credential", ErrPermissionDenied)
	case domain.CredentialStatusRevoked:
		return fmt.Errorf("%w: credential revoked; stop retrying with this credential", ErrPermissionDenied)
	case domain.CredentialStatusExpired:
		return fmt.Errorf("%w: credential expired; stop retrying with this credential", ErrPermissionDenied)
	default:
		return fmt.Errorf("%w: credential status %q is not usable", ErrPermissionDenied, cred.Status)
	}
}

func generateCustomRuntimeName() string {
	var b [6]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "custom-" + time.Now().UTC().Format("20060102150405")
	}
	return "custom-" + hex.EncodeToString(b[:])
}

func validRuntimeChannelName(name string) bool {
	return runtimeChannelNameRe.MatchString(name)
}

func parseEd25519PublicKeyPEM(publicKeyPEM string) (ed25519.PublicKey, error) {
	block, _ := pem.Decode([]byte(publicKeyPEM))
	if block == nil {
		return nil, errors.New("missing PEM block")
	}
	key, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, err
	}
	pub, ok := key.(ed25519.PublicKey)
	if !ok {
		return nil, errors.New("not an Ed25519 public key")
	}
	return pub, nil
}
