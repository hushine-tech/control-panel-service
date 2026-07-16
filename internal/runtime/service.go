// Package service holds the control-panel-service business logic. The
// gRPC layer (grpc.go) is a thin proto-translation wrapper.
package runtime

import (
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"regexp"
	"strings"
	"time"

	"github.com/hushine-tech/control-panel-service/internal/auth"
	"github.com/hushine-tech/control-panel-service/internal/config"
	"github.com/hushine-tech/control-panel-service/internal/domain"
	cpnotify "github.com/hushine-tech/control-panel-service/internal/notification"
	"github.com/hushine-tech/control-panel-service/internal/plan"
	"github.com/hushine-tech/control-panel-service/internal/provision"
	"github.com/hushine-tech/control-panel-service/internal/repository"
	"github.com/hushine-tech/control-panel-service/internal/runtimecert"
	portfoliov1 "github.com/hushine-tech/core-service/gen/portfoliov1"
	"google.golang.org/grpc"
)

// Sentinel errors returned by Service. The gRPC layer maps these to
// status codes; tests assert against them directly.
var (
	ErrInvalidArgument  = errors.New("invalid argument")
	ErrPermissionDenied = errors.New("permission denied")
	ErrNotFound         = errors.New("not found")
	ErrUnpaired         = errors.New("runtime unpaired")
	ErrUnhealthy        = errors.New("runtime unhealthy")
	ErrEnded            = errors.New("runtime ended")
	ErrTokenMismatch    = errors.New("token mismatch")
	ErrQuotaExceeded    = errors.New("quota exceeded")
	ErrConflict         = errors.New("conflict")
	// ErrPlanLookupUnavailable: core-service couldn't be reached (or
	// returned a non-NotFound error) when resolving the user's plan. Maps
	// to gRPC Unavailable so callers can retry after backoff.
	ErrPlanLookupUnavailable = errors.New("plan lookup unavailable")
	// ErrSessionLookupUnavailable: core-service couldn't be reached when
	// checking runtime-bound session blockers. Runtime end is fail-closed
	// when this dependency is unavailable.
	ErrSessionLookupUnavailable = errors.New("session lookup unavailable")
	// ErrProvisionerUnavailable: the configured provisioner backend
	// refused the request (no-op default, daemon down, image missing,
	// etc.). Maps to gRPC FailedPrecondition.
	ErrProvisionerUnavailable = errors.New("provisioner unavailable")
	// ErrRegistrationTimeout: a freshly-provisioned runtime did not open
	// RuntimeChannel within the configured deadline. Maps to gRPC
	// FailedPrecondition; the runtime container is likely broken.
	ErrRegistrationTimeout = errors.New("runtime did not open RuntimeChannel in time")
)

const runtimeNamePattern = `^[a-z0-9][a-z0-9-]{1,46}[a-z0-9]$`

var runtimeNameRe = regexp.MustCompile(runtimeNamePattern)

var hostedNameAdjectives = []string{
	"amber", "brisk", "calm", "clear", "fresh", "glad", "keen", "lucky",
	"nimble", "quiet", "rapid", "steady",
}

var hostedNameNouns = []string{
	"atlas", "beacon", "canvas", "delta", "ember", "forge", "harbor", "ion",
	"juno", "kepler", "ledger", "matrix",
}

func validRuntimeName(name string) bool {
	return runtimeNameRe.MatchString(name)
}

func generateHostedRuntimeName() string {
	return hostedRuntimeNameForAttempt(sRandInt(len(hostedNameAdjectives) * len(hostedNameNouns)))
}

func sRandInt(max int) int {
	if max <= 0 {
		return 0
	}
	return int(time.Now().UnixNano() % int64(max))
}

// Service ties the repository, plan resolver, and clock together so the
// handlers can be tested with stubs.
type Service struct {
	repo                        repository.Repository
	plans                       *plan.Resolver
	provisioner                 provision.Provisioner
	provisioning                config.ProvisioningConfig
	runtimePlatform             config.RuntimePlatformConfig
	sessionClient               sessionClient
	streamCloser                runtimeStreamCloser
	hostedCredentialIssuer      hostedCredentialIssuer
	runtimeCertSigner           runtimeCertSigner
	runtimeServerCAPEM          []byte
	runtimeChannelTLSServerName string
	notifications               cpnotify.Publisher
	heartbeatGrace              time.Duration
	deathGrace                  time.Duration
	bareRuntimeDeathGrace       time.Duration
	now                         func() time.Time
}

type sessionClient interface {
	ListRunningSessions(ctx context.Context, in *portfoliov1.ListRunningSessionsRequest, opts ...grpc.CallOption) (*portfoliov1.ListRunningSessionsResponse, error)
	MarkRuntimeSessionsRecoverable(ctx context.Context, in *portfoliov1.MarkRuntimeSessionsRecoverableRequest, opts ...grpc.CallOption) (*portfoliov1.MarkRuntimeSessionsRecoverableResponse, error)
}

type runtimeStreamCloser interface {
	CloseStreamForRuntime(ctx context.Context, runtimeID string) (bool, error)
}

type hostedCredentialIssuer interface {
	IssueHostedInternalRuntimeCredential(ctx context.Context, userID int64, runtimeID, name string) (domain.IssuedCredential, error)
}

type runtimeCertSigner interface {
	CAPEM() []byte
	SignRuntimeClientCertificate(runtimecert.SignRequest) ([]byte, *x509.Certificate, error)
}

// Config bundles the timeouts injected from config.RuntimePlatformConfig.
type Config struct {
	HeartbeatGrace time.Duration
	DeathGrace     time.Duration
	// BareRuntimeDeathGrace only applies to local/debug bare runtimes. When
	// unset it inherits DeathGrace so existing deployments keep the old
	// behavior.
	BareRuntimeDeathGrace time.Duration
	// Provisioning carries the operator-tunable provisioning settings:
	// container image, advertise host, port range, registration timeout,
	// and resource profiles. EnsureHostedRuntime reads it; other paths
	// don't.
	Provisioning config.ProvisioningConfig
	// Provisioner is the backend used by EnsureHostedRuntime. Nil falls
	// back to provision.NoOpProvisioner so service-layer logic stays
	// uniform whether the operator wired Docker or not.
	Provisioner provision.Provisioner
	// SessionClient checks runtime-bound active sessions before EndRuntime
	// and marks sessions recoverable when runtimes become terminal. EndRuntime
	// fails closed if this dependency is unavailable.
	SessionClient sessionClient
	// RuntimeStreamCloser is optional. When set, EndRuntime actively
	// closes the self-hosted RuntimeChannel stream for the ended runtime.
	RuntimeStreamCloser runtimeStreamCloser
	// HostedCredentialIssuer creates platform-internal credentials for hosted
	// runtimes so hosted containers can join the same credential-authenticated
	// RuntimeChannel path without exposing secret material to users.
	HostedCredentialIssuer hostedCredentialIssuer
	// NotificationPublisher publishes runtime/session lifecycle events to the
	// async notification stream. Nil falls back to a no-op publisher.
	NotificationPublisher cpnotify.Publisher
	// RuntimePlatform carries platform gates used outside plan resolution,
	// especially internal bare runtime certificate bootstrap.
	RuntimePlatform config.RuntimePlatformConfig
	// RuntimeCertSigner signs runtime client certificates for hosted and bare
	// bootstrap paths.
	RuntimeCertSigner runtimeCertSigner
	// RuntimeServerCAPEM is the CA/trust bundle runtime clients use to verify
	// the RuntimeChannel server TLS certificate.
	RuntimeServerCAPEM []byte
	// RuntimeChannelTLSServerName is the TLS authority runtime clients should
	// verify when the dial address is an IP literal or Docker host alias.
	RuntimeChannelTLSServerName string
}

func New(repo repository.Repository, plans *plan.Resolver, cfg Config) *Service {
	if cfg.HeartbeatGrace <= 0 {
		cfg.HeartbeatGrace = 30 * time.Second
	}
	if cfg.DeathGrace <= 0 {
		cfg.DeathGrace = 5 * time.Minute
	}
	if cfg.BareRuntimeDeathGrace <= 0 {
		cfg.BareRuntimeDeathGrace = cfg.DeathGrace
	}
	if cfg.Provisioner == nil {
		cfg.Provisioner = provision.NoOpProvisioner{}
	}
	if cfg.Provisioning.RegistrationTimeoutSeconds <= 0 {
		cfg.Provisioning.RegistrationTimeoutSeconds = 30
	}
	if cfg.NotificationPublisher == nil {
		cfg.NotificationPublisher = cpnotify.NoopPublisher{}
	}
	if len(cfg.RuntimePlatform.BareBootstrapIPAllowlist) == 0 {
		cfg.RuntimePlatform.BareBootstrapIPAllowlist = []string{"127.0.0.1/32"}
	}
	if cfg.RuntimePlatform.BareCertificateTTL <= 0 {
		cfg.RuntimePlatform.BareCertificateTTL = 8 * time.Hour
	}
	s := &Service{
		repo:                        repo,
		plans:                       plans,
		provisioner:                 cfg.Provisioner,
		provisioning:                cfg.Provisioning,
		runtimePlatform:             cfg.RuntimePlatform,
		sessionClient:               cfg.SessionClient,
		streamCloser:                cfg.RuntimeStreamCloser,
		hostedCredentialIssuer:      cfg.HostedCredentialIssuer,
		runtimeCertSigner:           cfg.RuntimeCertSigner,
		runtimeServerCAPEM:          append([]byte(nil), cfg.RuntimeServerCAPEM...),
		runtimeChannelTLSServerName: strings.TrimSpace(cfg.RuntimeChannelTLSServerName),
		notifications:               cfg.NotificationPublisher,
		heartbeatGrace:              cfg.HeartbeatGrace,
		deathGrace:                  cfg.DeathGrace,
		bareRuntimeDeathGrace:       cfg.BareRuntimeDeathGrace,
		now:                         time.Now,
	}
	return s
}

// SetClock lets tests replace the time source.
func (s *Service) SetClock(now func() time.Time) { s.now = now }

func (s *Service) publishRuntimeEvent(ctx context.Context, rt domain.Runtime, eventType, severity, message string) {
	if s == nil || s.notifications == nil || rt.UserID <= 0 {
		return
	}
	_ = s.notifications.Publish(ctx, cpnotify.Event{
		UserID:      rt.UserID,
		Category:    cpnotify.CategorySystem,
		EventType:   eventType,
		Severity:    severity,
		RuntimeID:   rt.RuntimeID,
		RuntimeName: rt.Name,
		Title:       runtimeEventTitle(eventType),
		Message:     message,
		DedupeKey:   fmt.Sprintf("%s:%s", eventType, rt.RuntimeID),
	})
}

func runtimeEventTitle(eventType string) string {
	switch eventType {
	case cpnotify.EventRuntimeStarted:
		return "Runtime started"
	case cpnotify.EventRuntimeRecovered:
		return "Runtime recovered"
	case cpnotify.EventRuntimeUnhealthy:
		return "Runtime unhealthy"
	case cpnotify.EventRuntimeEnded:
		return "Runtime ended"
	default:
		return "Runtime event"
	}
}

// resourceProfileAllowed returns true when the runtime's resource_profile
// is permitted by the user's plan. An empty allow-list is treated as
// "deny all" so an under-configured plan can't accidentally accept any
// profile (matches the strict-by-default convention used elsewhere).
func resourceProfileAllowed(profile string, allowed []string) bool {
	if profile == "" {
		return false
	}
	for _, a := range allowed {
		if a == profile {
			return true
		}
	}
	return false
}

// resolvePlan wraps `s.plans.Resolve` so the fail-closed contract is enforced
// uniformly. Errors are translated to service sentinels so the gRPC layer can
// map them to the right gRPC status code:
//
//	plan.ErrUserNotFound       → service.ErrNotFound       (NotFound)
//	plan.ErrPlanLookupUnavailable → service.ErrPlanLookupUnavailable (Unavailable)
//	any other error            → wrapped as Unavailable    (Unavailable)
func (s *Service) resolvePlan(ctx context.Context, userID int64) (plan.EffectiveLimits, error) {
	limits, err := s.plans.Resolve(ctx, userID)
	if err == nil {
		return limits, nil
	}
	if errors.Is(err, plan.ErrUserNotFound) {
		return plan.EffectiveLimits{}, fmt.Errorf("%w: %v", ErrNotFound, err)
	}
	// plan.ErrPlanLookupUnavailable AND any unknown error → Unavailable.
	return plan.EffectiveLimits{}, fmt.Errorf("%w: %v", ErrPlanLookupUnavailable, err)
}

// ── List ────────────────────────────────────────────────────────────────────

type ListArgs struct {
	UserID       int64
	StatusFilter string
	SourceFilter string
	Limit        int
	Offset       int
}

type ListResult struct {
	Runtimes []domain.Runtime
	Total    int64
	HasMore  bool
}

func (s *Service) ListRuntimes(ctx context.Context, args ListArgs) (ListResult, error) {
	if args.UserID <= 0 {
		return ListResult{}, fmt.Errorf("%w: user_id is required", ErrInvalidArgument)
	}
	items, total, hasMore, err := s.repo.ListRuntimes(ctx, args.UserID, args.StatusFilter, args.SourceFilter, args.Limit, args.Offset)
	if err != nil {
		return ListResult{}, err
	}
	return ListResult{Runtimes: items, Total: total, HasMore: hasMore}, nil
}

type ListAdmissionFailuresArgs struct {
	UserID int64
	Limit  int
}

func (s *Service) ListRuntimeAdmissionFailures(ctx context.Context, args ListAdmissionFailuresArgs) ([]domain.RuntimeAdmissionFailure, error) {
	if args.UserID <= 0 {
		return nil, fmt.Errorf("%w: user_id is required", ErrInvalidArgument)
	}
	return s.repo.ListRuntimeAdmissionFailuresByUser(ctx, args.UserID, args.Limit)
}

type GetRuntimeArgs struct {
	UserID    int64
	RuntimeID string
}

func (s *Service) GetRuntime(ctx context.Context, args GetRuntimeArgs) (domain.Runtime, error) {
	if args.UserID <= 0 {
		return domain.Runtime{}, fmt.Errorf("%w: user_id is required", ErrInvalidArgument)
	}
	if args.RuntimeID == "" {
		return domain.Runtime{}, fmt.Errorf("%w: runtime_id is required", ErrInvalidArgument)
	}
	rt, err := s.repo.GetRuntime(ctx, args.RuntimeID)
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			return domain.Runtime{}, ErrNotFound
		}
		return domain.Runtime{}, err
	}
	if rt.UserID != args.UserID {
		return domain.Runtime{}, ErrNotFound
	}
	return rt, nil
}

type BootstrapBareRuntimeCertificateArgs struct {
	UserID          int64
	RuntimeID       string
	Name            string
	CSRPEM          string
	RemoteIP        string
	Capabilities    []string
	ResourceProfile string
	Version         string
}

type BootstrapBareRuntimeCertificateResult struct {
	RuntimeID           string
	Name                string
	ClientCertPEM       string
	ServerCAPEM         string
	ClientCertExpiresAt time.Time
}

func (s *Service) BootstrapBareRuntimeCertificate(ctx context.Context, args BootstrapBareRuntimeCertificateArgs) (BootstrapBareRuntimeCertificateResult, error) {
	if !s.runtimePlatform.DebugBareRuntimeEnabled {
		return BootstrapBareRuntimeCertificateResult{}, fmt.Errorf("%w: bare runtime debug gate disabled", ErrPermissionDenied)
	}
	if !ipAllowed(args.RemoteIP, s.runtimePlatform.BareBootstrapIPAllowlist) {
		return BootstrapBareRuntimeCertificateResult{}, fmt.Errorf("%w: remote IP is not allowlisted for bare runtime bootstrap", ErrPermissionDenied)
	}
	args.RuntimeID = strings.TrimSpace(args.RuntimeID)
	args.Name = strings.TrimSpace(args.Name)
	if args.UserID <= 0 || args.RuntimeID == "" {
		return BootstrapBareRuntimeCertificateResult{}, fmt.Errorf("%w: user_id and runtime_id are required", ErrInvalidArgument)
	}
	if args.Name == "" {
		args.Name = "bare-" + args.RuntimeID
	}
	if !validRuntimeName(args.Name) {
		return BootstrapBareRuntimeCertificateResult{}, fmt.Errorf("%w: name must match %s", ErrInvalidArgument, runtimeNamePattern)
	}
	if s.runtimeCertSigner == nil {
		return BootstrapBareRuntimeCertificateResult{}, fmt.Errorf("%w: runtime certificate signer is not configured", ErrProvisionerUnavailable)
	}
	if len(s.runtimeServerCAPEM) == 0 {
		return BootstrapBareRuntimeCertificateResult{}, fmt.Errorf("%w: runtime server ca is not configured", ErrProvisionerUnavailable)
	}
	now := s.now().UTC()
	certPEM, cert, err := s.runtimeCertSigner.SignRuntimeClientCertificate(runtimecert.SignRequest{
		CSRPEM:    []byte(args.CSRPEM),
		RuntimeID: args.RuntimeID,
		UserID:    args.UserID,
		Source:    domain.RuntimeSourceBare,
		Role:      string(domain.CredentialRoleExecutor),
		Name:      args.Name,
		TTL:       s.runtimePlatform.BareCertificateTTL,
		Now:       now,
	})
	if err != nil {
		return BootstrapBareRuntimeCertificateResult{}, fmt.Errorf("%w: sign bare runtime certificate: %v", ErrInvalidArgument, err)
	}
	expiresAt := cert.NotAfter
	downloadedAt := now
	cred := domain.RuntimeCredential{
		KeyID:                 "bare-" + args.RuntimeID,
		UserID:                args.UserID,
		Label:                 "bare:" + args.Name,
		Role:                  domain.CredentialRoleExecutor,
		Status:                domain.CredentialStatusDownloaded,
		CreatedAt:             now,
		DownloadedAt:          &downloadedAt,
		ExpiresAt:             &expiresAt,
		ClientCertPEM:         string(certPEM),
		ClientCertFingerprint: runtimecert.Fingerprint(cert),
		ClientCertExpiresAt:   &expiresAt,
		Issuer:                domain.RuntimeCredentialIssuerBareDebug,
	}
	if err := s.repo.CreateRuntimeCredential(ctx, cred); err != nil {
		if errors.Is(err, repository.ErrConflict) {
			return BootstrapBareRuntimeCertificateResult{}, fmt.Errorf("%w: bare runtime credential already exists for runtime_id", ErrConflict)
		}
		return BootstrapBareRuntimeCertificateResult{}, fmt.Errorf("persist bare runtime credential: %w", err)
	}
	return BootstrapBareRuntimeCertificateResult{
		RuntimeID:           args.RuntimeID,
		Name:                args.Name,
		ClientCertPEM:       string(certPEM),
		ServerCAPEM:         string(s.runtimeServerCAPEM),
		ClientCertExpiresAt: expiresAt,
	}, nil
}

func ipAllowed(ipText string, cidrs []string) bool {
	ipText = strings.TrimSpace(ipText)
	ip := net.ParseIP(ipText)
	if ip == nil {
		if host, _, err := net.SplitHostPort(ipText); err == nil {
			ip = net.ParseIP(host)
		}
	}
	if ip == nil {
		return false
	}
	for _, c := range cidrs {
		c = strings.TrimSpace(c)
		if c == "" {
			continue
		}
		if _, network, err := net.ParseCIDR(c); err == nil {
			if network.Contains(ip) {
				return true
			}
			continue
		}
		if allowedIP := net.ParseIP(c); allowedIP != nil && allowedIP.Equal(ip) {
			return true
		}
	}
	return false
}

// ── End ────────────────────────────────────────────────────────────────────

type EndRuntimeArgs struct {
	UserID    int64
	RuntimeID string
	// Force is reserved for a future admin-only force-end path. No public
	// RPC sets it today; ordinary user flows are always guarded by active
	// session blockers.
	Force *ForceEndRequest
}

// ForceEndRequest is the internal shape reserved for a future admin path.
// The current service rejects it because no admin authority/RBAC surface is
// implemented yet.
type ForceEndRequest struct {
	ActorUserID int64
	Authority   string
	Reason      string
}

type ForceEndAudit struct {
	RuntimeID          string
	ActorUserID        int64
	Authority          string
	Reason             string
	AffectedSessionIDs []string
}

func (r ForceEndRequest) Audit(runtimeID string, sessions []*portfoliov1.StrategySessionEntry) ForceEndAudit {
	ids := make([]string, 0, len(sessions))
	for _, sess := range sessions {
		if sess == nil || sess.GetSessionId() == "" {
			continue
		}
		ids = append(ids, sess.GetSessionId())
	}
	return ForceEndAudit{
		RuntimeID:          runtimeID,
		ActorUserID:        r.ActorUserID,
		Authority:          r.Authority,
		Reason:             r.Reason,
		AffectedSessionIDs: ids,
	}
}

func (s *Service) EndRuntime(ctx context.Context, args EndRuntimeArgs) (domain.Runtime, error) {
	rt, err := s.GetRuntime(ctx, GetRuntimeArgs{UserID: args.UserID, RuntimeID: args.RuntimeID})
	if err != nil {
		return domain.Runtime{}, err
	}

	if domain.IsRuntimeTerminalStatus(rt.Status) {
		return rt, nil
	}

	blockers, err := s.listRuntimeEndBlockers(ctx, rt.RuntimeID)
	if err != nil {
		return domain.Runtime{}, err
	}
	if args.Force != nil {
		_ = args.Force.Audit(rt.RuntimeID, blockers)
		return domain.Runtime{}, fmt.Errorf("%w: force runtime end is reserved for future admin authority", ErrPermissionDenied)
	}
	if len(blockers) > 0 {
		return domain.Runtime{}, fmt.Errorf("%w: runtime %s has active session blocker %s", ErrConflict, rt.RuntimeID, describeSessionBlocker(blockers[0]))
	}

	ended, err := s.repo.EndRuntime(ctx, rt.RuntimeID, domain.RuntimeEndedReasonUserCancelled, s.now().UTC())
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			return domain.Runtime{}, ErrNotFound
		}
		return domain.Runtime{}, err
	}
	rt = ended

	var cleanup runtimeCleanupResult
	if rt.Source == domain.RuntimeSourceHosted {
		cleanup = s.deprovisionHostedRuntime(rt.RuntimeID)
	} else if rt.Source == domain.RuntimeSourceSelfHosted && s.streamCloser != nil {
		// Self-hosted containers are user-owned, so control-panel cannot
		// remove the Docker container. It can and should close the active
		// RuntimeChannel stream so the runtime becomes unroutable
		// immediately instead of waiting for heartbeat timeout.
		_, _ = s.streamCloser.CloseStreamForRuntime(ctx, rt.RuntimeID)
		cleanup = s.recordSelfHostedCleanupGuidance(rt.RuntimeID)
	} else if rt.Source == domain.RuntimeSourceSelfHosted {
		cleanup = s.recordSelfHostedCleanupGuidance(rt.RuntimeID)
	}
	if cleanup.Status != "" {
		rt.CleanupStatus = cleanup.Status
		rt.CleanupReason = cleanup.Reason
		rt.CleanupAt = &cleanup.At
	}

	s.markRuntimeSessionsRecoverable(ctx, rt.RuntimeID, fmt.Sprintf("runtime %s was ended by control-panel", rt.RuntimeID))
	s.publishRuntimeEvent(ctx, rt, cpnotify.EventRuntimeEnded, cpnotify.SeverityInfo, fmt.Sprintf("Runtime %s ended: %s.", rt.Name, rt.EndedReason))
	return rt, nil
}

type runtimeCleanupResult struct {
	Status string
	Reason string
	At     time.Time
}

type runtimeDiagnosticsProvider interface {
	Diagnostics(ctx context.Context, handle string) (string, error)
}

func (s *Service) deprovisionHostedRuntime(runtimeID string) runtimeCleanupResult {
	return s.deprovisionHostedRuntimeHandle(runtimeID, hostedRuntimeHandle(runtimeID))
}

func (s *Service) deprovisionHostedRuntimeHandle(runtimeID, handle string) runtimeCleanupResult {
	// Hosted runtime handles are deterministic container names. Cleanup is
	// best-effort so a missing local Docker container does not make the
	// already-ended registry row routeable again.
	cleanupTimeout := 10 * time.Second
	if provider, ok := s.provisioner.(provision.CleanupTimeoutProvider); ok {
		if declared := provider.DeprovisionTimeout(); declared > cleanupTimeout {
			cleanupTimeout = declared
		}
	}
	cleanupCtx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
	defer cancel()
	status := domain.RuntimeCleanupStatusSucceeded
	reason := ""
	if err := s.provisioner.Deprovision(cleanupCtx, handle); err != nil {
		status = domain.RuntimeCleanupStatusFailed
		reason = err.Error()
	}
	at := s.now().UTC()
	s.recordRuntimeCleanupState(runtimeID, status, reason, at)
	return runtimeCleanupResult{Status: status, Reason: reason, At: at}
}

func (s *Service) recordSelfHostedCleanupGuidance(runtimeID string) runtimeCleanupResult {
	at := s.now().UTC()
	reason := "self-hosted container is user-owned; stop/remove it on the Docker host if it did not exit"
	s.recordRuntimeCleanupState(runtimeID, domain.RuntimeCleanupStatusUserOwned, reason, at)
	return runtimeCleanupResult{Status: domain.RuntimeCleanupStatusUserOwned, Reason: reason, At: at}
}

func (s *Service) recordRuntimeCleanupState(runtimeID, status, reason string, at time.Time) {
	if s.repo == nil || runtimeID == "" || status == "" {
		return
	}
	reason = strings.TrimSpace(reason)
	if len(reason) > 1000 {
		reason = reason[:1000]
	}
	if err := s.repo.UpdateRuntimeCleanupState(context.Background(), runtimeID, status, reason, at); err != nil && !errors.Is(err, repository.ErrNotFound) {
		// Cleanup state is diagnostic. Do not make an already-terminal
		// runtime routable again because diagnostics failed to persist.
		return
	}
}

func (s *Service) listRuntimeEndBlockers(ctx context.Context, runtimeID string) ([]*portfoliov1.StrategySessionEntry, error) {
	if runtimeID == "" {
		return nil, nil
	}
	if s.sessionClient == nil {
		return nil, fmt.Errorf("%w: core-service session client is not configured", ErrSessionLookupUnavailable)
	}
	resp, err := s.sessionClient.ListRunningSessions(ctx, &portfoliov1.ListRunningSessionsRequest{RuntimeId: runtimeID})
	if err != nil {
		return nil, fmt.Errorf("%w: list runtime sessions for %s: %v", ErrSessionLookupUnavailable, runtimeID, err)
	}
	if resp == nil {
		return nil, nil
	}
	return resp.GetSessions(), nil
}

func describeSessionBlocker(sess *portfoliov1.StrategySessionEntry) string {
	if sess == nil {
		return "<unknown>"
	}
	if sess.GetStatus() == "" {
		return sess.GetSessionId()
	}
	return fmt.Sprintf("%s(status=%s)", sess.GetSessionId(), sess.GetStatus())
}

func hostedRuntimeHandle(runtimeID string) string {
	return fmt.Sprintf("hushine-runtime-%s", runtimeID)
}

// ReapStaleRuntimes marks active/paired runtimes whose heartbeat is older
// than HeartbeatGrace as unhealthy, then terminally ends runtimes older than
// DeathGrace. Sessions are marked recoverable only for runtimes that became
// ended; unhealthy alone is still an observable degraded state.
func (s *Service) ReapStaleRuntimes(ctx context.Context) ([]domain.Runtime, error) {
	now := s.now().UTC()
	staleCutoff := now.Add(-s.heartbeatGrace)
	stale, err := s.repo.MarkStaleRuntimesUnhealthy(ctx, staleCutoff)
	if err != nil {
		return nil, err
	}
	for _, rt := range stale {
		s.publishRuntimeEvent(ctx, rt, cpnotify.EventRuntimeUnhealthy, cpnotify.SeverityWarn, fmt.Sprintf("Runtime %s missed heartbeat.", rt.Name))
	}
	deadCutoff := now.Add(-s.deathGrace)
	bareDeadCutoff := now.Add(-s.bareRuntimeDeathGrace)
	ended, err := s.repo.EndDeadRuntimesBySourceCutoffs(ctx, deadCutoff, bareDeadCutoff, domain.RuntimeEndedReasonHeartbeatStale, now)
	if err != nil {
		return nil, err
	}
	for _, rt := range ended {
		if rt.Source == domain.RuntimeSourceHosted {
			s.deprovisionHostedRuntime(rt.RuntimeID)
		} else if rt.Source == domain.RuntimeSourceSelfHosted {
			s.recordSelfHostedCleanupGuidance(rt.RuntimeID)
		}
		s.markRuntimeSessionsRecoverable(ctx, rt.RuntimeID, fmt.Sprintf("runtime %s heartbeat stale; session marked recoverable by control-panel watchdog", rt.RuntimeID))
		s.publishRuntimeEvent(ctx, rt, cpnotify.EventRuntimeEnded, cpnotify.SeverityError, fmt.Sprintf("Runtime %s ended: %s.", rt.Name, rt.EndedReason))
	}
	return append(stale, ended...), nil
}

func (s *Service) markRuntimeSessionsRecoverable(ctx context.Context, runtimeID, errMsg string) {
	if s.sessionClient == nil || runtimeID == "" {
		return
	}
	_, _ = s.sessionClient.MarkRuntimeSessionsRecoverable(ctx, &portfoliov1.MarkRuntimeSessionsRecoverableRequest{
		RuntimeId: runtimeID,
		Error:     errMsg,
	})
}

// ── Resolve ─────────────────────────────────────────────────────────────────

type ResolveByIDArgs struct {
	UserID    int64
	RuntimeID string
	// Role is the intended session role. Empty means executor for backward
	// compatibility with existing strategy launch paths.
	Role string
	// Environment is the requested portfolio/session environment when the caller has it.
	// Debugger routes require backtest; executor routes support backtest/demo.
	Environment int
}

type ResolveResult struct {
	Runtime domain.Runtime
}

func (s *Service) ResolveRuntimeRouteByID(ctx context.Context, args ResolveByIDArgs) (ResolveResult, error) {
	rt, err := s.GetRuntime(ctx, GetRuntimeArgs{UserID: args.UserID, RuntimeID: args.RuntimeID})
	if err != nil {
		return ResolveResult{}, err
	}
	return s.resolveRuntimeRouteForRuntimeWithPolicy(ctx, args.UserID, rt, args.Role, args.Environment)
}

func (s *Service) resolveRuntimeRouteForRuntime(ctx context.Context, userID int64, rt domain.Runtime) (ResolveResult, error) {
	return s.resolveRuntimeRouteForRuntimeWithPolicy(ctx, userID, rt, string(domain.CredentialRoleExecutor), 0)
}

func (s *Service) resolveRuntimeRouteForRuntimeWithPolicy(ctx context.Context, userID int64, rt domain.Runtime, role string, environment int) (ResolveResult, error) {
	switch rt.Status {
	case domain.RuntimeStatusHeartbeatStale, domain.RuntimeStatusEnded, domain.RuntimeStatusCancelled, domain.RuntimeStatusFailed:
		s.markRuntimeSessionsRecoverable(ctx, rt.RuntimeID, fmt.Sprintf("runtime %s is terminal (%s); session marked recoverable during route resolution", rt.RuntimeID, rt.Status))
		return ResolveResult{}, ErrEnded
	case "unpaired":
		return ResolveResult{}, ErrUnpaired
	}
	role = strings.ToLower(strings.TrimSpace(role))
	if role == "" {
		role = string(domain.CredentialRoleExecutor)
	}
	switch domain.CredentialRole(role) {
	case domain.CredentialRoleExecutor:
		if rt.Role != "" && rt.Role != domain.CredentialRoleExecutor {
			return ResolveResult{}, fmt.Errorf("%w: runtime role %q is not eligible for executor sessions", ErrPermissionDenied, rt.Role)
		}
		if environment != 0 && environment != 1 {
			return ResolveResult{}, fmt.Errorf("%w: executor runtime environment %d is not supported", ErrPermissionDenied, environment)
		}
	case domain.CredentialRoleDebugger:
		if rt.Role != domain.CredentialRoleDebugger {
			return ResolveResult{}, fmt.Errorf("%w: runtime role %q is not eligible for debugger sessions", ErrPermissionDenied, rt.Role)
		}
		if environment != 0 {
			return ResolveResult{}, fmt.Errorf("%w: debugger runtime only supports backtest environment", ErrPermissionDenied)
		}
		blockers, err := s.listRuntimeEndBlockers(ctx, rt.RuntimeID)
		if err != nil {
			return ResolveResult{}, err
		}
		if len(blockers) > 0 {
			return ResolveResult{}, fmt.Errorf("%w: debugger runtime already has active session %s", ErrConflict, describeSessionBlocker(blockers[0]))
		}
	default:
		return ResolveResult{}, fmt.Errorf("%w: unsupported runtime route role %q", ErrInvalidArgument, role)
	}

	// Heartbeat freshness check: stale heartbeat = unhealthy.
	if rt.Status == domain.RuntimeStatusUnhealthy {
		return ResolveResult{}, ErrUnhealthy
	}
	if rt.HeartbeatAt == nil {
		return ResolveResult{}, ErrUnhealthy
	}
	now := s.now().UTC()
	if now.Sub(*rt.HeartbeatAt) > s.heartbeatGrace {
		return ResolveResult{}, ErrUnhealthy
	}
	if rt.ConnectionOwnerInstanceID == "" || rt.ConnectionOwnerHeartbeatAt == nil {
		return ResolveResult{}, ErrUnhealthy
	}
	if now.Sub(*rt.ConnectionOwnerHeartbeatAt) > s.heartbeatGrace {
		return ResolveResult{}, ErrUnhealthy
	}

	// Quota guard at resolve time: counts of non-ended runtimes vs plan.
	limits, err := s.resolvePlan(ctx, userID)
	if err != nil {
		return ResolveResult{}, err
	}
	counts, err := s.repo.CountRuntimesByUser(ctx, userID)
	if err != nil {
		return ResolveResult{}, err
	}
	// 0 = hard cap (forbidden); -1 = unlimited (skip check); >0 = real cap.
	if limits.MaxHostedRuntimes == 0 && counts.Hosted > 0 {
		return ResolveResult{}, fmt.Errorf("%w: plan %q forbids hosted runtimes (user has %d)", ErrQuotaExceeded, limits.PlanCode, counts.Hosted)
	}
	if limits.MaxHostedRuntimes > 0 && counts.Hosted > int64(limits.MaxHostedRuntimes) {
		return ResolveResult{}, fmt.Errorf("%w: plan %q caps hosted runtimes at %d, user has %d", ErrQuotaExceeded, limits.PlanCode, limits.MaxHostedRuntimes, counts.Hosted)
	}
	if limits.MaxSelfHostedRuntimes == 0 && counts.SelfHosted > 0 {
		return ResolveResult{}, fmt.Errorf("%w: plan %q forbids self_hosted runtimes (user has %d)", ErrQuotaExceeded, limits.PlanCode, counts.SelfHosted)
	}
	if limits.MaxSelfHostedRuntimes > 0 && counts.SelfHosted > int64(limits.MaxSelfHostedRuntimes) {
		return ResolveResult{}, fmt.Errorf("%w: plan %q caps self_hosted runtimes at %d, user has %d", ErrQuotaExceeded, limits.PlanCode, limits.MaxSelfHostedRuntimes, counts.SelfHosted)
	}

	return ResolveResult{Runtime: rt}, nil
}

// ── EnsureHostedRuntime ─────────────────────────────────────────────────────

// EnsureHostedRuntimeArgs is the input to EnsureHostedRuntime.
type EnsureHostedRuntimeArgs struct {
	UserID          int64
	Name            string
	ResourceProfile string
}

// EnsureHostedRuntimeResult: the route + provenance flag.
type EnsureHostedRuntimeResult struct {
	Runtime domain.Runtime
	// Provisioned is true when a fresh container was started by this
	// call, false when an existing healthy runtime was returned.
	Provisioned bool
}

// EnsureHostedRuntime is the lazy-creation entry point handler uses on
// strategy start. See proto comment for the contract.
//
// Order of checks:
//  1. user_id required; manual name must match the runtime-name contract
//  2. fast path for manual name: existing runtime is healthy → return it
//  3. plan / quota / profile checks fail-closed
//  4. allocate runtime_id and hosted RuntimeChannel credential
//  5. call provisioner.Provision
//  6. wait for runtime to connect through RuntimeChannel (poll repo)
//  7. return runtime
//
// The wait in step 6 polls `s.repo.GetRuntime` until the row exists with
// status='paired' or 'active'. A registration timeout deprovisions the
// container and surfaces ErrRegistrationTimeout.
func (s *Service) EnsureHostedRuntime(ctx context.Context, args EnsureHostedRuntimeArgs) (EnsureHostedRuntimeResult, error) {
	if args.UserID <= 0 {
		return EnsureHostedRuntimeResult{}, fmt.Errorf("%w: user_id is required", ErrInvalidArgument)
	}
	name := args.Name
	autoName := name == ""
	if !autoName && !validRuntimeName(name) {
		return EnsureHostedRuntimeResult{}, fmt.Errorf("%w: name must match %s", ErrInvalidArgument, runtimeNamePattern)
	}
	profileName := args.ResourceProfile
	if profileName == "" {
		profileName = "small"
	}

	if !autoName {
		// Fast path: explicit display names can reuse an existing healthy
		// hosted runtime. Any historical row with the same display name
		// blocks fresh allocation so names stay unambiguous for audit.
		rt, ok, err := s.findRuntimeByUserNameSource(ctx, args.UserID, name, "", true)
		if err != nil {
			return EnsureHostedRuntimeResult{}, fmt.Errorf("lookup hosted runtime: %w", err)
		}
		if ok {
			if rt.Source == domain.RuntimeSourceHosted {
				if existing, ok := s.tryReuseExisting(rt); ok {
					return existing, nil
				}
			}
			if rt.Source == domain.RuntimeSourceHosted {
				// Row exists but is not routeable. It still occupies the hosted
				// display name; the user/operator must end it explicitly before
				// a new hosted runtime can be created with that same name.
				return EnsureHostedRuntimeResult{}, fmt.Errorf("%w: hosted runtime name occupied by %s; end it before starting a replacement", ErrConflict, rt.RuntimeID)
			}
			return EnsureHostedRuntimeResult{}, fmt.Errorf("%w: runtime name occupied by %s", ErrConflict, rt.RuntimeID)
		}
	}
	var hostedSlot domain.Runtime
	hostedSlotFound := false
	if autoName {
		rt, ok, err := s.findHostedRuntimeSlot(ctx, args.UserID)
		if err != nil {
			return EnsureHostedRuntimeResult{}, fmt.Errorf("lookup hosted runtime slot: %w", err)
		}
		if ok {
			if existing, ok := s.tryReuseExisting(rt); ok {
				return existing, nil
			}
			hostedSlot = rt
			hostedSlotFound = true
		}
	}

	// Slow path: plan + quota + profile checks → provision.
	limits, err := s.resolvePlan(ctx, args.UserID)
	if err != nil {
		return EnsureHostedRuntimeResult{}, err
	}
	counts, err := s.repo.CountRuntimesByUser(ctx, args.UserID)
	if err != nil {
		return EnsureHostedRuntimeResult{}, err
	}
	if limits.MaxHostedRuntimes == 0 {
		return EnsureHostedRuntimeResult{}, fmt.Errorf("%w: plan %q forbids hosted runtimes", ErrQuotaExceeded, limits.PlanCode)
	}
	if limits.MaxHostedRuntimes > 0 && counts.Hosted >= int64(limits.MaxHostedRuntimes) {
		return EnsureHostedRuntimeResult{}, fmt.Errorf("%w: plan %q caps hosted runtimes at %d", ErrQuotaExceeded, limits.PlanCode, limits.MaxHostedRuntimes)
	}
	if !resourceProfileAllowed(profileName, limits.AllowedResourceProfiles) {
		return EnsureHostedRuntimeResult{}, fmt.Errorf("%w: plan %q does not allow resource_profile %q", ErrQuotaExceeded, limits.PlanCode, profileName)
	}
	profileLimits, ok := s.provisioning.Profiles[profileName]
	if !ok {
		return EnsureHostedRuntimeResult{}, fmt.Errorf("%w: resource_profile %q not defined in provisioning.profiles", ErrInvalidArgument, profileName)
	}
	if autoName {
		if hostedSlotFound {
			return EnsureHostedRuntimeResult{}, fmt.Errorf("%w: hosted runtime slot occupied by %s; end it before starting a replacement", ErrConflict, hostedSlot.RuntimeID)
		}
	} else if rt, ok, err := s.findHostedRuntimeSlot(ctx, args.UserID); err != nil {
		return EnsureHostedRuntimeResult{}, fmt.Errorf("lookup hosted runtime slot: %w", err)
	} else if ok {
		return EnsureHostedRuntimeResult{}, fmt.Errorf("%w: hosted runtime slot occupied by %s; end it before starting a replacement", ErrConflict, rt.RuntimeID)
	}

	if autoName {
		generated, err := s.generateAvailableHostedRuntimeName(ctx, args.UserID)
		if err != nil {
			return EnsureHostedRuntimeResult{}, err
		}
		name = generated
	}

	runtimeID := auth.GenerateRuntimeID()
	port := s.allocatePort(args.UserID)
	var hostedCredential domain.IssuedCredential
	if s.hostedCredentialIssuer != nil {
		hostedCredential, err = s.hostedCredentialIssuer.IssueHostedInternalRuntimeCredential(ctx, args.UserID, runtimeID, name)
		if err != nil {
			return EnsureHostedRuntimeResult{}, fmt.Errorf("%w: issue hosted runtime credential: %v", ErrProvisionerUnavailable, err)
		}
	}
	plan := provision.Plan{
		RuntimeID:           runtimeID,
		UserID:              args.UserID,
		Name:                name,
		EndpointHost:        s.provisioning.AdvertiseHost,
		GRPCPort:            port,
		Image:               s.provisioning.Image,
		Limits:              profileLimits,
		ResourceProfileName: profileName,
		Capabilities:        []string{"strategy", "spot", "futures"},
	}
	if hostedCredential.KeyID != "" {
		plan.RuntimeCredentialKeyID = hostedCredential.KeyID
		plan.RuntimeCredentialPrivateKeyPEM = hostedCredential.PrivateKeyPEM
		plan.RuntimeClientCertPEM = hostedCredential.ClientCertPEM
		plan.RuntimeClientKeyPEM = hostedCredential.ClientKeyPEM
		plan.RuntimeServerCAPEM = hostedCredential.ServerCAPEM
		plan.RuntimeChannelTLSServerName = s.runtimeChannelTLSServerName
	}

	handle, err := s.provisioner.Provision(ctx, plan)
	if err != nil {
		if errors.Is(err, provision.ErrNotConfigured) {
			return EnsureHostedRuntimeResult{}, fmt.Errorf("%w: %v", ErrProvisionerUnavailable, err)
		}
		return EnsureHostedRuntimeResult{}, fmt.Errorf("%w: %v", ErrProvisionerUnavailable, err)
	}

	// Wait for the runtime to complete RuntimeChannel HELLO and land a row.
	timeout := time.Duration(s.provisioning.RegistrationTimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	rt, err := s.waitForRegistration(ctx, runtimeID, timeout)
	if err != nil {
		// Best-effort cleanup. The result is persisted when the runtime row
		// exists, so hosted deprovision failures are visible in Runtime
		// Management instead of being reduced to a one-off startup error.
		if structuredErr, recorded := s.recordHostedRuntimeStartupFailure(err, handle, plan); recorded {
			err = structuredErr
		} else {
			err = s.withRuntimeStartupDiagnostics(err, handle)
		}
		s.deprovisionHostedRuntimeHandle(runtimeID, handle)
		return EnsureHostedRuntimeResult{}, err
	}

	return EnsureHostedRuntimeResult{
		Runtime:     rt,
		Provisioned: true,
	}, nil
}

// tryReuseExisting decides whether an existing runtime row is good enough
// to return without re-provisioning. Returns (result, true) on reuse,
// (zero, false) on "needs fresh provisioning".
//
// Freshness rule: prefer `heartbeat_at`, but for runtimes that just
// registered and haven't sent their first heartbeat yet, fall back to
// `updated_at`. Without this fallback, two `EnsureHostedRuntime` calls
// within the heartbeat-grace window would race: the first provisions a
// fresh container; the second sees `paired` + nil heartbeat, decides
// "stale", and re-provisions, cancelling the first runtime that just
// came up. The fallback covers that initial sub-grace window.
func (s *Service) tryReuseExisting(rt domain.Runtime) (EnsureHostedRuntimeResult, bool) {
	switch rt.Status {
	case domain.RuntimeStatusHeartbeatStale, domain.RuntimeStatusEnded, domain.RuntimeStatusCancelled, domain.RuntimeStatusFailed, "unpaired", domain.RuntimeStatusUnhealthy:
		return EnsureHostedRuntimeResult{}, false
	}
	now := s.now().UTC()
	lastSeen := rt.UpdatedAt
	if rt.HeartbeatAt != nil {
		lastSeen = *rt.HeartbeatAt
	}
	if now.Sub(lastSeen) > s.heartbeatGrace {
		return EnsureHostedRuntimeResult{}, false
	}
	if rt.ConnectionOwnerInstanceID == "" || rt.ConnectionOwnerHeartbeatAt == nil {
		return EnsureHostedRuntimeResult{}, false
	}
	if now.Sub(*rt.ConnectionOwnerHeartbeatAt) > s.heartbeatGrace {
		return EnsureHostedRuntimeResult{}, false
	}
	return EnsureHostedRuntimeResult{
		Runtime:     rt,
		Provisioned: false,
	}, true
}

// allocatePort picks a deterministic port from the configured pool so a
// given (user_id, name) lands at a stable host:port. D1 is
// single-host so collision avoidance via a pool index is enough; D2/D3
// will replace this with a real allocator when multi-host comes in.
func (s *Service) allocatePort(userID int64) int {
	base := s.provisioning.PortRangeBase
	size := s.provisioning.PortRangeSize
	if base <= 0 {
		base = 50100
	}
	if size <= 0 {
		size = 200
	}
	return base + int(userID)%size
}

// waitForRegistration polls the repository for the runtime row to reach
// the `active` state and to have a RuntimeChannel owner. Hosted runtime
// session traffic is proxy-only now, so heartbeat alone is not enough:
// the control-plane must know which control-panel instance owns the stream.
//
// Returns the latest row on success; ErrRegistrationTimeout when the
// deadline elapses.
func (s *Service) waitForRegistration(ctx context.Context, runtimeID string, timeout time.Duration) (domain.Runtime, error) {
	deadline := time.Now().Add(timeout)
	const pollInterval = 200 * time.Millisecond
	for {
		rt, err := s.repo.GetRuntime(ctx, runtimeID)
		if err == nil {
			if rt.Status == domain.RuntimeStatusActive && rt.ConnectionOwnerInstanceID != "" {
				return rt, nil
			}
		} else if !errors.Is(err, repository.ErrNotFound) {
			return domain.Runtime{}, err
		}
		if !time.Now().Before(deadline) {
			return domain.Runtime{}, fmt.Errorf("%w: runtime_id=%s waited %s", ErrRegistrationTimeout, runtimeID, timeout)
		}
		select {
		case <-ctx.Done():
			return domain.Runtime{}, ctx.Err()
		case <-time.After(pollInterval):
		}
	}
}

func (s *Service) withRuntimeStartupDiagnostics(err error, handle string) error {
	if err == nil || handle == "" || s.provisioner == nil {
		return err
	}
	diagProvider, ok := s.provisioner.(runtimeDiagnosticsProvider)
	if !ok {
		return err
	}
	diagCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	diag, diagErr := diagProvider.Diagnostics(diagCtx, handle)
	if diagErr != nil {
		if text := strings.TrimSpace(diagErr.Error()); text != "" {
			return fmt.Errorf("%w; runtime diagnostics failed: %s", err, text)
		}
		return err
	}
	diag = strings.TrimSpace(diag)
	if diag == "" {
		return err
	}
	return fmt.Errorf("%w; runtime diagnostics: %s", err, diag)
}

func (s *Service) recordHostedRuntimeStartupFailure(original error, handle string, plan provision.Plan) (error, bool) {
	if !errors.Is(original, ErrRegistrationTimeout) || handle == "" || s.provisioner == nil {
		return original, false
	}
	provider, ok := s.provisioner.(provision.StartupFailureProvider)
	if !ok {
		return original, false
	}
	lookupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	failure, found, err := provider.StartupFailure(lookupCtx, handle)
	if err != nil || !found || !validHostedRuntimeStartupFailure(failure) {
		return original, false
	}

	reason := fmt.Sprintf(
		"module=%q profile=%q version=%q image_build_id=%q reason=%q",
		failure.Module,
		failure.ProfileName,
		failure.ProfileVersion,
		failure.ImageBuildID,
		failure.Reason,
	)
	now := s.now().UTC()
	recordCtx, cancelRecord := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelRecord()
	recordErr := s.repo.RecordRuntimeAdmissionFailure(recordCtx, domain.RuntimeAdmissionFailure{
		UserID:             plan.UserID,
		CredentialKeyID:    plan.RuntimeCredentialKeyID,
		RequestedRuntimeID: plan.RuntimeID,
		RequestedName:      plan.Name,
		Source:             domain.RuntimeSourceHosted,
		Role:               domain.CredentialRoleExecutor,
		FailureCode:        failure.Code,
		Reason:             reason,
		FirstSeenAt:        now,
		LastSeenAt:         now,
		AttemptCount:       1,
	})
	if recordErr != nil {
		return fmt.Errorf("%w: %s; structured startup failure could not be recorded", original, failure.Code), true
	}
	return fmt.Errorf("%w: %s: %s", original, failure.Code, reason), true
}

func validHostedRuntimeStartupFailure(failure provision.StartupFailure) bool {
	if failure.Code != "RUNTIME_DEPENDENCY_PROFILE_INVALID" ||
		failure.Source != domain.RuntimeSourceHosted ||
		failure.Reason != "runtime dependency profile verification failed" {
		return false
	}
	if failure.Module != "" && !safeRuntimeStartupFailureFact(failure.Module, 128) {
		return false
	}
	return safeRuntimeStartupFailureFact(failure.ProfileName, 256) &&
		safeRuntimeStartupFailureFact(failure.ProfileVersion, 256) &&
		safeRuntimeStartupFailureFact(failure.ImageBuildID, 256)
}

func safeRuntimeStartupFailureFact(value string, maxLength int) bool {
	return value != "" && len(value) <= maxLength && strings.TrimSpace(value) == value && !strings.ContainsAny(value, "\r\n\x00")
}

func (s *Service) findRuntimeByUserNameSource(ctx context.Context, userID int64, name, source string, includeEnded bool) (domain.Runtime, bool, error) {
	const pageSize = 200
	for offset := 0; ; offset += pageSize {
		items, _, hasMore, err := s.repo.ListRuntimes(ctx, userID, "", source, pageSize, offset)
		if err != nil {
			return domain.Runtime{}, false, err
		}
		for _, rt := range items {
			if rt.Name == name && (includeEnded || !domain.IsRuntimeTerminalStatus(rt.Status)) {
				return rt, true, nil
			}
		}
		if !hasMore {
			return domain.Runtime{}, false, nil
		}
	}
}

func (s *Service) findHostedRuntimeSlot(ctx context.Context, userID int64) (domain.Runtime, bool, error) {
	const pageSize = 200
	for offset := 0; ; offset += pageSize {
		items, _, hasMore, err := s.repo.ListRuntimes(ctx, userID, "", domain.RuntimeSourceHosted, pageSize, offset)
		if err != nil {
			return domain.Runtime{}, false, err
		}
		for _, rt := range items {
			if !domain.IsRuntimeTerminalStatus(rt.Status) {
				return rt, true, nil
			}
		}
		if !hasMore {
			return domain.Runtime{}, false, nil
		}
	}
}

func (s *Service) generateAvailableHostedRuntimeName(ctx context.Context, userID int64) (string, error) {
	maxAttempts := len(hostedNameAdjectives) * len(hostedNameNouns)
	if maxAttempts <= 0 {
		maxAttempts = 1
	}
	offset := sRandInt(maxAttempts)
	for attempt := 0; attempt < maxAttempts; attempt++ {
		name := hostedRuntimeNameForAttempt(offset + attempt)
		if _, ok, err := s.findRuntimeByUserNameSource(ctx, userID, name, "", true); err != nil {
			return "", err
		} else if !ok {
			return name, nil
		}
	}
	return "", fmt.Errorf("%w: unable to allocate hosted runtime name", ErrConflict)
}

func hostedRuntimeNameForAttempt(attempt int) string {
	if attempt < 0 {
		attempt = 0
	}
	total := len(hostedNameAdjectives) * len(hostedNameNouns)
	if total == 0 {
		return "hosted-runtime"
	}
	n := attempt % total
	adjective := hostedNameAdjectives[n%len(hostedNameAdjectives)]
	noun := hostedNameNouns[(n/len(hostedNameAdjectives))%len(hostedNameNouns)]
	return fmt.Sprintf("hosted-%s-%s", adjective, noun)
}
