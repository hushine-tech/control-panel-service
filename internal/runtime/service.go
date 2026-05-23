// Package service holds the control-panel-service business logic. The
// gRPC layer (grpc.go) is a thin proto-translation wrapper.
//
// Auth model (D1, token-only — see Phase D1 Resolved Decisions):
//   - registration_token: returned at RegisterRuntime, used for Heartbeat.
//   - caller_token:       returned per ResolveRuntimeRoute call, presented by
//     handler→runtime gRPC calls. Verification is wired
//     in section 6 (handler cutover); section 2 just
//     issues opaque tokens.
package runtime

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	accountv1 "github.com/hushine-tech/core-service/gen/accountv1"
	"github.com/hushine-tech/control-panel-service/internal/auth"
	"github.com/hushine-tech/control-panel-service/internal/calltoken"
	"github.com/hushine-tech/control-panel-service/internal/config"
	"github.com/hushine-tech/control-panel-service/internal/domain"
	cpnotify "github.com/hushine-tech/control-panel-service/internal/notification"
	"github.com/hushine-tech/control-panel-service/internal/plan"
	"github.com/hushine-tech/control-panel-service/internal/provision"
	"github.com/hushine-tech/control-panel-service/internal/repository"
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
	// ErrPlanLookupUnavailable: account-service couldn't be reached (or
	// returned a non-NotFound error) when resolving the user's plan. Maps
	// to gRPC Unavailable so callers can retry after backoff.
	ErrPlanLookupUnavailable = errors.New("plan lookup unavailable")
	// ErrSessionLookupUnavailable: account-service couldn't be reached when
	// checking runtime-bound session blockers. Runtime end is fail-closed
	// when this dependency is unavailable.
	ErrSessionLookupUnavailable = errors.New("session lookup unavailable")
	// ErrProvisionerUnavailable: the configured provisioner backend
	// refused the request (no-op default, daemon down, image missing,
	// etc.). Maps to gRPC FailedPrecondition.
	ErrProvisionerUnavailable = errors.New("provisioner unavailable")
	// ErrRegistrationTimeout: a freshly-provisioned runtime did not
	// self-register within the configured deadline. Maps to gRPC
	// FailedPrecondition; the runtime container is likely broken.
	ErrRegistrationTimeout = errors.New("runtime did not self-register in time")
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
	repo                   repository.Repository
	plans                  *plan.Resolver
	provisioner            provision.Provisioner
	provisioning           config.ProvisioningConfig
	sessionClient          sessionClient
	streamCloser           runtimeStreamCloser
	hostedCredentialIssuer hostedCredentialIssuer
	notifications          cpnotify.Publisher
	callerTokens           *calltoken.Store
	callerTTL              time.Duration
	heartbeatGrace         time.Duration
	deathGrace             time.Duration
	now                    func() time.Time
}

type sessionClient interface {
	ListRunningSessions(ctx context.Context, in *accountv1.ListRunningSessionsRequest, opts ...grpc.CallOption) (*accountv1.ListRunningSessionsResponse, error)
	MarkRuntimeSessionsRecoverable(ctx context.Context, in *accountv1.MarkRuntimeSessionsRecoverableRequest, opts ...grpc.CallOption) (*accountv1.MarkRuntimeSessionsRecoverableResponse, error)
}

type runtimeStreamCloser interface {
	CloseStreamForRuntime(ctx context.Context, runtimeID string) (bool, error)
}

type hostedCredentialIssuer interface {
	IssueHostedInternalRuntimeCredential(ctx context.Context, userID int64, runtimeID, name string) (domain.IssuedCredential, error)
}

// Config bundles the timeouts injected from config.RuntimePlatformConfig.
type Config struct {
	HeartbeatGrace time.Duration
	DeathGrace     time.Duration
	CallerTokenTTL time.Duration
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
}

func New(repo repository.Repository, plans *plan.Resolver, cfg Config) *Service {
	if cfg.HeartbeatGrace <= 0 {
		cfg.HeartbeatGrace = 30 * time.Second
	}
	if cfg.DeathGrace <= 0 {
		cfg.DeathGrace = 5 * time.Minute
	}
	if cfg.CallerTokenTTL <= 0 {
		cfg.CallerTokenTTL = 60 * time.Second
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
	s := &Service{
		repo:                   repo,
		plans:                  plans,
		provisioner:            cfg.Provisioner,
		provisioning:           cfg.Provisioning,
		sessionClient:          cfg.SessionClient,
		streamCloser:           cfg.RuntimeStreamCloser,
		hostedCredentialIssuer: cfg.HostedCredentialIssuer,
		notifications:          cfg.NotificationPublisher,
		callerTTL:              cfg.CallerTokenTTL,
		heartbeatGrace:         cfg.HeartbeatGrace,
		deathGrace:             cfg.DeathGrace,
		now:                    time.Now,
	}
	s.callerTokens = calltoken.NewStore(func() time.Time { return s.now() })
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

// issueCallerToken generates and registers a caller_token bound to
// (userID, runtimeID) with the configured TTL. The token + expiry are
// returned so the response can include them. Used by both
// ResolveRuntimeRoute and EnsureHostedRuntime.
func (s *Service) issueCallerToken(userID int64, runtimeID string) (token string, expiresAt time.Time) {
	token = auth.GenerateOpaqueToken()
	expiresAt = s.now().UTC().Add(s.callerTTL)
	s.callerTokens.Issue(token, calltoken.Binding{
		UserID:    userID,
		RuntimeID: runtimeID,
		ExpiresAt: expiresAt,
	})
	return token, expiresAt
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

// ── Register ────────────────────────────────────────────────────────────────

type RegisterArgs struct {
	RuntimeID       string
	Source          string // RuntimeSourceHosted only; self_hosted uses RuntimeChannel
	BindUserID      int64  // required for hosted
	Name            string // user-visible label; generated for hosted if empty
	EndpointHost    string
	GRPCPort        int32
	DebugPort       int32
	Capabilities    []string
	ResourceProfile string
	Version         string
}

type RegisterResult struct {
	Runtime           domain.Runtime
	RegistrationToken string // issued plaintext; caller treats as secret
}

func (s *Service) RegisterRuntime(ctx context.Context, args RegisterArgs) (RegisterResult, error) {
	if args.Source != domain.RuntimeSourceHosted {
		return RegisterResult{}, fmt.Errorf("%w: RegisterRuntime supports hosted source only; self_hosted uses RuntimeChannel HELLO", ErrInvalidArgument)
	}
	if args.EndpointHost == "" || args.GRPCPort <= 0 {
		return RegisterResult{}, fmt.Errorf("%w: endpoint_host and grpc_port are required", ErrInvalidArgument)
	}
	if args.ResourceProfile == "" {
		return RegisterResult{}, fmt.Errorf("%w: resource_profile is required", ErrInvalidArgument)
	}
	if args.BindUserID <= 0 {
		return RegisterResult{}, fmt.Errorf("%w: bind_user_id is required for hosted source", ErrInvalidArgument)
	}

	name := args.Name
	autoName := name == ""
	if name == "" {
		name = generateHostedRuntimeName()
	} else if !validRuntimeName(name) {
		return RegisterResult{}, fmt.Errorf("%w: name must match %s", ErrInvalidArgument, runtimeNamePattern)
	}
	runtimeID := args.RuntimeID
	if runtimeID == "" {
		runtimeID = auth.GenerateRuntimeID()
	}

	limits, err := s.resolvePlan(ctx, args.BindUserID)
	if err != nil {
		return RegisterResult{}, err
	}
	counts, err := s.repo.CountRuntimesByUser(ctx, args.BindUserID)
	if err != nil {
		return RegisterResult{}, err
	}
	// 0 = hard cap (forbidden); -1 = unlimited; >0 = real cap.
	if limits.MaxHostedRuntimes == 0 {
		return RegisterResult{}, fmt.Errorf("%w: plan %q forbids hosted runtimes", ErrQuotaExceeded, limits.PlanCode)
	}
	if limits.MaxHostedRuntimes > 0 && counts.Hosted >= int64(limits.MaxHostedRuntimes) {
		return RegisterResult{}, fmt.Errorf("%w: plan %q caps hosted runtimes at %d", ErrQuotaExceeded, limits.PlanCode, limits.MaxHostedRuntimes)
	}
	if !resourceProfileAllowed(args.ResourceProfile, limits.AllowedResourceProfiles) {
		return RegisterResult{}, fmt.Errorf("%w: plan %q does not allow resource_profile %q", ErrQuotaExceeded, limits.PlanCode, args.ResourceProfile)
	}

	now := s.now().UTC()
	registrationToken := auth.GenerateRegistrationToken()

	rt := domain.Runtime{
		RuntimeID:       runtimeID,
		Name:            name,
		Source:          domain.RuntimeSourceHosted,
		Role:            domain.CredentialRoleExecutor,
		EndpointHost:    args.EndpointHost,
		GRPCPort:        args.GRPCPort,
		DebugPort:       args.DebugPort,
		Capabilities:    args.Capabilities,
		ResourceProfile: args.ResourceProfile,
		Version:         args.Version,
		TokenHash:       auth.HashToken(registrationToken),
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	rt.UserID = args.BindUserID
	rt.Status = domain.RuntimeStatusStarting
	paired := now
	rt.PairedAt = &paired

	// Hosted source: admission is strict. A non-ended hosted runtime
	// already occupying this user's name must be explicitly ended before
	// a replacement can register.
	maxNameAttempts := len(hostedNameAdjectives) * len(hostedNameNouns)
	if !autoName {
		maxNameAttempts = 1
	}
	if maxNameAttempts <= 0 {
		maxNameAttempts = 1
	}
	nameOffset := sRandInt(maxNameAttempts)
	for attempt := 0; attempt < maxNameAttempts; attempt++ {
		if autoName && attempt > 0 {
			rt.Name = hostedRuntimeNameForAttempt(nameOffset + attempt)
		}
		if err := s.repo.CreateOrReplaceHostedRuntime(ctx, rt); err != nil {
			if errors.Is(err, repository.ErrConflict) {
				if autoName {
					continue
				}
				return RegisterResult{}, fmt.Errorf("%w: hosted runtime name occupied; end the existing runtime first", ErrConflict)
			}
			return RegisterResult{}, err
		}
		return RegisterResult{Runtime: rt, RegistrationToken: registrationToken}, nil
	}

	return RegisterResult{}, fmt.Errorf("%w: unable to allocate hosted runtime name", ErrConflict)
}

// ── Heartbeat ───────────────────────────────────────────────────────────────

type HeartbeatResult struct {
	HeartbeatAt       time.Time
	ShutdownRequested bool
	TerminalReason    string
}

func (s *Service) HeartbeatRuntime(ctx context.Context, runtimeID, presentedToken string) (HeartbeatResult, error) {
	if runtimeID == "" {
		return HeartbeatResult{}, fmt.Errorf("%w: runtime_id is required", ErrInvalidArgument)
	}
	if presentedToken == "" {
		return HeartbeatResult{}, fmt.Errorf("%w: registration/session token required in metadata", ErrInvalidArgument)
	}
	rt, err := s.repo.GetRuntime(ctx, runtimeID)
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			return HeartbeatResult{}, ErrNotFound
		}
		return HeartbeatResult{}, err
	}
	if auth.HashToken(presentedToken) != rt.TokenHash {
		return HeartbeatResult{}, ErrTokenMismatch
	}
	if domain.IsRuntimeTerminalStatus(rt.Status) {
		result := HeartbeatResult{
			ShutdownRequested: true,
			TerminalReason:    rt.EndedReason,
		}
		if rt.HeartbeatAt != nil {
			result.HeartbeatAt = *rt.HeartbeatAt
		}
		return result, nil
	}
	at := s.now().UTC()
	if err := s.repo.UpdateRuntimeHeartbeat(ctx, runtimeID, at); err != nil {
		return HeartbeatResult{}, err
	}
	switch rt.Status {
	case domain.RuntimeStatusStarting, domain.RuntimeStatusPaired:
		s.publishRuntimeEvent(ctx, rt, cpnotify.EventRuntimeStarted, cpnotify.SeverityInfo, fmt.Sprintf("Runtime %s is active.", rt.Name))
	case domain.RuntimeStatusUnhealthy:
		s.publishRuntimeEvent(ctx, rt, cpnotify.EventRuntimeRecovered, cpnotify.SeverityInfo, fmt.Sprintf("Runtime %s recovered.", rt.Name))
	}
	return HeartbeatResult{HeartbeatAt: at}, nil
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

func (r ForceEndRequest) Audit(runtimeID string, sessions []*accountv1.StrategySessionEntry) ForceEndAudit {
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

func (s *Service) deprovisionHostedRuntime(runtimeID string) runtimeCleanupResult {
	return s.deprovisionHostedRuntimeHandle(runtimeID, hostedRuntimeHandle(runtimeID))
}

func (s *Service) deprovisionHostedRuntimeHandle(runtimeID, handle string) runtimeCleanupResult {
	// Hosted runtime handles are deterministic container names. Cleanup is
	// best-effort so a missing local Docker container does not make the
	// already-ended registry row routeable again.
	cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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

func (s *Service) listRuntimeEndBlockers(ctx context.Context, runtimeID string) ([]*accountv1.StrategySessionEntry, error) {
	if runtimeID == "" {
		return nil, nil
	}
	if s.sessionClient == nil {
		return nil, fmt.Errorf("%w: account-service session client is not configured", ErrSessionLookupUnavailable)
	}
	resp, err := s.sessionClient.ListRunningSessions(ctx, &accountv1.ListRunningSessionsRequest{RuntimeId: runtimeID})
	if err != nil {
		return nil, fmt.Errorf("%w: list runtime sessions for %s: %v", ErrSessionLookupUnavailable, runtimeID, err)
	}
	if resp == nil {
		return nil, nil
	}
	return resp.GetSessions(), nil
}

func describeSessionBlocker(sess *accountv1.StrategySessionEntry) string {
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
	ended, err := s.repo.EndDeadRuntimes(ctx, deadCutoff, domain.RuntimeEndedReasonHeartbeatStale, now)
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
	_, _ = s.sessionClient.MarkRuntimeSessionsRecoverable(ctx, &accountv1.MarkRuntimeSessionsRecoverableRequest{
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
	// Mode is the requested account/session mode when the caller has it.
	// Debugger routes require mode 0; executor routes support mode 0/2.
	Mode int
}

type ResolveResult struct {
	Runtime              domain.Runtime
	GRPCEndpoint         string
	DebugEndpoint        string
	CallerToken          string
	CallerTokenExpiresAt time.Time
}

func (s *Service) ResolveRuntimeRouteByID(ctx context.Context, args ResolveByIDArgs) (ResolveResult, error) {
	rt, err := s.GetRuntime(ctx, GetRuntimeArgs{UserID: args.UserID, RuntimeID: args.RuntimeID})
	if err != nil {
		return ResolveResult{}, err
	}
	return s.resolveRuntimeRouteForRuntimeWithPolicy(ctx, args.UserID, rt, args.Role, args.Mode)
}

func (s *Service) resolveRuntimeRouteForRuntime(ctx context.Context, userID int64, rt domain.Runtime) (ResolveResult, error) {
	return s.resolveRuntimeRouteForRuntimeWithPolicy(ctx, userID, rt, string(domain.CredentialRoleExecutor), 0)
}

func (s *Service) resolveRuntimeRouteForRuntimeWithPolicy(ctx context.Context, userID int64, rt domain.Runtime, role string, mode int) (ResolveResult, error) {
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
		if mode != 0 && mode != 2 {
			return ResolveResult{}, fmt.Errorf("%w: executor runtime mode %d is not supported", ErrPermissionDenied, mode)
		}
	case domain.CredentialRoleDebugger:
		if rt.Role != domain.CredentialRoleDebugger {
			return ResolveResult{}, fmt.Errorf("%w: runtime role %q is not eligible for debugger sessions", ErrPermissionDenied, rt.Role)
		}
		if mode != 0 {
			return ResolveResult{}, fmt.Errorf("%w: debugger runtime only supports mode=0", ErrPermissionDenied)
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

	if rt.Source == domain.RuntimeSourceSelfHosted {
		return ResolveResult{Runtime: rt}, nil
	}

	callerToken, callerExpiry := s.issueCallerToken(userID, rt.RuntimeID)
	grpcEndpoint := fmt.Sprintf("%s:%d", rt.EndpointHost, rt.GRPCPort)
	debugEndpoint := ""
	if rt.DebugPort > 0 {
		debugEndpoint = fmt.Sprintf("%s:%d", rt.EndpointHost, rt.DebugPort)
	}
	return ResolveResult{
		Runtime:              rt,
		GRPCEndpoint:         grpcEndpoint,
		DebugEndpoint:        debugEndpoint,
		CallerToken:          callerToken,
		CallerTokenExpiresAt: callerExpiry,
	}, nil
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
	Runtime              domain.Runtime
	GRPCEndpoint         string
	DebugEndpoint        string
	CallerToken          string
	CallerTokenExpiresAt time.Time
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
//  4. allocate runtime_id + port + token
//  5. call provisioner.Provision
//  6. wait for runtime to call RegisterRuntime back (poll repo)
//  7. return route
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
		ControlPanelGRPC:    "", // operator-controlled; the runtime container env carries it
	}
	if hostedCredential.KeyID != "" {
		plan.RuntimeCredentialKeyID = hostedCredential.KeyID
		plan.RuntimeCredentialPrivateKeyPEM = hostedCredential.PrivateKeyPEM
	}

	handle, err := s.provisioner.Provision(ctx, plan)
	if err != nil {
		if errors.Is(err, provision.ErrNotConfigured) {
			return EnsureHostedRuntimeResult{}, fmt.Errorf("%w: %v", ErrProvisionerUnavailable, err)
		}
		return EnsureHostedRuntimeResult{}, fmt.Errorf("%w: %v", ErrProvisionerUnavailable, err)
	}

	// Wait for the runtime's section-4 self-register code to land a row.
	timeout := time.Duration(s.provisioning.RegistrationTimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	rt, err := s.waitForRegistration(ctx, runtimeID, timeout)
	if err != nil {
		// Best-effort cleanup. The result is persisted when the runtime row
		// exists, so hosted deprovision failures are visible in Runtime
		// Management instead of being reduced to a one-off startup error.
		s.deprovisionHostedRuntimeHandle(runtimeID, handle)
		return EnsureHostedRuntimeResult{}, err
	}

	callerToken, callerExpiry := s.issueCallerToken(args.UserID, rt.RuntimeID)
	debugEndpoint := ""
	if rt.DebugPort > 0 {
		debugEndpoint = fmt.Sprintf("%s:%d", rt.EndpointHost, rt.DebugPort)
	}
	return EnsureHostedRuntimeResult{
		Runtime:              rt,
		GRPCEndpoint:         fmt.Sprintf("%s:%d", rt.EndpointHost, rt.GRPCPort),
		DebugEndpoint:        debugEndpoint,
		CallerToken:          callerToken,
		CallerTokenExpiresAt: callerExpiry,
		Provisioned:          true,
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
	callerToken, callerExpiry := s.issueCallerToken(rt.UserID, rt.RuntimeID)
	debugEndpoint := ""
	if rt.DebugPort > 0 {
		debugEndpoint = fmt.Sprintf("%s:%d", rt.EndpointHost, rt.DebugPort)
	}
	_ = now // 'now' is no longer used but kept above for the lastSeen guard
	return EnsureHostedRuntimeResult{
		Runtime:              rt,
		GRPCEndpoint:         fmt.Sprintf("%s:%d", rt.EndpointHost, rt.GRPCPort),
		DebugEndpoint:        debugEndpoint,
		CallerToken:          callerToken,
		CallerTokenExpiresAt: callerExpiry,
		Provisioned:          false,
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

// ── ValidateCallerToken ─────────────────────────────────────────────────────

// ValidateCallerTokenArgs is the input to ValidateCallerToken.
type ValidateCallerTokenArgs struct {
	Token     string
	RuntimeID string
}

// ValidateCallerTokenResult mirrors the gRPC response shape.
type ValidateCallerTokenResult struct {
	Valid  bool
	UserID int64
	Reason string
}

// ValidateCallerToken is the runtime-side hook for verifying inbound
// `x-caller-token` metadata. The strategy-runtime gRPC interceptor calls
// this before invoking the actual RPC handler.
//
// Returns Valid=false with a Reason when the token is unknown / expired
// / bound to a different runtime. The gRPC layer maps the typed result
// into a 200 response (not an error) so the runtime can decide
// PermissionDenied vs Unauthenticated based on its own policy.
func (s *Service) ValidateCallerToken(ctx context.Context, args ValidateCallerTokenArgs) (ValidateCallerTokenResult, error) {
	_ = ctx // ctx unused for the in-memory store; kept for API symmetry
	if args.Token == "" {
		return ValidateCallerTokenResult{Valid: false, Reason: string(calltoken.ReasonUnknown)}, nil
	}
	uid, valid, reason := s.callerTokens.Validate(args.Token, args.RuntimeID)
	return ValidateCallerTokenResult{
		Valid:  valid,
		UserID: uid,
		Reason: string(reason),
	}, nil
}
