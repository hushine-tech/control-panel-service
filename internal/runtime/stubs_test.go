package runtime

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/hushine-tech/control-panel-service/internal/config"
	"github.com/hushine-tech/control-panel-service/internal/domain"
	"github.com/hushine-tech/control-panel-service/internal/plan"
	"github.com/hushine-tech/control-panel-service/internal/provision"
	"github.com/hushine-tech/control-panel-service/internal/repository"
	accountv1 "github.com/hushine-tech/core-service/gen/accountv1"
	"google.golang.org/grpc"
)

// fakeProvisioner is the test double for provision.Provisioner. It can
// be configured per-test to:
//   - succeed and synchronously insert a runtime row (mimics the runtime
//     container booting and self-registering before the service-layer
//     timeout fires);
//   - succeed but never insert (mimics a runtime container that started
//     but failed to self-register, exercising the timeout path);
//   - fail (mimics a misconfigured backend / image missing, exercising
//     the ErrProvisionerUnavailable path).
type fakeProvisioner struct {
	repo *stubRepo
	// onProvision controls behavior:
	//   "ok"              → success; insert runtime row immediately
	//   "ok_no_register"  → success; do NOT insert row (timeout test)
	//   "fail"            → return provision.ErrProvisionFailed
	//   "not_configured"  → return provision.ErrNotConfigured
	onProvision        string
	now                func() time.Time
	calls              int
	lastPlan           provision.Plan
	deprovisions       int
	deprovisionHandles []string
	deprovisionErr     error
}

type fakeHostedCredentialIssuer struct {
	calls  int
	issued domain.IssuedCredential
	err    error
}

type fakeSessionClient struct {
	listCalls []*accountv1.ListRunningSessionsRequest
	listResp  []*accountv1.StrategySessionEntry
	listErr   error
	markCalls []*accountv1.MarkRuntimeSessionsRecoverableRequest
	markErr   error
}

type fakeRuntimeStreamCloser struct {
	runtimeIDs []string
	err        error
}

func (f *fakeSessionClient) MarkRuntimeSessionsRecoverable(_ context.Context, req *accountv1.MarkRuntimeSessionsRecoverableRequest, _ ...grpc.CallOption) (*accountv1.MarkRuntimeSessionsRecoverableResponse, error) {
	f.markCalls = append(f.markCalls, req)
	if f.markErr != nil {
		return nil, f.markErr
	}
	return &accountv1.MarkRuntimeSessionsRecoverableResponse{SessionsMarked: 1}, nil
}

func (f *fakeSessionClient) ListRunningSessions(_ context.Context, req *accountv1.ListRunningSessionsRequest, _ ...grpc.CallOption) (*accountv1.ListRunningSessionsResponse, error) {
	f.listCalls = append(f.listCalls, req)
	if f.listErr != nil {
		return nil, f.listErr
	}
	return &accountv1.ListRunningSessionsResponse{Sessions: f.listResp}, nil
}

func (f *fakeRuntimeStreamCloser) CloseStreamForRuntime(_ context.Context, runtimeID string) (bool, error) {
	f.runtimeIDs = append(f.runtimeIDs, runtimeID)
	if f.err != nil {
		return false, f.err
	}
	return true, nil
}

func (f *fakeProvisioner) Provision(_ context.Context, p provision.Plan) (string, error) {
	f.calls++
	f.lastPlan = p
	switch f.onProvision {
	case "fail":
		return "", provision.ErrProvisionFailed
	case "not_configured":
		return "", provision.ErrNotConfigured
	case "ok_no_register":
		return "handle-" + p.RuntimeID, nil
	default:
		// Default: simulate the runtime booting + self-registering
		// before the service layer's wait loop polls again. We insert
		// the row in 'paired' state here so waitForRegistration sees
		// it on the very first iteration.
		now := time.Now().UTC()
		if f.now != nil {
			now = f.now()
		}
		f.repo.mu.Lock()
		// Mimic the production sequence: register stamps paired,
		// first heartbeat flips to active, control-panel only
		// considers `active` as ready. The fake collapses both
		// steps so waitForRegistration short-circuits.
		f.repo.runtimes[p.RuntimeID] = domain.Runtime{
			RuntimeID:                  p.RuntimeID,
			UserID:                     p.UserID,
			Name:                       p.Name,
			Source:                     domain.RuntimeSourceHosted,
			Role:                       domain.CredentialRoleExecutor,
			EndpointHost:               p.EndpointHost,
			GRPCPort:                   int32(p.GRPCPort),
			Capabilities:               p.Capabilities,
			ResourceProfile:            p.ResourceProfileName,
			Status:                     domain.RuntimeStatusActive,
			HeartbeatAt:                &now,
			ConnectionOwnerInstanceID:  "cp-test",
			ConnectionOwnerAcquiredAt:  &now,
			ConnectionOwnerHeartbeatAt: &now,
			CreatedAt:                  now,
			UpdatedAt:                  now,
		}
		if f.onProvision == "ok_no_owner" {
			rt := f.repo.runtimes[p.RuntimeID]
			rt.ConnectionOwnerInstanceID = ""
			rt.ConnectionOwnerAcquiredAt = nil
			rt.ConnectionOwnerHeartbeatAt = nil
			f.repo.runtimes[p.RuntimeID] = rt
		}
		f.repo.mu.Unlock()
		return "handle-" + p.RuntimeID, nil
	}
}

func (f *fakeHostedCredentialIssuer) IssueHostedInternalRuntimeCredential(_ context.Context, userID int64, runtimeID, name string) (domain.IssuedCredential, error) {
	f.calls++
	if f.err != nil {
		return domain.IssuedCredential{}, f.err
	}
	issued := f.issued
	issued.UserID = userID
	if issued.KeyID == "" {
		issued.KeyID = "hosted-key-" + runtimeID
	}
	if issued.PrivateKeyPEM == "" {
		issued.PrivateKeyPEM = "private-key-pem"
	}
	issued.Role = domain.CredentialRoleExecutor
	issued.HostedInternal = true
	return issued, nil
}

func (f *fakeProvisioner) Deprovision(_ context.Context, handle string) error {
	f.deprovisions++
	f.deprovisionHandles = append(f.deprovisionHandles, handle)
	return f.deprovisionErr
}

// stubRepo is the in-memory repository.Repository used by service tests.
// It is intentionally permissive: every method is straightforward and only
// implements the invariants the service code depends on (NotFound on miss,
// Conflict on duplicate hosted slot / credential binding).
type stubRepo struct {
	mu       sync.Mutex
	runtimes map[string]domain.Runtime
}

func newStubRepo() *stubRepo {
	return &stubRepo{
		runtimes: map[string]domain.Runtime{},
	}
}

func (s *stubRepo) Close() error { return nil }

func (s *stubRepo) CreateRuntime(_ context.Context, rt domain.Runtime) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	rt = normalizeStubRuntimeForWrite(rt)
	if _, ok := s.runtimes[rt.RuntimeID]; ok {
		return repository.ErrConflict
	}
	s.runtimes[rt.RuntimeID] = rt
	return nil
}

// CreateOrReplaceHostedRuntime mirrors production admission: any existing
// row owning (user_id, name) blocks the insert and is not mutated.
func (s *stubRepo) CreateOrReplaceHostedRuntime(_ context.Context, rt domain.Runtime) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	rt = normalizeStubRuntimeForWrite(rt)
	if rt.Source != domain.RuntimeSourceHosted || rt.UserID <= 0 {
		return repository.ErrConflict
	}
	if _, ok := s.runtimes[rt.RuntimeID]; ok {
		return repository.ErrConflict
	}
	for _, e := range s.runtimes {
		if e.UserID == rt.UserID && e.Name == rt.Name {
			return repository.ErrConflict
		}
	}
	s.runtimes[rt.RuntimeID] = rt
	return nil
}

func (s *stubRepo) CreateOrReplaceSelfHostedRuntime(_ context.Context, rt domain.Runtime) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	rt = normalizeStubRuntimeForWrite(rt)
	if rt.Source != domain.RuntimeSourceSelfHosted || rt.UserID <= 0 {
		return repository.ErrConflict
	}
	if rt.CredentialKeyID == "" {
		return repository.ErrConflict
	}
	if existing, ok := s.runtimes[rt.RuntimeID]; ok {
		if domain.IsRuntimeTerminalStatus(existing.Status) {
			return repository.ErrConflict
		}
		if existing.UserID != rt.UserID || existing.Source != rt.Source || existing.CredentialKeyID != rt.CredentialKeyID {
			return repository.ErrConflict
		}
	}
	for id, e := range s.runtimes {
		if id == rt.RuntimeID {
			continue
		}
		if e.CredentialKeyID != "" && e.CredentialKeyID == rt.CredentialKeyID && !domain.IsRuntimeTerminalStatus(e.Status) {
			return repository.ErrConflict
		}
		if e.UserID == rt.UserID && e.Name == rt.Name {
			return repository.ErrConflict
		}
	}
	s.runtimes[rt.RuntimeID] = rt
	return nil
}

func (s *stubRepo) GetRuntime(_ context.Context, runtimeID string) (domain.Runtime, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rt, ok := s.runtimes[runtimeID]
	if !ok {
		return domain.Runtime{}, repository.ErrNotFound
	}
	return rt, nil
}

func (s *stubRepo) ListRuntimes(_ context.Context, userID int64, statusFilter, sourceFilter string, limit, offset int) ([]domain.Runtime, int64, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if limit <= 0 {
		limit = 20
	}
	var matched []domain.Runtime
	for _, e := range s.runtimes {
		if e.UserID != userID {
			continue
		}
		if statusFilter != "" && e.Status != statusFilter {
			continue
		}
		if sourceFilter != "" && e.Source != sourceFilter {
			continue
		}
		matched = append(matched, e)
	}
	sort.Slice(matched, func(i, j int) bool {
		left := matched[i].CreatedAt
		if matched[i].StartedAt != nil {
			left = *matched[i].StartedAt
		}
		right := matched[j].CreatedAt
		if matched[j].StartedAt != nil {
			right = *matched[j].StartedAt
		}
		if !left.Equal(right) {
			return left.After(right)
		}
		return matched[i].RuntimeID > matched[j].RuntimeID
	})
	total := int64(len(matched))
	if offset > len(matched) {
		offset = len(matched)
	}
	page := matched[offset:]
	hasMore := false
	if len(page) > limit {
		page = page[:limit]
		hasMore = true
	}
	return page, total, hasMore, nil
}

func (s *stubRepo) UpdateRuntimeStatus(_ context.Context, runtimeID, status string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	rt, ok := s.runtimes[runtimeID]
	if !ok {
		return repository.ErrNotFound
	}
	rt.Status = status
	rt.UpdatedAt = time.Now().UTC()
	s.runtimes[runtimeID] = rt
	return nil
}

func (s *stubRepo) MarkStaleRuntimesUnhealthy(_ context.Context, cutoff time.Time) ([]domain.Runtime, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var result []domain.Runtime
	for id, rt := range s.runtimes {
		if rt.Status != domain.RuntimeStatusActive && rt.Status != domain.RuntimeStatusStarting && rt.Status != domain.RuntimeStatusPaired {
			continue
		}
		lastSeen := rt.UpdatedAt
		if rt.HeartbeatAt != nil {
			lastSeen = *rt.HeartbeatAt
		}
		if !lastSeen.Before(cutoff) {
			continue
		}
		rt.Status = domain.RuntimeStatusUnhealthy
		rt.UpdatedAt = time.Now().UTC()
		s.runtimes[id] = rt
		result = append(result, rt)
	}
	return result, nil
}

func (s *stubRepo) UpdateRuntimeHeartbeat(_ context.Context, runtimeID string, at time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	rt, ok := s.runtimes[runtimeID]
	if !ok {
		return repository.ErrNotFound
	}
	if domain.IsRuntimeTerminalStatus(rt.Status) {
		return repository.ErrNotFound
	}
	t := at
	rt.HeartbeatAt = &t
	if rt.Status == domain.RuntimeStatusStarting || rt.Status == domain.RuntimeStatusPaired || rt.Status == domain.RuntimeStatusUnhealthy {
		rt.Status = domain.RuntimeStatusActive
	}
	if rt.Status == domain.RuntimeStatusActive && rt.StartedAt == nil {
		rt.StartedAt = &t
	}
	rt.UpdatedAt = at
	s.runtimes[runtimeID] = rt
	return nil
}

func (s *stubRepo) RecordRuntimeConnectionOwner(_ context.Context, runtimeID, instanceID string, at time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	rt, ok := s.runtimes[runtimeID]
	if !ok || domain.IsRuntimeTerminalStatus(rt.Status) {
		return repository.ErrNotFound
	}
	acquired := at
	if rt.ConnectionOwnerInstanceID == instanceID && rt.ConnectionOwnerAcquiredAt != nil {
		acquired = *rt.ConnectionOwnerAcquiredAt
	}
	heartbeat := at
	rt.ConnectionOwnerInstanceID = instanceID
	rt.ConnectionOwnerAcquiredAt = &acquired
	rt.ConnectionOwnerHeartbeatAt = &heartbeat
	rt.UpdatedAt = at
	s.runtimes[runtimeID] = rt
	return nil
}

func (s *stubRepo) ClearRuntimeConnectionOwner(_ context.Context, runtimeID, instanceID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	rt, ok := s.runtimes[runtimeID]
	if !ok {
		return nil
	}
	if rt.ConnectionOwnerInstanceID != instanceID {
		return nil
	}
	rt.ConnectionOwnerInstanceID = ""
	rt.ConnectionOwnerAcquiredAt = nil
	rt.ConnectionOwnerHeartbeatAt = nil
	s.runtimes[runtimeID] = rt
	return nil
}

func (s *stubRepo) CountRuntimesByUser(_ context.Context, userID int64) (domain.RuntimeUsageCounts, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var c domain.RuntimeUsageCounts
	for _, e := range s.runtimes {
		if e.UserID != userID || domain.IsRuntimeTerminalStatus(e.Status) {
			continue
		}
		switch e.Source {
		case domain.RuntimeSourceHosted:
			c.Hosted++
		case domain.RuntimeSourceSelfHosted:
			c.SelfHosted++
		}
	}
	return c, nil
}

func (s *stubRepo) EndRuntime(_ context.Context, runtimeID, reason string, endedAt time.Time) (domain.Runtime, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rt, ok := s.runtimes[runtimeID]
	if !ok {
		return domain.Runtime{}, repository.ErrNotFound
	}
	if domain.IsRuntimeTerminalStatus(rt.Status) {
		return rt, nil
	}
	rt.Status = domain.RuntimeTerminalStatusForReason(reason)
	rt.EndedReason = reason
	t := endedAt
	rt.EndedAt = &t
	rt.UpdatedAt = endedAt
	s.runtimes[runtimeID] = rt
	return rt, nil
}

func (s *stubRepo) UpdateRuntimeCleanupState(_ context.Context, runtimeID, status, reason string, at time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	rt, ok := s.runtimes[runtimeID]
	if !ok {
		return repository.ErrNotFound
	}
	rt.CleanupStatus = status
	rt.CleanupReason = reason
	t := at
	rt.CleanupAt = &t
	rt.UpdatedAt = at
	s.runtimes[runtimeID] = rt
	return nil
}

func normalizeStubRuntimeForWrite(rt domain.Runtime) domain.Runtime {
	if rt.Role == "" {
		rt.Role = domain.CredentialRoleExecutor
	}
	if rt.Status != domain.RuntimeStatusActive || rt.StartedAt != nil {
		return rt
	}
	started := firstStubRuntimeTime(rt.HeartbeatAt, rt.PairedAt)
	if started == nil && !rt.UpdatedAt.IsZero() {
		t := rt.UpdatedAt
		started = &t
	}
	if started == nil && !rt.CreatedAt.IsZero() {
		t := rt.CreatedAt
		started = &t
	}
	if started == nil {
		t := time.Now().UTC()
		started = &t
	}
	t := started.UTC()
	rt.StartedAt = &t
	return rt
}

func firstStubRuntimeTime(candidates ...*time.Time) *time.Time {
	for _, candidate := range candidates {
		if candidate == nil || candidate.IsZero() {
			continue
		}
		t := *candidate
		return &t
	}
	return nil
}

func attachRuntimeOwner(t testingT, repo *stubRepo, runtimeID string, at time.Time) {
	t.Helper()
	if err := repo.RecordRuntimeConnectionOwner(context.Background(), runtimeID, "cp-test", at); err != nil {
		t.Fatalf("RecordRuntimeConnectionOwner(%s): %v", runtimeID, err)
	}
}

type testingT interface {
	Helper()
	Fatalf(format string, args ...interface{})
}

func (s *stubRepo) EndRuntimesByCredentialKey(_ context.Context, keyID, reason string, endedAt time.Time) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var ended int64
	for id, rt := range s.runtimes {
		if rt.CredentialKeyID != keyID || domain.IsRuntimeTerminalStatus(rt.Status) {
			continue
		}
		rt.Status = domain.RuntimeTerminalStatusForReason(reason)
		rt.EndedReason = reason
		t := endedAt
		rt.EndedAt = &t
		rt.UpdatedAt = endedAt
		s.runtimes[id] = rt
		ended++
	}
	return ended, nil
}

func (s *stubRepo) EndDeadRuntimes(_ context.Context, cutoff time.Time, reason string, endedAt time.Time) ([]domain.Runtime, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var result []domain.Runtime
	for id, rt := range s.runtimes {
		if rt.Status != domain.RuntimeStatusUnhealthy {
			continue
		}
		lastSeen := rt.UpdatedAt
		if rt.HeartbeatAt != nil {
			lastSeen = *rt.HeartbeatAt
		}
		if !lastSeen.Before(cutoff) {
			continue
		}
		rt.Status = domain.RuntimeTerminalStatusForReason(reason)
		rt.EndedReason = reason
		t := endedAt
		rt.EndedAt = &t
		rt.UpdatedAt = endedAt
		s.runtimes[id] = rt
		result = append(result, rt)
	}
	return result, nil
}

// makeService builds a Service backed by the stubRepo and a deterministic
// plan resolver. “planCode“ is the plan_code returned for any user_id.
func makeService(repo *stubRepo, planCode string, plans map[string]config.RuntimePlan, platform config.RuntimePlatformConfig, now time.Time) *Service {
	if planCode == "" {
		planCode = "pro"
	}
	if plans == nil {
		plans = map[string]config.RuntimePlan{
			"pro":  {MaxHostedRuntimes: 5, MaxSelfHostedRuntimes: 5, MaxConcurrentSessionsTotal: 20, AllowedResourceProfiles: []string{"small", "medium", "large"}, AllowSelfHostedRuntime: true},
			"free": {MaxHostedRuntimes: 1, MaxSelfHostedRuntimes: 0, MaxConcurrentSessionsTotal: 2, AllowedResourceProfiles: []string{"small"}, AllowSelfHostedRuntime: false},
		}
	}
	if platform.DefaultPlanCode == "" {
		platform.DefaultPlanCode = "pro"
	}
	if platform.HeartbeatGraceSeconds == 0 {
		platform.HeartbeatGraceSeconds = 30
	}
	if platform.DeathGraceSeconds == 0 {
		platform.DeathGraceSeconds = 300
	}
	// Under the new "0 = forbid, -1 = unlimited" semantics, an unset Go
	// zero would forbid every runtime. Default to unlimited at the
	// platform level for tests that don't explicitly exercise platform
	// caps; tests that do should set MaxTotal* explicitly.
	if platform.MaxTotalHostedRuntimes == 0 {
		platform.MaxTotalHostedRuntimes = -1
	}
	if platform.MaxTotalSelfHostedRuntimes == 0 {
		platform.MaxTotalSelfHostedRuntimes = -1
	}
	resolver := plan.NewResolver(constLookup{code: planCode}, plans, platform)
	svc := New(repo, resolver, Config{
		HeartbeatGrace: time.Duration(platform.HeartbeatGraceSeconds) * time.Second,
		DeathGrace:     time.Duration(platform.DeathGraceSeconds) * time.Second,
		CallerTokenTTL: time.Duration(platform.CallerTokenTTLSeconds) * time.Second,
		SessionClient:  &fakeSessionClient{},
	})
	clock := now
	svc.SetClock(func() time.Time { return clock })
	return svc
}

// makeServiceWithProvisioner builds a Service with a custom provisioner
// + a deterministic provisioning config. Used by EnsureHostedRuntime tests.
func makeServiceWithProvisioner(repo *stubRepo, planCode string, prov provision.Provisioner, now time.Time, regTimeoutSec int) *Service {
	plans := map[string]config.RuntimePlan{
		"pro": {
			MaxHostedRuntimes:          5,
			MaxSelfHostedRuntimes:      5,
			MaxConcurrentSessionsTotal: 20,
			AllowedResourceProfiles:    []string{"small", "medium", "large"},
			AllowSelfHostedRuntime:     true,
		},
		"free": {
			MaxHostedRuntimes:          1,
			MaxSelfHostedRuntimes:      0,
			MaxConcurrentSessionsTotal: 2,
			AllowedResourceProfiles:    []string{"small"},
			AllowSelfHostedRuntime:     false,
		},
	}
	platform := config.RuntimePlatformConfig{
		DefaultPlanCode:            "pro",
		HeartbeatGraceSeconds:      30,
		DeathGraceSeconds:          300,
		MaxTotalHostedRuntimes:     -1,
		MaxTotalSelfHostedRuntimes: -1,
	}
	resolver := plan.NewResolver(constLookup{code: planCode}, plans, platform)
	if regTimeoutSec <= 0 {
		regTimeoutSec = 1 // tight timeout to keep tests fast
	}
	provCfg := config.ProvisioningConfig{
		Image:                      "hushine/strategy-runtime:test",
		AdvertiseHost:              "127.0.0.1",
		PortRangeBase:              50100,
		PortRangeSize:              200,
		RegistrationTimeoutSeconds: regTimeoutSec,
		Profiles: map[string]config.ResourceProfile{
			"small":  {NanoCPUs: "0.5", MemoryMB: 512},
			"medium": {NanoCPUs: "1.0", MemoryMB: 1024},
			"large":  {NanoCPUs: "2.0", MemoryMB: 2048},
		},
	}
	svc := New(repo, resolver, Config{
		HeartbeatGrace: 30 * time.Second,
		DeathGrace:     5 * time.Minute,
		CallerTokenTTL: 60 * time.Second,
		Provisioning:   provCfg,
		Provisioner:    prov,
		SessionClient:  &fakeSessionClient{},
	})
	clock := now
	svc.SetClock(func() time.Time { return clock })
	return svc
}

// constLookup is the deterministic plan-lookup fixture for service tests.
// Setting `err` simulates core-service failures (NotFound, Unavailable,
// etc.) so tests can exercise the fail-closed contract end-to-end.
type constLookup struct {
	code string
	err  error
}

func (c constLookup) GetUserPlanCode(_ context.Context, _ int64) (string, error) {
	return c.code, c.err
}

// ── Phase D3 credential stubs ──────────────────────────────────────────────
//
// The runtime package's tests construct *Service via runtime.New(repo, ...).
// The credential RPCs live on a separate credential.Service that has its own
// repository surface; runtime tests don't exercise that path. Stubbing these
// out so stubRepo satisfies the full repository.Repository interface keeps
// the runtime tests untouched.

func (s *stubRepo) CreateRuntimeCredential(_ context.Context, _ domain.RuntimeCredential) error {
	return nil
}

func (s *stubRepo) GetRuntimeCredential(_ context.Context, _ string) (domain.RuntimeCredential, error) {
	return domain.RuntimeCredential{}, repository.ErrNotFound
}

func (s *stubRepo) ListRuntimeCredentialsByUser(_ context.Context, _ int64, _ bool) ([]domain.RuntimeCredential, error) {
	return nil, nil
}

func (s *stubRepo) ListRuntimeCredentialsByUserPage(_ context.Context, _ int64, _ bool, _, _ int) ([]domain.RuntimeCredential, int64, bool, error) {
	return nil, 0, false, nil
}

func (s *stubRepo) RevokeRuntimeCredential(_ context.Context, _ string, _ int64) (domain.RuntimeCredential, error) {
	return domain.RuntimeCredential{}, repository.ErrNotFound
}

func (s *stubRepo) TouchRuntimeCredentialUsed(_ context.Context, _ string, _ time.Time) error {
	return nil
}

func (s *stubRepo) CreateRuntimeChannelLease(_ context.Context, _ domain.RuntimeChannelLease) error {
	return nil
}

func (s *stubRepo) GetRuntimeChannelLeaseByHash(_ context.Context, _ string) (domain.RuntimeChannelLease, error) {
	return domain.RuntimeChannelLease{}, repository.ErrNotFound
}

func (s *stubRepo) TouchRuntimeChannelLease(_ context.Context, _, _ string, _ time.Time) error {
	return nil
}

func (s *stubRepo) RotateRuntimeChannelLease(_ context.Context, _, _, _ string, _ time.Time, _ time.Time) error {
	return nil
}

func (s *stubRepo) RecordRuntimeAdmissionFailure(_ context.Context, _ domain.RuntimeAdmissionFailure) error {
	return nil
}

func (s *stubRepo) ListRuntimeAdmissionFailuresByUser(_ context.Context, _ int64, _ int) ([]domain.RuntimeAdmissionFailure, error) {
	return nil, nil
}

func (s *stubRepo) ClaimNextRuntimeCommand(_ context.Context, _, _ string, _ time.Time, _ int) (domain.RuntimeCommand, bool, error) {
	return domain.RuntimeCommand{}, false, nil
}

func (s *stubRepo) AcknowledgeRuntimeCommand(_ context.Context, _ string, _ time.Time) (domain.RuntimeCommand, error) {
	return domain.RuntimeCommand{}, repository.ErrNotFound
}

func (s *stubRepo) MarkRuntimeCommandRunning(_ context.Context, _ string, _ time.Time) (domain.RuntimeCommand, error) {
	return domain.RuntimeCommand{}, repository.ErrNotFound
}

func (s *stubRepo) CompleteRuntimeCommand(_ context.Context, _, _ string, _ []byte, _ string, _ time.Time) (domain.RuntimeCommand, error) {
	return domain.RuntimeCommand{}, repository.ErrNotFound
}

func (s *stubRepo) RuntimeCommandCircuitOpen(_ context.Context, _ string, _ time.Time, _ int64) (bool, int64, error) {
	return false, 0, nil
}
