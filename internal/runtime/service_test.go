package runtime

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	accountv1 "github.com/hushine-tech/core-service/gen/accountv1"
	"github.com/hushine-tech/control-panel-service/internal/config"
	"github.com/hushine-tech/control-panel-service/internal/domain"
	"github.com/hushine-tech/control-panel-service/internal/plan"
)

var fixedNow = time.Date(2026, 5, 3, 12, 0, 0, 0, time.UTC)

// ── Register ────────────────────────────────────────────────────────────────

func TestRegister_SelfHostedRejected(t *testing.T) {
	repo := newStubRepo()
	svc := makeService(repo, "pro", nil, config.RuntimePlatformConfig{}, fixedNow)

	_, err := svc.RegisterRuntime(context.Background(), RegisterArgs{
		Source:          domain.RuntimeSourceSelfHosted,
		EndpointHost:    "10.0.0.5",
		GRPCPort:        50053,
		ResourceProfile: "small",
	})
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("err = %v, want ErrInvalidArgument", err)
	}
}

func TestRegister_HostedStartsInStartingState(t *testing.T) {
	repo := newStubRepo()
	svc := makeService(repo, "pro", nil, config.RuntimePlatformConfig{}, fixedNow)

	res, err := svc.RegisterRuntime(context.Background(), RegisterArgs{
		Source:          domain.RuntimeSourceHosted,
		BindUserID:      42,
		EndpointHost:    "10.0.0.5",
		GRPCPort:        50053,
		ResourceProfile: "small",
	})
	if err != nil {
		t.Fatalf("RegisterRuntime: %v", err)
	}
	if res.Runtime.Status != domain.RuntimeStatusStarting {
		t.Errorf("status = %q, want starting", res.Runtime.Status)
	}
	if res.Runtime.UserID != 42 {
		t.Errorf("user_id = %d, want 42", res.Runtime.UserID)
	}
}

func TestRegister_Hosted_QuotaExceeded(t *testing.T) {
	repo := newStubRepo()
	svc := makeService(repo, "free", nil, config.RuntimePlatformConfig{DefaultPlanCode: "free"}, fixedNow)

	for i := 0; i < 1; i++ {
		if _, err := svc.RegisterRuntime(context.Background(), RegisterArgs{
			Source: domain.RuntimeSourceHosted, BindUserID: 7,
			EndpointHost: "h", GRPCPort: 1, ResourceProfile: "small",
		}); err != nil {
			t.Fatalf("first register: %v", err)
		}
	}
	_, err := svc.RegisterRuntime(context.Background(), RegisterArgs{
		Source: domain.RuntimeSourceHosted, BindUserID: 7,
		EndpointHost: "h", GRPCPort: 2, ResourceProfile: "small",
	})
	if !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("err = %v, want ErrQuotaExceeded", err)
	}
}

func TestRegister_RejectsBadSource(t *testing.T) {
	repo := newStubRepo()
	svc := makeService(repo, "pro", nil, config.RuntimePlatformConfig{}, fixedNow)
	_, err := svc.RegisterRuntime(context.Background(), RegisterArgs{
		Source: "garbage", EndpointHost: "h", GRPCPort: 1, ResourceProfile: "small",
	})
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("err = %v, want ErrInvalidArgument", err)
	}
}

func TestRegister_HostedRequiresUserID(t *testing.T) {
	repo := newStubRepo()
	svc := makeService(repo, "pro", nil, config.RuntimePlatformConfig{}, fixedNow)
	_, err := svc.RegisterRuntime(context.Background(), RegisterArgs{
		Source: domain.RuntimeSourceHosted, EndpointHost: "h", GRPCPort: 1, ResourceProfile: "small",
	})
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("err = %v, want ErrInvalidArgument", err)
	}
}

// ── Heartbeat ───────────────────────────────────────────────────────────────

func TestHeartbeat_FlipsStatusToActive(t *testing.T) {
	repo := newStubRepo()
	svc := makeService(repo, "pro", nil, config.RuntimePlatformConfig{}, fixedNow)
	res, err := svc.RegisterRuntime(context.Background(), RegisterArgs{
		Source: domain.RuntimeSourceHosted, BindUserID: 42,
		EndpointHost: "h", GRPCPort: 1, ResourceProfile: "small",
	})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	if _, err := svc.HeartbeatRuntime(context.Background(), res.Runtime.RuntimeID, res.RegistrationToken); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	got, _ := repo.GetRuntime(context.Background(), res.Runtime.RuntimeID)
	if got.Status != domain.RuntimeStatusActive {
		t.Errorf("status = %q, want active", got.Status)
	}
	if got.HeartbeatAt == nil {
		t.Errorf("heartbeat_at not set")
	}
}

func TestHeartbeat_TokenMismatch(t *testing.T) {
	repo := newStubRepo()
	svc := makeService(repo, "pro", nil, config.RuntimePlatformConfig{}, fixedNow)
	res, err := svc.RegisterRuntime(context.Background(), RegisterArgs{
		Source: domain.RuntimeSourceHosted, BindUserID: 42,
		EndpointHost: "h", GRPCPort: 1, ResourceProfile: "small",
	})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	_, err = svc.HeartbeatRuntime(context.Background(), res.Runtime.RuntimeID, "wrong-token")
	if !errors.Is(err, ErrTokenMismatch) {
		t.Fatalf("err = %v, want ErrTokenMismatch", err)
	}
}

func TestHeartbeat_TerminalRuntimeReturnsShutdownInstruction(t *testing.T) {
	repo := newStubRepo()
	svc := makeService(repo, "pro", nil, config.RuntimePlatformConfig{}, fixedNow)
	res, err := svc.RegisterRuntime(context.Background(), RegisterArgs{
		Source: domain.RuntimeSourceHosted, BindUserID: 42,
		EndpointHost: "h", GRPCPort: 1, ResourceProfile: "small",
	})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	if _, err := repo.EndRuntime(context.Background(), res.Runtime.RuntimeID, domain.RuntimeEndedReasonUserCancelled, fixedNow); err != nil {
		t.Fatalf("end: %v", err)
	}
	got, err := svc.HeartbeatRuntime(context.Background(), res.Runtime.RuntimeID, res.RegistrationToken)
	if err != nil {
		t.Fatalf("heartbeat terminal: %v", err)
	}
	if !got.ShutdownRequested || got.TerminalReason != domain.RuntimeEndedReasonUserCancelled {
		t.Fatalf("heartbeat result = %+v, want shutdown with user_cancelled", got)
	}
	stored, _ := repo.GetRuntime(context.Background(), res.Runtime.RuntimeID)
	if stored.Status != domain.RuntimeStatusCancelled {
		t.Fatalf("stored status = %q, want cancelled", stored.Status)
	}
}

// ── Resolve ─────────────────────────────────────────────────────────────────

func TestResolveRuntimeRouteByID_HappyPathLegacyCoverage(t *testing.T) {
	repo := newStubRepo()
	svc := makeService(repo, "pro", nil, config.RuntimePlatformConfig{}, fixedNow)
	reg, err := svc.RegisterRuntime(context.Background(), RegisterArgs{
		Source: domain.RuntimeSourceHosted, BindUserID: 42,
		EndpointHost: "host.example", GRPCPort: 50053, DebugPort: 5678, ResourceProfile: "small",
	})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	if _, err := svc.HeartbeatRuntime(context.Background(), reg.Runtime.RuntimeID, reg.RegistrationToken); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	attachRuntimeOwner(t, repo, reg.Runtime.RuntimeID, fixedNow)
	res, err := svc.ResolveRuntimeRouteByID(context.Background(), ResolveByIDArgs{UserID: 42, RuntimeID: reg.Runtime.RuntimeID})
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if res.GRPCEndpoint != "host.example:50053" {
		t.Errorf("grpc_endpoint = %q, want host.example:50053", res.GRPCEndpoint)
	}
	if res.DebugEndpoint != "host.example:5678" {
		t.Errorf("debug_endpoint = %q, want host.example:5678", res.DebugEndpoint)
	}
	if res.CallerToken == "" {
		t.Errorf("caller_token missing")
	}
}

func TestResolveRuntimeRouteByID_RejectsDebuggerRuntimeForExecutorRoute(t *testing.T) {
	repo := newStubRepo()
	svc := makeService(repo, "pro", nil, config.RuntimePlatformConfig{}, fixedNow)
	heartbeat := fixedNow
	if err := repo.CreateRuntime(context.Background(), domain.Runtime{
		RuntimeID:       "rt-debugger",
		UserID:          42,
		Name:            "debugger-one",
		Source:          domain.RuntimeSourceSelfHosted,
		Role:            domain.CredentialRoleDebugger,
		ResourceProfile: "small",
		Status:          domain.RuntimeStatusActive,
		HeartbeatAt:     &heartbeat,
		CreatedAt:       fixedNow,
		UpdatedAt:       fixedNow,
	}); err != nil {
		t.Fatalf("CreateRuntime: %v", err)
	}

	_, err := svc.ResolveRuntimeRouteByID(context.Background(), ResolveByIDArgs{
		UserID:    42,
		RuntimeID: "rt-debugger",
	})
	if !errors.Is(err, ErrPermissionDenied) {
		t.Fatalf("ResolveRuntimeRouteByID err = %v, want ErrPermissionDenied", err)
	}
}

func TestResolveRuntimeRouteByID_DebuggerModeAndCapacityPolicy(t *testing.T) {
	repo := newStubRepo()
	svc := makeService(repo, "pro", nil, config.RuntimePlatformConfig{}, fixedNow)
	heartbeat := fixedNow
	if err := repo.CreateRuntime(context.Background(), domain.Runtime{
		RuntimeID:       "rt-debugger",
		UserID:          42,
		Name:            "debugger-one",
		Source:          domain.RuntimeSourceSelfHosted,
		Role:            domain.CredentialRoleDebugger,
		ResourceProfile: "small",
		Status:          domain.RuntimeStatusActive,
		HeartbeatAt:     &heartbeat,
		CreatedAt:       fixedNow,
		UpdatedAt:       fixedNow,
	}); err != nil {
		t.Fatalf("CreateRuntime: %v", err)
	}
	attachRuntimeOwner(t, repo, "rt-debugger", fixedNow)

	_, err := svc.ResolveRuntimeRouteByID(context.Background(), ResolveByIDArgs{
		UserID:    42,
		RuntimeID: "rt-debugger",
		Role:      string(domain.CredentialRoleDebugger),
		Mode:      2,
	})
	if !errors.Is(err, ErrPermissionDenied) {
		t.Fatalf("mode=2 debugger err = %v, want ErrPermissionDenied", err)
	}

	blockers := &fakeSessionClient{listResp: []*accountv1.StrategySessionEntry{{
		SessionId: "sess-debug",
		RuntimeId: "rt-debugger",
		Status:    "running",
	}}}
	svc.sessionClient = blockers
	_, err = svc.ResolveRuntimeRouteByID(context.Background(), ResolveByIDArgs{
		UserID:    42,
		RuntimeID: "rt-debugger",
		Role:      string(domain.CredentialRoleDebugger),
		Mode:      0,
	})
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("active debugger session err = %v, want ErrConflict", err)
	}

	svc.sessionClient = &fakeSessionClient{}
	res, err := svc.ResolveRuntimeRouteByID(context.Background(), ResolveByIDArgs{
		UserID:    42,
		RuntimeID: "rt-debugger",
		Role:      string(domain.CredentialRoleDebugger),
		Mode:      0,
	})
	if err != nil {
		t.Fatalf("mode=0 debugger route: %v", err)
	}
	if res.Runtime.RuntimeID != "rt-debugger" {
		t.Fatalf("resolved runtime = %+v", res.Runtime)
	}
}

func TestResolveRuntimeRouteByID_ExecutorModeTwoSharedAcrossSources(t *testing.T) {
	repo := newStubRepo()
	svc := makeService(repo, "pro", nil, config.RuntimePlatformConfig{}, fixedNow)
	heartbeat := fixedNow
	for _, rt := range []domain.Runtime{
		{
			RuntimeID:       "rt-hosted",
			UserID:          42,
			Name:            "hosted-one",
			Source:          domain.RuntimeSourceHosted,
			Role:            domain.CredentialRoleExecutor,
			EndpointHost:    "127.0.0.1",
			GRPCPort:        50053,
			ResourceProfile: "small",
			Status:          domain.RuntimeStatusActive,
			HeartbeatAt:     &heartbeat,
			CreatedAt:       fixedNow,
			UpdatedAt:       fixedNow,
		},
		{
			RuntimeID:       "rt-self",
			UserID:          42,
			Name:            "self-one",
			Source:          domain.RuntimeSourceSelfHosted,
			Role:            domain.CredentialRoleExecutor,
			ResourceProfile: "small",
			Status:          domain.RuntimeStatusActive,
			HeartbeatAt:     &heartbeat,
			CreatedAt:       fixedNow,
			UpdatedAt:       fixedNow,
		},
	} {
		if err := repo.CreateRuntime(context.Background(), rt); err != nil {
			t.Fatalf("CreateRuntime(%s): %v", rt.RuntimeID, err)
		}
		attachRuntimeOwner(t, repo, rt.RuntimeID, fixedNow)
	}
	for _, runtimeID := range []string{"rt-hosted", "rt-self"} {
		res, err := svc.ResolveRuntimeRouteByID(context.Background(), ResolveByIDArgs{
			UserID:    42,
			RuntimeID: runtimeID,
			Role:      string(domain.CredentialRoleExecutor),
			Mode:      2,
		})
		if err != nil {
			t.Fatalf("ResolveRuntimeRouteByID(%s mode=2): %v", runtimeID, err)
		}
		if res.Runtime.RuntimeID != runtimeID {
			t.Fatalf("resolved runtime = %+v, want %s", res.Runtime, runtimeID)
		}
	}
}

func TestGetRuntime_OwnershipChecked(t *testing.T) {
	repo := newStubRepo()
	svc := makeService(repo, "pro", nil, config.RuntimePlatformConfig{}, fixedNow)
	reg, err := svc.RegisterRuntime(context.Background(), RegisterArgs{
		Source: domain.RuntimeSourceHosted, BindUserID: 42,
		EndpointHost: "host.example", GRPCPort: 50053, ResourceProfile: "small",
	})
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	got, err := svc.GetRuntime(context.Background(), GetRuntimeArgs{UserID: 42, RuntimeID: reg.Runtime.RuntimeID})
	if err != nil {
		t.Fatalf("GetRuntime owner: %v", err)
	}
	if got.RuntimeID != reg.Runtime.RuntimeID {
		t.Fatalf("runtime_id = %q, want %q", got.RuntimeID, reg.Runtime.RuntimeID)
	}

	_, err = svc.GetRuntime(context.Background(), GetRuntimeArgs{UserID: 7, RuntimeID: reg.Runtime.RuntimeID})
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("cross-user err = %v, want ErrNotFound", err)
	}
}

func TestEndRuntime_HostedCancelsAndDeprovisions(t *testing.T) {
	repo := newStubRepo()
	prov := &fakeProvisioner{repo: repo, onProvision: "ok", now: func() time.Time { return fixedNow }}
	svc := makeServiceWithProvisioner(repo, "pro", prov, fixedNow, 1)
	rt := domain.Runtime{
		RuntimeID:       "rt_stop",
		UserID:          42,
		Name:            "default",
		Source:          domain.RuntimeSourceHosted,
		EndpointHost:    "127.0.0.1",
		GRPCPort:        50053,
		ResourceProfile: "small",
		Status:          domain.RuntimeStatusActive,
		CreatedAt:       fixedNow,
		UpdatedAt:       fixedNow,
	}
	if err := repo.CreateRuntime(context.Background(), rt); err != nil {
		t.Fatalf("CreateRuntime: %v", err)
	}

	got, err := svc.EndRuntime(context.Background(), EndRuntimeArgs{UserID: 42, RuntimeID: "rt_stop"})
	if err != nil {
		t.Fatalf("EndRuntime: %v", err)
	}
	if got.Status != domain.RuntimeStatusCancelled {
		t.Fatalf("status = %q, want cancelled", got.Status)
	}
	if prov.deprovisions != 1 || len(prov.deprovisionHandles) != 1 || prov.deprovisionHandles[0] != "hushine-runtime-rt_stop" {
		t.Fatalf("deprovision = %d/%v, want one hushine-runtime-rt_stop", prov.deprovisions, prov.deprovisionHandles)
	}
	if got.CleanupStatus != domain.RuntimeCleanupStatusSucceeded {
		t.Fatalf("cleanup_status = %q, want succeeded", got.CleanupStatus)
	}
	stored, err := repo.GetRuntime(context.Background(), "rt_stop")
	if err != nil {
		t.Fatalf("GetRuntime: %v", err)
	}
	if stored.Status != domain.RuntimeStatusCancelled {
		t.Fatalf("stored status = %q, want cancelled", stored.Status)
	}
	if stored.CleanupStatus != domain.RuntimeCleanupStatusSucceeded || stored.CleanupAt == nil {
		t.Fatalf("stored cleanup = %q/%v, want succeeded timestamp", stored.CleanupStatus, stored.CleanupAt)
	}
}

func TestEndRuntime_HostedRecordsDeprovisionFailure(t *testing.T) {
	repo := newStubRepo()
	prov := &fakeProvisioner{repo: repo, onProvision: "ok", now: func() time.Time { return fixedNow }, deprovisionErr: errors.New("docker rm failed")}
	svc := makeServiceWithProvisioner(repo, "pro", prov, fixedNow, 1)
	if err := repo.CreateRuntime(context.Background(), domain.Runtime{
		RuntimeID:       "rt_cleanup_fail",
		UserID:          42,
		Name:            "hosted-cleanup-fail",
		Source:          domain.RuntimeSourceHosted,
		EndpointHost:    "127.0.0.1",
		GRPCPort:        50053,
		ResourceProfile: "small",
		Status:          domain.RuntimeStatusActive,
		CreatedAt:       fixedNow,
		UpdatedAt:       fixedNow,
	}); err != nil {
		t.Fatalf("CreateRuntime: %v", err)
	}

	got, err := svc.EndRuntime(context.Background(), EndRuntimeArgs{UserID: 42, RuntimeID: "rt_cleanup_fail"})
	if err != nil {
		t.Fatalf("EndRuntime: %v", err)
	}
	if got.CleanupStatus != domain.RuntimeCleanupStatusFailed || !strings.Contains(got.CleanupReason, "docker rm failed") {
		t.Fatalf("cleanup = %q/%q, want failed docker reason", got.CleanupStatus, got.CleanupReason)
	}
	stored, err := repo.GetRuntime(context.Background(), "rt_cleanup_fail")
	if err != nil {
		t.Fatalf("GetRuntime: %v", err)
	}
	if stored.CleanupStatus != domain.RuntimeCleanupStatusFailed || stored.CleanupAt == nil {
		t.Fatalf("stored cleanup = %q/%v, want failed timestamp", stored.CleanupStatus, stored.CleanupAt)
	}
}

func TestEndRuntime_SelfHostedClosesRuntimeChannelStream(t *testing.T) {
	repo := newStubRepo()
	closer := &fakeRuntimeStreamCloser{}
	svc := makeService(repo, "pro", nil, config.RuntimePlatformConfig{}, fixedNow)
	svc.streamCloser = closer
	if err := repo.CreateRuntime(context.Background(), domain.Runtime{
		RuntimeID:       "selfhosted-key-1",
		UserID:          42,
		Name:            "custom-alpha",
		Source:          domain.RuntimeSourceSelfHosted,
		ResourceProfile: "small",
		Status:          domain.RuntimeStatusActive,
		CreatedAt:       fixedNow,
		UpdatedAt:       fixedNow,
	}); err != nil {
		t.Fatalf("CreateRuntime: %v", err)
	}

	got, err := svc.EndRuntime(context.Background(), EndRuntimeArgs{UserID: 42, RuntimeID: "selfhosted-key-1"})
	if err != nil {
		t.Fatalf("EndRuntime: %v", err)
	}
	if got.Status != domain.RuntimeStatusCancelled {
		t.Fatalf("status = %q, want cancelled", got.Status)
	}
	if len(closer.runtimeIDs) != 1 || closer.runtimeIDs[0] != "selfhosted-key-1" {
		t.Fatalf("closed streams = %v, want [selfhosted-key-1]", closer.runtimeIDs)
	}
	if got.CleanupStatus != domain.RuntimeCleanupStatusUserOwned {
		t.Fatalf("cleanup_status = %q, want user_owned", got.CleanupStatus)
	}
}

func TestEndRuntime_BlocksActiveSession(t *testing.T) {
	repo := newStubRepo()
	sessions := &fakeSessionClient{
		listResp: []*accountv1.StrategySessionEntry{
			{SessionId: "sess-active", RuntimeId: "rt_busy", Status: "running"},
		},
	}
	closer := &fakeRuntimeStreamCloser{}
	svc := makeService(repo, "pro", nil, config.RuntimePlatformConfig{}, fixedNow)
	svc.sessionClient = sessions
	svc.streamCloser = closer
	if err := repo.CreateRuntime(context.Background(), domain.Runtime{
		RuntimeID:       "rt_busy",
		UserID:          42,
		Source:          domain.RuntimeSourceSelfHosted,
		ResourceProfile: "small",
		Status:          domain.RuntimeStatusActive,
		CreatedAt:       fixedNow,
		UpdatedAt:       fixedNow,
	}); err != nil {
		t.Fatalf("CreateRuntime: %v", err)
	}

	_, err := svc.EndRuntime(context.Background(), EndRuntimeArgs{UserID: 42, RuntimeID: "rt_busy"})
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("EndRuntime err = %v, want ErrConflict", err)
	}
	if err == nil || !strings.Contains(err.Error(), "sess-active") {
		t.Fatalf("EndRuntime err = %v, want active session blocker id", err)
	}
	stored, err := repo.GetRuntime(context.Background(), "rt_busy")
	if err != nil {
		t.Fatalf("GetRuntime: %v", err)
	}
	if stored.Status != domain.RuntimeStatusActive {
		t.Fatalf("stored status = %q, want still active", stored.Status)
	}
	if len(sessions.listCalls) != 1 || sessions.listCalls[0].GetRuntimeId() != "rt_busy" {
		t.Fatalf("list calls = %+v, want runtime-scoped blocker lookup", sessions.listCalls)
	}
	if len(sessions.markCalls) != 0 {
		t.Fatalf("recoverable calls = %+v, want none while blocked", sessions.markCalls)
	}
	if len(closer.runtimeIDs) != 0 {
		t.Fatalf("closed streams = %v, want none while blocked", closer.runtimeIDs)
	}
}

func TestEndRuntime_ForceShapeReservedForAdminOnly(t *testing.T) {
	repo := newStubRepo()
	sessions := &fakeSessionClient{
		listResp: []*accountv1.StrategySessionEntry{
			{SessionId: "sess-active", RuntimeId: "rt_force", Status: "running"},
		},
	}
	svc := makeService(repo, "pro", nil, config.RuntimePlatformConfig{}, fixedNow)
	svc.sessionClient = sessions
	if err := repo.CreateRuntime(context.Background(), domain.Runtime{
		RuntimeID:       "rt_force",
		UserID:          42,
		Source:          domain.RuntimeSourceSelfHosted,
		ResourceProfile: "small",
		Status:          domain.RuntimeStatusActive,
		CreatedAt:       fixedNow,
		UpdatedAt:       fixedNow,
	}); err != nil {
		t.Fatalf("CreateRuntime: %v", err)
	}

	_, err := svc.EndRuntime(context.Background(), EndRuntimeArgs{
		UserID:    42,
		RuntimeID: "rt_force",
		Force: &ForceEndRequest{
			ActorUserID: 99,
			Authority:   "admin",
			Reason:      "maintenance",
		},
	})
	if !errors.Is(err, ErrPermissionDenied) {
		t.Fatalf("force EndRuntime err = %v, want ErrPermissionDenied", err)
	}
	stored, err := repo.GetRuntime(context.Background(), "rt_force")
	if err != nil {
		t.Fatalf("GetRuntime: %v", err)
	}
	if stored.Status != domain.RuntimeStatusActive {
		t.Fatalf("stored status = %q, want still active", stored.Status)
	}
	if len(sessions.markCalls) != 0 {
		t.Fatalf("recoverable calls = %+v, want none for rejected force-end", sessions.markCalls)
	}
}

func TestEndRuntime_MarksBoundSessionsRecoverable(t *testing.T) {
	repo := newStubRepo()
	sessions := &fakeSessionClient{}
	svc := makeService(repo, "pro", nil, config.RuntimePlatformConfig{}, fixedNow)
	svc.sessionClient = sessions
	if err := repo.CreateRuntime(context.Background(), domain.Runtime{
		RuntimeID: "rt_stop_sessions",
		UserID:    42,
		Source:    domain.RuntimeSourceSelfHosted,
		Status:    domain.RuntimeStatusActive,
		CreatedAt: fixedNow,
		UpdatedAt: fixedNow,
	}); err != nil {
		t.Fatalf("CreateRuntime: %v", err)
	}

	if _, err := svc.EndRuntime(context.Background(), EndRuntimeArgs{UserID: 42, RuntimeID: "rt_stop_sessions"}); err != nil {
		t.Fatalf("EndRuntime: %v", err)
	}
	if len(sessions.markCalls) != 1 {
		t.Fatalf("recoverable calls = %d, want 1", len(sessions.markCalls))
	}
	got := sessions.markCalls[0]
	if got.GetRuntimeId() != "rt_stop_sessions" || got.GetError() == "" {
		t.Fatalf("recoverable request = %+v, want runtime-scoped failure reason", got)
	}
}

func TestEndRuntime_AlreadyEndedHasNoRepeatedSideEffects(t *testing.T) {
	repo := newStubRepo()
	prov := &fakeProvisioner{repo: repo, onProvision: "ok", now: func() time.Time { return fixedNow }}
	sessions := &fakeSessionClient{}
	svc := makeServiceWithProvisioner(repo, "pro", prov, fixedNow, 1)
	svc.sessionClient = sessions
	endedAt := fixedNow.Add(-time.Minute)
	if err := repo.CreateRuntime(context.Background(), domain.Runtime{
		RuntimeID:   "rt_already_ended",
		UserID:      42,
		Name:        "ended-runtime",
		Source:      domain.RuntimeSourceHosted,
		Status:      domain.RuntimeStatusEnded,
		EndedAt:     &endedAt,
		CreatedAt:   fixedNow.Add(-time.Hour),
		UpdatedAt:   endedAt,
		EndedReason: domain.RuntimeEndedReasonUserCancelled,
	}); err != nil {
		t.Fatalf("CreateRuntime: %v", err)
	}

	if _, err := svc.EndRuntime(context.Background(), EndRuntimeArgs{UserID: 42, RuntimeID: "rt_already_ended"}); err != nil {
		t.Fatalf("EndRuntime: %v", err)
	}
	if prov.deprovisions != 0 {
		t.Fatalf("deprovisions = %d, want 0 for already-ended runtime", prov.deprovisions)
	}
	if len(sessions.markCalls) != 0 {
		t.Fatalf("recoverable calls = %+v, want none for already-ended runtime", sessions.markCalls)
	}
}

func TestReapStaleRuntimes_MarksUnhealthyWithoutRecoveringSessions(t *testing.T) {
	repo := newStubRepo()
	svc := makeService(repo, "pro", nil, config.RuntimePlatformConfig{HeartbeatGraceSeconds: 30, DeathGraceSeconds: 300}, fixedNow)
	sessions := &fakeSessionClient{}
	svc.sessionClient = sessions
	staleHeartbeat := fixedNow.Add(-time.Minute)
	if err := repo.CreateRuntime(context.Background(), domain.Runtime{
		RuntimeID:   "rt_stale",
		UserID:      42,
		Source:      domain.RuntimeSourceHosted,
		Status:      domain.RuntimeStatusActive,
		HeartbeatAt: &staleHeartbeat,
		CreatedAt:   fixedNow.Add(-time.Hour),
		UpdatedAt:   staleHeartbeat,
	}); err != nil {
		t.Fatalf("CreateRuntime stale: %v", err)
	}
	freshHeartbeat := fixedNow.Add(-5 * time.Second)
	if err := repo.CreateRuntime(context.Background(), domain.Runtime{
		RuntimeID:   "rt_fresh",
		UserID:      43,
		Source:      domain.RuntimeSourceHosted,
		Status:      domain.RuntimeStatusActive,
		HeartbeatAt: &freshHeartbeat,
		CreatedAt:   fixedNow.Add(-time.Hour),
		UpdatedAt:   freshHeartbeat,
	}); err != nil {
		t.Fatalf("CreateRuntime fresh: %v", err)
	}

	stale, err := svc.ReapStaleRuntimes(context.Background())
	if err != nil {
		t.Fatalf("ReapStaleRuntimes: %v", err)
	}
	if len(stale) != 1 || stale[0].RuntimeID != "rt_stale" {
		t.Fatalf("stale = %+v, want rt_stale", stale)
	}
	gotStale, _ := repo.GetRuntime(context.Background(), "rt_stale")
	if gotStale.Status != domain.RuntimeStatusUnhealthy {
		t.Fatalf("stale status = %q, want unhealthy", gotStale.Status)
	}
	gotFresh, _ := repo.GetRuntime(context.Background(), "rt_fresh")
	if gotFresh.Status != domain.RuntimeStatusActive {
		t.Fatalf("fresh status = %q, want active", gotFresh.Status)
	}
	if len(sessions.markCalls) != 0 {
		t.Fatalf("recoverable calls = %+v, want none before runtime is ended", sessions.markCalls)
	}
}

func TestReapStaleRuntimes_EndsDeadRuntimeAndMarksSessionsRecoverable(t *testing.T) {
	repo := newStubRepo()
	svc := makeService(repo, "pro", nil, config.RuntimePlatformConfig{HeartbeatGraceSeconds: 30, DeathGraceSeconds: 300}, fixedNow)
	sessions := &fakeSessionClient{}
	svc.sessionClient = sessions
	deadHeartbeat := fixedNow.Add(-10 * time.Minute)
	if err := repo.CreateRuntime(context.Background(), domain.Runtime{
		RuntimeID:   "rt_dead",
		UserID:      42,
		Source:      domain.RuntimeSourceHosted,
		Status:      domain.RuntimeStatusUnhealthy,
		HeartbeatAt: &deadHeartbeat,
		CreatedAt:   fixedNow.Add(-time.Hour),
		UpdatedAt:   deadHeartbeat,
	}); err != nil {
		t.Fatalf("CreateRuntime dead: %v", err)
	}

	ended, err := svc.ReapStaleRuntimes(context.Background())
	if err != nil {
		t.Fatalf("ReapStaleRuntimes: %v", err)
	}
	if len(ended) != 1 || ended[0].RuntimeID != "rt_dead" {
		t.Fatalf("ended = %+v, want rt_dead", ended)
	}
	gotDead, _ := repo.GetRuntime(context.Background(), "rt_dead")
	if gotDead.Status != domain.RuntimeStatusHeartbeatStale {
		t.Fatalf("dead status = %q, want heartbeat_stale", gotDead.Status)
	}
	if gotDead.EndedReason != domain.RuntimeEndedReasonHeartbeatStale {
		t.Fatalf("ended_reason = %q, want heartbeat_stale", gotDead.EndedReason)
	}
	if len(sessions.markCalls) != 1 || sessions.markCalls[0].GetRuntimeId() != "rt_dead" {
		t.Fatalf("recoverable calls = %+v, want rt_dead", sessions.markCalls)
	}
}

func TestReapStaleRuntimes_DeprovisionsDeadHostedRuntime(t *testing.T) {
	repo := newStubRepo()
	prov := &fakeProvisioner{repo: repo, onProvision: "ok", now: func() time.Time { return fixedNow }}
	svc := makeServiceWithProvisioner(repo, "pro", prov, fixedNow, 1)
	svc.sessionClient = &fakeSessionClient{}
	deadHeartbeat := fixedNow.Add(-10 * time.Minute)
	if err := repo.CreateRuntime(context.Background(), domain.Runtime{
		RuntimeID:   "rt_dead_hosted",
		UserID:      42,
		Source:      domain.RuntimeSourceHosted,
		Status:      domain.RuntimeStatusUnhealthy,
		HeartbeatAt: &deadHeartbeat,
		CreatedAt:   fixedNow.Add(-time.Hour),
		UpdatedAt:   deadHeartbeat,
	}); err != nil {
		t.Fatalf("CreateRuntime dead: %v", err)
	}

	_, err := svc.ReapStaleRuntimes(context.Background())
	if err != nil {
		t.Fatalf("ReapStaleRuntimes: %v", err)
	}
	if prov.deprovisions != 1 || len(prov.deprovisionHandles) != 1 || prov.deprovisionHandles[0] != "hushine-runtime-rt_dead_hosted" {
		t.Fatalf("deprovisions = %d/%v, want one hushine-runtime-rt_dead_hosted", prov.deprovisions, prov.deprovisionHandles)
	}
}

func TestEndRuntime_CrossUserFailsClosed(t *testing.T) {
	repo := newStubRepo()
	svc := makeService(repo, "pro", nil, config.RuntimePlatformConfig{}, fixedNow)
	if err := repo.CreateRuntime(context.Background(), domain.Runtime{
		RuntimeID: "rt_other",
		UserID:    42,
		Source:    domain.RuntimeSourceSelfHosted,
		Status:    domain.RuntimeStatusActive,
		CreatedAt: fixedNow,
		UpdatedAt: fixedNow,
	}); err != nil {
		t.Fatalf("CreateRuntime: %v", err)
	}

	_, err := svc.EndRuntime(context.Background(), EndRuntimeArgs{UserID: 7, RuntimeID: "rt_other"})
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("cross-user err = %v, want ErrNotFound", err)
	}
}

func TestResolveRuntimeRouteByID_HappyPath(t *testing.T) {
	repo := newStubRepo()
	svc := makeService(repo, "pro", nil, config.RuntimePlatformConfig{}, fixedNow)
	reg, err := svc.RegisterRuntime(context.Background(), RegisterArgs{
		Source: domain.RuntimeSourceHosted, BindUserID: 42,
		EndpointHost: "host.example", GRPCPort: 50053, DebugPort: 5678, ResourceProfile: "small",
	})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	if _, err := svc.HeartbeatRuntime(context.Background(), reg.Runtime.RuntimeID, reg.RegistrationToken); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	attachRuntimeOwner(t, repo, reg.Runtime.RuntimeID, fixedNow)

	res, err := svc.ResolveRuntimeRouteByID(context.Background(), ResolveByIDArgs{UserID: 42, RuntimeID: reg.Runtime.RuntimeID})
	if err != nil {
		t.Fatalf("resolve by id: %v", err)
	}
	if res.Runtime.RuntimeID != reg.Runtime.RuntimeID {
		t.Fatalf("runtime_id = %q, want %q", res.Runtime.RuntimeID, reg.Runtime.RuntimeID)
	}
	if res.GRPCEndpoint != "host.example:50053" || res.CallerToken == "" {
		t.Fatalf("bad route: endpoint=%q token=%q", res.GRPCEndpoint, res.CallerToken)
	}
}

func TestResolveRuntimeRouteByID_CrossUserFailsClosed(t *testing.T) {
	repo := newStubRepo()
	svc := makeService(repo, "pro", nil, config.RuntimePlatformConfig{}, fixedNow)
	reg, err := svc.RegisterRuntime(context.Background(), RegisterArgs{
		Source: domain.RuntimeSourceHosted, BindUserID: 42,
		EndpointHost: "host.example", GRPCPort: 50053, ResourceProfile: "small",
	})
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	_, err = svc.ResolveRuntimeRouteByID(context.Background(), ResolveByIDArgs{UserID: 7, RuntimeID: reg.Runtime.RuntimeID})
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("cross-user resolve err = %v, want ErrNotFound", err)
	}
}

func TestResolveRuntimeRouteByID_EndedMarksSessionsRecoverable(t *testing.T) {
	repo := newStubRepo()
	sessions := &fakeSessionClient{}
	svc := makeService(repo, "pro", nil, config.RuntimePlatformConfig{}, fixedNow)
	svc.sessionClient = sessions
	if err := repo.CreateRuntime(context.Background(), domain.Runtime{
		RuntimeID: "rt_ended",
		UserID:    42,
		Name:      "default",
		Source:    domain.RuntimeSourceHosted,
		Status:    domain.RuntimeStatusEnded,
		CreatedAt: fixedNow,
		UpdatedAt: fixedNow,
	}); err != nil {
		t.Fatalf("CreateRuntime: %v", err)
	}

	_, err := svc.ResolveRuntimeRouteByID(context.Background(), ResolveByIDArgs{UserID: 42, RuntimeID: "rt_ended"})
	if !errors.Is(err, ErrEnded) {
		t.Fatalf("err = %v, want ErrEnded", err)
	}
	if len(sessions.markCalls) != 1 {
		t.Fatalf("recoverable calls = %d, want 1", len(sessions.markCalls))
	}
	got := sessions.markCalls[0]
	if got.GetRuntimeId() != "rt_ended" || got.GetError() == "" {
		t.Fatalf("recoverable request = %+v, want runtime-scoped failure reason", got)
	}
}

func TestRuntimeToProtoIncludesCredentialKeyID(t *testing.T) {
	startedAt := fixedNow.Add(time.Minute)
	endedAt := fixedNow.Add(2 * time.Minute)
	rt := domain.Runtime{
		RuntimeID:       "rt-self",
		CredentialKeyID: "key-1",
		UserID:          42,
		Name:            "default",
		Source:          domain.RuntimeSourceSelfHosted,
		ResourceProfile: "small",
		Status:          domain.RuntimeStatusEnded,
		StartedAt:       &startedAt,
		EndedAt:         &endedAt,
		EndedReason:     domain.RuntimeEndedReasonUserCancelled,
		CleanupStatus:   domain.RuntimeCleanupStatusUserOwned,
		CleanupReason:   "self-hosted container is user-owned",
		CleanupAt:       &endedAt,
		CreatedAt:       fixedNow,
		UpdatedAt:       fixedNow,
	}
	got := runtimeToProto(rt)
	if got.GetCredentialKeyId() != "key-1" {
		t.Fatalf("credential_key_id = %q, want key-1", got.GetCredentialKeyId())
	}
	if got.GetName() != "default" || got.GetStartedAt() == nil || got.GetEndedAt() == nil || got.GetEndedReason() != domain.RuntimeEndedReasonUserCancelled {
		t.Fatalf("runtime proto = %+v, want name/timestamps/ended_reason mapped", got)
	}
	if got.GetCleanupStatus() != domain.RuntimeCleanupStatusUserOwned || got.GetCleanupAt() == nil {
		t.Fatalf("runtime proto cleanup = %q/%v, want user_owned timestamp", got.GetCleanupStatus(), got.GetCleanupAt())
	}
}

func TestResolve_SelfHostedDoesNotIssueCallerTokenOrEndpoint(t *testing.T) {
	repo := newStubRepo()
	svc := makeService(repo, "pro", nil, config.RuntimePlatformConfig{}, fixedNow)
	heartbeat := fixedNow
	repo.runtimes["rt-self"] = domain.Runtime{
		RuntimeID:       "rt-self",
		CredentialKeyID: "key-1",
		UserID:          42,
		Name:            "default",
		Source:          domain.RuntimeSourceSelfHosted,
		EndpointHost:    "user-laptop.local",
		GRPCPort:        50053,
		DebugPort:       5678,
		ResourceProfile: "small",
		Status:          domain.RuntimeStatusActive,
		HeartbeatAt:     &heartbeat,
		CreatedAt:       fixedNow,
		UpdatedAt:       fixedNow,
	}
	attachRuntimeOwner(t, repo, "rt-self", fixedNow)

	res, err := svc.ResolveRuntimeRouteByID(context.Background(), ResolveByIDArgs{UserID: 42, RuntimeID: "rt-self"})
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if res.CallerToken != "" || !res.CallerTokenExpiresAt.IsZero() {
		t.Fatalf("self-hosted route returned caller token: token=%q expires=%v", res.CallerToken, res.CallerTokenExpiresAt)
	}
	if res.GRPCEndpoint != "" || res.DebugEndpoint != "" {
		t.Fatalf("self-hosted route returned direct endpoints: grpc=%q debug=%q", res.GRPCEndpoint, res.DebugEndpoint)
	}
}

func TestResolveRuntimeRouteByID_NoRuntimeChannelOwnerFailsClosed(t *testing.T) {
	repo := newStubRepo()
	svc := makeService(repo, "pro", nil, config.RuntimePlatformConfig{}, fixedNow)
	heartbeat := fixedNow
	repo.runtimes["rt-ownerless"] = domain.Runtime{
		RuntimeID:       "rt-ownerless",
		CredentialKeyID: "key-1",
		UserID:          42,
		Name:            "ownerless",
		Source:          domain.RuntimeSourceSelfHosted,
		Role:            domain.CredentialRoleExecutor,
		ResourceProfile: "small",
		Status:          domain.RuntimeStatusActive,
		HeartbeatAt:     &heartbeat,
		CreatedAt:       fixedNow,
		UpdatedAt:       fixedNow,
	}

	_, err := svc.ResolveRuntimeRouteByID(context.Background(), ResolveByIDArgs{UserID: 42, RuntimeID: "rt-ownerless"})
	if !errors.Is(err, ErrUnhealthy) {
		t.Fatalf("err = %v, want ErrUnhealthy", err)
	}
}

func TestResolve_NotFound(t *testing.T) {
	repo := newStubRepo()
	svc := makeService(repo, "pro", nil, config.RuntimePlatformConfig{}, fixedNow)
	_, err := svc.ResolveRuntimeRouteByID(context.Background(), ResolveByIDArgs{UserID: 42, RuntimeID: "missing"})
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
}

func TestResolve_Unpaired(t *testing.T) {
	repo := newStubRepo()
	svc := makeService(repo, "pro", nil, config.RuntimePlatformConfig{}, fixedNow)
	// D3 self-hosted registration now happens through RuntimeChannel and lands
	// active immediately. Inject an unpaired row directly so route resolution
	// still covers defensive legacy data.
	rt := domain.Runtime{
		RuntimeID:       "rt-1",
		UserID:          42,
		Name:            "default",
		Source:          domain.RuntimeSourceSelfHosted,
		EndpointHost:    "h",
		GRPCPort:        1,
		ResourceProfile: "small",
		Status:          "unpaired",
		CreatedAt:       fixedNow,
		UpdatedAt:       fixedNow,
	}
	if err := repo.CreateRuntime(context.Background(), rt); err != nil {
		t.Fatalf("seed: %v", err)
	}
	_, err := svc.ResolveRuntimeRouteByID(context.Background(), ResolveByIDArgs{UserID: 42, RuntimeID: "rt-1"})
	if !errors.Is(err, ErrUnpaired) {
		t.Fatalf("err = %v, want ErrUnpaired", err)
	}
}

func TestResolve_StaleHeartbeat(t *testing.T) {
	repo := newStubRepo()
	platform := config.RuntimePlatformConfig{HeartbeatGraceSeconds: 5}
	svc := makeService(repo, "pro", nil, platform, fixedNow)
	reg, err := svc.RegisterRuntime(context.Background(), RegisterArgs{
		Source: domain.RuntimeSourceHosted, BindUserID: 42,
		EndpointHost: "h", GRPCPort: 1, ResourceProfile: "small",
	})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	if _, err := svc.HeartbeatRuntime(context.Background(), reg.Runtime.RuntimeID, reg.RegistrationToken); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	attachRuntimeOwner(t, repo, reg.Runtime.RuntimeID, fixedNow)
	// Advance clock past grace.
	svc.SetClock(func() time.Time { return fixedNow.Add(60 * time.Second) })
	_, err = svc.ResolveRuntimeRouteByID(context.Background(), ResolveByIDArgs{UserID: 42, RuntimeID: reg.Runtime.RuntimeID})
	if !errors.Is(err, ErrUnhealthy) {
		t.Fatalf("err = %v, want ErrUnhealthy", err)
	}
}

func TestResolveRuntimeRouteByID_RequiresRuntimeID(t *testing.T) {
	repo := newStubRepo()
	svc := makeService(repo, "pro", nil, config.RuntimePlatformConfig{}, fixedNow)
	if _, err := svc.ResolveRuntimeRouteByID(context.Background(), ResolveByIDArgs{UserID: 42}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("err = %v, want ErrInvalidArgument", err)
	}
}

// ── List ────────────────────────────────────────────────────────────────────

func TestList_Filters(t *testing.T) {
	repo := newStubRepo()
	svc := makeService(repo, "pro", nil, config.RuntimePlatformConfig{}, fixedNow)
	for i := 0; i < 3; i++ {
		repo.runtimes[fmt.Sprintf("rt-self-%d", i)] = domain.Runtime{
			RuntimeID:       fmt.Sprintf("rt-self-%d", i),
			CredentialKeyID: fmt.Sprintf("key-%d", i),
			UserID:          42,
			Name:            fmt.Sprintf("self-%d", i),
			Source:          domain.RuntimeSourceSelfHosted,
			ResourceProfile: "small",
			Status:          domain.RuntimeStatusActive,
			HeartbeatAt:     &fixedNow,
			CreatedAt:       fixedNow,
			UpdatedAt:       fixedNow,
		}
	}
	_, err := svc.RegisterRuntime(context.Background(), RegisterArgs{
		Source: domain.RuntimeSourceHosted, BindUserID: 42,
		EndpointHost: "h", GRPCPort: 60000, ResourceProfile: "small",
	})
	if err != nil {
		t.Fatalf("register hosted: %v", err)
	}

	res, err := svc.ListRuntimes(context.Background(), ListArgs{UserID: 42, SourceFilter: domain.RuntimeSourceHosted})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(res.Runtimes) != 1 {
		t.Errorf("expected 1 hosted runtime, got %d", len(res.Runtimes))
	}
}

func TestList_RequiresUserID(t *testing.T) {
	repo := newStubRepo()
	svc := makeService(repo, "pro", nil, config.RuntimePlatformConfig{}, fixedNow)
	_, err := svc.ListRuntimes(context.Background(), ListArgs{UserID: 0})
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("err = %v, want ErrInvalidArgument", err)
	}
}

// ── Fail-closed plan-lookup contract (#2) ───────────────────────────────────

// makeServiceWithLookup builds a Service wired to a caller-controlled
// PlanLookup, so tests can simulate account-service NotFound / Unavailable.
func makeServiceWithLookup(repo *stubRepo, lookup plan.PlanLookup) *Service {
	plans := map[string]config.RuntimePlan{
		"pro": {MaxHostedRuntimes: 5, MaxSelfHostedRuntimes: 5, MaxConcurrentSessionsTotal: 20, AllowedResourceProfiles: []string{"small", "medium", "large"}, AllowSelfHostedRuntime: true},
	}
	platform := config.RuntimePlatformConfig{
		DefaultPlanCode:            "pro",
		HeartbeatGraceSeconds:      30,
		CallerTokenTTLSeconds:      60,
		MaxTotalHostedRuntimes:     -1,
		MaxTotalSelfHostedRuntimes: -1,
	}
	resolver := plan.NewResolver(lookup, plans, platform)
	svc := New(repo, resolver, Config{
		HeartbeatGrace: 30 * time.Second,
		DeathGrace:     5 * time.Minute,
		CallerTokenTTL: 60 * time.Second,
	})
	clock := fixedNow
	svc.SetClock(func() time.Time { return clock })
	return svc
}

// errLookup is a plan.PlanLookup that always fails with the supplied error.
type errLookup struct{ err error }

func (e errLookup) GetUserPlanCode(_ context.Context, _ int64) (string, error) {
	return "", e.err
}

// TestRegister_AccountServiceNotFound_FailsClosed: hosted RegisterRuntime
// with bind_user_id=42 but account-service returns NotFound for that user
// MUST refuse the request rather than silently allocate a default plan.
func TestRegister_AccountServiceNotFound_FailsClosed(t *testing.T) {
	repo := newStubRepo()
	svc := makeServiceWithLookup(repo, errLookup{err: status.Error(codes.NotFound, "user not found")})

	_, err := svc.RegisterRuntime(context.Background(), RegisterArgs{
		Source:          domain.RuntimeSourceHosted,
		BindUserID:      42,
		EndpointHost:    "10.0.0.5",
		GRPCPort:        50053,
		ResourceProfile: "small",
		Name:            "default",
	})
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound (fail-closed for missing user)", err)
	}
}

// TestRegister_AccountServiceUnavailable_FailsClosed: account-service
// being unreachable MUST NOT result in a runtime being registered. The
// pre-fix behavior was to silently fall back to default plan (pro), which
// would let an arbitrary caller bind a hosted runtime to any user_id
// while account-service was down.
func TestRegister_AccountServiceUnavailable_FailsClosed(t *testing.T) {
	repo := newStubRepo()
	svc := makeServiceWithLookup(repo, errLookup{err: status.Error(codes.Unavailable, "boom")})

	_, err := svc.RegisterRuntime(context.Background(), RegisterArgs{
		Source:          domain.RuntimeSourceHosted,
		BindUserID:      42,
		EndpointHost:    "10.0.0.5",
		GRPCPort:        50053,
		ResourceProfile: "small",
		Name:            "default",
	})
	if !errors.Is(err, ErrPlanLookupUnavailable) {
		t.Fatalf("err = %v, want ErrPlanLookupUnavailable", err)
	}
}

// ── Quota 0 semantics (#3) ────────────────────────────────────────────────

// TestRegister_Hosted_RejectsDisallowedResourceProfile: hosted register
// must check resource_profile against plan.AllowedResourceProfiles. The
// caller (hosted-runtime provisioner) is trusted but not infallible, and
// this is the last line of defense.
func TestRegister_Hosted_RejectsDisallowedResourceProfile(t *testing.T) {
	repo := newStubRepo()
	svc := makeService(repo, "free", nil, config.RuntimePlatformConfig{}, fixedNow)
	// "free" plan allows only "small" per stubs_test.go default; request "large".
	_, err := svc.RegisterRuntime(context.Background(), RegisterArgs{
		Source: domain.RuntimeSourceHosted, BindUserID: 42,
		EndpointHost: "h", GRPCPort: 50053, ResourceProfile: "large",
	})
	if !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("err = %v, want ErrQuotaExceeded for disallowed profile", err)
	}
}

// ── EnsureHostedRuntime (Phase D1 section 5) ──────────────────────────────

// TestEnsureHostedRuntime_HappyPath: a fresh user, no runtime yet,
// provisioner succeeds and the runtime self-registers immediately.
// Expect Provisioned=true and a fresh route.
func TestEnsureHostedRuntime_HappyPath(t *testing.T) {
	repo := newStubRepo()
	prov := &fakeProvisioner{repo: repo, onProvision: "ok", now: func() time.Time { return fixedNow }}
	svc := makeServiceWithProvisioner(repo, "pro", prov, fixedNow, 5)

	res, err := svc.EnsureHostedRuntime(context.Background(), EnsureHostedRuntimeArgs{
		UserID: 42, Name: "default", ResourceProfile: "small",
	})
	if err != nil {
		t.Fatalf("EnsureHostedRuntime: %v", err)
	}
	if !res.Provisioned {
		t.Errorf("Provisioned = false, want true (fresh provision)")
	}
	if res.GRPCEndpoint == "" {
		t.Errorf("GRPCEndpoint empty")
	}
	if prov.calls != 1 {
		t.Errorf("provisioner calls = %d, want 1", prov.calls)
	}
}

func TestEnsureHostedRuntime_InjectsHostedInternalCredential(t *testing.T) {
	repo := newStubRepo()
	prov := &fakeProvisioner{repo: repo, onProvision: "ok", now: func() time.Time { return fixedNow }}
	issuer := &fakeHostedCredentialIssuer{
		issued: domain.IssuedCredential{
			RuntimeCredential: domain.RuntimeCredential{
				KeyID:          "hosted-internal-key",
				Role:           domain.CredentialRoleExecutor,
				HostedInternal: true,
			},
			PrivateKeyPEM: "hosted-private-key",
		},
	}
	svc := makeServiceWithProvisioner(repo, "pro", prov, fixedNow, 5)
	svc.hostedCredentialIssuer = issuer

	_, err := svc.EnsureHostedRuntime(context.Background(), EnsureHostedRuntimeArgs{
		UserID: 42, Name: "hosted-credential-test", ResourceProfile: "small",
	})
	if err != nil {
		t.Fatalf("EnsureHostedRuntime: %v", err)
	}
	if issuer.calls != 1 {
		t.Fatalf("hosted credential issuer calls = %d, want 1", issuer.calls)
	}
	if prov.lastPlan.RuntimeCredentialKeyID != "hosted-internal-key" {
		t.Fatalf("plan credential key = %q, want hosted-internal-key", prov.lastPlan.RuntimeCredentialKeyID)
	}
	if prov.lastPlan.RuntimeCredentialPrivateKeyPEM != "hosted-private-key" {
		t.Fatalf("plan credential private key not injected")
	}
}

func TestEnsureHostedRuntime_AutoGeneratesHostedName(t *testing.T) {
	repo := newStubRepo()
	prov := &fakeProvisioner{repo: repo, onProvision: "ok", now: func() time.Time { return fixedNow }}
	svc := makeServiceWithProvisioner(repo, "pro", prov, fixedNow, 5)

	res, err := svc.EnsureHostedRuntime(context.Background(), EnsureHostedRuntimeArgs{
		UserID: 42, ResourceProfile: "small",
	})
	if err != nil {
		t.Fatalf("EnsureHostedRuntime: %v", err)
	}
	if !validRuntimeName(res.Runtime.Name) || len(res.Runtime.Name) < len("hosted--") || res.Runtime.Name[:7] != "hosted-" {
		t.Fatalf("generated name = %q, want hosted-* valid runtime name", res.Runtime.Name)
	}
}

func TestEnsureHostedRuntime_AutoNameReusesExistingHealthyHostedRuntime(t *testing.T) {
	repo := newStubRepo()
	prov := &fakeProvisioner{repo: repo, onProvision: "ok", now: func() time.Time { return fixedNow }}
	svc := makeServiceWithProvisioner(repo, "pro", prov, fixedNow, 5)
	heartbeat := fixedNow
	if err := repo.CreateRuntime(context.Background(), domain.Runtime{
		RuntimeID:       "rt_existing_hosted",
		UserID:          42,
		Name:            "hosted-existing",
		Source:          domain.RuntimeSourceHosted,
		Role:            domain.CredentialRoleExecutor,
		EndpointHost:    "127.0.0.1",
		GRPCPort:        50106,
		ResourceProfile: "small",
		Status:          domain.RuntimeStatusActive,
		HeartbeatAt:     &heartbeat,
		CreatedAt:       fixedNow,
		UpdatedAt:       fixedNow,
	}); err != nil {
		t.Fatalf("CreateRuntime: %v", err)
	}
	attachRuntimeOwner(t, repo, "rt_existing_hosted", fixedNow)

	res, err := svc.EnsureHostedRuntime(context.Background(), EnsureHostedRuntimeArgs{
		UserID: 42, ResourceProfile: "small",
	})
	if err != nil {
		t.Fatalf("EnsureHostedRuntime: %v", err)
	}
	if res.Provisioned {
		t.Fatalf("Provisioned = true, want false")
	}
	if res.Runtime.RuntimeID != "rt_existing_hosted" {
		t.Fatalf("runtime_id = %q, want rt_existing_hosted", res.Runtime.RuntimeID)
	}
	if prov.calls != 0 {
		t.Fatalf("provisioner calls = %d, want 0", prov.calls)
	}
}

func TestEnsureHostedRuntime_ManualDifferentNameConflictsWithExistingHostedSlot(t *testing.T) {
	repo := newStubRepo()
	prov := &fakeProvisioner{repo: repo, onProvision: "ok", now: func() time.Time { return fixedNow }}
	svc := makeServiceWithProvisioner(repo, "pro", prov, fixedNow, 5)
	heartbeat := fixedNow
	if err := repo.CreateRuntime(context.Background(), domain.Runtime{
		RuntimeID:       "rt_existing_hosted",
		UserID:          42,
		Name:            "hosted-existing",
		Source:          domain.RuntimeSourceHosted,
		Role:            domain.CredentialRoleExecutor,
		EndpointHost:    "127.0.0.1",
		GRPCPort:        50106,
		ResourceProfile: "small",
		Status:          domain.RuntimeStatusActive,
		HeartbeatAt:     &heartbeat,
		CreatedAt:       fixedNow,
		UpdatedAt:       fixedNow,
	}); err != nil {
		t.Fatalf("CreateRuntime: %v", err)
	}

	_, err := svc.EnsureHostedRuntime(context.Background(), EnsureHostedRuntimeArgs{
		UserID: 42, Name: "hosted-new", ResourceProfile: "small",
	})
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("err = %v, want ErrConflict", err)
	}
	if prov.calls != 0 {
		t.Fatalf("provisioner calls = %d, want 0", prov.calls)
	}
}

func TestEnsureHostedRuntime_AutoNameSkipsConflicts(t *testing.T) {
	repo := newStubRepo()
	svc := makeService(repo, "pro", nil, config.RuntimePlatformConfig{}, fixedNow)
	available := hostedRuntimeNameForAttempt(17)
	total := len(hostedNameAdjectives) * len(hostedNameNouns)
	for i := 0; i < total; i++ {
		name := hostedRuntimeNameForAttempt(i)
		if name == available {
			continue
		}
		repo.runtimes[fmt.Sprintf("rt_name_%d", i)] = domain.Runtime{
			RuntimeID: fmt.Sprintf("rt_name_%d", i),
			UserID:    42,
			Name:      name,
			Source:    domain.RuntimeSourceHosted,
			Status:    domain.RuntimeStatusActive,
			CreatedAt: fixedNow,
			UpdatedAt: fixedNow,
		}
	}

	got, err := svc.generateAvailableHostedRuntimeName(context.Background(), 42)
	if err != nil {
		t.Fatalf("generateAvailableHostedRuntimeName: %v", err)
	}
	if got != available {
		t.Fatalf("name = %q, want only available name %q", got, available)
	}
}

func TestEnsureHostedRuntime_AutoNameSkipsEndedHistoricalNames(t *testing.T) {
	repo := newStubRepo()
	svc := makeService(repo, "pro", nil, config.RuntimePlatformConfig{}, fixedNow)
	available := hostedRuntimeNameForAttempt(9)
	total := len(hostedNameAdjectives) * len(hostedNameNouns)
	for i := 0; i < total; i++ {
		name := hostedRuntimeNameForAttempt(i)
		if name == available {
			continue
		}
		endedAt := fixedNow.Add(-time.Hour)
		repo.runtimes[fmt.Sprintf("rt_ended_name_%d", i)] = domain.Runtime{
			RuntimeID:   fmt.Sprintf("rt_ended_name_%d", i),
			UserID:      42,
			Name:        name,
			Source:      domain.RuntimeSourceHosted,
			Status:      domain.RuntimeStatusEnded,
			EndedAt:     &endedAt,
			EndedReason: domain.RuntimeEndedReasonUserCancelled,
			CreatedAt:   fixedNow.Add(-2 * time.Hour),
			UpdatedAt:   endedAt,
		}
	}

	got, err := svc.generateAvailableHostedRuntimeName(context.Background(), 42)
	if err != nil {
		t.Fatalf("generateAvailableHostedRuntimeName: %v", err)
	}
	if got != available {
		t.Fatalf("name = %q, want only available name %q", got, available)
	}
}

func TestEnsureHostedRuntime_RejectsInvalidManualName(t *testing.T) {
	repo := newStubRepo()
	prov := &fakeProvisioner{repo: repo, onProvision: "ok", now: func() time.Time { return fixedNow }}
	svc := makeServiceWithProvisioner(repo, "pro", prov, fixedNow, 5)

	_, err := svc.EnsureHostedRuntime(context.Background(), EnsureHostedRuntimeArgs{
		UserID: 42, Name: "Bad_Name", ResourceProfile: "small",
	})
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("err = %v, want ErrInvalidArgument", err)
	}
	if prov.calls != 0 {
		t.Fatalf("provisioner calls = %d, want 0", prov.calls)
	}
}

func TestEnsureHostedRuntime_ManualNameConflictsWithEndedHistoryBeforeProvisioning(t *testing.T) {
	repo := newStubRepo()
	prov := &fakeProvisioner{repo: repo, onProvision: "ok", now: func() time.Time { return fixedNow }}
	svc := makeServiceWithProvisioner(repo, "pro", prov, fixedNow, 5)
	endedAt := fixedNow.Add(-time.Hour)
	repo.runtimes["rt_ended_manual_name"] = domain.Runtime{
		RuntimeID:   "rt_ended_manual_name",
		UserID:      42,
		Name:        "ended-manual",
		Source:      domain.RuntimeSourceHosted,
		Status:      domain.RuntimeStatusEnded,
		EndedAt:     &endedAt,
		EndedReason: domain.RuntimeEndedReasonUserCancelled,
		CreatedAt:   fixedNow.Add(-2 * time.Hour),
		UpdatedAt:   endedAt,
	}

	_, err := svc.EnsureHostedRuntime(context.Background(), EnsureHostedRuntimeArgs{
		UserID: 42, Name: "ended-manual", ResourceProfile: "small",
	})
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("err = %v, want ErrConflict", err)
	}
	if prov.calls != 0 {
		t.Fatalf("provisioner calls = %d, want 0 before historical name conflict is resolved", prov.calls)
	}
}

func TestEnsureHostedRuntime_ManualNameConflictBeyondFirstPage(t *testing.T) {
	repo := newStubRepo()
	prov := &fakeProvisioner{repo: repo, onProvision: "ok", now: func() time.Time { return fixedNow }}
	svc := makeServiceWithProvisioner(repo, "pro", prov, fixedNow, 5)
	for i := 0; i < 205; i++ {
		repo.runtimes[fmt.Sprintf("rt_filler_%03d", i)] = domain.Runtime{
			RuntimeID: fmt.Sprintf("rt_filler_%03d", i),
			UserID:    42,
			Name:      fmt.Sprintf("filler-%03d", i),
			Source:    domain.RuntimeSourceHosted,
			Status:    domain.RuntimeStatusEnded,
			CreatedAt: fixedNow.Add(time.Duration(i) * time.Second),
			UpdatedAt: fixedNow.Add(time.Duration(i) * time.Second),
		}
	}
	repo.runtimes["rt_old_conflict"] = domain.Runtime{
		RuntimeID: "rt_old_conflict",
		UserID:    42,
		Name:      "old-conflict",
		Source:    domain.RuntimeSourceHosted,
		Status:    domain.RuntimeStatusEnded,
		CreatedAt: fixedNow.Add(-time.Hour),
		UpdatedAt: fixedNow.Add(-time.Hour),
	}

	_, err := svc.EnsureHostedRuntime(context.Background(), EnsureHostedRuntimeArgs{
		UserID: 42, Name: "old-conflict", ResourceProfile: "small",
	})
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("err = %v, want ErrConflict", err)
	}
	if prov.calls != 0 {
		t.Fatalf("provisioner calls = %d, want 0 before paged historical name conflict is resolved", prov.calls)
	}
}

func TestEnsureHostedRuntime_AutoNameSkipsConflictBeyondFirstPage(t *testing.T) {
	repo := newStubRepo()
	svc := makeService(repo, "pro", nil, config.RuntimePlatformConfig{}, fixedNow)
	available := hostedRuntimeNameForAttempt(21)
	for i := 0; i < 205; i++ {
		repo.runtimes[fmt.Sprintf("rt_filler_auto_%03d", i)] = domain.Runtime{
			RuntimeID: fmt.Sprintf("rt_filler_auto_%03d", i),
			UserID:    42,
			Name:      fmt.Sprintf("filler-auto-%03d", i),
			Source:    domain.RuntimeSourceHosted,
			Status:    domain.RuntimeStatusEnded,
			CreatedAt: fixedNow.Add(time.Duration(i) * time.Second),
			UpdatedAt: fixedNow.Add(time.Duration(i) * time.Second),
		}
	}
	total := len(hostedNameAdjectives) * len(hostedNameNouns)
	for i := 0; i < total; i++ {
		name := hostedRuntimeNameForAttempt(i)
		if name == available {
			continue
		}
		repo.runtimes[fmt.Sprintf("rt_auto_name_%03d", i)] = domain.Runtime{
			RuntimeID: fmt.Sprintf("rt_auto_name_%03d", i),
			UserID:    42,
			Name:      name,
			Source:    domain.RuntimeSourceHosted,
			Status:    domain.RuntimeStatusEnded,
			CreatedAt: fixedNow.Add(-time.Duration(i+1) * time.Hour),
			UpdatedAt: fixedNow.Add(-time.Duration(i+1) * time.Hour),
		}
	}

	got, err := svc.generateAvailableHostedRuntimeName(context.Background(), 42)
	if err != nil {
		t.Fatalf("generateAvailableHostedRuntimeName: %v", err)
	}
	if got != available {
		t.Fatalf("name = %q, want only available name %q", got, available)
	}
}

// TestEnsureHostedRuntime_ReusesHealthyRuntime: when a healthy runtime
// already exists for (user, name), EnsureHostedRuntime returns
// it without provisioning. Provisioned=false signals reuse.
func TestEnsureHostedRuntime_ReusesHealthyRuntime(t *testing.T) {
	repo := newStubRepo()
	prov := &fakeProvisioner{repo: repo, onProvision: "ok", now: func() time.Time { return fixedNow }}
	svc := makeServiceWithProvisioner(repo, "pro", prov, fixedNow, 5)

	// Plant a healthy runtime row.
	now := fixedNow
	repo.runtimes["rt_existing"] = domain.Runtime{
		RuntimeID: "rt_existing", UserID: 42, Name: "default",
		Source:       domain.RuntimeSourceHosted,
		EndpointHost: "127.0.0.1", GRPCPort: 50142, ResourceProfile: "small",
		Status: domain.RuntimeStatusActive, HeartbeatAt: &now,
		CreatedAt: now, UpdatedAt: now,
	}
	attachRuntimeOwner(t, repo, "rt_existing", fixedNow)

	res, err := svc.EnsureHostedRuntime(context.Background(), EnsureHostedRuntimeArgs{
		UserID: 42, Name: "default", ResourceProfile: "small",
	})
	if err != nil {
		t.Fatalf("EnsureHostedRuntime: %v", err)
	}
	if res.Provisioned {
		t.Errorf("Provisioned = true, want false (reuse path)")
	}
	if res.Runtime.RuntimeID != "rt_existing" {
		t.Errorf("RuntimeID = %q, want rt_existing", res.Runtime.RuntimeID)
	}
	if prov.calls != 0 {
		t.Errorf("provisioner calls = %d, want 0 (reuse should skip provisioning)", prov.calls)
	}
}

func TestEnsureHostedRuntime_DuplicateNameConflictsAcrossSources(t *testing.T) {
	repo := newStubRepo()
	prov := &fakeProvisioner{repo: repo, onProvision: "ok", now: func() time.Time { return fixedNow }}
	svc := makeServiceWithProvisioner(repo, "pro", prov, fixedNow, 5)

	now := fixedNow
	repo.runtimes["rt-self"] = domain.Runtime{
		RuntimeID:       "rt-self",
		CredentialKeyID: "key-1",
		UserID:          42,
		Name:            "default",
		Source:          domain.RuntimeSourceSelfHosted,
		ResourceProfile: "small",
		Status:          domain.RuntimeStatusActive,
		HeartbeatAt:     &now,
		CreatedAt:       now,
		UpdatedAt:       now,
	}

	_, err := svc.EnsureHostedRuntime(context.Background(), EnsureHostedRuntimeArgs{
		UserID: 42, Name: "default", ResourceProfile: "small",
	})
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("err = %v, want ErrConflict", err)
	}
	if prov.calls != 0 {
		t.Fatalf("provisioner calls = %d, want 0", prov.calls)
	}
	self, err := repo.GetRuntime(context.Background(), "rt-self")
	if err != nil {
		t.Fatalf("lookup self-hosted: %v", err)
	}
	if self.Status != domain.RuntimeStatusActive {
		t.Fatalf("self-hosted status = %q, want active", self.Status)
	}
}

// TestEnsureHostedRuntime_QuotaExceeded: a free user (cap=1) already at
// quota cannot get another runtime even via lazy provisioning.
func TestEnsureHostedRuntime_QuotaExceeded(t *testing.T) {
	repo := newStubRepo()
	prov := &fakeProvisioner{repo: repo, onProvision: "ok", now: func() time.Time { return fixedNow }}
	svc := makeServiceWithProvisioner(repo, "free", prov, fixedNow, 5)

	// Plant the user's existing hosted runtime at the cap (free=1).
	now := fixedNow
	repo.runtimes["rt_already"] = domain.Runtime{
		RuntimeID: "rt_already", UserID: 42, Name: "alt",
		Source:       domain.RuntimeSourceHosted,
		EndpointHost: "127.0.0.1", GRPCPort: 50100, ResourceProfile: "small",
		Status: domain.RuntimeStatusActive, HeartbeatAt: &now,
		CreatedAt: now, UpdatedAt: now,
	}

	_, err := svc.EnsureHostedRuntime(context.Background(), EnsureHostedRuntimeArgs{
		UserID: 42, Name: "default", ResourceProfile: "small",
	})
	if !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("err = %v, want ErrQuotaExceeded", err)
	}
	if prov.calls != 0 {
		t.Errorf("provisioner calls = %d, want 0 (quota check must precede provision)", prov.calls)
	}
}

// TestEnsureHostedRuntime_DisallowedProfile: free plan with
// AllowedResourceProfiles=["small"] cannot request "large".
func TestEnsureHostedRuntime_DisallowedProfile(t *testing.T) {
	repo := newStubRepo()
	prov := &fakeProvisioner{repo: repo, onProvision: "ok", now: func() time.Time { return fixedNow }}
	svc := makeServiceWithProvisioner(repo, "free", prov, fixedNow, 5)

	_, err := svc.EnsureHostedRuntime(context.Background(), EnsureHostedRuntimeArgs{
		UserID: 42, Name: "default", ResourceProfile: "large",
	})
	if !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("err = %v, want ErrQuotaExceeded for disallowed profile", err)
	}
	if prov.calls != 0 {
		t.Errorf("provisioner calls = %d, want 0", prov.calls)
	}
}

// TestEnsureHostedRuntime_UnknownProfile: a profile permitted by the
// plan but missing from provisioning.profiles config is rejected with
// InvalidArgument so operators see the misconfig clearly.
func TestEnsureHostedRuntime_UnknownProfile(t *testing.T) {
	repo := newStubRepo()
	prov := &fakeProvisioner{repo: repo, onProvision: "ok", now: func() time.Time { return fixedNow }}

	// Custom plan permits "weird" but provisioning.profiles only has
	// small/medium/large.
	plans := map[string]config.RuntimePlan{
		"pro": {
			MaxHostedRuntimes:       -1,
			AllowedResourceProfiles: []string{"weird"},
		},
	}
	platform := config.RuntimePlatformConfig{
		DefaultPlanCode:        "pro",
		HeartbeatGraceSeconds:  30,
		MaxTotalHostedRuntimes: -1,
	}
	resolver := plan.NewResolver(constLookup{code: "pro"}, plans, platform)
	provCfg := config.ProvisioningConfig{
		Image: "x", AdvertiseHost: "127.0.0.1",
		PortRangeBase: 50100, PortRangeSize: 100,
		RegistrationTimeoutSeconds: 5,
		Profiles: map[string]config.ResourceProfile{
			"small": {NanoCPUs: "0.5", MemoryMB: 512},
		},
	}
	svc := New(repo, resolver, Config{
		HeartbeatGrace: 30 * time.Second, DeathGrace: 5 * time.Minute, CallerTokenTTL: 60 * time.Second,
		Provisioning: provCfg, Provisioner: prov,
	})
	clock := fixedNow
	svc.SetClock(func() time.Time { return clock })

	_, err := svc.EnsureHostedRuntime(context.Background(), EnsureHostedRuntimeArgs{
		UserID: 42, Name: "default", ResourceProfile: "weird",
	})
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("err = %v, want ErrInvalidArgument", err)
	}
}

// TestEnsureHostedRuntime_RegistrationTimeout: provisioner reports
// success but the runtime never calls RegisterRuntime back. Service
// fails closed with ErrRegistrationTimeout and Deprovisions the
// half-started container.
func TestEnsureHostedRuntime_RegistrationTimeout(t *testing.T) {
	repo := newStubRepo()
	prov := &fakeProvisioner{repo: repo, onProvision: "ok_no_register"}
	// 1s registration timeout so the test wraps within a few seconds.
	svc := makeServiceWithProvisioner(repo, "pro", prov, fixedNow, 1)
	// Don't lock the clock — waitForRegistration uses time-based deadline.
	svc.SetClock(time.Now)

	_, err := svc.EnsureHostedRuntime(context.Background(), EnsureHostedRuntimeArgs{
		UserID: 42, Name: "default", ResourceProfile: "small",
	})
	if !errors.Is(err, ErrRegistrationTimeout) {
		t.Fatalf("err = %v, want ErrRegistrationTimeout", err)
	}
	if prov.calls != 1 {
		t.Errorf("provisioner calls = %d, want 1", prov.calls)
	}
	if prov.deprovisions != 1 {
		t.Errorf("provisioner deprovisions = %d, want 1 (cleanup on timeout)", prov.deprovisions)
	}
}

func TestEnsureHostedRuntime_RegistrationWithoutChannelOwnerTimesOut(t *testing.T) {
	repo := newStubRepo()
	prov := &fakeProvisioner{repo: repo, onProvision: "ok_no_owner"}
	svc := makeServiceWithProvisioner(repo, "pro", prov, time.Time{}, 1)

	_, err := svc.EnsureHostedRuntime(context.Background(), EnsureHostedRuntimeArgs{
		UserID: 42, Name: "ownerless", ResourceProfile: "small",
	})
	if !errors.Is(err, ErrRegistrationTimeout) {
		t.Fatalf("err = %v, want ErrRegistrationTimeout", err)
	}
	if prov.deprovisions != 1 {
		t.Fatalf("deprovisions = %d, want 1", prov.deprovisions)
	}
}

// TestEnsureHostedRuntime_BackendNotConfigured: NoOpProvisioner is the
// default; calling EnsureHostedRuntime without wiring a real backend
// fails closed with ErrProvisionerUnavailable.
func TestEnsureHostedRuntime_BackendNotConfigured(t *testing.T) {
	repo := newStubRepo()
	prov := &fakeProvisioner{repo: repo, onProvision: "not_configured"}
	svc := makeServiceWithProvisioner(repo, "pro", prov, fixedNow, 5)

	_, err := svc.EnsureHostedRuntime(context.Background(), EnsureHostedRuntimeArgs{
		UserID: 42, Name: "default", ResourceProfile: "small",
	})
	if !errors.Is(err, ErrProvisionerUnavailable) {
		t.Fatalf("err = %v, want ErrProvisionerUnavailable", err)
	}
}

// TestEnsureHostedRuntime_ReusesFreshButNotYetHeartbeating: a runtime
// that just registered (status=paired, HeartbeatAt=nil, UpdatedAt=now)
// must be reusable for the heartbeatGrace window. Pre-fix the second
// EnsureHostedRuntime call inside that window would re-provision and
// end the just-launched container — bug #11.
func TestEnsureHostedRuntime_ReusesFreshButNotYetHeartbeating(t *testing.T) {
	repo := newStubRepo()
	prov := &fakeProvisioner{repo: repo, onProvision: "ok", now: func() time.Time { return fixedNow }}
	svc := makeServiceWithProvisioner(repo, "pro", prov, fixedNow, 5)

	// Plant a freshly-registered runtime: paired, no heartbeat yet,
	// updated_at = now (just landed via section-4 self-register).
	repo.runtimes["rt_fresh"] = domain.Runtime{
		RuntimeID: "rt_fresh", UserID: 42, Name: "default",
		Source:       domain.RuntimeSourceHosted,
		EndpointHost: "127.0.0.1", GRPCPort: 50142, ResourceProfile: "small",
		Status: domain.RuntimeStatusPaired, HeartbeatAt: nil,
		CreatedAt: fixedNow, UpdatedAt: fixedNow,
	}
	attachRuntimeOwner(t, repo, "rt_fresh", fixedNow)

	res, err := svc.EnsureHostedRuntime(context.Background(), EnsureHostedRuntimeArgs{
		UserID: 42, Name: "default", ResourceProfile: "small",
	})
	if err != nil {
		t.Fatalf("EnsureHostedRuntime: %v", err)
	}
	if res.Provisioned {
		t.Errorf("Provisioned = true, want false (should reuse fresh runtime, not re-provision)")
	}
	if res.Runtime.RuntimeID != "rt_fresh" {
		t.Errorf("RuntimeID = %q, want rt_fresh", res.Runtime.RuntimeID)
	}
	if prov.calls != 0 {
		t.Errorf("provisioner calls = %d, want 0 (must not re-provision a fresh runtime)", prov.calls)
	}
}

// TestEnsureHostedRuntime_StaleNoHeartbeatStillOccupiesSlot: a stale paired
// runtime still owns the hosted slot until the user/operator explicitly
// ends it.
func TestEnsureHostedRuntime_StaleNoHeartbeatStillOccupiesSlot(t *testing.T) {
	repo := newStubRepo()
	prov := &fakeProvisioner{repo: repo, onProvision: "ok", now: func() time.Time { return fixedNow }}
	svc := makeServiceWithProvisioner(repo, "pro", prov, fixedNow, 5)

	// Stale paired runtime: updated_at way before heartbeat grace window.
	staleAt := fixedNow.Add(-10 * time.Minute)
	repo.runtimes["rt_stale"] = domain.Runtime{
		RuntimeID: "rt_stale", UserID: 42, Name: "default",
		Source:       domain.RuntimeSourceHosted,
		EndpointHost: "127.0.0.1", GRPCPort: 50142, ResourceProfile: "small",
		Status: domain.RuntimeStatusPaired, HeartbeatAt: nil,
		CreatedAt: staleAt, UpdatedAt: staleAt,
	}

	_, err := svc.EnsureHostedRuntime(context.Background(), EnsureHostedRuntimeArgs{
		UserID: 42, Name: "default", ResourceProfile: "small",
	})
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("err = %v, want ErrConflict", err)
	}
	if prov.calls != 0 {
		t.Errorf("provisioner calls = %d, want 0", prov.calls)
	}
}

// TestEnsureHostedRuntime_RejectsZeroUserID
func TestEnsureHostedRuntime_RejectsZeroUserID(t *testing.T) {
	repo := newStubRepo()
	prov := &fakeProvisioner{repo: repo, onProvision: "ok"}
	svc := makeServiceWithProvisioner(repo, "pro", prov, fixedNow, 5)
	_, err := svc.EnsureHostedRuntime(context.Background(), EnsureHostedRuntimeArgs{UserID: 0})
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("err = %v, want ErrInvalidArgument", err)
	}
}

// ── ValidateCallerToken (Phase D1 section 6.5) ────────────────────────────

// TestValidateCallerToken_ResolveThenValidate proves the round trip
// quant-handler + strategy-runtime use:
//  1. handler calls ResolveRuntimeRouteByID → gets caller_token
//  2. handler dials runtime, attaches token in metadata
//  3. runtime's interceptor calls ValidateCallerToken → ok
func TestValidateCallerToken_ResolveThenValidate(t *testing.T) {
	repo := newStubRepo()
	svc := makeService(repo, "pro", nil, config.RuntimePlatformConfig{}, fixedNow)

	// Plant a healthy runtime.
	now := fixedNow
	repo.runtimes["rt_42_default"] = domain.Runtime{
		RuntimeID: "rt_42_default", UserID: 42, Name: "default",
		Source:       domain.RuntimeSourceHosted,
		EndpointHost: "10.0.0.5", GRPCPort: 50053, ResourceProfile: "small",
		Status: domain.RuntimeStatusActive, HeartbeatAt: &now,
		CreatedAt: now, UpdatedAt: now,
	}
	attachRuntimeOwner(t, repo, "rt_42_default", fixedNow)

	resolveResult, err := svc.ResolveRuntimeRouteByID(context.Background(), ResolveByIDArgs{UserID: 42, RuntimeID: "rt_42_default"})
	if err != nil {
		t.Fatalf("ResolveRuntimeRouteByID: %v", err)
	}
	if resolveResult.CallerToken == "" {
		t.Fatal("CallerToken not issued")
	}

	// Step 3: validate the issued token against the runtime that
	// received the call.
	res, err := svc.ValidateCallerToken(context.Background(), ValidateCallerTokenArgs{
		Token:     resolveResult.CallerToken,
		RuntimeID: "rt_42_default",
	})
	if err != nil {
		t.Fatalf("ValidateCallerToken: %v", err)
	}
	if !res.Valid {
		t.Errorf("Valid = false reason=%q, want true", res.Reason)
	}
	if res.UserID != 42 {
		t.Errorf("UserID = %d, want 42", res.UserID)
	}
}

// TestValidateCallerToken_RuntimeMismatch_RejectsCrossRuntimeUse: a
// token issued for runtime A must not validate when presented by
// runtime B. Defense against a compromised runtime trying to forward
// tokens to its peers.
func TestValidateCallerToken_RuntimeMismatch_RejectsCrossRuntimeUse(t *testing.T) {
	repo := newStubRepo()
	svc := makeService(repo, "pro", nil, config.RuntimePlatformConfig{}, fixedNow)

	now := fixedNow
	repo.runtimes["rt_a"] = domain.Runtime{
		RuntimeID: "rt_a", UserID: 42, Name: "a",
		Source:       domain.RuntimeSourceHosted,
		EndpointHost: "h", GRPCPort: 1, ResourceProfile: "small",
		Status: domain.RuntimeStatusActive, HeartbeatAt: &now,
		CreatedAt: now, UpdatedAt: now,
	}
	attachRuntimeOwner(t, repo, "rt_a", fixedNow)

	resolveResult, err := svc.ResolveRuntimeRouteByID(context.Background(), ResolveByIDArgs{UserID: 42, RuntimeID: "rt_a"})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}

	res, err := svc.ValidateCallerToken(context.Background(), ValidateCallerTokenArgs{
		Token:     resolveResult.CallerToken,
		RuntimeID: "rt_b", // different runtime
	})
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if res.Valid {
		t.Errorf("Valid = true; expected runtime_mismatch rejection")
	}
	if res.Reason != "runtime_mismatch" {
		t.Errorf("Reason = %q, want runtime_mismatch", res.Reason)
	}
}

func TestValidateCallerToken_UnknownToken(t *testing.T) {
	repo := newStubRepo()
	svc := makeService(repo, "pro", nil, config.RuntimePlatformConfig{}, fixedNow)
	res, err := svc.ValidateCallerToken(context.Background(), ValidateCallerTokenArgs{
		Token: "definitely-not-issued", RuntimeID: "rt_x",
	})
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if res.Valid {
		t.Error("Valid=true for unknown token")
	}
	if res.Reason != "unknown" {
		t.Errorf("Reason = %q, want unknown", res.Reason)
	}
}

func TestValidateCallerToken_EmptyToken(t *testing.T) {
	repo := newStubRepo()
	svc := makeService(repo, "pro", nil, config.RuntimePlatformConfig{}, fixedNow)
	res, err := svc.ValidateCallerToken(context.Background(), ValidateCallerTokenArgs{
		Token: "", RuntimeID: "rt_x",
	})
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if res.Valid {
		t.Error("Valid=true for empty token")
	}
	if res.Reason != "unknown" {
		t.Errorf("Reason = %q, want unknown", res.Reason)
	}
}

// ── Hosted RegisterRuntime slot admission ──────────────────────────────────

func TestRegister_HostedOccupiedSlotConflictsWithoutMutation(t *testing.T) {
	repo := newStubRepo()
	svc := makeService(repo, "pro", nil, config.RuntimePlatformConfig{}, fixedNow)

	first, err := svc.RegisterRuntime(context.Background(), RegisterArgs{
		Source: domain.RuntimeSourceHosted, BindUserID: 42,
		Name: "default", EndpointHost: "10.0.0.5", GRPCPort: 50053,
		ResourceProfile: "small",
	})
	if err != nil {
		t.Fatalf("first register: %v", err)
	}

	_, err = svc.RegisterRuntime(context.Background(), RegisterArgs{
		Source: domain.RuntimeSourceHosted, BindUserID: 42,
		Name: "default", EndpointHost: "10.0.0.5", GRPCPort: 50053,
		ResourceProfile: "small",
	})
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("second register err = %v, want ErrConflict", err)
	}

	got1, err := repo.GetRuntime(context.Background(), first.Runtime.RuntimeID)
	if err != nil {
		t.Fatalf("lookup first: %v", err)
	}
	if got1.Status != domain.RuntimeStatusStarting {
		t.Errorf("first status = %q, want starting", got1.Status)
	}
	counts, err := repo.CountRuntimesByUser(context.Background(), 42)
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if counts.Hosted != 1 {
		t.Errorf("hosted count = %d, want 1", counts.Hosted)
	}
}

func TestRegister_HostedEndedRuntimeKeepsManualNameConflict(t *testing.T) {
	repo := newStubRepo()
	svc := makeService(repo, "pro", nil, config.RuntimePlatformConfig{}, fixedNow)

	first, err := svc.RegisterRuntime(context.Background(), RegisterArgs{
		Source: domain.RuntimeSourceHosted, BindUserID: 42,
		Name: "default", EndpointHost: "10.0.0.5", GRPCPort: 50053,
		ResourceProfile: "small",
	})
	if err != nil {
		t.Fatalf("first register: %v", err)
	}
	if _, err := svc.EndRuntime(context.Background(), EndRuntimeArgs{UserID: 42, RuntimeID: first.Runtime.RuntimeID}); err != nil {
		t.Fatalf("end first: %v", err)
	}

	_, err = svc.RegisterRuntime(context.Background(), RegisterArgs{
		Source: domain.RuntimeSourceHosted, BindUserID: 42,
		Name: "default", EndpointHost: "10.0.0.5", GRPCPort: 50053,
		ResourceProfile: "small",
	})
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("second register after end err = %v, want ErrConflict", err)
	}
	got1, err := repo.GetRuntime(context.Background(), first.Runtime.RuntimeID)
	if err != nil {
		t.Fatalf("lookup first: %v", err)
	}
	if got1.Status != domain.RuntimeStatusCancelled {
		t.Errorf("first status = %q, want cancelled", got1.Status)
	}
	counts, err := repo.CountRuntimesByUser(context.Background(), 42)
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if counts.Hosted != 0 {
		t.Errorf("hosted count = %d, want 0 (ended row should be excluded)", counts.Hosted)
	}
}

// TestResolveByID_AccountServiceNotFound_FailsClosed: resolving for a user
// that doesn't exist anymore MUST surface NotFound — not return a stale
// runtime that happens to still be in registry.
func TestResolveByID_AccountServiceNotFound_FailsClosed(t *testing.T) {
	repo := newStubRepo()
	// Plant a healthy runtime for user 42 directly in the stub.
	now := fixedNow
	repo.runtimes["rt_42_default"] = domain.Runtime{
		RuntimeID:    "rt_42_default",
		UserID:       42,
		Name:         "default",
		Source:       domain.RuntimeSourceHosted,
		EndpointHost: "10.0.0.5", GRPCPort: 50053,
		ResourceProfile: "small",
		Status:          domain.RuntimeStatusActive,
		HeartbeatAt:     &now,
		CreatedAt:       now, UpdatedAt: now,
	}
	attachRuntimeOwner(t, repo, "rt_42_default", fixedNow)
	svc := makeServiceWithLookup(repo, errLookup{err: status.Error(codes.NotFound, "user not found")})

	_, err := svc.ResolveRuntimeRouteByID(context.Background(), ResolveByIDArgs{UserID: 42, RuntimeID: "rt_42_default"})
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
}
