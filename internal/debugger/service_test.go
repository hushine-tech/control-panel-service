package debugger

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/hushine-tech/control-panel-service/internal/domain"
	"github.com/hushine-tech/control-panel-service/internal/repository"
	"github.com/hushine-tech/control-panel-service/internal/runtimechannel"
)

type fakeRepo struct {
	runtime       domain.Runtime
	workspace     domain.DebugWorkspaceState
	dataset       domain.DebugDatasetState
	workspaceErr  string
	replaceCalled bool
}

func (r *fakeRepo) GetRuntime(_ context.Context, runtimeID string) (domain.Runtime, error) {
	if r.runtime.RuntimeID != runtimeID {
		return domain.Runtime{}, repository.ErrNotFound
	}
	return r.runtime, nil
}

func (r *fakeRepo) SaveDebugWorkspaceState(_ context.Context, runtimeID string, state domain.DebugWorkspaceState) error {
	if r.runtime.RuntimeID != runtimeID {
		return repository.ErrNotFound
	}
	r.workspace = state
	return nil
}

func (r *fakeRepo) MarkDebugWorkspaceError(_ context.Context, _ string, errText string) error {
	r.workspaceErr = errText
	return nil
}

func (r *fakeRepo) ReplaceActiveDebugDataset(_ context.Context, state domain.DebugDatasetState) error {
	r.dataset = state
	r.replaceCalled = true
	return nil
}

func (r *fakeRepo) GetLatestDebugDataset(_ context.Context, userID int64, runtimeID string) (domain.DebugDatasetState, error) {
	if r.dataset.UserID != userID || r.dataset.RuntimeID != runtimeID {
		return domain.DebugDatasetState{}, repository.ErrNotFound
	}
	return r.dataset, nil
}

type fakeCommander struct {
	commandType string
	payload     []byte
	response    []byte
	err         error
}

func (c *fakeCommander) InvokeRuntimeCommand(_ context.Context, _ int64, _ string, commandType string, payload []byte) ([]byte, error) {
	c.commandType = commandType
	c.payload = payload
	if c.err != nil {
		return nil, c.err
	}
	return c.response, nil
}

type fakeKlines struct {
	rows []runtimechannel.KlineRow
	err  error
}

func (f fakeKlines) FetchKlines(_ context.Context, _ runtimechannel.KlineQuery) ([]runtimechannel.KlineRow, error) {
	return f.rows, f.err
}

func testRuntime(role domain.CredentialRole) domain.Runtime {
	return domain.Runtime{
		RuntimeID: "rt-debug",
		UserID:    7,
		Name:      "local-debug",
		Source:    domain.RuntimeSourceSelfHosted,
		Role:      role,
		Status:    domain.RuntimeStatusActive,
	}
}

func TestPrepareDebugWorkspaceSendsRuntimeCommand(t *testing.T) {
	repo := &fakeRepo{runtime: testRuntime(domain.CredentialRoleDebugger)}
	commander := &fakeCommander{response: []byte(`{
		"host_path": "/host/ws",
		"container_path": "/workspace",
		"template_path": "/workspace/self_hosted_strategy.py",
		"prepared_at_ms": 1760000000000
	}`)}
	svc := New(repo, commander, nil)

	state, err := svc.PrepareDebugWorkspace(context.Background(), PrepareWorkspaceArgs{
		UserID:        7,
		RuntimeID:     "rt-debug",
		HostPath:      "/host/ws",
		ContainerPath: "/workspace",
	})
	if err != nil {
		t.Fatalf("PrepareDebugWorkspace: %v", err)
	}
	if commander.commandType != CommandPrepareDebugWorkspace {
		t.Fatalf("command_type = %q", commander.commandType)
	}
	if state.TemplatePath != "/workspace/self_hosted_strategy.py" {
		t.Fatalf("template_path = %q", state.TemplatePath)
	}
	if repo.workspace.ContainerPath != "/workspace" {
		t.Fatalf("stored container_path = %q", repo.workspace.ContainerPath)
	}
}

func TestLoadDebugDatasetRejectsExecutorRuntime(t *testing.T) {
	repo := &fakeRepo{runtime: testRuntime(domain.CredentialRoleExecutor)}
	svc := New(repo, &fakeCommander{}, nil)

	_, err := svc.LoadDebugDataset(context.Background(), LoadDatasetArgs{
		UserID: 7, AccountID: 10, RuntimeID: "rt-debug", Market: "spot", Symbol: "ETHUSDT", Interval: "1m",
		StartTimeMS: 1760000000000, EndTimeMS: 1760000060000,
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("code = %v, want FailedPrecondition", status.Code(err))
	}
}

func TestLoadDebugDatasetStoresMetadataAfterRuntimeAccepts(t *testing.T) {
	start := int64(1760000000000)
	repo := &fakeRepo{runtime: testRuntime(domain.CredentialRoleDebugger)}
	commander := &fakeCommander{response: []byte(`{"ok":true}`)}
	svc := New(repo, commander, fakeKlines{rows: []runtimechannel.KlineRow{
		{Exchange: "binance", Market: "spot", Symbol: "ETHUSDT", Interval: "1m", OpenTime: start, CloseTime: start + 59999, Open: 1, High: 2, Low: 1, Close: 2, Volume: 10},
	}})
	svc.SetClock(func() time.Time { return time.UnixMilli(start).UTC() })

	state, err := svc.LoadDebugDataset(context.Background(), LoadDatasetArgs{
		UserID: 7, AccountID: 10, RuntimeID: "rt-debug", Market: "spot", Symbol: "ETHUSDT", Interval: "1m",
		StartTimeMS: start, EndTimeMS: start + 60000,
	})
	if err != nil {
		t.Fatalf("LoadDebugDataset: %v", err)
	}
	if !repo.replaceCalled {
		t.Fatal("expected ReplaceActiveDebugDataset")
	}
	if state.State != "active" || state.BarCount != 1 {
		t.Fatalf("dataset state = %+v", state)
	}
	var payload loadDatasetPayload
	if err := json.Unmarshal(commander.payload, &payload); err != nil {
		t.Fatalf("unmarshal command payload: %v", err)
	}
	if payload.DatasetID == "" || len(payload.Klines) != 1 {
		t.Fatalf("payload = %+v", payload)
	}
}

func TestLoadDebugDatasetRejectsMissingCoverage(t *testing.T) {
	start := int64(1760000000000)
	repo := &fakeRepo{runtime: testRuntime(domain.CredentialRoleDebugger)}
	svc := New(repo, &fakeCommander{}, fakeKlines{rows: nil})

	_, err := svc.LoadDebugDataset(context.Background(), LoadDatasetArgs{
		UserID: 7, AccountID: 10, RuntimeID: "rt-debug", Market: "spot", Symbol: "ETHUSDT", Interval: "1m",
		StartTimeMS: start, EndTimeMS: start + 60000,
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("code = %v, want FailedPrecondition", status.Code(err))
	}
}
