package debugger

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/hushine-tech/control-panel-service/internal/domain"
	"github.com/hushine-tech/control-panel-service/internal/repository"
	"github.com/hushine-tech/control-panel-service/internal/runtimechannel"
)

const (
	CommandPrepareDebugWorkspace = "prepare_debug_workspace"
	CommandLoadDebugDataset      = "load_debug_dataset"
	PlatformStartDebugReplay     = "debug.StartDebugReplay"
	MaxDebugDatasetBars          = 5000
)

type Repository interface {
	GetRuntime(ctx context.Context, runtimeID string) (domain.Runtime, error)
	SaveDebugWorkspaceState(ctx context.Context, runtimeID string, state domain.DebugWorkspaceState) error
	MarkDebugWorkspaceError(ctx context.Context, runtimeID string, errText string) error
	ReplaceActiveDebugDataset(ctx context.Context, state domain.DebugDatasetState) error
	GetLatestDebugDataset(ctx context.Context, userID int64, runtimeID string) (domain.DebugDatasetState, error)
}

type RuntimeCommander interface {
	InvokeRuntimeCommand(ctx context.Context, userID int64, runtimeID string, commandType string, payload []byte) ([]byte, error)
}

type KlineFetcher interface {
	FetchKlines(ctx context.Context, req runtimechannel.KlineQuery) ([]runtimechannel.KlineRow, error)
}

type Service struct {
	repo     Repository
	commands RuntimeCommander
	klines   KlineFetcher
	now      func() time.Time
}

func New(repo Repository, commands RuntimeCommander, klines KlineFetcher) *Service {
	return &Service{
		repo:     repo,
		commands: commands,
		klines:   klines,
		now:      time.Now,
	}
}

func (s *Service) SetClock(now func() time.Time) {
	if now != nil {
		s.now = now
	}
}

type PrepareWorkspaceArgs struct {
	UserID        int64
	RuntimeID     string
	HostPath      string
	ContainerPath string
}

func (s *Service) PrepareDebugWorkspace(ctx context.Context, args PrepareWorkspaceArgs) (domain.DebugWorkspaceState, error) {
	rt, err := s.requireDebuggerRuntime(ctx, args.UserID, args.RuntimeID)
	if err != nil {
		return domain.DebugWorkspaceState{}, err
	}
	payload, err := json.Marshal(map[string]string{
		"host_path":      strings.TrimSpace(args.HostPath),
		"container_path": defaultString(args.ContainerPath, "/workspace"),
	})
	if err != nil {
		return domain.DebugWorkspaceState{}, status.Errorf(codes.Internal, "marshal workspace command: %v", err)
	}
	raw, err := s.commands.InvokeRuntimeCommand(ctx, args.UserID, rt.RuntimeID, CommandPrepareDebugWorkspace, payload)
	if err != nil {
		_ = s.repo.MarkDebugWorkspaceError(ctx, rt.RuntimeID, err.Error())
		return domain.DebugWorkspaceState{}, err
	}
	state, err := decodeWorkspaceState(raw)
	if err != nil {
		_ = s.repo.MarkDebugWorkspaceError(ctx, rt.RuntimeID, err.Error())
		return domain.DebugWorkspaceState{}, err
	}
	if err := s.repo.SaveDebugWorkspaceState(ctx, rt.RuntimeID, state); err != nil {
		return domain.DebugWorkspaceState{}, mapRepoErr(err)
	}
	return state, nil
}

type LoadDatasetArgs struct {
	UserID      int64
	AccountID   int64
	RuntimeID   string
	Market      string
	Symbol      string
	Interval    string
	StartTimeMS int64
	EndTimeMS   int64
}

func (s *Service) LoadDebugDataset(ctx context.Context, args LoadDatasetArgs) (domain.DebugDatasetState, error) {
	rt, err := s.requireDebuggerRuntime(ctx, args.UserID, args.RuntimeID)
	if err != nil {
		return domain.DebugDatasetState{}, err
	}
	if args.AccountID <= 0 {
		return domain.DebugDatasetState{}, status.Error(codes.InvalidArgument, "account_id is required")
	}
	if s.klines == nil {
		return domain.DebugDatasetState{}, status.Error(codes.FailedPrecondition, "market-data query is not configured")
	}
	rows, err := s.klines.FetchKlines(ctx, runtimechannel.KlineQuery{
		Exchange:    "binance",
		Market:      args.Market,
		Symbol:      args.Symbol,
		Interval:    args.Interval,
		StartTimeMS: args.StartTimeMS,
		EndTimeMS:   args.EndTimeMS,
		Limit:       MaxDebugDatasetBars,
	})
	if err != nil {
		return domain.DebugDatasetState{}, status.Errorf(codes.FailedPrecondition, "load historical klines: %v", err)
	}
	coverage := runtimechannel.ValidateKlineRows(args.Interval, args.StartTimeMS, args.EndTimeMS, rows)
	if !coverage.OK {
		return domain.DebugDatasetState{}, status.Error(codes.FailedPrecondition, coverageFailureMessage(coverage))
	}
	datasetID := "dbg-" + randomHex(12)
	now := s.now().UTC()
	state := domain.DebugDatasetState{
		DatasetID:      datasetID,
		UserID:         args.UserID,
		AccountID:      args.AccountID,
		RuntimeID:      rt.RuntimeID,
		Market:         strings.ToLower(strings.TrimSpace(args.Market)),
		Symbol:         strings.ToUpper(strings.TrimSpace(args.Symbol)),
		Interval:       strings.TrimSpace(args.Interval),
		StartAt:        time.UnixMilli(args.StartTimeMS).UTC(),
		EndAt:          time.UnixMilli(args.EndTimeMS).UTC(),
		BarCount:       int64(len(rows)),
		CoverageStatus: "complete",
		LoadedAt:       now,
		State:          "active",
	}
	payload, err := json.Marshal(loadDatasetPayload{
		DatasetID:   state.DatasetID,
		UserID:      state.UserID,
		AccountID:   state.AccountID,
		RuntimeID:   state.RuntimeID,
		Market:      state.Market,
		Symbol:      state.Symbol,
		Interval:    state.Interval,
		StartTimeMS: args.StartTimeMS,
		EndTimeMS:   args.EndTimeMS,
		BarCount:    state.BarCount,
		LoadedAtMS:  state.LoadedAt.UnixMilli(),
		Klines:      klineRowsToPayload(rows),
	})
	if err != nil {
		return domain.DebugDatasetState{}, status.Errorf(codes.Internal, "marshal debug dataset: %v", err)
	}
	if _, err := s.commands.InvokeRuntimeCommand(ctx, args.UserID, rt.RuntimeID, CommandLoadDebugDataset, payload); err != nil {
		state.State = "failed"
		state.LastError = err.Error()
		_ = s.repo.ReplaceActiveDebugDataset(ctx, state)
		return domain.DebugDatasetState{}, err
	}
	if err := s.repo.ReplaceActiveDebugDataset(ctx, state); err != nil {
		return domain.DebugDatasetState{}, mapRepoErr(err)
	}
	return state, nil
}

func (s *Service) GetRuntimeDebugDataset(ctx context.Context, userID int64, runtimeID string) (domain.DebugDatasetState, error) {
	if userID <= 0 {
		return domain.DebugDatasetState{}, status.Error(codes.InvalidArgument, "user_id is required")
	}
	if strings.TrimSpace(runtimeID) == "" {
		return domain.DebugDatasetState{}, status.Error(codes.InvalidArgument, "runtime_id is required")
	}
	state, err := s.repo.GetLatestDebugDataset(ctx, userID, runtimeID)
	if err != nil {
		return domain.DebugDatasetState{}, mapRepoErr(err)
	}
	return state, nil
}

func (s *Service) StartDebugReplay(ctx context.Context, userID int64, runtimeID string, requestedName string) (string, string, domain.DebugDatasetState, error) {
	rt, err := s.requireDebuggerRuntime(ctx, userID, runtimeID)
	if err != nil {
		return "", "", domain.DebugDatasetState{}, err
	}
	dataset, err := s.repo.GetLatestDebugDataset(ctx, userID, runtimeID)
	if err != nil {
		return "", "", domain.DebugDatasetState{}, mapRepoErr(err)
	}
	if dataset.State != "active" {
		return "", "", domain.DebugDatasetState{}, status.Errorf(codes.FailedPrecondition, "debug dataset is not active: %s", dataset.State)
	}
	name := strings.TrimSpace(requestedName)
	if name == "" {
		name = fmt.Sprintf("debug-%s-%s", rt.Name, s.now().UTC().Format("20060102-150405"))
	}
	return "debug-" + randomHex(16), name, dataset, nil
}

func (s *Service) requireDebuggerRuntime(ctx context.Context, userID int64, runtimeID string) (domain.Runtime, error) {
	if userID <= 0 {
		return domain.Runtime{}, status.Error(codes.InvalidArgument, "user_id is required")
	}
	runtimeID = strings.TrimSpace(runtimeID)
	if runtimeID == "" {
		return domain.Runtime{}, status.Error(codes.InvalidArgument, "runtime_id is required")
	}
	rt, err := s.repo.GetRuntime(ctx, runtimeID)
	if err != nil {
		return domain.Runtime{}, mapRepoErr(err)
	}
	if rt.UserID != userID {
		return domain.Runtime{}, status.Error(codes.PermissionDenied, "runtime does not belong to user")
	}
	if rt.Source != domain.RuntimeSourceSelfHosted {
		return domain.Runtime{}, status.Error(codes.FailedPrecondition, "debugger requires self-hosted runtime")
	}
	if rt.Role != domain.CredentialRoleDebugger {
		return domain.Runtime{}, status.Error(codes.FailedPrecondition, "runtime role is not debugger")
	}
	if rt.Status != domain.RuntimeStatusActive {
		return domain.Runtime{}, status.Error(codes.FailedPrecondition, "runtime is not connected")
	}
	return rt, nil
}

type workspaceResponse struct {
	HostPath              string `json:"host_path"`
	ContainerPath         string `json:"container_path"`
	TemplatePath          string `json:"template_path"`
	ArchivedTemplatePath  string `json:"archived_template_path"`
	VSCodeLaunchCreated   bool   `json:"vscode_launch_created"`
	VSCodeLaunchPreserved bool   `json:"vscode_launch_preserved"`
	PyCharmDocCreated     bool   `json:"pycharm_doc_created"`
	PyCharmDocPreserved   bool   `json:"pycharm_doc_preserved"`
	PreparedAtMS          int64  `json:"prepared_at_ms"`
	LastError             string `json:"last_error"`
}

func decodeWorkspaceState(raw []byte) (domain.DebugWorkspaceState, error) {
	var resp workspaceResponse
	if len(raw) == 0 {
		raw = []byte("{}")
	}
	if err := json.Unmarshal(raw, &resp); err != nil {
		return domain.DebugWorkspaceState{}, fmt.Errorf("decode workspace response: %w", err)
	}
	var preparedAt *time.Time
	if resp.PreparedAtMS > 0 {
		t := time.UnixMilli(resp.PreparedAtMS).UTC()
		preparedAt = &t
	}
	return domain.DebugWorkspaceState{
		HostPath:              resp.HostPath,
		ContainerPath:         defaultString(resp.ContainerPath, "/workspace"),
		TemplatePath:          resp.TemplatePath,
		ArchivedTemplatePath:  resp.ArchivedTemplatePath,
		VSCodeLaunchCreated:   resp.VSCodeLaunchCreated,
		VSCodeLaunchPreserved: resp.VSCodeLaunchPreserved,
		PyCharmDocCreated:     resp.PyCharmDocCreated,
		PyCharmDocPreserved:   resp.PyCharmDocPreserved,
		PreparedAt:            preparedAt,
		LastError:             resp.LastError,
	}, nil
}

type loadDatasetPayload struct {
	DatasetID   string         `json:"dataset_id"`
	UserID      int64          `json:"user_id"`
	AccountID   int64          `json:"account_id"`
	RuntimeID   string         `json:"runtime_id"`
	Market      string         `json:"market"`
	Symbol      string         `json:"symbol"`
	Interval    string         `json:"interval"`
	StartTimeMS int64          `json:"start_time_ms"`
	EndTimeMS   int64          `json:"end_time_ms"`
	BarCount    int64          `json:"bar_count"`
	LoadedAtMS  int64          `json:"loaded_at_ms"`
	Klines      []klinePayload `json:"klines"`
}

type klinePayload struct {
	Exchange    string  `json:"exchange"`
	Market      string  `json:"market"`
	Symbol      string  `json:"symbol"`
	Interval    string  `json:"interval"`
	OpenTimeMS  int64   `json:"open_time_ms"`
	CloseTimeMS int64   `json:"close_time_ms"`
	Open        float64 `json:"open"`
	High        float64 `json:"high"`
	Low         float64 `json:"low"`
	Close       float64 `json:"close"`
	Volume      float64 `json:"volume"`
}

func klineRowsToPayload(rows []runtimechannel.KlineRow) []klinePayload {
	out := make([]klinePayload, 0, len(rows))
	for _, row := range rows {
		out = append(out, klinePayload{
			Exchange:    row.Exchange,
			Market:      row.Market,
			Symbol:      row.Symbol,
			Interval:    row.Interval,
			OpenTimeMS:  row.OpenTime,
			CloseTimeMS: row.CloseTime,
			Open:        row.Open,
			High:        row.High,
			Low:         row.Low,
			Close:       row.Close,
			Volume:      row.Volume,
		})
	}
	return out
}

func coverageFailureMessage(result runtimechannel.KlineValidationResult) string {
	if len(result.MissingGaps) == 0 {
		return result.Reason
	}
	gap := result.MissingGaps[0]
	return fmt.Sprintf(
		"missing kline rows: first gap %d-%d expected=%d actual=%d/%d",
		gap.StartMS,
		gap.EndMS,
		gap.ExpectedCount,
		result.ActualCount,
		result.ExpectedCount,
	)
}

func defaultString(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}

func randomHex(n int) string {
	if n <= 0 {
		n = 12
	}
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}

func mapRepoErr(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, repository.ErrNotFound):
		return status.Error(codes.NotFound, "not found")
	case errors.Is(err, repository.ErrConflict):
		return status.Error(codes.FailedPrecondition, "conflict")
	default:
		return status.Errorf(codes.Internal, "%v", err)
	}
}
