package runtime

import (
	"context"
	"errors"
	"net"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	cpv1 "github.com/hushine-tech/control-panel-service/gen/controlpanelv1"
	"github.com/hushine-tech/control-panel-service/internal/credential"
	"github.com/hushine-tech/control-panel-service/internal/debugger"
	"github.com/hushine-tech/control-panel-service/internal/domain"
	cpnotify "github.com/hushine-tech/control-panel-service/internal/notification"
	"github.com/hushine-tech/control-panel-service/internal/runtimechannel"
	portfoliov1 "github.com/hushine-tech/core-service/gen/portfoliov1"
	strategyv1 "github.com/hushine-tech/strategy-service/gen/strategyv1"
)

// ControlPanelGRPCService is the wire-layer wrapper over Service. It also
// dispatches the Phase D3 credential RPCs to a separate credential.Service
// (introduced in `internal/credential/`).
type ControlPanelGRPCService struct {
	cpv1.UnimplementedControlPanelServiceServer
	svc          *Service
	credSvc      *credential.Service
	debugSvc     *debugger.Service
	channelSvc   *runtimechannel.Service
	statusReader strategyStatusReader
}

type strategyStatusReader interface {
	GetSession(ctx context.Context, in *portfoliov1.GetSessionRequest, opts ...grpc.CallOption) (*portfoliov1.GetSessionResponse, error)
}

// NewControlPanelGRPCService constructs the gRPC adapter. credSvc may be
// nil during early bring-up; if nil, the three credential RPCs return
// FailedPrecondition. main.go SHOULD always pass a real credential
// service.
func NewControlPanelGRPCService(svc *Service, credSvc *credential.Service, channelSvc *runtimechannel.Service, statusReaderOpt ...strategyStatusReader) *ControlPanelGRPCService {
	var statusReader strategyStatusReader
	if len(statusReaderOpt) > 0 {
		statusReader = statusReaderOpt[0]
	}
	return &ControlPanelGRPCService{svc: svc, credSvc: credSvc, channelSvc: channelSvc, statusReader: statusReader}
}

func (g *ControlPanelGRPCService) SetDebuggerService(debugSvc *debugger.Service) {
	g.debugSvc = debugSvc
}

// ── ListRuntimes ────────────────────────────────────────────────────────────

func (g *ControlPanelGRPCService) ListRuntimes(ctx context.Context, req *cpv1.ListRuntimesRequest) (*cpv1.ListRuntimesResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	result, err := g.svc.ListRuntimes(ctx, ListArgs{
		UserID:       req.GetUserId(),
		StatusFilter: req.GetStatus(),
		SourceFilter: req.GetSource(),
		Limit:        int(req.GetLimit()),
		Offset:       int(req.GetOffset()),
	})
	if err != nil {
		return nil, mapErrorToStatus(err)
	}
	resp := &cpv1.ListRuntimesResponse{
		Runtimes: make([]*cpv1.Runtime, 0, len(result.Runtimes)),
		HasMore:  result.HasMore,
		Total:    result.Total,
	}
	for i := range result.Runtimes {
		resp.Runtimes = append(resp.Runtimes, runtimeToProto(result.Runtimes[i]))
	}
	return resp, nil
}

func (g *ControlPanelGRPCService) GetRuntime(ctx context.Context, req *cpv1.GetRuntimeRequest) (*cpv1.GetRuntimeResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	rt, err := g.svc.GetRuntime(ctx, GetRuntimeArgs{
		UserID:    req.GetUserId(),
		RuntimeID: req.GetRuntimeId(),
	})
	if err != nil {
		return nil, mapErrorToStatus(err)
	}
	return &cpv1.GetRuntimeResponse{Runtime: runtimeToProto(rt)}, nil
}

func (g *ControlPanelGRPCService) EndRuntime(ctx context.Context, req *cpv1.EndRuntimeRequest) (*cpv1.EndRuntimeResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	rt, err := g.svc.EndRuntime(ctx, EndRuntimeArgs{
		UserID:    req.GetUserId(),
		RuntimeID: req.GetRuntimeId(),
	})
	if err != nil {
		return nil, mapErrorToStatus(err)
	}
	return &cpv1.EndRuntimeResponse{Runtime: runtimeToProto(rt)}, nil
}

func (g *ControlPanelGRPCService) ResolveRuntimeRouteByID(ctx context.Context, req *cpv1.ResolveRuntimeRouteByIDRequest) (*cpv1.ResolveRuntimeRouteResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	result, err := g.svc.ResolveRuntimeRouteByID(ctx, ResolveByIDArgs{
		UserID:      req.GetUserId(),
		RuntimeID:   req.GetRuntimeId(),
		Role:        req.GetRole(),
		Environment: int(req.GetEnvironment()),
	})
	if err != nil {
		return nil, mapErrorToStatus(err)
	}
	return &cpv1.ResolveRuntimeRouteResponse{
		Runtime: runtimeToProto(result.Runtime),
	}, nil
}

// ── EnsureHostedRuntime ─────────────────────────────────────────────────────

func (g *ControlPanelGRPCService) EnsureHostedRuntime(ctx context.Context, req *cpv1.EnsureHostedRuntimeRequest) (*cpv1.EnsureHostedRuntimeResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	result, err := g.svc.EnsureHostedRuntime(ctx, EnsureHostedRuntimeArgs{
		UserID:          req.GetUserId(),
		Name:            req.GetName(),
		ResourceProfile: req.GetResourceProfile(),
	})
	if err != nil {
		return nil, mapErrorToStatus(err)
	}
	resp := &cpv1.EnsureHostedRuntimeResponse{
		Runtime:     runtimeToProto(result.Runtime),
		Provisioned: result.Provisioned,
	}
	return resp, nil
}

// ── helpers ─────────────────────────────────────────────────────────────────

func runtimeToProto(rt domain.Runtime) *cpv1.Runtime {
	out := &cpv1.Runtime{
		RuntimeId:                 rt.RuntimeID,
		UserId:                    rt.UserID,
		Name:                      rt.Name,
		Source:                    rt.Source,
		Role:                      string(rt.Role),
		EndpointHost:              rt.EndpointHost,
		GrpcPort:                  rt.GRPCPort,
		DebugPort:                 rt.DebugPort,
		Capabilities:              rt.Capabilities,
		ResourceProfile:           rt.ResourceProfile,
		Version:                   rt.Version,
		Status:                    rt.Status,
		CreatedAt:                 timestamppb.New(rt.CreatedAt),
		UpdatedAt:                 timestamppb.New(rt.UpdatedAt),
		CredentialKeyId:           rt.CredentialKeyID,
		EndedReason:               rt.EndedReason,
		CleanupStatus:             rt.CleanupStatus,
		CleanupReason:             rt.CleanupReason,
		ConnectionOwnerInstanceId: rt.ConnectionOwnerInstanceID,
		DebugWorkspace:            debugWorkspaceToProto(rt.DebugWorkspace),
		DebugDataset:              debugDatasetToProto(rt.DebugDataset),
	}
	if rt.PairedAt != nil {
		out.PairedAt = timestamppb.New(*rt.PairedAt)
	}
	if rt.HeartbeatAt != nil {
		out.HeartbeatAt = timestamppb.New(*rt.HeartbeatAt)
	}
	if rt.StartedAt != nil {
		out.StartedAt = timestamppb.New(*rt.StartedAt)
	}
	if rt.EndedAt != nil {
		out.EndedAt = timestamppb.New(*rt.EndedAt)
	}
	if rt.CleanupAt != nil {
		out.CleanupAt = timestamppb.New(*rt.CleanupAt)
	}
	if rt.ConnectionOwnerAcquiredAt != nil {
		out.ConnectionOwnerAcquiredAt = timestamppb.New(*rt.ConnectionOwnerAcquiredAt)
	}
	if rt.ConnectionOwnerHeartbeatAt != nil {
		out.ConnectionOwnerHeartbeatAt = timestamppb.New(*rt.ConnectionOwnerHeartbeatAt)
	}
	return out
}

func debugWorkspaceToProto(state domain.DebugWorkspaceState) *cpv1.DebugWorkspaceState {
	out := &cpv1.DebugWorkspaceState{
		HostPath:              state.HostPath,
		ContainerPath:         state.ContainerPath,
		TemplatePath:          state.TemplatePath,
		ArchivedTemplatePath:  state.ArchivedTemplatePath,
		VscodeLaunchCreated:   state.VSCodeLaunchCreated,
		VscodeLaunchPreserved: state.VSCodeLaunchPreserved,
		PycharmDocCreated:     state.PyCharmDocCreated,
		PycharmDocPreserved:   state.PyCharmDocPreserved,
		LastError:             state.LastError,
	}
	if state.PreparedAt != nil {
		out.PreparedAtMs = state.PreparedAt.UnixMilli()
	}
	return out
}

func debugDatasetToProto(state *domain.DebugDatasetState) *cpv1.DebugDatasetState {
	if state == nil {
		return nil
	}
	return &cpv1.DebugDatasetState{
		DatasetId:      state.DatasetID,
		UserId:         state.UserID,
		PortfolioId:    state.PortfolioID,
		RuntimeId:      state.RuntimeID,
		Market:         state.Market,
		Symbol:         state.Symbol,
		Interval:       state.Interval,
		StartTimeMs:    state.StartAt.UnixMilli(),
		EndTimeMs:      state.EndAt.UnixMilli(),
		BarCount:       state.BarCount,
		CoverageStatus: state.CoverageStatus,
		LoadedAtMs:     state.LoadedAt.UnixMilli(),
		State:          state.State,
		LastError:      state.LastError,
	}
}

// mapErrorToStatus translates Service sentinels and unrelated errors to
// gRPC status codes. Adheres to the fail-closed contract: ambiguous errors
// surface as Unavailable (not OK).
func mapErrorToStatus(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, ErrInvalidArgument):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, ErrPermissionDenied):
		return status.Error(codes.PermissionDenied, err.Error())
	case errors.Is(err, ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, ErrUnhealthy),
		errors.Is(err, ErrEnded),
		errors.Is(err, ErrTokenMismatch),
		errors.Is(err, ErrProvisionerUnavailable),
		errors.Is(err, ErrRegistrationTimeout):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, ErrQuotaExceeded):
		return status.Error(codes.ResourceExhausted, err.Error())
	case errors.Is(err, ErrConflict):
		return status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, ErrPlanLookupUnavailable),
		errors.Is(err, ErrSessionLookupUnavailable):
		return status.Error(codes.Unavailable, err.Error())
	default:
		return status.Error(codes.Unavailable, err.Error())
	}
}

// ── Runtime credentials (Phase D3) ──────────────────────────────────────────

func (g *ControlPanelGRPCService) IssueRuntimeCredential(ctx context.Context, req *cpv1.IssueRuntimeCredentialRequest) (*cpv1.IssueRuntimeCredentialResponse, error) {
	if g.credSvc == nil {
		return nil, status.Error(codes.FailedPrecondition, "credential service is not configured")
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	issued, err := g.credSvc.Issue(ctx, credential.IssueArgs{
		UserID: req.GetUserId(),
		Label:  req.GetLabel(),
		Role:   domain.CredentialRole(req.GetRole()),
	})
	if err != nil {
		return nil, mapCredentialError(err)
	}
	resp := &cpv1.IssueRuntimeCredentialResponse{
		KeyId:         issued.KeyID,
		PrivateKeyPem: issued.PrivateKeyPEM,
		PublicKeyPem:  issued.PublicKeyPEM,
		CreatedAt:     timestamppb.New(issued.CreatedAt),
		Role:          string(issued.Role),
		ClientCertPem: issued.ClientCertPEM,
		ClientKeyPem:  issued.ClientKeyPEM,
		ServerCaPem:   issued.ServerCAPEM,
	}
	if issued.ClientCertExpiresAt != nil {
		resp.ClientCertExpiresAt = timestamppb.New(*issued.ClientCertExpiresAt)
	}
	return resp, nil
}

func (g *ControlPanelGRPCService) BootstrapBareRuntimeCertificate(ctx context.Context, req *cpv1.BootstrapBareRuntimeCertificateRequest) (*cpv1.BootstrapBareRuntimeCertificateResponse, error) {
	if g.svc == nil {
		return nil, status.Error(codes.FailedPrecondition, "runtime service is not configured")
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	res, err := g.svc.BootstrapBareRuntimeCertificate(ctx, BootstrapBareRuntimeCertificateArgs{
		UserID:          req.GetUserId(),
		RuntimeID:       req.GetRuntimeId(),
		Name:            req.GetName(),
		CSRPEM:          req.GetCsrPem(),
		RemoteIP:        remoteIPFromContext(ctx),
		Capabilities:    append([]string(nil), req.GetCapabilities()...),
		ResourceProfile: req.GetResourceProfile(),
		Version:         req.GetVersion(),
	})
	if err != nil {
		return nil, mapErrorToStatus(err)
	}
	return &cpv1.BootstrapBareRuntimeCertificateResponse{
		RuntimeId:           res.RuntimeID,
		Name:                res.Name,
		ClientCertPem:       res.ClientCertPEM,
		ServerCaPem:         res.ServerCAPEM,
		ClientCertExpiresAt: timestamppb.New(res.ClientCertExpiresAt),
	}, nil
}

func remoteIPFromContext(ctx context.Context) string {
	p, ok := peer.FromContext(ctx)
	if !ok || p.Addr == nil {
		return ""
	}
	addr := p.Addr.String()
	if host, _, err := net.SplitHostPort(addr); err == nil {
		return host
	}
	return addr
}

func (g *ControlPanelGRPCService) ListRuntimeCredentials(ctx context.Context, req *cpv1.ListRuntimeCredentialsRequest) (*cpv1.ListRuntimeCredentialsResponse, error) {
	if g.credSvc == nil {
		return nil, status.Error(codes.FailedPrecondition, "credential service is not configured")
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	includeInactive := req.GetIncludeInactive()
	if req.GetLimit() > 0 || req.GetOffset() > 0 {
		creds, total, hasMore, err := g.credSvc.ListPage(ctx, req.GetUserId(), includeInactive, int(req.GetLimit()), int(req.GetOffset()))
		if err != nil {
			return nil, mapCredentialError(err)
		}
		out := make([]*cpv1.RuntimeCredential, 0, len(creds))
		for _, c := range creds {
			out = append(out, credentialToProto(c))
		}
		return &cpv1.ListRuntimeCredentialsResponse{Credentials: out, HasMore: hasMore, Total: total}, nil
	}
	creds, err := g.credSvc.List(ctx, req.GetUserId(), includeInactive)
	if err != nil {
		return nil, mapCredentialError(err)
	}
	out := make([]*cpv1.RuntimeCredential, 0, len(creds))
	for _, c := range creds {
		out = append(out, credentialToProto(c))
	}
	return &cpv1.ListRuntimeCredentialsResponse{Credentials: out, Total: int64(len(out))}, nil
}

func (g *ControlPanelGRPCService) ListRuntimeAdmissionFailures(ctx context.Context, req *cpv1.ListRuntimeAdmissionFailuresRequest) (*cpv1.ListRuntimeAdmissionFailuresResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if g.svc == nil {
		return nil, status.Error(codes.FailedPrecondition, "runtime service is not configured")
	}
	failures, err := g.svc.ListRuntimeAdmissionFailures(ctx, ListAdmissionFailuresArgs{
		UserID: req.GetUserId(),
		Limit:  int(req.GetLimit()),
	})
	if err != nil {
		return nil, mapErrorToStatus(err)
	}
	out := make([]*cpv1.RuntimeAdmissionFailure, 0, len(failures))
	for _, failure := range failures {
		out = append(out, admissionFailureToProto(failure))
	}
	return &cpv1.ListRuntimeAdmissionFailuresResponse{Failures: out}, nil
}

func (g *ControlPanelGRPCService) RevokeRuntimeCredential(ctx context.Context, req *cpv1.RevokeRuntimeCredentialRequest) (*cpv1.RevokeRuntimeCredentialResponse, error) {
	if g.credSvc == nil {
		return nil, status.Error(codes.FailedPrecondition, "credential service is not configured")
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	res, err := g.credSvc.Revoke(ctx, req.GetUserId(), req.GetKeyId())
	if err != nil {
		return nil, mapCredentialError(err)
	}
	return &cpv1.RevokeRuntimeCredentialResponse{
		Credential:    credentialToProto(res.Credential),
		StreamsClosed: int32(res.StreamsClosed),
		RuntimesEnded: int32(res.RuntimesEnded),
	}, nil
}

// ── RuntimeChannel (Phase D3 self-hosted runtimes) ─────────────────────────

func (g *ControlPanelGRPCService) RuntimeChannel(stream cpv1.ControlPanelService_RuntimeChannelServer) error {
	return status.Error(codes.FailedPrecondition, "RuntimeChannel is served on the dedicated runtime_channel_server listener")
}

func (g *ControlPanelGRPCService) PrepareDebugWorkspace(ctx context.Context, req *cpv1.PrepareDebugWorkspaceRequest) (*cpv1.PrepareDebugWorkspaceResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if g.debugSvc == nil {
		return nil, status.Error(codes.FailedPrecondition, "debugger service is not configured")
	}
	state, err := g.debugSvc.PrepareDebugWorkspace(ctx, debugger.PrepareWorkspaceArgs{
		UserID:        req.GetUserId(),
		RuntimeID:     req.GetRuntimeId(),
		HostPath:      req.GetHostPath(),
		ContainerPath: req.GetContainerPath(),
	})
	if err != nil {
		return nil, err
	}
	return &cpv1.PrepareDebugWorkspaceResponse{Ok: true, Workspace: debugWorkspaceToProto(state)}, nil
}

func (g *ControlPanelGRPCService) LoadDebugDataset(ctx context.Context, req *cpv1.LoadDebugDatasetRequest) (*cpv1.LoadDebugDatasetResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if g.debugSvc == nil {
		return nil, status.Error(codes.FailedPrecondition, "debugger service is not configured")
	}
	state, err := g.debugSvc.LoadDebugDataset(ctx, debugger.LoadDatasetArgs{
		UserID:      req.GetUserId(),
		PortfolioID: req.GetPortfolioId(),
		RuntimeID:   req.GetRuntimeId(),
		Market:      req.GetMarket(),
		Symbol:      req.GetSymbol(),
		Interval:    req.GetInterval(),
		StartTimeMS: req.GetStartTimeMs(),
		EndTimeMS:   req.GetEndTimeMs(),
	})
	if err != nil {
		return nil, err
	}
	return &cpv1.LoadDebugDatasetResponse{Ok: true, Dataset: debugDatasetToProto(&state)}, nil
}

func (g *ControlPanelGRPCService) GetRuntimeDebugDataset(ctx context.Context, req *cpv1.GetRuntimeDebugDatasetRequest) (*cpv1.GetRuntimeDebugDatasetResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if g.debugSvc == nil {
		return nil, status.Error(codes.FailedPrecondition, "debugger service is not configured")
	}
	state, err := g.debugSvc.GetRuntimeDebugDataset(ctx, req.GetUserId(), req.GetRuntimeId())
	if err != nil {
		return nil, err
	}
	return &cpv1.GetRuntimeDebugDatasetResponse{Dataset: debugDatasetToProto(&state)}, nil
}

func (g *ControlPanelGRPCService) PublishRuntimeNotification(ctx context.Context, req *cpv1.PublishRuntimeNotificationRequest) (*cpv1.PublishRuntimeNotificationResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if g.svc == nil {
		return nil, status.Error(codes.FailedPrecondition, "runtime service is not configured")
	}
	rt, err := g.svc.GetRuntime(ctx, GetRuntimeArgs{
		UserID:    req.GetUserId(),
		RuntimeID: strings.TrimSpace(req.GetRuntimeId()),
	})
	if err != nil {
		return nil, mapErrorToStatus(err)
	}
	if domain.IsRuntimeTerminalStatus(rt.Status) {
		return nil, status.Errorf(codes.FailedPrecondition, "runtime %s is terminal: %s", rt.RuntimeID, rt.Status)
	}
	message := strings.TrimSpace(req.GetMessage())
	if message == "" {
		return nil, status.Error(codes.InvalidArgument, "notification message is required")
	}
	category := normalizeNotificationCategory(req.GetCategory())
	severity := normalizeNotificationSeverity(req.GetSeverity())
	event := cpnotify.Event{
		SchemaVersion: cpnotify.SchemaVersion,
		UserID:        rt.UserID,
		Category:      category,
		EventType:     notificationEventType(category, severity),
		Severity:      severity,
		RuntimeID:     rt.RuntimeID,
		RuntimeName:   rt.Name,
		PortfolioID:   req.GetPortfolioId(),
		StrategyID:    req.GetStrategyId(),
		SessionID:     strings.TrimSpace(req.GetSessionId()),
		Title:         strings.TrimSpace(req.GetTitle()),
		Message:       message,
		DedupeKey:     strings.TrimSpace(req.GetDedupeKey()),
	}
	if g.svc.notifications != nil {
		if err := g.svc.notifications.Publish(ctx, event); err != nil {
			return nil, status.Errorf(codes.Unavailable, "publish notification: %v", err)
		}
	}
	return &cpv1.PublishRuntimeNotificationResponse{Accepted: true}, nil
}

func (g *ControlPanelGRPCService) RunStrategy(ctx context.Context, req *strategyv1.RunStrategyRequest) (*strategyv1.RunStrategyResponse, error) {
	if g.channelSvc == nil {
		return nil, status.Error(codes.FailedPrecondition, "runtime channel service is not configured")
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	resp := &strategyv1.RunStrategyResponse{}
	if err := g.channelSvc.InvokeStrategyUnaryByRuntimeID(ctx, req.GetUserId(), req.GetRuntimeId(), "RunStrategy", req, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

func (g *ControlPanelGRPCService) PreviewRunStrategy(ctx context.Context, req *strategyv1.PreviewRunStrategyRequest) (*strategyv1.PreviewRunStrategyResponse, error) {
	if g.channelSvc == nil {
		return nil, status.Error(codes.FailedPrecondition, "runtime channel service is not configured")
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	resp := &strategyv1.PreviewRunStrategyResponse{}
	if err := g.channelSvc.InvokeStrategyUnaryByRuntimeID(ctx, req.GetUserId(), req.GetRuntimeId(), "PreviewRunStrategy", req, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

func (g *ControlPanelGRPCService) ValidateStrategySource(ctx context.Context, req *strategyv1.ValidateStrategySourceRequest) (*strategyv1.ValidateStrategySourceResponse, error) {
	if g.channelSvc == nil {
		return nil, status.Error(codes.FailedPrecondition, "runtime channel service is not configured")
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	resp := &strategyv1.ValidateStrategySourceResponse{}
	if err := g.channelSvc.InvokeStrategyUnaryByRuntimeID(ctx, req.GetUserId(), req.GetRuntimeId(), "ValidateStrategySource", req, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

func (g *ControlPanelGRPCService) StopStrategy(ctx context.Context, req *strategyv1.StopStrategyRequest) (*strategyv1.StopStrategyResponse, error) {
	if g.channelSvc == nil {
		return nil, status.Error(codes.FailedPrecondition, "runtime channel service is not configured")
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	resp := &strategyv1.StopStrategyResponse{}
	if err := g.channelSvc.InvokeStrategyUnaryByRuntimeID(ctx, req.GetUserId(), req.GetRuntimeId(), "StopStrategy", req, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

func (g *ControlPanelGRPCService) GetStrategyStatus(ctx context.Context, req *strategyv1.GetStrategyStatusRequest) (*strategyv1.GetStrategyStatusResponse, error) {
	if g.channelSvc == nil {
		return nil, status.Error(codes.FailedPrecondition, "runtime channel service is not configured")
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	resp := &strategyv1.GetStrategyStatusResponse{}
	if err := g.channelSvc.InvokeStrategyUnaryByRuntimeID(ctx, req.GetUserId(), req.GetRuntimeId(), "GetStrategyStatus", req, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

func normalizeNotificationCategory(category string) string {
	switch strings.ToLower(strings.TrimSpace(category)) {
	case cpnotify.CategorySystem:
		return cpnotify.CategorySystem
	case cpnotify.CategoryStrategy:
		return cpnotify.CategoryStrategy
	default:
		return cpnotify.CategoryCustom
	}
}

func normalizeNotificationSeverity(severity string) string {
	switch strings.ToLower(strings.TrimSpace(severity)) {
	case cpnotify.SeverityWarn:
		return cpnotify.SeverityWarn
	case cpnotify.SeverityError:
		return cpnotify.SeverityError
	default:
		return cpnotify.SeverityInfo
	}
}

func notificationEventType(category, severity string) string {
	if category == cpnotify.CategoryCustom {
		switch severity {
		case cpnotify.SeverityWarn:
			return cpnotify.EventCustomWarn
		case cpnotify.SeverityError:
			return cpnotify.EventCustomError
		default:
			return cpnotify.EventCustomInfo
		}
	}
	if category == cpnotify.CategoryStrategy {
		return cpnotify.EventStrategyMessage
	}
	return cpnotify.EventSystemMessage
}

func credentialToProto(c domain.RuntimeCredential) *cpv1.RuntimeCredential {
	out := &cpv1.RuntimeCredential{
		KeyId:                 c.KeyID,
		UserId:                c.UserID,
		Label:                 c.Label,
		Status:                string(c.Status),
		PublicKeyPem:          c.PublicKeyPEM,
		CreatedAt:             timestamppb.New(c.CreatedAt),
		Role:                  string(c.Role),
		ConsumedRuntimeId:     c.ConsumedRuntimeID,
		HostedInternal:        c.HostedInternal,
		ClientCertFingerprint: c.ClientCertFingerprint,
		Issuer:                c.Issuer,
	}
	if c.DownloadedAt != nil {
		out.DownloadedAt = timestamppb.New(*c.DownloadedAt)
	}
	if c.ConsumedAt != nil {
		out.ConsumedAt = timestamppb.New(*c.ConsumedAt)
	}
	if c.ExpiresAt != nil {
		out.ExpiresAt = timestamppb.New(*c.ExpiresAt)
	}
	if c.LastUsedAt != nil {
		out.LastUsedAt = timestamppb.New(*c.LastUsedAt)
	}
	if c.RevokedAt != nil {
		out.RevokedAt = timestamppb.New(*c.RevokedAt)
	}
	if c.ClientCertExpiresAt != nil {
		out.ClientCertExpiresAt = timestamppb.New(*c.ClientCertExpiresAt)
	}
	return out
}

func admissionFailureToProto(f domain.RuntimeAdmissionFailure) *cpv1.RuntimeAdmissionFailure {
	out := &cpv1.RuntimeAdmissionFailure{
		AdmissionFailureId: f.AdmissionFailureID,
		UserId:             f.UserID,
		CredentialKeyId:    f.CredentialKeyID,
		RequestedRuntimeId: f.RequestedRuntimeID,
		RequestedName:      f.RequestedName,
		Source:             f.Source,
		Role:               string(f.Role),
		FailureCode:        f.FailureCode,
		Reason:             f.Reason,
		ConsumedRuntimeId:  f.ConsumedRuntimeID,
		AttemptCount:       int32(f.AttemptCount),
	}
	if !f.FirstSeenAt.IsZero() {
		out.FirstSeenAt = timestamppb.New(f.FirstSeenAt)
	}
	if !f.LastSeenAt.IsZero() {
		out.LastSeenAt = timestamppb.New(f.LastSeenAt)
	}
	return out
}

func mapCredentialError(err error) error {
	switch {
	case errors.Is(err, credential.ErrInvalidArgument):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, credential.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, credential.ErrPermissionDenied):
		return status.Error(codes.PermissionDenied, err.Error())
	case errors.Is(err, credential.ErrCertificateSignerUnavailable):
		return status.Error(codes.FailedPrecondition, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}
