package runtimechannel

import (
	"context"
	"encoding/json"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"

	cpv1 "github.com/hushine-tech/control-panel-service/gen/controlpanelv1"
	mdv1 "github.com/hushine-tech/control-panel-service/gen/marketdatav1"
	"github.com/hushine-tech/control-panel-service/internal/domain"
	"github.com/hushine-tech/control-panel-service/internal/logger"
	cpnotify "github.com/hushine-tech/control-panel-service/internal/notification"
	accountv1 "github.com/hushine-tech/core-service/gen/accountv1"
	orderv1 "github.com/hushine-tech/core-service/gen/orderv1"
)

type AccountPlatformClient interface {
	GetAccount(ctx context.Context, in *accountv1.GetAccountRequest, opts ...grpc.CallOption) (*accountv1.GetAccountResponse, error)
	GetSession(ctx context.Context, in *accountv1.GetSessionRequest, opts ...grpc.CallOption) (*accountv1.GetSessionResponse, error)
	GetPortfolioSnapshot(ctx context.Context, in *accountv1.GetPortfolioSnapshotRequest, opts ...grpc.CallOption) (*accountv1.GetPortfolioSnapshotResponse, error)
	UpdatePortfolioSnapshot(ctx context.Context, in *accountv1.UpdatePortfolioSnapshotRequest, opts ...grpc.CallOption) (*accountv1.UpdatePortfolioSnapshotResponse, error)
	UpdateAccountWalletState(ctx context.Context, in *accountv1.UpdateAccountWalletStateRequest, opts ...grpc.CallOption) (*accountv1.UpdateAccountWalletStateResponse, error)
	PreflightStrategySession(ctx context.Context, in *accountv1.PreflightStrategySessionRequest, opts ...grpc.CallOption) (*accountv1.PreflightStrategySessionResponse, error)
	GetActiveStrategy(ctx context.Context, in *accountv1.GetActiveStrategyRequest, opts ...grpc.CallOption) (*accountv1.GetActiveStrategyResponse, error)
	SaveSession(ctx context.Context, in *accountv1.SaveSessionRequest, opts ...grpc.CallOption) (*accountv1.SaveSessionResponse, error)
	UpdateSession(ctx context.Context, in *accountv1.UpdateSessionRequest, opts ...grpc.CallOption) (*accountv1.UpdateSessionResponse, error)
}

type OrderPlatformClient interface {
	PlaceOrder(ctx context.Context, in *orderv1.PlaceOrderRequest, opts ...grpc.CallOption) (*orderv1.PlaceOrderResponse, error)
	ResolveOrderAttempt(ctx context.Context, in *orderv1.ResolveOrderAttemptRequest, opts ...grpc.CallOption) (*orderv1.ResolveOrderAttemptResponse, error)
}

type MarketDataPlatformServer interface {
	GetMarketDataStreamStatus(context.Context, *mdv1.GetMarketDataStreamStatusRequest) (*mdv1.GetMarketDataStreamStatusResponse, error)
	CreateOrRenewMarketDataLease(context.Context, *mdv1.CreateOrRenewMarketDataLeaseRequest) (*mdv1.CreateOrRenewMarketDataLeaseResponse, error)
	ReleaseMarketDataLease(context.Context, *mdv1.ReleaseMarketDataLeaseRequest) (*mdv1.ReleaseMarketDataLeaseResponse, error)
	CreateSessionMarketDataSubscriptions(context.Context, *mdv1.CreateSessionMarketDataSubscriptionsRequest) (*mdv1.CreateSessionMarketDataSubscriptionsResponse, error)
	ReleaseSessionMarketDataSubscriptions(context.Context, *mdv1.ReleaseSessionMarketDataSubscriptionsRequest) (*mdv1.ReleaseSessionMarketDataSubscriptionsResponse, error)
}

type KlineQuerier interface {
	FetchKlines(context.Context, KlineQuery) ([]KlineRow, error)
}

type DatasetDeliverer interface {
	DeliverDatasetChunk(context.Context, DatasetChunkDelivery) error
}

type DebugReplayStarter interface {
	StartDebugReplay(ctx context.Context, userID int64, runtimeID string, requestedName string) (sessionID string, sessionName string, dataset domain.DebugDatasetState, err error)
}

type PlatformProxy struct {
	account          AccountPlatformClient
	order            OrderPlatformClient
	marketData       MarketDataPlatformServer
	klineQuery       KlineQuerier
	datasetDeliverer DatasetDeliverer
	debugReplay      DebugReplayStarter
	notifications    cpnotify.Publisher
}

type datasetDeliveryRequest struct {
	SessionID   string
	RuntimeID   string
	StartTimeMS int64
	EndTimeMS   int64
	ChunkSize   int
	Streams     []KlineQuery
}

func NewPlatformProxy(account AccountPlatformClient, order OrderPlatformClient, marketData MarketDataPlatformServer) *PlatformProxy {
	return &PlatformProxy{
		account:    account,
		order:      order,
		marketData: marketData,
	}
}

func (p *PlatformProxy) SetMarketDataQuery(query KlineQuerier) {
	if p != nil {
		p.klineQuery = query
	}
}

func (p *PlatformProxy) SetDatasetDeliverer(deliverer DatasetDeliverer) {
	if p != nil {
		p.datasetDeliverer = deliverer
	}
}

func (p *PlatformProxy) SetDebugReplayStarter(starter DebugReplayStarter) {
	if p != nil {
		p.debugReplay = starter
	}
}

func (p *PlatformProxy) SetNotificationPublisher(publisher cpnotify.Publisher) {
	if p != nil {
		p.notifications = publisher
	}
}

func (p *PlatformProxy) DispatchRuntimeRequest(ctx context.Context, rt AuthenticatedRuntime, method string, payload *anypb.Any) (proto.Message, error) {
	if rt.UserID <= 0 {
		return nil, status.Error(codes.PermissionDenied, "authenticated runtime user_id is required")
	}
	switch canonicalPlatformMethod(method) {
	case "account.GetPortfolioSnapshot":
		req := &accountv1.GetPortfolioSnapshotRequest{}
		if err := unpackRuntimePayload(payload, req); err != nil {
			return nil, err
		}
		if req.GetUserId() != 0 && req.GetUserId() != rt.UserID {
			return nil, status.Error(codes.PermissionDenied, "user_id does not match authenticated runtime")
		}
		req.UserId = rt.UserID
		if err := p.ensureAccountOwner(ctx, rt, req.GetAccountId()); err != nil {
			return nil, err
		}
		return p.requireAccount().GetPortfolioSnapshot(ctx, req)

	case "account.UpdatePortfolioSnapshot":
		return nil, status.Error(codes.Unimplemented, "account.UpdatePortfolioSnapshot is deprecated for runtime sessions; use account.UpdateAccountWalletState")

	case "account.UpdateAccountWalletState":
		req := &accountv1.UpdateAccountWalletStateRequest{}
		if err := unpackRuntimePayload(payload, req); err != nil {
			return nil, err
		}
		if req.GetUserId() != 0 && req.GetUserId() != rt.UserID {
			return nil, status.Error(codes.PermissionDenied, "user_id does not match authenticated runtime")
		}
		req.UserId = rt.UserID
		if err := p.ensureAccountOwner(ctx, rt, req.GetAccountId()); err != nil {
			return nil, err
		}
		if strings.TrimSpace(req.GetSessionId()) == "" {
			return nil, status.Error(codes.InvalidArgument, "session_id is required")
		}
		if err := p.ensureSessionOwner(ctx, rt, req.GetSessionId()); err != nil {
			return nil, err
		}
		return p.requireAccount().UpdateAccountWalletState(ctx, req)

	case "account.PreflightStrategySession":
		req := &accountv1.PreflightStrategySessionRequest{}
		if err := unpackRuntimePayload(payload, req); err != nil {
			return nil, err
		}
		if req.GetUserId() != 0 && req.GetUserId() != rt.UserID {
			return nil, status.Error(codes.PermissionDenied, "user_id does not match authenticated runtime")
		}
		req.UserId = rt.UserID
		if err := p.ensureAccountOwner(ctx, rt, req.GetAccountId()); err != nil {
			return nil, err
		}
		return p.requireAccount().PreflightStrategySession(ctx, req)

	case "account.GetActiveStrategy":
		req := &accountv1.GetActiveStrategyRequest{}
		if err := unpackRuntimePayload(payload, req); err != nil {
			return nil, err
		}
		if err := p.ensureAccountOwner(ctx, rt, req.GetAccountId()); err != nil {
			return nil, err
		}
		return p.requireAccount().GetActiveStrategy(ctx, req)

	case "account.SaveSession":
		req := &accountv1.SaveSessionRequest{}
		if err := unpackRuntimePayload(payload, req); err != nil {
			return nil, err
		}
		if err := p.ensureAccountOwner(ctx, rt, req.GetAccountId()); err != nil {
			return nil, err
		}
		if req.GetRuntimeId() != "" && req.GetRuntimeId() != rt.RuntimeID {
			return nil, status.Error(codes.PermissionDenied, "session runtime_id does not match authenticated runtime")
		}
		req.RuntimeId = rt.RuntimeID
		req.RuntimeSource = runtimeSourceFromAuthenticated(rt)
		req.RuntimeName = rt.Name
		return p.requireAccount().SaveSession(ctx, req)

	case "account.UpdateSession":
		req := &accountv1.UpdateSessionRequest{}
		if err := unpackRuntimePayload(payload, req); err != nil {
			return nil, err
		}
		if req.GetRuntimeId() != "" && req.GetRuntimeId() != rt.RuntimeID {
			return nil, status.Error(codes.PermissionDenied, "session runtime_id does not match authenticated runtime")
		}
		if err := p.ensureSessionOwner(ctx, rt, req.GetSessionId()); err != nil {
			return nil, err
		}
		req.RuntimeId = rt.RuntimeID
		return p.requireAccount().UpdateSession(ctx, req)

	case "order.PlaceOrder":
		req := &orderv1.PlaceOrderRequest{}
		if err := unpackRuntimePayload(payload, req); err != nil {
			return nil, err
		}
		if err := p.ensureAccountOwner(ctx, rt, req.GetAccountId()); err != nil {
			return nil, err
		}
		if req.GetSessionId() != "" {
			if err := p.ensureSessionOwner(ctx, rt, req.GetSessionId()); err != nil {
				return nil, err
			}
		}
		return p.requireOrder().PlaceOrder(ctx, req)

	case "order.ResolveOrderAttempt":
		req := &orderv1.ResolveOrderAttemptRequest{}
		if err := unpackRuntimePayload(payload, req); err != nil {
			return nil, err
		}
		if err := p.ensureAccountOwner(ctx, rt, req.GetAccountId()); err != nil {
			return nil, err
		}
		return p.requireOrder().ResolveOrderAttempt(ctx, req)

	case "marketdata.GetMarketDataStreamStatus":
		req := &mdv1.GetMarketDataStreamStatusRequest{}
		if err := unpackRuntimePayload(payload, req); err != nil {
			return nil, err
		}
		return p.requireMarketData().GetMarketDataStreamStatus(ctx, req)

	case "marketdata.FetchKlines":
		req, err := unpackKlineQueryPayload(payload)
		if err != nil {
			return nil, err
		}
		query := p.requireKlineQuery()
		if query == nil {
			return nil, status.Error(codes.FailedPrecondition, "market-data query is not configured")
		}
		rows, err := query.FetchKlines(ctx, req)
		if err != nil {
			return nil, status.Errorf(codes.Unavailable, "fetch platform klines: %v", err)
		}
		return klineRowsToStruct(rows)

	case "marketdata.DeliverDataset":
		req, err := unpackDatasetDeliveryPayload(payload)
		if err != nil {
			return nil, err
		}
		if req.RuntimeID != "" && req.RuntimeID != rt.RuntimeID {
			return nil, status.Error(codes.PermissionDenied, "dataset runtime_id does not match authenticated runtime")
		}
		if err := p.ensureSessionOwner(ctx, rt, req.SessionID); err != nil {
			return nil, err
		}
		return p.deliverDataset(ctx, rt, req)

	case "marketdata.CreateOrRenewMarketDataLease":
		req := &mdv1.CreateOrRenewMarketDataLeaseRequest{}
		if err := unpackRuntimePayload(payload, req); err != nil {
			return nil, err
		}
		if req.GetAccountId() > 0 {
			if err := p.ensureAccountOwner(ctx, rt, req.GetAccountId()); err != nil {
				return nil, err
			}
		}
		if err := p.ensureSessionOwner(ctx, rt, req.GetSessionId()); err != nil {
			return nil, err
		}
		return p.requireMarketData().CreateOrRenewMarketDataLease(ctx, req)

	case "marketdata.ReleaseMarketDataLease":
		req := &mdv1.ReleaseMarketDataLeaseRequest{}
		if err := unpackRuntimePayload(payload, req); err != nil {
			return nil, err
		}
		if err := p.ensureSessionOwner(ctx, rt, req.GetSessionId()); err != nil {
			return nil, err
		}
		return p.requireMarketData().ReleaseMarketDataLease(ctx, req)

	case "marketdata.CreateSessionMarketDataSubscriptions":
		req := &mdv1.CreateSessionMarketDataSubscriptionsRequest{}
		if err := unpackRuntimePayload(payload, req); err != nil {
			return nil, err
		}
		if req.GetUserId() != 0 && req.GetUserId() != rt.UserID {
			return nil, status.Error(codes.PermissionDenied, "user_id does not match authenticated runtime")
		}
		if req.GetRuntimeId() != "" && req.GetRuntimeId() != rt.RuntimeID {
			return nil, status.Error(codes.PermissionDenied, "subscription runtime_id does not match authenticated runtime")
		}
		req.UserId = rt.UserID
		req.RuntimeId = rt.RuntimeID
		return p.requireMarketData().CreateSessionMarketDataSubscriptions(ctx, req)

	case "marketdata.ReleaseSessionMarketDataSubscriptions":
		req := &mdv1.ReleaseSessionMarketDataSubscriptionsRequest{}
		if err := unpackRuntimePayload(payload, req); err != nil {
			return nil, err
		}
		if req.GetRuntimeId() != "" && req.GetRuntimeId() != rt.RuntimeID {
			return nil, status.Error(codes.PermissionDenied, "subscription runtime_id does not match authenticated runtime")
		}
		req.RuntimeId = rt.RuntimeID
		if err := p.ensureSessionOwner(ctx, rt, req.GetSessionId()); err != nil {
			return nil, err
		}
		return p.requireMarketData().ReleaseSessionMarketDataSubscriptions(ctx, req)

	case "logs.Emit":
		return p.emitRuntimeLog(ctx, rt, payload)

	case "notification.Publish":
		return p.publishRuntimeNotification(ctx, rt, payload)

	case "debug.StartDebugReplay":
		req := &cpv1.StartDebugReplayRequest{}
		if err := unpackRuntimePayload(payload, req); err != nil {
			return nil, err
		}
		if req.GetUserId() != 0 && req.GetUserId() != rt.UserID {
			return nil, status.Error(codes.PermissionDenied, "user_id does not match authenticated runtime")
		}
		if req.GetRuntimeId() != "" && req.GetRuntimeId() != rt.RuntimeID {
			return nil, status.Error(codes.PermissionDenied, "debug runtime_id does not match authenticated runtime")
		}
		starter := p.requireDebugReplayStarter()
		sessionID, sessionName, dataset, err := starter.StartDebugReplay(ctx, rt.UserID, rt.RuntimeID, req.GetRequestedName())
		if err != nil {
			return nil, err
		}
		return &cpv1.StartDebugReplayResponse{
			SessionId:   sessionID,
			SessionName: sessionName,
			Dataset:     debugDatasetStateToProto(dataset),
		}, nil

	default:
		return nil, status.Errorf(codes.Unimplemented, "unsupported runtime platform method: %s", method)
	}
}

func (p *PlatformProxy) requireAccount() AccountPlatformClient {
	if p == nil || p.account == nil {
		return unavailableAccountClient{}
	}
	return p.account
}

func (p *PlatformProxy) requireOrder() OrderPlatformClient {
	if p == nil || p.order == nil {
		return unavailableOrderClient{}
	}
	return p.order
}

func (p *PlatformProxy) requireMarketData() MarketDataPlatformServer {
	if p == nil || p.marketData == nil {
		return unavailableMarketDataClient{}
	}
	return p.marketData
}

func (p *PlatformProxy) requireKlineQuery() KlineQuerier {
	if p == nil || p.klineQuery == nil {
		return nil
	}
	return p.klineQuery
}

func (p *PlatformProxy) requireDatasetDeliverer() DatasetDeliverer {
	if p == nil || p.datasetDeliverer == nil {
		return nil
	}
	return p.datasetDeliverer
}

func (p *PlatformProxy) requireDebugReplayStarter() DebugReplayStarter {
	if p == nil || p.debugReplay == nil {
		return unavailableDebugReplayStarter{}
	}
	return p.debugReplay
}

func (p *PlatformProxy) requireNotificationPublisher() cpnotify.Publisher {
	if p == nil || p.notifications == nil {
		return cpnotify.NoopPublisher{}
	}
	return p.notifications
}

func (p *PlatformProxy) ensureAccountOwner(ctx context.Context, rt AuthenticatedRuntime, accountID int64) error {
	if accountID <= 0 {
		return status.Error(codes.InvalidArgument, "account_id is required")
	}
	resp, err := p.requireAccount().GetAccount(ctx, &accountv1.GetAccountRequest{
		AccountId: accountID,
		UserId:    rt.UserID,
	})
	if err != nil {
		return err
	}
	if resp == nil || resp.GetAccount() == nil {
		return status.Error(codes.NotFound, "account not found")
	}
	if resp.GetAccount().GetUserId() != 0 && resp.GetAccount().GetUserId() != rt.UserID {
		return status.Error(codes.PermissionDenied, "account does not belong to authenticated runtime user")
	}
	return nil
}

func (p *PlatformProxy) ensureSessionOwner(ctx context.Context, rt AuthenticatedRuntime, sessionID string) error {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return status.Error(codes.InvalidArgument, "session_id is required")
	}
	resp, err := p.requireAccount().GetSession(ctx, &accountv1.GetSessionRequest{
		SessionId: sessionID,
		UserId:    rt.UserID,
	})
	if err != nil {
		return err
	}
	session := resp.GetSession()
	if session == nil {
		return status.Error(codes.NotFound, "session not found")
	}
	if session.GetUserId() != 0 && session.GetUserId() != rt.UserID {
		return status.Error(codes.PermissionDenied, "session does not belong to authenticated runtime user")
	}
	if session.GetRuntimeId() == "" {
		return status.Error(codes.FailedPrecondition, "session is not bound to a runtime")
	}
	if session.GetRuntimeId() != rt.RuntimeID {
		return status.Error(codes.PermissionDenied, "session does not belong to authenticated runtime")
	}
	switch strings.ToLower(strings.TrimSpace(session.GetStatus())) {
	case "running", "stopping":
		return nil
	default:
		return status.Errorf(codes.FailedPrecondition, "session %s is not active: %s", sessionID, session.GetStatus())
	}
}

func unpackRuntimePayload(payload *anypb.Any, out proto.Message) error {
	if payload == nil {
		return status.Error(codes.InvalidArgument, "runtime platform request payload is required")
	}
	if err := payload.UnmarshalTo(out); err != nil {
		return status.Errorf(codes.InvalidArgument, "unpack runtime platform request: %v", err)
	}
	return nil
}

func runtimeSourceFromAuthenticated(rt AuthenticatedRuntime) string {
	if strings.TrimSpace(rt.Source) != "" {
		return rt.Source
	}
	return domain.RuntimeSourceSelfHosted
}

func canonicalPlatformMethod(method string) string {
	method = strings.TrimSpace(method)
	method = strings.TrimPrefix(method, "/")
	switch method {
	case "GetPortfolioSnapshot", "account.v1.AccountService/GetPortfolioSnapshot":
		return "account.GetPortfolioSnapshot"
	case "UpdatePortfolioSnapshot", "account.v1.AccountService/UpdatePortfolioSnapshot":
		return "account.UpdatePortfolioSnapshot"
	case "UpdateAccountWalletState", "account.v1.AccountService/UpdateAccountWalletState":
		return "account.UpdateAccountWalletState"
	case "PreflightStrategySession", "account.v1.AccountService/PreflightStrategySession":
		return "account.PreflightStrategySession"
	case "GetActiveStrategy", "account.v1.AccountService/GetActiveStrategy":
		return "account.GetActiveStrategy"
	case "SaveSession", "account.v1.AccountService/SaveSession":
		return "account.SaveSession"
	case "UpdateSession", "account.v1.AccountService/UpdateSession":
		return "account.UpdateSession"
	case "PlaceOrder", "order.v1.OrderService/PlaceOrder":
		return "order.PlaceOrder"
	case "ResolveOrderAttempt", "order.v1.OrderService/ResolveOrderAttempt":
		return "order.ResolveOrderAttempt"
	case "GetMarketDataStreamStatus", "marketdata.v1.MarketDataControlPlaneService/GetMarketDataStreamStatus", "controlpanel.marketdata.v1.MarketDataControlPlaneService/GetMarketDataStreamStatus":
		return "marketdata.GetMarketDataStreamStatus"
	case "FetchKlines":
		return "marketdata.FetchKlines"
	case "DeliverDataset", "marketdata.DeliverDataset":
		return "marketdata.DeliverDataset"
	case "CreateOrRenewMarketDataLease", "marketdata.v1.MarketDataControlPlaneService/CreateOrRenewMarketDataLease", "controlpanel.marketdata.v1.MarketDataControlPlaneService/CreateOrRenewMarketDataLease":
		return "marketdata.CreateOrRenewMarketDataLease"
	case "ReleaseMarketDataLease", "marketdata.v1.MarketDataControlPlaneService/ReleaseMarketDataLease", "controlpanel.marketdata.v1.MarketDataControlPlaneService/ReleaseMarketDataLease":
		return "marketdata.ReleaseMarketDataLease"
	case "CreateSessionMarketDataSubscriptions", "marketdata.v1.MarketDataControlPlaneService/CreateSessionMarketDataSubscriptions", "controlpanel.marketdata.v1.MarketDataControlPlaneService/CreateSessionMarketDataSubscriptions":
		return "marketdata.CreateSessionMarketDataSubscriptions"
	case "ReleaseSessionMarketDataSubscriptions", "marketdata.v1.MarketDataControlPlaneService/ReleaseSessionMarketDataSubscriptions", "controlpanel.marketdata.v1.MarketDataControlPlaneService/ReleaseSessionMarketDataSubscriptions":
		return "marketdata.ReleaseSessionMarketDataSubscriptions"
	case "EmitLog", "logs.Emit":
		return "logs.Emit"
	case "PublishNotification", "notification.Publish":
		return "notification.Publish"
	case "StartDebugReplay", "debug.StartDebugReplay":
		return "debug.StartDebugReplay"
	default:
		return method
	}
}

func debugDatasetStateToProto(state domain.DebugDatasetState) *cpv1.DebugDatasetState {
	return &cpv1.DebugDatasetState{
		DatasetId:      state.DatasetID,
		UserId:         state.UserID,
		AccountId:      state.AccountID,
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

func unpackKlineQueryPayload(payload *anypb.Any) (KlineQuery, error) {
	st := &structpb.Struct{}
	if err := unpackRuntimePayload(payload, st); err != nil {
		return KlineQuery{}, err
	}
	fields := st.GetFields()
	return KlineQuery{
		Exchange:    stringField(fields, "exchange"),
		Market:      stringField(fields, "market"),
		Symbol:      stringField(fields, "symbol"),
		Interval:    stringField(fields, "interval"),
		StartTimeMS: int64(numberField(fields, "start_time_ms")),
		EndTimeMS:   int64(numberField(fields, "end_time_ms")),
		Limit:       int(numberField(fields, "limit")),
	}, nil
}

func unpackDatasetDeliveryPayload(payload *anypb.Any) (datasetDeliveryRequest, error) {
	st := &structpb.Struct{}
	if err := unpackRuntimePayload(payload, st); err != nil {
		return datasetDeliveryRequest{}, err
	}
	fields := st.GetFields()
	req := datasetDeliveryRequest{
		SessionID:   strings.TrimSpace(stringField(fields, "session_id")),
		RuntimeID:   strings.TrimSpace(stringField(fields, "runtime_id")),
		StartTimeMS: int64(numberField(fields, "start_time_ms")),
		EndTimeMS:   int64(numberField(fields, "end_time_ms")),
		ChunkSize:   int(numberField(fields, "chunk_size")),
	}
	if req.SessionID == "" {
		return datasetDeliveryRequest{}, status.Error(codes.InvalidArgument, "session_id is required")
	}
	if req.StartTimeMS <= 0 || req.EndTimeMS <= 0 || req.EndTimeMS <= req.StartTimeMS {
		return datasetDeliveryRequest{}, status.Error(codes.InvalidArgument, "invalid dataset time range")
	}
	if req.ChunkSize <= 0 {
		req.ChunkSize = defaultKlineFetchLimit
	}
	if req.ChunkSize > maxKlineFetchLimit {
		req.ChunkSize = maxKlineFetchLimit
	}
	streamValue := fields["streams"]
	if streamValue == nil {
		return datasetDeliveryRequest{}, status.Error(codes.InvalidArgument, "streams are required")
	}
	streams := streamValue.GetListValue()
	if streams == nil || len(streams.GetValues()) == 0 {
		return datasetDeliveryRequest{}, status.Error(codes.InvalidArgument, "streams are required")
	}
	for _, value := range streams.GetValues() {
		streamStruct := value.GetStructValue()
		if streamStruct == nil {
			return datasetDeliveryRequest{}, status.Error(codes.InvalidArgument, "stream entry must be an object")
		}
		streamFields := streamStruct.GetFields()
		kind := strings.ToLower(strings.TrimSpace(stringField(streamFields, "kind")))
		if kind == "" {
			kind = "kline"
		}
		if kind != "kline" {
			return datasetDeliveryRequest{}, status.Errorf(codes.InvalidArgument, "unsupported dataset stream kind %q", kind)
		}
		query := KlineQuery{
			Exchange: strings.ToLower(strings.TrimSpace(stringField(streamFields, "exchange"))),
			Market:   strings.ToLower(strings.TrimSpace(stringField(streamFields, "market"))),
			Symbol:   strings.ToUpper(strings.TrimSpace(stringField(streamFields, "symbol"))),
			Interval: strings.ToLower(strings.TrimSpace(stringField(streamFields, "interval"))),
		}
		if query.Exchange == "" {
			query.Exchange = "binance"
		}
		req.Streams = append(req.Streams, query)
	}
	return req, nil
}

func (p *PlatformProxy) deliverDataset(ctx context.Context, rt AuthenticatedRuntime, req datasetDeliveryRequest) (*structpb.Struct, error) {
	query := p.requireKlineQuery()
	if query == nil {
		return nil, status.Error(codes.FailedPrecondition, "market-data query is not configured")
	}
	deliverer := p.requireDatasetDeliverer()
	if deliverer == nil {
		return nil, status.Error(codes.FailedPrecondition, "dataset delivery is not configured")
	}
	chunks := 0
	totalRows := 0
	for _, stream := range req.Streams {
		nextStart := req.StartTimeMS
		stepMS, err := intervalStepMS(stream.Interval)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "dataset interval: %v", err)
		}
		for nextStart < req.EndTimeMS {
			stream.StartTimeMS = nextStart
			stream.EndTimeMS = req.EndTimeMS
			stream.Limit = req.ChunkSize
			rows, err := query.FetchKlines(ctx, stream)
			if err != nil {
				return nil, status.Errorf(codes.Unavailable, "fetch dataset klines: %v", err)
			}
			if len(rows) == 0 {
				break
			}
			payload, err := klineRowsToDatasetPayload(rows)
			if err != nil {
				return nil, err
			}
			chunks++
			totalRows += len(rows)
			if err := deliverer.DeliverDatasetChunk(ctx, DatasetChunkDelivery{
				UserID:    rt.UserID,
				RuntimeID: rt.RuntimeID,
				SessionID: req.SessionID,
				DatasetID: datasetIDForKlineStream(stream),
				Payload:   payload,
			}); err != nil {
				return nil, err
			}
			lastOpen := rows[len(rows)-1].OpenTime
			nextStart = lastOpen + stepMS
			if len(rows) < req.ChunkSize || nextStart >= req.EndTimeMS {
				break
			}
		}
	}
	chunks++
	if err := deliverer.DeliverDatasetChunk(ctx, DatasetChunkDelivery{
		UserID:    rt.UserID,
		RuntimeID: rt.RuntimeID,
		SessionID: req.SessionID,
		DatasetID: "dataset:" + req.SessionID,
		Payload:   []byte(`{"klines":[]}`),
		End:       true,
	}); err != nil {
		return nil, err
	}
	return structpb.NewStruct(map[string]any{
		"rows":   float64(totalRows),
		"chunks": float64(chunks),
	})
}

func klineRowsToDatasetPayload(rows []KlineRow) ([]byte, error) {
	out := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		out = append(out, map[string]any{
			"exchange":   row.Exchange,
			"market":     row.Market,
			"symbol":     row.Symbol,
			"interval":   row.Interval,
			"open_time":  row.OpenTime,
			"close_time": row.CloseTime,
			"timestamp":  row.Timestamp,
			"open":       row.Open,
			"high":       row.High,
			"low":        row.Low,
			"close":      row.Close,
			"volume":     row.Volume,
		})
	}
	return json.Marshal(map[string]any{"klines": out})
}

func datasetIDForKlineStream(stream KlineQuery) string {
	exchange := stream.Exchange
	if exchange == "" {
		exchange = "binance"
	}
	return strings.Join([]string{
		strings.ToLower(exchange),
		strings.ToLower(stream.Market),
		"kline",
		strings.ToUpper(stream.Symbol),
		strings.ToLower(stream.Interval),
	}, "/")
}

func klineRowsToStruct(rows []KlineRow) (*structpb.Struct, error) {
	values := make([]*structpb.Value, 0, len(rows))
	for _, row := range rows {
		st, err := structpb.NewStruct(map[string]any{
			"exchange":   row.Exchange,
			"market":     row.Market,
			"symbol":     row.Symbol,
			"interval":   row.Interval,
			"open_time":  float64(row.OpenTime),
			"close_time": float64(row.CloseTime),
			"timestamp":  float64(row.Timestamp),
			"open":       row.Open,
			"high":       row.High,
			"low":        row.Low,
			"close":      row.Close,
			"volume":     row.Volume,
		})
		if err != nil {
			return nil, status.Errorf(codes.Internal, "encode kline row: %v", err)
		}
		values = append(values, structpb.NewStructValue(st))
	}
	return structpb.NewStruct(map[string]any{
		"klines": structpb.NewListValue(&structpb.ListValue{Values: values}).AsInterface(),
		"count":  float64(len(rows)),
	})
}

func stringField(fields map[string]*structpb.Value, name string) string {
	if fields == nil || fields[name] == nil {
		return ""
	}
	return fields[name].GetStringValue()
}

func numberField(fields map[string]*structpb.Value, name string) float64 {
	if fields == nil || fields[name] == nil {
		return 0
	}
	return fields[name].GetNumberValue()
}

func (p *PlatformProxy) emitRuntimeLog(ctx context.Context, rt AuthenticatedRuntime, payload *anypb.Any) (*structpb.Struct, error) {
	st := &structpb.Struct{}
	if err := unpackRuntimePayload(payload, st); err != nil {
		return nil, err
	}
	fields := st.GetFields()
	if userID := int64(numberField(fields, "user_id")); userID != 0 && userID != rt.UserID {
		return nil, status.Error(codes.PermissionDenied, "log user_id does not match authenticated runtime")
	}
	if accountID := int64(numberField(fields, "account_id")); accountID > 0 {
		if err := p.ensureAccountOwner(ctx, rt, accountID); err != nil {
			return nil, err
		}
	}
	if sessionID := stringField(fields, "session_id"); strings.TrimSpace(sessionID) != "" {
		if err := p.ensureSessionOwner(ctx, rt, sessionID); err != nil {
			return nil, err
		}
	}

	logType := strings.TrimSpace(stringField(fields, "log_type"))
	if logType == "" {
		logType = "root"
	}
	level := strings.ToLower(strings.TrimSpace(stringField(fields, "level")))
	msg := runtimeLogMessage(rt, st)
	switch level {
	case "debug":
		logger.Debug(ctx, logType, msg)
	case "warn", "warning":
		logger.Warn(ctx, logType, msg)
	case "error", "critical", "fatal":
		logger.Error(ctx, logType, msg)
	default:
		logger.Info(ctx, logType, msg)
	}
	return &structpb.Struct{}, nil
}

func (p *PlatformProxy) publishRuntimeNotification(ctx context.Context, rt AuthenticatedRuntime, payload *anypb.Any) (*structpb.Struct, error) {
	st := &structpb.Struct{}
	if err := unpackRuntimePayload(payload, st); err != nil {
		return nil, err
	}
	fields := st.GetFields()
	if userID := int64(numberField(fields, "user_id")); userID != 0 && userID != rt.UserID {
		return nil, status.Error(codes.PermissionDenied, "notification user_id does not match authenticated runtime")
	}
	if runtimeID := strings.TrimSpace(stringField(fields, "runtime_id")); runtimeID != "" && runtimeID != rt.RuntimeID {
		return nil, status.Error(codes.PermissionDenied, "notification runtime_id does not match authenticated runtime")
	}
	accountID := int64(numberField(fields, "account_id"))
	if accountID > 0 {
		if err := p.ensureAccountOwner(ctx, rt, accountID); err != nil {
			return nil, err
		}
	}
	sessionID := strings.TrimSpace(stringField(fields, "session_id"))
	if sessionID != "" {
		if err := p.ensureSessionOwner(ctx, rt, sessionID); err != nil {
			return nil, err
		}
	}
	category := normalizeNotificationCategory(stringField(fields, "category"))
	severity := normalizeNotificationSeverity(stringField(fields, "severity"))
	message := strings.TrimSpace(stringField(fields, "message"))
	if message == "" {
		return nil, status.Error(codes.InvalidArgument, "notification message is required")
	}
	event := cpnotify.Event{
		SchemaVersion: cpnotify.SchemaVersion,
		UserID:        rt.UserID,
		Category:      category,
		EventType:     notificationEventType(category, severity),
		Severity:      severity,
		RuntimeID:     rt.RuntimeID,
		RuntimeName:   rt.Name,
		AccountID:     accountID,
		StrategyID:    int64(numberField(fields, "strategy_id")),
		SessionID:     sessionID,
		Title:         strings.TrimSpace(stringField(fields, "title")),
		Message:       message,
		DedupeKey:     strings.TrimSpace(stringField(fields, "dedupe_key")),
	}
	if err := p.requireNotificationPublisher().Publish(ctx, event); err != nil {
		return nil, status.Errorf(codes.Unavailable, "publish notification: %v", err)
	}
	return structpb.NewStruct(map[string]any{"accepted": true})
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

func runtimeLogMessage(rt AuthenticatedRuntime, st *structpb.Struct) string {
	data := map[string]any{}
	if st != nil {
		for k, v := range st.AsMap() {
			if isRuntimeLogReservedKey(k) {
				continue
			}
			data[k] = v
		}
	}
	data["source"] = "self_hosted_runtime"
	data["runtime_id"] = rt.RuntimeID
	data["user_id"] = rt.UserID
	data["runtime_name"] = rt.Name
	b, err := json.Marshal(data)
	if err != nil {
		return "self_hosted_runtime log encode failed"
	}
	return string(b)
}

func isRuntimeLogReservedKey(key string) bool {
	switch key {
	case "source", "runtime_id", "user_id", "runtime_name", "service_name":
		return true
	default:
		return false
	}
}

type unavailableAccountClient struct{}

func (unavailableAccountClient) GetAccount(context.Context, *accountv1.GetAccountRequest, ...grpc.CallOption) (*accountv1.GetAccountResponse, error) {
	return nil, status.Error(codes.Unavailable, "core-service platform client is not configured")
}
func (unavailableAccountClient) GetSession(context.Context, *accountv1.GetSessionRequest, ...grpc.CallOption) (*accountv1.GetSessionResponse, error) {
	return nil, status.Error(codes.Unavailable, "core-service platform client is not configured")
}
func (unavailableAccountClient) GetPortfolioSnapshot(context.Context, *accountv1.GetPortfolioSnapshotRequest, ...grpc.CallOption) (*accountv1.GetPortfolioSnapshotResponse, error) {
	return nil, status.Error(codes.Unavailable, "core-service platform client is not configured")
}
func (unavailableAccountClient) UpdatePortfolioSnapshot(context.Context, *accountv1.UpdatePortfolioSnapshotRequest, ...grpc.CallOption) (*accountv1.UpdatePortfolioSnapshotResponse, error) {
	return nil, status.Error(codes.Unavailable, "core-service platform client is not configured")
}
func (unavailableAccountClient) UpdateAccountWalletState(context.Context, *accountv1.UpdateAccountWalletStateRequest, ...grpc.CallOption) (*accountv1.UpdateAccountWalletStateResponse, error) {
	return nil, status.Error(codes.Unavailable, "core-service platform client is not configured")
}
func (unavailableAccountClient) PreflightStrategySession(context.Context, *accountv1.PreflightStrategySessionRequest, ...grpc.CallOption) (*accountv1.PreflightStrategySessionResponse, error) {
	return nil, status.Error(codes.Unavailable, "core-service platform client is not configured")
}
func (unavailableAccountClient) GetActiveStrategy(context.Context, *accountv1.GetActiveStrategyRequest, ...grpc.CallOption) (*accountv1.GetActiveStrategyResponse, error) {
	return nil, status.Error(codes.Unavailable, "core-service platform client is not configured")
}
func (unavailableAccountClient) SaveSession(context.Context, *accountv1.SaveSessionRequest, ...grpc.CallOption) (*accountv1.SaveSessionResponse, error) {
	return nil, status.Error(codes.Unavailable, "core-service platform client is not configured")
}
func (unavailableAccountClient) UpdateSession(context.Context, *accountv1.UpdateSessionRequest, ...grpc.CallOption) (*accountv1.UpdateSessionResponse, error) {
	return nil, status.Error(codes.Unavailable, "core-service platform client is not configured")
}

type unavailableOrderClient struct{}

func (unavailableOrderClient) PlaceOrder(context.Context, *orderv1.PlaceOrderRequest, ...grpc.CallOption) (*orderv1.PlaceOrderResponse, error) {
	return nil, status.Error(codes.Unavailable, "order-service platform client is not configured")
}
func (unavailableOrderClient) ResolveOrderAttempt(context.Context, *orderv1.ResolveOrderAttemptRequest, ...grpc.CallOption) (*orderv1.ResolveOrderAttemptResponse, error) {
	return nil, status.Error(codes.Unavailable, "order-service platform client is not configured")
}

type unavailableMarketDataClient struct{}

func (unavailableMarketDataClient) GetMarketDataStreamStatus(context.Context, *mdv1.GetMarketDataStreamStatusRequest) (*mdv1.GetMarketDataStreamStatusResponse, error) {
	return nil, status.Error(codes.Unavailable, "market-data platform client is not configured")
}
func (unavailableMarketDataClient) CreateOrRenewMarketDataLease(context.Context, *mdv1.CreateOrRenewMarketDataLeaseRequest) (*mdv1.CreateOrRenewMarketDataLeaseResponse, error) {
	return nil, status.Error(codes.Unavailable, "market-data platform client is not configured")
}
func (unavailableMarketDataClient) ReleaseMarketDataLease(context.Context, *mdv1.ReleaseMarketDataLeaseRequest) (*mdv1.ReleaseMarketDataLeaseResponse, error) {
	return nil, status.Error(codes.Unavailable, "market-data platform client is not configured")
}
func (unavailableMarketDataClient) CreateSessionMarketDataSubscriptions(context.Context, *mdv1.CreateSessionMarketDataSubscriptionsRequest) (*mdv1.CreateSessionMarketDataSubscriptionsResponse, error) {
	return nil, status.Error(codes.Unavailable, "market-data platform client is not configured")
}
func (unavailableMarketDataClient) ReleaseSessionMarketDataSubscriptions(context.Context, *mdv1.ReleaseSessionMarketDataSubscriptionsRequest) (*mdv1.ReleaseSessionMarketDataSubscriptionsResponse, error) {
	return nil, status.Error(codes.Unavailable, "market-data platform client is not configured")
}

type unavailableDebugReplayStarter struct{}

func (unavailableDebugReplayStarter) StartDebugReplay(context.Context, int64, string, string) (string, string, domain.DebugDatasetState, error) {
	return "", "", domain.DebugDatasetState{}, status.Error(codes.Unavailable, "debug replay starter is not configured")
}
