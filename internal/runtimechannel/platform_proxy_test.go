package runtimechannel

import (
	"context"
	"encoding/json"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	cpv1 "github.com/hushine-tech/control-panel-service/gen/controlpanelv1"
	mdv1 "github.com/hushine-tech/control-panel-service/gen/marketdatav1"
	cpnotify "github.com/hushine-tech/control-panel-service/internal/notification"
	orderv1 "github.com/hushine-tech/core-service/gen/orderv1"
	portfoliov1 "github.com/hushine-tech/core-service/gen/portfoliov1"
	cerrors "github.com/hushine-tech/golang-lib/pkg/errors"
	strategyv1 "github.com/hushine-tech/strategy-service/gen/strategyv1"
)

func TestStreamErrorToStatusPreservesDependencyDetails(t *testing.T) {
	dependency := &strategyv1.RuntimeDependencyError{
		Code:                  "STRATEGY_DEPENDENCY_UNAVAILABLE",
		Module:                "google.cloud",
		RuntimeProfile:        "platform-python-3.13",
		RuntimeProfileVersion: "1.0.0",
		ImageBuildId:          "build-1",
		Message:               "Python module 'google.cloud' is not available",
	}
	err := streamErrorToStatus(&cpv1.StreamError{
		Code:            "FailedPrecondition",
		Message:         dependency.GetMessage(),
		DependencyError: dependency,
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("status code = %s, want FailedPrecondition", status.Code(err))
	}
	common := cerrors.FromGRPCStatus(status.Convert(err))
	want := map[string]string{
		"code":                    dependency.GetCode(),
		"module":                  dependency.GetModule(),
		"runtime_profile":         dependency.GetRuntimeProfile(),
		"runtime_profile_version": dependency.GetRuntimeProfileVersion(),
		"image_build_id":          dependency.GetImageBuildId(),
		"message":                 dependency.GetMessage(),
	}
	if !reflect.DeepEqual(common.Details, want) {
		t.Fatalf("details = %#v, want %#v", common.Details, want)
	}
	if common.HTTPStatus != http.StatusBadRequest {
		t.Fatalf("http status = %d, want %d", common.HTTPStatus, http.StatusBadRequest)
	}
}

func TestPlatformProxySaveSessionBindsAuthenticatedRuntime(t *testing.T) {
	portfolio := &fakePortfolioPlatformClient{}
	proxy := NewPlatformProxy(portfolio, nil, nil)
	payload, err := anypb.New(&portfoliov1.SaveSessionRequest{
		SessionId:   "sess-1",
		PortfolioId: 7,
		StrategyId:  9,
		Environment: 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = proxy.DispatchRuntimeRequest(
		context.Background(),
		AuthenticatedRuntime{UserID: 42, RuntimeID: "runtime-1", Name: "desk", Source: "self_hosted"},
		"portfolio.SaveSession",
		payload,
	)
	if err != nil {
		t.Fatalf("DispatchRuntimeRequest: %v", err)
	}

	if portfolio.getPortfolioReq.GetUserId() != 42 || portfolio.getPortfolioReq.GetPortfolioId() != 7 {
		t.Fatalf("GetPortfolio req = %+v", portfolio.getPortfolioReq)
	}
	if portfolio.saveReq.GetRuntimeId() != "runtime-1" ||
		portfolio.saveReq.GetRuntimeSource() != "self_hosted" ||
		portfolio.saveReq.GetRuntimeName() != "desk" {
		t.Fatalf("SaveSession runtime binding = %+v", portfolio.saveReq)
	}
}

func TestPlatformProxySaveSessionPreservesHostedRuntimeSource(t *testing.T) {
	portfolio := &fakePortfolioPlatformClient{}
	proxy := NewPlatformProxy(portfolio, nil, nil)
	payload, err := anypb.New(&portfoliov1.SaveSessionRequest{
		SessionId:   "sess-1",
		PortfolioId: 7,
		StrategyId:  9,
		Environment: 0,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = proxy.DispatchRuntimeRequest(
		context.Background(),
		AuthenticatedRuntime{UserID: 42, RuntimeID: "rt-hosted", Name: "hosted-test", Source: "hosted"},
		"portfolio.SaveSession",
		payload,
	)
	if err != nil {
		t.Fatalf("DispatchRuntimeRequest: %v", err)
	}

	if portfolio.saveReq.GetRuntimeId() != "rt-hosted" ||
		portfolio.saveReq.GetRuntimeSource() != "hosted" ||
		portfolio.saveReq.GetRuntimeName() != "hosted-test" {
		t.Fatalf("SaveSession runtime binding = %+v", portfolio.saveReq)
	}
}

func TestPlatformProxyGetSessionAllowsOwningRuntime(t *testing.T) {
	portfolio := &fakePortfolioPlatformClient{
		session: &portfoliov1.StrategySessionEntry{
			SessionId: "sess-1",
			UserId:    42,
			RuntimeId: "runtime-1",
			Status:    "running",
		},
	}
	proxy := NewPlatformProxy(portfolio, nil, nil)
	payload, err := anypb.New(&portfoliov1.GetSessionRequest{SessionId: "sess-1"})
	if err != nil {
		t.Fatal(err)
	}

	resp, err := proxy.DispatchRuntimeRequest(
		context.Background(),
		AuthenticatedRuntime{UserID: 42, RuntimeID: "runtime-1", Name: "desk"},
		"portfolio.GetSession",
		payload,
	)
	if err != nil {
		t.Fatalf("DispatchRuntimeRequest: %v", err)
	}

	sessionResp, ok := resp.(*portfoliov1.GetSessionResponse)
	if !ok || sessionResp.GetSession().GetSessionId() != "sess-1" {
		t.Fatalf("response = %+v", resp)
	}
	if portfolio.getSessionReq.GetUserId() != 42 || portfolio.getSessionReq.GetSessionId() != "sess-1" {
		t.Fatalf("GetSession req = %+v", portfolio.getSessionReq)
	}
}

func TestPlatformProxyListSessionsBindsUserAndRuntime(t *testing.T) {
	portfolio := &fakePortfolioPlatformClient{
		listSessionsResp: []*portfoliov1.StrategySessionEntry{{
			SessionId: "sess-1",
			UserId:    42,
			RuntimeId: "runtime-1",
			Status:    "running",
		}},
	}
	proxy := NewPlatformProxy(portfolio, nil, nil)
	payload, err := anypb.New(&portfoliov1.ListSessionsRequest{Limit: 1, RuntimeId: "runtime-other"})
	if err != nil {
		t.Fatal(err)
	}

	resp, err := proxy.DispatchRuntimeRequest(
		context.Background(),
		AuthenticatedRuntime{UserID: 42, RuntimeID: "runtime-1", Name: "desk"},
		"portfolio.ListSessions",
		payload,
	)
	if err != nil {
		t.Fatalf("DispatchRuntimeRequest: %v", err)
	}

	listResp, ok := resp.(*portfoliov1.ListSessionsResponse)
	if !ok || len(listResp.GetSessions()) != 1 {
		t.Fatalf("response = %+v", resp)
	}
	if portfolio.listSessionsReq.GetUserId() != 42 || portfolio.listSessionsReq.GetRuntimeId() != "runtime-1" || portfolio.listSessionsReq.GetLimit() != 1 {
		t.Fatalf("ListSessions req = %+v", portfolio.listSessionsReq)
	}
}

func TestPlatformProxySaveStrategyIndicatorsInjectsAuthenticatedUser(t *testing.T) {
	portfolio := &fakePortfolioPlatformClient{
		session: &portfoliov1.StrategySessionEntry{
			SessionId: "sess-1",
			UserId:    42,
			RuntimeId: "runtime-1",
			Status:    "finished",
		},
	}
	proxy := NewPlatformProxy(portfolio, nil, nil)
	payload, err := anypb.New(&portfoliov1.SaveStrategyIndicatorsRequest{
		SessionId: "sess-1",
		Definitions: []*portfoliov1.StrategyIndicatorDefinition{{
			StreamKey:    "binance:perpetual_futures:ETHUSDT:1m",
			IndicatorKey: "alpha",
			Type:         "line",
			Pane:         "strategy",
		}},
		Chunks: []*portfoliov1.StrategyIndicatorChunk{{
			StreamKey:    "binance:perpetual_futures:ETHUSDT:1m",
			IndicatorKey: "alpha",
			ChunkIndex:   0,
			StartTimeMs:  1000,
			EndTimeMs:    1000,
			IntervalMs:   60000,
			Count:        1,
			ValuesJson:   `{"values":[1.0]}`,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = proxy.DispatchRuntimeRequest(
		context.Background(),
		AuthenticatedRuntime{UserID: 42, RuntimeID: "runtime-1", Name: "desk"},
		"portfolio.SaveStrategyIndicators",
		payload,
	)
	if err != nil {
		t.Fatalf("DispatchRuntimeRequest: %v", err)
	}

	if portfolio.saveIndicatorsReq.GetUserId() != 42 ||
		portfolio.saveIndicatorsReq.GetSessionId() != "sess-1" ||
		len(portfolio.saveIndicatorsReq.GetDefinitions()) != 1 ||
		len(portfolio.saveIndicatorsReq.GetChunks()) != 1 {
		t.Fatalf("SaveStrategyIndicators req = %+v", portfolio.saveIndicatorsReq)
	}
}

func TestPlatformProxyRejectsSessionMutationFromDifferentRuntime(t *testing.T) {
	portfolio := &fakePortfolioPlatformClient{
		session: &portfoliov1.StrategySessionEntry{
			SessionId: "sess-1",
			UserId:    42,
			RuntimeId: "runtime-other",
		},
	}
	proxy := NewPlatformProxy(portfolio, nil, nil)
	payload, err := anypb.New(&portfoliov1.UpdateSessionRequest{
		SessionId: "sess-1",
		Status:    "stopped",
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = proxy.DispatchRuntimeRequest(
		context.Background(),
		AuthenticatedRuntime{UserID: 42, RuntimeID: "runtime-1", Name: "desk"},
		"portfolio.UpdateSession",
		payload,
	)
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("code = %v, want PermissionDenied (err=%v)", status.Code(err), err)
	}
	if portfolio.updateReq != nil {
		t.Fatalf("UpdateSession should not be called: %+v", portfolio.updateReq)
	}
}

func TestPlatformProxyRejectsUnknownPortfolioWalletMethods(t *testing.T) {
	portfolio := &fakePortfolioPlatformClient{}
	proxy := NewPlatformProxy(portfolio, nil, nil)

	getPayload, err := anypb.New(&portfoliov1.GetPortfolioSnapshotRequest{PortfolioId: 7})
	if err != nil {
		t.Fatal(err)
	}
	updatePayload, err := anypb.New(&portfoliov1.UpdatePortfolioSnapshotRequest{PortfolioId: 7})
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name    string
		method  string
		payload *anypb.Any
	}{
		{name: "unknown wallet read", method: "portfolio.RemovedWalletRead", payload: getPayload},
		{name: "unknown wallet write", method: "portfolio.RemovedWalletWrite", payload: updatePayload},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := proxy.DispatchRuntimeRequest(
				context.Background(),
				AuthenticatedRuntime{UserID: 42, RuntimeID: "runtime-1", Name: "desk"},
				tc.method,
				tc.payload,
			)
			if status.Code(err) != codes.Unimplemented {
				t.Fatalf("code = %v, want Unimplemented (err=%v)", status.Code(err), err)
			}
			if portfolio.getPortfolioReq != nil || portfolio.portfolioGetReq != nil || portfolio.portfolioUpdateReq != nil {
				t.Fatalf("removed method touched portfolio client: get=%+v portfolio_get=%+v portfolio_update=%+v", portfolio.getPortfolioReq, portfolio.portfolioGetReq, portfolio.portfolioUpdateReq)
			}
		})
	}
}

func TestPlatformProxyGetPortfolioSnapshotInjectsAuthenticatedUser(t *testing.T) {
	portfolio := &fakePortfolioPlatformClient{}
	proxy := NewPlatformProxy(portfolio, nil, nil)
	payload, err := anypb.New(&portfoliov1.GetPortfolioSnapshotRequest{
		PortfolioId: 7,
	})
	if err != nil {
		t.Fatal(err)
	}

	resp, err := proxy.DispatchRuntimeRequest(
		context.Background(),
		AuthenticatedRuntime{UserID: 42, RuntimeID: "runtime-1", Name: "desk"},
		"portfolio.v1.PortfolioService/GetPortfolioSnapshot",
		payload,
	)
	if err != nil {
		t.Fatalf("DispatchRuntimeRequest: %v", err)
	}

	if _, ok := resp.(*portfoliov1.GetPortfolioSnapshotResponse); !ok {
		t.Fatalf("response = %T, want GetPortfolioSnapshotResponse", resp)
	}
	if portfolio.portfolioGetReq.GetUserId() != 42 || portfolio.portfolioGetReq.GetPortfolioId() != 7 {
		t.Fatalf("GetPortfolioSnapshot req = %+v", portfolio.portfolioGetReq)
	}
}

func TestPlatformProxyGetPortfolioSnapshotRejectsDifferentUser(t *testing.T) {
	portfolio := &fakePortfolioPlatformClient{}
	proxy := NewPlatformProxy(portfolio, nil, nil)
	payload, err := anypb.New(&portfoliov1.GetPortfolioSnapshotRequest{
		PortfolioId: 7,
		UserId:      99,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = proxy.DispatchRuntimeRequest(
		context.Background(),
		AuthenticatedRuntime{UserID: 42, RuntimeID: "runtime-1", Name: "desk"},
		"GetPortfolioSnapshot",
		payload,
	)
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("code = %v, want PermissionDenied (err=%v)", status.Code(err), err)
	}
	if portfolio.portfolioGetReq != nil {
		t.Fatalf("GetPortfolioSnapshot should not be called: %+v", portfolio.portfolioGetReq)
	}
}

func TestPlatformProxyPreflightStrategySessionInjectsAuthenticatedUser(t *testing.T) {
	portfolio := &fakePortfolioPlatformClient{}
	proxy := NewPlatformProxy(portfolio, nil, nil)
	payload, err := anypb.New(&portfoliov1.PreflightStrategySessionRequest{
		PortfolioId: 7,
		RequiredRoutes: []*portfoliov1.RequiredRoute{
			{Exchange: 1, Market: 2},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	resp, err := proxy.DispatchRuntimeRequest(
		context.Background(),
		AuthenticatedRuntime{UserID: 42, RuntimeID: "runtime-1", Name: "desk"},
		"portfolio.PreflightStrategySession",
		payload,
	)
	if err != nil {
		t.Fatalf("DispatchRuntimeRequest: %v", err)
	}

	if _, ok := resp.(*portfoliov1.PreflightStrategySessionResponse); !ok {
		t.Fatalf("response = %T, want PreflightStrategySessionResponse", resp)
	}
	if portfolio.preflightReq.GetUserId() != 42 ||
		portfolio.preflightReq.GetPortfolioId() != 7 ||
		len(portfolio.preflightReq.GetRequiredRoutes()) != 1 {
		t.Fatalf("PreflightStrategySession req = %+v", portfolio.preflightReq)
	}
}

func TestPlatformProxyOrderPlacePreservesAdvancedOrderFields(t *testing.T) {
	portfolio := &fakePortfolioPlatformClient{}
	order := &fakeOrderPlatformClient{}
	proxy := NewPlatformProxy(portfolio, order, nil)
	goodTillDate := timestamppb.New(time.Unix(1893456000, 0).UTC())
	payload, err := anypb.New(&orderv1.PlaceOrderRequest{
		PortfolioId:  7,
		Symbol:       "BTCUSDT",
		Side:         "BUY",
		Qty:          0.1,
		OrderType:    "LIMIT",
		TimeInForce:  "GTD",
		PostOnly:     false,
		GoodTillDate: goodTillDate,
		ReduceOnly:   true,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = proxy.DispatchRuntimeRequest(
		context.Background(),
		AuthenticatedRuntime{UserID: 42, RuntimeID: "runtime-1", Name: "desk"},
		"order.PlaceOrder",
		payload,
	)
	if err != nil {
		t.Fatalf("DispatchRuntimeRequest: %v", err)
	}

	if order.placeReq == nil {
		t.Fatal("PlaceOrder was not called")
	}
	if order.placeReq.GetPortfolioId() != 7 || order.placeReq.GetSymbol() != "BTCUSDT" {
		t.Fatalf("PlaceOrder route = %+v", order.placeReq)
	}
	if order.placeReq.GetPostOnly() || !order.placeReq.GetReduceOnly() || order.placeReq.GetGoodTillDate().AsTime().Unix() != 1893456000 {
		t.Fatalf("advanced fields = %+v, want post_only=false reduce_only=true good_till_date=1893456000", order.placeReq)
	}
}

func TestPlatformProxyUpdatePortfolioSnapshotIsRejected(t *testing.T) {
	portfolio := &fakePortfolioPlatformClient{}
	proxy := NewPlatformProxy(portfolio, nil, nil)
	payload, err := anypb.New(&portfoliov1.UpdatePortfolioSnapshotRequest{
		PortfolioId:    7,
		SnapshotReason: 2,
		StrategyId:     9,
		SessionId:      "sess-1",
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = proxy.DispatchRuntimeRequest(
		context.Background(),
		AuthenticatedRuntime{UserID: 42, RuntimeID: "runtime-1", Name: "desk"},
		"portfolio.UpdatePortfolioSnapshot",
		payload,
	)
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("code = %v, want Unimplemented (err=%v)", status.Code(err), err)
	}
	if portfolio.portfolioUpdateReq != nil {
		t.Fatalf("UpdatePortfolioSnapshot should not be called: %+v", portfolio.portfolioUpdateReq)
	}
}

func TestPlatformProxyUpdatePortfolioWalletStateChecksSessionAndInjectsUser(t *testing.T) {
	portfolio := &fakePortfolioPlatformClient{
		session: &portfoliov1.StrategySessionEntry{
			SessionId: "sess-1",
			UserId:    42,
			RuntimeId: "runtime-1",
			Status:    "running",
		},
	}
	proxy := NewPlatformProxy(portfolio, nil, nil)
	payload, err := anypb.New(&portfoliov1.UpdatePortfolioWalletStateRequest{
		PortfolioId:    7,
		TotalValue:     1200,
		WalletBalance:  1100,
		SnapshotReason: 1,
		StrategyId:     9,
		SessionId:      "sess-1",
		Futures: &portfoliov1.FuturesWallet{
			MarginMode:       "cross",
			PositionMode:     "one_way",
			WalletBalance:    1100,
			AvailableBalance: 1000,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	resp, err := proxy.DispatchRuntimeRequest(
		context.Background(),
		AuthenticatedRuntime{UserID: 42, RuntimeID: "runtime-1", Name: "desk"},
		"portfolio.UpdatePortfolioWalletState",
		payload,
	)
	if err != nil {
		t.Fatalf("DispatchRuntimeRequest: %v", err)
	}

	if _, ok := resp.(*portfoliov1.UpdatePortfolioWalletStateResponse); !ok {
		t.Fatalf("response = %T, want UpdatePortfolioWalletStateResponse", resp)
	}
	if portfolio.walletStateUpdateReq.GetUserId() != 42 ||
		portfolio.walletStateUpdateReq.GetPortfolioId() != 7 ||
		portfolio.walletStateUpdateReq.GetSessionId() != "sess-1" ||
		portfolio.walletStateUpdateReq.GetFutures().GetWalletBalance() != 1100 {
		t.Fatalf("UpdatePortfolioWalletState req = %+v", portfolio.walletStateUpdateReq)
	}
}

func TestPlatformProxyAllowsStrategyStartWalletUpdateForPendingSession(t *testing.T) {
	portfolio := &fakePortfolioPlatformClient{
		session: &portfoliov1.StrategySessionEntry{
			SessionId: "sess-pending",
			UserId:    42,
			RuntimeId: "runtime-1",
			Status:    "pending",
		},
	}
	proxy := NewPlatformProxy(portfolio, nil, nil)
	payload, err := anypb.New(&portfoliov1.UpdatePortfolioWalletStateRequest{
		PortfolioId:    7,
		SessionId:      "sess-pending",
		SnapshotReason: 2,
		Futures:        &portfoliov1.FuturesWallet{WalletBalance: 1000},
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = proxy.DispatchRuntimeRequest(
		context.Background(),
		AuthenticatedRuntime{UserID: 42, RuntimeID: "runtime-1", Name: "desk"},
		"portfolio.UpdatePortfolioWalletState",
		payload,
	)
	if err != nil {
		t.Fatalf("strategy_start wallet update on pending session: %v", err)
	}
	if portfolio.walletStateUpdateReq.GetUserId() != 42 ||
		portfolio.walletStateUpdateReq.GetSessionId() != "sess-pending" ||
		portfolio.walletStateUpdateReq.GetSnapshotReason() != 2 {
		t.Fatalf("wallet update req = %+v", portfolio.walletStateUpdateReq)
	}
}

func TestPlatformProxyRejectsPeriodicWalletUpdateForPendingSession(t *testing.T) {
	portfolio := &fakePortfolioPlatformClient{
		session: &portfoliov1.StrategySessionEntry{
			SessionId: "sess-pending",
			UserId:    42,
			RuntimeId: "runtime-1",
			Status:    "pending",
		},
	}
	proxy := NewPlatformProxy(portfolio, nil, nil)
	payload, err := anypb.New(&portfoliov1.UpdatePortfolioWalletStateRequest{
		PortfolioId:    7,
		SessionId:      "sess-pending",
		SnapshotReason: 6,
		Futures:        &portfoliov1.FuturesWallet{WalletBalance: 1000},
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = proxy.DispatchRuntimeRequest(
		context.Background(),
		AuthenticatedRuntime{UserID: 42, RuntimeID: "runtime-1", Name: "desk"},
		"portfolio.UpdatePortfolioWalletState",
		payload,
	)
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("code = %v, want FailedPrecondition (err=%v)", status.Code(err), err)
	}
	if portfolio.walletStateUpdateReq != nil {
		t.Fatalf("periodic wallet update should not reach core: %+v", portfolio.walletStateUpdateReq)
	}
}

func TestPlatformProxyUpdatePortfolioSnapshotRejectsDifferentRuntimeSession(t *testing.T) {
	portfolio := &fakePortfolioPlatformClient{
		session: &portfoliov1.StrategySessionEntry{
			SessionId: "sess-1",
			UserId:    42,
			RuntimeId: "runtime-other",
			Status:    "running",
		},
	}
	proxy := NewPlatformProxy(portfolio, nil, nil)
	payload, err := anypb.New(&portfoliov1.UpdatePortfolioSnapshotRequest{
		PortfolioId: 7,
		SessionId:   "sess-1",
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = proxy.DispatchRuntimeRequest(
		context.Background(),
		AuthenticatedRuntime{UserID: 42, RuntimeID: "runtime-1", Name: "desk"},
		"portfolio.v1.PortfolioService/UpdatePortfolioSnapshot",
		payload,
	)
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("code = %v, want Unimplemented (err=%v)", status.Code(err), err)
	}
	if portfolio.portfolioUpdateReq != nil {
		t.Fatalf("UpdatePortfolioSnapshot should not be called: %+v", portfolio.portfolioUpdateReq)
	}
}

func TestPlatformProxyUpdatePortfolioSnapshotRejectsEmptySessionID(t *testing.T) {
	portfolio := &fakePortfolioPlatformClient{}
	proxy := NewPlatformProxy(portfolio, nil, nil)
	payload, err := anypb.New(&portfoliov1.UpdatePortfolioSnapshotRequest{
		PortfolioId: 7,
		UserId:      42,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = proxy.DispatchRuntimeRequest(
		context.Background(),
		AuthenticatedRuntime{UserID: 42, RuntimeID: "runtime-1", Name: "desk"},
		"portfolio.UpdatePortfolioSnapshot",
		payload,
	)
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("code = %v, want Unimplemented (err=%v)", status.Code(err), err)
	}
	if portfolio.portfolioUpdateReq != nil {
		t.Fatalf("UpdatePortfolioSnapshot should not be called: %+v", portfolio.portfolioUpdateReq)
	}
}

func TestPlatformProxyFetchKlinesReturnsStructPayload(t *testing.T) {
	resp, err := klineRowsToStruct([]KlineRow{{
		Exchange:  "binance",
		Market:    "futures",
		Symbol:    "ETHUSDT",
		Interval:  "1m",
		OpenTime:  1000,
		CloseTime: 2000,
		Timestamp: 2000,
		Open:      1,
		High:      2,
		Low:       0.5,
		Close:     1.5,
		Volume:    10,
	}})
	if err != nil {
		t.Fatal(err)
	}
	klines := resp.GetFields()["klines"].GetListValue().GetValues()
	if len(klines) != 1 {
		t.Fatalf("klines len = %d, want 1", len(klines))
	}
	row := klines[0].GetStructValue().GetFields()
	if row["symbol"].GetStringValue() != "ETHUSDT" || row["open_time"].GetNumberValue() != 1000 {
		t.Fatalf("row = %+v", row)
	}

	payload, err := anypb.New(structpb.NewStructValue(resp).GetStructValue())
	if err != nil {
		t.Fatal(err)
	}
	req, err := unpackKlineQueryPayload(payload)
	if err != nil {
		t.Fatal(err)
	}
	if req.Symbol != "" {
		t.Fatalf("unexpected symbol from response-shaped payload: %q", req.Symbol)
	}
}

func TestPlatformProxyFetchBacktestPageUsesFixedPageSize(t *testing.T) {
	query := &fakeKlineQuery{rows: makeKlineRows(9000, 1000, 1000)}
	proxy := NewPlatformProxy(nil, nil, nil)
	proxy.SetMarketDataQuery(query)

	payload, err := anypb.New(mustStruct(t, map[string]any{
		"exchange":            "binance",
		"market":              "futures",
		"kind":                "kline",
		"symbol":              "ETHUSDT",
		"interval":            "1s",
		"start_after_time_ms": float64(0),
		"end_time_ms":         float64(10_000_000),
		"limit":               float64(999999),
	}))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := proxy.DispatchRuntimeRequest(
		context.Background(),
		AuthenticatedRuntime{UserID: 42, RuntimeID: "runtime-1", Name: "desk"},
		"marketdata.FetchBacktestPage",
		payload,
	)
	if err != nil {
		t.Fatalf("DispatchRuntimeRequest: %v", err)
	}
	if len(query.calls) != 1 {
		t.Fatalf("query calls = %d, want 1", len(query.calls))
	}
	if query.calls[0].Limit != 8192 {
		t.Fatalf("query limit = %d, want 8192", query.calls[0].Limit)
	}
	got := resp.(*structpb.Struct)
	if got.Fields["limit"].GetNumberValue() != 8192 {
		t.Fatalf("response limit = %v, want 8192", got.Fields["limit"].GetNumberValue())
	}
	if !got.Fields["has_more"].GetBoolValue() {
		t.Fatal("has_more = false, want true for full page")
	}
	if got.Fields["next_cursor_time_ms"].GetNumberValue() != 8192000 {
		t.Fatalf("next cursor = %v, want 8192000", got.Fields["next_cursor_time_ms"].GetNumberValue())
	}
}

func TestPlatformProxyFetchBacktestPageAppliesCursor(t *testing.T) {
	query := &fakeKlineQuery{rows: makeKlineRows(3, 1000, 1000)}
	proxy := NewPlatformProxy(nil, nil, nil)
	proxy.SetMarketDataQuery(query)

	payload, err := anypb.New(mustStruct(t, map[string]any{
		"exchange":            "binance",
		"market":              "futures",
		"kind":                "kline",
		"symbol":              "ETHUSDT",
		"interval":            "1s",
		"start_after_time_ms": float64(1000),
		"end_time_ms":         float64(5000),
	}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := proxy.DispatchRuntimeRequest(
		context.Background(),
		AuthenticatedRuntime{UserID: 42, RuntimeID: "runtime-1", Name: "desk"},
		"FetchBacktestPage",
		payload,
	); err != nil {
		t.Fatalf("DispatchRuntimeRequest: %v", err)
	}
	if len(query.calls) != 1 {
		t.Fatalf("query calls = %d, want 1", len(query.calls))
	}
	call := query.calls[0]
	if call.StartTimeMS != 2000 {
		t.Fatalf("StartTimeMS = %d, want 2000", call.StartTimeMS)
	}
	if call.EndTimeMS != 5000 {
		t.Fatalf("EndTimeMS = %d, want 5000", call.EndTimeMS)
	}
}

func TestPlatformProxyDeliverDatasetSendsChunksAndEnd(t *testing.T) {
	portfolio := &fakePortfolioPlatformClient{
		session: &portfoliov1.StrategySessionEntry{
			SessionId: "sess-1",
			UserId:    42,
			RuntimeId: "runtime-1",
			Status:    "running",
		},
	}
	proxy := NewPlatformProxy(portfolio, nil, nil)
	query := &fakeKlineQuery{
		rows: []KlineRow{
			{
				Exchange:  "binance",
				Market:    "futures",
				Symbol:    "ETHUSDT",
				Interval:  "1m",
				OpenTime:  1000,
				CloseTime: 2000,
				Timestamp: 2000,
				Open:      1,
				High:      2,
				Low:       0.5,
				Close:     1.5,
				Volume:    10,
			},
			{
				Exchange:  "binance",
				Market:    "futures",
				Symbol:    "ETHUSDT",
				Interval:  "1m",
				OpenTime:  2000,
				CloseTime: 3000,
				Timestamp: 3000,
				Open:      2,
				High:      3,
				Low:       1.5,
				Close:     2.5,
				Volume:    20,
			},
		},
	}
	deliverer := &captureDatasetDeliverer{}
	proxy.SetMarketDataQuery(query)
	proxy.SetDatasetDeliverer(deliverer)

	payload, err := anypb.New(mustStruct(t, map[string]any{
		"session_id":    "sess-1",
		"runtime_id":    "runtime-1",
		"start_time_ms": float64(1000),
		"end_time_ms":   float64(2000),
		"chunk_size":    float64(1),
		"streams": []any{
			map[string]any{
				"exchange": "binance",
				"market":   "futures",
				"kind":     "kline",
				"symbol":   "ETHUSDT",
				"interval": "1m",
			},
		},
	}))
	if err != nil {
		t.Fatal(err)
	}

	resp, err := proxy.DispatchRuntimeRequest(
		context.Background(),
		AuthenticatedRuntime{UserID: 42, RuntimeID: "runtime-1", Name: "desk"},
		"marketdata.DeliverDataset",
		payload,
	)
	if err != nil {
		t.Fatalf("DispatchRuntimeRequest: %v", err)
	}

	if len(deliverer.chunks) != 2 {
		t.Fatalf("chunks = %d, want data chunk + end chunk", len(deliverer.chunks))
	}
	if deliverer.chunks[0].End {
		t.Fatal("first chunk must carry data before terminal end")
	}
	if !strings.Contains(string(deliverer.chunks[0].Payload), `"symbol":"ETHUSDT"`) {
		t.Fatalf("data payload = %s", deliverer.chunks[0].Payload)
	}
	if strings.Contains(string(deliverer.chunks[0].Payload), `"open_time":2000`) {
		t.Fatalf("data payload included end-exclusive boundary row: %s", deliverer.chunks[0].Payload)
	}
	if !deliverer.chunks[1].End {
		t.Fatal("last chunk must mark dataset end")
	}
	st, ok := resp.(*structpb.Struct)
	if !ok {
		t.Fatalf("response = %T, want *structpb.Struct", resp)
	}
	if st.GetFields()["rows"].GetNumberValue() != 1 ||
		st.GetFields()["chunks"].GetNumberValue() != 2 {
		t.Fatalf("response = %+v", st.AsMap())
	}
}

func TestPlatformProxyEmitLogRejectsDifferentRuntimeSession(t *testing.T) {
	portfolio := &fakePortfolioPlatformClient{
		session: &portfoliov1.StrategySessionEntry{
			SessionId: "sess-1",
			UserId:    42,
			RuntimeId: "runtime-other",
		},
	}
	proxy := NewPlatformProxy(portfolio, nil, nil)
	payload, err := anypb.New(mustStruct(t, map[string]any{
		"level":      "INFO",
		"log_type":   "root",
		"logger":     "strategy_service.test",
		"message":    "hello",
		"session_id": "sess-1",
	}))
	if err != nil {
		t.Fatal(err)
	}

	_, err = proxy.DispatchRuntimeRequest(
		context.Background(),
		AuthenticatedRuntime{UserID: 42, RuntimeID: "runtime-1", Name: "desk"},
		"logs.Emit",
		payload,
	)
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("code = %v, want PermissionDenied (err=%v)", status.Code(err), err)
	}
}

func TestPlatformProxyPublishNotificationUsesAuthenticatedRuntime(t *testing.T) {
	portfolio := &fakePortfolioPlatformClient{
		session: &portfoliov1.StrategySessionEntry{
			SessionId:   "sess-1",
			UserId:      42,
			RuntimeId:   "runtime-1",
			PortfolioId: 7,
			StrategyId:  9,
			Status:      "running",
		},
	}
	pub := &captureNotificationPublisher{}
	proxy := NewPlatformProxy(portfolio, nil, nil)
	proxy.SetNotificationPublisher(pub)
	payload, err := anypb.New(mustStruct(t, map[string]any{
		"category":     "custom",
		"severity":     "warn",
		"message":      "threshold reached",
		"session_id":   "sess-1",
		"portfolio_id": float64(7),
		"strategy_id":  float64(9),
	}))
	if err != nil {
		t.Fatal(err)
	}

	resp, err := proxy.DispatchRuntimeRequest(
		context.Background(),
		AuthenticatedRuntime{UserID: 42, RuntimeID: "runtime-1", Name: "desk"},
		"notification.Publish",
		payload,
	)
	if err != nil {
		t.Fatalf("DispatchRuntimeRequest: %v", err)
	}
	if len(pub.events) != 1 {
		t.Fatalf("events = %d, want 1", len(pub.events))
	}
	event := pub.events[0]
	if event.UserID != 42 ||
		event.RuntimeID != "runtime-1" ||
		event.RuntimeName != "desk" ||
		event.SessionID != "sess-1" ||
		event.PortfolioID != 7 ||
		event.StrategyID != 9 ||
		event.Category != cpnotify.CategoryCustom ||
		event.EventType != cpnotify.EventCustomWarn ||
		event.Message != "threshold reached" {
		t.Fatalf("event = %+v", event)
	}
	st, ok := resp.(*structpb.Struct)
	if !ok {
		t.Fatalf("response = %T, want *structpb.Struct", resp)
	}
	if !st.GetFields()["accepted"].GetBoolValue() {
		t.Fatalf("response = %+v, want accepted", st.AsMap())
	}
}

func TestPlatformProxyPublishNotificationRejectsDifferentRuntimeSession(t *testing.T) {
	portfolio := &fakePortfolioPlatformClient{
		session: &portfoliov1.StrategySessionEntry{
			SessionId: "sess-1",
			UserId:    42,
			RuntimeId: "runtime-other",
			Status:    "running",
		},
	}
	pub := &captureNotificationPublisher{}
	proxy := NewPlatformProxy(portfolio, nil, nil)
	proxy.SetNotificationPublisher(pub)
	payload, err := anypb.New(mustStruct(t, map[string]any{
		"category":   "custom",
		"severity":   "info",
		"message":    "hello",
		"session_id": "sess-1",
	}))
	if err != nil {
		t.Fatal(err)
	}

	_, err = proxy.DispatchRuntimeRequest(
		context.Background(),
		AuthenticatedRuntime{UserID: 42, RuntimeID: "runtime-1", Name: "desk"},
		"notification.Publish",
		payload,
	)
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("code = %v, want PermissionDenied (err=%v)", status.Code(err), err)
	}
	if len(pub.events) != 0 {
		t.Fatalf("notification should not be published: %+v", pub.events)
	}
}

func TestPlatformProxyAllowsCleanupCallsForStoppedSession(t *testing.T) {
	portfolio := &fakePortfolioPlatformClient{
		session: &portfoliov1.StrategySessionEntry{
			SessionId: "sess-stopped",
			UserId:    42,
			RuntimeId: "runtime-1",
			Status:    "stopped",
		},
	}
	marketData := &fakeMarketDataPlatformServer{}
	proxy := NewPlatformProxy(portfolio, nil, marketData)

	leasePayload, err := anypb.New(&mdv1.ReleaseMarketDataLeaseRequest{
		SessionId: "sess-stopped",
		StreamId:  99,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := proxy.DispatchRuntimeRequest(
		context.Background(),
		AuthenticatedRuntime{UserID: 42, RuntimeID: "runtime-1", Name: "desk"},
		"marketdata.ReleaseMarketDataLease",
		leasePayload,
	); err != nil {
		t.Fatalf("ReleaseMarketDataLease on stopped session: %v", err)
	}
	if marketData.releaseLeaseReq.GetSessionId() != "sess-stopped" {
		t.Fatalf("release lease req = %+v", marketData.releaseLeaseReq)
	}

	walletPayload, err := anypb.New(&portfoliov1.UpdatePortfolioWalletStateRequest{
		PortfolioId:    7,
		SessionId:      "sess-stopped",
		SnapshotReason: portfolioSnapshotReasonStrategyEnd,
		Futures:        &portfoliov1.FuturesWallet{WalletBalance: 1000},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := proxy.DispatchRuntimeRequest(
		context.Background(),
		AuthenticatedRuntime{UserID: 42, RuntimeID: "runtime-1", Name: "desk"},
		"portfolio.UpdatePortfolioWalletState",
		walletPayload,
	); err != nil {
		t.Fatalf("strategy_end wallet update on stopped session: %v", err)
	}
	if portfolio.walletStateUpdateReq.GetSnapshotReason() != portfolioSnapshotReasonStrategyEnd {
		t.Fatalf("wallet update req = %+v", portfolio.walletStateUpdateReq)
	}
}

func TestPlatformProxyRejectsNonCleanupWalletUpdateForStoppedSession(t *testing.T) {
	portfolio := &fakePortfolioPlatformClient{
		session: &portfoliov1.StrategySessionEntry{
			SessionId: "sess-stopped",
			UserId:    42,
			RuntimeId: "runtime-1",
			Status:    "stopped",
		},
	}
	proxy := NewPlatformProxy(portfolio, nil, nil)
	payload, err := anypb.New(&portfoliov1.UpdatePortfolioWalletStateRequest{
		PortfolioId:    7,
		SessionId:      "sess-stopped",
		SnapshotReason: 1,
		Futures:        &portfoliov1.FuturesWallet{WalletBalance: 1000},
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = proxy.DispatchRuntimeRequest(
		context.Background(),
		AuthenticatedRuntime{UserID: 42, RuntimeID: "runtime-1", Name: "desk"},
		"portfolio.UpdatePortfolioWalletState",
		payload,
	)
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("code = %v, want FailedPrecondition (err=%v)", status.Code(err), err)
	}
	if portfolio.walletStateUpdateReq != nil {
		t.Fatalf("non-cleanup wallet update should not reach core: %+v", portfolio.walletStateUpdateReq)
	}
}

func TestRuntimeLogMessageIncludesAuthenticatedRuntime(t *testing.T) {
	st := mustStruct(t, map[string]any{
		"level":   "INFO",
		"message": "hello",
	})

	msg := runtimeLogMessage(AuthenticatedRuntime{
		UserID:    42,
		RuntimeID: "runtime-1",
		Name:      "desk",
	}, st)

	for _, want := range []string{
		`"source":"self_hosted_runtime"`,
		`"runtime_id":"runtime-1"`,
		`"user_id":42`,
		`"runtime_name":"desk"`,
		`"message":"hello"`,
	} {
		if !strings.Contains(msg, want) {
			t.Fatalf("runtimeLogMessage missing %s: %s", want, msg)
		}
	}
}

func TestRuntimeLogMessageDoesNotAllowPayloadToOverrideAttribution(t *testing.T) {
	st := mustStruct(t, map[string]any{
		"source":       "spoofed",
		"runtime_id":   "runtime-spoofed",
		"user_id":      999,
		"runtime_name": "spoofed-name",
		"service_name": "legacy-spoofed-name",
		"message":      "hello",
	})

	msg := runtimeLogMessage(AuthenticatedRuntime{
		UserID:    42,
		RuntimeID: "runtime-1",
		Name:      "desk",
	}, st)

	var got map[string]any
	if err := json.Unmarshal([]byte(msg), &got); err != nil {
		t.Fatalf("runtimeLogMessage JSON: %v (%s)", err, msg)
	}
	if got["source"] != "self_hosted_runtime" {
		t.Fatalf("source = %v, want self_hosted_runtime", got["source"])
	}
	if got["runtime_id"] != "runtime-1" {
		t.Fatalf("runtime_id = %v, want runtime-1", got["runtime_id"])
	}
	if got["user_id"] != float64(42) {
		t.Fatalf("user_id = %v, want 42", got["user_id"])
	}
	if got["runtime_name"] != "desk" {
		t.Fatalf("runtime_name = %v, want desk", got["runtime_name"])
	}
	if _, ok := got["service_name"]; ok {
		t.Fatalf("service_name should not be accepted as runtime attribution: %s", msg)
	}
	if got["message"] != "hello" {
		t.Fatalf("message = %v, want hello", got["message"])
	}
}

func mustStruct(t *testing.T, fields map[string]any) *structpb.Struct {
	t.Helper()
	st, err := structpb.NewStruct(fields)
	if err != nil {
		t.Fatal(err)
	}
	return st
}

type fakePortfolioPlatformClient struct {
	getPortfolioReq      *portfoliov1.GetPortfolioRequest
	getSessionReq        *portfoliov1.GetSessionRequest
	listSessionsReq      *portfoliov1.ListSessionsRequest
	listSessionsResp     []*portfoliov1.StrategySessionEntry
	saveReq              *portfoliov1.SaveSessionRequest
	updateReq            *portfoliov1.UpdateSessionRequest
	saveIndicatorsReq    *portfoliov1.SaveStrategyIndicatorsRequest
	portfolioGetReq      *portfoliov1.GetPortfolioSnapshotRequest
	portfolioUpdateReq   *portfoliov1.UpdatePortfolioSnapshotRequest
	walletStateUpdateReq *portfoliov1.UpdatePortfolioWalletStateRequest
	preflightReq         *portfoliov1.PreflightStrategySessionRequest
	session              *portfoliov1.StrategySessionEntry
}

func (f *fakePortfolioPlatformClient) GetPortfolio(_ context.Context, req *portfoliov1.GetPortfolioRequest, _ ...grpc.CallOption) (*portfoliov1.GetPortfolioResponse, error) {
	f.getPortfolioReq = req
	return &portfoliov1.GetPortfolioResponse{
		Portfolio: &portfoliov1.PortfolioRegistryEntry{
			PortfolioId: req.GetPortfolioId(),
			UserId:      req.GetUserId(),
		},
	}, nil
}

func (f *fakePortfolioPlatformClient) GetSession(_ context.Context, req *portfoliov1.GetSessionRequest, _ ...grpc.CallOption) (*portfoliov1.GetSessionResponse, error) {
	f.getSessionReq = req
	session := f.session
	if session == nil {
		session = &portfoliov1.StrategySessionEntry{
			SessionId: req.GetSessionId(),
			UserId:    req.GetUserId(),
			RuntimeId: "runtime-1",
			Status:    "running",
		}
	}
	return &portfoliov1.GetSessionResponse{Session: session}, nil
}

func (f *fakePortfolioPlatformClient) ListSessions(_ context.Context, req *portfoliov1.ListSessionsRequest, _ ...grpc.CallOption) (*portfoliov1.ListSessionsResponse, error) {
	f.listSessionsReq = req
	return &portfoliov1.ListSessionsResponse{Sessions: f.listSessionsResp, Total: int64(len(f.listSessionsResp))}, nil
}

func (f *fakePortfolioPlatformClient) GetPortfolioSnapshot(_ context.Context, req *portfoliov1.GetPortfolioSnapshotRequest, _ ...grpc.CallOption) (*portfoliov1.GetPortfolioSnapshotResponse, error) {
	f.portfolioGetReq = req
	return &portfoliov1.GetPortfolioSnapshotResponse{
		Snapshot: &portfoliov1.PortfolioSnapshot{PortfolioId: req.GetPortfolioId(), UserId: req.GetUserId()},
	}, nil
}

func (f *fakePortfolioPlatformClient) UpdatePortfolioSnapshot(_ context.Context, req *portfoliov1.UpdatePortfolioSnapshotRequest, _ ...grpc.CallOption) (*portfoliov1.UpdatePortfolioSnapshotResponse, error) {
	f.portfolioUpdateReq = req
	return &portfoliov1.UpdatePortfolioSnapshotResponse{
		Snapshot: &portfoliov1.PortfolioSnapshot{PortfolioId: req.GetPortfolioId(), UserId: req.GetUserId()},
	}, nil
}

func (f *fakePortfolioPlatformClient) UpdatePortfolioWalletState(_ context.Context, req *portfoliov1.UpdatePortfolioWalletStateRequest, _ ...grpc.CallOption) (*portfoliov1.UpdatePortfolioWalletStateResponse, error) {
	f.walletStateUpdateReq = req
	return &portfoliov1.UpdatePortfolioWalletStateResponse{
		Wallet: &portfoliov1.PortfolioWalletState{
			TotalValue: req.GetTotalValue(),
			Futures:    req.GetFutures(),
			Spot:       req.GetSpot(),
		},
	}, nil
}

func (f *fakePortfolioPlatformClient) PreflightStrategySession(_ context.Context, req *portfoliov1.PreflightStrategySessionRequest, _ ...grpc.CallOption) (*portfoliov1.PreflightStrategySessionResponse, error) {
	f.preflightReq = req
	return &portfoliov1.PreflightStrategySessionResponse{
		Ok: true,
		ResolvedVenues: []*portfoliov1.VenueEntry{
			{
				PortfolioId: req.GetPortfolioId(),
				UserId:      req.GetUserId(),
				Exchange:    1,
				Market:      2,
			},
		},
	}, nil
}

func (f *fakePortfolioPlatformClient) GetActiveStrategy(context.Context, *portfoliov1.GetActiveStrategyRequest, ...grpc.CallOption) (*portfoliov1.GetActiveStrategyResponse, error) {
	return &portfoliov1.GetActiveStrategyResponse{}, nil
}

func (f *fakePortfolioPlatformClient) SaveSession(_ context.Context, req *portfoliov1.SaveSessionRequest, _ ...grpc.CallOption) (*portfoliov1.SaveSessionResponse, error) {
	f.saveReq = req
	return &portfoliov1.SaveSessionResponse{}, nil
}

func (f *fakePortfolioPlatformClient) UpdateSession(_ context.Context, req *portfoliov1.UpdateSessionRequest, _ ...grpc.CallOption) (*portfoliov1.UpdateSessionResponse, error) {
	f.updateReq = req
	return &portfoliov1.UpdateSessionResponse{}, nil
}

func (f *fakePortfolioPlatformClient) SaveStrategyIndicators(_ context.Context, req *portfoliov1.SaveStrategyIndicatorsRequest, _ ...grpc.CallOption) (*portfoliov1.SaveStrategyIndicatorsResponse, error) {
	f.saveIndicatorsReq = req
	return &portfoliov1.SaveStrategyIndicatorsResponse{
		DefinitionsSaved: int32(len(req.GetDefinitions())),
		ChunksSaved:      int32(len(req.GetChunks())),
	}, nil
}

type fakeOrderPlatformClient struct {
	placeReq *orderv1.PlaceOrderRequest
}

func (f *fakeOrderPlatformClient) PlaceOrder(_ context.Context, req *orderv1.PlaceOrderRequest, _ ...grpc.CallOption) (*orderv1.PlaceOrderResponse, error) {
	f.placeReq = req
	return &orderv1.PlaceOrderResponse{}, nil
}

func (fakeOrderPlatformClient) ResolveOrderAttempt(context.Context, *orderv1.ResolveOrderAttemptRequest, ...grpc.CallOption) (*orderv1.ResolveOrderAttemptResponse, error) {
	return &orderv1.ResolveOrderAttemptResponse{}, nil
}

type fakeMarketDataPlatformServer struct {
	releaseLeaseReq                *mdv1.ReleaseMarketDataLeaseRequest
	releaseSessionSubscriptionsReq *mdv1.ReleaseSessionMarketDataSubscriptionsRequest
}

func (fakeMarketDataPlatformServer) GetMarketDataStreamStatus(context.Context, *mdv1.GetMarketDataStreamStatusRequest) (*mdv1.GetMarketDataStreamStatusResponse, error) {
	return &mdv1.GetMarketDataStreamStatusResponse{}, nil
}

func (fakeMarketDataPlatformServer) CreateOrRenewMarketDataLease(context.Context, *mdv1.CreateOrRenewMarketDataLeaseRequest) (*mdv1.CreateOrRenewMarketDataLeaseResponse, error) {
	return &mdv1.CreateOrRenewMarketDataLeaseResponse{}, nil
}

func (f *fakeMarketDataPlatformServer) ReleaseMarketDataLease(_ context.Context, req *mdv1.ReleaseMarketDataLeaseRequest) (*mdv1.ReleaseMarketDataLeaseResponse, error) {
	f.releaseLeaseReq = req
	return &mdv1.ReleaseMarketDataLeaseResponse{}, nil
}

func (fakeMarketDataPlatformServer) CreateSessionMarketDataSubscriptions(context.Context, *mdv1.CreateSessionMarketDataSubscriptionsRequest) (*mdv1.CreateSessionMarketDataSubscriptionsResponse, error) {
	return &mdv1.CreateSessionMarketDataSubscriptionsResponse{}, nil
}

func (f *fakeMarketDataPlatformServer) ReleaseSessionMarketDataSubscriptions(_ context.Context, req *mdv1.ReleaseSessionMarketDataSubscriptionsRequest) (*mdv1.ReleaseSessionMarketDataSubscriptionsResponse, error) {
	f.releaseSessionSubscriptionsReq = req
	return &mdv1.ReleaseSessionMarketDataSubscriptionsResponse{}, nil
}

type fakeKlineQuery struct {
	rows  []KlineRow
	calls []KlineQuery
}

func (f *fakeKlineQuery) FetchKlines(_ context.Context, req KlineQuery) ([]KlineRow, error) {
	f.calls = append(f.calls, req)
	out := make([]KlineRow, 0, len(f.rows))
	for _, row := range f.rows {
		if row.OpenTime >= req.StartTimeMS && row.OpenTime < req.EndTimeMS {
			out = append(out, row)
			if req.Limit > 0 && len(out) >= req.Limit {
				break
			}
		}
	}
	return out, nil
}

func makeKlineRows(n int, startMS int64, stepMS int64) []KlineRow {
	rows := make([]KlineRow, 0, n)
	for i := 0; i < n; i++ {
		open := startMS + int64(i)*stepMS
		rows = append(rows, KlineRow{
			Exchange:  "binance",
			Market:    "futures",
			Symbol:    "ETHUSDT",
			Interval:  "1s",
			OpenTime:  open,
			CloseTime: open + stepMS - 1,
			Timestamp: open,
			Open:      1000,
			High:      1001,
			Low:       999,
			Close:     1000,
			Volume:    1,
		})
	}
	return rows
}

type captureDatasetDeliverer struct {
	chunks []DatasetChunkDelivery
}

func (c *captureDatasetDeliverer) DeliverDatasetChunk(_ context.Context, chunk DatasetChunkDelivery) error {
	c.chunks = append(c.chunks, chunk)
	return nil
}

type captureNotificationPublisher struct {
	events []cpnotify.Event
}

func (c *captureNotificationPublisher) Publish(_ context.Context, event cpnotify.Event) error {
	c.events = append(c.events, event)
	return nil
}
