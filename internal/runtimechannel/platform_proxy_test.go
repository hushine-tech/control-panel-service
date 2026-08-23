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
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
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

func TestIndicatorProtoV1CoexistsWithV2(t *testing.T) {
	service := portfoliov1.File_portfolio_service_proto.Services().
		ByName("PortfolioService")
	if service == nil {
		t.Fatal("PortfolioService descriptor is missing")
	}
	for _, method := range []string{
		"SaveStrategyIndicators",
		"SaveStrategyIndicatorsV2",
		"FinalizeStrategyIndicatorChunksV2",
	} {
		if service.Methods().ByName(protoreflect.Name(method)) == nil {
			t.Fatalf("indicator coexistence method is missing: %s", method)
		}
	}

	portfolio := &fakePortfolioPlatformClient{
		session: &portfoliov1.StrategySessionEntry{
			SessionId: "sess-coexist",
			UserId:    42,
			RuntimeId: "runtime-coexist",
			Status:    "running",
		},
	}
	proxy := NewPlatformProxy(portfolio, nil, nil)
	runtime := AuthenticatedRuntime{
		UserID:    42,
		RuntimeID: "runtime-coexist",
	}
	requests := []struct {
		method  string
		payload proto.Message
	}{
		{
			method: "portfolio.SaveStrategyIndicators",
			payload: &portfoliov1.SaveStrategyIndicatorsRequest{
				SessionId: "sess-coexist",
			},
		},
		{
			method: "portfolio.SaveStrategyIndicatorsV2",
			payload: &portfoliov1.SaveStrategyIndicatorsV2Request{
				SessionId: "sess-coexist",
			},
		},
		{
			method: "portfolio.FinalizeStrategyIndicatorChunksV2",
			payload: &portfoliov1.FinalizeStrategyIndicatorChunksV2Request{
				SessionId: "sess-coexist",
			},
		},
	}
	for _, request := range requests {
		payload, err := anypb.New(request.payload)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := proxy.DispatchRuntimeRequest(
			context.Background(),
			runtime,
			request.method,
			payload,
		); err != nil {
			t.Fatalf("%s: %v", request.method, err)
		}
	}
	if portfolio.saveIndicatorsReq == nil ||
		portfolio.saveIndicatorsV2Req == nil ||
		portfolio.finalizeIndicatorsV2Req == nil {
		t.Fatalf(
			"coexistence dispatch did not preserve distinct clients: v1=%v save_v2=%v finalize_v2=%v",
			portfolio.saveIndicatorsReq != nil,
			portfolio.saveIndicatorsV2Req != nil,
			portfolio.finalizeIndicatorsV2Req != nil,
		)
	}
}

func TestPlatformProxyIndicatorV2WritesInjectIdentityAndPreserveRevision(t *testing.T) {
	portfolio := &fakePortfolioPlatformClient{
		session: &portfoliov1.StrategySessionEntry{
			SessionId: "sess-v2",
			UserId:    42,
			RuntimeId: "runtime-1",
			Status:    "running",
		},
	}
	proxy := NewPlatformProxy(portfolio, nil, nil)
	savePayload, err := anypb.New(
		&portfoliov1.SaveStrategyIndicatorsV2Request{
			SessionId: "sess-v2",
			Chunks: []*portfoliov1.StrategyIndicatorChunkV2{{
				StreamKey:       "binance:spot:BTCUSDT:1m",
				IndicatorKey:    "alpha",
				Count:           1,
				Revision:        1,
				ProtocolVersion: 2,
			}},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := proxy.DispatchRuntimeRequest(
		context.Background(),
		AuthenticatedRuntime{
			UserID:    42,
			RuntimeID: "runtime-1",
			Name:      "desk",
		},
		"portfolio.SaveStrategyIndicatorsV2",
		savePayload,
	); err != nil {
		t.Fatalf("SaveStrategyIndicatorsV2: %v", err)
	}
	if portfolio.saveIndicatorsV2Req.GetUserId() != 42 ||
		portfolio.saveIndicatorsV2Req.GetChunks()[0].GetRevision() != 1 {
		t.Fatalf("save V2 request = %+v", portfolio.saveIndicatorsV2Req)
	}

	finalizePayload, err := anypb.New(
		&portfoliov1.FinalizeStrategyIndicatorChunksV2Request{
			SessionId: "sess-v2",
			Chunks: []*portfoliov1.StrategyIndicatorChunkFinalizationV2{{
				StreamKey:        "binance:spot:BTCUSDT:1m",
				IndicatorKey:     "alpha",
				ExpectedRevision: 1,
			}},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := proxy.DispatchRuntimeRequest(
		context.Background(),
		AuthenticatedRuntime{
			UserID:    42,
			RuntimeID: "runtime-1",
			Name:      "desk",
		},
		"portfolio.FinalizeStrategyIndicatorChunksV2",
		finalizePayload,
	); err != nil {
		t.Fatalf("FinalizeStrategyIndicatorChunksV2: %v", err)
	}
	if portfolio.finalizeIndicatorsV2Req.GetUserId() != 42 ||
		portfolio.finalizeIndicatorsV2Req.GetChunks()[0].
			GetExpectedRevision() != 1 {
		t.Fatalf(
			"finalize V2 request = %+v",
			portfolio.finalizeIndicatorsV2Req,
		)
	}
}

func TestPlatformProxyIndicatorV2WritesRejectSpoofedUserAndForeignRuntime(
	t *testing.T,
) {
	for _, method := range []string{
		"portfolio.SaveStrategyIndicatorsV2",
		"portfolio.FinalizeStrategyIndicatorChunksV2",
	} {
		t.Run(method+"/spoofed-user", func(t *testing.T) {
			portfolio := &fakePortfolioPlatformClient{
				session: &portfoliov1.StrategySessionEntry{
					SessionId: "sess-v2",
					UserId:    42,
					RuntimeId: "runtime-1",
					Status:    "running",
				},
			}
			proxy := NewPlatformProxy(portfolio, nil, nil)
			payload := indicatorV2ProxyPayload(
				t,
				method,
				"sess-v2",
				99,
			)
			_, err := proxy.DispatchRuntimeRequest(
				context.Background(),
				AuthenticatedRuntime{
					UserID:    42,
					RuntimeID: "runtime-1",
				},
				method,
				payload,
			)
			if status.Code(err) != codes.PermissionDenied {
				t.Fatalf(
					"code = %v, want PermissionDenied (err=%v)",
					status.Code(err),
					err,
				)
			}
			if portfolio.saveIndicatorsV2Req != nil ||
				portfolio.finalizeIndicatorsV2Req != nil {
				t.Fatal("spoofed V2 write reached core-service")
			}
		})
		t.Run(method+"/foreign-runtime", func(t *testing.T) {
			portfolio := &fakePortfolioPlatformClient{
				session: &portfoliov1.StrategySessionEntry{
					SessionId: "sess-v2",
					UserId:    42,
					RuntimeId: "runtime-other",
					Status:    "running",
				},
			}
			proxy := NewPlatformProxy(portfolio, nil, nil)
			payload := indicatorV2ProxyPayload(
				t,
				method,
				"sess-v2",
				0,
			)
			_, err := proxy.DispatchRuntimeRequest(
				context.Background(),
				AuthenticatedRuntime{
					UserID:    42,
					RuntimeID: "runtime-1",
				},
				method,
				payload,
			)
			if status.Code(err) != codes.PermissionDenied {
				t.Fatalf(
					"code = %v, want PermissionDenied (err=%v)",
					status.Code(err),
					err,
				)
			}
			if portfolio.saveIndicatorsV2Req != nil ||
				portfolio.finalizeIndicatorsV2Req != nil {
				t.Fatal("foreign-runtime V2 write reached core-service")
			}
		})
	}
}

func indicatorV2ProxyPayload(
	t *testing.T,
	method string,
	sessionID string,
	userID int64,
) *anypb.Any {
	t.Helper()
	var (
		payload *anypb.Any
		err     error
	)
	switch method {
	case "portfolio.SaveStrategyIndicatorsV2":
		payload, err = anypb.New(
			&portfoliov1.SaveStrategyIndicatorsV2Request{
				SessionId: sessionID,
				UserId:    userID,
			},
		)
	case "portfolio.FinalizeStrategyIndicatorChunksV2":
		payload, err = anypb.New(
			&portfoliov1.FinalizeStrategyIndicatorChunksV2Request{
				SessionId: sessionID,
				UserId:    userID,
			},
		)
	default:
		t.Fatalf("unsupported V2 proxy method: %s", method)
	}
	if err != nil {
		t.Fatal(err)
	}
	return payload
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

func TestPlatformProxyCommitStrategySessionStartInjectsAuthenticatedRuntimeAndPreservesStructuredResponse(t *testing.T) {
	expected := &portfoliov1.CommitStrategySessionStartResponse{
		Issues: []*portfoliov1.PreflightIssue{
			{
				Code:      "LEVERAGE_CONFIRMATION_FAILED",
				Message:   "readback mismatch",
				Exchange:  1,
				Market:    2,
				Symbol:    "BTCUSDT",
				VenueId:   41,
				Retryable: true,
				Source:    "exchange",
			},
		},
		ConfirmedTargetFacts: []*portfoliov1.SessionTargetLeverageFact{
			{
				SessionId:         "sess-commit-1",
				VenueId:           41,
				Exchange:          1,
				Environment:       1,
				Market:            2,
				Symbol:            "ETHUSDT",
				EffectiveLeverage: 3,
				LeverageSource:    "order_target",
				PreviousLeverage:  proto.Uint32(2),
				ConfirmedLeverage: 3,
			},
		},
		TargetResults: []*portfoliov1.FuturesLeverageTargetResult{
			{
				VenueId:           41,
				Exchange:          1,
				Market:            2,
				Symbol:            "BTCUSDT",
				EffectiveLeverage: 5,
				LeverageSource:    "strategy_default",
				PreviousLeverage:  proto.Uint32(2),
				CurrentLeverage:   proto.Uint32(2),
				ConfirmedLeverage: proto.Uint32(5),
				ChangeRequired:    true,
				Status:            "rolled_back",
				ErrorCode:         "LEVERAGE_CONFIRMATION_FAILED",
				ErrorMessage:      "readback mismatch",
				Retryable:         true,
			},
		},
		RollbackFailed: true,
		Code:           "LEVERAGE_ROLLBACK_FAILED",
	}
	portfolio := &fakePortfolioPlatformClient{commitResp: expected}
	proxy := NewPlatformProxy(portfolio, nil, nil)
	payload, err := anypb.New(&portfoliov1.CommitStrategySessionStartRequest{
		LaunchOperationId: "launch-commit-1",
		Session: &portfoliov1.SaveSessionRequest{
			SessionId:      "sess-commit-1",
			PortfolioId:    7,
			StrategyId:     9,
			Environment:    1,
			Interval:       "5m",
			StartTimeMs:    1000,
			EndTimeMs:      2000,
			RuntimeId:      "runtime-1",
			RuntimeSource:  "hosted",
			RuntimeName:    "spoofed-name",
			SessionType:    "live",
			RuntimeVersion: "v2",
			SessionName:    "momentum",
			Leverage:       99,
			InitialStatus:  "pending",
		},
		RequiredRoutes: []*portfoliov1.RequiredRoute{{Exchange: 1, Market: 2}},
		RequiredSymbols: []*portfoliov1.RequiredSymbol{
			{
				Exchange:           1,
				Market:             2,
				Symbol:             "BTCUSDT",
				OrderTarget:        true,
				RequiredOrderTypes: []string{"MARKET", "LIMIT"},
				EffectiveLeverage:  5,
				LeverageSource:     "strategy_default",
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	resp, err := proxy.DispatchRuntimeRequest(
		context.Background(),
		AuthenticatedRuntime{UserID: 42, RuntimeID: "runtime-1", Name: "desk", Source: "self_hosted"},
		"portfolio.CommitStrategySessionStart",
		payload,
	)
	if err != nil {
		t.Fatalf("DispatchRuntimeRequest: %v", err)
	}

	if !proto.Equal(resp, expected) {
		t.Fatalf("response = %+v, want %+v", resp, expected)
	}
	request := portfolio.commitReq
	if request.GetLaunchOperationId() != "launch-commit-1" ||
		request.GetSession().GetUserId() != 42 ||
		request.GetSession().GetSessionId() != "sess-commit-1" ||
		request.GetSession().GetPortfolioId() != 7 ||
		request.GetSession().GetStrategyId() != 9 ||
		request.GetSession().GetEnvironment() != 1 ||
		request.GetSession().GetInterval() != "5m" ||
		request.GetSession().GetStartTimeMs() != 1000 ||
		request.GetSession().GetEndTimeMs() != 2000 ||
		request.GetSession().GetRuntimeId() != "runtime-1" ||
		request.GetSession().GetRuntimeSource() != "self_hosted" ||
		request.GetSession().GetRuntimeName() != "desk" ||
		request.GetSession().GetSessionType() != "live" ||
		request.GetSession().GetRuntimeVersion() != "v2" ||
		request.GetSession().GetSessionName() != "momentum" ||
		request.GetSession().GetInitialStatus() != "pending" ||
		request.GetSession().GetLeverage() != 0 ||
		len(request.GetRequiredRoutes()) != 1 ||
		len(request.GetRequiredSymbols()) != 1 ||
		request.GetRequiredSymbols()[0].GetEffectiveLeverage() != 5 ||
		request.GetRequiredSymbols()[0].GetLeverageSource() != "strategy_default" {
		t.Fatalf("CommitStrategySessionStart req = %+v", request)
	}
}

func TestPlatformProxyCommitStrategySessionStartRejectsSpoofedIdentity(t *testing.T) {
	tests := []struct {
		name    string
		userID  int64
		runtime string
	}{
		{name: "user", userID: 99, runtime: "runtime-1"},
		{name: "runtime", runtime: "runtime-other"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			portfolio := &fakePortfolioPlatformClient{}
			proxy := NewPlatformProxy(portfolio, nil, nil)
			payload, err := anypb.New(&portfoliov1.CommitStrategySessionStartRequest{
				LaunchOperationId: "launch-spoof-1",
				Session: &portfoliov1.SaveSessionRequest{
					SessionId:   "sess-spoof-1",
					PortfolioId: 7,
					UserId:      tt.userID,
					RuntimeId:   tt.runtime,
				},
			})
			if err != nil {
				t.Fatal(err)
			}

			_, err = proxy.DispatchRuntimeRequest(
				context.Background(),
				AuthenticatedRuntime{UserID: 42, RuntimeID: "runtime-1", Name: "desk"},
				"portfolio.CommitStrategySessionStart",
				payload,
			)
			if status.Code(err) != codes.PermissionDenied {
				t.Fatalf("code = %v, want PermissionDenied (err=%v)", status.Code(err), err)
			}
			if portfolio.commitReq != nil {
				t.Fatalf("CommitStrategySessionStart should not be called: %+v", portfolio.commitReq)
			}
			if portfolio.getPortfolioReq != nil {
				t.Fatalf("spoofed identity should be rejected before core lookup: %+v", portfolio.getPortfolioReq)
			}
		})
	}
}

func TestPlatformProxyCommitStrategySessionStartPropagatesDeadlineAndCancellation(t *testing.T) {
	t.Run("deadline", func(t *testing.T) {
		portfolio := &fakePortfolioPlatformClient{}
		proxy := NewPlatformProxy(portfolio, nil, nil)
		payload, err := anypb.New(&portfoliov1.CommitStrategySessionStartRequest{
			Session: &portfoliov1.SaveSessionRequest{PortfolioId: 7},
		})
		if err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(time.Minute).Round(0)
		ctx, cancel := context.WithDeadline(context.Background(), deadline)
		defer cancel()

		_, err = proxy.DispatchRuntimeRequest(
			ctx,
			AuthenticatedRuntime{UserID: 42, RuntimeID: "runtime-1", Name: "desk"},
			"portfolio.v1.PortfolioService/CommitStrategySessionStart",
			payload,
		)
		if err != nil {
			t.Fatalf("DispatchRuntimeRequest: %v", err)
		}
		forwardedDeadline, ok := portfolio.commitCtx.Deadline()
		if !ok || !forwardedDeadline.Equal(deadline) {
			t.Fatalf("forwarded deadline = %v, %v; want %v, true", forwardedDeadline, ok, deadline)
		}
	})

	t.Run("cancellation", func(t *testing.T) {
		portfolio := &fakePortfolioPlatformClient{}
		proxy := NewPlatformProxy(portfolio, nil, nil)
		payload, err := anypb.New(&portfoliov1.CommitStrategySessionStartRequest{
			Session: &portfoliov1.SaveSessionRequest{PortfolioId: 7},
		})
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err = proxy.DispatchRuntimeRequest(
			ctx,
			AuthenticatedRuntime{UserID: 42, RuntimeID: "runtime-1", Name: "desk"},
			"portfolio.CommitStrategySessionStart",
			payload,
		)
		if err != context.Canceled {
			t.Fatalf("error = %v, want context.Canceled", err)
		}
		if portfolio.commitCtx != ctx {
			t.Fatal("CommitStrategySessionStart did not receive the dispatch context")
		}
	})
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

// TestFuturesRuntimeChannelOrderAndStopProxyUnchanged is the named release
// guard for Futures requests crossing the RuntimeChannel platform proxy after
// shared Spot routing and stop changes.
func TestFuturesRuntimeChannelOrderAndStopProxyUnchanged(t *testing.T) {
	t.Run("ordinary limit order preserves Futures route and exact fields", func(t *testing.T) {
		portfolio := &fakePortfolioPlatformClient{session: &portfoliov1.StrategySessionEntry{
			SessionId: "futures-running", UserId: 42, RuntimeId: "runtime-1", Status: "running",
			PortfolioId: 7, StrategyId: 9, Environment: 1,
		}}
		order := &fakeOrderPlatformClient{}
		proxy := NewPlatformProxy(portfolio, order, nil)
		price := "2499.50000000"
		payload, err := anypb.New(&orderv1.PlaceOrderRequest{
			PortfolioId: 7, StrategyId: 9, SessionId: "futures-running",
			Exchange: 1, Market: 2, PositionSide: 0, Symbol: "ETHUSDT", Side: "BUY",
			OrderType: "LIMIT", TimeInForce: "GTC", QtyDecimal: "0.25000000",
			PriceDecimal: &price, MarkPriceDecimal: "2500.00000000",
		})
		if err != nil {
			t.Fatal(err)
		}

		if _, err := proxy.DispatchRuntimeRequest(context.Background(), AuthenticatedRuntime{
			UserID: 42, RuntimeID: "runtime-1", Name: "hosted",
		}, "order.v1.OrderService/PlaceOrder", payload); err != nil {
			t.Fatalf("DispatchRuntimeRequest: %v", err)
		}
		got := order.placeReq
		if got == nil || got.GetExchange() != 1 || got.GetMarket() != 2 || got.GetSymbol() != "ETHUSDT" || got.GetOrderType() != "LIMIT" || got.GetTimeInForce() != "GTC" {
			t.Fatalf("forwarded Futures route/order = %#v", got)
		}
		if got.GetQtyDecimal() != "0.25000000" || got.GetPriceDecimal() != price || got.GetMarkPriceDecimal() != "2500.00000000" || got.GetReduceOnly() {
			t.Fatalf("forwarded exact/control fields = %#v", got)
		}
		if portfolio.getPortfolioReq.GetUserId() != 42 || portfolio.getSessionReq.GetUserId() != 42 {
			t.Fatalf("ownership requests portfolio=%#v session=%#v", portfolio.getPortfolioReq, portfolio.getSessionReq)
		}
	})

	t.Run("stopping session may issue only its owned Futures reduce-only close", func(t *testing.T) {
		portfolio := &fakePortfolioPlatformClient{session: &portfoliov1.StrategySessionEntry{
			SessionId: "futures-stopping", UserId: 42, RuntimeId: "runtime-1", Status: "stopping",
			PortfolioId: 7, StrategyId: 9, Environment: 1,
		}}
		order := &fakeOrderPlatformClient{}
		proxy := NewPlatformProxy(portfolio, order, nil)
		payload, err := anypb.New(&orderv1.PlaceOrderRequest{
			PortfolioId: 7, StrategyId: 9, SessionId: "futures-stopping",
			Exchange: 1, Market: 2, PositionSide: 0, Symbol: "BTCUSDT", Side: "SELL",
			OrderType: "MARKET", ReduceOnly: true, QtyDecimal: "0.01000000",
			MarkPriceDecimal: "50000.00000000",
		})
		if err != nil {
			t.Fatal(err)
		}

		if _, err := proxy.DispatchRuntimeRequest(context.Background(), AuthenticatedRuntime{
			UserID: 42, RuntimeID: "runtime-1", Name: "hosted",
		}, "order.PlaceOrder", payload); err != nil {
			t.Fatalf("DispatchRuntimeRequest: %v", err)
		}
		got := order.placeReq
		if got == nil || got.GetMarket() != 2 || got.GetSide() != "SELL" || got.GetOrderType() != "MARKET" || !got.GetReduceOnly() || got.GetSessionId() != "futures-stopping" {
			t.Fatalf("forwarded Futures stop order = %#v", got)
		}

		order.placeReq = nil
		portfolio.session.RuntimeId = "runtime-other"
		_, callErr := proxy.DispatchRuntimeRequest(context.Background(), AuthenticatedRuntime{
			UserID: 42, RuntimeID: "runtime-1", Name: "hosted",
		}, "order.PlaceOrder", payload)
		if status.Code(callErr) != codes.PermissionDenied || order.placeReq != nil {
			t.Fatalf("foreign stop route err=%v forwarded=%#v", callErr, order.placeReq)
		}
	})
}

func TestCloseSpotTargetsProxyRequiresActiveSessionOwnershipAndForwardsCanonicalFacts(t *testing.T) {
	portfolio := &fakePortfolioPlatformClient{
		session: &portfoliov1.StrategySessionEntry{
			SessionId: "session-1", UserId: 42, RuntimeId: "runtime-1", Status: "running",
			PortfolioId: 8, StrategyId: 9, Environment: 1,
		},
		venue: &portfoliov1.VenueEntry{
			VenueId: 10, UserId: 42, PortfolioId: 8, Exchange: 1, Market: 1, Environment: 1, Status: 1,
		},
	}
	order := &fakeOrderPlatformClient{}
	proxy := NewPlatformProxy(portfolio, order, nil)
	payload, err := anypb.New(&orderv1.CloseSpotTargetsRequest{
		PortfolioId: 8, StrategyId: 9, SessionId: "session-1", OperationId: "stop-1",
		Targets: []*orderv1.SpotCloseTarget{{VenueId: 10, Exchange: 1, Market: 1, Symbol: "btcusdt"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := proxy.DispatchRuntimeRequest(context.Background(), AuthenticatedRuntime{
		RuntimeID: "runtime-1", UserID: 42,
	}, "order.CloseSpotTargets", payload); err != nil {
		t.Fatal(err)
	}
	if order.closeReq == nil || order.closeReq.GetUserId() != 42 || order.closeReq.GetOperationId() != "stop-1" ||
		order.closeReq.GetTargets()[0].GetSymbol() != "BTCUSDT" {
		t.Fatalf("forwarded close request=%+v", order.closeReq)
	}
}

func TestCloseSpotTargetsProxyRejectsSessionRuntimeMismatchBeforeCoreOrderCall(t *testing.T) {
	portfolio := &fakePortfolioPlatformClient{session: &portfoliov1.StrategySessionEntry{
		SessionId: "session-1", UserId: 42, RuntimeId: "runtime-other", Status: "running",
		PortfolioId: 8, StrategyId: 9, Environment: 1,
	}}
	order := &fakeOrderPlatformClient{}
	proxy := NewPlatformProxy(portfolio, order, nil)
	payload, err := anypb.New(&orderv1.CloseSpotTargetsRequest{
		PortfolioId: 8, StrategyId: 9, SessionId: "session-1", OperationId: "stop-1",
		Targets: []*orderv1.SpotCloseTarget{{VenueId: 10, Exchange: 1, Market: 1, Symbol: "BTCUSDT"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, callErr := proxy.DispatchRuntimeRequest(context.Background(), AuthenticatedRuntime{
		RuntimeID: "runtime-1", UserID: 42,
	}, "order.CloseSpotTargets", payload)
	if status.Code(callErr) != codes.PermissionDenied || order.closeReq != nil {
		t.Fatalf("err=%v closeReq=%+v", callErr, order.closeReq)
	}
}

func TestCloseSpotTargetsProxyRejectsRouteOutsideSessionPortfolio(t *testing.T) {
	portfolio := &fakePortfolioPlatformClient{
		session: &portfoliov1.StrategySessionEntry{
			SessionId: "session-1", UserId: 42, RuntimeId: "runtime-1", Status: "running",
			PortfolioId: 8, StrategyId: 9, Environment: 1,
		},
		venue: &portfoliov1.VenueEntry{VenueId: 10, UserId: 42, PortfolioId: 99, Exchange: 1, Market: 1, Environment: 1, Status: 1},
	}
	order := &fakeOrderPlatformClient{}
	proxy := NewPlatformProxy(portfolio, order, nil)
	payload, _ := anypb.New(&orderv1.CloseSpotTargetsRequest{
		PortfolioId: 8, StrategyId: 9, SessionId: "session-1", OperationId: "stop-1",
		Targets: []*orderv1.SpotCloseTarget{{VenueId: 10, Exchange: 1, Market: 1, Symbol: "BTCUSDT"}},
	})
	_, callErr := proxy.DispatchRuntimeRequest(context.Background(), AuthenticatedRuntime{
		RuntimeID: "runtime-1", UserID: 42,
	}, "order.CloseSpotTargets", payload)
	if status.Code(callErr) != codes.PermissionDenied || order.closeReq != nil {
		t.Fatalf("err=%v closeReq=%+v", callErr, order.closeReq)
	}
}

func TestListOrderLifecycleEventsProxyRequiresSessionOwnershipAndForwardsCursor(t *testing.T) {
	portfolio := &fakePortfolioPlatformClient{session: &portfoliov1.StrategySessionEntry{
		SessionId: "session-1", UserId: 42, RuntimeId: "runtime-1", Status: "stopping",
		PortfolioId: 8, StrategyId: 9, Environment: 1,
	}}
	order := &fakeOrderPlatformClient{}
	proxy := NewPlatformProxy(portfolio, order, nil)
	payload, err := anypb.New(&orderv1.ListOrderLifecycleEventsRequest{
		SessionId: "session-1", AfterEventId: 10, Limit: 50,
	})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := proxy.DispatchRuntimeRequest(context.Background(), AuthenticatedRuntime{
		RuntimeID: "runtime-1", UserID: 42,
	}, "order.ListOrderLifecycleEvents", payload); err != nil {
		t.Fatal(err)
	}
	if order.lifecycleReq == nil || order.lifecycleReq.GetSessionId() != "session-1" ||
		order.lifecycleReq.GetAfterEventId() != 10 || order.lifecycleReq.GetLimit() != 50 {
		t.Fatalf("forwarded lifecycle request=%+v", order.lifecycleReq)
	}
}

func TestListOrderLifecycleEventsProxyAllowsOwnedPendingSessionDuringActivation(t *testing.T) {
	portfolio := &fakePortfolioPlatformClient{session: &portfoliov1.StrategySessionEntry{
		SessionId: "session-pending", UserId: 42, RuntimeId: "runtime-1", Status: "pending",
		PortfolioId: 8, StrategyId: 9, Environment: 0,
	}}
	order := &fakeOrderPlatformClient{}
	proxy := NewPlatformProxy(portfolio, order, nil)
	payload, err := anypb.New(&orderv1.ListOrderLifecycleEventsRequest{
		SessionId: "session-pending", AfterEventId: 0, Limit: 500,
	})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := proxy.DispatchRuntimeRequest(context.Background(), AuthenticatedRuntime{
		RuntimeID: "runtime-1", UserID: 42,
	}, "order.ListOrderLifecycleEvents", payload); err != nil {
		t.Fatal(err)
	}
	if order.lifecycleReq == nil || order.lifecycleReq.GetSessionId() != "session-pending" {
		t.Fatalf("forwarded lifecycle request=%+v", order.lifecycleReq)
	}
}

func TestListOrderLifecycleEventsProxyRejectsRuntimeMismatchBeforeCoreOrderCall(t *testing.T) {
	portfolio := &fakePortfolioPlatformClient{session: &portfoliov1.StrategySessionEntry{
		SessionId: "session-1", UserId: 42, RuntimeId: "runtime-other", Status: "running",
		PortfolioId: 8, StrategyId: 9, Environment: 1,
	}}
	order := &fakeOrderPlatformClient{}
	proxy := NewPlatformProxy(portfolio, order, nil)
	payload, err := anypb.New(&orderv1.ListOrderLifecycleEventsRequest{SessionId: "session-1"})
	if err != nil {
		t.Fatal(err)
	}

	_, callErr := proxy.DispatchRuntimeRequest(context.Background(), AuthenticatedRuntime{
		RuntimeID: "runtime-1", UserID: 42,
	}, "order.ListOrderLifecycleEvents", payload)
	if status.Code(callErr) != codes.PermissionDenied || order.lifecycleReq != nil {
		t.Fatalf("err=%v lifecycleReq=%+v", callErr, order.lifecycleReq)
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
	getPortfolioReq         *portfoliov1.GetPortfolioRequest
	getSessionReq           *portfoliov1.GetSessionRequest
	listSessionsReq         *portfoliov1.ListSessionsRequest
	listSessionsResp        []*portfoliov1.StrategySessionEntry
	saveReq                 *portfoliov1.SaveSessionRequest
	updateReq               *portfoliov1.UpdateSessionRequest
	saveIndicatorsReq       *portfoliov1.SaveStrategyIndicatorsRequest
	saveIndicatorsV2Req     *portfoliov1.SaveStrategyIndicatorsV2Request
	finalizeIndicatorsV2Req *portfoliov1.FinalizeStrategyIndicatorChunksV2Request
	portfolioGetReq         *portfoliov1.GetPortfolioSnapshotRequest
	portfolioUpdateReq      *portfoliov1.UpdatePortfolioSnapshotRequest
	walletStateUpdateReq    *portfoliov1.UpdatePortfolioWalletStateRequest
	preflightReq            *portfoliov1.PreflightStrategySessionRequest
	commitReq               *portfoliov1.CommitStrategySessionStartRequest
	commitResp              *portfoliov1.CommitStrategySessionStartResponse
	commitCtx               context.Context
	session                 *portfoliov1.StrategySessionEntry
	venue                   *portfoliov1.VenueEntry
}

func (f *fakePortfolioPlatformClient) GetVenue(_ context.Context, req *portfoliov1.GetVenueRequest, _ ...grpc.CallOption) (*portfoliov1.GetVenueResponse, error) {
	venue := f.venue
	if venue == nil {
		venue = &portfoliov1.VenueEntry{VenueId: req.GetVenueId(), UserId: req.GetUserId(), PortfolioId: 8, Exchange: 1, Market: 1, Environment: 1, Status: 1}
	}
	return &portfoliov1.GetVenueResponse{Venue: venue}, nil
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

func (f *fakePortfolioPlatformClient) CommitStrategySessionStart(ctx context.Context, req *portfoliov1.CommitStrategySessionStartRequest, _ ...grpc.CallOption) (*portfoliov1.CommitStrategySessionStartResponse, error) {
	f.commitCtx = ctx
	f.commitReq = req
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if f.commitResp != nil {
		return f.commitResp, nil
	}
	return &portfoliov1.CommitStrategySessionStartResponse{Ok: true}, nil
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

func (f *fakePortfolioPlatformClient) SaveStrategyIndicatorsV2(_ context.Context, req *portfoliov1.SaveStrategyIndicatorsV2Request, _ ...grpc.CallOption) (*portfoliov1.SaveStrategyIndicatorsV2Response, error) {
	f.saveIndicatorsV2Req = req
	return &portfoliov1.SaveStrategyIndicatorsV2Response{
		DefinitionsSaved: int32(len(req.GetDefinitions())),
		ChunksSaved:      int32(len(req.GetChunks())),
	}, nil
}

func (f *fakePortfolioPlatformClient) FinalizeStrategyIndicatorChunksV2(_ context.Context, req *portfoliov1.FinalizeStrategyIndicatorChunksV2Request, _ ...grpc.CallOption) (*portfoliov1.FinalizeStrategyIndicatorChunksV2Response, error) {
	f.finalizeIndicatorsV2Req = req
	return &portfoliov1.FinalizeStrategyIndicatorChunksV2Response{
		ChunksFinalized: int32(len(req.GetChunks())),
	}, nil
}

type fakeOrderPlatformClient struct {
	placeReq     *orderv1.PlaceOrderRequest
	closeReq     *orderv1.CloseSpotTargetsRequest
	lifecycleReq *orderv1.ListOrderLifecycleEventsRequest
}

func (f *fakeOrderPlatformClient) CloseSpotTargets(_ context.Context, req *orderv1.CloseSpotTargetsRequest, _ ...grpc.CallOption) (*orderv1.CloseSpotTargetsResponse, error) {
	f.closeReq = req
	return &orderv1.CloseSpotTargetsResponse{Status: "stopped", OperationId: req.GetOperationId()}, nil
}

func (f *fakeOrderPlatformClient) ListOrderLifecycleEvents(_ context.Context, req *orderv1.ListOrderLifecycleEventsRequest, _ ...grpc.CallOption) (*orderv1.ListOrderLifecycleEventsResponse, error) {
	f.lifecycleReq = req
	return &orderv1.ListOrderLifecycleEventsResponse{}, nil
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
