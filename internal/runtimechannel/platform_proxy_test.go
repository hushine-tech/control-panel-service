package runtimechannel

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"

	cpnotify "github.com/hushine-tech/control-panel-service/internal/notification"
	accountv1 "github.com/hushine-tech/core-service/gen/accountv1"
	orderv1 "github.com/hushine-tech/core-service/gen/orderv1"
)

func TestPlatformProxySaveSessionBindsAuthenticatedRuntime(t *testing.T) {
	account := &fakeAccountPlatformClient{}
	proxy := NewPlatformProxy(account, nil, nil)
	payload, err := anypb.New(&accountv1.SaveSessionRequest{
		SessionId:  "sess-1",
		AccountId:  7,
		StrategyId: 9,
		Mode:       2,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = proxy.DispatchRuntimeRequest(
		context.Background(),
		AuthenticatedRuntime{UserID: 42, RuntimeID: "runtime-1", Name: "desk", Source: "self_hosted"},
		"account.SaveSession",
		payload,
	)
	if err != nil {
		t.Fatalf("DispatchRuntimeRequest: %v", err)
	}

	if account.getAccountReq.GetUserId() != 42 || account.getAccountReq.GetAccountId() != 7 {
		t.Fatalf("GetAccount req = %+v", account.getAccountReq)
	}
	if account.saveReq.GetRuntimeId() != "runtime-1" ||
		account.saveReq.GetRuntimeSource() != "self_hosted" ||
		account.saveReq.GetRuntimeName() != "desk" {
		t.Fatalf("SaveSession runtime binding = %+v", account.saveReq)
	}
}

func TestPlatformProxySaveSessionPreservesHostedRuntimeSource(t *testing.T) {
	account := &fakeAccountPlatformClient{}
	proxy := NewPlatformProxy(account, nil, nil)
	payload, err := anypb.New(&accountv1.SaveSessionRequest{
		SessionId:  "sess-1",
		AccountId:  7,
		StrategyId: 9,
		Mode:       0,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = proxy.DispatchRuntimeRequest(
		context.Background(),
		AuthenticatedRuntime{UserID: 42, RuntimeID: "rt-hosted", Name: "hosted-test", Source: "hosted"},
		"account.SaveSession",
		payload,
	)
	if err != nil {
		t.Fatalf("DispatchRuntimeRequest: %v", err)
	}

	if account.saveReq.GetRuntimeId() != "rt-hosted" ||
		account.saveReq.GetRuntimeSource() != "hosted" ||
		account.saveReq.GetRuntimeName() != "hosted-test" {
		t.Fatalf("SaveSession runtime binding = %+v", account.saveReq)
	}
}

func TestPlatformProxyRejectsSessionMutationFromDifferentRuntime(t *testing.T) {
	account := &fakeAccountPlatformClient{
		session: &accountv1.StrategySessionEntry{
			SessionId: "sess-1",
			UserId:    42,
			RuntimeId: "runtime-other",
		},
	}
	proxy := NewPlatformProxy(account, nil, nil)
	payload, err := anypb.New(&accountv1.UpdateSessionRequest{
		SessionId: "sess-1",
		Status:    "stopped",
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = proxy.DispatchRuntimeRequest(
		context.Background(),
		AuthenticatedRuntime{UserID: 42, RuntimeID: "runtime-1", Name: "desk"},
		"account.UpdateSession",
		payload,
	)
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("code = %v, want PermissionDenied (err=%v)", status.Code(err), err)
	}
	if account.updateReq != nil {
		t.Fatalf("UpdateSession should not be called: %+v", account.updateReq)
	}
}

func TestPlatformProxyRejectsWalletUpdateForTerminalSession(t *testing.T) {
	account := &fakeAccountPlatformClient{
		session: &accountv1.StrategySessionEntry{
			SessionId: "sess-terminal",
			UserId:    42,
			RuntimeId: "runtime-1",
			Status:    "recoverable",
		},
	}
	proxy := NewPlatformProxy(account, nil, nil)
	payload, err := anypb.New(&accountv1.UpdateAccountWalletStateRequest{
		AccountId:      7,
		StrategyId:     9,
		SessionId:      "sess-terminal",
		SnapshotReason: 6,
		Futures: &accountv1.FuturesWallet{
			WalletBalance: 1000,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = proxy.DispatchRuntimeRequest(
		context.Background(),
		AuthenticatedRuntime{UserID: 42, RuntimeID: "runtime-1", Name: "desk"},
		"account.UpdateAccountWalletState",
		payload,
	)
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("code = %v, want FailedPrecondition (err=%v)", status.Code(err), err)
	}
	if account.walletReq != nil {
		t.Fatalf("UpdateAccountWalletState should not be called: %+v", account.walletReq)
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

func TestPlatformProxyDeliverDatasetSendsChunksAndEnd(t *testing.T) {
	account := &fakeAccountPlatformClient{
		session: &accountv1.StrategySessionEntry{
			SessionId: "sess-1",
			UserId:    42,
			RuntimeId: "runtime-1",
			Status:    "running",
		},
	}
	proxy := NewPlatformProxy(account, nil, nil)
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
	account := &fakeAccountPlatformClient{
		session: &accountv1.StrategySessionEntry{
			SessionId: "sess-1",
			UserId:    42,
			RuntimeId: "runtime-other",
		},
	}
	proxy := NewPlatformProxy(account, nil, nil)
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
	account := &fakeAccountPlatformClient{
		session: &accountv1.StrategySessionEntry{
			SessionId:  "sess-1",
			UserId:     42,
			RuntimeId:  "runtime-1",
			AccountId:  7,
			StrategyId: 9,
			Status:     "running",
		},
	}
	pub := &captureNotificationPublisher{}
	proxy := NewPlatformProxy(account, nil, nil)
	proxy.SetNotificationPublisher(pub)
	payload, err := anypb.New(mustStruct(t, map[string]any{
		"category":    "custom",
		"severity":    "warn",
		"message":     "threshold reached",
		"session_id":  "sess-1",
		"account_id":  float64(7),
		"strategy_id": float64(9),
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
		event.AccountID != 7 ||
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
	account := &fakeAccountPlatformClient{
		session: &accountv1.StrategySessionEntry{
			SessionId: "sess-1",
			UserId:    42,
			RuntimeId: "runtime-other",
			Status:    "running",
		},
	}
	pub := &captureNotificationPublisher{}
	proxy := NewPlatformProxy(account, nil, nil)
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

type fakeAccountPlatformClient struct {
	getAccountReq *accountv1.GetAccountRequest
	saveReq       *accountv1.SaveSessionRequest
	updateReq     *accountv1.UpdateSessionRequest
	walletReq     *accountv1.UpdateAccountWalletStateRequest
	session       *accountv1.StrategySessionEntry
}

func (f *fakeAccountPlatformClient) GetAccount(_ context.Context, req *accountv1.GetAccountRequest, _ ...grpc.CallOption) (*accountv1.GetAccountResponse, error) {
	f.getAccountReq = req
	return &accountv1.GetAccountResponse{
		Account: &accountv1.AccountRegistryEntry{
			AccountId: req.GetAccountId(),
			UserId:    req.GetUserId(),
		},
	}, nil
}

func (f *fakeAccountPlatformClient) GetSession(_ context.Context, req *accountv1.GetSessionRequest, _ ...grpc.CallOption) (*accountv1.GetSessionResponse, error) {
	session := f.session
	if session == nil {
		session = &accountv1.StrategySessionEntry{
			SessionId: req.GetSessionId(),
			UserId:    req.GetUserId(),
			RuntimeId: "runtime-1",
			Status:    "running",
		}
	}
	return &accountv1.GetSessionResponse{Session: session}, nil
}

func (f *fakeAccountPlatformClient) GetOnlineAccountInfo(context.Context, *accountv1.GetOnlineAccountInfoRequest, ...grpc.CallOption) (*accountv1.GetOnlineAccountInfoResponse, error) {
	return &accountv1.GetOnlineAccountInfoResponse{}, nil
}

func (f *fakeAccountPlatformClient) GetActiveStrategy(context.Context, *accountv1.GetActiveStrategyRequest, ...grpc.CallOption) (*accountv1.GetActiveStrategyResponse, error) {
	return &accountv1.GetActiveStrategyResponse{}, nil
}

func (f *fakeAccountPlatformClient) SaveSession(_ context.Context, req *accountv1.SaveSessionRequest, _ ...grpc.CallOption) (*accountv1.SaveSessionResponse, error) {
	f.saveReq = req
	return &accountv1.SaveSessionResponse{}, nil
}

func (f *fakeAccountPlatformClient) UpdateSession(_ context.Context, req *accountv1.UpdateSessionRequest, _ ...grpc.CallOption) (*accountv1.UpdateSessionResponse, error) {
	f.updateReq = req
	return &accountv1.UpdateSessionResponse{}, nil
}

func (f *fakeAccountPlatformClient) UpdateAccountWalletState(_ context.Context, req *accountv1.UpdateAccountWalletStateRequest, _ ...grpc.CallOption) (*accountv1.UpdateAccountWalletStateResponse, error) {
	f.walletReq = req
	return &accountv1.UpdateAccountWalletStateResponse{}, nil
}

type fakeOrderPlatformClient struct{}

func (fakeOrderPlatformClient) PlaceOrder(context.Context, *orderv1.PlaceOrderRequest, ...grpc.CallOption) (*orderv1.PlaceOrderResponse, error) {
	return &orderv1.PlaceOrderResponse{}, nil
}

func (fakeOrderPlatformClient) ResolveOrderAttempt(context.Context, *orderv1.ResolveOrderAttemptRequest, ...grpc.CallOption) (*orderv1.ResolveOrderAttemptResponse, error) {
	return &orderv1.ResolveOrderAttemptResponse{}, nil
}

type fakeKlineQuery struct {
	rows []KlineRow
}

func (f *fakeKlineQuery) FetchKlines(_ context.Context, req KlineQuery) ([]KlineRow, error) {
	out := make([]KlineRow, 0, len(f.rows))
	for _, row := range f.rows {
		if row.OpenTime >= req.StartTimeMS && row.OpenTime < req.EndTimeMS {
			out = append(out, row)
		}
	}
	return out, nil
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
