package runtimechannel

import (
	"context"
	"io"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"

	cpv1 "github.com/hushine-tech/control-panel-service/gen/controlpanelv1"
	"github.com/hushine-tech/control-panel-service/internal/domain"
	orderv1 "github.com/hushine-tech/core-service/gen/orderv1"
	portfoliov1 "github.com/hushine-tech/core-service/gen/portfoliov1"
)

func TestDeliverLiveKlineBatchSendsRuntimeChannelDataFrame(t *testing.T) {
	svc := NewWithInstanceID(&stubRepo{}, "cp-1")
	svc.SetClock(func() time.Time { return time.Date(2026, 5, 16, 12, 0, 0, 0, time.UTC) })
	stream, err := svc.registry.Register(AuthenticatedRuntime{
		KeyID:     "key-1",
		UserID:    42,
		RuntimeID: "rt-1",
		Role:      domain.CredentialRoleExecutor,
	}, svc.now().UTC())
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	var sent []*cpv1.RuntimeFrame
	stream.setSender(func(frame *cpv1.RuntimeFrame) error {
		sent = append(sent, frame)
		return nil
	})
	packed := packKlineStruct(t, "BTCUSDT")

	if err := svc.DeliverLiveKlineBatch(context.Background(), LiveKlineDeliveryBatch{
		UserID:    42,
		RuntimeID: "rt-1",
		SessionID: "sess-1",
		StreamKey: "binance/futures/kline/BTCUSDT/1m",
		Klines:    []*anypb.Any{packed},
	}); err != nil {
		t.Fatalf("DeliverLiveKlineBatch: %v", err)
	}

	if len(sent) != 1 {
		t.Fatalf("sent frames = %d, want 1", len(sent))
	}
	frame := sent[0]
	if frame.GetFrameType() != cpv1.FrameType_FRAME_TYPE_LIVE_KLINE_BATCH {
		t.Fatalf("frame_type = %v, want LIVE_KLINE_BATCH", frame.GetFrameType())
	}
	if frame.GetLiveKlineBatch().GetSessionId() != "sess-1" ||
		frame.GetLiveKlineBatch().GetStreamKey() != "binance/futures/kline/BTCUSDT/1m" ||
		frame.GetLiveKlineBatch().GetSequence() != 1 {
		t.Fatalf("live batch = %+v", frame.GetLiveKlineBatch())
	}
	if len(frame.GetLiveKlineBatch().GetKlines()) != 1 {
		t.Fatalf("klines = %d, want 1", len(frame.GetLiveKlineBatch().GetKlines()))
	}
}

func TestDeliverDatasetChunkSendsRuntimeChannelDataFrame(t *testing.T) {
	svc := NewWithInstanceID(&stubRepo{}, "cp-1")
	stream, err := svc.registry.Register(AuthenticatedRuntime{
		KeyID:     "key-1",
		UserID:    42,
		RuntimeID: "rt-1",
		Role:      domain.CredentialRoleDebugger,
	}, time.Now().UTC())
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	var sent []*cpv1.RuntimeFrame
	stream.setSender(func(frame *cpv1.RuntimeFrame) error {
		sent = append(sent, frame)
		return nil
	})

	if err := svc.DeliverDatasetChunk(context.Background(), DatasetChunkDelivery{
		UserID:    42,
		RuntimeID: "rt-1",
		SessionID: "sess-0",
		DatasetID: "dataset-1",
		Payload:   []byte(`{"klines":[]}`),
		End:       true,
	}); err != nil {
		t.Fatalf("DeliverDatasetChunk: %v", err)
	}
	if len(sent) != 1 {
		t.Fatalf("sent frames = %d, want 1", len(sent))
	}
	frame := sent[0]
	if frame.GetFrameType() != cpv1.FrameType_FRAME_TYPE_DATASET_CHUNK {
		t.Fatalf("frame_type = %v, want DATASET_CHUNK", frame.GetFrameType())
	}
	if frame.GetDatasetChunk().GetDatasetId() != "dataset-1" ||
		frame.GetDatasetChunk().GetSessionId() != "sess-0" ||
		frame.GetDatasetChunk().GetSequence() != 1 ||
		!frame.GetDatasetChunk().GetEnd() {
		t.Fatalf("dataset chunk = %+v", frame.GetDatasetChunk())
	}
}

func TestDeliverOrderLifecycleBatchSendsRuntimeChannelDataFrame(t *testing.T) {
	svc := NewWithInstanceID(&stubRepo{}, "cp-1")
	svc.SetClock(func() time.Time { return time.Date(2026, 6, 21, 12, 0, 0, 0, time.UTC) })
	stream, err := svc.registry.Register(AuthenticatedRuntime{
		KeyID:     "key-1",
		UserID:    42,
		RuntimeID: "rt-1",
		Role:      domain.CredentialRoleExecutor,
	}, svc.now().UTC())
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	var sent []*cpv1.RuntimeFrame
	stream.setSender(func(frame *cpv1.RuntimeFrame) error {
		sent = append(sent, frame)
		return nil
	})
	packed, err := anypb.New(&orderv1.OrderLifecycleEventEntry{
		EventId:     100,
		SessionId:   "sess-1",
		PortfolioId: 7,
		VenueId:     10,
		EventType:   "fill",
	})
	if err != nil {
		t.Fatalf("anypb.New: %v", err)
	}

	err = svc.DeliverOrderLifecycleBatch(context.Background(), OrderLifecycleDeliveryBatch{
		UserID:    42,
		RuntimeID: "rt-1",
		SessionID: "sess-1",
		Sequence:  100,
		Events:    []*anypb.Any{packed},
	})
	if err != nil {
		t.Fatalf("DeliverOrderLifecycleBatch: %v", err)
	}

	if len(sent) != 1 {
		t.Fatalf("sent frames = %d, want 1", len(sent))
	}
	frame := sent[0]
	if frame.GetFrameType() != cpv1.FrameType_FRAME_TYPE_ORDER_UPDATE_BATCH {
		t.Fatalf("frame_type = %v, want ORDER_UPDATE_BATCH", frame.GetFrameType())
	}
	if frame.GetOrderUpdateBatch().GetSessionId() != "sess-1" ||
		frame.GetOrderUpdateBatch().GetStreamKey() != "order_lifecycle" ||
		frame.GetOrderUpdateBatch().GetSequence() != 100 {
		t.Fatalf("order update batch = %+v", frame.GetOrderUpdateBatch())
	}
	if len(frame.GetOrderUpdateBatch().GetEvents()) != 1 {
		t.Fatalf("events = %d, want 1", len(frame.GetOrderUpdateBatch().GetEvents()))
	}
}

func TestDeliverIncomeBatchSendsAckTrackedRuntimeChannelDataFrame(t *testing.T) {
	svc := NewWithInstanceID(&stubRepo{}, "cp-1")
	authenticated := AuthenticatedRuntime{
		KeyID:           "key-1",
		UserID:          42,
		RuntimeID:       "rt-1",
		Role:            domain.CredentialRoleExecutor,
		AuthenticatedAt: time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC),
	}
	stream, err := svc.registry.Register(authenticated, time.Now().UTC())
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	sent := make(chan *cpv1.RuntimeFrame, 1)
	stream.setSender(func(frame *cpv1.RuntimeFrame) error {
		sent <- frame
		return nil
	})
	observer := &incomeDeliveryObserverStub{owner: incomeDeliveryConnection(stream.Runtime), attemptToken: "attempt-1", sequence: 11}
	svc.SetIncomeDeliveryObserver(observer)

	err = svc.DeliverIncomeBatch(context.Background(), IncomeDeliveryBatch{
		UserID:       42,
		RuntimeID:    "rt-1",
		ConnectionID: incomeDeliveryConnection(stream.Runtime).ConnectionID,
		AttemptToken: observer.attemptToken,
		SessionID:    "sess-1",
		StreamKey:    "income/sess-1",
		Sequence:     11,
		Entries: []*portfoliov1.VenueIncomeEntry{
			{IncomeEntryId: 10, SessionId: "sess-1", Status: "confirmed"},
			{IncomeEntryId: 11, SessionId: "sess-1", Status: "confirmed"},
		},
	})
	if err != nil {
		t.Fatalf("DeliverIncomeBatch: %v", err)
	}
	var frame *cpv1.RuntimeFrame
	select {
	case frame = <-sent:
	case <-time.After(time.Second):
		t.Fatal("Income frame was not physically sent")
	}
	if frame.GetFrameType() != cpv1.FrameType_FRAME_TYPE_INCOME_BATCH {
		t.Fatalf("frame_type = %v, want INCOME_BATCH", frame.GetFrameType())
	}
	batch := frame.GetIncomeBatch()
	if batch.GetSessionId() != "sess-1" || batch.GetStreamKey() != "income/sess-1" || batch.GetSequence() != 11 || len(batch.GetEntries()) != 2 {
		t.Fatalf("income batch = %+v", batch)
	}

	svc.handleRuntimeDataAck(stream, &cpv1.RuntimeFrame{Payload: &cpv1.RuntimeFrame_DataAck{DataAck: &cpv1.RuntimeDataAck{
		SessionId: "sess-1", StreamKey: "income/sess-1", Sequence: 10,
	}}})
	if observer.ackCount != 0 {
		t.Fatalf("stale ACK notifications = %d, want 0", observer.ackCount)
	}
	foreignRuntime, err := svc.registry.Register(AuthenticatedRuntime{UserID: 42, RuntimeID: "rt-other"}, time.Now().UTC())
	if err != nil {
		t.Fatalf("register foreign runtime: %v", err)
	}
	svc.handleRuntimeDataAck(foreignRuntime, &cpv1.RuntimeFrame{Payload: &cpv1.RuntimeFrame_DataAck{DataAck: &cpv1.RuntimeDataAck{
		SessionId: "sess-1", StreamKey: "income/sess-1", Sequence: 11,
	}}})
	if observer.ackCount != 0 {
		t.Fatalf("foreign-runtime ACK notifications = %d, want 0", observer.ackCount)
	}
	foreignUser := newRuntimeStream(AuthenticatedRuntime{UserID: 7, RuntimeID: stream.Runtime.RuntimeID}, time.Now().UTC())
	svc.handleRuntimeDataAck(foreignUser, &cpv1.RuntimeFrame{Payload: &cpv1.RuntimeFrame_DataAck{DataAck: &cpv1.RuntimeDataAck{
		SessionId: "sess-1", StreamKey: "income/sess-1", Sequence: 11,
	}}})
	if observer.ackCount != 0 {
		t.Fatalf("foreign-user ACK notifications = %d, want 0", observer.ackCount)
	}
	staleRuntime := newRuntimeStream(AuthenticatedRuntime{UserID: 42, RuntimeID: stream.Runtime.RuntimeID, ConnectionID: "rt-1/stale"}, time.Now().UTC())
	svc.handleRuntimeDataAck(staleRuntime, &cpv1.RuntimeFrame{Payload: &cpv1.RuntimeFrame_DataAck{DataAck: &cpv1.RuntimeDataAck{
		SessionId: "sess-1", StreamKey: "income/sess-1", Sequence: 11,
	}}})
	if observer.ackCount != 0 {
		t.Fatalf("stale-connection ACK notifications = %d, want 0", observer.ackCount)
	}
	svc.handleRuntimeDataAck(stream, &cpv1.RuntimeFrame{Payload: &cpv1.RuntimeFrame_DataAck{DataAck: &cpv1.RuntimeDataAck{
		SessionId: "sess-1", StreamKey: "income/sess-1", Sequence: 11,
	}}})
	if observer.ackCount != 1 || observer.sequence != 11 {
		t.Fatalf("matching ACK observer = count %d sequence %d, want 1/11", observer.ackCount, observer.sequence)
	}
	svc.handleRuntimeDataAck(stream, &cpv1.RuntimeFrame{Payload: &cpv1.RuntimeFrame_DataAck{DataAck: &cpv1.RuntimeDataAck{
		SessionId: "sess-1", StreamKey: "income/sess-1", Sequence: 11,
	}}})
	if observer.ackCount != 1 {
		t.Fatalf("duplicate ACK notifications = %d, want 1", observer.ackCount)
	}
}

func TestDeliverIncomeBatchRejectsStaleRuntimeConnection(t *testing.T) {
	svc := NewWithInstanceID(&stubRepo{}, "cp-1")
	oldRuntime := AuthenticatedRuntime{
		KeyID: "key-1", UserID: 42, RuntimeID: "rt-1",
		AuthenticatedAt: time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC),
	}
	newRuntime := oldRuntime
	oldStream, err := svc.registry.Register(oldRuntime, oldRuntime.AuthenticatedAt)
	if err != nil {
		t.Fatalf("register old connection: %v", err)
	}
	newRuntime.AuthenticatedAt = oldRuntime.AuthenticatedAt
	stream, err := svc.registry.Register(newRuntime, newRuntime.AuthenticatedAt)
	if err != nil {
		t.Fatalf("register replacement connection: %v", err)
	}
	oldConnectionID := incomeDeliveryConnection(oldStream.Runtime).ConnectionID
	newConnectionID := incomeDeliveryConnection(stream.Runtime).ConnectionID
	if oldConnectionID == newConnectionID {
		t.Fatalf("replacement connection ID = old ID %q, want unique current-connection identity", oldConnectionID)
	}
	var sent int
	stream.setSender(func(*cpv1.RuntimeFrame) error {
		sent++
		return nil
	})

	err = svc.DeliverIncomeBatch(context.Background(), IncomeDeliveryBatch{
		UserID:       42,
		RuntimeID:    "rt-1",
		ConnectionID: oldConnectionID,
		AttemptToken: "attempt-old",
		SessionID:    "sess-1",
		StreamKey:    "income/sess-1",
		Sequence:     10,
		Entries: []*portfoliov1.VenueIncomeEntry{{
			IncomeEntryId: 10, SessionId: "sess-1", Status: "confirmed",
		}},
	})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("stale-connection delivery error = %v, want Unavailable", err)
	}
	if sent != 0 {
		t.Fatalf("replacement connection received %d stale frames, want 0", sent)
	}
}

type incomeDeliveryObserverStub struct {
	ackCount          int
	sequence          int64
	owner             IncomeDeliveryConnection
	attemptToken      string
	backpressureCount int
}

func (s *incomeDeliveryObserverStub) IncomeDeliveryAttempt(connection IncomeDeliveryConnection, sessionID, streamKey string, sequence int64) (string, bool) {
	ok := connection == s.owner && sessionID == "sess-1" && streamKey == "income/sess-1" && sequence == s.sequence && s.attemptToken != ""
	return s.attemptToken, ok
}

func (s *incomeDeliveryObserverStub) IncomeBackpressureAttempt(connection IncomeDeliveryConnection, sessionID, streamKey string) (int64, string, bool) {
	ok := connection == s.owner && sessionID == "sess-1" && streamKey == "income/sess-1" && s.sequence > 0 && s.attemptToken != ""
	return s.sequence, s.attemptToken, ok
}

func (s *incomeDeliveryObserverStub) HandleIncomeAck(_ IncomeDeliveryConnection, _ string, _ string, sequence int64, _ string) {
	s.ackCount++
	s.sequence = sequence
}

func (s *incomeDeliveryObserverStub) HandleIncomeBackpressure(IncomeDeliveryConnection, string, string, int64, string, time.Time) {
	s.backpressureCount++
}

func TestDeliverLiveKlineBatchTransfersWhenConnectionOwnerDiffers(t *testing.T) {
	svc := NewWithInstanceID(&stubRepo{}, "cp-delivery")
	transfer := &captureTransfer{}
	svc.SetDataTransfer(transfer)

	if err := svc.DeliverLiveKlineBatch(context.Background(), LiveKlineDeliveryBatch{
		UserID:    42,
		RuntimeID: "rt-remote-owner",
		SessionID: "sess-1",
		StreamKey: "binance/futures/kline/BTCUSDT/1m",
		Klines:    []*anypb.Any{packKlineStruct(t, "BTCUSDT")},
	}); err != nil {
		t.Fatalf("DeliverLiveKlineBatch transfer: %v", err)
	}
	if len(transfer.batches) != 1 || transfer.batches[0].RuntimeID != "rt-remote-owner" {
		t.Fatalf("transferred batches = %+v", transfer.batches)
	}
	if transfer.batches[0].Sequence != 1 {
		t.Fatalf("transferred sequence = %d, want 1", transfer.batches[0].Sequence)
	}
}

func TestRunLiveDeliveryLoopConsumesSource(t *testing.T) {
	svc := NewWithInstanceID(&stubRepo{}, "cp-1")
	stream, err := svc.registry.Register(AuthenticatedRuntime{
		KeyID:     "key-1",
		UserID:    42,
		RuntimeID: "rt-1",
		Role:      domain.CredentialRoleExecutor,
	}, time.Now().UTC())
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	var sent int
	stream.setSender(func(frame *cpv1.RuntimeFrame) error {
		if frame.GetFrameType() == cpv1.FrameType_FRAME_TYPE_LIVE_KLINE_BATCH {
			sent++
		}
		return nil
	})
	source := &sliceLiveSource{batches: []LiveKlineDeliveryBatch{{
		UserID:    42,
		RuntimeID: "rt-1",
		SessionID: "sess-1",
		StreamKey: "binance/futures/kline/BTCUSDT/1m",
		Klines:    []*anypb.Any{packKlineStruct(t, "BTCUSDT")},
	}}}

	if err := svc.RunLiveDeliveryLoop(context.Background(), source); err != nil {
		t.Fatalf("RunLiveDeliveryLoop: %v", err)
	}
	if sent != 1 {
		t.Fatalf("sent = %d, want 1", sent)
	}
}

func packKlineStruct(t *testing.T, symbol string) *anypb.Any {
	t.Helper()
	st, err := structpb.NewStruct(map[string]any{
		"symbol": symbol,
		"market": "futures",
	})
	if err != nil {
		t.Fatalf("NewStruct: %v", err)
	}
	packed, err := anypb.New(st)
	if err != nil {
		t.Fatalf("anypb.New: %v", err)
	}
	return packed
}

type captureTransfer struct {
	batches []LiveKlineDeliveryBatch
}

func (c *captureTransfer) TransferLiveKlineBatch(_ context.Context, batch LiveKlineDeliveryBatch) error {
	c.batches = append(c.batches, batch)
	return nil
}

type sliceLiveSource struct {
	batches []LiveKlineDeliveryBatch
	idx     int
}

func (s *sliceLiveSource) Next(context.Context) (LiveKlineDeliveryBatch, error) {
	if s.idx >= len(s.batches) {
		return LiveKlineDeliveryBatch{}, io.EOF
	}
	batch := s.batches[s.idx]
	s.idx++
	return batch, nil
}
