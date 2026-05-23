package runtimechannel

import (
	"context"
	"io"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"

	cpv1 "github.com/hushine-tech/control-panel-service/gen/controlpanelv1"
	"github.com/hushine-tech/control-panel-service/internal/domain"
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
