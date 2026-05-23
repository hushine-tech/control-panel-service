package runtimechannel

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"

	cpv1 "github.com/hushine-tech/control-panel-service/gen/controlpanelv1"
)

type RuntimeDataTransfer interface {
	TransferLiveKlineBatch(ctx context.Context, batch LiveKlineDeliveryBatch) error
}

type LiveKlineDeliveryBatch struct {
	UserID    int64
	RuntimeID string
	SessionID string
	StreamKey string
	Sequence  int64
	Klines    []*anypb.Any
}

type LiveBatchSource interface {
	Next(ctx context.Context) (LiveKlineDeliveryBatch, error)
}

type DatasetChunkDelivery struct {
	UserID    int64
	RuntimeID string
	SessionID string
	DatasetID string
	Sequence  int64
	Payload   []byte
	End       bool
}

func (s *Service) RunLiveDeliveryLoop(ctx context.Context, source LiveBatchSource) error {
	if source == nil {
		return status.Error(codes.FailedPrecondition, "live delivery source is not configured")
	}
	for {
		batch, err := source.Next(ctx)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := s.DeliverLiveKlineBatch(ctx, batch); err != nil {
			return err
		}
	}
}

func (s *Service) DeliverLiveKlineBatch(ctx context.Context, batch LiveKlineDeliveryBatch) error {
	if s == nil {
		return status.Error(codes.FailedPrecondition, "runtime channel service is not configured")
	}
	if batch.UserID <= 0 || batch.RuntimeID == "" || batch.SessionID == "" || batch.StreamKey == "" {
		return status.Error(codes.InvalidArgument, "user_id, runtime_id, session_id, and stream_key are required")
	}
	seq := batch.Sequence
	if seq <= 0 {
		chunk, err := s.dataWindow.Enqueue(batch.SessionID, batch.StreamKey, nil, s.now().UTC())
		if err != nil {
			return err
		}
		seq = chunk.Sequence
	}
	frame := &cpv1.RuntimeFrame{
		FrameType: cpv1.FrameType_FRAME_TYPE_LIVE_KLINE_BATCH,
		Payload: &cpv1.RuntimeFrame_LiveKlineBatch{
			LiveKlineBatch: &cpv1.RuntimeLiveKlineBatch{
				SessionId: batch.SessionID,
				StreamKey: batch.StreamKey,
				Sequence:  seq,
				Klines:    batch.Klines,
			},
		},
	}
	stream := s.registry.FindByRuntimeID(batch.UserID, batch.RuntimeID)
	if stream == nil {
		if s.dataTransfer != nil {
			batch.Sequence = seq
			return s.dataTransfer.TransferLiveKlineBatch(ctx, batch)
		}
		return status.Error(codes.Unavailable, s.missingRuntimeStreamReason(batch.UserID, batch.RuntimeID))
	}
	return stream.sendFrame(frame)
}

func (s *Service) missingRuntimeStreamReason(userID int64, runtimeID string) string {
	snapshot := s.registry.Snapshot()
	if len(snapshot) == 0 {
		return fmt.Sprintf("runtime connection is not owned by this control-panel instance: runtime_id=%s user_id=%d active_streams=0", runtimeID, userID)
	}
	parts := make([]string, 0, len(snapshot))
	for _, rt := range snapshot {
		parts = append(parts, fmt.Sprintf("%s/user=%d/source=%s", rt.RuntimeID, rt.UserID, rt.Source))
	}
	return fmt.Sprintf(
		"runtime connection is not owned by this control-panel instance: runtime_id=%s user_id=%d active_streams=%s",
		runtimeID,
		userID,
		strings.Join(parts, ","),
	)
}

func (s *Service) DeliverDatasetChunk(ctx context.Context, chunk DatasetChunkDelivery) error {
	if s == nil {
		return status.Error(codes.FailedPrecondition, "runtime channel service is not configured")
	}
	if chunk.UserID <= 0 || chunk.RuntimeID == "" || chunk.SessionID == "" || chunk.DatasetID == "" {
		return status.Error(codes.InvalidArgument, "user_id, runtime_id, session_id, and dataset_id are required")
	}
	seq := chunk.Sequence
	if seq <= 0 {
		windowChunk, err := s.dataWindow.Enqueue(chunk.SessionID, chunk.DatasetID, chunk.Payload, s.now().UTC())
		if err != nil {
			return err
		}
		seq = windowChunk.Sequence
	}
	frame := &cpv1.RuntimeFrame{
		FrameType: cpv1.FrameType_FRAME_TYPE_DATASET_CHUNK,
		Payload: &cpv1.RuntimeFrame_DatasetChunk{
			DatasetChunk: &cpv1.RuntimeDatasetChunk{
				DatasetId: chunk.DatasetID,
				SessionId: chunk.SessionID,
				Sequence:  seq,
				Payload:   append([]byte(nil), chunk.Payload...),
				End:       chunk.End,
			},
		},
	}
	stream := s.registry.FindByRuntimeID(chunk.UserID, chunk.RuntimeID)
	if stream == nil {
		return status.Error(codes.Unavailable, "runtime connection is not owned by this control-panel instance")
	}
	return stream.sendFrame(frame)
}

func (s *Service) handleRuntimeDataAck(frame *cpv1.RuntimeFrame) {
	ack := frame.GetDataAck()
	if ack == nil || s == nil || s.dataWindow == nil {
		return
	}
	s.dataWindow.Ack(ack.GetSessionId(), ack.GetStreamKey(), ack.GetSequence())
}
