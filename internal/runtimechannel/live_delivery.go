package runtimechannel

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"

	cpv1 "github.com/hushine-tech/control-panel-service/gen/controlpanelv1"
	portfoliov1 "github.com/hushine-tech/core-service/gen/portfoliov1"
)

type RuntimeDataTransfer interface {
	TransferLiveKlineBatch(ctx context.Context, batch LiveKlineDeliveryBatch) error
}

const orderLifecycleStreamKey = "order_lifecycle"

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

type OrderLifecycleDeliveryBatch struct {
	UserID    int64
	RuntimeID string
	SessionID string
	Sequence  int64
	Events    []*anypb.Any
}

type OrderLifecycleDeliverer interface {
	DeliverOrderLifecycleBatch(ctx context.Context, batch OrderLifecycleDeliveryBatch) error
}

type IncomeDeliveryBatch struct {
	UserID       int64
	RuntimeID    string
	ConnectionID string
	SessionID    string
	StreamKey    string
	Sequence     int64
	Entries      []*portfoliov1.VenueIncomeEntry
}

type IncomeBatchDeliverer interface {
	DeliverIncomeBatch(ctx context.Context, batch IncomeDeliveryBatch) error
}

type IncomeDeliveryConnection struct {
	UserID       int64
	RuntimeID    string
	ConnectionID string
}

type IncomeDeliveryObserver interface {
	OwnsIncomeDelivery(connection IncomeDeliveryConnection, sessionID, streamKey string, sequence int64) bool
	OwnsIncomeStream(connection IncomeDeliveryConnection, sessionID, streamKey string) bool
	HandleIncomeAck(connection IncomeDeliveryConnection, sessionID, streamKey string, sequence int64)
	HandleIncomeBackpressure(connection IncomeDeliveryConnection, sessionID, streamKey string, resumeAfter time.Time)
}

func (s *Service) SetIncomeDeliveryObserver(observer IncomeDeliveryObserver) {
	if s != nil {
		s.incomeDeliveryObserver = observer
	}
}

func (s *Service) ResetIncomeDeliveryStream(sessionID, streamKey string) {
	if s != nil && s.incomeDataWindow != nil && streamKey == incomeStreamKey(sessionID) {
		s.incomeDataWindow.ForgetStream(sessionID, streamKey)
	}
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

func (s *Service) DeliverOrderLifecycleBatch(ctx context.Context, batch OrderLifecycleDeliveryBatch) error {
	if s == nil {
		return status.Error(codes.FailedPrecondition, "runtime channel service is not configured")
	}
	if batch.UserID <= 0 || batch.RuntimeID == "" || batch.SessionID == "" {
		return status.Error(codes.InvalidArgument, "user_id, runtime_id, and session_id are required")
	}
	if len(batch.Events) == 0 {
		return nil
	}
	seq := batch.Sequence
	if seq <= 0 {
		seq = int64(len(batch.Events))
	}
	frame := &cpv1.RuntimeFrame{
		FrameType: cpv1.FrameType_FRAME_TYPE_ORDER_UPDATE_BATCH,
		Payload: &cpv1.RuntimeFrame_OrderUpdateBatch{
			OrderUpdateBatch: &cpv1.RuntimeOrderUpdateBatch{
				SessionId: batch.SessionID,
				StreamKey: orderLifecycleStreamKey,
				Sequence:  seq,
				Events:    batch.Events,
			},
		},
	}
	stream := s.registry.FindByRuntimeID(batch.UserID, batch.RuntimeID)
	if stream == nil {
		return status.Error(codes.Unavailable, s.missingRuntimeStreamReason(batch.UserID, batch.RuntimeID))
	}
	return stream.sendFrame(frame)
}

func (s *Service) DeliverIncomeBatch(ctx context.Context, batch IncomeDeliveryBatch) error {
	if s == nil {
		return status.Error(codes.FailedPrecondition, "runtime channel service is not configured")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if batch.UserID <= 0 || batch.RuntimeID == "" || batch.ConnectionID == "" || batch.SessionID == "" {
		return status.Error(codes.InvalidArgument, "user_id, runtime_id, connection_id, and session_id are required")
	}
	canonicalStreamKey := incomeStreamKey(batch.SessionID)
	if batch.StreamKey != canonicalStreamKey {
		return status.Errorf(codes.InvalidArgument, "Income stream_key must be %q", canonicalStreamKey)
	}
	if len(batch.Entries) == 0 {
		return nil
	}
	lastID := int64(0)
	for _, entry := range batch.Entries {
		if entry == nil || entry.GetSessionId() != batch.SessionID || entry.GetIncomeEntryId() <= lastID {
			return status.Error(codes.InvalidArgument, "Income entries must be non-nil, exact-Session, and strictly ascending")
		}
		switch entry.GetStatus() {
		case "confirmed", "calculated":
		default:
			return status.Error(codes.InvalidArgument, "Income entries must be deliverable")
		}
		lastID = entry.GetIncomeEntryId()
	}
	if batch.Sequence == 0 {
		batch.Sequence = lastID
	}
	if batch.Sequence != lastID {
		return status.Error(codes.InvalidArgument, "Income sequence must equal the final income_entry_id")
	}
	stream := s.registry.FindByRuntimeID(batch.UserID, batch.RuntimeID)
	if stream == nil {
		return status.Error(codes.Unavailable, s.missingRuntimeStreamReason(batch.UserID, batch.RuntimeID))
	}
	if incomeDeliveryConnection(stream.Runtime).ConnectionID != batch.ConnectionID {
		return status.Error(codes.Unavailable, "Income delivery route belongs to a stale runtime connection")
	}
	if _, err := s.incomeDataWindow.Track(batch.SessionID, batch.StreamKey, batch.Sequence, nil, s.now().UTC()); err != nil {
		return err
	}
	cancelTracked := true
	defer func() {
		if cancelTracked {
			s.incomeDataWindow.Cancel(batch.SessionID, batch.StreamKey, batch.Sequence)
		}
	}()
	frame := &cpv1.RuntimeFrame{
		FrameType: cpv1.FrameType_FRAME_TYPE_INCOME_BATCH,
		Payload: &cpv1.RuntimeFrame_IncomeBatch{IncomeBatch: &cpv1.RuntimeIncomeBatch{
			SessionId: batch.SessionID,
			StreamKey: batch.StreamKey,
			Sequence:  batch.Sequence,
			Entries:   batch.Entries,
		}},
	}
	if err := stream.sendFrame(frame); err != nil {
		return err
	}
	cancelTracked = false
	return nil
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

func (s *Service) handleRuntimeDataAck(rt AuthenticatedRuntime, frame *cpv1.RuntimeFrame) {
	ack := frame.GetDataAck()
	if ack == nil || s == nil || s.dataWindow == nil {
		return
	}
	connection := incomeDeliveryConnection(rt)
	if ack.GetStreamKey() == incomeStreamKey(ack.GetSessionId()) {
		if s.incomeDeliveryObserver == nil || !s.incomeDeliveryObserver.OwnsIncomeDelivery(
			connection,
			ack.GetSessionId(),
			ack.GetStreamKey(),
			ack.GetSequence(),
		) {
			return
		}
		if s.incomeDataWindow == nil || !s.incomeDataWindow.Ack(ack.GetSessionId(), ack.GetStreamKey(), ack.GetSequence()) {
			return
		}
		s.incomeDeliveryObserver.HandleIncomeAck(connection, ack.GetSessionId(), ack.GetStreamKey(), ack.GetSequence())
		return
	}
	if !s.dataWindow.Ack(ack.GetSessionId(), ack.GetStreamKey(), ack.GetSequence()) {
		return
	}
}
