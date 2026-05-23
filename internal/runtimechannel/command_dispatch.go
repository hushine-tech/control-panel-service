package runtimechannel

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/anypb"

	cpv1 "github.com/hushine-tech/control-panel-service/gen/controlpanelv1"
	"github.com/hushine-tech/control-panel-service/internal/domain"
)

const commandPayloadJSONTypeURL = "type.googleapis.com/controlpanel.v1.RuntimeCommandPayloadJSON"

func (s *Service) DispatchNextRuntimeCommand(ctx context.Context, runtimeID string, inFlightLimit int) (domain.RuntimeCommand, bool, error) {
	if s == nil || s.repo == nil {
		return domain.RuntimeCommand{}, false, status.Error(codes.FailedPrecondition, "runtime command repository is not configured")
	}
	cmd, ok, err := s.repo.ClaimNextRuntimeCommand(ctx, runtimeID, s.instanceID, s.now().UTC(), inFlightLimit)
	if err != nil || !ok {
		return cmd, ok, err
	}
	stream := s.registry.FindByRuntimeID(cmd.UserID, cmd.RuntimeID)
	if stream == nil {
		_, _ = s.repo.CompleteRuntimeCommand(ctx, cmd.CommandID, domain.RuntimeCommandStatusFailed, nil, "runtime stream is not connected", s.now().UTC())
		return cmd, false, status.Error(codes.Unavailable, "runtime stream is not connected")
	}
	if err := stream.sendFrame(runtimeCommandToFrame(cmd)); err != nil {
		_, _ = s.repo.CompleteRuntimeCommand(ctx, cmd.CommandID, domain.RuntimeCommandStatusFailed, nil, err.Error(), s.now().UTC())
		return cmd, false, err
	}
	return cmd, true, nil
}

func (s *Service) RuntimeCommandCircuitOpen(ctx context.Context, runtimeID string, since time.Time, threshold int64) (bool, int64, error) {
	if s == nil || s.repo == nil {
		return false, 0, status.Error(codes.FailedPrecondition, "runtime command repository is not configured")
	}
	return s.repo.RuntimeCommandCircuitOpen(ctx, runtimeID, since, threshold)
}

func (s *Service) InvokeRuntimeCommand(ctx context.Context, userID int64, runtimeID string, commandType string, payload []byte) ([]byte, error) {
	if userID <= 0 {
		return nil, status.Error(codes.InvalidArgument, "user_id is required")
	}
	if runtimeID == "" {
		return nil, status.Error(codes.InvalidArgument, "runtime_id is required")
	}
	if commandType == "" {
		return nil, status.Error(codes.InvalidArgument, "runtime command_type is required")
	}
	stream := s.waitForRuntimeStream(ctx, userID, runtimeID)
	if stream == nil {
		return nil, status.Error(codes.Unavailable, "runtime is not connected")
	}

	commandID := newCorrelationID()
	replyCh := stream.registerCall(commandID)
	defer stream.unregisterCall(commandID)

	deadlineMS := int64(0)
	if deadline, ok := ctx.Deadline(); ok {
		deadlineMS = deadline.UnixMilli()
	}
	if err := stream.sendFrame(&cpv1.RuntimeFrame{
		CorrelationId:  commandID,
		FrameType:      cpv1.FrameType_FRAME_TYPE_COMMAND,
		DeadlineUnixMs: deadlineMS,
		Payload: &cpv1.RuntimeFrame_Command{
			Command: &cpv1.RuntimeCommandFrame{
				CommandId:   commandID,
				CommandType: commandType,
				RuntimeId:   runtimeID,
				Payload: &anypb.Any{
					TypeUrl: commandPayloadJSONTypeURL,
					Value:   append([]byte(nil), payload...),
				},
			},
		},
	}); err != nil {
		return nil, err
	}

	for {
		select {
		case <-ctx.Done():
			return nil, status.FromContextError(ctx.Err()).Err()
		case <-stream.closed:
			return nil, status.Error(codes.Unavailable, "runtime stream disconnected")
		case frame, ok := <-replyCh:
			if !ok {
				return nil, status.Error(codes.Unavailable, "runtime stream disconnected")
			}
			switch frame.GetFrameType() {
			case cpv1.FrameType_FRAME_TYPE_COMMAND_ACK:
				continue
			case cpv1.FrameType_FRAME_TYPE_COMMAND_RESULT:
				result := frame.GetCommandResult()
				if result == nil {
					return nil, status.Error(codes.Internal, "runtime returned empty command result")
				}
				st := result.GetStatus()
				if st == "" {
					st = domain.RuntimeCommandStatusSucceeded
				}
				if st != domain.RuntimeCommandStatusSucceeded {
					reason := result.GetFailureReason()
					if reason == "" {
						reason = "runtime command failed"
					}
					return nil, status.Error(codes.FailedPrecondition, reason)
				}
				raw, err := marshalAnyJSON(result.GetResult())
				if err != nil {
					return nil, status.Errorf(codes.InvalidArgument, "marshal command result: %v", err)
				}
				return raw, nil
			default:
				return nil, status.Errorf(codes.Internal, "unexpected runtime command frame_type=%s", frame.GetFrameType().String())
			}
		}
	}
}

func runtimeCommandToFrame(cmd domain.RuntimeCommand) *cpv1.RuntimeFrame {
	return &cpv1.RuntimeFrame{
		CorrelationId:  cmd.CommandID,
		FrameType:      cpv1.FrameType_FRAME_TYPE_COMMAND,
		DeadlineUnixMs: cmd.DeadlineAt.UnixMilli(),
		Payload: &cpv1.RuntimeFrame_Command{
			Command: &cpv1.RuntimeCommandFrame{
				CommandId:   cmd.CommandID,
				CommandType: cmd.CommandType,
				RuntimeId:   cmd.RuntimeID,
				SessionId:   cmd.SessionID,
				Payload: &anypb.Any{
					TypeUrl: commandPayloadJSONTypeURL,
					Value:   append([]byte(nil), cmd.Payload...),
				},
			},
		},
	}
}

func (s *Service) handleRuntimeCommandFrame(ctx context.Context, frame *cpv1.RuntimeFrame) error {
	if s == nil || s.repo == nil {
		return status.Error(codes.FailedPrecondition, "runtime command repository is not configured")
	}
	now := s.now().UTC()
	switch frame.GetFrameType() {
	case cpv1.FrameType_FRAME_TYPE_COMMAND_ACK:
		ack := frame.GetCommandAck()
		if ack == nil || ack.GetCommandId() == "" {
			return status.Error(codes.InvalidArgument, "COMMAND_ACK requires command_id")
		}
		if ack.GetStatus() == domain.RuntimeCommandStatusRunning {
			_, err := s.repo.MarkRuntimeCommandRunning(ctx, ack.GetCommandId(), now)
			return err
		}
		_, err := s.repo.AcknowledgeRuntimeCommand(ctx, ack.GetCommandId(), now)
		return err
	case cpv1.FrameType_FRAME_TYPE_COMMAND_RESULT:
		result := frame.GetCommandResult()
		if result == nil || result.GetCommandId() == "" {
			return status.Error(codes.InvalidArgument, "COMMAND_RESULT requires command_id")
		}
		st := result.GetStatus()
		if st == "" {
			st = domain.RuntimeCommandStatusSucceeded
		}
		if !domain.IsRuntimeCommandTerminalStatus(st) {
			return status.Errorf(codes.InvalidArgument, "invalid command result status %q", st)
		}
		raw, err := marshalAnyJSON(result.GetResult())
		if err != nil {
			return status.Errorf(codes.InvalidArgument, "marshal command result: %v", err)
		}
		_, err = s.repo.CompleteRuntimeCommand(ctx, result.GetCommandId(), st, raw, result.GetFailureReason(), now)
		return err
	default:
		return nil
	}
}

func marshalAnyJSON(v *anypb.Any) ([]byte, error) {
	if v == nil {
		return []byte("{}"), nil
	}
	if v.GetTypeUrl() == commandPayloadJSONTypeURL {
		if len(v.GetValue()) == 0 {
			return []byte("{}"), nil
		}
		return append([]byte(nil), v.GetValue()...), nil
	}
	raw, err := protojson.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("any payload: %w", err)
	}
	return raw, nil
}
