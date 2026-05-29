package runtimechannel

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	cpv1 "github.com/hushine-tech/control-panel-service/gen/controlpanelv1"
)

const reconnectWait = 2 * time.Second

func (s *Service) InvokeStrategyUnaryByRuntimeID(
	ctx context.Context,
	userID int64,
	runtimeID string,
	method string,
	req proto.Message,
	resp proto.Message,
) error {
	if userID <= 0 {
		return status.Error(codes.InvalidArgument, "user_id is required")
	}
	if runtimeID == "" {
		return status.Error(codes.InvalidArgument, "runtime_id is required")
	}
	if method == "" {
		return status.Error(codes.InvalidArgument, "strategy method is required")
	}
	stream := s.waitForRuntimeStream(ctx, userID, runtimeID)
	return s.invokeStrategyUnaryOnStream(ctx, stream, method, req, resp)
}

func (s *Service) invokeStrategyUnaryOnStream(
	ctx context.Context,
	stream *runtimeStream,
	method string,
	req proto.Message,
	resp proto.Message,
) error {
	if stream == nil {
		return status.Error(codes.Unavailable, "runtime is not connected")
	}
	requestAny, err := anypb.New(req)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "pack strategy request: %v", err)
	}
	correlationID := newCorrelationID()
	replyCh := stream.registerCall(correlationID)
	defer stream.unregisterCall(correlationID)

	deadlineMS := int64(0)
	if deadline, ok := ctx.Deadline(); ok {
		deadlineMS = deadline.UnixMilli()
	}
	if err := stream.sendFrame(&cpv1.RuntimeFrame{
		CorrelationId:  correlationID,
		FrameType:      cpv1.FrameType_FRAME_TYPE_REQUEST,
		DeadlineUnixMs: deadlineMS,
		Payload: &cpv1.RuntimeFrame_Request{
			Request: &cpv1.StrategyRequest{
				Method:       method,
				Request:      requestAny,
				TraceContext: injectTraceContext(ctx),
			},
		},
	}); err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			_ = stream.sendFrame(&cpv1.RuntimeFrame{
				CorrelationId: correlationID,
				FrameType:     cpv1.FrameType_FRAME_TYPE_ABORT,
				Payload: &cpv1.RuntimeFrame_Abort{
					Abort: &cpv1.StrategyAbort{Reason: ctx.Err().Error()},
				},
			})
			return status.FromContextError(ctx.Err()).Err()
		case <-stream.closed:
			return status.Error(codes.Unavailable, "runtime stream disconnected")
		case frame, ok := <-replyCh:
			if !ok {
				return status.Error(codes.Unavailable, "runtime stream disconnected")
			}
			switch frame.GetFrameType() {
			case cpv1.FrameType_FRAME_TYPE_PROGRESS:
				continue
			case cpv1.FrameType_FRAME_TYPE_ERROR:
				return streamErrorToStatus(frame.GetError())
			case cpv1.FrameType_FRAME_TYPE_RESPONSE:
				if frame.GetResponse() == nil || frame.GetResponse().GetResponse() == nil {
					return status.Error(codes.Internal, "runtime response payload is empty")
				}
				if err := frame.GetResponse().GetResponse().UnmarshalTo(resp); err != nil {
					return status.Errorf(codes.Internal, "unpack runtime response: %v", err)
				}
				return nil
			default:
				return status.Errorf(codes.Internal, "unexpected runtime frame_type=%s", frame.GetFrameType().String())
			}
		}
	}
}

func (s *Service) waitForRuntimeStream(ctx context.Context, userID int64, runtimeID string) *runtimeStream {
	deadline := time.Now().Add(reconnectWait)
	for {
		if stream := s.registry.FindByRuntimeID(userID, runtimeID); stream != nil {
			return stream
		}
		if time.Now().After(deadline) {
			return nil
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func streamErrorToStatus(errFrame *cpv1.StreamError) error {
	if errFrame == nil {
		return status.Error(codes.Internal, "runtime returned empty error frame")
	}
	code := codes.Internal
	switch errFrame.GetCode() {
	case "InvalidArgument":
		code = codes.InvalidArgument
	case "PermissionDenied":
		code = codes.PermissionDenied
	case "NotFound":
		code = codes.NotFound
	case "FailedPrecondition":
		code = codes.FailedPrecondition
	case "DeadlineExceeded":
		code = codes.DeadlineExceeded
	case "Unavailable":
		code = codes.Unavailable
	case "Unimplemented":
		code = codes.Unimplemented
	}
	return status.Error(code, errFrame.GetMessage())
}

func newCorrelationID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return time.Now().UTC().Format("20060102150405.000000000")
	}
	return hex.EncodeToString(b[:])
}
