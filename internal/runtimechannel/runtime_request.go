package runtimechannel

import (
	"context"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/emptypb"

	cpv1 "github.com/hushine-tech/control-panel-service/gen/controlpanelv1"
)

type PlatformDispatcher interface {
	DispatchRuntimeRequest(ctx context.Context, rt AuthenticatedRuntime, method string, payload *anypb.Any) (proto.Message, error)
}

func (s *Service) handleRuntimeRequest(parent context.Context, stream *runtimeStream, frame *cpv1.RuntimeFrame) {
	req := frame.GetRequest()
	if s.platform == nil {
		_ = stream.sendFrame(runtimeRequestErrorFrame(
			frame.GetCorrelationId(),
			status.Error(codes.Unimplemented, "runtime platform proxy is not configured"),
		))
		return
	}

	ctx := parent
	cancel := func() {}
	if deadlineMS := frame.GetDeadlineUnixMs(); deadlineMS > 0 {
		deadline := time.UnixMilli(deadlineMS)
		ctx, cancel = context.WithDeadline(parent, deadline)
	}
	defer cancel()

	resp, err := s.platform.DispatchRuntimeRequest(ctx, stream.Runtime, req.GetMethod(), req.GetRequest())
	if err != nil {
		_ = stream.sendFrame(runtimeRequestErrorFrame(frame.GetCorrelationId(), err))
		return
	}
	packed, err := packRuntimeResponse(resp)
	if err != nil {
		_ = stream.sendFrame(runtimeRequestErrorFrame(frame.GetCorrelationId(), err))
		return
	}
	_ = stream.sendFrame(&cpv1.RuntimeFrame{
		CorrelationId: frame.GetCorrelationId(),
		FrameType:     cpv1.FrameType_FRAME_TYPE_RESPONSE,
		Payload: &cpv1.RuntimeFrame_Response{
			Response: packed,
		},
	})
}

func packRuntimeResponse(resp proto.Message) (*cpv1.StrategyResponse, error) {
	if resp == nil {
		resp = &emptypb.Empty{}
	}
	packed, err := anypb.New(resp)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "pack runtime platform response: %v", err)
	}
	return &cpv1.StrategyResponse{Response: packed}, nil
}

func runtimeRequestErrorFrame(correlationID string, err error) *cpv1.RuntimeFrame {
	code := codes.Internal
	message := "internal runtime platform proxy error"
	if err != nil {
		if st, ok := status.FromError(err); ok {
			code = st.Code()
			message = st.Message()
		} else {
			message = err.Error()
		}
	}
	return &cpv1.RuntimeFrame{
		CorrelationId: correlationID,
		FrameType:     cpv1.FrameType_FRAME_TYPE_ERROR,
		Payload: &cpv1.RuntimeFrame_Error{
			Error: &cpv1.StreamError{
				Code:    grpcCodeName(code),
				Message: message,
			},
		},
	}
}

func grpcCodeName(code codes.Code) string {
	switch code {
	case codes.InvalidArgument:
		return "InvalidArgument"
	case codes.PermissionDenied:
		return "PermissionDenied"
	case codes.NotFound:
		return "NotFound"
	case codes.FailedPrecondition:
		return "FailedPrecondition"
	case codes.DeadlineExceeded:
		return "DeadlineExceeded"
	case codes.Unavailable:
		return "Unavailable"
	case codes.Unimplemented:
		return "Unimplemented"
	default:
		return "Internal"
	}
}
