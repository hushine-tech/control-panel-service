package runtimechannel

import (
	"context"
	"fmt"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"

	cpv1 "github.com/hushine-tech/control-panel-service/gen/controlpanelv1"
	"github.com/hushine-tech/control-panel-service/internal/logger"
	portfoliov1 "github.com/hushine-tech/core-service/gen/portfoliov1"
)

const statusPatchPersistTimeout = 10 * time.Second

func (s *Service) handleRuntimeStatusPatch(parent context.Context, stream *runtimeStream, frame *cpv1.RuntimeFrame) {
	if s == nil || stream == nil || s.platform == nil || !s.registry.IsCurrent(stream) {
		return
	}
	rt := stream.Runtime
	patch := frame.GetStatusPatch()
	if patch == nil || strings.TrimSpace(patch.GetSessionId()) == "" {
		return
	}
	statusText := strings.TrimSpace(patch.GetStatus())
	if !statusPatchSessionStatus(statusText) {
		return
	}

	req := statusPatchUpdateSessionRequest(rt.RuntimeID, patch, statusText)

	payload, err := anypb.New(req)
	if err != nil {
		logger.Warn(context.Background(), "system", fmt.Sprintf("runtime status patch pack failed: runtime_id=%s session_id=%s err=%v", rt.RuntimeID, req.GetSessionId(), err))
		return
	}
	connectionCtx, connectionCancel := stream.connectionContext(parent)
	defer connectionCancel()
	ctx, cancel := context.WithTimeout(connectionCtx, statusPatchPersistTimeout)
	defer cancel()
	if !s.registry.IsCurrent(stream) || ctx.Err() != nil {
		return
	}
	if _, err := s.platform.DispatchRuntimeRequest(ctx, rt, "portfolio.UpdateSession", payload); err != nil {
		if status.Code(err) == codes.FailedPrecondition {
			logger.Warn(context.Background(), "system", fmt.Sprintf(
				"runtime status patch rejected: runtime_id=%s session_id=%s status=%s err=%v",
				rt.RuntimeID,
				req.GetSessionId(),
				req.GetStatus(),
				err,
			))
			return
		}
		logger.Warn(context.Background(), "system", fmt.Sprintf(
			"runtime status patch persist failed: runtime_id=%s session_id=%s status=%s err=%v",
			rt.RuntimeID,
			req.GetSessionId(),
			req.GetStatus(),
			err,
		))
	}
}

func statusPatchUpdateSessionRequest(runtimeID string, patch *cpv1.RuntimeStatusPatch, statusText string) *portfoliov1.UpdateSessionRequest {
	req := &portfoliov1.UpdateSessionRequest{
		SessionId: patch.GetSessionId(),
		Status:    statusText,
		Error:     patch.GetReason(),
		RuntimeId: runtimeID,
	}
	if payload := patch.GetPayload(); payload != nil {
		embedded := &portfoliov1.UpdateSessionRequest{}
		if err := payload.UnmarshalTo(embedded); err == nil {
			req = embedded
			if req.GetSessionId() == "" {
				req.SessionId = patch.GetSessionId()
			}
			if req.GetStatus() == "" {
				req.Status = statusText
			}
			if req.GetError() == "" {
				req.Error = patch.GetReason()
			}
		}
	}
	req.RuntimeId = runtimeID
	return req
}

func statusPatchSessionStatus(raw string) bool {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "running", "stopping", "recoverable", "finished", "stopped", "failed", "stop_failed", "preflight_failed":
		return true
	default:
		return false
	}
}
