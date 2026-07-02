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
	accountv1 "github.com/hushine-tech/core-service/gen/accountv1"
)

const statusPatchPersistTimeout = 10 * time.Second

func (s *Service) handleRuntimeStatusPatch(rt AuthenticatedRuntime, frame *cpv1.RuntimeFrame) {
	if s.platform == nil {
		return
	}
	patch := frame.GetStatusPatch()
	if patch == nil || strings.TrimSpace(patch.GetSessionId()) == "" {
		return
	}
	statusText := strings.TrimSpace(patch.GetStatus())
	if !statusPatchSessionStatus(statusText) {
		return
	}

	req := &accountv1.UpdateSessionRequest{
		SessionId: patch.GetSessionId(),
		Status:    statusText,
		Error:     patch.GetReason(),
		RuntimeId: rt.RuntimeID,
	}
	if payload := patch.GetPayload(); payload != nil {
		embedded := &accountv1.UpdateSessionRequest{}
		if err := payload.UnmarshalTo(embedded); err == nil {
			if embedded.GetSessionId() != "" {
				req.SessionId = embedded.GetSessionId()
			}
			if embedded.GetStatus() != "" {
				req.Status = embedded.GetStatus()
			}
			req.BarsProcessed = embedded.GetBarsProcessed()
			if embedded.GetError() != "" {
				req.Error = embedded.GetError()
			}
		}
	}
	req.RuntimeId = rt.RuntimeID

	payload, err := anypb.New(req)
	if err != nil {
		logger.Warn(context.Background(), "system", fmt.Sprintf("runtime status patch pack failed: runtime_id=%s session_id=%s err=%v", rt.RuntimeID, req.GetSessionId(), err))
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), statusPatchPersistTimeout)
	defer cancel()
	if _, err := s.platform.DispatchRuntimeRequest(ctx, rt, "account.UpdateSession", payload); err != nil {
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

func statusPatchSessionStatus(raw string) bool {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "running", "stopping", "recoverable", "finished", "completed", "stopped", "failed", "stop_failed", "preflight_failed":
		return true
	default:
		return false
	}
}
