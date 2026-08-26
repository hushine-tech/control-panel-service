package runtimechannel

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	cpv1 "github.com/hushine-tech/control-panel-service/gen/controlpanelv1"
	cpnotify "github.com/hushine-tech/control-panel-service/internal/notification"
)

func (s *Service) handleRuntimeDataBackpressure(parent context.Context, stream *runtimeStream, frame *cpv1.RuntimeFrame) {
	if s == nil || stream == nil || frame == nil || frame.GetDataBackpressure() == nil || !s.registry.IsCurrent(stream) {
		return
	}
	rt := stream.Runtime
	ctx, cancel := stream.connectionContext(parent)
	defer cancel()
	bp := frame.GetDataBackpressure()
	sessionID := strings.TrimSpace(bp.GetSessionId())
	if sessionID == "" || rt.UserID <= 0 {
		return
	}
	streamKey := strings.TrimSpace(bp.GetStreamKey())
	connection := incomeDeliveryConnection(rt)
	if streamKey == incomeStreamKey(sessionID) {
		if s.incomeDeliveryObserver == nil {
			return
		}
		accepted := false
		s.registry.withCurrent(stream, func() {
			sequence, attemptToken, ok := s.incomeDeliveryObserver.IncomeBackpressureAttempt(connection, sessionID, streamKey)
			if !ok || attemptToken == "" || s.incomeDataWindow == nil ||
				!s.incomeDataWindow.CancelAttempt(sessionID, streamKey, sequence, attemptToken) {
				return
			}
			s.incomeDeliveryObserver.HandleIncomeBackpressure(
				connection,
				sessionID,
				streamKey,
				sequence,
				attemptToken,
				time.UnixMilli(bp.GetResumeAfterUnixMs()).UTC(),
			)
			accepted = true
		})
		if !accepted {
			return
		}
	}
	reason := strings.TrimSpace(bp.GetReason())
	eventType := cpnotify.EventRuntimeDataDelayed
	title := "Runtime data delayed"
	message := fmt.Sprintf("Runtime data is delayed because the strategy consumer is lagging: %s", fallbackString(streamKey, "unknown stream"))
	dedupePrefix := "runtime-data-delayed"
	if strings.HasPrefix(reason, "data_dropped:") {
		eventType = cpnotify.EventRuntimeDataDropped
		title = "Runtime data dropped"
		message = fmt.Sprintf("Runtime data was dropped because the strategy consumer is lagging: %s", fallbackString(streamKey, "unknown stream"))
		dedupePrefix = "runtime-data-dropped"
	}
	metadata := map[string]string{
		"stream_key": streamKey,
		"reason":     reason,
	}
	if rt.Source != "" {
		metadata["runtime_source"] = rt.Source
	}
	publisher := s.notifications
	if publisher == nil {
		publisher = cpnotify.NoopPublisher{}
	}
	if !s.registry.IsCurrent(stream) || ctx.Err() != nil {
		return
	}
	if err := publisher.Publish(ctx, cpnotify.Event{
		SchemaVersion: cpnotify.SchemaVersion,
		UserID:        rt.UserID,
		Category:      cpnotify.CategorySystem,
		EventType:     eventType,
		Severity:      cpnotify.SeverityWarn,
		SourceService: "control-panel-service",
		RuntimeID:     rt.RuntimeID,
		RuntimeName:   rt.Name,
		SessionID:     sessionID,
		Title:         title,
		Message:       message,
		DedupeKey:     fmt.Sprintf("%s:%s:%s", dedupePrefix, sessionID, streamKey),
		Metadata:      metadata,
	}); err != nil {
		log.Printf("publish runtime data backpressure notification failed session=%s runtime=%s stream=%s err=%v", sessionID, rt.RuntimeID, streamKey, err)
	}
}

func fallbackString(value string, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}
