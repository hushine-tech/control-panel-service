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

func (s *Service) handleRuntimeDataBackpressure(ctx context.Context, rt AuthenticatedRuntime, frame *cpv1.RuntimeFrame) {
	if s == nil || frame == nil || frame.GetDataBackpressure() == nil {
		return
	}
	bp := frame.GetDataBackpressure()
	sessionID := strings.TrimSpace(bp.GetSessionId())
	if sessionID == "" || rt.UserID <= 0 {
		return
	}
	streamKey := strings.TrimSpace(bp.GetStreamKey())
	connection := incomeDeliveryConnection(rt)
	if streamKey == incomeStreamKey(sessionID) && s.incomeDeliveryObserver != nil {
		if !s.incomeDeliveryObserver.OwnsIncomeStream(connection, sessionID, streamKey) {
			return
		}
		s.incomeDeliveryObserver.HandleIncomeBackpressure(
			connection,
			sessionID,
			streamKey,
			time.UnixMilli(bp.GetResumeAfterUnixMs()).UTC(),
		)
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
