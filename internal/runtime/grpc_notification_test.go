package runtime

import (
	"context"
	"testing"

	cpv1 "github.com/hushine-tech/control-panel-service/gen/controlpanelv1"
	"github.com/hushine-tech/control-panel-service/internal/config"
	"github.com/hushine-tech/control-panel-service/internal/domain"
	cpnotify "github.com/hushine-tech/control-panel-service/internal/notification"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestPublishRuntimeNotificationPublishesCustomEvent(t *testing.T) {
	repo := newStubRepo()
	pub := &recordingPublisher{}
	svc := makeService(repo, "pro", nil, config.RuntimePlatformConfig{}, fixedNow)
	svc.notifications = pub
	heartbeat := fixedNow
	if err := repo.CreateRuntime(context.Background(), domain.Runtime{
		RuntimeID:   "rt_notify",
		UserID:      42,
		Name:        "steady-atlas",
		Source:      domain.RuntimeSourceSelfHosted,
		Status:      domain.RuntimeStatusActive,
		HeartbeatAt: &heartbeat,
		CreatedAt:   fixedNow,
		UpdatedAt:   fixedNow,
	}); err != nil {
		t.Fatalf("CreateRuntime: %v", err)
	}
	g := NewControlPanelGRPCService(svc, nil, nil)

	resp, err := g.PublishRuntimeNotification(context.Background(), &cpv1.PublishRuntimeNotificationRequest{
		UserId:    42,
		RuntimeId: "rt_notify",
		Category:  cpnotify.CategoryCustom,
		Severity:  cpnotify.SeverityInfo,
		Message:   "custom message",
	})
	if err != nil {
		t.Fatalf("PublishRuntimeNotification: %v", err)
	}
	if !resp.GetAccepted() {
		t.Fatalf("accepted = false, want true")
	}
	if len(pub.events) != 1 || pub.events[0].EventType != cpnotify.EventCustomInfo || pub.events[0].RuntimeID != "rt_notify" {
		t.Fatalf("events = %+v, want custom.info for rt_notify", pub.events)
	}
}

func TestPublishRuntimeNotificationRejectsTerminalRuntime(t *testing.T) {
	repo := newStubRepo()
	svc := makeService(repo, "pro", nil, config.RuntimePlatformConfig{}, fixedNow)
	if err := repo.CreateRuntime(context.Background(), domain.Runtime{
		RuntimeID: "rt_done",
		UserID:    42,
		Status:    domain.RuntimeStatusEnded,
		CreatedAt: fixedNow,
		UpdatedAt: fixedNow,
	}); err != nil {
		t.Fatalf("CreateRuntime: %v", err)
	}
	g := NewControlPanelGRPCService(svc, nil, nil)

	_, err := g.PublishRuntimeNotification(context.Background(), &cpv1.PublishRuntimeNotificationRequest{
		UserId:    42,
		RuntimeId: "rt_done",
		Category:  cpnotify.CategoryCustom,
		Severity:  cpnotify.SeverityInfo,
		Message:   "custom message",
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("code = %v, want FailedPrecondition (err=%v)", status.Code(err), err)
	}
}
