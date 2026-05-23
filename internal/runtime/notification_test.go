package runtime

import (
	"context"
	"testing"
	"time"

	"github.com/hushine-tech/control-panel-service/internal/config"
	"github.com/hushine-tech/control-panel-service/internal/domain"
	cpnotify "github.com/hushine-tech/control-panel-service/internal/notification"
)

type recordingPublisher struct {
	events []cpnotify.Event
}

func (p *recordingPublisher) Publish(_ context.Context, event cpnotify.Event) error {
	p.events = append(p.events, event)
	return nil
}

func TestHeartbeatFirstActivePublishesStartedOnce(t *testing.T) {
	repo := newStubRepo()
	pub := &recordingPublisher{}
	svc := makeService(repo, "pro", nil, config.RuntimePlatformConfig{}, fixedNow)
	svc.notifications = pub

	res, err := svc.RegisterRuntime(context.Background(), RegisterArgs{
		Source: domain.RuntimeSourceHosted, BindUserID: 42,
		EndpointHost: "h", GRPCPort: 1, ResourceProfile: "small",
	})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	if _, err := svc.HeartbeatRuntime(context.Background(), res.Runtime.RuntimeID, res.RegistrationToken); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	if _, err := svc.HeartbeatRuntime(context.Background(), res.Runtime.RuntimeID, res.RegistrationToken); err != nil {
		t.Fatalf("second heartbeat: %v", err)
	}
	if len(pub.events) != 1 || pub.events[0].EventType != cpnotify.EventRuntimeStarted {
		t.Fatalf("events = %+v, want one runtime.started", pub.events)
	}
}

func TestReapStaleRuntimesPublishesUnhealthy(t *testing.T) {
	repo := newStubRepo()
	pub := &recordingPublisher{}
	svc := makeService(repo, "pro", nil, config.RuntimePlatformConfig{HeartbeatGraceSeconds: 30, DeathGraceSeconds: 300}, fixedNow)
	svc.notifications = pub

	staleHeartbeat := fixedNow.Add(-time.Minute)
	if err := repo.CreateRuntime(context.Background(), domain.Runtime{
		RuntimeID:   "rt_stale",
		UserID:      42,
		Name:        "steady-atlas",
		Source:      domain.RuntimeSourceHosted,
		Status:      domain.RuntimeStatusActive,
		HeartbeatAt: &staleHeartbeat,
		CreatedAt:   fixedNow.Add(-time.Hour),
		UpdatedAt:   staleHeartbeat,
	}); err != nil {
		t.Fatalf("CreateRuntime: %v", err)
	}

	if _, err := svc.ReapStaleRuntimes(context.Background()); err != nil {
		t.Fatalf("ReapStaleRuntimes: %v", err)
	}
	if len(pub.events) != 1 || pub.events[0].EventType != cpnotify.EventRuntimeUnhealthy || pub.events[0].RuntimeID != "rt_stale" {
		t.Fatalf("events = %+v, want runtime.unhealthy for rt_stale", pub.events)
	}
}

func TestEndRuntimePublishesEnded(t *testing.T) {
	repo := newStubRepo()
	pub := &recordingPublisher{}
	svc := makeService(repo, "pro", nil, config.RuntimePlatformConfig{}, fixedNow)
	svc.notifications = pub
	heartbeat := fixedNow.Add(-time.Second)
	if err := repo.CreateRuntime(context.Background(), domain.Runtime{
		RuntimeID:   "rt_end",
		UserID:      42,
		Name:        "steady-atlas",
		Source:      domain.RuntimeSourceSelfHosted,
		Status:      domain.RuntimeStatusActive,
		HeartbeatAt: &heartbeat,
		CreatedAt:   fixedNow.Add(-time.Hour),
		UpdatedAt:   heartbeat,
	}); err != nil {
		t.Fatalf("CreateRuntime: %v", err)
	}

	if _, err := svc.EndRuntime(context.Background(), EndRuntimeArgs{UserID: 42, RuntimeID: "rt_end"}); err != nil {
		t.Fatalf("EndRuntime: %v", err)
	}
	if len(pub.events) != 1 || pub.events[0].EventType != cpnotify.EventRuntimeEnded || pub.events[0].RuntimeID != "rt_end" {
		t.Fatalf("events = %+v, want runtime.ended for rt_end", pub.events)
	}
}
