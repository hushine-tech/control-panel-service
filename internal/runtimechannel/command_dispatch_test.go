package runtimechannel

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"

	cpv1 "github.com/hushine-tech/control-panel-service/gen/controlpanelv1"
	"github.com/hushine-tech/control-panel-service/internal/domain"
)

func jsonAny(t *testing.T, raw []byte) *anypb.Any {
	t.Helper()
	return &anypb.Any{TypeUrl: commandPayloadJSONTypeURL, Value: raw}
}

func TestDispatchNextRuntimeCommandSendsCommandFrameThroughOwner(t *testing.T) {
	repo, _, now := newAuthFixture(t, domain.CredentialStatusActive)
	repo.claimCommand = domain.RuntimeCommand{
		CommandID:   "cmd-1",
		UserID:      42,
		RuntimeID:   "runtime-1",
		SessionID:   "sess-1",
		CommandType: domain.RuntimeCommandTypeStopSession,
		Status:      domain.RuntimeCommandStatusSent,
		Payload:     []byte(`{"session_id":"sess-1"}`),
		DeadlineAt:  now.Add(time.Minute),
	}
	svc := NewWithInstanceID(repo, "cp-owner")
	stream, err := svc.registry.Register(AuthenticatedRuntime{
		KeyID:     "key-1",
		UserID:    42,
		RuntimeID: "runtime-1",
		Role:      domain.CredentialRoleExecutor,
	}, now)
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	sent := make(chan *cpv1.RuntimeFrame, 1)
	stream.setSender(func(frame *cpv1.RuntimeFrame) error {
		sent <- frame
		return nil
	})

	cmd, ok, err := svc.DispatchNextRuntimeCommand(context.Background(), "runtime-1", 2)
	if err != nil {
		t.Fatalf("DispatchNextRuntimeCommand: %v", err)
	}
	if !ok || cmd.CommandID != "cmd-1" {
		t.Fatalf("cmd=%+v ok=%v, want cmd-1", cmd, ok)
	}
	if repo.claimOwner != "cp-owner" || repo.claimLimit != 2 {
		t.Fatalf("claim owner/limit = %q/%d, want cp-owner/2", repo.claimOwner, repo.claimLimit)
	}
	frame := <-sent
	if frame.GetFrameType() != cpv1.FrameType_FRAME_TYPE_COMMAND {
		t.Fatalf("frame type = %s, want COMMAND", frame.GetFrameType())
	}
	if frame.GetCommand().GetCommandId() != "cmd-1" || frame.GetCommand().GetSessionId() != "sess-1" {
		t.Fatalf("command frame = %+v", frame.GetCommand())
	}
}

func TestRuntimeCommandAckAndResultFramesPersistState(t *testing.T) {
	repo, _, now := newAuthFixture(t, domain.CredentialStatusActive)
	svc := NewWithInstanceID(repo, "cp-owner")
	svc.SetClock(func() time.Time { return now })

	if err := svc.handleRuntimeCommandFrame(context.Background(), &cpv1.RuntimeFrame{
		FrameType: cpv1.FrameType_FRAME_TYPE_COMMAND_ACK,
		Payload: &cpv1.RuntimeFrame_CommandAck{CommandAck: &cpv1.RuntimeCommandAck{
			CommandId: "cmd-ack",
			Status:    domain.RuntimeCommandStatusAcked,
		}},
	}); err != nil {
		t.Fatalf("ack frame: %v", err)
	}
	if repo.ackedCommandID != "cmd-ack" {
		t.Fatalf("acked command id = %q, want cmd-ack", repo.ackedCommandID)
	}

	if err := svc.handleRuntimeCommandFrame(context.Background(), &cpv1.RuntimeFrame{
		FrameType: cpv1.FrameType_FRAME_TYPE_COMMAND_RESULT,
		Payload: &cpv1.RuntimeFrame_CommandResult{CommandResult: &cpv1.RuntimeCommandResult{
			CommandId:     "cmd-result",
			Status:        domain.RuntimeCommandStatusFailed,
			FailureReason: "worker exited",
		}},
	}); err != nil {
		t.Fatalf("result frame: %v", err)
	}
	if repo.completedCommandID != "cmd-result" ||
		repo.completedStatus != domain.RuntimeCommandStatusFailed ||
		repo.completedFailure != "worker exited" {
		t.Fatalf("completed = id:%q status:%q failure:%q", repo.completedCommandID, repo.completedStatus, repo.completedFailure)
	}
}

func TestInvokeRuntimeCommandUnblocksWhenStreamUnregisters(t *testing.T) {
	repo, _, now := newAuthFixture(t, domain.CredentialStatusActive)
	svc := NewWithInstanceID(repo, "cp-owner")
	stream, err := svc.registry.Register(AuthenticatedRuntime{
		KeyID:     "key-1",
		UserID:    42,
		RuntimeID: "runtime-1",
		Role:      domain.CredentialRoleDebugger,
	}, now)
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	sent := make(chan *cpv1.RuntimeFrame, 1)
	stream.setSender(func(frame *cpv1.RuntimeFrame) error {
		sent <- frame
		return nil
	})

	done := make(chan error, 1)
	go func() {
		_, err := svc.InvokeRuntimeCommand(
			context.Background(),
			42,
			"runtime-1",
			"prepare_debug_workspace",
			[]byte(`{"container_path":"/workspace"}`),
		)
		done <- err
	}()

	select {
	case <-sent:
	case <-time.After(time.Second):
		t.Fatal("runtime did not receive command")
	}
	svc.registry.Unregister("runtime-1")

	select {
	case err := <-done:
		if status.Code(err) != codes.Unavailable {
			t.Fatalf("code = %v, want Unavailable (err=%v)", status.Code(err), err)
		}
	case <-time.After(time.Second):
		t.Fatal("InvokeRuntimeCommand did not unblock after stream unregister")
	}
}

func TestInvokeRuntimeCommandAcceptsCommandIDOnlyResultFrames(t *testing.T) {
	repo, _, now := newAuthFixture(t, domain.CredentialStatusActive)
	svc := NewWithInstanceID(repo, "cp-owner")
	stream, err := svc.registry.Register(AuthenticatedRuntime{
		KeyID:     "key-1",
		UserID:    42,
		RuntimeID: "runtime-1",
		Role:      domain.CredentialRoleDebugger,
	}, now)
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	sent := make(chan *cpv1.RuntimeFrame, 1)
	stream.setSender(func(frame *cpv1.RuntimeFrame) error {
		sent <- frame
		return nil
	})

	done := make(chan error, 1)
	go func() {
		raw, err := svc.InvokeRuntimeCommand(
			context.Background(),
			42,
			"runtime-1",
			"prepare_debug_workspace",
			[]byte(`{"container_path":"/workspace"}`),
		)
		if err == nil && string(raw) != `{"ok":true}` {
			err = status.Errorf(codes.Internal, "raw result = %s", raw)
		}
		done <- err
	}()

	cmd := <-sent
	commandID := cmd.GetCommand().GetCommandId()
	if commandID == "" {
		t.Fatal("command_id is empty")
	}
	if !stream.deliver(&cpv1.RuntimeFrame{
		FrameType: cpv1.FrameType_FRAME_TYPE_COMMAND_ACK,
		Payload: &cpv1.RuntimeFrame_CommandAck{CommandAck: &cpv1.RuntimeCommandAck{
			CommandId: commandID,
			Status:    domain.RuntimeCommandStatusAcked,
		}},
	}) {
		t.Fatal("command ack was not delivered by command_id")
	}
	if !stream.deliver(&cpv1.RuntimeFrame{
		FrameType: cpv1.FrameType_FRAME_TYPE_COMMAND_RESULT,
		Payload: &cpv1.RuntimeFrame_CommandResult{CommandResult: &cpv1.RuntimeCommandResult{
			CommandId: commandID,
			Status:    domain.RuntimeCommandStatusSucceeded,
			Result:    jsonAny(t, []byte(`{"ok":true}`)),
		}},
	}) {
		t.Fatal("command result was not delivered by command_id")
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("InvokeRuntimeCommand: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("InvokeRuntimeCommand did not complete")
	}
}
