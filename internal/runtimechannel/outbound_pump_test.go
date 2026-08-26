package runtimechannel

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	cpv1 "github.com/hushine-tech/control-panel-service/gen/controlpanelv1"
	portfoliov1 "github.com/hushine-tech/core-service/gen/portfoliov1"
)

func TestRuntimeStreamBlockedDataSendClosesConnectionAtBoundedDeadline(t *testing.T) {
	timeout := make(chan time.Time, 1)
	svc := NewWithInstanceID(&stubRepo{}, "cp-1")
	stream, err := svc.registry.Register(AuthenticatedRuntime{UserID: 42, RuntimeID: "runtime-1"}, time.Time{})
	if err != nil {
		t.Fatalf("register runtime: %v", err)
	}
	stream.configureOutbound(outboundPumpConfig{
		queueCapacity: 4,
		sendTimeout:   time.Minute,
		after: func(time.Duration) <-chan time.Time {
			return timeout
		},
	})
	physicalStarted := make(chan struct{})
	physicalExited := make(chan struct{})
	stream.setSender(func(*cpv1.RuntimeFrame) error {
		close(physicalStarted)
		<-stream.closed
		close(physicalExited)
		return errors.New("connection closed")
	})

	if err := svc.DeliverIncomeBatch(context.Background(), IncomeDeliveryBatch{
		UserID:       42,
		RuntimeID:    "runtime-1",
		ConnectionID: stream.Runtime.ConnectionID,
		AttemptToken: "attempt-1",
		SessionID:    "session-a",
		StreamKey:    "income/session-a",
		Sequence:     1,
		Entries: []*portfoliov1.VenueIncomeEntry{{
			IncomeEntryId: 1, SessionId: "session-a", Status: "confirmed",
		}},
	}); err != nil {
		t.Fatalf("DeliverIncomeBatch enqueue: %v", err)
	}
	receiveSignal(t, physicalStarted, "blocked physical Income send")
	controlDone := make(chan error, 1)
	if err := stream.enqueueOutbound(&outboundFrame{frame: heartbeatAckFrame(), done: controlDone}); err != nil {
		t.Fatalf("enqueue heartbeat: %v", err)
	}

	timeout <- time.Now()
	receiveSignal(t, stream.closed, "bounded connection close")
	receiveSignal(t, physicalExited, "physical sender exit")
	select {
	case err := <-controlDone:
		if statusCodeOK(err) {
			t.Fatalf("queued heartbeat error = %v, want closed connection", err)
		}
	case <-time.After(time.Second):
		t.Fatal("queued heartbeat remained blocked after send deadline")
	}
}

func TestRuntimeStreamPumpPrioritizesControlAndFairlySchedulesSessions(t *testing.T) {
	stream := newRuntimeStream(AuthenticatedRuntime{UserID: 42, RuntimeID: "runtime-1"}, time.Time{})
	stream.configureOutbound(outboundPumpConfig{queueCapacity: 8, sendTimeout: time.Minute})
	release := make(chan struct{})
	observed := make(chan string, 8)
	stream.setSender(func(frame *cpv1.RuntimeFrame) error {
		observed <- outboundFrameLabel(frame)
		<-release
		return nil
	})
	defer stream.close()

	if err := stream.enqueueDataFrame(incomeFrame("session-a", 1), nil); err != nil {
		t.Fatalf("enqueue session-a/1: %v", err)
	}
	if got := receiveLabel(t, observed); got != "session-a/1" {
		t.Fatalf("first physical send = %q, want session-a/1", got)
	}
	for _, frame := range []*cpv1.RuntimeFrame{
		incomeFrame("session-a", 2),
		incomeFrame("session-a", 3),
		incomeFrame("session-b", 1),
	} {
		if err := stream.enqueueDataFrame(frame, nil); err != nil {
			t.Fatalf("enqueue %s: %v", outboundFrameLabel(frame), err)
		}
	}
	controlDone := make(chan error, 1)
	if err := stream.enqueueOutbound(&outboundFrame{frame: heartbeatAckFrame(), done: controlDone}); err != nil {
		t.Fatalf("enqueue heartbeat: %v", err)
	}

	want := []string{"heartbeat", "session-a/2", "session-b/1", "session-a/3"}
	for _, label := range want {
		release <- struct{}{}
		if got := receiveLabel(t, observed); got != label {
			t.Fatalf("next physical send = %q, want %q", got, label)
		}
	}
	release <- struct{}{}
	select {
	case err := <-controlDone:
		if err != nil {
			t.Fatalf("heartbeat send: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("heartbeat caller did not complete")
	}
}

func TestRuntimeStreamDataQueueBackpressurePreservesControlCapacity(t *testing.T) {
	stream := newRuntimeStream(AuthenticatedRuntime{UserID: 42, RuntimeID: "runtime-1"}, time.Time{})
	stream.configureOutbound(outboundPumpConfig{queueCapacity: 1, sendTimeout: time.Minute})
	physicalStarted := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	stream.setSender(func(*cpv1.RuntimeFrame) error {
		once.Do(func() { close(physicalStarted) })
		<-release
		return nil
	})
	defer stream.close()

	if err := stream.enqueueDataFrame(incomeFrame("session-a", 1), nil); err != nil {
		t.Fatalf("enqueue in-flight data: %v", err)
	}
	receiveSignal(t, physicalStarted, "in-flight data send")
	if err := stream.enqueueDataFrame(incomeFrame("session-a", 2), nil); err != nil {
		t.Fatalf("fill data queue: %v", err)
	}
	if err := stream.enqueueDataFrame(incomeFrame("session-b", 1), nil); !errors.Is(err, ErrRuntimeDataBackpressure) {
		t.Fatalf("overflow data enqueue error = %v, want ErrRuntimeDataBackpressure", err)
	}
	controlDone := make(chan error, 1)
	if err := stream.enqueueOutbound(&outboundFrame{frame: heartbeatAckFrame(), done: controlDone}); err != nil {
		t.Fatalf("enqueue reserved heartbeat: %v", err)
	}
	for i := 0; i < 3; i++ {
		release <- struct{}{}
	}
	select {
	case err := <-controlDone:
		if err != nil {
			t.Fatalf("reserved control enqueue/send: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("control send did not complete through full data queue")
	}
}

func TestRuntimeStreamShutdownFailsQueuedSendAndReplacementRunsIndependently(t *testing.T) {
	old := newRuntimeStream(AuthenticatedRuntime{UserID: 42, RuntimeID: "runtime-1", ConnectionID: "old"}, time.Time{})
	old.configureOutbound(outboundPumpConfig{queueCapacity: 2, sendTimeout: time.Minute})
	started := make(chan struct{})
	old.setSender(func(*cpv1.RuntimeFrame) error {
		close(started)
		<-old.closed
		return errors.New("old closed")
	})
	if err := old.enqueueDataFrame(incomeFrame("session-a", 1), nil); err != nil {
		t.Fatalf("enqueue old data: %v", err)
	}
	receiveSignal(t, started, "old physical send")
	queued := make(chan error, 1)
	go func() { queued <- old.sendFrame(heartbeatAckFrame()) }()
	old.close()
	select {
	case err := <-queued:
		if statusCodeOK(err) {
			t.Fatalf("old queued send error = %v, want connection failure", err)
		}
	case <-time.After(time.Second):
		t.Fatal("old queued send did not fail on shutdown")
	}

	replacement := newRuntimeStream(AuthenticatedRuntime{UserID: 42, RuntimeID: "runtime-1", ConnectionID: "new"}, time.Time{})
	replacement.setSender(func(*cpv1.RuntimeFrame) error { return nil })
	defer replacement.close()
	if err := replacement.sendFrame(heartbeatAckFrame()); err != nil {
		t.Fatalf("replacement control send: %v", err)
	}
}

func incomeFrame(sessionID string, sequence int64) *cpv1.RuntimeFrame {
	return &cpv1.RuntimeFrame{
		FrameType: cpv1.FrameType_FRAME_TYPE_INCOME_BATCH,
		Payload: &cpv1.RuntimeFrame_IncomeBatch{IncomeBatch: &cpv1.RuntimeIncomeBatch{
			SessionId: sessionID,
			StreamKey: "income/" + sessionID,
			Sequence:  sequence,
		}},
	}
}

func heartbeatAckFrame() *cpv1.RuntimeFrame {
	return &cpv1.RuntimeFrame{
		FrameType: cpv1.FrameType_FRAME_TYPE_HEARTBEAT_ACK,
		Payload:   &cpv1.RuntimeFrame_HeartbeatAck{HeartbeatAck: &cpv1.RuntimeHeartbeatAck{RuntimeId: "runtime-1"}},
	}
}

func outboundFrameLabel(frame *cpv1.RuntimeFrame) string {
	if batch := frame.GetIncomeBatch(); batch != nil {
		return fmt.Sprintf("%s/%d", batch.GetSessionId(), batch.GetSequence())
	}
	if frame.GetHeartbeatAck() != nil {
		return "heartbeat"
	}
	return frame.GetFrameType().String()
}

func receiveLabel(t *testing.T, labels <-chan string) string {
	t.Helper()
	select {
	case label := <-labels:
		return label
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for physical send")
		return ""
	}
}

func statusCodeOK(err error) bool {
	return err == nil
}
