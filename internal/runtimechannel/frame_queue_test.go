package runtimechannel

import (
	"testing"
	"time"

	cpv1 "github.com/hushine-tech/control-panel-service/gen/controlpanelv1"
)

func TestPriorityFrameQueueDispatchesControlBeforeData(t *testing.T) {
	q := NewPriorityFrameQueue(8)
	if err := q.Enqueue(&cpv1.RuntimeFrame{
		FrameType: cpv1.FrameType_FRAME_TYPE_DATASET_CHUNK,
		Payload:   &cpv1.RuntimeFrame_DatasetChunk{DatasetChunk: &cpv1.RuntimeDatasetChunk{Sequence: 1}},
	}); err != nil {
		t.Fatalf("enqueue data: %v", err)
	}
	if err := q.Enqueue(&cpv1.RuntimeFrame{
		FrameType: cpv1.FrameType_FRAME_TYPE_SHUTDOWN,
		Payload:   &cpv1.RuntimeFrame_Shutdown{Shutdown: &cpv1.RuntimeShutdown{Reason: "cancelled"}},
	}); err != nil {
		t.Fatalf("enqueue shutdown: %v", err)
	}

	first, ok := q.Dequeue()
	if !ok {
		t.Fatal("empty queue")
	}
	if first.GetFrameType() != cpv1.FrameType_FRAME_TYPE_SHUTDOWN {
		t.Fatalf("first frame = %s, want SHUTDOWN", first.GetFrameType())
	}
	second, ok := q.Dequeue()
	if !ok || second.GetFrameType() != cpv1.FrameType_FRAME_TYPE_DATASET_CHUNK {
		t.Fatalf("second frame = %+v ok=%v, want DATASET_CHUNK", second, ok)
	}
}

func TestPriorityFrameQueueHeartbeatAndStopDuringDataBacklog(t *testing.T) {
	q := NewPriorityFrameQueue(8)
	for i := int64(1); i <= 4; i++ {
		if err := q.Enqueue(&cpv1.RuntimeFrame{
			FrameType: cpv1.FrameType_FRAME_TYPE_LIVE_KLINE_BATCH,
			Payload: &cpv1.RuntimeFrame_LiveKlineBatch{LiveKlineBatch: &cpv1.RuntimeLiveKlineBatch{
				SessionId: "sess-1",
				StreamKey: "futures:ETHUSDT:1m",
				Sequence:  i,
			}},
		}); err != nil {
			t.Fatalf("enqueue data %d: %v", i, err)
		}
	}
	if err := q.Enqueue(&cpv1.RuntimeFrame{
		FrameType: cpv1.FrameType_FRAME_TYPE_HEARTBEAT,
		Payload:   &cpv1.RuntimeFrame_Heartbeat{Heartbeat: &cpv1.Heartbeat{SentAtUnixMs: 1}},
	}); err != nil {
		t.Fatalf("enqueue heartbeat: %v", err)
	}
	if err := q.Enqueue(&cpv1.RuntimeFrame{
		FrameType: cpv1.FrameType_FRAME_TYPE_COMMAND,
		Payload: &cpv1.RuntimeFrame_Command{Command: &cpv1.RuntimeCommandFrame{
			CommandId:   "cmd-stop",
			CommandType: "stop_session",
			SessionId:   "sess-1",
		}},
	}); err != nil {
		t.Fatalf("enqueue stop command: %v", err)
	}

	first, ok := q.Dequeue()
	if !ok || first.GetFrameType() != cpv1.FrameType_FRAME_TYPE_HEARTBEAT {
		t.Fatalf("first = %+v ok=%v, want HEARTBEAT", first, ok)
	}
	second, ok := q.Dequeue()
	if !ok || second.GetFrameType() != cpv1.FrameType_FRAME_TYPE_COMMAND {
		t.Fatalf("second = %+v ok=%v, want COMMAND", second, ok)
	}
}

func TestRuntimeDataWindowSequenceAckAndBackpressure(t *testing.T) {
	window := NewRuntimeDataWindow(2)
	now := time.Date(2026, 5, 16, 12, 0, 0, 0, time.UTC)
	first, err := window.Enqueue("sess-1", "BTCUSDT:1m", []byte("a"), now)
	if err != nil {
		t.Fatalf("enqueue first: %v", err)
	}
	second, err := window.Enqueue("sess-1", "BTCUSDT:1m", []byte("b"), now)
	if err != nil {
		t.Fatalf("enqueue second: %v", err)
	}
	if first.Sequence != 1 || second.Sequence != 2 {
		t.Fatalf("sequences = %d/%d, want 1/2", first.Sequence, second.Sequence)
	}
	if _, err := window.Enqueue("sess-1", "BTCUSDT:1m", []byte("c"), now); err != ErrRuntimeDataBackpressure {
		t.Fatalf("third enqueue err = %v, want ErrRuntimeDataBackpressure", err)
	}
	if !window.Ack("sess-1", "BTCUSDT:1m", 1) {
		t.Fatal("ack sequence 1 failed")
	}
	third, err := window.Enqueue("sess-1", "BTCUSDT:1m", []byte("c"), now)
	if err != nil {
		t.Fatalf("enqueue after ack: %v", err)
	}
	if third.Sequence != 3 {
		t.Fatalf("third sequence = %d, want 3", third.Sequence)
	}
	expired := window.Expired(now.Add(10*time.Second), 5*time.Second)
	if len(expired) != 2 || expired[0].Sequence != 2 || expired[1].Sequence != 3 {
		t.Fatalf("expired = %+v, want sequences 2 and 3", expired)
	}
}
