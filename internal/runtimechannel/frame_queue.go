package runtimechannel

import (
	"errors"
	"sort"
	"strconv"
	"sync"
	"time"

	cpv1 "github.com/hushine-tech/control-panel-service/gen/controlpanelv1"
)

var ErrRuntimeDataBackpressure = errors.New("runtime data backpressure")

type PriorityFrameQueue struct {
	mu       sync.Mutex
	capacity int
	control  []*cpv1.RuntimeFrame
	data     []*cpv1.RuntimeFrame
}

func NewPriorityFrameQueue(capacity int) *PriorityFrameQueue {
	if capacity <= 0 {
		capacity = 1
	}
	return &PriorityFrameQueue{capacity: capacity}
}

func (q *PriorityFrameQueue) Enqueue(frame *cpv1.RuntimeFrame) error {
	if frame == nil {
		return nil
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.control)+len(q.data) >= q.capacity {
		return ErrRuntimeDataBackpressure
	}
	if runtimeFrameIsControl(frame) {
		q.control = append(q.control, frame)
	} else {
		q.data = append(q.data, frame)
	}
	return nil
}

func (q *PriorityFrameQueue) Dequeue() (*cpv1.RuntimeFrame, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.control) > 0 {
		frame := q.control[0]
		q.control = q.control[1:]
		return frame, true
	}
	if len(q.data) > 0 {
		frame := q.data[0]
		q.data = q.data[1:]
		return frame, true
	}
	return nil, false
}

func runtimeFrameIsControl(frame *cpv1.RuntimeFrame) bool {
	switch frame.GetFrameType() {
	case cpv1.FrameType_FRAME_TYPE_HELLO,
		cpv1.FrameType_FRAME_TYPE_HEARTBEAT,
		cpv1.FrameType_FRAME_TYPE_ABORT,
		cpv1.FrameType_FRAME_TYPE_ERROR,
		cpv1.FrameType_FRAME_TYPE_COMMAND,
		cpv1.FrameType_FRAME_TYPE_COMMAND_ACK,
		cpv1.FrameType_FRAME_TYPE_COMMAND_RESULT,
		cpv1.FrameType_FRAME_TYPE_STATUS_PATCH,
		cpv1.FrameType_FRAME_TYPE_SHUTDOWN:
		return true
	default:
		return false
	}
}

type RuntimeDataChunk struct {
	SessionID string
	StreamKey string
	Sequence  int64
	Payload   []byte
	SentAt    time.Time
}

type RuntimeDataWindow struct {
	mu        sync.Mutex
	capacity  int
	nextSeq   map[string]int64
	unacked   map[string]RuntimeDataChunk
	orderKeys []string
}

func NewRuntimeDataWindow(capacity int) *RuntimeDataWindow {
	if capacity <= 0 {
		capacity = 1
	}
	return &RuntimeDataWindow{
		capacity: capacity,
		nextSeq:  map[string]int64{},
		unacked:  map[string]RuntimeDataChunk{},
	}
}

func (w *RuntimeDataWindow) Enqueue(sessionID, streamKey string, payload []byte, at time.Time) (RuntimeDataChunk, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.unacked) >= w.capacity {
		return RuntimeDataChunk{}, ErrRuntimeDataBackpressure
	}
	key := dataWindowStreamKey(sessionID, streamKey)
	seq := w.nextSeq[key] + 1
	w.nextSeq[key] = seq
	chunk := RuntimeDataChunk{
		SessionID: sessionID,
		StreamKey: streamKey,
		Sequence:  seq,
		Payload:   append([]byte(nil), payload...),
		SentAt:    at.UTC(),
	}
	ackKey := dataWindowAckKey(sessionID, streamKey, seq)
	w.unacked[ackKey] = chunk
	w.orderKeys = append(w.orderKeys, ackKey)
	return chunk, nil
}

func (w *RuntimeDataWindow) Ack(sessionID, streamKey string, sequence int64) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	key := dataWindowAckKey(sessionID, streamKey, sequence)
	if _, ok := w.unacked[key]; !ok {
		return false
	}
	delete(w.unacked, key)
	for i, existing := range w.orderKeys {
		if existing == key {
			w.orderKeys = append(w.orderKeys[:i], w.orderKeys[i+1:]...)
			break
		}
	}
	return true
}

func (w *RuntimeDataWindow) Expired(now time.Time, maxAge time.Duration) []RuntimeDataChunk {
	w.mu.Lock()
	defer w.mu.Unlock()
	var expired []RuntimeDataChunk
	for _, key := range w.orderKeys {
		chunk, ok := w.unacked[key]
		if !ok {
			continue
		}
		if now.Sub(chunk.SentAt) >= maxAge {
			expired = append(expired, chunk)
		}
	}
	sort.SliceStable(expired, func(i, j int) bool {
		if expired[i].SessionID != expired[j].SessionID {
			return expired[i].SessionID < expired[j].SessionID
		}
		if expired[i].StreamKey != expired[j].StreamKey {
			return expired[i].StreamKey < expired[j].StreamKey
		}
		return expired[i].Sequence < expired[j].Sequence
	})
	return expired
}

func dataWindowStreamKey(sessionID, streamKey string) string {
	return sessionID + "\x00" + streamKey
}

func dataWindowAckKey(sessionID, streamKey string, sequence int64) string {
	return dataWindowStreamKey(sessionID, streamKey) + "\x00" + strconv.FormatInt(sequence, 10)
}
