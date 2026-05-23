package runtimechannel

import (
	"testing"

	cpv1 "github.com/hushine-tech/control-panel-service/gen/controlpanelv1"
)

func TestRuntimeChannelCommandAndDataFrameTypesExist(t *testing.T) {
	required := []cpv1.FrameType{
		cpv1.FrameType_FRAME_TYPE_COMMAND,
		cpv1.FrameType_FRAME_TYPE_COMMAND_ACK,
		cpv1.FrameType_FRAME_TYPE_COMMAND_RESULT,
		cpv1.FrameType_FRAME_TYPE_STATUS_PATCH,
		cpv1.FrameType_FRAME_TYPE_SHUTDOWN,
		cpv1.FrameType_FRAME_TYPE_DATASET_CHUNK,
		cpv1.FrameType_FRAME_TYPE_LIVE_KLINE_BATCH,
		cpv1.FrameType_FRAME_TYPE_DATA_ACK,
		cpv1.FrameType_FRAME_TYPE_DATA_BACKPRESSURE,
		cpv1.FrameType_FRAME_TYPE_DATA_END,
		cpv1.FrameType_FRAME_TYPE_HELLO_ACK,
		cpv1.FrameType_FRAME_TYPE_RESUME,
	}
	for _, frameType := range required {
		if frameType == cpv1.FrameType_FRAME_TYPE_UNSPECIFIED {
			t.Fatalf("frame type %s is unspecified", frameType)
		}
	}

	_ = &cpv1.RuntimeCommandFrame{}
	_ = &cpv1.RuntimeCommandAck{}
	_ = &cpv1.RuntimeCommandResult{}
	_ = &cpv1.RuntimeStatusPatch{}
	_ = &cpv1.RuntimeShutdown{}
	_ = &cpv1.RuntimeDatasetChunk{}
	_ = &cpv1.RuntimeLiveKlineBatch{}
	_ = &cpv1.RuntimeDataAck{}
	_ = &cpv1.RuntimeDataBackpressure{}
	_ = &cpv1.RuntimeDataEnd{}
	_ = &cpv1.RuntimeHelloAck{}
	_ = &cpv1.RuntimeResume{}
	_ = &cpv1.RuntimeAdmissionFailure{}
	_ = &cpv1.ListRuntimeAdmissionFailuresRequest{}
	_ = &cpv1.ListRuntimeAdmissionFailuresResponse{}
}
