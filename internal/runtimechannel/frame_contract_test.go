package runtimechannel

import (
	"testing"

	cpv1 "github.com/hushine-tech/control-panel-service/gen/controlpanelv1"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

func requireDependencyFrameMessage(t *testing.T, file protoreflect.FileDescriptor, name protoreflect.Name) protoreflect.MessageDescriptor {
	t.Helper()
	message := file.Messages().ByName(name)
	if message == nil {
		t.Fatalf("%s.%s is missing", file.Package(), name)
	}
	return message
}

func assertDependencyFrameFields(t *testing.T, message protoreflect.MessageDescriptor, expected map[protoreflect.Name]protoreflect.FieldNumber) {
	t.Helper()
	if message.Fields().Len() != len(expected) {
		t.Fatalf("%s has %d fields, want %d", message.FullName(), message.Fields().Len(), len(expected))
	}
	for name, number := range expected {
		field := message.Fields().ByName(name)
		if field == nil {
			t.Fatalf("%s.%s is missing", message.FullName(), name)
		}
		if field.Number() != number {
			t.Fatalf("%s.%s tag = %d, want %d", message.FullName(), name, field.Number(), number)
		}
	}
}

func assertDependencyFrameMessageField(t *testing.T, message protoreflect.MessageDescriptor, name protoreflect.Name, number protoreflect.FieldNumber, target protoreflect.FullName) {
	t.Helper()
	field := message.Fields().ByName(name)
	if field == nil {
		t.Fatalf("%s.%s is missing", message.FullName(), name)
	}
	if field.Number() != number || field.Message() == nil || field.Message().FullName() != target {
		t.Fatalf("%s.%s = tag %d type %v, want tag %d type %s", message.FullName(), name, field.Number(), field.Message(), number, target)
	}
}

func assertDependencyFrameMethod(t *testing.T, file protoreflect.FileDescriptor, name protoreflect.Name, input, output protoreflect.FullName) {
	t.Helper()
	service := file.Services().ByName("ControlPanelService")
	if service == nil {
		t.Fatal("ControlPanelService is missing")
	}
	method := service.Methods().ByName(name)
	if method == nil {
		t.Fatalf("ControlPanelService.%s is missing", name)
	}
	if method.Input().FullName() != input || method.Output().FullName() != output {
		t.Fatalf("%s input/output = %s/%s, want %s/%s", method.FullName(), method.Input().FullName(), method.Output().FullName(), input, output)
	}
}

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

func TestRuntimeDependencyFrameContract(t *testing.T) {
	file := cpv1.File_control_panel_service_proto
	hello := requireDependencyFrameMessage(t, file, "RuntimeHello")
	assertDependencyFrameMessageField(t, hello, "dependency_profile", 15, "strategy.v1.RuntimeDependencyProfile")
	resume := requireDependencyFrameMessage(t, file, "RuntimeResume")
	assertDependencyFrameMessageField(t, resume, "dependency_profile", 4, "strategy.v1.RuntimeDependencyProfile")
	streamError := requireDependencyFrameMessage(t, file, "StreamError")
	assertDependencyFrameMessageField(t, streamError, "dependency_error", 3, "strategy.v1.RuntimeDependencyError")

	startupRequest := requireDependencyFrameMessage(t, file, "ReportRuntimeStartupFailureRequest")
	assertDependencyFrameFields(t, startupRequest, map[protoreflect.Name]protoreflect.FieldNumber{
		"key_id": 1, "runtime_id": 2, "source": 3, "issued_at_unix_ms": 4,
		"nonce": 5, "dependency_error": 6, "actual_profile": 7, "signature": 8,
	})
	assertDependencyFrameMessageField(t, startupRequest, "dependency_error", 6, "strategy.v1.RuntimeDependencyError")
	assertDependencyFrameMessageField(t, startupRequest, "actual_profile", 7, "strategy.v1.RuntimeDependencyProfile")
	startupResponse := requireDependencyFrameMessage(t, file, "ReportRuntimeStartupFailureResponse")
	assertDependencyFrameFields(t, startupResponse, map[protoreflect.Name]protoreflect.FieldNumber{"recorded": 1})

	assertDependencyFrameMethod(t, file, "ValidateStrategySource", "strategy.v1.ValidateStrategySourceRequest", "strategy.v1.ValidateStrategySourceResponse")
	assertDependencyFrameMethod(t, file, "ReportRuntimeStartupFailure", "controlpanel.v1.ReportRuntimeStartupFailureRequest", "controlpanel.v1.ReportRuntimeStartupFailureResponse")

	dynamicRequest := dynamicpb.NewMessage(startupRequest)
	runtimeID := startupRequest.Fields().ByName("runtime_id")
	dynamicRequest.Set(runtimeID, protoreflect.ValueOfString("runtime-1"))
	if got := dynamicRequest.Get(runtimeID).String(); got != "runtime-1" {
		t.Fatalf("dynamic runtime_id = %q", got)
	}
}
