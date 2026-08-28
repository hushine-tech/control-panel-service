package runtimechannel

import (
	"testing"

	cpv1 "github.com/hushine-tech/control-panel-service/gen/controlpanelv1"
	portfoliov1 "github.com/hushine-tech/core-service/gen/portfoliov1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

func TestStatusPatchRejectsRemovedCompletedSessionStatus(t *testing.T) {
	if statusPatchSessionStatus("completed") {
		t.Fatal("removed completed status is still accepted")
	}
}

func TestStatusPatchCopiesCompleteEmbeddedSessionUpdateBeforeRuntimeBinding(t *testing.T) {
	embedded := &portfoliov1.UpdateSessionRequest{
		SessionId: "sess-typed", Status: "failed", BarsProcessed: 17,
		Error: "legacy display text", RuntimeId: "spoofed-runtime",
		IndicatorFinalizationPending: proto.Bool(true), ExpectedStatus: "running",
		ErrorCode: "ORDER_REQUEST_REJECTED", ErrorMessage: "order request was rejected",
		ErrorDetailJson: `{"venue_id":17}`,
	}
	payload, err := anypb.New(embedded)
	if err != nil {
		t.Fatal(err)
	}
	got := statusPatchUpdateSessionRequest("authenticated-runtime", &cpv1.RuntimeStatusPatch{
		SessionId: "outer-session", Status: "failed", Reason: "outer reason", Payload: payload,
	}, "failed")

	if got.GetSessionId() != embedded.GetSessionId() || got.GetStatus() != embedded.GetStatus() ||
		got.GetBarsProcessed() != embedded.GetBarsProcessed() || got.GetError() != embedded.GetError() ||
		got.GetRuntimeId() != "authenticated-runtime" ||
		got.IndicatorFinalizationPending == nil || !got.GetIndicatorFinalizationPending() ||
		got.GetExpectedStatus() != embedded.GetExpectedStatus() ||
		got.GetErrorCode() != embedded.GetErrorCode() ||
		got.GetErrorMessage() != embedded.GetErrorMessage() ||
		got.GetErrorDetailJson() != embedded.GetErrorDetailJson() {
		t.Fatalf("status patch update = %+v", got)
	}
}
