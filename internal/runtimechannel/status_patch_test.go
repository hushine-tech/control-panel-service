package runtimechannel

import "testing"

func TestStatusPatchRejectsRemovedCompletedSessionStatus(t *testing.T) {
	if statusPatchSessionStatus("completed") {
		t.Fatal("removed completed status is still accepted")
	}
}
