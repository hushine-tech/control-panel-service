package runtimechannel

import (
	"strings"
	"testing"
)

func TestValidateKlineRowsFindsGap(t *testing.T) {
	rows := []KlineRow{
		{OpenTime: 1000},
		{OpenTime: 61000},
		{OpenTime: 181000},
	}

	got := ValidateKlineRows("1m", 1000, 241000, rows)

	if got.OK {
		t.Fatalf("expected validation to find a gap")
	}
	if got.ExpectedCount != 4 {
		t.Fatalf("ExpectedCount = %d, want 4", got.ExpectedCount)
	}
	if got.ActualCount != 3 {
		t.Fatalf("ActualCount = %d, want 3", got.ActualCount)
	}
	if len(got.MissingGaps) != 1 {
		t.Fatalf("MissingGaps len = %d, want 1: %#v", len(got.MissingGaps), got.MissingGaps)
	}
	gap := got.MissingGaps[0]
	if gap.StartMS != 121000 || gap.EndMS != 181000 || gap.ExpectedCount != 1 {
		t.Fatalf("gap = %#v, want start=121000 end=181000 expected=1", gap)
	}
}

func TestValidateKlineRowsAcceptsCompleteWindow(t *testing.T) {
	rows := []KlineRow{
		{OpenTime: 1000},
		{OpenTime: 61000},
		{OpenTime: 121000},
	}

	got := ValidateKlineRows("1m", 1000, 181000, rows)

	if !got.OK {
		t.Fatalf("expected validation OK, got reason=%q gaps=%#v", got.Reason, got.MissingGaps)
	}
	if got.ExpectedCount != 3 {
		t.Fatalf("ExpectedCount = %d, want 3", got.ExpectedCount)
	}
	if got.ActualCount != 3 {
		t.Fatalf("ActualCount = %d, want 3", got.ActualCount)
	}
	if len(got.MissingGaps) != 0 {
		t.Fatalf("MissingGaps len = %d, want 0: %#v", len(got.MissingGaps), got.MissingGaps)
	}
}

func TestValidateKlineRowsDeduplicatesAndSortsOpenTimes(t *testing.T) {
	rows := []KlineRow{
		{OpenTime: 181000},
		{OpenTime: 1000},
		{OpenTime: 1000},
		{OpenTime: 61000},
	}

	got := ValidateKlineRows("1m", 1000, 241000, rows)

	if got.OK {
		t.Fatalf("expected duplicate/out-of-order rows not to hide the missing bar")
	}
	if got.ExpectedCount != 4 {
		t.Fatalf("ExpectedCount = %d, want 4", got.ExpectedCount)
	}
	if got.ActualCount != 3 {
		t.Fatalf("ActualCount = %d, want 3 unique in-window rows", got.ActualCount)
	}
	if len(got.MissingGaps) != 1 {
		t.Fatalf("MissingGaps len = %d, want 1: %#v", len(got.MissingGaps), got.MissingGaps)
	}
	gap := got.MissingGaps[0]
	if gap.StartMS != 121000 || gap.EndMS != 181000 || gap.ExpectedCount != 1 {
		t.Fatalf("gap = %#v, want start=121000 end=181000 expected=1", gap)
	}
}

func TestValidateKlineRowsRejectsNonAlignedRange(t *testing.T) {
	got := ValidateKlineRows("1m", 1001, 181000, []KlineRow{
		{OpenTime: 1000},
		{OpenTime: 61000},
		{OpenTime: 121000},
	})

	if got.OK {
		t.Fatalf("expected non-aligned range to fail validation")
	}
	if !strings.Contains(strings.ToLower(got.Reason), "align") {
		t.Fatalf("Reason = %q, want explicit alignment reason", got.Reason)
	}
}
