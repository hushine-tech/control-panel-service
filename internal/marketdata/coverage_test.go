package marketdata

import (
	"context"
	"testing"
	"time"

	mdv1 "github.com/hushine-tech/control-panel-service/gen/marketdatav1"
	"github.com/hushine-tech/control-panel-service/internal/domain"
	"github.com/hushine-tech/control-panel-service/internal/runtimechannel"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func coverageKey(interval string) *mdv1.StreamKey {
	return &mdv1.StreamKey{
		Exchange: "binance",
		Market:   "futures",
		Kind:     "kline",
		Symbol:   "BTCUSDT",
		Interval: interval,
	}
}

func coverageSegment(start, end time.Time) *mdv1.MarketDataCoverageSegment {
	return &mdv1.MarketDataCoverageSegment{
		Key:      coverageKey("1d"),
		Year:     int32(start.UTC().Year()),
		StartAt:  timestamppb.New(start),
		EndAt:    timestamppb.New(end),
		RowCount: 1,
		Source:   "test",
	}
}

func mustReportCoverage(t *testing.T, svc *Service, segments ...*mdv1.MarketDataCoverageSegment) *mdv1.ReportMarketDataCoverageSegmentsResponse {
	t.Helper()
	resp, err := svc.ReportMarketDataCoverageSegments(context.Background(), &mdv1.ReportMarketDataCoverageSegmentsRequest{
		Segments: segments,
	})
	if err != nil {
		t.Fatalf("ReportMarketDataCoverageSegments: %v", err)
	}
	return resp
}

func assertInvalidArgument(t *testing.T, err error) {
	t.Helper()
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("code = %v, want InvalidArgument, err=%v", status.Code(err), err)
	}
}

func TestReportCoverageSegmentsMergesBridgeSegment(t *testing.T) {
	svc := newSvc()
	may1 := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	may2 := time.Date(2026, 5, 2, 0, 0, 0, 0, time.UTC)
	may3 := time.Date(2026, 5, 3, 0, 0, 0, 0, time.UTC)
	may4 := time.Date(2026, 5, 4, 0, 0, 0, 0, time.UTC)

	mustReportCoverage(t, svc, coverageSegment(may1, may2), coverageSegment(may3, may4))
	resp := mustReportCoverage(t, svc, coverageSegment(may2, may3))

	merged := resp.GetMergedSegments()
	if len(merged) != 1 {
		t.Fatalf("merged segments = %d, want 1", len(merged))
	}
	got := merged[0]
	if !got.GetStartAt().AsTime().Equal(may1) || !got.GetEndAt().AsTime().Equal(may4) {
		t.Fatalf("merged range = [%s, %s), want [%s, %s)",
			got.GetStartAt().AsTime(), got.GetEndAt().AsTime(), may1, may4)
	}
	if got.GetRowCount() != 3 {
		t.Fatalf("row_count = %d, want 3", got.GetRowCount())
	}

	query, err := svc.QueryMarketDataCoverage(context.Background(), &mdv1.QueryMarketDataCoverageRequest{
		Key:     coverageKey("1d"),
		StartAt: timestamppb.New(may1),
		EndAt:   timestamppb.New(may4),
	})
	if err != nil {
		t.Fatalf("QueryMarketDataCoverage: %v", err)
	}
	if len(query.GetCoveredSegments()) != 1 {
		t.Fatalf("covered segments = %d, want 1", len(query.GetCoveredSegments()))
	}
	if query.GetCoveredSegments()[0].GetRowCount() != 3 {
		t.Fatalf("query row_count = %d, want 3", query.GetCoveredSegments()[0].GetRowCount())
	}
}

func TestQueryCoverageRejectsNonGridAlignedWindow(t *testing.T) {
	svc := newSvc()
	start := time.Date(2026, 5, 1, 0, 0, 30, 0, time.UTC)
	end := time.Date(2026, 5, 1, 0, 2, 30, 0, time.UTC)

	_, err := svc.QueryMarketDataCoverage(context.Background(), &mdv1.QueryMarketDataCoverageRequest{
		Key:     coverageKey("1m"),
		StartAt: timestamppb.New(start),
		EndAt:   timestamppb.New(end),
	})
	assertInvalidArgument(t, err)
}

func TestQueryCoverageReturnsMiddleGap(t *testing.T) {
	svc := newSvc()
	may1 := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	may2 := time.Date(2026, 5, 2, 0, 0, 0, 0, time.UTC)
	may3 := time.Date(2026, 5, 3, 0, 0, 0, 0, time.UTC)
	may4 := time.Date(2026, 5, 4, 0, 0, 0, 0, time.UTC)

	mustReportCoverage(t, svc, coverageSegment(may1, may2), coverageSegment(may3, may4))

	resp, err := svc.QueryMarketDataCoverage(context.Background(), &mdv1.QueryMarketDataCoverageRequest{
		Key:     coverageKey("1d"),
		StartAt: timestamppb.New(may1),
		EndAt:   timestamppb.New(may4),
	})
	if err != nil {
		t.Fatalf("QueryMarketDataCoverage: %v", err)
	}
	if resp.GetComplete() {
		t.Fatal("complete = true, want false")
	}
	if resp.GetExpectedCount() != 3 {
		t.Fatalf("expected_count = %d, want 3", resp.GetExpectedCount())
	}
	if resp.GetCoveredCount() != 2 {
		t.Fatalf("covered_count = %d, want 2", resp.GetCoveredCount())
	}
	missing := resp.GetMissingSegments()
	if len(missing) != 1 {
		t.Fatalf("missing segments = %d, want 1", len(missing))
	}
	if !missing[0].GetStartAt().AsTime().Equal(may2) || !missing[0].GetEndAt().AsTime().Equal(may3) {
		t.Fatalf("missing range = [%s, %s), want [%s, %s)",
			missing[0].GetStartAt().AsTime(), missing[0].GetEndAt().AsTime(), may2, may3)
	}
	if missing[0].GetExpectedCount() != 1 {
		t.Fatalf("missing expected_count = %d, want 1", missing[0].GetExpectedCount())
	}
}

func TestQueryCoverageUsesRawKlineRowsWhenIndexIsMissing(t *testing.T) {
	query := &stubKlineQuerier{
		rows: []runtimechannel.KlineRow{
			{OpenTime: 60000},
			{OpenTime: 120000},
			{OpenTime: 240000},
		},
	}
	svc := NewService(newStubRepo(), WithMarketDataQuery(query))

	resp, err := svc.QueryMarketDataCoverage(context.Background(), &mdv1.QueryMarketDataCoverageRequest{
		Key:     coverageKey("1m"),
		StartAt: timestamppb.New(time.UnixMilli(60000)),
		EndAt:   timestamppb.New(time.UnixMilli(300000)),
	})
	if err != nil {
		t.Fatalf("QueryMarketDataCoverage: %v", err)
	}
	if len(query.calls) != 1 {
		t.Fatalf("FetchKlines calls = %d, want 1", len(query.calls))
	}
	if resp.GetComplete() {
		t.Fatal("complete = true, want false")
	}
	if resp.GetExpectedCount() != 4 || resp.GetCoveredCount() != 3 {
		t.Fatalf("counts = expected:%d covered:%d, want expected:4 covered:3", resp.GetExpectedCount(), resp.GetCoveredCount())
	}
	covered := resp.GetCoveredSegments()
	if len(covered) != 2 {
		t.Fatalf("covered segments = %d, want 2", len(covered))
	}
	if covered[0].GetStartAt().AsTime().UnixMilli() != 60000 ||
		covered[0].GetEndAt().AsTime().UnixMilli() != 180000 ||
		covered[0].GetRowCount() != 2 ||
		covered[0].GetSource() != "raw_storage" {
		t.Fatalf("first covered segment = %#v, want [60000,180000) row_count=2 source=raw_storage", covered[0])
	}
	if covered[1].GetStartAt().AsTime().UnixMilli() != 240000 ||
		covered[1].GetEndAt().AsTime().UnixMilli() != 300000 ||
		covered[1].GetRowCount() != 1 ||
		covered[1].GetSource() != "raw_storage" {
		t.Fatalf("second covered segment = %#v, want [240000,300000) row_count=1 source=raw_storage", covered[1])
	}
	missing := resp.GetMissingSegments()
	if len(missing) != 1 {
		t.Fatalf("missing segments = %d, want 1", len(missing))
	}
	if missing[0].GetStartAt().AsTime().UnixMilli() != 180000 ||
		missing[0].GetEndAt().AsTime().UnixMilli() != 240000 ||
		missing[0].GetExpectedCount() != 1 {
		t.Fatalf("missing segment = %#v, want [180000,240000) expected=1", missing[0])
	}
}

func TestReportCoverageSegmentsRejectsWrongYear(t *testing.T) {
	svc := newSvc()
	may1 := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	may2 := time.Date(2026, 5, 2, 0, 0, 0, 0, time.UTC)
	seg := coverageSegment(may1, may2)
	seg.Year = 2025

	_, err := svc.ReportMarketDataCoverageSegments(context.Background(), &mdv1.ReportMarketDataCoverageSegmentsRequest{
		Segments: []*mdv1.MarketDataCoverageSegment{seg},
	})
	assertInvalidArgument(t, err)
}

func TestReportCoverageSegmentsRejectsRangeBeyondYearBoundary(t *testing.T) {
	svc := newSvc()
	start := time.Date(2026, 12, 31, 0, 0, 0, 0, time.UTC)
	end := time.Date(2027, 1, 2, 0, 0, 0, 0, time.UTC)
	seg := coverageSegment(start, end)
	seg.RowCount = 2

	_, err := svc.ReportMarketDataCoverageSegments(context.Background(), &mdv1.ReportMarketDataCoverageSegmentsRequest{
		Segments: []*mdv1.MarketDataCoverageSegment{seg},
	})
	assertInvalidArgument(t, err)
}

func TestReportCoverageSegmentsRejectsRowCountMismatch(t *testing.T) {
	svc := newSvc()
	may1 := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	may3 := time.Date(2026, 5, 3, 0, 0, 0, 0, time.UTC)
	seg := coverageSegment(may1, may3)
	seg.RowCount = 1

	_, err := svc.ReportMarketDataCoverageSegments(context.Background(), &mdv1.ReportMarketDataCoverageSegmentsRequest{
		Segments: []*mdv1.MarketDataCoverageSegment{seg},
	})
	assertInvalidArgument(t, err)
}

func TestQueryCoverageCoveredCountUsesUnionForOverlaps(t *testing.T) {
	repo := newStubRepo()
	svc := NewService(repo)
	key := domain.StreamKey{
		Exchange: "binance",
		Market:   "futures",
		Kind:     "kline",
		Symbol:   "BTCUSDT",
		Interval: "1d",
	}
	may1 := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	may2 := time.Date(2026, 5, 2, 0, 0, 0, 0, time.UTC)
	may3 := time.Date(2026, 5, 3, 0, 0, 0, 0, time.UTC)
	may4 := time.Date(2026, 5, 4, 0, 0, 0, 0, time.UTC)

	repo.coverage[1] = domain.MarketDataCoverageSegment{
		SegmentID: 1,
		Key:       key,
		Year:      2026,
		StartAt:   may1,
		EndAt:     may3,
		RowCount:  2,
		Source:    "legacy",
	}
	repo.coverage[2] = domain.MarketDataCoverageSegment{
		SegmentID: 2,
		Key:       key,
		Year:      2026,
		StartAt:   may2,
		EndAt:     may4,
		RowCount:  2,
		Source:    "manual",
	}

	resp, err := svc.QueryMarketDataCoverage(context.Background(), &mdv1.QueryMarketDataCoverageRequest{
		Key:     coverageKey("1d"),
		StartAt: timestamppb.New(may1),
		EndAt:   timestamppb.New(may4),
	})
	if err != nil {
		t.Fatalf("QueryMarketDataCoverage: %v", err)
	}
	if resp.GetExpectedCount() != 3 {
		t.Fatalf("expected_count = %d, want 3", resp.GetExpectedCount())
	}
	if !resp.GetComplete() {
		t.Fatal("complete = false, want true")
	}
	if resp.GetCoveredCount() != 3 {
		t.Fatalf("covered_count = %d, want 3", resp.GetCoveredCount())
	}
}

func TestReportCoverageSegmentsExactRetryIsIdempotent(t *testing.T) {
	svc := newSvc()
	may1 := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	may2 := time.Date(2026, 5, 2, 0, 0, 0, 0, time.UTC)

	mustReportCoverage(t, svc, coverageSegment(may1, may2))
	resp := mustReportCoverage(t, svc, coverageSegment(may1, may2))

	if len(resp.GetMergedSegments()) != 1 {
		t.Fatalf("merged segments = %d, want 1", len(resp.GetMergedSegments()))
	}
	query, err := svc.QueryMarketDataCoverage(context.Background(), &mdv1.QueryMarketDataCoverageRequest{
		Key:     coverageKey("1d"),
		StartAt: timestamppb.New(may1),
		EndAt:   timestamppb.New(may2),
	})
	if err != nil {
		t.Fatalf("QueryMarketDataCoverage: %v", err)
	}
	if len(query.GetCoveredSegments()) != 1 {
		t.Fatalf("covered segments = %d, want 1", len(query.GetCoveredSegments()))
	}
}
