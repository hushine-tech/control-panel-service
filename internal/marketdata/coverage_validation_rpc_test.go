package marketdata

import (
	"context"
	"errors"
	"testing"
	"time"

	mdv1 "github.com/hushine-tech/control-panel-service/gen/marketdatav1"
	"github.com/hushine-tech/control-panel-service/internal/runtimechannel"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type stubKlineQuerier struct {
	calls []runtimechannel.KlineQuery
	rows  []runtimechannel.KlineRow
	err   error
	fetch func(runtimechannel.KlineQuery) ([]runtimechannel.KlineRow, error)
}

func (q *stubKlineQuerier) FetchKlines(_ context.Context, req runtimechannel.KlineQuery) ([]runtimechannel.KlineRow, error) {
	q.calls = append(q.calls, req)
	if q.fetch != nil {
		return q.fetch(req)
	}
	if q.err != nil {
		return nil, q.err
	}
	return q.rows, nil
}

func TestValidateCoverageRequiresMarketDataQuery(t *testing.T) {
	svc := newSvc()

	_, err := svc.ValidateMarketDataCoverage(context.Background(), &mdv1.ValidateMarketDataCoverageRequest{
		Key:     coverageKey("1m"),
		StartAt: timestamppb.New(time.UnixMilli(60000)),
		EndAt:   timestamppb.New(time.UnixMilli(180000)),
	})

	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("code = %v, want FailedPrecondition, err=%v", status.Code(err), err)
	}
}

func TestValidateCoverageFetchesRawKlinesEndExclusive(t *testing.T) {
	query := &stubKlineQuerier{
		rows: []runtimechannel.KlineRow{
			{OpenTime: 60000},
			{OpenTime: 120000},
			{OpenTime: 240000},
		},
	}
	svc := NewService(newStubRepo(), WithMarketDataQuery(query))

	resp, err := svc.ValidateMarketDataCoverage(context.Background(), &mdv1.ValidateMarketDataCoverageRequest{
		Key:     coverageKey("1m"),
		StartAt: timestamppb.New(time.UnixMilli(60000)),
		EndAt:   timestamppb.New(time.UnixMilli(300000)),
	})
	if err != nil {
		t.Fatalf("ValidateMarketDataCoverage: %v", err)
	}
	if len(query.calls) != 1 {
		t.Fatalf("FetchKlines calls = %d, want 1", len(query.calls))
	}
	call := query.calls[0]
	if call.StartTimeMS != 60000 || call.EndTimeMS != 300000 || call.Limit != 5000 {
		t.Fatalf("FetchKlines request = %#v, want start=60000 end=300000 limit=5000", call)
	}
	if resp.GetOk() {
		t.Fatal("ok = true, want false")
	}
	if resp.GetExpectedCount() != 4 || resp.GetActualCount() != 3 {
		t.Fatalf("counts = expected:%d actual:%d, want expected:4 actual:3", resp.GetExpectedCount(), resp.GetActualCount())
	}
	if len(resp.GetMissingSegments()) != 1 {
		t.Fatalf("missing segments = %d, want 1", len(resp.GetMissingSegments()))
	}
	missing := resp.GetMissingSegments()[0]
	if missing.GetStartAt().AsTime().UnixMilli() != 180000 || missing.GetEndAt().AsTime().UnixMilli() != 240000 || missing.GetExpectedCount() != 1 {
		t.Fatalf("missing segment = %#v, want [180000,240000) expected=1", missing)
	}
}

func TestValidateCoverageFetchesRawKlinesInChunks(t *testing.T) {
	query := &stubKlineQuerier{}
	query.fetch = func(req runtimechannel.KlineQuery) ([]runtimechannel.KlineRow, error) {
		switch req.StartTimeMS {
		case 60000:
			return []runtimechannel.KlineRow{{OpenTime: 120000}, {OpenTime: 60000}}, nil
		case 180000:
			return []runtimechannel.KlineRow{{OpenTime: 180000}, {OpenTime: 240000}}, nil
		default:
			return nil, nil
		}
	}
	svc := NewService(newStubRepo(), WithMarketDataQuery(query))

	resp, err := svc.ValidateMarketDataCoverage(context.Background(), &mdv1.ValidateMarketDataCoverageRequest{
		Key:     coverageKey("1m"),
		StartAt: timestamppb.New(time.UnixMilli(60000)),
		EndAt:   timestamppb.New(time.UnixMilli(300000)),
	})
	if err != nil {
		t.Fatalf("ValidateMarketDataCoverage: %v", err)
	}
	if !resp.GetOk() {
		t.Fatalf("ok = false, want true: reason=%q missing=%#v", resp.GetReason(), resp.GetMissingSegments())
	}
	if len(query.calls) != 2 {
		t.Fatalf("FetchKlines calls = %d, want 2", len(query.calls))
	}
	if query.calls[0].StartTimeMS != 60000 || query.calls[1].StartTimeMS != 180000 {
		t.Fatalf("FetchKlines starts = [%d, %d], want [60000, 180000]", query.calls[0].StartTimeMS, query.calls[1].StartTimeMS)
	}
	for _, call := range query.calls {
		if call.EndTimeMS != 300000 || call.Limit != 5000 {
			t.Fatalf("FetchKlines request = %#v, want end=300000 limit=5000", call)
		}
	}
}

func TestValidateCoverageMapsRawFetchErrorToUnavailable(t *testing.T) {
	query := &stubKlineQuerier{err: errors.New("market data db is temporarily unavailable")}
	svc := NewService(newStubRepo(), WithMarketDataQuery(query))

	_, err := svc.ValidateMarketDataCoverage(context.Background(), &mdv1.ValidateMarketDataCoverageRequest{
		Key:     coverageKey("1m"),
		StartAt: timestamppb.New(time.UnixMilli(60000)),
		EndAt:   timestamppb.New(time.UnixMilli(180000)),
	})

	if status.Code(err) != codes.Unavailable {
		t.Fatalf("code = %v, want Unavailable, err=%v", status.Code(err), err)
	}
}

func TestQueryMarketDataKlinesFetchesRawRows(t *testing.T) {
	query := &stubKlineQuerier{
		rows: []runtimechannel.KlineRow{
			{
				Exchange:  "binance",
				Market:    "futures",
				Symbol:    "BTCUSDT",
				Interval:  "1m",
				OpenTime:  60000,
				CloseTime: 119999,
				Open:      100.1,
				High:      101.2,
				Low:       99.3,
				Close:     100.7,
				Volume:    12.5,
				Timestamp: 119999,
			},
		},
	}
	svc := NewService(newStubRepo(), WithMarketDataQuery(query))

	resp, err := svc.QueryMarketDataKlines(context.Background(), &mdv1.QueryMarketDataKlinesRequest{
		Key:     coverageKey("1m"),
		StartAt: timestamppb.New(time.UnixMilli(60000)),
		EndAt:   timestamppb.New(time.UnixMilli(180000)),
		Limit:   50,
	})
	if err != nil {
		t.Fatalf("QueryMarketDataKlines: %v", err)
	}
	if len(query.calls) != 1 {
		t.Fatalf("FetchKlines calls = %d, want 1", len(query.calls))
	}
	call := query.calls[0]
	if call.StartTimeMS != 60000 || call.EndTimeMS != 180000 || call.Limit != 51 {
		t.Fatalf("FetchKlines request = %#v, want start=60000 end=180000 limit=51", call)
	}
	if resp.GetLimit() != 50 {
		t.Fatalf("response limit = %d, want 50", resp.GetLimit())
	}
	if resp.GetRowCount() != 1 || len(resp.GetRows()) != 1 {
		t.Fatalf("rows = count:%d len:%d, want 1", resp.GetRowCount(), len(resp.GetRows()))
	}
	row := resp.GetRows()[0]
	if row.GetOpenTime().AsTime().UnixMilli() != 60000 ||
		row.GetCloseTime().AsTime().UnixMilli() != 119999 ||
		row.GetOpen() != 100.1 ||
		row.GetHigh() != 101.2 ||
		row.GetLow() != 99.3 ||
		row.GetClose() != 100.7 ||
		row.GetVolume() != 12.5 {
		t.Fatalf("row = %#v, want raw kline values preserved", row)
	}
}
