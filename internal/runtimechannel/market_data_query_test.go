package runtimechannel

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lib/pq"
)

func TestDatabaseNameForYearRejectsFixedExchangeDatabase(t *testing.T) {
	if got, err := databaseNameForYear("{exchange}_{year}", "binance", 2026); err != nil || got != "binance_2026" {
		t.Fatalf("databaseNameForYear template = %q/%v, want binance_2026", got, err)
	}
	if _, err := databaseNameForYear("binance", "binance", 2026); err == nil || !strings.Contains(err.Error(), "fixed exchange database") {
		t.Fatalf("fixed db err = %v, want fixed exchange database rejection", err)
	}
	if _, err := databaseNameForYear("market_data", "binance", 2026); err == nil || !strings.Contains(err.Error(), "must include {year}") {
		t.Fatalf("missing year err = %v, want template rejection", err)
	}
}

func TestIsMissingMarketDataStorageError(t *testing.T) {
	for _, code := range []string{"3D000", "42P01"} {
		err := fmt.Errorf("query klines: %w", &pq.Error{Code: pq.ErrorCode(code)})
		if !isMissingMarketDataStorageError(err) {
			t.Fatalf("code %s not detected as missing market-data storage", code)
		}
	}

	err := fmt.Errorf("query klines: %w", &pq.Error{Code: pq.ErrorCode("08006")})
	if isMissingMarketDataStorageError(err) {
		t.Fatal("connection failure must not be treated as missing market-data storage")
	}
}

func TestIsTransientMarketDataQueryError(t *testing.T) {
	cases := []struct {
		err  error
		want bool
	}{
		{fmt.Errorf("query klines futures/ETHUSDT/1m: dial tcp 192.168.88.10:5432: connect: operation timed out"), true},
		{fmt.Errorf("query klines: pq: relation does not exist"), false},
		{fmt.Errorf("invalid symbol"), false},
	}
	for _, tc := range cases {
		if got := isTransientMarketDataQueryError(tc.err); got != tc.want {
			t.Fatalf("isTransientMarketDataQueryError(%q) = %v, want %v", tc.err, got, tc.want)
		}
	}
}

func TestYearsInRangeUsesEndExclusiveBoundary(t *testing.T) {
	start := time.Date(2026, 12, 31, 23, 59, 0, 0, time.UTC).UnixMilli()
	end := time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli()

	got := yearsInRange(start, end)
	if len(got) != 1 || got[0] != 2026 {
		t.Fatalf("yearsInRange = %#v, want [2026]", got)
	}
}

func TestMarketDataQueryFetchFundingUsesExactHalfOpenMultiYearExchangeRouting(t *testing.T) {
	state := &fundingSQLState{rowsByYear: map[int][][]driver.Value{
		2026: {{"BTCUSDT", time.Date(2026, 12, 31, 23, 30, 0, 0, time.UTC), "0.000100000000000001", "20000.123456789012345678"}},
		2027: {{"BTCUSDT", time.Date(2027, 1, 1, 0, 30, 0, 0, time.UTC), "-0.000200000000000002", "20001.000000000000000001"}},
	}}
	driverName := registerFundingSQLDriver(state)
	query := NewMarketDataQuery(MarketDataQueryConfig{
		Host: "market-data", Database: "{exchange}_{year}",
		OpenDB: func(_, dsn string) (*sql.DB, error) { return sql.Open(driverName, dsn) },
	})
	start := time.Date(2026, 12, 31, 23, 0, 0, 0, time.UTC)
	end := time.Date(2027, 1, 1, 2, 0, 0, 0, time.UTC)

	rows, err := query.FetchFunding(context.Background(), FundingQuery{
		Exchange: "okx", Market: "futures", Symbol: "btcusdt",
		StartTimeMS: start.UnixMilli(), EndTimeMS: end.UnixMilli(),
	})
	if err != nil {
		t.Fatalf("FetchFunding: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("Funding rows = %d, want 2: %#v", len(rows), rows)
	}
	if rows[0].Exchange != "okx" || rows[0].Market != "futures" || rows[0].Symbol != "BTCUSDT" ||
		rows[0].FundingTimeMS != time.Date(2026, 12, 31, 23, 30, 0, 0, time.UTC).UnixMilli() ||
		rows[0].FundingRateDecimal != "0.000100000000000001" || rows[0].MarkPriceDecimal != "20000.123456789012345678" {
		t.Fatalf("first exact Funding row = %#v", rows[0])
	}
	if rows[1].FundingRateDecimal != "-0.000200000000000002" || rows[1].MarkPriceDecimal != "20001.000000000000000001" {
		t.Fatalf("second exact Funding row = %#v", rows[1])
	}

	state.mu.Lock()
	defer state.mu.Unlock()
	if len(state.calls) != 2 || !strings.Contains(state.calls[0].dsn, "dbname=okx_2026") || !strings.Contains(state.calls[1].dsn, "dbname=okx_2027") {
		t.Fatalf("Funding database calls = %#v, want okx_2026 then okx_2027", state.calls)
	}
	for _, call := range state.calls {
		if len(call.args) < 3 || call.args[0].Value != "BTCUSDT" || call.args[1].Value != start.UnixMilli() || call.args[2].Value != end.UnixMilli() {
			t.Fatalf("Funding half-open query args = %#v, want symbol/start/end literals", call.args)
		}
		if !strings.Contains(call.query, "time >= to_timestamp($2/1000.0)") || !strings.Contains(call.query, "time < to_timestamp($3/1000.0)") {
			t.Fatalf("Funding query is not half-open: %s", call.query)
		}
	}
}

type fundingSQLCall struct {
	dsn   string
	query string
	args  []driver.NamedValue
}

type fundingSQLState struct {
	mu         sync.Mutex
	rowsByYear map[int][][]driver.Value
	calls      []fundingSQLCall
}

var fundingSQLDriverSequence atomic.Uint64

func registerFundingSQLDriver(state *fundingSQLState) string {
	name := fmt.Sprintf("funding_query_test_%d", fundingSQLDriverSequence.Add(1))
	sql.Register(name, fundingSQLDriver{state: state})
	return name
}

type fundingSQLDriver struct{ state *fundingSQLState }

func (d fundingSQLDriver) Open(dsn string) (driver.Conn, error) {
	year := 0
	for candidate := range d.state.rowsByYear {
		if strings.Contains(dsn, fmt.Sprintf("_%d", candidate)) {
			year = candidate
			break
		}
	}
	return &fundingSQLConn{state: d.state, dsn: dsn, year: year}, nil
}

type fundingSQLConn struct {
	state *fundingSQLState
	dsn   string
	year  int
}

func (*fundingSQLConn) Prepare(string) (driver.Stmt, error) {
	return nil, fmt.Errorf("Prepare not supported")
}
func (*fundingSQLConn) Close() error              { return nil }
func (*fundingSQLConn) Begin() (driver.Tx, error) { return nil, fmt.Errorf("Begin not supported") }
func (c *fundingSQLConn) QueryContext(_ context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	c.state.mu.Lock()
	c.state.calls = append(c.state.calls, fundingSQLCall{dsn: c.dsn, query: query, args: append([]driver.NamedValue(nil), args...)})
	rows := append([][]driver.Value(nil), c.state.rowsByYear[c.year]...)
	c.state.mu.Unlock()
	return &fundingSQLRows{rows: rows}, nil
}

type fundingSQLRows struct {
	rows [][]driver.Value
	idx  int
}

func (*fundingSQLRows) Columns() []string {
	return []string{"symbol", "time", "funding_rate", "mark_price"}
}
func (*fundingSQLRows) Close() error { return nil }
func (r *fundingSQLRows) Next(dest []driver.Value) error {
	if r.idx >= len(r.rows) {
		return io.EOF
	}
	copy(dest, r.rows[r.idx])
	r.idx++
	return nil
}
