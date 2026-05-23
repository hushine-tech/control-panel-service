package runtimechannel

import (
	"fmt"
	"strings"
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

func TestYearsInRangeUsesEndExclusiveBoundary(t *testing.T) {
	start := time.Date(2026, 12, 31, 23, 59, 0, 0, time.UTC).UnixMilli()
	end := time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli()

	got := yearsInRange(start, end)
	if len(got) != 1 || got[0] != 2026 {
		t.Fatalf("yearsInRange = %#v, want [2026]", got)
	}
}
