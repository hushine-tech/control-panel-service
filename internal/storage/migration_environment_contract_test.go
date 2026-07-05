package storage_test

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

func TestSessionDeliveryMigrationsUseEnvironmentNotLegacyPortfolioRouting(t *testing.T) {
	files := []string{
		"migrations/0017_create_runtime_data_delivery_leases.sql",
		"migrations/0022_create_stream_delivery_failures.sql",
	}
	legacyPortfolioRoutingColumn := regexp.MustCompile(`(?i)\b[m]ode\b`)
	for _, path := range files {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		if legacyPortfolioRoutingColumn.Match(raw) {
			t.Fatalf("%s must use environment, not the legacy portfolio-routing column", path)
		}
	}
}

func TestPortfolioHardCutMigrationBackfillsAppliedControlPanelSchemas(t *testing.T) {
	raw, err := os.ReadFile("migrations/0031_rename_account_columns_to_portfolio.sql")
	if err != nil {
		t.Fatalf("read portfolio hard-cut compatibility migration: %v", err)
	}
	sql := strings.ToLower(string(raw))
	for _, required := range []string{
		"market_data_requests",
		"market_data_leases",
		"market_data_history_requests",
		"runtime_debug_datasets",
		"rename column account_id to portfolio_id",
	} {
		if !strings.Contains(sql, required) {
			t.Fatalf("compatibility migration must include %q", required)
		}
	}
}
