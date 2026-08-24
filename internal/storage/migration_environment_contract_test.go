package storage_test

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

func TestCurrentControlPanelBaselineContainsFinalContracts(t *testing.T) {
	raw, err := os.ReadFile("migrations/0001_current_schema_baseline.sql")
	if err != nil {
		t.Fatalf("read current schema baseline: %v", err)
	}
	sql := strings.ToLower(string(raw))

	for _, forbidden := range []string{
		"rename column",
		"drop table",
		"drop column",
		"account_id",
		"runtime_pairings",
	} {
		if strings.Contains(sql, forbidden) {
			t.Fatalf("current baseline contains historical operation/name %q", forbidden)
		}
	}
	legacyPortfolioRoutingColumn := regexp.MustCompile(`(?i)\bmode\b`)
	if legacyPortfolioRoutingColumn.MatchString(sql) {
		t.Fatal("current baseline must use environment, not the legacy portfolio-routing column")
	}

	for _, required := range []string{
		"create table if not exists runtime_registry",
		"create table if not exists runtime_credentials",
		"create table if not exists runtime_commands",
		"create table if not exists runtime_channel_leases",
		"create table if not exists runtime_admission_failures",
		"create table if not exists market_data_streams",
		"create table if not exists market_data_requests",
		"create table if not exists market_data_history_requests",
		"create table if not exists market_data_leases",
		"create table if not exists market_data_writer_leases",
		"create table if not exists market_data_coverage_segments",
		"create table if not exists session_market_data_subscriptions",
		"create table if not exists stream_delivery_leases",
		"create table if not exists stream_delivery_failures",
		"create table if not exists runtime_debug_datasets",
		"create table if not exists runtime_session_cleanup_outbox",
		"references runtime_registry(runtime_id) on delete cascade",
		"idx_runtime_session_cleanup_outbox_due",
		"array['hosted'::text, 'self_hosted'::text, 'bare'::text]",
		"array['executor'::text, 'debugger'::text]",
		"client_cert_fingerprint",
		"connection_owner_instance_id",
		"uq_runtime_registry_user_name",
		"environment integer not null",
	} {
		if !strings.Contains(sql, required) {
			t.Fatalf("current baseline missing final control contract %q", required)
		}
	}
}

func TestCurrentBaselineCleanupOutboxIsDurableCredentialFreeAndHasNoBackfill(t *testing.T) {
	raw, err := os.ReadFile("migrations/0001_current_schema_baseline.sql")
	if err != nil {
		t.Fatalf("read current schema baseline: %v", err)
	}
	sql := strings.ToLower(string(raw))
	start := strings.Index(sql, "create table if not exists runtime_session_cleanup_outbox")
	if start < 0 {
		t.Fatal("current baseline cleanup outbox table is missing")
	}
	end := strings.Index(sql[start:], "\n);")
	if end < 0 {
		t.Fatal("current baseline cleanup outbox table definition is incomplete")
	}
	outboxSQL := sql[start : start+end+3]
	for _, required := range []string{
		"create table if not exists runtime_session_cleanup_outbox",
		"references runtime_registry(runtime_id) on delete cascade",
		"next_attempt_at",
		"attempt_count",
	} {
		if !strings.Contains(outboxSQL, required) {
			t.Fatalf("current baseline cleanup outbox missing %q", required)
		}
	}
	for _, forbidden := range []string{
		"api_key",
		"api_secret",
		"credential_json",
		"token_hash",
	} {
		if strings.Contains(outboxSQL, forbidden) {
			t.Fatalf("current baseline cleanup outbox contains obsolete or secret-bearing SQL %q", forbidden)
		}
	}
	if strings.Contains(sql, "insert into runtime_session_cleanup_outbox") {
		t.Fatal("current baseline must not backfill the cleanup outbox")
	}
}
