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

func TestRuntimeSessionCleanupMigrationIsDurableAndCredentialFree(t *testing.T) {
	raw, err := os.ReadFile("migrations/0002_runtime_session_cleanup_outbox.sql")
	if err != nil {
		t.Fatalf("read runtime Session cleanup migration: %v", err)
	}
	sql := strings.ToLower(string(raw))
	for _, required := range []string{
		"create table if not exists runtime_session_cleanup_outbox",
		"references runtime_registry(runtime_id) on delete cascade",
		"next_attempt_at",
		"attempt_count",
		"on conflict (runtime_id) do nothing",
	} {
		if !strings.Contains(sql, required) {
			t.Fatalf("runtime Session cleanup migration missing %q", required)
		}
	}
	for _, forbidden := range []string{"api_key", "api_secret", "credential_json", "token_hash"} {
		if strings.Contains(sql, forbidden) {
			t.Fatalf("runtime Session cleanup migration contains secret-bearing field %q", forbidden)
		}
	}
}
