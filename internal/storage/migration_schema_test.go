package storage_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	_ "github.com/lib/pq"
)

func TestControlPanelMigrationsExposeRuntimeIdentitySchema(t *testing.T) {
	dsn := os.Getenv("CONTROL_PANEL_TEST_DSN")
	if dsn == "" {
		t.Skip("CONTROL_PANEL_TEST_DSN is required for migration schema tests")
	}

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("open postgres: %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		t.Skipf("postgres unavailable for migration schema tests: %v", err)
	}

	schema := fmt.Sprintf("migration_runtime_identity_%d", time.Now().UnixNano())
	if _, err := db.ExecContext(ctx, `CREATE SCHEMA `+quoteIdent(schema)); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	defer func() {
		_, _ = db.ExecContext(context.Background(), `DROP SCHEMA `+quoteIdent(schema)+` CASCADE`)
	}()
	if _, err := db.ExecContext(ctx, `SET search_path TO `+quoteIdent(schema)); err != nil {
		t.Fatalf("set search_path: %v", err)
	}

	migrationsDir := filepath.Join(repoRoot(t), "internal", "storage", "migrations")
	files := migrationFiles(t, migrationsDir)
	applyMigrationFiles(ctx, t, db, files, "fresh")
	applyMigrationFiles(ctx, t, db, filesWithPrefixAtLeast(files, "0014_"), "new-schema-idempotency")
	assertRuntimeIdentitySchema(ctx, t, db, schema)
}

func applyMigrationFiles(ctx context.Context, t *testing.T, db *sql.DB, files []string, label string) {
	t.Helper()
	for _, m := range files {
		body, err := os.ReadFile(m)
		if err != nil {
			t.Fatalf("read %s: %v", m, err)
		}
		sqlText := strings.TrimSpace(string(body))
		if sqlText == "" {
			continue
		}
		if _, err := db.ExecContext(ctx, sqlText); err != nil {
			t.Fatalf("exec %s during %s: %v", filepath.Base(m), label, err)
		}
	}
}

func filesWithPrefixAtLeast(files []string, minPrefix string) []string {
	out := make([]string, 0, len(files))
	for _, f := range files {
		if filepath.Base(f) >= minPrefix {
			out = append(out, f)
		}
	}
	return out
}

func assertRuntimeIdentitySchema(ctx context.Context, t *testing.T, db *sql.DB, schema string) {
	t.Helper()

	requiredRuntimeColumns := []string{
		"runtime_id",
		"user_id",
		"source",
		"role",
		"name",
		"status",
		"started_at",
		"ended_at",
		"ended_reason",
		"cleanup_status",
		"cleanup_reason",
		"cleanup_at",
		"heartbeat_at",
		"last_heartbeat_at",
		"connection_owner_instance_id",
		"connection_owner_acquired_at",
		"connection_owner_heartbeat_at",
	}
	for _, column := range requiredRuntimeColumns {
		if !columnExists(ctx, t, db, schema, "runtime_registry", column) {
			t.Fatalf("runtime_registry.%s is missing after migrations", column)
		}
	}

	indexDef := indexDefinition(ctx, t, db, schema, "uq_runtime_registry_user_name")
	if !strings.Contains(indexDef, "UNIQUE INDEX") ||
		!strings.Contains(indexDef, "runtime_registry USING btree (user_id, name)") ||
		strings.Contains(indexDef, "status") {
		t.Fatalf("uq_runtime_registry_user_name must enforce permanent per-user names, got: %s", indexDef)
	}
	statusConstraint := constraintDefinition(ctx, t, db, schema, "runtime_registry", "ck_runtime_registry_status")
	for _, status := range []string{"starting", "active", "unhealthy", "heartbeat_stale", "cancelled", "failed", "ended"} {
		if !strings.Contains(statusConstraint, status) {
			t.Fatalf("ck_runtime_registry_status missing %q in %s", status, statusConstraint)
		}
	}

	requiredCredentialColumns := []string{
		"role",
		"status",
		"downloaded_at",
		"consumed_at",
		"consumed_runtime_id",
		"expires_at",
		"revoked_at",
		"hosted_internal",
	}
	for _, column := range requiredCredentialColumns {
		if !columnExists(ctx, t, db, schema, "runtime_credentials", column) {
			t.Fatalf("runtime_credentials.%s is missing after migrations", column)
		}
	}

	requiredCommandColumns := []string{
		"command_id",
		"runtime_id",
		"session_id",
		"idempotency_key",
		"command_type",
		"status",
		"deadline_at",
		"acked_at",
		"completed_at",
		"payload",
		"result",
		"failure_reason",
	}
	for _, column := range requiredCommandColumns {
		if !columnExists(ctx, t, db, schema, "runtime_commands", column) {
			t.Fatalf("runtime_commands.%s is missing after migrations", column)
		}
	}

	requiredSubscriptionColumns := []string{
		"subscription_id",
		"session_id",
		"runtime_id",
		"market",
		"symbol",
		"interval",
		"mode",
		"status",
	}
	for _, column := range requiredSubscriptionColumns {
		if !columnExists(ctx, t, db, schema, "session_market_data_subscriptions", column) {
			t.Fatalf("session_market_data_subscriptions.%s is missing after migrations", column)
		}
	}

	requiredDeliveryLeaseColumns := []string{
		"lease_id",
		"subscription_id",
		"owner_instance_id",
		"status",
		"last_heartbeat_at",
		"expires_at",
		"last_delivery_at",
		"last_topic",
		"last_partition",
		"last_offset",
	}
	for _, column := range requiredDeliveryLeaseColumns {
		if !columnExists(ctx, t, db, schema, "stream_delivery_leases", column) {
			t.Fatalf("stream_delivery_leases.%s is missing after migrations", column)
		}
	}

	requiredWriterLeaseColumns := []string{
		"lease_id",
		"exchange",
		"market",
		"kind",
		"symbol",
		"interval",
		"year",
		"owner_instance_id",
		"collector_id",
		"status",
		"last_heartbeat_at",
		"expires_at",
	}
	for _, column := range requiredWriterLeaseColumns {
		if !columnExists(ctx, t, db, schema, "market_data_writer_leases", column) {
			t.Fatalf("market_data_writer_leases.%s is missing after migrations", column)
		}
	}
}

func repoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot resolve caller path")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}

func migrationFiles(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read migrations dir: %v", err)
	}
	files := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		files = append(files, filepath.Join(dir, entry.Name()))
	}
	sort.Strings(files)
	return files
}

func columnExists(ctx context.Context, t *testing.T, db *sql.DB, schema, table, column string) bool {
	t.Helper()
	var exists bool
	err := db.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM information_schema.columns
			WHERE table_schema = $1
			  AND table_name = $2
			  AND column_name = $3
			)`, schema, table, column).Scan(&exists)
	if err != nil {
		t.Fatalf("check column %s.%s: %v", table, column, err)
	}
	return exists
}

func indexDefinition(ctx context.Context, t *testing.T, db *sql.DB, schema, indexName string) string {
	t.Helper()
	var def string
	err := db.QueryRowContext(ctx, `
		SELECT indexdef
		FROM pg_indexes
		WHERE schemaname = $1
		  AND indexname = $2`, schema, indexName).Scan(&def)
	if err != nil {
		t.Fatalf("read index %s: %v", indexName, err)
	}
	return def
}

func constraintDefinition(ctx context.Context, t *testing.T, db *sql.DB, schema, table, constraintName string) string {
	t.Helper()
	var def string
	err := db.QueryRowContext(ctx, `
		SELECT pg_get_constraintdef(c.oid)
		FROM pg_constraint c
		JOIN pg_class tbl ON tbl.oid = c.conrelid
		JOIN pg_namespace n ON n.oid = tbl.relnamespace
		WHERE n.nspname = $1
		  AND tbl.relname = $2
		  AND c.conname = $3`, schema, table, constraintName).Scan(&def)
	if err != nil {
		t.Fatalf("read constraint %s: %v", constraintName, err)
	}
	return def
}

func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}
