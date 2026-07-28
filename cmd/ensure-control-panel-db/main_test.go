package main

import (
	"database/sql"
	"errors"
	"strings"
	"testing"
)

func TestConfiguredControlPanelDatabaseName(t *testing.T) {
	t.Setenv("PGDATABASE_CONTROL_PANEL", "hushine_indicator_chain_abc_control")
	got, err := configuredControlPanelDatabaseName()
	if err != nil {
		t.Fatalf("configuredControlPanelDatabaseName: %v", err)
	}
	if got != "hushine_indicator_chain_abc_control" {
		t.Fatalf("database name = %q", got)
	}
}

func TestConfiguredControlPanelDatabaseNameRejectsUnsafeValue(t *testing.T) {
	t.Setenv("PGDATABASE_CONTROL_PANEL", `control_panel"; DROP DATABASE postgres; --`)
	if _, err := configuredControlPanelDatabaseName(); err == nil {
		t.Fatal("unsafe database name was accepted")
	}
}

func TestEnsureControlPanelDBSuccessMessageUsesActualTarget(t *testing.T) {
	got := ensureControlPanelDBSuccessMessage("control_panel_acceptance")
	if !strings.Contains(got, "database control_panel_acceptance + migrations") {
		t.Fatalf("success message = %q", got)
	}
}

func TestApplyMigrationAtomicallyCommitsSQLAndLedgerTogether(t *testing.T) {
	tx := &recordingMigrationTx{}

	err := applyMigrationAtomically(
		func() (migrationTransaction, error) { return tx, nil },
		"0001_test.sql",
		"CREATE TABLE test_table (id BIGINT PRIMARY KEY)",
	)
	if err != nil {
		t.Fatalf("applyMigrationAtomically: %v", err)
	}
	if !tx.committed {
		t.Fatal("migration transaction was not committed")
	}
	if tx.rolledBack {
		t.Fatal("committed migration transaction was rolled back")
	}
	if len(tx.queries) != 2 {
		t.Fatalf("executed %d statements, want migration SQL and ledger insert", len(tx.queries))
	}
	if !strings.Contains(tx.queries[1], "INSERT INTO schema_migrations") {
		t.Fatalf("second statement = %q, want migration ledger insert", tx.queries[1])
	}
}

func TestApplyMigrationAtomicallyRollsBackWhenLedgerWriteFails(t *testing.T) {
	tx := &recordingMigrationTx{failAt: 2}

	err := applyMigrationAtomically(
		func() (migrationTransaction, error) { return tx, nil },
		"0001_test.sql",
		"CREATE TABLE test_table (id BIGINT PRIMARY KEY)",
	)
	if err == nil || !strings.Contains(err.Error(), "record migration") {
		t.Fatalf("applyMigrationAtomically error = %v, want ledger failure", err)
	}
	if tx.committed {
		t.Fatal("failed migration transaction was committed")
	}
	if !tx.rolledBack {
		t.Fatal("failed migration transaction was not rolled back")
	}
}

type recordingMigrationTx struct {
	queries    []string
	failAt     int
	committed  bool
	rolledBack bool
}

func (tx *recordingMigrationTx) Exec(query string, _ ...any) (sql.Result, error) {
	tx.queries = append(tx.queries, query)
	if tx.failAt == len(tx.queries) {
		return nil, errors.New("injected exec failure")
	}
	return recordingMigrationResult(0), nil
}

func (tx *recordingMigrationTx) Commit() error {
	tx.committed = true
	return nil
}

func (tx *recordingMigrationTx) Rollback() error {
	tx.rolledBack = true
	return nil
}

type recordingMigrationResult int64

func (r recordingMigrationResult) LastInsertId() (int64, error) {
	return int64(r), nil
}

func (r recordingMigrationResult) RowsAffected() (int64, error) {
	return int64(r), nil
}
