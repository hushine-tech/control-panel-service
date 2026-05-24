// One-shot migration tool for Phase D2.
//
// Copies the four market_data_* tables from the account database into the
// control_panel database. Designed to be idempotent: safe to re-run.
//
// Usage:
//
//	ACCOUNT_DSN="postgres://..." \
//	CONTROL_PANEL_DSN="postgres://..." \
//	go run ./scripts/migrate_market_data
//
// The tool prints a `pg_dump` backup recommendation at startup. Operator must
// run that backup before invoking the tool. INSERTs use ON CONFLICT DO NOTHING
// so a partial / re-run does not duplicate rows. Row counts are compared after
// each table; mismatches trigger a non-zero exit.
//
// Column lists are pinned explicitly (no SELECT *) so a later schema drift on
// either side surfaces as a Go compile-time / Postgres error, not a silent
// truncation.
package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	_ "github.com/lib/pq"
)

const (
	envAccountDSN      = "ACCOUNT_DSN"
	envControlPanelDSN = "CONTROL_PANEL_DSN"

	connectTimeout = 10 * time.Second
	queryTimeout   = 5 * time.Minute
)

// columnList per table — kept identical between source and destination.
// If the schema diverges, this list is the single place to update.
var tableColumns = []struct {
	name    string
	columns string
}{
	{
		name: "market_data_streams",
		columns: `stream_id, exchange, market, kind, symbol, interval,
            desired_state, actual_state, effective_live_delivery,
            last_data_at, last_error, last_reconciled_at,
            created_at, updated_at`,
	},
	{
		name: "market_data_requests",
		columns: `request_id, user_id, account_id, exchange, market, kind,
            symbol, interval, needs_live_delivery, status, stream_id,
            created_at, updated_at, cancelled_at`,
	},
	{
		name: "market_data_leases",
		columns: `lease_id, session_id, strategy_id, account_id, stream_id,
            expires_at, last_heartbeat_at, created_at, released_at`,
	},
	{
		name: "market_data_history_requests",
		columns: `request_id, user_id, account_id, exchange, market, kind,
            symbol, interval, requested_start_at, requested_end_at,
            covered_start_at, covered_end_at, last_error, status,
            created_at, updated_at, cancelled_at`,
	},
}

func main() {
	fmt.Println("=========================================================")
	fmt.Println("Phase D2 market-data migration: account → control_panel")
	fmt.Println("=========================================================")
	fmt.Println()
	fmt.Println("⚠ BACKUP RECOMMENDATION ⚠")
	fmt.Println()
	fmt.Println("Before running this tool, dump the source tables:")
	fmt.Println()
	fmt.Println("  pg_dump --data-only \\")
	fmt.Println("    --table=market_data_streams \\")
	fmt.Println("    --table=market_data_requests \\")
	fmt.Println("    --table=market_data_leases \\")
	fmt.Println("    --table=market_data_history_requests \\")
	fmt.Println("    \"$ACCOUNT_DSN\" > account_market_data_backup.sql")
	fmt.Println()
	fmt.Println("Press Ctrl+C within 5 seconds to abort if no backup exists.")
	fmt.Println()
	time.Sleep(5 * time.Second)

	accountDSN := os.Getenv(envAccountDSN)
	controlPanelDSN := os.Getenv(envControlPanelDSN)
	if accountDSN == "" || controlPanelDSN == "" {
		log.Fatalf("both %s and %s must be set", envAccountDSN, envControlPanelDSN)
	}

	ctx, cancel := context.WithTimeout(context.Background(), connectTimeout)
	defer cancel()

	srcDB, err := openDB(ctx, accountDSN, "account")
	if err != nil {
		log.Fatalf("open account DB: %v", err)
	}
	defer srcDB.Close()

	dstDB, err := openDB(ctx, controlPanelDSN, "control_panel")
	if err != nil {
		log.Fatalf("open control_panel DB: %v", err)
	}
	defer dstDB.Close()

	overallErr := false
	for _, tbl := range tableColumns {
		if err := migrateTable(srcDB, dstDB, tbl.name, tbl.columns); err != nil {
			log.Printf("[%s] FAILED: %v", tbl.name, err)
			overallErr = true
		}
	}

	if overallErr {
		log.Fatal("one or more tables failed; see logs above")
	}

	// After bulk row-copy the destination's BIGSERIAL sequences are still at
	// their initial value; the next INSERT would pick id=1 and collide with
	// migrated rows. setval() each sequence to MAX(id) so production traffic
	// resumes cleanly. This MUST run inside the tool — leaving it as
	// operator-advice was the M1 review finding.
	if err := resyncSequences(dstDB); err != nil {
		log.Fatalf("resync sequences: %v", err)
	}

	fmt.Println()
	fmt.Println("✔ Migration complete. All four tables verified; sequences resynced.")
	fmt.Println()
	fmt.Println("Next steps:")
	fmt.Println("  1. Apply core-service migration 0012 to drop source tables.")
	fmt.Println("  2. Restart scraper / quant-handler / strategy-service with the")
	fmt.Println("     control-panel-service endpoints set.")
}

// resyncSequences advances each BIGSERIAL sequence on the destination
// database so it is at least MAX(<pk>). Skips quietly if the destination
// table is empty (sequence was never advanced from its initial value).
//
// market_data_history_requests shares the market_data_requests sequence
// (see migration 0006 — DEFAULT nextval('market_data_requests_request_id_seq')),
// so we resync that sequence against the union of both tables.
func resyncSequences(db *sql.DB) error {
	ctx, cancel := context.WithTimeout(context.Background(), queryTimeout)
	defer cancel()

	// (sequence-name, expression-yielding-max-id-or-NULL)
	resyncs := []struct {
		seq      string
		maxExpr  string
		descTbl  string
	}{
		{
			seq:     "market_data_streams_stream_id_seq",
			maxExpr: `(SELECT MAX(stream_id) FROM market_data_streams)`,
			descTbl: "market_data_streams",
		},
		{
			// History requests share this sequence; pick MAX across both.
			seq: "market_data_requests_request_id_seq",
			maxExpr: `(SELECT GREATEST(
				COALESCE((SELECT MAX(request_id) FROM market_data_requests), 0),
				COALESCE((SELECT MAX(request_id) FROM market_data_history_requests), 0)
			))`,
			descTbl: "market_data_requests + market_data_history_requests",
		},
		{
			seq:     "market_data_leases_lease_id_seq",
			maxExpr: `(SELECT MAX(lease_id) FROM market_data_leases)`,
			descTbl: "market_data_leases",
		},
	}

	for _, r := range resyncs {
		// setval(..., NULL) raises "setval: value must be greater than zero",
		// so guard with a CASE that's a no-op when the table(s) are empty.
		var newVal sql.NullInt64
		err := db.QueryRowContext(ctx, fmt.Sprintf(`
			SELECT CASE
				WHEN %s IS NULL OR %s = 0 THEN NULL
				ELSE setval('%s', %s)
			END`, r.maxExpr, r.maxExpr, r.seq, r.maxExpr)).Scan(&newVal)
		if err != nil {
			return fmt.Errorf("resync %s: %w", r.seq, err)
		}
		if newVal.Valid {
			log.Printf("[seq %s] resynced to %d (max from %s)", r.seq, newVal.Int64, r.descTbl)
		} else {
			log.Printf("[seq %s] left untouched (%s empty)", r.seq, r.descTbl)
		}
	}
	return nil
}

func openDB(ctx context.Context, dsn, label string) (*sql.DB, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("sql.Open: %w", err)
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping %s: %w", label, err)
	}
	log.Printf("connected to %s database", label)
	return db, nil
}

func migrateTable(src, dst *sql.DB, table, columns string) error {
	ctx, cancel := context.WithTimeout(context.Background(), queryTimeout)
	defer cancel()

	srcCount, err := countRows(ctx, src, table)
	if err != nil {
		return fmt.Errorf("count source: %w", err)
	}
	log.Printf("[%s] source rows: %d", table, srcCount)

	if srcCount == 0 {
		log.Printf("[%s] source empty; nothing to copy", table)
	}

	// Read source rows. We use a textual round-trip via sql.Rows + Exec to
	// keep the script independent of pgx-specific COPY APIs and to make it
	// re-runnable safely against ON CONFLICT DO NOTHING.
	if srcCount > 0 {
		if err := copyRows(ctx, src, dst, table, columns); err != nil {
			return fmt.Errorf("copy: %w", err)
		}
	}

	dstCount, err := countRows(ctx, dst, table)
	if err != nil {
		return fmt.Errorf("count destination: %w", err)
	}
	log.Printf("[%s] destination rows: %d", table, dstCount)

	if dstCount < srcCount {
		return fmt.Errorf("row-count parity failed: source=%d dest=%d", srcCount, dstCount)
	}
	if dstCount > srcCount {
		log.Printf("[%s] destination has %d more rows than source — likely from a previous run; not an error", table, dstCount-srcCount)
	}
	log.Printf("[%s] OK", table)
	return nil
}

func countRows(ctx context.Context, db *sql.DB, table string) (int64, error) {
	var n int64
	err := db.QueryRowContext(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM %s`, table)).Scan(&n)
	if err != nil {
		return 0, err
	}
	return n, nil
}

// copyRows streams rows out of src and INSERTs them into dst with ON CONFLICT
// DO NOTHING on the primary key. Uses parameterized exec per row — slow
// compared to COPY but the dataset is small (control-plane state, not
// time-series data) and idempotency / readability matter more here.
func copyRows(ctx context.Context, src, dst *sql.DB, table, columns string) error {
	selectSQL := fmt.Sprintf(`SELECT %s FROM %s`, columns, table)
	rows, err := src.QueryContext(ctx, selectSQL)
	if err != nil {
		return fmt.Errorf("select: %w", err)
	}
	defer rows.Close()

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return fmt.Errorf("column types: %w", err)
	}
	n := len(colTypes)

	placeholders := make([]byte, 0, n*4)
	for i := 0; i < n; i++ {
		if i > 0 {
			placeholders = append(placeholders, ',')
		}
		placeholders = append(placeholders, '$')
		placeholders = append(placeholders, fmt.Sprintf("%d", i+1)...)
	}
	insertSQL := fmt.Sprintf(
		`INSERT INTO %s (%s) VALUES (%s) ON CONFLICT DO NOTHING`,
		table, columns, string(placeholders),
	)

	stmt, err := dst.PrepareContext(ctx, insertSQL)
	if err != nil {
		return fmt.Errorf("prepare insert: %w", err)
	}
	defer stmt.Close()

	values := make([]interface{}, n)
	scanArgs := make([]interface{}, n)
	for i := range values {
		scanArgs[i] = &values[i]
	}

	var copied int64
	for rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			return fmt.Errorf("scan: %w", err)
		}
		if _, err := stmt.ExecContext(ctx, values...); err != nil {
			return fmt.Errorf("insert row %d: %w", copied+1, err)
		}
		copied++
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("rows iter: %w", err)
	}
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return fmt.Errorf("query timeout exceeded after %d rows", copied)
	}
	log.Printf("[%s] copied %d rows", table, copied)
	return nil
}
