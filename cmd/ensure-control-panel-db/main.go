// ensure-control-panel-db connects to PostgreSQL, creates the configured
// control-panel database if missing, and applies SQL migrations.
//
// Usage:
//
//	go run ./cmd/ensure-control-panel-db
//	PGHOST=127.0.0.1 PGUSER=postgres PGPASSWORD=postgres PGDATABASE_CONTROL_PANEL=control_panel go run ./cmd/ensure-control-panel-db
package main

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/lib/pq"
)

const defaultControlPanelDatabase = "control_panel"

var controlPanelDatabaseNamePattern = regexp.MustCompile(`^[a-z][a-z0-9_]{0,62}$`)

type migrationTransaction interface {
	Exec(query string, args ...any) (sql.Result, error)
	Commit() error
	Rollback() error
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "ensure-control-panel-db: %v\n", err)
		os.Exit(1)
	}
	database, err := configuredControlPanelDatabaseName()
	if err != nil {
		fmt.Fprintf(os.Stderr, "ensure-control-panel-db: %v\n", err)
		os.Exit(1)
	}
	fmt.Println(ensureControlPanelDBSuccessMessage(database))
}

func configuredControlPanelDatabaseName() (string, error) {
	database := strings.TrimSpace(os.Getenv("PGDATABASE_CONTROL_PANEL"))
	if database == "" {
		database = defaultControlPanelDatabase
	}
	if !controlPanelDatabaseNamePattern.MatchString(database) {
		return "", fmt.Errorf(
			"PGDATABASE_CONTROL_PANEL must match %s",
			controlPanelDatabaseNamePattern.String(),
		)
	}
	return database, nil
}

func ensureControlPanelDBSuccessMessage(database string) string {
	return fmt.Sprintf(
		"ensure-control-panel-db: OK (database %s + migrations)",
		database,
	)
}

func run() error {
	host := getenv("PGHOST", "192.168.88.10")
	port := getenv("PGPORT", "5432")
	user := getenv("PGUSER", "postgres")
	pass := getenv("PGPASSWORD", "postgres")
	dbnameAdmin := getenv("PGDATABASE_ADMIN", "postgres")
	database, err := configuredControlPanelDatabaseName()
	if err != nil {
		return err
	}

	adminDSN := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		host, port, user, pass, dbnameAdmin)

	if err := func() error {
		admin, err := sql.Open("postgres", adminDSN)
		if err != nil {
			return fmt.Errorf("open admin: %w", err)
		}
		defer admin.Close()
		if err := admin.Ping(); err != nil {
			return fmt.Errorf("ping postgres: %w", err)
		}

		var exists bool
		if err := admin.QueryRow(`SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)`, database).Scan(&exists); err != nil {
			return fmt.Errorf("check database: %w", err)
		}
		if !exists {
			if _, err := admin.Exec(`CREATE DATABASE ` + pq.QuoteIdentifier(database)); err != nil {
				return fmt.Errorf("CREATE DATABASE %s: %w", database, err)
			}
			fmt.Printf("created database: %s\n", database)
		} else {
			fmt.Printf("database %s already exists\n", database)
		}
		return nil
	}(); err != nil {
		return err
	}

	dsn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		host, port, user, pass, database)
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return fmt.Errorf("open %s db: %w", database, err)
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		return fmt.Errorf("ping %s: %w", database, err)
	}

	root, err := findModuleRoot()
	if err != nil {
		return err
	}
	migDir := filepath.Join(root, "internal", "storage", "migrations")
	entries, err := os.ReadDir(migDir)
	if err != nil {
		return fmt.Errorf("read migrations: %w", err)
	}
	var files []string
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") {
			continue
		}
		files = append(files, filepath.Join(migDir, e.Name()))
	}
	sort.Strings(files)

	for _, f := range files {
		base := filepath.Base(f)
		applied, err := migrationApplied(db, base)
		if err != nil {
			return fmt.Errorf("check migration %s: %w", base, err)
		}
		if applied {
			fmt.Println("skipped:", base)
			continue
		}

		body, err := os.ReadFile(f)
		if err != nil {
			return fmt.Errorf("read %s: %w", f, err)
		}
		sqlText := strings.TrimSpace(string(body))
		if sqlText == "" {
			continue
		}
		if err := applyMigrationAtomically(func() (migrationTransaction, error) {
			return db.Begin()
		}, base, sqlText); err != nil {
			return err
		}
		fmt.Println("applied:", base)
	}
	return nil
}

func applyMigrationAtomically(
	begin func() (migrationTransaction, error),
	filename string,
	sqlText string,
) (err error) {
	tx, err := begin()
	if err != nil {
		return fmt.Errorf("begin migration %s: %w", filename, err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	if _, err = tx.Exec(sqlText); err != nil {
		return fmt.Errorf("exec %s: %w", filename, err)
	}
	if _, err = tx.Exec(
		`INSERT INTO schema_migrations (filename) VALUES ($1) ON CONFLICT (filename) DO NOTHING`,
		filename,
	); err != nil {
		return fmt.Errorf("record migration %s: %w", filename, err)
	}
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("commit migration %s: %w", filename, err)
	}
	committed = true
	return nil
}

func migrationApplied(db *sql.DB, filename string) (bool, error) {
	var tableExists bool
	if err := db.QueryRow(`SELECT to_regclass('public.schema_migrations') IS NOT NULL`).Scan(&tableExists); err != nil {
		return false, err
	}
	if !tableExists {
		return false, nil
	}

	var applied bool
	if err := db.QueryRow(`SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE filename = $1)`, filename).Scan(&applied); err != nil {
		return false, err
	}
	return applied, nil
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func findModuleRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("go.mod not found from cwd")
		}
		dir = parent
	}
}
