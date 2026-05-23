// ensure-control-panel-db connects to PostgreSQL, creates database
// "control_panel" if missing, and applies SQL migrations.
//
// Usage:
//
//	go run ./cmd/ensure-control-panel-db
//	PGHOST=192.168.88.10 PGUSER=postgres PGPASSWORD=postgres go run ./cmd/ensure-control-panel-db
package main

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	_ "github.com/lib/pq"
)

const dbName = "control_panel"

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "ensure-control-panel-db: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("ensure-control-panel-db: OK (database %s + migrations)\n", dbName)
}

func run() error {
	host := getenv("PGHOST", "192.168.88.10")
	port := getenv("PGPORT", "5432")
	user := getenv("PGUSER", "postgres")
	pass := getenv("PGPASSWORD", "postgres")
	dbnameAdmin := getenv("PGDATABASE_ADMIN", "postgres")

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
		if err := admin.QueryRow(`SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)`, dbName).Scan(&exists); err != nil {
			return fmt.Errorf("check database: %w", err)
		}
		if !exists {
			if _, err := admin.Exec(fmt.Sprintf(`CREATE DATABASE %s`, dbName)); err != nil {
				return fmt.Errorf("CREATE DATABASE %s: %w", dbName, err)
			}
			fmt.Printf("created database: %s\n", dbName)
		} else {
			fmt.Printf("database %s already exists\n", dbName)
		}
		return nil
	}(); err != nil {
		return err
	}

	dsn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		host, port, user, pass, dbName)
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return fmt.Errorf("open %s db: %w", dbName, err)
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		return fmt.Errorf("ping %s: %w", dbName, err)
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
		if _, err := db.Exec(sqlText); err != nil {
			return fmt.Errorf("exec %s: %w", filepath.Base(f), err)
		}
		if _, err := db.Exec(
			`INSERT INTO schema_migrations (filename) VALUES ($1) ON CONFLICT (filename) DO NOTHING`,
			base,
		); err != nil {
			return fmt.Errorf("record migration %s: %w", base, err)
		}
		fmt.Println("applied:", base)
	}
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
