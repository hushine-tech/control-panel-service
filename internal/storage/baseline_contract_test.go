package storage_test

import (
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"
)

func TestCurrentControlPanelMigrationSetIsExplicit(t *testing.T) {
	entries, err := os.ReadDir("migrations")
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".sql") {
			got = append(got, entry.Name())
		}
	}
	sort.Strings(got)
	want := []string{
		"0000_create_schema_migrations.sql",
		"0001_current_schema_baseline.sql",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("control-panel migration set = %v, want %v", got, want)
	}
}
