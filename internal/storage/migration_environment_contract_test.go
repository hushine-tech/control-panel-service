package storage_test

import (
	"os"
	"regexp"
	"testing"
)

func TestSessionDeliveryMigrationsUseEnvironmentNotLegacyAccountRouting(t *testing.T) {
	files := []string{
		"migrations/0017_create_runtime_data_delivery_leases.sql",
		"migrations/0022_create_stream_delivery_failures.sql",
	}
	legacyAccountRoutingColumn := regexp.MustCompile(`(?i)\b[m]ode\b`)
	for _, path := range files {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		if legacyAccountRoutingColumn.Match(raw) {
			t.Fatalf("%s must use environment, not the legacy account-routing column", path)
		}
	}
}
