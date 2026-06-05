package config

import "testing"

func TestApplyEnvOverridesUsesCoreServiceGRPCAddr(t *testing.T) {
	t.Setenv("CORE_SERVICE_GRPC_ADDR", "core.internal:50051")

	cfg := Default()
	cfg.ApplyEnvOverrides()

	if got := cfg.Dependencies.AccountServiceGRPC; got != "core.internal:50051" {
		t.Fatalf("AccountServiceGRPC = %q, want core service addr", got)
	}
}
