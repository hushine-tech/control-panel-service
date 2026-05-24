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

func TestApplyEnvOverridesKeepsLegacyAccountServiceGRPCAddr(t *testing.T) {
	t.Setenv("ACCOUNT_SERVICE_GRPC_ADDR", "legacy.internal:50051")

	cfg := Default()
	cfg.ApplyEnvOverrides()

	if got := cfg.Dependencies.AccountServiceGRPC; got != "legacy.internal:50051" {
		t.Fatalf("AccountServiceGRPC = %q, want legacy account service addr", got)
	}
}
