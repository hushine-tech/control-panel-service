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

func TestRuntimeChannelServerDefaults(t *testing.T) {
	cfg := Default()
	if cfg.RuntimeChannelServer.GRPCAddr != ":50055" {
		t.Fatalf("runtime channel grpc addr = %q, want :50055", cfg.RuntimeChannelServer.GRPCAddr)
	}
	if !cfg.RuntimeChannelServer.TLS.Enabled {
		t.Fatalf("runtime channel TLS should be enabled by default")
	}
	if cfg.RuntimePlatform.DebugBareRuntimeEnabled {
		t.Fatalf("bare runtime debug gate must default to false")
	}
}

func TestRuntimeChannelServerEnvOverrides(t *testing.T) {
	t.Setenv("RUNTIME_CHANNEL_SERVER_GRPC_ADDR", ":55055")
	t.Setenv("RUNTIME_CHANNEL_SERVER_TLS_ENABLED", "false")
	t.Setenv("RUNTIME_CHANNEL_SERVER_TLS_CERT_FILE", "/tmp/cp.crt")
	t.Setenv("RUNTIME_CHANNEL_SERVER_TLS_KEY_FILE", "/tmp/cp.key")
	t.Setenv("RUNTIME_CHANNEL_SERVER_TLS_CLIENT_CA_FILE", "/tmp/client-ca.crt")
	t.Setenv("RUNTIME_PLATFORM_DEBUG_BARE_RUNTIME_ENABLED", "true")

	cfg := Default()
	cfg.ApplyEnvOverrides()

	if cfg.RuntimeChannelServer.GRPCAddr != ":55055" {
		t.Fatalf("runtime channel grpc addr = %q", cfg.RuntimeChannelServer.GRPCAddr)
	}
	if cfg.RuntimeChannelServer.TLS.Enabled {
		t.Fatalf("TLS enabled = true, want false from env")
	}
	if cfg.RuntimeChannelServer.TLS.CertFile != "/tmp/cp.crt" {
		t.Fatalf("cert file = %q", cfg.RuntimeChannelServer.TLS.CertFile)
	}
	if cfg.RuntimeChannelServer.TLS.KeyFile != "/tmp/cp.key" {
		t.Fatalf("key file = %q", cfg.RuntimeChannelServer.TLS.KeyFile)
	}
	if cfg.RuntimeChannelServer.TLS.ClientCAFile != "/tmp/client-ca.crt" {
		t.Fatalf("client ca file = %q", cfg.RuntimeChannelServer.TLS.ClientCAFile)
	}
	if !cfg.RuntimePlatform.DebugBareRuntimeEnabled {
		t.Fatalf("debug bare runtime enabled = false, want true")
	}
}
