package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestApplyEnvOverridesUsesCoreServiceGRPCAddr(t *testing.T) {
	t.Setenv("CORE_SERVICE_GRPC_ADDR", "core.internal:50051")

	cfg := Default()
	cfg.ApplyEnvOverrides()

	if got := cfg.Dependencies.PortfolioServiceGRPC; got != "core.internal:50051" {
		t.Fatalf("PortfolioServiceGRPC = %q, want core service addr", got)
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
	t.Setenv("RUNTIME_CHANNEL_SERVER_TLS_SERVER_NAME", "runtime-channel.local")
	t.Setenv("RUNTIME_CHANNEL_SERVER_TLS_CLIENT_CA_FILE", "/tmp/client-ca.crt")
	t.Setenv("RUNTIME_CHANNEL_SERVER_TLS_CLIENT_CA_KEY_FILE", "/tmp/client-ca.key")
	t.Setenv("RUNTIME_PLATFORM_DEBUG_BARE_RUNTIME_ENABLED", "true")
	t.Setenv("RUNTIME_PLATFORM_BARE_BOOTSTRAP_IP_ALLOWLIST", "127.0.0.1/32,192.168.0.0/16")
	t.Setenv("RUNTIME_PLATFORM_BARE_CERTIFICATE_TTL", "4h")
	t.Setenv("RUNTIME_PLATFORM_BARE_RUNTIME_DEATH_GRACE_SECONDS", "1800")

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
	if cfg.RuntimeChannelServer.TLS.ServerName != "runtime-channel.local" {
		t.Fatalf("server name = %q", cfg.RuntimeChannelServer.TLS.ServerName)
	}
	if cfg.RuntimeChannelServer.TLS.ClientCAFile != "/tmp/client-ca.crt" {
		t.Fatalf("client ca file = %q", cfg.RuntimeChannelServer.TLS.ClientCAFile)
	}
	if cfg.RuntimeChannelServer.TLS.ClientCAKeyFile != "/tmp/client-ca.key" {
		t.Fatalf("client ca key file = %q", cfg.RuntimeChannelServer.TLS.ClientCAKeyFile)
	}
	if !cfg.RuntimePlatform.DebugBareRuntimeEnabled {
		t.Fatalf("debug bare runtime enabled = false, want true")
	}
	if len(cfg.RuntimePlatform.BareBootstrapIPAllowlist) != 2 {
		t.Fatalf("bare bootstrap allowlist = %#v", cfg.RuntimePlatform.BareBootstrapIPAllowlist)
	}
	if cfg.RuntimePlatform.BareCertificateTTL != 4*time.Hour {
		t.Fatalf("bare certificate ttl = %v", cfg.RuntimePlatform.BareCertificateTTL)
	}
	if cfg.RuntimePlatform.BareRuntimeDeathGraceSeconds != 1800 {
		t.Fatalf("bare runtime death grace seconds = %d", cfg.RuntimePlatform.BareRuntimeDeathGraceSeconds)
	}
}

func TestRuntimeChannelServerBareBootstrapDefaults(t *testing.T) {
	cfg := Default()

	if len(cfg.RuntimePlatform.BareBootstrapIPAllowlist) != 1 || cfg.RuntimePlatform.BareBootstrapIPAllowlist[0] != "127.0.0.1/32" {
		t.Fatalf("bare bootstrap allowlist = %#v", cfg.RuntimePlatform.BareBootstrapIPAllowlist)
	}
	if cfg.RuntimePlatform.BareCertificateTTL != 8*time.Hour {
		t.Fatalf("bare certificate ttl = %v", cfg.RuntimePlatform.BareCertificateTTL)
	}
	if cfg.RuntimePlatform.BareRuntimeDeathGraceSeconds != cfg.RuntimePlatform.DeathGraceSeconds {
		t.Fatalf("bare runtime death grace seconds = %d, want death grace %d", cfg.RuntimePlatform.BareRuntimeDeathGraceSeconds, cfg.RuntimePlatform.DeathGraceSeconds)
	}
}

func TestDockerProvisioningRuntimeChannelDialAddrYAML(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(`
provisioning:
  docker:
    runtime_channel_dial_addr: "runtime-channel.internal:50055"
`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if got := cfg.Provisioning.Docker.RuntimeChannelDialAddr; got != "runtime-channel.internal:50055" {
		t.Fatalf("RuntimeChannelDialAddr = %q", got)
	}
}

func TestLoadRejectsHostedRuntimeEnv(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(`
provisioning:
  docker:
    runtime_env:
      CORE_SERVICE_GRPC_ADDR: "127.0.0.1:50051"
      KAFKA_BROKERS: "127.0.0.1:19092"
`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	_, err := Load(path)
	if err == nil || !strings.Contains(err.Error(), "provisioning.docker.runtime_env") {
		t.Fatalf("Load error = %v, want Runtime isolation error", err)
	}
}

func TestLoadAllowsUnsetHostedRuntimeEnv(t *testing.T) {
	tests := []struct {
		name string
		yaml string
	}{
		{name: "absent", yaml: "provisioning:\n  docker:\n    runtime_channel_dial_addr: runtime-channel.internal:50055\n"},
		{name: "null", yaml: "provisioning:\n  docker:\n    runtime_env: null\n"},
		{name: "empty", yaml: "provisioning:\n  docker:\n    runtime_env: {}\n"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.yaml")
			if err := os.WriteFile(path, []byte(tt.yaml), 0o600); err != nil {
				t.Fatalf("write config: %v", err)
			}
			if _, err := Load(path); err != nil {
				t.Fatalf("Load: %v", err)
			}
		})
	}
}
