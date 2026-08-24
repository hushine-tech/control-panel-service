package config

import (
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

const expectedRuntimeDependencyContractSHA256 = "8457b3c35618558fc8bfc74d4135b7eb52e00c33a8c9a49d202830f3fd5b62c5"

func TestDefaultDependencyProfileAdmissionIsPinned(t *testing.T) {
	profile := Default().RuntimeChannelServer.DependencyProfile
	if profile.SchemaVersion != 1 ||
		profile.Name != "platform-python-3.13" ||
		profile.Version != "1.0.0" ||
		profile.ContractSHA256 != expectedRuntimeDependencyContractSHA256 {
		t.Fatalf("dependency admission = %+v", profile)
	}
}

func TestDependencyProfileValidationRejectsInvalidValues(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*RuntimeDependencyProfileConfig)
	}{
		{name: "zero schema", mutate: func(profile *RuntimeDependencyProfileConfig) { profile.SchemaVersion = 0 }},
		{name: "blank name", mutate: func(profile *RuntimeDependencyProfileConfig) { profile.Name = "" }},
		{name: "whitespace name", mutate: func(profile *RuntimeDependencyProfileConfig) { profile.Name = " platform-python-3.13" }},
		{name: "blank version", mutate: func(profile *RuntimeDependencyProfileConfig) { profile.Version = "\t" }},
		{name: "whitespace version", mutate: func(profile *RuntimeDependencyProfileConfig) { profile.Version = "1.0.0 " }},
		{name: "short digest", mutate: func(profile *RuntimeDependencyProfileConfig) { profile.ContractSHA256 = "deadbeef" }},
		{name: "uppercase digest", mutate: func(profile *RuntimeDependencyProfileConfig) {
			profile.ContractSHA256 = strings.ToUpper(expectedRuntimeDependencyContractSHA256)
		}},
		{name: "whitespace digest", mutate: func(profile *RuntimeDependencyProfileConfig) {
			profile.ContractSHA256 = " " + expectedRuntimeDependencyContractSHA256
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			profile := Default().RuntimeChannelServer.DependencyProfile
			tt.mutate(&profile)
			if err := profile.Validate(); err == nil {
				t.Fatal("Validate() error = nil, want invalid dependency profile")
			}
		})
	}
}

func TestDependencyProfileEnvOverrides(t *testing.T) {
	t.Setenv("RUNTIME_DEPENDENCY_SCHEMA_VERSION", "2")
	t.Setenv("RUNTIME_DEPENDENCY_PROFILE_NAME", "platform-python-3.14")
	t.Setenv("RUNTIME_DEPENDENCY_PROFILE_VERSION", "2.0.0")
	t.Setenv("RUNTIME_DEPENDENCY_CONTRACT_SHA256", strings.Repeat("a", 64))

	cfg := Default()
	if err := cfg.ApplyEnvOverrides(); err != nil {
		t.Fatalf("ApplyEnvOverrides: %v", err)
	}
	profile := cfg.RuntimeChannelServer.DependencyProfile
	if profile.SchemaVersion != 2 || profile.Name != "platform-python-3.14" ||
		profile.Version != "2.0.0" || profile.ContractSHA256 != strings.Repeat("a", 64) {
		t.Fatalf("dependency admission = %+v", profile)
	}
}

func TestDependencyProfileEnvOverridesRejectInvalidValues(t *testing.T) {
	tests := []struct {
		name  string
		key   string
		value string
	}{
		{name: "invalid schema", key: "RUNTIME_DEPENDENCY_SCHEMA_VERSION", value: "not-a-number"},
		{name: "empty schema", key: "RUNTIME_DEPENDENCY_SCHEMA_VERSION", value: ""},
		{name: "whitespace name", key: "RUNTIME_DEPENDENCY_PROFILE_NAME", value: " platform-python-3.13"},
		{name: "empty name", key: "RUNTIME_DEPENDENCY_PROFILE_NAME", value: ""},
		{name: "whitespace version", key: "RUNTIME_DEPENDENCY_PROFILE_VERSION", value: "1.0.0 "},
		{name: "empty version", key: "RUNTIME_DEPENDENCY_PROFILE_VERSION", value: ""},
		{name: "invalid digest", key: "RUNTIME_DEPENDENCY_CONTRACT_SHA256", value: "ABCDEF"},
		{name: "empty digest", key: "RUNTIME_DEPENDENCY_CONTRACT_SHA256", value: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(tt.key, tt.value)
			if err := Default().ApplyEnvOverrides(); err == nil {
				t.Fatal("ApplyEnvOverrides() error = nil, want invalid dependency profile override")
			}
		})
	}
}

func TestLoadRejectsInvalidDependencyProfile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(`
runtime_channel_server:
  dependency_profile:
    schema_version: 0
`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	if _, err := Load(path); err == nil || !strings.Contains(err.Error(), "dependency_profile") {
		t.Fatalf("Load() error = %v, want dependency profile validation error", err)
	}
}

func TestApplyEnvOverridesUsesCanonicalServiceAddresses(t *testing.T) {
	t.Setenv("SERVER_HTTP_ADDR", ":18082")
	t.Setenv("SERVER_GRPC_ADDR", ":18054")
	t.Setenv("DEPENDENCIES_CORE_SERVICE_GRPC", "core.internal:50051")
	t.Setenv("DEPENDENCIES_ORDER_SERVICE_GRPC", "orders.internal:50051")

	cfg := Default()
	if err := cfg.ApplyEnvOverrides(); err != nil {
		t.Fatalf("ApplyEnvOverrides: %v", err)
	}

	if got := cfg.Dependencies.PortfolioServiceGRPC; got != "core.internal:50051" {
		t.Fatalf("PortfolioServiceGRPC = %q, want core service addr", got)
	}
	if got := cfg.Dependencies.OrderServiceGRPC; got != "orders.internal:50051" {
		t.Fatalf("OrderServiceGRPC = %q, want order service addr", got)
	}
	if cfg.Server.HTTPAddr != ":18082" || cfg.Server.GRPCAddr != ":18054" {
		t.Fatalf("server = %+v", cfg.Server)
	}
}

func TestRemovedEnvironmentAliasesDoNotOverrideCanonicalConfiguration(t *testing.T) {
	t.Setenv("HTTP_ADDR", "legacy-http:1")
	t.Setenv("GRPC_ADDR", "legacy-grpc:2")
	t.Setenv("TIMESCALEDB_DSN", "host=legacy-db dbname=legacy")
	t.Setenv("CORE_SERVICE_GRPC_ADDR", "legacy-core:3")
	t.Setenv("ORDER_SERVICE_GRPC_ADDR", "legacy-order:4")
	cfg := Default()
	if err := cfg.ApplyEnvOverrides(); err != nil {
		t.Fatalf("ApplyEnvOverrides: %v", err)
	}
	if cfg.Server.HTTPAddr != ":8082" || cfg.Server.GRPCAddr != ":50054" {
		t.Fatalf("server = %+v, want canonical defaults", cfg.Server)
	}
	if cfg.Database.DBName != "control_panel" || cfg.Database.Host != "192.168.88.10" {
		t.Fatalf("database = %+v, want canonical defaults", cfg.Database)
	}
	if cfg.Dependencies.PortfolioServiceGRPC != "127.0.0.1:50051" || cfg.Dependencies.OrderServiceGRPC != "127.0.0.1:50051" {
		t.Fatalf("dependencies = %+v, want canonical defaults", cfg.Dependencies)
	}
}

func TestProvisioningConfigOmitsRemovedRuntimePoolAndEnvironmentKeys(t *testing.T) {
	assertNoYAMLTags := func(value any, removed ...string) {
		t.Helper()
		typ := reflect.TypeOf(value)
		for i := 0; i < typ.NumField(); i++ {
			tag := strings.Split(typ.Field(i).Tag.Get("yaml"), ",")[0]
			for _, name := range removed {
				if tag == name {
					t.Errorf("%s still exposes removed YAML key %q", typ.Name(), name)
				}
			}
		}
	}
	assertNoYAMLTags(ProvisioningConfig{}, "advertise_host", "port_range_base", "port_range_size")
	assertNoYAMLTags(DockerProvisioningConfig{}, "runtime_env")
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

func TestDockerCoverageDefaultsDisabled(t *testing.T) {
	coverage := Default().Provisioning.Docker.Coverage

	if coverage.Enabled {
		t.Fatal("coverage enabled by default")
	}
	if got, want := coverage.Image, "hushine/strategy-runtime:executor-coverage"; got != want {
		t.Fatalf("coverage image = %q, want %q", got, want)
	}
	if coverage.OutputDir != "" {
		t.Fatalf("coverage output dir = %q, want empty", coverage.OutputDir)
	}
	if got, want := coverage.StopTimeoutSeconds, 10; got != want {
		t.Fatalf("coverage stop timeout = %d, want %d", got, want)
	}
}

func TestDockerCoverageYAML(t *testing.T) {
	outputDir := t.TempDir()
	path := filepath.Join(t.TempDir(), "config.yaml")
	contents := `
provisioning:
  docker:
    coverage:
      enabled: true
      image: "hushine/strategy-runtime:test-cover"
      output_dir: "` + outputDir + `"
      stop_timeout_seconds: 17
`
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	coverage := cfg.Provisioning.Docker.Coverage
	if !coverage.Enabled {
		t.Fatal("coverage disabled, want enabled from YAML")
	}
	if got, want := coverage.Image, "hushine/strategy-runtime:test-cover"; got != want {
		t.Fatalf("coverage image = %q, want %q", got, want)
	}
	if coverage.OutputDir != outputDir {
		t.Fatalf("coverage output dir = %q, want %q", coverage.OutputDir, outputDir)
	}
	if got, want := coverage.StopTimeoutSeconds, 17; got != want {
		t.Fatalf("coverage stop timeout = %d, want %d", got, want)
	}
}

func TestDockerCoverageEnvOverrides(t *testing.T) {
	t.Setenv("RUNTIME_COVERAGE_ENABLED", "true")
	t.Setenv("RUNTIME_COVERAGE_OUTPUT_DIR", "/tmp/census/run-1/coverage/runtime-agent")
	t.Setenv("RUNTIME_COVERAGE_IMAGE", "hushine/strategy-runtime:test-cover")

	cfg := Default()
	cfg.ApplyEnvOverrides()
	if err := cfg.Provisioning.ValidateRuntimeIsolation(); err != nil {
		t.Fatalf("ValidateRuntimeIsolation: %v", err)
	}

	coverage := cfg.Provisioning.Docker.Coverage
	if !coverage.Enabled {
		t.Fatal("coverage disabled, want enabled from environment")
	}
	if got, want := coverage.OutputDir, "/tmp/census/run-1/coverage/runtime-agent"; got != want {
		t.Fatalf("coverage output dir = %q, want %q", got, want)
	}
	if got, want := coverage.Image, "hushine/strategy-runtime:test-cover"; got != want {
		t.Fatalf("coverage image = %q, want %q", got, want)
	}
}

func TestDockerCoverageValidationRejectsMissingOutputDir(t *testing.T) {
	for _, outputDir := range []string{"", "   "} {
		t.Run(outputDir, func(t *testing.T) {
			cfg := Default()
			cfg.Provisioning.Docker.Coverage.Enabled = true
			cfg.Provisioning.Docker.Coverage.OutputDir = outputDir

			err := cfg.Provisioning.ValidateRuntimeIsolation()
			if err == nil || !strings.Contains(err.Error(), "provisioning.docker.coverage.output_dir") {
				t.Fatalf("ValidateRuntimeIsolation error = %v, want missing coverage output dir error", err)
			}
		})
	}
}

func TestDockerCoverageValidationRejectsRelativeOutputDir(t *testing.T) {
	cfg := Default()
	cfg.Provisioning.Docker.Coverage.Enabled = true
	cfg.Provisioning.Docker.Coverage.OutputDir = "census/run-1/coverage/runtime-agent"

	err := cfg.Provisioning.ValidateRuntimeIsolation()
	if err == nil || !strings.Contains(err.Error(), "absolute") {
		t.Fatalf("ValidateRuntimeIsolation error = %v, want absolute coverage output dir error", err)
	}
}

func TestDockerCoverageValidationRejectsMissingImage(t *testing.T) {
	for name, image := range map[string]string{
		"empty":      "",
		"whitespace": " \t ",
	} {
		t.Run(name, func(t *testing.T) {
			cfg := Default()
			cfg.Provisioning.Docker.Coverage.Enabled = true
			cfg.Provisioning.Docker.Coverage.Image = image
			cfg.Provisioning.Docker.Coverage.OutputDir = t.TempDir()

			err := cfg.Provisioning.ValidateRuntimeIsolation()
			if err == nil || !strings.Contains(err.Error(), "provisioning.docker.coverage.image") {
				t.Fatalf("ValidateRuntimeIsolation error = %v, want missing coverage image error", err)
			}
		})
	}
}

func TestDockerCoverageValidationRejectsWhitespaceImageEnvOverride(t *testing.T) {
	t.Setenv("RUNTIME_COVERAGE_ENABLED", "true")
	t.Setenv("RUNTIME_COVERAGE_OUTPUT_DIR", t.TempDir())
	t.Setenv("RUNTIME_COVERAGE_IMAGE", " \t ")

	cfg := Default()
	cfg.ApplyEnvOverrides()
	if got, want := cfg.Provisioning.Docker.Coverage.Image, " \t "; got != want {
		t.Fatalf("coverage image = %q, want exact environment override %q", got, want)
	}

	err := cfg.Provisioning.ValidateRuntimeIsolation()
	if err == nil || !strings.Contains(err.Error(), "provisioning.docker.coverage.image") {
		t.Fatalf("ValidateRuntimeIsolation error = %v, want missing coverage image error", err)
	}
}

func TestDockerCoverageValidationRejectsNonPositiveStopTimeout(t *testing.T) {
	for _, timeout := range []int{0, -1} {
		t.Run(strconv.Itoa(timeout), func(t *testing.T) {
			cfg := Default()
			cfg.Provisioning.Docker.Coverage.Enabled = true
			cfg.Provisioning.Docker.Coverage.OutputDir = t.TempDir()
			cfg.Provisioning.Docker.Coverage.StopTimeoutSeconds = timeout

			err := cfg.Provisioning.ValidateRuntimeIsolation()
			if err == nil || !strings.Contains(err.Error(), "provisioning.docker.coverage.stop_timeout_seconds") {
				t.Fatalf("ValidateRuntimeIsolation error = %v, want stop timeout error", err)
			}
		})
	}
}
