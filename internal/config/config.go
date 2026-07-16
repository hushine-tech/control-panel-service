package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	elog "github.com/hushine-tech/golang-lib/pkg/log"
	"gopkg.in/yaml.v3"
)

type Config struct {
	Server               ServerConfig               `yaml:"server"`
	RuntimeChannelServer RuntimeChannelServerConfig `yaml:"runtime_channel_server"`
	Database             DatabaseConfig             `yaml:"database"`
	MarketData           MarketDataConfig           `yaml:"market_data"`
	Dependencies         DependenciesConfig         `yaml:"dependencies"`
	RuntimePlatform      RuntimePlatformConfig      `yaml:"runtime_platform"`
	RuntimePlans         map[string]RuntimePlan     `yaml:"runtime_plans"`
	Provisioning         ProvisioningConfig         `yaml:"provisioning"`
	Notification         NotificationConfig         `yaml:"notification"`
	Log                  elog.Config                `yaml:"log"`
}

type ServerConfig struct {
	HTTPAddr string `yaml:"http_addr"`
	GRPCAddr string `yaml:"grpc_addr"`
}

type RuntimeChannelServerConfig struct {
	GRPCAddr          string                         `yaml:"grpc_addr"`
	TLS               RuntimeChannelServerTLSConfig  `yaml:"tls"`
	DependencyProfile RuntimeDependencyProfileConfig `yaml:"dependency_profile"`
}

type RuntimeDependencyProfileConfig struct {
	SchemaVersion  uint32 `yaml:"schema_version"`
	Name           string `yaml:"name"`
	Version        string `yaml:"version"`
	ContractSHA256 string `yaml:"contract_sha256"`
}

var runtimeDependencyContractSHA256RE = regexp.MustCompile(`^[0-9a-f]{64}$`)
var runtimeDependencyProfileFactRE = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._+-]{0,127}$`)

func (c RuntimeDependencyProfileConfig) Validate() error {
	if c.SchemaVersion == 0 {
		return fmt.Errorf("runtime_channel_server.dependency_profile.schema_version must be greater than zero")
	}
	if !runtimeDependencyProfileFactRE.MatchString(c.Name) {
		return fmt.Errorf("runtime_channel_server.dependency_profile.name must be a safe non-empty profile identifier")
	}
	if !runtimeDependencyProfileFactRE.MatchString(c.Version) {
		return fmt.Errorf("runtime_channel_server.dependency_profile.version must be a safe non-empty profile identifier")
	}
	if !runtimeDependencyContractSHA256RE.MatchString(c.ContractSHA256) {
		return fmt.Errorf("runtime_channel_server.dependency_profile.contract_sha256 must be 64 lowercase hexadecimal characters")
	}
	return nil
}

type RuntimeChannelServerTLSConfig struct {
	Enabled         bool   `yaml:"enabled"`
	CertFile        string `yaml:"cert_file"`
	KeyFile         string `yaml:"key_file"`
	ServerName      string `yaml:"server_name"`
	ClientCAFile    string `yaml:"client_ca_file"`
	ClientCAKeyFile string `yaml:"client_ca_key_file"`
}

type DatabaseConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	DBName   string `yaml:"dbname"`
	SSLMode  string `yaml:"sslmode"`
}

type MarketDataConfig struct {
	Host                string   `yaml:"host"`
	Port                int      `yaml:"port"`
	User                string   `yaml:"user"`
	Password            string   `yaml:"password"`
	Database            string   `yaml:"database"`
	SSLMode             string   `yaml:"sslmode"`
	LiveDeliveryEnabled bool     `yaml:"live_delivery_enabled"`
	KafkaBrokers        []string `yaml:"kafka_brokers"`
}

type DependenciesConfig struct {
	PortfolioServiceGRPC string `yaml:"portfolio_service_grpc"`
	OrderServiceGRPC     string `yaml:"order_service_grpc"`
}

type NotificationConfig struct {
	Enabled bool                    `yaml:"enabled"`
	Kafka   NotificationKafkaConfig `yaml:"kafka"`
}

type NotificationKafkaConfig struct {
	Brokers  []string `yaml:"brokers"`
	Topic    string   `yaml:"topic"`
	ClientID string   `yaml:"client_id"`
}

// RuntimePlatformConfig holds platform-wide caps that apply on top of
// per-user plan limits. effective_limit = min(user_plan_limit, platform_limit).
type RuntimePlatformConfig struct {
	MaxTotalHostedRuntimes     int    `yaml:"max_total_hosted_runtimes"`
	MaxTotalSelfHostedRuntimes int    `yaml:"max_total_self_hosted_runtimes"`
	DefaultPlanCode            string `yaml:"default_plan_code"`
	// HeartbeatGraceSeconds is how stale a heartbeat may be before route
	// resolution treats the runtime as unhealthy. Must be > 0; falls back
	// to 30 if unset.
	HeartbeatGraceSeconds int `yaml:"heartbeat_grace_seconds"`
	// DeathGraceSeconds is how stale a runtime may remain before the
	// watchdog terminally ends it and marks bound sessions recoverable.
	// Must be > 0; falls back to 300 if unset.
	DeathGraceSeconds int `yaml:"death_grace_seconds"`
	// BareRuntimeDeathGraceSeconds overrides DeathGraceSeconds only for
	// local/debug bare runtimes. Must be > 0; falls back to DeathGraceSeconds
	// when unset.
	BareRuntimeDeathGraceSeconds int `yaml:"bare_runtime_death_grace_seconds"`
	// DebugBareRuntimeEnabled allows internal bare runtime certificate
	// bootstrap only for local/debug control-panel deployments.
	DebugBareRuntimeEnabled bool `yaml:"debug_bare_runtime_enabled"`
	// BareBootstrapIPAllowlist limits which source IPs may request a short-
	// lived bare runtime client certificate when debug bare runtime is enabled.
	BareBootstrapIPAllowlist []string `yaml:"bare_bootstrap_ip_allowlist"`
	// BareCertificateTTL controls the lifetime of bare runtime debugger
	// client certificates. Must be positive; defaults to 8h.
	BareCertificateTTL time.Duration `yaml:"bare_certificate_ttl"`
}

// RuntimePlan describes a single per-user plan tier (free / developer / pro / etc.).
// Loaded from config.yaml so plans are tunable without code changes.
type RuntimePlan struct {
	MaxHostedRuntimes               int      `yaml:"max_hosted_runtimes"`
	MaxSelfHostedRuntimes           int      `yaml:"max_self_hosted_runtimes"`
	MaxRoutingEnabledRuntimes       int      `yaml:"max_routing_enabled_runtimes"`
	MaxConcurrentSessionsTotal      int      `yaml:"max_concurrent_sessions_total"`
	MaxConcurrentSessionsPerRuntime int      `yaml:"max_concurrent_sessions_per_runtime"`
	AllowedResourceProfiles         []string `yaml:"allowed_resource_profiles"`
	AllowSelfHostedRuntime          bool     `yaml:"allow_self_hosted_runtime"`
	AllowIDEDebug                   bool     `yaml:"allow_ide_debug"`
}

// ResourceProfile defines container-runtime limits applied when the
// hosted-runtime provisioner spins up a strategy-runtime container.
//
// Translation to Docker:
//   - NanoCPUs    → --cpus <value>   (e.g. "0.5" = half a core)
//   - MemoryMB    → --memory <value>m
//   - PidsLimit   → --pids-limit
type ResourceProfile struct {
	NanoCPUs  string `yaml:"nano_cpus"`  // human form, e.g. "0.5", "1.0"
	MemoryMB  int    `yaml:"memory_mb"`  // hard memory cap
	PidsLimit int    `yaml:"pids_limit"` // 0 = unset (use Docker default)
}

// ProvisioningConfig aggregates the inputs the hosted-runtime provisioner
// needs at runtime time: per-profile resource limits, the strategy-runtime
// container image to start, and the legacy host:port pool kept only for
// historical registry fields. Runtime session traffic is RuntimeChannel-only.
type ProvisioningConfig struct {
	// Backend selects the provisioner implementation:
	//   ""      → NoOpProvisioner (default; EnsureHostedRuntime fails closed)
	//   "docker" → DockerProvisioner (Phase D1 section 5.5)
	// Other backends (k8s, nomad) are out of D1 scope.
	Backend string `yaml:"backend"`

	// Image is the container image strategy-runtime is launched from.
	// Defaults to "hushine/strategy-runtime:executor-dev".
	Image string `yaml:"image"`

	// AdvertiseHost is retained for older registry rows and operator display.
	// Runtime session traffic uses RuntimeChannel.
	AdvertiseHost string `yaml:"advertise_host"`

	// PortRangeBase / PortRangeSize are retained for historical registry
	// compatibility. Hosted runtime traffic no longer publishes these ports.
	PortRangeBase int `yaml:"port_range_base"`
	PortRangeSize int `yaml:"port_range_size"`

	// RegistrationTimeoutSeconds is how long EnsureHostedRuntime waits
	// for a freshly-provisioned runtime to connect through RuntimeChannel.
	// Defaults to 30s.
	RegistrationTimeoutSeconds int `yaml:"registration_timeout_seconds"`

	// Profiles maps resource_profile name → Docker limits.
	Profiles map[string]ResourceProfile `yaml:"profiles"`

	// Docker holds DockerProvisioner-specific settings. Ignored when
	// Backend != "docker".
	Docker DockerProvisioningConfig `yaml:"docker"`
}

// DockerProvisioningConfig is the operator-controlled docker run
// configuration applied to every hosted runtime container.
type DockerProvisioningConfig struct {
	// NetworkMode passed verbatim to `docker run --network`. D1 single-
	// host default is "host" so the container can reach RuntimeChannel
	// on localhost. Empty string falls back to "host" for single-host dev.
	// Cluster operators set "bridge" or a custom network together with an
	// explicit RuntimeChannelDialAddr.
	NetworkMode string `yaml:"network_mode"`

	// RuntimeChannelDialAddr is the value the runtime container should use
	// to dial the dedicated RuntimeChannel listener on control-panel-service.
	// Distinct from `cfg.RuntimeChannelServer.GRPCAddr` because that is the
	// BIND address (e.g. ":50055") and not a valid container dial target.
	//
	// Defaults: host networking → "127.0.0.1:50055". Bridge / custom
	// networks → operator MUST set explicitly (e.g.
	// "host.docker.internal:50055" or a service DNS name).
	RuntimeChannelDialAddr string `yaml:"runtime_channel_dial_addr"`

	// RuntimeEnv is retained only to parse legacy configuration and return
	// a useful isolation error. Every non-empty map is rejected and no item
	// is forwarded to hosted runtime containers.
	RuntimeEnv map[string]string `yaml:"runtime_env"`

	// Coverage holds the explicitly modeled settings for opt-in hosted
	// runtime coverage. Coverage settings are never sourced from RuntimeEnv.
	Coverage DockerCoverageConfig `yaml:"coverage"`

	// LabelPrefix is the docker label namespace used for traceability:
	//   <prefix>.runtime_id, <prefix>.user_id, <prefix>.name.
	// Defaults to "hushine.runtime".
	LabelPrefix string `yaml:"label_prefix"`
}

// DockerCoverageConfig controls the instrumented image and durable host
// output root used only when hosted runtime coverage is explicitly enabled.
type DockerCoverageConfig struct {
	Enabled            bool   `yaml:"enabled"`
	Image              string `yaml:"image"`
	OutputDir          string `yaml:"output_dir"`
	StopTimeoutSeconds int    `yaml:"stop_timeout_seconds"`
}

// ValidateRuntimeIsolation rejects legacy operator-provided environment
// variables and invalid typed coverage settings that would cross the hosted
// Runtime isolation boundary.
func (c ProvisioningConfig) ValidateRuntimeIsolation() error {
	if len(c.Docker.RuntimeEnv) > 0 {
		keys := make([]string, 0, len(c.Docker.RuntimeEnv))
		for key := range c.Docker.RuntimeEnv {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		return fmt.Errorf(
			"provisioning.docker.runtime_env is not supported for Runtime isolation; configure explicit Runtime fields instead: %s",
			strings.Join(keys, ", "),
		)
	}

	coverage := c.Docker.Coverage
	if !coverage.Enabled {
		return nil
	}
	if strings.TrimSpace(coverage.Image) == "" {
		return fmt.Errorf("provisioning.docker.coverage.image is required when coverage is enabled")
	}
	if strings.TrimSpace(coverage.OutputDir) == "" {
		return fmt.Errorf("provisioning.docker.coverage.output_dir is required when coverage is enabled")
	}
	if !filepath.IsAbs(coverage.OutputDir) {
		return fmt.Errorf("provisioning.docker.coverage.output_dir must be an absolute host path")
	}
	if coverage.StopTimeoutSeconds <= 0 {
		return fmt.Errorf("provisioning.docker.coverage.stop_timeout_seconds must be greater than zero")
	}
	return nil
}

// Default returns a baseline config so env-driven deployments can start
// even when config.yaml is missing.
func Default() *Config {
	logCfg := elog.DefaultConfig()
	logCfg.OutputDir = "./logs"
	logCfg.Tracing.ServiceName = "control-panel-service"
	if logCfg.Kafka.Topic == "" {
		logCfg.Kafka.Topic = "app-logs"
	}
	if logCfg.Kafka.TopicPrefix == "" {
		logCfg.Kafka.TopicPrefix = "app-logs"
	}
	return &Config{
		Server: ServerConfig{
			HTTPAddr: ":8082",
			GRPCAddr: ":50054",
		},
		RuntimeChannelServer: RuntimeChannelServerConfig{
			GRPCAddr: ":50055",
			TLS: RuntimeChannelServerTLSConfig{
				Enabled: true,
			},
			DependencyProfile: RuntimeDependencyProfileConfig{
				SchemaVersion:  1,
				Name:           "platform-python-3.13",
				Version:        "1.0.0",
				ContractSHA256: "8457b3c35618558fc8bfc74d4135b7eb52e00c33a8c9a49d202830f3fd5b62c5",
			},
		},
		Database: DatabaseConfig{
			Host:     "192.168.88.10",
			Port:     5432,
			User:     "postgres",
			Password: "postgres",
			DBName:   "control_panel",
			SSLMode:  "disable",
		},
		MarketData: MarketDataConfig{
			Host:                "192.168.88.10",
			Port:                5432,
			User:                "postgres",
			Password:            "postgres",
			Database:            "binance_{year}",
			SSLMode:             "disable",
			LiveDeliveryEnabled: false,
			KafkaBrokers:        []string{"192.168.88.10:19092"},
		},
		Dependencies: DependenciesConfig{
			PortfolioServiceGRPC: "127.0.0.1:50051",
			OrderServiceGRPC:     "127.0.0.1:50051",
		},
		RuntimePlatform: RuntimePlatformConfig{
			MaxTotalHostedRuntimes:       50,
			MaxTotalSelfHostedRuntimes:   100,
			DefaultPlanCode:              "pro",
			HeartbeatGraceSeconds:        30,
			DeathGraceSeconds:            300,
			BareRuntimeDeathGraceSeconds: 300,
			DebugBareRuntimeEnabled:      false,
			BareBootstrapIPAllowlist:     []string{"127.0.0.1/32"},
			BareCertificateTTL:           8 * time.Hour,
		},
		RuntimePlans: defaultPlans(),
		Provisioning: ProvisioningConfig{
			Image:                      "hushine/strategy-runtime:executor-dev",
			AdvertiseHost:              "127.0.0.1",
			PortRangeBase:              50100,
			PortRangeSize:              200,
			RegistrationTimeoutSeconds: 30,
			Profiles:                   defaultResourceProfiles(),
			Docker: DockerProvisioningConfig{
				Coverage: DockerCoverageConfig{
					Image:              "hushine/strategy-runtime:executor-coverage",
					StopTimeoutSeconds: 10,
				},
			},
		},
		Notification: NotificationConfig{
			Enabled: false,
			Kafka: NotificationKafkaConfig{
				Brokers:  []string{"192.168.88.10:19092"},
				Topic:    "notification.events",
				ClientID: "control-panel-service",
			},
		},
		Log: *logCfg,
	}
}

func defaultResourceProfiles() map[string]ResourceProfile {
	return map[string]ResourceProfile{
		"small":  {NanoCPUs: "0.5", MemoryMB: 512, PidsLimit: 256},
		"medium": {NanoCPUs: "1.0", MemoryMB: 1024, PidsLimit: 512},
		"large":  {NanoCPUs: "2.0", MemoryMB: 2048, PidsLimit: 1024},
	}
}

func defaultPlans() map[string]RuntimePlan {
	return map[string]RuntimePlan{
		"free": {
			MaxHostedRuntimes:               1,
			MaxSelfHostedRuntimes:           0,
			MaxRoutingEnabledRuntimes:       1,
			MaxConcurrentSessionsTotal:      2,
			MaxConcurrentSessionsPerRuntime: 2,
			AllowedResourceProfiles:         []string{"small"},
			AllowSelfHostedRuntime:          false,
			AllowIDEDebug:                   false,
		},
		"developer": {
			MaxHostedRuntimes:               2,
			MaxSelfHostedRuntimes:           2,
			MaxRoutingEnabledRuntimes:       3,
			MaxConcurrentSessionsTotal:      5,
			MaxConcurrentSessionsPerRuntime: 3,
			AllowedResourceProfiles:         []string{"small", "medium"},
			AllowSelfHostedRuntime:          true,
			AllowIDEDebug:                   false,
		},
		"pro": {
			MaxHostedRuntimes:               5,
			MaxSelfHostedRuntimes:           10,
			MaxRoutingEnabledRuntimes:       10,
			MaxConcurrentSessionsTotal:      20,
			MaxConcurrentSessionsPerRuntime: 5,
			AllowedResourceProfiles:         []string{"small", "medium", "large"},
			AllowSelfHostedRuntime:          true,
			AllowIDEDebug:                   true,
		},
	}
}

func (d DatabaseConfig) DSN() string {
	return fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		d.Host, d.Port, d.User, d.Password, d.DBName, d.SSLMode,
	)
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config %s: %w", path, err)
	}
	cfg := Default()
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parse config %s: %w", path, err)
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("validate config %s: %w", path, err)
	}
	return cfg, nil
}

func (c *Config) Validate() error {
	if c == nil {
		return errors.New("config is required")
	}
	if err := c.RuntimeChannelServer.DependencyProfile.Validate(); err != nil {
		return err
	}
	return c.Provisioning.ValidateRuntimeIsolation()
}

func (c *Config) ApplyEnvOverrides() error {
	if v := os.Getenv("SERVER_HTTP_ADDR"); v != "" {
		c.Server.HTTPAddr = v
	} else if v := os.Getenv("HTTP_ADDR"); v != "" {
		c.Server.HTTPAddr = v
	}
	if v := os.Getenv("SERVER_GRPC_ADDR"); v != "" {
		c.Server.GRPCAddr = v
	} else if v := os.Getenv("GRPC_ADDR"); v != "" {
		c.Server.GRPCAddr = v
	}
	if v := os.Getenv("RUNTIME_CHANNEL_SERVER_GRPC_ADDR"); v != "" {
		c.RuntimeChannelServer.GRPCAddr = v
	}
	if v := os.Getenv("RUNTIME_CHANNEL_SERVER_TLS_ENABLED"); v != "" {
		c.RuntimeChannelServer.TLS.Enabled = v == "1" || strings.EqualFold(v, "true") || strings.EqualFold(v, "yes")
	}
	if v := os.Getenv("RUNTIME_CHANNEL_SERVER_TLS_CERT_FILE"); v != "" {
		c.RuntimeChannelServer.TLS.CertFile = v
	}
	if v := os.Getenv("RUNTIME_CHANNEL_SERVER_TLS_KEY_FILE"); v != "" {
		c.RuntimeChannelServer.TLS.KeyFile = v
	}
	if v := os.Getenv("RUNTIME_CHANNEL_SERVER_TLS_SERVER_NAME"); v != "" {
		c.RuntimeChannelServer.TLS.ServerName = v
	}
	if v := os.Getenv("RUNTIME_CHANNEL_SERVER_TLS_CLIENT_CA_FILE"); v != "" {
		c.RuntimeChannelServer.TLS.ClientCAFile = v
	}
	if v := os.Getenv("RUNTIME_CHANNEL_SERVER_TLS_CLIENT_CA_KEY_FILE"); v != "" {
		c.RuntimeChannelServer.TLS.ClientCAKeyFile = v
	}
	if v, ok := os.LookupEnv("RUNTIME_DEPENDENCY_SCHEMA_VERSION"); ok {
		n, err := strconv.ParseUint(v, 10, 32)
		if err != nil {
			return fmt.Errorf("RUNTIME_DEPENDENCY_SCHEMA_VERSION must be a positive uint32: %w", err)
		}
		c.RuntimeChannelServer.DependencyProfile.SchemaVersion = uint32(n)
	}
	if v, ok := os.LookupEnv("RUNTIME_DEPENDENCY_PROFILE_NAME"); ok {
		c.RuntimeChannelServer.DependencyProfile.Name = v
	}
	if v, ok := os.LookupEnv("RUNTIME_DEPENDENCY_PROFILE_VERSION"); ok {
		c.RuntimeChannelServer.DependencyProfile.Version = v
	}
	if v, ok := os.LookupEnv("RUNTIME_DEPENDENCY_CONTRACT_SHA256"); ok {
		c.RuntimeChannelServer.DependencyProfile.ContractSHA256 = v
	}

	if dsn := os.Getenv("TIMESCALEDB_DSN"); dsn != "" {
		c.Database.parseDSN(dsn)
	}
	if v := os.Getenv("DATABASE_HOST"); v != "" {
		c.Database.Host = v
	}
	if v := os.Getenv("DATABASE_PORT"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			c.Database.Port = n
		}
	}
	if v := os.Getenv("DATABASE_USER"); v != "" {
		c.Database.User = v
	}
	if v := os.Getenv("DATABASE_PASSWORD"); v != "" {
		c.Database.Password = v
	}
	if v := os.Getenv("DATABASE_DBNAME"); v != "" {
		c.Database.DBName = v
	}
	if v := os.Getenv("DATABASE_SSLMODE"); v != "" {
		c.Database.SSLMode = v
	}
	if v := os.Getenv("MARKET_DATA_DB_HOST"); v != "" {
		c.MarketData.Host = v
	}
	if v := os.Getenv("MARKET_DATA_DB_PORT"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			c.MarketData.Port = n
		}
	}
	if v := os.Getenv("MARKET_DATA_DB_USER"); v != "" {
		c.MarketData.User = v
	}
	if v := os.Getenv("MARKET_DATA_DB_PASSWORD"); v != "" {
		c.MarketData.Password = v
	}
	if v := os.Getenv("MARKET_DATA_DB_DATABASE"); v != "" {
		c.MarketData.Database = v
	}
	if v := os.Getenv("MARKET_DATA_DB_SSLMODE"); v != "" {
		c.MarketData.SSLMode = v
	}
	if v := os.Getenv("MARKET_DATA_LIVE_DELIVERY_ENABLED"); v != "" {
		c.MarketData.LiveDeliveryEnabled = v == "1" || strings.EqualFold(v, "true") || strings.EqualFold(v, "yes")
	}
	if v := os.Getenv("MARKET_DATA_KAFKA_BROKERS"); v != "" {
		c.MarketData.KafkaBrokers = splitCSV(v)
	}
	if v := os.Getenv("NOTIFICATION_ENABLED"); v != "" {
		c.Notification.Enabled = v == "1" || strings.EqualFold(v, "true") || strings.EqualFold(v, "yes")
	}
	if v := os.Getenv("NOTIFICATION_KAFKA_BROKERS"); v != "" {
		c.Notification.Kafka.Brokers = splitCSV(v)
	}
	if v := os.Getenv("NOTIFICATION_KAFKA_TOPIC"); v != "" {
		c.Notification.Kafka.Topic = v
	}
	if v := os.Getenv("NOTIFICATION_KAFKA_CLIENT_ID"); v != "" {
		c.Notification.Kafka.ClientID = v
	}

	if v := os.Getenv("DEPENDENCIES_CORE_SERVICE_GRPC"); v != "" {
		c.Dependencies.PortfolioServiceGRPC = v
	} else if v := os.Getenv("CORE_SERVICE_GRPC_ADDR"); v != "" {
		c.Dependencies.PortfolioServiceGRPC = v
	}
	if v := os.Getenv("DEPENDENCIES_ORDER_SERVICE_GRPC"); v != "" {
		c.Dependencies.OrderServiceGRPC = v
	} else if v := os.Getenv("ORDER_SERVICE_GRPC_ADDR"); v != "" {
		c.Dependencies.OrderServiceGRPC = v
	}

	if v := os.Getenv("RUNTIME_PLATFORM_DEFAULT_PLAN_CODE"); v != "" {
		c.RuntimePlatform.DefaultPlanCode = v
	}
	if v := os.Getenv("RUNTIME_PLATFORM_DEBUG_BARE_RUNTIME_ENABLED"); v != "" {
		c.RuntimePlatform.DebugBareRuntimeEnabled = v == "1" || strings.EqualFold(v, "true") || strings.EqualFold(v, "yes")
	}
	if v := os.Getenv("RUNTIME_PLATFORM_BARE_BOOTSTRAP_IP_ALLOWLIST"); v != "" {
		c.RuntimePlatform.BareBootstrapIPAllowlist = splitCSV(v)
	}
	if v := os.Getenv("RUNTIME_PLATFORM_BARE_CERTIFICATE_TTL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			c.RuntimePlatform.BareCertificateTTL = d
		}
	}
	if v := os.Getenv("RUNTIME_PLATFORM_BARE_RUNTIME_DEATH_GRACE_SECONDS"); v != "" {
		if seconds, err := strconv.Atoi(v); err == nil && seconds > 0 {
			c.RuntimePlatform.BareRuntimeDeathGraceSeconds = seconds
		}
	}

	if v := os.Getenv("RUNTIME_COVERAGE_ENABLED"); v != "" {
		c.Provisioning.Docker.Coverage.Enabled = v == "1" || strings.EqualFold(v, "true") || strings.EqualFold(v, "yes")
	}
	if v := os.Getenv("RUNTIME_COVERAGE_OUTPUT_DIR"); v != "" {
		c.Provisioning.Docker.Coverage.OutputDir = v
	}
	if v := os.Getenv("RUNTIME_COVERAGE_IMAGE"); v != "" {
		c.Provisioning.Docker.Coverage.Image = v
	}
	return c.Validate()
}

func splitCSV(v string) []string {
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func (d *DatabaseConfig) parseDSN(dsn string) {
	for _, kv := range strings.Fields(dsn) {
		parts := strings.SplitN(kv, "=", 2)
		if len(parts) != 2 {
			continue
		}
		switch parts[0] {
		case "host":
			d.Host = parts[1]
		case "port":
			fmt.Sscanf(parts[1], "%d", &d.Port)
		case "user":
			d.User = parts[1]
		case "password":
			d.Password = parts[1]
		case "dbname":
			d.DBName = parts[1]
		case "sslmode":
			d.SSLMode = parts[1]
		}
	}
}
