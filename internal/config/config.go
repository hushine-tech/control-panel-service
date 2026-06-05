package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	elog "github.com/hushine-tech/golang-lib/pkg/log"
	"gopkg.in/yaml.v3"
)

type Config struct {
	Server          ServerConfig           `yaml:"server"`
	Database        DatabaseConfig         `yaml:"database"`
	MarketData      MarketDataConfig       `yaml:"market_data"`
	Dependencies    DependenciesConfig     `yaml:"dependencies"`
	RuntimePlatform RuntimePlatformConfig  `yaml:"runtime_platform"`
	RuntimePlans    map[string]RuntimePlan `yaml:"runtime_plans"`
	Provisioning    ProvisioningConfig     `yaml:"provisioning"`
	Notification    NotificationConfig     `yaml:"notification"`
	Log             elog.Config            `yaml:"log"`
}

type ServerConfig struct {
	HTTPAddr string `yaml:"http_addr"`
	GRPCAddr string `yaml:"grpc_addr"`
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
	AccountServiceGRPC string `yaml:"account_service_grpc"`
	OrderServiceGRPC   string `yaml:"order_service_grpc"`
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
	// CallerTokenTTLSeconds is the lifetime of caller_token issued on each
	// ResolveRuntimeRoute. Falls back to 60 if unset.
	CallerTokenTTLSeconds int `yaml:"caller_token_ttl_seconds"`
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
// container image to start, and the host:port range to advertise back to
// quant-handler. D1 hosted-only single-host operation; multi-host runtime
// placement is out of scope.
type ProvisioningConfig struct {
	// Backend selects the provisioner implementation:
	//   ""      → NoOpProvisioner (default; EnsureHostedRuntime fails closed)
	//   "docker" → DockerProvisioner (Phase D1 section 5.5)
	// Other backends (k8s, nomad) are out of D1 scope.
	Backend string `yaml:"backend"`

	// Image is the container image strategy-runtime is launched from.
	// Defaults to "hushine/strategy-runtime:executor-dev".
	Image string `yaml:"image"`

	// AdvertiseHost is the value control-panel-service stores as
	// runtime_registry.endpoint_host. quant-handler dials this directly.
	// Single-host development: "127.0.0.1". Cluster: the routable LAN IP.
	AdvertiseHost string `yaml:"advertise_host"`

	// PortRangeBase / PortRangeSize define the gRPC port pool the
	// provisioner allocates from. Default base=50100 size=200.
	PortRangeBase int `yaml:"port_range_base"`
	PortRangeSize int `yaml:"port_range_size"`

	// RegistrationTimeoutSeconds is how long EnsureHostedRuntime waits
	// for a freshly-provisioned runtime to call RegisterRuntime back via
	// its self-register code (Phase D1 section 4). Defaults to 30s.
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
	// host default is "host" so the container can reach
	// core-service / order-service / kafka / timescaledb on
	// localhost without extra config. Empty string falls back to "host"
	// for single-host dev. Cluster operators set "bridge" or a custom
	// network and use RuntimeEnv to point at routable addresses.
	NetworkMode string `yaml:"network_mode"`

	// ControlPanelDialAddr is the value the runtime container should use
	// to dial control-panel-service for self-registration + heartbeat.
	// Distinct from `cfg.Server.GRPCAddr` because that is the BIND
	// address (e.g. ":50054") and not a valid dial target.
	//
	// Defaults: host networking → "127.0.0.1:50054". Bridge / custom
	// networks → operator MUST set explicitly (e.g.
	// "host.docker.internal:50054" or a service DNS name).
	ControlPanelDialAddr string `yaml:"control_panel_dial_addr"`

	// RuntimeEnv is a static map of env vars forwarded to every runtime
	// container as `-e KEY=VALUE`. Used for upstream service addresses
	// (CORE_SERVICE_GRPC_ADDR, ORDER_SERVICE_GRPC_ADDR, KAFKA_BROKERS,
	// TIMESCALEDB_DSN, etc.) that the runtime needs but the platform
	// doesn't generate per-runtime.
	RuntimeEnv map[string]string `yaml:"runtime_env"`

	// LabelPrefix is the docker label namespace used for traceability:
	//   <prefix>.runtime_id, <prefix>.user_id, <prefix>.name.
	// Defaults to "hushine.runtime".
	LabelPrefix string `yaml:"label_prefix"`

	// RuntimeUserGRPCPort is the in-container gRPC port strategy-runtime
	// listens on. Mapped to the per-runtime allocated host port via
	// `-p HOST:CONTAINER`. Defaults to 50053 (the strategy-service
	// default in run_grpc_server.py).
	RuntimeUserGRPCPort int `yaml:"runtime_user_grpc_port"`
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
			AccountServiceGRPC: "127.0.0.1:50051",
			OrderServiceGRPC:   "127.0.0.1:50051",
		},
		RuntimePlatform: RuntimePlatformConfig{
			MaxTotalHostedRuntimes:     50,
			MaxTotalSelfHostedRuntimes: 100,
			DefaultPlanCode:            "pro",
			HeartbeatGraceSeconds:      30,
			DeathGraceSeconds:          300,
			CallerTokenTTLSeconds:      60,
		},
		RuntimePlans: defaultPlans(),
		Provisioning: ProvisioningConfig{
			Image:                      "hushine/strategy-runtime:executor-dev",
			AdvertiseHost:              "127.0.0.1",
			PortRangeBase:              50100,
			PortRangeSize:              200,
			RegistrationTimeoutSeconds: 30,
			Profiles:                   defaultResourceProfiles(),
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
	return cfg, nil
}

func (c *Config) ApplyEnvOverrides() {
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
		c.Dependencies.AccountServiceGRPC = v
	} else if v := os.Getenv("CORE_SERVICE_GRPC_ADDR"); v != "" {
		c.Dependencies.AccountServiceGRPC = v
	}
	if v := os.Getenv("DEPENDENCIES_ORDER_SERVICE_GRPC"); v != "" {
		c.Dependencies.OrderServiceGRPC = v
	} else if v := os.Getenv("ORDER_SERVICE_GRPC_ADDR"); v != "" {
		c.Dependencies.OrderServiceGRPC = v
	}

	if v := os.Getenv("RUNTIME_PLATFORM_DEFAULT_PLAN_CODE"); v != "" {
		c.RuntimePlatform.DefaultPlanCode = v
	}
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
