package provision

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/hushine-tech/control-panel-service/internal/config"
)

// CommandRunner is the os/exec seam the DockerProvisioner uses. Tests
// inject a stub that records what would have been executed; production
// uses ExecCommandRunner.
type CommandRunner interface {
	// Run executes `name args...` and returns the combined stdout+stderr
	// + the exit error if any.
	Run(ctx context.Context, name string, args ...string) ([]byte, error)
}

var diagnosticsSensitivePatterns = []struct {
	pattern     *regexp.Regexp
	replacement string
}{
	{regexp.MustCompile(`(?i)("?(?:api[_-]?secret|api[_-]?key|token|password|private[_-]?key(?:_pem)?)"?\s*:\s*)("[^"]*"|[^,\s}]+)`), "${1}<redacted>"},
	{regexp.MustCompile(`(?i)\b(api[_-]?secret|api[_-]?key|token|password|private[_-]?key(?:_pem)?)\b(\s*[:=]\s*)("[^"]*"|'[^']*'|[^\s|,;]+)`), "${1}${2}<redacted>"},
}

func redactDiagnostics(text string) string {
	for _, item := range diagnosticsSensitivePatterns {
		text = item.pattern.ReplaceAllString(text, item.replacement)
	}
	return text
}

// ExecCommandRunner is the real os/exec backend.
type ExecCommandRunner struct{}

func (ExecCommandRunner) Run(ctx context.Context, name string, args ...string) ([]byte, error) {
	return exec.CommandContext(ctx, name, args...).CombinedOutput()
}

// DockerProvisioner spins up hosted strategy-runtime containers via
// `docker run` invoked through CommandRunner. D1 scope: single-host
// hosted runtime; multi-host placement and image registry pull are out
// of scope. The image is assumed pre-built (see
// `strategy-service/scripts/build_strategy_runtime.sh`).
//
// The provisioner returns the container ID (long form) as the handle
// the service layer carries forward; Deprovision uses it to call
// `docker rm -f`.
type DockerProvisioner struct {
	runner CommandRunner
	cfg    config.ProvisioningConfig
	// RuntimeChannelGRPC is what the runtime container needs to dial for
	// RuntimeChannel. Set at construction (the operator decides whether
	// that is "127.0.0.1:50055" for host networking, "control-panel:50055"
	// in a docker network, etc.).
	runtimeChannelGRPC string
}

// NewDockerProvisioner constructs a DockerProvisioner. Pass
// ExecCommandRunner{} in production; tests inject a stub.
func NewDockerProvisioner(runner CommandRunner, cfg config.ProvisioningConfig, runtimeChannelGRPC string) *DockerProvisioner {
	if runner == nil {
		runner = ExecCommandRunner{}
	}
	return &DockerProvisioner{
		runner:             runner,
		cfg:                cfg,
		runtimeChannelGRPC: runtimeChannelGRPC,
	}
}

// Provision starts a strategy-runtime container per `p`. The container
// is detached (`-d`), bound by name, labeled for traceability, and
// resource-limited via cgroup flags. On success returns the container
// ID; on failure returns the wrapped command output for diagnostics.
func (d *DockerProvisioner) Provision(ctx context.Context, p Plan) (string, error) {
	if err := d.cfg.ValidateRuntimeIsolation(); err != nil {
		return "", fmt.Errorf("%w: %v", ErrProvisionFailed, err)
	}
	if d.cfg.Image == "" {
		return "", fmt.Errorf("%w: provisioning.image is empty", ErrNotConfigured)
	}
	if p.RuntimeID == "" || p.UserID <= 0 || p.Name == "" {
		return "", fmt.Errorf("%w: incomplete Plan (runtime_id / user_id / name required)", ErrProvisionFailed)
	}

	args := d.buildRunArgs(p)
	out, err := d.runner.Run(ctx, "docker", args...)
	if err != nil {
		output := strings.TrimSpace(string(out))
		if partialHandle := partialContainerHandleFromDockerRunOutput(output); partialHandle != "" {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			cleanupErr := d.Deprovision(cleanupCtx, partialHandle)
			cancel()
			if cleanupErr != nil {
				return "", fmt.Errorf("%w: docker run failed: %v: %s; cleanup partial container %s failed: %v", ErrProvisionFailed, err, output, partialHandle, cleanupErr)
			}
		}
		return "", fmt.Errorf("%w: docker run failed: %v: %s", ErrProvisionFailed, err, output)
	}
	containerID := strings.TrimSpace(string(out))
	if containerID == "" {
		return "", fmt.Errorf("%w: docker run returned empty container id", ErrProvisionFailed)
	}
	return containerID, nil
}

// Deprovision removes the container via `docker rm -f`. Best-effort:
// the service layer ignores the error so a stale half-started container
// surfaces only as a log line.
func (d *DockerProvisioner) Deprovision(ctx context.Context, handle string) error {
	if handle == "" {
		return errors.New("deprovision: empty handle")
	}
	_, err := d.runner.Run(ctx, "docker", "rm", "-f", handle)
	return err
}

// Diagnostics returns a compact snapshot of a started container. It is used
// when RuntimeChannel registration times out so the API error can point at the
// real process failure instead of only saying "waited 2m".
func (d *DockerProvisioner) Diagnostics(ctx context.Context, handle string) (string, error) {
	if handle == "" {
		return "", errors.New("diagnostics: empty handle")
	}
	inspectOut, inspectErr := d.runner.Run(ctx, "docker", "inspect",
		"--format", "state={{.State.Status}} exit={{.State.ExitCode}} error={{.State.Error}} started={{.State.StartedAt}} finished={{.State.FinishedAt}}",
		handle,
	)
	logsOut, logsErr := d.runner.Run(ctx, "docker", "logs", "--tail", "80", handle)

	var parts []string
	if text := strings.TrimSpace(string(inspectOut)); text != "" {
		parts = append(parts, "inspect: "+text)
	}
	if inspectErr != nil {
		parts = append(parts, "inspect_error: "+inspectErr.Error())
	}
	if text := strings.TrimSpace(string(logsOut)); text != "" {
		parts = append(parts, "logs: "+text)
	}
	if logsErr != nil {
		parts = append(parts, "logs_error: "+logsErr.Error())
	}
	if len(parts) == 0 {
		return "", nil
	}
	out := redactDiagnostics(strings.Join(parts, " | "))
	if len(out) > 4000 {
		out = out[:4000] + "...<truncated>"
	}
	return out, nil
}

// buildRunArgs assembles the `docker run` argument list. Exposed for
// unit tests so they can assert on the exact command shape without
// running real docker.
func (d *DockerProvisioner) buildRunArgs(p Plan) []string {
	dc := d.cfg.Docker
	labelPrefix := dc.LabelPrefix
	if labelPrefix == "" {
		labelPrefix = "hushine.runtime"
	}
	networkMode := dc.NetworkMode
	if networkMode == "" {
		networkMode = "host"
	}

	containerName := fmt.Sprintf("hushine-runtime-%s", p.RuntimeID)

	args := []string{
		"run",
		"-d",
		"--name", containerName,
		"--label", fmt.Sprintf("%s.runtime_id=%s", labelPrefix, p.RuntimeID),
		"--label", fmt.Sprintf("%s.user_id=%d", labelPrefix, p.UserID),
		"--label", fmt.Sprintf("%s.name=%s", labelPrefix, p.Name),
		"--label", fmt.Sprintf("%s.resource_profile=%s", labelPrefix, p.ResourceProfileName),
	}

	// Resource limits. Skip empty/zero so docker uses its default.
	if p.Limits.NanoCPUs != "" {
		args = append(args, "--cpus", p.Limits.NanoCPUs)
	}
	if p.Limits.MemoryMB > 0 {
		args = append(args, "--memory", fmt.Sprintf("%dm", p.Limits.MemoryMB))
	}
	if p.Limits.PidsLimit > 0 {
		args = append(args, "--pids-limit", strconv.Itoa(p.Limits.PidsLimit))
	}

	// Runtime session traffic is RuntimeChannel/proxy-only. Hosted
	// containers do not publish strategy gRPC ports in any network mode.
	if networkMode == "host" {
		args = append(args, "--network", "host")
	} else {
		args = append(args, "--network", networkMode)
		if needsDockerHostGateway(d.runtimeChannelGRPC) {
			args = append(args, "--add-host", "host.docker.internal:host-gateway")
		}
	}

	// Per-runtime env vars consumed by the Go runtime-agent.
	// Hosted containers use RuntimeChannel only. The runtime_id is
	// provided so the first HELLO binds to the id allocated by control-panel;
	// a later process restart still fails because the bootstrap credential is
	// one-time-use and the resume token is not persisted.
	args = append(args,
		"-e", "RUNTIME_SOURCE=hosted",
		"-e", fmt.Sprintf("RUNTIME_RUNTIME_ID=%s", p.RuntimeID),
		"-e", fmt.Sprintf("RUNTIME_NAME=%s", p.Name),
		"-e", fmt.Sprintf("RUNTIME_RESOURCE_PROFILE=%s", p.ResourceProfileName),
		"-e", fmt.Sprintf("RUNTIME_CHANNEL_GRPC_ADDR=%s", d.runtimeChannelGRPC),
	)
	if p.RuntimeCredentialKeyID != "" && p.RuntimeCredentialPrivateKeyPEM != "" {
		credentialJSON, _ := json.Marshal(map[string]any{
			"version":         1,
			"key_id":          p.RuntimeCredentialKeyID,
			"private_key_pem": p.RuntimeCredentialPrivateKeyPEM,
		})
		args = append(args, "-e", "RUNTIME_CREDENTIAL_JSON="+string(credentialJSON))
	}
	if p.RuntimeClientCertPEM != "" && p.RuntimeClientKeyPEM != "" && p.RuntimeServerCAPEM != "" {
		bundleJSON, _ := json.Marshal(map[string]string{
			"client_cert_pem": p.RuntimeClientCertPEM,
			"client_key_pem":  p.RuntimeClientKeyPEM,
			"server_ca_pem":   p.RuntimeServerCAPEM,
		})
		args = append(args,
			"-e", "RUNTIME_CHANNEL_TLS_ENABLED=true",
			"-e", "RUNTIME_CHANNEL_TLS_BUNDLE_JSON="+string(bundleJSON),
		)
		if p.RuntimeChannelTLSServerName != "" {
			args = append(args, "-e", "RUNTIME_CHANNEL_TLS_SERVER_NAME="+p.RuntimeChannelTLSServerName)
		}
	}

	args = append(args, d.cfg.Image)
	return args
}

func needsDockerHostGateway(addr string) bool {
	host := strings.TrimSpace(addr)
	if i := strings.LastIndex(host, ":"); i >= 0 {
		host = host[:i]
	}
	host = strings.Trim(host, "[]")
	return strings.EqualFold(host, "host.docker.internal")
}

func partialContainerHandleFromDockerRunOutput(output string) string {
	for _, field := range strings.Fields(output) {
		candidate := strings.Trim(field, "\"'`")
		if looksLikeContainerID(candidate) {
			return candidate
		}
	}
	return ""
}

func looksLikeContainerID(s string) bool {
	if len(s) < 12 || len(s) > 64 {
		return false
	}
	for _, r := range s {
		if (r >= '0' && r <= '9') || (r >= 'a' && r <= 'f') {
			continue
		}
		return false
	}
	return true
}
