package provision

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
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
	// ControlPanelGRPC is what the runtime container needs to dial to
	// self-register. Set at construction (the operator decides whether
	// that is "127.0.0.1:50054" for host networking, "control-panel:50054"
	// in a docker network, etc.).
	controlPanelGRPC string
}

// NewDockerProvisioner constructs a DockerProvisioner. Pass
// ExecCommandRunner{} in production; tests inject a stub.
func NewDockerProvisioner(runner CommandRunner, cfg config.ProvisioningConfig, controlPanelGRPC string) *DockerProvisioner {
	if runner == nil {
		runner = ExecCommandRunner{}
	}
	return &DockerProvisioner{
		runner:           runner,
		cfg:              cfg,
		controlPanelGRPC: controlPanelGRPC,
	}
}

// Provision starts a strategy-runtime container per `p`. The container
// is detached (`-d`), bound by name, labeled for traceability, and
// resource-limited via cgroup flags. On success returns the container
// ID; on failure returns the wrapped command output for diagnostics.
func (d *DockerProvisioner) Provision(ctx context.Context, p Plan) (string, error) {
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
	}

	// Per-runtime env vars consumed by strategy-service/run_grpc_server.py.
	// Hosted containers use outbound RuntimeChannel only. The runtime_id is
	// provided so the first HELLO binds to the id allocated by control-panel;
	// a later process restart still fails because the bootstrap credential is
	// one-time-use and the resume token is not persisted.
	args = append(args,
		"-e", "RUNTIME_INGRESS_MODE=outbound",
		"-e", fmt.Sprintf("RUNTIME_RUNTIME_ID=%s", p.RuntimeID),
		"-e", fmt.Sprintf("RUNTIME_NAME=%s", p.Name),
		"-e", fmt.Sprintf("RUNTIME_RESOURCE_PROFILE=%s", p.ResourceProfileName),
		"-e", fmt.Sprintf("CONTROL_PANEL_SERVICE_GRPC_ADDR=%s", d.controlPanelGRPC),
	)
	if p.RuntimeCredentialKeyID != "" && p.RuntimeCredentialPrivateKeyPEM != "" {
		credentialJSON, _ := json.Marshal(map[string]any{
			"version":         1,
			"key_id":          p.RuntimeCredentialKeyID,
			"private_key_pem": p.RuntimeCredentialPrivateKeyPEM,
		})
		args = append(args, "-e", "RUNTIME_CREDENTIAL_JSON="+string(credentialJSON))
	}

	// Operator-supplied static env (account-service / order-service /
	// kafka / timescaledb addresses, etc.).
	//
	// Platform-controlled keys are intentionally rejected here so a
	// misconfigured `runtime_env` cannot shadow per-runtime values
	// (RUNTIME_BIND_USER_ID=999 would otherwise pin every container to
	// user 999; CONTROL_PANEL_SERVICE_GRPC_ADDR=evil would redirect
	// self-register traffic).
	for k, v := range dc.RuntimeEnv {
		if isPlatformReservedEnvKey(k) {
			// We don't fail-fast on the operator's behalf — log via the
			// args themselves would leak through; instead skip silently.
			// A unit test asserts these keys never reach docker.
			continue
		}
		args = append(args, "-e", fmt.Sprintf("%s=%s", k, v))
	}

	args = append(args, d.cfg.Image)
	return args
}

// isPlatformReservedEnvKey identifies env-var keys whose values the
// platform owns. operators MUST NOT override them via runtime_env.
//
// Reserved set:
//   - RUNTIME_*                     — per-runtime identity / endpoint
//   - CONTROL_PANEL_SERVICE_GRPC_ADDR — runtime → control-panel dial target
//   - SERVER_GRPC_ADDR              — strategy-runtime listen address
func isPlatformReservedEnvKey(k string) bool {
	switch k {
	case "CONTROL_PANEL_SERVICE_GRPC_ADDR", "SERVER_GRPC_ADDR":
		return true
	}
	return strings.HasPrefix(k, "RUNTIME_")
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
