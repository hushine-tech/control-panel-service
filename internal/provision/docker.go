package provision

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
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
	{regexp.MustCompile(`(?is)(\b(?:private[_-]?key(?:_pem)?|client[_-]?key(?:_pem)?)\b["']?\s*[:=]\s*(?:[>|][+-]?\s*)?["']?\s*)-----BEGIN[ \t]+[A-Z0-9 _-]+-----.*?(?:-----END[ \t]+[A-Z0-9 _-]+-----|$)["']?`), "${1}<redacted>"},
	{regexp.MustCompile(`(?i)("?(?:api[_-]?secret|api[_-]?key|token|password|private[_-]?key(?:_pem)?|client[_-]?key(?:_pem)?)"?\s*:\s*)("[^"]*"|[^,\s}]+)`), "${1}<redacted>"},
	{regexp.MustCompile(`(?i)\b(api[_-]?secret|api[_-]?key|token|password|private[_-]?key(?:_pem)?|client[_-]?key(?:_pem)?)\b(\s*[:=]\s*)("[^"]*"|'[^']*'|[^\s|,;]+)`), "${1}${2}<redacted>"},
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

type dockerCoverageRun struct {
	hostRoot string
	runID    string
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

	var coverageRun *dockerCoverageRun
	if d.cfg.Docker.Coverage.Enabled {
		prepared, err := prepareDockerCoverageRun(d.cfg.Docker.Coverage.OutputDir, p.RuntimeID)
		if err != nil {
			return "", fmt.Errorf("%w: prepare coverage output: %v", ErrProvisionFailed, err)
		}
		coverageRun = &prepared
	}

	args := d.buildRunArgs(p, coverageRun)
	out, err := d.runner.Run(ctx, "docker", args...)
	if err != nil {
		output := strings.TrimSpace(string(out))
		if partialHandle := partialContainerHandleFromDockerRunOutput(output); partialHandle != "" {
			cleanupTimeout := d.DeprovisionTimeout()
			cleanupCtx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
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

// Deprovision removes the container. Coverage containers receive a bounded
// graceful stop first so instrumented processes can flush mounted output;
// forced removal is still attempted even when stopping fails.
func (d *DockerProvisioner) Deprovision(ctx context.Context, handle string) error {
	if handle == "" {
		return errors.New("deprovision: empty handle")
	}
	labelPrefix := strings.TrimSpace(d.cfg.Docker.LabelPrefix)
	if labelPrefix == "" {
		labelPrefix = "hushine.runtime"
	}
	inspectCtx, cancelInspect := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	inspectOut, inspectErr := d.runner.Run(
		inspectCtx,
		"docker",
		"inspect",
		"--format",
		fmt.Sprintf(`{{index .Config.Labels %q}}`, labelPrefix+".coverage"),
		handle,
	)
	cancelInspect()
	coverageFact := strings.ToLower(strings.TrimSpace(string(inspectOut)))
	stopFirst := inspectErr != nil || coverageFact == "true"
	var factErr error
	if inspectErr != nil {
		factErr = fmt.Errorf("inspect container coverage fact: %w", inspectErr)
	} else if coverageFact != "" && coverageFact != "<no value>" && coverageFact != "false" && coverageFact != "true" {
		stopFirst = true
		factErr = fmt.Errorf("inspect container coverage fact: unexpected label value")
	}

	var stopErr error
	if stopFirst {
		stopSeconds := d.coverageStopTimeoutSeconds()
		stopCtx, cancelStop := context.WithTimeout(context.WithoutCancel(ctx), time.Duration(stopSeconds+2)*time.Second)
		_, stopErr = d.runner.Run(stopCtx, "docker", "stop", "--time", strconv.Itoa(stopSeconds), handle)
		cancelStop()
	}
	removeCtx, cancelRemove := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	_, removeErr := d.runner.Run(removeCtx, "docker", "rm", "-f", handle)
	cancelRemove()
	return errors.Join(factErr, stopErr, removeErr)
}

func (d *DockerProvisioner) DeprovisionTimeout() time.Duration {
	return time.Duration(d.coverageStopTimeoutSeconds()+17) * time.Second
}

func (d *DockerProvisioner) coverageStopTimeoutSeconds() int {
	if seconds := d.cfg.Docker.Coverage.StopTimeoutSeconds; seconds > 0 {
		return seconds
	}
	return 10
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

// buildRunArgs assembles the `docker run` argument list. The optional coverage
// run contains only paths and labels prepared by the provisioner.
func (d *DockerProvisioner) buildRunArgs(p Plan, coverageRun *dockerCoverageRun) []string {
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
	if coverageRun != nil {
		args = append(args,
			"--label", fmt.Sprintf("%s.coverage=true", labelPrefix),
			"--label", fmt.Sprintf("%s.coverage_run_id=%s", labelPrefix, coverageRun.runID),
		)
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
	if coverageRun != nil {
		args = append(args, "--mount", "type=bind,src="+coverageRun.hostRoot+",dst=/coverage")
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
	if coverageRun != nil {
		args = append(args,
			"-e", "GOCOVERDIR=/coverage/go",
			"-e", "HUSHINE_RUNTIME_COVERAGE_DIR=/coverage",
		)
	}
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

	image := d.cfg.Image
	if coverageRun != nil {
		image = d.cfg.Docker.Coverage.Image
	}
	args = append(args, image)
	return args
}

func prepareDockerCoverageRun(outputDir, runtimeID string) (dockerCoverageRun, error) {
	if !isSafePathComponent(runtimeID) {
		return dockerCoverageRun{}, fmt.Errorf("runtime_id %q must be one safe path component", runtimeID)
	}

	root := filepath.Clean(outputDir)
	runID, err := dockerCoverageRunID(root)
	if err != nil {
		return dockerCoverageRun{}, err
	}
	runtimeRoot := filepath.Join(root, "runtimes", runtimeID)
	if err := requirePathWithin(root, runtimeRoot); err != nil {
		return dockerCoverageRun{}, fmt.Errorf("runtime coverage path: %w", err)
	}

	if err := os.MkdirAll(root, 0o700); err != nil {
		return dockerCoverageRun{}, fmt.Errorf("create coverage root: %w", err)
	}
	if err := makeCoverageDirectory(root); err != nil {
		return dockerCoverageRun{}, err
	}
	resolvedRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		return dockerCoverageRun{}, fmt.Errorf("resolve coverage root: %w", err)
	}
	for _, path := range []string{
		filepath.Join(root, "runtimes"),
		runtimeRoot,
		filepath.Join(runtimeRoot, "go"),
		filepath.Join(runtimeRoot, "python"),
	} {
		if err := makeCoverageDirectory(path); err != nil {
			return dockerCoverageRun{}, err
		}
		resolvedPath, err := filepath.EvalSymlinks(path)
		if err != nil {
			return dockerCoverageRun{}, fmt.Errorf("resolve coverage directory %q: %w", path, err)
		}
		if err := requirePathWithin(resolvedRoot, resolvedPath); err != nil {
			return dockerCoverageRun{}, fmt.Errorf("coverage directory %q: %w", path, err)
		}
	}

	resolvedRuntimeRoot, err := filepath.EvalSymlinks(runtimeRoot)
	if err != nil {
		return dockerCoverageRun{}, fmt.Errorf("resolve runtime coverage directory: %w", err)
	}
	if err := requirePathWithin(resolvedRoot, resolvedRuntimeRoot); err != nil {
		return dockerCoverageRun{}, fmt.Errorf("runtime coverage directory: %w", err)
	}
	// Every managed parent is mode 0700 before this canonical path is returned,
	// preventing untrusted users from replacing descendants. Returning the
	// resolved path also makes a lexical ancestor symlink unable to retarget the
	// later Docker bind mount.
	return dockerCoverageRun{hostRoot: resolvedRuntimeRoot, runID: runID}, nil
}

func dockerCoverageRunID(outputDir string) (string, error) {
	cleaned := filepath.Clean(outputDir)
	runID := filepath.Base(cleaned)
	if runID == "runtime-agent" && filepath.Base(filepath.Dir(cleaned)) == "coverage" {
		runID = filepath.Base(filepath.Dir(filepath.Dir(cleaned)))
	}
	if !isSafePathComponent(runID) {
		return "", fmt.Errorf("cannot derive a safe coverage run id from output directory")
	}
	return runID, nil
}

func isSafePathComponent(value string) bool {
	return value != "" &&
		value != "." &&
		value != ".." &&
		!filepath.IsAbs(value) &&
		filepath.Clean(value) == value &&
		filepath.Base(value) == value &&
		!strings.ContainsAny(value, `/\\`)
}

func makeCoverageDirectory(path string) error {
	err := os.Mkdir(path, 0o700)
	if err != nil && !errors.Is(err, os.ErrExist) {
		return fmt.Errorf("create coverage directory %q: %w", path, err)
	}
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("inspect coverage directory %q: %w", path, err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("coverage directory %q must not be a symlink", path)
	}
	if !info.IsDir() {
		return fmt.Errorf("coverage path %q is not a directory", path)
	}
	if err := os.Chmod(path, 0o700); err != nil {
		return fmt.Errorf("secure coverage directory %q: %w", path, err)
	}
	return nil
}

func requirePathWithin(root, path string) error {
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return fmt.Errorf("compare with root: %w", err)
	}
	if filepath.IsAbs(rel) || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return fmt.Errorf("path escapes configured coverage root")
	}
	return nil
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
