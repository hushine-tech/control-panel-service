package provision

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hushine-tech/control-panel-service/internal/config"
)

// fakeRunner is a CommandRunner stub: records each call and returns a
// canned output / error.
type fakeRunner struct {
	calls    []fakeRunnerCall
	output   []byte
	err      error
	multiOut [][]byte // indexed per call when more than one is expected
	multiErr []error
	onRun    func(callIndex int)
}

type fakeRunnerCall struct {
	name        string
	args        []string
	contextErr  error
	deadline    time.Time
	hasDeadline bool
}

func (f *fakeRunner) Run(ctx context.Context, name string, args ...string) ([]byte, error) {
	idx := len(f.calls)
	deadline, hasDeadline := ctx.Deadline()
	f.calls = append(f.calls, fakeRunnerCall{
		name:        name,
		args:        append([]string(nil), args...),
		contextErr:  ctx.Err(),
		deadline:    deadline,
		hasDeadline: hasDeadline,
	})
	if f.onRun != nil {
		f.onRun(idx)
	}
	if idx < len(f.multiOut) {
		var err error
		if idx < len(f.multiErr) {
			err = f.multiErr[idx]
		}
		return f.multiOut[idx], err
	}
	return f.output, f.err
}

func defaultPlan() Plan {
	return Plan{
		RuntimeID:           "rt_abc123",
		UserID:              42,
		Name:                "hosted-steady-river",
		EndpointHost:        "127.0.0.1",
		GRPCPort:            50142,
		Image:               "hushine/strategy-runtime:executor-dev",
		ResourceProfileName: "small",
		Limits:              config.ResourceProfile{NanoCPUs: "0.5", MemoryMB: 512, PidsLimit: 256},
		Capabilities:        []string{"strategy", "spot", "futures"},
	}
}

func defaultCfg() config.ProvisioningConfig {
	return config.ProvisioningConfig{
		Backend:       "docker",
		Image:         "hushine/strategy-runtime:executor-dev",
		AdvertiseHost: "127.0.0.1",
		Docker: config.DockerProvisioningConfig{
			NetworkMode: "host",
			LabelPrefix: "hushine.runtime",
			RuntimeEnv:  map[string]string{},
		},
	}
}

func TestDockerProvisioner_Provision_BuildsExpectedRunArgs(t *testing.T) {
	runner := &fakeRunner{output: []byte("container_full_id_abc\n")}
	prov := NewDockerProvisioner(runner, defaultCfg(), "127.0.0.1:50055")

	handle, err := prov.Provision(context.Background(), defaultPlan())
	if err != nil {
		t.Fatalf("Provision: %v", err)
	}
	if handle != "container_full_id_abc" {
		t.Errorf("handle = %q, want container_full_id_abc (trimmed)", handle)
	}
	if len(runner.calls) != 1 {
		t.Fatalf("runner calls = %d, want 1", len(runner.calls))
	}
	call := runner.calls[0]
	if call.name != "docker" {
		t.Errorf("called %q, want docker", call.name)
	}
	args := call.args
	// First positional must be "run".
	if len(args) == 0 || args[0] != "run" {
		t.Fatalf("args[0] = %v, want run", args)
	}
	// Image is always the last arg.
	if args[len(args)-1] != "hushine/strategy-runtime:executor-dev" {
		t.Errorf("last arg = %q, want image", args[len(args)-1])
	}
	// Detached mode required so the call returns the container id.
	if !contains(args, "-d") {
		t.Error("missing -d flag")
	}
	// Resource limits applied.
	assertFlagValue(t, args, "--cpus", "0.5")
	assertFlagValue(t, args, "--memory", "512m")
	assertFlagValue(t, args, "--pids-limit", "256")
	// Runtime session traffic uses RuntimeChannel; no published runtime port.
	assertFlagValue(t, args, "--network", "host")
	if contains(args, "-p") {
		t.Error("hosted RuntimeChannel proxy mode should not produce -p mapping")
	}
	// Per-runtime env vars.
	assertHasEnv(t, args, "RUNTIME_SOURCE=hosted")
	assertHasEnv(t, args, "RUNTIME_RUNTIME_ID=rt_abc123")
	assertHasEnv(t, args, "RUNTIME_NAME=hosted-steady-river")
	assertHasEnv(t, args, "RUNTIME_RESOURCE_PROFILE=small")
	assertHasEnv(t, args, "RUNTIME_CHANNEL_GRPC_ADDR=127.0.0.1:50055")
	wantEnvKeys := map[string]struct{}{
		"RUNTIME_SOURCE":            {},
		"RUNTIME_RUNTIME_ID":        {},
		"RUNTIME_NAME":              {},
		"RUNTIME_RESOURCE_PROFILE":  {},
		"RUNTIME_CHANNEL_GRPC_ADDR": {},
	}
	gotEnvKeys := envKeys(args)
	if len(gotEnvKeys) != len(wantEnvKeys) {
		t.Fatalf("hosted Runtime env keys = %v, want exactly %v", gotEnvKeys, wantEnvKeys)
	}
	for key := range gotEnvKeys {
		if _, ok := wantEnvKeys[key]; !ok {
			t.Fatalf("unmodeled hosted Runtime env key present: %s", key)
		}
	}
	// Labels for traceability.
	assertHasLabel(t, args, "hushine.runtime.runtime_id=rt_abc123")
	assertHasLabel(t, args, "hushine.runtime.user_id=42")
	assertHasLabel(t, args, "hushine.runtime.name=hosted-steady-river")
	assertHasLabel(t, args, "hushine.runtime.resource_profile=small")
}

func TestDockerProvisioner_Provision_CoverageDisabledPreservesExactRunArgs(t *testing.T) {
	runner := &fakeRunner{output: []byte("container_full_id_abc\n")}
	prov := NewDockerProvisioner(runner, defaultCfg(), "127.0.0.1:50055")

	if _, err := prov.Provision(context.Background(), defaultPlan()); err != nil {
		t.Fatalf("Provision: %v", err)
	}

	want := []string{
		"run",
		"-d",
		"--name", "hushine-runtime-rt_abc123",
		"--label", "hushine.runtime.runtime_id=rt_abc123",
		"--label", "hushine.runtime.user_id=42",
		"--label", "hushine.runtime.name=hosted-steady-river",
		"--label", "hushine.runtime.resource_profile=small",
		"--cpus", "0.5",
		"--memory", "512m",
		"--pids-limit", "256",
		"--network", "host",
		"-e", "RUNTIME_SOURCE=hosted",
		"-e", "RUNTIME_RUNTIME_ID=rt_abc123",
		"-e", "RUNTIME_NAME=hosted-steady-river",
		"-e", "RUNTIME_RESOURCE_PROFILE=small",
		"-e", "RUNTIME_CHANNEL_GRPC_ADDR=127.0.0.1:50055",
		"hushine/strategy-runtime:executor-dev",
	}
	if got := runner.calls[0].args; !equalStringSlice(got, want) {
		t.Fatalf("docker args = %v, want exact disabled args %v", got, want)
	}
}

func TestDockerProvisioner_Provision_CoverageBuildsExactRunArgsAndDirectories(t *testing.T) {
	outputDir := filepath.Join(t.TempDir(), "manual-coverage-run", "coverage", "runtime-agent")
	runner := &fakeRunner{output: []byte("coverage_container_id\n")}
	prov := NewDockerProvisioner(runner, coverageCfg(outputDir), "127.0.0.1:50055")
	plan := defaultPlan()
	plan.RuntimeID = "rt-123"

	handle, err := prov.Provision(context.Background(), plan)
	if err != nil {
		t.Fatalf("Provision: %v", err)
	}
	if handle != "coverage_container_id" {
		t.Fatalf("handle = %q, want coverage_container_id", handle)
	}

	runtimeRoot := filepath.Join(outputDir, "runtimes", "rt-123")
	canonicalRuntimeRoot, err := filepath.EvalSymlinks(runtimeRoot)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"run",
		"-d",
		"--name", "hushine-runtime-rt-123",
		"--label", "hushine.runtime.runtime_id=rt-123",
		"--label", "hushine.runtime.user_id=42",
		"--label", "hushine.runtime.name=hosted-steady-river",
		"--label", "hushine.runtime.resource_profile=small",
		"--label", "hushine.runtime.coverage=true",
		"--label", "hushine.runtime.coverage_run_id=manual-coverage-run",
		"--cpus", "0.5",
		"--memory", "512m",
		"--pids-limit", "256",
		"--network", "host",
		"--mount", "type=bind,src=" + canonicalRuntimeRoot + ",dst=/coverage",
		"-e", "RUNTIME_SOURCE=hosted",
		"-e", "RUNTIME_RUNTIME_ID=rt-123",
		"-e", "RUNTIME_NAME=hosted-steady-river",
		"-e", "RUNTIME_RESOURCE_PROFILE=small",
		"-e", "RUNTIME_CHANNEL_GRPC_ADDR=127.0.0.1:50055",
		"-e", "GOCOVERDIR=/coverage/go",
		"-e", "HUSHINE_RUNTIME_COVERAGE_DIR=/coverage",
		"hushine/strategy-runtime:test-cover",
	}
	if got := runner.calls[0].args; !equalStringSlice(got, want) {
		t.Fatalf("docker args = %v, want exact coverage args %v", got, want)
	}

	for _, path := range []string{runtimeRoot, filepath.Join(runtimeRoot, "go"), filepath.Join(runtimeRoot, "python")} {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("Stat(%q): %v", path, err)
		}
		if !info.IsDir() {
			t.Fatalf("coverage path %q is not a directory", path)
		}
		if got, want := info.Mode().Perm(), os.FileMode(0o700); got != want {
			t.Fatalf("coverage path %q mode = %o, want %o", path, got, want)
		}
	}
}

func TestDockerProvisioner_Provision_CoverageTightensExistingDirectoryModes(t *testing.T) {
	outputDir := filepath.Join(t.TempDir(), "existing-output")
	runtimeRoot := filepath.Join(outputDir, "runtimes", "rt-existing")
	paths := []string{
		outputDir,
		filepath.Join(outputDir, "runtimes"),
		runtimeRoot,
		filepath.Join(runtimeRoot, "go"),
		filepath.Join(runtimeRoot, "python"),
	}
	for _, path := range paths {
		if err := os.MkdirAll(path, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(path, 0o755); err != nil {
			t.Fatal(err)
		}
	}

	runner := &fakeRunner{output: []byte("coverage_container_id\n")}
	prov := NewDockerProvisioner(runner, coverageCfg(outputDir), "127.0.0.1:50055")
	plan := defaultPlan()
	plan.RuntimeID = "rt-existing"
	if _, err := prov.Provision(context.Background(), plan); err != nil {
		t.Fatalf("Provision: %v", err)
	}

	for _, path := range paths {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if got, want := info.Mode().Perm(), os.FileMode(0o700); got != want {
			t.Fatalf("managed coverage directory %q mode = %o, want %o", path, got, want)
		}
	}
}

func TestDockerProvisioner_Provision_CoverageMountsCanonicalRuntimeRoot(t *testing.T) {
	base := t.TempDir()
	resolvedParent := filepath.Join(base, "resolved")
	if err := os.Mkdir(resolvedParent, 0o700); err != nil {
		t.Fatal(err)
	}
	aliasParent := filepath.Join(base, "alias")
	if err := os.Symlink(resolvedParent, aliasParent); err != nil {
		t.Skipf("create symlink: %v", err)
	}
	outputDir := filepath.Join(aliasParent, "runtime-output")
	runner := &fakeRunner{output: []byte("coverage_container_id\n")}
	prov := NewDockerProvisioner(runner, coverageCfg(outputDir), "127.0.0.1:50055")

	if _, err := prov.Provision(context.Background(), defaultPlan()); err != nil {
		t.Fatalf("Provision: %v", err)
	}
	wantRoot, err := filepath.EvalSymlinks(filepath.Join(resolvedParent, "runtime-output", "runtimes", defaultPlan().RuntimeID))
	if err != nil {
		t.Fatal(err)
	}
	assertFlagValue(t, runner.calls[0].args, "--mount", "type=bind,src="+wantRoot+",dst=/coverage")
}

func TestDockerProvisioner_Provision_CoverageUsesFallbackRunIDAndLabelPrefix(t *testing.T) {
	outputDir := filepath.Join(t.TempDir(), "custom-runtime-output")
	cfg := coverageCfg(outputDir)
	cfg.Docker.LabelPrefix = "example.runtime"
	runner := &fakeRunner{output: []byte("coverage_container_id\n")}
	prov := NewDockerProvisioner(runner, cfg, "127.0.0.1:50055")

	if _, err := prov.Provision(context.Background(), defaultPlan()); err != nil {
		t.Fatalf("Provision: %v", err)
	}
	args := runner.calls[0].args
	assertHasLabel(t, args, "example.runtime.coverage=true")
	assertHasLabel(t, args, "example.runtime.coverage_run_id=custom-runtime-output")
	for i, arg := range args {
		if arg == "--label" && i+1 < len(args) && strings.Contains(args[i+1], outputDir) {
			t.Fatalf("coverage label exposes host output path: %q", args[i+1])
		}
	}
}

func TestDockerProvisioner_Provision_CoverageRejectsInvalidRuntimeIDBeforeDocker(t *testing.T) {
	absoluteID := filepath.Join(t.TempDir(), "escape")
	for _, runtimeID := range []string{".", "..", "../x", "nested/x", `nested\x`, absoluteID} {
		t.Run(strings.ReplaceAll(runtimeID, string(os.PathSeparator), "_"), func(t *testing.T) {
			runner := &fakeRunner{output: []byte("must_not_run\n")}
			prov := NewDockerProvisioner(runner, coverageCfg(t.TempDir()), "127.0.0.1:50055")
			plan := defaultPlan()
			plan.RuntimeID = runtimeID

			if _, err := prov.Provision(context.Background(), plan); !errors.Is(err, ErrProvisionFailed) {
				t.Fatalf("Provision runtime ID %q error = %v, want ErrProvisionFailed", runtimeID, err)
			}
			if len(runner.calls) != 0 {
				t.Fatalf("docker called for invalid runtime ID %q: %+v", runtimeID, runner.calls)
			}
		})
	}
}

func TestDockerProvisioner_Provision_CoverageRejectsInvalidDerivedRunIDBeforeDocker(t *testing.T) {
	runner := &fakeRunner{output: []byte("must_not_run\n")}
	prov := NewDockerProvisioner(runner, coverageCfg(string(os.PathSeparator)), "127.0.0.1:50055")

	if _, err := prov.Provision(context.Background(), defaultPlan()); !errors.Is(err, ErrProvisionFailed) {
		t.Fatalf("Provision error = %v, want ErrProvisionFailed for invalid coverage run ID", err)
	}
	if len(runner.calls) != 0 {
		t.Fatalf("docker called for invalid coverage run ID: %+v", runner.calls)
	}
}

func TestDockerProvisioner_Provision_CoverageRejectsSymlinkEscapeBeforeDocker(t *testing.T) {
	for _, tc := range []struct {
		name    string
		linkRel string
	}{
		{name: "output root", linkRel: "."},
		{name: "runtimes directory", linkRel: "runtimes"},
		{name: "runtime directory", linkRel: filepath.Join("runtimes", "rt-123")},
		{name: "go directory", linkRel: filepath.Join("runtimes", "rt-123", "go")},
		{name: "python directory", linkRel: filepath.Join("runtimes", "rt-123", "python")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base := t.TempDir()
			outputDir := filepath.Join(base, "output")
			outside := filepath.Join(base, "outside")
			linkPath := filepath.Join(outputDir, tc.linkRel)
			if err := os.MkdirAll(filepath.Dir(linkPath), 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.MkdirAll(outside, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(outside, linkPath); err != nil {
				t.Skipf("create symlink: %v", err)
			}

			assertCoverageProvisionFailsBeforeDocker(t, outputDir, "rt-123")
		})
	}
}

func assertCoverageProvisionFailsBeforeDocker(t *testing.T, outputDir, runtimeID string) {
	t.Helper()
	runner := &fakeRunner{output: []byte("must_not_run\n")}
	prov := NewDockerProvisioner(runner, coverageCfg(outputDir), "127.0.0.1:50055")
	plan := defaultPlan()
	plan.RuntimeID = runtimeID

	if _, err := prov.Provision(context.Background(), plan); !errors.Is(err, ErrProvisionFailed) {
		t.Fatalf("Provision error = %v, want ErrProvisionFailed", err)
	}
	if len(runner.calls) != 0 {
		t.Fatalf("docker called for escaping coverage path: %+v", runner.calls)
	}
}

func TestDockerProvisioner_Provision_InjectsHostedRuntimeCredentialJSON(t *testing.T) {
	runner := &fakeRunner{output: []byte("container_full_id_abc\n")}
	prov := NewDockerProvisioner(runner, defaultCfg(), "127.0.0.1:50055")
	plan := defaultPlan()
	plan.RuntimeCredentialKeyID = "hosted-key-1"
	plan.RuntimeCredentialPrivateKeyPEM = "hosted-private-key"

	_, err := prov.Provision(context.Background(), plan)
	if err != nil {
		t.Fatalf("Provision: %v", err)
	}
	args := runner.calls[0].args
	raw := envValue(args, "RUNTIME_CREDENTIAL_JSON")
	if raw == "" {
		t.Fatalf("RUNTIME_CREDENTIAL_JSON missing")
	}
	var got map[string]any
	if err := json.Unmarshal([]byte(raw), &got); err != nil {
		t.Fatalf("RUNTIME_CREDENTIAL_JSON malformed: %v", err)
	}
	if got["version"].(float64) != 1 || got["key_id"] != "hosted-key-1" || got["private_key_pem"] != "hosted-private-key" {
		t.Fatalf("credential json = %+v, want hosted key bundle", got)
	}
}

func TestDockerProvisioner_Provision_InjectsRuntimeMTLSBundleJSON(t *testing.T) {
	runner := &fakeRunner{output: []byte("container_full_id_abc\n")}
	prov := NewDockerProvisioner(runner, defaultCfg(), "127.0.0.1:50055")
	plan := defaultPlan()
	plan.RuntimeClientCertPEM = "client-cert"
	plan.RuntimeClientKeyPEM = "client-key"
	plan.RuntimeServerCAPEM = "server-ca"
	plan.RuntimeChannelTLSServerName = "runtime-channel.local"

	_, err := prov.Provision(context.Background(), plan)
	if err != nil {
		t.Fatalf("Provision: %v", err)
	}
	args := runner.calls[0].args

	assertHasEnv(t, args, "RUNTIME_CHANNEL_TLS_ENABLED=true")
	assertHasEnv(t, args, "RUNTIME_CHANNEL_TLS_SERVER_NAME=runtime-channel.local")
	assertHasEnvPrefix(t, args, "RUNTIME_CHANNEL_TLS_BUNDLE_JSON=")
	for _, key := range []string{
		"RUNTIME_CHANNEL_TLS_CLIENT_CERT_FILE",
		"RUNTIME_CHANNEL_TLS_CLIENT_KEY_FILE",
		"RUNTIME_CHANNEL_TLS_ROOT_CERT_FILE",
	} {
		if envKeyIsPresent(args, key) {
			t.Fatalf("hosted runtime should use TLS bundle JSON only; unexpected %s in args: %v", key, args)
		}
	}
}

func TestDockerProvisioner_Provision_BridgeNetworkDoesNotPublishRuntimePort(t *testing.T) {
	cfg := defaultCfg()
	cfg.Docker.NetworkMode = "bridge"
	runner := &fakeRunner{output: []byte("container_xyz\n")}
	prov := NewDockerProvisioner(runner, cfg, "control-panel:50055")

	_, err := prov.Provision(context.Background(), defaultPlan())
	if err != nil {
		t.Fatalf("Provision: %v", err)
	}
	args := runner.calls[0].args
	assertFlagValue(t, args, "--network", "bridge")
	if contains(args, "-p") {
		t.Fatalf("bridge hosted runtime should not publish a session port: %v", args)
	}
	if envKeyIsPresent(args, "SERVER_GRPC_ADDR") {
		t.Error("hosted runtime should not receive SERVER_GRPC_ADDR")
	}
}

func TestDockerProvisioner_Provision_BridgeHostDockerInternalAddsHostGateway(t *testing.T) {
	cfg := defaultCfg()
	cfg.Docker.NetworkMode = "bridge"
	runner := &fakeRunner{output: []byte("container_xyz\n")}
	prov := NewDockerProvisioner(runner, cfg, "host.docker.internal:50055")

	_, err := prov.Provision(context.Background(), defaultPlan())
	if err != nil {
		t.Fatalf("Provision: %v", err)
	}
	args := runner.calls[0].args
	assertFlagValue(t, args, "--network", "bridge")
	assertFlagValue(t, args, "--add-host", "host.docker.internal:host-gateway")
	assertHasEnv(t, args, "RUNTIME_CHANNEL_GRPC_ADDR=host.docker.internal:50055")
}

func TestDockerProvisioner_Provision_FailsWhenImageEmpty(t *testing.T) {
	cfg := defaultCfg()
	cfg.Image = ""
	prov := NewDockerProvisioner(&fakeRunner{}, cfg, "127.0.0.1:50055")
	_, err := prov.Provision(context.Background(), defaultPlan())
	if !errors.Is(err, ErrNotConfigured) {
		t.Errorf("err = %v, want ErrNotConfigured", err)
	}
}

func TestDockerProvisioner_Provision_RejectsRuntimeEnvBeforeDocker(t *testing.T) {
	cfg := defaultCfg()
	cfg.Docker.RuntimeEnv = map[string]string{
		"CORE_SERVICE_GRPC_ADDR": "127.0.0.1:50051",
		"KAFKA_BROKERS":          "127.0.0.1:19092",
		"DATABASE_PASSWORD":      "secret",
		"MY_CUSTOM_VAR":          "also-not-an-explicit-runtime-field",
	}
	runner := &fakeRunner{output: []byte("container_xyz\n")}
	prov := NewDockerProvisioner(runner, cfg, "127.0.0.1:50055")

	_, err := prov.Provision(context.Background(), defaultPlan())
	if err == nil || !strings.Contains(err.Error(), "provisioning.docker.runtime_env") {
		t.Fatalf("Provision error = %v, want Runtime isolation error", err)
	}
	if len(runner.calls) != 0 {
		t.Fatalf("docker was called despite invalid runtime_env: %+v", runner.calls)
	}
}

func envKeys(args []string) map[string]struct{} {
	keys := make(map[string]struct{})
	for i, arg := range args {
		if arg != "-e" || i+1 >= len(args) {
			continue
		}
		key, _, ok := strings.Cut(args[i+1], "=")
		if ok {
			keys[key] = struct{}{}
		}
	}
	return keys
}

func envKeyIsPresent(args []string, key string) bool {
	prefix := key + "="
	for i, a := range args {
		if a == "-e" && i+1 < len(args) && strings.HasPrefix(args[i+1], prefix) {
			return true
		}
	}
	return false
}

func envValue(args []string, key string) string {
	prefix := key + "="
	for i, a := range args {
		if a == "-e" && i+1 < len(args) && strings.HasPrefix(args[i+1], prefix) {
			return strings.TrimPrefix(args[i+1], prefix)
		}
	}
	return ""
}

func assertHasEnvPrefix(t *testing.T, args []string, prefix string) {
	t.Helper()
	for i, a := range args {
		if a == "-e" && i+1 < len(args) && strings.HasPrefix(args[i+1], prefix) {
			return
		}
	}
	t.Fatalf("env prefix %q missing in args: %v", prefix, args)
}

func TestDockerProvisioner_Provision_DockerErrorWraps(t *testing.T) {
	runner := &fakeRunner{
		output: []byte("docker: Error response from daemon: image not found"),
		err:    errors.New("exit status 125"),
	}
	prov := NewDockerProvisioner(runner, defaultCfg(), "127.0.0.1:50055")
	_, err := prov.Provision(context.Background(), defaultPlan())
	if !errors.Is(err, ErrProvisionFailed) {
		t.Fatalf("err = %v, want ErrProvisionFailed", err)
	}
	if !strings.Contains(err.Error(), "image not found") {
		t.Errorf("error should propagate docker output; got %v", err)
	}
}

func TestDockerProvisioner_Provision_DockerRunFailureRemovesPartialContainer(t *testing.T) {
	partialID := "78ba67562094c58ad9cbc4bc9956030d01f9389dea1ea017487edffa6f81b12d"
	runner := &fakeRunner{
		multiOut: [][]byte{
			[]byte(partialID + "\ndocker: Error response from daemon: failed to set up container networking"),
			[]byte(""),
		},
		multiErr: []error{
			errors.New("exit status 125"),
			nil,
		},
	}
	prov := NewDockerProvisioner(runner, defaultCfg(), "127.0.0.1:50055")

	_, err := prov.Provision(context.Background(), defaultPlan())
	if !errors.Is(err, ErrProvisionFailed) {
		t.Fatalf("err = %v, want ErrProvisionFailed", err)
	}
	if len(runner.calls) != 2 {
		t.Fatalf("calls = %d, want docker run + docker rm", len(runner.calls))
	}
	want := []string{"rm", "-f", partialID}
	if !equalStringSlice(runner.calls[1].args, want) {
		t.Fatalf("cleanup args = %v, want %v", runner.calls[1].args, want)
	}
}

func TestDockerProvisioner_Provision_CoverageDockerRunFailureRemovesPartialContainerWhenStopFails(t *testing.T) {
	partialID := "78ba67562094c58ad9cbc4bc9956030d01f9389dea1ea017487edffa6f81b12d"
	stopErr := errors.New("container is not running")
	runner := &fakeRunner{
		multiOut: [][]byte{
			[]byte(partialID + "\ndocker: Error response from daemon: failed to set up container networking"),
			[]byte(""),
			[]byte(""),
		},
		multiErr: []error{
			errors.New("exit status 125"),
			stopErr,
			nil,
		},
	}
	prov := NewDockerProvisioner(runner, coverageCfg(t.TempDir()), "127.0.0.1:50055")

	_, err := prov.Provision(context.Background(), defaultPlan())
	if !errors.Is(err, ErrProvisionFailed) {
		t.Fatalf("err = %v, want ErrProvisionFailed", err)
	}
	if strings.Contains(err.Error(), "cleanup partial container") {
		t.Fatalf("successful forced removal reported as cleanup failure: %v", err)
	}
	if len(runner.calls) != 3 {
		t.Fatalf("calls = %d, want docker run + docker stop + docker rm", len(runner.calls))
	}
	wantStop := []string{"stop", "--time", "10", partialID}
	if !equalStringSlice(runner.calls[1].args, wantStop) {
		t.Fatalf("stop args = %v, want %v", runner.calls[1].args, wantStop)
	}
	wantRemove := []string{"rm", "-f", partialID}
	if !equalStringSlice(runner.calls[2].args, wantRemove) {
		t.Fatalf("remove args = %v, want %v", runner.calls[2].args, wantRemove)
	}
}

func TestDockerProvisioner_Deprovision_CallsDockerRm(t *testing.T) {
	runner := &fakeRunner{output: []byte("")}
	prov := NewDockerProvisioner(runner, defaultCfg(), "127.0.0.1:50055")
	if err := prov.Deprovision(context.Background(), "container_xyz"); err != nil {
		t.Fatalf("Deprovision: %v", err)
	}
	if len(runner.calls) != 1 {
		t.Fatalf("calls = %d, want 1", len(runner.calls))
	}
	call := runner.calls[0]
	if call.name != "docker" {
		t.Errorf("called %q, want docker", call.name)
	}
	want := []string{"rm", "-f", "container_xyz"}
	if !equalStringSlice(call.args, want) {
		t.Errorf("args = %v, want %v", call.args, want)
	}
}

func TestDockerProvisioner_Deprovision_CoverageStopsThenAlwaysRemoves(t *testing.T) {
	stopErr := errors.New("stop failed")
	removeErr := errors.New("remove failed")
	for _, tc := range []struct {
		name       string
		stopErr    error
		removeErr  error
		wantErrors []error
	}{
		{name: "success"},
		{name: "stop failure is cleared by removal", stopErr: stopErr},
		{name: "remove failure", removeErr: removeErr, wantErrors: []error{removeErr}},
		{name: "stop and remove failure", stopErr: stopErr, removeErr: removeErr, wantErrors: []error{stopErr, removeErr}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runner := &fakeRunner{
				multiOut: [][]byte{[]byte(""), []byte("")},
				multiErr: []error{tc.stopErr, tc.removeErr},
			}
			prov := NewDockerProvisioner(runner, coverageCfg(t.TempDir()), "127.0.0.1:50055")

			err := prov.Deprovision(context.Background(), "container_xyz")
			if len(tc.wantErrors) == 0 && err != nil {
				t.Fatalf("Deprovision error = %v, want nil", err)
			}
			for _, wantErr := range tc.wantErrors {
				if !errors.Is(err, wantErr) {
					t.Fatalf("Deprovision error = %v, want %v", err, wantErr)
				}
			}
			if len(runner.calls) != 2 {
				t.Fatalf("calls = %d, want stop + rm", len(runner.calls))
			}
			wantStop := []string{"stop", "--time", "10", "container_xyz"}
			if !equalStringSlice(runner.calls[0].args, wantStop) {
				t.Fatalf("stop args = %v, want %v", runner.calls[0].args, wantStop)
			}
			wantRemove := []string{"rm", "-f", "container_xyz"}
			if !equalStringSlice(runner.calls[1].args, wantRemove) {
				t.Fatalf("remove args = %v, want %v", runner.calls[1].args, wantRemove)
			}
		})
	}
}

func TestDockerProvisioner_Deprovision_CoverageRemovalGetsFreshBoundedContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runner := &fakeRunner{
		multiOut: [][]byte{[]byte(""), []byte("")},
		multiErr: []error{context.Canceled, nil},
		onRun: func(callIndex int) {
			if callIndex == 0 {
				cancel()
			}
		},
	}
	prov := NewDockerProvisioner(runner, coverageCfg(t.TempDir()), "127.0.0.1:50055")
	startedAt := time.Now()

	if err := prov.Deprovision(ctx, "container_xyz"); err != nil {
		t.Fatalf("Deprovision: %v", err)
	}
	if len(runner.calls) != 2 {
		t.Fatalf("calls = %d, want stop + rm", len(runner.calls))
	}
	removeCall := runner.calls[1]
	if removeCall.contextErr != nil {
		t.Fatalf("remove context was already canceled: %v", removeCall.contextErr)
	}
	if !removeCall.hasDeadline {
		t.Fatal("remove context has no deadline")
	}
	if timeout := removeCall.deadline.Sub(startedAt); timeout <= 0 || timeout > 11*time.Second {
		t.Fatalf("remove context timeout = %v, want a fresh bound near 10s", timeout)
	}
}

func TestDockerProvisioner_DiagnosticsIncludesInspectAndTailLogs(t *testing.T) {
	runner := &fakeRunner{
		multiOut: [][]byte{
			[]byte("state=exited exit=1 error= started=2026-06-19T05:17:29Z finished=2026-06-19T05:17:30Z\n"),
			[]byte("Failed to spawn: hushine-runtime\n"),
		},
	}
	prov := NewDockerProvisioner(runner, defaultCfg(), "127.0.0.1:50055")

	diag, err := prov.Diagnostics(context.Background(), "container_xyz")
	if err != nil {
		t.Fatalf("Diagnostics: %v", err)
	}
	if !strings.Contains(diag, "inspect: state=exited exit=1") {
		t.Fatalf("diag = %q, want inspect output", diag)
	}
	if !strings.Contains(diag, "logs: Failed to spawn: hushine-runtime") {
		t.Fatalf("diag = %q, want logs output", diag)
	}
	if len(runner.calls) != 2 {
		t.Fatalf("calls = %d, want inspect + logs", len(runner.calls))
	}
	assertFlagValue(t, runner.calls[0].args, "--format", "state={{.State.Status}} exit={{.State.ExitCode}} error={{.State.Error}} started={{.State.StartedAt}} finished={{.State.FinishedAt}}")
	wantLogs := []string{"logs", "--tail", "80", "container_xyz"}
	if !equalStringSlice(runner.calls[1].args, wantLogs) {
		t.Fatalf("logs args = %v, want %v", runner.calls[1].args, wantLogs)
	}
}

func TestDockerProvisioner_DiagnosticsRedactsSensitiveLogValues(t *testing.T) {
	runner := &fakeRunner{
		multiOut: [][]byte{
			[]byte("state=exited exit=1 error= started=2026-06-19T05:17:29Z finished=2026-06-19T05:17:30Z\n"),
			[]byte("API_SECRET=super-secret token: runtime-token password=plain private_key_pem\":\"pem-value\"\n"),
		},
	}
	prov := NewDockerProvisioner(runner, defaultCfg(), "127.0.0.1:50055")

	diag, err := prov.Diagnostics(context.Background(), "container_xyz")
	if err != nil {
		t.Fatalf("Diagnostics: %v", err)
	}
	for _, leaked := range []string{"super-secret", "runtime-token", "password=plain", "pem-value"} {
		if strings.Contains(diag, leaked) {
			t.Fatalf("diag leaked sensitive value %q: %s", leaked, diag)
		}
	}
	if !strings.Contains(diag, "API_SECRET=<redacted>") || !strings.Contains(diag, "token: <redacted>") {
		t.Fatalf("diag = %q, want redacted markers", diag)
	}
}

// ── helpers ──────────────────────────────────────────────────────────

func contains(args []string, want string) bool {
	for _, a := range args {
		if a == want {
			return true
		}
	}
	return false
}

func assertFlagValue(t *testing.T, args []string, flag, value string) {
	t.Helper()
	for i, a := range args {
		if a == flag {
			if i+1 >= len(args) {
				t.Errorf("flag %s has no value", flag)
				return
			}
			if args[i+1] != value {
				t.Errorf("flag %s = %q, want %q", flag, args[i+1], value)
			}
			return
		}
	}
	t.Errorf("flag %s not found in %v", flag, args)
}

func assertHasEnv(t *testing.T, args []string, want string) {
	t.Helper()
	for i, a := range args {
		if a == "-e" && i+1 < len(args) && args[i+1] == want {
			return
		}
	}
	t.Errorf("env %q not found", want)
}

func assertHasLabel(t *testing.T, args []string, want string) {
	t.Helper()
	for i, a := range args {
		if a == "--label" && i+1 < len(args) && args[i+1] == want {
			return
		}
	}
	t.Errorf("label %q not found", want)
}

func equalStringSlice(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func coverageCfg(outputDir string) config.ProvisioningConfig {
	cfg := defaultCfg()
	cfg.Docker.Coverage = config.DockerCoverageConfig{
		Enabled:            true,
		Image:              "hushine/strategy-runtime:test-cover",
		OutputDir:          outputDir,
		StopTimeoutSeconds: 10,
	}
	return cfg
}
