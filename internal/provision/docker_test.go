package provision

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

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
}

type fakeRunnerCall struct {
	name string
	args []string
}

func (f *fakeRunner) Run(_ context.Context, name string, args ...string) ([]byte, error) {
	idx := len(f.calls)
	f.calls = append(f.calls, fakeRunnerCall{name: name, args: append([]string(nil), args...)})
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
			NetworkMode:         "host",
			LabelPrefix:         "hushine.runtime",
			RuntimeUserGRPCPort: 50053,
			RuntimeEnv: map[string]string{
				"ACCOUNT_SERVICE_GRPC_ADDR": "127.0.0.1:50051",
				"KAFKA_BROKERS":             "127.0.0.1:19092",
			},
		},
	}
}

func TestDockerProvisioner_Provision_BuildsExpectedRunArgs(t *testing.T) {
	runner := &fakeRunner{output: []byte("container_full_id_abc\n")}
	prov := NewDockerProvisioner(runner, defaultCfg(), "127.0.0.1:50054")

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
	assertHasEnv(t, args, "RUNTIME_INGRESS_MODE=outbound")
	if envIsPresent(args, "RUNTIME_REGISTER_WITH_CONTROL_PANEL=1") {
		t.Error("hosted runtime should register through RuntimeChannel credential path, not direct RegisterRuntime")
	}
	assertHasEnv(t, args, "RUNTIME_RUNTIME_ID=rt_abc123")
	assertHasEnv(t, args, "RUNTIME_NAME=hosted-steady-river")
	assertHasEnv(t, args, "RUNTIME_RESOURCE_PROFILE=small")
	assertHasEnv(t, args, "CONTROL_PANEL_SERVICE_GRPC_ADDR=127.0.0.1:50054")
	if envKeyIsPresent(args, "SERVER_GRPC_ADDR") {
		t.Error("outbound hosted runtime should not receive SERVER_GRPC_ADDR")
	}
	// Operator-supplied static env forwarded.
	assertHasEnv(t, args, "ACCOUNT_SERVICE_GRPC_ADDR=127.0.0.1:50051")
	assertHasEnv(t, args, "KAFKA_BROKERS=127.0.0.1:19092")
	// Labels for traceability.
	assertHasLabel(t, args, "hushine.runtime.runtime_id=rt_abc123")
	assertHasLabel(t, args, "hushine.runtime.user_id=42")
	assertHasLabel(t, args, "hushine.runtime.name=hosted-steady-river")
	assertHasLabel(t, args, "hushine.runtime.resource_profile=small")
}

func TestDockerProvisioner_Provision_InjectsHostedRuntimeCredentialJSON(t *testing.T) {
	runner := &fakeRunner{output: []byte("container_full_id_abc\n")}
	prov := NewDockerProvisioner(runner, defaultCfg(), "127.0.0.1:50054")
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

func TestDockerProvisioner_Provision_BridgeNetworkDoesNotPublishRuntimePort(t *testing.T) {
	cfg := defaultCfg()
	cfg.Docker.NetworkMode = "bridge"
	cfg.Docker.RuntimeUserGRPCPort = 50053
	runner := &fakeRunner{output: []byte("container_xyz\n")}
	prov := NewDockerProvisioner(runner, cfg, "control-panel:50054")

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
		t.Error("outbound hosted runtime should not receive SERVER_GRPC_ADDR")
	}
}

func TestDockerProvisioner_Provision_FailsWhenImageEmpty(t *testing.T) {
	cfg := defaultCfg()
	cfg.Image = ""
	prov := NewDockerProvisioner(&fakeRunner{}, cfg, "127.0.0.1:50054")
	_, err := prov.Provision(context.Background(), defaultPlan())
	if !errors.Is(err, ErrNotConfigured) {
		t.Errorf("err = %v, want ErrNotConfigured", err)
	}
}

// Operator-supplied runtime_env MUST NOT be able to shadow platform-
// controlled env vars (RUNTIME_*, CONTROL_PANEL_SERVICE_GRPC_ADDR,
// SERVER_GRPC_ADDR). Defense against operator footgun (#20).
func TestDockerProvisioner_Provision_FiltersReservedEnvKeys(t *testing.T) {
	cfg := defaultCfg()
	cfg.Docker.RuntimeEnv = map[string]string{
		// Forbidden — must be ignored.
		"RUNTIME_BIND_USER_ID":            "999",
		"RUNTIME_RUNTIME_ID":              "rt_attacker",
		"CONTROL_PANEL_SERVICE_GRPC_ADDR": "evil:50054",
		"SERVER_GRPC_ADDR":                ":1",
		// Allowed — legit operator env.
		"ACCOUNT_SERVICE_GRPC_ADDR": "127.0.0.1:50051",
		"MY_CUSTOM_VAR":             "hello",
	}
	runner := &fakeRunner{output: []byte("container_xyz\n")}
	prov := NewDockerProvisioner(runner, cfg, "127.0.0.1:50054")

	_, err := prov.Provision(context.Background(), defaultPlan())
	if err != nil {
		t.Fatalf("Provision: %v", err)
	}
	args := runner.calls[0].args

	// Platform values must remain.
	assertHasEnv(t, args, "RUNTIME_RUNTIME_ID=rt_abc123")
	assertHasEnv(t, args, "CONTROL_PANEL_SERVICE_GRPC_ADDR=127.0.0.1:50054")
	if envKeyIsPresent(args, "SERVER_GRPC_ADDR") {
		t.Error("reserved SERVER_GRPC_ADDR from runtime_env should not reach outbound hosted runtime")
	}
	// Forbidden runtime_env entries must NOT appear.
	for _, forbidden := range []string{
		"RUNTIME_BIND_USER_ID=999",
		"RUNTIME_RUNTIME_ID=rt_attacker",
		"CONTROL_PANEL_SERVICE_GRPC_ADDR=evil:50054",
		"SERVER_GRPC_ADDR=:1",
	} {
		if envIsPresent(args, forbidden) {
			t.Errorf("forbidden runtime_env entry %q reached docker args", forbidden)
		}
	}
	// Allowed entries DO appear.
	assertHasEnv(t, args, "ACCOUNT_SERVICE_GRPC_ADDR=127.0.0.1:50051")
	assertHasEnv(t, args, "MY_CUSTOM_VAR=hello")
}

// envIsPresent is a strict-form-only existence check (different from
// assertHasEnv which is positive-test).
func envIsPresent(args []string, want string) bool {
	for i, a := range args {
		if a == "-e" && i+1 < len(args) && args[i+1] == want {
			return true
		}
	}
	return false
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

func TestDockerProvisioner_Provision_DockerErrorWraps(t *testing.T) {
	runner := &fakeRunner{
		output: []byte("docker: Error response from daemon: image not found"),
		err:    errors.New("exit status 125"),
	}
	prov := NewDockerProvisioner(runner, defaultCfg(), "127.0.0.1:50054")
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
	prov := NewDockerProvisioner(runner, defaultCfg(), "127.0.0.1:50054")

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

func TestDockerProvisioner_Deprovision_CallsDockerRm(t *testing.T) {
	runner := &fakeRunner{output: []byte("")}
	prov := NewDockerProvisioner(runner, defaultCfg(), "127.0.0.1:50054")
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
