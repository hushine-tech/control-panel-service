// Package provision is the hosted-runtime provisioning seam used by
// `Service.EnsureHostedRuntime`. The seam exists so the service-layer
// quota / plan / readiness logic can be tested without spinning up real
// containers, and so the actual container-runtime backend (Docker via
// os/exec for D1, future Kubernetes / Nomad later) is swappable.
//
// D1 ships with `NoOpProvisioner` (default) and `Disabled` for tests.
// `DockerProvisioner` is a follow-up task — see Phase D1 section 5
// implementation notes in `progress/phase-d-runtime-control-plane.md`.
package provision

import (
	"context"
	"errors"
	"fmt"

	"github.com/hushine-tech/control-panel-service/internal/config"
)

// Sentinel errors returned by Provisioner implementations.
var (
	// ErrNotConfigured: the operator did not wire a provisioner backend
	// (Docker / etc.). Service surfaces this as a typed error so the
	// gRPC layer can translate to FailedPrecondition rather than crash.
	ErrNotConfigured = errors.New("provision: backend not configured")

	// ErrUnknownProfile: the requested resource_profile has no
	// corresponding entry in `provisioning.profiles`. Distinct from a
	// plan-policy rejection (that is checked before the provisioner is
	// called).
	ErrUnknownProfile = errors.New("provision: unknown resource_profile")

	// ErrProvisionFailed: the backend tried to start a container and
	// failed (image missing, port collision, daemon down, etc.). Wraps
	// the backend-specific error.
	ErrProvisionFailed = errors.New("provision: backend rejected start")

	// ErrRegistrationTimeout: the container started but did not call
	// RegisterRuntime within the configured timeout. The runtime is
	// likely broken; caller should treat this the same way it would
	// treat an unhealthy runtime.
	ErrRegistrationTimeout = errors.New("provision: runtime did not self-register in time")
)

// Plan describes the runtime container the caller wants spun up. Filled
// in by Service.EnsureHostedRuntime from plan + config + the user's
// request, then passed to the provisioner.
type Plan struct {
	// RuntimeID is the platform-generated identity the container will
	// register with. The provisioner forwards this via env var so the
	// runtime's section-4 self-register code uses the same id.
	RuntimeID string

	// UserID is the runtime owner. Name is the immutable user-visible label.
	UserID int64
	Name   string

	// EndpointHost + GRPCPort: what the runtime advertises to the
	// control panel at registration time, and what quant-handler will
	// dial directly (D1 direct-dial; D3 proxied).
	EndpointHost string
	GRPCPort     int

	// Image / ResourceLimits come from `ProvisioningConfig`.
	Image  string
	Limits config.ResourceProfile

	// ResourceProfileName carries the human profile name into the
	// runtime registry row so reconciliation can map back to plan
	// constraints. (DockerProvisioner uses Limits to set actual
	// constraints; this field is for audit only.)
	ResourceProfileName string

	// Capabilities passed through to the runtime row + advertised on
	// register.
	Capabilities []string

	// ControlPanelGRPC is the address the runtime should dial for self-
	// registration + heartbeat. The provisioner sets this as an env var
	// when starting the container.
	ControlPanelGRPC string

	// RuntimeCredential* are platform-generated hosted-internal credentials.
	// They are injected into the container by the provisioner and are never
	// exposed through user-facing credential APIs.
	RuntimeCredentialKeyID         string
	RuntimeCredentialPrivateKeyPEM string
}

// Provisioner is the abstraction Service.EnsureHostedRuntime calls to
// start a hosted strategy-runtime container. Implementations:
//
//   - NoOpProvisioner — production default; refuses every call. Used
//     when no backend is configured.
//   - (future) DockerProvisioner — calls `docker run` via os/exec.
//   - Mock implementations live in tests; they should mimic
//     "container started successfully and runtime called RegisterRuntime"
//     by directly inserting a runtime registry row.
type Provisioner interface {
	// Provision starts a container per `p`. On success the container
	// is starting; the caller is responsible for waiting until the
	// runtime self-registers (NOT the provisioner — that responsibility
	// stays in the service layer so timeout / repo polling logic is
	// uniform across backends).
	//
	// Returns a backend-specific handle (container id / pod name) for
	// audit; the handle is opaque to the service layer.
	Provision(ctx context.Context, p Plan) (handle string, err error)

	// Deprovision removes the runtime's container. Best-effort; failures
	// are logged but not propagated since we may be cancelling a
	// half-started container after a registration timeout.
	Deprovision(ctx context.Context, handle string) error
}

// NoOpProvisioner refuses every call with ErrNotConfigured. This is the
// safe default when the operator has not wired a backend; it produces a
// clean, machine-readable failure rather than crashing.
type NoOpProvisioner struct{}

func (NoOpProvisioner) Provision(_ context.Context, p Plan) (string, error) {
	return "", fmt.Errorf("%w (runtime_id=%s user=%d)", ErrNotConfigured, p.RuntimeID, p.UserID)
}

func (NoOpProvisioner) Deprovision(_ context.Context, _ string) error { return nil }
