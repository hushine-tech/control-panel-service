# control-panel-service

Runtime and market-data control plane. It owns runtime registry, route
resolution, hosted runtime provisioning, self-hosted RuntimeChannel streams,
runtime credentials, per-user plan/quota, and the D2 market-data control
plane. Current behavior is defined by this repository, the shared protobuf
contracts, and the project architecture/runtime documentation; archived design
changes are historical context only.

## Run

```bash
make ensure-db        # apply migrations to the control_panel database
make dev              # foreground; uses config.yaml
make start            # background; logs in ./logs/, pid in .run.pid
make stop
make test             # auth + plan + service unit tests
make proto            # regenerate gen/controlpanelv1 from proto/
```

## Endpoints

| Surface | Address | Notes |
|---|---|---|
| HTTP health | `:8082/healthz`, `:8082/readyz` | always-200 + ready-gate |
| gRPC | `:50054` | `controlpanel.v1.ControlPanelService` |
| RuntimeChannel gRPC | `:50055` | dedicated runtime stream listener; optional server TLS / mTLS |

## RPC overview

| RPC | Purpose | Status |
|---|---|---|
| `ListRuntimes` | list per-user runtime registry rows | landed |
| `ResolveRuntimeRouteByID` | look up a selected `runtime_id` and source | route metadata only |
| `EnsureHostedRuntime` | lazy-create hosted RuntimeChannel runtime if missing; idempotent reuse | hosted path |
| `RuntimeChannel` | hosted/self-hosted/bare-debug runtime outbound bidi stream | dedicated listener |
| `RunStrategy` / `PreviewRunStrategy` / `StopStrategy` / `GetStrategyStatus` | proxy strategy RPCs over `RuntimeChannel` | all runtime sources |
| `ValidateStrategySource` | side-effect-free source validation routed by explicit `runtime_id` | existing active runtime only |
| `IssueRuntimeCredential` / `ListRuntimeCredentials` / `RevokeRuntimeCredential` | runtime credential lifecycle: HELLO signing key + optional mTLS client certificate metadata | self-hosted path |

## Runtime Traffic Paths

Runtime traffic has one supported path now:

| Runtime source | Handler path | Runtime process mode | Auth primitive |
|---|---|---|---|
| `hosted` | `quant-handler` → control-panel proxy RPC → `RuntimeChannel` REQUEST frame | Go `runtime-agent` in a platform-managed container; one Python worker per session | hosted internal credential + provisioned mTLS bundle |
| `self_hosted` | `quant-handler` → control-panel proxy RPC → `RuntimeChannel` REQUEST frame | Go `runtime-agent` in a user-managed container or bare machine | user-issued credential + mTLS bundle when TLS is enabled |
| `bare` | `quant-handler` → control-panel proxy RPC → `RuntimeChannel` REQUEST frame | Go `runtime-agent` via the guarded debugpy launcher | debug-gated mTLS client certificate bootstrap |

The `bare` source is accepted only when
`runtime_platform.debug_bare_runtime_enabled=true`; production deployments
should keep it false.

Backtest market data also uses this route now. RuntimeChannel runtimes call
`marketdata.FetchBacktestPage`; control-panel-service reads `{exchange}_{year}`
market-data tables and returns pages of at most `8192` bars. Large backtests are
therefore streamed page by page instead of being pushed to the runtime as one
dataset blob.

## Runtime dependency profile admission

`runtime_channel_server.dependency_profile` is the control-plane deployment
expectation:

```yaml
runtime_channel_server:
  dependency_profile:
    schema_version: 1
    name: platform-python-3.13
    version: 1.0.0
    contract_sha256: 8457b3c35618558fc8bfc74d4135b7eb52e00c33a8c9a49d202830f3fd5b62c5
```

HELLO and RESUME must carry a structurally complete profile: schema/name/
version/digest, Hosted Python, sorted unique public import roots,
strategy-service/library commits, and image build ID. Admission compares the
first four fields exactly with configuration and fail-closes if any remaining
fact is missing or unsafe. A mismatch is recorded as
`RUNTIME_DEPENDENCY_PROFILE_MISMATCH`; it never creates or refreshes a route.

runtime-agent performs its local installed-closure probe before connecting. A
Hosted failure is accepted only through the provisioner's single-line bounded
startup record. A Self-hosted failure is accepted only by the dedicated
credential-bound, timestamped, nonce-protected, Ed25519-signed failure RPC.
That RPC permits only source `self_hosted`, records
`RUNTIME_DEPENDENCY_PROFILE_INVALID`, and cannot register a runtime or send
RuntimeChannel frames. Bare does not use this failure-report surface.

Strategy proxy calls continue to route only by `(user_id, runtime_id)`.
`ValidateStrategySource` calls the existing stream and returns validation
issues/profile without provisioning a Runtime or creating a Session.
Preview/Run/download errors preserve only six allowlisted dependency fields:
`code`, `module`, `runtime_profile`, `runtime_profile_version`,
`image_build_id`, and bounded single-line `message`. The stable cross-service
codes are:

- `UNSUPPORTED_STRATEGY_DEPENDENCY`
- `STRATEGY_DEPENDENCY_UNAVAILABLE`
- `STRATEGY_IMPORT_FAILED`
- `RUNTIME_DEPENDENCY_PROFILE_INVALID`
- `RUNTIME_DEPENDENCY_PROFILE_MISMATCH`

## Provisioning

`EnsureHostedRuntime` is the lazy-creation entry point quant-handler
calls on strategy start. Order of checks:

1. user_id required; `name` is display-only and may be omitted to generate `hosted-*`
2. explicit `runtime_id` route lookup is used for strategy start/status/stop
3. plan / quota / `resource_profile` checks fail-closed
4. allocate `runtime_id` + hosted bootstrap credential
5. call `Provisioner.Provision`
6. wait up to `provisioning.registration_timeout_seconds` for the
   runtime's RuntimeChannel HELLO to land a row
7. return route; on timeout, deprovision and fail closed

### Provisioner backends

The `internal/provision/` package exposes a `Provisioner` interface so
the service-layer logic above is backend-agnostic. Current backends:

- **NoOpProvisioner** (default) — refuses every call with
  `ErrNotConfigured`. Service surfaces this as `FailedPrecondition` so
  operators get a clean failure rather than a half-started state.
- **DockerProvisioner** — calls `docker run` via `os/exec` with env vars
  and CPU/memory/pids limits from `provisioning.profiles`.

### `provisioning` config block

```yaml
provisioning:
  image: "hushine/strategy-runtime:executor-dev"   # built by strategy-service/scripts/build_strategy_runtime.sh
  registration_timeout_seconds: 30        # wait window for RuntimeChannel HELLO
  profiles:
    small:  { nano_cpus: "0.5", memory_mb: 512,  pids_limit: 256 }
    medium: { nano_cpus: "1.0", memory_mb: 1024, pids_limit: 512 }
    large:  { nano_cpus: "2.0", memory_mb: 2048, pids_limit: 1024 }
```

### Plan / quota convention

`runtime_plans` and `runtime_platform.max_total_*` use the same
`applyPlatformCap` rule (see `internal/plan/resolver.go`):

| value | meaning |
|---|---|
| `0` | hard cap of 0 (forbidden); takes precedence |
| `-1` | unlimited on this side |
| `>0` | real cap; smaller of (plan, platform) wins; if the other side is unlimited, this side wins |

The `0 = forbid` convention is the post-2026-05-03 fix; the previous
`minNonZero` definition silently turned `max_self_hosted_runtimes: 0`
into "unlimited" by inheriting the platform fallback.

## RuntimeChannel rollout sequence

For local smoke, create or update the ignored `config.local.yaml` locally
to select the Docker backend and handler control-panel routing. Before
restarting, remove any stale `provisioning.docker.runtime_env`; every
non-empty map is rejected. Other environments should flip the cutover
toggles in this order to avoid half-cutover states (handler routes via
control-panel but provisioner is NoOp → fail-closed; or provisioner runs
containers but handler is not pointed at control-panel):

1. **Apply migrations**: `make ensure-dbs` at repo root (creates
   `control_panel` DB and applies `users.plan_code` to `portfolio` DB).
2. **Build the runtime image**:
   ```bash
   bash strategy-service/scripts/build_strategy_runtime.sh dev
   ```
   This builds `hushine/strategy-runtime:executor-dev`.
3. **Switch control-panel to docker backend** in
   `control-panel-service/config.local.yaml`:
   ```yaml
   provisioning:
     backend: "docker"     # was "noop"
     docker:
       network_mode: "bridge"                  # Docker Desktop friendly
       runtime_channel_dial_addr: "host.docker.internal:50055"
   ```
   Database, market-data Kafka, notification Kafka, log Kafka, core, and
   order addresses remain platform-side control-panel inputs and are never
   passed to a hosted Runtime.
4. **Restart control-panel-service** so the new backend takes effect.
5. **Smoke**: start a hosted runtime from the frontend or handler flow and
   confirm `runtime_registry.status=active` after the RuntimeChannel HELLO.
6. **Restart handler** after pointing
   `dependencies.control_panel_service_grpc` at this service. Strategy
   traffic is always routed through RuntimeChannel now.
To roll back runtime provisioning, switch control-panel backend back to
`noop` and restart control-panel-service. Handler strategy traffic still
requires a registered runtime and RuntimeChannel route.

## Runtime Onboarding

Recommended smoke/onboarding sequence:

1. **Start platform services on the Mac**:
   ```bash
   ./restart.sh
   ```
2. **Build the runtime image**:
   ```bash
   bash strategy-service/scripts/build_strategy_runtime.sh dev
   ```
3. **Generate a self-hosted credential** in quant-frontend:
   Runtime Management -> Runtime Credentials -> Generate new credential.
   Download the `.cred` file once and keep it out of browser storage.
4. **Start a self-hosted runtime**:
     ```bash
     docker run --rm \
       -v $HOME/.hushine/runtime.cred:/etc/hushine/runtime.cred:ro \
       -e RUNTIME_CREDENTIAL_PATH=/etc/hushine/runtime.cred \
       -e RUNTIME_CHANNEL_GRPC_ADDR=host.docker.internal:50055 \
       hushine/strategy-runtime:executor-dev
     ```
5. **Start a bare debug runtime** only when the control-panel debug gate is enabled:
   ```bash
   cd strategy-service
   make build
   DEBUG_WAIT=0 scripts/start-bare-runtime-debugpy.sh --user-id <users.id> --platform-host 127.0.0.1
   ```
6. **Observe the stream**: the runtime registry should show
   `source=hosted`, `source=self_hosted`, or `source=bare` with
   `status=active`. Operator signals to watch are stream uptime,
   last-frame latency, in-flight calls, and dropped-command counters/log
   lines from `internal/runtimechannel`.

Credential loss or suspected leak uses the disaster-recovery flow in the
next section: revoke the old credential, confirm streams close and runtime
rows cancel, generate a new `.cred`, then restart the runtime container.

## Runtime credential file contract (Phase D3)

A self-hosted strategy-runtime container reads its runtime credential from a JSON
file at startup. The Ed25519 key signs RuntimeChannel HELLO for replay-resistant
runtime identity; the optional mTLS fields materialize the client certificate
bundle used by the RuntimeChannel TLS connection. This section is the canonical
reference for the file path / permissions / schema / failure modes; the UI
download flow (`/settings/runtime-credentials` in `quant-frontend`) and the SDK
loader MUST stay aligned with it.

### File path

- Default: `/etc/hushine/runtime.cred`
- Override: env var `RUNTIME_CREDENTIAL_PATH=<absolute path>`
- The file is mounted into the container at `docker run` time:
  ```
  docker run -v $HOME/.hushine/runtime.cred:/etc/hushine/runtime.cred:ro ...
  ```

### Permissions

- Production: `chmod 0600` on the host path before mounting.
- The runtime SDK checks the bits at load time. Permissions more
  permissive than `0600` (e.g. `0644`, world-readable) emit a
  WARN-level log entry but DO NOT reject the file. CI environments
  and certain container runtimes do not preserve permissions
  reliably; rejecting would block legitimate dev setups.
- The WARN line is monitorable — production deployments should alert
  on it.

### Schema (`version: 1`)

```json
{
  "version": 1,
  "key_id": "<base64url-encoded id>",
  "private_key_pem": "<Ed25519 private key in PEM (PKCS#8)>",
  "client_cert_pem": "<optional RuntimeChannel client certificate PEM>",
  "client_key_pem": "<optional RuntimeChannel client private key PEM>",
  "server_ca_pem": "<optional RuntimeChannel server CA PEM>",
  "server_name": "runtime-channel.local"
}
```

- `version` is mandatory. `version != 1` → fail-closed at boot.
- `client_cert_pem`, `client_key_pem`, and `server_ca_pem` are consumed as a
  bundle: when all three are present the runtime writes them to local files and
  enables mTLS for RuntimeChannel. If TLS is required by deployment config and
  the bundle is missing or incomplete, startup fails before HELLO.
- Reserved for future extensions: `algorithm`, `expires_at`, `endpoint_hint`.
  SDK MAY ignore unknown fields when `version == 1`.

### Failure modes — all fail-closed at boot

The runtime MUST exit with status 1 in any of these cases. There is no
fallback to anonymous registration:

- File missing at the configured path
- File unreadable (permissions, FS error)
- File is not valid JSON
- `version` field absent or `!= 1`
- `key_id` absent / empty
- `private_key_pem` absent / not parseable as PKCS#8 Ed25519
- TLS is enabled and the mTLS bundle is incomplete or cannot be materialized

The exit message names the path and the specific field that failed
validation. Operators see the cause proximate to the symptom rather
than chasing a "registration failed" log line whose root cause is a
typo in the mount path.

### Disaster recovery — lost credential file

The private key is returned exactly once at issue time and is never
stored on the platform. If the user loses the download (laptop
crashed, file deleted, etc.):

1. User signs into `quant-frontend` and goes to
   **Settings → Runtime Credentials**.
2. Click "Revoke" on the lost credential. Any open
   `RuntimeChannel` streams keyed by it close within ~1s; associated
   `runtime_registry` rows transition to `ended`.
3. Click "Generate new credential" — fresh keypair issued, fresh
   download.
4. Mount the new `.cred` file and restart the runtime container.

Revocation is irreversible. The user does not need to talk to support
or run any platform-side recovery tool.

## D3 threat-model review

- **Phishing resistance**: the private key is downloaded exactly once and
  should only be mounted into a runtime container. The UI must not ask the
  user to paste a private key back into the platform after issuance. If a
  key is lost, revoke + reissue; do not "recover" it.
- **Replay resistance**: RuntimeChannel HELLO uses Ed25519 over the
  canonical HELLO payload plus `issued_at_unix_ms` and `nonce`; the server
  rejects timestamps outside the +/-5 minute window and keeps a 30 minute
  nonce LRU.
- **Credential leak handling**: revoke by `key_id`. Revocation closes live
  streams indexed by that key and cancels associated runtime rows via
  `credential_key_id`, so leaked credentials cannot remain routable after
  the control-plane observes the revoke.
- **Server compromise blast radius**: the platform persists only public
  keys; private keys are not stored. A DB-only leak reveals key ownership
  and audit metadata, not signing material. Full control-panel compromise
  can still mint/revoke credentials and proxy strategy requests, so runtime
  credential controls do not replace normal service hardening and audit.
## Auth model

| Credential | Issued by | Verified by | Lifecycle |
|---|---|---|---|
| hosted internal runtime credential | `EnsureHostedRuntime` | server TLS + mTLS when enabled, then RuntimeChannel HELLO signature verification | one runtime bootstrap; revoked when runtime ends |
| self-hosted runtime credential | `IssueRuntimeCredential` | server TLS + mTLS when enabled, then RuntimeChannel HELLO signature verification | user-held; revoked via `RevokeRuntimeCredential` |
| bare debug user id | guarded local runtime-agent launcher | mTLS client identity + `RuntimeChannel` HELLO debug gate | only when `debug_bare_runtime_enabled=true` and bootstrap IP is allowlisted |

The RuntimeChannel listener supports server TLS and optional mTLS through
the `runtime_channel_server.tls` config block.

## Database

Owned tables in the `control_panel` database (single-instance TimescaleDB):

- `runtime_registry` — every runtime the control plane knows about;
  `source=hosted/self_hosted/bare`, `role=executor/debugger`, per-user permanent
  display-name uniqueness, terminal lifecycle timestamps/reasons, and
  RuntimeChannel connection owner fields. Runtime routing is always by
  `runtime_id`; `name` is display only.
- `runtime_credentials` — Ed25519 public keys plus mTLS client certificate
  metadata for RuntimeChannel identity: `role`, `status`,
  `downloaded_at`, `consumed_at`, `consumed_runtime_id`, `expires_at`,
  `revoked_at`, `hosted_internal`, `client_cert_fingerprint`,
  `client_cert_expires_at`, and `issuer`. Private keys are returned once and
  never stored.
- `runtime_commands` — durable runtime command queue for start/stop/finish,
  shutdown, and status-affecting operations. Rows include target
  `runtime_id`, optional `session_id`, idempotency key, status, deadline,
  ack/completion timestamps, payload/result JSON, and failure reason.
- `session_market_data_subscriptions` — session-scoped data delivery
  authorization derived from the strategy input universe and bound to
  `(session_id, runtime_id, market, symbol, interval, environment)`.
- `stream_delivery_leases` — delivery worker ownership/heartbeat/expiry for
  RuntimeChannel live-data transfer; progress columns track the last delivered
  topic/partition/offset/time.
- `stream_delivery_failures` — non-sensitive delivery diagnostics and rollups
  for failed RuntimeChannel market-data delivery.
- `market_data_writer_leases` — scraper write ownership for
  `(exchange, market, kind, symbol, interval, year)` before records enter
  `{exchange}_{year}` databases.
- `market_data_streams` — physical kline stream aggregate state
  (`desired_state`, `actual_state`, freshness, delivery).
- `market_data_requests` — user-owned demand for live market-data streams.
- `market_data_leases` — live-session TTL claims that keep a stream alive
  while a strategy is consuming it.
- `market_data_history_requests` — finite historical backfill / coverage
  requests.
- `market_data_coverage_segments` — coverage index for backtest preflight,
  Market Data timelines, and download-and-run decisions.
- `runtime_channel_leases` — hashed RuntimeChannel resume tokens; raw tokens
  never leave the runtime process. Bare debug rows may have empty
  `credential_key_id`.
- `runtime_admission_failures` — HELLO/RESUME failure rollups for UI/operator
  diagnosis.
- `runtime_debug_datasets` — self-hosted debugger dataset metadata; bars stay
  in runtime memory.
- `schema_migrations` — applied-migration ledger.

`runtime_pairings` is not part of the current schema or runtime flow.

`users` and `users.plan_code` live in the `portfolio` database owned by
`core-service`; control-panel-service reads `plan_code` via the
`core-service` `GetUser` gRPC.

## Tests

```bash
cd control-panel-service
go test ./...      # auth + plan + service unit tests
```

Integration tests against a real TimescaleDB are not yet wired; the
`TimescaleRepository` is exercised via the cross-service smoke landing
in D1 section 7.
