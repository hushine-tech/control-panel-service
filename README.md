# control-panel-service

Runtime and market-data control plane. It owns runtime registry, route
resolution, hosted runtime provisioning, self-hosted RuntimeChannel streams,
runtime credentials, per-user plan/quota, and the D2 market-data control
plane. The active unification work lives at
`openspec/changes/unify-runtime-control-plane-and-debugger-isolation/`.

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

## RPC overview

| RPC | Purpose | Status |
|---|---|---|
| `RegisterRuntime` | hosted runtime self-register on startup; returns `registration_token` | hosted path only |
| `HeartbeatRuntime` | hosted runtime heartbeat; verifies `x-runtime-token` metadata | hosted path only |
| `ListRuntimes` | list per-user runtime registry rows | landed |
| `ResolveRuntimeRouteByID` | look up a selected `runtime_id` and source | hosted direct-dial path |
| `EnsureHostedRuntime` | lazy-create hosted runtime if missing; idempotent reuse | hosted path |
| `RuntimeChannel` | self-hosted runtime outbound bidi stream | D3 path |
| `RunStrategy` / `PreviewRunStrategy` / `StopStrategy` / `GetStrategyStatus` | proxy strategy RPCs over `RuntimeChannel` | self-hosted path only |
| `IssueRuntimeCredential` / `ListRuntimeCredentials` / `RevokeRuntimeCredential` | keypair credential lifecycle | D3 path |

## Runtime traffic paths (Phase D3)

D3 intentionally runs **two paths**:

| Runtime source | Handler path | Runtime process mode | Auth primitive |
|---|---|---|---|
| `hosted` | `quant-handler` → `ResolveRuntimeRouteByID` / `EnsureHostedRuntime` → direct gRPC dial to runtime | `RUNTIME_INGRESS_MODE=inbound` (default) | short-lived `caller_token` metadata |
| `self_hosted` | `quant-handler` → control-panel proxy RPC → `RuntimeChannel` REQUEST frame | `RUNTIME_INGRESS_MODE=outbound` | Ed25519 signed HELLO + stream registry |

Hosted containers are untouched by D3. The hosted provisioner does not need
to set `RUNTIME_INGRESS_MODE`; the SDK default is `inbound`, so the D1
direct-dial server and `CallerTokenInterceptor` continue to run.

Self-hosted containers MUST set `RUNTIME_INGRESS_MODE=outbound`. In this
mode the runtime does not bind an inbound gRPC port, does not install the
`CallerTokenInterceptor`, loads `/etc/hushine/runtime.cred`, opens
`RuntimeChannel`, sends HEARTBEAT every 30s, and receives strategy calls as
REQUEST frames. See task 4.2 in the D3 change for the SDK switch and task
6.3 for handler-side `caller_token` branching.

`RegisterRuntime(source=self_hosted)`, `PairRuntime`, `runtime_pairings`,
and pairing-code generation were D1 scaffold-only surfaces and have been
removed in D3. The supported self-hosted onboarding path is runtime
credentials + outbound RuntimeChannel.

## Provisioning (Phase D1 section 5)

`EnsureHostedRuntime` is the lazy-creation entry point quant-handler
calls on strategy start. Order of checks:

1. user_id required; `name` is display-only and may be omitted to generate `hosted-*`
2. explicit `runtime_id` route lookup is used for strategy start/status/stop
3. plan / quota / `resource_profile` checks fail-closed
4. allocate `runtime_id` + port + token
5. call `Provisioner.Provision`
6. wait up to `provisioning.registration_timeout_seconds` for the
   runtime's section-4 self-register code to land a row
7. return route; on timeout, deprovision and fail closed

### Provisioner backends

The `internal/provision/` package exposes a `Provisioner` interface so
the service-layer logic above is backend-agnostic. D1 ships:

- **NoOpProvisioner** (default) — refuses every call with
  `ErrNotConfigured`. Service surfaces this as `FailedPrecondition` so
  operators get a clean failure rather than a half-started state.
- **DockerProvisioner** — calls `docker run` via `os/exec` with env vars
  and CPU/memory/pids limits from `provisioning.profiles`.

### `provisioning` config block

```yaml
provisioning:
  image: "hushine/strategy-runtime:executor-dev"   # built by strategy-service/scripts/build_strategy_runtime.sh
  advertise_host: "127.0.0.1"             # what handler dials; LAN IP in cluster mode
  port_range_base: 50100                  # gRPC port pool start
  port_range_size: 200                    # pool size
  registration_timeout_seconds: 30        # wait window for runtime self-register
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

## D1 cutover rollout sequence

For D3 local smoke, `config.local.yaml` is already set to the Docker
backend and handler control-panel routing. Other environments should flip
the cutover toggles in this order to avoid half-cutover states (handler
routes via control-panel but provisioner is NoOp → fail-closed; or
provisioner runs containers but handler dials the legacy fixed
strategy-service → orphans):

1. **Apply migrations**: `make ensure-dbs` at repo root (creates
   `control_panel` DB and applies `users.plan_code` to `account` DB).
2. **Build the runtime image**:
   ```bash
   bash strategy-service/scripts/build_strategy_runtime.sh dev
   ```
   This builds both role images:
   - `hushine/strategy-runtime:executor-dev` for hosted/self-hosted executor runtimes.
   - `hushine/strategy-runtime:debugger-dev` for self-hosted debugger runtimes with `hushine-debug`.
3. **Switch control-panel to docker backend** in
   `control-panel-service/config.local.yaml`:
   ```yaml
   provisioning:
     backend: "docker"     # was "noop"
     docker:
       network_mode: "bridge"                  # Docker Desktop friendly
       control_panel_dial_addr: "host.docker.internal:50054"
       runtime_env:
         ACCOUNT_SERVICE_GRPC_ADDR: "host.docker.internal:50051"
         # ... etc
   ```
4. **Restart control-panel-service** so the new backend takes effect.
5. **Smoke**: `cd control-panel-service && go run scripts/smoke_ensure_runtime.go -user <id>`
   — validates EnsureHostedRuntime → docker run → self-register → token round-trip.
6. **Switch handler feature flag** in
   `gateway/quant-handler/config.local.yaml`:
   ```yaml
   features:
     control_panel_route_resolution: true   # was false
   ```
7. **Restart handler**.
8. After observation window: optionally enable interceptor strict mode
   on the runtime side via `RUNTIME_CALLER_TOKEN_ENFORCE=1` (default
   true; flip to `0` if you want log-only during a long bake).

To roll back: flip handler feature flag to false (instant), then
optionally switch control-panel backend back to `noop`. Existing
running runtime containers continue to be reachable via direct dial
until they're ended / restart out.

## D2 cutover rollout sequence (market-data control plane)

Phase D2 (2026-05-06) moved the demand-driven market-data control plane
(4 tables + 10 RPCs) out of `account-service` into this service.
control-panel-service now owns both the runtime control plane and the
market-data control plane on `:50054`.

Hard cut: same PR removes the source RPCs/tables from account-service
and brings the destination online. Operator runs the migration script
once between `pg_dump` backup and the rolling restart of the 3 callers
(scraper / quant-handler / strategy-service).

1. **Pause writes to the source tables** (or accept a small data window
   loss — strategy-service mode=2 lease renewals during the cutover are
   not catastrophic since leases auto-expire and are renewed every
   30s).
2. **Backup the source tables**:
   ```bash
   pg_dump --data-only \
     --table=market_data_streams \
     --table=market_data_requests \
     --table=market_data_leases \
     --table=market_data_history_requests \
     "$ACCOUNT_DSN" > account_market_data_backup.sql
   ```
3. **Apply control-panel migrations**: `make ensure-dbs` (creates the 4
   `market_data_*` tables in the `control_panel` DB via control-panel
   migrations 0003-0006).
4. **Run the one-shot migration tool**:
   ```bash
   ACCOUNT_DSN="..." CONTROL_PANEL_DSN="..." \
     go run ./scripts/migrate_market_data
   ```
   Copies all 4 tables row-by-row (`ON CONFLICT DO NOTHING` for
   idempotency) and resyncs destination BIGSERIAL sequences via
   `setval(MAX(<pk>))` so the next INSERT does not collide. Exits
   non-zero on row-count parity failure.
5. **Restart all 3 callers in any order** — they now dial
   control-panel-service for market-data RPCs:
   - `scraper`: `market_data.control_plane.market_data_control_panel_grpc:
     "127.0.0.1:50054"` (default).
   - `quant-handler`: reuses `dependencies.control_panel_service_grpc`.
   - `strategy-service`: `dependencies.market_data_control_panel_grpc`
     (defaults to `dependencies.control_panel_service_grpc` if unset).
6. **Apply account-service migration 0012** to drop the now-orphaned
   source tables: `cd account-service && make ensure-db` runs
   `0012_drop_market_data_control_plane.sql`.
7. **Verify** the live path end-to-end: `quant-frontend` market-data
   CRUD; scraper reconcile loop; mode=2 strategy session preflight +
   lease renewal.

Roll back: there is no in-product rollback after step 6. Restore from
the `pg_dump` taken in step 2; revert the same PR; rebuild.

## D3 self-hosted runtime onboarding

D3 leaves existing hosted containers untouched. Hosted runtimes still use
the D1 direct-dial path (`RUNTIME_INGRESS_MODE=inbound`, the default) and
short-lived `caller_token`; only `source=self_hosted` routes go through the
control-panel proxy and RuntimeChannel.

Recommended smoke/onboarding sequence:

1. **Start platform services on the Mac**:
   ```bash
   ./restart.sh
   ```
   `config.local.yaml` points DB/Kafka/Jaeger at `192.168.88.10`, while
   service-to-service gRPC stays on `127.0.0.1`.
2. **Build the runtime image and prove hosted/default mode still works**:
   ```bash
   USER_ID=<account.users.id> scripts/smoke_d3_hosted_runtime.sh
   ```
   This runs `EnsureHostedRuntime`, starts a normal Docker runtime, waits
   for hosted self-registration, and validates the caller-token round trip.
3. **Generate a self-hosted credential** in quant-frontend:
   Runtime Management -> Runtime Credentials -> Generate new credential.
   Download the `.cred` file once and keep it out of browser storage.
4. **Start a self-hosted runtime locally**:
   ```bash
   CREDENTIAL_FILE=$HOME/.hushine/runtime.cred \
   CONTROL_PANEL_ADDR=host.docker.internal:50054 \
   scripts/smoke_d3_self_hosted_runtime.sh
   ```
5. **Start a remote self-hosted runtime** to simulate a user machine:
   ```bash
   CREDENTIAL_FILE=$HOME/.hushine/runtime.cred \
   REMOTE_HOST=192.168.88.10 \
   REMOTE_USER=hushine-tech \
   CONTROL_PANEL_ADDR=<mac-lan-ip>:50054 \
   SYNC_IMAGE=1 \
   scripts/smoke_d3_self_hosted_runtime.sh
   ```
   `CONTROL_PANEL_ADDR` must be reachable from the remote Docker host. The
   script copies the credential to
   `/home/hushine-tech/.hushine/runtime.cred`, fixes permissions to `0600`,
   optionally syncs the image, and runs the container with
   `RUNTIME_INGRESS_MODE=outbound`.
6. **Observe the stream**: the runtime registry should show
   `source=self_hosted`, `status=active`, and `credential_key_id` populated.
   Operator signals to watch are stream uptime, last-frame latency,
   in-flight calls, and dropped-command counters/log lines from
   `internal/runtimechannel`.
7. **Run a mode=0 strategy/backtest from the frontend** with the handler
   cutover flag enabled. Hosted users should still route through direct
   dial; self-hosted users should route through the control-panel proxy.

Credential loss or suspected leak uses the disaster-recovery flow in the
next section: revoke the old credential, confirm streams close and runtime
rows cancel, generate a new `.cred`, then restart the runtime container.

## Runtime credential file contract (Phase D3)

A self-hosted strategy-runtime container reads its Ed25519 credential
from a JSON file at startup. This section is the canonical reference for
the file path / permissions / schema / failure modes; the UI download
flow (`/settings/runtime-credentials` in `quant-frontend`) and the SDK
loader (`strategy-runtime` task 4.1) MUST stay aligned with it.

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
  "private_key_pem": "<Ed25519 private key in PEM (PKCS#8)>"
}
```

- `version` is mandatory. `version != 1` → fail-closed at boot.
- Reserved for future extensions: `algorithm`, `expires_at`,
  `endpoint_hint`. SDK MAY ignore unknown fields when `version == 1`.

### Failure modes — all fail-closed at boot

The runtime SDK (per `RUNTIME_INGRESS_MODE=outbound` semantics) MUST
exit with status 1 in any of these cases. There is NO fallback to
anonymous registration:

- File missing at the configured path
- File unreadable (permissions, FS error)
- File is not valid JSON
- `version` field absent or `!= 1`
- `key_id` absent / empty
- `private_key_pem` absent / not parseable as PKCS#8 Ed25519

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
- **Hosted path boundary**: `caller_token` remains hosted-only after D3.
  It is not accepted as a self-hosted authentication primitive; self-hosted
  admission is signed HELLO + live RuntimeChannel only.

## Auth model

Phase D1 token-only:

| Token | Issued by | Verified by | Lifecycle |
|---|---|---|---|
| `registration_token` | `RegisterRuntime` | `HeartbeatRuntime` | hosted runtime lifetime |
| `caller_token` | `ResolveRuntimeRouteByID` / `EnsureHostedRuntime` | hosted runtime gRPC server | ≤60s; hosted-only after D3 |
| runtime credential private key | `IssueRuntimeCredential` | `RuntimeChannel` HELLO signature verification | user-held; revoked via `RevokeRuntimeCredential` |

mTLS is deferred to D3+ per `openspec/changes/phase-d1-runtime-control-plane/design.md` Decision 8.

## Database

Owned tables in the `control_panel` database (single-instance TimescaleDB):

- `runtime_registry` — every runtime the control plane knows about;
  `source=hosted/self_hosted`, `role=executor/debugger`, per-user permanent
  display-name uniqueness, terminal lifecycle timestamps/reasons, and
  RuntimeChannel connection owner fields. Runtime routing is always by
  `runtime_id`; `name` is display only.
- `runtime_credentials` — Ed25519 public keys and audit metadata for
  RuntimeChannel HELLO verification: `role`, `status`,
  `downloaded_at`, `consumed_at`, `consumed_runtime_id`, `expires_at`,
  `revoked_at`, and `hosted_internal`. Private keys are returned once and
  never stored.
- `runtime_commands` — durable runtime command queue for start/stop/finish,
  shutdown, and status-affecting operations. Rows include target
  `runtime_id`, optional `session_id`, idempotency key, status, deadline,
  ack/completion timestamps, payload/result JSON, and failure reason.
- `session_market_data_subscriptions` — session-scoped data delivery
  authorization derived from the strategy input universe and bound to
  `(session_id, runtime_id, market, symbol, interval, mode)`.
- `stream_delivery_leases` — delivery worker ownership/heartbeat/expiry for
  RuntimeChannel live-data transfer.
- `market_data_writer_leases` — scraper write ownership for
  `(exchange, market, kind, symbol, interval, year)` before records enter
  `{exchange}_{year}` databases.
- `market_data_streams` — physical kline stream aggregate state
  (`desired_state`, `actual_state`, freshness, delivery).
- `market_data_requests` — user-owned demand for live market-data streams.
- `market_data_leases` — mode=2 session TTL claims that keep a stream alive
  while a strategy is consuming it.
- `market_data_history_requests` — finite historical backfill / coverage
  requests.
- `schema_migrations` — applied-migration ledger.

`runtime_pairings` is not part of the final D3 schema. Historical migration
`0002` creates it for replayability, and `0009_drop_runtime_pairings.sql`
drops it.

`users` and `users.plan_code` live in the `account` database owned by
`account-service`; control-panel-service reads `plan_code` via the
`account-service` `GetUser` gRPC.

## Tests

```bash
cd control-panel-service
go test ./...      # auth + plan + service unit tests
```

Integration tests against a real TimescaleDB are not yet wired; the
`TimescaleRepository` is exercised via the cross-service smoke landing
in D1 section 7.
