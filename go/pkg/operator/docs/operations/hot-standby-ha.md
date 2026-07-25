# Hot-Standby HA

This runbook covers operator-managed Postgres-style hot standby for Antfly
clusters running in Standalone mode. It is separate from the Raft metadata HA path:
hot standby is a single-primary data-plane strategy with one or more standby
processes receiving and applying HA WAL records.

> [!IMPORTANT]
> Hot-standby HA is release-gated off by default. The operator must be started
> with `--enable-hot-standby-ha=true` before it will admit or reconcile an
> `AntflyCluster` with `spec.highAvailability.mode: HotStandby`.

## Control Surfaces

Use the typed admin API for normal automation:

| Surface | Use |
|---------|-----|
| `/admin/v1/ha` | Operator and SDK control-plane actions |
| `antfly ha ...` | Human and break-glass commands, plus pod-local helpers |
| `/internal/v1` | Runtime-to-runtime replication traffic only |

The Kubernetes operator lives in `go/pkg/operator` and should use the Go SDK
admin wrapper generated from `specs/openapi/antfly/admin.yaml`. It should not
parse CLI output or duplicate admin request and response types for remote HA
operations. CLI-backed Kubernetes Jobs are reserved for local volume work, file
replacement, or explicit break-glass execution.

## Prerequisites

Hot standby requires an `AntflyCluster` with:

- `spec.highAvailability.mode: HotStandby`;
- `spec.highAvailability.identity` with a cluster id, current primary id,
  timeline, and epoch;
- standby topology, including standby names, slots, and admin URLs;
- durable runtime paths for the primary HA log, standby received-WAL log,
  progress WAL, fence WAL, and former-primary log where applicable;
- `spec.highAvailability.admin.primaryURL` for primary-scoped actions;
- `spec.highAvailability.admin.executePlannedActions: true` when the operator
  is expected to execute typed admin actions;
- matching admin bearer-token environment injection for the operator and
  Antfly pods when admin auth is enabled.

Automatic failover additionally requires a supported fencing authority, a
primary-route selector, a promotion target with safe-read progress, and admin
URLs for every node the operator may promote, demote, rewind, or reseed.

## Admin Token Handling

Prefer `ANTFLY_HA_ADMIN_TOKEN` for both the operator and Antfly pods.
Kubernetes should inject it from a Secret into process environments; the
operator does not need broad Secret read permissions just to call the HA admin
API.

When Antfly pods use `spec.highAvailability.runtime.adminTokenSecretRef`, set
`optional: false` or omit `optional` so Kubernetes fails pod startup if the
token Secret is missing. This field is a pod/Job `SecretKeySelector`; the
operator does not read the Secret value from the Kubernetes API. Operator status
probes and typed HA admin actions still require the token to be injected into
the operator pod through `spec.highAvailability.admin.tokenEnvVar`. Use
`spec.standalone.envFrom` only when the same Secret is already being injected for
other runtime configuration.

When using CLI commands, pass `--ha-token-env ANTFLY_HA_ADMIN_TOKEN`. Do not
put raw tokens in command-line flags because argv can be exposed through process
inspection and job history.

## Daily Checks

Inspect the HA status:

```bash
kubectl get antflycluster my-cluster -o jsonpath='{.status.haStatus}' | jq
```

Inspect HA conditions:

```bash
kubectl get antflycluster my-cluster -o jsonpath='{.status.conditions[?(@.type=="HAAvailable")]}' | jq
kubectl get antflycluster my-cluster -o jsonpath='{.status.conditions[?(@.type=="HADegraded")]}' | jq
kubectl get antflycluster my-cluster -o jsonpath='{.status.conditions[?(@.type=="HAAutomaticFailoverReady")]}' | jq
```

The key conditions are:

| Condition | Check |
|-----------|-------|
| `HAAvailable` | At least one desired standby is safe for reads |
| `HADegraded` | Synchronous durability or admin reachability is degraded |
| `HAUnhealthy` | A desired standby is missing, inactive, or reporting errors |
| `HALagging` | A desired standby has replication lag |
| `HARetentionPressure` | Slots are forcing WAL retention beyond policy |
| `HAReseedRequired` | One or more standbys require reseed |
| `HAAutomaticFailoverReady` | Fenced automatic promotion prerequisites are met |

In `status.haStatus`, check:

- `primaryAdminReachable`, `primaryAdminLastError`, and
  `primaryAdminStatusCode`;
- `primaryLSN`;
- `standbys[*].receivedLSN`, `standbys[*].appliedLSN`,
  `standbys[*].safeReadLSN`, `standbys[*].status`, and
  `standbys[*].lastError`;
- `sync.degraded`, `sync.mode`, `sync.required`, and `sync.satisfied`;
- `retention.reseedRecommended`;
- `fencing.ready`, `fencing.authority`, `fencing.generation`, and
  `fencing.reason`;
- `primaryRoute`;
- `formerPrimary`;
- `plannedActions`.

## Bootstrap and Reseed

Standby bootstrap should be visible through `status.haStatus.plannedActions`.
Expected actions include slot creation, standby seed scheduling, seed
bootstrap, and seed completion. Each executable action should include:

- `adminMethod`, `adminPath`, `adminURL`, and `adminNodeID`;
- `adminJobName` and `adminJobPhase`;
- `adminResult.actionID`, `adminResult.actionKind`,
  `adminResult.actionTarget`, `adminResult.actionState`, and
  `adminResult.actionNodeID` after success;
- seed evidence such as `seedManifestPath`, `seedContentRoot`,
  `adminResult.manifestID`, and `adminResult.backupLSN`.

A lagging standby must not pin WAL forever. If retained WAL is no longer
sufficient, the operator should mark the standby as reseed-required and publish
the reseed action instead of silently dropping required records or holding
unbounded retention.

## Promotion

Automatic promotion requires a machine-checkable fence. The operator should not
promote a standby just because the primary admin URL is unreachable.

For Kubernetes Lease fencing, validate the Lease against the exact promotion
boundary:

- cluster id;
- shard or table identity;
- current primary id;
- timeline;
- epoch;
- observed primary LSN.

A stale Lease from an older identity, timeline, epoch, primary, or LSN blocks
automatic promotion even if the holder name and renewal time look valid.

Promotion should publish planned actions for fence acquisition, promotion
assessment, standby promotion, primary-route update, and former-primary repair.
Safe promotion and forced lossy promotion must produce distinct receipts. A
forced promotion should be treated as an explicit RPO decision, not as the
default failure path.

## Former Primary Return

A former primary must not resume writes after promotion. It must observe the
newer timeline and take one of these paths:

| Path | Requirement |
|------|-------------|
| Demote | The node can stop accepting writes and remain out of the primary route |
| Rewind | Retained WAL and fork evidence are sufficient to repair local state |
| Reseed | Rewind is unsafe, impossible, or retention has expired |

Former-primary rewind targets the former primary's admin URL because it needs
that node's local storage and WAL evidence. Former-primary reseed coordination
targets the current primary for slot and seed scheduling, then uses a pod-local
helper only for the local data replacement step on the former primary.

The status and receipt should prove the node that was acted on. Reject action
results whose `adminResult.actionNodeID` does not match the planned
`adminNodeID`.

## Alerts and Evidence

Alert on:

- admin 401 or 403 responses;
- missing or unreachable admin URLs;
- missing typed result evidence;
- unhealthy or lagging standbys;
- retention pressure and reseed requirements;
- degraded synchronous commit;
- stale or unsupported fences;
- unsafe promotion requests;
- old-primary write attempts after promotion.

For incidents, preserve:

- `status.haStatus`;
- `status.haStatus.plannedActions`;
- typed admin receipts;
- fence token, holder, generation, and reason;
- timeline, epoch, and LSN boundaries;
- primary-route update status;
- operator logs around direct admin API execution.

This evidence should explain why the operator promoted, refused to promote,
rewound a former primary, or required reseed.

## Related Design

See `zig/HA.md` for the storage and control-plane design, including the
production readiness and Postgres-parity checklist.
