# Hot Standby

This runbook covers operator-managed Postgres-style hot standby for Antfly
clusters running in Standalone mode. It is separate from the Raft metadata HA path:
hot standby is a single-primary data-plane strategy with one or more standby
processes receiving and applying HA WAL records.

## Control Surfaces

Use the typed admin API for normal automation:

| Surface | Use |
|---------|-----|
| `/admin/v1/standby` | Operator and SDK control-plane actions |
| `antfly standby ...` | Human and break-glass commands, plus pod-local helpers |
| `/internal/v1` | Runtime-to-runtime replication traffic only |

The old `/admin/v1/ha` paths and the `antfly ha` command name still work as
aliases for one minor release. The Kubernetes operator negotiates the spelling
per server (`--standby-admin-path-style=auto`, the default): it sends the
`/admin/v1/standby` path first and, if a node answers with an unrouted 404,
retries the old spelling once and remembers the answer for that node's URL.
Mixed-version clusters therefore keep working through a rolling upgrade, and
nothing has to be flipped when 0.3 becomes the minimum server. `legacy` and
`canonical` force one spelling for troubleshooting. The operator reads its
own token from `ANTFLY_STANDBY_ADMIN_TOKEN`, falling back to
`ANTFLY_HA_ADMIN_TOKEN`; the variable injected into managed pods keeps its
`ANTFLY_HA_ADMIN_TOKEN` default (`spec.highAvailability.admin.tokenEnvVar`),
since renaming it would roll every cluster.

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
- `spec.highAvailability.admin.primaryURL` through the production route for
  primary status and authority observations;
- optional `spec.highAvailability.admin.primaryActionURL` for primary-scoped
  slot and seed actions that must remain reachable while that route is
  intentionally unready (actions fall back to `primaryURL` when omitted);
- `spec.highAvailability.admin.executePlannedActions: true` when the operator
  is expected to execute typed admin actions;
- matching admin bearer-token environment injection for the operator and
  Antfly pods when admin auth is enabled.

Automatic failover additionally requires a supported fencing authority, a
primary-route selector, a promotion target with safe-read progress, and admin
URLs for every node the operator may promote, demote, rewind, or reseed.

### Data layout on the pod volume

Default runtime paths live under `/antflydb/standby/` (`primary.wal`, `slots`,
`log.wal`, `progress.wal`, `fence.wal`, `seed-captures/`,
`standby-generations/`). Clusters created before 0.3 have the same files under
`/antflydb/ha/` with `standby.wal` and `standby-progress.wal`. The operator
records which layout it renders in `status.haStatus.dataLayout` (`ha` or
`standby`) and never moves it back; the pod template's
`antfly.io/hot-standby-data-layout` annotation carries the same value and is
what the operator trusts if a status write was lost. New clusters start on
`standby`; a cluster counts as new only when it has neither a StatefulSet nor
a surviving volume claim. An existing cluster, or a surviving volume, stays on
`ha` until the operator's admin client has
negotiated the 0.3 `/admin/v1/standby` paths with one of its nodes, which
proves the nodes run a server that can migrate; the operator then flips the
status, emits a `HotStandbyLayoutStandby` event, and the next pod rollout
carries the new paths. On
that start each node renames `ha/` to `standby/` once (everything inside moves
with it) and renames the two standby files; nothing is deleted or overwritten.
Explicit `spec.highAvailability.runtime.*Path` values are rendered as given
and never migrated. Do not switch a cluster to explicit `/antflydb/standby/`
paths by hand while it still runs a 0.2 image: a 0.2 server has no migration
and would start empty at the new paths.

## Admin Token Handling

Inject the token from a Secret into process environments; the operator does
not need broad Secret read permissions just to call the admin API. The
operator itself reads `ANTFLY_STANDBY_ADMIN_TOKEN`, falling back to
`ANTFLY_HA_ADMIN_TOKEN`. Antfly pods keep `ANTFLY_HA_ADMIN_TOKEN` as the
default injected name (`spec.highAvailability.admin.tokenEnvVar` overrides it),
and the server accepts whatever name it is told, so the two sides can differ.

When Antfly pods use `spec.highAvailability.runtime.adminTokenSecretRef`, set
`optional: false` or omit `optional` so Kubernetes fails pod startup if the
token Secret is missing. This field is a pod/Job `SecretKeySelector`; the
operator does not read the Secret value from the Kubernetes API. Operator status
probes and typed hot-standby admin actions still require the token to be injected into
the operator pod through `spec.highAvailability.admin.tokenEnvVar`. Use
`spec.standalone.envFrom` only when the same Secret is already being injected for
other runtime configuration.

When using CLI commands against a running node, pass `--admin-url <url>`; the
token is read from `ANTFLY_STANDBY_ADMIN_TOKEN` (falling back to the deprecated
`ANTFLY_HA_ADMIN_TOKEN`) when one of those variables is set, or from the
variable named by `--admin-token-env`. On the node itself,
`antfly standby --data-dir /antflydb status primary` opens the local HA state without
any path or identity flags. Do not put raw tokens in command-line flags because
argv can be exposed through process inspection and job history. The older
`--ha-url` and `--ha-token-env` spellings still work.

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
- `plannedActions`, including `operationID`, `executionStateVersion`,
  `attemptCount`, `retryBudgetUsed`, `inFlightAttempt`, `attemptID`,
  `reservationExpiresAt`, `prerequisiteDeadlineAt`, `firstAttemptAt`,
  `lastAttemptAt`, `nextRetryAt`, `completedAt`, `retryable`, and `errorClass`.

Typed admin requests use a durable, bounded exponential retry policy. The
defaults are eight retry-budget charges, a five-second initial delay, a
two-minute maximum delay, a 30-second in-flight reservation, and a ten-minute
prerequisite deadline. Override them with `admin.directRetryLimit`,
`admin.directRetryBaseSeconds`, `admin.directRetryMaxSeconds`,
`admin.directReservationSeconds`, and
`admin.directPrerequisiteTimeoutSeconds`.

Before every typed request, the operator persists an exact frozen action and
an in-flight reservation. It sends at most one request, checkpoints the typed
result (and promotion receipt, when applicable), and ends that reconcile. A
restart cannot replay the request before `reservationExpiresAt`; after expiry,
the exact request may be replayed only through the runtime's idempotent receipt
contract. An expired uncertain request consumes retry budget. Status conflicts
retry only the narrow checkpoint and never repeat the external request.

`attemptCount` is monotonic and records every dispatched request.
`retryBudgetUsed` records retryable request failures and expired uncertain
reservations. A valid promotion assessment that is still behind the frozen LSN
increments `attemptCount` but not `retryBudgetUsed`; it waits until the bounded
`prerequisiteDeadlineAt`. Exhausting either bounded failure path produces a
terminal `Failed` action. Ordinary LSN progress and regenerated reason text do
not create a new operation or reset a terminal budget. After an operator has
corrected the cause of a terminal failure, increment
`admin.retryGeneration` to explicitly authorize one new execution identity;
the webhook rejects decreasing this nonce.

CLI-backed Jobs remain bounded by their Kubernetes Job backoff and deadline
settings. They use `restartPolicy: Never`, and `attemptCount` is derived from
started pods owned by the exact current Job UID, not Job counters, names, or
labels. TTL cleanup is armed only on a later reconcile after terminal evidence
has been checkpointed, so even `jobTTLSecondsAfterFinished: 0` cannot delete
the only result before status is durable.

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

### Portable seed artifacts

Configure `standbys[*].seedArtifact` when the source backup and the standby do
not share a filesystem. The same path is used for initial bootstrap and reseed:

The object-store credentials must permit `ListBucketVersions` and
`DeleteObjectVersion` in addition to ordinary list/get/put/delete operations.
When HA is disabled or an instance is deleted, Antfly inventories and deletes
every version and delete marker below the exact instance seed prefix before it
publishes a successful cleanup receipt. This remains required for unversioned
buckets because S3-compatible providers expose the same version-list contract.

1. capture the base backup in the primary runtime and freeze its manifest boundary;
2. publish every file and bounded chunk to an immutable object-store generation, then publish `COMPLETE.json` last;
3. reverify the complete remote v3 generation before garbage-collecting old local source captures;
4. restore and verify that exact topology-bound generation on the standby PVC;
5. atomically activate the verified local generation;
6. activate the replication slot from the target activation receipt;
7. copy that exact raw slot receipt into an immutable, action-scoped ConfigMap and garbage-collect old target generations;
8. only then prune older complete remote generations.

```yaml
spec:
  highAvailability:
    admin:
      executePlannedActions: true
    standbys:
      - name: standby-a
        slotName: standby-a
        seedManifestPath: /source/seed/manifest.afha
        seedContentRoot: /source/seed/content
        seedArtifact:
          location: s3://company-ha-seeds/my-cluster
          generation: seed-standby-a-42
          topologyID: production-us-west
          topologyGeneration: 42
          nodeID: standby-a
          targetPVCUID: 21ea57e0-79d5-48d0-b124-ef42ed73fc3f
          stagingRoot: /target/seed/current
          retainGenerations: 2
          credentialsSecretRef:
            name: ha-seed-object-store
          sourcePVC:
            claimName: primary-data
            mountPath: /source
          targetPVC:
            claimName: standby-a-data
            mountPath: /target
```

Source volumes are mounted only by publish/source-GC Jobs and target volumes
only by restore/activate/target-GC Jobs. Every Job is scoped to one PVC. Before
dispatch the controller compares the persisted topology generation and desired
PVC UID with the current spec and live Kubernetes PVC; the Job name and owner
references bind that exact PVC incarnation. A replacement PVC or topology
generation therefore cannot inherit an old action.

When a live StatefulSet pod already mounts a publish-source RWO PVC, the
operator adds required same-node pod affinity to the artifact Job using the
stable StatefulSet pod-name label. Terminating pods remain consumers until
deleted, preventing an attach race during shutdown. RWX sources need no
consumer-derived placement; RWOP sources cannot be shared with any live
non-owner pod. A restore or activation target must have no live consumer at
all. Only pods controlled by the exact current Job owner UID are excluded; a
same-name stale Job pod is not trusted. Multiple RWO consumers, an
unidentifiable consumer, or an unmounted publish source that is not already
bound to a stable PV fails closed instead of guessing placement.

Legacy unbound publish and restore evidence accepts artifact receipt formats v1
and v2. Production topology-bound transport requires v3, whose `COMPLETE.json`
carries the exact topology ID/generation, target node, slot, target PVC name and
UID. Restore rejects a missing or mismatched binding before writing its durable
activation receipt. Activation, local GC, and remote-prune receipts use their
action-specific v1 schemas. Unknown versions, scopes, fields, trailing JSON,
or identity mismatches fail closed.

The restore helper never clears an arbitrary directory. It accepts a missing or
empty target, a partial directory carrying the matching operator staging
marker, or an already-complete directory whose receipt and every file reverify.
A non-empty unowned directory, wrong cluster/shard/table/timeline/epoch, stale
checkpoint, path traversal, size mismatch, CRC mismatch, SHA-256 mismatch, or
missing `COMPLETE.json` fails before standby bootstrap changes durable state.

Use `s3://` or `gs://` in production and `file://` only for local or KinD
fixtures. Inject credentials through `credentialsSecretRef` or workload
identity; never place credentials in the URI. Configure provider-side TLS,
server-side encryption/KMS, bucket versioning, lifecycle policy, and least-
privilege access scoped to this cluster prefix. Artifact Jobs emit receipts on
stdout, while durable action progress, retries, terminal errors, object-store
location, generation, and retention appear in
`status.haStatus.plannedActions`.

For local KinD validation, use a filesystem-backed object-store fixture mounted
at a `file://` URI and distinct source and target PVCs. Delete the publish or
restore Job/pod between attempts to exercise idempotent restart behavior; never
use `kubectl cp`, `kubectl exec`, a hostPath bridge, or a test-only catalog.

### Durable seed lifecycle evidence

The runtime appends the exact topology-bound receipt to a durable local ledger
only after capture `COMPLETE.json` or target activation `ACTIVE` is fsynced.
Retries with the same identity and digest return the original cursor; reuse of
that identity with different bytes fails closed. The bounded payload history
survives runtime restart and generation garbage collection, while a compact
identity/digest index prevents old identities from being reused after history
truncation.

Controllers read the authenticated, read-only endpoint
`GET /admin/v1/standby/seed-lifecycle/receipts` (the old
`/admin/v1/ha/seed-lifecycle/receipts` path is served as an alias). The
required `kind` query is
`capture` or `activation`; `after` is an exclusive durable WAL cursor and
`limit` is between 1 and 1000. Responses include `first_cursor`, `end_cursor`,
`next_cursor`, `history_truncated`, `gap`, and `has_more`. A `gap` means the
requested cursor predates the retained payload prefix and must never be treated
as proof that an action completed. Each entry carries the exact receipt JSON
and SHA-256, topology generation, node and PVC UID binding, pod UID, timestamp,
and whether the authoritative filesystem receipt is still retained. Runtime
observation includes the serving node, role, pod UID, and current fenced state.

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

## Planned Switchover

Use a planned switchover when the primary is healthy and you want to move the
primary role to a standby for maintenance, a node replacement, or a zone move.
The operator's automatic promotion path is failover only and stays out of the
way while the primary is reachable, so a planned change is driven from the CLI:

```bash
antfly standby --admin-url http://primary-a:8080 switchover \
  --to http://standby-a:8080 \
  --follower http://standby-b:8080 \
  --wait-timeout 60s
```

The command runs the sequence described in `zig/HOT_STANDBY.md` under
"switchover": preflight both nodes without writing, fence the old primary so
writes stop, wait for the standby to reach the primary's final LSN, fence and
promote the standby with the same fence generation, run the former-primary
rejoin assessment (rewind is reported as pending until the old node restarts as
a standby; reseed is applied immediately), and repoint each `--follower` at the
new primary.
Every step prints its typed receipt; add `--json` to capture them for the change
record and `--dry-run` to stop after preflight.

Fence generation: with Kubernetes Lease fencing, pass `--generation` with the
Lease's current transition value. Without a Lease authority, omit it and the
primary allocates the next generation; the CLI reuses that generation when it
fences the standby so both nodes hold one fence.

If the command fails after the primary was fenced, the old primary is read-only
and no data was lost. Either rerun the switchover once the standby has caught
up, or promote with `force` as an explicit RPO decision. After a successful
switchover, roll the former primary's pod into the standby role (the operator
does this from `spec.highAvailability.runtime.standby`) with
`former_primary_log` configured, then run `antfly standby rejoin rewind` against it;
a running primary owns its log, so the rewind cannot happen before the restart.

### Repointing a standby

A standby can be moved to a different primary without a restart:

```bash
antfly standby --admin-url http://standby-b:8080 follow \
  --upstream-url http://standby-a:8080 --slot standby-b \
  --cluster-id 1 --timeline-id 3 --epoch 3
```

The identity flags are a precondition: the request is rejected with a 409 if
the standby's current timeline or epoch differs, so a stale runbook step cannot
repoint a node that has since been promoted or reseeded. Repeating the same
request is a no-op (`changed: false`).

### Using the config file

`antfly standby --config /etc/antfly/config.json` reads the server's
`hot_standby` section (the deprecated `ha` section is still read). When it
names `hot_standby.admin.url` the command targets that endpoint and reads the
token from the variable in `hot_standby.admin.token_env`; otherwise the
section's paths and identity are used as local handles. On a node with the
standard layout, `antfly standby --data-dir /antflydb status primary` needs no
flags at all.

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

The operator metrics endpoint exports bounded-label HA execution telemetry:

| Metric | Meaning |
|--------|---------|
| `antfly_operator_ha_action_attempts_total` | Actual direct API or Kubernetes Job execution attempts |
| `antfly_operator_ha_action_retries_total` | Attempts after the first for an exact action identity |
| `antfly_operator_ha_action_failures_total` | Retryable and terminal failures with a bounded error class |
| `antfly_operator_ha_action_waits_total` | Successful bounded prerequisite observations, such as promotion-boundary waits |
| `antfly_operator_ha_action_duration_seconds` | First-attempt to terminal-completion latency |
| `antfly_operator_ha_seed_artifact_bytes` | Size distribution for successful portable seed operations |
| `antfly_operator_ha_seed_artifact_files` | File-count distribution for successful portable seed operations |

Page on terminal failures, repeated retry-budget exhaustion, and sustained
seed-action latency. Raw Kubernetes Job reasons and response bodies remain in
status and logs; they are deliberately collapsed into bounded metric labels to
avoid cardinality growth.

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

See `zig/HOT_STANDBY.md` for the storage and control-plane design, including the
production readiness and Postgres-parity checklist.
