# Managed HN rc.10 sample qualification — November 6, 2026

Runtime: `v0.3.0-rc.10`, source
`ad6f9b0c575253b01d4c42de321f07ee372f66d4`, OCI index
`sha256:c0561f5fd192ff494c96dedd11a7749ed4b24bcb9de415d39a38745b9df44bc2`.
The existing single-node Cloud instance has 1 CPU, 4 GiB memory and 20 GiB engine
storage. The archive and writable current table use tenant-scoped GCS access.
This record covers a frozen sample: 175 archive rows and 117 current rows.

## Release artifact

The published macOS server and embedded library were checksum-verified against
release metadata. The five standalone-auth/text/dense/sparse/hybrid E2Es pass
against that server in 90.27 seconds. These include unchanged-row receipt reads,
numeric inclusion/exclusion, newer writes, retained cuts and restart.

The E2E expiry assertion was updated for the released capability-sharing contract:
five-minute retention is a minimum, with expiry rounded up within a three-second
cohort. The lower bound remains five minutes and the upper bound remains bounded.

## Existing-state upgrade barrier

The rc.9 deployment used the earlier JSON lake WAL. rc.10 requires the shared
managed indexed WAL and rejects that prefix with `ManagedWalMigrationRequired`.
Existing derived serving metadata also failed to decode. Kubernetes readiness and
the Cloud version reconciler completed, while actual queries returned HTTP 500.
A clean-fixture release test does not establish existing-state upgrade safety.

The first attempt was returned to rc.9, and reads were verified before exporting
both tables, definitions, the saved source and the four accepted transactions.
Each WAL header and payload was verified against its content hash; replaying the
four transactions reconstructs all 117 current rows. A content-addressed export
is retained in the existing private checkpoint bucket. The checkpoint-4 portable
producer backup restores with rc.10, preserving its epoch, checkpoint and empty
pending intent.

Frozen logical row digests (rows sorted by `hn_id`, JSON keys sorted, compact
separators) are:

| Table | Rows | SHA-256 |
| --- | ---: | --- |
| Archive | 175 | `46fbe3c44d7d61b4e3387ed021f285719a94b11d14d58de9f4d185f50e4092d5` |
| Current | 117 | `cdc0de527ccaa35ae6e0c1d877a48469f8318ab6b2312ab2c1d98a458d51b719` |

## Fresh-incarnation procedure

Build new object tables under staging names. Keep the archive's immutable input;
use a fresh managed-Iceberg prefix for current. Use new relational-index names,
backend-assigned table identities and freshly published serving artifacts.
Replay the original four batches, preserving source epoch, batch IDs, checkpoints,
key fields and row images. Verify publication, complete row equality, index
readiness, exact retries of every original operation and the saved-overlay query.
Only then retain the original tables under dated names and rename the verified
staging tables to `hn_archive` and `hn_current`. The immutable `hackernews` saved
source continues to reference those public names.

The procedure was rehearsed locally with the published rc.10 binary, the exact
archive Parquet payload and all four original WAL payloads. Fresh tables,
complete row equality, the four renames, 72 unique `database` overlay matches and
restart passed. Original table definitions and object data are retained for
rollback; this procedure does not delete legacy tables or rewrite their WAL.

An incarnation change invalidates old receipt/cursor bindings. Qualify new
cursors across restart after cutover; do not reinterpret a prior incarnation's
capability against the replacement table. Keep the source writer stopped during
export, replay and cutover, then resume only the verified durable owner.

## Live verification

The existing Cloud instance is ready on the pinned rc.10 image, with version
reconciliation idle. Live staging passed complete row equality, publication,
index readiness, all four exact retries and the 72-match saved-overlay check.
The original bindings remain as `hn_archive_rc9_20261106` and
`hn_current_rc9_20261106`; no legacy table or object data was deleted. The saved
source retained its identity and definition, and the existing scoped read key
returned the same ranked first-page IDs for History, Recent and All after cutover.

One unchanged-row upsert advanced the original source epoch from checkpoint 4 to
5 at WAL LSN 5. Immediate accepted reads returned the exact row; combining its
numeric inclusion predicate with an exclusion returned zero rows. Exact replay
returned LSN 5, and the published read returned the same row. After one controlled
Cloud restart, accepted reads, published reads and exact replay still passed.

All three gateway cursors issued before that restart resumed without repeated
rows. Each initial page contained 20 rows; the continuation pages contained
20 History, 17 Recent and 20 All rows. Single request observations were:

| View | Before-restart first page | After-restart continuation |
| --- | ---: | ---: |
| History | 0.438 s | 3.783 s |
| Recent | 1.045 s | 3.932 s |
| All | 2.540 s | 10.580 s |

These requests exercise different pages and cache states through the managed
proxy and private gateway; they are not a matched benchmark or percentile
estimate. The 10.580-second All continuation remains a performance concern even
on this sample. Functional acceptance does not establish public-launch latency.

The authoritative Lite source owner is now checkpoint 5, with the original epoch
and no pending intent. A portable backup was restored and all 497 stored records
were compared with the owner. Its SHA-256 is
`a73f3b12c99132b0841d766752d4a24ed417d9dc2b4a589474c834ba4d6c0b5e`;
the content-addressed backup is retained in the private checkpoint bucket.
Checkpoint-4 backups are archival and must not be resumed as the current writer.
The continuous producer remains stopped and the gateway remains private. No
additional instance, VM or serving capacity was provisioned for this run.

## Remaining scope

These are bounded sample checks. Full-archive construction/performance,
continuous producer supervision and public launch retain their separate gates.
A general rollout needs a format-aware preflight and a resumable fenced migration
workflow; pod readiness alone did not detect this existing-state incompatibility.
Legacy bindings can still emit format-related maintenance warnings; their eventual
retirement needs a separate reviewed cleanup after the rollback retention period.
