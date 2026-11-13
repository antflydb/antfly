# 1M GCS failure investigation (2026-11-12)

The final optimized 1M case failed; it is not a qualified performance result.
Evidence remains in the original `test-gcs-bulk-qualification` worktree under
`zig/.benchmark-results/gcs-bulk/local-1m-final-optimized/`. Do not overwrite it.

## Observed native failure

`case/build.log` records `WriteFailed` after 3,017.935988 seconds. The worker
recorded exit 1, 69.076069 user seconds, 32.464969 system seconds and
174,718,976 bytes peak RSS. Identity verification had not completed; 377
`bulk_tee` calls consumed 200.131517 seconds. No source construction receipt,
completed tree receipt, or published index was recorded. Batch count is not a
verified row count. The queued incremental and bulk-control cases did not run.

The error is ambiguous. `std.Io.Writer` local file adapters and HTTP socket
writers can both return `WriteFailed`. The failure log has no operation/path
attribution and its nested cause is null. Available free space after the event
does not establish available space when the write failed.

There **are remote writes during identity ingestion**. `Stage.add` calls
`Tracker.append`, which flushes row-key chunks at roughly 192 KiB through
`ArtifactStore.putWithCancellation`. Native `ObjectStore.putInScope` creates
immutable objects with `if_none_match=true`; GCS implements this with media
`POST` and `ifGenerationMatch=0`. HTTPx's default retry policy retries connection
errors for idempotent HTTP methods. `POST` is excluded; `WriteFailed` is not a
safe-unsent retry error. A connection failure on this path therefore can abort
ingestion without retry. This is a concrete resilience gap, **not proof that
this was the failing operation in the historical run**. Source GET retries,
local spool/SST writes, and credential refresh remain other possible origins.

Any retry fix must retain conditional-create semantics and authenticate an
existing object after an ambiguous successful upload. Do not globally enable
retries for all POST requests or treat a precondition failure as success without
verification. Cancellation, request deadlines and retry limits must remain
bounded.

## Controller lifecycle

`local.json` contains only the initial report, and `final-scale.json` originally
still said running. Neither the wrapper nor its controller survived. The worker
was deliberately in a separate session and did survive long enough to record
the native failure. The controller's worker-wait `finally` would have recorded
an exit status on an ordinary exception or handled SIGTERM.

A local reproduction using the actual termination handler showed:

| Signal | Controller exit | Finally executed | Worker survived |
| --- | ---: | --- | --- |
| SIGHUP before fix | -1 | no | yes |
| SIGTERM | 143 | yes | no |

The real snapshotted-controller regression also failed before the fix with
exit -1 instead of 129. The controller now handles SIGHUP through the existing
cancellation/cleanup path. This closes a reproducible orphan/stale-status bug.
It does not establish which signal killed the original controller; SIGKILL and
external process termination cannot be reconstructed from the retained files.
A targeted macOS unified-log query found no matching Python ReportCrash or
kernel memory-termination event in the run interval; absence of a log entry is
not proof that memory pressure was impossible.

The original stale suite status was preserved separately before reconciliation.
The reconciled status records the failed case and unstarted queued cases.

## Follow-up fixes and verification

Create-only GCS media POSTs now explicitly admit replay under the existing
bounded HTTP retry policy. Unconditional POSTs and resumable-session creation
retain ordinary method safety. A precondition conflict still reaches the
artifact layer for verification; it is never blindly converted to success.
Authentication and HTTP execution share the captured request deadline. Replay
admission does not follow redirects or override a zero retry ceiling.

Offline transport fixtures exercise disconnects and truncated responses,
successful retry, ambiguous commit followed by a 412 conflict, transient 503,
terminal 403, an exhausted retry budget, and an ordinary POST that must not
replay. Cancellation and deadline fixtures interrupt retry backoff; buffered and
writer redirect fixtures require replay admission to be cleared. An artifact-layer
fixture additionally checks that a lost success plus 412 accepts matching bytes
under generation 42 and rejects corrupt bytes.

Probe-only operation records distinguish identity source reads, spill append
and reduction, identity chunk upload/finish, exact-vector flush, and spool
append/finish. The latter retain the file writer's underlying error when
available. Diagnostics use fixed names and error tags, never paths or payloads.
SIGHUP and SIGTERM tests exercise the real snapshotted controller and native
descendants; terminal reports retain cancellation status, controller exit code,
worker exit code and elapsed time.

GCS synchronous uploads also borrow their immutable body instead of copying
it. A paired 8 MiB request-setup allocation fixture measured:

| Body representation | Bytes allocated during setup |
| --- | ---: |
| Previous owned body | 8,388,855 |
| Borrowed body | 247 |

Both cases stop at the same pre-transport interceptor. The fixture checks body
ownership and identity, and all attempts finish before the owner releases the
body. This demonstrates removal of one payload-sized allocation/copy, not a
full-build speedup. The planned source range path already skips metadata probes;
an assumed extra metadata request is not a measured performance opportunity.

The live GCS round-trip passed against the existing development bucket with
local ADC: conditional create, conflict preservation, pinned range reads,
generation CAS and conditional deletion of the created test object. No cloud
worker or infrastructure was created.

## Fresh review follow-up

The first hangup fix still allowed a signal between worker creation and entry
to its cleanup `try`. An isolated real-controller reproduction started the native
child, injected SIGHUP before `Popen` returned, and observed a surviving worker
with status still running and no worker exit/elapsed fields. A separate injected
post-worker metadata failure left status `worker_finished` and exit zero without
a controller error. The historical failed 1M result also confirmed that failure
paths omitted the disk sampler's in-memory minimum-free-space observations.

The controller/worker now use signal-recording cancellation scopes and process
owners established before child creation. Re-exec preserves pending signals;
cleanup ignores repeated cancellation until children are drained and reaped.
Native children remain in the worker group for controller escalation. One outer
reporting boundary handles admission, worker and post-validation failures.
Disk samples are periodically checkpointed and finalized on failure/cancellation,
with explicit sampling errors and unavailable resource measurements.

ADC qualification rejects inherited static bearer tokens and alternate GCS
endpoints before execution, reports names rather than values, and records its
admitted provider mode/endpoints. Explicit ADC files remain supported. Tests cover
signals during launch and exec, repeated signals during cleanup, process creation
failure, post-worker source validation, cancellation while hashing, disk sampling
failure and cancellation after successful native completion.

The measured publication optimization retains the 256 KiB reader cap: a fresh
bulk owner selects a 262,037-byte target and admits the tracker in its existing
private quota. Full one-million-key write/readback fixtures reduced chunk puts
489→367 (96-byte keys) and 550→413 (108-byte keys). A 1 MiB candidate failed
reader validation and was rejected. See the raw
[publication observations](bulk_identity_publication_results.json); these are
artifact API counts, not full-build throughput measurements.

The ongoing GCS 1M run is frozen to the earlier executable/harness snapshot.
Edits in this follow-up do not change that running build's inputs. Any subsequent
reopen has its own executable and harness hashes and must be identified by those
hashes rather than assumed to use the same workspace revision.

## Remaining scale evidence

Follow-up validation passed all 99 native build steps (including ABI integration,
the runtime probe, and the publication/readback fixture) and 45 Python regression
tests. A fresh frozen 1,024-row live GCS case using ADC also qualified: build,
reopen/recall, unchanged source generation and frozen input hashes, and finalized
scratch sampling all passed. Its executable SHA-256 was
`7d367deb1eb7f30c7eabe3a700ecd08a27b58e20f2d2197ce97c813d7e932594`.
The 28.73-second worker wall time is a small correctness check, not a scale or
performance qualification.

The historical failure's exact root cause remains unknown. A new qualification
must retain the operation diagnostics rather than treating the retry fix as
proof of that cause. The original 1M result remains failed and the successful
100K result remains unchanged.

Launch any long suite under an explicit durable supervisor with retained exit
records, and keep cancellation ownership over its worker sessions. Only after a
successful 1M publication and reopen/recall should a paired control or 10M case
be interpreted as a performance comparison.
