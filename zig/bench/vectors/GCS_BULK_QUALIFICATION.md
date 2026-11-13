# Local GCS bulk-builder qualification

`gcs_bulk_local.py` runs the non-test native probe on the local machine against
GCS source and artifact prefixes. It creates no cloud worker, disk, IAM or bucket.
Existing local application-default credentials refresh through the native GCS
client; credentials are not copied into reports. Source hashes, recall payloads
and object generations are verified before/after execution. Results include
local-to-GCS WAN effects and do not establish same-region throughput.

Build `lake-execution-runtime-probe-build` for the host with `-Doptimize=fast`,
`-Dmetal=false`, and worktree-owned caches. Use Python 3.11+ and run
`gcs_bulk_local.py --help`. Inputs are hash-verified authored Cohere Parquet
with `id` and `emb` columns; recall uses the established raw float32 /
float64-accumulation L2 oracle. Source uploads and fixture verification are
outside measured child time. Use `--upload` for absent objects in a new test
prefix; generation-match-zero prevents replacement. Framed 10M bodies stream
directly to GCS without another raw corpus on disk. The 10M case requires at
least 140 GiB free scratch headroom. The staging bucket's eight-day lifecycle
applies to temporary source and artifact objects.

Variants:

- `incremental`: omit `lake_bulk_build` and use the established insertion path.
  Test at 100K; its existing 512 MiB input / 1 GiB output admission cannot
  admit the 1M corpus. Larger comparisons use the explicit bulk control below.
- `control`: bulk Lloyd clustering with fused identity and parent preassignment
  disabled. This is an ablation, not an old release binary.
- `optimized`: fused source/identity ingestion and parent preassignment with
  ordinary Lloyd clustering. This isolates architecture from e-means.
- `optimized --interrupt 1` or `2`: return cancellation immediately after saving
  the source or completed-tree receipt, then start a separate process from the
  saved catalog and require successful remote reopen and recall.

The controller hashes the binary, tracked changes, harnesses, source body and
recall payloads. Each case uses a unique artifact prefix and has a wall-time
limit. Timeout cleanup terminates the process group. Raw logs and process
measurements survive failed qualification.

Reports retain child wall/CPU/RSS, native operation time and phase/work/recall
records. RSS uses native `wait4` units. Network counts are absent on macOS.
Two-second case-directory samples exclude the native builder's `/tmp` scratch;
host minimum free space includes unrelated work and is not an attributed scratch
peak. No object-store request-count claim is made. Phase timers are nested.

The native timer includes immutable artifact publication and saving the test
catalog; source opening and recall are outside that timer but inside child wall
time. The local lease is not the managed catalog CAS/Raft control plane. Receipt
tests cover process restart at phase boundaries, not power loss or arbitrary
SIGKILL. Source-backed Parquet reranking remains outside this experiment.

Remote recall uses the pooled serving reader's 64 MiB cache plus an owned disk
tier and 8 MiB pinned-block target. It is outside construction timing. Use a
fixed hash-bound ten-query subset for remote scale checks, explicitly separate
from the existing 100-query filesystem qualification. A completed publication
can be verified in another process using `--reopen-from <prior-output>`; its
reported build time is zero and cannot substitute for an actual build timing.
The interrupted original attempt remains failed/partial in the evidence even
when a separate reopen verifies the publication.

Start at 100K and require semantic qualification before advancing to 1M/10M.
Report paired comparisons separately from single scale checks. A workstation
with other workloads is not an isolated performance laboratory. Same-region
qualification can follow `examples/hackernews/regional.py` when needed.

## Local GCS observations (2026-11-12)

The [recorded 100K data](gcs_bulk_local_100k.json) retains binary and source
hashes, raw process measurements and parsed qualification records.

The first 100K construction completed in 599.578 seconds. Its original uncached
100-query recall attempt was stopped; that attempt remains partial. A separate
process reopened the retained publication and passed the fixed ten-query oracle.
A bounded I/O capability around the fused source-receipt handoff then completed
a new construction and its remote recall in the same process:

| Measurement | Original build | Bounded source upload |
| --- | ---: | ---: |
| Native construction, seconds | 599.578 | 374.675 |
| Identity phase, seconds | 150.337 | 149.422 |
| Source receipt phase, seconds | 359.518 | 127.677 |
| Dense phase, seconds | 81.039 | 88.559 |
| Dense allocator peak, bytes | 53,475,836 | 53,475,836 |
| Source allocator peak, bytes | 39,504,883 | 39,504,883 |
| Reused source bytes | 315,512,750 | 315,512,750 |
| New source bytes | 0 | 0 |

The ten-query results match exactly: 977/1000 hits at width 512 and 989/1000
at widths 1024 through 8192, with identical vectors-scored counts. The original
publication was verified separately; there is no paired complete child-wall
comparison. The revised complete child wall was 703.628 seconds, including cold
remote serving. One pair on a shared workstation supports retaining the bounded
upload change, but does not establish same-region throughput or an RSS speedup.

Original construction overlapped one one-second stack sample, one local scratch
inventory, and one GCS artifact inventory. Revised construction overlapped one
one-second stack sample. Neither construction overlapped this experiment's
compilation, staging, or cache collection. The revised sample caught the final
dense publication, which still uses serial uploads in this direct probe path.

A 1024-row filesystem correctness check also passed with unknown and low reported
allocator headroom. Its 3.23 MB source spans more than eight upload chunks, so the
required-inline fallback was exercised. These are correctness checks only.

The 100K established-insertion diagnostic was stopped after 1215.711 seconds
of child wall time, still before vector construction, with no completed
publication. Its one-second stack sample showed logical-fragment preparation
repeatedly translating physical rows through remote identity-tree pages and a
16-page cache. This is a censored observation, not a completed latency or a
bulk-versus-incremental speedup. The probe oracle must translate logical fragment
hits back through the authenticated per-index coordinate space before consulting
the physical-row truth; raw fragment ordinals are not physical Parquet ordinals.


## Review hardening

Publication scopes now check usable CPU capacity after the I/O reserve and use
nonwaiting admission. Disabled, undersized or busy schedulers keep inline
progress. The same scope covers source receipts, completed-tree receipts and
final dense publication. Native checks cover a scheduler with only 8 MiB total
memory, disabled lanes, busy admission, nested inheritance, failed uploads and
cancellation while resuming CPU admission.

Uploads have a whole-transfer deadline, including blocked stdin writes. The
controller terminates the upload's process group and reaps it on timeout. Local
source verification and upload remain outside measured construction time.

Receipt tests require the requested source/tree phase, bind the saved catalog to
the exact emitted manifest and require the resumed builder to accept that same
receipt. The native dense builder must consume zero new input rows on resume and must not repeat the fused
source handoff. Completed-tree resume must perform zero clustering calls and emit
no new clustering work record. These checks prove reuse as well as successful
reopen; they remain graceful phase-boundary process restarts, not power-loss tests.

The review validation build overlaps the frozen 1M scale observation. That run
must not be used as a controlled throughput comparison. Fresh timing comparisons
use frozen revised binaries after compilation and tests finish.


The [review measurements](gcs_bulk_review_results.json) retain the native and
offline test results. An 8,192-row shuffled-coordinate fixture needed 52 remote
navigation reads with the sequential join versus 857 with the previous tiled
lookup strategy and its 16-page/2 MiB cache. These instrumented page-store reads
are not full HTTP request counts. Both use the same authenticated input trees;
the map checks preserve IDs, value evidence and physical-to-logical coordinates.

The frozen 1M attempt failed with `UnexpectedEof` after 1,362.012 seconds of child
wall time, before saving a construction receipt. It supplies neither a completed
native time nor recall qualification. The error name alone does not localize the
cause. The probe now emits completed phase counters on build failures, and the
harness retains them even when qualification fails. Any retry is a separate
attempt and must not replace this failed record.


Both strict GCS receipt checks passed on a synthetic 1,024-row, 768d fixture.
Separate resume processes accepted the exact saved source/tree manifests and
consumed zero new dense input rows. Source resume clustered once; completed-tree
resume clustered zero times and emitted no clustering work record. Both reopened
and returned 197/200 oracle hits at all five widths. Cache collection overlapped
these correctness checks; their elapsed times are not performance evidence. The
final native build passed 87/87 steps, with 27 tests passed, one guarded S3 test
skipped, and no failures or leaks. All 18 Python regression tests passed.


## Final publication and reproducibility checks

After merging main through `25a794d6c7`, the same 100K GCS corpus qualified in
299.899 seconds of native construction. Identity took 155.582 seconds, source
receipt 105.231 seconds, dense construction 30.148 seconds, and final dense upload
13.222 seconds. The checkpoint-only version took 374.675 seconds overall and
44.081 seconds for final dense upload. Dense/source allocator peaks remained
53,475,836 / 39,504,883 bytes, and all 315,512,750 source bytes were reused.
Recall and scored counts match the earlier fixed ten-query observations exactly.
This is another single shared-workstation WAN observation. No experiment-owned
compilation, staging or cache collection overlapped construction. Small synthetic
control/incremental correctness checks overlapped serving recall only; child wall
and query timings are therefore not isolated comparisons.

Each controller now copies the executable, controller, worker, validator and
recall inputs into its owned output directory and re-execs the copied controller.
Verification-only reopen also copies the retained case descriptor and catalog.
The case descriptor and generated native configuration are read-only and hashed;
all copied input hashes are checked again before qualification. The workspace Git
revision is context, not proof of an executable's compilation revision. The
executable hash identifies the actual frozen artifact. Frozen input size is
recorded separately from sampled child scratch.

Both runners clear inherited bulk/probe flags before applying a case's settings.
Local-file overrides, compact mode, resume flags and headroom experiments cannot
silently change a GCS variant. The GCS worker records its explicit probe settings.
SIGTERM and SIGHUP follow the same cleanup path as timeout/interrupt, signal the
owned worker group and reap its leader. Cleanup also signals surviving descendants
when the group leader has already exited and is safe to call repeatedly.

The native probe emits the actual complete Parquet inventory. Qualification
matches file URIs, lengths and immutable revisions against the admitted objects,
including files that emit no vectors. An explicit generation is checked directly;
a prefix reader's metadata ETag must match the admitted alias and is enforced
on the planned media GET. Planned range reads already skip metadata probes.
The probe checks the owned expected inventory
before beginning construction, and the worker checks the emitted inventory too.
Unexpected vectorless files therefore fail admission even when vector count and
recall would otherwise pass. This is probe-only admission; no publication format
or production source-pinning semantics change.

The shared phase validator requires complete, unique, typed nonnegative counters.
GCS checks also require variant-specific input/work/receipt evidence and reject
construction hidden in verification-only reopen. Offline regressions exercise
live-file mutation after snapshot, late validator imports, leader-exit descendants,
SIGTERM through the actual snapshotted controller, inherited probe flags, empty
phase arrays, malformed counters and generation/ETag mismatches.


Final live GCS admission checks passed for source receipt, completed-tree receipt
and verification-only reopen with the owned expected inventory. A separate
source prefix containing an extra vectorless Parquet file was rejected before
construction: no pending catalog, dense input, clustering work or receipt was
created. Before early admission was added, that same negative fixture completed
native construction and recall and would satisfy all other qualification
contracts when both files were admitted; this isolates the inventory regression.
The recorded report retains both observations. Final validation passed all 87
build steps, 27 native tests (one guarded S3 skip, no failures/leaks), and all 26
Python regressions. These synthetic checks are correctness evidence only.

## Failed 1M scale attempt and follow-up

The final optimized 1M attempt failed with `WriteFailed` after 3,017.935988
seconds during identity ingestion. It produced no construction receipt or
published index. Its controller disappeared without a final report; the worker
retained the native failure. The original stale suite report was preserved
before reconciling it as failed, with the queued incremental/control cases
unstarted. This is not a qualified 1M performance result.

The [investigation](GCS_BULK_FAILURE_INVESTIGATION.md) records the ambiguous
historical error separately from a reproduced SIGHUP cleanup defect and an
unretried conditional GCS POST path. Follow-up coverage exercises interrupted
uploads, ambiguous commits, digest verification after conflicts, cancellation,
deadlines and controller cleanup. Native failure records now identify the
operation boundary, including underlying spool file errors when available.

The follow-up also removes a payload-sized GCS request-body copy. An 8 MiB
pre-transport comparison allocated 8,388,855 bytes for the owned body and 247
for the borrowed body. This is an allocation measurement, not a new full-build
timing. The successful 100K construction remains 299.9 seconds; another 1M
publication/reopen/recall is required before making scale or throughput claims.

The fresh-review follow-up closes launch/re-exec cancellation windows and covers
all controller phases with terminal reporting. Native children remain in the
worker group for escalation, and resource-aware waits preserve usage when
available. Failed and cancelled cases retain disk-sampling checkpoints and
final samples. Real-GCS admission rejects ambient storage endpoint overrides;
ADC mode also rejects nonrefreshable token overrides without logging their
values. Provider mode and canonical endpoints are attested in the worker report.

`lake-identity-publication-test` compares the existing identity chunk target with
the largest compatible target, writes and rereads one million keys per case, and
requires count/order/digest equality for both supported key widths. Measured
chunk puts fall about 25%, under the same 256 KiB reader limit and an 8 MiB test
allocator limit. The fresh bulk tracker uses its native owner's existing private
quota; other tracker callers keep their existing target. See
[the raw observations](bulk_identity_publication_results.json). Filesystem times
and logical artifact puts do not establish WAN throughput or HTTP request counts.

Active cases keep their frozen executable/controller/worker/validator inputs.
Do not replace those inputs when rebuilding the workspace during a scale check.
