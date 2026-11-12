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
