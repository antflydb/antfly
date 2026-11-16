# Production owner redesign qualification, 2026-11-02

This summarizes cleanup through `f4b81822ca`, later owner screening and the
November 6 main integration. Current contracts and
open work live in [production compilation costs](../production-cost-plan.md)
and [COMPILATION.md](../../../../zig/COMPILATION.md). Counts below describe dated
checkpoints; they are not continuing product guarantees.

## Accepted changes

Common tests share compiler artifacts, concrete plans avoid receiver-specific
instantiations, and foreign callers no longer emit their provider's complete
dispatcher. Focused builds and watch mode improve the edit loop without changing
production archive composition.

Complete PDF/image operations use the existing media owner, with allocator,
cancellation, budget and output ownership preserved. Template PDF ownership is
default on after accepting its measured server/library size tradeoff; broader
retained/image migrations remain opt-in.

Whole lake-index attempts can execute in the serverless owner, while callers
retain leases and publication authority. Creator-issued capacity queries restore
parallel admission across the foreign allocator boundary without relaxing either
hard quota. Unknown/low capacity remains serial. The migration remains opt-in.

Local query execution now lives beside private DB state. Relational/document
read sessions, statement snapshots, Reader/query-lease handles and disposable
restore materialization use owner-issued capabilities and whole-page operations.
Exact NUMERIC/SQLARRAY values, cancellation, custom allocators, page release and
post-close result ownership are covered. At the cleanup checkpoint, general DB
lifecycle, statement fences and retained covers/query cuts remained follow-up
work; later fence/local-lease qualification is recorded below.

Public batch decoding imports public values and rejects private command fields;
the internal codec retains private replication/LEANN commands. Preparation can
retry only before primary commit. A failing negative control reproduced duplicate
`$inc` application and swallowed publication failure without that disposition
gate. CPU-only builds also fence concrete CUDA cleanup and execution at compile
time.

## Product measurements and limits

The frozen CPU-only macOS ARM64 ReleaseFast comparison used Zig 0.17, baseline
CPU, stripped products, and Metal/CUDA/system BLAS disabled. It compared
`6dd5b52232` with `7bedd1eb66`, an earlier owner milestone with image/lake enabled,
not the final mutation/read-session/capacity redesign.

| Metric | Baseline | Earlier owner milestone |
| --- | ---: | ---: |
| Installed antfly bytes | 222,918,872 | 220,079,240 |
| Installed libantfly bytes | 80,389,920 | 77,151,552 |
| Combined cold build seconds | 3,596.41 | 3,172.23 |
| Combined warm build seconds | 9.53 | 12.86 |
| Sampled aggregate compiler peak GiB | 7.878 | 7.783 |

This single serialized, contended screen showed 11.8% shorter cold builds and
2.84/3.24 MB smaller products. Warm builds regressed; peak memory was nearly
unchanged. It establishes no order-of-magnitude or final-head Release gain.

The earlier template-only screen reduced antfly by 6,719,536 bytes and increased
libantfly by 347,008 bytes. That tradeoff was accepted for template ownership;
it did not establish a build-time or memory improvement. The size deltas from
different checkpoints are not additive.

Final-link CAPI attribution identified real codec duplication; archive symbol
extents and server instruction-fragment matches were investigative leads only.
Code dominated the C library; fonts/tokenizer assets were secondary. A server
final-link map and controlled optimized final-head captures remain open.

## Later owner screening before main integration

CPU-only ReleaseFast image transport passed 111 source/native observations,
including four VP8 cases in each mode. This establishes equivalence for the
screened fixtures, not GPU coverage or a final-product performance win.

The optimized lake probe used 1,024 rows/file and four alternating rounds
(24 observations). Native/source median attempt ratios were 1.094 cold, 1.119
on checkpoint resume and 1.036 on incremental append. Five parallel native jobs
retained both hard quotas; separate unknown/low-capacity runs passed 12
observations each and stayed serial. Source/store opening was under a
millisecond; most of the gap was physical execution. Lake remains opt-in.

A successful frozen unstripped Debug compiler report at `cefbda93a5` passed 31/31 steps.
Storage/API compiler wall times were 159.6/159.8 seconds, with LLVM accounting
for about 78–79%. Internal batch parsing was the largest API frontend declaration
(4.30 seconds semantic analysis, 4.28 seconds code generation); the mutation
worker was 0.57/0.44 seconds. These are diagnostic attribution leads. They do not
rank individual LLVM functions or establish recoverable savings, and the merged
main source needs a fresh comparison.

The November 6 merged-source report also passed 31/31 steps, this time with
`-Dstrip=true`. Storage/API had 55,853/67,295 generic instances, 45.30/17.48
seconds of semantic CPU time and 11.80/6.85 seconds of IR-codegen CPU time.
Generic formatting, block sorting and JSON parsing/stringification led frontend
costs. Private batch parsing reported only 73.5/51.7 milliseconds of semantic/
codegen time; the older ranking does not justify making it the next redesign.
LLVM emission stages reported 480.4/238.7 seconds of wall time on a busy host,
while LLVM pass timers reported 68.2/47.6 seconds of CPU time. These have different
scopes and must not be summed or interpreted as one slow function. Both source
and debug-information settings changed from the earlier capture, so these are
attribution leads, not a controlled speed comparison.

## Qualification checkpoints

- Protected retained-read cache matrix: six phases, each 88 steps and five
  fixtures. Actual Reader/lease/document-reader private fields preserve API,
  kernel and independent native-client caches; shared Request edits invalidate
  all participants. An earlier cache-evicted attempt was rejected.
- Media: eleven retained-PDF and nine image Debug cache phases, source/native
  equivalence, counted endpoint-layout rejection, allocator failure and repeated
  cancellation. Earlier integration included 13 PDF/OCR/query HTTP regressions.
- Lake: final same-executable auto/unknown/low profiles passed 48 observations;
  auto admitted five native writers while unknown/low stayed serial. Real S3
  replay passed ten tests with no skips, failures or leaks. Retained metadata
  reopen, table identity and retired-snapshot fences were exercised.
- Earlier lake Debug timings regressed on the shared host. Foreign I/O dispatch
  A/B timings overlapped. These are not rollout or throughput acceptance.
- Prepared commit: three new regressions plus existing admission/recovery cases
  passed 67 build steps; disabling the disposition gate failed the negative
  control. Close/reopen and ordered-receipt replay are not process-kill coverage.
- After main integration at `ee62b32b72`: 172 product/CAPI smoke/archive/read/mutation
  steps passed; five retained-read fixtures, three mutation tests, five public
  decoder tests, LEANN round-trip/malformed-command tests, nine PDF tests, all ten
  archive guards, the Apache boundary and 210 PR-policy tests passed.
- Final `f4b81822ca` also merged Ollama completion changes. Both products rebuilt;
  generating/agent tests passed 88 steps and 47 build-reported tests. Custom
  runners additionally reported 219 inference cases passed/one skipped, seven
  local cases and three incomplete-generation HTTP cases. Socket-blocked sandbox
  runs were rerun successfully with local networking enabled.

The November 6 integration of `c8afb96719` passed 156/156 stripped Debug product,
CAPI smoke and native-client steps, 58/58 lease/privacy steps and the retained
generation open/read/close regression. All six independent native read-session
fixtures passed. The five-phase capture matrix preserved API/kernel/client
caches across actual private fence/statement fields and rebuilt all participants
for a shared-handle edit. A separate unstripped rebuild passed 67/67 steps and
all ten archive guards. Cache cleanup passed 14 tests, including a failing
negative control for diagnostic-bearing process-inspection results.

Normal-runner optimized qualification of the final source remains required.
Raw logs, linker maps, source snapshots and benchmark JSON remain ignored under
`.benchmark-results/`; qualification workflows retain their own artifacts.

## Rejected or deferred experiments

- Tiny independent index lifecycle and tensor archives proved cache mechanics,
  not a safe complete-operation split or a production size/runtime benefit.
  Their standalone providers, clients and ABI were removed during PR cleanup.
- Q8 projection-loop variants reduced estimated object text and passed numerical
  screens, but small-row timing was mixed. No production kernel change was
  applied; the exploratory overlay generator was removed.
- Mutation merge outlining saved only 24,304 archive bytes without controlled
  runtime/compiler evidence. Original code was restored. A later partial Debug
  declaration report failed its final archive link and is diagnostic only.
- ThinLTO worked in tiny ELF probes but the pinned compiler rejected Mach-O LTO.
  Safe code-folding flags were unsupported in the tested driver. Neither became
  a product default.
- A switch-based foreign I/O dispatcher showed no material runtime win; production
  retains the original linear body. Qualification hooks remain to guard real
  lake work and capacity behavior.

## Pinned detailed records

The full pre-cleanup records remain at commit `f4b81822ca42eaedf3c7bae6f1e12afc18fa8dca`.
These preserve detailed fixtures, raw receipt names and intermediate measurements
without carrying their chronological checklists in the living documentation.

| Record | Detailed evidence |
| --- | --- |
| Shared image operations: implementation and Debug qualification | [Pinned record](https://github.com/antflydb/antfly/blob/f4b81822ca42eaedf3c7bae6f1e12afc18fa8dca/docs/design/compilation/history/image-owner-2026-11-02.md) |
| Lake execution ownership prototype | [Pinned record](https://github.com/antflydb/antfly/blob/f4b81822ca42eaedf3c7bae6f1e12afc18fa8dca/docs/design/compilation/history/lake-execution-owner-2026-11-02.md) |
| Pinned linker capability screening | [Pinned record](https://github.com/antflydb/antfly/blob/f4b81822ca42eaedf3c7bae6f1e12afc18fa8dca/docs/design/compilation/history/linker-capabilities-2026-11-01.md) |
| Retained media session and measurement implementation | [Pinned record](https://github.com/antflydb/antfly/blob/f4b81822ca42eaedf3c7bae6f1e12afc18fa8dca/docs/design/compilation/history/media-session-2026-11-01.md) |
| Opaque read sessions and complete restore decoder operations | [Pinned record](https://github.com/antflydb/antfly/blob/f4b81822ca42eaedf3c7bae6f1e12afc18fa8dca/docs/design/compilation/history/opaque-read-session-and-restore-decoder-2026-11-02.md) |
| Operation ownership and borrowed batch experiments, 2026-11-01 | [Pinned record](https://github.com/antflydb/antfly/blob/f4b81822ca42eaedf3c7bae6f1e12afc18fa8dca/docs/design/compilation/history/operation-owner-probes-2026-11-01.md) |
| Operation owner rollout qualification (2026-11-02) | [Pinned record](https://github.com/antflydb/antfly/blob/f4b81822ca42eaedf3c7bae6f1e12afc18fa8dca/docs/design/compilation/history/operation-owner-rollout-2026-11-02.md) |
| Production inference retention, 2026-11-01 | [Pinned record](https://github.com/antflydb/antfly/blob/f4b81822ca42eaedf3c7bae6f1e12afc18fa8dca/docs/design/compilation/history/production-retention-2026-11-01.md) |
| Q8K grouped projection loop screen | [Pinned record](https://github.com/antflydb/antfly/blob/f4b81822ca42eaedf3c7bae6f1e12afc18fa8dca/docs/design/compilation/history/q8-projection-loops-2026-11-02.md) |
| Owner-issued query execution capabilities | [Pinned record](https://github.com/antflydb/antfly/blob/f4b81822ca42eaedf3c7bae6f1e12afc18fa8dca/docs/design/compilation/history/query-execution-capability-2026-11-02.md) |
| Production query-owner redesign, 2026-11-01 | [Pinned record](https://github.com/antflydb/antfly/blob/f4b81822ca42eaedf3c7bae6f1e12afc18fa8dca/docs/design/compilation/history/query-owner-redesign-2026-11-01.md) |
| Retained PDF operation ownership | [Pinned record](https://github.com/antflydb/antfly/blob/f4b81822ca42eaedf3c7bae6f1e12afc18fa8dca/docs/design/compilation/history/retained-pdf-owner-2026-11-02.md) |
| Template PDF owner migration | [Pinned record](https://github.com/antflydb/antfly/blob/f4b81822ca42eaedf3c7bae6f1e12afc18fa8dca/docs/design/compilation/history/template-pdf-owner-2026-11-02.md) |
| Compile work avoidance, 2026-11-01 | [Pinned record](https://github.com/antflydb/antfly/blob/f4b81822ca42eaedf3c7bae6f1e12afc18fa8dca/docs/design/compilation/history/work-avoidance-2026-11-01.md) |
| Compile work reduction, 2026-11-01 | [Pinned record](https://github.com/antflydb/antfly/blob/f4b81822ca42eaedf3c7bae6f1e12afc18fa8dca/docs/design/compilation/history/work-reduction-2026-11-01.md) |
