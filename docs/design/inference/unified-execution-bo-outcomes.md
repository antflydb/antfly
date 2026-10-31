# BO: Integrating the new CUDA encoder and projection programs

This batch merges main at `51864a57b0562353f8ce8db8889cf13738504367`, including
#1331's OCR throughput work, into the combined Tile/native consolidation branch.
It preserves that work's buffer cache and model admission defaults alongside the
branch's allocation, profiling and readback contracts.

Main subsequently advanced to `ebb4bfce947952a7ec2198a4ae3ad13fb7308289`
with #1427's artifact-only compression change, which is merged as well. That
change selects size compression (7,942,584 bytes). This branch retains its
previously qualified fast compression (15,000,488 bytes) and stricter export
checks, preserving the measured startup tradeoff. The branch artifact is larger
than today's main; the earlier 74.3% saving is against the uncompressed artifact
batch baseline, not today's main. The historical main receipt remains intact.
All ten artifact-regeneration tests pass after resolving the overlap. This
second merge changes no production source or artifact bytes from the tested
candidate.

## Operation families and shared phases

Four new model-named native owners become `attention_inputs_f32.zig`,
`encoder_block_bf16.zig`, `projection_epilogues_bf16.zig` and
`projection_top1_bf16.zig`. Precision-profile parsing stays in the model/host
adapter. Existing environment values, diagnostics and exported device symbols
remain compatibility contracts. Native filenames and the authoring guide now
describe operation, storage and fixed schedule rather than a checkpoint.

`EmbeddedModule` and the typed `EmbeddedPrograms` bank share transactional image
loading and entry lookup. A failed lookup does not publish a partial owner;
failed context selection or unload retains the owner for retry. Cached program
access preserves the original absence of additional driver/context calls.
Launch geometry, arguments, dynamic shared memory and launch accounting remain
with each tuned family. Common region admission permits read aliases and empty
optional inputs while rejecting truncated, overflowing or overlapping outputs.

The GELU and residual projection producers share one Triton dot/bias phase,
including the separately rounded FP32 bias addition. Tiles, tensor-core operand
precision and epilogues remain static. Regenerating all eight projection images
with the pinned Triton 3.7.1/CUDA 13.0 toolchain produces byte-identical PTX.
Both top-1 images also regenerate byte-identically; their producer now requires
exactly the serving launch's 32768/16 shared-memory bytes. All twelve renamed
PTX images preserve their complete original bytes. Renaming compatibility
device symbols would not earn a reduction and is deliberately unnecessary.

## Ownership accounting and cache identity

The CUDA JIT dependency closure now includes all new source owners, images,
producers and their shared phase, along with buffer/cache/library dependencies.
The independent runtime-cache test confirms enabled-backend invalidation.
The source ledger now counts deleted owners, Python producers and PTX payloads,
so a rename cannot masquerade as a reduction. It also handles nested generated
regions. The ledger against the post-merge parent is retained with the receipts;
this integration adds tests and validation and earns no net source-size or
device-image reduction credit.

The final native-source ledger reports 100 additional authored lines / 5,067
bytes, including ownership and admission tests and the regenerated error table;
generated payload bytes are unchanged. These are this batch's net figures,
separate from prior batches and from main's newly added kernels.

## Qualification

The cross-compiled CUDA suite and benchmark build successfully. Glowworm runs
103 tests successfully; one unrelated EmbeddingGemma media-composition test
skips because its external fixture is unset. The new owner, region-admission,
readback and encoder tests pass. Qualification-tool tests (17), reader-pool tests
(5), concurrency tests (3), engine-error freshness, license checks and runtime
cache-identity checks pass.

The local Metal multi-row paired-projection regression also passes after the
merge with both API and shader validation enabled (one selected test, no skips).
The sandboxed attempt stalled before Metal initialization; the physical GPU run
uses the same compiled executable outside the sandbox.

Independent checkpoint references pass without changing any tolerances:

- Attention normalization, gate extraction and partial M-RoPE: 40 cases across
  the real 0.8B/4B checkpoints, with the existing 2e-4 absolute budget.
- Projection/top-1: 12 cases, exact selected tokens, unchanged 2e-4 winning-score
  budget, suppression cases, read-only operands and canaries.
- BF16 encoder blocks: 118 records through the actual serving `Module.block`
  qualification ABI, using every checkpoint layer and the existing independent
  Transformers/PyTorch reference. Alias, alignment, shape and negative controls
  pass. This is a block correctness result, not whole-OCR throughput evidence.

An initial reference attempt used an older incomplete checkpoint download and
failed before qualification. Its logs are retained. The successful run uses the
complete checkpoints from the recent OCR qualification work. An early timing
launch before transfer completion also failed to find its configuration; these
logs are retained separately from completed measurements.

Frozen binaries, source overlay, original image hashes, regeneration receipts,
reference reports and timing runs live under
`.benchmark-assets/1166/bo-campaign`. GPU comparisons use Glowworm's GB10;
physical L4/A100 checks remain explicitly deferred. No global clocks, affinity
or another checkout's processes are changed.

## Post-integration full-model comparison

The frozen baseline is main `51864a57b0` after the OCR throughput merge, with
only the shared benchmark harness added. The candidate is the native source
committed in `a3007b8b05`; the subsequent artifact-only main merge leaves those
sources and artifact bytes unchanged. Receipt binary hashes match the frozen
manifest. All six cases pass AA, AB and reversed BA: **36 timing gates and 288
finite, byte-identical output processes**. Each matrix retains 16 balanced arms,
25 warmups, 301 measured iterations and unchanged 1.05 median / 1.10 p95 limits.

| Case | AB median / p95 | BA median / p95 |
|---|---:|---:|
| minilm-cuda-b1-s4 | 0.99981 / 1.04184 | 1.00234 / 1.02439 |
| minilm-cuda-b1-s8 | 1.00209 / 0.97527 | 0.99842 / 1.06567 |
| minilm-cuda-b1-s16 | 0.99636 / 0.99903 | 0.99934 / 1.00884 |
| minilm-cuda-b1-s128 | 1.00144 / 1.00189 | 1.00084 / 1.00081 |
| minilm-cuda-b2-s16 | 0.36030 / 0.34245 | 0.34780 / 0.35051 |
| reranker-q4k-cuda | 1.00165 / 0.99738 | 1.00085 / 1.00029 |

Batch-two median/p95 time falls 64.0–65.2% / 64.9–65.8%. Other cases have
maximum median/p95 ratios of 1.00234 / 1.06567. AA maxima are 1.00984 / 1.00789.
These are warmed, pretokenized model execution results, not whole-server or
whole-OCR throughput. The original cold tradeoff remains: the newly pinned
24 KiB case's first warmup is 1.75933 / 1.51820 times baseline in AB / BA.
The first warmup of batch-two is also 1.19959 / 1.15458 times baseline. No cold
latency parity is claimed. The baseline precedes main's size-compression update;
these cold ratios must not be presented as comparisons against that packaging.

The runner's premature configuration-not-found attempts, incomplete-checkpoint
reference attempt and all completed raw runs are archived separately. The final
receipts are SHA-256 inventoried before disposable worktree cleanup.
