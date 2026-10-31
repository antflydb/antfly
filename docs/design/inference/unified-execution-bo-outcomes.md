# BO: Integrating the new CUDA encoder and projection programs

This batch merges main at `51864a57b0562353f8ce8db8889cf13738504367`, including
#1331's OCR throughput work, into the combined Tile/native consolidation branch.
It preserves that work's buffer cache and model admission defaults alongside the
branch's allocation, profiling and readback contracts.

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
