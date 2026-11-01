# Production inference retention, 2026-11-01

This follows [compile work avoidance](work-avoidance-2026-11-01.md), with the
focus on the production `antfly` and `libantfly` owners rather than test caller
compilation. The current architecture is in [COMPILATION.md](../../../../zig/COMPILATION.md).

## Disabled CUDA implementation was still generated

The stripped native CPU-only ReleaseFast inference archive contained an
estimated 1,324,972 bytes of text in the `ops.cuda` namespace, including the
119,368-byte `CudaCompute.deinit`. Mach-O symbol extents are address-gap
estimates, including padding, rather than ELF function sizes.

Following Mach-O branch relocations for the CUDA constructor and destructor
back to their containing text symbols identified two external owners:

- `boundary_training_backend.Owner.deinit` conditionally freed an optional
  CUDA owner at runtime. Initialization already rejected that backend when
  CUDA was disabled, but cleanup still instantiated the CUDA destructor.
- `embedding_gemma2_runner.run` remained callable for CPU-only training dry
  runs. The CLI rejected real CUDA training without CUDA support, but the
  shared runner still compiled its complete execution path.

Cleanup now uses the same compile-time CUDA guard as initialization. The
runner keeps dry-run admission before a compile-time execution fence using
the existing `CudaTrainingBuildRequired` error. CUDA-enabled execution follows
the same implementation. No public ABI, backend defaults, optimization mode,
inference feature advertisement, archive count, or compiler reservation changes.

A wider reference audit found CUDA dispatch in Florence depthwise/MLP,
image-prompt concatenation and cache compaction, plus EmbeddingGemma2 media
and prepared-row execution. Those paths checked the runtime backend tag without
the build capability. They now require CUDA support before calling the concrete
implementation. Portable tensor operations remain the fallback. Video row
packing uses one capability decision for allocation and zero padding, keeping
their ownership and initialization rules consistent.

The existing CPU-only ReleaseFast profiling workflow rejects emitted text from
`ops.cuda.cuda_compute` in the physical inference archive. This checks the
actual compiler output rather than relying on a source-level flag assertion.
The workflow remains opt-in; no normal CI lane or release configuration changes.
The absence gate is specific to the optimized profile: Debug can retain small
unsupported-backend stubs that optimized builds remove.

Archive retention and final product retention are different measurements.
Removing code from the shared inference compiler can benefit builds of both
products even if the embedded final linker already discarded that code.
Neither a smaller archive nor a test result establishes a whole-product
compile-time or peak-memory ratio.

## Rejected: outlining merge validation in mutation commit

The storage archive's largest function, `batchInternalPreparedOwned`, had an
estimated 1,036,420 bytes of text. An experiment moved cold merge page admission
and artifact validation into two concrete, non-inlined helpers. It preserved
the apply lock in the caller and returned an explicit completion result for
fenced or replayed pages.

The experiment reduced the function to 1,005,960 bytes, aggregate attributed
storage text from 44,764,208 to 44,743,520 bytes, and the storage archive from
64,326,672 to 64,302,368 bytes. The archive saving was only 24,304 bytes (0.038%).
The sampled storage compiler peak was 8,192,294,912 bytes; the preceding native
qualification had sampled 4,864,262,144 bytes. These were not matched cold
runner measurements, so the difference does not establish a general memory
regression. It also provides no evidence for accepting the split as a memory
improvement. The original storage code was restored, and the rebuilt archive
matches the baseline size and SHA256 exactly.

The restored original subsequently sampled 11,510,890,496 bytes on this busy
host. That counterexample reinforces the limit: these macOS RSS observations
cannot rank the variants. The split remains a rejected, unqualified candidate,
not evidence that all phase outlining increases memory.

The diagnostic run was stopped during its superseded inference compilation;
it is evidence for the emitted storage experiment, not a successful product
qualification. Its patch, symbols, traces and logs remain in the ignored
`.benchmark-results/compile-costs` directory.

## Production qualification

Profile: Zig 0.17.0, native Apple Silicon, macOS 27.0.1 / SDK 27.0,
baseline CPU, ReleaseFast, stripped, Metal/CUDA/system BLAS disabled. Both
products keep embedded inference and the existing runtime composition.

| Artifact | Before, bytes | After, bytes | Reduction |
| --- | ---: | ---: | ---: |
| `antfly` | 232,058,632 | 229,621,752 | 2,436,880 (1.05%) |
| `libantfly.dylib` | 80,489,232 | 80,026,400 | 462,832 (0.58%) |
| Physical inference archive | 50,925,464 | 47,953,232 | 2,972,232 (5.84%) |
| Inference attributed text, estimated | 33,640,776 | 31,901,492 | 1,739,284 (5.17%) |

These reductions compare with the immediately preceding work-avoidance build.
Against the [pre-query-owner executable](query-owner-redesign-2026-11-01.md)
of 225,639,896 bytes with the same flags, the current server remains 3,981,856
bytes larger (1.76%). This increment recovers part of that split's size cost;
it does not establish a cumulative executable size reduction across the redesign.

The new audit rejects the preceding archive with 518 CUDA-compute text
symbols and accepts the candidate with zero. Across the entire `ops.cuda`
namespace, estimated text falls from 1,324,972 to 7,184 bytes; the remaining
symbols are portable admission/planning, artifact hashing and video inspection,
not the concrete CUDA compute implementation. All ten production owner
archives pass emitted-code audits, including the existing foreign-dispatcher
exclusions. Serving, the command dispatcher and the EmbeddingGemma2 training
CLI entry remain linked; admission was not removed to obtain the size reduction.

Validation:

- Native product build: 133/133 steps, including 57 compiled storage owner
  regressions, three opaque query regressions, C API export and consumer linkage
  checks (134 public exports), linked inference ABI integration, and CLI smoke.
- Focused CPU fallback/packing/training run: 36 passed, 28 hardware cases
  skipped, plus 37 ancillary inference checks. This includes cache compaction,
  cancellation, borrowed tensors, prepared media masks, zero padding and
  training owner teardown. Compile-time filters kept this caller focused.
- Public HTTP query and deadline regressions: 10 passed in 20.72 seconds.
- Ownership audit script tests: 32 passed. Syntax, formatting, workflow lint,
  licensing and whitespace checks passed.

The final trace sampled 8,137,424,896 bytes for the inference compiler. These
are native CPU observations, not GPU/Linux qualification. The preceding
inference compiler was stopped before completion, and the host had other
native builds and concurrent test compilation, so there is no matched cold
before/after compile-time or peak-memory ratio. No whole-product 10x build
improvement is demonstrated. GPU execution and a pinned-model training dry run
were not performed here.

Detailed receipts are the ignored `production-cuda-retention.*`,
`production-archive-work.json`, `production-retention-artifacts.json`,
`production-retention-qualification.json`, `production-backend-fallback-tests.log`
and `production-retention-http-tests.log` under
`.benchmark-results/compile-costs`.
