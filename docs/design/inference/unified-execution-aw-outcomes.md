# AW: Prepared-slot Metal ternary FFN

Baseline: `52e76654fe`. Implementation: `e7ade0f736`.

TL1/TL2 gated FFNs now use the same prepared linear slots and native parameter
owner as ternary attention. One slot-backed C/Zig bridge replaces two raw-weight
bridges, removing **11,426 maintained production bytes / 210 lines**: 6,006 bytes
of Metal host bindings and 5,420 bytes of Zig contracts. Inline tests, test tools
and documentation are counted separately. CPU, CUDA, generated source, Metal
shader source and device images are unchanged.

Warm calls reuse the existing gate/up/down buffers and metadata. They no longer
parse three raw codec views or wrap three raw weight buffers per FFN. The shared
validator handles whole-matrix packing and rejects missing, malformed,
shape-incompatible or mixed-codec slots before acquiring resources. Native
parameter bytes, weight offsets, pipeline identities, dispatch geometry, profile
labels and the original six/seven-encoder schedule remain intact. Both codecs
share one direct slot bridge; codec selection reads the admitted slot tag.
There is no runtime descriptor map or shader interpreter. The old internal
raw-weight C/Zig APIs are retired together.

The prepared-slot cache can introduce three private uploads if the model has
not already prepared those slots. For the tested 1536/4096 FFN this retains
about 4.72 MB for TL1 or 3.94 MB for TL2, in addition to the model's source
storage. Existing slot lifetime and teardown own the buffers. Warm reuse is the
benefit; startup and peak-memory improvement are not claimed.

The direct host specialization only implements one row and no post-down RMS.
The old Zig adapter silently sent multi-row requests to the single-row C ABI
and ignored post-down RMS. It now declines both requests so the enclosing
fallback can implement the requested operation. Regression checks require the
output to remain untouched on each decline.

The VM-aware const-input admission introduced by AV now also protects FFNs.
Heap inputs retain the existing no-copy path where supported; read-only or
unqualified VM regions use an owned copy. No caller-address ownership cache is
kept across mapping lifetimes. Warm slot reuse and these admission checks are
included in the performance measurements.

## Correctness and source qualification

The frozen-original device fixture passes **32 profiles**, covering TL1/TL2,
1536/1536 and 1536/4096 FFNs, GELU/SiLU, optional post-gate RMS, aliased and
forced-copy outputs. It checks an independent arithmetic reference, bitwise
original output, real command traces, output canaries and read-only operands.
Allocation events are excluded from differential command traces because the
three weight wrappers are intentionally removed. Native command and parameter
records must still match exactly.

**204 candidate failure checks** pass: every remaining owned allocation, each
encoder, command creation/completion, each unprepared projection slot and an
invalid format. Explicit expected statuses replace the old allocation-by-index
comparison because preparation removes three allocations. This is not a claim
that old and new invalid-slot status precedence is identical.

The real Zig-to-C tests pass both codecs, first preparation, cached reuse, heap
and constant-backed inputs, fallback declines, and the migrated TL1 ABI test.
The focused executable reports **40 passed, zero skipped**, including module
reachability entries. The permanent serialized Metal host suite also passes
256 K-block FFN profiles / 176 failures and 128 ternary attention profiles /
240 failures. Combined CPU/Metal/CUDA compilation, formatting and generated
freshness (70 files + 129 native regions) pass.

Source proof checks unchanged shaders and generated regions, the new C/Zig
parameter names/order, and byte-identical reproduction of the frozen header.
Frozen original functions are test-only. Initial fixture failures are retained:
a label-renaming error in the frozen header, a compile-time trace-label error,
and an anonymous request type that could not be mutated by the regression test.

## Performance qualification

The GPU assay passes **all 24 profile/order checks**, including AA controls. Candidate median ratios range **0.999070–0.999904**; maximum candidate p95 is **1.002595** and maximum trial median **1.000228**.

The unwrapped host wall-time assay passes **all 12 profile/order checks**, including AA controls. Candidate median ratios range **0.986075–0.992794**; maximum candidate p95 is **1.001652** and maximum trial median **0.994068**.

The experiment is declared before samples: a shared macOS host permits
concurrent CPU work and makes no quiet-host claim. Profiles use hidden=1536,
intermediate=4096, GELU, both codecs and optional post-gate RMS; GPU duration also
covers aliased and forced-copy outputs. Host wall time uses the unwrapped queue
with normal output admission. Each AA/AB/BA order measures 64 adjacent pairs,
four calls per arm, and four consecutive 16-pair trial medians. Preset limits
are 1.05 for paired median/trial medians and 1.10 for paired p95. No failed timing
experiment is retried. These are component measurements, not model-throughput
claims. The separate AV GPU p95 miss remains open.

## Reproduction and evidence

`prepare_metal_ternary_ffn_qualification.py` retrieves the unmodified original
owner and both wrappers, renames test symbols, preserves original profile labels
and emits source hashes. Its module docstring gives the standalone build and
separate timing commands. The default baseline reproduces the qualified header
byte for byte. No numeric or ABI correction is applied to that baseline.

Evidence is retained in `.benchmark-assets/1166/aw-campaign/`: original sources,
function snapshots, source accounting/proof, baseline hashes, qualified binary,
raw timing samples and summaries, and all build/test logs. The permanent checker
uses an independent oracle without the frozen header; original differential
checks and timing require the generated header.
