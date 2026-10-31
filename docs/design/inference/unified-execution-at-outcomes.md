# AT: Ternary Metal host FFN execution

Baseline: `d5da7c336c`. Implementation: `fff019fcc0`. TL1 and TL2 now share one always-inline host allocation
and encoder owner. Their public wrappers retain codec-specific parameter
structures and validation. The codec flag folds at each caller; pipeline reads
remain at their original use sites. No runtime format interpreter, new pipeline
lookup, shader change, or dispatch change is introduced.

The Metal host file loses **10,783 bytes / 95 lines**. The repository accounting
reports **10,782 net maintained production bytes / 94 lines** after separating
inline tests, test tooling and generated source. Shader, generated and CPU/CUDA
production source changes are zero. These numbers describe host execution code.

The audit also found two C/Zig declaration mismatches:

- TL1's C definition had an unused `size_t attention_input_size` parameter absent
  from its Zig declaration and callers. It shifted subsequent arguments.
- The I2_S slot FFN's C definition had an unused `float eps` parameter absent from
  its Zig declaration and callers. Its existing normalization already uses zero
  epsilon; removing the unused argument makes the declarations agree.

Both C-only arguments are removed. No Zig caller signature changes. An argument
count audit of 334 matching C definitions/Zig declarations finds no remaining
count mismatches; this is a count check, not a full ABI type proof.

The original TL1 signature is retained in the evidence. For differential
qualification only that unused argument is removed from the frozen original,
so the consolidation is compared against an explicitly ABI-corrected original.
TL2's original implementation is unchanged in the differential baseline.

Qualification:

- **32 real-device ternary cases** pass an independent arithmetic reference and
  preserve bitwise original outputs and real command traces. They cover TL1/TL2,
  1536×1536 and 1536×4096 FFNs, GELU/SiLU, optional post-gate normalization,
  aliased and forced-copy outputs, canaries, and read-only inputs/weights.
- Traces compare pipeline identities, buffer bindings/offsets, initialized
  parameter bytes, dispatch geometry, allocations, encoder boundaries and
  commit/wait order. The original six/seven-encoder schedules remain intact.
- **212 failure-path comparisons** cover every host-owned intermediate and
  parameter allocation decline, each encoder, command creation and completion.
  Original status and trace must both match.
- The existing **256 K-block cases / 176 failure comparisons** pass too.
- Both real Zig-to-C ABI regression tests pass, including one/three-row I2_S
  outputs and output bounds. The focused executable reports **40 passed,
  zero skipped**; 38 entries are module reachability tests.
- The permanent `test-metal-host-ffn` target, generated freshness (70 files +
  129 native regions), and CPU/Metal/CUDA compilation check pass.
- Source comparison proves no edits outside the three selected host functions
  and the new owner, ignoring blank-line separation. All shaders are unchanged.
- Optimized LLVM fully inlines the owner. Each codec retains 211 calls,
  85 Objective-C sends, two retains and 29 releases. This is a call/ownership
  comparison, not a claim of identical machine instructions.

The first test fixture incorrectly expected seven/eight encoders. The unchanged
original schedule has six/seven. That failed checker and log are retained;
correcting its explicit contract produces the passing results above.

The separately declared **shared-host GPU-duration assay passes all 24
profile/order checks**, including AA controls. Candidate median ratios are
**0.999793–1.000074**; maximum candidate p95 is **1.002081**. Each profile/order also passes
four trials of the existing 32-arm paired Metal estimator. The preset limits
are 1.05 for adjacent-pair median and trial medians, and 1.10 for p95.

The assay covers TL1/TL2 × optional post-gate normalization × aliased/forced-copy
output at hidden size 1536 and intermediate size 4096, with AA/AB/BA ordering,
64 adjacent pairs and four existing command-buffer GPU durations per arm.
There are no additional GPU commands. The host had ongoing CPU test/compile
load; this is explicitly shared-host evidence. It does not qualify quiet-host
performance or host wall-clock latency. The AS host p95 limitation remains open.

The original quiet-host experiment never collected timing samples because other
jobs remained active. Its controllers/logs and unrun receipt are retained. Before
any timing samples, a separate shared-host protocol was declared with the same
estimators, controls and limits. No failed timing experiment was retried. The
completed receipt is `shared-host/gpu-timing.json` under the evidence directory.

Evidence: `.benchmark-assets/1166/at-campaign/`. Source pins, the unmodified and
ABI-corrected originals, differential headers, checker binaries, logs, optimized
IR, source accounting and the timing protocol are retained there.
