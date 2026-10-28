# Large-win audit after AP

Audited head: `25c557cc70`. This is an audit and proposed implementation scope,
not another completed consolidation campaign. No production source changed.

## Recommendation

Prioritize whole lifecycle and execution-family owners. The next changes should
remove tens of kilobytes each and have explicit source and runtime acceptance
criteria before broad migration. Stop treating small shared arithmetic fragments
as the principal route toward a further 20% reduction.

| Priority | Change | Current maintained scope | Net-source budget / goal |
| --- | --- | ---: | --- |
| 1 | Metal pipeline catalog, followed separately by demand-loaded/shared pipeline families | 462 complete declaration/create/clear triplets, 114,932 bytes | Approximately 40–70 KB; prototype-backed size budget, pending implementation |
| 2 | One Metal decoder phase owner across existing direct/planned and dense/gated routes | 125,478 Zig + 164,114 Objective-C bytes | Prototype goal 35–60 KB; not yet demonstrated |
| 3 | One CPU quantized projection family with fixed output cardinality | 97,098 bytes across nine selected functions | Prototype goal 20–35 KB; not yet demonstrated |
| 4 | CUDA attention launch preparation from existing checked descriptors | 67,692 bytes across five launchers | Prototype goal 10–20 KB; not yet demonstrated |

These scopes are disjoint selections, but common plumbing costs must be charged
once when calculating any combined result. Exposed source is not removable
source. The latter three ranges are planning goals, not measured savings.
Most of the first two opportunities are host/runtime code. They must not be
reported as a 20% reduction of device arithmetic.

## 1. Metal pipeline lifecycle and startup

`metal_kernels.m` currently has 560 plain scalar pipeline fields. Its constructor
contains 497 simple pipeline-creation statements; 464 are unconditional at the
constructor's top level. The audit excludes 33 conditional statements and all
array, lazy or custom-loader cases from the initial migration.

462 entries have all three matching forms: scalar declaration, unconditional
creation using the ordinary or precise library, and destruction clear. Together
they occupy 114,932 bytes. A concrete catalog representation occupies 39,676
bytes. Allowing 3,000 bytes for shared machinery gives:

- **42,945 bytes removed** when field declarations remain in place, preserving
  the runtime structure's existing layout.
- **72,256 bytes removed** if declarations can also consume the catalog.

That is a size prototype, not a completed implementation. Creation/error order,
precise-library selection, required-symbol diagnostics, retirement ordering and
field-layout policy require explicit preservation; additional metadata costs
must be charged. The proposed 40–70 KB budget leaves these details visible.
Named pipeline fields can remain direct hot-path accesses: a runtime map or
interpreter is unnecessary.

A separate performance change should stop eagerly constructing every optional
pipeline family for every runtime. Prepare the model's required families before
execution/capture, then reuse immutable pipeline states with a key that includes
device, library identity, math policy and specialization constants. Preserve
required-symbol failures and precise-library provenance. Measure cold creation,
warm session creation, pipeline count and retained memory. No startup speedup
percentage has been measured in this audit.

Evidence locations: runtime fields around line 1219, creation around 30136,
destruction around 31382, and `termite_metal_make_pipeline` around 18813.

## 2. Metal whole-decoder execution phases

The selected Zig scope includes:

- `runGatedDecoderBlockF32KvDevice`
- `finishGatedDecoderBlockFromAttentionOutputMt`
- `runGatedDecoderBlockF32KvDeviceMt`
- `runGatedDecoderBlockOp` / `runDenseDecoderBlockOp`
- `runActiveGatedLayerFromAttentionInputDeviceMt`

The selected C scope includes planned attention/FFN/PLE, gated FFN residual,
quantized device-KV attention blocks and dense FFN/layer-norm orchestration.
It excludes the lifecycle functions counted in item 1.

There are already operation descriptors, planned-layer cursors and encoder
leaves. The long-term change should make direct and planned callers consume
those same phase owners, eliminating repeated validation, temporary/resource
ownership, projection setup and FFN completion. Do not add another plan language
or retain a new generic path alongside all old bespoke paths.

Keep dense versus quantized schedules, attention algorithms, precision boundaries
and KV formats as explicit policies. Share orchestration rather than merging
register tiles. Hold dispatch count, barriers, command-buffer boundaries,
resource leases, cancellation and frame ownership constant initially. Any later
removal of transitions needs actual frame/model measurements.

A successful prototype should delete at least 30 KB net before extending across
the full 289,592-byte scope. Validate encoder/dispatch counts, exact outputs,
read-only sources, memory admission and cancellation under multiple live frames.
The audit does not establish an end-to-end throughput improvement yet.

## 3. CPU projection family

The nine selected functions are `linearNoBiasQuantizedWithPolicy`,
`linearNoBiasQuantizedPair`, `linearQuantizedTriple`, `linearQ8_0Range`,
`linearQ8_0PreparedPanelRange`, `linearQ8_0Pair`, `linearQ8_0Triple`,
`q1_0PreparedActivationPairApply` and `q1_0PreparedActivationTripleApply`.
Their **97,098 maintained bytes exclude generated regions**.

The pair/triple top-level routes still repeat format admission, prepared-layout
selection and output-plane handling; Q8/Q1 pairs and triples repeat matching
traversals. Introduce a compile-time fixed-cardinality projection family using
the existing quantized format/activation owners. Keep public wrappers thin and
keep the existing format-specific and MR-specific kernels selected explicitly.

A fixed cardinality must not become a runtime loop or array of accumulators in
the dot microkernels. Earlier campaigns found real register/instruction
regressions from combining row/register schedules. Preserve SDOT/NEON leaves,
accumulation/store order, stack limits, cached layouts, optional biases and
parallel partitioning. Existing generated activation pipelines earn no new
source-reduction credit.

Require a net 20 KB prototype reduction, independent codec references and
AA/AB/BA measurements covering MR1/MR2/MR4/MR8, tails, native/non-SDOT and real
QKV/FFN consumers. Reusing input quantization is already implemented in several
routes and must not be advertised as a newly discovered performance win.

## 4. CUDA attention launch family

The selected launchers are `launchGqaAttentionF32`,
`launchGqaAttentionDecodeScalarsF32`, `launchGqaAttentionDecodeTurboquantF32`,
`launchGqaAttentionDecodeTurboquantScorePreworkF32` and
`launchGeneratedGqaAttentionDecodeF32` in `cuda/kernels.zig`.

Use the existing checked geometry, descriptors and native ABI owners to share
request preparation and schedule selection for the whole family. Retain wire
formats, physical KV addressing, graph-progress scalars, split-stage scratch,
score-prework variants and mutable live argument slots explicitly. Replacing
another handful of pointer arrays alone will not meet the size goal.

Require a 10 KB net prototype reduction, identical launch parameters/schedules,
malformed-geometry and partial-admission coverage, graph-capture/replay checks
and host-submission timing. Device images should remain unchanged for this
host consolidation.

## What the audit rules out

- Raw file size is not a kernel denominator. `native_compute.zig` contains
  468,673 generated bytes and 712,274 inline-test bytes. The principal CUDA
  artifact contains 216,480 generated bytes. Those do not represent untouched
  authored duplicate kernels.
- The scan initially exposed several apparently large CUDA row-tile clones;
  their bodies are already generated. Removing stored expansions again would
  overstate new maintained-source savings.
- The Metal literal scan finds 305 explicit unmanaged kernels and 741,947
  decoded standalone literal bytes. It excludes macro expansions and is not a
  complete compiled-shader size measure. Remaining exact matches are mostly
  smaller attention/reduction phases or different schedules.
- CUDA short/M64 encoder attention and several row/register-tile combinations
  still appear similar but have prior negative consolidation evidence. They
  should not be the first large-win prototypes without new architectural proof.
- A generic tensor/kernel DSL is not justified by this audit. It would add a
  substantial compiler and require requalifying all schedules; the catalog and
  phase owners above fit existing infrastructure.

## Evidence and scope limits

Fresh inventory, generated/test exclusions, function boundaries, exact-token
clone candidates, the concrete pipeline catalog prototype, selected-family
source ranges and source SHA256 pins are retained under
`.benchmark-assets/1166/large-win-audit/`. The token scan is discovery evidence;
matching windows overlap and are not byte savings. Private test/qualification
helpers outside inline tests can remain in broad file totals; selected family
budgets use manually reviewed production functions.

No GPU jobs, timing experiments or production changes were needed for this
source audit. Only item 1 currently has a concrete representation-size budget.
None of these opportunities establishes a further whole-kernel 20% reduction.
