# BI: Tile prototype integration with shared native programs

Inputs: implementation `99de25b5f9`, origin/main `37a99e8ba8`, and
[PR #1404](https://github.com/antflydb/antfly/pull/1404) at `46084e480b`.
Main was merged as `7230ec4a8a`; that commit is the source-accounting baseline.

## Ownership and design

The portable Tile IR, CPU vector lowering, scalar references, Metal/CUDA/OpenCL
emitters and prototype Whisper CLI are integrated. Production native programs
retain their existing arithmetic, precision, layouts, synchronization, launch
ABIs and tuned schedules. The portable model shaders remain prototypes; this
integration does not enable full-model GPU execution.

The rebased registry has 175 disjoint native regions covering 477 instances.
Existing quant-generator regions retain their canonical owners. Superseded PR
renderers are removed instead of maintaining another copy of the same program.
Thirty exact source templates cover 122 instances; their expansion introduces no
device call or runtime policy switch. New owner names describe operations and
schedules. Historical exported model names remain compatibility adapters.

Both generators check freshness, and Tile checks unique region boundaries and
rejects overlapping generator ownership. Its checked source-input manifest
enters both enabled backend qualification identities; Metal also includes the
optional logits header. Cache tests verify independent bundle hashes and source
edit invalidation. A deliberate unlisted-source probe is rejected by freshness
checking. The existing pipeline catalog, typed fields and cache lifecycle remain
canonical.

CUDA folding preserves opaque ABI lists and macro-named native instantiations.
Its argument parser ignores comments, including commas within them. Regression
tests cover all three cases and require exact expansion back to the original.
The remaining concrete generated variants share force-inlined bodies.

## Validation

Local Metal:

- The complete default shader is byte-identical to the frozen post-main shader.
- Segmented attention: 118 correctness cases and 10 resource checks.
- Matrix phases: 52 correctness cases, 34 resource checks and 12 timing gates;
  every gate passes.
- The actual optional catalog logits hook: 16 independent-reference cases and
  six AA/AB/BA timing gates, all passing. The two AB and two BA median ratios
  are approximately 0.70, about 30% faster on this machine. Ties, suppression,
  grammar overrides, chunk tails, full vocabulary and all-negative-infinity
  inputs are covered, with output canaries. The existing all-negative-infinity
  LSE behavior differs; the prototype reducer skips negative infinity. The
  existing default pipeline remains selected unless the flag is enabled.
- All prototype Metal model kernels compile in default and safe math modes.
- Three latest-main host-output copyback regressions pass through the current
  APIs. TL1/TL2 fixtures use their specialized packed-slot preparation metadata.

Glowworm, NVIDIA GB10, pinned NVCC 13.2.86:

- Original and candidate SM121 images are byte-identical, SHA-256
  `51f92612e1fc4095e8b9f1d33a6550f9ec41c2d91651691b6da91042e3cecec8`.
- All 453 SM121 entrypoints and all 3,171 functions across the seven shipping
  architectures have identical native instructions and resource records.
- Portable PTX and the fatbin are regenerated. The standalone SM89 image is
  byte-identical to the pre-integration image. Prototype logits and full-model
  CUDA sources compile for SM121; full-model GPU execution is not claimed.
- Independent hardware correctness passes: 89 row cases, 21 M64 WMMA cases and
  90 prefix/pooling cases.
- Hardware timings are retained, including failures. All median ratios are
  below 1.023. Row tail gates fail 10/30 AA, 11/30 AB and 13/30 BA; WMMA fails
  1/6 AA and zero AB/BA; pooling fails 1/2 AA, 2/2 AB and zero BA. These assays
  do not establish tail timing parity. The failing AA controls and identical
  SM121 images demonstrate why they must not be presented as a clean timing
  pass. No competing compute process was observed at the final inspection.

Host checks pass: Tile lowering tests, both generator freshness checks, the
prototype CPU Whisper CLI build, 112 selected compiler/proof/folding tests,
12 source-accounting tests, five native-comparator tests, enabled-backend cache
identity tests, and 11 model-explorer tests plus generated-data freshness.
A full model/transcript or whole-inference-suite qualification was not run.

A stale pre-existing benchmark-image assertion was repaired separately from
performance measurements. The historical proof image from `adc9e4d78d` hashes to
`0cc41e...850d`; the current SM89 image hashes to `0f5eb5...217e`. Direct native
comparison confirms that both benchmark-qualified Q4 programs have identical
instructions and resources. A checked receipt binds those exact programs to the
current image while retaining the historical proof and timings unchanged.

## Source budget

Against `7230ec4a8a`, this integration **adds 234,622 authored production bytes**
and 5,680 authored lines after charging the Tile IR, renderers, native instances,
metadata and replacement owners. Markdown, tests, generated text and images are
separate budgets. Moving native source into generated regions earns no savings.
This batch is an integration/authoring capability, not a net kernel reduction.

The Tile Zig tree is 512,754 physical bytes versus 667,543 in PR #1404: 154,789
fewer bytes, or 23.2%, by rebasing on the already-shared native owners. That is a
comparison of the proposed Tile implementation only, not a 23.2% reduction of
inference kernels or of total production source.

The portable IR now provides a place for new genuinely shared algorithms. Future
production migrations still require independent correctness, native resource and
hardware qualification before replacing a tuned native schedule.
