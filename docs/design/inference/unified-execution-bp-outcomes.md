# BP: Native program ownership and dead-code audit

This batch extends the Tile generation path to the remaining direct native
programs in the central Metal and CUDA sources. It also removes unused host and
device helpers rather than carrying their old implementations into new owners.
The batch baseline is `e71ea0b6ab1c88b50e0beadcea709ff320b27b32`.

## Ownership and shared producers

The registry grows from 89 to 119 native families. Another 372 entrypoints gain
Tile ownership: 139 direct CUDA definitions, 166 direct Metal definitions and
67 Metal ABI-macro definitions. Fourteen compatibility shells remain with the
quant generator, which already owns their numerical implementations; the two
generators do not overlap.

Readable operation-family `.cu`, `.metal` and `.m` sources replace long encoded
Zig literals for the newly adopted programs. The compile-time reader checks
entry names and boundaries. The generator rejects new direct CUDA, Metal or
Metal ABI exports outside owned regions. Source manifests and the independent
cache-identity check include every new owner. There is no serving-time parsing
or device-side dispatch introduced by this representation.

Four CUDA phases now have single source definitions: self-attention scores,
masked cross-attention scores, QKV planes and suppressed Q6 selection. Their
rendered operation order and static schedules are unchanged.

The twelve recently integrated OCR standalone images share a typed catalog of
files, exports, producer, architecture, launch resources and precision policies.
Host launchers and producers consume that same catalog. The offline regenerator
pins NVCC/Triton, validates the complete export/resource set before publishing
any output, and checks every image for byte identity. Existing older standalone
families retain their existing producers; this catalog does not yet subsume
all standalone images in the repository.

## Dead-code removal

The audit removes 88 unused named functions, five private constants or namespaces, and an
unused device cache. It includes CUDA packing/training helpers, Metal selection and
quantization helpers, unused CUDA/Metal host wrappers, superseded prefill/decode
adapters, WebGPU wrappers, graph helpers, model adapter functions, GGUF shape
helpers and obsolete GPU-hosted stub namespaces. The removed definitions total
1,667 lines / 68,169 bytes before accounting for surrounding formatting and
regenerated copies. An obsolete helper-only CLIP packing round-trip test is
also retired; actual model parity tests remain.

Candidates were checked against repository references, generated references and
reflection sites. Helpers emitted by dynamic renderers remain live even when a
simple search of the generated monolith suggests otherwise. Qualification
producers and fixtures are retained when they still serve validation.

The native source ledger charges all replacement owners and metadata. This
batch is approximately 384 authored lines / 18.5 KB smaller, while generated
copies grow by 9,247 lines / 655,906 bytes. These figures are not a 20% source
reduction and do not count moves into generated regions as savings. The production-only ledger, with Zig tests separated, reports a net reduction
of 484 lines / 23,772 bytes. Tooling accounting is retained separately with
the receipts.

## Shared attention layout and CLIP simplification

Broader validation exposed a real CUDA correctness bug: SDPA treated CLIP's
head-major buffers as token-major. A shared checked layout classifier now
recognizes canonical 2D token-major and 3D/4D head-major buffers. CUDA uses its
existing head-major launch for those inputs, rejects mixed multi-head layouts,
and preserves tuned token-major routes. CPU and Metal use the same shape
classification contract. An independent F64 CUDA regression covers batches,
layouts, mask, bias and invalid layout combinations.

CLIP now uses the common token-major contract directly. This removes three QKV
packing transposes and one output unpacking transpose per layer, four private
conversion helpers and 135 lines from the adapter. No CLIP-specific kernel is
introduced. The change is verified by existing host-reference and physical
Metal/CUDA parity tests; no whole-CLIP throughput improvement is claimed here.

## Device identity and qualification

After excluding ownership markers and the proven dead definitions, the live
CUDA and Metal compiler inputs match the frozen originals. Glowworm regenerates
all twelve standalone PTX images byte-identically. Pinned canonical regeneration
also preserves the fatbin and SM89 cubin byte-for-byte, and portable PTX after
its canonical whitespace normalization. The fatbin remains 15,000,488 bytes;
source cleanup does not shrink code that the compiler already eliminated.

The broad physical CUDA suite selects 588 tests: 538 pass and 50 skip for
unsupported hardware or missing optional fixtures. The final token-major CLIP
and attention-layout suite passes 41 selected tests with no skips. The final
source build additionally exercises catalog-image users and passes 53 of 55
selected tests; Metal and the optional external GGUF fixture skip on Glowworm. Initial
failures are retained: six incomplete fixture/cwd failures, an unqualified
cuBLAS version, an undefined audio-capture test buffer and the genuine CLIP
layout bug. The corrected suite uses the already qualified cuBLAS 13.1.1; the
frozen reference was not relaxed to admit cuBLAS 13.4.1.

Six warmed CUDA model cases pass AA, AB and reversed BA with 16 balanced arms,
25 warmups, 301 measured iterations and unchanged 1.05 median / 1.10 p95 limits.
All 288 output processes are finite and byte-identical. AB median ratios span
0.98037–1.00360; BA spans 0.99884–1.00246. Maximum p95 ratios are 1.02787 and
1.03847 respectively; AA maxima are 1.00059 median / 1.02175 p95. The frozen
pre-batch benchmark and candidate hashes are recorded in the receipts. Subsequent
CLIP-only simplification and dead-function deletion do not change these MiniLM
and reranker execution paths. This is warmed model execution on GB10, not
whole-server or whole-OCR throughput. Physical L4/A100 validation remains deferred.

After the dead-code cleanup, the local CPU/Metal regression suite selects 147
tests: 145 pass and two skip (disabled CUDA and an optional external GGUF
fixture), with both Metal API and shader validation enabled. The Tile tests
(17), offline regeneration tests (3), generated-source freshness and engine-error
freshness checks pass. Enabled-backend cache-identity checks pass after the
layout change.

Receipts, frozen binaries, exact deleted definitions, negative ownership
controls, image regeneration reports, initial failures and source ledgers are
retained under `.benchmark-assets/1166/bp-campaign`.
