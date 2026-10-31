# BR: Exact native numerical phases

Repeated projection indexing, attention scores and reduction/publication phases
still had independent copies across native families. This batch shares 20 more
phases and extends the existing QKV-plane phase to another family. The 44
replacement sites use compile-time source expansion; they add no device helper,
runtime branch, temporary allocation or synchronization.

CUDA shares pair/QKV tile ownership, Q4/Q8 paired accumulation, Q4K biased
accumulation, visible key-window selection, cross-attention scores, tensor
attention geometry and the gate/up publication phase. Metal shares self,
relative and causal attention score phases, segmented tensor geometry and
publication, row geometry/maximum reduction, Q4K pair reduction, suppressed
argmax candidates/partial merging and BF16 gate/up tile/scalar staging.

The Metal canonical phases are readable MSL. Raw MSL owners expand them before
encoding; existing Objective-C owners use the same phases encoded at their
source boundary. ABI macro splices remain in their owners. The newline policy
is explicit because the existing boundary encoder consumes terminated lines.

The source-template scanner now recognizes identifier tags while preserving
native aggregate initializers. An existing `float sumf[2][2] = {{0.0f, ...}}`
initializer exposed the old assumption that kernel text never contains `{{`.
The regression covers numeric and identifier aggregates, nested positive and
negative sections, and a source brace adjacent to a tag. Existing specialization
templates retain their own instance contexts.

Forward/backward relative-attention denominator phases were inspected and left
separate: their invalid-key handling differs. Similarly, tuned attention tiles
retain their different scratch layouts, reduction schedules and probability
publication boundaries. Similar-looking source is not sufficient evidence that
these programs should be interchangeable.

## Preservation and qualification

All three complete affected native compiler inputs match the frozen
`f354f6a8600dd33c744114113bc526109fcff5b6` originals byte for byte:

| Compiler input | Bytes | SHA-256 |
| --- | ---: | --- |
| CUDA inference bundle | 681,503 | `82cf13bca38e6ac4214f6511a23a792ff9f759a674a0a0e7383f8dd7c2eb93c6` |
| Metal Objective-C owner | 4,565,088 | `36f9d1a65ce6ad6425e7d370565f3f8e7da1577063b156dabd151d9ae7728448` |
| CUDA encoder programs | 68,149 | `8055f4c6fef27561b1517f634a750b6af4f672950c6caa463af8480624a7cdaa` |

This establishes preservation of the compiler inputs, including the tuned
schedules, explicit rounding, arithmetic association and barriers. CPU programs
and checked-in device images are unchanged. This batch claims no binary-size
reduction, throughput improvement or new whole-model timing result. The prior
CPU/Metal/Glowworm performance qualification remains separately reported in BP.

Both generator freshness checks and all 18 Tile tests pass, including the
new template regression and existing generator ownership checks. Canonical
license and Apache boundary checks pass. Independent enabled-backend cache
identity qualification also checks that the added phase owner participates in
source identity. Receipts, before/after hashes, source accounting and initial
failed generator attempts are retained in
`.benchmark-assets/1166/br-campaign`.

## Source and dead-code accounting

Against `f354f6a860`, the ledger reports **39 authored production lines / 6,868
bytes removed**, charging every replacement owner, the boundary adapter and
parser changes. The new regression adds 15 lines / 697 bytes separately. All
production generated regions remain unchanged. Moving source between owners or
replacing escaped MSL with readable MSL earns no independent reduction credit;
the ledger measures the complete replacement footprint.

The dead-code audit also checked the two unreferenced Q4 HMMA prototype files.
Both contain standalone correctness/benchmark executables, host entrypoints,
independent references and documented manual invocation. They remain useful
qualification tools and are retained. No additional proven-dead production
program was found in this batch; earlier verified deletions remain recorded in
BP. The audit does not equate absence of imports with dead code.
