# AV: Prepared-slot Metal ternary attention

Baseline: `02e4f2709b`. Implementation: `4b8876d248`. TL1 and TL2 attention now share a native slot-backed
projection owner and two host entry points, replacing six raw-weight host
functions and four repeated Zig bridge declarations/call paths.

Net maintained production reduction is **14,463 bytes / 292 lines**:
9,824 bytes in Metal host bindings and 4,639 in Zig contracts. Test fixtures,
qualification tooling and documentation are counted separately. Metal shader,
generated, CPU and CUDA production source and device images are unchanged.
This reduces host execution code; it does not shrink shader arithmetic.

The new bridge consumes the existing prepared linear slot. Immutable weights
and native codec metadata are uploaded/prepared once, then reused. Warm Zig
calls use the prepared-kind cache instead of reparsing raw codec metadata.
TL1/TL2 retain their original native parameter layouts, pipelines, dispatch
geometry, encoder boundaries, RMS ordering, scratch allocation policy and
profile labels. The internal C/Zig host bridge changes together; this is not
an unchanged-ABI claim.

Ternary matrices need a whole-matrix packed-layout validator. The generic
fixed-block slot validator cannot describe them: their placeholder block
sizes do not represent the stored compressed matrix. The new admission check
validates format, preparation, shape, tile geometry, packed lengths and scale
bounds before acquiring host buffers. Span admission also checks that query
head geometry matches the projected input width.

Preparation can introduce a private weight upload when the slot was not
already prepared by another native route. The tested matrices retain about
0.49–1.57 MB each. One-time setup durations are recorded separately; neither
startup nor peak memory improvement is claimed. Existing slot ownership and
teardown release these buffers.

## Correctness and failure qualification

The original direct TL1/TL2 attention wrappers returned success without copying
the output back when Metal declined a no-copy buffer. Four cases reproduce
this defect against the frozen original. The replacement copies successful
results back after command completion. Failed commands leave copied outputs
untouched. For fair differential output and timing comparison, the original
comparison functions receive only that successful-output copyback correction;
the unfixed originals remain in the test fixture to prove the defect.

The frozen-original comparison passes **128 real-device cases** covering both
codecs, input widths 1536/4096, output width 1536, direct and compressed-span
attention, all four pre/post RMS combinations, zero/nonzero epsilon, aliased
and forced-copy outputs. Each case checks an independent arithmetic reference,
bitwise original output, real Metal command trace, output canaries, read-only
operands and private weight readback. Traces exclude resource allocation events
because preparation deliberately replaces per-call raw weight wrapping.

**240 failure cases** pass: every direct-path owned allocation, each encoder,
command creation/completion, and malformed slot format, offset, tile geometry
or packed length. This is not exhaustive failure injection into the compressed
attention implementation, which is unchanged.

The real Zig-to-C regression passes both routes for TL1/TL2 with heap and
constant-backed inputs, exercising first preparation and cached reuse. The
executable reports **39 passed, zero skipped**, including module reachability entries. Native declaration parameter
names/order agree at both new bridges. The serialized permanent
`test-metal-host-ffn` target passes the new 128/240 suite plus the existing
32/212 ternary FFN and 256/176 K-block FFN suites. Combined CPU/Metal/CUDA
compilation, Zig formatting and generated freshness (70 files + 129 native
regions) pass.

## Performance qualification

The final GPU-duration assay passes **95 of 96 profile/order checks**, including
all AA controls. Candidate median ratios range **0.994696–1.010527** and all trial
medians pass. One TL2 compressed-span, 1536-input, no-RMS, forced-copy BA check
has paired p95 **1.100617**, above the preset 1.10 cap; its median is **0.994696**
and maximum trial median **1.021141**. This tail qualification remains
**incomplete**. The passing unguarded experiment does not substitute for it.
The failed measurement is retained and is not retried.

The final unwrapped host wall-time assay passes **all 48 profile/order checks**,
including AA controls. Candidate paired medians range **0.992156–1.000278**;
maximum paired p95 is **1.061514** and maximum trial median **1.003417**.
The VM admission checks are included in these measurements.

The declared experiment permits concurrent CPU work on this shared macOS host;
it makes no quiet-host claim. It measures 64 adjacent pairs in AA/AB/BA order,
four calls per arm and four consecutive 16-pair trial medians. Limits are 1.05
for paired median/trial medians and 1.10 for paired p95. GPU duration uses the
actual completed command buffer; host wall time uses the unwrapped queue and
includes allocation, encoding, submission and wait. Both paths retain the
original GPU command schedule. No failed timing measurement is retried.
These are component measurements, not model-throughput claims.

## Reproduction and evidence

`prepare_metal_ternary_attention_qualification.py` retrieves the six original
functions and produces a comparison header plus source hashes. Its default
baseline reproduces the qualified header byte for byte. Compile
`check_metal_ternary_attention.m` with the generated header as documented in
the preparer's module docstring. The checker without that header is part of
the permanent build target and checks the candidate against the independent
oracle; frozen-original differential checks and timing require the header.

Evidence is retained in `.benchmark-assets/1166/av-campaign/`: original source,
function snapshots, source accounting/proof, source/binary pins, qualification
binary, raw paired samples, summaries and build/test logs. Early fixture compile
errors and the constant-backed Zig fixture crash are retained, as is the
heap-backed integration fixture's passing run. A separate C probe reproduces
the same constant-backed span crash against the
unchanged original and the initial candidate. Metal can accept a no-copy view
of read-only host pages and a subsequent CPU read faults. The final owner queries
the current VM region before wrapping const host inputs: writable regions keep
the original no-copy path, while read-only or unqualified regions use an owned
copy. No address-range cache is kept across caller-owned mapping lifetimes.
The corrected standalone probe and the restored constant-backed Zig regression
pass. This protection is scoped to the two new ternary attention host bridges;
other legacy host bridges still need their own ownership audit.

The first, unguarded implementation passed both timing assays. The final
VM-admission implementation receives a new declared experiment and new source
pins in `.benchmark-assets/1166/av-guard-campaign/`; the original samples are
retained. This is an implementation change, not a failed-measurement retry.
