# Model-owned Metal constants: accepted qualification

The final source passes all seven production checks, 66 actual Metal device
tests, and 54 actual Linux ARM64 portable model tests (eight Metal skips).
[Qualification receipt](qualification.json) binds reports, logs and artifact
hashes; [production provenance](staged-production-provenance.json) pins all
changed executable sources and the production, streamed-test and Linux binaries.
[Commands](production-commands.json) reproduce the five comparison runs with the
pinned local checkpoint. The configured TS logs record the two streamed gates.

| Final ReleaseFast Metal comparison | Vectors | Query rankings/top-k | Maximum coordinate error |
|---|---:|---:|---:|
| AV1 scalable layers, original 4096 MiB | 12 | 2 | 3.9023e-7 |
| AV1 formats, original 4096 MiB | 11 | 2 | 3.9023e-7 |
| 52-second clip and queries | 5 | 4 | 8.7964e-7 |
| Five WebM videos and queries | 8 | 3 | 6.2213e-7 |
| Twelve depth/chroma videos and queries | 14 | 2 | 3.7626e-7 |

Every comparison passes its complete numerical, score/ranking, cancellation,
retry, half-close, concurrent-load, completion-accounting and cleanup gate.
Full-clip load runs four requests at concurrency two; both constrained AV1
suites retain their original 4096 MiB budget and 16-retry limit. Streamed TS
on Metal and native CPU preserves all six buffered window vectors exactly,
including retained-output retry and verification of 12 acknowledged AUs.
No injected library or disabling precision override is used in accepted runs.

## Implementation

The Metal provider owns checkpoint-named immutable vision/projector F32 slots
across cohorts and requests. Language and vision each have a 640 MiB ceiling,
share the same model/process controller and combined 512-entry cap. The pinned
checkpoint contributes 114 eligible matrices: 608,556,032 payload bytes including
zero biases, of which 607,911,936 bytes are unique owned weight buffers. Position
tables, norms, audio and request tensors are excluded. CPU cache policy is unchanged.

Relevant admission pressure first reclaims idle vision slots. If the unchanged
request is still denied, optional Metal language slots yield next. Each class is
attempted once; nonblocking provider locks never span admission. Active/submitted
frames, checkpoint aliases and shared weights cannot be discarded. Canonical
zero biases remain intact. Cancellation, forced/permanent/unrelated denial,
request reservations, process limits and external retry policy remain unchanged.
Native teardown drains its autorelease pool; only exclusively owned weight
contents are marked empty before slot destruction and exact credit release.

Unified-memory BF16 preparation widens directly into the final shared Metal
buffer with the existing nonallocating Zig SIMD conversion. Publication remains
transactional; private/non-unified or failed direct setup retains the old
fallback. Direct writes still count as initialized bytes. Owned HF vision frames
also retire MPS activation views after their existing wait, preserving weights
and multiplication plans. Externally owned frames keep their completion boundary.
An independent CPU oracle checks actual MPS dispatch, per-layer view retirement
and outer-frame validity. BF16/F32 snapshot checks include source mutation,
cancellation, invalid input and rejected replacement.

## Measured resource effects

First long-clip constructor initialization falls from 9,758,012,520 to
698,315,880 bytes. Warm initialization falls from 9,695,085,672 to 27,477,096
bytes per clip (99.72% less). The four-request warm load has zero net live owned
bytes/objects. Steady owned buffer allocation increases from 632,222,724 to
1,240,134,660 bytes; final long-clip owned peak is 1,570,847,748 bytes.
These are logical initialization and runtime allocated-buffer observations,
not bus traffic, physical residency or whole-pipeline memory. Preparation
textures/buffers, decoder surfaces and opaque driver storage remain outside
these gauges. Shared-host timings establish neither speedup nor capacity.

The isolated activation-view comparison reduces first-video tracked owned peak
from 3,250,624,996 to 1,429,726,980 bytes (56.0%), private peak by 86.8%, and
preserves the packed F32 vector byte for byte. Both historical whole suites
still failed; [comparison scope](views-resource-comparison.json) retains that
fact. Ordered two-class reclamation is the final additional acceptance change.

## Linux and retained controls

The actual Linux pretrained WebM run passes eight vectors, three rankings and
all lifecycle/load gates (maximum error 7.8232e-7). Its exact earlier binary and
source epoch are in `discard-production-linux-provenance.json`; later Metal-only
arithmetic changes do not execute on Linux. Final portable admission behavior
is covered by the fresh 54-pass Linux suite. `linux-linkage.json` verifies that
the executed pretrained AArch64 ELF has no interpreter or DT_NEEDED libraries.
`linux-library-source-scope.json` compares the separate 252-pass decoder receipt:
255 of 256 sources match; the sole change exposes unchanged test fixture paths
to license inspection, with its complete test-only diff retained.

Historical prototype, vision-only, discard-only, pool-only, direct-widening and
activation-view reports remain failed where their overall gates failed. Their
patches and provenance distinguish them from the accepted final source. Debug
success does not substitute for production success. The standalone resource
probe is diagnostic only. `diagnostic-loader-audit.json` records that system
Python stripped shell-prefixed DYLD injection from early controls. Only later
inside-Python injection has confirmed activation. The active malloc sampler
released zero pages and did not resolve admission; the process-stat interposer
crashed before model loading and is invalid production evidence. No allocator
relief hook ships. Stripped stack export proximity does not identify exact callees.

Qualification covers the pinned corpora, checkpoint and recorded ARM64 hosts.
It does not establish arbitrary codec/origin conformance, x86 hardware performance
or end-user retrieval relevance. The requirement audit is in
[the non-CUDA audit](../../../../../docs/plans/native-video-noncuda-audit.md).

The latest main (`5fdb3ea392`) merges cleanly in `baea270013`. All nine bound
build/runtime sources remain identical; the fresh post-merge Metal/model gate
passes 66/66, and the Apache boundary gate passes 35 tests and scans 2,235
sources. [Merge scope](main-merge-scope.json) records the exact changed files
and raw checks. Production reports retain their earlier measured binary epoch.
