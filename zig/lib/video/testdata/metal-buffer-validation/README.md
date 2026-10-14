# Runtime Metal buffer allocation and lifetime qualification

All 712 maintained `newBufferWithLength`, `newBufferWithBytes` and
`newBufferWithBytesNoCopy` expressions in the inference bridge now pass through
the allocation observer. A token associated with each Metal object records its
last release, including resources retained by views/commands. The helper consumes
and returns retained ARC ownership, preserving the original new-buffer lifetime
without depending on a caller autorelease pool. Duplicate observation is ignored;
failed creation adds nothing.

Owned resources record `MTLResource.allocatedSize`, split into private and
host-visible live bytes/peaks with an owned total and live/peak object counts.
No-copy resources instead record logical aliased length/counts; mapped views can
overlap and are never added to owned bytes. Allocation/release bytes and object
counters are monotone. Successful initialized-buffer creation records logical
initialization bytes/calls separately from explicit upload/download/copy APIs.

The device oracle checks exact allocated/released bytes and counts for real
Shared/Private resources, an independently retained view, a page-aligned no-copy
mapping and 32 concurrent allocation/destruction operations. Live gauges return
to their starting values after the last references disappear. The complete
video model/device suite also checks the existing resident pipeline and transfers.
Portable tests verify disabled-platform zeros and unsigned gauge rendering;
the Python contracts distinguish decreasing live gauges from counter/peak resets.

The full-request measurement uses the maintained 52-second Sintel clip and
pinned pretrained PyTorch CPU reference, including rankings, reset/retry,
half-close, concurrency-two completion accounting and released admission. All
ordinary numerical/load gates remain mandatory. `qualification.json` records
terminal results, commands and source/binary/log hashes.

These are worker-process allocated-buffer observations, not physical residency
or a per-request memory peak. Peaks are lifetime high watermarks, live deltas are
signed net changes, and independent atomic scrapes may straddle operations.
Preparation buffers/textures, VideoToolbox surfaces, driver/pipeline allocations,
the optional MTP-head extension and other libraries remain outside these gauges. GPU timing
retains its previous runtime-factory scope. Initializer/copy byte counts are
logical operations rather than measured bus traffic. The existing historical
telemetry receipt remains bound to its earlier implementation.

## Full-F32 vision regression

The first full-clip run completes the lifecycle/load gates but fails the unchanged
reference vector/score gate. It records maximum video coordinate error 0.00022155.
With exactly the same binary and only `TERMITE_METAL_DISABLE_GEMMA4_VISION_FLASH=1`,
the full gate passes (maximum video error 1.1791e-6). Both reports and their exact
measurement-tool snapshot are retained. They isolate the newly merged Gemma4
vision Flash route, which rounds Q/K/V and softmax probabilities to half.

EmbeddingGemma 2 now selects a separate full-F32 tiled vision operation. The
32-query kernel uses float inputs and probabilities and reuses Q scratch for
the final output tail, requiring 25.25 KiB of threadgroup storage. A dedicated
dispatch counter distinguishes it from a generic fallback. The device test
compares every output against generic F32 and selected outputs against independent
F64 attention for two cohorts, both heads and first/middle/final query rows.
Neither vector nor ranking tolerances change. Final production measurements are
separate from the initial/control binary and retain their own hashes.

The production F32 run passes without a disabling environment override. Maximum
video coordinate error is 8.7964e-7 (cosine 0.9999999999787), with 256 dedicated
F32 tiled dispatches. All four query score errors are below 1.34e-7. The one-video
corpus has trivial rankings; multi-video WebM/expanded-format checks are separate.
All four sustained requests at concurrency two return exact repeat vectors,
with zero capacity denials, matched completion accounting, released admission
and zero net live-buffer bytes/counts across the warmed load.

The full clip initializes 9,758,012,520 logical buffer bytes in 1,869 constructor
calls. The process lifetime owned-buffer high watermark is 3,183,421,188 bytes;
this excludes the other pipeline storage listed above. The 20.31-second first
clip and 73.87-second four-request load were measured on a shared host and are
diagnostic observations. They do not establish a speedup over the initial or
control runs, which had different system load and attention policies.
