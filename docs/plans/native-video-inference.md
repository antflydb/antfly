# Native video inference design

Native video input connects portable container parsing and maintained codec
reconstruction to Antfly's CPU/Metal inference runtime. Model adapters decide
sampling, geometry, tokens and normalization. Libraries expose explicit source,
clock, memory and cancellation contracts so the same machinery can serve offline
files, object ranges and live windows.

Current contracts are in [MEDIA.md](../../zig/lib/media/MEDIA.md) and
[VIDEO.md](../../zig/lib/video/VIDEO.md). The
[non-CUDA qualification summary](native-video-noncuda-audit.md) records acceptance,
reproduction, platform scope and access to historical evidence. This document
retains design decisions and the CUDA extension plan rather than a chronological
implementation log.

## Boundaries

```text
bytes / positional ranges / live transport
                   |
               lib/media
                   |
         packet clocks + leases
                   |
               lib/video
                   |
     native surfaces + preparation
                   |
       model-specific vision/tokens
                   |
       backbone + pooling/normalize
```

Media stays independent of model registries and device runtimes. Codec algorithms
are maintained Zig source; Apple hardware decoding is an explicit platform route.
Reference FFmpeg/PyTorch tools generate fixtures and run comparisons offline;
they are not runtime or codec-build dependencies. Optional object-store transport
uses existing maintained clients behind the range interface.

## Ownership and bounded work

Packet leases remain valid through decoder use; native surfaces remain valid
through preparation; completed model inputs retain their output buffers until
consumers finish. Each asynchronous owner fences or joins its work before
releasing source state. Cancellation propagates across reads, decode, preparation
and inference without dropping resources still referenced by GPU commands.

Shared admission covers source/index/cache, decoder, preparation and model
owners. Budgets account for actual retained allocations and construction-growth
overlap where implemented. Per-owner limits still bound pixels, packets,
references, pending pictures, output bytes and command depth. Cached optional
weights yield to unchanged admitted work under relevant pressure; caches do not
create extra process allowance. Rejected preflight work can retry under the
original deadline and retry policy.

## Sampling and numerical policy

Container packets are not necessarily pictures. Logical indexes assemble fields,
slices, partitions and separate planes, and preserve layered/superframe output
identity. Source presentation time remains distinct from raw clocks. Frame
selection supports pinned processor FPS behavior and explicit half-open timestamp
windows, with declared overflow and VFR tie policies.

EmbeddingGemma 2 uses the pinned video processor policy, bounded geometry and
ordered modality composition. Display/color policy is explicit and part of
embedding identity. Unsupported transforms/conflicting metadata fail. HDR uses
a declared reproducible tone policy, not inferred exposure. Reference parity
checks compare embeddings, cosine scores, rankings and top-k; they do not claim
end-user task relevance.

## CPU and Metal execution

CPU supplies portable decoding/preparation and an independent native arithmetic
route. Metal can import eligible VideoToolbox surfaces or stage native integer
planes without first materializing host RGB. Geometry coefficients and declared
transfer tables are reused under admission.

Equal-geometry frames batch in bounded waves. Preparation overlaps other admitted
model execution. Metal vision/projector outputs, device token composition,
backbone inputs and pooling/normalization remain resident until response readback.
EmbeddingGemma 2 preserves full-F32 vision attention. Model-owned immutable
weights survive requests; completed owned frames retire activation views at their
existing wait boundary. Explicit host readback remains available for reference
checks and final response serialization.

## Live ingestion and recovery

DASH and HLS readers retain source/configuration epochs and transactional cursors.
Growing fMP4 and normalized AVC TS can deliver model windows before HTTP EOF.
Acknowledged prefix hashes reject changed/shortened retries; accepted outputs are
retained across delivery failure before more transport work. Pending logical
pictures cap watermarks, preventing incomplete fields or partitions from becoming
model frames. Continuity changes drain epochs and report gaps explicitly.

Ordinary inference fails on corrupt codec input. Explicit recovery can restart
from validated synchronization and reports skipped intervals. Container sync
flags alone never authorize dropping AVC dependencies.

## CUDA/NVDEC extension plan

CUDA remains outside the delivered non-CUDA scope. Add an optional backend using
the same source, logical-picture, admission and cancellation contracts:

1. Bind NVDEC through a narrow native API with explicit device/context ownership
   and runtime codec/profile capability negotiation.
2. Retain decoded surfaces through CUDA stream completion; make context/stream
   handoffs and any copies observable. Hardware and portable software routes
   expose their actual selected capabilities.
3. Implement native-depth/chroma conversion, declared display/HDR policy, resize
   and patch packing with the same independent reference oracles.
4. Hand device inputs to vision/projector, token composition, backbone and final
   normalization without unnecessary host boundaries. Coordinate decode,
   preparation and inference admission across requests.
5. Qualify actual Linux GPU execution: vectors/scores/rankings, completed timings,
   transfers, memory ownership, cancellation/retry and sustained load. Separate
   hardware availability from compile-time support and cross-compilation.

## Evidence policy

Track generators, fixture inputs, regression seeds, independent reference oracles,
licenses/provenance and concise qualification summaries. Run outputs go under
ignored `.benchmark-results/`; CI may retain them as artifacts. Historical logs,
experiments and detailed reports remain recoverable at the commit linked from
the qualification summary. A cleanup does not refresh old measurements or expand
codec/origin conformance claims.

## Apple system decoder rollout

Keep maintained Zig decoders in standard builds. Optional VideoToolbox selection
is independent of CPU/Metal inference and has explicit portable/Apple overrides,
actual-backend counters, and pre-publication capability fallback. Extend the
existing H.264 lane with fixed-format VP9/AV1 WebM NV12/P010 replay, preserving the
logical index, display policy, admission and surface lifetimes. Formats outside
that qualified lane continue through Zig. Native decode/consumer errors remain
explicit; no automatic replay follows publication.

Pin the existing macOS 15 binary deployment floor and correct the Python wheel
floor to match it. Use the pinned 15.5 SDK cross-compilation check and native
macOS 15/26 decoder/model jobs. Qualification must distinguish absent hardware
from successful decode, compare portable pixels/model inputs and pretrained
rankings, and retain independent references. Apple-only packaging that removes
portable fallback remains deferred until a separately measured coverage/size
tradeoff is justified.
