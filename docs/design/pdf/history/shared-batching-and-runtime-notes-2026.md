# PDF.md Shared Batching and Runtime Notes (2026)

> Relocated verbatim from `zig/PDF.md` (lines 4894–5117 and 5193–5244 at commit 0f7fec50c5) on 2026-10-12. Durable decisions were folded into [PDF.md](../../../../zig/PDF.md), [BATCHING.md](../../../../zig/pkg/inference/BATCHING.md), and [MANAGERS.md](../../../../zig/MANAGERS.md) before the move.

These notes recorded inference-runtime work (tensor-forward fusion, composite runtimes, worker transport, the BackendRuntime maintenance scheduler and lane profile) while it was tracked in the PDF design doc. Lines 5118–5192 (the OCR verification narrative) are in [performance-and-ocr-verification-2026.md](performance-and-ocr-verification-2026.md).

## Shared model-forward batching

Document scheduling and inference scheduling remain separate admission layers.
The inference Node now provides a reusable tensor-forward dispatcher for
explicitly row-independent pipeline stages. It complements the typed Florence,
embedding, GLiNER and generation schedulers rather than forcing them through a
single result or prompt interface. Text rerankers, BERT-style NER, rewriting,
REBEL and Whisper stages use the same dispatcher on local and distributed Node
paths. NER request arrays form bounded native windows; rewriting arrays expose
bounded independent encoder/decode sequences with masked input padding.

The dispatcher groups by session generation, task, non-batch tensor geometry,
execution gate, supervision boundary and resource policy. It packs no more than
eight caller submissions / 64 rows / 64 MiB per window, admits additional
packing/compute before execution, and scatters validated zero-copy output views.
The physical output allocation and its lease survive until the last consumer
finishes. Cancellation never lets one expired caller discard a healthy peer's
rows. Unsupported shapes bypass fusion; capacity fallback occurs before a fused
forward, and backend failures do not trigger singleton replay.

Qualification remains explicit: dynamic graph metadata plus pipeline opt-in, or
a native session's concrete row-independence callback. Existing whole-request
GPU generation and multimodal paths retain their locks until their mutable
backend state is safely isolated. Fixed chunking has no neural forward to fuse,
and external providers control their own execution. See
[`pkg/inference/BATCHING.md`](../../../../zig/pkg/inference/BATCHING.md) for the coverage matrix
and remaining exclusions.

### Runtime ownership and stage-level performance

Keep three lifetimes separate: prepared document assets, a pinned model runtime,
and a physical execution window. ModelManager lazily owns composite encoder /
decoder sessions, tokenizers and metadata under its existing single-flight,
LRU/TTL and memory-pressure policies. Rewriting, REBEL and split Whisper borrow
these sessions through handles, allowing compatible requests to share the same
session generation. The existing manager load executor handles cold construction;
request cancellation abandons only that waiter while healthy peers remain.
Backend selection, resident admission and session destruction remain owned by
ManagedSession and the existing backend runtime; no per-request pool is added.
Composite generation keys include the validated transitive artifact dependency
closure (ONNX external data and native tensor shards), not only the selected
graph paths. Serving lookups reuse the component-plan dependency list, while
tokenizer, configuration and managed receipt changes also invalidate the key.

Queued tensor permits retain input bytes but yield idle compute. A physical
forward admits execution once, releases packed buffers after use and retains
only output residency for its consumers. A yielded permit must reacquire compute
for every subsequent direct/fallback execution. Native stage descriptors provide
concrete output and workspace geometry before direct or fused host-tensor
execution; Whisper encoder hidden states and decoder logits have distinct plans.
Reusable permits recheck larger later windows rather than freezing the first
invocation's geometry. `Session.planShapes` and `Session.planRun` now share an
allocation-free shape view, including configured decoder vocabulary widths and
native workspace estimates. Rewriting admits a prepared token owner before any
tokenizer work, validates the whole bounded request with those IDs, then plans
eight-row execution windows using actual padded widths. HTTP/direct validation,
inference and usage reuse IDs/counts rather than re-encoding input text. Planning accounts
for encoder residency and the worst decoder stage with one workspace per
physical stage, not per sequence. Stage gates include admission and packing so
this residency model remains valid. Permanent limits reduce execution width and
the prepared owner reserves token storage and one tensor window. Execution admission
remains the authority under concurrent pressure.

Eligible masked rewriting/reranking stages use bounded length classes, with
planning and materialization agreeing on padded widths. Late-interaction
reranking uses its admitted trimmed query/document lengths. Shared encoder rows
carry allocation provenance, enabling later compatible groups to borrow
contiguous storage without repacking while preserving per-request result slots.
Those borrowed columns receive residency credit only when a live source lease
covers the same admission domain; uncovered borrowed bytes are counted once,
while noncontiguous groups still reserve their packed copy and uncovered inputs.
The generic decoder reuses token IDs and reads argmax directly from logits.

Remaining backend work: define and qualify request-owned incremental decoder
adapters for separate init/past graphs and native Whisper, plus device-resident
cache and fused row gather/scatter support. The first concrete generic adapter
now handles qualified merged seq2seq exports using the existing ONNX KV owner.
It performs prefill once, submits only appended tokens, and retains immutable
cross-attention state when cached branches return empty cross outputs. Both
generic seq2seq and transcription use it when the selected artifact exposes
the explicit ABI. Composite runtime selection considers an optional merged
decoder and qualifies its declared graph signature before backend construction,
then verifies the loaded ABI. Backend-neutral generation fingerprints cover all
considered graphs and external data, while executable backend policy covers only
the selected encoder/decoder pair. Selection shares the runtime single-flight
and generation cache; unqualified candidates incur no backend session or weight
residency. Unsupported candidates preserve the ordinary route.
Native generation / Florence caching are unchanged. This is not a new
prefix-cache implementation.
Cache output geometry and old/new cache overlap are admitted against host and
KV limits before execution; allocation failure and cancellation unwind the
request-owned state. Other layouts retain their existing execution path.
Qualified merged steps now fuse both prefill and cached invocations with matching
positions. Small broadcast controls are explicit and their values participate
in compatibility; they are never concatenated as batch rows. Empty cross-cache
results retain the prior per-request owner. Hermetic tests show three physical
forwards instead of six for two independent three-step sequences, with matching
context-specific logits. This is a forward-count result, not a hardware speedup
claim. Device-resident cache transport and device-side row gather/scatter remain
unimplemented; the host KV path continues to account for old/new overlap.

The follow-up scheduler now partitions the entire bounded tokenized rewrite queue
by length class before padding, materializes at most eight rows at a time, and
restores request order after execution. Capacity
failures subdivide groups before forwarding instead of discarding all batching;
physical subgroups have distinct execution identities. Imported seq2seq stages
use named-input output projections, with explicit transformed time axes and
feature-width bounds. Missing non-text geometry disables fusion. Execution
mutexes belong to the pinned runtime/stage instead of a pointer-hashed array.

Extend length bucketing to other pipelines only with masking and output-position
parity tests. Accelerator-backed parity, peak-memory and throughput measurements
remain release requirements; hermetic batching tests do not establish a hardware
speedup.

### Worker transport and enforced preparation admission

The supervised embedded worker preserves the same task-neutral provider contract
as in-process execution. Protocol v4 forwards JSON plus borrowed encoded/raster
attachments and item/source/page references. The RPC sender uses scatter/gather
segments, retaining caller buffers for the invocation; the receiver reserves its
bounded logical payload before accepting chunks. No base64 or sender-side media
slab is introduced. This applies to readers, generators, embedders and any other
executor using the attachment ABI, independently of deployment placement.

Typed numeric provider responses preserve dense-vector/score kinds and row
lengths using little-endian f32 payloads. The host owns decoded rows until response
destruction. Invalid lengths, nonfinite values and missing typed results fail
closed: a typed-memory plan cannot silently fall back to JSON parsing. Existing
RPC cancellation, resource callbacks, response ownership and protocol-version
handshakes remain authoritative.

Prepared text uses an admission-backed allocator for every tokenizer allocation,
including normalization, Unigram Viterbi scratch, retained IDs and realloc overlap.
Admission credit is acquired before backing allocation in 64 KiB quanta, with an
exact-size retry when slack would exceed the limit. Individual allocations still
use the backing heap; their frees return credit locally without resource RPCs.
Preparation trims unused credit, retaining IDs, allocation bookkeeping and bounded
staging. The owner tracks every live allocation and reclaims even blocks abandoned
by a failing tokenizer before releasing its leases. Metaspace also cleans up its
own partial words/lists; direct and owner-wrapped real-tokenizer allocation-failure
tests cover both contracts. A thousand sequential scratch allocations reuse one
admission reservation. Length-class
queue indices/result descriptors have their own admitted allowance. A 16 KiB
tokenizer-scratch regression rejects over-budget growth before the backing
allocation; allocation-failure tests cover complete cleanup.

Worker route capabilities constrain rendering before materialization: the 64 MiB
logical request ceiling reserves 1 MiB of attachment metadata plus worst-case
attachment descriptors/MIME strings. The payload ceiling is distinct from model
decoded-pixel limits: only raw PDF planning derives a tightly packed RGBA pixel
ceiling through `renderPixelLimit(true)`. Encoded images keep the model's pixel
allowance, and raw invocation accounting includes stride padding. Oversized fixed
transforms disable raw transport and retain encoded execution. Metadata is checked
using actual serialized JSON, including escaping; UTF-8 length divided by a
worst-case expansion factor is not an input limit. Text-only requests retain the
original logical JSON ceiling. Regressions cover compressed 25-megapixel inputs,
a 100 KB ASCII prompt, and escaping that actually exceeds the metadata allowance.

Linked-worker embedding windows also enforce that metadata ceiling before
dispatch. Planning and ABI serialization share the embedding request structure
and binary-part projection. An allocation-free prefix counter avoids rescanning
earlier items, including JSON escaping, model/task/instruction fields, array separators,
and attachment-count digits. Mixed text/image windows split without shrinking
valid text-only batches to the attachment limit. Tests cover two 600 KiB text
items mixed with an image in either order, escaped text, exact-boundary requests,
and attachment-count digit transitions. Oversized indivisible metadata fails
before invoking the provider; no post-inference retry is needed.

The worker also advertises `attachment_envelope_max_bytes`, its complete 64 MiB
request-body ceiling. This is independent of the 1 MiB attachment-metadata limit
and applies even with zero attachments. Embedding windows check both constraints;
their exact total includes the envelope header, attachment descriptors, MIME
strings, serialized metadata, and binary payloads. Incremental framing arithmetic
is shared with the envelope encoder rather than duplicated in the planner.
Regressions cover two 6 MiB NUL-containing text items (about 36 MiB of JSON each,
72 MiB together), exact-limit and one-byte-over-limit requests, and binary payload
overhead. Valid text-only requests above 1 MiB remain supported; a single item that
cannot fit the complete envelope is rejected before provider dispatch.

Direct and fused output storage still release tensors independently. Related
self-cache and cross-cache columns coalesce byte-credit reductions: physical
buffers are freed immediately, while their credit remains conservative until the
last column of that cohort dies. Other outputs keep independent credit lifetimes.
The final cohort releases its lease without a redundant reduction RPC. A hermetic
129-output regression observes two intermediate resource callbacks rather than
one per tensor. Retained cross-cache cannot pin obsolete logits/self-cache storage.

Host seq2seq decoding can detach a lone surviving row from oversized immutable
batch columns. A lifetime hook proves no other view or view producer remains;
the optimization requires at least 64 KiB of savings and a twofold column shrink.
Admission covers old/new overlap, copies commit atomically, and allocation or
capacity failure retains the original valid cache. Failure-injection and shared
row tests cover rollback, live-peer refusal, values and byte-credit cleanup.
This is last-survivor host compaction, not device-side multi-survivor gather/scatter.

For sixteen alternating short/long rewrites, global width grouping allows six
physical forwards instead of twelve from adjacent eight-item sorting, while
preserving original result order. This is an achievable scheduling improvement,
not a guaranteed count or a measured accelerator throughput claim. Stage fusion
is opportunistic: worker saturation, inline `Group.async` execution, and expired
coalescing windows may produce smaller valid batches. Rewriting regressions
therefore assert bounded calls, exactly-once stage rows, padding work and ordered
results, including with no spare workers. Complete-window tensor fusion and
shared-output lifetimes are tested deterministically at the executor boundary.

Private provider errors must be diagnosed at their owning runtime before the
first ABI or worker-RPC hop normalizes them. Both linked and isolated inference
use one bounded operation/error/model diagnostic policy, retain stable errors,
and normalize private causes to `InferenceProviderFailure`. The receiving bridge
must not re-log normalized errors or expose backend-specific error names in its
wire ABI. Provider-envelope decoding and serialization failures obey the same
rule; logs never include request bodies or media bytes.

Resident PDF OCR additionally separates cross-attention cache backing ownership
from active tensor shape. In-place EOS compaction moves surviving rows but does
not resize the backing allocation. The decoder publishes exact active-prefix
views after each compaction and uses those views for attention. Original buffers
outlive all views, including CUDA's non-retaining row views; Metal keeps retained
device views. Repeated compaction replaces only view descriptors, not KV payloads.
Teardown releases views before backing storage, and allocation failures unwind
both. Hermetic regressions exercise 7-to-3-to-1 rows, ordered values, stable
payload allocation, borrowed ownership and every allocation failure.



`BackendRuntime` lazily owns one maintenance scheduler. TTL, transaction recovery,
text merging, sparse compaction, enrichment, resolution/promotion, derived-index
replay, artifact repair and quarantine retry register work instead of parking a
thread per shard/index. A callback performs a pass, then parks or returns a retry
delay; notifications coalesce and survive a running pass. Derived workers yield
after a replay window while retaining their session until its idle deadline or
an explicit close request. Runnable scans rotate fairly, and completed callbacks
are joined before dispatch so newly available capacity cannot strand ready work.

The default 48-worker durable I/O lane is unchanged. At most half its capacity is
admitted to maintenance callbacks, with separate reserved shares for maintenance,
producers, derived publication and propagation. This prevents producer waiters
from starving their derived dependencies, and propagation writes from starving
the target's enrichment. Scheduler activation requires at least eight configured
durable workers, leaving capacity for its coordinator, durable-job reaper and
storage/commit work. Smaller configured lanes fail explicitly at activation.
Lane statistics expose registrations, active/class counts, peaks, dispatches and
dispatch retries. No capacity error is converted into a larger thread ceiling.

Dedicated service workers are part of the same `BackendRuntimeLaneLimits`
profile, not an additional independent pool allowance. `worker_capacity`
reserves 32 slots for Raft senders, listeners, observers and other service owners;
API and inference lanes default to 48 each, while durable work remains 48,
Raft inbound/outbound remain 32 each, control remains 8 and PDF rendering remains
4. The combined default is 252 under the 256-slot aggregate ceiling. Validation
rejects overcommitted profiles before allocating executors. Worker reservations
remain lazy and isolated, with capacity returned only after executor teardown;
unused sibling capacity cannot be borrowed and starve dependencies. A zero
worker capacity explicitly disables dedicated worker admission.

The bounded image-row decoder uses the shared vectorized `antfly_hash.Adler32`
implementation and validates its finalized checksum against the zlib trailer.
Native, test, benchmark and WASM PDF modules all import the shared checksum
module; malformed/truncated stream checks and predictor handling are unchanged.

Registration handles are lifetime leases. Stop detaches queued work and joins
active callbacks before the owner is freed; cancellation-sensitive compaction
also cancels its active I/O future. Runtime shutdown closes registration and
waits for handles to drain in release builds before destroying the coordinator.
Tests cover hundreds of owners on a tiny lane, in-flight wakeups, dependency
capacity, cancellation, shutdown fencing, session reuse and retry recovery.

Model loading also separates a flight's join eligibility from its reference
lifetime. Completed failed flights are detached when a new request retries;
existing waiters still receive the original result and the task may finish its
cleanup independently. This applies to ordinary models and composite assets
(including Whisper metadata). A failed load is not an implicit negative cache,
and cleanup of the old flight cannot remove a new flight registered under the
same key. A deterministic regression holds the old task reference across retry
and replacement, reproducing the immediate-retry CI failure without sleeps.
