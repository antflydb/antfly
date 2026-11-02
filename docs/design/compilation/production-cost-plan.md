# Production binary size, compilation time and memory plan

Status: implementation in progress. Source/artifact investigation and baseline
are from 2026-11-01, source `9b3fda9f22`, Zig 0.17.0. The validated first slice
is recorded in [media session implementation](history/media-session-2026-11-01.md).
The full production PDF/image migration and subsequent redesigns remain open.

Current architecture and acceptance policy remain in
[COMPILATION.md](../../../zig/COMPILATION.md). Recent accepted results and the
rejected mutation outlining experiment are recorded in
[production retention](history/production-retention-2026-11-01.md).

## Implementation status

| Item | Current state |
| --- | --- |
| P0 measurement tools | Both-product CPU profiling, tested Mach-O map reader, pinned compiler time-report collector, scoped Linux memory snapshots implemented; full baseline qualification and server map still pending |
| P1a opaque PDF session | Retained state, stable allocator/deadline adapters, operation cancellation, directly charged PNG output and compiled source-free tests implemented |
| Media build isolation | Fixed automatic storage/inference linking into media-only tests; provider/client/contract cache matrix passed |
| P1b/P1c production migration | Template PDF defaults have a tested opt-in owner migration; [product screening is pending](history/template-pdf-owner-2026-11-02.md). Batch/raster/analysis contracts and remaining PDF/image callers/caches are open |
| P2–P7 redesigns | Open, apart from the media cache matrix above; acceptance gates remain unchanged |

P1a and the measurement tools do not establish smaller production binaries or
faster complete builds. The native product regression passed; both installed product sizes remained
unchanged. No whole-product size reduction or cold-build speedup is claimed.

## Decision

Start by completing the shared **PDF/image compute owner** that already exists.
Then investigate lake execution in the API kernel, split public request values
from private command decoding, and remove private layout dependencies from
remaining clients. Treat mutation resource ownership and quantized kernels as
separate compiler-memory experiments. Improve cache reuse throughout.

Do not begin by adding more general-purpose archives, rewriting all JSON, or
promoting the existing tiny index/tensor probes. Each production migration must
remove its old implementation roots and improve the measured combined product.

There are four separate outcomes to track:

| Outcome | Primary measurement | Do not substitute |
| --- | --- | --- |
| Binary size | Installed stripped `antfly` and `libantfly`, section/atom sizes | Sum of static archive sizes |
| Cold build speed | Both products from equally cold compiler caches, per-unit time and critical path | A cached build or a small test caller |
| Compiler memory | Individual compiler peaks, simultaneous aggregate/cgroup peak, swap | Sum of unrelated per-process maxima |
| Runtime memory/performance | Request allocations, peak live bytes, latency/throughput | Binary size or compiler RSS |

## Evidence collected for this plan

The baseline is native Apple Silicon/macOS, baseline CPU, stripped ReleaseFast,
with Metal, CUDA and system BLAS disabled. Other target profiles need their own
qualification.

| Artifact | Bytes |
| --- | ---: |
| `antfly` | 229,621,752 |
| `libantfly.dylib` | 80,026,400 |
| Inference archive | 47,953,232 |

The server remains 3,981,856 bytes above the earlier same-profile pre-query-owner
baseline of 225,639,896 bytes. Recover that cumulative cost before claiming a
net executable-size improvement across the redesign.

### Actual linked C library

The current external Mach-O link already retains a map through
`build_support/embedded/shared_library.zig` and `tools/capi_exports.py --map`.
The cached dylib accompanying the map was SHA256-checked against the installed
library: `0c41276f91c90969bfb03c8f94cf63e6e403066ed5ff30bcd5388a01c3b29e67`.
This is final-link evidence, rather than archive address-gap inference.

| Linked region/owner | Bytes |
| --- | ---: |
| Entire `__TEXT,__text` | 66,600,612 |
| Storage owner text | 41,667,916 |
| Inference owner text | 23,297,164 |
| Shared enrichment owner text | 1,568,992 |
| `__TEXT,__const` | 6,503,996 |
| `__TEXT,__cstring` | 1,665,076 |
| `__DATA_CONST,__const` | 1,464,144 |
| `__TEXT,__eh_frame` | 2,656,368 |

Code is about 83% of the library file. The two shared PDF font arrays total
1,029,364 bytes and a tokenizer literal is 711,662 bytes. These are secondary
size levers; fonts already have one physical owner. Zero-filled BSS and TLS
regions are runtime reservations, not bytes to subtract from the file size.

| Function | Final `libantfly` instances | Linked atom bytes, total |
| --- | ---: | ---: |
| JPEG 2000 `parseState` | 3 | 300,496 |
| JPEG `decodeRgbaWithOptions` | 3 | 105,864 |
| PDF `Reader.decodeImageToRgbaGuardedAlloc` | 3 | 154,588 |
| TLS `client_compat.init` | 2 | 140,056 |
| Batch `parseBatchRequestWithOptions` | 1 | 12,060 |
| Schema `deserializeSchemaOwned` | 1 | 71,572 |

Grouping exact text names across objects, excluding `__func_` specializations,
identifies a nominal 3,848,196 bytes beyond the largest instance of each repeated
name. That is an upper bound for that identified set if its implementations can
be unified. It is not a forecast, a complete duplication inventory, or additive
with the table above. Equal names alone do not establish equivalent code.

### Server retention and archive leads

An exploratory matcher extracted each archive's Mach-O text, skipped relocated
instruction bytes, and checked several unchanged code fragments at consistent
offsets in the stripped products. It found candidate instances as follows:

| Family | `antfly` candidates | `libantfly` candidates |
| --- | ---: | ---: |
| JPEG 2000 parser | 7 | 3 |
| JPEG decoder | 7 | 3 |
| PDF guarded image decoder | 7 | 3 |
| TLS initialization | 8 | 2 |
| Batch parser | 4 | 1 |
| Schema deserialization | 4 | 1 |

The C-library counts agree with the exact linker map. Server matches remain
exploratory, not a replacement for a server link map; a missing match would not
prove absence. Match results from different archive variants were deduplicated
by final-image address.

The API kernel emits about 48.0 MB of attributed text, storage 44.8 MB, and
inference 31.9 MB. Across all ten archives, heuristic categories attribute
16.22 MB to JSON encoding/decoding, 3.19 MB to image codecs and 1.17 MB to TLS.
These are emitted-work leads, not final-size savings. PDF is undercounted by the
current name heuristic because many of its symbols start with `reader`,
`syntax`, `jbig2`, or font-related names rather than `pdf`.

The API kernel also has 4.07 MB under lake-index names, including a 171,896-byte
native text-index builder. The category includes readers and publication code;
it must not be described as 4.07 MB of duplicated builder code.

Raw exploration receipts are ignored under `.benchmark-results/compile-costs/`:
`exploration-libantfly.map`, `exploration-libantfly-live.json`,
`exploration-retention.json`, and the exploratory matching script/log. No new
whole-product builds were used to obtain these findings.

## P0: make experiments decisive

**Files:** `.github/workflows/profile-zig-build.yml`,
`scripts/diagnose-zig-build-memory.sh`, `scripts/report_zig_archive_work.py`,
`scripts/report_zig_binary_size.py`, `zig/tools/capi_exports.py`; new report
collectors/parsers where needed.

1. Extend the opt-in profile to build `antfly capi runtime-archives` together.
   Its current single-step, x86_64 GNU, unstripped profile is useful for
   attribution but is not the stripped ARM64 musl release qualification.
2. Retain the already-generated CAPI map. Add server final-link attribution on
   the pinned linker. If the driver cannot expose a map, keep any alternative
   unstripped attribution profile separate and compare actual production
   artifacts independently; do not assume post-link stripping recreates an
   otherwise identical `-Dstrip=true` build.
3. Add a tested map reader: live versus dead atoms, sections, input objects,
   aliases, exact linked sizes and repeated implementation candidates. Decode
   maps with invalid symbol-name bytes safely; preserve raw bytes for audit.
   Improve source-family attribution using explicit source mappings and the
   declaration/file report; do not interpret the current empty PDF category as
   absence. Keep unknowns visible and overlapping estimates separate.
4. Collect the pinned compiler's detailed time report. Zig 0.17's
   `std/Build/abi.zig` exposes reachable/imported files, generic instances,
   inline calls, parse/AST/sema/codegen/link times, per-declaration times and
   LLVM pass timing text. `zig build --time-report` forces rebuilding and
   implies `--webui`: capture its versioned WebSocket protocol separately.
   It must never be enabled during a warm-cache isolation experiment.
5. Preserve raw per-process RSS and same-poll aggregate samples. On Linux also
   record cgroup peak and swap; report compiler peaks separately from tests,
   configuration, generators and linkers. Do not lower scheduler claims from
   the noisy native observations in the previous record.

Freeze source, compiler and stdlib hashes, target/CPU, SDK/libc, backend flags,
sanitizers, PIC, strip, generator versions, version metadata, CPU allocation and
memory/swap limits. Prime dependencies/generators equivalently or label them
cold on both sides. Distinguish compiler-cold timing from fully cold end-to-end
timing. Keep diagnostics overhead out of the primary acceptance timing.

Screen a candidate with one paired run; qualify a promising candidate with at
least three controlled baseline/candidate pairs in alternating order on the
normal runner. Start with native CPU for local screening, ARM64 Linux musl for
the original memory failure, and the actual release backend profiles for
acceptance. GPU/default-Metal results do not inherit CPU-only qualification.

**Exit:** the same frozen baseline reproduces within recorded variance, both
final artifacts have attribution, and the collector separates semantic work
from LLVM/link work. No new architectural split is justified by symbol size alone.

## P1: complete shared PDF/image ownership

**Why first:** confirmed duplication affects both products, and the physical
enrichment archive is already shared. `embedded/embedded.zig` constructs it;
`pkg/antfly/build/runtime.zig` reuses that exact artifact for the server.

**Existing owner:** `pkg/antfly-embedded/src/enrichment_compute_root.zig`,
`storage/enrichment_compute_provider.zig`, `storage/enrichment_compute_abi.zig`.
Existing operations cover extraction streams and single-page PNG rendering.

**Direct consumers to inventory/migrate:**

- `pkg/inference/src/documents/sources.zig` and `pipelines/media_chunking.zig`.
- `pkg/antfly-embedded/src/storage/db/enrichment/document_extraction.zig`.
- `pkg/antfly-embedded/src/storage/background_runtime.zig` PDF caches.
- `pkg/antfly-embedded/src/template_remote.zig` and remaining image consumers.
- Module wiring in `build_support/antfly/dependencies.zig`, `imports.zig`,
  `build_support/embedded/embedded.zig`, and the PDF/image build helpers.

### Boundary

Add a private, storage-free media contract with opaque document/session handles,
bounded batched page requests, metadata/text views and owned raster/encoded
results. The provider owns parsing, fonts, masks, color conversion, codec state,
parsed-page caches and cleanup. Callers retain routing, model selection,
admission, durable orchestration and response formatting.

Render/extract a complete page or bounded page batch per call. Do not introduce
per-pixel, glyph, component, tile or packet data-operation callbacks. Preserve
bounded cancellation/budget probes and normalize deadline clock domains.
Preserve a parsed PDF
session across pages; repeatedly invoking today's single-page PNG operation
would reparse documents and could add PNG encode/decode round trips.

Session input lifetime must be explicit too: retain the existing source/backing
lease until session close, or own a budgeted copy when retention is impossible.
Do not keep a parsed view into a temporary request body, or silently copy every
whole document merely to make the new API convenient.

For native OCR/model consumers, support direct raster results where existing
semantics permit them. Keep PNG output for consumers that require PNG. Keep
PDF's U16/component-plane/color processing inside the provider: an RGBA8-only
low-level replacement would lose supported PDF behavior. Source-only codec
package APIs retain their existing precision and representations.

Result lifetime is an implementation gate, not a detail to defer. Never retain
a borrowed stack-backed budget allocator after the call. Follow existing
provider-owned buffer/destructor conventions, retain caller admission through
result destruction, and account both result and scratch bytes. Where a decoder
uses one allocator for both, use a stable owned budget frame or separate result
and scratch allocation explicitly. Prove equivalent charging before migration;
do not gain compilation isolation by bypassing process memory accounting.

### Module construction

Create explicit implementation and client module graphs. The media owner gets
raw PDF/image implementations; production consumers get contract/client
facades. The raw PDF implementation uses the raw image implementation in the
same owner. A source-only/WASM construction continues using source code.

Preserve the current page executor, parallelism and acceleration choices. The
move must not silently force CPU rendering or transfer GPU device ownership to
the media provider. Resolve executor/device borrowing in the contract prototype
without importing the inference Node or creating a media/inference cycle.

Do not mutate one shared module instance's options per consumer or let the
provider import its own client facade. Both would risk identity conflicts or
recursive dispatch. Standalone codec tests keep source construction; separate
compiled-provider tests exercise the production path.

### Delivery slices

1. **P1a:** contract, ownership/budget prototype, opaque PDF session and compiled
   client tests, initially outside the default consumer graph.
2. **P1b:** migrate all complete PDF operations/caches. Shared geometry and
   raster metadata may remain source-only values; private Reader/font/mask
   representations must leave consumers. Inventory remaining roots after each
   migration; one retained direct path can keep the whole renderer linked.
3. **P1c:** migrate complete encoded-image inspect/decode and needed encode
   operations into that same provider. Keep small processing math local unless
   profiling identifies meaningful duplication. Add extra sample/metadata
   operations only when the production caller inventory requires them.

**Exit:** the full migration has one physical JPEG/JPEG2000 implementation and
one PDF renderer in each product; old consumer implementation symbols disappear.
Both final products and aggregate emitted text shrink. A media implementation
edit rebuilds the media owner and relinks products while storage, inference and
API LLVM owners remain cached. P1a alone must not be reported as that result.

**Validation:** PNG/JPEG/JP2/J2K/JBIG2, 8/16-bit PDF images, masks/ICC/palettes,
reduced-resolution decode, cache reuse across pages, exact supported output,
malformed inputs, cancellation, timeout, allocation failures and destruction
after partial initialization. Preserve existing decode limits. Run PDF safety,
codec conformance using the hash-verified shared fixtures, compiled consumer
tests, public read/OCR paths, and relevant CPU/Metal/CUDA serving qualification.

**Stop/revise:** extra full-raster copies, repeated PDF parsing, hidden
serialization of concurrent callers, uncharged result memory, growing either
product, a provider cycle, or consumers still compiling the old implementation.

## P2: put lake execution behind a complete operation

**Files:** `api/lake_index_publication.zig`,
`lake_index_publication_builders.zig`, `lake_index_native_{text,rows,sparse,dense}.zig`,
their contracts/readers, `runtime_api_kernel_root.zig`,
`runtime_serverless_root.zig`, and runtime build/import wiring.

Separate request/auth/catalog/publication control from physical lake work.
Prototype a whole build/rebuild operation that owns the row-source scan,
native index writers, attempt-private artifacts, cancellation and cleanup.
Return owned declarations/contribution descriptors plus an explicit outcome.
Keep publication/fencing under the existing authorized leader and preserve
retry/idempotency and failed-attempt reclamation.

First test reuse of the existing serverless execution owner. Its current text
is about 12.9 MB, below the API kernel's 48 MB; moving ownership may lower the
largest unit without creating another archive. This is conditional: audit
serverless-to-API dependencies and avoid a runtime reentrant cycle. If it cannot
own the complete operation cleanly, use a narrowly justified lake-execution
owner, not a second copy of the serverless runtime.

Current builder arguments expose `ServingSource`, `ArtifactStore`, a row-source
provider and a parallel scheduler. Pass opaque operation resources or let the
execution owner construct them. Never replace these with per-row foreign
callbacks. Apply the same rule to complete pinned lake queries if their physical
readers keep the implementation rooted in the API owner.

**Exit:** the API kernel no longer emits the migrated writers/physical execution;
publication semantics are unchanged; implementation-only edits stay in the
execution owner; the receiving owner's growth, aggregate work and normal-runner
critical path are measured. The 4.07 MB namespace figure is a lead, not a saving.

**Validation:** pinned snapshots, incremental/base reuse, source changes during
construction, canceled/stale publications, crash/retry/reopen, sparse/dense/text
and ordered indexes, workload limits and object-store failure paths.

## P3: separate request values, then share codecs selectively

**Files:** `pkg/antfly-embedded/src/api/batch.zig`,
`storage/db/mutation_values.zig`, `storage/db/types.zig`,
`api/public_table_http.zig`, `api/distributed_txn.zig`, `api/transactions.zig`,
server write/Raft adapters and CAPI batch entry points.

`OwnedBatchRequest` currently contains public writes plus ownership for private
merge, restore, policy, topology, artifact and transaction controls. It imports
`selected_root.zig`. `parseBatchRequestWithOptions` selects public/internal and
local-owner behavior with runtime booleans. Exposing that type through one new
library would preserve coupling; making the booleans comptime could instead
duplicate the large parser in owners needing both modes.

1. **P3a:** introduce a storage-implementation-free public mutation value/view
   contract, reusing existing concrete mutation values. Separate private command
   values and their owning parse state. Move type definitions once and import
   them from implementations; do not fork similar DTOs in each owner. A type
   move can change `@typeName`-based native IDs: version the affected private
   contracts and verify existing wire bytes/schema digests remain unchanged.
2. Keep public rejection, trusted internal decoding and local-owner predicate
   permissions as separate entry paths. An untrusted JSON field must never
   select internal privileges. Preserve every existing private wire version.
3. **P3b:** move a measured complete parse/validate/partition/encode family to
   one owner using those contracts. Decide placement after P0 timings: putting
   more parsing into the already largest storage compiler may worsen memory;
   a small request-codec owner is justified only by measured removal elsewhere.
4. Use provider-owned parsed frames or bulk views with explicit input lifetime
   and one destructor. Preserve typed relational/native callers without forcing
   them through JSON. Native handles never travel over replication transport.
5. Keep public and private implementation branches physically separable so a
   public-only CAPI link retains its current small parser path. Assert absence
   of private decoder roots in the final map. A shared runtime-flag parser that
   drags a 177 KB implementation into the current 12 KB CAPI path is a failure.

**Validation:** exact numeric spellings above 2^53 and integer bounds; nested
transform values; public/private and mixed-command rejection; transactional
range/identity/version proofs; borrowed inputs; manual typed requests; complete
allocation-failure cleanup; durable and ambiguous commit outcomes.

The existing borrowed document ABI is useful transport groundwork, not proof
of a speedup: its prior 1,000-document difference was within observed variance.
Profile allocation/copy costs before changing default transports.

If detailed timing shows generated JSON specialization dominates beyond these
families, pilot a descriptor-driven codec for one cold DTO family. Do not replace
all typed serialization with DOM conversion: that can add allocations, normalize
number tokens and slow hot responses. The 16.22 MB archive JSON total is neither
identical code nor a recoverable-size estimate; linked CAPI JSON text is 2.65 MB.

## P4: finish opaque ownership at remaining native boundaries

**Files:** `storage/engine.zig`, `engine_exports.zig`, `local_query_abi.zig`,
`pkg/antfly/src/storage/local_query_provider.zig`, relevant resource/session
contracts and `build_support/embedded/storage_boundary.zig`.

Some high-level clients are opaque already. The remaining engine descriptor
still checks DB/Core/IndexManager/DocStore/prepared-owner/session layouts, and
the query provider resolves a checked native DB borrow. `TypeContract.of` uses
size/alignment and a shallow layout hash; it still requires those private type
definitions. Simply deleting checks would remove safety without removing the
borrow or its compilation dependency.

Migrate one native-borrow family at a time to owner-issued handles and concrete
operation values. Start with complete operations whose client does not need
private fields. Preserve checked native borrows until each replacement passes
its tests; version changed private contracts/accessors together.

For queries, prototype an owner-issued retained read session with bulk physical
plan execution and owned results. The storage owner retains DB/cut/generation
leases. The query owner retains parsing/fusion/response responsibilities only
where they are genuinely independent. Crossing count must depend on whole query
phases/bounded batches, not posting or candidate count. If that is impossible,
co-locate the inseparable operation rather than create per-record dispatch.

**Exit:** a private DB layout edit rebuilds its physical owner and relinks
consumers; unrelated API/CLI/distributed owners stay cached. The completed query
migration must also eliminate its DB-layout dependency, not just rename it.
Editing public contracts still rebuilds their actual consumers, intentionally.

**Validation:** exact ABI mismatch failures, callback and error identity,
cancellation, IO/allocator domains, close/drain, retained query continuation after
closing/removing the donor, snapshots, backup/restore and lifetime failures.

## P5: redesign the mutation resource frame before splitting more libraries

**Files:** `storage/db/mutation_commit.zig`, `mutation_plan.zig`,
`mutation_preparation.zig`, `mutation_materialization.zig`,
`execution_resources.zig`, `local_mutation.zig`.

The main worker already has a concrete `PreparedBatchOwner`. Current
`mutation_plan.PreparedMutation` handles request coalescing/transforms; it is not
a complete commit-effect plan. Two resource aggregates already have non-inlined
destructors. Repeating those completed changes is not a new optimization.

The remaining worker emits about 1 MB of machine code. First use declaration/
LLVM timing and diagnostic IR to determine whether its cost is cleanup/control
flow, inlining, or surrounding type analysis. Keep IR generation out of normal
builds and acceptance timings.

1. **P5a:** inventory acquisition/release order, locks and all fallible exits.
   Introduce one explicit prepared-commit resource frame, initially preserving
   every existing lifetime. Frame destruction must handle partial initialization
   and use bounded, non-inlined cleanup. Do not casually shorten schema,
   snapshot, graph-publication or IO-domain leases.
2. **P5b:** separate request-only preparation, locked revalidation/effect staging,
   the durable store/replay/participant commit, and post-commit publication/waits
   into concrete functions borrowing that frame. Preserve version/generation
   revalidation under the original fence. First migrate one complete ordinary
   document effect family; extend to graph/relational/control effects after it.
3. Carry the existing committed disposition/receipt through every exit. The
   irreversible point remains `putBatchWithReplayAndParticipant` (or the
   equivalent transaction resolution). After it, preserve `primary_committed`,
   durable outboxes, exact errors and repair-required/ambiguous outcomes.
   A post-commit failure must not become an automatic retry of the mutation.

No new archive is required for this experiment. Smaller functions/resource
lifetimes can change optimizer work within the existing compilation unit;
moving code between files alone cannot provide that isolation.

**Exit:** repeated normal-runner reduction in the relevant compiler phase and
peak/critical path, with no lost semantics or runtime regression. The prior
137-line outlining patch saved only 24 KB and has no controlled performance
qualification. Keep it as a diagnostic reference, not an accepted design.

**Validation:** ordinary and generated writes, transforms/retries, graph and
relational effects, transaction resolution, replication/receipts/outboxes,
split/merge and restore controls, crashes around commit, ambiguous outcomes,
visibility waits, OOM and production-LSM/simulation compatibility suites.

## P6: bound quantized-kernel specialization

**Files:** `pkg/inference/src/ops/native_compute.zig`, quantized dot/panel helpers,
inference build/runtime construction; new pure kernel module and benchmark if
the profile supports extraction.

`linearQ8KQ8KActivationGroupedRange` has separate two/three-projection instances;
the largest is about 244 KB. Its roughly 530-line body combines MR4/MR2/MR1,
panel widths 16/8/4 and tails, with nested inline projection/lane loops. Its
inputs are already mostly raw slices and bounds, making it more promising for
isolation than a helper that imports the whole model/backend object.

1. Compare bounded concrete tile/reduction kernels with the current expanded
   body. Keep SIMD and the existing accumulation policy. Try sharing the large
   tile body across projection counts before making every loop dynamic.
2. Keep dispatch, panel construction/cache accounting and parallel scheduling
   explicit. A smaller function that adds calls inside every quantized block
   or repeats input packing can lose serving throughput.
3. If owner profiling still identifies a material inference bottleneck, move a
   complete quantized projection/range family to a compiled pure-kernel owner.
   The boundary accepts raw tensor/panel views, validated shapes, cancellation
   and necessary IO/allocator borrows; it must not import `NativeCompute`, model
   registries or Node. Calls are bounded by whole operations/worker ranges,
   never scalar multiply, dot-product block or vector candidate count.
4. Add another family such as Q4/Q5 only after the Q8K production comparison.
   Measure support-code duplication, both artifacts and aggregate memory before
   adding another archive. The existing RoPE/GEGLU probe is a correctness/cache
   prerequisite, not evidence that this expensive family has been isolated.

**Validation:** pair/triple, bias/no bias, MR/panel/tail shapes, SDOT/fallback,
packing states, prefill versus single-row decode, threading/cancellation, existing
numerical oracles without relaxed tolerances, and representative model latency/
throughput. Add a self-contained matrix benchmark; keep packing and steady-state
compute timings separate. Reject a reproducible serving regression by default.

## P7: make unchanged production work disappear

This work accompanies P0–P6 rather than waiting until the end.

- Extend real archive cache tests, not just mock build-graph tests. An
  implementation-only edit rebuilds its owning archive and final links. A
  consumer-only edit leaves the provider cached. An ABI change rebuilds the
  affected participants. Include media, request codecs, private DB layouts,
  lake execution, quant kernels, release metadata and SDK changes.
- Use implementation edits that affect reachable code in isolated overlays,
  alongside existing comment-based checks; artifact status alone is not a
  compiler-work measurement. Remove isolated caches afterward.
- Cache compiler outputs by the complete compiler/stdlib, target, backend,
  sanitizer, SDK/libc, generator, implementation and contract inputs. ABI-only
  keys would reuse stale implementations. A whole-repository commit hash as the
  sole compiler-cache key would unnecessarily lose reuse.
- Keep final release artifact/provenance keys tied to the exact source commit.
  Reusing an owner's compile output must not mislabel an old product as a new
  release. Keep volatile version metadata at final composition, as today.
- Pin the SDK explicitly in repeated Mac comparisons. Audit current
  artifact/cache workflows before adding another overlapping cache system.
- Use the existing local watch paths for development and compile-time
  `-Dtest-filter` for focused root inference compilation. Runtime `--test-filter`
  selects execution only. Neither is a whole-product cold-build speedup.

## Lower-priority experiments and exclusions

| Option | Decision and prerequisite |
| --- | --- |
| Shared TLS | Confirmed 8/2 initialization candidates, but only 140 KB of linked CAPI init code and 271 KB under TLS names. Consider a complete pooled HTTP-client operation owner after media, not per-record encryption callbacks. Preserve streaming, pool/IO lifetime, auth, cancellation and backpressure. |
| More schema/JSON deduplication | One schema parser in CAPI already; server has several candidates. Use actual retained size and per-declaration cost to select a concrete operation. |
| Backend capability census | Extend the successful disabled-CUDA approach to genuinely disabled implementations. Maintain allowlists for portable planners/stubs; a name containing `metal` or `cuda` is not proof of accidental GPU execution. |
| Full physical index split | The existing lifecycle probe is incomplete. Move complete query/mutation/persistence operations only after a measured family is selected. Per-posting/backend callbacks or a second backend implementation fail the boundary policy. |
| Safe identical-code folding | The pinned driver rejects `-Wl,--icf=safe`; no external LLD is installed locally. Keep this deferred pending a qualified external ELF linker. Verify address-significance/function-pointer semantics and all exports. Similar source is not proof of foldability. |
| ThinLTO | Tiny cross-library ELF probes passed with `.lto = .thin`, but the pinned compiler rejects Mach-O LTO (`LTO requires using LLD`; forcing LLD: `using LLD to link macho files is unsupported`). Do not integrate into the shared default product. Linux-only experiments still need production concurrency, cache and memory qualification. See [capability receipt](history/linker-capabilities-2026-11-01.md). |
| Full LTO | Avoid as the default solution to memory pressure: it recombines optimization work. |
| ReleaseSmall | Outside the current ReleaseFast product constraint. A separate product decision, not an equivalent comparison. |
| Fonts/tokenizer compression | Secondary: the measured large literals are already small relative to code. Measure startup/decompression memory and asset access before changing delivery. |
| Removing unwind tables | Not a free saving. Preserve crash diagnostics/unwinding requirements; function deduplication should reduce unwind data naturally. |
| Optional-feature SKUs/plugins | Could produce much smaller specialized products, but change the current full-feature/single-executable contract. Keep separate from this default-product plan. |

LLVM documents [ThinLTO's summary/index, parallel backend and cache model](https://clang.llvm.org/docs/ThinLTO.html)
and [LLD's code-folding design](https://lld.llvm.org/NewLLD.html).
Those mechanisms are candidates, not proof of support or benefit in the pinned
Zig driver and current link topology.

## Runtime memory guardrails

The principal memory goal above is compiler peak memory. Track runtime effects
separately: peak decoded/live raster bytes, PDF caches across page batches,
JSON DOM versus retained raw values, packed tensors, output ownership and
concurrent requests. Passing a raster directly can avoid an intermediate PNG
round trip; repeated reparsing, an extra full raster, or retaining an entire
request longer can negate that gain.

The map also shows repeated 256 KB zero-filled thread signal-stack regions and
about 1.46 MB of audio-recipe BSS. These are leads for a separate TLS/runtime
state study, not file-size savings or proof of resident RAM use. Preserve
allocator/IO domains, thread initialization and diagnostics before sharing them.

## Ordered implementation queue and decision gates

| Slice | Depends on | Reviewable deliverable | Required decision |
| --- | --- | --- | --- |
| P0 | Baseline | Product/map/timing/RSS collector and matched runner protocol | Evidence is comparable |
| P1a | P0 | Media/session ABI, owned result budget proof, compiled tests | Safe lifetime and accounting |
| P1b | P1a | Complete PDF consumer/cache migration | Old PDF roots disappear |
| P1c | P1b | Remaining image operations in the same owner | Complete duplication and size win |
| P2 | P0 | Whole lake execution prototype and publication contract | API shrink beats receiver growth |
| P3a | P0 | Public mutation values and separate private owning state | No implementation imports in public values |
| P3b | P3a | One shared complete codec family and migrated callers | Public CAPI stays small; net emitted work falls |
| P4 | P3a, measured borrow inventory | One opaque native-operation family per increment | Private-layout invalidation is removed safely |
| P5a/P5b | P0 declaration/pass attribution | Resource frame, then complete mutation phase migrations | Controlled compiler improvement, exact commit behavior |
| P6 | P0, real kernel benchmark | Bounded Q8K kernel experiment; optional compiled owner | Compiler gain without serving regression |
| P7 | Every slice | Actual provider/consumer cache matrix and cache/provenance rules | Unchanged owners remain cached |

The first product milestone is P1a–P1c, with P2 as a separate server-focused cut.
Working size goals are to recover the server's pre-query-owner baseline and
bring the current 80.03 MB C library below 78 MB. These are experiment targets,
not predicted savings. A prerequisite may be retained only when it is bounded
and names its immediate follow-up; a completed migration must show a net win.

For cold builds, retain the existing normal-runner objective of at most 380
seconds per critical unit (prefer below 350). Intermediate candidates need a
repeatable 30–45 second reduction and a credible next cut. For the 15 GB
developer-machine goal, investigate a CPU-profile budget of at most 6 GiB per
compiler and 12 GiB aggregate with no swap dependency. Those proposed budgets
need measurements; GPU profiles and memory reservations change only after
their own qualification.

Target an order-of-magnitude improvement for specifically measured media/parser
implementation-edit rebuilds. Do not generalize it to every storage edit, cold
build, binary size, or runtime memory. No current evidence supports a 10x
full-feature binary reduction.

Every enabled slice must pass the existing architecture/import/source/licensing
gates; native ownership/callback/error and ABI mismatch suites; public CAPI
exports/linkage; storage/query/CLI/HTTP smoke; and relevant LSM, simulation,
platform and inference tests. Examples of current production targets are
`antfly-storage-owner-test`, `opaque-query-owner-test`, `capi-smoke`,
`linked-inference-abi-integration-test`, and `cli-smoke-test`.

Never claim a peak-memory win from a smaller single compiler while aggregate
normal-concurrency memory grows. Never claim a cold-build win from cache reuse,
or a product win from a test caller. Preserve the existing CAPI size gate and
record incremental and cumulative executable deltas. Keep each experiment
reversible, retain its exact baseline/results, and revert a failed ownership cut
rather than relaxing behavior, runtime budgets or acceptance thresholds.
