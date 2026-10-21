# Logical fields and query semantics

## Decision

Keep the existing Bleve-shaped DSL and boolean composition. Add explicit logical
value operations rather than making callers choose `._keyword`, `._substring`,
`._2gram`, `._3gram`, or prefix companion names. The server owns representation
selection, candidate generation and verification. No grep-specific API or client
field-capability routing protocol is introduced.

The compatibility boundary is semantic: `match` remains analyzed full text;
`term`, `prefix`, and `regexp` remain indexed-term operations. They do not acquire
whole-value, autocomplete, or original-content semantics implicitly. Explicit
physical-field queries remain supported as advanced controls. Resolve companions
through declared source associations, never by stripping an underscore suffix.
Reserved metadata such as `_id`, query-computed `_score`, and hierarchy traversal
keys keep their explicit synthetic semantics; they are not string companions.

## Public contract

New leaf forms compose wherever text queries compose:

```json
{"equals":"Distributed Systems","field":"title"}
{"contains":"tributed Sys","field":"title","case_sensitive":true}
{"matches":"Distributed\\s+Systems","field":"title"}
{"autocomplete":"distributed sy","field":"title"}
```

`equals`, `contains`, and `matches` operate on original string values. Missing,
null and non-string values do not match. Arrays match if any string element
matches; matching never crosses element boundaries. Dotted paths traverse
objects and arrays. No punctuation stripping, whitespace folding, stemming or
Unicode normalization is implicit. Case sensitivity defaults to true; false
requests Unicode simple folding, not locale-specific or multi-character folding.
`contains` accepts empty strings; they match any present string. Regex search is
unanchored unless the pattern says otherwise; anchors apply to the whole string,
not independently to each line. The regex dialect is native Unicode regular
search, distinct from the term-dictionary automaton and from a claim of full
PCRE2 compatibility. Invalid or unsupported syntax fails explicitly.

`autocomplete` is analyzed bool-prefix matching using declared search-as-you-type
representations and their configured analyzers. It preserves the existing
`multi_match` `bool_prefix` semantics: completed terms and the final token
prefix are alternatives, with matching evidence contributing to relevance.
For example, `distributed sy` can match both `Distributed Systems` and
`distributed storage`; it is not an all-words constraint. Absence of a compatible mapping
is an error, not an implicit raw prefix query.

An independent `execution.value_verification` request policy controls permission to
scan and maximum documents/bytes verified. Defaults forbid unindexed fallback;
limits are per shard request, count repeated source loads, and fail explicitly
rather than returning successful partial results. Existing timeout and
cancellation apply. Regex execution additionally charges input bytes multiplied
by compiled states against `max_regex_work` (default 100 million), before matching;
compilation/scratch is bounded per matcher. Cluster-wide accounting is a separate coordinator concern;
per-shard limits must not be described as global budgets.

## Planning and execution

A representation can establish exact membership only when its analyzer and
coverage preserve the requested semantics. Otherwise its postings can be used
only as a conservative superset, followed by verification of original values.
Never equate normalized substring terms with raw-value containment, or a
lowercased keyword term with case-sensitive whole-value equality.

Choose candidates only from declared compatible mappings. Do not guess suffixes
without checking the mapping, choose an arbitrary conflicting document schema,
or use a partially covered index as proof of absence. Short, punctuation-only,
non-provable regex, or otherwise unsafe patterns use a permitted scan or fail.
Candidate generation may be absent without changing matching semantics.

Verification participates in native boolean membership through request-owned
producers. It occurs before counts, sort windows, cursor boundaries, aggregations,
and limits. In particular, negation must subtract verified matches, never a
candidate superset. Source and index identities must refer to the same retained
read cut. Native primary-backed plans require a complete primary projection with no active
backfill and an applied watermark through the text-relevant replay tail.
Retained remote checkpoints must be read through their immutable storage
capability, not through host filesystem paths.
Use the existing replay hints to distinguish unrelated projection work from
missing text updates; a global sequence gap alone is not evidence of lag.
An empty replicated batch can advance even a text hint without an index commit.
A checkpoint gap is harmless only with proof that both the primary source and
pinned text projection are empty; source emptiness alone does not excuse stale
index rows or make boolean counts safe. Lagging or partial
projections return the existing retryable index-rebuilding error; declaration or
field presence alone never proves coverage.
Source I/O errors, cancellation and resource exhaustion propagate;
none may become an empty result. Source-less or incompatible providers must fail
closed rather than silently treating index payloads as complete originals.

Compile patterns once per request leaf, reuse span-only execution scratch, bypass
regex compilation for sensitive literals, intersect existing candidate membership
before source loads, and bound scratch and decoded-source lifetimes. Keep physical
index representations out of client configuration.

### Reusing exact verification work

Two to 64 direct original-value OR siblings stream through each loaded document
as one bounded execution cohort. Each predicate keeps independent membership,
boost, case mode and source path. Regex, equality and different-field clauses no
longer disable compatible literal groups in that cohort. AND/nested branches keep
their incoming-candidate scope; only cheap same-field literal cohorts speculate
across AND siblings. Indexed regex/equality siblings retain their own selective
candidate iterator instead of expanding to a broader sibling's source domain.
Groups of at least three case-sensitive
`contains` leaves on the same field use a bounded multi-output byte automaton;
construction admits at most 1 MiB of transition storage. Larger literal groups or
allocation refusal fall back to individual matchers within the cohort. Wholly
literal cohorts retain a direct automaton path, bypassing generic sibling dispatch.
Coverage ownership is resolved once per segment invocation, and already evaluated
siblings do not repeat deadline/cancellation checks inside the cohort loop.

The demanded predicate runs first. Optional siblings reuse that same parsed
source, under the same regex-work budget and cancellation/deadline control. A
sibling evaluation failure is unknown, never a negative fact: its own candidate
iterator must evaluate it normally if demanded. Eager traversal reaching a source
limit falls back to individual leaf semantics, preserving early matches. Successful
cohorts retain one shared coverage bitmap plus each leaf's positive matches;
partially evaluated documents retain only the facts actually established. Source
bodies are discarded after the document. This bounds working memory by a source
scratch buffer, matcher state and compact membership, rather than expanded
corpus size. Preparing another leaf after execution preserves existing coverage
before regrouping.

Separate execution components can additionally share a loader-scoped decoded
source owner bounded to 8 MiB and 4,096 entries. Single-leaf execution uses scratch;
cohort execution does not populate this body cache. Refusal preserves ordinary
exact verification. Document/byte budgets count actual source loads; regex work
counts actual evaluations, including optional successful sibling evaluations.
Verification snapshots segment tombstones before source I/O, excludes deleted
postings and jumps over deleted runs on fallback. Superseded slots do not consume
document/byte budgets.

Retained pagination may reuse verified positive **and negative** predicate facts
from an admitted immutable query cut. A trusted storage scope binds the physical
DB root, cut generation ID and document identity namespace. Entries additionally
bind the selected index, immutable segment ID/document count, logical field,
operation, literal/pattern, and case mode. Scores/boosts and output projections do
not affect predicate membership. Each entry distinguishes verified nonmatches
from documents never evaluated under an earlier candidate constraint; only known
members may bypass verification. Completing an unconstrained conservative
candidate evaluation also proves absence outside those candidates. That complete
segment membership can bypass future candidate expansion; a partial membership
can only be intersected with current candidates. Completeness is a scalar flag,
so a billion-document segment with one matching ID never allocates a billion-bit
coverage map. Completed facts cannot be displaced by a concurrent partial builder. Ordinary mutable DB searches cannot install a
retained scope, even through internal request fields.

This process-local cache retains ID bitmaps only: no source text, credentials,
read capability, snapshot lease, output, or client-visible authorization decision.
Every request still passes authorization, retained-cut validation/expiry, source
read admission, projection readiness, and its own candidate constraints. A changed
query or cut misses. Partial failed segment evaluations are never published.
Concurrent builders may discard one another's cached subsets without changing
results. Queue eviction, expiry and allocator refusal always fall back to exact
verification. Storage is bounded to 16 MiB of admitted allocations, including the dynamically sized hash directory,
with a 2 MiB entry ceiling; nothing is written to disk. The directory has no fixed entry-count cliff. Newly admitted facts enter a probationary queue; successful reuse promotes them to a protected queue. Pressure evicts unpinned probationary entries first, preventing a one-shot scan from replacing a warm working set. Hash lookups and queue updates take constant expected time. Expiry walks entries only when the earliest recorded deadline has passed. Exact-clone storage bounds
are checked before eviction. Oversized predicates cannot evict valid entries.
Readers pin immutable facts with reference-counted leases; expiry and eviction
release only directory ownership. Pinned retired entries continue consuming the
same budget until the last reader releases them. Cloning/populating bitmaps occurs
outside the directory mutex. Completed hits clone only the owned result bitmap; constrained hits clone the
smaller of incoming candidates and cached membership before intersecting.
Partial builders clone mutable facts outside the lock. If all capacity is pinned,
new admission falls back to verification without exceeding the memory bound.
Admission counts only reclaimable, unpinned entries as available eviction space;
a doomed insertion under pin pressure cannot flush unrelated useful entries.
Expired entries are reclaimed on subsequent cache access. Deployment restarts and owner moves may
lose reuse, but preserve correctness.

A declared lossless keyword representation can establish nonempty case-sensitive
whole-string equality without loading source. The proof requires complete, ready
primary coverage, agreed source associations across document schemas, string-only
source extraction, and an effective built-in keyword analyzer. Only the original
term postings participate; synonym expansion must never establish raw equality.
Other mappings retain original-value verification. Profiles identify this plan as
`lossless_keyword` and report zero source documents/bytes when no verification
was needed.

## Capabilities, sorting, aggregations and explanation

Field capabilities describes logical operations, semantics, and available
execution paths; it is not an instruction for every client to reproduce routing.
Physical emitted names and analyzer details remain diagnostic information. The
same runtime representation resolver should serve queries, capabilities, sort,
and aggregation planning. Coverage/readiness must be distinguished from declared
support. Schema identity is necessary before clients cache this information.

The public capability response has one entry per proven logical `field`, with
`kind`, `value_operations`, `sortable`, `sort_lifecycle_state`,
`sort_requires_scoring`, and nested `representations`. Source entries group all
schema-associated concrete views. Each representation carries its concrete
`field`, type, analyzer, schema/provenance, index-relative query modes, missing/null
policy, and coverage/readiness diagnostics. The duplicate public `logical_field`
and `emitted_name` properties are removed. Queries continue to use `field`.

Observed physical fields with unknown source associations use `kind: unassociated`,
leave the outer `field` absent, and retain concrete names only inside
`representations`. Pattern-only dynamic mappings use `kind: template`. These
entries advertise no original-value operations. They must not invent a JSON
source path from an emitted field name. `_id` uses `kind: reserved`; `_score` uses
`kind: computed`, has no stored index representation, and sets
`sort_requires_scoring: true`. Reserved/computed entries do not pretend to be JSON
source strings. Logical sort availability uses the same runtime doc-value resolver
as executable queries; ambiguous companions remain unsupported. Autocomplete is
advertised only when the shared root resolver identifies one compatible root.

The immutable definition cache owns the runtime schema used for resolution and
precomputed public groups under its existing byte budget. Observed coverage merges
remain conservative within each representation. Public grouping uses a hash index
and moves owned representation metadata rather than cloning it again; normal
status reads reuse cached groups. This changes the capability response contract:
SDKs and callers consuming flat type/analyzer/query-mode properties must read
`representations` instead. Query expressions and physical-field escape hatches
retain their contracts.

For example, a logical text source with one sortable keyword view is exposed as:

```json
{
  "field": "title",
  "kind": "source",
  "value_operations": ["equals", "contains", "matches"],
  "sortable": true,
  "sort_lifecycle_state": "declared",
  "sort_requires_scoring": false,
  "representations": [{
    "field": "title._keyword",
    "type": "keyword",
    "analyzer": "keyword",
    "query_modes": ["exact"],
    "sortable": true,
    "sort_lifecycle_state": "declared",
    "doc_value_coverage": "schema_declared",
    "queryability_state": "declared",
    "missing_null_policy": "missing_rejected",
    "provenance": "document_schema"
  }]
}
```

MCP raw-query discovery exposes the same `execution.value_verification` budgets and
`remote_snapshot` cursor control as REST. Sequential `hierarchy.children`
traversal accepts its retained snapshot token and rejects value-search policy,
because that mode performs no relevance/filter verification phase.
Declared named search-as-you-type roots retain their configured
analyzers; shorthand mappings may additionally resolve declared generated shingle
representations. Conflicting roots are rejected rather than selected arbitrarily.

Sort and aggregation requests should accept logical source fields and resolve
compatible typed/keyword doc values. Preserve existing ordering, null and
collation semantics. If multiple representations imply different orderings, use
an explicit schema default or reject ambiguity; do not silently select one.

Explain output should expose the requested operation, selected representation,
exact-versus-candidate status, verification obligation and fallback reason, along
with measured candidates/source bytes. Explain must use the executable plan,
not duplicate a separate routing implementation. Cost controls belong outside
the matching expression. Existing physical field access remains an escape hatch.

## antfly-grep

Use ordinary logical `contains` queries for provably necessary literals, followed
by the CLI's final matcher. Let Antfly choose companions and verify values; no
`content_index` configuration is necessary. Do not forward a regex unless the
server and CLI semantics are known compatible. Case, inversion, complete-file
counts, statistics, unrestricted decision search and unsupported encodings retain
exhaustive traversal where candidate pruning would change observable behavior.

## Validation

Compare indexed and permitted scan execution on the same original values,
including normalization false positives, Unicode, punctuation, arrays, empty and
missing values. Exercise boolean exclusion, cursor pages, exact counts and
aggregations with false positives before a page boundary. Cover incompatible or
missing mappings, exhausted budgets, invalid regex, cancelled reads, source-less
providers and concurrent revisions. Test that existing term/match/regexp and
explicit companion queries preserve their behavior.

Implementation status and remaining qualification must be stated separately
from this decision; this design is not evidence that every provider or optimized
execution path has been qualified.


## Implementation and qualification

The native primary-backed planner implements the operations above, shared
verification budgets and executable-plan profiling. Antfly-grep uses the ordinary
logical query contract, and generated Go, TypeScript, Python and native models
include the public types. Logical keyword sorting resolves the same source
associations at both HTTP admission and database execution.

The [qualification report](../../zig/pkg/antfly-grep/bench/results/README.md)
pins source and binary hashes and records local CLI, native-contract, SDK and
real-server HTTP checks. Qualification covers macOS and native one/three-shard
HTTP execution, including retained remote storage views. It does not establish
new throughput guarantees, every external provider's original-value support,
or Linux/Windows runtime qualification of these new server paths.


[Verification reuse qualification](../../zig/pkg/antfly-grep/bench/results/README.md)
pins the follow-up implementation and merged-main revision, final artifact hashes,
all workload records, and native/HTTP/SDK checks. It covers named autocomplete
mappings, unknown observed source associations, deleted-slot budgets, cache
isolation after writes, shared source decoding, literal fusion, and lossless
keyword proof. Its small shared-host Debug workload measures verification loads
and end-to-end latency; it establishes no production-scale RSS or platform parity.

Named detailed search-as-you-type representations use a plain lowercase query
analyzer, not the prefix-expanding index analyzer. A query for `distributed` must
not match `Dinosaur Bones` through the generated `di` token. Existing generated
shingle/root analyzers retain their configured semantics.

[Mixed streaming qualification](../../zig/pkg/antfly-grep/bench/results/README.md)
pins the merged source and artifacts, every trial, the initial slower literal
control and the final optimized comparison. It covers the one-pass 4,096-document
policy, both named autocomplete aliases, immutable pins across eviction/expiry,
non-destructive admission under pin pressure, and allocation-bounded selective
intersection. These shared-host Debug measurements are not a production memory,
cold-storage or platform-parity qualification.


### Named autocomplete root completeness

Named `search_as_you_type` roots index complete Unicode word tokens with lowercase normalization. The bounded 2–20 character edge-prefix representation is an accelerator, not the root dictionary. Root prefix expansion remains available for one-character words, long prefixes, and full tokens beyond the accelerator window. Query analysis never expands edge n-grams. Generated shorthand mappings retain their independently verified shingle and prefix companions.

Indexes built with the prior prefix-only named analyzer require a replacement generation. The physical generation's existing text-projection provenance now incorporates named-autocomplete index semantics revision 2. This is independent of public schema versions and the provenance envelope codec version. Both `search_as_you_type` and `search_as_you_type_analyzer` aliases participate, whether selected by index configuration, exact mappings, dynamic templates or document-schema variants. Unaffected analyzers retain their historical witness.

Opening a nonempty generation with the old witness fails closed with `TextProjectionProvenanceMismatch`, preserving the configured index and its load failure for operator inspection. The old witness cannot distinguish prefix-only postings from complete-word postings produced before this revision was introduced, so both require replacement when they use these named analyzers. It cannot accept new writes under a different normalization or serve partially upgraded results. The normal explicit index-repair/rebuild flow builds a private replacement and publishes it through the existing activation fences. The public full-text repair entry point is `POST /tables/{table}/repair/run` with `{"target":"index","index":"<index-name>","force":true}`; continuation passes omit `force` so they converge the same replacement rather than repeatedly creating generations. Opening a physically empty writable generation can install the current witness; read-only opens cannot relabel it. Changing query normalization or updating metadata alone cannot recreate missing terms. Upgrade tests create actual prefix-only postings, verify refusal without changing schema/configuration identity, and admit a freshly built replacement.

### Bounded verification dataflow

Retained predicate facts use a hash directory and intrusive hot/cold queues within
the existing 16 MiB process allocation budget. New facts enter the cold queue;
reuse promotes them. Directory metadata, immutable bitmap blocks, reader pins
and unpublished builders all count against that budget. There is no fixed entry
count. Lookup does not copy bitmaps, and construction occurs outside the directory
lock. An indexed min-heap owns one deadline node per resident entry, sharing the same byte budget. Removal on expiry, replacement and eviction updates both indexes; there are no accumulated expiry tombstones. Lookup checks only the earliest deadline and expiry costs O(log N) per removed entry, rather than scanning surviving entries for each staggered deadline. Expiry releases the empty directory and heap too. Tests interleave 4,096 distinct expirations with live lookups, bound heap comparisons, and keep a reader pin across removal. This is optional memory-only
acceleration; it never persists document bodies or bypasses read authorization,
snapshot validation or revision isolation.

Indexed producers read original values through the existing multi-read callback
under the same read generation. A query-local controller measures required provider reads in scalar and power-of-two size classes from two through 64 rows. It starts with a bounded 16-row window, samples each feasible class using real work, then selects the lowest observed read time per row. Periodic bounded probes and decayed samples allow adjustment; there are no duplicate reads or synthetic calibration requests. Measurements exclude parsing and output. Scalar fallback trains the scalar arm; a truncated batch updates byte estimates without training the requested full class or completing its exploration. The controller targets roughly 256 KiB of source payload using the learned average and rounds the byte-derived row cap up to a power-of-two class (below 512 KiB of estimated payload) and carries its bounded state across segments. It may choose scalar reads when batching is more expensive. Fixed-size benchmark controls bypass learning. Window allocations have an 8 MiB ceiling and may use at most a quarter
of provable reusable allocator headroom. Unknown, monotonic and externally
reserved budgets retain scalar reads. Failed optional windows shrink, release
their memory and restore document reservations before scalar fallback. Deleted,
already known and already covered candidates do not enter the window. Successful
speculative reads count against shared document/byte allowances even when a
consumer later stops early. Primary scans without a text index retain their
existing scalar storage traversal.

The compiled-engine boundary canonicalizes only the exact, unwrapped process
libc allocator: separate compilation units have distinct allocator-vtable
addresses despite sharing malloc/free. Quota, arena, tracking and custom
allocators pass through unchanged. Optional admission must recognize a backing
allocator without stripping constraints.

Large originals use a query-local field-path trie and sparse JSON projection.
The parser validates the complete input, including duplicate keys and unused
values, while materializing only selected string paths and their array/object
structure. Plain selected ASCII strings use a vector-assisted delimiter/validation scan and borrow the original bytes. Skipped value strings validate escapes, UTF-8 and UTF-16 surrogate pairs directly without generating a temporary token for every fragment. These adapters are confined to complete-input value positions; keys, diagnostics, streaming modes and escaped selected-string decoding retain the standard scanner. Generated Unicode/escape/truncation cases and byte mutations compare acceptance and structural continuation with that scanner. Unselected values use iterative validation, so deeply nested unused data does not force recursive DOM construction. Tiny documents use the standard
parser directly; excessive selected depth or unavailable compilation retains
the ordinary full-DOM path. Sparse projections never enter the full-document
source cache. Matcher semantics and work limits remain authoritative.

Profile metrics with `verification_metrics_version: 1` expose source read/parse
time, batch/scalar read counts, projected-source counts, batch refusals and peak
individual-window allocation. Merging sums work and time, and takes the maximum
window peak; it does not claim simultaneous process memory. Legacy components
make the merged metric version zero rather than suggesting complete coverage.
These counters make batching and projection observable through actual HTTP
requests, rather than relying only on callback fixtures.

[Scalable verification qualification](../../zig/pkg/antfly-grep/bench/results/README.md)
records native/HTTP checks, the reproducible optimized baseline, per-run CPU,
first-result latency, throughput, memory and batching/projection/window controls.
It retains the initial observations and benchmark-settling correction. Linux uses
a native ARM64 VM with a two-CPU quota; its selected-string control remains mixed.
These are warm-cache observations, not independent physical Linux or cold-storage
qualification.


### Remaining storage work and performance qualification

Logical verification uses the document store's revision-bound projection authority, upgraded from root-only `DPD1` to bounded object-path `DPD2`. The header remains bound to primary length, kind and SHA-256 digest; current mutation revision is checked independently in the same authenticated source snapshot. Each field keeps its decoded name, extent and digest. Parent index and value kind occupy previously unused high offset bits, retaining the existing 50-byte per-entry overhead. Version 2 supports source offsets below 2^53 bytes, at most 512 nodes, at most 32 KiB of encoded metadata and at most one sixteenth of original size.

The directory stores root fields first, then breadth-first sibling groups with strictly sorted decoded names. Read-time validation checks parent ordering, containment, nonempty ranges, group boundaries and a maximum hierarchy depth of 128. SQL root lookup remains compatible, including literal dotted field names. Logical dot paths descend through proven objects, so `grep.content` can avoid unrelated `grep.regions` metadata. Literal dotted keys are distinct from nested paths.

Construction strictly validates the complete original, including duplicate keys. Object-path discovery has an explicit linear work allowance of three additional source-sized coverage units; reservations are not refunded for discarded work. Arrays remain opaque, and object traversal stops at the hierarchy, node, work or encoded-size ceiling. An object whose complete child directory cannot fit becomes an opaque subtree. This preserves exact fallback without a document-size-times-region-count loop or an unbounded recursive index. Small deep branches can remain indexed when the work allowance permits them.

A query-owned immutable source reader pins schema/key encoding and a native primary snapshot across windows. Native snapshot ownership shares underlying values. Required reads first establish that originals are large enough to benefit; small documents and ordinal storage retain their existing loaders. Predicate paths select the narrowest proven nodes, with arrays and opaque subtrees kept intact. Overlapping parent/child selections are pruned. Output reconstructs only the necessary object scaffolding, preserving array boundaries and source strings.

Metadata and payload requests are sorted and deduplicated in windows of at most 64 documents. Every selected range is checked against its stored digest before use. Selection must occupy less than half the original before range projection is admitted. Authorization, cancellation/deadline checks and allocator admission retain their existing ownership. Work budgets charge original bytes and document count, not the smaller projected payload. Projected sources never enter the complete-document cache. `stored_field_sources` counts authority-backed reads; `projected_sources` includes both stored projection and sparse JSON parsing. Older v1 metric producers contribute zero to the supplementary counter.

`DPD1` remains readable as a coarse root authority. Missing, stale or old authority uses conservative reads and wakes the existing bounded backfill once per query on writable owners. Read-only retained cuts keep their immutable fallback. Current unsupported-value markers do not repeatedly request repair. New metadata replaces the same disposable authority key; no second cache namespace or new primary format is introduced. Authority records persist alongside stored documents; full sources remain in their existing primary records. Query windows and projected JSON are memory-only. Older binaries reject the new magic and use their existing full-read path; mixed-version maintenance may rebuild an older cache representation, affecting performance rather than source correctness.

Adaptive I/O measures identity/window preparation, allocation and provider work, excluding matching/output. Scalar fallbacks train the scalar arm; truncated batches do not train a full arm or finish its exploration. Representation changes restart learning while preserving allocator ceilings and excluding one-time snapshot setup from steady-state samples. Original-size prediction stays separate from payload-size prediction. Read-window arenas retain at most 512 KiB between windows, within the existing 8 MiB live-allocation ceiling and enclosing request admission.

Stored-field admission also observes the required full-source parse. The sparse parser sums selected decoded string bytes once, with overlapping predicate paths deduplicated by its existing trie. This is a lower bound on the original selected extents: escaping can make a value larger on disk, never smaller. An exponentially weighted selected-byte fraction avoids opening a stored-field reader when selected values dominate the source. Every subsequent full parse updates that estimate, allowing heterogeneous inputs to become eligible again without another scan or a persistent blacklist.

When directory inspection finds no projected rows, optional inspections back off for 32 through 1,024 source rows, with at most one batch of overshoot. A successful projection resets that backoff. Once a reader is pinned, complete reads during backoff, small-document windows, and root-wide predicates continue through the same transaction; they do not return to a live provider. Complete-read mode bypasses authority inspection and does not request repair just because metadata was intentionally skipped. Original-byte work charges, cancellation, validation and complete-document cache ownership remain the same. The policy uses fixed query-local counters and does not add persisted state.

Opaque arrays and objects remain a conservative cost boundary: queries can match through them but must read the complete selected subtree. Finer array-element or typed-column layouts require equivalent revision, normalization, array and work-budget guarantees. Independent physical Linux and device-cold qualification remain distinct from process restarts or page-cache advice.

The value-string microbenchmark is reproducible with `zig/lib/json/bench_value_strings.zig` and alternates standard/native order over nine samples. Its scanner-only gains must not be presented as complete-query gains. End-to-end qualification must compare optimized binaries with source hashes and identical corpora, include default/scalar/fixed-size controls, and retain CPU, first-result latency, memory and all raw trials. Warm ARM64 Linux VM measurements remain distinct from independent hardware or cold-storage qualification.


The follow-up benchmark protocol records three one-second pre-ingestion idle CPU
samples and one untimed query, then requires three full quiet windows relative
to median idle CPU plus five percent of one core and one accounting tick. Each
of the nine timed queries must still verify every document. Warmup, idle samples,
settling windows and host load accompany the raw trials. Idle calibration is
independent of measured query results: native control-plane maintenance can
otherwise make an absolute five-percent threshold impossible on a healthy
server. Earlier variable trials and the rejected absolute-threshold observation
remain separate from qualified comparisons.


[Verification-cost qualification](../../zig/pkg/antfly-grep/bench/results/README.md)
pins the final artifacts and source, both real process-upgrade/public-repair
checks, native contracts and 14 CLI/server HTTP integrations per platform. It
retains 16 primary benchmark configurations, a paired Linux comparison and all
initial observations. The macOS selected-string and escaped-array medians improve
about 2.1× and 2.8×; tiny sources are effectively unchanged. Shared-VM Linux
comparisons vary substantially in absolute time, including an unchanged decoder
control. Fixed windows sometimes outperform the default policy, and sampled
process RSS is not the window allocation bound. Default-window peaks below 1 MiB
are higher than the preserved baseline's roughly 0.5 MiB, within the unchanged
8 MiB ceiling. These results support the
measured implementation on the recorded corpora; physical Linux, cold-storage
and fleet-wide concurrency qualification remain separate work.


### Source-reader performance qualification

`zig/pkg/antfly-grep/bench/logical_sources.py` compares pinned baseline/candidate artifacts, alternating execution order across rounds and retaining every HTTP trial. It runs a five-corpus latency/CPU/memory/window suite, including a selected-dominant source that declines projection and nested `grep.content` data, and a simultaneous-client suite with one/four clients by default. Each concurrent configuration restarts the server, records its first cold-process query, then records nine synchronized request groups with per-request latency, first-byte time, original verification work, group CPU and endpoint memory observations. Restart tests also verify projected reads after source updates. Endpoint RSS samples are not an allocation bound or peak RSS measurement.

Run the same driver on native hosts or inside CPU/memory constrained containers. Reports preserve the binary/driver/fixture hashes, corpus digest, controls and platform/quota information. `ANTFLY_BENCH_VALUE_SOURCE_FULL_READ=1` is a qualification-only control for the existing complete-source reader. Scalar and fixed batch controls remain independent. A process restart does not clear operating-system or device caches; these fixtures never claim cold-storage or independent physical-hardware results.


On Linux, `--advise-file-cache` adds `fsync` and `POSIX_FADV_DONTNEED` advice only for regular files in the isolated fixture's own temporary root while its server is stopped. Symlinks and caller-supplied directories are ineligible. Each restart records advised file counts/bytes and process I/O counters before/after the first query and each warm group, including startup I/O. The advice is not a guarantee of device-cache eviction, and startup can reload pages. This supplies reproducible cache controls without a global cache-drop operation or a claim of physical cold-storage qualification.

The [source-reader qualification](../../zig/pkg/antfly-grep/bench/results/README.md)
records 60 final configurations over pinned `cf71262234` artifacts: two reversed
rounds of default, full-original, scalar and fixed-16/32/64 controls on macOS and
a one-CPU/1 GiB native ARM64 Linux VM, followed by a two-CPU concurrency repeat.
Every timed verification covers its complete corpus with fresh predicate facts.
All raw latency, first-byte, process CPU, window, memory and I/O observations are
retained, along with preceding diagnostics and cross-version artifact identities.

Nested `grep.content` verification falls from 107.47 to 8.60 ms on macOS and
105.19 to 10.52 ms in the one-CPU VM; unused escaped arrays improve about 2.2×
and 2.1×. The initial dominant-value regression motivated required-parse byte
feedback and bounded metadata backoff. The final dominant-value path tracks the
same-binary full-reader control. Small cases remain mixed against the preserved
baseline, with substantial reverse-round idle/parser variation also visible in
the full-reader control. Default admitted windows stay below 0.8 MiB in these
corpora; retained capacity stays capped at 512 KiB and live admission at 8 MiB.
Sampled RSS is not a peak allocation bound or a uniform memory guarantee.

Both optimized servers pass 18 real HTTP/CLI integration and upgrade cases,
including DPD1 migration without re-ingestion, restart, old-reader rollback and
write, and re-upgrade. The full Linux correctness suite uses 4 GiB because its
expanded substring-index fixture exceeds smaller automatic build budgets; the
preserved baseline exhibits the same 1 GiB limitation. The timed query matrix
uses 1 GiB. The shared-host VM and fixture-only file advice remain distinct from
independent physical Linux, device-cold storage and Windows runtime qualification.

### Bounded substring token production and source bootstrap

The substring indexing pipeline can produce many more tokens than source words.
Index construction consumes its `unicode_words → lowercase → shingle(1..2,
separator=none) → suffix(2..32)` pipeline through a borrowed-token producer.
The producer recognizes the complete configured pipeline rather than an analyzer
name. Other pipelines retain the owned-token implementation. Highlight analysis
continues to use its source-offset-aware path.

The producer preserves ordered unigram-then-bigram emissions with two sequential
passes over the source. It retains a fixed normalized window, borrows each term
until the next emission, and interns each distinct term once. Frequencies,
positions, repeated-field position offsets, and field norms remain identical to
the materialized pipeline. The document accumulator holds the first two positions
inline; dictionary storage and larger vectors grow through a reclaimable allocator
that preserves the existing quota chain. Their scoped owner releases them after
the posting builder copies terms and positions. Keys stay document-arena owned.
This avoids retained arena growth capacities without introducing a second term
map or allocating a heap vector for every rare term. Inputs below 128 bytes keep
the existing owned-token path and small unique-token fast path. Streaming removes
the expanded token array and duplicate term allocations, not necessary postings.
Allocation failure aborts the document without publishing partial postings. Ordered-token differentials include generated Unicode/cap
boundaries, and postings differentials include repeated field contributions.

Logical source verification first reads a two-document bootstrap window to
establish source sizes before the normal adaptive reader explores its batch
sizes. This bounds initial complete-body reads without probing optional metadata
for every small document. Opening a projected reader retains the established
bootstrap and restarts only steady-state cost learning. The first required payload
sample seeds the byte estimate directly rather than blending with a placeholder;
representation changes also seed payload size while excluding one-time snapshot
setup from timing arms. A buffered row has no
provider read and supplies no I/O sample, so it does not read the monotonic clock.
Original-byte/document budgets, pinned-reader fallback, and memory ceilings are
unchanged. Fixed-window/scalar benchmark controls continue to bypass adaptation.

Performance comparisons must pin baseline and candidate after the same main
merge, alternate run order, retain raw samples, and separate server verification
from local CLI matching. The allocation/throughput benchmark is an opt-in native
introducer test; it rotates the pre-stream owned accumulator, the new owned
control and the new stream across repetitive, source-like and distinct-term
corpora. Complete postings digests verify norms, frequencies and positions.
Streaming production time is charged with `term_accum_ns`; `analyzer_ns` measures
owned-token analysis, so compare complete build CPU rather than treating phase
movement as eliminated work. Server qualification uses the existing scalable and
concurrent HTTP fixtures. Neither a process restart nor fixture-file cache advice establishes
device-cold behavior or performance on independent Linux hardware.

The [substring/startup qualification](../../zig/pkg/antfly-grep/bench/results/README.md)
pins the production implementation at `34091b088b` after merging main
`dd49f55102`, with a baseline built after that same merge. Both optimized servers
pass all 18 HTTP integration/upgrade cases; the Linux server now passes within
1 GiB, superseding the preceding expanded-substring fixture's 4 GiB qualification.
The component benchmark verifies identical complete postings across 81 rotated
trials per platform and separates construction allocation/time from whole-server
batch-write CPU and teardown. The portable `introducer-test-build` target installs
the implementation test owner after import binding, allowing identical tests to
run on native Linux without executing foreign binaries during cross-compilation.

Streaming cuts tracked component allocation from roughly 101–219 MiB to 2–23 MiB
on the three 180 KiB corpora. Throughput is mixed: the distinct-word Linux case
takes about 26% longer. In the paired macOS server comparison, nested-source
verification changes from 9.15 to 8.27 ms and its largest observed window from
776 to 219 KiB; small/full-selected and concurrent cases have higher medians.
The report retains those regressions, all raw trials, ingestion CPU and sampled
process memory. Necessary postings, verification, and provider I/O remain real
costs. The bounded design does not promise zero work or uniform throughput gains.

### Canonical execution controls and read identity

Logical intent stays in `query`: `equals`, `contains` and `matches` verify
original values, while `autocomplete` performs analyzed bool-prefix matching.
The execution envelope governs admitted work without changing those meanings:

```json
{
  "query": {"contains": "needle", "field": "content"},
  "execution": {
    "value_verification": {
      "allow_scan": true,
      "max_documents": 100000,
      "max_bytes": 67108864,
      "max_regex_work": 100000000
    }
  },
  "profile": true
}
```

`profile.value_verification` describes measured work, selected representations,
source reads/parsing and allocation admission. The typed OpenAPI profile includes
these counters; aggregate window bytes are a maximum, not summed process memory.
Limits remain per shard and exhaustion fails explicitly. Omitting the envelope
retains conservative defaults; it never grants scan permission implicitly.
The CLI's explicit scan policy and connection configuration use the same envelope.

`read_requirements` governs minimum freshness, with `after_write` carrying a
write receipt. `remote_snapshot` instead identifies the exact retained read for
pagination. Both concepts remain distinct. Sequential `hierarchy.children`
accepts a retained snapshot token alongside its revision-bound position cursor;
it rejects value-verification execution controls because it has no matching phase.
The removed public `lake_read`, SQL `lake_visibility` and
`value_search_execution` options are rejected rather than silently ignored.
Internal lake read capabilities and storage visibility types remain implementation
contracts, not alternative public request spellings.

Keep `autocomplete` as the public intent-level name: it selects documents, not
suggested query strings. [MongoDB Search](https://www.mongodb.com/docs/search/query/operators-collectors/autocomplete/)
uses the same intent name. Elasticsearch exposes
[`match_bool_prefix`](https://www.elastic.co/docs/reference/query-languages/query-dsl/query-dsl-match-bool-prefix-query)
and recommends querying root/shingle representations for
[`search_as_you_type`](https://www.elastic.co/docs/reference/elasticsearch/mapping-reference/search-as-you-type).
Antfly resolves those representations from the logical field; callers do not
need to reproduce that physical query construction.

### Bounded exploration and immutable membership views

Source-reader calibration explores coarse windows before finer classes. Each
batch class needs one required-read sample, while scalar reads use three;
clipped and previously sampled classes are skipped. With stable row caps and completed reads, calibration finishes
within ten provider calls, including the two-row bootstrap. Refused or truncated
reads do not complete untested timing arms. The bootstrap seeds
payload size but contributes no steady-state timing. Later bounded probes still
adapt to changing costs, and allocation refusal continues to shrink admission.
This avoids sixteen scalar calls per exploration class on every new execution.
Fixed-window and scalar qualification controls remain independent.

Borrowed substring terms are packed into document-owned key slabs. Hash-table
entries retain stable slices, while dictionary/vector scratch remains reclaimable
and quota-bound. Single-word sources skip the otherwise useless shingle pass.
Postings digests and allocation-failure tests apply to the packed path as well.

A complete predicate-cache entry can lend an immutable bitmap through an explicit
reference-counted lease. Direct count-only producer filters consume cardinality directly and
subtract current deletions through a compressed intersection count. They allocate
no result bitmap, read no original source and release the pin even when the cache
entry expires. Mutable and partial results keep the owned-bitmap path. Tests use
an allocator that admits zero bytes, positive deletions and propagated errors.

Cross-snapshot reuse requires a validated physical owner's source/publication
proof, covering its lifetime nonce, primary revision, schema/index publication,
replay progress and namespace. The cut manifest is validated before using that
proof. Missing or legacy proofs and lazy remote readers retain snapshot-scoped
keys. A segment ID alone is insufficient: unindexed source values can change
without the term representation proving that change. Any source/publication
change invalidates this whole-generation proof. Reusing unrelated segments across
such a change requires separate, maintained per-segment source-binding
certificates; this change does not infer those certificates from segment IDs.


The [incremental execution qualification](../../zig/pkg/antfly-grep/bench/results/README.md)
retains the corresponding native/VM artifacts and alternating before/after
measurements. Direct-count tests establish zero result-bitmap allocation;
complete HTTP requests retain parsing, authorization and snapshot-validation
costs. Timing improvements vary by input and platform. The full ingestion stress
case also reaches the default per-index build allowance on small nodes despite
lower token-construction allocation, so component memory measurements must not
be presented as end-to-end ingestion admission guarantees.

### Query-union preservation and bounded repeated execution

Python query deserialization must reject unknown keys for closed query variants
before trying another union member. An all-optional range model must not accept
and erase a Boolean filter. The generator template enforces this only for query
models and the Boolean body; response models and opaque receipts keep their
existing forward-compatible decoding behavior. Round-trip coverage includes all
query variants in scoring, filter and exclusion positions, and singleton/array
clauses nested under each Boolean operator. This is dispatch validation, not a
replacement for server-side semantic validation.

Complete retained predicate facts cache their matched cardinality once at
publication, outside the cache lock. A borrowed direct count with no current
deletions consumes that number without rescanning bitmap words. Cardinality is
part of the immutable lease and remains valid across expiry while pinned.

These facts also support bounded Boolean count execution. A stack-resident postfix program holds at most 64 immutable bitmap leases and
128 instructions, with a nesting limit of 32. It merges nonzero word streams,
uses bit-sliced counters for minimum-should-match, and applies current deletions
before counting. Binary intersections, unions and differences use compressed
container kernels directly. Cache misses, unsupported leaves and oversized
expressions release all acquired leases and fall back to the existing executor.
No successful result is approximated. Controls apply during compilation and
execution; deletion masks remain protected during the synchronous count.

Source-reader timing hints are separate from predicate facts. A fixed 64-entry
memory-only table retains sufficiently sampled provider costs for at most one
minute, partitioned by source namespace, provider and original/projected reader.
A new request still measures its own bounded bootstrap payload, inherits no work
counters, obeys allocator-imposed caps, and keeps periodic adaptation. Hints
contain no documents, result IDs, authorization or snapshot capabilities. They
influence batching only; stale or colliding hints cannot establish membership.

Large physical text fields may spill postings before the surrounding document
finishes. Unique field contributions release their temporary term keys between
representations. Repeated physical fields (including multiple `_all`
contributions) use reusable sorted indices, ordered by field name and original
contribution position. Discovery costs O(fields log fields); each group retains
its original token offsets and combined norm, releases its analysis scratch, and
can spill before the next physical field. Splitting a repeated field into
independent document postings would corrupt its positions and is forbidden.
Field maps and position vectors receive the build's reclaiming allocator
explicitly; only packed copied term bytes use the document region. Opaque
allocator wrappers do not change scratch ownership. Both native publication
preparation and direct sink builds enable the same private postings spool.
Publication preparation keeps its encoded result allocator separate from
transient construction, releases scratch before publication, and retains the
existing producer-credit ownership of finished segment bytes. Failure paths
release unadopted encoded results as well as private runs. A native preparation
regression exercises the path used by ordinary derived writes under a 128 MiB
build allowance; testing only direct sink construction does not cover it.
Fresh large dictionaries reserve capacity once, and terms with
a single occurrence and at most one position stay inline in a tagged posting
accumulator. Promotion allocates owned arrays only when another occurrence
arrives; accessors derive slices after hash-table movement. General mergers keep
owned arrays and their bounded capacity policy. Serialization and allocation
failure tests cover both representations. Private posting runs remain temporary
disk scratch; this adds no persistent query-result cache.

Two distinct ownership changes remain necessary for the broader cost target:

- **Request-wide verification budgets:** coordinator-owned, non-duplicable work
  grants must cover every participating shard, retry, graph expansion and query
  phase. A timed-out or ambiguously completed attempt cannot have its grant
  refunded as though it did no work. Reallocation needs explicit attempt
  ownership, cancellation and exhaustion tests. Dividing a limit by shard count
  alone is not an elastic global budget, and rejecting an oversized aggregate
  after execution does not bound work already performed. Current public limits
  remain per shard request; no request-wide guarantee is advertised.
- **Reuse across unrelated writes:** publication must maintain segment/source
  binding certificates that cover every original value a predicate may inspect,
  including unindexed field updates. Schema/analyzer changes, source replacement,
  owner restart, compaction, repair and deletion handling need explicit fences.
  A stable segment ID or unchanged analyzed terms alone is insufficient. Current
  whole-generation proofs remain authoritative until these certificates exist.

The cost target is absence of redundant reads and result allocation on proven
warm paths. Initial indexing, changed input, authorization, snapshot validation,
network transfer and requested output retain physical costs. Component allocation
bounds and isolated timing improvements are not complete-request guarantees.


### Bounded membership maintenance and qualification artifacts

A retained-membership lookup removes at most eight expired heap entries and, if
necessary, the requested expired key. It never treats an uncollected expired
entry as a hit. Directory ownership moves to an intrusive retired list while the
mutex is held; releasing blocks, reader ownership, and empty directory storage
happens after unlocking. Outstanding leases remain charged to the same byte
budget until their last release. Shutdown drains all remaining ownership.
Optional cache admission examines at most 64 LRU nodes per preflight or
eviction and limits eviction attempts to 64; pressure can refuse acceleration
without changing query results.
Regressions exercise an expiry burst with a live key and a pinned expired key,
check heap work bounds, and instrument reclamation outside the mutex.

Raw benchmark trials, profiler samples and exploratory reports belong in ignored
`output/` directories or downloadable CI artifacts. Tracked qualification
summaries retain versions, artifact and corpus hashes, commands, measurements and
limitations. The grep benchmark archive manifest names every removed historical
report with its original Git blob and SHA-256 digest. Its restoration command
checks original bytes. This removes checkout clutter without rewriting history
or removing the differential, parser and dictionary fixtures used by tests.


Public canonical Boolean lowering preserves an explicit
`minimum_should_match`, including zero and thresholds greater than one. Required
non-scoring filters affect omission defaults, but cannot erase an explicit
positive threshold. Public request parsing and internal worker serialization
must preserve the same requirement; exercising only a nested DSL parser is
insufficient coverage. Real-server retained-count tests use one and three shards
and combine conjunction, disjunction, exclusion, nesting and threshold queries.


The borrowed Boolean word executor evaluates the expression with all inputs
empty to determine its default value outside the input union. A false default
counts matching bits; a true default starts from the live-document cardinality
and subtracts only exceptions within that union. Neither conjunctions nor
complements walk absent document-number words. One-input expressions reduce to
constants, the input, or its complement and reuse the publication-time
cardinality, with compressed deletion intersections when needed. General nested
negations retain sparse word execution. Regressions cover the maximum u32
document domain, clipped final words and deletion IDs outside the domain.
