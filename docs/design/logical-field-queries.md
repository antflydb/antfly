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

An independent `value_search_execution` request policy controls permission to
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

Three to 64 compatible case-sensitive `contains` leaves on the same source field
can use a bounded multi-output byte automaton. It scans each source string once
and retains each leaf's identity, boolean membership and boost, including
independent matches across array elements. Construction admits at most 1 MiB of
transition storage; larger or incompatible groups use ordinary matchers. Eager
traversal that reaches a source limit falls back to the requested leaf, preserving
its existing early-match behavior.

Other multi-leaf requests share a bounded decoded-source owner. A document is loaded
and decoded once while it fits the owner's 8 MiB allocation budget and 4,096-entry
ceiling. The owner is tied to the stored-document loader, contains only original
JSON, and ends with the request. Single-leaf queries keep their streaming scratch
path. Capacity refusal uses ordinary per-document scratch; it never changes
membership or permits incomplete results. Document and byte budgets charge actual
source loads, including repeats after cache refusal; regex work is still charged
for each predicate evaluation. Verification snapshots the segment tombstone mask
before source I/O, excludes deleted postings, and jumps over deleted runs on scan
fallback. Superseded physical slots do not consume document/byte budgets.

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
results. LRU eviction, expiry and allocator refusal always fall back to exact
verification. Storage is bounded to 128 entries / 16 MiB of bitmap allocations,
with a 2 MiB entry ceiling; nothing is written to disk. Expired entries are
reclaimed on subsequent cache access. Deployment restarts and owner moves may
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

Observed physical fields with unknown source associations leave `logical_field`
and logical operations absent. They must not invent a JSON source path from an
emitted field name. Declared named search-as-you-type roots retain their configured
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

The [qualification report](../../zig/pkg/antfly-grep/bench/results/logical-field-validation.json)
pins source and binary hashes and records local CLI, native-contract, SDK and
real-server HTTP checks. Qualification covers macOS and native one/three-shard
HTTP execution, including retained remote storage views. It does not establish
new throughput guarantees, every external provider's original-value support,
or Linux/Windows runtime qualification of these new server paths.
