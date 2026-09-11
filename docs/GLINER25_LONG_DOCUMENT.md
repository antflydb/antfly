# GLiNER2.5 long-document extension

`pipelines/gliner_boundary_long_document.zig` implements an internal, explicit
version-1 window plan and global score/record merge. This foundation is not yet
wired into the public extractor. Ordinary requests retain their length checks.
The module defaults to `reject`; a caller must select `windowed` to split a
document. It never truncates or changes the original UTF-8 text.

The pinned upstream has explicit `extract_long`/`batch_extract_long` helpers,
generic chunk-output merging, and separate constrained-classification
aggregation. The policy below is a declared Antfly extension; its ownership,
aggregation and hard-constraint behavior are not an upstream parity claim.

## Planning and identity

Windows use the processor's exact source-word splitter. The body-word budget
includes a synthetic final word when the processor adds one; URLs and ASCII
char-split runs can absorb that period. Enum prefixes and encoded schema/text
tokens have separate processor admission budgets. Every window must still pass
the real tokenizer's token limit before encoder execution.

Overlapping context windows receive disjoint owned-word ranges, divided at the
midpoint of each overlap. Their weights sum to the document's word count. Empty
text gets a single unit weight. Ownership breaks equal-score ties; it does not
discard a long mention proposed only from a neighboring context window.

One immutable document offset index supports UTF-8 byte, Unicode codepoint and
UTF-16 coordinates. Local bounds and character boundaries are validated before
rebasing. No lowercased/normalized string is used to reconstruct source spans.

Every score packet identifies its window, source plan, compiled schema and an
inference fingerprint. The caller binds that fingerprint to the exact model,
artifact/quantization and inference/calibration settings. Unbound profiles,
mixed identities, duplicate windows and missing windows are errors. Window
completion order cannot change merge order.

## Global decisions

- Classification uses the owned-word-weighted mean of every raw label logit.
  Structured selection runs once against the complete compiled constraint
  program. Ordinary classification can apply its existing activation/fallback
  policy to the same aggregate. Merging local label winners is insufficient.
- JointIE accepts candidates before local graph decoding. Typed absolute
  source identities deduplicate nodes; duplicate scores use the maximum
  calibrated utility and retain its associated probability. Window-local
  slots/hypotheses/count alternatives receive separate namespaces. One global
  solver and final validator enforce endpoint, overlap, degree, uniqueness,
  symmetry/inverse and acyclicity constraints. Physical graph limits reject an
  oversized union rather than silently pruning it.
- Entity mentions merge before global overlap resolution, separately per
  entity type. The winning origin identifies one complete attribute payload;
  attributes from conflicting window predictions are not spliced together.
- Legacy structures (`mode=null`) remain one record. Independent fields merge
  globally: source values deduplicate by exact span before overlap/scalar
  presentation, and enum choices use their maximum calibrated probability and
  declaration-order tie rules. Required fields and enum validators apply after
  the global merge, so separate windows can supply different required fields.
- Natural records use the structure and exact global anchor as identity, and
  select one complete record. Latent records retain an internal field/source
  seed when available; this is never exposed as a natural anchor. Distinct
  learned instances may show identical fields after scalar formatting, so
  displayed values alone cannot erase their multiplicity.
- Latent records without a source seed and anchorless records use per-field
  source-span multisets plus the occurrence ordinal within each window. This
  is an explicit conservative extension: identical signatures match by ordinal
  across windows and retain the maximum observed multiplicity. Optional semantic
  identity instead uses Unicode casefold plus whitespace-normalized value
  multisets and collapses matching records. Value multiplicity within a field
  remains significant. A wholly source-free record without a source seed has
  no occurrence identity across windows and raises an ambiguity error unless
  the caller explicitly selects semantic identity.
- Required/cardinality checks and validators run on record candidates.
  Exclusive-field conflicts use bounded global selection of complete record
  alternatives. An identity is a mutex, and exclusive fields consume exact
  source-occurrence resources (normalized values for derived choices).
  Same-anchor alternatives with different resource footprints remain eligible;
  only dominated alternatives with identical identity/resources are collapsed.
  The objective maximizes summed whole-record ranking, then record count, then
  deterministic source/ownership order. A lower-scoring alternative can preserve
  another valid record. The solver never combines fields from separate records.
  Its exact/beam/exhausted diagnostics apply to the retained complete records;
  this does not claim optimality over unseen cross-window field assignments.

The returned descriptor names the window policy, classification aggregation,
record identity and solver optimality scope. A proof is limited to the retained
candidate graph; it cannot certify that the model proposed every possible fact.
Strict mode rejects a final search-budget exhaustion. Explicit best effort may
return a globally valid witness with `exhausted=true`; cancellation,
infeasibility and exhaustion without a witness remain errors.

## Resource and integration boundary

Omitted decoder settings retain the native automatic global JointIE solver.
Single-window JointIE uses the pinned Fastino beam profile, while the document
solver keeps independent slot and count-alternative resources for each window.
Explicit `auto`, `exact`, and `beam` choices retain their native global behavior.
The inference identity includes the resolved global solver policy, and release
qualification distinguishes it from the single-window source profile.

Planning bounds source bytes/words, windows, repeated scan bytes and live
allocation. Merge stages bound input/output candidates, text, work and live
allocation; solvers retain their own physical/search limits. The bounded
allocator sits below each arena so retained arena chunks remain charged.
Serving must additionally place the complete plan, all retained window scores
and every concurrent merge under the request's shared admission/memory owner.

The focused tests cover Unicode rebasing, source/profile identity,
synthetic-word capacity, complete-window admission, weighted classification,
strict/best-effort decisions, cross-window JointIE cycles, per-type global
overlap, legacy global required fields/enums, latent source seeds, anchorless
multiplicity, whole-record exclusivity, allocation failures and cancellation.
The record selector also compares exact and complete-beam results against
independent exhaustive enumeration of 128 small tied resource graphs.
They do not qualify model quality or end-to-end long-document serving. Next
integration work must preserve raw classifier/JointIE scores, merge ordinary
relation outputs with the global relation deduplicator, rebase all selected
payloads, and validate actual long requests through the service path.
