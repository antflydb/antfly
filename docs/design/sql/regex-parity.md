# Native PostgreSQL regular-expression parity

Status: required target architecture and qualification plan, not a completion
claim. The native replacement exists; exhaustive contract reconciliation does
not. This document owns the remaining regex work. Original SQL-case evidence
remains authoritative in [the parity inventory](../sql-parity-inventory.md).

## Contract and reference identity

The target is PostgreSQL 19 regex behavior, including SQL-visible results and
errors, implemented in native Zig. PostgreSQL 18 is a separate compatibility
and development-oracle lane, not sufficient evidence for a 19 claim. Pin the
exact server build/source revision, encoding, collation identity/provider/data
version and relevant session settings in every generated reference manifest.
Do not silently merge differing 18/19 expectations or use a moving `current`
documentation URL as the semantic version.

"Exhaustive parity" means every specified contract has executable evidence and
every upstream regression has an audited disposition. It does not mean that a
finite suite proves equivalence for infinitely many patterns and subjects.
Use bounded exhaustive enumeration, differential fuzzing and algorithm review
to exercise interactions beyond the specification checklist. More passing
samples alone cannot close an unreviewed contract.

The initial full grammar/SQL qualification milestone is UTF-8 with C/POSIX
behavior. That is not the final, unqualified PostgreSQL-parity claim: supported
deterministic collation profiles also need implementation and independent
qualification. Other database encodings and arbitrary host locale/provider
versions must be identified explicitly as unsupported, not silently mapped to
C. A release claim must name its version, encoding, collation profiles and
resource envelope. Nondeterministic regex collations require PostgreSQL's
rejection semantics, not an invented matcher interpretation.

Reference specifications:

- [PostgreSQL 19 pattern matching](https://www.postgresql.org/docs/19/functions-matching.html).
- [PostgreSQL 19 collation support](https://www.postgresql.org/docs/19/collation.html).
- PostgreSQL regression sources and expected outputs at the pinned source
  revision, including its regex test module and SQL string/pattern tests.
  Preserve provenance and applicable licenses when extracting test cases;
  importing tests does not introduce a production C dependency.

## Existing evidence versus unfinished work

The replacement commit `f3d0a9071f` introduced `antfly_regex.captures` and removed
the C engine/bridge. Existing byte/FST regex semantics are unchanged. The SQL
adapter owns flag parsing, caches, replacements and diagnostics. Its current
1,025 PostgreSQL 18 C-collation component contracts, allocation/cancellation
tests, scaling checks and import-free WASM execution are useful baseline
evidence, not exhaustive syntax or public-SQL certification.

The work below is required and unfinished unless an executable evidence record
explicitly closes it. A listed family may already have substantial code; the
gap can be audit, interaction coverage or public activation rather than an
absent implementation. Do not infer a confirmed defect from an audit target.

## Qualification progress

### Implemented qualification foundation (2026-10-09)

`scripts/check_sql_regex_parity.py` now provides an independent `selection-v1`
grammar campaign and a permanent regression-witness mode. Its default oracle
major is 19; explicitly selecting `--oracle-major 18` gives compatibility-lane
evidence only. Reference artifacts pin the server build, binary SHA-256,
encoding/collation and timeout. The standalone `parity-probe` verifies accepted
results, complete character/capture spans and compile SQLSTATEs. Reports bind
the exact witness bytes and probe binary. Every live campaign also proves that
a deliberately incorrect witness is detected. Oracle timeouts are inconclusive;
native resource refusals remain mismatches. Eight harness self-tests verify
enumeration completeness, deterministic IDs, profile separation and rejection
of truncated, foreign or unbound native reports.

The qualified PostgreSQL 18.6 C/UTF8 run checks 24,575 distinct cases: all 390
declared pattern templates against every binary subject through length three
and every valid start, additional boundary/rejection witnesses, and 5,000 seeded
generated patterns (seed 19). It has zero mismatches. This is exhaustive within
those explicit enumeration bounds, not exhaustive ARE grammar or PostgreSQL 19
evidence. Discovery corrected fixed-bound preference inheritance, zero-bound
elision, nullable capture participation, required zero-width participation,
BRE anchor/star context, grouped assertions and complemented-shorthand newline
membership. Match extent and empty-capture preference are now distinct program
metadata rather than pattern-specific exceptions.

Thirty-seven minimized accepted/rejected witnesses are committed in
`zig/lib/sql_regex/src/testdata/selection-postgres.json`, with exact oracle
identity. They run in the regular native gate and twice in import-free WASM
(1,062 combined component contracts). Ten additional scalar witnesses verify
function values, result OIDs and errors through SQL binding/execution; that
scalar fixture now has 71 contracts. This is not mounted HTTP/pgwire or durable
expression certification and does not change original SQL dispositions.

Still unfinished: the complete machine-readable specification/upstream
inventory, PostgreSQL 19 execution, broader grammar/backreference campaigns,
automatic grammar-aware shrinking, non-C profiles, all public SQL surfaces,
durable semantic-version activation and mandatory live-oracle CI lanes. The
new campaign is a foundation for those requirements, not their completion.

## Contract inventory requirements

Create a machine-readable inventory separate from the original SQL extraction
cases. Each row records a stable contract ID, reference revision/section,
semantic profile, implementation owner, status, evidence gate and any exact
counterexample. Distinguish implemented-and-qualified, partial, unresolved,
PostgreSQL-required rejection and deliberate product restriction. The last is
not parity success. No denominator changes or generic fixture credits.

Required families:

| Family | Required reconciliation |
| --- | --- |
| Grammar | BRE, ERE, ARE and literal modes; directives, ordered flags, inline options, expanded syntax/comments, escapes, bounds, anchors and malformed syntax |
| Character domains | Classes, complements, ranges, collating elements/equivalence classes, case-insensitive closure, Unicode offsets, newline modes and word constraints |
| Selection and captures | Earliest start; whole-expression/subexpression preferences; alternation, fixed versus variable bounds, nested/nullable repetition, empty versus unmatched captures |
| Nonregular constructs | Backreferences, repeated capture histories, lookahead/behind and their capture/reference restrictions, interactions with greedy/lazy selection |
| SQL surface | All regex operator/function overloads, substring forms, SQL-pattern translation, global iteration, split results, replacements and set-returning behavior |
| SQL errors and types | NULL/strictness, unknown literals, coercions, parameter OIDs, result types/dimensions, argument validation, SQLSTATE and error timing |
| Execution and ownership | Scalar/vector agreement, constant/dynamic preparation, cursor suspension, cancellation, cache eviction, allocator failure and concurrent immutable reuse |
| Durability and optimization | Defaults/generated/CHECK/index expressions, reopen/restore, semantic-version fences, sound pushdown and no false-negative index pruning |

Prioritize interaction audits: `{m}` versus `{m,m}` preference inheritance,
BRE anchor/escape context, bracket complement plus newline/case behavior,
octal/backreference ambiguity, nullable repeated capture histories, and nested
assertions. These are audit targets, not claims that the current implementation
fails each one.

## Independent differential qualification

1. Inventory all applicable upstream regex and SQL pattern regressions at the
   pinned reference revision. Adapt test harness syntax only; preserve the
   behavioral input and expected distinction. Record harness-only exclusions
   individually. Internal debug-output tests need an equivalent semantic
   witness or an explicit non-public-contract rationale, not silent omission.
2. Generate valid and deliberately invalid BRE/ERE/ARE patterns from an
   independent grammar, not the native parser. Compare compile acceptance,
   SQLSTATE, whole/capture spans, unmatched versus empty captures, occurrences,
   replacement/split values, labels/types and relevant side effects. Use an
   independent SQL query for every public contract, not just a span oracle.
3. Enumerate every pattern and subject within checked-in finite bounds over a
   small discriminating alphabet, plus Unicode/newline/locale boundary sets.
   Publish exact grammar/node/length bounds and case counts. Enumerate ordered
   option transitions and valid start/occurrence/subexpression arguments too.
4. Run seeded, shape-weighted fuzzing beyond those bounds, combining nested
   repetition, captures, assertions and backreferences. Shrink mismatches to
   permanent regression witnesses. Retain failing seed/profile/server identity;
   never replace an inconvenient golden with native output.
5. Bound both oracle and native runs. Classify an oracle timeout as inconclusive
   and a native quota refusal as a resource-bound outcome, never semantic
   agreement. Retest within a suitable envelope. Unexpected native refusal on
   a required supported contract blocks qualification; resource limits cannot
   be a blanket excuse for mismatches.

## Long-term implementation boundaries

Keep one native capture engine in `lib/regex`, separate from byte/FST matching.
Retain immutable programs and execution-owned scratch/frontiers. Correct match
extent and capture ordering are independent invariants; optimizations cannot
substitute Perl-style first-success behavior. Backreference-heavy execution
remains bounded and cancelable without a false linear-time promise.

Introduce an explicit immutable character-semantics profile for classification,
case closure and bracket behavior. Generate versioned native tables for
PostgreSQL builtin deterministic Unicode profiles before expanding to other
named providers. Do not approximate locale semantics with generic Unicode
case folding. Provider/version-specific parity needs a pinned reference and
native data/algorithms; arbitrary libc/ICU installation equivalence is not
established by supporting a similarly named locale. Never use process-global
locale state or a C fallback. Carry profile identity from SQL collation binding
into program/cache keys and execution admission.

SQL-specific coercion, occurrence advancement, replacement interpretation,
set-returning rows and pattern translation stay in shared SQL adapters. Cover
`regexp_like/count/instr/substr/match/matches/replace/split_to_array/split_to_table`,
the four regex operators and their quantified forms, regex `substring`, and
`SIMILAR TO`/SQL substring translation. Audit existing implementations before
adding code. LIKE/ILIKE remain a distinct matching contract, not an ARE rewrite.

Persist a regex semantic version with durable expression programs and include
it in prepared/cache identity. Changes to accepted syntax, captures, collation
or evaluation semantics must not silently change a stored CHECK, generated
value or partial-index membership. Use existing catalog capability admission
and staged revalidation/rebuild machinery for upgrades, standby promotion and
restore; do not introduce an independent regex migration system. Optimizer
rewrites and index prefilters require evidence for the same profile/version.

## Performance, CI and completion gates

Keep fast deterministic contracts on every PR. Add a bounded-enumeration gate
with declared coverage and a scheduled longer differential campaign. A missing
PostgreSQL oracle fails its required gate; it is not a successful skip. Replay
saved witnesses on native debug/release, supported server platforms and
freestanding WASM. Public integration gates cover mounted HTTP/pgwire and
durable-expression/reopen/restore paths, not just direct engine calls.

Measure compile versus cold/warm execution, actual resident bytes, allocation
counts and charged work separately. Scale subject length, pattern width,
capture count, repetition bounds, assertion depth and output size independently.
Enforce shape-specific bounds rather than declaring every ARE linear. Cover
late failures and rejected/adversarial inputs, cache churn and time-to-cancel.
Retain the existing zero-extra-allocation warm-owner contract where applicable.

Implementation order:

1. Pin the oracle/profile identities; build the complete contract/upstream
   inventory and reusable mismatch/shrinking harness.
2. Close grammar, selection/capture and rejection contracts under C/POSIX;
   audit optimizations against generated interactions, not just happy paths.
3. Close every SQL surface and execution/durable ownership contract.
4. Implement and qualify deterministic Unicode profiles and versioned
   collation binding; account explicitly for any remaining provider boundary.
5. Make qualification mandatory and publish the exact coverage/resource
   envelope, benchmark evidence and unresolved-contract count.

Completion requires zero unresolved contracts within the declared profile,
zero unexplained upstream/differential mismatches, passing public/durable/fault
gates and measured resource behavior. Full C-profile completion is a useful
named milestone; it does not authorize an unqualified all-collation claim.
None of these component credits automatically changes original SQL-case
dispositions. Final original-case credit still requires unchanged-source
public behavior and independent PostgreSQL evidence.
