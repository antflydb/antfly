# SQL extraction: typed rows, arrays, precise types and exact NUMERIC log (2026-10)

> Summary of a verbatim log. Full text: [docs/design/sql/history/typed-values-log-2026-10.md@f3fd4d3](https://github.com/antflydb/antfly/blob/f3fd4d35258791fe16ab927a1e7f9b3af247b074/docs/design/sql/history/typed-values-log-2026-10.md). Originally relocated verbatim from `docs/sql-extraction.md` (lines 3767–4362, 4395–4706, 4886–5262, 5293–5901, 5930–6667) at commit 3ce5aff4ea on 2026-10-12; durable decisions were folded into [SQL extraction](../extraction.md) before the move.

Between 2026-10-06 and 2026-10-08 (and the following increments), SQL extraction replaced JSON-placeholder values with lossless typed Datums: typed relation rows, PostgreSQL arrays (results, storage, durable expressions), precise builtin scalar domains, NULL-aware tuple membership, and an exact NUMERIC type from kernel through public columns with precision/scale modifiers. PostgreSQL is the semantic authority (PostgreSQL 19 target; oracle on an isolated PostgreSQL 18.6 instance). Most entries are infrastructure that deliberately earned no original-case credit; counts and test totals are as of the time.

## Standing rules stated in the log

- Neither oracle admission nor an old schema's readiness/cardinality annotations grant completion or authority. SQL that disagrees with PostgreSQL must not gain parity credit through SQLite emulation or fixture-only authority.
- Infrastructure, kernel or component tests do not credit original corpus cases; only exact-source mounted native/PostgreSQL execution evidence changes dispositions.
- JSON shape never chooses a SQL type; document JSON arrays are not reinterpreted as SQL arrays, and SQL arrays are not inferred from JSON array properties.
- Restore verifies, never repairs: physically valid checksums do not admit out-of-domain values, and constrained stored values are never rounded on restore.
- Guards (array DDL, array-valued ordered keys, modifier publication) are removed only after the complete typed path is verified; parsing never silently publishes an unconstrained column or drops quantization.
- Validation-gate failures are reported, not hidden: the process RSS limit was not raised to mask the 1.04 GB observation, and no production validation was relaxed to make a fixture pass.

## Original-case inventory over time

Original inventory: 1,586 cases, unchanged throughout.

| Point in log | Implemented | Rejected | Superseded | Unresolved |
| --- | --- | --- | --- | --- |
| 2026-10-06 campaign (103 contracts resolved: 54 reads, 49 document mutations) | — | — | — | 1,066 |
| Typed relation row boundary (2026-10-07) | 358 | 136 | 73 | 1,019 |
| `sql-0561` grouped multi-percentile passes | 365 | 136 | 73 | 1,012 |
| Stored typed-array reads (14 cases) | 379 | 136 | 73 | 998 |
| Constraint-equivalent native mutations (13 cases) | 426 | 136 | 73 | 951 |
| Named conflict arbiters (`sql-1390`, `sql-1392`, `sql-1393`, `sql-1439`, `sql-1463`, `sql-1465`) | 432 | 136 | 73 | 945 |
| Exact NUMERIC kernel onward | 448 | 136 | 73 | 929 |

Other dispositions: six correlated/tuple-membership mutations (`sql-0600`–`sql-0602`, `sql-0610`–`sql-0612`) pass mounted HTTP; `sql-1487` stays a verified rejection, now at catalog binding with 42704. The family audit identified DDL (340 open contracts) as the largest remaining family.

## Durable schema format / capability ladder

| Format / capability | Adds |
| --- | --- |
| 16 | Optional precise SQL descriptor per physical column; format-15 catalogs remain readable without a write on open |
| ASCH 17 | `sql_array` column type bound to precise element identity |
| 18 | Durable numeric programs with builtin widths; ordered `case_when` conditional programs (max fifteen branches) |
| 19 | Durable `IN`/`NOT IN` membership and integer remainder |
| 20 | Native scalar NUMERIC and NUMERIC array physical descriptors |
| 21 | Exact-NUMERIC schema VM programs (even with integer/boolean outputs) |
| 22 | Exact NUMERIC public scalar validation (separate from 20 and 21) |
| 23 | NUMERIC precision/signed-scale modifiers on columns and in expression programs |
| 24 | Array-dependent expression programs, independent of physical array columns |

Older capabilities cannot authorize newer layouts; serialization refuses downgrades that would drop a contract.

## Typed rows and arrays

### Decisions
- **Relation pages retain owned complete Datums** with one name-to-ordinal directory per page, instead of a JSON object plus SQL-null and pattern side channels per row. Arrays keep the lossless typed-row path until the column-page codec represents them.
- **Internal vs. public binding** — internal binding retains array-valued outputs across derived boundaries; public binding still rejects unsupported wire types before execution.
- **Common set/VALUES types** — each set arm is coerced before the node compares, hashes or emits (an outer float4 widening cannot collapse bigint arrays before an inner DISTINCT). VALUES selects one common type across all rows. Unknown results become text inside their producer and cannot later infer a numeric parameter from outside.
- **Lossless array wire envelope** — dimensions (`length`, `lower_bound`), flat row-major values and aligned SQL-null flags; integers as exact decimal strings; NaN/infinities as explicit tokens; JSONB null distinct from SQL NULL. Element type is a bound-column descriptor.
- **Public array activation** — pgwire advertises exact PostgreSQL array OIDs; prepared/cursor shape checks include element identity. DataRow admission reserves all remaining length headers so individually fitting cells cannot publish an oversized frame.
- **Array equality requires identical element types** (unlike numeric promotion in UNION/VALUES); incompatible signatures give PostgreSQL's undefined-operator diagnostic.
- **Array functions** (`array_position(s)`, `array_remove`, `array_replace`, `array_append`/`prepend`/`cat`, `||`, overlap, `array_to_string`) follow PostgreSQL's anycompatible resolution; typed incompatible arguments produce undefined-function rather than an implicit text cast; results keep actual subscripts and lower bounds.
- **Declared array dimensions** are not type identity (PostgreSQL does not enforce them).
- **Compact schema-bound array codec** — NULL bitmap, fixed slots or variable offsets, rank checkpoints every 64 cells; header-only shape projection is not a restore gate.
- **Semantic hashing** covers typed elements, shape, bounds and SQL NULLs, not offsets/padding/representation; physical float zero signs survive storage while hashes agree.
- **Array-valued ordered/unique/FK index keys remain guarded** until typed ordering exists; arrays are never silently indexed as blobs.
- **Logical row-value domain separated from ordered-key operands** in the durable VM; arrays are pinned bytes plus element identity.
- **Durable array casts** — identity casts and NUMERIC-array modifier casts only; element-changing casts and scalar/array reinterpretation are rejected.

### Rejected approaches
- **Ordinary equality hash joins for row-valued `IN`/`NOT IN`** — cannot express three-valued logic (`(NULL, 1)` vs `(2, 2)` is definitely unequal, vs `(2, 1)` potentially equal). Replaced by a shared-prefix trie in `sql/tuple_membership.zig`, avoiding per-outer-row scans, JSON key serialization and exponential NULL-mask tables. PostgreSQL reproduces 1,740 truth pairs over 110 right-hand multisets.
- **Generic dynamic JSON parser for borrowed JSONB comparison** — it owns tokens even with allocation-if-needed; the standard scanner is used instead (a 128 KiB string element compares within 64 KiB of scratch).
- **Python list conversion as evidence** for array mutation readback — not accepted; mutation readback stays guarded until a lossless codec exists.
- **Shared JSON memory writer mapping buffer exhaustion to `WriteFailed`** — exposed by allocation-failure tests; now propagates `OutOfMemory`.

### Key measurements (local Debug unless noted; absolute samples, not speedups)

| Workload | Result |
| --- | --- |
| Compact codec, 4,096 cells, 1/8 NULL | boolean 4,372 B, int16 7,956 B, int64 29,460 B vs PostgreSQL wire 19,988 / 23,572 / 45,076 B |
| Streaming array output | 262,144 cells / 3,439,056 bytes in about 119 ms, zero encoder allocations |
| Borrowed int64 array comparison, 10,000 elements ×100 | 178 ms vs 201 ms decoding one vector per comparison (baseline peak 1,320,344 bytes) |
| Trusted projection, 10,000 lookups | about 3.7 ms (4,096 cells), 3.6 ms (65,536 cells) |
| 10,000-cell NUMERIC-array assignment | 161,270 output bytes, five backing allocations, 505,224 charged bytes, about 10 ms |
| Envelope admission, four passes | about 7 ms (4,096 cells), 126 ms (65,536 cells, explicit 8 MiB work budget; default 1 MiB rejects) |

## Mutations and conflict handling

- **UPDATE FROM / DELETE USING fanout** — selects one coherent matched source row per target before affected-row admission, rather than rejecting PostgreSQL's legal UPDATE fanout or mixing columns from different matches; which row is chosen is not promised. MERGE keeps its own cardinality contract. (This supersedes the SQLSTATE 21000 rejection recorded in [integration-status-log.md](integration-status-log.md).)
- Joined RETURNING binds the full join scope with dense execution slots; source types stay independent of assignment coercion. Peak statement memory for the 1,024-target workload fell from 4,632,337 to 4,432,621 bytes. With 128 cold source columns, 128/512/1,024 targets read exactly 384/1,536/3,072 physical rows through one capture.
- `ON CONFLICT DO UPDATE SET column = DEFAULT` omits the field so native preparation evaluates the declared default; it never substitutes NULL or reuses the INSERT value.
- `ON CONFLICT ON CONSTRAINT` binds one exact durable unique generation, never widening to equivalent constraints. Absent names → 42704, CHECK/FK names → 42809 before row evaluation; deferrable arbiters → 55000 after proposed defaults, matching PostgreSQL timing.
- Constraint-equivalent native campaigns derive primary keys via the production DDL builder; duplicates/NULL keys fail with 23505/23502. Isolated owner fixtures do not prove distributed read cuts or multi-owner activation.
- **Defect:** a primary-key DDL lifetime bug — inserting required columns could move the parent schema object's slots; lowering now borrows the nested properties map independently.

## Durable expressions and CHECKs

- Durable numeric programs keep builtin widths so narrow overflow and float4 rounding survive persistence. Out-of-range integer defaults are accepted at DDL time and raise when used.
- Durable CASE uses an ordered `case_when` node, not eager evaluation or duplicated boolean rewrites; UNKNOWN is never treated as FALSE. Integer remainder returns zero for minInt modulo -1. Exact decimal and floating remainder are not approximated by binary-float programs.
- **Rejected: CHECK evaluation by serializing row cells into ordered index keys** — column/operator CHECKs and explicit expressions now compile to the same typed plan; persistent key size limits are still enforced by actual index encoding (wide blob still reports `RelationalIndexKeyTooLarge` on key encoding while its CHECK succeeds).
- **Rejected: cast-only literal interpreter for partial-index bounds** — removed; bounds fold through the schema-expression compiler/evaluator at DDL time. The DDL entry point does not yet forward a request-wide cancellation checkpoint.
- Shared execution context now spans normalization, default/generated evaluation, recursive NUMERIC constraints, CHECKs and physical restore validation, with sticky work/cancellation; failures remain errors, not bad-row findings.

## Exact NUMERIC

### Decisions
- **Canonical base-10000 limbs**, identity separate from display scale, no floating-point intermediates; dense multiplication/division stay quadratic and explicitly work-bounded.
- **Rounding domains** — NUMERIC casts/rounding are half-away-from-zero; double rounding is ties-to-even; float-to-NUMERIC uses PostgreSQL's six/fifteen significant digits with ties-to-even.
- **Mixed NUMERIC/real** operators and membership use double precision; CASE/COALESCE common domains retain real (matches PostgreSQL 18).
- **Binary codec** — short framing 08P01; invalid framing/signs/scales/groups 22P03. Encoder preserves PostgreSQL's ignored infinity scale-field convention.
- **Ordered keys** encode PostgreSQL total order directly in bytes (separate ranks for -inf, negative, zero, positive, +inf, NaN); keys omit display scale so equivalent values share unique identity; components are self-delimiting and prefix-free.
- **Public identity** — scalar OID 1700, array OID 1231; results are decimal strings preserving scale; special values use PostgreSQL text tokens.
- **SUM/AVG** use flat grouped lanes of signed i128 base-10000 buckets; aggregate checkpoint version 2 binds exact input element identity and retains buckets rather than prematurely finalizing. AVG divides the widened sum before enforcing range, so an overflowing SUM can still yield a valid AVG.
- **Exact literals** — decimal/scientific and beyond-i64 literals keep exact source spelling in the AST; integer plus decimal literal stays exact NUMERIC.
- **sqrt** uses an exact integer Newton kernel; display scale clamped to 0..1000 with at least sixteen significant digits.
- **Native storage** — distinct `numeric` physical column (capability 20); unannotated `number` fields keep floating-point semantics; generic document predicates reject NUMERIC cells rather than comparing as text or via f64. Native expression lowering refused NUMERIC instructions until the VM had an exact decimal domain.
- **Modifiers** — precision/signed scale carried through casts, result descriptors (pgwire typmod encoding; retained only when every contributing expression has the same modifier), storage (format 23), assignment before dependent defaults/generated expressions, and finally public column annotation `x-antfly-sql-numeric-modifier`. Constrained infinities are rejected; NaN follows PostgreSQL. Modifier range failures report 22023, not XX000.
- Go's serializer must not emit a zero-valued modifier for unconstrained array declarations (the modifier is genuinely optional).

### Rejected approaches / defects
- A scalar-only NUMERIC selection could bypass typed adaptation into generic JSON-number coercion; an already-rounded f64 is now rejected.
- Canonical logical admission did not enforce the request's coefficient cap; an old quota test reused an already failed context and could not catch it.
- Retrying a smaller constant-cache candidate after a sticky quota flag could hide a later genuine allocation failure; preparation now stops once its cohort is exhausted.
- Immutable kernel addition for SUM: repeated immutable kernel addition took about 499 ms and 10,000 allocations vs about 0.6 ms with zero hot allocations for the bucket lane (10,000 copies of 1.2300).

### Key measurements

| Item | Result |
| --- | --- |
| PostgreSQL arithmetic oracle | 312 → 799 → 893 contracts (94 sqrt cases added); 537-case arithmetic/cast/order subset runs through the native VM |
| Binary codec oracle | 65 sender, 47 receiver cases |
| Ordering oracle | 235 dense ranks, 55,225 pairs per direction |
| Constraint oracle | 70 predicates; partial-index bounds 44; modifiers 46 values / 56 scalar cases and 12 descriptors; assignment 11 |
| Multiplication work units | 64/256/1024 digits → 289/4225/66049; 1024-digit run about 0.53 ms |
| Division, 2048 / 1024 digits | three allocations (quotient), two (remainder), about 0.8 ms |
| Ordered key size | dense 2,048-digit value 1,029 bytes; 1e131071 and 1e-16383 seven bytes |
| sqrt 1024-digit (ReleaseFast, single shot) | irregular about 274 µs / six allocations; all-nines about 71 µs / four; irregular uses roughly 149,000 work units (not claimed to fit the default 65,536 budget) |
| Borrowed vs owned validation, 98,836-byte 4,096-element array (ReleaseFast) | 24–34 µs, zero allocations vs 194–208 µs, four allocations |
| Grouped oracle check (2,000-row spill fixture) | sums 2996.20/2998.20, averages 1498.1000000000000000/1499.1000000000000000 |

## Test infrastructure and build-resource findings

- **Debug allocator retaining unwind buffers** — Zig 0.17's default debug allocator is a process-lifetime arena that kept DWARF unwind VM buffers; the SQL build reported about 1.04 GB against its declared 384 MiB RSS bound. The shared test runner now supplies an independent reclaiming debug allocator. Grouped-output fault test max RSS fell from 115,916,800 to 9,748,480 bytes (about 92%) at about 13.5 s either way; the 212-test server shard then peaked at 17,252,352 bytes.
- Allocation-fault harnesses now disable address-dependent arena remaps so failure indexes are deterministic.
- Broad optimized SQL compilation reported 3.44 GB against a 3.22 GB claim; the broad durable-runtime target reported a 14.46 GB compiler peak against 13.96 GB plus duplicate ownership of a guarded graph replay raft-batch test. Neither was claimed fixed. `zig build sql-numeric-test` isolates 34 NUMERIC contracts at 526 MB.
- The relational public-API test owner imported fixtures from a smoke test excluded by its own filters; restoring the import brought back three FK publication/initial-create tests.
- The 84-test pgwire gate needs permission to bind loopback listeners (sandbox gives EPERM in three tests).
- Last recorded gate: 626 local SQL tests (three skips), 226 server SQL tests, 74 local schema tests and seven public server expression tests.

## Remaining work recorded at the end of the log

- Array: dynamic array constructors, element-changing durable casts, non-NULL SQL array defaults, array-valued ordered keys, broader array functions, spill-backed tuple membership, and mounted array parity campaigns; array DDL guards for unfinished shapes remain.
- NUMERIC: dynamic text casts, non-finite floating-domain casts, exact power and broader functions, mixed-domain join-key normalization, native NUMERIC vector kernels, and shared statement/reducer/schema work admission.
- Shared admission still missing in canonical JSONB serialization (sorting/number emission) and some low-level codec/hash paths.
- Recursive CTE mutation cases remain unresolved until their fixtures exercise a nontrivial recursive step; regex operations and renamed-constraint DDL were separate gaps.
