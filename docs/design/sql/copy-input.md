# Transactional SQL COPY input

The PostgreSQL wire listener admits text, CSV and binary `COPY FROM STDIN`
through the existing SQL transaction and native mutation graph. This closes the
unchanged originals `sql-0370`, `sql-0371` and `sql-0379`; the immutable original
inventory and SQL text are unchanged.

## Ownership and execution

Description pins the authorized target and immutable column domains before
CopyInResponse. A description-only write resolver expands implicit columns,
omits generated columns and preserves the actual owner's row-ID capability.
It neither allocates row IDs nor grants read or mutation authority. Public
connection resources own their copied description bytes after the plan-cache
lease ends.

One incremental decoder owns one bounded record buffer. Frame boundaries have
no semantic meaning. CSV quoting, text escape decoding, NULL provenance, binary
header/length validation and UTF-8 admission finish before typed row conversion.
SQL NULL and JSON null remain distinct. PostgreSQL 18's literal CSV backslash-dot
and text end-marker behavior are independently checked.

Input becomes typed parameters to cached INSERT templates. Only parsed names
are quoted into templates; values never become SQL text. Batches contain at most
128 rows and remain below the parameter limit. A 256 KiB raw-payload target
flushes variable-width batches early; the connection/frame budget bounds a
single larger row. Array, exact NUMERIC, temporal, range and JSON codecs reuse
the existing typed ingress. One statement clock spans all batches. COPY applies
identity override semantics while omitted columns retain ordinary defaults.

An idle connection begins one implicit transaction; an explicit transaction
retains its ordinary timing and savepoint state. Every batch uses current
credential, catalog-binding and native constraint checks. COMMIT uses the shared
durable distributed decision machinery. This adds no alternate coordinator,
restore path, side journal or mutation-replay policy. Framing memory is bounded;
the existing transaction admission limits still bound retained staged writes.

Native normalization retains its schema/policy owner across rows. The pgwire
guard now forwards that shared preparation port, with authority checks on open,
prepare and finish. Closing remains unconditional after revocation/cancellation.
This also repairs ordinary pgwire inserts into tables with stateful defaults.
It does not grant SELECT permission to a write-only principal.

## Column options and rejected rows

Input supports `HEADER MATCH`, `FORCE_NULL` and `FORCE_NOT_NULL` column lists or
`*`, `DEFAULT`, `ON_ERROR`, `REJECT_LIMIT` and `LOG_VERBOSITY`. Column masks are
resolved once, before CopyInResponse. A matching header must contain exactly the
selected column names in order; missing and mismatched headers fail. Quoted CSV
NULL markers and default markers retain their provenance. Text DEFAULT matching
uses the raw field before backslash decoding. A DEFAULT marker on a column with
no default is a hard framing error, even with ON_ERROR IGNORE.

Default markers become native INSERT DEFAULT cells; their bytes never become
SQL text or SQL NULL. Placeholder ordinals advance only for actual values, so
mixed default/value rows share the existing assignment, identity, generated-value
and constraint machinery. Compiled numeric-modifier programs apply the target
precision and scale at the input boundary before deciding whether to reject a
row. They are bound once per input column, including numeric-array domains.

IGNORE catches only conversion failures. A reusable scratch arena owns one
candidate row, and accepted typed values are cloned once into bounded batches.
Rejected rows cannot accumulate allocations in the batch arena. STOP keeps its
direct value ownership path. Framing/encoding errors, missing defaults, native
constraints, authorization, resource failures and uncertain commits cannot be
ignored. Exceeding REJECT_LIMIT aborts every staged batch with 22P02 and an
explicit limit diagnostic. Default verbosity reports a skipped-row summary;
verbose also reports input row ordinals, and silent suppresses those notices.
Notices never include raw input values.

## Failure and policy behavior

On a framing/conversion failure, earlier accepted buffered rows are validated
once before rollback. Their earlier native errors retain precedence, and their
nontransactional sequence effects match PostgreSQL. An explicit admission guard
prevents retrying a native mutation that was attempted, including a failed or
uncertain one. Cancellation/resource failures do not trigger extra admission.
An implicit COPY error rolls back every staged batch. A local decoder failure
inside an explicit transaction marks that transaction failed. A lost or malformed
upload cannot commit without CopyDone. Queued CopyData is discarded after a
reported statement error, and pipelined extended-query Sync retains correct
ReadyForQuery behavior. An uncertain commit preserves the existing transaction
receipt and never permits automatic replay.

Active RLS rejects COPY input with PostgreSQL's `0A000`; policy-protected writes
use INSERT. The native adapter checks this at description and batch binding.
Ordinary row filtering, proof validation, credential revocation and commit-time
integrity remain part of the existing execution graph.

## Evidence and limits

`scripts/generate_sql_copy_input_reference.py --check
zig/pkg/antfly/src/pgwire/fixtures/copy_input_reference.json` uses an isolated
PostgreSQL 18+ server. Its three exact originals import 300 rows each and record
complete typed postimages, defaults/generated values and twelve atomic failure
postimages. Two additional text/CSV profiles check defaults, precision/scale,
quote provenance and error skipping against complete typed postimages. Sixteen
additional failure postimages cover constraints, malformed/absent headers,
missing defaults, framing and rejection limits after a staged batch. Twenty-one
independent option contracts check admission and exact diagnostic codes. The
oracle also checks identity counter advancement before a failed upload, RLS denial and the removed OIDS
option's exact original SQL for both true and false. OIDS false is a PostgreSQL semantic supersession,
not positive functionality.

`antfly-api-sql-copy-input-lifecycle-test` runs real compiled metadata, session
and data owners. It verifies every row, SQL NULL provenance and primary versions/
content digests after duplicate, CHECK, NOT NULL and framing failures; explicit
rollback; read-only denial; incomplete uploads; identity writes; RLS; and cold
recovery. Its retained-preparation regression checks allocation failure,
write-only authority, revocation, cancellation and exact owner cleanup. Tests
request a bounded 512-row verification budget; production query limits are
unchanged. Recorded Debug and PostgreSQL evidence commands live in the parity
ledger. The native fixture still emits background maintenance warnings for its
policy-protected table; this qualification does not claim that maintenance path.

The option cohort's full Debug validation passed 1,672 SQL tests (four optional
benchmark skips), all 163 pgwire tests and both native lifecycle tests (no skips
or leaks). The PostgreSQL oracle reproduced all 28 failure postimages and 21
option contracts. Inventory validation and its 21 tests, formatting, license and
Apache boundary checks also passed.

The wire suite checks simple/extended framing and arbitrary input fragmentation.
A Debug microbenchmark processes 10,000 warm records per codec with no additional
allocator storage: 409 bytes retained for text/CSV and 424 bytes for binary.
Observed local times were approximately 12–25 ms per codec. These measure the
framing decoder, not end-to-end ingestion throughput. A separate structural
assertion verifies that 300 input rows use three mutation batches.

COPY WHERE and FREEZE remain unactivated, including the exact all-options
original that requests FREEZE. Server-file/PROGRAM execution and non-UTF8
encodings are not provided. This cohort does not claim those original cases or
unrestricted transaction sizes, and it does not complete SQL parity.

WHERE requires a shared candidate-preparation boundary: apply typed input and
omitted/stateful defaults first, evaluate the predicate with the captured clock,
then apply generated values and constraints only to retained candidates. It
must support predicates on omitted defaulted columns, preserve sequence effects
for excluded rows and reject generated-column references. Filtering already
validated mutations or limiting predicates to supplied fields would violate
that contract. FREEZE additionally needs proof of creation/truncation in the
current subtransaction and compatible snapshot ownership; it cannot be accepted
as an unchecked transport option.
