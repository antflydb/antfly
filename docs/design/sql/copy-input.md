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

## Failure and policy behavior

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
postimages. It also checks identity counter behavior and RLS denial.

`antfly-api-sql-copy-input-lifecycle-test` runs real compiled metadata, session
and data owners. It verifies every row, SQL NULL provenance and primary versions/
content digests after duplicate, CHECK, NOT NULL and framing failures; explicit
rollback; read-only denial; incomplete uploads; identity writes; RLS; and cold
recovery. Its retained-preparation regression checks allocation failure,
write-only authority, revocation, cancellation and exact owner cleanup. The
first completed qualification passed both tests and all 83 Debug build steps,
with no skips or leaks. Tests request a bounded 512-row verification budget;
production query limits are unchanged.

The final combined Debug qualification passed all 110 build steps: 1,671 SQL
tests passed with four optional skips, 159 pgwire tests passed, and both native
COPY tests passed without skips or leaks. The PostgreSQL reference check,
inventory validator and its 21 tests, license and Apache boundary checks also
passed. The native fixture still emits background maintenance warnings for its
policy-protected table; this qualification does not claim that maintenance path.

The wire suite checks simple/extended framing and arbitrary input fragmentation.
A Debug microbenchmark processes 10,000 warm records per codec with no additional
allocator storage: 280 bytes retained for text/CSV and 424 bytes for binary.
Observed local times were approximately 12–20 ms per codec. These measure the
framing decoder, not end-to-end ingestion throughput. A separate structural
assertion verifies that 300 input rows use three mutation batches.

Advanced COPY WHERE, FORCE_NULL/FORCE_NOT_NULL, DEFAULT markers, FREEZE and
error-skipping options remain unactivated. Server-file/PROGRAM execution is not
provided. This cohort does not claim those original cases or unrestricted
transaction sizes, and it does not complete the remaining SQL parity objective.
