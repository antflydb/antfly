# Explicit write execution capability

TableWriteSource appends private callback capabilities for stateless batch and
stable-ID commits with a borrowed invocation Team. A null team preserves the
existing cancellation contract. A supplied team requires the new callback;
unsupported sources fail before any legacy callback can run. Stable-ID callers
can inspect support before sealing durable SQL recovery debt. The team is local
trusted state and must never be serialized or outlive its joined callback.

The focused direct contract root executes two named callback regressions plus
one discovery test (3 total) on native macOS and pinned Nix/glibc GNU AArch64.
Tests exercise direct and compiled callback dispatch, pointer identity, bounded
allocator exhaustion, joined reuse, cancellation before resize, stable transaction
ID/timestamp and exact overload/unknown-decision error identity. They verify no
legacy fallback when a supplied capability is unsupported. The initial consumer
module roots reported only one discovery test; those preliminary green logs do
not prove either regression ran. The direct embedded root fixes discovery.

SQL HTTP/pgwire ingress, request context, provisioned/hosted/bound storage and
coordinator forwarding remain supplemental pending full compilation/runtime
qualification. Ordinary writes reserve coordinator plus bounded children on
one retained root; protected COMMIT remains serial on its control slot. Child
arenas use the process-owned request byte partition, not a request arena.
Single-group fast paths validate the same execution and keep their serial path.
The proposal rejects unsupported stable-ID execution before sealing the SQL
plan and preserves existing post-seal unknown/repair outcomes. These properties
are source proposals, not yet production qualification results.

Pooling stays disabled. Accepted completion I/O parking, physical durability
handoffs, actual numerical teams and whole-node/default proof remain required.
Compile memory reservations are not runtime memory limits.

The supplemental patch is relative to 3c516d88b2 and includes inherited pending
production changes and both process fixture files. Reverse-apply checking
confirms the exact source proposal. It is not proof of runtime qualification.
