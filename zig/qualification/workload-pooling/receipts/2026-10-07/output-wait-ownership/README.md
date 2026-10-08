# Caller-owned output waits

Optional paired WriteControl wait hooks run on the calling task at socket writes,
contended lock sleeps and HTTP/2 credit waits. Fence callbacks remain separate
and may run on a timeout monitor. The operation restores or rejects execution
ownership on IO failure and cancellation before returning to formatter code.
Resume failures occur outside transient socket or credit-tick timeout retries.
Without ownership hooks, existing output behavior is preserved.

Native ReleaseSafe selected 45 tests: 35 passed, 10 Linux-only skipped. Pinned
Nix/glibc GNU AArch64 passed all 45 in a 3 GiB container. The new transition test
covers success, IO failure, denied park, denied resume, cancellation after park
and an incomplete hook pair. An actual H2 stream with zero credit checks that
parking releases its connection mutex, then credit replenishment permits DATA
serialization only after resume; valid payload and END_STREAM are preserved.
These hooks have not yet been connected to actual API owner capsules through
the runtime archive boundary. They do not independently prove physical scheduler
ownership. Production formatting currently remains inside the whole-callback
park until that integration is implemented. Socket framing can hold the write
mutex during a resume; mixed connection/control schedules need qualification.

Native hashes were captured after completion and match the GNU launch manifest.
The manifest includes runtime bridge and standalone provenance; the gate compiles
HTTP library bodies, not those frontend bodies. The first native attempt ran in
the networking-restricted sandbox and failed TCP setup; that output is preserved.
The qualified native run had loopback networking available. Server default
bounds, OS worker/model behavior and overload qualification remain incomplete.
