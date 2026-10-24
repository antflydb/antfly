# Initial HTTP error output has a separate bounded cleanup budget

HTTP/1 and handler HTTP/2 rejection responses use one 250ms monotonic deadline
across formatting, mutex acquisition, flow-control credit and physical writes.
Cleanup blocks task cancellation while it completes its bounded attempt. Failed
or uncertain output retires the socket; successful HTTP/1 callers close their
connection as before. HTTP/1 uses a 2KiB fixed local allocation buffer instead
of the application heap. HTTP/2 response headers use a fixed local header array;
HPACK still uses the transport allocation owner.

Handler HTTP/2 cleanup releases the connection mutex while waiting for credit,
allowing sibling control frames to progress. Receive-pump rejection already owns
that mutex: it attempts the fixed error immediately without waiting for credit.
If credit is insufficient after headers, it resets only that stream. Error output
and unread-request reset share one deadline. The oversized-body pre-dispatch
regression now observes actual TCP, decoded HPACK, DATA and its terminal reset.

Final native ReleaseSafe selects 162 tests: 149 pass and 13 Linux-only tests skip.
Pinned Nix/glibc GNU AArch64 passes all 162. This includes all 119 server tests,
protected-dispatch and memory-exhaustion cases, all selected socket/H2 controls,
and new actual-cancellation, held-mutex, exhausted-credit and stalled TCP tests.
Native hashes were captured after completion and match Linux launch hashes.
Runtime/standalone hashes are provenance only for this library gate.

The initial 61-test native gate passed; GNU failed one fallback HTTP/1 stall
expectation because the small response completed after delayed TCP progress.
The final fixture confirms five consecutive 100ms single-byte timeouts before
trying cleanup. Preliminary 63-test gates passed both platforms. The expanded
server gate first found the old in-memory writer fixture at the new concrete
socket boundary; it was converted to actual TCP. All preliminary results and
failures are preserved, without replacing the final exact-source gate.

This bounds transport cleanup, not independently admitted control CPU. Control
execution ownership must be admitted outside connection mutexes, and retained
through parked waits and physical cleanup. Full production/API qualification,
actual model workers, stuck-reap quarantine, durability and whole-node/default
enablement gates remain required. Pooling remains disabled. Compiler/container
memory reservations are not runtime resource bounds.
