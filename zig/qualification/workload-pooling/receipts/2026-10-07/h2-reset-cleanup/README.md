# Bounded HTTP/2 reset cleanup

Cancellation cleanup uses its own 250ms monotonic budget across mutex acquisition
and the fixed 13-byte RST_STREAM(CANCEL). It protects cleanup from task
cancellation without reacquiring an expired foreground grant. If the connection
mutex cannot be acquired, it shuts down the socket without mutating stream state
outside the lock. After acquiring the mutex it resets local stream state before
sending; a failed reset shuts down the socket and wakes the connection owner.
The remaining-budget calculation saturates across a clock tick.

Native ReleaseSafe selects 51 HTTP tests: 40 pass and 11 Linux-only tests skip.
Pinned Nix/glibc GNU AArch64 passes all 51 tests. Focused regressions cover a held mutex,
actual task cancellation, a stalled native socket and Linux fallback IO with no
async allowance. Successful reset framing and late published-header behavior
remain covered by the existing HTTP/2 tests.

The first stalled-peer fixture filled the socket using large writes. On Linux,
TCP could accept the tiny reset after a delayed acknowledgment, so the assertion
that cleanup must consume the timeout was invalid. A second reset-sized fill also failed: smaller frame fragments could still
fit. The final fixture bounds both TCP buffers and fills single-byte writes until
that write times out. The original native pass and both Linux failures are
preserved; these changed the fixture, not cleanup. The initial single-byte
fill bound was sufficient on Linux but too small for macOS. Its native failure
and Linux pass are also preserved; the final bound allows up to 1 MiB of one-byte writes. macOS reports a
roughly 514 KiB receive buffer despite the requested 4 KiB; the final fixture
sets the inherited window before the handshake and can fill that larger buffer.
The credit-wait fixture also now probes mutex availability only before publishing
its first wait: later probes could race a legitimate peer window update.

This is bounded transport housekeeping, not independently admitted control CPU.
Production archive integration, actual OS model-worker output, numerical/foreign
teams, whole-node resource/overload/durability qualification and enabling pooling
by default remain required. Runtime/standalone hashes are provenance only for
this focused HTTP gate. Compiler/container reservations are not runtime bounds.

Final native stalled reset took 251ms after 637965 single-byte prefill bytes.
Native hashes were captured after completion and match the GNU launch hashes.
