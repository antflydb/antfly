# HTTP/2 atomic writes unlock before weighted reentry

Controlled DATA frames and complete HEADERS/CONTINUATION blocks retain their
connection mutex through physical IO and protocol state updates, then unlock
before returning the execution loan. A deferred wait begins once at the first
socket write and resumes once outside that critical section; DATA resumes between
frames. HPACK encoding and payload preparation run before IO parking. Small fixed
wire/state bookkeeping completes under the IO wait. No stream pointer survives
an unlocked weighted wait. Original controls and uncertain-output retirement
remain intact. Callers must not hold the mutex when using these controlled APIs.

Completed headers are marked published even when weighted reentry subsequently
fails. The server resets an already-started stream on mapped late failures instead
of sending duplicate status headers or leaving it open. A real TCP regression
observes one HEADERS block followed by RST_STREAM(CANCEL), with the connection
still usable. A multi-frame header test validates atomic HEADERS/CONTINUATION
framing and an unlocked denied resume.

Native ReleaseSafe selected 47 HTTP tests: 37 passed, 10 Linux-only skipped.
Pinned Nix/glibc GNU AArch64 passed all 47. Both platforms pass the two selected
owner fixtures, 26 RPC contracts and six descriptor contracts; some unchanged
contracts are reused from the cache as documented in the build summaries.
The supplemental real API owner fixture exercises H1 and H2 original deadline
and cancellation against actual TCP backpressure. Every H2 reentry checks the
mutex is free. Query/control peers progress while output is stalled, retained
bytes stay charged, uncertain output reaches peer EOF, and callbacks/owners drain.
Cancellation waits for a stable in-progress write; the earlier fixture canceled
between completed frames, where keeping the H2 connection alive was correct.
That initial fixture failure and the initial nested-helper compile failure are
preserved. Native hashes were captured after completion and match GNU launches.

The HTTP library component is independently qualified. The supplemental owner
fixture uses proposed ABI 33 runtime callbacks and RPC/owner test dependencies;
supplemental-owner-fixture.patch.gz preserves those changes for replay onto this
component commit. It excludes the unqualified production worker implementation.
Manifest worker/standalone entries are provenance only. Production archive
integration, actual OS model workers, numerical/foreign-team behavior, whole-node
bounds/overload/durability and default enablement remain pending. Pooling remains
disabled. Compiler/container reservations are not runtime memory bounds.
