# Actual HTTP cleanup borrows shared protected execution

HTTPx now accepts a frontend-local cleanup capability. Error responses and late
stream resets acquire a fresh, fail-fast protected control capsule before
formatting or taking the connection mutex. The shared ABI 3 owner enforces the
separate operation/byte subquota and 250ms total deadline. Socket, mutex and
credit waits park CPU; fair reentry happens after the frame lock is released.
Admission/reentry failure retires the socket before owning finish. The adapter
retains its owner lease until the listener and all writers join. Standalone
wiring remains supplemental pending the full production build.

Real public-listener/TCP tests use the actual checked shared owner with ordinary
CPU capacity occupied. H1 sends complete retryable 503. H2 starts with zero peer
credit, sends headers without DATA, acknowledges PING while cleanup is parked,
and closes on the bounded cleanup deadline before releasing the capsule. Park
checks prove retained accounting and cleanup exclusion while health roots run;
start/reentry checks prove the connection mutex is free. These are transport and
owner tests; the foreground reservation is not a complete SQL/model request.

The listener exposed a real flow-control defect: new H2 streams ignored the
peer initial window and started with 65535 bytes of credit. StreamManager now
remembers the peer initial send window for both incoming and local new streams.
A regression checks zero credit, explicit credit, and delta changes after bytes
have been consumed. Preliminary failing listener receipts are preserved.

Native passes 233 of 246 selected transport/server tests with 13 platform skips;
pinned Nix/glibc GNU AArch64 passes all 246. Owner/listener, worker handoff, actual
subprocess output/reaping, RPC and descriptors pass all 55 tests on each platform.
Native and GNU exact source hashes agree. Earlier Darwin backpressure fixture
failures are preserved; bulk filling before single-byte filling settles delayed
ACK credit without changing the actual 250ms cleanup deadline or assertions.

This does not qualify full standalone production, actual Worker.Client/model
execution, whole-node accounting, durable completion, or default enablement.
Other frame-pump resets/GOAWAY/watchdogs and classifier ownership still require
audit. Pooling stays disabled. Compiler reservations are not runtime bounds.
