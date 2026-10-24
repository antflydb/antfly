# Retained execution reservations survive denied resumes and actual reaping

A published execution now uses waitRetained at cooperative yields and physical
wait reentry. Failure releases runnable/queue/block state but retains the entry's
operation and byte reservations until its original owner calls finish. Initial
unpublished acquisition retains its existing failure retirement. This prevents
an expired or canceled borrowed worker from silently freeing outstanding capacity
before consumer cleanup/reaping. No ABI change is needed for this scheduler fix.

Native and pinned Nix/glibc GNU AArch64 pass all 26 scheduler/admission tests.
New regressions cover canceled/deadline/queue-timeout reentry and cancellation
just after a cooperative yield grants a peer. Stopped entries keep outstanding
capacity charged, reject excess fresh work and drain on owning finish.

Both platforms pass the real owner, pipe handoff and subprocess/output gates:
native 16/16 build steps, GNU 15/15, with 16/16 executed tests on each. Unchanged
RPC/descriptor contracts are reused from cache as recorded in summaries. The
subprocess test runs four schedules: HTTP/1 and HTTP/2, each with original
cancellation and deadline expiry. The child executes production acknowledged
RPC using real inherited pipes and emits deterministic bounded chunks. Actual
TCP backpressure parks the real API capsule, permits query/control peers and
retains the complete recorded reservation before AND after actual child.kill
reaping. Only then does the original caller finish. Uncertain output reaches
peer EOF; callbacks, operations, retained bytes and runnable slots drain.

The initial subprocess fixture exposed zero outstanding ownership at reaping
because failed reentry called Scheduler.finish. Exact native/GNU failures are
preserved. The former broker test expected that premature retirement; it now
requires the parent reservation until its caller finishes. Early build-path and
fixture assertion failures plus diagnostic/first-pass results are also preserved.
Final hashes match both platforms; the broker test-root hash was captured after
completion while source remained unchanged.

The scheduler component and broker expectation are committed independently.
supplemental-process-fixture.patch.gz preserves the new subprocess fixture,
build step and its proposed ABI 33/RPC dependencies for replay onto this commit.
It excludes the unqualified production worker/standalone bodies. Recorded hashes
of those bodies are provenance only. This is an actual OS subprocess with the
production RPC, not the production Worker.Client startup or a numerical model.
It does not qualify native/foreign model teams, bounded stuck-process quarantine,
full current API/archive integration, protected durability, whole-node overload
or default enablement. Pooling remains disabled. Compiler/container reservations
are not runtime resource bounds.
