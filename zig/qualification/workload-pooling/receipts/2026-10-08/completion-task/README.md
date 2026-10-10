# Preauthorized joined completion task

Task.reserve allocates its gate and context from the caller's bounded concurrent
allocator, reserves actual Io.concurrent capacity before work can start, and
reserves completion on the same admitted execution. Unsupported capabilities,
malformed descriptors, different executors, exhausted metadata/thread capacity
and expired foreground reservation all fail before physical work. There is no
async or synchronous fallback. Failed reservation cancels and joins the gated
task before releasing its metadata. An unactivated task can be abandoned safely.

runAndJoin checks actual foreground cancellation and activation fences before
releasing the gate. The independent task checks its retained completion fence
before the callback. The foreground join blocks cancellation propagation into
that task and restores its prior cancellation protection after joining. Callback
errors remain exact. The caller retains execution, callback context, allocator
and executor through deinit and finishes the root only after that joined cleanup.
The callback must join all its storage children or complete an authoritative
recovery transfer before returning; the primitive does not prove that obligation.

The preserved negative regression removes join protection and reproduces lost
completion work: one test passes, the cancellation test fails (expected callback
count one, found zero). Both final named task contracts and all 32 SQL-team,
owner/inference, worker/output and public cleanup regressions execute and pass on
native macOS and pinned Nix/glibc GNU AArch64: 34 tests per platform, no skips.
The foreground cancellation test uses two actually bounded concurrent tasks and
an actual cancellation point in the independent completion task, not a mocked
cancellation flag. It also permits health progress, expires the foreground fence,
closes admission and verifies retained metadata/root ownership until completion
and join. These tests perform no physical storage write.

Task metadata is bounded here; executor-internal future/thread allocations still
need whole-node accounting. SQL sealing/participant acceptance has not been moved
into this callback yet. Real storage/quorum wait fences, ambiguous outcomes and
authoritative durable recovery transfer remain required. No production caller
uses this primitive or activates completion yet; write suspension is unsupported
and pooling remains disabled by default. Full archive qualification remains
pending. The supplemental proposal is relative to e544c94a6b plus this commit's
primitive and build registration, and contains both process fixture files.
