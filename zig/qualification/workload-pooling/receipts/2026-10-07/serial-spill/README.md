# Serial spill verification

The blocking continuation fixture passes on macOS ReleaseSafe and the pinned
Nix/glibc GNU Linux ReleaseSafe toolchain. One named fixture covers six scenarios:
sorting, grouped aggregation and window continuation, each with serial spill I/O
and parallel execution. It checks bounded delivery, retained memory, cancellation
and cleanup. The Linux container uses two CPUs, 2 GiB memory and no additional
swap allowance.

Scheduled SQL owns serial spill I/O independently of parallel CPU execution.
Manager-controlled submissions decline parallel work when that capability is
disabled; sorting, grouping and partition joins follow the same gate.

The initial full native SQL corpus passed 263 of 264 tests with no leaks. Its
exhaustive pull-stream allocation test failed with NondeterministicMemoryUsage
and reproduced in isolation on macOS. The same isolated test passes on Linux.
The initial native compiler peaked at 1,928,757,248 bytes, exceeding its old
1,610,612,736-byte build scheduling estimate. The compiler estimate is corrected
to 2 GiB on macOS; runtime memory limits are unchanged.

The API suite is still building. Corrected exhaustive SQL checks, protocol
qualification, independent parallel-child ownership, genuine backend wait
suspension and default qualification remain outstanding. Initial source hashes
and failures are preserved separately from subsequent verification.

## Subsequent verification

The corrected pull-stream fault sweep now uses allocate/copy growth with the
same backing ownership checks. All 264 local SQL tests pass on macOS without
leaks. The server SQL shard passes 117 of 118: its plan-cache eviction fixture
hits a compile limit after allocator growth changes during churn. A standalone
reproduction fails at iteration 13 with ordinary growth; deterministic growth
passes all 30 acquisitions, performs 28 evictions and peaks at 5,200 bytes. Both
runs keep the original 8,192-byte cache bound and 4,096-byte compile bound. That
fixture correction is pending until the live API compilation releases its inputs.

The full Linux SQL target passes 264 local and 118 server tests. Its initial
compiler exceeded the old 1.5 GiB estimate before the partition retry; the measured
1,947,938,816-byte peak requires a 2 GiB compiler estimate on Linux as well. A
clean rerun with that estimate succeeds in all nine build steps, reruns all 264
local tests without leaks, and reuses the verified 118-test server shard. Runtime
limits remain unchanged.

The native API pooling target passes all 33 local and 38 API tests, including the
restored ordered pgwire cursor lifecycle. It reports an eight-byte leak from the
new protected rollback fixture, whose wire metadata used a raw allocator instead
of the enclosing caller arena specified by pgwire Result ownership. That fixture
now supplies an arena and releases its native result before the arena. The full
API rebuild is still running; the leaked initial result remains failed.

## Corrected API result

The compiled corrected API artifact passes all 38 selected API tests with no
skips, failures or leaks when loopback sockets are enabled. The full build's
33-test local shard also passes without leaks. The first run of the corrected
artifact inside the filesystem/network sandbox failed two real-socket fixtures
and skipped the transaction socket fixture; those results remain preserved.
The permitted direct rerun uses the exact compiled artifact and original suite
filters, and confirms the caller-arena fix, real HTTP/H2 and pgwire queueing,
protected controls and ordered cursor lifecycle. This is native API qualification,
not full Linux protocol, metrics-endpoint or automatic-default qualification.

## Corrected native SQL result

All 264 local and 118 server SQL tests now pass without leaks in the full native
SQL target; all ten build steps succeed. The plan-cache fixture uses deterministic
allocate/copy growth and retains its original 8,192-byte cache and 4,096-byte
compile bounds. Production cache limits are unchanged. The API artifact above
preceded this test-only plan-cache correction; its production code is unchanged.

## GNU Linux protocol result

The isolated snapshot at a19cf06f23 compiles with pinned Nix/glibc Zig and its
local pooling shard passes all 33 tests. The API artifact passes all 38 tests
without skips, failures or leaks in a separate two-CPU, 2 GiB runtime container
with no additional swap. This snapshot precedes the private spill wait hooks.

The initial full build remains failed: its compiler peaked at 24,105,984,000
bytes, above the former 14 GiB compilation estimate, and its read-only source
mount prevented std.testing.tmpDir from creating .zig-cache scratch. The direct
runtime rerun retains the original artifact and suite filters while providing
a writable scratch mount. The current Linux compiler estimate is 26 GiB based
on that observation; runtime limits remain 2/4 GiB. Compiler and runtime gates
are recorded separately rather than treating the initial build as green.


The later private-spill-wait qualification supersedes the 26 GiB scheduling
estimate above with 23 GiB after the 26 GiB estimate was rejected before
compilation because it exceeded available VM memory. The isolated wait-hook
snapshot then passes the full Linux API target and both exact artifacts pass
at the unchanged 2 GiB runtime limit. See the [spill-wait receipt](../spill-waits/README.md)
for the rejection, successful build, and runtime logs.
