# Zig 0.17 Evented qualification

`antfly_platform.Evented` selects the local io_uring compatibility backend on
Linux and the local Dispatch compatibility backend on macOS. Production storage
and network defaults remain `std.Io.Threaded`. LMDB's existing
`-Dlmdb_evented_async_io=true` option selects Dispatch on macOS; Linux LMDB still
uses Threaded. The optional enrichment executor supports both tested platforms.

Both compatibility copies preserve Zig's MIT license and record the upstream
source SHA-256. Dispatch adapts the release vtable, keeps timer cancellation state
alive through resumption, and publishes future completion after switching away
from the completed stack. On AArch64 and x86_64 it uses a C ABI assembly switch
to preserve callee-saved registers; the release inline switch corrupted live
values in optimized builds. New fibers retain the upstream entry-message
convention. Allocator protection uses an OS mutex and short group critical
sections use a thread spin lock, since Dispatch callbacks cannot suspend a fiber.
Group awaiting acknowledges parent cancellation after all children finish.
Group child completion leaves the fiber stack before freeing it and waking its
awaiter. Backend teardown also drains completion callbacks when a group token
was already empty at join time. Contended Dispatch mutexes wake the next owner
and release reservations held by canceled waiters.

The strict `zig build evented-enrichment-test` gate covers backend identity,
group bookkeeping, parent cancellation while awaiting `std.Io.Group` children,
sleeping tasks, immediate and delayed cancellation, repeated
concurrent completion, positional file read/write, and file synchronization.
The macOS gate additionally exercises contended stderr locks with canceled
waiters and immediate teardown after group completion with a slow allocator.
The Linux compatibility backend acknowledges timer cancellation after completion
so cancellation of an already-submitted timeout returns `error.Canceled`.
Run it in both debug and ReleaseFast modes. Linux requires a sufficiently recent
kernel and a sandbox that permits io_uring; initialization failures are failures
in this gate. macOS requires libc. The gate is also cross-compiled for Intel
macOS and executed under Rosetta in Debug and ReleaseFast modes. Native Intel
hardware has not been tested; the macOS measurements use Apple Silicon.

Dispatch still lacks complete networking. Its positional file operations call
blocking `preadv`, `pwritev`, and `fsync` from dispatched fibers; this experiment
does not add kernel asynchronous file submission on macOS. Each upstream fiber
allocation reserves approximately 60 MiB for its stack. Debug allocators may
touch that reservation, so high concurrency can consume substantial memory.
These limitations need consideration before broader enablement.

The LMDB async publisher now bounds coalesced writes to the same 1 GiB chunks as
the synchronous publisher. Previously, a large coalesced span could exceed
Darwin's signed syscall size limit and fail even with Threaded. Its regression
test reserves virtual memory without touching pages and verifies chunk sizes,
short-write retries, offsets, and error propagation.

## Comparisons

From `zig/`, build the positional I/O benchmark and both LMDB variants:

```sh
zig build --build-file lib/platform/build.zig io-backend-bench -Doptimize=ReleaseFast
zig build lmdb-bench -Doptimize=ReleaseFast --prefix /tmp/lmdb-threaded
zig build lmdb-bench -Doptimize=ReleaseFast -Dlmdb_evented_async_io=true --prefix /tmp/lmdb-evented
python3 tools/compare_io_backends.py \
  --binary lib/platform/zig-out/bin/io-backend-bench \
  --lmdb-threaded /tmp/lmdb-threaded/bin/lmdb_bench_zig \
  --lmdb-evented /tmp/lmdb-evented/bin/lmdb_bench_zig \
  --output bench/baselines/evented-io-macos.json
```

For Linux on an ARM64 Docker host:

```sh
zig build --build-file lib/platform/build.zig io-backend-bench \
  -Dtarget=aarch64-linux-musl -Doptimize=ReleaseFast --prefix /tmp/io-linux
python3 tools/compare_io_backends.py --binary /tmp/io-linux/bin/io-backend-bench \
  --docker-image alpine:3.22 --output bench/baselines/evented-io-linux.json
```

The isolated Linux benchmark container has networking disabled, a read-only
root filesystem, no capabilities, and a 512 MiB memory limit. It permits
io_uring through its seccomp setting and stores fixtures on an anonymous disk
volume, rather than tmpfs. Remove or replace the ARM64 target for other hosts.

The positional benchmark performs 16,384 permuted 4 KiB reads from a warmed
64 MiB file, or 128 disjoint 4 KiB writes with `fsync` after every write. Both
backends perform equal work at concurrency 1, 8, and 32, with one unrecorded
warmup per case and seven alternating-order samples. Read markers and aggregate
checksums are checked; written blocks are read back and checked in full outside
the measured interval. Fixture creation and backend initialization are excluded
from positional timings; task creation and joining are included.

The LMDB comparison uses the same Zig implementation and `--async-io` commit
policy for both builds. `--kv-only` selects eight durable write/reopen cycles
with 512 inserted keys per cycle. The runner warms both executables, alternates
separate process runs, checks the reported runtime, and retains every phase
measurement. This workload includes LMDB environment lifecycle costs. Read and
write synchronization policy is identical across variants.

Results measure elapsed time, not CPU or peak memory. Warm page-cache reads do
not predict cold-device performance. Docker uses a VM filesystem and host
storage/cache, so these numbers do not establish bare-metal Linux behavior.
`fsync` testing does not establish power-loss recovery or macOS `F_FULLFSYNC`
durability. Positional measurements do not measure complete Lite, LSM, text, or
vector queries; fd-cache and mmap paths often bypass `std.Io` altogether.

## Recorded results (2026-10-05)

Seven samples per case on an Apple M4 Max running macOS 15.6.1, and on an ARM64
Docker VM running Linux 7.0.14-linuxkit with an anonymous disk volume. The
following values are median elapsed milliseconds; a ratio above 1 means Evented
was slower. These results retain all raw samples and code revisions in
[`evented-io-macos.json`](../../bench/baselines/evented-io-macos.json) and
[`evented-io-linux.json`](../../bench/baselines/evented-io-linux.json).

| Platform | Workload | Tasks | Threaded ms | Evented ms | Evented / Threaded |
| --- | --- | ---: | ---: | ---: | ---: |
| macos | cached_read | 1 | 9.363 | 9.796 | 1.05 |
| macos | cached_read | 8 | 12.298 | 12.510 | 1.02 |
| macos | cached_read | 32 | 15.197 | 15.211 | 1.00 |
| macos | durable_write | 1 | 2.333 | 2.261 | 0.97 |
| macos | durable_write | 8 | 3.116 | 3.235 | 1.04 |
| macos | durable_write | 32 | 4.299 | 4.071 | 0.95 |
| linux | cached_read | 1 | 8.561 | 12.145 | 1.42 |
| linux | cached_read | 8 | 2.624 | 3.918 | 1.49 |
| linux | cached_read | 32 | 2.959 | 4.226 | 1.43 |
| linux | durable_write | 1 | 19.038 | 25.556 | 1.34 |
| linux | durable_write | 8 | 8.011 | 9.490 | 1.18 |
| linux | durable_write | 32 | 5.986 | 8.839 | 1.48 |

The macOS file-operation differences are small enough that this run gives no
clear reason to switch defaults. Linux Evented was 42–49% slower for cached reads
and 18–48% slower for writes plus fsync in this VM.

The actual macOS LMDB async-commit workload had median complete roundtrip times
of 19.017 ms with Threaded and 19.539 ms with Evented. Its accumulated commit
phase was 1.682 ms versus 2.137 ms (27% slower with Evented); the publication
phase was 0.876 ms versus 1.005 ms (15% slower). These small, warm workloads show
no measured advantage from broad Evented enablement. Cold storage, batched I/O,
real query latency, memory cost, and recovery still need separate qualification.
