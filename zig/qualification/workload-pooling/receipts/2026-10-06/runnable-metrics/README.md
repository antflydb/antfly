# Runnable scheduler telemetry verification

Sixteen tests pass on macOS ReleaseSafe and Nix/glibc GNU Linux ReleaseSafe
against the real runtime ABI root: thirteen scheduler fixtures, two telemetry
fixtures and the request-admission import fixture. The Linux test is bounded to
two CPUs and 2 GiB memory, with no swap allowance beyond that memory limit.
Compiler path, version and SHA-256 are recorded from the actual image.

Telemetry distinguishes configured capacity from disabled scheduling, execution
slots from retained metadata, protected metadata ownership from completion
promotion, and live ownership from cumulative grants. Retirement releases live
charges while preserving counters. Metrics use five fixed workload labels.

The separately compiled local pooling shard passes all 31 selected tests,
including canonical runnable-policy parsing and round trips. Its production build
module/sharding arguments were reused exactly, with only compiler-listen mode
replaced by an explicit test artifact and the normal runtime suite filters.
The authored production boundary audit passes for 735 sources.

The data-node/standalone metrics paths and versioned API bridge are connected in
the worktree, but full protocol and runtime-endpoint qualification remain pending.
The prior direct API run aborted in the new fixture because its setup allocated a
retained transaction outside the server allocator; that setup is now corrected.
Neither these focused results nor the local shard qualifies those protocol paths
or automatic defaults. Backend waits, parallel child ownership, broader workload
scheduling and distributed/release qualification remain required for the full goal.
