# Native idle-client qualification

The manifest fixes counts and acceptance thresholds before the candidate runs.
Run against a built native binary:

```sh
python3 zig/qualification/workload-pooling/run_local.py \
  --binary /path/to/antfly \
  --manifest zig/qualification/workload-pooling/macos-small.json \
  --output /tmp/pooling-idle-receipt.json
```

The launcher creates isolated temporary storage and a random authenticated admin,
starts native HTTP and pgwire listeners, measures the process, closes all peers,
and joins shutdown. Passwords are passed through child environments and omitted
from the receipt. Data and server logs remain in the temporary directory printed
in the receipt. It never connects to user deployment data.

The probe records RSS, virtual address size, thread count, numeric file descriptor
count, status latency, idle peaks, post-close recovery, binary SHA-256, configuration
and exit status. It verifies that every offered pgwire startup socket remains open;
connect success alone would conceal immediate listener shedding.

This small profile covers 32 HTTP keep-alive sockets plus 32 pre-authentication
pgwire startup sockets. The recovery allowance includes allocator/runtime caches;
it requires FD recovery but does not require cached worker threads or RSS to drop
immediately. It does not prove per-thread committed stack size, cursor snapshot
cost, authenticated pooling behavior, throughput, Linux/container envelopes, or
distributed recovery. Those need their own fixed manifests and receipts. Do not
use this result to enable automatic sizing or query waiting by default.

The original macOS receipts retain the failed 146 MiB idle-growth measurement.
The shared signal-stack candidate passes the unchanged 128 MiB RSS growth and
recovery limits; the manifest has not been relaxed. See the implementation ledger
and the receipt's binary hash for the measured revision.

Run the authenticated cursor profile with the isolated launcher:

```sh
python3 zig/qualification/workload-pooling/run_local.py \
  --binary /path/to/antfly \
  --manifest zig/qualification/workload-pooling/macos-authenticated-cursors.json \
  --output /tmp/pooling-authenticated-receipt.json
```

This profile creates a relational fixture only in the launcher's isolated
storage. It retains 32 authenticated pgwire sessions and 32 HTTP peers, with 16
cursors held across COMMIT. It verifies exact integers above 2^53 and all remaining
cursor rows, then closes cursors and sessions and measures resource recovery.
Fixture failures produce a failed receipt. Credentials and raw pgwire error
messages are excluded. It covers retained cursors; extended-protocol prepared
portals and slow-consumer overload still require their own performance profiles.

`linux-combined-pressure.json` retains 32 authenticated pgwire peers, eight
cursors across COMMIT, 32 HTTP keep-alive peers and eight unfinished 4 MiB
uploads throughout the open-loop phases. The mixed phases also include slow
response readers and existing-row writes. The profile keeps the original
latency, throughput, RSS, descriptor and recovery gates, requires measured
waiting, and compares three rotated repetitions against main and fail-fast.
Setup rejects cursor errors; verification rejects closed upload sockets,
reconnected HTTP peers or missing cursor rows. All retained clients close before
descriptor recovery is sampled. Declared upload bytes describe the offered
pressure; they are not a claim about actual allocator capacity or total RSS.

Linux default startup also initializes the optional local ChatGPT credential
store. Zig's non-iterable Linux directory handles use `O_PATH`, which cannot be
used with `fchmod`. The candidate opens an iterable handle before enforcing
`0700`. When an older baseline needs the same fix, CI applies
`baseline-linux-startup.patch` and retains the applied diff alongside its build
artifacts. Such results compare against patched main; the patch does not change
pooling policy or performance thresholds. Original startup failures are retained
and cannot be interpreted as latency or throughput measurements.

Older main also runs idle pgwire sessions on the shared outbound API executor.
Those long-lived tasks exhaust its concurrency quota and close later accepted
connections. The candidate uses a listener-owned executor bounded by the
configured connection count. CI applies `baseline-pgwire-transport.patch` when
needed and retains the applied diff; it changes only listener task ownership,
leaving main's SQL admission and allocation policies intact. These are patched
main comparisons. The saturation regression retains 32 sockets while an
independent outbound executor is full, then verifies joined shutdown and port
reuse. Historical failed setup receipts remain failed; fresh measurements are
required to qualify the repaired baseline.

## Resource ledger and operator controls

[RESOURCE_OWNERSHIP.md](RESOURCE_OWNERSHIP.md) records nested versus independent
owners, acquisition order and progress boundaries. Native data/standalone API
allocations now charge the node memory manager used by managed storage/models;
HTTP application output and pgwire heap charge the corresponding API envelopes.
Raw transport buffers, native stacks/TLS and backend mappings still need separate
qualification. A total-process RSS guarantee is not implied.

Authenticated administrators can GET `/admin/v1/workload/query` for configured
and effective query limits, policy version, queue timing, retiring waiter count,
drain/suspension state and allocation envelope counters. PUT replaces a policy:

```json
{
  "expected_version": 1,
  "capacity": 2,
  "waiting": {"max_queued_requests": 2, "max_queued_bytes": 1048576, "max_wait_ms": 100},
  "suspended": false
}
```

Limits may shrink and later return to startup reservations. A stale version
returns 409; increasing finite capacity, queue size/bytes or wait ceiling beyond
startup reservations returns 422. Explicit startup capacity zero remains
unlimited; changing a finite startup capacity to zero is rejected. Queue shrinking
retires newest excess waiters; existing grants survive. Suspension retires queued
work and rejects fresh queries while transaction controls remain available.
Existing waiters keep their original finite wait ceiling/deadline; a replacement
wait ceiling applies to subsequent enqueue operations. PUT
`{"expected_version":2,"action":"drain"}` permanently closes query admission.
Quiescence requires active and queued counts and notified-but-unretired entries
to reach zero; transport/background-owner joins are separate shutdown barriers.
Policies are node-local and reset to configuration after restart.

## Reproducible pressure comparison

`run_matrix.py` runs origin/main (when supplied), candidate fail-fast, and candidate
fixed waiting in independent temporary stores. Candidate fail-fast uses the same
startup heaps and configuration as fixed waiting, with only query waiting disabled. `load_clients.py` records every
offered/submitted/completed/successful/failed/generator-dropped request, zero
retries, successful and all-response p95/p99, resource peaks, control latency and
recovery. Latency starts at the scheduled offer time, including client queueing.
Mixed workloads validate exact query rows, issue writes, and delay reading large
results. The generator has a finite task envelope; dropping offered work fails
its manifest gate. Successful throughput excludes failures and includes drain
elapsed time. A status code without valid rows is not a successful query.

```sh
python3 zig/qualification/workload-pooling/run_matrix.py \
  --candidate /path/to/candidate/antfly --baseline /path/to/main/antfly \
  --manifest zig/qualification/workload-pooling/macos-mixed-load.json \
  --repetitions 3 \
  --output-dir /tmp/fresh-pooling-comparison
```

The Linux profiles require exact enforced cgroup-v2 limits and fail on macOS or
an unlimited Linux host. Build native GNU Linux binaries outside the measurement
container, then run the small profiles with two CPUs, 2 GiB, 512 PIDs and 512 FDs;
the large profile uses four CPUs, 4 GiB, 768 PIDs and 1024 FDs. The Dockerfile and
manual `Zig Workload Pooling Qualification` workflow provide that environment,
build the selected baseline with the same stripped ReleaseSafe optimization and pinned
Nix glibc-linked Zig toolchain, verify compiler version and dynamic linkage, run
the GNU signal-stack fixture, and retain all receipts. Small/large authenticated profiles
also exercise extended-protocol prepared portal suspension/continuation.

Repeated matrices rotate the variant order so each binary/policy occupies every
position. Every repetition retains its own receipts and comparisons; one failed
repetition fails the matrix. The manual workflow uses three repetitions with
ReleaseSafe binaries. Per-phase receipts also separate cheap queries, expensive
queries, writes and slow readers, record server CPU consumed and preserve
before/after operator counters for candidate policies. Operator snapshots occur
outside the timed phase. Per-kind comparison ratios are diagnostic; the original
aggregate latency and successful-throughput gates remain unchanged. The fixed
variant must also report SQL waiting as effective after executing each phase;
startup configuration alone cannot qualify an unavailable worker pool. CI builds
portable x86_64 GNU binaries on the existing heavy build runner, then measures
them on a separate Ubuntu runner in the enforced small/large Docker envelopes.

The separate `macos-waiting-pressure.json` and `linux-waiting-pressure.json`
profiles use one execution slot and offer 750 requests/s during sustained load.
They retain the mixed-load resource and performance gates and require at least
one completed admission wait in that phase. The original 200 requests/s profile
remains a separate measurement: on ReleaseSafe it can measure overhead without
forcing requests to queue. A pressure receipt with no completed waits fails.

The comparison requires identical manifest hashes, enforced limits, offered
work and zero retries. It checks a 25% low-load/recovery p95 regression ceiling
and 90% successful-throughput floor, in addition to each profile's absolute
limits. These are proposed release gates fixed before measurement, not achieved
performance claims. Passing one comparison never enables automatic defaults;
Linux, whole-node ownership and distributed-progress gates remain explicit.
Receipts cannot be overwritten accidentally. `test_harness.py` verifies the
comparison and resource gates fail closed.

The cold and warm macOS load profiles are separate measurements. The warm
manifest explicitly records warm-up operations and uses cheap low-load/recovery
queries; it is not a replacement for a failed cold profile. Historical development
[receipts](receipts/2026-10-06/README.md) retain both outcomes and binary provenance.
The API accounting port shares the actual storage owner in linked composition.
Managed inference and public HTTP transport now share the node accounting owner.
Unmanaged native and independently supplied service allocations retain their owners;
see [the ownership ledger](RESOURCE_OWNERSHIP.md) before interpreting health
counters as a whole-process cap.

Same-binary comparisons reject configuration differences other than waiting and
the ephemeral pgwire bind port. Each new receipt preserves a redacted server log
beside its measurements. Missing idle control samples fail with `null`, never a
passing zero. Final Linux comparisons currently fail sustained throughput; see
[recorded outcomes](receipts/2026-10-06/README.md).
