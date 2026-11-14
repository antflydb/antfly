# Production open-file limits

Antfly supports a soft `RLIMIT_NOFILE` of **256 or higher**. The 256 profile is a constrained qualification target, not a production sizing recommendation. Configure **at least 65,536** for production, with a hard limit at least as large. A soft limit of 1,024 is supported, including vector ingestion and overlapping query generations, but leaves less headroom for other services in the process.

Standalone startup rejects limits below 256 and warns below 65,536. It does not silently change an operator's limits. Check the actual server process limit, rather than the shell or Docker host limit. These examples assume Antfly is PID 1 in its container; substitute its actual PID on a host:

```sh
cat /proc/1/limits | grep 'Max open files'
ls /proc/1/fd | wc -l
```

For Docker, set the container limit explicitly:

```sh
docker run --ulimit nofile=65536:65536 ... ghcr.io/antflydb/antfly:latest
```

For systemd, set `LimitNOFILE=65536` in the service's `[Service]` section and restart the service. Kubernetes inherits the container runtime's limits; configure that runtime or the node service and verify the limit inside the actual pod. Raising only the host's hard limit does not raise a container's inherited soft limit.

## Descriptor ownership

Native storage shares one process-wide descriptor admission pool, including ordinary cached reads, temporary file operations, locks, and mapped vector-block positional reads. Its capacity is half the soft limit, capped at 1,024. Public inbound connections independently receive at most one quarter of the soft limit, capped at 256, leaving headroom for listeners, Raft, providers, logs, and recovery operations. The storage pool also reserves headroom for lifetime locks and publication.

An immutable vector mapping pins its inode without retaining a permanent descriptor. Multiple readers of a block share the admitted descriptor cache; idle entries can be evicted while their mappings and active generation leases remain valid. A positional read may reopen a cached pathname only when its device/inode matches the original mapping. Replacement, unlink, or descriptor pressure falls back to the original mapping. Reads never wait for a second descriptor while holding maintenance output permits. Actual positional I/O errors propagate through checked storage callbacks; they do not trigger a mapped-byte fallback.

Publication and recovery still require short-lived descriptors. Raising the limit is useful headroom, not a replacement for admission. Above the 1,024-slot storage-pool cap, reduce simultaneous storage owners or maintenance work if admission remains exhausted; raising the OS limit further does not expand that pool. Monitor storage descriptor admission/cache statistics together with the process's total descriptors: socket, provider, and other process descriptors are not all storage pool entries.

## Resource-pressure errors

Descriptor exhaustion retains `ProcessFdQuotaExceeded`, `SystemFdQuotaExceeded`, `PersistentDescriptorAdmissionExhausted`, or `StorageDescriptorPressure` through storage owner boundaries. Public batch requests report HTTP 503 with `storage_descriptor_pressure`, an open-file-limit diagnostic, and `Retry-After: 1`, rather than an opaque `StorageKernelFailure`. Check process and system limits and current descriptor demand before retrying according to the request's normal outcome and idempotency rules.

The focused regression creates 1,024 immutable vector blocks, reopens them into overlapping generations, reads concurrently with publication of a replacement, checks old-query bytes after unlink and pathname replacement, verifies recovery and shutdown, and exercises combined mapped/ordinary pool pressure. The installed `vector-block-fd-tests` executable can be run in isolated children at soft limits 256 and 1,024 so the compiler and test driver keep their own limits. Run `scripts/ci/zig-vector-block-fd-test.sh` to build and execute both limit profiles; the low-FD CI lane runs the same script.

## Ingestion and latency qualification

`bench_descriptor_pressure.py` exercises the public API and records the binary
hash, corpus digest, batch acknowledgement latency, concurrent self-query
p50/p95/p99/max latency, sampled process FD/RSS use, and visibility after a graceful
shutdown and reopen. Each limit gets a fresh database. The runner does not retry
failed writes or queries. Synthetic rows reproduce the reported shape (768
external cosine dimensions, eight permission fields, one shard, batches of 256,
`full_index`), without claiming Cohere recall or distribution parity.

```sh
make build
uv run --project zig/e2e/antfly python \
  zig/e2e/antfly/bench_descriptor_pressure.py zig/zig-out/bin/antfly \
  --documents 100000 --limits 256 1024 65536 --steady-seconds 30 \
  --output zig/.benchmark-results/descriptor-pressure
```

Only the server child receives the requested soft limit; the client and build
retain their limits. A high-limit control helps distinguish low-limit effects
from ordinary ingestion and maintenance costs. Timings include concurrent
background work on the host and have no machine-specific pass threshold. FD/RSS
sampling is every 50 ms; Linux additionally samples resident `.afvb` mapping bytes
(including deleted generations) about once a second. Sampled peaks are lower
bounds, not quotas. macOS reports process RSS/footprint; per-vector-mapping RSS is
explicitly unavailable there. Query latencies are separated into ingestion and
steady phases. Schema 2 assigns requests spanning a phase boundary to a separate
`transition` bucket; those requests never enter either phase's percentiles. A
phase with no fully contained requests is reported as `null`, including valid
single-batch runs with no ingestion-query observations. `phase_measurements`
retains the full `initial` and `recovery` process lifetimes, from launch before
readiness through shutdown, with FD/RSS and mapping-RSS samples and per-lifetime
peaks. Top-level peaks cover both lifetimes. Sampling failures stop qualification before further batches and wake the steady
wait. Only process-lookup failures after exit end sampling normally; permission
and I/O errors remain failures even when they race with exit.
Logs and partial reports survive failures, including initial startup and recovery
readiness or process-observer failures. Startup diagnostics are copied before
temporary-root cleanup, and diagnostic errors remain secondary to the startup
exception. Visibility polling checks monitor and query health before and after
each status request, uses a five-second HTTP timeout, and uses interruptible waits within its overall
five-minute deadline. Only timed-out read-only status observations are repeated
within that deadline, including response-body read timeouts wrapped by Requests
as connection errors. Other connection and TLS failures propagate; commit, query and explicit HTTP failures remain failures. Diagnostic-copy and cleanup errors
cannot skip server termination or other cleanup, and the primary failure is
preserved. Failed thread startup never joins an unstarted worker; on an operation
failure, cleanup stops the server before its bounded query-worker join. The low-FD CI lane
runs a smaller 1,024-document correctness qualification and retains its reports.

The runner also accepts `--data PATH` for the paper runner's prepared
`vectors.npy` and `permissions.npy` (requires NumPy). Use identical corpus digests
and binary hashes when comparing profiles. This does not substitute for the
original Cohere qualification.

For real model execution beyond the model-free ABI constructor probes:

```sh
ANTFLY_BIN="$PWD/zig/zig-out/bin/antfly" \
  uv run --project zig/e2e/antfly pytest -q \
  zig/e2e/antfly/test_media_chunk_execution.py
```

This uses the shared real-model fixture policy: reuse a complete local
EmbeddingGemma2 bundle, or explicitly enable model downloads through the existing
E2E setting. Both 128-MiB and 1-GiB chunk working ceilings exercise WAV range
transport, decoding, model embeddings, paging and repeated continuation. Model
weights and their separately admitted workspace are not the chunk working budget.

The native-provider counterpart runs without a probe and verifies executed
embeddings for input, page and replay calls, model/source/plan identities, and
admission release at both working ceilings. It also checks rejection at a
one-byte node ceiling. Supply an existing audio-capable EmbeddingGemma2 bundle;
unset model input explicitly skips the optional test:

```sh
cd zig
ANTFLY_NATIVE_CHUNK_EXECUTION_MODEL=/absolute/path/to/embeddinggemma2-bundle \
  python3 tools/run_bounded_zig_build.py --zig zig -- build \
  inference-native-chunk-execution-test
```

### Recorded synthetic qualification

The [schema-2 100,000-document run](../../zig/bench/vectors/descriptor_pressure_100k.json)
passed at all three limits with identical corpus digests, concurrent self-queries,
full recovered visibility, and successful graceful exits before and after reopen.
It supersedes the earlier summary whose phase attribution and recovery sampling
were incomplete. Resource peaks below include both complete process lifetimes;
query percentiles exclude intervals crossing either phase boundary.

| Soft limit | Ingestion seconds | Overall peak FDs | Recovery peak FDs | Overall peak RSS (MiB) | Ingestion query p99 (ms) | Steady query p99 (ms) |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 256 | 674.3 | 205 | 186 | 822.3 | 480.7 | 101.4 |
| 1,024 | 687.0 | 544 | 332 | 739.9 | 474.1 | 103.2 |
| 65,536 | 658.5 | 711 | 331 | 759.8 | 445.1 | 64.9 |

These are single ordered observations on a shared macOS ARM64 host with warm OS
caches and a fixed self-query, not evidence of a speed advantage for any limit.
Process RSS includes the index, mappings and other server allocations; it does
not isolate vector mapping residency on macOS. The JSON pins the measured engine
and qualifier commits/hashes and retains transition-query distributions and
per-lifetime sample counts and peaks. Later changes strengthen error handling and
fail-fast behavior without changing the successful-run measurement semantics.
The Linux CI runner additionally records per-mapping RSS for its smaller corpus.
