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
