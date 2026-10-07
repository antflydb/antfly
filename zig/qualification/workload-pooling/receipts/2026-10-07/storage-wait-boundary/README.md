# Checked native storage execution-wait boundary

The common wait boundary carries borrowed runnable ownership through the checked
native callback ABI. It validates method/signature/layout/version before calling
an independently generated owner, transports stable C status rather than native
Zig error integers, and preserves caller-owned result cleanup after failed resume.
It is exported by both source catalogs. This primitive is not yet installed in
storage/RPC/read-index wait paths; existing SQL private spill hooks keep their
separate local interface until integration.

Scheduling errors have append-only stable details, preserving all older numeric
IDs and distinguishing timeout, unavailable, retryable, internal transition and
unsupported-wait classes. Both resume hooks preserve deadline/cancellation,
policy/catalog revocation and allocation errors. An unsupported provider must
not silently treat a whole CPU-executing backend call as a resource wait.

Run the reproducible qualifier from either native or pinned Nix/glibc Linux:

```sh
python3 zig/qualification/workload-pooling/check_execution_wait.py \
  --zig /path/to/zig --output-dir /tmp/unique-checked-wait-receipt \
  --cache-dir /tmp/zig-cache --global-cache-dir /tmp/zig-global-cache
```

Add `--target aarch64-linux-gnu` for this Linux profile. Output directories cannot
be reused. The qualifier separately compiles a callback owner object, links it
into the consumer test root, runs exactly three wait fixtures, then all 25 error
ABI fixtures. The independent fixture asserts that the returned dispatcher has a
different address from the consumer's local dispatcher; both forced and true
independent C paths are exercised. It checks exact error identity, invalid owner
layout/version rejection, and caller heap cleanup when the other owner rejects
resumption. The owned-result fixture covers successful transfer, failed begin,
failed I/O, failed end, and combined I/O/end failure.

All three stages pass on macOS Zig 0.17.0 ReleaseSafe and pinned Nix/glibc Zig
0.17.0 GNU AArch64. Linux uses two CPUs, 1 GiB and no additional swap. The exact
nine source hashes match on both platforms and are rechecked after execution;
owner artifact hashes and commands are in adjacent receipts. The explicit
production boundary audit includes common/execution_wait.zig and covers 738
authored sources with no server coordination imports. This qualifies the
native wait interface and append-only error transport, not real storage wait
suspension, parallel-child use, remote attempt completion, defaults, or the full
design. The boundary borrows one operation's stable execution owner and may not
be shared concurrently by unscheduled children.

The next integration must propagate request-local wait authority to exact sleep,
event or socket waits, retain CPU capacity for preparation/decoding, reacquire and
validate before consuming results, and preserve original deadlines and read-index
request identity. Accepted durable work keeps its existing completion owner.

The repository commit hook reformatted the qualifier. Both complete qualifiers
were rerun after that change; the adjacent receipts match the committed runner
and all eight other sources exactly.
