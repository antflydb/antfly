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
