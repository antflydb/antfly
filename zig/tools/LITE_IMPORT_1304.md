# Lite import qualification for issue 1304

The branch started at `origin/main` commit `031742c340` and was rebased onto
`6647f3ce7b` while the fixes were being qualified. The issue and both
comments describe several different generations of the importer and database.

## Findings and ownership

| Finding | Current behavior and change |
| --- | --- |
| Conflict resolution opens every table per statement | Main already resolves standalone arbiters against the selected table and reads Lite dependency metadata without opening unrelated runtimes (`capi/sql.zig`). |
| Managed-index LSM WAL checkpoint on every SQL commit | Main already coalesces asynchronous applied-sequence publication. The physical checkpoint remains at its required durable publication/capture boundary; this change does not weaken that ordering. |
| A maintenance turn repeatedly scans `liveStats` | Main already gates exact size assessment by growth and reclamation activity and retains cached assessment status. |
| First authored write runs a whole-file allocator proof | Native connection owners now append while their detached proof is pending. No unverified free page can be consumed. Ordinary manually owned files retain synchronous verification. |
| Foreground writes invalidate a completed detached proof | Capture records the originating header. Locally authored append-only descendants can inherit its proof. Adopting a foreign header, replacing the inode, or an uncertain publication rejects inheritance. |
| Short-lived processes repeatedly abandon the verified free pool | Clean native owner shutdown publishes an opaque metadata certificate binding both recovery slots and the complete canonical allocator ledger. Cold reopen checks it before opening index sources. Only an already verified writer can publish it. Stale certificates fall back to detached verification and append-only admission. |
| Both recovery slots reread identical payloads | The active graph records checked immutable leaf bounds. The fallback still validates its own ordering, duplicate references and page limits, while reusing eligible payload checks. Exact active ownership, pending retirements and CRC checks remain mandatory. |
| Normal exit leaves lease markers and retained runtimes | Proof readers now borrow allocator/executor lifetime separately from mutating maintenance captures. They do not force a fresh foreground runtime on every request. Their last release immediately collects retired runtimes. Detaching the artifact registry expires idle cached descriptors while preserving explicit readers and active uses. A bounded marker sweep runs before the detached proof, including turns canceled before the proof completes. |

A proof is still an integrity traversal, and an external writer invalidates a
captured generation. The clean-close certificate hashes both recovery slots,
allocator roots, every ownership count and each ordered pending retirement with
BLAKE3. Reopen loads the CRC-checked ledger and matches the complete digest; no
file-size, timestamp or sampled-page shortcut is used. Explicit integrity checks
still traverse the graphs. Certificate publication is one regular metadata
checkpoint at owner shutdown, never a checkpoint per SQL statement. It changes
neither the wire format nor old-reader compatibility: an older writer's next
publication simply invalidates the certificate. Interrupted publication preserves
the previous committed prefix; an unpublished tail uses ordinary recovery.

A legacy file without a valid certificate can append while its detached proof
runs. If that process closes before verification completes, it cannot certify
that state. The append window remains bounded by the configured file-size cap.

## Source volume is not the cited JSONL size

The Codex importer materializes `ThreadPlan.chain`, not just the cited rollout
file. Its `--thread` argument also selects the current thread tip from the state
database. Reading the live home during this investigation selected a newer tip
and produced a dry-run count of 244,034 items; that count changed while the thread
continued. The resulting import contained 244,109 raw history rows. These live
runs are diagnostic observations, not paired performance measurements.

A frozen home was then made from copies of the issue's **specified October 4
rollout**, its ancestors, and the 15 supplied thread tips. It contains 25 rollout
segments totaling 503,498,968 bytes across the 15 threads. With the specified
rollout selected as the first thread's tip, the importer materializes **25,303
items** for that thread. Benchmarks must freeze both the chosen tip and its
lineage before comparing builds.

A read-only census of the supplied 9.7 GB file found approximately 1.064 GB of
primary document payloads, 357 MB of packed relational row payloads and 199 MB of
index artifacts. `live_file_count` counts internal live records, including
identity, revision and relational support records; it is not an operating-system
file count or an index-artifact count. Lease aliases can reference the same
artifact repeatedly, so summing their logical value lengths is also not a
physical-size measurement. A sample of 200 large primary values contained 200
different complete payloads. No payload-sharing or format change is included.

## Reproduce the process workload

Use a dedicated directory for each output database and the same frozen home,
importer executable, library build settings and ordered thread list for both
builds:

```sh
python3 scripts/bench/lite_import_processes.py \
  --importer /path/to/codex-antfly-import \
  --library /path/to/libantfly.dylib \
  --codex-home /path/to/frozen-home \
  --database /path/to/new-run/session.aflite \
  --thread FIRST_THREAD --thread NEXT_THREAD
```

The harness launches one process per thread, including open/close time in its
measurements. It reports file growth and both total and unlocked UUID lease
markers. Importer output stays in adjacent local log files. `--append` measures
additional processes on an existing file. Private rollout contents and generated
databases are not repository fixtures.

## Validation

The allocator/reclamation gate passes 73 tests, including cross-process certificate
reuse, changed recovery headers, altered ownership counters and interrupted
certificate writes. The targeted proof and lease gate
passes seven tests, including corrupt values, incorrect positive reference counts
and denied optional read-window admission. The new owner-detach gate passes all
six lease tests, including active reads against a retired inode after Store close.
The initial C ABI gate passed all 170 tests, including three focused SQL/maintenance
contracts; the final close-path changes are also exercised by a repeated C ABI run.

The supplied `55d97ce423` library imported the frozen original-rollout case in
34.364 seconds including startup/shutdown (33.7 seconds reported by the importer).
It produced a 354,414,592-byte file and left three unlocked UUID lease markers.
The earlier live-tip diagnostic took 1,045.8 seconds in the importer, produced a
roughly 4 GB file and retained 244,109 raw / 78,611 normalized rows. These inputs
are different, and those timings must not be compared as a speed improvement.
All local timings were measured with other builds active on the machine.

On an APFS clone of the supplied 9.7 GB file, the supplied baseline library took
1.256 seconds to open and 143.954 seconds for `CREATE TABLE` as its first authored
statement; open/write/close took 145.438 seconds. The clone was isolated from the
original database and used the same Python C ABI caller as the candidate probe.
