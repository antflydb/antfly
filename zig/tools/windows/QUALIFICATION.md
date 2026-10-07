# PR #987 follow-up qualification

The review branch merges PR head `ce319462af8b` with main `e6d4ce9bbc71`.
During qualification, `origin/main` advanced to `b79388a7d6`, including the
embedded-source layout refactor. These results remain pinned to the merged
`e6d4ce9bbc71` snapshot; the newer main layout is not included in this branch.
Follow-up source commit `99003f02a2` replaces whole-output Windows LSM buffering
with bounded staging and flushes published Windows files after rename.
Commit `4cc39651e3` extends staging to the production borrowed-executor path.

## Memory and publication checks

The staging writer uses a 64 KiB buffer and a 64 KiB CRC scratch buffer. Its
integration test publishes an 8 MiB output through an 80 KiB fixed allocator,
checks replacement visibility and abort cleanup, and retains owned I/O runtimes
after storage shutdown. The runtime bridge's borrowed `IoStorage` also uses
bounded staging on Windows; its executor must outlive the sink.
Boundary-crossing header patches and CRC ranges pass.
A truncated staging read prevents subsequent append and publication. A mocked
Windows publication flush fails observably and still closes its handle.

Native macOS storage tests: 36 passed, 1 Windows-only test skipped. Windows
ReleaseFast storage tests: 16 passed, 21 POSIX tests skipped, including the
borrowed-executor bounded-write and authority checks.
Staging: 2 passed on both platforms.
Object publication: 1 Windows test passed, 1 POSIX test skipped; the macOS
objectstore suite passed 77 tests with 2 skips before the Windows-only test
was added. CrossOver executed the Windows binaries successfully.
A separate diagnostic with 1,000 thread-pool dispatches and eight background
sleepers passes under CrossOver; it does not reproduce the HTTP stall.
Two diagnostics pass 1,000 dispatches/completions through the actual
executor bridge between separately compiled Windows archives, with deadline
cancellation and either the polling or protected completion wait. Both pass
on native Windows as well as CrossOver.

Commit `deefe3fb98` changes HTTP batch completion to block on the future instead of polling
every millisecond. Cancellation protection prevents `Future.await` from
forwarding caller I/O cancellation into mutation work before its durability
is known; the request token still controls the batch's safe visibility waits.
Its cancellation regression passes on macOS and CrossOver and fails when
the protection is removed. This change alone does not establish the cause
of the sustained-write stalls.

## Native Windows reset checks

The disposable GCE runner uses `windows-server-2022-dc-v20260909`, Windows
Server build 20348, an NTFS boot volume, an e2-standard-4 machine and an 80 GB
pd-balanced boot disk. It has no external IP; HTTP is reached through IAP.
Compatibility, staging, storage and publication-flush executable exit codes
are all zero on native Windows.

The Debug application SHA-256 is
`9bd35fcb285d97c1375c3aaa6475fcc51131cb6b4dca6d424ee8340507b164df`.
It runs Lite with `--fsync true`. A separate macOS controller fsyncs a JSONL
ledger only after successful `full_index` responses, then verifies each body
hash and full-text entry after `gcloud compute instances reset`.
This is a [hard reset](https://docs.cloud.google.com/sdk/gcloud/reference/compute/instances/reset),
without a graceful guest shutdown.

| Run | Requested payload bytes | Acknowledged writes | Recovery |
| --- | ---: | ---: | --- |
| 1 | 4096 | 13 | All bodies and search entries survived |
| 2 | 4096 | 12 | All bodies and search entries survived |
| 3 | 1024 | 17 | All bodies and search entries survived |

The first two reset tests followed a stalled request. The third reset
interrupted an active stream. An interrupted request may commit without being
acknowledged; the verifier deliberately permits that. The first ledger was
also verified again after the second reset.

An offline native `lite check` after the recovery runs exits zero:
`valid: true`, file size and valid prefix both 1,429,504 bytes, zero tail bytes,
506 records and no reported issue.

Local evidence is retained under `/private/tmp/antfly-pr987-ntfs-results`:
the three acknowledgment ledgers, binary manifest, recovery-results JSON,
Windows minidumps and symbolized stacks. These contain only generated test
data. The Debug application also passes the standard CrossOver HTTP smoke:
64 concurrent queries across hard kill/reopen, followed by `lite check` with
`valid: true` and zero tail bytes.

## Updated native Debug run

With bounded borrowed writes and protected batch completion, the Debug SHA-256
is `5615babfd7fd3e345220275beb6c24037375c2068b1ebe0b92e5cafe4d9363c8`.
All eight focused native executables exit zero, including the borrowed writer,
cancellation regression and both executor-bridge diagnostics.

A 4096-byte stream reached 31 acknowledgments without a timeout before an
active-stream hard reset. All 31 bodies and full-text entries verify after
recovery. Offline `lite check` exits zero with `valid: true`, file size and
valid prefix both 1,949,696 bytes, zero tail bytes, 781 records and no issue.
The standard CrossOver smoke also passes again with 64 queries, hard-kill
recovery and zero tail bytes.

## Executor ownership fix

Commit `557c43a153` fixes the batch offload's executor selection. The handler
used a raw `BackendRuntime` pointer to rebuild a `Threaded` vtable in the API
archive, bypassing the already imported durable executor. On Windows, the
archives have separate parked-worker wakeup and thread-local state. Dispatch
and completion must call the owning executor through the imported view.
The existing selector also preserves intentionally unavailable imported views.

A reconstructed-vtable diagnostic hangs under CrossOver. A new regression
compiles its owner and borrower separately, parks the owning workers before
64 dispatches, and awaits every completion through the borrow. It passes on
macOS, CrossOver and native Windows. The earlier diagnostics kept background
workers active and did not expose the parked-worker case.

The fixed Debug application SHA-256 is
`bb9ab2b2b64e3ef0a1a26b3a87c05975893ae73e292c2bbedee4380408cc4b72`.
Its CrossOver extended smoke completes all 64 indexed writes, verifies all
64 body hashes and full-text entries after hard-kill recovery, runs 64
concurrent queries, and passes `lite check`: valid prefix/file size 1,699,840,
zero tail bytes and no issue. The full Debug build passes 46/46 steps.

A larger CrossOver run completes 1,000 indexed writes and verifies all 1,000
bodies and full-text entries after hard-kill recovery. Its final offline check
is valid with file size/prefix 14,229,504 bytes, zero tail bytes and no issue.

The native fixed Debug run completes 149 acknowledgments without a timeout
before an active-stream hard reset. All 149 bodies and full-text entries
verify afterward. All nine native executables exit zero. Offline integrity
checking exits zero with valid prefix/file size 4,571,136, zero tail bytes
and no issue. See `status-owner-debug.json`, `acknowledged-owner-debug.jsonl`
and `check-owner-debug.json` in the retained evidence directory.

## Final optimized build

The executor-fixed ReleaseFast application SHA-256 is
`38684ad67827ae13442d55cda905964374c1c3b6eee91d0a34e62ea1c8035e6e`.
The complete build passes 46/46 steps. Its CrossOver workload completes 1,000
indexed writes and verifies all 1,000 bodies and full-text entries after
hard-kill recovery. All 64 concurrent queries pass. Offline integrity checking
is valid with prefix/file size 14,360,576, zero tail bytes and no issue.

The native ReleaseFast run reaches 141 acknowledgments without a timeout
before an active-stream hard reset. All 141 body hashes and full-text entries
verify afterward. All nine focused executables exit zero on the new boot.
Offline `lite check` exits zero with `valid: true`, file size and valid prefix
both 6,557,696 bytes, zero tail bytes, 3,072 records and no issue. The retained
evidence includes `acknowledged-owner-release.jsonl`,
`status-owner-release-recovered.json`, `verify-owner-release.log` and
`check-owner-release.json`.

Before this fix, the consistent ReleaseFast binary
`83d377bc2f8ff1d6069cef34a2e6d1ded71762cde89132c3ae02326d2cf2e071`
stalled after 8 acknowledgments natively and 4 under CrossOver. All 8 native
acknowledgments survive its hard reset; offline integrity checking exits zero
with valid prefix/file size 2,150,400, zero tail bytes and no issue. Its native
snapshot has the HTTP handler awaiting batch completion, idle executor
workers, and metadata/maintenance threads waiting on the Lite store mutex.

## Cleanup

The disposable VM, auto-delete boot disk, artifact bucket, service account,
IAP firewall rule, subnet and network were deleted after the final integrity
check. Resource listings confirm their absence. Both local IAP tunnels were
stopped. Generated evidence remains in the local temporary directory above.
All review changes are committed locally; no branch was pushed.

## Remaining qualification limits

A hypervisor reset tests loss of guest cache; it does not remove power from
the physical disk/controller. Windows directory sync remains a no-op, so new
ancestor directories and deletion remain unqualified. The HTTP reset workload
uses Lite; it does not establish LSM compaction crash durability. Focused LSM
writer tests establish bounded memory and publication behavior. These bounded
workloads do not qualify an endurance run or a supported Windows port.
