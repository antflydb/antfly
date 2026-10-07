# PR #987 follow-up qualification

The review branch merges PR head `ce319462af8b` with main `e6d4ce9bbc71`.
Follow-up source commit `99003f02a2` replaces whole-output Windows LSM buffering
with bounded staging and flushes published Windows files after rename.

## Memory and publication checks

The staging writer uses a 64 KiB buffer and a 64 KiB CRC scratch buffer. Its
integration test publishes an 8 MiB output through an 80 KiB fixed allocator,
checks replacement visibility and abort cleanup, and retains the I/O runtime
after storage shutdown. Boundary-crossing header patches and CRC ranges pass.
A truncated staging read prevents subsequent append and publication. A mocked
Windows publication flush fails observably and still closes its handle.

Native macOS storage tests: 36 passed. Windows ReleaseFast storage tests:
15 passed, 21 POSIX-only tests skipped. Staging: 2 passed on both platforms.
Object publication: 1 Windows test passed, 1 POSIX test skipped; the macOS
objectstore suite passed 77 tests with 2 skips before the Windows-only test
was added. CrossOver executed the Windows binaries successfully.
A separate diagnostic with 1,000 thread-pool dispatches and eight background
sleepers passes under CrossOver; it does not reproduce the HTTP stall.

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

## Remaining qualification limits

Repeated 4096-byte writes timed out after 13 and 12 acknowledgments on native
Windows and after 3 on CrossOver. Readiness remained available. A native dump
shows an HTTP handler waiting for an offloaded batch and backend workers
waiting on conditions; other threads perform metadata/status work. A Wine
stack includes a file-stat call while holding the Lite store mutex. These
snapshots do not establish the cause, and the two stalls may have different
causes. Sustained write load is not qualified by the successful reset checks.

A hypervisor reset tests loss of guest cache; it does not remove power from
the physical disk/controller. Windows directory sync remains a no-op, so new
ancestor directories and deletion remain unqualified. The HTTP reset workload
uses Lite; it does not establish LSM compaction crash durability. Focused LSM
writer tests establish bounded memory and publication behavior.
