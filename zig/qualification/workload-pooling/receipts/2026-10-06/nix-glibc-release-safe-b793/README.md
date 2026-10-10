# Nix glibc ReleaseSafe qualification against b79388a7d6

The original binaries compare origin/main `b79388a7d67f812f46d34c75cabe404836ed2a6a`
with the feature snapshot based on merge `d453116cdcbd2c1c93a448a131d31c4da8a4574b`.
`provenance/source.json.gz`, the compressed patches and untracked source files
identify the actual dirty snapshot; the commit alone does not identify it.
The baseline has only the recorded compiler scheduling claim adjustment.
Both production builds completed all 45 steps with identical stripped
ReleaseSafe settings using pinned Nix glibc Zig 0.17.0, native GNU ARM64.
Compiler SHA-256: `4818e9e28e752456421034db9229035c165c0e3392bf534421016e79de33d022`.

`startup-failure-small` retains 36 profiles; `startup-failure-large` retains nine.
Every profile failed during startup before measurement, in both variants.
The default optional ChatGPT connector called `fchmod` on an `O_PATH` directory
handle, returning `EBADF`; see `startup-diagnostic.txt`. These are startup failure
receipts, not throughput or latency measurements. Disabling Docker seccomp did
not resolve the error. No qualification uses disabled seccomp.

The focused fix opens a readable/iterable directory before enforcing `0700`.
It is applied identically to the baseline and candidate, with the patch retained
in provenance. Subsequent comparisons therefore use patched main. No pooling
configuration, measurement resource ceiling or acceptance threshold changes.
The separate post-build test audit wiring patch affects test orchestration,
not runtime code; it is recorded independently.

`harness-smoke-unqualified` exercises the combined-pressure client setup with the
optional connector explicitly disabled while the compiler is active. It is not
a performance qualification run. It found a candidate pgwire startup allocator
mismatch: ParameterStatus allocated from the ordinary owner and freed through
the partitioned wire owner. The corrected message uses the wire owner for both
operations, and the bulk-OOM regression now verifies its protocol output and
charge retirement. The candidate fix and file hash are recorded separately in
provenance before the startup-fixed candidate rebuild. The focused pgwire target
also needed an explicit protocol import: its earlier two-test summaries counted
anonymous import tests only. The corrected target passes all three named
startup/frame/control cases, plus those two import tests, on macOS and GNU Linux.
The original broad pooling target did execute the two original protocol cases;
its named output is retained independently.
The full pgwire protocol/lifecycle suite now passes all 87 cases on both macOS
and GNU Linux in ReleaseSafe.

Native ReleaseSafe checks pass: 54 pooling cases, 12 protected HTTP socket cases,
12 connection-helper cases, three named pgwire contracts, and the real linked
forwarding fixture with the read heap exhausted and the control executor full.
The linked target completed all 91 build steps. GNU Linux passed the signal-stack
overflow fixture and all 99 resource/transaction/filesystem cases.

The credential-store fixture passes on macOS and in an unstripped ReleaseSafe
Linux test binary, including persistence, reopen and directory permissions.
Its stripped Linux test compilation crashed with SIGSEGV, including a direct
reproduction with a 64 MiB compiler stack. An earlier graph invocation also
lacked `uv`; a smaller compiler budget was rejected before compilation.
Those diagnostics are retained in `verification`, with exact response files
and separate execution counts. This fixture's unstripped setting does not change
the stripped production comparison binaries.

Both startup-fixed production binaries completed all 45 build steps. Patched
main reached default standalone startup with the connector enabled and joined
shutdown with exit zero. The candidate then completed the full profiles with
the corrected pgwire startup allocator. Final binary hashes, supplemental source
patches and source audit are in `provenance/startup-fixed-builds.json.gz`.

## Completed ReleaseSafe matrices

The [machine-readable summary](summary.json) counts the final profile selection.
Each row has three rotated repetitions of both candidate policies and main.

| Profile | Candidate absolute checks | Main absolute checks | Fixed/fail-fast comparisons | Fixed/main comparisons |
| --- | --- | --- | --- | --- |
| Small authenticated idle/cursors | 6/6 pass | 0/3; pgwire setup fails | Not a load profile | Not a load profile |
| Large authenticated idle/cursors | 6/6 pass | 0/3; pgwire setup fails | Not a load profile | Not a load profile |
| Mixed open-loop load | 6/6 pass | 3/3 pass | 3/3 pass | 3/3 pass |
| One-slot waiting pressure | 6/6 pass | 3/3 pass | 3/3 pass | 3/3 pass |
| Combined retention, one slot | 6/6 pass | 0/3; pgwire setup fails | 3/3 pass | Unavailable; all three fail baseline qualification |

All **30 candidate absolute profiles**, all **nine fixed/fail-fast comparisons**,
and all **six comparable fixed/main load comparisons** pass unchanged gates.
Candidate idle runs retain 32/64 authenticated sessions and 16/32 held cursors,
with approximately 41/78 MiB idle RSS growth and zero descriptor growth after
close. These measurements do not assign passing zero-latency or zero-growth
values to main's missing idle samples.

`startup-fixed-small` and `startup-fixed-large` preserve the first complete
matrices. The first combined profile correctly failed its completed-wait gate:
16 execution slots exceeded its observed peak of 11 active queries. All retained
candidate clients and other absolute checks passed, but those fixed profiles
remain failures. `combined-one-slot-small` uses one execution slot for all
variants, retains identical load/client/resource settings and acceptance gates,
and records 1,587, 1,548 and 1,580 completed sustained waits. The exact workload
change is recorded in `provenance/combined-one-slot-workload.json`.
All 32 HTTP peers, 32 authenticated pgwire peers, eight held cursors and eight
unfinished 4 MiB uploads remain valid through load and close before recovery.

The raw matrices remain **failed** because main cannot complete idle/combined
pgwire setup. Combined versus main has no valid timing comparison. Do not mark
the matrices or automatic defaults qualified. Whole-process native/backend
accounting and physical mutating storage completion under arbitrary root
exhaustion are not established by these named contracts.

Compressed files retain their original content and basenames; original temporary
paths in receipts describe the measurement run. Measurements use enforced cgroup
CPU, memory and PID ceilings on a shared developer host, not exclusive host CPUs.
