# Recorded qualification attempts, 2026-10-06

These receipts preserve successful and failed attempts; they do not qualify automatic defaults.

## macOS development measurements

Baseline: unmodified origin/main `59b0d9cb84c22b2fddb63c86f80db413fef8fe17`.
Both binaries used pinned Zig 0.17.0, native Debug optimization. Manifests and binary hashes are embedded in each receipt. Large raw receipts are losslessly compressed as `.json.gz`; comparison summaries remain plain JSON. `compare_receipts.py` accepts either format. Candidate includes the stage-three controls/accounting work before the final linked-storage accounting correction and AArch64 Linux compatibility correction; these receipts are historical development measurements rather than final binary certification.

- `macos-cold`: all absolute workload profiles passed. Relative low-load p95 failed: fixed waiting was 1.456 times main and 1.365 times candidate fail-fast. Preserve this failure.
- `macos-warm`: a separate manifest records 16 warm-up operations and cheap low-load/recovery queries. All profiles and comparisons passed: low-load p95 was 1.010 times main, sustained successful throughput was 0.988 times main. This distinct workload does not invalidate or explain away the cold failure.

API-key bootstrap happens once before timing. No retries or generator drops were accepted. Latencies include scheduled client queueing and successful throughput includes phase drain. The operator probe checks unauthorized denial, suspend, stale-version rejection, resume, and fresh-query rejection while suspended. Temporary credentials and SQL payloads are omitted from receipts.

The Linux small/large cgroup matrices, a coordinated whole-process memory ceiling, and distributed durable completion remain separate release gates. The qualification workflow retains future raw receipts as artifacts, including failures.


## GNU Linux AArch64 measurements

Docker Desktop GNU Ubuntu 24.04, pinned official Zig 0.17.0 AArch64 toolchain,
Debug optimization and `-Dcpu=baseline` for both binaries. The unmodified main
revision is the same as above. Candidate is the native implementation subsequently
committed as `347dd47c80`; the compiler allowance and explanatory comments do not
change runtime behavior. Binary identities are embedded in every receipt:
main `6540b54822876aa1b7294bf54239f2c20d2789404bf4e464ae46802e9cc16000`,
candidate `bdb00ea2cdfa785f6db662b50792e16bee279a562cdd98a4250f1ab1643ab894`.
No build workloads were running during measurement. Server logs are retained
beside receipts after redacting temporary credentials.

The initial candidate-failfast variant used baseline API heap defaults, whereas
fixed waiting used 64 MiB foreground/retained startup envelopes. These initial
attempts remain preserved but do not isolate waiting's cost. The historical macOS
same-candidate comparison has that same limitation. The corrected harness applies
identical candidate startup configuration to both variants and changes only query
waiting; its comparison gate now rejects other same-binary configuration changes.
Main comparison remains between the recorded full product configurations under
identical enforced process/container limits.

- `linux-small-initial` and `linux-large-initial`: candidate absolute profiles pass;
  main resets a connection during authenticated pgwire setup. The initial small
  throughput comparison fails (fixed/main ratio 0.868).
- `linux-small-equal-heaps`: both candidate idle profiles and all three load
  profiles pass absolute gates. Fixed waiting retains all 32 HTTP and 32 pgwire
  sessions, validates 16 held cursors/two portals, grows RSS 47.4 MiB, and returns
  to zero FD growth after close. Main again resets a connection after 32 HTTP
  clients and five constructed pgwire peers; no idle peak/control sample is claimed.
- The corrected small mixed comparison **fails** its sustained successful
  throughput gate: fixed waiting 87.83/s, main 106.52/s (ratio 0.825), equal-heap
  candidate fail-fast 109.32/s (ratio 0.803). Low-load/recovery latency gates pass.
  All 1000 sustained offered requests complete, with zero retries/generator drops;
  overload errors are recorded 503 responses. Status remains responsive and all
  servers join shutdown with exit zero. These failures prevent automatic waiting.

Older raw failed small receipts use JSON's Python extension `Infinity` for a
missing control sample; corrected receipts record `null` and retained peer counts.
Do not reinterpret a missing sample as a passing zero-latency observation.

- `linux-large-equal-heaps`: both candidate policies pass the corrected larger
  profile. Fixed waiting validates all 64 authenticated sessions, 32 held cursors
  and two portals; RSS growth 85.3 MiB, status maximum 244 ms, zero FD growth after
  close. Main again fails during pgwire setup. All three shutdowns exit zero.
