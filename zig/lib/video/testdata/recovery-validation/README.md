# WebM entropy-corruption recovery qualification

Recorded run results and output filenames below describe the measured historical
epoch. Generated reports/logs were removed from the current tree; see
[qualification and historical recovery](../../../../../docs/plans/native-video-noncuda-audit.md#artifact-policy-and-historical-evidence).
Fixture inputs and independent reference oracles remain checked in.

`walkRecoveringIndexed` is an opt-in library walker for callers that handle
explicit missing-picture gaps. It recreates the decoder at the next parsed
codec-key restart, retaining original picture IDs and clocks and AV1 sequence
bootstrap. Model inference continues to use strict replay.

Tests cover VP8/VP9/AV1 entropy mutations, a Sintel-derived VP8 stream,
container sync hints that disagree with codec headers, independently retained
outputs, corrupt restart and tail packets, bounded skips/restarts, source and
callback failures, cancellation, shared admission denial and every allocation
failure in the recovery lifecycle. Surviving output hashes are compared against
clean reconstruction. This establishes restart equivalence; it is not an
independent corruption-concealment or codec-conformance oracle.

Two fresh exploratory campaigns found transform arithmetic overflows. The
original failing reports and Smith inputs are retained. VP8 products now use
64-bit arithmetic. VP9's 8-point ADST accumulators now use `tran_high_t`, and
normal/high-depth transform range checks propagate `InvalidVideoData` through
the existing owner error path. The bounds follow the maintained upstream
`vpx_dsp/inv_txfm.h`: signed 16/18/20-bit intermediate coefficients for
8/10/12-bit valid streams. Existing independent clean-picture tests verify
that rejecting out-of-range corrupted coefficients preserves qualified output.
A build exit code alone is insufficient for Zig fuzz qualification: the runner
can return success while printing a panic. Inspect the full campaign report.

Reproduce focused regressions with:

```sh
zig build test-video -Doptimize=Debug -Dvideo-test-filter='WebM recovery'
zig build fuzz-video -Doptimize=Debug -Dvideo-fuzz-filter='WebM recovery fuzz' --fuzz=10000
zig build test-video -Doptimize=Debug
zig build install-video-tests -Dtarget=aarch64-linux-musl -Doptimize=ReleaseSafe --prefix /tmp/recovery-linux
```

Execute `/tmp/recovery-linux/bin/video-tests` on actual Linux. The complete
current results, binary identities, source hashes and report hashes are in
`qualification.json`. Earlier failing campaigns are evidence of defects found,
not successful qualification counts. A fresh campaign cache is required to
avoid adding earlier exploratory runs to the successful final count.

Limitations remain explicit: a valid header index is required, undetected or
concealed corruption is not reported, only coded-data decoder failures qualify
for restart, and source/control/callback/resource failures remain terminal.
H.264/TS resynchronization, malformed-header recovery, broader mixed-tool
corpora and sustained campaigns remain in the full active non-CUDA goal.

The final fresh campaign passed 13,497 executions, 801 unique cases and
14,021/51,841 reported coverage edges, with four positively accepted Smith
seeds. This is a bounded recovery campaign, not sustained-production coverage.
The independent clean-corpus recheck matched all 4,623 pictures across six
480p/1080p VP8/VP9/AV1 streams after both transform fixes.

Retained logs normalize the decorative `FUZZING REPORT` banner to avoid
Git interpreting its leading equals signs as a merge-conflict marker. Report
counts and diagnostic contents are preserved.
