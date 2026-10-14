# Stateful VP9 / AV1 sequence qualification

The harness mutates complete packet tapes, retaining up to four independent
output batches across later interframes, skipped output, malformed packets and
cancellation. It validates geometry and SHA-256 picture bytes before and after
subsequent decoder calls, and after decoder destruction. A poisoned session is
never reentered; retryable pre-entry rejection is bounded. The testing allocator
also checks cleanup. Timestamp high-byte flags vary all/none/index-zero selection
and cancellation without changing the ordinary clean seeds.

The positive gate decodes all twelve packets of fifteen streams: VP9 8/10/12-bit
and chroma variants; AV1 depth/chroma, grain, odd geometry, scalable layers and
Annex B; and textured 480p Sintel packets for both codecs. A separate deterministic
gate checks skipped interframe output and cancellation with retained pictures.
The ordinary video CI root imports the harness, so accepted seed replay is not
limited to manually invoked fuzz builds. The original ownership gates pass 10/10 on macOS ARM64 and in the actual
Linux ARM64 container. The added independent pixel gate compares all 24
textured Sintel pictures with FFmpeg 9.0.2 (native VP9 / libdav1d AV1), using
planar Y/U/V SHA-256 hashes. The expanded gate passes 11/11 on macOS ARM64 and in the pinned Linux ARM64
container. Source hashes, the Linux executable hash and both reports are retained.

The earlier synthetic-seed VP9 campaign completed 407,798 runs, 3,765 unique
runs and 17,432/52,391 instrumented edges. Its exact historical harness snapshot
and report are retained separately. That report does **not** qualify AV1,
Annex B or the later real-picture harness. This is bounded qualification, not a
claim of exhaustive codec conformance or sustained production fuzz coverage.

[Receipt](receipt.json) binds sources, platform, binary and harness limits.
The clean current harness is `fuzz_test.final.zig`; the historical synthetic
harness is `fuzz_test.synthetic.zig`. The codec core is unchanged from the base
commit recorded in the receipt. Real seed [provenance](../sequence-fuzz/provenance.json)
includes attribution, hashes and packet-copy commands.

Reproduce from `zig/` with the repository's Zig compiler:

```sh
zig build test-video -Doptimize=Debug -Dvideo-test-filter=sequence --summary all
zig build install-video-tests -Dtarget=aarch64-linux-musl -Doptimize=Debug \
  -Dvideo-test-filter=sequence --prefix /tmp/video-sequence-linux --summary all
```

Run `/tmp/video-sequence-linux/bin/video-tests` on Linux without runtime test
filters. Instrumented campaigns must use separate fresh local caches and an
explicit target filter (`sequence VP9`, `sequence AV1`, or `sequence Annex B`).
Inspect the complete report for panics as well as build status; Zig's build exit
status alone does not establish successful fuzz qualification.

The final-source VP9 campaign completes 20,005 runs, 15 unique runs and
10,982/48,143 instrumented edges; Annex B completes 10,475 runs, 363 unique
runs and 5,062/48,139 edges. These short campaigns are not sustained-coverage
claims. The final-source AV1 campaign completes 10,734 runs, 241 unique runs
and 7,258/48,147 edges in a reported thirty-minute shared-host campaign. The
longer VP9 campaign remains in progress; no completion or coverage result is
attributed to it yet.

For maintained longer campaigns, run the independent-target driver from `zig/`:

```sh
python3 lib/video/scripts/qualify_sequence_fuzz.py --zig /path/to/zig \
  --iterations 100000 --output /tmp/sequence-fuzz-campaign
```

The output directory must be new. The driver runs VP9, AV1 and Annex B
separately, captures source/seed hashes and full logs, and requires a fresh,
complete report for the requested target. Crash text fails qualification even
when the build exit status is zero. Five parser checks cover misleading exit
status, wrong targets, reused counts and incomplete/ambiguous reports. Partial
receipts retain running/failed state and never claim unfinished targets passed.

The broader macOS native-device suite passes 262/264 tests with two skips.
That run predates the added differential test, which separately passes on both
platforms. The restricted full run failed to create Metal/VideoToolbox resources;
both reports are retained. The driver also passes an actual Annex B instrumented
smoke campaign (119 runs); this verifies the runner, not sustained coverage.
