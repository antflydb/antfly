# Expanded native AV1 qualification

Recorded run results and output filenames below describe the measured historical
epoch. Generated reports/logs were removed from the current tree; see
[qualification and historical recovery](../../../../../docs/plans/native-video-noncuda-audit.md#artifact-policy-and-historical-evidence).
Fixture inputs and independent reference oracles remain checked in.

Antfly's maintained pure Zig AV1 decoder now exposes profiles 0–2, 8/10/12-bit
native samples, monochrome/4:2:0/4:2:2/4:4:4, Annex B and explicit operating-point
selection. No runtime or native build dependency was added.

`golden.json` pins nine additional odd-size format streams and one independently
encoded Annex B stream: 120 displayed pictures, each compared byte-for-byte with
AOM 3.13.1 output. `layers.json` pins a two-spatial/three-temporal-layer stream
and eight operating-point/output policies: 78 reference outputs. Native tests
check each policy with both low-overhead and Annex B framing. Reference picture
ordering is checked against AOM's raw output as well as per-picture files.

Fixtures use Antfly-generated test patterns and retain the repository's Apache
license. AOM/FFmpeg are offline reference tools, not Antfly dependencies.
`provenance.json` records the pinned upstream commit, reference-tool fingerprints
and actual generation commands. AOM's SVC output is grouped by timestamp into
complete temporal units. Explicit 12-bit non-420 profile selection has a CLI
initialization-order defect in AOM 3.13.1; automatic profile promotion generates
the correct Professional profile without modifying reference code.

Current execution: 150 macOS video passes with actual Metal and two platform
skips; 129 Linux ARM64 video passes and 23 platform skips; 25 AV1 Debug passes.
Both Linux architectures compile/link statically. Final coverage-guided campaigns
completed 101427 AV1 reconstruction runs and 101612 Annex B runs without failure.
Reported edge coverage belongs to the instrumented binary, including shared code,
and is not a per-codec conformance percentage.

## Contracts

Set `Options.av1_framing = .annex_b` for elementary Annex B input. The adapter
validates nested lengths, redundant OBU sizes, delimiter placement, extension
headers and frame-unit structure before decoder entry. Normalization storage is
charged against the session's shared decode budget and released before admission
on failure. `max_annex_b_frame_units` bounds framing work. Pre-entry malformed,
capacity and cancellation failures remain retryable; post-entry failures
invalidate the session and require fresh synchronization-point replay.

`av1_operating_point` defaults to zero. Unavailable explicit indices fail instead
of falling back. `av1_all_layers` defaults to true; false selects the highest
spatial layer in the chosen operating point. Supply complete temporal units for
this policy. Separately draining individual spatial-frame packets can expose
lower layers before later packets arrive. Batch outputs retain caller timestamps;
layer filtering does not synthesize a timeline. WebM still uses low-overhead OBUs
and rejects Annex B session options.

## Reproduce

Build pinned AOM `v3.13.1` reference tools outside the Antfly source tree, then:

```sh
python zig/lib/video/scripts/generate_av1_extended_fixtures.py \
  --ffmpeg /path/to/ffmpeg --aomenc /path/to/aomenc \
  --aomdec /path/to/aomdec --svc-encoder /path/to/svc_encoder_rtc \
  --output /tmp/av1-extended
zig build --build-file zig/lib/video/build.zig test-video \
  -Dvideo-test-filter=AV1 -Doptimize=ReleaseSafe
zig build --build-file zig/lib/video/build.zig fuzz-video \
  -Dvideo-fuzz-filter='video fuzz AV1 reconstruction' \
  -Doptimize=ReleaseSafe --fuzz=100K
zig build --build-file zig/lib/video/build.zig fuzz-video \
  -Dvideo-fuzz-filter='video fuzz AV1 Annex B' \
  -Doptimize=ReleaseSafe --fuzz=100K
```

Execution counts, source/artifact fingerprints and bounded fuzz results are
recorded in `qualification.json`. This corpus does not establish complete AV1
conformance, isolated throughput, broader model display-policy parity or
hardware decoding support. Linux ARM64 execution uses a local Linux VM;
Linux x86_64 compilation alone does not establish runtime qualification.
