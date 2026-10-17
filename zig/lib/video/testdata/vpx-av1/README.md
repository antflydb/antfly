# Native VP9/AV1 reconstruction fixtures

Recorded run results and output filenames below describe the measured historical
epoch. Generated reports/logs were removed from the current tree; see
[qualification and historical recovery](../../../../../docs/plans/native-video-noncuda-audit.md#artifact-policy-and-historical-evidence).
Fixture inputs and independent reference oracles remain checked in.

These are first-party synthetic test-pattern clips, each containing 12 visible
pictures at 6 fps. They contain no downloaded media or model weights. IVF
fixtures exercise native packet decoding; WebM remuxes exercise range-reader
integration. `golden.json` records SHA-256 per visible picture over tightly packed
planar Y, U, V bytes, preserving ceil chroma geometry and native sample depth
(right-aligned little-endian u16 above 8-bit). The native tests reconstruct the
same planar byte order from owned semiplanar output before hashing.

The initial receipts used FFmpeg 9.0.2, libvpx encoder 1.17.0 and SVT-AV1 4.2.0. Raw
reference pixels were produced by FFmpeg in a separate process. During porting,
all 144 visible pictures were also compared byte for byte with that raw output.
Normal tests need only checked-in IVF/WebM bytes and the pixel hashes, without
FFmpeg or any codec library.

| Fixture group | Input | Encoding policy |
|---|---|---|
| `av1`, `vp9` | testsrc2, 128x96 | 8-bit 4:2:0, interframes, GOP 6 |
| `av1-10` | testsrc, 128x96 | Main profile, 10-bit 4:2:0 |
| `av1-grain` | testsrc, 128x96 | Main profile, grain strength 10, denoising disabled |
| `av1-odd` | testsrc, 127x95 | Main profile, 8-bit 4:2:0 odd dimensions |
| `vp9-yuv*` | testsrc, 127x95 | Profiles 0–3, 8/10/12-bit 4:2:0/4:2:2/4:4:4 |

Regenerate into a temporary directory before reviewing/replacing receipts:

```sh
python3 zig/lib/video/scripts/generate_vpx_av1_fixtures.py \
  --output /tmp/antfly-vpx-av1-reference
zig build --build-file zig/lib/video/build.zig test-video \
  -Dvideo-test-filter='VP9 AV1' -Doptimize=Debug --summary all
```

The generator records encoder/reference commands, tool versions and packet
hashes. Encoding bytes may change with tool versions; update reference hashes
only after reviewing decoder differences and running differential qualification.
These short fixtures and deterministic malformed-input tests establish initial
reconstruction parity, rather than broad real-world conformance, coverage-guided
fuzz qualification or a throughput claim. Actual Linux execution belongs to the
Linux qualification workflow; local cross-compilation is not runtime evidence.

[qualification.json](https://github.com/antflydb/antfly/blob/bf9d632d025a6b12837997f6aeb86e1a818ec831/zig/lib/video/testdata/vpx-av1/qualification.json) records the source hashes and local
receipts: 14/14 focused Debug tests, 136/138 ReleaseSafe video tests with two
platform skips, actual decoded-frame CPU/Metal preparation parity, and Linux
x86_64/ARM64 compile/link checks. Its referenced logs distinguish executed
tests from cross-compilation.


`vp9-sintel.webm` and `av1-sintel.webm` are 192x128 derivatives of the pinned
Sintel scene-1 excerpt. Sintel © Blender Foundation / durian.blender.org,
[CC BY 3.0](https://creativecommons.org/licenses/by/3.0/). Retain this attribution.
`real-streams.json` records source provenance, commands and all 96 displayed
picture hashes from independent FFmpeg decoders. Regenerate offline with
`zig/pkg/inference/scripts/embeddinggemma2/generate_webm_real.py`; native builds never invoke it.
