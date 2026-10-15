# WebM model qualification

Recorded run results and output filenames below describe the measured historical
epoch. Generated reports/logs were removed from the current tree; see
[qualification and historical recovery](../../../../../docs/plans/native-video-noncuda-audit.md#artifact-policy-and-historical-evidence).
Fixture inputs and independent reference oracles remain checked in.

These receipts bind the maintained VP8/VP9/AV1 path to the pinned pretrained
EmbeddingGemma 2 checkpoint, F32 PyTorch reference, production CPU/Metal binaries
and an actually executed Linux ARM64 binary. Five documents include three
synthetic codec clips and two attributed real Sintel derivatives; three text
queries check reference preservation without end-user relevance labels.

All eight vectors agree: maximum coordinate error is `9.32e-7` on macOS CPU,
`3.58e-7` on Metal and `7.98e-7` on Linux ARM64. All three rankings and top-k
sets match exactly (k=1/5/10, capped to the five documents). Native/Metal damaged
first-packet requests return HTTP 400 and clean pretrained retries match.
The six existing MP4/MOV pretrained cases also pass on the current CPU/Metal
binary, including token usage, normalized 128-dimensional output, forced
admission retry and damaged-selected-frame cleanup/retry.
Concurrent/sustained request and disconnect/retry checks are retained in the
full reports. Disconnect/retry does not prove cancellation completion.

The integration uncovered a sampling defect: inferring duration from rounded
WebM PTS made two-second clips shorter than two seconds, selecting one picture
instead of the reference's two. The reader now preserves declared Segment
Duration after TimestampScale parsing. Regression tests qualify `[0,6]` for the
6 FPS clips; missing duration retains an explicit inference/fallback policy.

Current native gates: 141 macOS video passes with actual Metal and two platform
skips; 120 Linux ARM64 video passes and 23 platform skips; 15 native/Metal model
passes; 56 media passes. Linux executes in a local ARM64 container VM with a
statically linked musl binary and no codec runtime libraries. Linux x86_64
runtime remains unverified. The two new real streams match all 96 independently
decoded pictures, extending the earlier 144-picture synthetic corpus.

Coverage-guided campaigns completed without failure: 405402 H.264 runs,
101365 VP9 runs and 101382 AV1 runs. Logs report instrumented-binary coverage,
including reachable shared code; percentages are not per-codec conformance.
These bounded campaigns do not replace larger continuous corpus/fuzz coverage.

`qualification.json` pins current sources and every retained report/log. The
older `../vpx-av1/qualification.json` remains historical evidence for its original
source phase. Earlier model receipts retain their original binary fingerprints.
Current measurements shared the host with compilation and other qualification;
use them as correctness/lifetime receipts, not isolated throughput claims.

## Reproduce

With the pinned checkpoint acquired using `tools/embeddinggemma2/reference.py`,
run from the repository root (reference encoders/decoders are offline tools):

```sh
python tools/embeddinggemma2/video_compare.py --backend cpu \
  --corpus tools/embeddinggemma2/testdata/retrieval/webm-corpus.json \
  --model-dir .benchmark-assets/native-video/model \
  --warmups 0 --iterations 1 --output /tmp/webm-reference.json
python tools/embeddinggemma2/video_compare.py --backend native \
  --corpus tools/embeddinggemma2/testdata/retrieval/webm-corpus.json \
  --model-dir .benchmark-assets/native-video/model \
  --binary zig/pkg/inference/zig-out/bin/antfly-inference-bench-server \
  --reference /tmp/webm-reference.json --warmups 0 --iterations 1 \
  --output /tmp/webm-native.json
```

Use `--backend metal` on macOS for the device path. Run
`video_webm_errors.py` with the same binary/model/corpus/reference and an output
path for damaged-packet rejection and clean retries. The manual Linux workflow
runs independent pinned PyTorch and native WebM gates.

```sh
zig build --build-file zig/lib/video/build.zig fuzz-video \
  -Dvideo-fuzz-filter='video fuzz VP9' -Doptimize=ReleaseSafe --fuzz=100K
zig build --build-file zig/lib/video/build.zig fuzz-video \
  -Dvideo-fuzz-filter='video fuzz AV1' -Doptimize=ReleaseSafe --fuzz=100K
zig build --build-file zig/lib/video/build.zig install-video-tests \
  -Dtarget=aarch64-linux-musl -Doptimize=ReleaseSafe --prefix /tmp/video-linux
```

Execute `/tmp/video-linux/bin/video-tests` on the target Linux host. Test binaries
are static; native builds require no codec C compilation or source downloads.
Expanded AV1 profiles, Annex B and layer selection have subsequent
[qualification receipts](../av1-extended/README.md). Field/partition and
timestamp/window model policies, device normalization, cross-request scheduling,
isolated performance and DASH/HLS expansion remain work in the active plan.


The `codec fuzz seed` gate exercises all 13 VP9/AV1/Annex B seeds through
`Smith.sliceWithHash`, then requires successful pixel reconstruction under the
campaign's actual byte, pixel, allocation and control limits. Smith's seed
serialization includes a four-byte length prefix; the decoder receives only
the raw coded payload. The metadata AV1 target uses the same serialization.
