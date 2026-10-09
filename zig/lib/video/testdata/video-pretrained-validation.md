# Pretrained video qualification — 2026-10-09

The earlier integration receipt did not have weights. This follow-up acquired the
public official checkpoint, verified its LFS digest, and generated an independent
F32 reference with pinned upstream source. The six cases qualify the tested
native CPU/Metal model paths and ordered HTTP contract, under explicit RGB
policies. They do not establish application retrieval quality or general video
codec conformance.

## Reproducible inputs

- Checkpoint: `google/embeddinggemma-2`, revision
  `914f7f89142e33e77833254d9c9b90c3cef7303b`.
- Original safetensors: 1,488,915,288 bytes, SHA256
  `197a32965d4b1105faf060417baa899e193fb73cd401f42ec9295234d5553d79`.
  The acquisition receipt verifies all 13 files; weights stay outside Git.
- Transformers: commit `92cd495f2720c064bc78eb2d93e28704c5bce51f`,
  `5.19.0.dev0`; source hashes are recorded and enforced by `reference.py`.
- Python 3.12.11, PyTorch 2.10.0, Torchvision 0.25.0, Pillow 12.3.0,
  FFmpeg 9.0.2. F32 inference, masked mean pooling, L2 normalization.
- [Small original fixtures, complete token IDs, and 768-D reference vectors](../../../../tools/embeddinggemma2/testdata/video/README.md).
  Oracle SHA256:
  `fd721cfb54d60b378afcabc8487d29f69526f119ef96b70a25e373ffd05cd2bd`.
  Encoded clip hashes, decoded RGB hashes, frame selections and reference versions
  are retained in the oracle. Regeneration produced identical encoded clips.

H.264 reference RGB uses independent FFmpeg decode. The MJPEG reference demuxes
original compressed packets with FFmpeg and decodes them independently with
Pillow/libjpeg, matching the native JPEG RGB policy. The colorful native RGB
and WASI RGB dumps both match the independent reference byte-for-byte:
`d418865b37d4cdb43e1996e804612243840800bf909344ec20a4bc8541787d67`.
Default FFmpeg MJPEG IDCT differs on 1,988 channel samples, by at most three byte
values; its separate hash and differences are retained in the oracle. Initial
FFmpeg-default model comparisons exceeded the strict CPU maximum-error threshold.
The corrected oracle changes the declared JPEG reference decoder, not native
pixels or model tolerance. Default FFmpeg/torchcodec JPEG byte parity is not claimed.

## Numerical and HTTP checks

All cases validate presentation sampling (including B-picture ordering), exact
selected frame ordinals and duplicate selections, token counts, finite normalized
vectors, and all 768 reference coordinates. Thresholds are CPU cosine ≥0.99999 /
max absolute error ≤1e-4, Metal cosine ≥0.9999 / error ≤1e-3. Actual cosines exceed
0.999999999997; vector norms differ from one by less than 1e-7.

| Case | Input tokens | CPU Debug max error | Metal Debug max error |
| --- | ---: | ---: | ---: |
| mjpeg_video | 266 | 1.86e-07 | 1.83e-07 |
| h264_video | 266 | 1.86e-07 | 2.69e-07 |
| text_video | 277 | 2.03e-07 | 2.08e-07 |
| video_text | 278 | 2.09e-07 | 2.51e-07 |
| repeated_video | 530 | 1.42e-07 | 1.91e-07 |
| two_videos | 543 | 2.84e-07 | 2.57e-07 |

The actual `/ai/v1/embed` handler accepts inline MP4 in an ordered text/video
group, returns HTTP 200, and emits the expected normalized 128-dimensional
truncation, a 64-character model identity, and zero remaining admission units.
Both backends report model identity
`2e3ab9d26dcdc60ebb4d0d6b2c744e0285545358eb6945b8fbbe9c0b1a6c2081`.

## Executed validation

- CPU Debug: one selected test, passed; all six cases and HTTP assertions completed,
  including allocation/leak checks. CPU and Metal ReleaseSafe: each selected one
  test and passed all six cases plus HTTP. Optimized Metal errors match the Debug
  table above; maximum is 2.69e-7.
- Linux ARM64 musl and wasm32-wasi qualification tool: both compile successfully.
  WASI executed all eight colorful MJPEG pictures and matched the independent RGB
  SHA256 above.
- Fresh child processes for blocked model rollback, blocked cached-executor
  destruction, and over-deadline close with stderr locked each exit with the
  required restart code 86. These preserve the stuck-driver safety behavior.
- Ruff lint/format, Zig format, and whitespace checks pass.

CPU optimized results:

| Case | CPU ReleaseSafe max error |
| --- | ---: |
| mjpeg_video | 1.42e-07 |
| h264_video | 2.38e-07 |
| text_video | 2.25e-07 |
| video_text | 2.09e-07 |
| repeated_video | 1.6e-07 |
| two_videos | 4.77e-07 |

CPU Debug executable SHA256:
`1c20fed7f77589dcffcf14db2191539eb5ed73e6ed16cfe38412a1cb9e1ab090`.
CPU ReleaseSafe executable SHA256:
`7cf02bf856ee8797a26d93e548c17c34b644026b991e62d8a3d6b4e9579e1312`.
Metal ReleaseSafe executable SHA256:
`78b030a4975bd9c56b629c7157aa39f7882f37d3aa0cd4cf7361f7e586d17953`.
CPU and optimized Metal numerical runs precede the teardown-scope fix; the final
Metal Debug regression run below exercises that process-required driver boundary.

Final Metal Debug: eight selected tests, seven passed, one explicit unconfigured
supervised-child fixture skip; zero failures. The pretrained test covers all six
cases and HTTP. Six teardown regressions include the new host-cleanup boundary.
The three configured fresh-child campaigns above run the skipped fixture separately.
Final Metal Debug executable SHA256:
`c6241edc110d32f0a43182f659accf782533fd729d0112a01614f7cf2edb55b7`.
The run completes with exit zero, including Debug allocation/leak checks.

[Durable result data](video-pretrained-results.json) retain complete measured
errors/norms/cosines, test logs, identities, binary/source hashes and child results.
Reproduction commands are in the fixture README. To include lifecycle regressions,
add `--test-filter 'model manager teardown'` to the Metal command.

The first Metal Debug run completed all numerical/HTTP checks but exited 86 in
teardown. LLDB identified the armed driver-close watchdog while Debug allocation
tracking freed host tokenizer strings, after session/driver cleanup returned.
`LoadedModel.deinit` now retains protection through all driver/cache/prefetch
cleanup, then disarms before CPU tokenizer/template destruction. A regression
observes tokenizer frees and verifies driver tickets have already disarmed;
existing nested close, cached-executor and isolation tests remain required.
The driver timeout and stuck-close behavior are unchanged.

## Retained limits

This is a small synthetic qualification set, not a production throughput,
retrieval/classification or broad real-world video campaign. H.264 is progressive
8-bit 4:2:0 SDR here; MJPEG includes the colorful original fixture and a repeated
single picture. High-depth/field/partition model sampling, HDR/rotation, unknown
live durations, CUDA/NVDEC and VP8/VP9/AV1 model decoding are not qualified by these
comparisons. Linux software decoding remains portable; this receipt's pretrained
model execution is macOS ARM64. Linux qualification-tool compilation and actual
WASI JPEG RGB execution qualify the diagnostic path, not Linux model numerical
parity. Resident vision/projector/backbone optimization remains separate work.
