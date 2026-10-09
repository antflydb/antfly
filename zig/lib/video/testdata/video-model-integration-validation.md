# Video transitions, live framing and model integration — 2026-10-09

This qualification extends the earlier [codec expansion receipt](h264-expansion-validation.md).
`origin/main` at `755cb12a68`, including EmbeddingGemma 2 PR #1014 at `d409767dc7`, is integrated into
`design/native-video-media`. Earlier receipts remain records of their original scope.

## Library behavior

- Compatible SPS changes preserve the prediction DPB, including changed crop and
  reference limits. Incompatible geometry starts a fresh bounded epoch at a complete
  non-IDR I picture. Missing prediction references and configuration changes within
  a picture fail. This is a qualified extension; arbitrary SPS activation inside a
  coded video sequence is not claimed as H.264 conformance.
- Category A precedes cross-packet B/C residuals. Slice identity, missing/duplicate
  partitions, ambiguous boundaries, aggregate bytes, dependencies and shared
  admission remain bounded. Allocation failures and cancellation release resources.
- `live_mp4.Ingest.nextSegment(end_of_stream)` discovers media boundaries from box
  headers across arbitrary chunking. The next `moof`/`styp` closes a completed media
  segment; explicit EOF closes the final segment. Initialization remains explicit.
  Partial/extended headers and EOF-sized payloads are tested. This is box framing,
  not an HTTP/HLS/DASH client or initialization reconfiguration.

The two new SPS fixtures are original generated I_PCM/skip streams. Their sample
hashes derive from known PCM values and independently decoded canonical IDR epochs;
FFmpeg is not used to claim conformance for the non-IDR geometry extension itself.
`scripts/generate_h264_transitions.py` regenerates both MP4s and their oracle JSON
byte-for-byte. Cross-packet tests repartition the existing independently qualified
JM partition payload without changing its NAL bytes. Allocation-failure campaigns
also exercise selected-order callbacks and release shared admission to zero.

## Model behavior

The ordered-group adapter applies the pinned 1 FPS / uniform 32-frame sampling,
140 pooled soft tokens per frame, centered Torchvision bicubic preparation and
video token ID 258884. Mixed text/image/audio/video share one sequence and pooled
embedding. Repeated sampled pictures reuse projection while retaining each sequence
position. Static AVC pixel preflight charges SPS dimensions rather than trusting the
container. SPS VUI and supported container color metadata govern preparation;
HDR, unsupported display policies, matrix and range conflicts fail explicitly.

HTTP media and borrowed binary attachments accept MP4/MOV only inside family-scoped
ordered groups. Original text/image/audio manifests remain compatible. The processor
identity validates the pinned video policy, and the existing SDK media schema carries
the bytes without introducing a parallel request type.

Metal preparation uses the inference device. The producer fence completes before
retaining its MTLBuffer into an inference tensor, and the producer is destroyed before
runtime buffer pooling can reuse it. The device test compares every prepared value
with CPU preparation, then consumes the retained tensor in a resident linear operation
after producer destruction. Existing position addition, vision pooling and token
composition still have host boundaries; fully resident model execution is not claimed.

## Checks

Run from the respective package directories with Zig 0.17.0:

```sh
# zig/lib/media
zig build test-media --summary all
# zig/lib/video
zig build test-video --summary all
zig build check-video -Dtarget=aarch64-linux-musl --summary all
zig build check-video -Dtarget=wasm32-wasi --summary all
# Execute the produced video test.wasm with Wasmtime.
# zig/pkg/inference
zig build test-video-model -Dmetal=true --summary all
zig build check-video-model -Dtarget=aarch64-linux-musl -Dmetal=false --summary all
zig build test-embeddinggemma2 -Dmetal=false --summary all -- --test-filter embeddinggemma2
zig build test-embeddinggemma2 -Dmetal=true --summary all -- --test-filter embeddinggemma2
```

- macOS video: 108 passed, two platform skips, 110 total.
- WASI video executed in Wasmtime: 90 passed, 20 platform skips, zero failures.
- Media: all 36 tests passed, including automatic framing and allocation failures.
- Linux ARM64 musl: video and shared consumer tests compile; model processor tests
  compile. Execution was not obtained this turn because the Docker daemon was
  unresponsive. Earlier actual Linux execution is documented in the expansion receipt.
- Focused Metal model gate: all 12 tests passed, including CPU/GPU prepared values,
  retained-buffer consumption, token layout, sampling, duplicate projection reuse
  and color policy.
- Focused native CPU model gate: 11 passed, one Metal-only skip, zero failures.
- Native CPU full model-family filter: 38 passed, 15 explicit platform/checkpoint
  skips, 53 selected, zero failures.
- Metal full model-family filter: 48 passed, five explicit checkpoint skips,
  53 selected, zero failures.
- The final expanded HTTP video error mapping compiles and its family/MIME
  classification regression passes after the full family runs.
- Zig formatting, Python generator/SDK lint and formatting, byte-identical fixture
  regeneration, whitespace checks and joined OpenAPI semantic comparison passed.

## Qualification limits

At the time of this integration run, no official pretrained weights or video
embedding oracle were available in this workspace. The follow-up
[pretrained validation](video-pretrained-validation.md) closes that numerical gap.
This historical receipt qualifies the processor, token composition, HTTP routing and
device lifecycle; it does not establish end-to-end pretrained video numerical or
retrieval parity. Existing checkpoint-dependent family tests skip explicitly.

Model FPS sampling currently requires progressive one-picture-per-MP4-sample AVC
or MJPEG. Field assembly and Extended-profile partition transport remain decoder
capabilities; their logical-picture sampling is rejected by the model adapter until
qualified. Display policy is square-pixel unrotated SDR. Video audio tracks require
explicit separate input. VP8/VP9/AV1 decoding, CUDA/NVDEC and opaque hardware memory
qualification remain future work.
