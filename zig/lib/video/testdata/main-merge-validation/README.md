# Main merge qualification — 2026-10-14

Merged `origin/main` at `4838edd96171b020eb865e62f16e027551d02a79`
into `design/native-video-media`, preserving native video decoding, frame caching,
resident token composition and device normalization. Main's batch-aware resident
vision core now also accepts already-positioned video tensors, with an explicit
frame batch axis and per-frame geometry. Main's image/audio broker falls back to
cached video execution for video and mixed groups. The backend-free model config
split retains video token validation and the branch's versioned recipe identity.

`qualification.json` binds the pre-receipt source tree, resolved source files,
test binaries and pinned oracle. Raw logs retain the selected counts and skips.
The device-enabled runs use actual Metal/VideoToolbox access. Initial sandbox
runs denied that access; their failures are environmental and are superseded by
the complete device-enabled runs. The initial combined media/video build log
also retains a resolved video syntax conflict; its media suite passed separately.

- Portable media: 168 passed.
- Filtered inference on native CPU and actual Metal: 98 passed, eight unconfigured
  checkpoint/CUDA checks skipped, no failures.
- Full video library on actual Metal/VideoToolbox: 265 passed, two platform checks
  skipped, no failures.
- Pinned pretrained CPU: six video cases and HTTP assertions passed. Largest
  logged coordinate error is 2.68e-7, below the existing 1e-4 gate. All logged
  cosines round to 1.000000000.
- Pinned pretrained Metal: six video cases and HTTP assertions passed. Largest
  coordinate error is 4.563e-5; smallest cosine is 0.999999940. Main's vision
  attention implementation changes numerical rounding from earlier receipts;
  these results pass the existing cosine >=0.9999 / error <=1e-3 gates.

Reproduce the inference checks from `zig/pkg/inference` with Zig 0.17.0:

```sh
ZIG_LOCAL_CACHE_DIR="$PWD/.zig-cache" zig build test-inference-unit \
  -Dmetal=true -Dtest-filter=embeddinggemma2 '-Dtest-filter=gemma4 video' \
  --summary all
```

For pretrained checks, execute the resulting test binary with
`--test-filter 'embeddinggemma2 pretrained video'`,
`ANTFLY_EMBEDDINGGEMMA2_MODEL` set to the verified checkpoint directory,
`ANTFLY_EMBEDDINGGEMMA2_VIDEO_ORACLE` set to
`tools/embeddinggemma2/testdata/video/video-oracle.json`, and
`ANTFLY_EMBEDDINGGEMMA2_METAL=true` or `false`. Run `zig build test-video test-media`
from `zig` with actual device access to reproduce the library checks.

This receipt qualifies the merge on macOS ARM64; it does not replace prior Linux
receipts or claim a new Linux execution, CUDA qualification, broader codec
conformance or end-user retrieval relevance.

The merge commit bypassed the formatter hook after pnpm dependency verification
requested an install for TypeScript files unchanged from main. Staged license
checks, formatting of resolved Zig files, and all ten Go module tidy checks
passed. No TypeScript runtime change is introduced by conflict resolution.
