# Image Corpus

This directory holds the curated image conformance corpus for the shared
`antfly-zig` / `antfly-inference-zig` image stack.

The goal is to keep a small checked-in core corpus that exercises the behavior
we claim to support, plus enough invalid inputs to lock down parser and decoder
error handling.

Directory layout:

- `jpeg/`
  - static JPEG fixtures
  - `upstream/<source>/` for curated primary-source imports kept with explicit
    provenance in `manifest.zon`
    - `libjpeg_turbo/` for the official `testimages/` subset
    - `libjpeg_turbo_seed_corpora/` for the official OSS-Fuzz seed-corpora subset
- `png/`
  - static PNG fixtures
- `gif/`
  - animated and static GIF fixtures
- `bmp/`
  - BMP fixtures, only if BMP parity is retained
- `pdf/`
  - product-path fixtures that exercise PDF image decode behavior
- `manifest.zon`
  - metadata for the checked-in fixtures

Fixture policy:

- Prefer a small curated subset over bulk-importing large upstream suites.
- Keep imported upstream fixtures in a dedicated source bucket and preserve
  their original filenames when practical.
- Keep fixtures grouped by the behavior they exist to verify.
- Store the expected output shape and a stable pixel hash in `manifest.zon`.
- Mark intentionally unsupported inputs as `known_unsupported`.
- Mark malformed or truncated inputs as `invalid`.

The current corpus is intentionally small. Add fixtures incrementally as each
codec lane is implemented or broadened.

For broader upstream sweeps that should not bloat the checked-in corpus, use the
opt-in harnesses in [`../../lib/image/e2e`](../../lib/image/e2e), starting with
the `libjpeg-turbo/seed-corpora` JPEG runner.
