# Image Support

This file describes the shared image-codec design used by `antfly-zig` and
`antfly-inference-zig` (termite). It also documents the conformance strategy
and corpus that back that design.

Sibling repo references:

- [`lib/image/src/mod.zig`](src/mod.zig)
- [`lib/pdf/src/reader.zig`](../pdf/src/reader.zig)
- [`build.zig`](../../build.zig)
- [`pkg/inference/src/pipelines/image.zig`](../../pkg/inference/src/pipelines/image.zig)
- [`lib/chunker/src/fixed_multimodal.zig`](../chunker/src/fixed_multimodal.zig)
- [`pkg/inference/build.zig`](../../pkg/inference/build.zig)

## Public Surface

`antfly-zig/lib/image` is the single shared image runtime for the combined
project, implemented in Zig with no dependency on `stb_image`, `@cImport`, or
image-related C compilation. `antfly-inference-zig` consumes this shared layer
rather than maintaining a second decode stack.

The shared layer owns:

- static image decode
- animated GIF frame extraction
- PNG encode
- PDF-oriented image decode helpers
- resize / normalize / CHW preprocessing

## Supported Formats

`antfly-zig/lib/image` is pure Zig for:

- JPEG decode
- PNG encode/decode
- GIF decode/frame extraction
- CCITT fax decode
- image preprocessing

`antfly-inference-zig` is switched onto the shared `antfly_image` path for
image decode and GIF extraction work, and image-related `stb_image` usage has
been removed from the shared image path (and, per the acceptance criteria in
this document, from the rest of the migration's C build hooks).

Current non-core format boundaries:

- PNG decode covers the checked-in 1/2/4/8/16-bit grayscale, grayscale+alpha,
  grayscale-with-`tRNS`, RGB, RGB-with-`tRNS`, RGBA, 1/2/4/8-bit
  indexed-palette, indexed-palette-with-`tRNS`, and Adam7-interlaced corpus
  fixtures, plus ignored ancillary `tEXt` chunks, malformed non-palette `tRNS`
  lengths, and ignored `gAMA`/`sRGB` ancillary chunks.
- GIF decode has corpus-backed coverage for animated local-palette and
  interlaced image-descriptor cases, transparent overlays over a non-empty
  canvas, disposal `background` and disposal `previous` over a non-empty
  canvas, ignored Comment Extensions, and an out-of-bounds image-descriptor
  failure path.
- BMP decode is owned by the shared image layer for user-supplied media paths,
  including inference preprocessing and DB `remoteMedia` embeddings. It covers
  uncompressed indexed/truecolor BMPs and rejects bitfields/RLE as typed
  unsupported inputs.
- WebP has a pure-Zig parser/probe boundary in the shared layer, VP8L
  still-image decode paths, and a constrained VP8 lossy still-image decoder
  for intra keyframes with VP8 loop filtering and optional `ALPH` composition.
  `Format.webp`, inference routing, fixture-backed corpus checks, and
  `image/webp` capability advertising are wired. Animated WebP is explicitly
  rejected until frame extraction semantics are implemented (see
  [Open work](#open-work)). The WebP corpus includes generated
  VP8/VP8L/ALPH fixtures, an upstream libwebp VP8 still image fixture, and Go
  x/image reference fixtures covering VP8L, VP8 loop filter variants, odd
  dimensions, photo-like lossy input, and extended ALPH composition. Imported
  WebP fixture provenance and license text are recorded in
  `testdata/image/THIRD_PARTY_FIXTURES.md`.

Current JPEG boundary:

- sequential JPEG is enabled for:
  - grayscale
  - YCbCr baseline, extended-sequential, and arithmetic
  - direct RGB baseline and 12-bit extended-sequential
  - Huffman lossless grayscale and 3-component 1x1-sampling scans,
    with checked upstream predictor-selection coverage for `1`, `2`, `3`,
    `4`, `5`, `6`, and `7`
  - Adobe APP14 CMYK
  - Adobe APP14 YCCK
- progressive JPEG is enabled for:
  - grayscale
  - 3-component YCbCr, including arithmetic-progressive `4:2:2`
  - 4-component Adobe APP14 CMYK
  - 4-component Adobe APP14 YCCK

Checked-in JPEG conformance coverage includes real file-backed `4:1:1` YCbCr
fixtures in baseline, progressive, arithmetic, and arithmetic-progressive
form, in addition to the older `4:4:4`, `4:2:2`, `4:2:0`, `4:1:0`, `4:4:0`,
baseline/progressive/arithmetic CMYK/YCCK, restart, arithmetic, 12-bit, and
lossless corpus slices.

The 8-bit YCbCr color path matches default `djpeg` fancy chroma upsampling for
the checked-in upstream `libjpeg-turbo/seed-corpora` subset, including the
Mozilla `kitty2.jpg` baseline `4:2:0` regression image. The reduced-to-`rgba8`
12-bit path keeps native sample precision through plane write, upsampling, and
final color conversion, matching scalar `djpeg` for the checked-in 12-bit
extended-sequential seeds while preserving the repo's `rgba8` contract.

The original migration goal — "replace `stb_image`" — is done: no
image-related C build hooks remain in either repo (verified by the absence of
`stb_image` references in this tree). What remains is conformance hardening,
broader corpus coverage, and non-core format decisions; see
[Open work](#open-work).

## Architecture

`antfly-zig/lib/image` is the only image codec boundary shared by both repos,
organized as:

- `decode.zig`: format sniffing, generic static-image decode entrypoints
- `jpeg.zig`: pure Zig JPEG decode
- `png.zig`: PNG encode plus pure Zig PNG decode
- `gif.zig`: pure Zig animated GIF decode and frame composition
- `bmp.zig`: pure Zig BMP decode
- `webp.zig`: pure Zig WebP decode (see WebP Scope below)
- `processing.zig`: shared resize / normalize / CHW routines
- `ccitt.zig`: PDF-oriented CCITT support

Shared API shape:

- `decode(alloc, bytes) -> DecodedImage`
- `decodeFormat(alloc, format, bytes) -> DecodedImage`
- `decodeGifFramesAlloc(alloc, bytes) -> []Frame`
- `png.encodeRgba(alloc, width, height, rgba) -> []u8`

Core types:

- `DecodedImage`: `pixels: []u8`, `width: u32`, `height: u32`,
  `format: PixelFormat`
- `Frame`: `rgba: []u8`, `width: u32`, `height: u32`, `delay_ms: u32`

## Format Rationale

This section records why each format is in scope and to what depth.

### JPEG

JPEG was the highest-risk codec in the migration: PDF test coverage in
`antfly-zig` was simple, but `antfly-inference-zig` accepts arbitrary
user-supplied images in vision flows. The practical target, now implemented,
is 8-bit JPEG decode for grayscale and YCbCr, common subsampling (4:4:4,
4:2:2, 4:2:0), restart markers, custom Huffman tables, and progressive JPEG.
Lossless JPEG variants are supported for grayscale and 3-component
1x1-sampling scans (see Supported Formats); broader lossless coverage was
never a hard blocker.

### GIF

GIF needed animation semantics, not just single-frame decode, because antfly
inference chunking depends on correct disposal/composition behavior — if
disposal logic is wrong, frame chunking silently regresses even when the
decoder looks correct on trivial samples. The shared GIF decoder covers global
and local color tables, the transparency index, interlaced GIFs, animation
delay extraction, disposal methods (none, background, previous), and
multi-frame composition onto a logical screen.

### PNG

The static PNG decoder covers grayscale, RGB, indexed, grayscale+alpha, and
RGBA at bit depths 1/2/4/8/16, all standard scanline filters, `PLTE`, `tRNS`,
`iCCP`/`sRGB`/`gAMA` and other ancillary-chunk handling needed for correct
decode and safe-ignore behavior, and Adam7 interlace. APNG is not implemented;
the PNG spec and tests keep room for it.

### BMP

Decision: BMP is part of the shared image layer because `remoteMedia` and
native image preprocessing can receive user-supplied BMPs through the same
media path as JPEG, PNG, and GIF. BMP support lives in `lib/image`, not in
antfly inference, so all callers get the same format detection, decode
errors, and corpus coverage.

Initial BMP decode covers uncompressed indexed and truecolor BMPs, top-down
and bottom-up orientation, 1/4/8-bit indexed BMPs with BGRA palettes, 24-bit
BGR, and 32-bit BGRX/BGRA (treating all-zero alpha planes as opaque BGRX).
Bitfields, RLE compression, OS/2 CORE headers, and color management metadata
are out of scope for the first implementation; those inputs fail as
unsupported BMP rather than falling through to a generic decode failure.

BMP conformance coverage lives in `testdata/image/bmp/` and
`testdata/image/manifest.zon`. The checked-in core corpus includes
orientation, indexed 1/4/8-bit, known-unsupported bitfields/RLE, and truncated
invalid fixtures. Broader BMP Suite imports are curated incrementally rather
than bulk-imported.

### WebP

Decision: WebP is owned by the shared pure-Zig image layer. WebP is not a thin
container-only feature: production support needs at least RIFF/WebP parsing
plus VP8 lossy and VP8L lossless image decode, and alpha (`ALPH`) for common
real-world files. Animated WebP remains an explicit unsupported case until
frame extraction semantics are implemented.

`webp.zig` implements:

- RIFF/WebP signature and chunk parsing (`probe`, shared `detectFormat()`)
- `VP8 ` lossy still-image decode: keyframe frame-tag/dimension and token
  partition metadata parsing; bool-coded first-partition header parsing;
  macroblock grid sizing; YUV-to-RGBA output; scalar inverse transform;
  luma/chroma intra-prediction primitives; padded YUV frame-plane allocation
  and cropped RGBA conversion; coefficient token/residual block decode and
  segment-aware quant/dequant matrix derivation; macroblock
  segment/skip/luma/chroma mode parsing for 16x16 and 4x4 luma mode paths;
  coefficient probability update parsing wired to the reference default and
  update-probability tables; row-based token partition selection;
  boundary-aware 16x16 intra prediction; Y2/Y1/UV block-ordered coefficient
  reading with token-context updates and Y2-to-Y1 DC propagation; constrained
  raster-order frame assembly for intra 16x16 keyframes; 4x4 luma macroblock
  reconstruction using VP8 edge defaults and already-reconstructed neighbor
  samples; and normal/simple VP8 loop filtering. `decodeRgba` routes
  supported `VP8 ` chunks through this constrained pure-Zig path.
- `VP8L` lossless still-image decode: `decodeRgba` implements prefix codes,
  LZ77, color cache, all inverse transforms, and meta-prefix Huffman groups.
- `VP8X` feature validation (`probe` validates flags and canvas size).
- `ALPH` composition when present: `probe` recognizes ALPH; raw/filter and
  VP8L-compressed alpha-plane decoding helpers exist, and supported lossy
  `VP8 ` decode composes the alpha plane into RGBA.
- Clear rejection for animation until frame extraction is implemented.

Inference calls the shared image layer for WebP and does not contain
independent WebP parsing. Local embedder capabilities advertise exact
supported image MIME types, including `image/webp`, rather than broad
`image/*`. WebP corpus coverage lives in `testdata/image/webp/` and
`testdata/image/manifest.zon`, with success, animated-known-unsupported, and
invalid fixtures checked by `verify-webp`. Imported upstream WebP fixtures
have provenance and license terms recorded in
`testdata/image/THIRD_PARTY_FIXTURES.md`.

### JPEG 2000 (PDF `/JPXDecode`)

Supported: `/JPXDecode` image XObjects whose decoded JPEG 2000 output is
1-component grayscale or 3-component RGB, with dimensions matching the PDF
image dictionary. Intentionally deferred: alpha-channel JPX, CMYK/four-
component JPX, full ICC profile semantics, PDF color-space overrides, and
chained pre-filters before `JPXDecode`.

The external `openjpeg-data` ISO corpus (populated at `/tmp/openjpeg-data` via
`zig build lib-image-conformance`) is the reference conformance suite for this
decoder. As of the last recorded pass, most of the ISO fixtures pass or are no
longer production blockers (PPM/PPT packed packet headers, tile-part PLT
markers, RESET/VSC code-block styles, BYPASS raw-segment padding, RPCL
position iteration, TERMALL code-block style, reduced-resolution component
planes, and mixed-component tile-part RGN shifts are all handled — see
[History](#history) for the fixture-by-fixture record). Two ISO fixtures
remain known pixel mismatches rather than hard failures: `p1_05`
(`512x512`, 3 components, `max_err=235`) and `p1_06` (`12x12`, 3 components,
`max_err=237`); both reach full decode after packed-header tier-2 consumption
fixes, but still disagree with the reference pixels.

## Conformance Strategy

The image stack uses a checked, repeatable conformance corpus with explicit
expected outcomes rather than only ad hoc unit tests.

Test layers:

1. **Spec and upstream corpus tests** — well-known upstream corpora covering
   valid and invalid edge cases.
2. **Differential tests** — compare Zig decoder output against `stb_image`
   (matches the behavior being replaced), `libjpeg-turbo` (JPEG), `giflib`
   (GIF), and PNG validators/reference decoders, used as a migration aid; the
   format specification and explicit project decisions win when oracles
   disagree.
3. **Golden output tests** — checked-in fixtures store expected width, height,
   pixel format, frame count, frame delays, output hash for full pixel
   buffers, and expected error class for invalid files.
4. **Real-project regression tests** — PDF image XObjects in `antfly-zig`,
   antfly inference vision preprocessing inputs, and termite GIF chunking
   inputs.

### Conformance Data By Format

**PNG**: W3C PNG Third Edition spec (https://www.w3.org/TR/png-3/) and
implementation report, plus PNGSuite
(https://www.libpng.org/pub/png/pngsuite.html) for baseline valid-image
coverage, filter/transparency/interlace handling, and ancillary-chunk
behavior. A curated PNGSuite subset plus manifest is checked in.

**JPEG**: `libjpeg-turbo` docs/source (`testimages/`) and the OSS-Fuzz
libjpeg-turbo project as the primary behavioral oracle while the Zig decoder
is under construction. Checked-in subsets: `testorig.jpg`, `testimgint.jpg`,
and `testimgari.jpg` from `libjpeg-turbo/testimages/`; a second subset from
`libjpeg-turbo/seed-corpora` for named decompress seeds covering real
restart/progressive edge cases, including large valid overflow-regression
seeds pinning the baseline path against DC/coefficient ranges that exceed
signed 32-bit intermediates. The opt-in upstream sweep in
`lib/image/e2e/README.md` runs the full `libjpeg-turbo/seed-corpora` checkout,
one JPEG per subprocess so malformed seeds show up as `CRASH` instead of
aborting the whole run.

**GIF**: the GIF89a specification as the normative reference, GIFLIB as a
comparison oracle where practical, and a focused in-house corpus (public
upstream suites are less complete for animation-composition edge cases than
PNGSuite is for PNG). Required fixture buckets: single-frame indexed GIF,
transparency, interlaced GIF, local color table overriding global table,
disposal none/background/previous, varying frame delays, and truncated/
malformed blocks.

**BMP**: the BMP Suite (https://entropymine.com/jason/bmpsuite/) for broader
indexed/truecolor/orientation/bitfield/invalid coverage, layered on top of
small hand-made fixtures for exact parser behavior.

### Corpus Layout

```
testdata/image/jpeg/
testdata/image/png/
testdata/image/gif/
testdata/image/bmp/
testdata/image/webp/
testdata/image/pdf/
testdata/image/manifest.zon
```

Each manifest entry describes file path, format, expected result
(success/known-unsupported/invalid), width and height when successful, frame
count and delays when animated, expected pixel hash, and notes on why the
sample exists. The checked-in corpus stays minimized: a core always-in-repo
bucket plus an optional larger external bucket for periodic validation.

## Out Of Scope

APNG decode, TIFF decode, PDF `JPXDecode` alpha/CMYK variants (see JPEG 2000
above), and PDF `JBIG2Decode` were not required for the original `stb_image`
migration and are not implemented.

## Acceptance Criteria

- `antfly-zig/lib/image` has pure Zig JPEG decode. Met.
- antfly inference static-image preprocessing uses shared pure Zig decode.
  Met.
- termite GIF chunking uses shared pure Zig GIF decode. Met.
- image-related C build hooks are removed from both repos. Met (no
  `stb_image` references remain in the tree).
- a checked conformance corpus exists for JPEG, PNG, and GIF. Met
  (`testdata/image/manifest.zon` and format-specific corpus directories).
- invalid-file behavior is tested, not just happy paths. Met for the formats
  above; WebP and BMP corpora also include invalid/unsupported fixtures.
- CI runs the core corpus and product regressions. Not verified from this
  document; confirm the CI wiring directly if this matters for a change.

## Open work

- WebP animation: frame extraction/composition semantics are not
  implemented. Animated WebP is explicitly rejected today rather than
  silently mishandled.
- JPEG 2000: ISO fixtures `p1_05` (`512x512`, 3 components) and `p1_06`
  (`12x12`, 3 components) reach full decode but still mismatch reference
  pixels (`max_err=235` and `max_err=237` respectively against tolerance 33).
  Alpha-channel JPX, CMYK/four-component JPX, full ICC profile semantics, PDF
  color-space overrides, and chained pre-filters before `JPXDecode` remain
  intentionally deferred.
- Broader BMP Suite and WebP corpus coverage should keep being curated
  incrementally rather than bulk-imported.
- CI wiring for "runs the core corpus and product regressions" is not
  confirmed by this document; verify directly before relying on it.
- Continue broadening VP8/VP8L conformance with additional external/reference
  fixtures as the corpus grows; do not use `libwebp` itself.

## History

### JPEG 2000 production-blocker fixes (April 2026)

- PPM/PPT packed packet headers are no longer a native-decode support
  blocker. The decoder collects main-header `PPM` and tile-part `PPT`
  streams, parses tier-2 packet headers from that split header payload, and
  consumes code-block bodies from the SOD payload. Regression coverage
  includes synthetic real-marker `PPM` and `PPT` streams built from a normal
  encoded codestream by moving packet-header bytes out of SOD and updating
  `Psot`.
- The ISO harness compares high-bit-depth fixtures through `decodeU16Bytes`,
  so 12-bit samples are checked against the PGX references at native
  precision instead of through the reduced `u8` path.
- `p1_04`: the remaining 12-bit 9/7 irreversible delta was tier-1 coefficient
  reconstruction. Foreign irreversible streams now use OpenJPEG-style doubled
  midpoint magnitudes and divide by two during dequantization, preserving
  exact-bitplane behavior for antfly-produced streams. With tile-part QCD
  selection, the fixture matches local OpenJPEG 2.5.x output (`max_err=253`
  against the bundled class-1 PGX; first samples match OpenJPEG exactly). The
  ISO harness pins `p1_04` to that OpenJPEG parity tolerance because the
  bundled PGX differs from current OpenJPEG output by the same maximum error.
- `p0_04` (TERMALL code-block style): tier-2 records per-pass code-block
  segment lengths for `cblksty=0x04`, tier-1 consumes those terminated MQ
  segments independently, and explicit precinct tag-tree geometry uses
  detail-subband precinct dimensions. Decodes as `PASS width=640 height=480
  components=3 max_err=6` against a tolerance of 33.
- `p0_05`: tier-2 packet tracing matches the Go/OpenJPEG-data reference path
  (`175` packets, `7080` code-block entries, `1294519` body bytes), and decode
  bookkeeping no longer writes first-inclusion or zero-bit-plane values back
  into tag trees after packet consumption, fixing premature parent-minimum
  propagation. All four component PGX comparisons are inside tolerance
  (`PASS width=1024 height=1024 components=4 max_err=2`).
- `p0_06`: decodes through the mixed-component path with tile-part RGN shifts
  applied in both Tier-1 bitplane planning and ROI de-shifting, promoted
  against the OpenJPEG nonregression PGX baseline with `max_err=1`.
- `p0_08`: reduced-resolution component-plane reconstruction now crops the
  decoded planes to the requested resolution (`PASS width=257 height=1536
  components=3 max_err=0`).
- `p1_02`: passes (`640x480`, 3 components, `max_err=2`) through the
  RESET/VSC code-block-style path with PPT packet headers. Brought the ISO
  matrix to `pass=21 fail=2 skip=0` at the time of this fix (later fixes below
  changed the exact pass/fail split further).
- `p1_03`: passes (`1024x1024`, 4 components, `max_err=2`) by keeping raw
  BYPASS segment decoding on JPEG2000's 7-bit-after-`0xff` stuffing rule and
  treating exhausted raw segment padding as zero bits instead of failing with
  `EndOfBitstream`.
- `p1_07`: passes (`2x12`, 2 components, `max_err=0`) by combining
  origin-aware RPCL position iteration, precinct-capped foreign code-block
  geometry, and true tile-grid offsets in component-plane multi-tile decode.
- `p1_05`/`p1_06`: no longer stop in packed-header tier-2 consumption
  (`p1_05` consumes PPM/PCRL tile headers, `p1_06` consumes tiny-tile
  PPT/default-precinct headers) but remain pixel mismatches — see
  [Open work](#open-work).
- `p0_07`: tile-part PLT markers are retained with their tile metadata,
  SOP/EPH-stripped packet payloads carry matching packet lengths into tier-2,
  packet-header byte stuffing consumes a terminal stuffed byte after `0xff`,
  and the fixture decodes as `PASS width=2048 height=2048 components=3
  max_err=0`.
- Generated precinct-coded streams keep encoder and decoder geometry in sync:
  COD writes the stored code-block exponent matching the encoder's
  OpenJPEG-style exponent, and detail-subband precinct assignment uses
  subband-local half-resolution precinct dimensions.
