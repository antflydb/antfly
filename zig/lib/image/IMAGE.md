# Image Support

This file defines the shared image-codec design for `antfly-zig` and
`antfly-inference-zig`.

Use it to answer:

- what "pure Zig image stack" means for the combined project
- which image features we actually need to preserve
- which conformance data we should trust before removing `stb_image`

Sibling repo references:

- [`lib/image/src/mod.zig`](/Users/ajroetker/go/src/github.com/antflydb/antfly-zig/lib/image/src/mod.zig)
- [`lib/pdf/src/reader.zig`](/Users/ajroetker/go/src/github.com/antflydb/antfly-zig/lib/pdf/src/reader.zig)
- [`build.zig`](/Users/ajroetker/go/src/github.com/antflydb/antfly-zig/build.zig)
- [`../antfly-inference-zig/src/pipelines/image.zig`](/Users/ajroetker/go/src/github.com/antflydb/antfly-inference-zig/src/pipelines/image.zig)
- [`../antfly-inference-zig/lib/chunker/src/fixed_multimodal.zig`](/Users/ajroetker/go/src/github.com/antflydb/antfly-inference-zig/lib/chunker/src/fixed_multimodal.zig)
- [`../antfly-inference-zig/build.zig`](/Users/ajroetker/go/src/github.com/antflydb/antfly-inference-zig/build.zig)

## Public Surface

The combined project should have one shared image runtime in
`antfly-zig/lib/image`, implemented in Zig, with no dependency on
`stb_image`, `@cImport`, or image-related C compilation.

That shared layer should own:

- static image decode
- animated GIF frame extraction
- PNG encode
- PDF-oriented image decode helpers
- resize / normalize / CHW preprocessing

`antfly-inference-zig` should consume that shared layer rather than maintaining a
second decode stack.

## Supported Formats

The shared image layer currently supports:

- `antfly-zig/lib/image` is pure Zig for:
  - JPEG decode
  - PNG encode/decode
  - GIF decode/frame extraction
  - CCITT fax decode
  - image preprocessing
- `antfly-inference-zig` has already been switched onto the shared `antfly_image` path
  for image decode and GIF extraction work
- image-related `stb_image` usage has been removed from the shared image path

Current non-core format boundaries:

- PNG decode currently covers the checked-in 1/2/4/8/16-bit grayscale,
  grayscale+alpha, grayscale-with-`tRNS`, RGB, RGB-with-`tRNS`, RGBA,
  1/2/4/8-bit indexed-palette, indexed-palette-with-`tRNS`, and
  Adam7-interlaced corpus fixtures
- PNG corpus hardening now also covers ignored ancillary `tEXt` chunks and
  malformed non-palette `tRNS` lengths
- PNG ancillary safe-ignore coverage now includes checked fixtures for `gAMA`
  and `sRGB`
- GIF decode now has corpus-backed coverage for animated local-palette and
  interlaced image-descriptor cases, transparent overlays over a non-empty
  canvas, plus disposal `background` and disposal `previous` over a non-empty
  canvas
- GIF hardening also covers ignored Comment Extensions and an out-of-bounds
  image-descriptor failure path
- BMP remains deliberately out of scope unless reintroduced explicitly

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

Checked-in JPEG conformance coverage now includes real file-backed `4:1:1`
YCbCr fixtures in baseline, progressive, arithmetic, and
arithmetic-progressive form, in addition to the older `4:4:4`, `4:2:2`,
`4:2:0`, `4:1:0`, `4:4:0`, baseline/progressive/arithmetic CMYK/YCCK,
restart, arithmetic, 12-bit, and lossless corpus slices.

The 8-bit YCbCr color path now matches default `djpeg` fancy chroma
upsampling for the checked-in upstream `libjpeg-turbo/seed-corpora` subset,
including the Mozilla `kitty2.jpg` baseline `4:2:0` regression image.

The reduced-to-`rgba8` 12-bit path now keeps native sample precision through
plane write, upsampling, and final color conversion, which brings the
checked-in 12-bit extended-sequential seeds into scalar `djpeg` parity while
preserving the repo's `rgba8` contract for those files.

So the remaining work is no longer "replace `stb_image`." The remaining work is
conformance hardening, broader corpus coverage, and non-core format decisions
such as BMP parity.

## Target Architecture

`antfly-zig/lib/image` should become the only image codec boundary shared by
both repos.

Recommended modules:

- `decode.zig`
  - format sniffing
  - generic static-image decode entrypoints
- `jpeg.zig`
  - pure Zig JPEG decode
- `png.zig`
  - existing PNG encode plus pure Zig PNG decode
- `gif.zig`
  - pure Zig animated GIF decode and frame composition
- `bmp.zig`
  - only if we choose to preserve BMP input parity in `antfly-inference-zig`
- `processing.zig`
  - shared resize / normalize / CHW routines
- `ccitt.zig`
  - existing PDF-oriented CCITT support

Recommended shared API shape:

- `decode(alloc, bytes) -> DecodedImage`
- `decodeFormat(alloc, format, bytes) -> DecodedImage`
- `decodeGifFramesAlloc(alloc, bytes) -> []Frame`
- `png.encodeRgba(alloc, width, height, rgba) -> []u8`

Recommended core types:

- `DecodedImage`
  - `pixels: []u8`
  - `width: u32`
  - `height: u32`
  - `format: PixelFormat`
- `Frame`
  - `rgba: []u8`
  - `width: u32`
  - `height: u32`
  - `delay_ms: u32`

## Required Format Surface

This is the practical minimum to replace the current behavior across both
repos.

### Required now

- JPEG decode
  - needed by PDF `DCTDecode`
  - needed by antfly inference vision pipelines
- PNG decode
  - needed by antfly inference vision pipelines
- GIF decode with animation frames and delays
  - needed by antfly inference chunking
- PNG encode
  - already present and needed for GIF frame chunk output

### Optional but likely needed for parity

- BMP decode
  - `antfly-inference-zig` currently documents and implements `JPEG/PNG/BMP/GIF`
    acceptance in its native image decode path

### Not required for the current migration

- APNG decode
- WebP decode
- TIFF decode
- PDF `JPXDecode`
- PDF `JBIG2Decode`

Those may be future work, but they should not block removing `stb_image` if the
rest of the project does not currently depend on them.

## JPEG Scope

JPEG is the highest-risk codec in this migration.

For the combined project, baseline-only JPEG support is probably not enough.
PDF test coverage in `antfly-zig` is currently simple, but `antfly-inference-zig`
accepts arbitrary user-supplied images in vision flows. That means the
practical first release target should be:

- 8-bit JPEG decode
- grayscale and YCbCr
- common subsampling:
  - 4:4:4
  - 4:2:2
  - 4:2:0
- restart markers
- custom Huffman tables
- progressive JPEG

Nice-to-have but not first-blocker:

- lossless JPEG variants

## GIF Scope

GIF is the second major risk because antfly inference chunking needs animation semantics,
not just single-frame decode.

The shared GIF decoder should cover:

- global and local color tables
- transparency index
- interlaced GIFs
- animation delay extraction
- disposal methods:
  - none
  - background
  - previous
- multi-frame composition onto a logical screen

If disposal logic is wrong, antfly inference frame chunking will silently regress even if
the decoder appears to work on trivial samples.

## PNG Scope

For the current migration, the static PNG decoder should cover:

- grayscale, RGB, indexed, grayscale+alpha, RGBA
- bit depths needed by real-world inputs:
  - 1
  - 2
  - 4
  - 8
  - 16 if we want broad decoder credibility
- all standard scanline filters
- `PLTE`
- `tRNS`
- `iCCP`, `sRGB`, `gAMA`, and ancillary-chunk handling at least to the extent
  required for correct decode and safe ignore behavior
- Adam7 interlace

APNG is not required for current parity, but the PNG spec and tests now include
it, so the doc and APIs should keep room for later expansion.

## BMP Scope

BMP should be treated as a parity decision, not as an automatic requirement.

Decision: BMP is currently dropped from the shared image layer. The shared
runtime target is JPEG, PNG, and GIF. `antfly-inference-zig` should be treated as having
that narrowed native decode contract unless we later decide to reintroduce BMP
for a concrete product reason.

If we want the shared layer to preserve termite's current input contract, BMP
decode should cover:

- uncompressed indexed and truecolor BMPs
- top-down and bottom-up orientation
- bitfields
- RLE only if we actually want strong BMP compatibility

If we do not want to support BMP, then termite's public/native decode contract
needs to be narrowed deliberately and documented.

## Migration Order

### Phase 1: Shared boundary and JPEG

- add a generic decode API to `antfly-zig/lib/image`
- replace `lib/image/src/jpeg.zig` with pure Zig JPEG decode
- switch `lib/pdf` to the new shared JPEG path
- add a narrow `lib/pdf` `/JPXDecode` renderer path through shared JPEG 2000
  decode
- keep `stb_image` alive in antfly inference during this phase
- use `stb_image` and `libjpeg-turbo` as temporary decode oracles in tests

Exit criteria:

- no image-related C build hooks remain in `antfly-zig`
- PDF `DCTDecode` tests pass
- PDF `JPXDecode` image XObject tests pass for an RGB JP2 stream
- shared JPEG corpus passes against expected outputs

PDF JPX scope:

- supported now: `/JPXDecode` image XObjects whose decoded JPEG 2000 output is
  1-component grayscale or 3-component RGB, with dimensions matching the PDF
  image dictionary
- intentionally deferred: alpha-channel JPX, CMYK/four-component JPX, full ICC
  profile semantics, PDF color-space overrides, and chained pre-filters before
  `JPXDecode`

JPEG 2000 production-blocker status, April 2026:

- PPM/PPT packed packet headers are no longer a native-decode support blocker.
  The decoder now collects main-header `PPM` and tile-part `PPT` streams,
  parses tier-2 packet headers from that split header payload, and consumes
  code-block bodies from the SOD payload.
- Regression coverage includes synthetic real-marker `PPM` and `PPT` streams
  built from a normal encoded codestream by moving packet-header bytes out of
  SOD and updating `Psot`.
- The external `openjpeg-data` ISO corpus is populated at
  `/tmp/openjpeg-data` via `zig build lib-image-conformance-fetch` and is part
  of this investigation.
- The ISO harness now compares high-bit-depth fixtures through
  `decodeU16Bytes`, so 12-bit samples are checked against the PGX references at
  native precision instead of through the reduced `u8` path.
- `p1_04` is no longer a production blocker. The remaining 12-bit 9/7
  irreversible delta was tier-1 coefficient reconstruction: foreign
  irreversible streams now use OpenJPEG-style doubled midpoint magnitudes and
  divide by two during dequantization, preserving exact-bitplane behavior for
  antfly-produced streams. With tile-part QCD selection, the fixture now matches
  local OpenJPEG 2.5.x output (`max_err=253` against the bundled class-1 PGX;
  first samples match OpenJPEG exactly). The ISO harness pins `p1_04` to that
  OpenJPEG parity tolerance because the bundled PGX differs from current
  OpenJPEG output by the same maximum error.
- `p0_04` is no longer a production blocker for the TERMALL code-block style.
  Tier-2 now records per-pass code-block segment lengths for `cblksty=0x04`,
  tier-1 consumes those terminated MQ segments independently, and explicit
  precinct tag-tree geometry uses detail-subband precinct dimensions. The ISO
  fixture now decodes as `PASS width=640 height=480 components=3 max_err=6`
  against a tolerance of 33.
- `p0_05` is no longer a production blocker. Tier-2 packet tracing matches the
  Go/OpenJPEG-data reference path (`175` packets, `7080` code-block entries,
  `1294519` body bytes), and decode bookkeeping no longer writes first-inclusion
  or zero-bit-plane values back into tag trees after packet consumption. That
  avoids premature parent-minimum propagation while sibling leaves are still
  unknown, fixing the p0_05 ZBP drift and bringing all four component PGX
  comparisons inside tolerance (`PASS width=1024 height=1024 components=4
  max_err=2`).
- `p0_06` is promoted against the OpenJPEG nonregression PGX baseline. It
  decodes through the mixed-component path with tile-part RGN shifts applied in
  both Tier-1 bitplane planning and ROI de-shifting, and now passes with
  `max_err=1`.
- `p0_08` is no longer a production blocker. Reduced-resolution component-plane
  reconstruction now crops the decoded planes to the requested resolution, and
  the fixture decodes as `PASS width=257 height=1536 components=3 max_err=0`.
- `p1_02` now passes (`640x480`, 3 components, `max_err=2`) through the
  RESET/VSC code-block-style path with PPT packet headers. The stale
  `SKIP_UNSUPPORTED` demotion was removed, bringing the ISO matrix to
  `pass=21 fail=2 skip=0`.
- `p1_03` now passes (`1024x1024`, 4 components, `max_err=2`). The fix keeps
  raw BYPASS segment decoding on JPEG2000's 7-bit-after-`0xff` stuffing rule
  and treats exhausted raw segment padding as zero bits instead of failing the
  whole fixture with `EndOfBitstream`.
- `p1_07` now passes (`2x12`, 2 components, `max_err=0`). The fix combines
  origin-aware RPCL position iteration, precinct-capped foreign code-block
  geometry, and true tile-grid offsets in component-plane multi-tile decode.
- `p1_05` and `p1_06` no longer stop in packed-header tier-2 consumption.
  `p1_05` now consumes its PPM/PCRL tile headers and reaches full decode, but
  remains a pixel mismatch (`512x512`, 3 components, `max_err=235`). `p1_06`
  now consumes tiny-tile PPT/default-precinct headers and reaches full decode,
  but remains a pixel mismatch (`12x12`, 3 components, `max_err=237`).
- `p0_07` is no longer a production blocker. Tile-part PLT markers are
  retained with their tile metadata, SOP/EPH-stripped packet payloads carry
  matching packet lengths into tier-2, packet-header byte stuffing consumes a
  terminal stuffed byte after `0xff`, and the fixture now decodes as
  `PASS width=2048 height=2048 components=3 max_err=0`.
- Generated precinct-coded streams now keep encoder and decoder geometry in
  sync: COD writes the stored code-block exponent matching the encoder's
  OpenJPEG-style exponent, and detail-subband precinct assignment uses
  subband-local half-resolution precinct dimensions.

### Phase 2: PNG decode and antfly inference static-image migration

- add pure Zig PNG decode
- switch `antfly-inference-zig/src/pipelines/image.zig` to shared `antfly_image`
- preserve preprocessing behavior and tensor layout
- decide explicitly whether BMP support is retained or dropped

Exit criteria:

- antfly inference vision preprocessing does not depend on `stb_image`
- JPEG and PNG inputs produce stable normalized tensors vs current oracle

### Phase 3: GIF decode and antfly inference chunker migration

- add pure Zig animated GIF decode and frame composition
- switch `antfly-inference-zig/lib/chunker/src/fixed_multimodal.zig`
- preserve frame count, frame order, frame delays, and per-frame RGBA output

Exit criteria:

- antfly inference chunker tests pass without `stb_image`
- animation disposal and delay corpus passes

### Phase 4: Remove remaining C hooks

- remove `stb_image_impl.c` from `antfly-inference-zig/build.zig`
- drop image-related include-path and `link_libc` requirements that existed only
  for image decode
- keep any unrelated C/ObjC build hooks separate from this work

Exit criteria:

- the combined project has no image-related C compilation
- all image conformance and differential tests pass in CI

## Conformance Strategy

We should not rely on ad hoc unit tests alone. The image stack needs a checked,
repeatable conformance corpus with explicit expected outcomes.

Recommended test layers:

### 1. Spec and upstream corpus tests

Use well-known upstream corpora to cover valid and invalid edge cases.

### 2. Differential tests

Before deleting `stb_image`, compare our Zig decoder outputs against:

- current `stb_image` behavior
- `libjpeg-turbo` for JPEG
- `giflib` for GIF when useful
- a strict PNG decoder or validator where needed

Differential testing is a migration aid, not the final contract. When oracles
disagree, the format spec and explicit project decisions win.

### 3. Golden output tests

For checked-in fixtures, store expected:

- width
- height
- pixel format
- frame count
- frame delays
- output hash for full pixel buffers
- expected error class for invalid files

### 4. Real-project regression tests

Keep and expand samples that exercise actual product paths:

- PDF image XObjects in `antfly-zig`
- antfly inference vision preprocessing inputs
- termite GIF chunking inputs

## Recommended Conformance Data

The suites below are the ones we should plan around first.

### PNG

Primary sources:

- W3C PNG Third Edition spec:
  - https://www.w3.org/TR/png-3/
- W3C PNG Third Edition implementation report:
  - https://w3c.github.io/png/Implementation_Report_3e/
- PNGSuite:
  - https://www.libpng.org/pub/png/pngsuite.html

What to use it for:

- baseline valid-image coverage across color types and bit depths
- filter handling
- transparency handling
- interlace coverage
- ancillary-chunk behavior
- a small amount of invalid-file coverage

Recommendation:

- check in a curated subset of PNGSuite plus a manifest
- add a second optional bucket for PNG Third Edition behavior we actually choose
  to support later, such as modern metadata and APNG

### JPEG

Primary sources:

- libjpeg-turbo project docs:
  - https://libjpeg-turbo.org/
- libjpeg-turbo source tree `testimages/`:
  - https://github.com/libjpeg-turbo/libjpeg-turbo/tree/main/testimages
- OSS-Fuzz project for libjpeg-turbo:
  - https://github.com/google/oss-fuzz/tree/master/projects/libjpeg-turbo

What to use it for:

- baseline and progressive JPEG coverage
- subsampling coverage
- grayscale coverage
- restart-marker coverage
- robustness and malformed-input regression seeds

Recommendation:

- use `libjpeg-turbo` as the primary behavioral oracle while the Zig decoder is
  under construction
- keep a small checked-in subset of official `libjpeg-turbo/testimages/`
  fixtures as provenance-tagged reference inputs; the current checked-in subset
  is `testorig.jpg`, `testimgint.jpg`, and `testimgari.jpg`
- keep a second small checked-in subset from the official
  `libjpeg-turbo/seed-corpora` repo for named decompress seeds that cover real
  restart/progressive edge cases without importing the whole corpus
- include large valid overflow-regression seeds in that checked-in upstream
  subset so the baseline path is pinned against DC/coefficient ranges that
  exceed signed 32-bit intermediates
- use the opt-in upstream sweep in
  `lib/image/e2e/README.md` for the full `libjpeg-turbo/seed-corpora` checkout;
  it runs each JPEG in a subprocess so malformed seeds show up as `CRASH`
  instead of aborting the entire run
- build a checked-in minimized JPEG corpus organized by feature bucket:
  - baseline
  - progressive
  - grayscale
  - 4:4:4 / 4:2:2 / 4:2:0
  - CMYK / YCCK if supported
  - invalid, truncated, and marker/table corruption

### GIF

Primary sources:

- GIF89a specification:
  - https://giflib.sourceforge.net/gifstandard/GIF89a.html
- GIFLIB documentation:
  - https://giflib.sourceforge.net/gif_lib.html

What to use it for:

- decoder semantics
- animation framing rules
- extension handling
- disposal and delay behavior

Recommendation:

- use the GIF89a spec as the normative reference
- use GIFLIB as a comparison oracle where practical
- build and check in our own focused GIF corpus because public upstream suites
  are less complete for animation-composition edge cases than PNGSuite is for
  PNG

Required GIF fixture buckets:

- single-frame indexed GIF
- transparency
- interlaced GIF
- local color table overriding global table
- disposal none
- disposal background
- disposal previous
- varying frame delays
- truncated and malformed blocks

### BMP

Primary sources:

- BMP Suite:
  - https://entropymine.com/jason/bmpsuite/

What to use it for:

- broad BMP shape coverage if we keep BMP support
- indexed, truecolor, orientation, bitfields, and invalid examples

Recommendation:

- do not start BMP work unless we explicitly reverse the current drop decision
- if BMP is kept, BMP Suite should be the initial corpus rather than hand-made
  samples only

## Corpus Layout

We should keep a small curated corpus in-repo and make it easy to extend.

Recommended layout:

- `testdata/image/jpeg/`
- `testdata/image/png/`
- `testdata/image/gif/`
- `testdata/image/bmp/`
- `testdata/image/pdf/`
- `testdata/image/manifest.json` or `manifest.zon`

Each manifest entry should describe:

- file path
- format
- expected result:
  - success
  - known-unsupported
  - invalid
- width and height when successful
- frame count when animated
- frame delays when relevant
- expected pixel hash
- notes on why the sample exists

The checked-in corpus should be minimized. Large upstream suites should be
curated into:

- a core always-in-repo bucket
- an optional larger external bucket for periodic validation

## Differential and Oracle Policy

During the migration, we should keep external or old decoders around as
temporary truth sources:

- `stb_image`
  - useful because it matches the current behavior we are replacing
- `libjpeg-turbo`
  - strongest JPEG comparison target
- `giflib`
  - useful for GIF decode and animation semantics
- PNG validators and reference decoders
  - useful for chunk/error handling decisions

But the final acceptance rule should be:

- follow the format specification when it is clear
- document deliberate compatibility decisions when popular decoders disagree
- prefer deterministic behavior over "whatever stb happened to do"

## Acceptance Criteria

We should not delete `stb_image` until all of these are true:

- `antfly-zig/lib/image` has pure Zig JPEG decode
- antfly inference static-image preprocessing uses shared pure Zig decode
- termite GIF chunking uses shared pure Zig GIF decode
- image-related C build hooks are removed from both repos
- a checked conformance corpus exists for JPEG, PNG, and GIF
- invalid-file behavior is tested, not just happy paths
- CI runs the core corpus and product regressions

## Immediate Next Steps

1. Land this plan and treat `antfly-zig/lib/image` as the shared owner.
2. Add a corpus manifest and initial fixture directories.
3. Start with pure Zig JPEG decode plus a strong differential harness.
4. Add PNG decode and migrate antfly inference static image preprocessing.
5. Add GIF decode and migrate antfly inference chunking.
6. Keep BMP out of scope unless the product surface deliberately adds it back.
