# JPEG 2000 codec — architectural map

Pure-Zig implementation of ISO/IEC 15444-1 Part 1. Public entry points live in `mod.zig`, `encode.zig`, `decode.zig`. Shared conformance entry points are exposed through `lib/image/src/conformance.zig`, mirroring `lib/audio/src/conformance.zig`.

## Running the tests

- `zig build test` — full lib suite. Includes every jpeg2000 module wired through `mod.zig` (box, markers, arithmetic, codeblock, wavelet, tagtree, tile, upsample, quantization, color_transform, tier1_encode, tier2_encode, encode, decode, rate_control, codestream_write, codestream, reconstruct, packet, cross_validation) and the ISO Part 1 conformance matrix through `lib-image-test`. The ISO matrix self-skips when `/tmp/openjpeg-data` is absent.
- `zig build unit-test` — same as above minus simulation/chaos suites; fastest way to iterate on jpeg2000 changes.
- `zig build lib-image-test` — shared image tests.
- `zig build lib-image-conformance-run` — checked-in JPEG 2000 conformance plus the external ISO matrix without fetching. The external matrix self-skips when `/tmp/openjpeg-data` is absent. Current baseline when fixtures are present: `pass=4, fail=1, skip=18` of 23 fixtures.
- `zig build lib-image-conformance-fetch` — one-time populate `/tmp/openjpeg-data` via shallow clone of `openjpeg-data` at a pinned commit.
- `zig build lib-image-conformance` — fetch and run the image conformance suites.
- `zig test lib/image/src/jpeg2000/conformance.zig --test-filter "checked-in jpeg2000 conformance corpus"` — 25-case synthetic round-trip suite (no fixtures needed).
- `zig test lib/image/src/jpeg2000/cross_validation.zig` — OpenJPEG interop; auto-skips when `opj_compress`/`opj_decompress` are not on PATH.
- `zig build image-jpeg2000-fuzz -- <corpus-dir>` — fuzz binary.

## Layering

| Concern                       | File(s) |
|-------------------------------|---------|
| Public API (encode/decode)    | `mod.zig`, `encode.zig`, `decode.zig` |
| JP2 container (boxes)         | `box.zig` |
| Codestream (markers + state)  | `codestream.zig`, `codestream_write.zig`, `markers.zig` |
| Tier-2 (packets, tag-trees)   | `tier2_encode.zig`, `packet.zig`, `tagtree.zig` |
| Tier-1 (EBCOT, MQ, bit-plane) | `arithmetic.zig`, `tier1_encode.zig`, `codeblock.zig` |
| Geometry (tiles/precincts)    | `tile.zig` |
| Wavelet                       | `wavelet.zig` |
| Quantization                  | `quantization.zig` |
| Color transforms (RCT/ICT/MCT)| `color_transform.zig`, `color.zig` |
| Rate control (PCRD)           | `rate_control.zig` |
| Upsampling (subsampled comp.) | `upsample.zig` |
| Reconstruction / IDWT plumbing| `reconstruct.zig` |
| Conformance harness           | `conformance.zig`, `test_support.zig` |
| Fuzz entry point              | `../jpeg2000_fuzz.zig` (binary) |

## Markers

Parsed and emitted end-to-end: `SOC`, `SIZ`, `COD`, `COC`, `QCD`, `QCC`, `SOT`, `SOD`, `EOC`, `POC`, `TLM`, `PLT`, `SOP`, `EPH`, `RGN`, `MCT`, `MCC`, `MCO`, `COM`.

Parsed + validated, not yet consumed during decode (packet headers still read from SOD bitstream): `PPM`, `PPT`.

Parsed-only (parse-and-preserve; not consumed during decode): `CRG`, `CBD`.

Key call sites:
- Parse dispatch: `codestream.zig:parseState`.
- Marker constants: `markers.zig`.
- Encode emission order: `encode.zig:writeCodestream` (SOC → SIZ → COD → QCD → POC → RGN → MCT/MCC/MCO → TLM → per-tile SOT/PLT/SOD → EOC).

## Scod bits (code-block coding style)

All end-to-end round-trippable through `encodeCodeblock` + `codeblock.executeContributionPassPlanMqWithSegments`:

| Bit   | Name    | Notes |
|-------|---------|-------|
| 0x01  | BYPASS  | Lazy / raw-bit SPP & MRP after bit-plane 4; CUP stays MQ. |
| 0x02  | RESET   | Resets MQ contexts at each coding-pass boundary. |
| 0x04  | TERMALL | Flush + restart MQ at every pass; each pass is its own segment. |
| 0x08  | VSC     | Vertically causal context. |
| 0x10  | PTERM   | Predictable termination (encoder emits; decoder accepts, lenient verify). |
| 0x20  | SEGSYM  | Segmentation symbols (0xA UNIFORM nibble after each CUP). |

Validation: `encode.zig:validateEncodeParams` accepts any combination of the above; `ERTERM` and unknown bits return `error.UnsupportedCodeBlockStyle`.

## Wavelet & quantization

- 5/3 reversible (integer): `wavelet.forward53Level` / `wavelet.inverse53LevelInPlace`. U16 lossless verified bit-exact (max_err=0) at 9–16 bpc via self round-trip; the final MQ flush guard in `tier1_encode.zig` now checks the full register tuple rather than just the output buffer.
- 9/7 irreversible (f32): `wavelet.forward97Level` / `wavelet.inverse97LevelInPlace`. Forward is wired in `encode.zig:forward97Pipeline`.
- Quantization formula (irreversible): `quantization.stepSizeIrreversible` implements ISO Δ_b = 2^(R_b−ε)·(1+μ/2048); encoder computes the matching (ε,μ) via `quantization.encodeStepValueIrreversible`.
- QCD styles: 0 (reversible, expn-only), 2 (scalar expounded, per-subband ε/μ). Style 1 (derived) decoded but not emitted.

## Color transforms

- Forward RCT `color_transform.forwardRct` (5/3 path, integer, in `encode.zig:encodeFromShiftedPlanes`).
- Forward ICT `color_transform.forwardIct` (9/7 path, f32, in `encode.zig:forward97Pipeline`).
- Custom MCT matrices (Annex J): `color_transform.CustomMctMatrix{,I32}`, `applyCustomMctForward/Inverse`, `invertMctMatrixGaussJordan`. Markers emitted via `encode.zig:writeCustomMctMarkers` when `EncodeParams.custom_mct != null`.
- Decoder reconstructs a custom 3-component f32 MCT from the MCT/MCC/MCO chain via `reconstruct.buildCustomMctMatrixFromState`; when the marker chain doesn't yield a usable matrix, the built-in ICT is used as the fallback. The MCC parsing matches the minimal placeholder layout emitted by the encoder (first u16 of the MCC payload is the referenced MCT id); full ISO MCC stage/tuple language is not yet implemented. Offsets are not yet serialized on the wire and are treated as zero on decode.

## Rate control

PCRD bisection lives in `rate_control.zig` (`optimizeTruncation`). Wired into encode via `encode.zig:applyPcrdTruncation`, invoked per tile when `EncodeParams.target_bytes` or `target_bitrate` is set.

## Decode-path gate

`codestream.State.fullNativeDecodeSupport` is the single decode-path gate. It accepts multi-tile, XRsiz/YRsiz != 1, styles 0/1/2, progression orders 0..4, 1–16 bpc, 1/3/4/N components. Both `decode.decodeU8Bytes` and `decode.decodeU16Bytes` reject codestreams whose `fullNativeDecodeSupport()` is not `.supported`.

## Subsampling

Decoder handles `XRsiz/YRsiz != 1` for U8 via per-component plane sizing (`tile.componentDimensions`) and bilinear upsampling to the image grid (`upsample.upsampleI32(.bilinear, …)` invoked by `reconstruct.zig`). U16 path rejects subsampled input with `error.UnsupportedSubsampling`. Encoder remains 1:1 only.

## Current interop parity (vs OpenJPEG 2.5.4)

- 5/3 lossless encode → `opj_decompress`: **max_err=0, PSNR=inf** (byte-exact).
- 9/7 lossy encode → `opj_decompress`: **PSNR 53–62 dB, max_err ≤ 4** across synthetic test set.
- 9/7 lossy `opj_compress` → our decoder (8-bit): pixel-match within tolerance.
- Decoder handles multi-tile, XRsiz/YRsiz != 1, styles 0/1/2, progression orders 0..4, 1–16 bpc, 1/3/4/N components.

## Producer discriminator (COM marker)

Encoder emits a general-use `COM` segment (`Rcom=1`) with ASCII payload `"antfly-zig j2k v1"` immediately after QCD (`encode.writeComMarker`). Decoder (`decode.policiesForState`) scans `state.comments` to decide reconstruction policy:

- Reversible 5/3 → always `.exact_bitplane` + `.exact_bitplane`.
- Irreversible 9/7, ours (tag present) → `.exact_bitplane` + `.exact_bitplane` (preserves round-trip PSNR).
- Irreversible 9/7, foreign (tag absent) AND `bits_per_component >= 12` → `.standard_additive` + `.midpoint` (ISO 15444-1 Annex E.1.2).
- Irreversible 9/7, foreign (tag absent) AND `bits_per_component < 12` → `.exact_bitplane` + `.exact_bitplane`. Empirically OpenJPEG-produced 8-bit 9/7 streams decode cleaner under exact at ≥1 bpp; the systematic ~200 LSB offset only surfaces at ≥12 bpc.

Gated by `USE_MIDPOINT_FOR_FOREIGN_IRREVERSIBLE` at the top of `decode.zig`; flip to `false` to force exact_bitplane for all streams.

## Known gaps

- **12-bit 9/7 p1_04 parity not yet verified**: midpoint reconstruction for foreign streams is wired (see above), but parity against OpenJPEG's p1_04 fixtures requires running the ISO conformance suite manually. The ~200 LSB systematic offset is expected to be resolved by the midpoint policy; a unit test in `decode.zig` demonstrates the effect on ours-encoded streams with the COM tag stripped. 8-bit 9/7 cross-validation (PSNR 53+ dB) is unaffected — it was already correct and continues to use the exact path on our own streams.
- **Encoder subsampling** not implemented; encoder emits 1:1 XRsiz/YRsiz regardless.
- **Multi-tile-part splitting** is simplistic: tile data is chunked evenly across `tile_parts_per_tile`; no per-packet rebalancing.
- **PTERM decode** does not cryptographically verify the terminator; it accepts any PTERM-tagged stream.
- **Rate control** uses a bit-plane-weighted distortion heuristic, not coefficient-level MSE. Works well enough for relative ordering but suboptimal compared to OpenJPEG's true MSE.
- **PPM/PPT packed packet headers** parsed + validated; streams carrying them are rejected at the decode gate with `.unsupported_packed_packet_headers`. Wiring consumption into tier-2 is a follow-up.
- **Full MCC/MCT stage/tuple language (Annex G)** not implemented; encoder emits and decoder consumes only the minimal single-matrix layout.

## Fuzz entry point

`zig build image-jpeg2000-fuzz` produces a binary that accepts a corpus directory on argv[1] and feeds every `.j2k` / `.jp2` / `.bin` through `box.parse` (when signatured), `codestream.parseState`, and `decode.decodeU8Bytes`. Reports per-file status and totals.

## Conformance harness

`conformance.zig:runSelfRoundTrip` takes `[]const RoundTripCase` and returns per-case reports (max_err, MSE, PSNR dB, pass/fail). 17 canonical cases cover: 5/3 lossless at various sizes/decomps/progressions/precincts/tile-parts/layers, 9/7 lossy with and without MCT, rate-capped 9/7, SOP+EPH, and the multi-layer regression.
