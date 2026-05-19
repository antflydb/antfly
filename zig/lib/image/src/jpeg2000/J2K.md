# JPEG 2000 Native Conformance Tracker

Reference implementation: `/Users/ajroetker/go/src/github.com/ajroetker/go-jpeg2000`.

Current ISO Part 1 baseline before this work:

```text
zig build lib-image-conformance-run
pass=4 fail=1 skip=18
```

Current ISO Part 1 result after promoting the OpenJPEG-parity fixtures:

```text
zig build lib-image-conformance-run
pass=23 fail=0 skip=0
```

Current nonzero tolerances are pinned to local OpenJPEG 2.5.x parity against
the same PGX references:

| Fixture | Reference set | OpenJPEG max_err | Zig max_err |
| --- | --- | ---: | ---: |
| `p0_04` | `baseline/conformance` | 2 | 2 |
| `p0_05` | `baseline/conformance` | 1 | 1 |
| `p0_06` | `baseline/nonregression` | 1 | 1 |
| `p1_02` | `baseline/conformance` | 2 | 2 |
| `p1_03` | `baseline/conformance` | 1 | 1 |
| `p1_04` | `baseline/conformance` | 253 | 253 |
| `p1_05` | `baseline/conformance` | 15 | 15 |
| `p1_06` | `baseline/conformance` | 1 | 1 |

## Performance Tracker

- [x] Add a ReleaseFast image benchmark target.
  - Command: `zig build lib-image-bench -- jpeg2000-decode <path> [iterations]`.
  - Command: `zig build lib-image-bench -- jpeg2000-openjpeg-compare <path> [iterations]`.
  - Alias: `zig build bench-image -- ...`.

- [x] Add OpenJPEG comparison timing.
  - The comparison shells out to `opj_decompress` and writes PGX output, so it measures the OpenJPEG CLI path rather than the libopenjp2 API directly.
  - Local baseline snapshots:
    - `p1_06`, 2 iterations: Zig `1.402 ms/iter`, OpenJPEG CLI `3.058 ms/iter`; tiny fixture dominated by process startup.
    - `p0_03`, 2 iterations: Zig `6.521 ms/iter`, OpenJPEG CLI `4.929 ms/iter`.
    - `p0_04`, 2 iterations: Zig `136.152 ms/iter`, OpenJPEG CLI `37.138 ms/iter`.

- [x] SIMD inverse RCT.
  - `color_transform.inverseRct` now processes eight i32 samples per lane group before the scalar tail. This targets reversible MCT codestreams without changing the arithmetic formula.

- [x] Reuse DWT line scratch buffers.
  - `wavelet.inverse53LevelInPlacePhase` and `wavelet.inverse97LevelInPlacePhase*` now allocate row/column lifting scratch once per level instead of allocating inside every line transform.
  - Local before/after snapshots:
    - `p1_05`: Zig `387.404 ms` -> `30.711 ms`; OpenJPEG CLI `15.122 ms` after the change; gap now `2.031x`.
    - `p0_04`: Zig `136.152 ms` -> `99.844 ms`; OpenJPEG CLI `33.991 ms` after the change; gap now `2.937x`.
    - `p0_05`: Zig `620.315 ms` -> `482.595 ms`; OpenJPEG CLI `140.434 ms` after the change; gap now `3.436x`.
    - `p0_07`: Zig `3081.505 ms` -> `2521.924 ms`; OpenJPEG CLI `1238.052 ms` after the change; gap now `2.037x`.

- [ ] Next performance targets.
  - Add a suite-style benchmark over selected ISO fixtures so small-file process startup does not hide decoder hot spots.
  - Profile Tier-1 coefficient decode and inverse 5/3/9/7 DWT; those are more likely to matter than color conversion on large codestreams.
  - Specialize U8/U16 sample reconstruction loops; `p0_05` remains the largest measured gap after DWT scratch reuse.
  - Consider a libopenjp2 binding later if we need an apples-to-apples library comparison instead of CLI comparison.

## Task List

- [x] Signed samples
  - Reference: Go `convertToRGBA` / `convertToRGBAFloat` always add `2^(bitDepth-1)` before producing unsigned display samples.
  - Zig status: native decode now accepts signed SIZ/BPCC components, U8/U16 reconstruction biases samples into unsigned output space, and ISO PGX comparison applies the same signed bias.
  - Fixture status: signed samples are no longer the first blocker; `p0_03`, `p0_07`, `p0_08`, and `p0_15` now pass.

- [x] Multiple tile-parts per tile
  - Reference: Go packet/tile decode consumes tile-part payloads as a tile-level stream.
  - Zig status: parser and payload collection append contiguous tile-parts per tile. The conformance harness now compares subsampled ISO references against decoded component-grid planes instead of full display-grid pixels.
  - Fixture status: `p0_10` passes as `64x64x3` with `max_err=0`.

- [x] CRG component registration
  - Reference: Go accepts the marker for reference conformance. Confirm whether offsets are neutral in ISO fixtures before applying registration shifts.
  - Zig status: CRG is accepted by the conformance harness instead of pre-skipped. No registration offset application has been needed yet.
  - Fixture status: CRG is no longer the first blocker; `p0_03` and `p0_15` now pass.

- [ ] POC progression-order changes
  - Reference: Go processes POC entries sequentially, clamps marker bounds to the actual tile/component/layer limits, suppresses duplicate packet coordinates across overlapping ranges, and lets tile-part POC entries override the main-header POC/COD for that tile.
  - Zig status: packet layout now builds POC windows from parsed `PocEntry` values, uses the requested progression order per window instead of the COD default, clamps oversized ISO marker bounds, suppresses duplicate packet coordinates, and forwards tile-part-scoped POC entries into per-tile decode states. No-precinct POC streams with positive-area but codeblock-empty resolution packets are enumerated for the high-component p0_13 layout.
  - Fixture status: `p0_03`, `p0_07`, `p0_13`, and `p0_15` pass. `p0_13` is validated against the class-0 `c0p0_13.pgx` reference because the shipped split class-1 PGX files are not OpenJPEG-parity for the first component-plane samples.

- [x] PLT packet-length accounting
  - Reference: Go parses packet boundaries directly from the tile stream while explicitly consuming SOP/EPH around each packet.
  - Zig status: tile-part-scoped `PLT` markers are retained on `TilePart`, packet lengths are adjusted for stripped SOP/EPH bytes before tier-2 parsing, and the packet-header reader consumes a pending stuffed byte when a header ends at `0xff 00` immediately before `EPH`.
  - Fixture status: `p0_07` advances from `TruncatedPacketBody` to `PASS width=2048 height=2048 components=3 max_err=0`. `p0_08` now passes at its reduced reference resolution (`257x1536`, `max_err=0`).

- [x] RGN max-shift ROI
  - Reference: Go adds the RGN shift to the code-block bitplane budget, then de-shifts raw coefficient magnitudes before dequantization/reconstruction when `abs(coeff) >= 1 << SPrgn`.
  - Zig status: RGN entries are parsed, ROI shifts feed Tier-1 bitplane planning, coefficient magnitudes are de-shifted before reconstruction, and multi-tile temporary states now forward `rgn_entries`.
  - Fixture status: `p0_03` and `p0_15` pass with `max_err=0`. `p0_06` now compares against openjpeg-data's `baseline/nonregression/opj_c1p0_06_*.pgx` references instead of the older class-1 conformance PGX files; current OpenJPEG differs from those nonregression references by at most 1 sample.

- [ ] Component counts beyond 1 or 3
  - Reference: Go image output handles 2-component grayscale-ish and 3+ component RGB/RGBA-style output by selecting/displaying supported channels.
  - Zig status: conformance decoding can now use component-grid planes for non-1/3 component counts, heterogeneous component dimensions, and U16 component-plane comparison without changing the public interleaved image output shape. Remaining blockers are fixture-specific rather than the old top-level component count gate.
  - Fixture status: `p0_04` passes against the three shipped class-1 references. `p0_05` passes component-plane comparison with `max_err=1`; `p0_06` passes against the OpenJPEG nonregression PGX set with tolerance 1; `p0_13` now decodes its 257-component packet structure and passes the class-0 reference. `p1_03` now passes with `max_err=1`.

- [x] Reduced-resolution references
  - Reference: OpenJPEG `opj_decompress -r 1` output for `p0_08` matches the bundled `c1p0_08_*.pgx` dimensions and samples.
  - Zig status: component-plane decode can discard the highest-resolution DWT levels by stopping inverse synthesis early and cropping the active top-left resolution. The ISO harness marks fixtures with `reduction_levels` and compares the reduced component planes directly.
  - Fixture status: `p0_08` now passes as `257x1536x3` with `max_err=0`.

- [x] Non-zero code-block style flags
  - Reference: Go EBCOT handles BYPASS, RESET, TERMALL, VSC, PTERM, and SEGSYM.
  - Zig status: the native style gate now accepts BYPASS, RESET, TERMALL, VSC, PTERM, and SEGSYM. `CodeBlockStyle` maps VSC to `0x08` and PTERM to `0x10`, RESET context state is reset after MQ passes, VSC is applied to zero-coding/sign/refinement contexts, Tier-2 reads BYPASS segment lengths using the OpenJPEG/Go segment pattern, and raw BYPASS reads synthesize OpenJPEG-style all-ones padding after segment end. The decoder keeps compatibility with the local encoder's cumulative per-pass boundaries.
  - Fixture status: `p0_02`, `p0_11`, and all promoted `p1_*` style fixtures now pass. `p1_02` decodes with RESET/VSC plus PPT at `max_err=2`; `p1_05` decodes its PCRL/PPM/BYPASS/PTERM stream at `max_err=15`, matching OpenJPEG's tolerance against the bundled class-1 PGX baseline; `p1_06` passes after singleton 9/7 synthesis became a no-op like OpenJPEG/Go; `p1_07` passes after origin-aware RPCL grouping and precinct-capped foreign code-block geometry.

- [x] Non-zero SIZ/tile-origin DWT parity
  - Reference: Go DWT synthesis passes per-resolution `X0/Y0` parity (`cas`) into the 5/3 and 9/7 inverse transforms.
  - Zig status: SIZ `XOsiz/YOsiz/XTOsiz/YTOsiz` are retained in `Header`, tile/component geometry uses origin-aware spans, 5/3 and 9/7 inverse synthesis accept per-resolution parity, singleton 9/7 lines are left unchanged, and multi-tile decode now builds tiles from the origin-aware tile grid.
  - Fixture status: `p1_01`, `p1_05`, and `p1_06` now pass. `p1_05` also uses OpenJPEG-compatible high-pass scaling/gain handling for foreign 9/7 codestreams.

- [ ] Existing pixel mismatch
  - Target fixture: `p1_04`.
  - Current status: passes under the current tolerance gate with `max_err=253`.
