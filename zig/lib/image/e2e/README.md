# Image E2E

This directory holds opt-in end-to-end harnesses for broad upstream image
corpora. These are not part of the checked-in conformance contract in
`testdata/image/`; they are a wider regression sweep.

Current harness:

- [`../src/image_jpeg_seed_corpora_e2e.zig`](../src/image_jpeg_seed_corpora_e2e.zig)
  - clones or reuses the official `libjpeg-turbo/seed-corpora` checkout
  - walks all `.jpg` / `.jpeg` files
  - probes each JPEG in a subprocess so decoder panics are isolated as `CRASH`
  - reports parse/decode/crash outcomes plus a final summary
  - can optionally triage the remaining `DECODE_FAIL` bucket with local `djpeg`
    to separate real compatibility gaps from files upstream rejects too
  - can compare one JPEG directly against `djpeg` output and report first-byte
    mismatch details for a reproducible conformance case

Suggested usage:

```sh
zig build lib-image-conformance
```

The target fetches missing JPEG seed and OpenJPEG corpora and reuses cached
fixtures. Add `-Dconformance-fetch=false` for an offline run, or
`-Dconformance-fixtures=/absolute/path` to change the cache directory.

For JPEG-specific triage, build the runner once and invoke it directly:

```sh
zig build image-jpeg-seed-corpora-e2e
./zig-out/bin/image-jpeg-seed-corpora-e2e run /tmp/libjpeg-turbo-seed-corpora
./zig-out/bin/image-jpeg-seed-corpora-e2e triage-djpeg /tmp/libjpeg-turbo-seed-corpora
```

Quick local status:

```sh
zig run lib/image/src/image_jpeg_seed_corpora_e2e.zig -- status /tmp/libjpeg-turbo-seed-corpora
```

Or directly:

```sh
zig run lib/image/src/image_jpeg_seed_corpora_e2e.zig -- run /tmp/libjpeg-turbo-seed-corpora
```

Optional `djpeg` triage:

```sh
zig run lib/image/src/image_jpeg_seed_corpora_e2e.zig -- triage-djpeg /tmp/libjpeg-turbo-seed-corpora
```

Optional `djpeg` pixel-parity sweep:

```sh
zig run lib/image/src/image_jpeg_seed_corpora_e2e.zig -- triage-djpeg-parity /tmp/libjpeg-turbo-seed-corpora
```

The `djpeg`-based commands pin the reference decoder to scalar
`JSIMD_FORCENONE=1 djpeg -dct int`, so parity results do not depend on local
default DCT settings or libjpeg-turbo SIMD backend differences.

Single-file parity inspection:

```sh
zig run lib/image/src/image_jpeg_seed_corpora_e2e.zig -- compare-one /tmp/libjpeg-turbo-seed-corpora bugs/decompress/github_347/overflow2.jpg
```

`compare-one` prints:
- parsed JPEG kind, dimensions, sample precision, component count, scan count,
  restart interval, and sampling
- decoded and `djpeg` hashes
- differing byte count
- differing byte counts per RGBA channel
- mismatch bounding box in image coordinates
- number of mismatching `8x8` blocks and their bounding box
- first mismatch byte index
- first mismatch pixel index, `x,y`, `8x8` block, and RGBA channel
- the exact decoded vs `djpeg` RGBA tuple at that pixel
- a short byte window around the first mismatch
