# Scalable AV1 model presentation corpus

This corpus adds the maintained two-spatial/three-temporal-layer AV1 fixture to
the nine depth/chroma controls. Each coded temporal unit is retained byte for
byte in deterministic WebM. At operating point zero the reference presents 12
128×96 pictures over two seconds; the library's explicit all-layer policy also
publishes the 12 lower-resolution dependency pictures.

Generate from the repository root:

```sh
python3 tools/embeddinggemma2/generate_av1_model_corpus.py \
  --include-scalable --output tools/embeddinggemma2/testdata/av1-layers
```

The generator checks every coded packet against the pinned IVF, probes the
presentation count/geometry and compares every independent FFmpeg decoded plane
hash against the pinned AOM operating-point-zero/top-layer oracle in
`zig/lib/video/testdata/av1-extended/layers.json`. `formats.json` retains these
checks. FFmpeg and AOM are offline qualification tools; production decoding
remains maintained Zig source with no codec runtime dependency.

Use `tools/embeddinggemma2/video_compare.py` with this `corpus.json`, the pinned
checkpoint and CPU/native/Metal backends. Qualification measures preservation of
the reference's vectors and rankings, not end-user application relevance.
Custom operating-point selection, spatial switching and arbitrary scalable
streams require their own model qualification.

The [model qualification receipts](../../../../zig/lib/video/testdata/av1-layer-model-validation/README.md)
record old-server failure and corrected CPU/Metal/Linux vector/ranking parity,
including independent stress outcomes and source hashes.
