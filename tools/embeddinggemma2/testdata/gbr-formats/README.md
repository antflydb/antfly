# Identity-matrix AV1/VP9 preservation corpus

Twelve generated two-second clips cover both codecs, odd/even 127×95/128×96
geometry and 8/10/12-bit full-range identity RGB. AV1 uses High/Professional;
VP9 uses profiles 1/3. `formats.json` records independent FFmpeg plane hashes,
color/profile probes, normalized generation commands and pinned encoder identity.
Every native decoded picture matches; repeated generation reproduces every coded
clip and the corpus byte-exactly. No application relevance labels are supplied.

Regenerate from the repository root:

```sh
python3 tools/embeddinggemma2/generate_gbr_model_corpus.py \
  --aomenc /path/to/pinned/aomenc \
  --output tools/embeddinggemma2/testdata/gbr-formats
```

The [qualification receipts](../../../../zig/lib/video/testdata/gbr-model-validation/README.md)
record pretrained CPU/Metal/Linux preservation, reset/retry/half-close/sustained
checks and the old server's rejection. Reference tools are offline only;
production remains maintained native Zig. Wide-gamut/HDR RGB, camera streams
and application usefulness require separate qualification.
