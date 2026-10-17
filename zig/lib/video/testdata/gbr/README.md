# Identity RGB codec fixtures

These clips are byte-identical to `zig/pkg/inference/scripts/embeddinggemma2/testdata/gbr-formats`.
`golden.json` contains all 144 independent planar picture hashes from its
`formats.json`. Native WebM decoding checks every picture and the identity,
full-range, 4:4:4, BT.709-primary/sRGB-transfer metadata. See the model corpus
and `../gbr-model-validation/README.md` for generation and qualification scope.
