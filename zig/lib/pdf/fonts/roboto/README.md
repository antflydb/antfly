# PDF fallback fonts

These unmodified Roboto 2 Regular and Bold fonts replace the PDF engine’s
previous dependency on server UI fonts. Their Apache-2.0 license and original
copyright notices accompany the files.

Source: https://github.com/googlefonts/roboto-2/tree/38062f4b4a0be4346d07a928408da21602545e9e/src/hinted

`scripts/embedded_asset_licenses.json` records the source URLs, sizes, and SHA-256
hashes. Changing font outlines can change fallback rendering; document widths
continue to come from the PDF. Explicitly embedded PDF fonts are unaffected.
