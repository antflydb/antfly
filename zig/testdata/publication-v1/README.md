# Runtime fixtures

The fixture models a selected Drive PDF and CSV plus an extraction failure and a scan
excluded from text search. Only synthetic document text is used. The CSV has a typed
sheet/row locator and the PDF page contains a multibyte character. `zero.json`,
`partial.json` and `full.json` freeze the same two keyword-searchable passages with
zero, one and two completed vectors. Each input builds independently.

```sh
export ANTFLY_BIN=/absolute/path/to/antfly
scripts/antfly-publication build --input zig/testdata/publication-v1/partial.json --out partial.aflite
scripts/antfly-publication inspect --artifact partial.aflite
```

The completed vectors were computed on macOS arm64 with `antfly inference embed`
on the native CPU backend at 768 dimensions. The document task was
`RETRIEVAL_DOCUMENT`. The query in `solar-query.json` uses `RETRIEVAL_QUERY` for
“What money is planned for solar energy?”. Pass raw text for these retrieval tasks.
The model applies its document title/text formatting and query search-result prefix.
The model manifest resolved to Hugging Face revision
`914f7f89142e33e77833254d9c9b90c3cef7303b`. The independently checked
`model.safetensors` SHA-256 was
`197a32965d4b1105faf060417baa899e193fb73cd401f42ec9295234d5553d79`.
The local inference executable reported model identity
`7c034a322f810a719cc3183d82c71e902f9a5588722cdf585dc6065849d43b78`.
The publication tests read the saved vectors and run offline.

Run `zig build publication-test` under `zig/` with the pinned toolchain, or run
`python3 -m unittest discover -s scripts -p test_publication.py -v` at the repository
root with `ANTFLY_BIN` set to that native binary. The desktop producer and the
authenticated gateway have their own integration suites.


The `*.aflite.gz` files are frozen native format-7 snapshots. Decompress before
inspection or transfer. `artifacts.json` records each uncompressed file's digest,
length and coverage. A fresh build can have a different physical checksum.
The golden-file test reopens the checked-in bytes and exercises real vector search.
