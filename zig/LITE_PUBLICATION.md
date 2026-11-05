# Shared-search publication v1

The owner freezes the selected Drive extraction results and the vectors already completed
for those revisions into a single input view. The runtime builds a fresh native Lite file
using that view. Each explicit build has an independent output path. A build has no
network or hosted update step. See [fixture inputs and table schema](testdata/publication-v1/).
The test vectors are orthogonal unit vectors for verifying storage and retrieval.

SearchAF's source table is `files`. Its `file` and `source_document` records, generated
`full_text_index_v<N>`, `document_chunks_v1` text and completed `document_chunkvec_v1`
values feeding `chunk_vectors` define the inputs. The producer resolves actual names from
its local catalog. Shared searches select text only. The physical publication has **one
embedded root** exposed logically as `files`, the `full_text_index_v0` keyword index
and the external, cosine, 768-dimensional `chunk_vectors` dense index. Application-local
image/graph indexes, extraction producers and control tables belong to the personal database.
The hosted reader opens each selected generation in query-readonly mode. Publication does
not translate a personal database or open its contents in the ordinary writable standalone
runtime. The gateway must dispatch published instances to this serving path.

## Producer

Canonical fixture `zero.json` shows every required top-level field. All strings are
UTF-8. Only the fixed fields in the fixture are accepted. Each discovered Drive file has
one outcome. `failures` and `exclusions` list its Drive document ID, provider revision
and a short machine reason. Successful documents have at least one nonempty chunk, a
display-only relative Drive path, filename, Google Drive URL, MIME type, original Drive
revision, extracted content and its lowercase SHA-256 as `document_revision`.
`corpus_revision` is an opaque owner-assigned ID for the frozen extraction run. An
embedding-only improvement can retain it.

Chunk spans are half-open offsets into the original UTF-8 bytes of the document.
Ordered spans may overlap but must cover the full text. Locators are tagged objects:
`{"kind":"text"}`, `{"kind":"page","page":1}`, `{"kind":"slide","slide":1}`,
`{"kind":"sheet","sheet":"Totals"}`, or
`{"kind":"rows","sheet":"Totals","row_start":1,"row_end":4}`.
Coordinates are one-based and row ranges inclusive. A chunk key is `c:` plus lowercase
SHA-256 of the five UTF-8 strings document ID, source revision, document revision,
decimal start and decimal end, separated by single NUL bytes, without a trailing NUL.
The parent row key is `d:` plus the Drive document ID. Document ID stays stable across
revisions; a changed source revision, extracted text or span yields a new chunk key.

Builders omit `embedding` for pending/missing vectors. Completed outputs carry their
originating chunk key, model, document task and 768 finite L2-normalized float32 values.
The exact document/query configuration is in `embedding.json`. Use the selected
model's retrieval task formatting, mean pooling and L2 normalization; reject overlong
requests. Never synthesize a vector or substitute a model. Register the vector once
in `_embeddings.chunk_vectors` on the passage row containing that exact text.

Native rows use the versioned `schema.json`. `d:*` stores the full extracted `content`
and provenance; `c:*` stores that byte slice in `content`, filename, revisions,
parent identity, offsets, locator and a checksum of the vector's little-endian float32
bytes when present. `publication` records the contract, coverage and outcomes. No
auth, storage credentials, application sessions or ownership records are imported.

## Process interface

Use Python 3.9+ and a compatible native Antfly binary. Executable
`scripts/antfly-publication` takes the following subcommands. Configure the worker-owned
absolute executable with `ANTFLY_BIN` or install `antfly` on PATH. JSON responses go to
stdout. A failure exits nonzero with a JSON error on stderr.

    build --input FILE.json --out COPY.aflite
    inspect --artifact COPY.aflite [--digest SHA256]
    prepare --root DIR --destination ID --operation ID --expected-generation N --digest SHA256 --artifact COPY.aflite
    validate --root DIR --destination ID --operation ID
    activate --root DIR --destination ID --operation ID
    status --root DIR --destination ID [--operation ID]
    cancel --root DIR --destination ID --operation ID
    cleanup --root DIR --destination ID
    search --root DIR --destination ID --generation N --input QUERY.json
    read --root DIR --destination ID --generation N --key ROW_KEY

Give each instance a private persistent root owned by the runtime. The worker maps
the authenticated instance to this root. Bind it once to the stable destination ID.
Operation IDs are durable within a destination; reusing one with a different digest
or expected generation is an error. Generation 0 means unpublished. Artifact identity
is the SHA-256 of the complete file, supplied outside it. `inspect` returns the
digest, native format version, schema version, mapping, embedding, corpus revision and
coverage. Coverage counts selected files = extracted + failed + excluded;
keyword_documents / extracted_documents and semantic_chunks / total_chunks.
Every published extracted passage is keyword indexed.

`prepare` reserves the single update slot and stages bytes in private storage.
`validate` checks file digest, native integrity, schema, catalog, every parent,
passage and vector, and frozen counts. `activate` binds the validated immutable file
to the destination in one durable transaction. Status states are `preparing`,
`prepared`, `validated`, `committed` and `cancelled`. Retry the same command and
identity after interruption. A failed precommit operation may be retried or cancelled.
Cancellation before commit frees the slot; after commit returns the committed receipt.
An acknowledgement lost after commit is recovered with status or repeat activation.
`generation` at the destination and `committed_generation` on the receipt are distinct.
Cleanup removes unreferenced bytes while retaining receipts.

Search input is `{"text":"solar","limit":20}` or
`{"vector":[...],"embedding":{...contents of embedding.json...},"limit":20}`.
Run keyword and vector arms separately to obtain candidates for local rank fusion and
reranking. Responses have `generation` and `hits` (Antfly `_id`, `_score`, `_source`).
Read returns `generation`, `_id`, `_source`. Readers request only the current
generation. In-flight reads finish on their pinned data; a later read or continuation
with a stale generation fails with `stale_generation`. The gateway must reauthorize
each call including continuations; this local interface opens no network listener.

## Compatibility and recovery

V1 requires a native reader/writer supporting indexed Lite format 7, external
embeddings, `lite root-catalog` and `lite scan-stream` from this branch. The worker's
contract version is independent of the engine-owned table schema version (0). Package
`antfly-publication` beside its matching native binary with
`python3 scripts/packaging/package_lite_publication.py OUTPUT`. It runs on POSIX
with Python 3.9+ and SQLite. The server archive includes the packed worker. Set
`ANTFLY_BIN` to the matching executable on the Mac or in the job container.

Use a new, empty PVC subdirectory per destination. An already running shared-search
worker can continue serving during staging, validation and activation. The runtime
atomically records the selected immutable artifact and the committed receipt in
`publication.sqlite`. The data filename is derived from a hash of the operation ID.
The service takes a shared filesystem lock while pinning and reading its chosen data
file. Cleanup takes the exclusive lock without waiting; retry `operation_busy` after
readers finish. Old receipts survive cleanup. All processes must share POSIX locks,
atomic renames and durable SQLite filesystem semantics. Never mount a published
copy as the data file of an ordinary writer. A configured runtime account is the only
writer to this directory. The boundary trusts the local builder's assertion of which
Drive folder and model produced the text/vector; validation checks revision bindings,
bytes, shape, physical postings counts and model compatibility. Grant checks remain
with Colony. Its gateway should map `stale_generation` / `stale_cursor` to HTTP 409.

The producer input limit is 64 MiB, each extracted document 4 MiB, each passage 64
KiB, the artifact 2 GiB and the row limit 100,000. These are engineering limits.
The validator scans live rows and keeps a digest and provenance per pending passage.
Search returns at most 100 candidates per arm. Set `page_size` below `limit` to
page that window and pass the returned `next_cursor` back as `cursor` with the same
query. A cursor binds destination, generation, query and ordered candidate IDs;
authorization is repeated by the caller. Reader-local ordering across sources belongs
to SearchAF.

Recovery uses the same root, destination and operation ID. On `preparing` retry
prepare with the original bytes and SHA-256. On `prepared` retry validation. On
`validated` retry activation. A committed receipt remains authoritative even if a
later operation has superseded it. A failed checksum, unsupported catalog, disk
error or process exit before commit leaves the previous generation selected.
