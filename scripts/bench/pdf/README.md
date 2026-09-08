# Remote-PDF ingestion comparison

Internal engineering benchmark using the OHR-Bench corpus and adapter from
`antflydb/antfly-circus`. The harness serves unchanged PDF bytes over loopback
HTTP; **Antfly** fetches, parses, OCRs, chunks, embeds, and indexes them. `pypdf`
only counts pages before timing. No mock inference or pre-extracted text is used.

OHR-Bench has conflicting license metadata. Follow Circus's stricter
research/non-commercial restriction; do not publish these as marketing results.

## Setup

Requires Python 3.11+, `pypdf>=5,<7`, `cryptography`, a read-only Circus checkout
containing `benchmarks/OHR-Bench-harness`, and two matched native Antfly builds.
Use the same Zig version, optimization, CPU target, Metal/BLAS options, models,
and host. Preserve each executable with its source revision, build command,
and SHA-256. This is a source-build comparison, not a release-package benchmark.

Keep downloaded assets and results outside git. Example (replace these paths):

```sh
uv venv /tmp/pdf-ab/venv
uv pip install --python /tmp/pdf-ab/venv/bin/python 'pypdf>=5,<7' cryptography
curl -fL --retry 3 https://huggingface.co/datasets/opendatalab/OHR-Bench/resolve/main/pdfs.zip -o /tmp/pdf-ab/pdfs.zip
antfly inference pull BAAI/bge-small-en-v1.5:safetensors --models-dir /tmp/pdf-ab/models
antfly inference pull antflydb/Florence-2-base:safetensors --models-dir /tmp/pdf-ab/models
/tmp/pdf-ab/venv/bin/python scripts/bench/pdf/benchmark.py prepare \
  --work-dir /tmp/pdf-ab --circus-dir /path/to/antfly-circus
```

`prepare` verifies the pinned archive SHA-256, extracts only the 11 curated PDFs,
and writes their hashes and page counts to `corpus.json`. Models are hashed for
every run; do not change the model directory between subjects. `--models-dir`
can override the default `WORK_DIR/models`.

## Run

```sh
/tmp/pdf-ab/venv/bin/python scripts/bench/pdf/benchmark.py run \
  --work-dir /tmp/pdf-ab --circus-dir /path/to/antfly-circus \
  --binary /path/to/frozen/antfly --revision FULL_GIT_SHA \
  --name rc5-auto-batch --suite small --mode auto --batch --trials 3
```

Repeat with the branch executable and a distinct `--name`. Each invocation
starts its own standalone server/database; each trial uses a fresh table.
An existing run directory is rejected. Only this harness's child process is
terminated. The fixed default ports are 29680/29681; use `--port` to change them.

- `--suite scan`: the single required-OCR scan, for quick model qualification.
- `--suite text`: 3 text-bearing/mixed PDFs, 9 pages. Includes the seven-page
  document, newspaper, and textbook page. Auto mode can still invoke OCR.
- `--suite small`: 4 PDFs, 10 pages, including a required-OCR scan and a
  seven-page document. Start here to qualify the pipeline.
- `--suite qualification`: all 11 curated PDFs, 252 pages, including encrypted,
  large, long, and URL-encoded fixtures. Not a full OHR retrieval evaluation.
- `--mode auto`: normal embedded-text extraction with OCR fallback.
- `--mode always`: OCR every page to exercise rendering and inference batching.
- `--ocr-model` and `--embed-model`: explicit model identities; defaults pin the
  Florence and BGE safetensors variants. Embedding dimension remains BGE's 384.
- `--batch`: submit all source rows together; omit for one request per PDF.
- `--read-profile`: enable per-stage reader diagnostics and record the override
  in provenance. Use for failure diagnosis, not timing comparisons.
- `--trials`: fresh tables within one server process. Model/runtime caches can
  be warm after the first trial. The first trial is **not** a cold-filesystem run.

Both subjects receive identical configuration, including a 16,000 MiB process
budget. Ambient `ANTFLY_*` variables are removed and their names recorded;
`--read-profile` explicitly reinstates only `ANTFLY_INFERENCE_READ_PROFILE=1`.
The only remote-content exception is the local byte origin. GPU acceleration
must be checked in runtime logs; compiling Metal support alone is not proof.

## Measurement and gates

`results.json` records insert-to-completion wall time, write acknowledgement,
startup, and table-setup time separately. Completion requires converged artifacts,
exact page coverage, nonempty chunks, zero OCR failures, selected OCR for the
scan, ready indexes, complete/healthy source coverage for every submitted
document, and published vectors. The byte-origin log must prove every PDF fetch.
The write API uses `sync_level=full_index`; acknowledgement alone is insufficient.

Raw manifests, index telemetry, origin access logs, server logs, exact table
configuration, model/binary hashes, host load, and Circus revision are retained.
A failed run writes `failure.json` and is **not a throughput result**. No retrieval
accuracy or OCR text-accuracy score is implied by the structural gate.

For performance conclusions, use a quiet host, alternate subject order, run
multiple fresh-process lifecycles, and compare the same page/chunk/vector/error
counts before computing speedups. Report first-process and warm trials separately.
Do not conflate model loading, filesystem caching, polling granularity (250 ms),
or background builds with batching improvements. Host load is recorded, not
controlled. This harness does not claim peak-memory measurements.

```sh
python3 -m unittest discover -s scripts/bench/pdf -p 'test_*.py' -v
```
