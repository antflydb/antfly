# Bounded local R2 scale fixtures

The post-main-merge 50,000-document run completes ingestion/publication and
accepted visibility, then its host process exits with signal-derived code 137
before a completed report is written. A local OS sample records a peak physical
footprint of about 22.6 GB. That run is incomplete and its logs/sample remain
preserved. This does not establish a native WASM memory failure: the fixture
holds the entire emulated bucket, including historical artifacts, in a Node
`Map`. Its payload storage is external to the native request-memory counters.

`qualification/benchmark.mjs` now selects a file-backed bucket for corpora of
10,000 documents or larger; `--file-bucket` selects it for smaller comparisons.
One append-only local file owns payloads. The in-memory index contains current
keys, ETags, extents and HTTP metadata, without payload buffers. It preserves
conditional-write serialization, read ranges, immutable prior extents for
in-flight readers, missing-object behavior, pagination and traffic counters.
Successful writes publish index entries only after complete append I/O. There
is a 64 GiB payload-file limit and a one-million-key index limit; this remains a
bounded test fixture rather than an arbitrary-scale storage service.

The file and its private directory live beneath the worktree's ignored
`dist/qualification` directory and are removed in `finally` on normal completion
or handled failure. A process killed externally can leave its private fixture
directory; retain reports and remove only that abandoned directory after
confirming its owner exited. The fixture does not provide process-crash recovery:
durable reopen, fencing and storage-loss behavior remain actual-workerd/native
fault-test responsibilities. No filesystem backend is added to the hosted
database, and no production R2 or Worker resource budget changes.

The benchmark also captures vector-root shape from bytes already resident during
uploads. It no longer sweeps every historical artifact after ingestion merely
to produce that diagnostic. This avoids touching the complete emulated bucket
outside measured stages. Root inspection adds no simulated object requests.

Contract tests compare conditional writes, concurrent CAS, ranges, pagination
and exact traffic counters against the original memory fixture, and prove byte
ownership and immutable old extents. A frozen 1,000-document model corpus also
matches the memory fixture exactly across all stage GET/byte/write/call counts,
native peak-memory/I/O counters, routing shape and all thirty recall samples.
Its evidence is `file-fixture-equivalence.json` in the ignored gate directory.
It does not qualify deployed latency, CPU, total isolate memory or diagnostic
coverage. The live gate and original failed runs remain unchanged.
