# DOCID Query Benchmark Diary

> Relocated verbatim from `zig/DOCID.md` (lines 1480–1747 at commit 271838a195, adjusted from the audit's original 1495–1747 boundary to the nearest clean sentence break so neither file ends mid-sentence) on 2026-09-16 during the documentation cleanup. This is a historical implementation log kept for context; the living design is [`DOCID.md`](../../../zig/DOCID.md), specifically its "Implementation Notes" section. Durable decisions from this log were folded into that document before the move. Note: the physical line break at the start of this excerpt was shifted by one word (from mid-code-span `zig build` / `antfly-storage-bench`) to land on that sentence boundary; no words were added, removed, or reordered.
>
> Relative links were re-based to this file's new location; no other text changed.

`zig build antfly-storage-bench` now provides a repeatable ReleaseFast benchmark for raw
  sorted `u32` ordinal arrays, direct roaring bitmaps, the current compact
  ordinal-list/bitmap document-set operators, sorted sparse `u64` IDs, and
  public DOCID-key baselines across small, medium, large, dense, and sparse
  layouts. `zig build antfly-storage-bench && ./zig-out/bin/storage_bench write --docs 512 --batch-size 128 --body-repeat 1` measures insert, update, and delete
  phases across write consistency levels and reports the isolated
  extraction, artifact-cleanup, identity-capacity, identity-metadata,
  derived-payload, and store-write timings from `BatchProfile` alongside
  resulting identity-table stats. `zig build antfly-storage-bench && ./zig-out/bin/storage_bench query --docs 4096 --queries 16 --repeats 8 --filter-size 256 --limit 32` now benchmarks
  direct DB query shapes that exercise the real filter bridges: match-all with a
  doc filter, full-text with a doc filter, and sparse-vector search with a doc
  filter. Each shape runs `public_ids` mode, where public document IDs are
  resolved on every query, and `ordinal_docset` mode, where the benchmark
  pre-resolves the request-local ShardDocSet-style ordinal filter at a stamped
  identity generation. Output includes checksums, hit counts, elapsed/average
  nanoseconds, per-shape `docid_query_bench_summary` rows, and doc-set planning
  counter deltas so correctness and projection work are visible together. The
  benchmark now fails correctness mismatches by default; `--allow-mismatch`
  keeps exploratory runs non-fatal, `--max-ordinal-ratio <ratio>` turns the
  ordinal/public timing comparison into an optional performance guard, and
  `--require-public-resolution-delta` asserts that the public-ID path still
  exercises request-time doc-set resolution while the pre-resolved ordinal path
  does not. It also emits a separate `sparse_id_projection` proxy for sorted
  sparse-ID intersection cost instead of pretending sparse native IDs are a
  public DB search mode. A local smoke sample (`docs=1024`, `queries=8`,
  `repeats=4`, `filter_size=128`) matched public-ID checksums for every real
  query shape, avoided 64 per-query doc-set resolutions per shape in ordinal
  mode, and showed ordinal-mode DB time roughly 8-12% lower on that small
  single-process run. A smaller guarded smoke
  (`docs=128`, `queries=3`, `repeats=2`, `filter_size=16`,
  `--max-ordinal-ratio 2.0`, `--require-public-resolution-delta`) passed with
  matching checksums/hit counts, public `resolved_set_delta=12`, ordinal
  `resolved_set_delta=0`, and ordinal/public ratios around 0.84-0.88 across the
  real DB shapes. These benchmarks are now the first pass/fail evidence hooks
  for validating whether the compact ordinal machinery is still earning its
  complexity as sparse-ID alternatives evolve. The storage comparisons now run within
  `python3 scripts/run_db_query_matrix.py --suite storage`. Use `--profile smoke`
  for the original small cases; the bounded profile preserves all three original
  workload sizes. The same matrix includes public query shapes (`--suite public`)
  and collects environment, command, status, raw output, and JSONL evidence under
  `bench/results/db-query-matrix/`. See [BENCHMARKS.md](../../../zig/BENCHMARKS.md).
  Lifecycle cutover, mixed-version, distributed snapshot, cache, compaction,
  and near-capacity boundary checks run in the owning suites:
  `zig build antfly-integration-test antfly-storage-db-test antfly-raft-test`.
  Additional focused coverage remains available through
  `zig build antfly-storage-db-query-test` and
  `zig build antfly-storage-test -- --test-filter "db lsm primary compaction preserves doc identity ordinals" --test-filter "db allocates final document ordinal with all index families present" --test-filter "db text compaction preserves ordinal filters across reopen" --test-filter "structured filter doc set cache separates shared namespace generation keys"`.
  Run `python3 scripts/run_db_query_matrix.py --profile smoke` from the repository
  root for storage and public-query smoke evidence. DOCID has no separate public
  test target, benchmark target, or matrix script.
  Run the owning suites and optional chaos campaigns directly from `zig/`:

  ```sh
  zig build antfly-integration-test antfly-storage-db-test antfly-raft-test \
    lib-metadata-vopr-transition-chaos-test lib-metadata-vopr-public-chaos-test \
    lib-lsm-backend-chaos-test
  ```

  A local operational pass at
  `bench/results/docid-operational-hardening/20260525T173023Z/` completed all
  four buckets: focused DOCID lifecycle, metadata split/merge transition chaos,
  public split/merge traffic chaos, and LSM backend compaction chaos.
  Performance evidence and auth checks can be invoked independently from the
  repository root:

  ```sh
  python3 scripts/run_db_query_matrix.py --suite public --public-docs 300000
  ANTFLY_BIN=/absolute/path/to/antfly \
    uv run --project zig/e2e/antfly pytest -q -x -s zig/e2e/antfly/test_auth.py \
    -k 'stateful_auth_enforces_table_permissions or stateful_auth_enforces_row_filters_on_lookup_and_scan'
  ```

  For old/new binary auth smoke checks, repeat the auth command with each
  binary's absolute path. The performance matrix retains its own timestamped
  evidence logs under `bench/results/db-query-matrix/`.
  The query-matrix cases cover the existing medium baseline, a selective
  small-filter shape, and a broad large-filter shape so future evidence is not
  limited to one favorable filter size. A local smoke matrix passed all three
  cases and produced 9 summary rows: the tiny and selective cases stayed below
  the `1.25` ordinal/public ratio guard, while the broad-filter case showed
  stronger benefit from skipping per-query public-ID resolution (`0.67`, `0.67`,
  and `0.81` ratios for match-all, full-text, and sparse search). A bounded
  default matrix run (`1024`/`2048` docs across the three cases) completed in
  roughly three minutes, matched correctness for all 9 shape/case summaries, and
  produced ordinal/public ratios of about `0.89-0.92` for the medium baseline,
  `0.96-0.98` for selective small filters, and `0.68-0.78` for broad filters.
  An attempted 8k release-scale selective run was intentionally left as an
  override-only profile because that single case ran past 10 minutes locally.
  A direct 100k-doc attempt exposed a degenerate setup path: with per-batch
  `full_index`, only 5k loaded after about 101s and the cost was degrading; with
  `--defer-full-index-load` but giant 10k write batches, the first 10k docs
  still took about 156s before the final full-index wait. The healthier setup is
  deferred indexing with smaller write batches: a 10k-doc run with 1k batches
  loaded in about 13s, then spent about 86s in the one-time `full_index` wait and
  about 17s preparing the resolved filter. The resulting single-query
  ordinal/public ratios were about `0.94`, `0.89`, and `0.96` for match-all,
  full-text, and sparse search. Follow-up profiling with
  `ANTFLY_BENCH_METRICS=1` showed where that time is going: the 10k deferred
  write/load stage spent about 13.6s of 14.0s in primary `store_write_ns`; the
  one-time index wait spent about 4.6s applying full-text and about 89.6s
  applying sparse-vector replay, with about 1.1s of replay-window collection per
  index. The benchmark therefore now has
  `--progress-every <docs>` and `--defer-full-index-load` to make large-load
  setup measurable, but 100k real full-text+sparse query evidence should use
  deferred indexing with bounded batches or a dedicated bulk-load path rather
  than repeated per-batch full-index barriers.
  Sparse-vector replay now has an internal bulk append path wired through
  backend batch options and resource-manager accounting. The path preserves the
  existing sparse on-disk layout but groups postings by term, writes complete
  chunks once, uses larger bulk replay batches, and lets replay/backfill callers
  skip per-doc existence probes when they have already applied deletes or are
  building a fresh index. A bounded 10k-doc DOCID query profile after the change
  still shows primary store load around 13-15s and full-text apply around
  4-4.5s; sparse apply improved from the original ~89.6s profile to roughly
  28-32s locally. That is a meaningful reduction, but not enough to call sparse
  loading solved. The remaining sparse load cost appears to be backend
  write/flush dominated rather than DOCID identity work, so future large-scale
  evidence should profile sparse backend batch commit/flush directly before
  using 100k full DB runs as a pass/fail signal.
  Follow-up sparse write profiling now emits `antfly_bench_sparse_write` rows.
  On the same 10k deferred-index run, sparse apply remained about `28-30s`.
  The profile attributed roughly `12-14s` to forward/reverse sparse row writes,
  about `5.2s` to commit, and only a few hundred milliseconds to grouped
  posting/chunk/meta writes. Sorted sparse artifact reads and a non-namespaced
  erased-batch `appendPut` hook did not materially improve this shape because
  the current LSM batch path still pays per-entry active-memtable mutation cost.
  A direct bulk-state append path now keeps append-only sparse rows in a
  transaction-local sorted-state buffer instead of mutating the active memtable
  per row, with arena-backed entry allocation and safe fallback copying when
  arena-owned entries must move into the normal mutable table. Sparse
  `fwd:`/`rev:`/`inv:` rows now use that append path during bulk replay. On the
  same 10k deferred-index profile, this removed most forward/reverse row-append
  time (`fwd_rev_put_ms` dropped to about `1.6s`) and fixed the earlier
  arena-fallback lifetime crash; however, sparse apply still measured about
  `26.4s` because commit-time WAL/table materialization grew to about `14.2s`.
  Sparse layout v2 now makes that breaking change while the feature is still
  undeployed: sparse keys use typed binary prefixes, bulk replay stores inverted
  postings in a compact segment blob, and bulk doc maps are stored in a compact
  doc-map segment instead of per-document `fwd:`/`rev:` rows. The old chunk-row
  shape remains only as the small incremental delta path, and search reads both
  segment blobs and delta chunks. On the same 10k deferred-index profile,
  sparse commit dropped from about `13-14s` to about `16ms`, sparse apply
  dropped to about `11.5s`, filter preparation dropped from about `13-14s` to
  about `68ms`, and the sparse query shape dropped from about `10.1s` to about
  `3.1s`. The remaining sparse apply time is no longer generic LSM commit; it is
  now dominated by replay artifact decode/grouping and doc-map/postings segment
  encoding. The next lever is to avoid materializing the whole sparse replay
  batch before segment encode, or to stream segment construction directly from
  replay artifacts under backend-runtime/resource-manager budgets.
  Sparse deferred replay now also has a prepared `SparseWrite` path for
  field-backed sparse indexes: replay reads borrowed document bytes with a
  sorted batched store read, extracts sparse vectors directly, and hands the
  prepared writes to the bulk sparse loader instead of first materializing
  `BatchWrite` document-value copies and then reparsing them inside
  `IndexManager`. On the same 10k deferred-index profile, sparse indexing
  itself dropped from roughly `1.4s` to about `67ms`, and total sparse apply
  dropped from roughly `2.7-2.8s` to about `2.1s`. The remaining measured cost
  is now mostly sparse extraction from JSON (`~1.34s`) plus replay document-key
  scan/key construction (`~0.67s`). That points to the next breaking-change
  lever if we need more: persist field-backed sparse vectors, or a compact
  sparse replay artifact, at write time so catch-up does not have to reread and
  reparse full JSON documents.
  Field-backed dense and sparse vectors remain ordinary stored document fields.
  Configured vector indexes still extract those fields during document replay and
  indexing, but they do not turn `field: []` payloads into embedding artifacts or
  strip them from persisted JSON. Explicit precomputed vectors use `_embeddings`,
  which is the artifact upload boundary alongside generated `_chunks` and
  `_edges`; Zig intentionally still rejects `_summaries`. This keeps benchmark
  field-backed vector cases measuring JSON document extraction plus index apply,
  while `_embeddings` cases measure artifact write/read/replay behavior. The
  earlier local experiment that promoted field-backed vectors into artifacts was
  reverted because it changed document round-tripping semantics.
  Sparse embedding artifacts still use a planar compact payload
  (`count | indices[] | values[]`) instead of interleaved `(index,value)` pairs.
  That keeps explicit/generated artifact payloads binary and lets replay borrow
  validated `[]const u32` and `[]const f32` slices directly from batched artifact
  reads when alignment and endian constraints allow it, falling back to allocated
  decode otherwise. Replay keeps the artifact read transaction open while the
  sparse bulk loader consumes borrowed slices. Artifact-key parser and borrowed
  decode optimizations apply to `_embeddings` and derived chunk embeddings; the
  field-backed sparse DOCID benchmark remains on the document-field replay path.
  Follow-up write-path work added `--bulk-load` to the DOCID query benchmark,
  routed benchmark loads through an internal primary-store bulk session, added
  append-oriented docstore bulk batches, batched text-projection ordinal lookup,
  and added a schema-less raw-text projection fast path for documents that do
  not require JSON string unescaping or stored-vector sanitization. Those
  changes did not materially improve the pathological local DOCID setup: a
  10k deferred/full-text+sparse run with `--bulk-load` still spent about
  `86.9s` loading, including about `69.4s` in primary `store_write_ns`, while
  sparse replay stayed around `140ms`. Public standalone guardrail comparisons show
  that this is not representative of the normal public write path: dense 100k
  standalone loading took about `32.4s` to insert and `67.3s` through index
  visibility, and schema-less hybrid 10k standalone loading took about `2.1s` to
  insert and `8.4s` through index visibility. The next DOCID profiling step is
  therefore primary-store/direct-ingest instrumentation for the local benchmark
  path: record whether direct bulk append is used or why it falls back, split
  WAL/sort/table-ingest/mutable-put timings, and compare record counts per
  document against the public guardrail path before treating 100k local DOCID
  profiles as product evidence.
  That instrumentation exposed the local-path bug: append-only primary records
  could direct-ingest, but small non-append metadata records stayed in the
  mutable table and blocked later append batches, forcing the next batch down
  the mutable flush path. The LSM bulk path now drains pending mutable bulk
  records into sorted ingest before declaring append direct-ingest ineligible.
  On a 1k deferred bulk profile this removed append fallback completely and cut
  `store_write_ns` from roughly `488ms` to `48ms`; on the 10k profile, load
  dropped from about `86.9s` to about `22.0s`. The new primary-LSM summary shows
  no flushes, two successful append direct-ingests, about `70k` direct-ingested
  primary entries, and about `7.7s` in sorted ingest/table construction. The
  remaining local DOCID setup cost is now split across extraction (`~4.6s`),
  identity metadata (`~3.2s`), derived artifact construction (`~5.0s`), and
  primary sorted ingest (`~7.7s`), with deferred index wait around `6.3s`
  (`~2.6s` full-text apply, `~78ms` sparse apply, and replay-window collection).
  Follow-up local bulk-load work made the in-memory LSM direct-ingest path take
  ownership of sorted arena-backed states instead of rebuilding table data, kept
  write-only deferred loads on thin replay records even without index workers,
  batched full-text ordinal lookup, and let full-text replay index borrowed store
  values while the read transaction is open instead of materializing a second
  owned write batch. On the same 10k deferred/full-text+sparse `--bulk-load`
  profile, total load dropped to about `10.8s`, `store_write_ns` stayed around
  `0.8s`, primary sorted ingest dropped to about `4ms`, and derived replay
  construction dropped to about `1.0s`. Deferred index wait is now about `6.1s`;
  sparse replay remains about `80ms`, and full-text apply is about `2.1s`, with
  `ANTFLY_BENCH_METRICS` showing the text indexer itself spends about `41ms`
  building the segment and about `705ms` inserting it. The remaining full-text
  wait is mostly replay-window collection plus document collection/read
  overhead, not sparse replay or segment construction.
  Follow-up write-path cleanup now removes more per-document overhead from the
  same measured phases: primary document keys and identity doc-to-ordinal keys
  are allocated at exact size instead of going through temporary array-list
  builders, all-new identity batches pre-reserve their KV write capacity, and
  full-text replay ordinal lookups use the replay arena for transient lookup
  keys instead of per-document long-lived allocator/free cycles. These are
  mechanical hot-path reductions; the last local end-to-end timing run was
  discarded because unrelated desktop load made multiple already-optimized
  phases regress together.
  The first optimization from that evidence specialized
  `ResolvedDocSet` ordinal set algebra: list/list operators now use direct
  sorted-array merge/intersection/difference, bitmap/bitmap operators use
  roaring `orWith`/`andWith`/`andNotWith`, and list/bitmap operators avoid
  flattening a bitmap unless the result representation requires it. This keeps
  the public empty/small/large representation contract while removing the
  previous flatten/sort/rebuild cost from the hot set-algebra path. Path-fact geo
  predicates and unpromoted schemaless path lookup filters now have the same
  guarded ordinal projection, so vector/sparse filter bridging can consume
  complete path and geo matches as document ordinals without first
  materializing public DOCIDs. The write benchmark also exposed that
  document-only deletes were dominated by per-document enrichment-artifact
  prefix scans rather than identity metadata; the storage path now persists an
  artifact-presence marker and keeps a conservative in-memory flag so fresh
  document-only stores skip those scans, while generated-enrichment targets and
  upgraded stores without the marker continue to take the safe cleanup path.
  Public query guardrail profiling then exposed a schema-less exact-filter
  gap: term/terms filters on ordinary string fields could not safely use the
  analyzed text postings, so hybrid vector queries fell back to widening
  through stored documents. Schema-less text extraction now also emits a
  bounded keyword companion using the Elasticsearch-style `.keyword` subfield,
  and structured term filters rewrite to that companion only when the target
  text snapshot actually contains the required postings. Explicit
  `search_as_you_type` schema derivation now follows Elasticsearch-style
  subfield names as well: `field._2gram`, `field._3gram`, and
  `field._index_prefix`, with `._index_prefix` carrying the edge-ngram prefix
  analyzer. Focused storage tests cover schema-less keyword projection,
  schema serialization, explicit/dynamic search-as-you-type variants, and the
  vector/filter ordinal bridge; 1k public query guardrail runs with and without
  schema both kept dense raw hits constrained at `20` rather than widening to
  the full document set.
