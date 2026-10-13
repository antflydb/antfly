# GLiNER2.5 base boundary qualification evidence (2026-09)

> Summary of a verbatim log. Full text: [docs/design/inference/history/gliner2/gliner25-base-qualification-2026-09.md@f3fd4d3](https://github.com/antflydb/antfly/blob/f3fd4d35258791fe16ab927a1e7f9b3af247b074/docs/design/inference/history/gliner2/gliner25-base-qualification-2026-09.md). Originally relocated verbatim from `zig/pkg/inference/models/gliner2/GLINER25.md` (at commits 3ce5aff4ea and a7c147e446) on 2026-10-12.
>
> Section numbers match the original record; code comments and other docs cite them as "GLINER25.md section N". Section 1 (identity digests) and the fp16 long-document follow-up of section 15 remain in [GLINER25.md](../../../../../zig/pkg/inference/models/gliner2/GLINER25.md).

Qualification of `fastino/gliner2.5-base-v1` (boundary architecture) for production extraction: single-window parity and geometry, then follow-ups that qualified long-document windowing for the real `examples/dogfood` schema, made it fast, fixed batching/admission incidents, and qualified an `fp16_encoder` single-window row. Standing rule throughout: a production row's `LengthContract` is widened only by new measurement, never by extrapolation or by inference from a passing test.

## Decisions at a glance

- Single-window and long-document rows are separate (disjoint `.single_window` / `.long_document` features); a request cannot borrow the other row's bounds. Row bounds are schema-shape-specific, not just document-size-specific.
- Long-document default window changed from 4096/128 to **1024/32** words; the qualified `window_words` upper bound stays 4096.
- GLiNER boundary executors advertise batch `mode = .none, max_items = 1` — multi-item requests were never qualified.
- Geometry-limit rejections return six named `GlinerBoundary*LimitExceeded` errors (HTTP 413) instead of generic `UnsupportedGlinerBoundaryRuntime`.
- `fp16_encoder` qualified for single-window only, with reviewed tolerance 7.5e-4; fp32 remains the default; selection is via a converted `-fp16` directory, not a `:fp16` pull variant.

### 2. Correctness

All ten canonical task fixtures (`testdata/gliner25/pipeline_cases_base.json`) match the pinned Python/Fastino reference byte-for-byte on native and through the full Metal pipeline. Both tests re-verify model/tokenizer/config digests first. 126 of 148 `gliner boundary` tests passed; the 22 skips were `small`/`multi` checkpoints not pulled (those variants stay unqualified).

### 3. Geometry (LengthContract bounds)

Measured single-window range (canonical fixtures, repro requests, and the real dogfood 11-entity/6-relation schema on a 107-word/610-byte paragraph):

```
document_bytes=[26,610] document_words=[5,112] window_words=[5,112] padded_sequence_tokens=[14,218]
```

with `request_items=[1,1]`, `window_count=[1,1]`. The schema vocabulary is encoded as a prefix, so the wider dogfood schema roughly doubles `padded_sequence_tokens` at the same document length (56 -> 118). Widening any bound requires new measurement.

### 4. Throughput

The 2026-09-09 [`BENCHMARK.md`](../../../../../zig/pkg/inference/scripts/gliner25/BENCHMARK.md) CPU comparison already covers this checkpoint: native/Python latency ratios 0.58-0.80; mixed-task median 36.174 ms native vs 45.159 ms Python, one CPU math thread. Not a serving/HTTP qualification.

### 5. End to end

`POST /ai/v1/extract` on Metal returned HTTP 200 in 1.2-2.5 s. On the identical input, compared with legacy `antflydb/gliner2-base-v1` (span, Q4_K): GLiNER2.5 labeled "Raft groups" `subsystem` (legacy: `component`, wrong) and got `tested_by` DataServer -> VOPR at 0.897 (legacy: backwards at 0.749, plus a spurious `depends_on` at 0.433); legacy latency 1.5 s. One qualitative sample, not a precision/recall benchmark.

### 6. Two more gates discovered and fixed during this qualification

Running the real end-to-end repro (not just unit tests) found blanket `gliner2.5` exclusions in `taskMatchesModelListing` (`server/server.zig`) and `modelSupportsCapability` (`models/capabilities.zig`), now gated on `!gliner_boundary.runtime_available`; and plain requests without `schema_version` were routed to the legacy span pipeline (`BoundaryExtractionRequiresSchema`) — fixed in section 8. These fixes only widen what a qualified artifact can do; exact-digest enforcement is unchanged.

### 7. Memory budget

The fp32 738 MiB checkpoint's admission estimate (worst case at `max_len=4096`) exceeded small defaults: 4096/4096/2048/8192/1024 MB budgets still gave `MEMORY_BUDGET_EXCEEDED`; `--host-budget-mb 16384 --backend-budget-mb 16384 --scratch-budget-mb 16384 --combined-budget-mb 32768 --kv-budget-mb 4096` succeeded. Capacity-planning note, not a qualification bound.

### 8. Follow-up: the in-process provider entry, and widening for the real dogfood schema

The section 6 upgrade lived only in the HTTP handler; the in-process provider calls `Node.extractDirectWithControl` directly. Fix: the `schema_version:2` upgrade moved into the shared `extractWithAdmission` (helper `Node.resolvesToBoundaryArchitecture`). In-process dogfood ingest: `BoundaryExtractionRequiresSchema` on 1,195 of 1,202 sections before, 0 after. Remaining failures were correct geometry refusals and `MemoryBudgetExceeded`; the embedded worker had no equivalent of the per-budget CLI flags (gap outside this module).

### 9. Follow-up: long-document windowing qualified for the real dogfood schema

- The existing long executor (PR #720) already windowed, rebased offsets, deduplicated entities, and merged within-window relations, with memory per window independent of document length. This section measured and reviewed it rather than building it.
- Corpus survey (2,355 Markdown-heading sections): p50 = 982 bytes, p90 = 4,378, p95 = 7,285, p99 = 23,428, max = 99,008 (`zig/PDF.md` "Review findings and required fixes").
- Long-document probe inputs are frozen snapshots (at `3ce5aff4ea`) under `zig/pkg/inference/testdata/gliner25/long_document_probe/` so editing design docs never changes them.
- Measured row at 4096/128: `document_bytes=[26,99008] document_words=[5,15894] window_count=[1,4] window_words=[5,4096] padded_sequence_tokens=[106,5708]`, features limited to dogfood's shape (entities + relations).
- Canonical schema_version 2 envelope verified on native and Metal through both HTTP and `Node.extractDirect`; relations never expose internal `head`/`tail`.
- Metal is process-required: harnesses loading through `Node`/`ModelManager` must set `process_termination_available = true`.
- Pitfall: the narrower 3-entity/2-relation schema falls below the row's 106-token floor and is correctly refused.
- Throughput at 4096 words: ~2.28 s/window, ~0.25 sections/s (12 sections, 21-40KB); concurrent submission did not beat sequential on one Metal device (9/12 returned 200).
- `examples/dogfood` now requests `"long_document":{"mode":"window"}`. In-process ingest still failed (1,198 `InferenceProviderFailure` + 5 `InferenceInvocationMemoryExceeded`) because the embedded worker's lite resource policy cannot satisfy admission; 5 `LongDocumentWorkLimitExceeded` were correct fail-closed outcomes against the 8 GiB `max_total_attention_work` default.

### 10. Follow-up: long-document throughput (window size, batching, precision)

Sweep on a 16-section subset (Metal, sequential):

| window_words | total wall | windows | s/window |
| ---: | ---: | ---: | ---: |
| 512 | 4.63 s | 20 (8 rejected by then-4-window bound) | 0.23 |
| 1024 | 7.42 s | 34 (2 rejected) | 0.22 |
| 2048 | 14.54 s | 26 | 0.56 |
| 4096 (prior default) | 26.68 s | 18 | 1.48 |

- 1024 chosen over 512 (512 needs roughly double the windows with no measured benefit). `window_count` bound widened 4 -> 17.
- Windows batched up to `Limits.window_batch_size` (default 4) per forward pass, except classification/JointIE schemas (not reviewed batched). Measured effect small (~0.26 s/window either way); kept because correct and bounded.
- Before/after on the subset: 26.68 s -> 12.82 s (~2.1x). Corpus-weighted estimate ~4.5 sections/s vs ~0.25 before and GLiNER2 base's ~7.9 reference (estimate, not a full timed ingest).
- Window mode adds no measurable overhead for a 610-byte document (0.044-0.050 s either path).
- First `fp16_encoder` probe: 774,366,564 -> 407,861,568 bytes, ~1e-4 sampled differences; informal, not a qualification.

### 11. Follow-up: the 9-request incident was batching, not document geometry, plus a widened bound and named length-limit errors

- Nine ingest failures (`request_bytes` 33,130 to 99,631) matched pairs of sections batched into one request: `resolvedExecutorBatchImplementation` advertised native batching (`max_items = 128`) for `.native_gliner_extraction`, violating `request_items = 1`. Fixed by advertising `mode = .none, max_items = 1`.
- Bound widened with synthetic concatenations of `zig/PDF.md`'s two largest sections (130KB and 182KB): `document_bytes=[26,182000] document_words=[5,28275] window_count=[1,29] window_words=[5,4096] padded_sequence_tokens=[106,5708]`. Real corpus max (docsaf-accurate) is 92,302 body bytes / 14,503 words / 15 windows.
- Six named limit errors map to HTTP 413 `EXTRACTION_LIMIT_EXCEEDED`.
- `OutOfMemory` during a window group retries the same position with a halved group (down to one window).
- `InferenceInvocationMemoryExceeded` (pre-flight, outside the executor) defaults to `.retryable_request` and wasted 5 retries; that fix and an unreproduced CPU-spin live in `zig/pkg/antfly/**`.
- Corpus-maximum 99,631-byte request served: HTTP 200, 279 entities, 13 relations, ~6-7 s.

### 12. Follow-up: fp16-encoder qualification attempt -- real-pipeline parity measured, does not yet clear the bar

- Conversion is deterministic: two runs produced byte-identical `model.gguf` (407,861,568 bytes, SHA-256 `1dce97cb1727e3b4e4c8242e88b46ad5f8f31801c2c9d24919393a8816a92d11`); sidecars unchanged.
- Real-pipeline parity: 61 of 62 confidence values within the fp32 `5e-4` bound; the miss (`entity_attributes`, idx 1) is 0.5588979 native / 0.5588977 Metal vs 0.5583068 expected on both backends. The fp32 tolerance became a named constant `fp32_confidence_tolerance = 5e-4` so a precision-specific tolerance can never loosen fp32 evidence.
- Decision at the time: not qualified (widening tolerance is a reviewed policy call, not inference from a test). Informal throughput: native slower with fp16, Metal faster in 2 of 3 reps — argues against fp16 as default on native. Do not hand-author a manifest to make an unqualified bundle runnable.

### 13. Follow-up: closing the in-process-vs-HTTP throughput gap -- measured equal, root cause is elsewhere

On an identical 40-section set: HTTP 14.841 s (2.695 sections/s) vs `Node.extractDirect` 14.698 s (2.721 sections/s). The gap to the real ingest's ~1.8 sections/s is not in `zig/pkg/inference`; likely causes are GPU contention with concurrent embedding on one Metal device and the out-of-process worker hop required on Metal. No code change.

### 14. Follow-up: the corpus-minimum document, the serial batch contract, and the plan-resolution floor

Post-merge ingest (2026-09-19) failures were all outside the model: the safetensors checkpoint fell through to `.compatibility` and advertised `serial_compatibility, max_items: 128` (now both concrete GLiNER executors share `.native_gliner_extraction`, `max_items = 1`); plan resolution budget (8x request bytes + 4 KB) was too small for tiny sections (now a fixed 1 MiB floor, planning only); and the 20-byte `TODO.mdSERVERLESS.md` section was below the 26-byte floor. Floors lowered by measurement to `document_bytes >= 1`, `document_words >= 1`, `window_words >= 2`, long-document `padded_sequence_tokens` floor 103 (single-window stays 14).

### 15. Follow-up: fp16-encoder single-window qualification -- root cause, reviewed tolerance, and selection wiring

- Root cause of the 5.9e-4 deviation: fp16 weight rounding, not a code defect. Only `.encoder_matrix` tensors narrow to F16; native and Metal both upcast per element and accumulate in FP32, and two independent kernels land on the same value.
- Full distribution (62 values): max abs diff 0.0005911 native / 0.0005909 Metal; mean 0.0000571 / 0.0000569; one value above 5e-4; every decision (entity, label, span) byte-identical to fp32. Reviewed tolerance: `fp16_encoder_confidence_tolerance = 7.5e-4`.
- Geometry re-run against the fp16 bundle reproduced both fp32 `LengthContract`s byte-for-byte, but only a single-window row was added: long-document needs its own fp16 correctness pass (done later; see below).
- Row `fastino_gliner25_base_v1_fp16_encoder` (native and Metal). `registry.zig`'s pull-time gate now reads the converted bundle receipt's recorded precision; `gliner25-convert` writes `model_manifest.json` inside the atomic publish, with non-empty tasks only for a reviewed precision (q8_0 still gets `"tasks":[]`).
- Single-window throughput (bundle-check, warm): native fp32 0.97-0.98 s vs fp16 1.43-1.50 s; Metal fp32 2.93-3.11 s vs fp16 2.51-2.53 s — the Metal speedup was later found to be an artifact of the harness path.

> The long-document follow-up to section 15 remains in [GLINER25.md](../../../../../zig/pkg/inference/models/gliner2/GLINER25.md#follow-up-fp16-encoder-long-document-qualification-and-corpus-shaped-throughput).
