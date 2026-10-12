# GLiNER2.5 Boundary Runtime Qualification

This document is the reviewed release record for the GLiNER2.5 boundary
architecture (`zig/pkg/inference/src/models/gliner_boundary.zig`,
`gliner_boundary_qualification.zig`). PR #720 landed the boundary decoder,
pipeline, and closed qualification policy with the production table
deliberately empty and `runtime_available = false`, so `POST /ai/v1/extract`
and `antfly inference pull` withheld the `extract` task for every boundary
checkpoint until a specific artifact was reviewed. This document records the
first such review.

See [GLINER25_FAMILY.md](GLINER25_FAMILY.md) for the multilingual boundary
artifacts and the ModernBERT Decide-1B architecture, tokenizer contracts,
backend limits, and reproducible family checks.

## Current status

| Artifact | Backbone | Precision | Backend | Status |
| --- | --- | --- | --- | --- |
| `fastino/gliner2.5-base-v1` (rev `72ac19b486cd4557424c8d61114e7530c243e9b0`) | base | fp32 (safetensors) | native | Qualified (single-window) |
| `fastino/gliner2.5-base-v1` (rev `72ac19b486cd4557424c8d61114e7530c243e9b0`) | base | fp32 (safetensors) | metal | Qualified (single-window) |
| `fastino/gliner2.5-base-v1` (rev `72ac19b486cd4557424c8d61114e7530c243e9b0`) | base | fp32 (safetensors) | native | Qualified (long-document windowing, up to 182,000 document bytes -- sections 9, 11) |
| `fastino/gliner2.5-base-v1` (rev `72ac19b486cd4557424c8d61114e7530c243e9b0`) | base | fp32 (safetensors) | metal | Qualified (long-document windowing, up to 182,000 document bytes -- sections 9, 11) |
| `fastino/gliner2.5-small-v1` | small | any | any | Not reviewed |
| `fastino/gliner2.5-multi-v1` (rev `2ca71aafb3446d9014e1c55c7ff51c9bc7209c47`) | multi | fp32 | native, metal | Qualified measured single-window family profiles; see [family record](GLINER25_FAMILY.md) |
| `fastino/GLiNER2.5-multi-Decide` (rev `a35a0cd3b7a0f00f2effc576f454cd48fa98aa5f`) | multi | fp32 | native, metal | Qualified measured single-window extraction and public Decide profiles; see [family record](GLINER25_FAMILY.md) |
| `fastino/gliner2.5-base-v1` (converted via `gliner25-convert`, same rev) | base | fp16_encoder | native, metal | Qualified (single-window -- section 15) and Qualified (long-document, up to the same 182,000-byte bound as fp32 -- section 15 follow-up); root-caused as fp16 weight-rounding noise (no code defect), plus a real (also root-caused, not a defect) cross-window merge-tie-break effect for long documents. Reviewed tolerances: `7.5e-4` (single-window), `2.5e-3` (long-document). **fp32 remains the default**: on Metal, fp16 is measured ~2.9x SLOWER than fp32 for the corpus-shaped long-document workload (0.94 vs 2.72 sections/s, section 15 follow-up) (root cause: fp16 is excluded from the Metal resident-weight `optimized_v2` fast path by design) |
| Any other digest, revision, or precision of `gliner2.5-base-v1` | base | any | any | Not reviewed |

`gliner_boundary.runtime_available` is now `true`, and
`gliner_boundary_qualification.zig`'s production table carries the eight
base rows (fp32 and fp16_encoder, each single-window and long-document,
each reviewed for native and Metal -- section 15), plus separate exact-byte
multilingual profiles recorded in [GLINER25_FAMILY.md](GLINER25_FAMILY.md).
Everything else --
including a re-downloaded `gliner2.5-base-v1` whose upstream revision
changes, or a quantized/GGUF conversion of it -- still fails closed with
`error.UnsupportedGlinerBoundaryRuntime` at request time, and pull-time
manifest synthesis still withholds the `extract` task for it (see
"Two-tier gate" below).

## Two-tier gate

Advertisement and execution are enforced independently, on purpose:

1. **Pull-time advertisement** (`zig/pkg/inference/src/registry/registry.zig`,
   `boundaryIdentityIsQualified`): after a `boundary`-architecture pull
   finishes staging, this hashes the actual downloaded `model.safetensors`
   once and checks it, together with the four sidecar digests and backbone,
   against the production table (`gliner_boundary_qualification.hasQualifiedIdentity`).
   Only then does manifest synthesis emit `"tasks":["extract"]` and derive
   capabilities (`extraction`, `classification` always; `relations` and
   `records` from the artifact's own `config.json` `boundary_head.enable_relations`
   / `enable_records`). This never runs on the per-request listing path --
   `ModelManifest.hasSupportedGlinerRuntime()` stays permanently `false` for
   boundary architecture there, by design (see its doc comment).
2. **Request-time execution** (`zig/pkg/inference/src/extractors/gliner_boundary_qualification.zig`'s
   `Gate`, backed by `gliner_boundary_qualification.require()`): every
   extraction request against a boundary model re-derives the exact
   identity of the live session's consumed weight/sidecar bytes, the actual
   backend (native or Metal), the full feature union of the request, and the
   real prepared geometry (words, windows, encoded sequence length) -- and
   requires all four to match one production row before any learned work
   runs. A mismatched digest, an unreviewed backend, a feature outside the
   row (e.g. long-document windowing), or a request outside the measured
   length bounds is refused, regardless of what pull-time advertised.

Nothing in this design lets `runtime_available` or an empty-vs-nonempty
production table "leak" execution permission to an unreviewed artifact:
adding a row only ever narrows down to those exact bytes.

## Reviewed contract

This section is the current contract the production rows in
`models/gliner_boundary_qualification.zig` encode. The dated measurements
behind it are in the [qualification evidence history](../../../../../docs/design/inference/history/gliner2/gliner25-base-qualification-2026-09.md).

### Production rows

| Row | Features | `LengthContract` |
| --- | --- | --- |
| `fastino_gliner25_base_v1_lengths` (single-window; fp32 and `fp16_encoder`) | `fastino_gliner25_base_v1_features` (`.single_window`) | `request_items=[1,1] document_bytes=[1,610] document_words=[1,112] window_count=[1,1] window_words=[2,112] padded_sequence_tokens=[14,218]` |
| `fastino_gliner25_base_v1_long_document_lengths` (fp32 and `fp16_encoder`) | `entities, relations, word_whitespace, overlap_flat, offset_utf8, decoder_auto, long_document, record_identity_occurrence, confidence, spans` | `request_items=[1,1] document_bytes=[1,182000] document_words=[1,28275] window_count=[1,29] window_words=[2,4096] padded_sequence_tokens=[103,5708]` |

Each row exists once per reviewed backend (native and Metal). Rules that
follow from how these bounds were measured:

- Bounds are the exact observed range of a measurement, never an
  extrapolation. Widening one requires new measurement through the geometry
  tests in `extractors/gliner_boundary_qualification.zig`, which tokenize and
  plan real documents (no model weights) against the dogfood schema.
- Bounds are schema-shape-specific. The schema's entity/relation vocabulary
  is encoded ahead of the document, so a narrower schema can produce a
  `padded_sequence_tokens` value below a row's floor and be refused even for a
  "shorter" request. Rows were measured with `examples/dogfood`'s
  11-entity/6-relation schema.
- The single-window and long-document rows require disjoint features
  (`.single_window` vs `.long_document`), so neither can borrow the other's
  bounds.
- Geometry depends only on the tokenizer and schema, so `fp16_encoder` rows
  reuse the fp32 `LengthContract`s unchanged (re-measured byte-for-byte
  against the converted bundle).
- The corpus floor (`document_bytes >= 1`) covers `examples/dogfood`'s
  smallest real section (`zig/SCHEMA.md`'s 20-byte "Related Docs" after link
  stripping) and a one-character document.

### Long-document execution

`extractors/gliner_boundary_long_executor.zig` plans overlapping windows,
rebases offsets to whole-document coordinates, deduplicates mentions across
windows, and resolves relations only when both endpoints share a window.
Model tensors are freed after each window, so per-window memory is
independent of document length.

- `options.long_document.window_words`/`overlap_words` default to
  **1024/32** (`extraction_v2.zig`); a request may opt into up to 4096 words
  per window. The default was chosen because attention cost is quadratic in
  window length: 1024-word windows measured ~2.1x faster end to end than the
  former 4096/128 default on real corpus sections.
- `Limits.window_batch_size` (default **4**) groups a document's windows into
  one forward pass, except for schemas with classification or JointIE, which
  run one window per call. An `error.OutOfMemory` on a group retries the same
  starting window with a halved group (down to one window) instead of failing
  the document.
- `Limits.max_total_attention_work` (default 8 GiB of attention-score
  elements) is a separate cumulative safety cap; a document inside the
  reviewed geometry can still fail closed with
  `error.LongDocumentWorkLimitExceeded`.
- `examples/dogfood` requests `"long_document":{"mode":"window"}` for every
  section; the wire default mode is `.reject`.

### Admission, batching, and errors

- Every row requires `request_items = 1`. The server advertises
  `mode = .none, max_items = 1, preferred_items = 1` for the
  `.native_gliner_extraction` executor kind (both the split GGUF bundle and
  the safetensors boundary checkpoint resolve to it), so the antfly
  asset-producer batcher never groups GLiNER extraction requests.
- When identity, backend, and features match a row but geometry does not,
  `require()` returns a named error
  (`error.GlinerBoundary{RequestItems,DocumentBytes,DocumentWords,WindowCount,WindowWords,PaddedSequence}LimitExceeded`),
  which `extraction_v2.zig` maps to HTTP 413 `EXTRACTION_LIMIT_EXCEEDED`.
  `error.UnsupportedGlinerBoundaryRuntime` (HTTP 400
  `UNSUPPORTED_EXTRACTION_FEATURE`) is reserved for an unreviewed identity,
  backend, or feature. At the provider ABI both collapse to
  `error.InferenceProviderFailure`, which the enrichment runtime treats as
  terminal.
- A plain request without `schema_version` against a boundary model is
  upgraded to `schema_version: 2` inside `extractWithAdmission`, the entry
  shared by the HTTP handler and the in-process provider
  (`Node.extractDirect`/`extractDirectWithControl`).
- `taskMatchesModelListing` and `modelSupportsCapability` gate GLiNER2.5 on
  `gliner_boundary.runtime_available`, not on the model-family name.
- The asset producer's plan-resolution budget has a 1 MiB floor
  (`invocation_resolution_floor_bytes` in `asset_producer.zig`), so tiny
  sections are not refused during planning.
- Metal requires process isolation: a harness that loads a Metal session
  through `Node`/`ModelManager` must set `process_termination_available = true`.

### Precision

- **fp32 is the default.** `fastino/gliner2.5-base-v1` pulls only the fp32
  safetensors checkpoint.
- `fp16_encoder` is opt-in: `antfly-inference-gliner25-convert --precision
  fp16_encoder` writes a bundle (weight digest
  `1dce97cb1727e3b4e4c8242e88b46ad5f8f31801c2c9d24919393a8816a92d11`,
  407,861,568 bytes) with its own `model_manifest.json`, selectable as
  `fastino/gliner2.5-base-v1-fp16`. Conversion synthesizes the manifest
  through `registry.synthesizePulledModelManifestJson`, so only a precision
  matching a reviewed row gets non-empty `tasks`; `boundaryIdentityIsQualified`
  checks a converted bundle against its receipt's recorded precision.
- Confidence tolerances (`pipelines/gliner_boundary_pipeline.zig`):
  `fp32_confidence_tolerance = 5e-4`, `fp16_encoder_confidence_tolerance =
  7.5e-4` (single-window), `fp16_encoder_long_document_confidence_tolerance =
  2.5e-3`. Decisions (label, text, span) must match exactly; only the
  confidence may differ. The fp16 residual is weight-rounding noise (both
  backends accumulate in FP32), and the wider long-document bound comes from
  the cross-window duplicate-mention tie-break picking a different window's
  estimate.
- fp16 is not faster: on Metal the managed session's resident-weight
  `optimized_v2` path is fp32-only, so fp16 measured ~2.9x slower on the
  corpus-shaped workload; on native it is a wash.

### Memory budget

The fp32 checkpoint's admission estimate assumes `max_len=4096` worst-case
single-window capacity regardless of request size. `antfly inference run`
serves it with `--host-budget-mb 16384 --backend-budget-mb 16384
--scratch-budget-mb 16384 --combined-budget-mb 32768 --kv-budget-mb 4096`
(the smaller `4096/4096/2048/8192/1024` set is rejected with
`MEMORY_BUDGET_EXCEEDED`). This is capacity planning, not part of the
`LengthContract`. The embedded Lite worker uses the same five values as its
per-lane defaults (`EmbeddedInferenceNodeOptions`, clamped to the detected
host envelope in `zig/pkg/antfly-embedded/src/storage/inference_provider.zig`),
since it has no operator to pass these flags.

### Throughput reference

Single-process, one request in flight, real dogfood schema with windowing,
on the 40-section corpus sample (32 sections of 1–8 KB, 8 of 20–40 KB):
fp32 Metal ~2.7 sections/s, fp32 native ~0.49 sections/s. HTTP and
in-process (`Node.extractDirect`) measure the same within noise; the lower
rate seen during a full `examples/dogfood` ingest was attributed to
contention with concurrent embedding on the one Metal device and to the
embedded worker's process hop, not to the extraction path. CPU direct-core latency versus pinned Fastino
CPU is recorded in the [CPU benchmark runs](../../../../../docs/design/inference/history/gliner2/gliner25-cpu-benchmark-runs.md).

### Open work

- An ingest once stalled at 100% CPU after a burst of
  `InferenceInvocationMemoryExceeded` retries. That error is now terminal in
  `enrichmentErrorDisposition`; the stall itself was never reproduced outside
  the embedded worker.
- `small` backbone remains unreviewed; any other digest, revision, or
  quantized conversion needs its own row.

## Evidence

All reproduction commands below run from the repository root, with
`ANTFLY_GLINER25_BASE_MODEL_DIR` set to the pulled artifact directory
(`~/.antfly/inference/models/fastino/gliner2.5-base-v1` by default).

> **Relocated:** the dated qualification and follow-up narrative (sections 2–8 and 12–14 in full, and the non-probe parts of sections 9–11 and 15) → [GLiNER2.5 base qualification evidence](../../../../../docs/design/inference/history/gliner2/gliner25-base-qualification-2026-09.md). Section numbers are unchanged; code comments cite them as "GLINER25.md section N".

- [2. Correctness](../../../../../docs/design/inference/history/gliner2/gliner25-base-qualification-2026-09.md#2-correctness): pinned ten-fixture Python parity, native and Metal.
- [3. Geometry (LengthContract bounds)](../../../../../docs/design/inference/history/gliner2/gliner25-base-qualification-2026-09.md#3-geometry-lengthcontract-bounds): first single-window geometry measurement.
- [4. Throughput](../../../../../docs/design/inference/history/gliner2/gliner25-base-qualification-2026-09.md#4-throughput): CPU comparison against pinned Fastino.
- [5. End to end](../../../../../docs/design/inference/history/gliner2/gliner25-base-qualification-2026-09.md#5-end-to-end): first `/ai/v1/extract` repro and legacy GLiNER2 comparison.
- [6. Two more gates discovered and fixed during this qualification](../../../../../docs/design/inference/history/gliner2/gliner25-base-qualification-2026-09.md#6-two-more-gates-discovered-and-fixed-during-this-qualification): listing, capability, and plain-request routing fixes.
- [7. Memory budget](../../../../../docs/design/inference/history/gliner2/gliner25-base-qualification-2026-09.md#7-memory-budget): budget flags that admit the fp32 checkpoint.
- [8. Follow-up: the in-process provider entry, and widening for the real dogfood schema](../../../../../docs/design/inference/history/gliner2/gliner25-base-qualification-2026-09.md#8-follow-up-the-in-process-provider-entry-and-widening-for-the-real-dogfood-schema): `extractWithAdmission` upgrade and dogfood-schema widening.
- [9. Follow-up: long-document windowing qualified for the real dogfood schema](../../../../../docs/design/inference/history/gliner2/gliner25-base-qualification-2026-09.md#9-follow-up-long-document-windowing-qualified-for-the-real-dogfood-schema): throughput, end-to-end, and in-process ingest results (design and geometry remain below).
- [10. Follow-up: long-document throughput (window size, batching, precision)](../../../../../docs/design/inference/history/gliner2/gliner25-base-qualification-2026-09.md#10-follow-up-long-document-throughput-window-size-batching-precision): window-size sweep, batching, and the first fp16 probe.
- [11. Follow-up: the 9-request incident was batching, not document geometry, plus a widened bound and named length-limit errors](../../../../../docs/design/inference/history/gliner2/gliner25-base-qualification-2026-09.md#11-follow-up-the-9-request-incident-was-batching-not-document-geometry-plus-a-widened-bound-and-named-length-limit-errors): batching root cause, named errors, OOM retry, retry-storm notes.
- [12. Follow-up: fp16-encoder qualification attempt -- real-pipeline parity measured, does not yet clear the bar](../../../../../docs/design/inference/history/gliner2/gliner25-base-qualification-2026-09.md#12-follow-up-fp16-encoder-qualification-attempt----real-pipeline-parity-measured-does-not-yet-clear-the-bar): first fp16 parity attempt.
- [13. Follow-up: closing the in-process-vs-HTTP throughput gap -- measured equal, root cause is elsewhere](../../../../../docs/design/inference/history/gliner2/gliner25-base-qualification-2026-09.md#13-follow-up-closing-the-in-process-vs-http-throughput-gap----measured-equal-root-cause-is-elsewhere): HTTP versus `Node.extractDirect` throughput.
- [14. Follow-up: the corpus-minimum document, the serial batch contract, and the plan-resolution floor](../../../../../docs/design/inference/history/gliner2/gliner25-base-qualification-2026-09.md#14-follow-up-the-corpus-minimum-document-the-serial-batch-contract-and-the-plan-resolution-floor): corpus-minimum floor, serial batch contract, planning floor.
- [15. Follow-up: fp16-encoder single-window qualification -- root cause, reviewed tolerance, and selection wiring](../../../../../docs/design/inference/history/gliner2/gliner25-base-qualification-2026-09.md#15-follow-up-fp16-encoder-single-window-qualification----root-cause-reviewed-tolerance-and-selection-wiring): fp16 root cause, tolerance, production row, and conversion manifest (the long-document follow-up remains below).

### 1. Identity

The pulled artifact's five files were hashed independently with
`shasum -a 256` and match `scripts/gliner25/oracle_manifest.json`'s `"base"`
entry exactly (byte-for-byte), which is also what
`oracle.py verify-fixtures`/`verify-references` cross-check for the
checked-in config/reference fixtures:

```sh
python3 zig/pkg/inference/scripts/gliner25/oracle.py verify-fixtures
python3 zig/pkg/inference/scripts/gliner25/oracle.py verify-references
```

| File | Size (bytes) | SHA-256 |
| --- | ---: | --- |
| `config.json` | 3,150 | `0eb92d00584d613aab32b2178f84a85176b62c87ae3689ce9084e83f6eba64d1` |
| `encoder_config/config.json` | 857 | `d36a845b9f25dcaf1ec45a1c4bdf65ea4ac20596537e14530ec9f660a63aeca4` |
| `model.safetensors` | 774,366,564 | `7274094de2e0c2a37a386f55fc4e23061a954da5bd7a335e7dfe56f2743c277a` |
| `tokenizer.json` | 8,341,713 | `cbc8ae6037812709c9c26f2a160f8dc48b0440bcb79c8141804259ae2d6adac3` |
| `tokenizer_config.json` | 645 | `0bf3ea0873234bd9bfdd3853c440395009ac6365a925b91654daed5396d655e1` |

These are the exact digests compiled into `gliner_boundary_qualification.zig`'s
`fastino_gliner25_base_v1` identity.

### 9. Follow-up: long-document windowing qualified for the real dogfood schema

Section 8 widened the single-window bound to `document_bytes<=610` for the
real dogfood schema, but recorded that most of `examples/dogfood`'s
longer design-doc sections still needed long-document windowing, which
remained unqualified (`.long_document` was not in the feature set) and
correctly failed closed with `error.UnsupportedGlinerBoundaryRuntime`. This
section qualifies it.

**Design already in place.** `extractors/gliner_boundary_long_executor.zig`
(landed with the boundary architecture in PR #720) already implements
windowed execution: overlapping windows sized to the checkpoint's own
declared per-window body-word capacity (`config.max_len` = 4096 words for
the base backbone, matching the wire's own `long_document.window_words`
default), offsets mapped back to whole-document coordinates
(`gliner_boundary_long_document.zig`'s `Plan`/`rebaseSource`), entities
deduplicated across overlapping windows (`mergeMentions`), and relations
resolved only when both endpoints fall in one window and then merged/
deduplicated document-wide (`gliner_boundary_long_relations.zig`). Model
tensors (encoder/head activations) are freed after every window; only
bounded scalar evidence survives to the next, so one window's memory
profile is independent of document length, and cumulative admission
(`Limits.max_total_encoded_tokens`/`max_total_attention_work`) bounds total
work across the whole document regardless of window count. This section's
job was purely to measure and review this existing design against real
long documents, not to build it.

**Document-size bound.** Section boundaries in this repository come from
`docsaf.MarkdownProcessor` (one section per Markdown heading, no minimum
merge threshold as `examples/dogfood` configures it -- see
`examples/dogfood/ingest.go`). A survey of every such section under
`zig/*.md` and `work-log/**/*.md` (2,355 sections) found: p50 = 982 bytes,
p90 = 4,378, **p95 = 7,285**, p99 = 23,428, **max = 99,008** (`zig/PDF.md`'s
"Review findings and required fixes"). This matches the follow-up brief's
"typical sections are 1-8 KB, some are 20-90 KB" characterization.

**Geometry.** `extractors/gliner_boundary_qualification.zig`'s new
`"gliner boundary qualification measures pinned base checkpoint
long-document production geometry"` test tokenizes and plans (no model
weights) real sections through the long executor's window planner, with
`long_document.mode=window` and the wire's default window/overlap words
(4096/128), against the real dogfood schema (11 entities, 6 relations):

- The two short single-window-shaped fixtures from section 3 (still sent
  with `long_document.mode=window`, since `examples/dogfood` now requests
  it unconditionally -- see below), producing exactly one window each.
- `zig/pkg/antfly/src/storage/lsm/LSM.md`'s "Read And Scan Work" (6,787
  bytes, ~p95): 1 window, window_words=1183, padded_sequence_tokens=1455.
- `zig/VOPR.md`'s "Completion-Claim Audit" (37,143 bytes): 2 windows,
  window_words=[2139,4096], padded_sequence_tokens=[3131,5708].
- `zig/PDF.md`'s "Review findings and required fixes" (99,008 bytes, the
  corpus max): 4 windows, window_words=[3994,4096],
  padded_sequence_tokens=[4594,4690].

Exact observed range across all cases: `document_bytes=[26,99008]
document_words=[5,15894] window_count=[1,4] window_words=[5,4096]
padded_sequence_tokens=[106,5708]`. This is the exact `LengthContract` on
the new `fastino_gliner25_base_v1_long_document_features` production rows
(native and Metal) in `models/gliner_boundary_qualification.zig`, with
features `entities, relations, word_whitespace, overlap_flat, offset_utf8,
decoder_auto, long_document, record_identity_occurrence, confidence, spans`
-- exactly `examples/dogfood`'s real schema shape (entities + relations
only) and nothing wider. This is a separate row from the single-window
row, not a widening of it: the two require disjoint features
(`.single_window` vs `.long_document`), so a single-window request cannot
borrow the long-document row's wider bounds, and vice versa.

**Canonical envelope shape.** The long executor's `mergeAll` builds the
same `pipeline.Sample` type the single-window executor does, so both are
serialized by the identical `extraction_v2.zig` `writeSample`/`endpoint`
code: entities carry `text`/`label`/`start`/`end`/`score`; relations carry
`type` and `source`/`target` objects resolved to `entity_index` (matched
against the final deduplicated entities array by exact rebased byte span
and text) plus `score` -- never the pipeline's internal `head`/`tail`
value representation. This was exercised end to end, on both backends,
through both the HTTP handler and the in-process provider entry (matching
the two real call sites of `extractWithAdmission` from section 8):

- `"gliner boundary long executor HTTP canonical schema_version 2 shape for
  a real multi-window document with relations native/Metal"`
  (`server/gliner_boundary_service_test.zig`): a real HTTP `/ai/v1/extract`
  request against `zig/VOPR.md`'s "Completion-Claim Audit" (37KB, 2
  windows), asserting every entity and relation in the response matches the
  canonical shape and that no `head`/`tail` key ever appears.
- `"gliner boundary long executor provider extractDirect canonical
  schema_version 2 relations shape for a windowed request native/Metal"`:
  the same assertions through `Node.extractDirect`, the entry point the
  in-process/embedded worker actually calls.

> **Relocated:** the rest of section 9 (Metal process isolation, schema-specific floor, throughput, end-to-end, dogfood windowing, in-process ingest) → [history](../../../../../docs/design/inference/history/gliner2/gliner25-base-qualification-2026-09.md#9-follow-up-long-document-windowing-qualified-for-the-real-dogfood-schema).

### 10. Follow-up: long-document throughput (window size, batching, precision)

> **Relocated:** the window-size sweep, batching design notes and measurements, and the first fp16 probe → [history](../../../../../docs/design/inference/history/gliner2/gliner25-base-qualification-2026-09.md#10-follow-up-long-document-throughput-window-size-batching-precision).

**Default changed, bound stays wide.** `extraction_v2.zig`'s
`LongDocument.window_words`/`overlap_words` defaults changed from
4096/128 to **1024/32**; the qualified `window_words` upper bound stays at
4096 (a request may still explicitly opt into up to 4096, e.g. to trade
throughput for fewer, larger windows), and `window_count`'s upper bound
widened from 4 to **17** (the real observed maximum, at 1024 words/window,
for `zig/PDF.md`'s 99,008-byte corpus-max section -- see the updated
`fastino_gliner25_base_v1_long_document_lengths` row, which is now the
union of the 1024-word and 4096-word sweeps' measured ranges). This
request option was already public (`options.long_document.window_words`);
only its default value and the qualified bound changed. No
`examples/dogfood` change was needed for this part (it does not set
`window_words`, so it picks up the new default automatically).

### 11. Follow-up: the 9-request incident was batching, not document geometry, plus a widened bound and named length-limit errors

A real `examples/dogfood ingest -reset` run against the corpus (1,205
sections; see `dogfood-followup-final2.log`, scratchpad) reported 9
extraction failures with `error.UnsupportedGlinerBoundaryRuntime`,
`request_bytes` from 33,130 to 99,631 -- all comfortably within section 9's
qualified `document_bytes<=99,008` bound, which made the failures look like a
geometry-qualification gap.

> **Relocated:** the batching root cause and fix, named length-limit errors, OOM group retry, and retry-storm notes → [history](../../../../../docs/design/inference/history/gliner2/gliner25-base-qualification-2026-09.md#11-follow-up-the-9-request-incident-was-batching-not-document-geometry-plus-a-widened-bound-and-named-length-limit-errors).

**Widened the long-document bound anyway, with real measured margin.**
Independently of the above, the corpus's real maximum section (per an exact
`docsaf`-accurate Go-side sweep of the whole ingest corpus, not the
approximate heading-to-heading text search sections 9-10's evidence used)
is `zig/PDF.md`'s "Review findings and required fixes" at 92,302 body bytes
/ 14,503 words / 15 windows at the default 1024-word window -- already
inside the section 9/10 bound, and no other real section comes close. To
qualify past today's corpus maximum with real margin (not extrapolation --
this file's stated policy), two synthetic documents were built by
concatenating `zig/PDF.md`'s two largest real sections verbatim (130KB and
182KB/28,275 words), fixtured under
`zig/pkg/inference/testdata/gliner25/long_document_probe/`, and swept
through `extractors/gliner_boundary_qualification.zig`'s existing
long-document geometry test at both window_words=1024 and 4096:

```
document_bytes=[26,182000] document_words=[5,28275] window_count=[1,29] window_words=[5,4096] padded_sequence_tokens=[106,5708]
```

`window_count` rose from 17 to 29 (window_words=1024: up to 29 windows;
window_words=4096: up to 8) and `document_bytes`/`document_words` rose to
182,000/28,275; `window_words`/`padded_sequence_tokens` were already wide
enough (the new synthetic documents' per-window token counts, up to 4,831,
stayed under the existing 5,708 maximum). This is now the production
`fastino_gliner25_base_v1_long_document_lengths` row.

**Verification.** `zig build inference-test -Doptimize=fast --
--test-filter "gliner boundary"` (native + Metal; `160` selected, `138`
passed, `22` skipped, `0` failed) and `--test-filter "extraction"` (`100`
selected, `99` passed, `1` skipped, `0` failed) after every change in this
section. A live `antfly inference run --port 8098` server (same budget
flags as section 9) served the real 99,631-byte corpus-maximum request
(`zig/PDF.md`'s "Review findings and required fixes") end to end: HTTP 200,
279 entities and 13 relations, ~6-7s, both before and after a full
`zig build -Doptimize=fast` rebuild with every change in this
section applied.

### 15. Follow-up: fp16-encoder single-window qualification -- root cause, reviewed tolerance, and selection wiring

> **Relocated:** the single-window root cause, tolerance review, production row, selection wiring, and single-window throughput → [history](../../../../../docs/design/inference/history/gliner2/gliner25-base-qualification-2026-09.md#15-follow-up-fp16-encoder-single-window-qualification----root-cause-reviewed-tolerance-and-selection-wiring).

#### Follow-up: fp16-encoder long-document qualification and corpus-shaped throughput

Reviewed 2026-09-20. `examples/dogfood` requests `long_document.mode=window`
for every section and essentially every real section needs it, so the
single-window fp16 row above cannot serve real traffic on its own. The
long-document row for `fastino_gliner25_base_v1_fp16_encoder` (native and
Metal) follows exactly the evidence pattern the fp32 long-document row used
(section 9), plus fp32-vs-fp16 parity through the long executor itself.

**Geometry.** Re-running the long-document geometry test with
`ANTFLY_GLINER25_BASE_MODEL_DIR` pointed at the fp16 bundle reproduces
`fastino_gliner25_base_v1_long_document_lengths` byte-for-byte at the 1024-
and 4096-word sweeps (the test reads only the tokenizer and JSON config, never
weights, so the substitution is valid). The row reuses the fp32
`LengthContract` and feature set unchanged.

**Correctness and shape.** The long-executor canonical-shape tests are now
parametrized over the bundle directory (`resolveModelDirectoryFromEnv` in
`server/gliner_boundary_service_test.zig`) and run against
`ANTFLY_GLINER25_BASE_FP16_MODEL_DIR`: the 37 KB `VOPR.md` "Completion-Claim
Audit" multi-window document through the HTTP handler and the in-process
provider entry on native and Metal, the ~99 KB `PDF.md` corpus-maximum
section through both entries, and the corpus-minimum documents (the 20-byte
`SCHEMA.md` section and a one-character document) through the provider entry
on both backends. All pass with the canonical schema_version 2 envelope and,
for the multi-window documents, a `window_count` proving the merge path ran.

**fp32-vs-fp16 parity through the long executor.** "gliner boundary long
executor fp32 vs fp16 encoder parity on real long documents native/Metal"
runs the same `VOPR.md`, `PDF.md`-maximum and corpus-minimum documents
through both bundles on each backend, matches every entity and relation
decision (label, text, span) between precisions order-independently by
identity, and bounds every matched confidence delta:

| Backend | matched decisions | decision mismatches | max abs diff | mean | median | p99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| native | 445 | 0 | 0.0016823 | 0.0001291 | 0.0000632 | 0.0010008 |
| metal | 445 | 0 | 0.0016727 | 0.0001291 | 0.0000636 | 0.0009984 |

The reviewed long-document bound is
`fp16_encoder_long_document_confidence_tolerance = 2.5e-3`
(`pipelines/gliner_boundary_pipeline.zig`; measured max plus ~49% headroom),
deliberately wider than the single-window `7.5e-4` and never a widening of
it. Root cause of the larger long-document residual, also not a defect: the
long executor's cross-window duplicate-mention tie-break picks one window's
estimate for a span that two overlapping windows both scored. Under fp16
weight rounding the two windows' independent estimates can differ by more
than any single-window delta, and when they are near-tied the two precisions
can pick different windows for the same span, so the reported confidence is
one window's value versus the other's. The decision (label, text, span) is
identical either way, which is why the parity test matches by identity and
the mismatch count is zero on both backends.

**Throughput, corpus-shaped.** The 40-section corpus sample from section 13
(32 sections of 1-8 KB and 8 of 20-40 KB, regenerated by the same recipe,
300,938 bytes total, `throughput_corpus_40.json`), fed sequentially through
the in-process provider entry (`Node.extractDirect`, the path the embedded
worker uses) with the real dogfood schema and windowing, ReleaseFast, warm
managed session, one request in flight:

| Backend | fp32 wall | fp32 sections/s | fp16_encoder wall | fp16_encoder sections/s |
| --- | ---: | ---: | ---: | ---: |
| native | 82.3 s | 0.486 | 85.4 s | 0.469 |
| metal | 14.7 s | 2.719 | 42.7 s | 0.937 |

fp16 is a wash on native and **2.9x slower on Metal**, the opposite of the
single-window harness above. The single-window harness creates sessions
directly and exercises the plain kernels; the production entry goes through
the model manager's managed session, whose Metal resident-weight
`optimized_v2` fast path is fp32-only by design, so the fp16 bundle falls
back to the per-dispatch path and loses far more than the halved weight
bandwidth gains. **fp32 stays the default.** The fp16 bundle is qualified so
it can be selected deliberately (memory-constrained hosts, or the native
backend where it costs nothing), not because it is faster.

## How to re-qualify a different or wider artifact

1. Pull the artifact and verify its digests against
   `scripts/gliner25/oracle_manifest.json` (extend that manifest first if it
   is a new revision or variant `oracle.py` does not yet know about).
2. Set `ANTFLY_GLINER25_<VARIANT>_MODEL_DIR` to the pulled directory and run
   `zig build inference-test -Doptimize=fast -- --test-filter "gliner
   boundary"`. Every previously-skipped pinned test for that variant must
   pass.
3. Add or extend a geometry-measuring test like the one in
   `extractors/gliner_boundary_qualification.zig` for the request shapes you
   intend to qualify, and record the printed bounds.
4. Run the CPU and/or Metal benchmark harnesses in `scripts/gliner25/` against
   the same artifact for throughput evidence.
5. Add a new `Entry` to `production_entries` in
   `models/gliner_boundary_qualification.zig` with the exact digests, the
   feature set you have real correctness evidence for (not the full enum --
   only what was measured), and the measured `LengthContract`. Add one row
   per reviewed backend.
6. Update this document's status table and evidence section.

A mismatched digest, an unreviewed backend, a feature outside the row, or
geometry outside the measured bounds must continue to fail closed with
`error.UnsupportedGlinerBoundaryRuntime` -- that is what the qualification
module's own tests in `models/gliner_boundary_qualification.zig` enforce.

### Embedded knowledge graph extraction and bounded windows

The exact pinned FP32 `GLiNER2.5-Decide-1B` artifact now qualifies native and
Metal entity/relation extraction using its ModernBERT encoder, marker span
representations, learned GRU count slots and span scoring heads. Its qualified
complete prompt ceiling is 1024 tokens, with one item per invocation. This
supersedes the earlier 198-token FP32 limit; the converted Q8 decision artifact
retains its separate 198-token qualification and does not advertise extraction.
Configure decision enrichments with `batch_size: 1`. Model execution still
requires memory admission, including resident weights and bounded head scratch.

The pinned base and multi-Decide FP32 boundary artifacts also qualify bounded
word windows with descriptions and classification, including prompts smaller
than the former 103-token floor. Each complete schema/body prompt is tokenized
before execution and must fit 1024 tokens. Window bodies shrink to fit schema
cost; oversized schemas fail before inference. Requests remain bounded by 64
windows, 1 MiB of text, 131072 words and runtime token/output limits. These are
composed per-window and merger ceilings, not a claim of model attention across
an entire document. Classification aggregates owned-word-weighted raw logits;
mentions and relations preserve global offsets and overlap semantics.

Independent pinned upstream captures and reproducible scripts live under
`src/extractors/testdata/knowledge_graph` and `scripts/gliner25`. Tests compare
encoded inputs, learned head intermediates, valid span scores and final outputs
on native and Metal. Unsupported structures/attributes, joint decoding and
relation endpoint constraints remain rejected. Pull metadata publishes
`qualified_limits` for verified artifacts; refresh older pulls to record it.
See [Lite knowledge graph usage](../../../../LITE_KNOWLEDGE_GRAPH.md).
