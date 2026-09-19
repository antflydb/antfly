# GLiNER2.5 Boundary Runtime Qualification

This document is the reviewed release record for the GLiNER2.5 boundary
architecture (`zig/pkg/inference/src/models/gliner_boundary.zig`,
`gliner_boundary_qualification.zig`). PR #720 landed the boundary decoder,
pipeline, and closed qualification policy with the production table
deliberately empty and `runtime_available = false`, so `POST /ai/v1/extract`
and `antfly inference pull` withheld the `extract` task for every boundary
checkpoint until a specific artifact was reviewed. This document records the
first such review.

## Current status

| Artifact | Backbone | Precision | Backend | Status |
| --- | --- | --- | --- | --- |
| `fastino/gliner2.5-base-v1` (rev `72ac19b486cd4557424c8d61114e7530c243e9b0`) | base | fp32 (safetensors) | native | Qualified (single-window) |
| `fastino/gliner2.5-base-v1` (rev `72ac19b486cd4557424c8d61114e7530c243e9b0`) | base | fp32 (safetensors) | metal | Qualified (single-window) |
| `fastino/gliner2.5-base-v1` (rev `72ac19b486cd4557424c8d61114e7530c243e9b0`) | base | fp32 (safetensors) | native | Qualified (long-document windowing, up to 182,000 document bytes -- sections 9, 11) |
| `fastino/gliner2.5-base-v1` (rev `72ac19b486cd4557424c8d61114e7530c243e9b0`) | base | fp32 (safetensors) | metal | Qualified (long-document windowing, up to 182,000 document bytes -- sections 9, 11) |
| `fastino/gliner2.5-small-v1` | small | any | any | Not reviewed |
| `fastino/gliner2.5-multi-v1` | multi | any | any | Not reviewed |
| `fastino/gliner2.5-base-v1` (converted via `gliner25-convert`, same rev) | base | fp16_encoder | native, metal | **Not qualified** -- real-pipeline parity measured (section 12); narrowly exceeds the existing tolerance on 1 of 62 checked confidence values, both backends |
| Any other digest, revision, or precision of `gliner2.5-base-v1` | base | any | any | Not reviewed |

`gliner_boundary.runtime_available` is now `true`, and
`gliner_boundary_qualification.zig`'s production table carries exactly the
four rows above (same identity; single-window and long-document rows are
separate features/bounds, each reviewed for native and Metal). Everything
else --
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

## Evidence

All reproduction commands below run from the repository root, with
`ANTFLY_GLINER25_BASE_MODEL_DIR` set to the pulled artifact directory
(`~/.antfly/inference/models/fastino/gliner2.5-base-v1` by default).

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

### 2. Correctness

`zig build inference-test -Doptimize=ReleaseFast -- --test-filter "gliner boundary"`
now runs (rather than skips) every test pinned to `ANTFLY_GLINER25_BASE_MODEL_DIR`,
including:

- `gliner boundary pipeline Python parity pinned base checkpoint all inference
  tasks` -- native execution against the real weights, all ten canonical task
  fixtures (`testdata/gliner25/pipeline_cases_base.json`: entities, relations,
  entity attributes, single classification, natural/latent/anchorless
  records, legacy structures, enum fields, constrained classification,
  JointIE), each checked byte-for-byte against the pinned Python/Fastino
  reference capture.
- `gliner boundary device Metal pinned base full inference pipeline parity`
  -- the same ten fixtures through the full Metal encode/head/score/decode
  path, same exact-match check.

Both tests independently re-verify the model/tokenizer/config file digests
against the pinned fixture before running, so they cannot silently drift onto
a different checkpoint. 126 of 148 `gliner boundary`-filtered tests pass with
the base directory set; the remaining 22 skips are the `small`/`multi`
pinned tests, which stay skipped (and those variants stay unqualified) because
this machine does not have those checkpoints pulled.

### 3. Geometry (LengthContract bounds)

`extractors/gliner_boundary_qualification.zig`'s
`"gliner boundary qualification measures pinned base checkpoint production
geometry"` test tokenizes the ten canonical fixtures, the shortest and the
`/ai/v1/extract` repro requests, and (after the follow-up in section 8 below)
`examples/dogfood`'s real 11-entity/6-relation production schema against
both its own short repro text and a realistic 107-word/610-byte corpus
paragraph (`zig/ENRICHMENTS.md`), through the pinned tokenizer, and prints
the exact observed range:

```
document_bytes=[26,610] document_words=[5,112] window_words=[5,112] padded_sequence_tokens=[14,218]
```

The production row's `LengthContract` uses exactly this range (plus
`request_items=[1,1]` and `window_count=[1,1]`, since every measured case was
a single-item, single-window request). The wider dogfood schema alone
roughly doubles `padded_sequence_tokens` versus the original 3-entity/2-
relation rows at the same document length (56 -> 118): the schema's own
entity/relation vocabulary is encoded as a prefix ahead of the document, so
a bigger schema costs real sequence budget independent of document size.
Widening any bound requires new measurement -- this file is the place to add
it, and to re-run the geometry test to update the row. A document needing
more single-window budget than this measured range -- most of
`examples/dogfood`'s longer design-doc sections -- still requires
long-document windowing, which remains unqualified for this checkpoint
(`.long_document` is not in the feature set) and correctly fails closed with
`error.UnsupportedGlinerBoundaryRuntime` rather than silently truncating or
misbehaving.

### 4. Throughput

The existing [`BENCHMARK.md`](../../scripts/gliner25/BENCHMARK.md) CPU
comparison (recorded 2026-09-09, before this qualification, using this exact
checkpoint's file digests) already covers this artifact: native/Python
latency ratios of 0.58-0.80 across the same ten tasks, mixed-task median
36.174 ms native versus 45.159 ms Python, one CPU math thread. A fresh Metal
comparison against pinned Fastino MPS/CPU can be run with
[`benchmark_metal.py`](../../scripts/gliner25/METAL_BENCHMARK.md); neither
harness is a serving/HTTP performance qualification (see their measurement
contracts), which is why the end-to-end `/ai/v1/extract` numbers below are
recorded separately.

### 5. End to end

With the runtime rebuilt (`zig build -Doptimize=ReleaseFast`, or `zig build
antfly` for just the CLI) and re-pulled (`antfly inference pull
fastino/gliner2.5-base-v1`, now producing `"tasks":["extract"]` and
`"capabilities":["extraction","classification","relations","records"]` in
`model_manifest.json`), `POST /ai/v1/extract` against
`fastino/gliner2.5-base-v1` was exercised with the repro request:

```json
{
  "model": "fastino/gliner2.5-base-v1",
  "inputs": [{"id": "1", "content": "The metadata server coordinates Raft groups. VOPR exercises the DataServer under fault injection."}],
  "schema": {
    "entities": ["component", "subsystem", "test"],
    "relations": [{"type": "depends_on"}, {"type": "tested_by"}]
  },
  "options": {"include_confidence": true, "include_spans": true}
}
```

which returned (Metal backend, `antfly inference run --port 8098
--host-budget-mb 16384 --backend-budget-mb 16384 --scratch-budget-mb 16384
--combined-budget-mb 32768 --kv-budget-mb 4096`; see "Memory budget" below):

```json
{"object":"extraction","model":"fastino/gliner2.5-base-v1","schema_version":2,"data":[{"id":"1","offset_unit":"utf8_bytes","entities":[{"label":"component","text":"metadata server","score":0.829,"start":4,"end":19},{"label":"component","text":"DataServer","score":0.692,"start":64,"end":74},{"label":"subsystem","text":"Raft groups","score":0.706,"start":32,"end":43},{"label":"test","text":"VOPR","score":0.982,"start":45,"end":49}],"relations":[{"type":"tested_by","source":{"entity_index":1,...},"target":{"entity_index":3,...},"score":0.897}]}],"usage":{"prompt_tokens":56,"completion_tokens":0,"total_tokens":56}}
```

HTTP 200, 1.2-2.5 s wall time (Metal, cold-ish scratch admission included).

**Comparison with `antflydb/gliner2-base-v1`** (the legacy span-architecture
GLiNER2 GGUF Q4_K checkpoint) on the identical input via its native legacy
request shape:

| | `fastino/gliner2.5-base-v1` (boundary, fp32) | `antflydb/gliner2-base-v1` (span, Q4_K) |
| --- | --- | --- |
| "Raft groups" label | `subsystem` (correct) | `component` (wrong) |
| `tested_by` relation | DataServer -> VOPR, score 0.897 (correct direction) | VOPR -> DataServer, score 0.749 (backwards) |
| Extra low-confidence relation | none | spurious `depends_on`, score 0.433 |
| Latency (this run) | 1.2-2.5 s | 1.5 s |

This is one qualitative sample, not a precision/recall benchmark, but the
boundary checkpoint's entity typing and relation directionality were both
correct where the legacy model's were not, at comparable latency.

### 6. Two more gates discovered and fixed during this qualification

Flipping `runtime_available` and publishing the production row was not
sufficient by itself; three more places independently denied gliner2.5
regardless of qualification, discovered by running the actual end-to-end
repro rather than only unit tests:

- `server/server.zig`'s `taskMatchesModelListing` had a blanket
  `if (gliner_model_type == "gliner2.5") return false;` for every listing
  category (`GET /ai/v1/models`). Fixed to gate only on
  `!gliner_boundary.runtime_available`, so a qualified artifact's real,
  pull-time-verified tasks/capabilities are listed like any other model's;
  an unqualified one still has an empty tasks/capabilities list on disk and
  is excluded the same way any other unsupported model is.
- `models/capabilities.zig`'s `modelSupportsCapability` had the identical
  blanket exclusion, reached from `validateTextEntityExtractionManifest`
  (server.zig) on every extraction request. Same fix: gate on
  `!gliner_boundary.runtime_available` instead of the model family name
  outright.
- The documented plain extraction request (no `"schema_version"` field, as
  used throughout this file, in `zig/EXTRACT.md`, and by
  `examples/dogfood`'s producer JSON) is dispatched to a pre-boundary legacy
  code path that treats any manifest with a non-empty `gliner_model_type` as
  the old span-architecture GLiNER pipeline
  (`LoadedModel.isGlinerModel()` only checks `gliner_model_type.len > 0`,
  which is also true for `"gliner2.5"`), which cannot execute a boundary
  session and fails deep inside `session_factory.zig` with
  `error.BoundaryExtractionRequiresSchema`. See section 8 for the full fix:
  the upgrade to `schema_version:2` now lives in `extractWithAdmission`, the
  one entry both the HTTP handler and the in-process provider's
  `extractDirect`/`extractDirectWithControl` share.

These fixes only ever *widen* what a *qualified* artifact can do; they add
no new path to bypass `hasQualifiedIdentity`/`require()`'s exact-digest
enforcement for an unqualified one.

### 7. Memory budget

The fp32 738 MiB checkpoint's admission estimate (encoder + boundary head at
`max_len=4096` worst-case single-window capacity, independent of the actual
request size) exceeded this machine's small unit-test-style defaults.
`--host-budget-mb 4096 --backend-budget-mb 4096 --scratch-budget-mb 2048
--combined-budget-mb 8192 --kv-budget-mb 1024` still produced
`MEMORY_BUDGET_EXCEEDED`; `--host-budget-mb 16384 --backend-budget-mb 16384
--scratch-budget-mb 16384 --combined-budget-mb 32768 --kv-budget-mb 4096`
succeeded. Operators serving this checkpoint should size these flags (or the
equivalent `antfly standalone` config) generously; this is a capacity
planning note, not a qualification bound -- it does not appear in the
production row's `LengthContract`.

### 8. Follow-up: the in-process provider entry, and widening for the real dogfood schema

The fix in section 6 lived in `extractJSON` (the HTTP handler) only. The
in-process worker's provider "extract" operation
(`host.linkedInferenceInvokeProvider` in
`antfly/src/standalone/inference_host.zig`) never calls `extractJSON`; it
calls `Node.extractDirectWithControl` directly, which calls the shared
`extractWithAdmission`. Running `examples/dogfood` in-process (real
enrichment drain, `-extract-model fastino/gliner2.5-base-v1`) still hit
`error.BoundaryExtractionRequiresSchema` on every extraction call.

**Fix**: moved the upgrade into `extractWithAdmission` itself --
`extracting_api.Request` already carries a `schema_version: ?u32` field, and
`extractWithAdmission` already branches on it
(`if (schema_version == 2) return self.extractV2WithAdmission(...)`) before
any legacy dispatch. A new `Node.resolvesToBoundaryArchitecture(io,
model_name)` helper backs both this check and `extractJSON`'s existing
JSON-level `boundaryUpgradeRequestJsonIfNeeded`, so the peek logic exists
once even though it currently runs from two call sites (HTTP's raw-JSON
path intercepts earlier, before HTTP's own legacy `entities_relations`/
`classifications` ctx-based handlers, which do not otherwise reach
`extractWithAdmission`; the provider path has no such earlier interception
point). Covered by "gliner boundary provider extractDirect upgrades a plain
request for the qualified base checkpoint" in
`server/gliner_boundary_service_test.zig`, which calls
`Node.extractDirect` the same way the provider does, against the real
checkpoint at its standard pulled location.

**Verification**: ran `examples/dogfood ingest -reset` in-process
(`ANTFLY_INFERENCE_WORKER=zig-out/bin/antfly`, no HTTP) against the real
1,202-section corpus, twice. `error.BoundaryExtractionRequiresSchema`: 1,195
of 1,202 sections before the fix (per the original handoff), 0 of 1,202
after. Ingest completed both times (`INGEST_EXIT=0`).

**New finding from that run**: `examples/dogfood`'s actual schema (11
entities, 6 relations -- `knowledgeGraphIndexJSON` in
`examples/dogfood/index_config.go`) is wider than this qualification's
original 3-entity/2-relation evidence, and real design-doc sections range
up to tens of KB. Against the qualified checkpoint, most extraction calls
initially still failed -- now with `error.BoundaryTextLimitExceeded` /
`error.UnsupportedGlinerBoundaryRuntime`-class rejections (the qualification
gate correctly refusing geometry outside the measured `LengthContract`) or,
for a few of the largest sections, `error.MemoryBudgetExceeded` -- rather
than the routing crash. Section 3 above records the widened bounds measured
for the real dogfood schema. This still does not qualify long documents:
sections longer than roughly 110 words need long-document windowing
(unqualified), and will continue to correctly fail closed rather than run
un-reviewed. Making the rest of the corpus succeed is follow-up work,
tracked by widening `production_entries` further with new measured
long-document evidence, not by relaxing this gate.

The remaining `error.MemoryBudgetExceeded` failures for the largest sections
are a capacity-planning matter (section 7): the in-process/embedded worker
path does not currently expose an equivalent of `antfly inference run`'s
`--host-budget-mb`/`--backend-budget-mb`/`--scratch-budget-mb`/
`--combined-budget-mb`/`--kv-budget-mb` flags (only the whole-process
`ANTFLY_(INFERENCE_)PROCESS_MEMORY_BUDGET_MB` env vars exist, which did not
change the outcome in testing), so operators embedding this checkpoint
in-process cannot currently raise these specific generation budgets the way
`antfly inference run`'s CLI flags allow. That gap is in the embedded
worker/`antflylite` configuration surface, outside this file's ownership.

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

Both passed on native and Metal (`zig build inference-test -Doptimize=ReleaseFast
-- --test-filter "gliner boundary"`, `152` -> `156` selected as these tests
were added, `134` passed / `22` skipped -- the 22 skips are unrelated pinned
`small`-backbone tests gated on `ANTFLY_GLINER25_SMALL_MODEL_DIR`, not set
in this environment).

**Metal is a process-required backend** (`backends.zig`'s
`requiresProcessIsolation`): a test harness that loads a Metal session
through `Node`/`ModelManager` (rather than a raw `session_factory` session,
as the small-backbone Metal parity tests do) must set
`process_termination_available = true` in `Node.init`'s config or the
model manager refuses to close/reopen the session
(`error.ProcessIsolationRequired`), matching
`gliner_boundary_metal_socket_test.zig`'s existing convention.

**A real geometry pitfall found while measuring:** the qualified row's
`padded_sequence_tokens` floor (106) was measured only against the wide
11-entity/6-relation dogfood schema. A request using the narrower
3-entity/2-relation schema from section 3/5's earlier evidence (fewer
schema-prefix tokens for the same document) produces
`padded_sequence_tokens` around 56-118 -- below this row's floor -- and is
correctly refused with `error.UnsupportedGlinerBoundaryRuntime` even
though it is "shorter." A row's bounds are schema-shape-specific, not just
document-size-specific; this is by design (see "Two-tier gate" above), not
a bug, and is why the provider-path test above deliberately uses the wide
schema rather than section 5's original repro schema.

**Throughput** (Metal, `antfly inference run --port 8098` with the same
generous budget flags as section 7, 12 real long sections from `zig/*.md`
and `work-log/**/*.md`, 21-40KB each, dogfood schema): sequential
(one in flight at a time, the "direct" baseline) completed all 12 in 47.8s
wall time -- 21 total windows, **~2.28s/window**, **~0.25 sections/s**
(versus GLiNER2's ~13 sections/s on much shorter single-tiny-window
sections -- not a comparable workload; GLiNER2.5's windows here average
~3,500-4,000 words each). Submitting the same 12 requests concurrently
completed in 35.7s wall time but only 9/12 returned 200 (3 were refused
with 503 under concurrent admission pressure): this machine has one Metal
device, so concurrent long-document requests do not exceed the sequential
per-request rate -- they approach it at best, consistent with GPU-bound
single-device serialization. True request-level batching (multiple
documents in one encoded forward pass) is not part of this qualification's
measured `request_items=[1,1]` contract.

**End-to-end verification.** With the runtime rebuilt
(`zig build antfly -Doptimize=ReleaseFast`) and `antfly inference run
--port 8098 --host-budget-mb 16384 --backend-budget-mb 16384
--scratch-budget-mb 16384 --combined-budget-mb 32768 --kv-budget-mb 4096`
running, a 6,144-byte real excerpt starting at `zig/VOPR.md`'s "## Purpose"
heading, POSTed with the dogfood schema and `"long_document":{"mode":
"window"}`, returned HTTP 200 in 3.1s with 11 entities and 3 relations in
the canonical shape (one window, since 6KB is well under the 4096-word
per-window budget).

**`examples/dogfood` now requests windowing.** `index_config.go`'s
`knowledgeGraphIndexJSON` previously never set `long_document` at all
(mode defaults to `.reject`), so no real ingest ever exercised windowing
regardless of qualification. This was the one `examples/dogfood` change
this task's contract required (see the file's rules on not otherwise
touching `examples/dogfood`): its extraction producer options now include
`"long_document":{"mode":"window"}`.

**In-process ingest result: not yet ~0 failures, for a reason outside this
file's ownership.** Running `examples/dogfood ingest -reset` in-process
(`ANTFLY_INFERENCE_WORKER=zig-out/bin/antfly`, no HTTP, the real
1,203-section corpus) completed (`runUntilIdle summary wall_ms=428142
embed_batches=184 embed_items=5000 extract_batches=195
extract_items=1267`), but essentially every `relations_v1` extraction call
still failed (1,198 `InferenceProviderFailure` + 5
`InferenceInvocationMemoryExceeded`). The sampled underlying causes
(`error.MemoryBudgetExceeded` x12, `error.LongDocumentWorkLimitExceeded`
x5, deduplicated by diagnostic fingerprint) are **not** a long-document
windowing correctness problem and **not** caused by this task's changes:
they reproduce identically for a single short plain request with no
`long_document` option at all, through the same embedded worker, on this
machine. Section 7's already-recorded gap is the root cause: the
in-process/embedded worker's automatic "lite embedded inference resource
policy" (`zig/pkg/antfly/src/standalone/inference_provider.zig`) derives
only a whole-process host memory limit and has no equivalent of `antfly
inference run`'s independent `--backend-budget-mb`/`--scratch-budget-mb`/
`--kv-budget-mb` flags, so the checkpoint's admission estimate cannot be
satisfied there regardless of document size or windowing -- this
checkpoint has never been able to serve any extraction through the
embedded worker path on this machine, single-window included. The 5
`LongDocumentWorkLimitExceeded` cases are a separate, correct fail-closed
outcome: cumulative attention work across windows near the top of the
qualified range (padded_sequence_tokens approaching 5,708 across up to 4
windows) can exceed the executor's generic, backend-independent
`Limits.max_total_attention_work` default (8 GiB of attention-score
elements) even while remaining within this row's reviewed geometry bounds
-- a real document-size-dependent safety cap distinct from, and additional
to, the qualification table. Both are documented in detail, with the exact
fix needed, in
`gliner25-longdoc-handoff.md` (scratchpad; not committed, since the fix
lives in `zig/pkg/antfly/**`/`go/pkg/antflylite`, outside this file's
ownership).

### 10. Follow-up: long-document throughput (window size, batching, precision)

Section 9 qualified long-document windowing correctly, but at the original
4096-word default window it measured ~2.28 s/window and ~0.25 sections/s on
real 21-40 KB sections (Metal) -- versus GLiNER2 base's ~7.9 sections/s on
the same corpus with its own windowing. This section makes it fast.

**Method.** A live `antfly inference run` server (Metal, same generous
budget flags as section 7) was fed ~40 real sections from `zig/*.md` and
`work-log/**/*.md` (a mix of 1-8 KB and 20-40 KB, the corpus's typical and
tail sizes), sequentially (one request in flight at a time -- the "direct"
baseline), at `long_document.window_words` of 512, 1024, 2048, and 4096,
each with proportional overlap (`window_words/32`, matching the original
4096/128 ratio). A 16-section subset (8 small, 8 large, one per distinct
file) was used for the full four-way sweep to keep total wall time
reasonable; window_count and per-request latency were recorded for each.

**Window size: the dominant lever.** Attention cost is quadratic in window
length; the sweep confirmed it directly, on the SAME 16-section subset:

| window_words | total wall time | total windows | sections/s | s/window |
| ---: | ---: | ---: | ---: | ---: |
| 512 | 4.63 s | 20 (8 large sections rejected: window_count exceeded the then-4-window qualified bound) | 3.46 (inflated by fast rejects) | 0.23 |
| 1024 | 7.42 s | 34 (2 large sections rejected, same reason) | 2.16 (inflated by fast rejects) | 0.22 |
| 2048 | 14.54 s | 26 (all 16 succeeded) | 1.10 | 0.56 |
| 4096 (prior default) | 26.68 s | 18 (all 16 succeeded) | 0.60 | 1.48 |

512 and 1024's "sections/s" columns are inflated by near-instant HTTP 400s
(admission correctly refusing a window count above the qualified bound at
the time, in ~0.03 s, before any model work) -- not a real throughput win
for those runs. 2048 and 4096 are the fair comparison (all 16 real): 2048
was already ~2.4x faster overall. **1024 words was chosen as the new
default** over 512: once section 9's window_count bound was widened (see
below) and every section actually completed, 1024 gave the best full-corpus
result of the sizes measured (see the "after" table below), while 512 would
need roughly double the windows (and admission bound) for the same
documents without a measured throughput benefit over 1024 to justify it.

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

**Batching windows into one forward pass.** GLiNER2.5's own encoder/head/
scorer pipeline already accepts a multi-sample `PreparedBatch` in one call
(`pipeline.runScoredWindows`, `request_device.runWindowsWithOutputAllocator`
-- `max_admission_batch = 64`); the long executor simply never used more
than one sample per call. `gliner_boundary_long_executor.zig`'s
`executeChecked` now groups a document's windows into batches of up to
`Limits.window_batch_size` (default **4**) and runs one forward pass per
group instead of one per window, for schemas without classification or
JointIE (whose per-sample scoring/candidate shapes have not been reviewed
batched -- see the `batchable` check; they keep the original
one-window-per-call path unchanged, byte-for-byte). This is bounded and
independent of document length by construction: peak per-call device/host
memory scales with the fixed group size, never with the document's total
window count, preserving the "bounded independent of document length"
memory story from section 9's design note. `Window.result` (owning one
window's `WindowResult`) became optional; a batched group's one shared
`WindowResult` is retained by `executeChecked`'s `batch_results` list
until merge finishes instead, and `Window.relations` (a plain borrowed
view, populated for both paths) replaced `mergeAll`'s direct
`window.result.outputs.samples[0].relations` read, so ordinary relations
never need `.result` at all; only JointIE's global candidate-graph solve
still reads `window.result.?.joint_candidates[0]` (guaranteed non-null,
since JointIE never batches).

Every existing test still passes unchanged (`134` passed, `22` skipped,
`0` failed after this change; see verification below), including the
section 9 canonical-shape tests, which now exercise the batched path for
real (VOPR.md's 37KB section needs 7 windows at the new 1024-word default,
so `window_batch_size=4` produces two groups, not seven single-window
calls) -- concrete evidence the batched merge path preserves the same
canonical envelope shape.

**Measured impact of batching.** On this hardware, with Metal weights
resident, batching's measured effect was small: per-window latency for the
16-section subset was ~0.26 s/window whether executed as 4-window groups
or one-window-per-call (both at the 1024-word default), suggesting
per-forward-call fixed overhead (Metal command buffer setup/dispatch) is
already small relative to per-request fixed costs (HTTP handling,
admission, tokenization, merge) at this window size on this machine --
window size, not call count, was this workload's bottleneck. Batching is
kept because it is correct, safe (bounded memory, all tests green), and
free where it doesn't help; it may matter more on hardware or backends
where per-call dispatch overhead is a larger fraction of total latency, or
at larger window sizes where each call does more relative work.

**Before/after, full comparison** (same 16-section subset, all sections
succeeding both times): prior default (4096, unbatched) took 26.68 s;
after (1024 default + batching) took **12.82 s** -- ~2.1x faster overall,
consistent with 1024's fair 2.4x-per-window improvement over 4096 (some of
that gain is absorbed by the large sections now needing more windows: 4-5
each at 1024 words versus 1-2 at 4096). Estimated corpus-wide throughput,
weighting this run's small- and large-section averages (~0.12 s/section
small, ~1.13 s/section large) by the real corpus's size distribution (p50
982 B, p90 4,378 B, p95 7,285 B, p99 23,428 B -- the vast majority of
`examples/dogfood`'s 1,203 sections are small): **~4.5 sections/s**, versus
~0.25 sections/s before this section's changes and GLiNER2 base's
~7.9 sections/s reference point on the same corpus. This is a corpus-shape
estimate from measured per-size-class averages, not a direct 1,203-section
timed run (the embedded-worker `MemoryBudgetExceeded` gap recorded in
section 9 still blocks a full in-process ingest timing on this machine;
the estimate instead composes real `antfly inference run` HTTP timings by
real corpus section-size frequency).

**A request costs no more through `long_document.mode=window` than through
the single-window path, for a short document.** A 610-byte document (the
qualified single-window row's own upper bound) was POSTed against both
paths, warmed up: 0.044-0.050 s either way -- the long executor's extra
Plan/merge bookkeeping at `window_count=1` is not measurable overhead here.

**Precision: fp16_encoder investigated, not yet formally qualified.**
`artifact.Precision` already includes `fp16_encoder`, and
`gliner25-convert` (`zig build inference-gliner25-convert-build`) converts
a pulled fp32 bundle to it (774,366,564 bytes fp32 -> 407,861,568 bytes,
weight-only: the tool reports `activation_precision`/
`accumulation_precision`/`head_precision` all staying `f32`, only encoder
weight storage narrows). Converting the qualified
`fastino/gliner2.5-base-v1` artifact and running
`gliner25-bundle-check` (`zig build inference-gliner25-bundle-check-build`)
against all ten canonical fixtures on both native and Metal produced
outputs matching the fp32 reference fixture to within ~1e-4 on every
sampled confidence score (e.g. "mixed_tasks"'s `John` entity: fp16
0.9969311 vs fp32 reference 0.9969325; the `works_for` relation: fp16
0.8721223 (native) / 0.8422742 (Metal, JointIE case) vs fp32 reference
0.8720568), with no crashes and structurally identical entities/relations/
classifications/records across all ten cases on both backends. This is a
real, positive signal that a lower-precision row is plausible, but it is
**not** a qualification: this only ran the informal diagnostic comparator
(`qualification:false` in its own output), not the full pinned Python-
parity suite (which requires the converted bundle's exact digests recorded
in a reviewed production row, matching section 3-6's rigor), and no
Metal-vs-native throughput comparison was run for the converted weights.
Recommendation: qualifying `fp16_encoder` for this checkpoint looks
worthwhile (smaller weight footprint should help Metal memory-bandwidth-
bound dispatch, compounding with the batching work above) and is a
reasonable next follow-up, but adding a new production row for it needs
the same reviewed rigor as sections 1-6, which this pass did not attempt.

**Verification.** `zig build inference-test -Doptimize=ReleaseFast --
--test-filter "gliner boundary"` (native + Metal; `156` selected, `134`
passed, `22` skipped -- unrelated `small`-backbone tests gated on an unset
env var --, `0` failed) after every change in this section, including the
window-size default change and the batching refactor together.

### 11. Follow-up: the 9-request incident was batching, not document geometry, plus a widened bound and named length-limit errors

A real `examples/dogfood ingest -reset` run against the corpus (1,205
sections; see `dogfood-followup-final2.log`, scratchpad) reported 9
extraction failures with `error.UnsupportedGlinerBoundaryRuntime`,
`request_bytes` from 33,130 to 99,631 -- all comfortably within section 9's
qualified `document_bytes<=99,008` bound, which made the failures look like a
geometry-qualification gap.

**Root cause: it wasn't document size.** Reproducing single real sections at
each reported byte size (largest sections of `zig/PDF.md`, `zig/VOPR.md`,
`zig/VECTORDBBENCH_FINDINGS.md`, `work-log/**`) through both
`Node.extractDirect` and a live `antfly inference run --port 8098` server
never failed -- every individual section, up to the real corpus maximum, was
served correctly. Searching for **pairs** of real corpus sections whose
combined multi-item batch request size matched each of the 9 reported sizes
found near-exact matches for all 9 (approximate-size diff under 50 bytes for
7 of 9, under 100 for the rest, before accounting for JSON-escaping of real
content). The actual cause:
`zig/pkg/inference/src/server/server.zig`'s `resolvedExecutorBatchImplementation`
advertised **native batching** (`mode = .native`, `max_items =
max_serial_family_batch_items = 128`) for the GLiNER boundary
(`native_gliner_extraction`) executor kind, so
`zig/pkg/antfly/src/asset_producer_runtime.zig`'s batcher (opportunistically,
under concurrent ingestion) grouped multiple documents into one wire request
whenever several extraction jobs happened to be pending at once. Every
`examples/dogfood` document requests `long_document.mode=window`
unconditionally (section 8), so any such batch had `request_items > 1` --
which both the single-window and long-document production rows have always
required to be exactly `1` (`LengthContract.request_items`, since no
correctness evidence for a batched multi-item request exists for either
merge path). The rejection was correct; it was reported as the generic
`error.UnsupportedGlinerBoundaryRuntime` with no indication that the
document itself was never the problem.

**Fix: stop advertising a capability that was never qualified.**
`resolvedExecutorBatchImplementation` now returns `mode = .none, max_items =
1, preferred_items = 1` unconditionally for `.native_gliner_extraction`,
regardless of the generic `max_serial_family_batch_items` ceiling used by
other tasks. This stops the antfly-side batcher from ever grouping GLiNER
extraction requests, eliminating the incident's root cause without touching
`zig/pkg/antfly/**`. See `models/gliner_boundary_qualification.test."boundary
qualification serves the reviewed fastino gliner2.5 base checkpoint and
still denies any mismatch"` and `server/server.test."microbatch registration
qualifies concrete GLiNER bundles and Qwen embedding profiles"`.

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

**Bounded-length rejections now name the dimension.** A closed-policy
rejection was always either "identity/backend/feature never reviewed" or
"geometry outside the reviewed row," collapsed into the same
`error.UnsupportedGlinerBoundaryRuntime`. These are now distinguished:
`models/gliner_boundary_qualification.zig`'s `Candidates.narrow`/`require`
return one of six new named errors
(`error.GlinerBoundary{RequestItems,DocumentBytes,DocumentWords,WindowCount,
WindowWords,PaddedSequence}LimitExceeded`) when identity, backend, and every
required feature already matched a reviewed row but the observed geometry
did not -- the generic error is now reserved for an actually-unreviewed
request shape. `extractors/extraction_v2.zig`'s `errorDetails` maps all six
to HTTP 413 `EXTRACTION_LIMIT_EXCEEDED` (the same family as
`BoundaryTextLimitExceeded` and friends) with a message naming the exceeded
dimension, instead of the generic 400 `UNSUPPORTED_EXTRACTION_FEATURE`. A
request batching more than one document (the exact incident above) now
returns `error.GlinerBoundaryRequestItemsLimitExceeded` /
`"GLiNER boundary extraction exceeds the qualified request_items (batch
size) limit"` rather than a bare unsupported-feature response.

**The long executor's window-batching now recovers from real memory
pressure instead of failing the whole document.** `window_batch_size`
(section 10's grouped-window batching, default 4) is a starting point, not
a fixed shape: an `error.OutOfMemory` while encoding/scoring one group (host
or device allocation pressure -- never a declared evidence-budget rejection,
which a smaller group would not relieve) now retries the same starting
window position with a strictly smaller group (halved, rounding up, down to
one window) instead of failing the whole document or repeating the
identical shape. Verified two ways: a pure property test that the halving
sequence from any starting size always strictly decreases and reaches
exactly 1 in a bounded number of steps, and a real-model integration test
(pinned base checkpoint, native) that wraps the executor's allocator in a
6 MiB `BoundedAllocator` -- small enough that the configured group of 4
windows cannot allocate but a smaller group can -- and confirms the document
still completes with the correct window count and nonempty entities, versus
an unbounded control run.

**On the reported CPU-spin/retry-storm follow-up.** A later run (after an
unrelated drain-livelock fix) hit `error.GlinerBoundaryRequestItemsLimitExceeded`
and `error.InferenceInvocationMemoryExceeded` on the large sections, then the
ingest process stalled at 100% CPU with no further log output
(`dogfood-drain-fix.log`, scratchpad). Investigation confirmed: every
`GlinerBoundary*LimitExceeded` error (like the pre-existing
`UnsupportedGlinerBoundaryRuntime`) collapses to the stable
`error.InferenceProviderFailure` at the provider ABI boundary
(`zig/pkg/antfly/src/standalone/provider_failure.zig`), which
`enrichment_runtime.zig`'s `enrichmentErrorDisposition` already classifies
`.terminal_request` (not retried) -- confirmed by code reading and by
reproducing the exact 2-item-batch rejection against a live `antfly
inference run` server with no hang or elevated CPU afterward. But
`error.InferenceInvocationMemoryExceeded` -- a deterministic,
request-byte-size-based pre-flight estimate in
`asset_producer_runtime.zig`/`asset_producer.zig`, raised **before** the
GLiNER executor is ever invoked, unrelated to `window_batch_size` -- is
**not** in that disposition table and defaults to `.retryable_request`; the
log shows exactly 5 wasted retries before a terminal give-up. This fix (and
the still-open CPU-spin root cause, which the standalone HTTP server could
not reproduce and needs an embedded-worker repro with `sample`/`lldb`) both
live in `zig/pkg/antfly/**`, outside this file's ownership -- documented in
`gliner25-retry-and-spin-handoff.md` (scratchpad).

**Verification.** `zig build inference-test -Doptimize=ReleaseFast --
--test-filter "gliner boundary"` (native + Metal; `160` selected, `138`
passed, `22` skipped, `0` failed) and `--test-filter "extraction"` (`100`
selected, `99` passed, `1` skipped, `0` failed) after every change in this
section. A live `antfly inference run --port 8098` server (same budget
flags as section 9) served the real 99,631-byte corpus-maximum request
(`zig/PDF.md`'s "Review findings and required fixes") end to end: HTTP 200,
279 entities and 13 relations, ~6-7s, both before and after a full
`zig build -Doptimize=ReleaseFast` rebuild with every change in this
section applied.

### 12. Follow-up: fp16-encoder qualification attempt -- real-pipeline parity measured, does not yet clear the bar

Section 10 left `fp16_encoder` as "investigated, not yet formally qualified,"
noting the informal `gliner25-bundle-check` comparator (`qualification:false`
in its own output) found ~1e-4 differences on a handful of sampled confidence
scores. This section does the qualification properly -- deterministic
artifact production with recorded digests, full canonical-fixture parity
through the real session/executor path (not bundle-check's raw diagnostic
dump), and a Metal-vs-native throughput comparison -- and the honest result
is that **it does not clear the bar this file holds fp32 to**, so no
production row is added.

**Deterministic artifact production.** `antfly-inference-gliner25-convert
--model-dir ~/.antfly/inference/models/fastino/gliner2.5-base-v1
--output-dir <dir> --precision fp16_encoder` was run twice into independent
output directories. Both runs produced byte-identical `model.gguf` files
(confirmed with `cmp` and independent `shasum -a 256`, not just the tool's
own receipt):

| File | Size (bytes) | SHA-256 |
| --- | ---: | --- |
| `model.gguf` (fp16_encoder, converted) | 407,861,568 | `1dce97cb1727e3b4e4c8242e88b46ad5f8f31801c2c9d24919393a8816a92d11` |
| `config.json`, `encoder_config/config.json`, `tokenizer.json`, `tokenizer_config.json` | unchanged | identical to the fp32 digests in section 1 (copied verbatim by the converter; `gliner_boundary_bundle.validate` enforces this) |

The converted bundle's own receipt (`antfly_inference_bundle.json`) records
`precision:"fp16_encoder"` and `source_files` pointing at the exact pinned
fp32 `model.safetensors` digest from section 1 -- so the bundle's own
metadata proves what it was converted from, independent of this document.
`antfly-inference-gliner25-convert --verify-dir <dir>` and
`session_factory.createNativeSession`/`createMetalSession` both load it
without error (334 tensors, ~193.6M parameters, ~407.8MB stored).

**Real-pipeline parity (not bundle-check).** Two new tests were added --
`"gliner boundary pipeline Python parity converted fp16 encoder base
checkpoint all inference tasks native"` and `"...metal"`
(`pipelines/gliner_boundary_pipeline.zig`) -- gated on
`ANTFLY_GLINER25_BASE_FP16_MODEL_DIR`. Unlike `gliner25-bundle-check` (which
only emits results for an external diff), these open the converted bundle
through the exact same `session_factory.createNativeSession`/
`createMetalSession` + `getManagedComputeBackend` + `encodeNative`/
`runNative` (native) or `gliner_boundary_request_device.run` (Metal) path
production uses, verify every sidecar byte-for-byte against the same pinned
fp32 fixture digests (proving the converter didn't touch them), verify the
bundle's own receipt records the exact pinned fp32 `model.safetensors` as
its source, and then assert each of the ten canonical fixtures with
`expectSample` -- the identical comparator the fp32 pinned tests use.

That comparator's confidence check already carried a tolerance
(`expectApproxEqAbs`, previously a bare `5e-4` literal at every call site);
this pass threaded it into a named, single-sourced constant
(`fp32_confidence_tolerance = 5e-4`) passed explicitly by every fp32 pinned
test (`publishedCheckpointParity`, the Metal full-pipeline parity test in
`gliner_boundary_scorer_device_test.zig`, and the long-executor canonical
test in `gliner_boundary_long_executor.zig` -- three call sites, none of
which had their behavior changed) so a future precision-specific tolerance
can never silently loosen the fp32 evidence.

**Exhaustive comparison, not sampled.** Running the two new tests directly
surfaced a failure the sampled bundle-check comparison missed: one attribute
label confidence in the `entity_attributes` fixture, off by 5.9e-4, just
outside the fp32 rows' 5e-4 bound. To see the complete picture rather than
stopping at the first failing assertion, `gliner25-bundle-check` was run
against the converted bundle on both backends and every leaf confidence
value in all ten fixtures' output was diffed against `pipeline_cases_base.
json`'s `expected` fixtures (62 comparable confidence values per backend,
after excluding record-instance-level confidence that `expectSample` itself
never checks):

| Backend | Values compared | Max abs diff | Count over 5e-4 |
| --- | ---: | ---: | ---: |
| native | 62 | 0.0005911 (`entity_attributes`, idx 1) | 1 |
| metal | 62 | 0.0005909 (`entity_attributes`, idx 1) | 1 |

The next-largest diffs on both backends are 3.1e-4 (`entity_attributes`,
another attribute), 2.3e-4 and 2.2e-4 (`record_latent`), 1.9e-4
(`record_natural`) -- comfortably inside tolerance. The one violation is the
**same fixture, same value index, same ~5.9e-4 magnitude on both backends**
(0.5588979 native / 0.5588977 metal vs. 0.5583068 expected) -- a real,
deterministic effect of narrowing the encoder to fp16, not measurement noise
or a backend-specific bug (native and Metal compute this with different
kernels but land on the same answer to 6 decimal places, because both start
from the same fp16-rounded weights).

**Decision: not qualified.** This document's own instructions for
qualifying a new artifact (see below) and this task's brief both frame the
bar as "matches the tolerance the existing rows use." 61 of 62 checked
values do; one doesn't, by 18% of the bound, reproducibly, on both backends.
That is a genuine (if narrow) miss, not a coin flip that a rerun would
clear, so **no `fp16_encoder` row was added to `production_entries`** in
`models/gliner_boundary_qualification.zig`. Widening the tolerance
specifically for a reduced-precision row is a real option (5.9e-4 has
essentially no operational significance for ranking/thresholding at typical
`threshold` settings), but it is a policy call about what "qualified" means
for a lower-precision artifact -- exactly the kind of reviewed release
decision this module's own design note says a row must never get by
inference from a successful test run. This document instead records a
`fp16_encoder_confidence_tolerance = 7.5e-4` constant (comment: measured
max 5.909e-4-5.911e-4, with headroom) used **only** by the two new
diagnostic tests above, so they stay a real regression guard (a converter
change that measurably regresses further still fails them) without
misrepresenting the artifact as meeting the fp32 bar. If a future reviewer
decides 7.5e-4 (or some other explicit, documented number) is an acceptable
bar for a `fp16_encoder` row specifically, adding the row is then a matter
of copying `fastino_gliner25_base_v1`'s identity in
`models/gliner_boundary_qualification.zig` with `precision = .fp16_encoder`,
the weight digest above, the same sidecar digests, the same
`fastino_gliner25_base_v1_features`/`_lengths` (geometry and tokenizer are
unaffected by encoder weight precision, so the single-window
`LengthContract` measured in section 3 applies unchanged), and updating this
table -- everything needed to do that is already measured and recorded
above; a long-document row would additionally need its own geometry/
correctness pass through the long executor, which was not attempted here.

**Throughput (informal, single-window, `gliner25-bundle-check`, same machine
running other concurrent builds -- treat as directional, not a clean
benchmark).** Repeated single-process runs of the ten canonical fixtures,
warm session, comparing a converted **fp32** bundle (via
`gliner25-convert --precision fp32`, so the comparison is bundle-vs-bundle,
not safetensors-vs-GGUF) against the fp16_encoder bundle:

| Backend | fp32 wall (3 runs) | fp16_encoder wall (3 runs) |
| --- | --- | --- |
| native | 1.04-2.87s | 1.89-4.59s (**slower**) |
| metal | 3.69-4.89s | 2.66-2.99s (**faster**) |

Native got *slower* with fp16 weights across every repetition, consistent
with the native/BLAS compute path dequantizing F16 rows to f32 before each
matmul (extra CPU work with no compensating bandwidth win on a path that
was never memory-bandwidth-bound); Metal got *faster* in 2 of 3 reps,
consistent with section 10's expectation that a smaller weight footprint
helps a bandwidth-bound GPU dispatch. Given the unqualified status above,
this is supporting color for a future reviewer's decision, not a throughput
qualification -- and it specifically argues *against* making fp16_encoder
the default on native regardless of what happens with the tolerance
question.

**`antfly inference pull` is unchanged, on purpose.** Because `fp16_encoder`
is not qualified, `pull`'s existing pull-time gate
(`registry.zig`'s `boundaryIdentityIsQualified`) correctly continues to
advertise only the fp32 identity's tasks/capabilities; it was not modified
to special-case a converted artifact it cannot know is a reviewed row.
**To produce and experiment with the bundle today** (informally -- request-time
`require()` will correctly refuse to execute against it, since no
production row exists):

```sh
antfly-inference-gliner25-convert \
  --model-dir ~/.antfly/inference/models/fastino/gliner2.5-base-v1 \
  --output-dir ~/.antfly/inference/models/fastino/gliner2.5-base-v1-fp16 \
  --precision fp16_encoder
```

places a normal model directory under the standard `owner/name` layout, so
it is discoverable by name (`fastino/gliner2.5-base-v1-fp16`) the same way
any pulled model is -- but pull-time synthesis never ran for it, so it has
no `model_manifest.json` and will not list a supported task. If a future
reviewer qualifies it, the production row is what actually authorizes
execution (the two-tier gate above); until then, deliberately do not
hand-author a `model_manifest.json` claiming `"tasks":["extract"]` for this
directory to make it runnable -- `require()` in
`gliner_boundary_qualification.zig` would still correctly refuse it with
`error.UnsupportedGlinerBoundaryRuntime` at request time regardless (its
identity matches no row), so doing so would only be misleading, not
functional. There is currently no way to select this precision through the
extractor producer config that actually executes, because it is not
qualified -- this is intentional, not a missing feature.

**Verification.** `zig build inference-test -Doptimize=ReleaseFast --
--test-filter "gliner boundary"` with `ANTFLY_GLINER25_BASE_MODEL_DIR` and
`ANTFLY_GLINER25_BASE_FP16_MODEL_DIR` both set: first run 162 selected, 139
passed, 22 skipped, 1 failed (`ResourceTemporarilyUnavailable` from Metal
live-memory admission on a long-document Metal test unrelated to this
section's changes, on a machine also running other concurrent Metal-using
processes); after the unrelated `ModelType.recognizer` -> `.extractor`
terminology rename applied across `zig/pkg/inference` (registry, manifest,
capabilities, session/model-manager, server listing, and the
`extractors/extractor.zig` executor -- see that rename's own commit for
detail; briefly, `model_manifest.json`'s `"type"` field and every internal
enum/identifier now say `extractor`, with `"recognizer"` still accepted and
normalized on load for previously-written manifests) and a full rebuild, a
clean re-run gave 163 selected, 140 passed, 23 skipped, 0 failed. The two new converted-bundle tests passed on both backends every
run. `--test-filter "extraction"` also passed unchanged (100/100). Log
paths (scratchpad, not committed): `fp16-convert-a.json`,
`fp16-convert-b.json`, `fp16-native-only.log`, `fp16-both-test1.log`,
`fp16-full-boundary-test.log`, `retest-windowed-metal.log`,
`fp16-bundlecheck-native.jsonl`, `fp16-bundlecheck-metal.jsonl`,
`diff_fp16.py`, `final-boundary-after-rename.log`,
`final-extraction-after-rename.log`.

### 13. Follow-up: closing the in-process-vs-HTTP throughput gap -- measured equal, root cause is elsewhere

Section 10 estimated ~4.5 sections/s for a live `antfly inference run` HTTP
server on a corpus-shaped mix of window sizes, versus a real
`examples/dogfood ingest` run that measured ~1.8 sections/s
(`extract_items=1231`, `extract_ns=678364084000` -- see
`dogfood-followup-final3.log`, scratchpad) through the in-process embedded
worker's provider path. This section measures both precisely on the
identical 40-section set (a reproducible corpus sample: 32 sections of
1-8KB and 8 of 20-40KB, drawn from `zig/*.md` and `work-log/**/*.md` by
Markdown heading, saved as `throughput-corpus.json`, scratchpad) to find out
whether the gap is in `zig/pkg/inference`'s extraction call path.

**Method.** (a) A live `antfly inference run --port 8098` server (same
budget flags as section 7), fed the 40 sections sequentially (one request in
flight, matching section 10's "direct" baseline) with the real dogfood
schema and `long_document.mode=window`, timed end to end including HTTP and
JSON overhead. (b) `server/gliner_boundary_service_test.zig`'s
`"gliner boundary provider extractDirect throughput on a real corpus
matches a live HTTP baseline"` test (gated on
`ANTFLY_GLINER25_THROUGHPUT_CORPUS`; already present in this tree),
which builds a warm `Node` directly (`process_termination_available = true`,
the same generous budget flags, one warm-up call, then sequential
`Node.extractDirectWithControl` calls) -- the same entry point
(`Node.extractDirect`/`extractDirectWithControl`) the in-process embedded
worker's "extract" provider operation calls (see section 8) -- with no HTTP,
no JSON transport, and critically **no concurrent embedding work**, unlike
the real dogfood ingest which runs the Qwen3 embedder and the extractor
concurrently on the same single Metal device.

**Result: (a) and (b) are the same, within noise.**

| | Wall time (40 sections) | sections/s |
| --- | ---: | ---: |
| (a) HTTP (`antfly inference run --port 8098`) | 14.841 s | 2.695 |
| (b) `Node.extractDirect` (direct, in-process) | 14.698 s | 2.721 |

Per-section latencies match within a few percent across the whole range (a
610-byte-class request: 83 ms HTTP / 70 ms direct; a 40,008-byte request:
1815 ms HTTP / 1794 ms direct; the corpus max in this sample, 37,142 bytes:
2192 ms HTTP / 2170 ms direct), with HTTP consistently a hair higher --
consistent with JSON/transport overhead being real but small, not a
multi-x gap. Both runs completed all 40 sections with zero errors.

Per this task's own instructions: **(b) equals (a)**, so the remaining gap
between this measurement (~2.7 sections/s on a sample deliberately
weighted toward large sections for statistical power -- 20% of this sample
is 20-40KB, versus the real corpus's actual tail, p99 = 23,428 bytes,
i.e. large sections are closer to 1% of the real corpus) and section 10's
~4.5 sections/s corpus-weighted estimate, and separately the much larger
gap down to the real ingest's measured 1.8 sections/s, is **not** caused by
any inefficiency in `Node.extractDirect`/the boundary executor itself --
there is no missing session reuse, no missing warm executor cache
(`isGlinerBoundaryResidentReady`/`.optimized_v2` is keyed off the shared
`model_manager` session cache, not the caller, so both paths get the same
resident-weight/async-submission optimization once the session is warm; see
`server.zig`'s `extractV2InMemory`), and no extra per-call tokenizer/schema
re-encoding specific to the direct path -- both call the identical
`extractV2WithAdmission` -> `extractV2Observed` -> `extractV2InMemory`
machinery, differing only in `admission_owner` (`.direct` vs `.http_route`)
and JSON-vs-typed input, neither of which this measurement shows costs
anything material.

**Where the real corpus's gap likely comes from instead (read-only
findings, outside this file's ownership to fix):**

- **GPU contention with concurrent embedding.** The real ingest's
  `extract_ns=678s` and `embed_ns=293s` sum to more than the run's
  `wall_ms=846s`, meaning embed and extract work overlap and compete for
  this machine's one Metal device; a request's wall-clock latency (which is
  what both (a) and (b) above measure) includes any time spent queued
  behind another session's GPU dispatch. Section 10's own throughput
  numbers, and this section's, only ever measured extraction in isolation.
  This is an admission/scheduling property of how much embed/extract
  concurrency `zig/pkg/antfly`'s enrichment runtime allows, not something
  `zig/pkg/inference`'s Node/executor controls.
- **The embedded worker's out-of-process hop, on Metal.** Reading (not
  editing) `zig/pkg/antfly/src/standalone/inference_host.zig`:
  `linkedInferenceCreateLocal` sets `use_worker = ... and
  inference.backends.BackendRuntime.availableRequiresProcessIsolation()`,
  and when true, `linkedInferenceInvokeProvider` routes every provider call
  (including "extract") through `worker_runtime.invokeProvider` to a
  **separate `antfly` worker process**, rather than calling
  `state.node.extractDirect` in the same process -- because the main
  standalone process runs with `process_termination_available = false` (it
  hosts the DB and cannot be killed/restarted the way a dedicated worker
  can) and Metal requires that capability to safely reload a session. This
  worktree's `examples/dogfood` run sets `ANTFLY_INFERENCE_WORKER=zig-out/
  bin/antfly` for exactly this reason. This benchmark's (b) measurement,
  and the pre-existing `gliner_boundary_service_test.zig` test it uses,
  intentionally construct a `Node` directly and never cross that IPC
  boundary (it lives in `zig/pkg/antfly`, outside this file's ownership) --
  so if the worker hop itself adds meaningful per-call latency, it would
  show up as a further gap beyond what (a)/(b) capture here, not within
  them.

Both explanations point at `zig/pkg/antfly`'s enrichment-runtime/embedded-
worker layer, not `zig/pkg/inference`. No code change was made here for
this section: the measurement shows there is nothing to fix on this side of
the boundary.

**Verification.** `antfly inference run --port 8098` was stopped after the
HTTP measurement (`pkill`). The provider-path test:
`ANTFLY_GLINER25_THROUGHPUT_CORPUS=<path> zig build inference-test
-Doptimize=ReleaseFast -- --test-filter "provider extractDirect throughput
on a real corpus"` -- 1 selected, 1 passed, 0 skipped, 0 failed. Log paths
(scratchpad): `http_throughput.py`, `http-throughput-out.log`,
`http-throughput-err.log`, `make_sections.py`, `bench40.json`,
`throughput-corpus.json`, `provider-throughput-test.log`.

## How to re-qualify a different or wider artifact

1. Pull the artifact and verify its digests against
   `scripts/gliner25/oracle_manifest.json` (extend that manifest first if it
   is a new revision or variant `oracle.py` does not yet know about).
2. Set `ANTFLY_GLINER25_<VARIANT>_MODEL_DIR` to the pulled directory and run
   `zig build inference-test -Doptimize=ReleaseFast -- --test-filter "gliner
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
