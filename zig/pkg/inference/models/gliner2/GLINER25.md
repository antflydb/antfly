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
| `fastino/gliner2.5-base-v1` (rev `72ac19b486cd4557424c8d61114e7530c243e9b0`) | base | fp32 (safetensors) | native | Qualified (long-document windowing, up to 99,008 document bytes -- section 9) |
| `fastino/gliner2.5-base-v1` (rev `72ac19b486cd4557424c8d61114e7530c243e9b0`) | base | fp32 (safetensors) | metal | Qualified (long-document windowing, up to 99,008 document bytes -- section 9) |
| `fastino/gliner2.5-small-v1` | small | any | any | Not reviewed |
| `fastino/gliner2.5-multi-v1` | multi | any | any | Not reviewed |
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
