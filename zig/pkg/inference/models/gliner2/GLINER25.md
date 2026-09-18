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
| `fastino/gliner2.5-base-v1` (rev `72ac19b486cd4557424c8d61114e7530c243e9b0`) | base | fp32 (safetensors) | native | Qualified |
| `fastino/gliner2.5-base-v1` (rev `72ac19b486cd4557424c8d61114e7530c243e9b0`) | base | fp32 (safetensors) | metal | Qualified |
| `fastino/gliner2.5-small-v1` | small | any | any | Not reviewed |
| `fastino/gliner2.5-multi-v1` | multi | any | any | Not reviewed |
| Any other digest, revision, or precision of `gliner2.5-base-v1` | base | any | any | Not reviewed |

`gliner_boundary.runtime_available` is now `true`, and
`gliner_boundary_qualification.zig`'s production table carries exactly the
two rows above (same identity, native and Metal). Everything else --
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
