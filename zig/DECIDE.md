# Decision API design

Status: implemented in this worktree, based on `origin/main` at
`f44ef220b7` (2026-09-29). The sections below retain the design rationale.

The query DSL/SQL integration below is implemented on `design/decision-functions`. See [FUNCTIONS.md](FUNCTIONS.md) for the shared expression
model, evaluation scope, and execution semantics.

## Query integration (2026-10-02)

Expose `ai_decide` consistently as a built-in in SQL and the JSON DSL. It accepts
text state, named questions, and a configured decider and returns structured
answers. Convenience built-ins expose one question as an ordinary scalar:

| Built-in | Result | Mapping to existing contract |
| --- | --- | --- |
| `ai_decide` | Structured named answers | `answers` with choice/score/noul shapes |
| `ai_choice` | Selected option ID | Highest-probability choice |
| `ai_score` | Expected ordinal score | Sum of zero-based level index times probability |
| `ai_probability` | Probability a statement is true | `noul` |

Keep `/decide`, `/ai/v1/decide`, and the existing wire vocabulary unchanged.
`ai_score` is not an arbitrary numeric rating. Probabilities are not assumed
calibrated or comparable across models merely because they sum to one.

Functions produce values consumed by computed predicates, projection, ordering,
and aggregation. A decision is not itself a full-text retrieval primitive or a
new graph node type. Named `compute` bindings retain results for multiple
consumers; `where` compares those values. An explicit evaluation scope separates
bounded, globally merged search candidates from all qualifying rows. Refer to
[FUNCTIONS.md](FUNCTIONS.md) for JSON and SQL examples.

### Decision providers

Follow the capability/configuration boundary in `lib/reranking/src/mod.zig`,
using `DecisionProvider` and `DeciderConfig`. A query references a configured
instance with `decider: "support-decider"`; its configuration identifies the
implementation with `provider: "antfly"`. Named configurations centralize
credentials, endpoint policy, rate limits, and model settings.

The implementation supports `antfly` (Antfly inference), `jev`, and `openai`
(OpenAI Decisions). OpenAI uses the published `/v1/decisions` contract and
generated wire types from the vendored official OpenAPI spec. Its adapter maps
Boolean `noul` questions to predicates and preserves Antfly’s choice and expected
ordinal score semantics. Refusals fail evaluation through `InvalidDecisionOutput`.
See [FUNCTIONS.md](FUNCTIONS.md) for configuration and endpoint defaults.

```text
DecisionProvider:
  capabilities(config)
  validate(specification)
  evaluateBatch(context, specification, inputs)
    -> ordered results + usage + provenance
```

Capabilities include question types, full-distribution support, input/question/
option limits, batch size, concurrency limits, usage reporting, determinism,
resolved model/version, and probability provenance. Bind and validate before
retrieval. Batch responses preserve row alignment and expose item failures;
errors fail the query unless an explicit alternative policy is selected.

Start with `antfly`. The current endpoint accepts one state, so an HTTP adapter
initially uses bounded concurrent requests. Add a multi-input inference contract
later for actual server-side batching. Embedded consumers should reuse direct
inference execution rather than loop back through HTTP. Both routes share
validation, resource admission, deadlines, and cancellation.

Implement the Jev adapter alongside Antfly, validating its contract against the
capability model. A typed enum or numeric answer alone does not establish a
probability distribution. Distinguish selected-value support from complete
probabilities and reject probability-dependent operations when unsupported.
Do not synthesize one-hot distributions or treat an LLM's self-reported
confidence as equivalent to classifier probability. Preserve the expected
ordinal meaning of score across adapters. Provider fallback is opt-in and
records the provider/model that actually answered.

### Execution, analytics, and stored decisions

Use `DecisionEval` as a batch operator that supplies columns to the ordinary
scalar evaluator. Avoid per-row network calls inside scalar evaluation. Apply
authorization and ordinary independent filters before inference. Candidate
evaluation follows global merging and precedes decision filtering/final limit;
SQL full-match evaluation streams qualifying rows; DSL matches evaluation
requires the complete relation to fit its explicit row and memory budgets. Budget exhaustion must not
silently convert analytics into a sample.

Return provider/model identity, usage, scope, and evaluation statistics. Define
NULL propagation, error policy, conditional evaluation, cache reuse, and
pagination consistently across SQL, JSON, and graph consumers, as specified in
[FUNCTIONS.md](FUNCTIONS.md).

Materialize decision outputs with the versioned decision asset producer for
indexed consumption and repeated analytics. Store source revision, specification hash, model identity,
and provenance. Refresh/invalidate explicitly as inputs or model specifications
change. Graph integration begins with expressions over completed matches;
traversal-time inference requires a separate frontier/work-budget contract.

### Query integration sequence

1. Shared function descriptors and expression binding.
2. Antfly, Jev, and OpenAI provider adapters and bounded `DecisionEval` execution.
3. Candidate-stage JSON DSL and SQL projection/filtering.
4. Full-match analytics using the same Antfly, Jev, and OpenAI adapters.
5. Graph-match expressions and versioned materialized enrichment.

Validate global candidate windows, row/batch alignment, reuse, NULLs, SQL Boolean
semantics, authorization, cancellation, budget failures, provider capabilities,
and provenance before advertising these query surfaces. See FUNCTIONS.md for concrete supported combinations and validation commands.

## Implementation notes

- `/ai/v1/decide` accepts the request shown below and returns all option
  probabilities for choice and score, or the true probability for noul.
  `usage.input_tokens` is the executor's encoded prompt token count, including
  repeated text when GLiNER splits tasks across sequences; `output_tokens` is
  zero for these classifier executors.
- Laya and the named `fastino/GLiNER2.5-Decide` span checkpoint reuse the
  extraction v2 admission, cancellation, model lock, token limits, and
  execution paths. The GLiNER adapter requests every option with `top_k` and
  `include_confidence`, then reconstructs the distribution from the existing
  flat classification response. No model weights are loaded for malformed
  decision requests.
- Registry pull synthesis adds `decide` and `typed_decisions` to the named
  GLiNER span model. It adds `decide` to Laya manifests that declare
  `typed_decisions`. Previously installed manifests need to be refreshed by
  pulling the model again to advertise the new task. Model listing and direct
  execution also require a valid Laya config or a declared GLiNER span
  classifier, while proxy routing requires the advertised task and capability.
- The source OpenAPI contract and public joined document, generated Zig, Go,
  TypeScript, and Python clients, and the Rust build-time generated client now
  include the route. The Go and TypeScript SDKs expose convenience methods.
- Verification in this worktree covers decision validation/presentation,
  inference server compilation, Go proxy/operator/SDK tests, TypeScript
  typechecking, Python generation consistency, and Rust SDK compilation.
  End-to-end numerical comparison on a loaded Laya or GLiNER checkpoint
  remains a deployment qualification step because those artifacts are not
  present in this workspace.

## Goal

Expose typed decisions at `POST /ai/v1/decide`. A caller supplies one state and
named questions; a model returns a choice distribution, an ordered score, or a
Boolean probability. A model artifact may serve both `/extract` and `/decide`.
The new API is a stable decision contract over the existing inference engines,
not a new model family or a replacement for extraction.

## What exists on this branch

- `/ai/v1/extract` already supports version 2 classification schemas and has a
  `decisions` response field (`specs/openapi/ai/extraction.yaml`). Its general
  response is organized around `inputs`, `schema`, and extracted `data`.
- Laya already runs choice, ordinal score, and Boolean questions through the
  extraction v2 path. `src/extractors/laya.zig` parses its model-specific
  classification schema, and `src/pipelines/laya.zig` computes distributions,
  expected values, and true probabilities. Laya declares `typed_decisions`.
  Its current response also contains classification projections and Laya-only
  fields such as `act_probability`.
- `fastino/GLiNER2.5-Decide` already runs classification through
  `src/extractors/gliner_span_v2_executor.zig`. Despite its name it is a
  **span** checkpoint, not the qualified GLiNER2.5 **boundary** checkpoint.
  The span executor has reference fixtures for model-facing tokens, logits,
  labels, descriptions, prompts, and multiple tasks. See
  `pkg/inference/models/gliner2/GLINER25_DECIDE.md`. Its current public
  presentation selects classification labels; `/decide` also needs every
  option's probability and explicit score/Boolean semantics.
- The boundary `fastino/gliner2.5-base-v1` checkpoint is separately qualified
  for `/extract` with exact artifact and request-time gates
  (`pkg/inference/models/gliner2/GLINER25.md`). That qualification must not
  automatically grant `decide` to this or a later checkpoint.
- Model discovery, executor descriptors, proxy routing, and generated clients
  currently use `extract` as the public task for classification. A standalone
  `decide` task is absent.

## Proposed wire contract

Keep the question and answer vocabulary close to Ollaya's documented
`/api/decide` and `/v1/systemone` formats, while versioning it as Antfly's
`/ai/v1/decide`. The first version accepts one UTF-8 text state and a nonempty
map of questions. It does not implicitly download a model or stream tokens.
The number of questions, state length, labels, and encoded schema bytes must
be bounded by both a route ceiling and the selected executor's lower limits.

```json
{
  "model": "fastino/GLiNER2.5-Decide",
  "state": "The customer was charged twice and wants a refund today.",
  "questions": {
    "intent": {
      "type": "choice",
      "instructions": "What does the customer want?",
      "criteria": {
        "refund": "Wants money back",
        "other": "Something else"
      }
    },
    "urgency": {
      "type": "score",
      "instructions": "How soon does this need attention?",
      "criteria": ["Can wait", "Soon", "Today"]
    },
    "asks_for_refund": {
      "type": "noul",
      "instructions": "The customer asks for a refund."
    }
  }
}
```

`state` is text in v1. Object or array states need an explicit canonical
rendering rule before they can be added: each model otherwise sees different
bytes for the same JSON value. Require `instructions` initially because the
existing Laya adapter requires a nonempty prompt. `choice.criteria` maps
stable option IDs to model-facing descriptions. `score.criteria` is ordered
from zero. `noul` maps internally to `false`, `true` in that order. Validate
question IDs and option IDs for uniqueness and UTF-8; reject unsupported
types, empty criteria, and excess limits before loading weights.

```json
{
  "model": "fastino/GLiNER2.5-Decide",
  "answers": {
    "intent": {
      "type": "choice",
      "choice": "refund",
      "probabilities": {"refund": 0.91, "other": 0.09}
    },
    "urgency": {
      "type": "score",
      "score": 1.74,
      "legend": {"0": "Can wait", "1": "Soon", "2": "Today"},
      "probabilities": {"0": 0.03, "1": 0.20, "2": 0.77}
    },
    "asks_for_refund": {"type": "noul", "noul": 0.88}
  },
  "usage": {"input_tokens": 87, "output_tokens": 0}
}
```

The numbers above illustrate the shape, not a model result. Choice returns
the highest-probability option and **all** option probabilities. Score is
`sum(level_index * probability)` and may be fractional. Noul is the
probability assigned to true. Keep probabilities in request option order when
serialized, even though JSON objects are logically unordered. The returned
`model` should identify the checkpoint that answered if a router selects one.
Do not call the probabilities calibrated merely because they sum to one.

Do not require an Ollaya-compatible `confidence` field in v1. The existing
Laya response uses normalized inverse entropy for choice/score and a
different method for Boolean answers; the extraction schema explicitly notes
that confidence is not a probability of correctness. If added, specify its
formula and method in the wire contract, and verify it for each executor.
Keep Laya's `act_probability` on `/extract` for now: a decision response does
not authorize an action.

## Execution design

Use a small shared internal representation: normalized state text, ordered
questions, ordered option IDs/descriptions, and per-question distributions.
Adapters validate model-specific support and return distributions. A single
response presenter then derives `choice`, `score`, and `noul`. This avoids
implementing two subtly different formulas in the Laya and GLiNER paths.

1. **Laya adapter:** reuse its parser's validation rules and
   `pipelines/laya.zig` execution, but return the internal decisions before
   `extractors/laya.zig` writes the extraction envelope. Keep the existing
   admission, cancellation, model lock, and executor limits. One API request
   need not imply one encoder forward; packed and split execution remain
   internal.
2. **GLiNER2.5-Decide adapter:** map question names to classification task
   names, descriptions to `label_definitions`, instructions to `prompt`, and
   options to labels. Reuse the span v2 compiler and encoder/classifier path.
   Refactor the presentation boundary to expose every label logit or
   probability before `top_k` and threshold remove candidates. Normalize a
   choice/score over its options; map noul to a two-label task. Validate those
   transforms against upstream reference fixtures, especially descriptions,
   prompts, temperature, and near ties. Reuse the existing task splitting
   rule when the shared text and schema exceed one encoded sequence. Do not
   silently truncate the state.
3. **Other multitask models:** opt in through the same decision executor
   interface only after they produce the required distributions. A generic
   classifier's selected label and score are insufficient evidence that it
   supports `/decide`.

The first release should support `choice`, `score`, and `noul` as a complete
capability. A later partial-capability catalog can expose individual question
types if there is a concrete need. Avoid advertising `typed_decisions` for
every model that merely declares `classification`.

## Capability and routing changes

- Make `decide` a public task and `typed_decisions` its serving capability.
  Laya already has the capability string; give its qualified/imported
  manifest the `decide` task as well as `extract`. Add `decide` to the
  GLiNER2.5-Decide manifest only after its decision adapter passes parity.
  A multitask artifact retains one model identity, one storage location, and
  one loaded session. No second model type is required.
- Add a `deciders` view to `/ai/v1/models`, alongside `extractors`, and list a
  multitask model in both. Membership must require the task, declared
  capability, a registered executor, and any artifact qualification gate.
  The current `taskMatchesModelListing` extraction shortcut must not leak an
  unqualified `decide` listing. Publish effective executor limits with the
  task, not just manifest aspirations.
- Add `decide` to normalized task names, executor descriptors, admission,
  result cardinality, metrics, readiness counts, preload task handling, and
  model-path resolution. Resolve an existing extractor-owned artifact by
  model ID; do not copy it into a `deciders` directory solely for listing.
  If a decision-only artifact later uses that directory, registry and path
  inference need an explicit rule for it.
- Add the route and operation to the Go inference proxy's mux, catalog
  operation map, request routing, and status/response handling. A proxy must
  route by `decide` support, not by the presence of `extract` or
  `classification`. Update the operator's task and warm-load mapping so a
  multitask checkpoint is loaded once.
- Define `DecideRequest` and `DecideResponse` in
  `specs/openapi/inference/api.yaml` or a shared AI schema referenced there.
  Regenerate the public OpenAPI document, Zig generated router/types, and
  Go, TypeScript, Python, and Rust SDK surfaces. Add a thin convenience method
  where the handwritten clients have one for extraction.
- If in-process Antfly consumers need decisions, add a direct `Node`/provider
  operation with the same parser and execution path. Do not route embedded
  inference back through HTTP merely to reuse the new endpoint.

## Implementation sequence and evidence

| Stage | Deliverable | Required evidence |
| --- | --- | --- |
| 1. Contract | OpenAPI shapes, bounds, examples, error codes, generated route/types | Schema generation and request/response serialization checks |
| 2. Laya | `/decide` backed by the existing Laya pipeline | Native/Metal end-to-end parity with `/extract` decision values; invalid and over-limit requests fail before model work |
| 3. Catalog and proxy | `deciders`, `decide` operation, one artifact advertised for both tasks | Discovery, proxy selection, no false advertising, and preload tests |
| 4. GLiNER2.5-Decide | Span v2 adapter exposing all option probabilities | Upstream logit/probability parity on pinned fixtures, all three answer types, multi-question ordering, task splitting, native/Metal and reviewed quantized variant |
| 5. Clients and integration | SDK methods and any embedded provider operation | Generated-client smoke tests and HTTP/proxy/direct response equivalence |

Test failures should distinguish malformed questions (400), unsupported
model/question capability (400), missing model (404), and admitted request
limits (413). Follow the existing extraction v2 cancellation, output-size,
resource-admission, and no-implicit-download behavior. Retain `/extract` as a
supported API; it may continue returning decision details to existing
clients. The new endpoint should be implemented over shared execution code
where possible so the two paths cannot drift numerically.

## V1 contract choices

1. V1 uses the `choice`/`score`/`noul` answer shapes without claiming full
   Ollaya TypeSafe SDK wire compatibility.
2. `usage.input_tokens` reports the executor's encoded prompt token count.
   GLiNER task splitting repeats the state and counts each encoded sequence.
3. V1 exposes no threshold, top-k, constraints, or action policy. Those remain
   extraction features until a portable decision meaning is defined.

## References

- Local: `zig/EXTRACT.md`, `pkg/inference/models/laya/LAYA.md`,
  `pkg/inference/models/gliner2/GLINER25_DECIDE.md`,
  `pkg/inference/models/gliner2/GLINER25.md`.
- External: [Ollaya API](https://ollaya.dev/docs/api),
  [Ollaya TypeSafe compatibility](https://ollaya.dev/docs/typesafe-compatibility),
  [Fastino GLiNER2](https://github.com/fastino-ai/GLiNER2).
