# Decisions

Standalone decisions use `POST /decisions` (`/ai/v1/decisions` on the standalone
server). Named question and answer arrays, `input`, `choices`, `levels` and
predicate `probability` follow the [OpenAI Decisions conventions](https://developers.openai.com/api/docs/guides/decisions).
Antfly adds embedding similarity answers, multi-choice selection and text
batches. Supported hosted providers also accept multimodal evidence; see the
[gateway decision contract](inference-gateway.md#public-contracts)
for provider and method restrictions. Native trained/embedding decisions and the
browser surface retain their text restrictions. Choice values are strings and
every question needs a unique name. See the [decision schema](../../specs/openapi/ai/decision.yaml).

`/extract` retains entities, relations, attributes, structured records and
ordinary classification. Standalone Laya, OpenDecider and EmbeddingGemma
workflows use `/decisions`. The unreleased extraction decision output and
Boolean and ordinal decision modes have been removed.

For the v0.3.0 development source documented here, standalone public inference
requires an explicit opt-in before calling `/ai/v1`. Save `antfly.json`:

```json
{"public_inference": {"enabled": true}}
```

```bash
antfly standalone --config antfly.json
```

There is no enablement CLI flag. Without this setting, public execution returns
HTTP 403 even when models are installed. See the
[public execution policy](inference-gateway.md#public-execution-policy).
This prerequisite applies to database API nodes on this source version; the
independent `antfly inference run` listener and older stable releases have their
own policies.

## Questions and answers

```json
{
  "model": "decision-model",
  "input": "Please refund the duplicate charge before tomorrow.",
  "questions": [
    {"name": "route", "type": "choice", "instructions": "Which team?", "choices": [{"value": "billing", "description": "Charges and refunds"}, {"value": "support", "description": "Product troubleshooting"}]},
    {"name": "urgency", "type": "score", "instructions": "How urgent?", "levels": [{"label": "routine"}, {"label": "soon"}, {"label": "immediate"}]},
    {"name": "refund", "type": "predicate", "instructions": "The input requests a refund."}
  ]
}
```

Trained answers include `name`, `type`, `decision_method: "typed"` and:

| Type | Fields |
| --- | --- |
| `choice` | `choice`, `probabilities: [{value, probability}]`, `confidence`, `confidence_method` |
| `score` | `score`, `probabilities: [{value, label, probability}]`, `confidence`, `confidence_method` |
| `predicate` | `probability`, optional confidence diagnostics |

Scores are expected zero-based ordinal indices and can be fractional. A
predicate estimates whether the statement is true. Confidence is a model
diagnostic, not the probability that a chosen label is correct. Models with
an action head retain `act_probability`. Applications apply acceptance policy
and validate arguments before executing an action.

A single response contains `model`, an `answers` array in question order, and
aggregate token `usage`. For batches, replace `input` with
`inputs: [{"id": "first", "input": "..."}, {"input": "..."}]`. The response uses
`data: [{input_index, id?, answers}]` instead of top-level `answers`. Questions
are shared; IDs and order are preserved. Limits: 128 inputs, 64 questions,
2–64 choices or levels, 1 MiB per input and 16 MiB per request. Model and
executor limits can be smaller.

## Shared context batches

Supporting Antfly servers accept the explicit `context_version: 1` transport
extension. Send immutable context strings once and reference their zero-based
indices from individual inputs:

```json
{
  "model": "decision-model",
  "context_version": 1,
  "contexts": ["File: retry.py\nCONTEXT: def retry(): ..."],
  "inputs": [
    {"id": "line-1", "input": "TARGET: retry_count += 1", "context_index": 0},
    {"id": "line-2", "input": "TARGET: return response", "context_index": 0}
  ],
  "questions": [{"name": "relevant", "type": "predicate", "instructions": "Does TARGET implement retry enforcement? Use CONTEXT only to interpret TARGET."}]
}
```

Each referenced item is evaluated with exactly `context + "\n" + input`.
Unreferenced items keep their original input. Answer IDs, indices, and question
semantics are unchanged. Unknown versions, missing context arrays, invalid
indices, and references without a version fail validation. Context arrays contain
at most 128 nonempty UTF-8 strings; effective inputs remain limited to 1 MiB each
and their combined expanded size is limited to 16 MiB before model admission.
Model limits may reject smaller requests. Shared transport reduces request bytes;
it does not imply shared model computation or lower billed token usage.

`antfly-grep --decision-context shared-v1` opts into this format. Its default
`legacy` transport remains compatible with existing endpoints.

## Embedding similarity and multi-choice

[EmbeddingGemma 2](../guides/models/embeddinggemma2.mdx) advertises `embedding_similarity` and
supports `choice` and `multi_choice`. Answers include
`decision_method: "embedding_similarity"`, `similarity_metric: "cosine"` and
`similarities: [{value, similarity}]`. Raw cosine values range from -1 to 1;
these answers have no probability or confidence fields. Cosine is fixed because
thresholds, margins and calibration depend on its units. Another metric would
require a separately qualified contract.

Request-wide `embedding_options` supplies only geometry defaults: `task_type`
(`CLUSTERING` or `CLASSIFICATION`) and `dimensions` (768, 512, 256 or 128).
Each question has its own acceptance `embedding_options`: `min_similarity`,
`min_margin` or `calibration_id`. The embedding endpoint also accepts
`task_type`; decision routing restricts the supported prompt profiles.

`calibration_id` is a public identifier for a qualified artifact at
`<model-dir>/calibrations/<id>.json`. The runtime verifies model identity,
prompt profile, dimensions, renderer, mode and that question's prototypes.
Calibration is mutually exclusive with manual thresholds. See the model guide
for fitting; thresholds must be evaluated on application data.

For single choice, `min_margin` is the top-two cosine gap. Ties abstain:
`choice` is null and `status` is `abstained`. Multi-choice requires
`similarity_thresholds` (one cosine threshold or a complete value-to-threshold
map), unless calibration supplies them. Its `min_margin` is the nearest absolute
distance to any label's threshold. The answer includes effective thresholds and
`choices`: `selected` means a nonempty accepted set, `empty` is a valid empty
set, and `abstained` means the boundary margin failed. These states are distinct.

## HTTP, CLI and bindings

```sh
curl --fail-with-body http://127.0.0.1:8080/ai/v1/decisions \
  -H 'Content-Type: application/json' --data-binary @decision.json
antfly inference decisions ./models --request decision.json --backend metal
```

The inference listener serves `/decisions` directly (default port 8090).
Existing binding methods consume the same JSON: C `antfly_inference_decide_json`,
Rust `Inference::decide`, Go `Inference.Decide`, Python `Inference.decide`,
TypeScript `Inference.decide` / `decideRaw`. HTTP methods remain Python
`AntflyClient.decide` and TypeScript `InferenceClient.decide`.

```python
response = client.decide(request)
route = next(answer for answer in response.answers if answer.name == "route")
print(route.choice)
```

```typescript
const response = await client.decide(request);
const route = response.answers?.find(answer => answer.name === "route");
if (route?.type === "choice") console.log(route.choice);
```

Reuse handles for cached execution. Bindings preserve error bodies and retry
metadata. Free C output buffers on success and failure and close handles; see
[C API ownership](../../zig/CAPI.md).

## SQL providers

```yaml
deciders:
  triage:
    provider: antfly
    model: decision-model
    max_rows: 10000
    max_input_tokens: 1000000
    batch_size: 32
```

SQL references the configured name. Omitting `url` uses linked inference when
available. For HTTP use `http://127.0.0.1:8090`, or the standalone base including
`/ai/v1`; the provider appends `/decisions`. OpenAI uses `/decisions`; Jev's
private adapter retains its upstream wire format. Responses normalize to the
same public answer arrays.

```sql
SELECT ai_decide('Please refund my charge.',
  '[{"name":"refund","type":"predicate","instructions":"The input requests a refund."}]'::jsonb,
  'triage');
SELECT ai_choice('Duplicate charge', 'Which team?',
  '{"billing":"Payments and refunds","support":"Product troubleshooting"}'::jsonb, 'triage');
SELECT ai_score('Respond before tomorrow.', 'How urgent?',
  '["Routine","Soon","Immediate"]'::jsonb, 'triage');
SELECT ai_probability('Please refund my charge.', 'The input requests a refund.', 'triage');
```

`ai_decide` accepts named question arrays and returns the full response.
Convenience functions return a choice, expected score or true probability.
Embedding deciders use `decision_method: embedding_similarity`; trusted config
can supply geometry and acceptance defaults. Serialization moves acceptance
onto each question; question policies override those defaults. `ai_choice`
returns SQL NULL on abstention, and `ai_decide` supports multi-choice. Scores and
predicates require trained deciders. A database handle alone does not configure
SQL providers.

Validate quality, thresholds and resource limits on your workload before
deployment. Preparation and limits are in [Laya](../guides/models/laya.mdx) and
[EmbeddingGemma 2](../guides/models/embeddinggemma2.mdx).

## Decider cascades

A cascade is a named decider that asks a cheap decider first and sends only
uncertain inputs to a stronger one. Configure it under `deciders` with
`decision_method: cascade`; each stage is an inline typed or generated decider.

```yaml
deciders:
  same-entity:
    decision_method: cascade
    cascade:
      stages:
        - name: fast
          decider: {provider: jev, model: jev-latest}
        - name: strong
          decider:
            decision_method: generated
            probabilities: verdict_only
            generator: {provider: anthropic, model: your-strong-model}
          when:
            question: same
            choice: same
            probability_between: [0.3, 0.9]
      budget: {max_model_calls: 2, deadline_ms: 10000}
```

Select it like any other decider: `"model": "deciders/same-entity"` on
`/ai/v1/decisions`, the decider name in SQL, or the inline configuration in a
resolver `adjudicate` policy or decision enrichment.

- The first stage answers every input. A later stage runs only for inputs whose
  previous answer falls inside its inclusive `probability_between` band, and
  those inputs share one call. `question` limits the band to one question, and
  `choice` measures that value's probability instead of the selected value's.
- The final answer is the last answering stage's. Usage is summed over the
  stages that ran for the input. Shared generated-batch usage is apportioned
  evenly across each call's inputs, with integer remainders assigned in input
  order, and marked `usage.estimated: true`. Request totals remain measured.
  If reported usage exceeds a stage's token limit, every completed call is
  charged before the stage fails and future windows stop. `keep_previous`
  retains those paid costs in the failed-stage trace and final usage; inputs
  that were not submitted incur no model usage.
- Only choice and predicate questions are accepted, because bands read
  probabilities. Embedding-similarity deciders and nested cascades cannot be
  stages, and a band cannot follow a `verdict_only` generated stage.
- Escalation applies to successful answers. It is not failover: a stage error
  ends the input unless that stage sets `on_failure: keep_previous`, which keeps
  the previous answer and records the error. The first stage defaults to `fail`;
  explicitly setting `on_failure: fail` is equivalent to omission, while
  `keep_previous` requires a preceding stage. A later stage's operational failure
  does not discard confident siblings that already completed an earlier stage.
- `budget.max_model_calls` (at most 8, at least the stage count) bounds stage
  invocations; `budget.deadline_ms` narrows the caller's deadline.
- Each stage keeps its own rate limits, row and token budgets, and media
  limits across batches in the same statement. Set row, token and batch limits
  on the stage deciders. The cascade admits images and message evidence only
  when every stage does. Question and choice selectors are validated against
  the request before any stage runs. Native model discovery runs only when a
  stage is reached, under its deadline and failure policy.

Responses carry a `cascade` trace per input (top-level for single input, on each
batch row otherwise):

```json
{"cascade": {"decider": "same-entity", "definition_sha256": "…", "answered_by": "strong",
  "stages": [
    {"name": "fast", "decision_method": "typed", "model": "jev-latest", "answers": [...], "usage": {...}, "escalated": true},
    {"name": "strong", "decision_method": "generated", "model": "your-strong-model", "answers": [...], "usage": {...}, "escalated": false}]}}
```

`definition_sha256` identifies the cascade definition without credentials.
Anthropic stages require `probabilities: verdict_only`, since they do not
provide token logprobs. Such stages can appear only last. Resolver probability
thresholds still apply to the probabilistic stages; a terminal verdict-only
stage must choose categorically, and its reported 0/1 distribution is not a
calibrated confidence estimate.
Stage traces include `model_revision` when the provider reports an immutable
revision. Resolver adjudication verifies the definition, escalation decisions,
stage order, and agreement between the trace and the final answer.
Probabilities from different models are not calibrated against each other, so
set each band on labelled examples for the models involved and measure how
often inputs escalate. The trace has what an evaluation needs: the answering
stage, whether each answer escalated, and per-stage usage.
