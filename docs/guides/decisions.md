# Decisions

Standalone decisions use `POST /decisions` (`/ai/v1/decisions` on the standalone
server). Named question and answer arrays, `input`, `choices`, `levels` and
predicate `probability` follow the [OpenAI Decisions conventions](https://developers.openai.com/api/docs/guides/decisions).
Antfly adds embedding similarity answers, multi-choice selection and text
batches. This is a text-only subset: choice values are strings and every
question needs a unique name. See the [decision schema](../../specs/openapi/ai/decision.yaml).

`/extract` retains entities, relations, attributes, structured records and
ordinary classification. Standalone Laya, OpenDecider and EmbeddingGemma
workflows use `/decisions`. The unreleased extraction decision output and
Boolean and ordinal decision modes have been removed.

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

## Embedding similarity and multi-choice

[EmbeddingGemma 2](embeddinggemma2.md) advertises `embedding_similarity` and
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
antfly-inference decisions ./models --request decision.json --backend metal
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
deployment. Preparation and limits are in [Laya](laya.md) and
[EmbeddingGemma 2](embeddinggemma2.md).
