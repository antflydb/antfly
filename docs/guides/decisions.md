# Typed decisions

Use decisions to classify input, rank it against an ordered rubric, or estimate
whether a statement is true. The contract is independent of the checkpoint:
applications supply named questions and consume named answers. Select a model
that advertises `typed_decisions` and the `decide` task in model discovery.

The same `DecideRequest` and `DecideResponse` JSON contracts from
[`specs/openapi/inference/api.yaml`](../../specs/openapi/inference/api.yaml)
are available through HTTP and embedded inference. SQL functions use the same
question and answer semantics through a named decision provider.

| Interface | Entry point | Model selection |
| --- | --- | --- |
| Standalone HTTP | `POST /ai/v1/decide` | Request `model` |
| Inference service HTTP | `POST /decide` | Request `model` |
| C embedded inference | `antfly_inference_decide_json` | Request `model` |
| Rust embedded inference | `Inference::decide` | Request `model` |
| Go embedded inference | `Inference.Decide` | Request `model` |
| Python embedded inference | `Inference.decide` | Request `model` |
| TypeScript embedded inference | `Inference.decide` / `decideRaw` | Request `model` |
| Python HTTP SDK | `AntflyClient.decide` | Request `model` |
| TypeScript HTTP SDK | `InferenceClient.decide` | Request `model` |
| SQL | `ai_decide`, `ai_choice`, `ai_score`, `ai_probability` | Named `DeciderConfig` |

## Request and answer semantics

A request contains a model, a nonempty text `state`, and a map of questions.
Each question needs a `type` and `instructions`.

| Type | Criteria | Answer |
| --- | --- | --- |
| `choice` | Object mapping stable option IDs to descriptions | `choice` option ID and full `probabilities` by ID |
| `score` | Array of descriptions ordered from lowest to highest | Expected zero-based `score`, `probabilities` by numeric string index, and `legend` |
| `noul` | Omit criteria | `noul`, the probability that the statement is true, from 0 to 1 |

Choice IDs are application values: changing a description does not require
changing the ID. A score is the probability-weighted mean of the level indices,
so it can be fractional. For three levels its range is 0–2. Boolean `noul` is a
probability, not a thresholded Boolean. The application chooses its threshold.

```json
{
  "model": "decision-model",
  "state": "Please refund my duplicate charge before tomorrow.",
  "questions": {
    "route": {
      "type": "choice",
      "instructions": "Which team should handle this request?",
      "criteria": {
        "billing": "Payments, charges, and refunds",
        "support": "Product usage and troubleshooting"
      }
    },
    "urgency": {
      "type": "score",
      "instructions": "How urgent is this request?",
      "criteria": ["Routine", "Time sensitive", "Immediate"]
    },
    "refund": {
      "type": "noul",
      "instructions": "The request asks for a refund."
    }
  }
}
```

Responses contain `model`, an `answers` map with the same question names, and
`usage.input_tokens` / `usage.output_tokens`. For example, the answer portion
could be:

```json
{
  "route": {"type": "choice", "choice": "billing", "probabilities": {"billing": 0.9, "support": 0.1}},
  "urgency": {"type": "score", "score": 1.1, "probabilities": {"0": 0.1, "1": 0.7, "2": 0.2}, "legend": {"0": "Routine", "1": "Time sensitive", "2": "Immediate"}},
  "refund": {"type": "noul", "noul": 0.95}
}
```

There can be up to 64 questions and 2–64 options or score levels per question.
The complete request JSON is limited to 1 MiB. Model token budgets and executor
limits can be smaller; oversized inputs fail instead of being silently truncated. A model
must explicitly support typed decisions. An arbitrary extractor or generator
is not interchangeable with a decider.

## HTTP and command hooks

Save the request as `decision.json`, using an installed model's ID, and start
`antfly standalone --models-dir ./models`. Then call:

```sh
curl --fail-with-body http://127.0.0.1:8080/ai/v1/decide \
  -H 'Content-Type: application/json' \
  --data-binary @decision.json
```

For `antfly inference run`, use its inference listener (default
`http://127.0.0.1:8090/decide`). A command invoked once per tool call can reuse
this HTTP service and its loaded models. Consume the named answers, then apply
application policy and validate arguments before executing an action. A
decision does not itself execute tools or supply free-form arguments.

The Python and TypeScript HTTP SDKs accept the same request:

```python
import json
from antfly import AntflyClient

with open("decision.json") as file:
    request = json.load(file)
response = AntflyClient("http://127.0.0.1:8080").decide(request)
print(response.answers["route"].choice)
```

```typescript
import { readFileSync } from "node:fs";
import { InferenceClient } from "@antfly/sdk";

const request = JSON.parse(readFileSync("decision.json", "utf8"));
const client = new InferenceClient({ baseUrl: "http://127.0.0.1:8080" });
const response = await client.decide(request);
console.log(response.answers.route.choice);
```

## Embedded inference

Open one inference handle and reuse it. No database or HTTP server is required.
Models load on first use and remain cached for the handle's lifetime.

```c
antfly_inference *inference = NULL;
antfly_error_code code = antfly_inference_open(NULL, &inference);
if (code == ANTFLY_OK) {
    antfly_buffer response = {0};
    /* request_bytes contains a DecideRequest JSON object. */
    code = antfly_inference_decide_json(inference, request_bytes, &response);
    /* Inspect response on success or failure, then release it either way. */
    antfly_buffer_free(&response);
    antfly_inference_close(inference);
}
```

```rust,no_run
use antfly_embedded::{Inference, InferenceOptions};

fn decide(request_json: &[u8]) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let inference = Inference::open(&InferenceOptions::new().models_dir("./models"))?;
    let response = inference.decide(request_json)?;
    inference.close()?;
    Ok(response)
}
```

```python
import json
from antfly_embedded import Inference

with open("decision.json") as file:
    request = json.load(file)
with Inference.open(models_dir="./models") as inference:
    response = inference.decide(request)
    print(response["answers"]["route"]["choice"])
    # Use raw=True to receive JSON bytes.
```

```typescript
import { readFileSync } from "node:fs";
import { Inference } from "@antfly/embedded";

const inference = await Inference.open({ modelsDir: "./models" });
try {
  // decideRaw returns JSON bytes as a Buffer; decide returns parsed JSON.
  const response = await inference.decideRaw(readFileSync("decision.json"));
  console.log(JSON.parse(response.toString("utf8")).answers.route.choice);
} finally {
  await inference.close();
}
```

Python embedded calls raise the matching exception class on failure. TypeScript
embedded calls throw `AntflyError` with the parsed runtime error in `.body`.
The HTTP SDKs preserve inference errors and capacity retry metadata.

The C call returns the runtime's JSON error body even when its return code is
an error; always free the output buffer. Rust exposes the code and body through
`InferenceError`. See [the C API contract](../../zig/CAPI.md) for resource budgets,
timeouts, handle lifetime, and thread requirements. Go exposes the corresponding
error through `InferenceError`. Install models before calling; these methods do
not download them on demand.

## SQL providers

Configure a named decider in the server configuration, for example:

```yaml
deciders:
  triage:
    provider: antfly
    model: decision-model
    max_rows: 10000
    max_input_tokens: 1000000
    batch_size: 32
```

With an embedded Antfly inference runtime, omitting `url` uses that runtime.
For a separate inference service, set `url: http://127.0.0.1:8090`; for standalone
HTTP set `url: http://127.0.0.1:8080/ai/v1`. The provider appends `/decide`.
`DeciderConfig` also supports `jev`, provider credentials, and rate limits;
see [the configuration schema](../../specs/openapi/antfly/config.yaml).

SQL functions reference the configured **name**, not an inline configuration:

```sql
SELECT ai_decide(
  'Please refund my duplicate charge.',
  '{"refund":{"type":"noul","instructions":"The request asks for a refund."}}'::jsonb,
  'triage'
);

SELECT ai_choice('Duplicate charge', 'Which team handles this?',
  '{"billing":"Payments and refunds","support":"Product troubleshooting"}'::jsonb,
  'triage');

SELECT ai_score('Please respond before tomorrow.', 'How urgent is this?',
  '["Routine","Time sensitive","Immediate"]'::jsonb, 'triage');

SELECT ai_probability('Please refund my charge.',
  'The request asks for a refund.', 'triage');
```

`ai_decide` returns the complete response; the convenience functions return the
selected option ID, expected score, or true probability. Embedded SQL execution
requires a decision provider supplied by the host. Opening a C or Rust database
handle alone does not configure named deciders; use the standalone inference
handle for direct decisions without that SQL setup.

Benchmark accuracy and calibration on your application inputs before selecting
models or thresholds. Keep policy, argument validation, and action execution in
the application. Model-specific preparation belongs in guides such as
[Laya](laya.md); the decision request remains the same across supported models.
