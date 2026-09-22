# Laya support investigation

Investigated 2026-09-19 against Antfly `origin/main` at
`8b87fb96d2ac67b45888f0c6d1831d445963730a`, in branch
`codex/investigate-laya`. This is a proposal; no runtime support is implemented.
Upstream source revision: `6a5819129eb220570792e417e49723d697efd76f`.

## Recommended placement

| Layer | Placement |
| --- | --- |
| Public capability / configured model category | **Extractor**, supporting typed classification |
| Public inference API | Existing **`POST /ai/v1/extract`** |
| Provider identifier for native or Antfly-served inference | Existing **`antfly`** |
| Internal model kind | **`classifier`**, with a dedicated Laya architecture/pipeline |

Antfly explicitly treats classification as a public extraction capability and
keeps `classifier` internal. See `taskMatchesModelListing` in
[`server.zig`](../../zig/pkg/inference/src/server/server.zig), the canonical
[`/extract` specification](../../specs/openapi/inference/api.yaml), and
[`test_classify.py`](../../zig/e2e/inference/test_classify.py).

Do not introduce a public classifiers catalog or generator provider for this
integration. Model family and serving provider are separate concepts. A new
`laya` value in `ExtractionProvider` would only make sense if we deliberately
supported a distinct Laya server protocol. The inspected upstream repository
provides a Python runtime and model artifacts; it does not establish a hosted
production API contract or an OpenAI-compatible endpoint.

## What Laya requires

Laya answers named `choice`, `score`, and `noul` questions over text or serialized
JSON. It encodes one sequence per question, batches those sequences, and returns
structured decisions without generated tokens. It requires its custom
`rl_agent_config.json`, tokenizer, encoder configuration, and safetensors weights.
See the pinned [runtime source](https://github.com/NandhaKishorM/laya/blob/6a5819129eb220570792e417e49723d697efd76f/laya/agent.py).

The model adds a question-type embedding and transformer head to an encoder,
gathers option-marker representations, and scores each option. An auxiliary
action head predicts whether to act. Native parity requires matching sequence
formatting, option order, truncation, padding, and these heads. For choice/ordinal
questions, upstream `confidence` is normalized inverse entropy, not the winning
label's probability. See the pinned [architecture source](https://github.com/NandhaKishorM/laya/blob/6a5819129eb220570792e417e49723d697efd76f/laya/common.py).

Inspected checkpoint configurations:

| Checkpoint | Encoder | Total sequence / question-head token budgets |
| --- | --- | --- |
| [laya](https://huggingface.co/convaiinnovations/laya/blob/main/rl_agent_config.json) | ModernBERT-large | 512 / 192 |
| [laya-multilingual](https://huggingface.co/convaiinnovations/laya-multilingual/blob/main/rl_agent_config.json) | mmBERT-base | 1024 / 256 |
| [laya-typed-decisions](https://huggingface.co/convaiinnovations/laya-typed-decisions/blob/main/rl_agent_config.json) | ModernBERT-large | 1024 / 256 |

Those budgets include the question and options, reducing available state text.
The multilingual configuration currently has unit temperatures and no
per-option-count calibration. Pin model revisions and preserve the configured
temperature-bucket precedence when importing artifacts.

## Existing integration points and gaps

1. [`zig/lib/extracting/src/mod.zig`](../../zig/lib/extracting/src/mod.zig)
   already provides extractor configuration, dispatch, HTTP transport, and a
   response envelope. Native support can retain `provider: antfly`.
2. [`ExtractionClassificationSchema`](../../specs/openapi/ai/extraction.yaml)
   already supports named tasks, `prompt`/`instruction`, label definitions, and
   `single`, `multi`, and `ordinal` modes. These provide much of the request shape.
3. [`pipelines/classification.zig`](../../zig/pkg/inference/src/pipelines/classification.zig)
   implements NLI premise/hypothesis scoring. Substituting a Laya checkpoint
   would produce incompatible inputs and head execution.
4. `extractV2InMemory` in
   [`server.zig`](../../zig/pkg/inference/src/server/server.zig) currently routes
   v2 requests through a GLiNER-specific executor. This is an implementation
   restriction, not an inherent requirement of the v2 API; see below.
5. [`modern_bert.zig`](../../zig/pkg/inference/src/architectures/modern_bert.zig)
   and [`session_factory.zig`](../../zig/pkg/inference/src/architectures/session_factory.zig)
   provide reusable encoder machinery. Laya's extra heads, weight prefixes,
   preprocessing, and calibration still require implementation and parity checks.
   mmBERT/tokenizer compatibility remains to be qualified separately.
6. [`manifest.zig`](../../zig/pkg/inference/src/models/manifest.zig),
   [`model_manager.zig`](../../zig/pkg/inference/src/server/model_manager.zig),
   [`capabilities.zig`](../../zig/pkg/inference/src/models/capabilities.zig), and
   [`registry.zig`](../../zig/pkg/inference/src/registry/registry.zig) need artifact
   recognition, loading, and capability metadata. Public listing currently gates
   internal classifiers on NLI zero-shot support; add an explicit typed-decision
   capability rather than falsely marking Laya as an NLI model.

## What the current v2 restriction means

`schema_version: 2` selects Antfly's extraction v2 request contract. Its native
execution path currently contains this check in `extractV2InMemory`:

```zig
if (manifest.gliner_architecture != .boundary)
    return error.UnsupportedExtractionModel;
```

Here, `boundary` identifies the GLiNER2.5 architecture defined in
[`gliner_boundary.zig`](../../zig/pkg/inference/src/models/gliner_boundary.zig).
After this check, the executor loads GLiNER boundary configuration and applies
the corresponding model/backend qualification checks. Even a classification-only
v2 request therefore rejects other architectures today. The older extraction
path supports additional classifiers, including NLI models.

The v2 schema itself is not inherently GLiNER-only: its named classifications,
instructions, and ordinal labels can provide the basis for Laya requests. Laya
still needs the boolean decision and richer result semantics described below.
Supporting it means introducing architecture/capability-based executor dispatch
and sharing the applicable parsing, validation, and response contract. Preserve
the existing GLiNER qualification checks within that executor and define Laya's
own supported-feature checks. Removing the architecture guard alone would send
Laya into incompatible GLiNER execution code.

## Public contract proposal

| Laya primitive | Extraction mapping | Additional semantics required |
| --- | --- | --- |
| `choice` | Named `single` classification; labels and descriptions become criteria | Full probability distribution, selected label, separately named confidence |
| `score` | Named `ordinal` classification with ordered rubric labels | Expected numeric level, distribution, and rubric; an argmax label is insufficient |
| `noul` | Explicit boolean decision mode/type | Preserve the `noul` question-type embedding and `P(true)`; two-label `choice` is not equivalent |

Keep existing per-label `score` as a probability. Add a typed per-task result
containing the distribution, expected ordinal value or true probability,
confidence semantics, and optional action probability. Exact field names need
API review; use generated schema fields rather than undocumented JSON extras.
Propagate the result through SDKs and enrichment materialization.

Initially support text-only classification tasks. Reject entities, relations,
spans, images, multi-label selection, examples, and constraints until explicitly
implemented. Validate whole batches before inference. Define truncation behavior
explicitly: upstream truncates state text, while extraction v2 promises explicit
long-document rejection unless supported windowing is requested.

## Tool selection and calling

Laya could supply decisions to an agent's tool-calling loop. This is a proposed
application of its primitives, not a claim that upstream provides a complete
tool-calling protocol or that Antfly already integrates it this way.

| Decision | Possible Laya question |
| --- | --- |
| Select the next tool | `choice` among available tools, `ask_user`, and `no_tool` |
| Decide whether a tool is needed | `noul` over the request and current state |
| Select a bounded argument | `choice` among known scopes, modes, or resource IDs |
| Assess expected usefulness | `score` over an explicit ordered rubric |

The agent would supply the available tools and their descriptions as criteria.
It would then construct arguments from application state, extraction results, or
a generative model, validate the tool call, and execute it. Laya does not generate
arbitrary search text, SQL, or nested argument objects. Finite argument choices
can be additional decisions; dependent choices may require another inference
step after selecting the tool.

The proposed flow is:

```text
Request and state -> Laya tool selection -> Argument construction and validation
                  -> Tool execution -> Updated state / next decision
```

Keep inference under extractors with `provider: antfly`; let agent orchestration
consume the result as a tool router. Tool execution and its authorization stay
with the existing agent/application layer. The auxiliary action probability
does not itself authorize or perform a call.

Evaluate tool-selection accuracy, `no_tool`/`ask_user` behavior, bounded argument
accuracy, and end-to-end task success on representative workflows. Calibrate
decision thresholds on those workflows; entropy-based confidence must not be
treated as the probability that the selected tool is correct.

## Suggested implementation sequence

1. Build a reference harness around the pinned Python package and one English
   checkpoint. Capture token IDs, marker positions, raw logits, probabilities,
   and final decisions for all three primitives. A development HTTP wrapper can
   exercise Antfly's extraction contract, but must implement the envelope and
   capability discovery expected by Antfly; it is not an existing upstream API.
2. Add the typed extraction schema/results and feature-specific validation.
   Regenerate affected OpenAPI bindings and SDKs.
3. Implement Laya artifact loading, native heads and preprocessing, and
   architecture-aware extraction dispatch using existing admission, cancellation,
   and memory limits. Keep GLiNER-specific execution and qualification in its
   existing executor. Expose the qualified Laya model under extractors.
4. Compare native outputs with the Python reference, then test HTTP and embedded
   extraction, batch ordering, unsupported features, token limits, cancellation,
   and enrichment output preservation. Qualify multilingual and additional
   backends after the first checkpoint works.
5. Optionally integrate a tool-selection consumer in agent orchestration using
   the same extraction results. Keep argument construction and execution outside
   the inference provider, and evaluate the complete tool workflow separately.

Use explicit checkpoint selection initially. Upstream reports weak zero-shot
performance on its typed-decisions benchmark, substantial language differences,
and calibration limitations; its fine-tuned checkpoint's results do not establish
accuracy for arbitrary Antfly workloads. Evaluate routing/triage datasets and
confidence thresholds before using decisions for automatic actions. See the
upstream [README and benchmark limitations](https://github.com/NandhaKishorM/laya/blob/6a5819129eb220570792e417e49723d697efd76f/README.md).

The initial investigation used source and model-configuration inspection only.
The implementation follow-up below adds model execution and measured validation.


## Implementation follow-up

Implemented on `codex/investigate-laya`: a pinned-checkpoint importer, native
ModernBERT/Laya decision heads, extraction-v2 dispatch, typed results and generated
SDKs, and enrichment preservation. See [usage and validation](../guides/laya.md).
The deterministic upstream-PyTorch fixture validates CPU and Metal tokenization
and logits within `2e-4`, reordered batches through 512 tasks, and HTTP/embedded
extraction. The released English checkpoint also matches all 192 sampled upstream
decisions on CPU and Metal, with maximum probability errors below `1e-5`, and
passes batch scaling through 128 tasks. See the
[accuracy, parity, and performance report](laya-qualification.md) for results,
regression fixes, and reproducible commands. CUDA, multilingual checkpoints,
fine-tuned checkpoints, and application-specific tool-routing accuracy remain
unqualified. Tool selection is available through classification results; agent
tool execution is outside this change.
