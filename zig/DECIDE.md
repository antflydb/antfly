# Decision API implementation

Standalone inference decisions use `POST /decisions` and `/ai/v1/decisions`
on the standalone server. The authoritative schemas live in
[`specs/openapi/ai/decision.yaml`](../specs/openapi/ai/decision.yaml), and
[`docs/guides/decisions.md`](../docs/guides/decisions.md) documents requests,
responses, provider configuration and examples.

The public contract uses text `input`, named question and answer arrays,
`choice` questions with `choices`, `score` questions with ordered `levels`, and
`predicate` answers with `probability`. Choice values are strings, every
question has a unique name, and scores are expected zero-based ordinal indices.
These conventions align with OpenAI Decisions. Antfly supports a text subset
and adds embedding similarity, multi-choice set selection and text batches.

## Execution and presentation

`lib/decisions/root.zig` validates the public contract and presents public
responses. Trained adapters lower questions into the existing private extraction
representation. Laya/OpenDecider and the declared GLiNER decision head retain
complete distributions, confidence diagnostics and optional action-head
probabilities. This internal reuse does not publish a second standalone API.

`lib/decisions/trained.zig` owns the private trained adapter. Its historical map,
`state` and `noul` vocabulary is confined to implementation details and Jev's
upstream provider format. HTTP requests, SDK results and SQL `ai_decide` use the
public named arrays and `predicate` vocabulary.

`/extract` retains entity, relation, attribute, record and ordinary
classification workflows. Decision-only Laya and EmbeddingGemma models are
excluded from extractor discovery. Public extraction rejects the removed
Boolean and ordinal decision modes and embedding decision options, and no longer exposes a
standalone `decisions` output. The unreleased contracts have no compatibility
aliases.

## Embedding decisions

EmbeddingGemma 2 advertises `embedding_similarity`, separately from trained
`typed_decisions`. Geometry defaults (`task_type`, `dimensions`) belong to the
request; acceptance and `calibration_id` belong to individual questions.

Answers expose cosine similarities, the fixed `cosine` metric, model identity
and prototype hash. Similarity values are not probabilities or confidence.
Single choice abstains on ties or failed thresholds. Multi-choice uses a manual
cosine threshold or qualified calibration for every label and distinguishes a
nonempty selection, a valid empty set and abstention. Its margin is the nearest
absolute distance to a label threshold; single choice uses the top-two gap.

Prototype caching, model identity verification, cancellation and resource
admission remain shared with managed inference. Calibration binds the model,
geometry, renderer, question prototypes and selection mode. Threshold quality
still needs application-specific held-out evaluation.

## Batches and limits

A single request uses `input` and returns `answers`. A batch uses
`inputs: [{id?, input}]` and returns ordered `data: [{input_index, id?, answers}]`.
Questions are shared across inputs. Token usage is aggregate and is not counted
again for every answer. Public limits are 128 inputs, 64 questions, 2–64 choices
or levels, 1 MiB per input and 16 MiB per request; model and executor limits can
narrow these ceilings. Executor admission counts inputs separately from their
expanded questions.

## SQL, providers and bindings

The shared decision provider serves `ai_decide`, `ai_choice`, `ai_score` and
`ai_probability`. `ai_decide` accepts the public question array and returns the
full public response; convenience functions return scalars. Embedding
abstention returns SQL NULL for `ai_choice`. See [FUNCTIONS.md](FUNCTIONS.md)
for query scopes, budgets, materialization and enrichment.

Antfly HTTP providers use `/decisions`; linked providers use the same direct
entry point. OpenAI uses its generated official request and response types.
Jev's upstream map format is translated privately. Normalization checks the
configured provider capabilities and complete distributions, preserves model
and usage metadata, and rejects malformed outputs. Score labels survive every
adapter. Multi-choice requires the embedding provider capability.

The CLI command is `antfly-inference decisions`. Existing C and language
binding method names (`decide`/`Decide`/`decideRaw`) consume the current JSON
contract. Generated OpenAPI and SDK types are refreshed together with manual
client methods and tests.

## Validation scope

Contract tests cover per-question policy isolation, duplicate names, batches,
labels, diagnostics, empty sets, abstention and rejection of removed aliases.
Provider HTTP mocks cover Antfly, Jev and OpenAI serialization, authentication,
budgets and normalization. Managed real-model tests exercise the checkpoint,
prototype reuse, identity pins, HTTP presentation and cancellation on Metal
and CPU. A small Laya fixture checks transport and batch conformance; it is not
an accuracy evaluation.

These checks qualify the API revision. Production quality and throughput claims
need current workload evidence; earlier performance artifacts retain their
original source provenance.
