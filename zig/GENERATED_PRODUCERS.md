# Constrained generation for artifacts and decisions

Generator artifacts with an output schema, generated deciders, and generated
entity/relation extractors share `lib/structured`. It owns schema admission,
positional batch identities, schema validation, and one repair attempt for
unanswered items. Provider execution stays in the normal generation runtime,
which owns credentials, quotas, cancellation, and deadlines.

## Structured generator artifact

```json
{
  "name": "claims_v1",
  "kind": "asset",
  "field": "search_text",
  "content_type": "application/json",
  "producer": {
    "type": "generator",
    "config": {
      "generator": {"provider": "openai", "model": "gpt-4.1"},
      "instructions": "Extract one concrete claim from the supplied text.",
      "output": "auto",
      "output_schema": {
        "type": "object",
        "properties": {
          "kind": {"type": "string", "enum": ["fact", "decision"]},
          "statement": {"type": "string"}
        },
        "required": ["kind", "statement"],
        "additionalProperties": false
      },
      "batch": {"max_items_per_call": 8, "max_input_bytes": 65536}
    }
  }
}
```

The artifact contains the schema's value, without the internal batch envelope.
Legacy flat generator configurations and ordinary tool-output selection retain
those interfaces. A schema producer cannot also select arbitrary legacy tools.
Unsupported JSON-schema keywords are rejected rather than ignored. Hosted strict
schema dialects are checked before generation as well: OpenAI object properties
must all be required and use `additionalProperties: false`. The shared validator
still checks unique array items when the wire dialect omits `uniqueItems`; a
violating item uses its one repair attempt. Generic output validation checks
every JSON field, including underscore-prefixed fields, through nested arrays
and schema composition. The document validator's metadata exemptions apply
only to stored document validation.

The admitted keywords assert independently: a nullable `anyOf` cannot bypass a
sibling `enum`, `const`, or `type`. Type-specific constraints apply only to that
instance type, so `minimum` constrains numbers without imposing a numeric type.
Enums, constants, and unique items use JSON value equality: object member order
is irrelevant and mathematically equal numbers compare equally. Integer and
numeric-literal comparisons retain decimal precision instead of rounding through
floating point. Provider replies, schema configuration, and native output checks
preserve decimal/exponent literals before validation, including tool arguments
and replay. This rejects values such as `1.0000000000000001` against a maximum
of `1`, and prevents `1e-400` from becoming integer zero. Typed score projection
happens after validation; `raw_structured_answer` retains its exact literal.
The owned constraint tree compiles once per batch and serves all
items and repair attempts. Validation collects evaluated-field and evaluated-item
coverage in the same traversal, retaining annotations only from successful
branches. Schemas without coverage consumers allocate no coverage state;
composition does not recursively revalidate subtrees to discover annotations.
Unique items use semantic fingerprints with equality checks on collisions,
avoiding pairwise comparisons of distinct items. Hashing and literal equality
walk instance values iteratively, so nested model values do not consume the
native call stack. Before ownership or provider serialization, the complete
schema JSON graph (including `const`, `enum`, and annotations) is limited to
128 levels and 65,536 values. Constrained schema keywords retain their tighter
24-level and 4,096-schema-node limits.

`auto` selects a forced, actual tool call, provider JSON-schema output, then
native grammar output, subject to the selected model's admitted capabilities.
A provider adapter's synthetic tool-call compatibility behavior does not count
as constrained tool output. Unknown hosted model families are rejected for
these contracts. Native execution uses resolved executor capabilities.

## Generated decisions

```json
{
  "decision_method": "generated",
  "generator": {"provider": "antfly", "model": "my-registered-generator"},
  "probabilities": "verdict_only",
  "output": "grammar",
  "instructions": "Compare the concepts, including their descriptions.",
  "batch": {"max_items_per_call": 40, "max_input_bytes": 65536}
}
```

Use this configuration wherever a decider is accepted. Public `/decisions`
requests use named question arrays and `input` or `inputs`, with the nested
`generator` replacing the trained `model` field. Inference nodes admit their
registered native generators. Configured enrichment and SQL runtimes also
support admitted hosted generators. Direct inference requests reject external
provider credentials, proxy URLs, tools, and provider rate limits; those settings
belong on configured generator producers.

Choice questions produce an enum, multiple-choice questions a unique enum
array, predicates a boolean, and scores a number in `[0,1]`. Answers retain
`decision_method: generated`, `probability_source`, and `raw_structured_answer`.

`token_logprobs` is the default. It requires actual, complete decoder evidence
for every allowed value. Evidence must bind to the correct batch item and JSON
field. Native JSON-schema decoding uses additive vocabulary bytes for grammar masks,
streaming, final text, and evidence. SentencePiece whitespace and byte fallback
survive without sequence-level cleanup. It isolates punctuation from each finite value
and admits one deterministic vocabulary token per value, preferring a regular
vocabulary piece over an equivalent byte-fallback piece. Quote-fused tokens and
alternate tokenizations cannot compete with those tokens. Every choice's JSON
escaped content (and both boolean literals) must exist as a standalone token in
the loaded model's vocabulary. Unsupported values, including multi-token labels,
are rejected before decoding with `UNSUPPORTED_CONSTRAINED_PROBABILITIES`; use
`verdict_only` for those questions. The diagnostic identifies the unsupported
value. The implementation rejects partial top-k evidence; it never fills missing
labels with zero or interprets a model-written number as probability. Choice and
predicate questions can use this mode. Multiple-choice and numeric score
questions currently require explicit `verdict_only`.

`verdict_only` records `probability_source: verdict`; selected choices have
probability one and unselected choices zero. Resolver acceptance and rejection
thresholds must be zero or one. Scores remain model ratings, not calibrated
confidence. Durable adjudication evidence retains the normalized answer and
raw structured answer. Successful siblings survive a repair or provider failure.

## Generated extraction

Extraction windows produce typed document outcomes. The first failed window in
source order retains its execution cause; successful sibling documents remain
available to embedded producers. The HTTP response uses these same outcomes:
capacity exhaustion remains retryable `MODEL_RESOURCE_BUSY` (503), deadlines
remain retryable `INFERENCE_TIMEOUT` (504), and unsupported finite probability
vocabularies remain permanent admission failures (400). Input byte-budget and
extraction materialization limits return `BATCH_TOO_LARGE` (413), without a
retryable capacity signal. An input exceeding its byte budget is rejected before
provider invocation.

```json
{
  "method": "generated",
  "generator": {"provider": "openai", "model": "gpt-4.1"},
  "schema": {
    "entities": ["person", "organization"],
    "relations": [{"type": "works_for"}]
  },
  "entity_definitions": {"person": "A named individual"},
  "probabilities": "verdict_only",
  "options": {
    "include_spans": true,
    "include_confidence": true,
    "offset_unit": "utf8_bytes",
    "long_document": {"mode": "window", "window_words": 1024, "overlap_words": 32}
  }
}
```

Use as an extractor producer configuration, or add `inputs` for `/extract`.
Generated extraction currently admits entity labels and relation type labels;
advanced trained-extractor schema tasks are rejected. Relations refer to
positions in the generated entity list. Relation endpoints and internal batch
identities use checked mathematical integer conversion, so equivalent JSON
spellings such as `0`, `0.0`, and `0e0` retain the same identity. Fractions,
negative values, and out-of-range indices fail validation and use the ordinary
repair policy. Alignment expands repeated source
mentions, maps relation endpoints to the resulting `entity_index` values, and
deduplicates overlap by source span and label. Unicode matching and window
geometry are shared with GLiNER, preserving original source coordinates.
Unaligned mentions are dropped and counted in `unaligned_mentions`.

The generator never supplies trusted offsets or confidence. Measured confidence
combines label and existence token evidence. Verdict-only mentions have score
one, and a consuming resolver must explicitly configure `min_confidence`.

## Evaluation

`scripts/eval_extract.py` reports per-label precision and recall, source alignment
failures when reported by the server, and measured token use. Supply an explicit
`evaluation_pricing` object with `input_usd_per_million` and
`output_usd_per_million` to calculate cost. Missing usage or pricing
remains unknown; it is not estimated as zero.

Generated execution preserves transient load and run admission failures as
`MODEL_RESOURCE_BUSY` with `retryable: true` and `retry_after_ms`, so callers can
back off using the same policy as ordinary generation and typed decisions.
