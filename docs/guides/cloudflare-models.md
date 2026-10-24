# Cloudflare models

Cloudflare Workers AI is an ordinary Antfly provider. Configure `account_id`, the
full `@cf/owner/model` or `@hf/owner/model` name, and an `api_key` secret reference.
Credentials default to `CLOUDFLARE_API_TOKEN`. Optional `gateway_id` uses
`cf-aig-gateway-id`; credentials, pools, quotas and deadlines stay in the existing
provider runtimes. Native library callers supply resolved tokens.

| Antfly task | Cloudflare path |
| --- | --- |
| Dense embedding and vector indexes | OpenAI-compatible embeddings; BGE, Qwen embedding and EmbeddingGemma text models |
| Reranking | Native `@cf/baai/bge-reranker-base`; scores retain original candidate indexes |
| Generation | Chat Completions, including streaming and model-supported tools |
| Reading | Vision Chat Completions; the qualified document-agent model is `@cf/mistralai/mistral-small-3.1-24b-instruct` |
| Extraction | Native Clairic flat records, or the existing generated extractor with a Cloudflare generator |
| Classification | Generated extraction classifications for caller-defined labels, or typed Clef decisions |
| Rewriting | A named generator-backed rewriter |
| Transcription | Native `@cf/openai/whisper-large-v3-turbo`, including language, duration, segments and word times when returned |
| Speech synthesis | Native Aura 2 English and MeloTTS MP3 output |
| Decisions | Native Clef/Clef Flash, generated decisions, or cosine routing through a named embedder |
| Chunking | Antfly retains chunk boundaries and offsets; use a Cloudflare embedder in the existing chunk/index enrichment pipeline |
| Tabular prediction | No compatible Cloudflare managed predictor endpoint is qualified |

Cloudflare does not expose a standalone chunking model matching Antfly's chunk
contract. Fixed-label sentiment and image classifiers do not implement arbitrary
caller labels, and VAD/end-of-turn detection does not imply timed audio segments.
Sparse embeddings, image embedding models without a qualified request schema,
Cloudflare Markdown conversion and tabular predictors are not advertised by these
adapters. Document agents retain Antfly's PDF/PPTX source handling and render pages
only when their policy needs visual reading. Generated vision output is marked
`interpretation`; the adapter does not invent OCR boxes or source citations.

## Configuration

```json
{
  "embedders": {
    "cf-text": {
      "provider": "cloudflare",
      "account_id": "account",
      "model": "@cf/baai/bge-base-en-v1.5",
      "dimensions": 768,
      "api_key": "${secret:cloudflare.api_token}"
    }
  },
  "generators": {
    "cf-chat": {
      "provider": "cloudflare",
      "account_id": "account",
      "model": "@cf/meta/llama-3.3-70b-instruct-fp8-fast",
      "api_key": "${secret:cloudflare.api_token}",
      "max_tokens": 1024
    }
  },
  "rewriters": {
    "clean-text": {
      "generator": "cf-chat",
      "instructions": "Rewrite clearly while preserving meaning. Return only the rewritten text."
    }
  },
  "deciders": {
    "route-by-similarity": {
      "decision_method": "embedding_similarity",
      "embedder": "cf-text",
      "embedding_options": { "dimensions": 768, "min_similarity": 0.5 }
    },
    "generated-choice": {
      "decision_method": "generated",
      "generator": {
        "provider": "cloudflare",
        "account_id": "account",
        "model": "@cf/meta/llama-3.3-70b-instruct-fp8-fast",
        "api_key": "${secret:cloudflare.api_token}"
      },
      "probabilities": "verdict_only"
    }
  }
}
```

Embedding response dimensions are validated, and fixed Cloudflare models are not
asked to resize vectors. Text requests are batched within the native 100-item
ceiling and retain one quota observation per HTTP attempt. Similarity decisions
batch and deduplicate input/category embeddings, normalize example centroids, and
return cosine scores with explicit tie/threshold abstentions. Their token
accounting uses a conservative byte upper bound marked `estimated: true`, because
the ordinary embedding interface does not expose measured token usage. Hosted
routes do not reuse fitted local model calibrations or claim immutable weights.

Generated extraction and decisions validate output through the existing typed
contracts. Qualified Cloudflare models use tool calls with explicit
`probabilities: "verdict_only"`; they do not claim unavailable token logprobs.
Model support for tools, images and structured output remains model-specific.
Clairic's native decimal precision is limited to 0–6. Whisper rejects diarization;
MP3 synthesis rejects unsupported speed and container overrides before dispatch.

Clairic validates the effective extraction schema version (1 or 2) before
dispatch. One extraction deadline covers input validation and all rows in a
serial batch; each upstream request receives only the remaining time. Explicit
request controls take precedence over the shared client's timeout and
cancellation defaults. Malformed successful vision responses are upstream
failures, reported as HTTP 502 by document endpoints.

See [declarative document models](declarative-document-models.md) for whole PDF,
slide and receipt sources and bounded reading/extraction policies. The provider
wire types are generated from pinned official model schemas; transport fixtures
exercise the adapters without consuming live model credits. Live model quality
and account-specific availability require separate qualification.

## Materialized similarity decisions

Persist a named embedder's configuration with the decision producer. The
`embedders` map freezes the dependency used for replay; it does not resolve
against a node's current named-model configuration. Changing that dependency
changes the specification hash and follows the normal enrichment rebuild
lifecycle. Credentials remain secret references or provider environment defaults.

```json
{
  "version": "v1",
  "decider": {
    "decision_method": "embedding_similarity",
    "embedder": "cf-text",
    "embedding_options": { "dimensions": 768, "min_similarity": 0.5 }
  },
  "embedders": {
    "cf-text": {
      "provider": "cloudflare",
      "account_id": "account",
      "model": "@cf/baai/bge-base-en-v1.5",
      "dimensions": 768,
      "api_key": "${secret:cloudflare.api_token}"
    }
  },
  "questions": [{
    "name": "route",
    "type": "choice",
    "instructions": "Choose the category that best describes the input.",
    "choices": [{ "value": "receipt" }, { "value": "slide" }]
  }]
}
```

Compatible rows share bounded embedding work and category prototypes. An invalid
query vector fails its own row; valid siblings remain available, and the batch's
conservative token estimate is retained even if scoring or dispatch fails. SQL
and HTTP execution borrow service-owned provider clients. Standalone enrichment
owners retain their own provider runtime across batches.

Generator-backed rewriting requires a complete generation result. Embedded
Antfly generators use the JSON callback to preserve finish reasons and usage;
legacy text-only callbacks are rejected before execution for this task.
