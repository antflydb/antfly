# Cloudflare wire schemas

`../cloudflare-openapi.json` contains pinned input/output schemas from Cloudflare's
[official model catalog](https://github.com/cloudflare/cloudflare-docs/tree/production/src/content/workers-ai-models).
`sources.json` records each original model URL and SHA-256 of the downloaded catalog
entry. Cloudflare authored these schemas; their CC BY 4.0 license is retained in
`LICENSE`. The adaptation wraps the schemas in OpenAPI 3.1 and lifts anonymous
objects into named components so the Zig generator produces useful nested types.
No model constraints were intentionally widened by this transformation.

Cloudflare also publishes [account API OpenAPI](https://github.com/cloudflare/api-schemas/blob/main/openapi.json).
The investigated snapshot (SHA-256
`4c1dd2c51c936ff4173d839f7459fdbb5a2e8e5eecc92b1b8a7f0f0dd24c7411`)
is 27.6 MB and its generic inference union omits Clairic and Clef. Importing that
union would give neither model-specific coverage nor a useful compilation cost.

`make generate` generates `cloudflare_api` types through the same types-only path
as OpenAI and Exa. Adapters use those types with Antfly's existing pooled HTTP,
secret resolution, quotas and cancellation instead of generating a second client.
Cloudflare's OpenAI-compatible chat and embedding surfaces reuse the existing
OpenAI wire adapter. Native reranking and speech use the model-specific types.

To update: download each catalog entry, review schema changes and its source
hash, replace the corresponding input/output components, lift nested object
schemas to components, and run `make generate` and provider fixture tests. Refresh
capability qualification separately: a new schema or model name does not establish
tool calling, transcription accuracy, image support or fitted calibration.
