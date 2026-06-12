Inference OpenAPI generated sources for `inference_api` live here.

Build behavior:
- by default, `build.zig` uses the checked-in generated module
- the source spec is `../../../../../specs/openapi/inference/api.yaml`
- you can override the source explicitly with `-Dinference-openapi-spec=...`

Intended use:
- migrate `/api/generate` toward an OpenAI-compatible core contract
- add inference-native extensions such as `grammar`, `draft_model`, and `speculative_k`
- keep API evolution for this repo unblocked from the external Antfly checkout
