Termite OpenAPI sources for `termite_api` code generation live here.

Build behavior:
- by default, `build.zig` uses `src/api/openapi.yaml`
- you can override the source explicitly with `-Dtermite-openapi-spec=...`

Intended use:
- migrate `/api/generate` toward an OpenAI-compatible core contract
- add termite-native extensions such as `grammar`, `draft_model`, and `speculative_k`
- keep API evolution for this repo unblocked from the external Antfly checkout
