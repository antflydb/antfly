Inference OpenAPI generated sources for `inference_api` live here.

Build behavior:
- by default, `build.zig` uses the checked-in generated module
- the source spec is `../../../../../specs/openapi/inference/api.yaml`
- you can override the source explicitly with `-Dinference-openapi-spec=...`

Intended use:
- migrate `/api/generate` toward an OpenAI-compatible core contract
- add inference-native extensions such as `grammar`, `draft_model`, and `speculative_k`
- keep API evolution for this repo unblocked from the external Antfly checkout

Standalone inference POST routes accept the optional header
`X-Antfly-Inference-Timeout-Ms`. Supply it exactly once as decimal milliseconds
from 1 through 3,600,000. Its relative application budget starts at route dispatch
and can shorten an existing deadline. Invalid or duplicate values return
`400 INVALID_INFERENCE_TIMEOUT` before request admission. Omission retains the
existing application deadline.

The execution control checks this budget during preprocessing and model work;
transport read/write limits remain separate. Cooperative expiration returns
`504 INFERENCE_TIMEOUT` after cleanup. A process-required unsafe driver call can
instead cause the hard watchdog to terminate its worker and close the connection;
the supervisor replaces that generation. Group metrics are generation scoped.
The named video deadline qualification evidence directory is unavailable in this
checkout. No pinned replacement is recorded here; recover the original evidence
before relying on its tested-shape or recovery/performance claims.
