# Anthropic Messages wire contract

`../anthropic-openapi.json` is derived from the typed models in Anthropic's
[official Python SDK v2.3.0](https://github.com/anthropics/anthropic-sdk-python/tree/v2.3.0),
matching the adapter's `anthropic-version: 2026-10-21` header. It is an SDK-derived
OpenAPI contract, not an unmodified upstream OpenAPI download. The historical
OpenAPI URLs published in the official SDK's `.stats.yml` return HTTP 403, and
current SDK releases no longer publish that URL. SDK license terms are in `LICENSE`.

`scripts/generate_anthropic_openapi.py` exports the official request TypedDict
and response model using pinned Pydantic and SDK versions. It lifts their
`$defs` into OpenAPI components without changing field definitions, combines
the streaming/non-streaming request variants through their common base and a
boolean `stream`, and adds the Messages endpoint wrapper. The script's uv lock
pins all dependencies; `sources.json` records the SDK revision and artifact hash.

`make generate` generates `anthropic_api` types from the committed JSON without
installing the SDK or accessing the network. Native and WASM generation module
graphs import those types. The adapter validates request/response envelopes,
custom tools, and JSON output formats against them. It serializes the original
request object to preserve signed replay blocks and compatible extension fields;
content translation and stream lifecycle validation remain handwritten.

OpenAPI does not establish model capabilities or the strict-output JSON Schema
subset. `zig/lib/structured/root.zig` separately selects native JSON output for
Claude Opus 5.5, Sonnet 5.5, and Fable 5.1 (which reject forced tools), including
Bedrock model IDs and inference profiles. Other admitted Claude models retain
forced tool support. Unsupported strict-schema bounds move into descriptions on
the wire while the original schema remains mandatory for local validation and
selective repair. Open objects and unsupported combinators are rejected before I/O.

To update:

1. Review the official SDK release and its API-version compatibility. Update the
   pinned versions, SDK commit, and API version in the exporter and lock file.
2. Run `uv lock --script scripts/generate_anthropic_openapi.py`, then
   `uv run --locked scripts/generate_anthropic_openapi.py`.
3. Run `make generate`, `make zig-openapi-check`, and the `lib-structured-test`,
   `lib-extracting-test`, `antfly-generating-test` and `antfly-inference-bedrock-test`
   Zig build steps. Review the HTTP fixtures for the actual native payloads.
4. Check the model-specific forced-tool restrictions and strict-schema dialect
   separately against Anthropic's docs; schema generation cannot verify them.

To check the SDK-derived input itself, run
`uv run --locked scripts/generate_anthropic_openapi.py --check`.
