# Fastino wire schemas

`../fastino-openapi.json` is an unmodified snapshot of Fastino's official
[OpenAPI 3.1 specification](https://api.fastino.ai/openapi.json).
`sources.json` records its source URL, API version, and SHA-256 hash.
The complete upstream snapshot is retained so schema updates can be reviewed
without maintaining a separate, manually narrowed contract.

`make generate` generates `fastino_api` through the same types-only path as
Cloudflare and OpenAI. The Pioneer adapter uses `ChatCompletionRequest`,
`ChatMessage`, and `ChatCompletionResponse` with Antfly's pooled HTTP client,
secret resolution, deadlines, and cancellation. No second HTTP client is generated.
Both the native and WASM extraction module graphs import the generated types.

The upstream `schema` field and response choice `message` are free-form JSON.
GLiNER schema compilation and result decoding therefore remain handwritten in
`zig/lib/extracting/src/pioneer.zig`. These checks still reject unsupported
features and validate output shapes, scores, and source spans. In particular,
GLiNER's multi-label `cls_threshold` belongs inside each classification task;
the request-level `threshold` controls span extraction.

GLiNER also indexes intermediate results by task name before grouping relations.
The adapter rejects relation names shared with classification or structure tasks
and reserves `entities` and `relation_extraction` for protocol sections. Its
decoder normalizes the zero-instance structure result `{}` to an empty record
array, while retaining nonempty single-object records and validating explicit
record arrays. The Pioneer fixture tests cover these free-form conventions in
addition to the generated envelope contract.

To update:

1. Download `https://api.fastino.ai/openapi.json` to `zig/specs/fastino-openapi.json`.
2. Review the upstream changes and update the version and SHA-256 in `sources.json`.
3. Run `make generate` and `make zig-openapi-check`.
4. Run `cd zig && zig build lib-extracting-test lib-structured-test` and review
   the Pioneer request/response fixtures. Generated types cannot verify GLiNER's
   free-form content contract; qualify changes to that contract separately.
