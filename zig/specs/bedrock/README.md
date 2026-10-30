# Bedrock Runtime wire contract

AWS's authoritative API contract is [Smithy](https://github.com/aws/api-models-aws),
which AWS uses to generate its SDKs. This directory vendors the unmodified
Bedrock Runtime `2023-09-30` JSON AST and upstream Apache-2.0 license.
`sources.json` pins the upstream commit and SHA-256 hashes of both the source
model and the derived `../bedrock-openapi.json`.

`scripts/generate_bedrock_openapi.py` is a focused, offline, standard-library-only
projection of the shapes reachable from `Converse` and `ConverseStream`. This is
an Antfly-generated OpenAPI input, not an upstream AWS OpenAPI publication or a
general-purpose Smithy converter. It follows the
[AWS restJson1 wire protocol](https://smithy.io/2.0/aws/protocols/aws-restjson1-protocol.html):

- `modelId` belongs in the URL path, never the JSON request body.
- Structures retain required fields, JSON wire names, and constraints. Maps,
  lists, enums, base64 blob strings, and arbitrary JSON documents retain their
  wire representation. Unsupported reachable types and HTTP bindings fail export.
- Each union becomes `oneOf` of named, single-member object wrappers. These
  wrappers let openapi-zig generate structural Zig unions without inventing a
  discriminator field that AWS does not send.
- `ConverseStream` returns binary AWS EventStream. An `x-aws-event-payload`
  annotation references the union of decoded event envelopes; it does not claim
  that the HTTP body is JSON or SSE. CRC checks, frame bounds, exception mapping,
  stream ordering, cancellation, and SSE translation stay in the existing adapter.

`bedrock_api` validates outgoing Converse and ConverseStream bodies, completed
response envelopes, and each decoded streaming event before publication. Request
bodies are assembled from supported fields; both request and response validation
permit opaque fields inside native blocks for upstream compatibility. The adapter
still enforces admitted content variants, visible-turn replay matching, known
field types, and stream lifecycle rules. Serialization retains the original
request tree, preserving reasoning signatures and document-valued schemas. Optional
JSON documents distinguish omission from explicit null, and retain scalar and
container values. Generated types
check field types, required fields, and enums; numeric/string bounds and model
capabilities still require application-level checks.

The contract intentionally excludes `InvokeModel` and the Bedrock control plane.
`InvokeModel` payloads are model-specific, so Titan/Cohere embedding payloads and
other model-specific adapters keep their separate contracts. Bedrock Runtime is
a native HTTP adapter; no new WASM transport or AWS SDK dependency is introduced.
Existing HTTP pooling, credential discovery, SigV4, and bearer authentication are
unchanged.

To update:

1. Select and review an upstream `aws/api-models-aws` commit. Download its
   `models/bedrock-runtime/service/2023-09-30/bedrock-runtime-2023-09-30.json`
   and `LICENSE` into this directory; set `REVISION` in the exporter.
2. Run `python3 scripts/generate_bedrock_openapi.py`.
3. Run `uv run --project scripts --locked python -m unittest
   scripts/test_generate_bedrock_openapi.py`.
4. From `zig/`, run `zig build regen-openapi`, then
   `zig build check-openapi antfly-inference-bedrock-test antfly-generating-test`.
   Review changes to the native HTTP/signing and streaming fixtures.

`python3 scripts/generate_bedrock_openapi.py --check` verifies the projection and
provenance without network access. Both `regen-openapi` and `check-openapi` run
this check so a changed source model cannot silently leave stale generated input.
