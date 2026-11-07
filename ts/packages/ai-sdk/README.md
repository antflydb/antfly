# @antfly/ai-sdk

Antfly provider, tools, chat history persistence and the asset flow for the
Vercel AI SDK (`ai` >=7.0.162 <8, `zod` >=4.1.8 <5). Requires Node 24+.

```ts
import { createAntfly, createMemoryTools, createSearchTool, createSqlTool } from "@antfly/ai-sdk";
import { generateText } from "ai";

const antfly = createAntfly({ baseUrl: "http://localhost:8080" });

const result = await generateText({
  model: antfly.languageModel("google/gemma-3-1b-it"),
  prompt: "What's in the catalog table?",
  tools: {
    ...createSearchTool({ client: antfly.client, table: "catalog" }),
    ...createSqlTool({ client: antfly.client, database: "default" }), // read-only by default
  },
});
```

## Provider

`createAntfly()` returns a `ProviderV4` (`@ai-sdk/provider@4`):
`languageModel()` (over `/ai/v1/generate`, the same contract as
`/ai/v1/chat/completions`), `embeddingModel()` (`/ai/v1/embed`) and
`rerankingModel()` (`/ai/v1/rerank`). `imageModel()` always throws
`NoSuchModelError` -- Antfly has no image generation model, and `ProviderV4`
requires the method to exist. It also exposes `.client` (`AntflyClient`) and
`.inference` (`InferenceClient`) for building tools and history below.

Known limitations, documented rather than silently handled: `stopSequences`
and `seed` have no server-side equivalent and are reported as call warnings;
`supportedUrls` is empty, so non-image file parts must be inline bytes or an
asset reference (below), not a remote URL. Gateway errors (e.g. local
Gemma 4's one-video-per-request limit) surface as `InferenceAPIError` /
`InferenceCapacityError` from `@antfly/sdk`, not swallowed.

## Asset flow (antfly#1505)

References aren't signed and can come from a browser, while the server resolves
them with its own credential. So nothing resolves without your decision: pass
`authorizeAsset` to `createAntfly` or `languageModel(id, { authorizeAsset })`, or
`authorize` to the middleware, and check the current user against the reference
there. Create the model per request when the decision depends on the user.

A `FilePart`/`FileUIPart` can carry `providerReference: { antfly: <id> }`
instead of bytes or a URL. The `<id>` is an opaque, base64url-encoded
reference built by `assetProviderReference({ table, key })`. It names a catalog
row, never a URL, so a browser or model can't point the server at an arbitrary
address, and your `authorizeAsset` decides which rows each user can name.

```ts
import { assetProviderReference, createAntfly } from "@antfly/ai-sdk";
import { generateText } from "ai";

const antfly = createAntfly({ baseUrl: "http://localhost:8080" });
const reference = assetProviderReference({ table: "files", key: "<catalog row id>" });

const result = await generateText({
  model: antfly.languageModel("ggml-org/gemma-4-e2b-it-gguf"), // client passed by createAntfly
  messages: [
    {
      role: "user",
      content: [
        { type: "text", text: "Describe this video." },
        { type: "file", mediaType: "video/mp4", data: { type: "reference", reference } },
      ],
    },
  ],
});
```

- **To Antfly itself** (`AntflyChatLanguageModel`, constructed with a
  `client`): the reference resolves natively into the typed `source` content
  part sent to the Antfly gateway. The gateway fetches and inlines the pinned
  bytes server-side before calling the model.
- **To an external model**: wrap it with `withAssetMiddleware(model, { client })`
  (or `createAssetMiddleware` + `wrapLanguageModel`). It fetches the asset
  with `If-Match` and inlines it by default. Set `allowVendorUpload: true`
  and `files: <that provider's files API>` to upload once per provider and
  cache by verified content digest and MIME type instead -- off by default, because revoking
  access in Antfly does not remove a vendor's own copy. Concurrent references
  share one fetch/upload while each reference still checks authorization.
  Cancelling one caller leaves other callers running; cancelling all callers
  aborts the shared request. The default LRU cache holds at most 1024 entries.
  Scope any explicitly shared cache to a single vendor account. Without a
  verified `metadata.sha256`, caching stays scoped to the asset, URI, version
  and MIME type; a strong ETag alone is not a global content identity.
- **Uploading a new asset**: `uploadFile({ client, sources, sourceId,
  localRoot, table, data, mimeType })` writes the bytes under a registered
  source's local content-addressed store and triggers a per-source sync run,
  waiting for `waitFor: "published"` (default) or `"searchable"` (a
  best-effort poll -- publishing the catalog row is not the same as being
  enrichment-indexed, and the server has no durable signal for the latter).
  Publication preserves existing verified objects and their inode/mtime pins.
  New files and directories use owner-only permissions; file and directory
  durability barriers finish before sync. Corrupt existing content fails without
  overwriting it. The filesystem must support atomic hard links and directory
  fsync. Each upload starts a fresh inventory (`restart: true`), including after
  a failed run.
  The helper checks the configured source and looks up its exact record key
  with `read_index`, verifying ownership and the pinned content digest. Table
  size and other sources with identical bytes do not affect confirmation.
  Pass `prefix` when the source has a configured prefix. Searchable polling
  is restricted to this same record. `timeoutMs` is an overall deadline,
  including file writing and stalled requests; `signal` also cancels requests.
  Uploads accept the MIME types in the shared
  [`local_media_types.json`](../../../go/pkg/sources/local_media_types.json)
  catalogue (including FLAC), normalize common aliases, and reject unsupported
  types and empty media before writing. The local provider uses the same canonical MIME types.
  Only the `local` antfly-sources provider is implemented; a bucket-backed
  connection needs the equivalent provider-specific PUT. `FilesV4` is not
  implemented for Antfly: an existing object can't be re-registered through
  a generic files API, only through source registration.
- **Displaying an asset in a browser**: `createAssetDisplayHandler({ client,
  authorize, referenceId })` builds a plain `Request -> Response` handler
  for a route in your own app (the pattern settled on antfly#1491).
  GET and HEAD preserve upstream method semantics; request cancellation also
  cancels catalog lookup and upstream delivery.
  `FileUIPart.url` points at that route; the capability URL never reaches
  the browser, and Antfly never fetches through it. Passive image, audio, and
  video formats use an explicit inline allowlist; HTML, SVG, and other formats
  download as attachments with a sandbox policy. Responses are private and
  uncached so each request checks authorization.
- **Known codes**: `src/asset/errors.ts` exports guards
  (`isVideoCodecUnavailable`, `isOneVideoPerRequest`,
  `isContextManagementWithMedia`, `isRetryableCapacityError`, ...) over the
  codes this flow can see, verified by reading
  `zig/pkg/antfly/src/api/httpx_handler.zig` and
  `zig/pkg/inference/src/server/server.zig` rather than guessed.

## Tools

`createMemoryTools`, `createSearchTool`, `createGraphTool` and `createSqlTool`
each take their scope (memory space, table, database/namespace) as
constructor options, not model-controllable tool arguments, per antfly#1505.

- **Memory** (`/memory/v1`): `recall`, `remember` (an observation) and
  `forget` (destructive, `needsApproval: true`). Covers only those three
  operations; the rest of the memory API (assertions, corrections,
  supersession, evaluation, lineage, entities, ...) is out of scope here.
- **Search**: full-text, or hybrid full-text + semantic (RRF) when `indexes`
  is configured -- the server embeds the query string itself.
- **Graph**: bounded canonical Antfly graph JSON (`match`/`traverse`/
  `shortest_path`/`k_shortest_paths`) against one fixed table/index. Cypher
  is not supported, matching `@antfly/langchain`'s graph tool.
- **SQL**: Antfly's normal permissions decide what a statement can do: every
  table a statement touches is checked against the caller's `read` or `write`
  grant. Give the tool an API key scoped to `"operations": ["read"]` (see
  `docs/reference/auth.md`). The tool also refuses statements that look like
  writes unless constructed with `readOnly: false`. That check is a guard against
  mistakes when a key is broader than it should be, or when a local server runs
  with authentication off and permissions aren't checked at all. It isn't a
  security boundary. Writing is never a tool argument.

Tool execution forwards the AI SDK cancellation signal to Antfly requests,
including memory mutations. Cancellation after a write has committed does not
undo it.

## Chat history

`createChatHistory({ client, table })` stores `ai` v7 `UIMessage[]` in a
fixed Antfly table, keyed by chat ID, for use with
`streamText(...).toUIMessageStreamResponse({ onFinish: history.onFinish(chatId) })`
and `load(chatId)` on the next request. Writes use `sync_level: "full_index"`:
on Lite storage a default-`sync_level` write may not be visible for a few
milliseconds, and chat history must read its own writes, so this trades a
little write latency for that guarantee rather than adding a read-side wait.

## Versions

`ai@^7`, Node only (no edge runtime yet -- see antfly#1505). Automatic memory
(middleware-driven context injection) is off; this package only ships
explicit memory tools.

## Development

From `ts/`: `pnpm --filter @antfly/ai-sdk build`, `pnpm --filter @antfly/ai-sdk test`.
`pnpm --filter @antfly/ai-sdk typecheck` also compile-checks the provider and
each model against the installed `@ai-sdk/provider` types.
