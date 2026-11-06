# @antfly/ai-sdk

Antfly provider, tools and chat history persistence for the Vercel AI SDK
(`ai` v7). Requires Node 24+. This is the first PR for this package
(antfly#1505): the asset/source-reference flow (file parts, `providerReference`,
uploads) is out of scope here and lands separately.

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
`supportedUrls` is empty, so non-image file parts must be inline bytes, not
remote URLs (see the asset-flow note above). Gateway errors (e.g. local
Gemma 4's one-video-per-request limit) surface as `InferenceAPIError` /
`InferenceCapacityError` from `@antfly/sdk`, not swallowed.

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
- **SQL**: read-only by default. This is enforced two ways: a client-side
  statement-shape check (not a security boundary -- documented on
  `AntflySqlToolOptions.readOnly`), and, for real enforcement, the
  credential's own server-side per-table `read`/`write` grant (checked in
  `zig/pkg/antfly/src/api/sql_execution.zig`). Construct the tool with a
  credential that only has `read` grants if you need that guarantee. Writing
  requires `readOnly: false` in code, never a tool argument.

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
