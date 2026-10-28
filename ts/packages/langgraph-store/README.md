# @antfly/langgraph-store

Persistent long-term memory for LangGraph.js, backed by Antfly. Requires Node 24+,
`@langchain/core >=1.3 <2` and `@langchain/langgraph >=1.5.3 <2`.

```ts
import { AntflyClient } from "@antfly/sdk";
import { AntflyStore } from "@antfly/langgraph-store";

const client = new AntflyClient({ baseUrl: "http://localhost:8080" });
const store = await AntflyStore.create({ client, table: "agent_memory" });
await store.put(["users", "alice"], "preference", { food: "apples" });
const memory = await store.get(["users", "alice"], "preference");
// Pass store to graph.compile({ store }); nodes/tools use runtime/config.store.
```

Constructors attach without creating resources. A new instance using the same
table and namespace reads persisted memory across processes and conversations.
Supply user identity from trusted application context, not model tool arguments.
See `examples/agent-memory.ts` for an actual compiled StateGraph; run with
`ANTFLY_URL`, `ANTFLY_TABLE`, `USER_ID` and optional `REMEMBER`. Pass `--create`
only for the initial run.

The adapter supports component-safe namespaces, ordered mixed batches, get/put/
delete, search filters, pagination, namespace prefix/suffix wildcards, timestamps
and index controls. A mixed batch is sequential, not an atomic transaction. Later
reads see completed earlier writes. Concurrent value writes to the same item are last writer wins. No per-item TTL or execution checkpointing is implemented.

Configure `index: { embeddings, dims, fields: ["$"] }` for semantic search using
any LangChain embeddings implementation. Paths support nested properties,
wildcard array elements and negative array indices. Per-put `false` disables
indexing; a path list overrides default fields. Multiple fields produce separate
vectors; the best cosine similarity ranks each item. Filters apply before
pagination. Supported operators: `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`.

Like the Python reference, single-field namespaces use Antfly's native cosine
index with server-side namespace and value filters. Once a namespace contains a
multi-field item, a persistent marker selects exact paginated maximum-cosine
pooling. Markers remain after updates/deletes for cross-process correctness.
This fallback costs O(namespace records); query pages contain the values, so
there is no lookup request per item. Namespace listing likewise reads pages.
`create` provisions the external cosine index (`indexName`, default
`memory_vectors`). For tables created by the earlier unindexed prototype,
explicitly provision that index and re-put all memories before semantic search.
Changing embedding models requires reindexing values. Atomic transforms preserve
creation timestamps and monotonic update times, following Python's behavior.

Build/test from `ts`: `pnpm --filter @antfly/langgraph-store... build`, then
`pnpm --filter @antfly/langgraph-store test`. Set `ANTFLY_LIVE=1` and `ANTFLY_URL`
to run the two-user runtime integration test. Tests create/drop unique tables.
The independent package verifier is shared with `@antfly/langchain` and verifies
all three SDK/integration tarballs. Publish SDK, LangChain, then this package via
the `ts/antfly/langgraph-store/v*` tag workflow after registry configuration.
