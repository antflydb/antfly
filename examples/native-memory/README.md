# Native memory

Antfly's native memory service (`/memory/v1`) stores what an agent learns as
typed claims with evidence: who said it, when it was true, and which later
claim replaced it. Recall returns the current answer and, on request, the
history behind it. The same actions run in-process on an embedded `.aflite`
file or over HTTP against a server, and MCP clients reach them at
`/memory/v1/mcp`.

For a step-by-step walkthrough over HTTP and embedded, see
[Give an Agent Long-Term Memory](../../docs/guides/agent-memory.mdx).

The memory contract is documented in [zig/MEMORY.md](../../zig/MEMORY.md):
evidence and temporal policy, identity and supersession judging, provenance,
and recorded explanations.

## A complete application: Codex on Antfly

[antflydb/codex](https://github.com/antflydb/codex/tree/antfly/integration), a
fork of OpenAI's Codex CLI, is the full worked example. On its
`antfly/integration` branch Codex keeps all of its state in Antfly instead of
SQLite and rollout files:

- **Knowledge across sessions.** Codex extracts decisions, rationale,
  preferences, lessons and constraints from sessions into a knowledge graph
  of claims and concepts, tracks which claims supersede others, and exposes
  it to the agent as a `knowledge.search` tool. Its client for the native
  memory service runs identity, relation and supersession judging through
  `/memory/v1`.
- **Decisions.** A local approval reviewer asks a typed-decision model what
  the worst effect of a tool call would be and records each verdict and its
  outcome, so approvals can be calibrated against precedent.
- **Searchable history.** Session history is indexed for full-text and
  semantic search.
- **Embedded or server.** One setting switches between a local `.aflite` file
  through `libantfly` and a hosted Antfly instance over HTTP and the
  PostgreSQL wire protocol.

The design, configuration, measurements and Antfly findings are in
[ANTFLY_INTEGRATION.md](https://github.com/antflydb/codex/blob/antfly/integration/ANTFLY_INTEGRATION.md).

For a smaller starting point that builds a knowledge graph with search over a
document corpus, see [antfly-docs-knowledge-base](../antfly-docs-knowledge-base/).
