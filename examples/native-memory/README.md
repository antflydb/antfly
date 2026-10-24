# Native memory without a model

This example exercises the same evidence/temporal policy over HTTP or the direct
embedded SDK. It imports newer deployment guidance before older guidance and
checks that current and historical recall remain correct.

```sh
# From the repository root, after installing the Python SDK:
python examples/native-memory/main.py --url http://localhost:8080 --space demo

# Embedded: install the embedded Python SDK and build/provide libantfly first.
python examples/native-memory/main.py --database demo.aflite --space demo
```

The HTTP SDK uses the server's configured authentication; set `ANTFLY_TOKEN` when
needed. Embedded access is governed by ownership of the database file. No model,
Go memory server, extension package or Wasmtime runtime is required.

For `docsaf`, map each source version/section to an immutable evidence ID and
retain its source URL, author and event time. Submit externally produced typed
assertions under external mode, or explicitly enable a qualified managed pipeline.
See [MEMORY.md](../../zig/MEMORY.md) for the complete native contract and research.

The example also prints a recorded explanation. To import an ordinary business
row, submit evidence with `resource_selector={"table": "business", "id": "row-id"}`
instead of copying its text. Native admission captures and pins the authorized
row version; add `expected_revision` to reject concurrent changes. Call
`memory.execute(space, "explain", id=id, body={"revision": revision})` for a historical explanation.

The Python SDK's `trace_event` and `resource_reference` helpers build versioned
ordinary-table rows and typed provenance links. Keep `retrieved`, `used`, and
`supported_by` separate. Writing a trace row alone does not admit a memory. See
[the shared provenance design](../../zig/MEMORY.md#shared-native-provenance-and-recorded-explanations)
for authorization, retention, source-change behavior, and completeness flags.

MCP clients connect to `/memory/v1/mcp`. Discover typed `memory_observe`,
`memory_recall`, `memory_explain` and the other native actions with `tools/list`.
Arguments use `{space, body}` plus `id` for resource operations and
`idempotency_key` for mutations. Memory tools and sessions are separate from the
ordinary database MCP server at `/mcp/v1`.
