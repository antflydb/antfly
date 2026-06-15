# MemoryAF 0.0.1 Extension Package

This is a reference Antfly extension package for MemoryAF. It declares the MemoryAF data shape, MCP tools, and a WASM runtime artifact that Antfly can host behind `/mcp/v1/extensions/memoryaf`.

The checked-in Rust crate is intentionally small. It uses `wit-bindgen` to export the generic `antfly-extension/v1` WIT world:

- `init(config-json: string) -> result<_, string>`
- `call-tool(name: string, request-json: string) -> result<tool-result, string>`

The WIT file in `wit/antfly-extension.wit` is the runtime ABI Antfly hosts through Wasmtime's component model.

## Build

```bash
rustup target add wasm32-wasip2
cargo build --release --target wasm32-wasip2
```

The manifest expects the compiled artifact at:

```text
target/wasm32-wasip2/release/memoryaf_extension.wasm
```

## Tools

The package declares three MCP tools:

- `store_memory`
- `search_memories`
- `list_memories`

The current Rust implementation returns planned host calls such as `db.write(memory_record)` and `ai.embed(content)`. Those planned calls should become real imported host calls through the WIT ABI as host capabilities are added.

## Package Layout

```text
extensions/memoryaf/0.0.1/
  extension.json
  Cargo.toml
  src/lib.rs
  wit/antfly-extension.wit
```
