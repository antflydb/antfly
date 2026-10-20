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
