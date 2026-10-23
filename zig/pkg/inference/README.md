# Antfly inference

Antfly's local inference engine provides embeddings, reranking, classification,
extraction, generation, transcription, and model management. All first-party
Antfly code is Apache-2.0.

Use the public `antfly` CLI:

```sh
antfly inference pull <model>
antfly inference list
antfly inference chat <model>
antfly inference run
antfly inference --help
```

`libantfly` exposes the same runtime in-process through the C API and embedded
language bindings. Neither the library nor its packages require a CLI executable.
See the [repository README](../../../README.md) and [C API](../../CAPI.md).

From `zig/`, build the public CLI with `zig build antfly`, the embedding library
with `zig build libantfly`, or both with `zig build release`.

For backend development and qualification, `zig build` inside this directory
builds `zig-out/bin/antfly-inference-dev`. It accepts direct commands such as
`antfly-inference-dev run`; benchmark and qualification scripts use it to avoid
building server adapters. It is a development tool and is not shipped in native
release archives or language packages. Browser inference retains its independent
WASM build targets.
