# Antfly Lite (legacy archive)

This legacy split-product archive contains the Apache-2.0 Antfly Lite executable, the Apache-2.0
inference worker, and the `libantfly` C ABI for embedding. The bundled native
library and worker must be kept together when installing the archive.

`antfly-lite help` lists the embedded database commands. The
`antfly-inference-worker` is a private executable used by embedded hosts for
isolated inference work; applications normally call inference through
`libantfly`. The separate Apache inference archive provides the public
`antfly-inference` CLI.

The product license is in `LICENSE`. Third-party licenses and notices are in
`LICENSES/third-party/` and `THIRD_PARTY_NOTICES.md`.
