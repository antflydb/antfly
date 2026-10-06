# Antfly Embedded

This Apache-2.0 archive contains the embedded database and inference runtime:

- `antfly-lite`: file-oriented database CLI; run `./antfly-lite help`.
- `antfly-inference`: public inference CLI; run `./antfly-inference --help`
  for commands including `run`, `embed`, `generate`, `chat`, `list`, and `pull`.
- `lib/libantfly` and `include/antfly.h`: the combined database and inference C API.
- `antfly-inference-worker`: private executable for isolated inference work.

Keep the native library, private worker, and runtime files together when
installing. Python and npm packages bundle the library and private worker without
installing public CLI commands. Go and Rust consumers link the same library.
Database HTTP serving belongs to the separate ELv2 Antfly server.

The product license is in `LICENSE`. Third-party licenses and notices are in
`LICENSES/third-party/` and `THIRD_PARTY_NOTICES.md`.
