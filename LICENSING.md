# Antfly licensing

All first-party Antfly code is licensed under the [Apache License 2.0](LICENSE).
This includes the database server and HTTP APIs, distributed coordination,
replication, serverless orchestration, local storage and search engine, SQL,
inference, CLI, SDKs, language bindings, and native and browser libraries.

## CLI and library

`antfly` is the public database and inference executable. Its `lite` commands manage local `.aflite`
files, its `inference` commands run models and the inference service, and
`standalone` starts the database server. Serve a Lite file with:

```sh
antfly standalone --storage-engine lite --storage-path app.aflite
```

`libantfly` provides the in-process database and inference C API. Language
bindings retain the `antfly-embedded` and `@antfly/embedded` package names.
The embedded native archive contains the library, public header, pkg-config
metadata, runtime assets, and notices; it has no CLI or worker executable.
The library runs inference in-process and does not require the `antfly` CLI.

The engine remains independently buildable without server implementations.
`zig/pkg/antfly-embedded` owns local database operations and the public C API;
`zig/pkg/inference` owns model execution; `zig/lib` contains shared libraries;
`zig/pkg/antfly` owns serving and distributed coordination. These are source
ownership boundaries under the same license.

## Third-party material

Third-party components retain their original licenses. See
[THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md) and
[LICENSES/third-party](LICENSES/third-party) for the canonical notices, including
Snowball, httpx, the OpenLDAP-licensed C LMDB test oracle, and adapted algorithms.
Model weights, tokenizer assets, datasets, GPU drivers, and optional external
runtime libraries retain their own licenses.

[scripts/source_license_roots.json](scripts/source_license_roots.json) records
first-party package roots. Additional files outside those roots appear in
[scripts/apache_engine_files.txt](scripts/apache_engine_files.txt). Preserved
upstream notices, frozen qualification evidence, and embedded assets have
separate provenance manifests; header normalization must not replace them.

Run `make license-check` to validate first-party headers and preserved notices.
`make apache-license-check` also checks embedded dependency and asset ownership.
The independent embedded native/WASM build verifies that library consumers do
not require server sources. Release archives and language packages carry the
Apache license and applicable third-party notices.
