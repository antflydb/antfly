# Antfly licensing

Antfly is a mixed-license repository. The root `LICENSE` is Elastic License 2.0
(ELv2), the default for the database server. File headers, package licenses and
the explicit Apache source list below identify exceptions.

## Apache License 2.0

The embedded database engine, Antfly Lite file-oriented CLI, native `libantfly`,
Zig embedding package, language bindings, and inference implementation and
executable are Apache-2.0. The engine includes local storage, search/indexing,
schemas, transactions, enrichment, maintenance, portable backups, and the SQL
compiler and execution engine used by the embedded C API. SQL HTTP/pgwire
adapters and distributed SQL coordination remain server-owned.

First-party engine sources live in `zig/pkg/antfly-embedded`, inference execution
in `zig/pkg/inference`, and reusable libraries in `zig/lib`. This includes native
Parquet/Iceberg lake readers and SQL cursors, and shared AWS/Google authentication.
The server package `zig/pkg/antfly` remains ELv2 and has no Apache source exceptions.
The source map [`scripts/source_license_roots.json`](scripts/source_license_roots.json)
declares package roots; [`scripts/apache_engine_files.txt`](scripts/apache_engine_files.txt)
records additional Apache files outside those roots. Shared build composition lives
in `zig/build_support`. Shared generated OpenAPI contracts belong to the embedded,
client, inference and server-API Apache packages according to their consumers;
their authored schema inputs are covered by `specs/LICENSE`.

The inference package, shared `zig/lib` implementations, client SDKs, Lite
bindings, and their existing Apache packages remain Apache. The full license
text is in [`LICENSES/Apache-2.0.txt`](LICENSES/Apache-2.0.txt).

## Elastic License 2.0

The [ELv2 license text](LICENSES/Elastic-2.0.txt) covers the standalone database
server, database HTTP serving, distributed control, cluster metadata service,
placement, hot standby and replication orchestration, and serverless
orchestration. These server owners consume the Apache engine. Portable
replication records, receipts, durable outboxes and borrowed commit interfaces
remain Apache engine contracts.

`lite serve` has been removed. Serve a Lite database with the ELv2 server:

```sh
antfly standalone --storage-engine lite --storage-path app.aflite
```

The independently runnable inference service is Apache; it is distinct from
the standalone database server. An Apache engine license permits third parties
to build and host their own services using that engine.

## Distributions and third-party material

Apache Lite archives contain the Apache LICENSE, the source license list,
and relevant third-party notices. This file is the repository's mixed-license
map. Full server archives retain the ELv2 LICENSE and also carry the Apache
license for their shared engine and native library. Language binding licenses
cover both binding and Apache native engine code, while third-party components
retain their original terms.

`THIRD_PARTY_NOTICES.md` records separately licensed adaptations and data,
including the BSD Snowball stemmers, MIT httpx, and the OpenLDAP-licensed C LMDB
test oracle in `zig/deps/lmdb`. The standalone Zig LMDB-compatible library in
`zig/lib/lmdb` is Apache-2.0. Generated Snowball sources retain their upstream license and
are deliberately excluded from the first-party Apache source list.
Model weights, tokenizer assets, datasets, GPU drivers, and optional external
runtime libraries retain their own licenses; they are not relicensed by an
Apache source header. Product source licensing does not certify every optional
runtime or bundled model as Apache-2.0.

Run `make apache-license-check` to check the Apache source list, its headers,
and reject ELv2 imports into the Apache engine owners. `make license-check`
also checks all existing first-party headers. This source check supplements native builds,
binding conformance tests, and release artifact inspection.

Frozen qualification helpers retain their recorded bytes and are covered by the
Apache inference package license. The header check validates their provenance
manifests instead of rewriting those sources or historical evidence. Complete
upstream notices are checked against `LICENSES/third-party` and against the
shared binary notice bundle. Product installs include those canonical notices.

The PDF engine embeds unmodified Apache-2.0 Roboto 2 fallback fonts from
`zig/lib/pdf/fonts/roboto`, independently of server UI fonts.
`scripts/embedded_asset_licenses.json` records their immutable upstream sources,
byte identities, and notices. The dependency checker rejects unreviewed embedded
fonts and embedded assets outside the Apache package/source scope. Unicode-derived
tables retain the full Unicode copyright and permission notice in generated
source and in `LICENSES/third-party/Unicode-V3.txt` in product distributions.

The embedded database WASM bundle and inference WASM modules (wasm32 and
wasm64) use the Apache first-party engine and inference sources. Their installs
include the Apache LICENSE, source map, asset manifest, and canonical third-party
notices. The SciPy-derived assignment solver retains its BSD-3-Clause notice in
source and in native and WASM distributions.

The public C API lives under `zig/pkg/antfly-embedded/src/local/capi` and
uses the embedded package’s `public_capi_root.zig` and `capi_embedded_root.zig`. Private server operations use the ELv2
`capi/server_owner.zig` and `storage/server_db_adapter.zig`. Borrowed read
consistency is local storage code; quorum tracking and Raft snapshot protocol
adapters remain server code. Explicit test-only imports do not expand a
product’s production license closure. The full Zig suite builds Lite, public C API, native lake readers and WASM
with the entire server package omitted, and builds the independent inference
product in the same staged tree. There are no server stubs. Import audits also
reject production dependencies on server sources and unreviewed assets. This
validation runs in the full suite on main merges rather than on every PR.
