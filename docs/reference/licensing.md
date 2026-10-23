# Licensing maintenance

All first-party Antfly code is Apache-2.0, including the server, distributed and
serverless runtime, embedded database, inference, CLI, SDKs, and bindings.
See [LICENSING.md](../../LICENSING.md) for third-party scope and maintenance checks.

## Build and distribution

`antfly` is the public database and inference executable: `antfly lite`, `antfly inference`, and
`antfly standalone` share the same engine. `libantfly` exposes in-process
database and inference APIs; language bindings retain the `antfly-embedded`
package names. They do not require the CLI.

Source build contract 6 declares `server` and `embedded` archive products.
The server archive contains `antfly`, `libantfly`, headers, assets, and notices.
The embedded archive contains the library, header, pkg-config metadata, assets,
and notices, with no executable or worker. Historical source contracts retain
their recorded layouts so immutable release payloads remain verifiable.

`package_lite_release.py` builds embedded Python wheels and npm platform
packages from the native library archive. `verify_lite_release.py` checks their
library bytes and notices against the archive, and rejects CLI executables.
The existing release controller seals commit-bound package snapshots and
publishes stable and RC versions; nightlies do not publish registry versions.
See [registry publishing](registry-publishing.md) for package ordering.

## Source ownership

`zig/pkg/antfly-embedded` owns local database operations and the public C API;
`zig/pkg/inference` owns model execution; `zig/lib` owns reusable libraries;
`zig/pkg/antfly` owns server adapters and distributed coordination. These are
architecture boundaries under one first-party license. The independent native
and WASM builds keep embedding consumers free of server dependencies.

The source-root map, additional Apache-file list, preserved-notice manifest,
asset manifest, and frozen qualification manifests remain authoritative for
header normalization and provenance checks. Third-party notices and recorded
evidence bytes must remain intact.
