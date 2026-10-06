# Licensing maintenance

See [LICENSING.md](../../LICENSING.md) for the license scope and
[source license roots](../../scripts/source_license_roots.json) for package
ownership and [additional Apache files](../../scripts/apache_engine_files.txt)
for files outside those roots.

## Products

The shared local database engine, embedded Lite APIs, native `libantfly`,
file-oriented Lite CLI, browser client and WebGPU shaders, language bindings,
and inference implementation and executable are Apache-2.0. Original licenses
remain in effect for third-party material, including bundled Snowball, httpx,
and the C LMDB oracle. The standalone Zig LMDB-compatible library under
`zig/lib/lmdb` is Apache-2.0; its upstream C test oracle under `zig/deps/lmdb`
retains the OpenLDAP Public License 2.8.

The standalone database server, database HTTP serving, distributed control,
cluster metadata service, placement, replication orchestration, and serverless
orchestration remain ELv2. The Apache engine can also be used by third parties
to build and host their own services.

`lite serve` is removed from both executables. Database HTTP serving uses the
existing standalone server:

```sh
antfly standalone --storage-engine lite --storage-path app.aflite
```

Independently runnable inference, including its inference-specific APIs,
remains Apache. It is distinct from the standalone database server.

## Implementation

The local DB, native SQL/lake readers, file CLI and public C API live in
`zig/pkg/antfly-embedded/src/local`. Model execution lives in `zig/pkg/inference`;
AWS and Google authentication mechanics live in shared libraries. Server Raft,
hot standby, provisioning, managed credential resolution and cluster publication
remain in `zig/pkg/antfly`. There are no per-file Apache exceptions in the server
package and no duplicate engine implementation.

`public_capi_root.zig` owns the public native C ABI, linking native enrichment
and inference archives independently of the server storage owner. The independent
CLI supports file commands and hidden worker re-execution. The full server
consumes the same local implementation through its source catalog.

The embedded package, inference package, shared libraries, schema inputs, and
generated OpenAPI contracts have explicit Apache licenses. Lite installation
and inference installation carry license and third-party notices.
`build_zig_release_archive.sh --product embedded` packages both public CLIs
(`antfly-lite` and `antfly-inference`), `libantfly`, `include/antfly.h`, and the
private `antfly-inference-worker`, with runtime files and license notices.
The inference CLI exposes commands such as `run`, `embed`, `generate`, and `pull`.
The default server archive retains ELv2 and includes the Apache license for the
shared engine and native library.

Release source contract schema 3 declares `server` and `embedded` products and
requires the Apache package and licensing inputs. The trusted controller validates
the contract at the exact source commit, builds its declared products, and records
the contract version in the immutable release request. Schema 3 promotion requires
one matching `antfly-embedded` archive for every server platform; missing embedded
artifacts fail the release.

Older contracts retain their immutable layouts. Schema 1 builds server archives
and CLI packages and skips embedded package assembly. Schema 2 builds separate
Lite and inference archives and requires both for every server platform. Its
package publisher verifies against the historical Lite archives. A contract
version is never reinterpreted as a different artifact layout.

The schema 3 release build creates a combined Apache Embedded archive for each
platform. `package_lite_release.py` assembles
platform wheels for `antfly-embedded` and native npm packages for
`@antfly/embedded`;
the bindings discover these artifacts without using the ELv2 server packages.
Both Rust embedded crates include the canonical Apache license in their Cargo
package; Go and Rust consumers obtain the native library from these archives or
a local build. The Rust SDK bundles its OpenAPI build input inside its crate;
`make generate` refreshes it from the joined public spec and SDK CI verifies both
synchronization and the build of the published archive. Publish `antfly-sdk`
before `antfly-postgres`, whose registry dependency requires SDK version `0.1.0`.
`verify_lite_release.py` compares each wheel and npm package with its Embedded
archive and rejects server executables and ELv2 license files. Both package
formats carry the package roots, additional-file map, and asset manifest in
`LICENSES/source-map`; verification compares those files with the archive. The immutable
package snapshot is produced by `.github/workflows/embedded-package.yml` as part
of the release build. After a successful tagged release build, dispatch
`.github/workflows/embedded-release-publish.yml` on `main` with that tag and build
run ID. It authenticates the successful release-controller workflow on `main`,
then checks its commit-bound release request against the immutable tag and the
protected release-source history. It verifies package hashes and archive equivalence
before publishing `antfly-embedded` wheels to PyPI and the
`@antfly/embedded` platform and selector packages to npm. The PyPI project and
each npm package must have trusted publishing configured for this workflow in
their registry settings.

Before the first release, the PyPI account owner must configure a pending
trusted publisher for project `antfly-embedded`, repository
`antflydb/antfly`, workflow `embedded-release-publish.yml`, and GitHub environment
`pypi`. A pending publisher does not reserve the name: the first successful
upload creates the project.

The crates.io names `antfly-sdk`, `antfly-embedded`, `antfly-embedded-sys`,
and `antfly-postgres` were established with `0.0.0` claim packages on
2026-10-05. These are setup versions with no runtime API; functional crates
live under `rs/crates`. The PostgreSQL crate is `antfly-postgres`, while its
Rust library, SQL extension and query-builder schema use `antfly_postgres`.
Crates.io organization ownership uses the existing GitHub team
`github:antflydb:engineering`, not a separate crates.io organization. All four
crates were verified with both that team and `ajroetker` as owners. Keep a
personal owner for ownership administration; team owners can publish and yank.
See [registry claims](../../registry-claims/README.md) for verification commands.

The four npm packages (`@antfly/embedded` and its Darwin ARM64, Linux ARM64,
and Linux x64 platform packages) were established with nonfunctional `0.0.0`
setup versions on 2026-10-05. All four trusted publishers are configured for
`antflydb/antfly`, `embedded-release-publish.yml`, environment `npm`.
Registry ownership and trusted-publisher settings are external account state;
verify them in the registries before promoting a release.

## Verification

```sh
make apache-license-check
python3 -m unittest discover -s scripts -p test_check_apache_boundary.py
python3 -m unittest discover -s scripts/packaging -p 'test_*.py'
cd zig
zig build --build-file embedded.build.zig lite -Doptimize=Debug -Dmetal=false -j1
zig build --build-file embedded.build.zig embedded-lake-test embedded-package-test aws-credentials-test capi-smoke capi-conformance embedded-capi-check wasm-test -Doptimize=Debug -Dmetal=false -j1
cd pkg/inference
zig build -Doptimize=Debug -Dmetal=false -j1
zig build test-cli -Doptimize=Debug -Dmetal=false -j1
```

The dependency check rejects ELv2 source edges, missing sources, and unreviewed
named modules. It verifies the Apache manifest's headers and detects retained
ELv2 notices. Release assembly regression tests verify product build selection,
archive licenses, native library/header inclusion, and third-party notices.

A staged source build omits the entire server package from the Apache
products' source tree. This supplements the static source check and catches
hidden compile-time dependencies. Generated Snowball sources are retained with
their upstream BSD license, independently of the first-party source list.
The always-on PR license check uses a GitHub-hosted runner. The staged source
build runs in the Zig suite after a push to `main`, including merged PRs, and
does not add build time to individual PR checks.

`make license-check` checks repository-wide first-party header consistency and
the Apache dependency boundary. The root and Zig Makefiles and CI use the same
checks. Upstream and combined-license notices are preserved and checked
separately; generated frontend assets retain upstream notices. Source generators
normalize their outputs with the same policy so regeneration preserves licensing.

## Maintaining the boundary

Keep local engine functionality in Apache owners and database serving or
orchestration in ELv2 facades. Update the package roots or additional-file source map when extracting
shared helpers, then run both repository-wide and Apache boundary checks.

The dependency checker tokenizes Zig source and traverses named modules,
`@import`, and `@embedFile` edges. Computed or escaped paths fail closed.
Bundled assets require reviewed provenance, hashes, and complete notices.
Use the shared product-license bundle helper for native, browser, focused,
and independently packaged inference artifacts.

Preserve upstream legal blocks, combined SPDX declarations, frozen qualification
fixtures, and usage comments during header normalization. Re-run the provenance,
asset, and packaging regressions when changing those materials.

Browser database builds use Zig 0.17 `safe`; native product validation uses `Debug`.
WASM debug stripping is optional. CI exercises the browser database and both
wasm32 and wasm64 inference packages.

The [implementation and validation record](licensing/history/apache2-lite-inference-2026-09.md)
preserves the dated evidence from the licensing split.
