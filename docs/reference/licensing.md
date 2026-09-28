# Licensing maintenance

See [LICENSING.md](../../LICENSING.md) for the license scope and
[scripts/apache_engine_files.txt](../../scripts/apache_engine_files.txt) for the
explicit shared source list.

## Products

The shared local database engine, embedded Lite APIs, native `libantfly`,
file-oriented Lite CLI, browser client and WebGPU shaders, language bindings,
and inference implementation and executable are Apache-2.0. Original licenses
remain in effect for third-party material, including bundled Snowball, httpx,
and LMDB implementations.

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

The engine remains in its existing source paths, with explicit Apache headers
and an audited source list, so both products use one implementation. Mixed
local and server modules now expose local helpers through separate Apache
files; ELv2 server facades consume those helpers. Server integration tests have
moved out of shared engine sources.

`runtime_lite_kernel_root.zig` owns the Apache engine and public native C ABI.
The Lite CLI and `libantfly` link this owner, enrichment, and inference. They
no longer link database API or distributed runtime archives. The dedicated
CLI uses a local dispatcher and retains inference worker re-execution.
The full server continues to use its separate storage and server owners over
the same engine code.

The embedded package, inference package, shared libraries, schema inputs, and
generated OpenAPI contracts have explicit Apache licenses. Lite installation
and inference installation carry license and third-party notices.
`build_zig_release_archive.sh --product lite` packages the Apache Lite CLI,
`libantfly`, and a private `antfly-inference-worker`. The `inference` product
packages the real Apache `antfly-inference` CLI with commands such as `run`,
`embed`, `generate`, and `pull`. The default server archive retains ELv2 and
includes the Apache license for the shared engine and native library.

The release build creates separate Apache Lite and inference archives for each
platform. `package_lite_release.py` assembles
platform wheels for `antfly-embedded` and native npm packages for
`@antfly/embedded`;
the bindings discover these artifacts without using the ELv2 server packages.
`verify_lite_release.py` compares each wheel and npm package with its Lite
archive and rejects server executables and ELv2 license files. The immutable
package snapshot is produced by `.github/workflows/lite-package.yml` as part
of the release build. After a successful tagged release build, dispatch
`.github/workflows/lite-release-publish.yml` on `main` with that tag and build
run ID. It verifies the immutable tag, package hashes, and archive equivalence
before publishing `antfly-embedded` wheels to PyPI and the
`@antfly/embedded` platform and selector packages to npm. The PyPI project and
each npm package must have trusted publishing configured for this workflow in
their registry settings.

Before the first release, the PyPI account owner must configure a pending
trusted publisher for project `antfly-embedded`, repository
`antflydb/antfly`, workflow `lite-release-publish.yml`, and GitHub environment
`pypi`. A pending publisher does not reserve the name: the first successful
upload creates the project. The crates.io owner can publish the tested
`registry-claims/antfly-embedded` and `registry-claims/antfly-embedded-sys`
version `0.0.0` placeholders, then grant the Antfly organization ownership
before publishing the functional Rust crates.
Registry ownership and trusted-publisher settings cannot be established by a
source commit; verify them in the registries before promoting a release.

## Verification

```sh
make apache-license-check
python3 -m unittest discover -s scripts -p test_check_apache_boundary.py
python3 -m unittest discover -s scripts/packaging -p 'test_*.py'
cd zig
zig build lite -Doptimize=Debug -Dmetal=false -j1
zig build lite-native-test lite-cmd-test capi-smoke capi-conformance capi-test -Doptimize=Debug -Dmetal=false -j1
cd pkg/inference
zig build -Doptimize=Debug -Dmetal=false -j1
zig build test-cli -Doptimize=Debug -Dmetal=false -j1
```

The dependency check rejects ELv2 source edges, missing sources, and unreviewed
named modules. It verifies the Apache manifest's headers and detects retained
ELv2 notices. Release assembly regression tests verify product build selection,
archive licenses, native library/header inclusion, and third-party notices.

A staged source build excludes server implementation files from the Apache
products' source tree. This supplements the static source check and catches
hidden compile-time dependencies. Generated Snowball sources are retained with
their upstream BSD license, independently of the first-party source list.
The always-on PR license check uses a GitHub-hosted runner; the staged source
build runs for same-repository PRs, merge queue checks, and pushes to `main`.

`make license-check` checks repository-wide first-party header consistency and
the Apache dependency boundary. The root and Zig Makefiles and CI use the same
checks. Upstream and combined-license notices are preserved and checked
separately; generated frontend assets retain upstream notices. Source generators
normalize their outputs with the same policy so regeneration preserves licensing.

## Maintaining the boundary

Keep local engine functionality in Apache owners and database serving or
orchestration in ELv2 facades. Update the explicit source map when extracting
shared helpers, then run both repository-wide and Apache boundary checks.

The dependency checker tokenizes Zig source and traverses named modules,
`@import`, and `@embedFile` edges. Computed or escaped paths fail closed.
Bundled assets require reviewed provenance, hashes, and complete notices.
Use the shared product-license bundle helper for native, browser, focused,
and independently packaged inference artifacts.

Preserve upstream legal blocks, combined SPDX declarations, frozen qualification
fixtures, and usage comments during header normalization. Re-run the provenance,
asset, and packaging regressions when changing those materials.

Browser database builds use ReleaseSafe; native product validation uses Debug.
WASM debug stripping is optional. CI exercises the browser database and both
wasm32 and wasm64 inference packages.

The [implementation and validation record](licensing/history/apache2-lite-inference-2026-09.md)
preserves the dated evidence from the licensing split.
