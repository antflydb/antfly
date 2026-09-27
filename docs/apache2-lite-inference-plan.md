# Apache Lite and inference boundary

Implemented from `origin/main` at
`cc9be026f930907667352884ddd35f0f3ba73e55` (2026-09-26).
See [LICENSING.md](../LICENSING.md) for the license scope and
[scripts/apache_engine_files.txt](../scripts/apache_engine_files.txt) for the
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
`build_zig_release_archive.sh --product lite` selects the Apache license and
Lite artifacts; the default server archive retains ELv2 and includes the
Apache license for the shared engine and native library.

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

`make license-check` checks repository-wide first-party header consistency and
the Apache dependency boundary. The root and Zig Makefiles and CI use the same
checks. Upstream and combined-license notices are preserved and checked
separately; generated frontend assets retain upstream notices. Source generators
normalize their outputs with the same policy so regeneration preserves licensing.

## Validation results

- Lite CLI and `libantfly` built in Debug from a staged tree with the ELv2
  server implementations removed. The CLI lifecycle smoke passed, including
  rejection of `lite serve`, search/vector/graph operations, and portable
  backup/restore. The browser WASM database also builds and passes its full
  hosted runtime smoke from that Apache-only source tree.
- The native Lite storage suite passed 279 tests (4 skipped); Lite command
  tests passed 34; C ABI tests passed 40 (1 skipped). C ABI smoke and conformance
  checks passed. The ELv2 server built in Debug and served a document over HTTP
  from a database initialized and populated by the Apache Lite CLI.
- Standalone inference built from that same tree; all 15 CLI tests passed.
- Apache dependency/header checks cover 1,821 source files, including separately
  licensed Snowball sources. Thirty-two boundary regression tests, ten header-policy
  tests, three qualification provenance tests, six asset/license generation tests,
  and 13 packaging tests passed.
- Go binding tests passed. Python: 52 passed, 3 skipped, 2 deselected.
  Rust: 32 passed, 1 filtered. TypeScript: 79 passed, 3 skipped.
  Optional cached Gemma generation tests were excluded from Debug conformance;
  the installed Qwen embedding tests ran successfully.
- The 114 baseline WASM compilation errors are fixed with platform-aware
  counters, allocator and host-storage ownership, portable WAL semantics,
  and native-only filesystem guards. Float16 vector widening avoids an LLVM
  WASM lowering error. Native embedded, platform, vector and WAL regressions
  pass. Inference WASM32 and WASM64 builds and ABI smoke tests pass.
  Browser database WASM build and full runtime smoke pass, including hosted
  batch writes, indexing, search, delete, and close/reopen persistence.
  Secure entropy is provided by the host and fails closed if unavailable.
  Freestanding timestamps avoid native I/O vtables; SST reads and reclamation
  follow one consistent backend mutex protocol on native and browser hosts.

## Review follow-up

Review compared 369 extracted declarations with their original implementations;
the bodies match after accounting for visibility and comments. Another 708 modified
source files have unchanged code after excluding license comments. The import checker
now tokenizes comments and string literals without treating quoted URLs as comments,
recognizes multiline imports, and checks visited first-party sources for conflicting
ELv2 notices. Regression tests cover these cases.

Release archives, native Lite license installs, and WASM license installs preserve
the relative source-list and Apache-license paths used by `LICENSING.md`. Archive
regressions verify those links and the source-list contents. The root README now
explains product modes and licenses near the quickstart, gives the separate Lite
and standalone commands, and identifies bundled third-party licensing.


Fresh review fixes give focused inference artifacts their own Apache entry point.
Named source modules are traversed recursively, including shared libraries and
source generators, so shared-module aliases cannot hide ELv2 imports. Native,
WASM, focused inference, and independent inference installs use one license-bundle
helper package. Both canonical license texts retain stable paths in product
bundles, so README and licensing links remain correct for Apache archives.

Repository-wide cleanup normalizes missing and stale first-party headers using
the product policy. It preserves the MIT httpx fork, Zig MIT adaptations, LMDB
notices, and combined Apache/BSD CUDA notice. Policy regression tests reject
missing upstream notices and verify idempotent header updates.

Fresh licensing review fixes preserve the byte identities of frozen GLiNER
helpers and their generated inventory source. Historical manifests and evidence
remain unchanged, and provenance tests run their original contract loaders.
Upstream notice checks validate entire legal blocks, including copyright,
conditions, and disclaimers. Binary releases and native/WASM installs include
the canonical third-party notice files, including both Zig MIT adaptations.

Embedded PDF fallback fonts now use pinned, unmodified Apache-2.0 Roboto 2
Regular/Bold from the official repository, with original copyright notices.
The source checker traverses literal `@embedFile` dependencies and rejects
unreviewed fonts and assets outside Apache source/package scope. Asset hashes
and notices are checked by the repository license check. Four Unicode-derived
tables retain complete Unicode notices; generators use one canonical renderer.
The inference case-fold table has a reproducible Unicode-15 generator/check.

Asset follow-up validation passed all 53 policy/packaging regressions, 464 PDF
tests and the OCR integration test. Debug Lite, C ABI, focused inference, and
independent inference builds passed; independent inference passed 15 CLI tests.
The rebuilt C ABI binary contains both complete Roboto fonts and neither Aeonik
font. Native, WASM-license, and independent inference bundles preserve every
canonical notice and the asset manifest. Unicode table bodies remain unchanged.

## WASM and packaging review follow-up

The dependency checker rejects computed `@import` and `@embedFile` paths and
unsupported escaped paths, instead of silently omitting them from the Apache
graph. A token scanner consumes comments and literals once before parsing
dependency calls, so backtracking cannot turn a quoted comment example into
a dependency. Comments between tokens and trailing commas are supported;
escaped and computed paths remain fail-closed. Regression tests distinguish
actual dependencies from comments and strings.

The adapted SciPy assignment solver retains its full BSD-3-Clause notice and
combined SPDX declaration. Native and WASM license bundles carry the canonical
notice; the independent inference source archive includes its original notice,
verified byte for byte from an actual Zig package fetch.

CI builds and exercises the embedded browser database and both wasm32/wasm64
inference packages. Browser builds retain ReleaseSafe; native product validation
uses Debug. Omitting WASM debug information is an explicit optional build flag.

The PR integrates the subsequent main updates for storage compilation and
substring/highlighting support. Highlighting and shared backup-pin control are
part of the Apache engine source map. Header normalization preserves adjacent
usage comments, including shebang scripts, with a regression test.
