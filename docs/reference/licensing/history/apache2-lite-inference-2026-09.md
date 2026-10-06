# Apache Lite and inference implementation record

Implemented from `origin/main` at
`cc9be026f930907667352884ddd35f0f3ba73e55` (2026-09-26), with subsequent main updates integrated.
This is historical validation evidence; see [licensing maintenance](../../licensing.md)
and [LICENSING.md](../../../../LICENSING.md) for current policy.

## Contributor provenance audit

The author audit used non-merge commits reachable from `origin/main`, scoped to
the Apache roots and exact shared-engine files classified by
`scripts/license_headers.py` at the time of this review. It found 13 commits
authored as `stinkbugaf` (including GitHub no-reply aliases), three as
`andreasontrace@gmail.com`, five as `andrew.batz@visitingmedia.com`, and 18 as
`codex@openai.com`. Of the latter, ten touch the narrower embedded/inference
paths; the others touch Apache tooling or packages. The previously reported
count of 11 Codex commits does not match this current path-scoped, non-merge
query, so it must not be treated as a verified consent inventory.
The 39 commit IDs, dates, author identities, and subjects are preserved in
[the author audit](apache-author-audit-2026-09.tsv).

This is evidence of authorship and affected paths, **not** evidence of an
Apache license grant or a DCO sign-off. The historical inbound rights for
each contribution still need a separate review before representing a changed
license boundary as cleared for release. The prospective contribution policy
in `CONTRIBUTING.md` does not change past contributions.

## Ownership confirmation (2026-10-05)

The repository owner confirmed that Antfly owns the contributions covered by
this relicensing work. This confirmation is separate from the historical
Git author audit above and resolves the outstanding ownership confirmation
recorded by that audit.

## Validation results

The Apache-only source build runs in the Zig suite after pushes to `main`. Its local run
removed the ELv2 server implementation files from the staged source and built
the Lite CLI and standalone inference package successfully. The companion
license boundary check runs on every PR independently of the admission-gated
Zig test suites.

WASM PR #896's September 28 PR CI run passed its build and test shards but
failed when merging E2E timing observations: a nested pytest probe in
`test_standalone_harness.py` inherited the parent's duration file and emitted
the same synthetic test ID in multiple lanes. This is a CI measurement issue,
not a WASM compilation or runtime failure. The nested probe now clears the
shard timing environment.

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
