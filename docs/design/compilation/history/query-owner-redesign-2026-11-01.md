# Production query-owner redesign, 2026-11-01

This completes the query-owner follow-up in the
[work reduction record](work-reduction-2026-11-01.md). Current topology and
commands are maintained in [COMPILATION.md](../../../../zig/COMPILATION.md).

## Production implementation

The server now always builds `antfly-runtime-local_query` as its eighth runtime
archive. This is the production provider, not an opt-in lifecycle probe. The
storage adapter no longer imports or exports the local-query implementation.
Final server executables and compiled owner tests link the new archive after
the storage adapter and before its downstream physical providers. Each serving
mode retains its existing entry point; the result remains one executable.
The embedded C library continues to link its Apache physical storage and
inference owners without acquiring the server query/coordinator roots.

`storage/local_query_abi.zig` contains C-layout requests, responses, cancellation
callbacks and an explicit synchronous DB borrow, with no DB implementation
imports. `kernel_owner_abi.zig` re-exports the value contracts for existing
callers. The internal storage/query epoch advances from 85 to 86; the public
C API remains unchanged. These nested private failure envelopes retain their
shared epoch, including exact origin, operation and semantic error identities.

A borrow contains an opaque pointer and the existing native TypeContract.
The query provider checks the ABI epoch, concrete type/layout, compiler identity
and alignment before casting or accessing the pointer. The storage caller still
owns the DB and retains its readable lease until the synchronous call returns.
Only the tiny request adapter specializes by caller type. No record, posting,
candidate or index handle crosses this new boundary.

The provider owns the complete operation: parse and validate its wire request,
apply execution controls, open/close retained cuts, acquire/release generation
leases, execute through the shared physical engine, collect complete aggregates,
encode the requested page, and free its own response buffer after the caller
copies it. Graph, text statistics, algebraic partials and preflight use the same
complete-operation boundary. Graph wire parsing/encoding now uses the existing
local graph contracts directly instead of declaring the distributed coordinator.

## Cache isolation

`tools/check_query_owner_cache.py` builds the actual production query and
storage-adapter archives in a private overlay inside the worktree cache. It
restores/removes the overlay automatically and preserves logs and a JSON report.
All five phases pass:

| Source change | Storage adapter | Query owner |
|---|---|---|
| Initial build | Build | Build |
| Warm build | Reuse | Reuse |
| Query-provider implementation | Reuse | Rebuild |
| Storage-adapter implementation | Rebuild | Reuse |
| Query ABI contract | Rebuild | Rebuild |

The source graph check also finds no literal import path from the storage
entry to the query provider, or from the query entry to the server owner.
Shared DB implementation/declarations still invalidate their actual consumers;
this is not a claim of independent compilation for every physical source file.

## Behavior and build verification

- All 57 compiled storage-owner regressions pass in both Debug and stripped
  ReleaseFast, including aggregation
  behavior, cancellation/deadlines, response lifetimes and exact error identities.
  The suite now rejects an incompatible non-null DB borrow before pointer use.
- A separately compiled caller executes the production query C ABI against a
  retained text generation. Both profiled and unprofiled continuation return
  the original checkpoint's page after later writes. Its normal storage and
  integration aggregates include this test.
- The contract test rejects altered ABI/type/layout/compiler fields and invalid
  alignment. It passes in Debug and stripped ReleaseFast and joins the normal
  storage/integration aggregates.
- The three topology fixtures (owner dependencies, Lite options, compute/storage
  ABI isolation), the WASM module-profile fixture, 32 compilation-audit tool
  tests, and 370 memory-policy tests pass.
- Both actual production archives cross-compile in stripped ReleaseFast for
  `aarch64-linux-musl` (26/26 build steps). This verifies the ELF/LLVM compilation
  path; Linux runtime execution was not performed on the macOS host.
- The measurement collector now resolves relative compiler response files from
  the actual Zig build directory. A successful build with failed attribution
  returns an error. Nine sampler/summarizer tests cover this, including the
  original build failure status and portable retained response files.

The opt-in profile workflow exposes the query contract, retained-query,
production owner-suite and query archive steps. Selecting an owner suite also
runs the real five-phase cache check. Normal release modes remain ReleaseFast.

## Release measurement scope

Native Apple Silicon, Zig 0.17.0, SDK 27.0, baseline CPU, stripped ReleaseFast,
Metal/CUDA/system BLAS disabled. Both split archives build successfully. Observed
per-compiler RSS peaks are 5,134,745,600 bytes for the storage adapter and
3,484,762,112 bytes for the query owner. These are one-second samples; they are
not allocation maxima or a portable memory reservation qualification.

Build-time claims for the new owner remain conservative (8 GiB Debug, 12 GiB
other modes), without transferring historical seven-unit qualification to it.
The preceding revision (`7c6ba94c7b`) was measured in a separate temporary
worktree with the same compiler and flags. Its combined storage/query compiler
sampled 4,708,696,064 bytes (4.385 GiB), compared with 4.782 GiB for the new storage
adapter and 3.245 GiB for query. The largest compiler sample is therefore about
9% higher in this observation; this experiment does not prove an overall
peak-memory reduction. The isolated query compiler is smaller than the old
combined compiler, and query edits demonstrably reuse storage code generation.

The old stripped archive is 38,206,248 bytes. The split archives are 29,101,400
bytes (storage) and 19,124,088 bytes (query), totaling 48,225,488 bytes, about 26%
more archive bytes. These include separately emitted private helpers; they do
not measure the final executable after section collection. SHA-256 digests,
exact flags, per-compiler traces, response files and logs are preserved in the
ignored `.benchmark-results/compile-costs` directory.

The complete native ReleaseFast executable and C library build with all
125 selected steps successful, including 57 owner regressions, the retained
query test and the public C API smoke suite. The executable is 232,628,712 bytes;
the C library is 80,489,232 bytes (below its 88 MiB gate). Ten end-to-end HTTP
tests pass against that executable: pruning/projection across shard counts,
query/join/graph deadlines, and the semantic embedding-cache path. The first HTTP
attempt was blocked before launch by the production disk-admission floor;
tests passed after space recovered without changing the guard.

The full native product sampled 11.473 GiB for API code generation, 9.628 GiB
for the physical storage owner, and 8.132 GiB for the inference owner. Those
remain outside this query-orchestration cut. Existing claims are not lowered
on the strength of this one native profile.

The preceding full executable also builds successfully (69/69 steps) with the
same flags. It is 225,639,896 bytes, versus 232,628,712 bytes for the redesign:
an increase of 6,988,816 bytes, or 3.10%, within the existing approximately 5%
experiment budget. The archive growth is therefore partly removed by final
linking, but this change still trades executable size for verified query-cache
isolation; it is not a binary-size optimization.

Concurrent unrelated host builds prevent a controlled cold wall-time claim.
The temporary baseline full build was stopped while candidate validation ran
to bound this task's simultaneous memory use, then resumed; no whole-product
timing comparison is implied. Its size, digest and build report were retained
before removing the temporary baseline worktree.
