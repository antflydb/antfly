# Dynamic native mass constraint generation CLI V1

This diagnostic-only CLI is the smallest reusable boundary after the reviewed
single-seed full-CG proof. It does not modify the inference runtime, fit key or
value stages, payload publication, or any qualification gate.

## Contract

`attention_mass_highs_v180_dynamic_cg_cli_v1.zig` imports three named modules:

- `attention_mass_lp`: reviewed LP-domain V4, SHA-256
  `ff1d98cab21450271694ab524dd0733294ae0ff15793c57ed105f4d18d9354de`
- `attention_mass_cg`: reviewed controller V5, SHA-256
  `abdcc63e7d914b174f3fb8db2302db16af126386b610fcf6e80bf1dbb2ec82fa`
- `attention_mass_highs`: reviewed HiGHS Adapter V4, SHA-256
  `f9962c9ec47d6399c15eeda30c674fee0993ead3fbcf6f4527bc8037c75e0dc8`

The input manifest is owner-relative for matrix and optional warm-row bytes and
uses absolute, hash-bound source evidence. Supported shapes are exactly
`912x352`, `912x288`, and `3000x288`. Matrix storage is little-endian C-order
float64. Warm rows are optional little-endian uint64 indices; their caller order
is retained and duplicate or out-of-range entries fail closed.

`solveDynamic` is the solver-neutral library boundary. The caller owns the
matrix and warm hints. The returned CG V5 union owns its accepted allocations.
The CLI supplies Adapter V4 and retains its exact fresh 1e-9 solve followed by
one fresh 1e-10 retry only after the original witness is rejected. CG selection,
pricing, witness, stored float32 conversion, cancellation, and absolute deadline
semantics remain in the pinned modules.

The accepted result writes raw float64 weights, float32 bias, stored float64
weights, mass, full-coordinate alpha/beta duals, selected rows, round trace,
per-attempt hashes/options/status, and balanced HiGHS lifecycle counts. A CG
terminal result writes no arrays and marks `rollback_required:true`. A fatal
provenance, I/O, cancellation, or final-deadline error leaves no `result.json`;
the outer worker must treat that as failure and retain its original payload.

Before output creation, the CLI rehashes the manifest, matrix, warm rows, source,
library closure, executable receipt, compiler, all named modules, linked archive,
and the running executable. Output directory creation is exclusive. Partial
vector files without `result.json` are never a completed result.

## Build and run shape

The caller must supply every path. A future reviewed build receipt uses the same
command structure as the proven restricted/full-CG drivers:

```text
ZIG build-exe -OReleaseSafe -fstrip \
  --dep attention_mass_lp --dep attention_mass_cg --dep attention_mass_highs \
  -Mroot=DRIVER \
  -OReleaseSafe -Mattention_mass_lp=LP_V4 \
  -OReleaseSafe --dep attention_mass_lp -Mattention_mass_cg=CG_V5 \
  -OReleaseSafe --dep attention_mass_lp --dep attention_mass_cg \
  -Mattention_mass_highs=ADAPTER_V4 \
  LIBHIGHS_A -lc++ --cache-dir LOCAL_CACHE \
  --global-cache-dir GLOBAL_CACHE -femit-bin=FRESH_BINARY
```

The executable receipt must bind that exact argv, compiler, driver, all three
modules, static-library receipt/archive, and binary. Runtime invocation is:

```text
FRESH_BINARY \
  --input-manifest INPUT_JSON --input-sha256 INPUT_SHA256 \
  --library-receipt LIBRARY_JSON --library-sha256 LIBRARY_SHA256 \
  --executable-receipt EXECUTABLE_JSON --executable-sha256 EXECUTABLE_SHA256 \
  --output-dir FRESH_OUTPUT --deadline-ms DEADLINE_LE_560000
```

The surrounding owner must still enforce the reviewed external 600-second,
1-GiB process-group RSS, zero-swap, free-memory, disk, PID/PGID, and complete
drain policy. This draft does not implement that parent guard.

## Tests and limits

Embedded Zig tests cover the three profiles, owner-relative path rules, request
identity, real warm-byte order/duplicate/range validation, invalid shape, and an
initial-deadline terminal that proves no solver call. The manifest builder tests
cover every supported shape, null and ordered warm hints, fresh output, matrix
length mutation, invalid profile, and cross-directory input rejection.

This is source-level diagnostic tooling. It has not been compiled or linked in
this version, has not run HiGHS, and has no full fitter, key/value-stage, serving
latency, held-data, publication, or production qualification authority.
