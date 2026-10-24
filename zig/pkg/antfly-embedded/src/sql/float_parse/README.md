# Controlled float scans

Snapshot of the pinned Zig 0.17.0 standard float parser, with the upstream
license in LICENSE.zig and original source hashes in UPSTREAM.json.
Numerical conversion kernels are unchanged. Control is added to the byte stream,
underscore validation, and slow decimal reparse/trailing-zero scan. Cancellation
stops scans and escapes through the original scalar work owner; no work quota or
input grammar is replaced. Keep differential and upstream regression tests when
updating the snapshot.
