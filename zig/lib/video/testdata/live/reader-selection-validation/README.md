# Content-bound reusable live model selections

The live adapter constructs an owned admitted selector list from its validated
coordinator index. Preflight, asynchronous preparation and synchronous fallback
verify a SHA-256 binding to track values, packet descriptors, packet bytes and
sampling policy, then copy the same selectors. Logical-picture reconstruction is
not repeated for sampling. Bytes/timing/display/policy mismatches fail before
reuse. Immutable input and the selection owner must outlive producer deinit;
caller read counters and URLs are neither consumed nor trusted as identity.
Construction accepts trusted library logical metadata, not an untrusted client
claim of validation. Selected-frame entropy reconstruction still runs normally.

Explicit little-endian value serialization avoids padding/pointer-address hashes.
Track optional/array/slice values are included by value, with slice lengths.
Selector construction charges temporary TimedPicture/sampling allocations and
retains a reservation only for the final selector array. Existing decoder,
parameter/display, preparation, process-supervision and cancellation boundaries
remain in effect. Public old reader APIs use the ordinary path; WithSelection
variants borrow the new owner. SampledVideo.selection requires reader input.

Final ReleaseSafe macOS gates pass 45 CPU/Metal checks. Actual Linux ARM64 passes
40 and skips five Metal checks in the pinned offline/read-only image. Tests cover:

- Field planning under 256 KiB admission headroom: bound planning succeeds while
  ordinary logical reconstruction fails admission; caller reads stay unchanged.
- Mutated encoded bytes, PTS, pixel aspect and sampling policy cannot reuse a plan.
- Ordinary and bound field preparation produce exact native and Metal patch hashes.
- Every construction allocation failure releases selector admission and temporary
  metadata. Previous native/Metal queue cancellation/lifetime gates remain green.

Fresh pinned pretrained CPU, Metal and actual Linux live execution preserves all
three real Sintel window vectors exactly against complete-clip baselines, with
246 tokens each, and passes pinned PyTorch score/ranking/top-1/top-3 gates.
Maximum element drift against the oracle remains CPU 2.98e-7, Metal 2.99e-7 and
Linux 9.26e-7. Checkpoint/oracle/source/binary hashes identify the exact evidence.
Completed timing is shared-host diagnostic evidence, not a performance gain claim.

Reproduce with Zig 0.17.0 from zig/pkg/inference:

```sh
zig build test-video-model build-embeddinggemma2-live -Doptimize=ReleaseSafe --prefix /tmp/reader-selection
zig build install-video-model-tests build-embeddinggemma2-live -Dtarget=aarch64-linux-musl -Doptimize=ReleaseSafe --prefix /tmp/reader-selection-linux
```

Use tools/embeddinggemma2/video_live_qualification.py with the pinned model and
prior pretrained-validation/pytorch.json oracle to verify native/Metal execution.
Linux uses read-only model/fixture/binary mounts and a writable output directory,
plus a bounded 64 MiB temporary work filesystem. No codec runtime library or
transport dependency is added. Pretrained field/dynamic stream parity, repeated
parameter planning, incremental append decode, cross-window projections and the
full broader non-CUDA goal remain open.
