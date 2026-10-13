# Selected VP9/AV1 output qualification

`Session.decodeOutputs` reconstructs every coded part, updates codec references,
validates native output layout, and drains all visible outputs. It materializes
only requested per-packet output ordinals. `Batch.output_count` includes skipped
outputs; owned frames retain their `output_index`. Native output-count limits
also bound skipped outputs. Output-byte limits apply to materialized ownership.
The existing `decode` API requests every output.

WebM replay uses this API for unselected preroll and container-invisible packets.
It advances logical IDs using native output counts, including superframes and
AV1 layers. `Options.copy_unselected_outputs` retains the full-materialization
oracle. Entropy corruption, cancellation and post-core ownership cleanup retain
the existing failure/recovery contract. No runtime dependency is added; selected
frames retain the existing portable SIMD planar-to-semiplanar conversion.

Debug and ReleaseSafe each pass 220 video tests on macOS (two skips).
ReleaseSafe passes 195 on actual Linux
ARM64 (27 platform skips), and 45 CPU/Metal model checks. New tests cover skipped
key/reference pictures, depth/chroma/monochrome, film grain, Annex B, scalable
layers, duplicate selectors, native-count caps, output-byte budgets, exhaustive
allocation failures, cancellation and corruption error parity. WebM checks cover
invisible preroll and selecting the second output of a VP9 superframe.

Completed serial comparisons use the same validated seek point, selected IDs,
decode packet counts and pixel/metadata hashes for full and filtered ownership.
Each real clip runs three samples on macOS and actual offline Linux ARM64.
480p Sintel materialization drops from 209,674,080 to 19,676,160 bytes; 1080p drops
from 298,598,400 to 99,532,800 bytes. These are copied output bytes, not RSS,
core-reference bytes or total model transfers. Shared-host timings are diagnostic.
VP8 already materializes selected frames; this benchmark rejects VP8 rather than
reporting untracked byte telemetry.

Fresh production-server pretrained CPU and Metal WebM checks preserve all eight
vectors/token counts and three query ranking/top-k/score gates against the pinned
PyTorch CPU reference. Maximum vector absolute errors are 9.32e-7 (CPU) and
3.58e-7 (Metal). This preserves reference behavior; it does not establish end-user
relevance or broader codec conformance. No fresh Linux pretrained claim is made
in this receipt; its Linux execution covers codecs and real-clip replay.

Explicit bounded coverage campaigns pass 101,436 VP9 runs, 102,079 AV1 runs
and 101,569 Annex B runs. The combined target separately records 406,395 H.264
runs. The logs retain coverage and accumulated run counts. Source, fixture, compiler, binary and artifact hashes are in
`qualification.json`. The full non-CUDA objective remains active, including
incremental append validation, cross-window model projection reuse, broader
pretrained field/dynamic/HDR qualification, and further scheduling/streaming work.

Reproduce the materialization comparison from `zig/lib/video`:

```
zig build install-webm-benchmark -Doptimize=ReleaseSafe --prefix /tmp/video-qual
/tmp/video-qual/bin/webm-benchmark testdata/benchmarks/webm/corpus/sintel-vp9-480p.webm replay_copy_compare 3 384 24
zig build fuzz-video -Doptimize=ReleaseSafe -Dvideo-fuzz-filter="video fuzz VP9 reconstruction" --summary all --fuzz=100000
zig build fuzz-video -Doptimize=ReleaseSafe -Dvideo-fuzz-filter="video fuzz AV1 reconstruction" --summary all --fuzz=100000
zig build fuzz-video -Doptimize=ReleaseSafe -Dvideo-fuzz-filter="video fuzz AV1 Annex B" --summary all --fuzz=100000
```

The benchmark flushes each completed comparison. Fuzzing uses the build-level
`--fuzz` flag, not arguments after `--`.
