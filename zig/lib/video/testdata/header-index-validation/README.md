# Header-only WebM index qualification

Default VP8/VP9/AV1 planning uses header metadata, with reconstruction retained
as an explicit oracle and for selected pixel replay. Each of six real-clip
streams is compared twice on macOS and actual Linux ARM64. Every output ordinal,
PTS/duration, geometry and selected frame ID agrees for 4,623 unique pictures.
Source clip provenance and independent pixel hashes are in
`../benchmarks/webm/corpus/manifest.json` and the surrounding benchmark receipts.

The reports contain diagnostic shared-host timings; owned compilers/model runs
were active. Index timing excludes sampling and oracle comparison. Comparison
RSS includes the full decoder, and does not measure header-only peak memory.

Reproduce with `zig build install-webm-benchmark -Doptimize=ReleaseSafe
-Dvideo-use-llvm=true`, then run `webm-benchmark CLIP index_compare 2` for each
clip in `../benchmarks/webm/corpus`. Cross-compile with
`-Dtarget=aarch64-linux-musl` and execute the installed binary on Linux.

The correctly Smith-serialized metadata campaign passes 101,937 runs. Separate
seed gates require all 13 decoder and five metadata seeds to reach valid output.
Current macOS CPU and Metal preserve eight pretrained vectors and three exact
query rankings/top-k sets. CPU completes all five concurrent load requests;
Metal records one completion and four retryable admission denials under host
pressure. Metal rejects three zeroed-first-packet cases and preserves clean-retry
vectors. Broader corruption recovery remains active. Current Linux pretrained
qualification remains in progress. The receipt explicitly records
which gates pass and which are pending. This does not close broader codec,
SIMD/scheduling, display or streaming requirements.

Gate logs trim terminal blank lines and label the fuzz-report divider to avoid
Git conflict-marker false positives; numeric results are unchanged.
