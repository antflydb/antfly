# CUDA Whisper qualification

The native Whisper architecture supports CUDA through device im2col plus GEMM,
FP16 projection/embedding matrices and immutable cross-attention caches, FP32
norms and logits, FP16 fused encoder
attention, split-key FP32 decoder attention, and a compact 16-value
token-selection readback with a shared suppression bitset and parallel vocabulary
reduction. Decoder Q/K/V project directly into cache rows; residual sums and layer
normalization share a kernel. Single-token matrix reads use aligned FP16 pairs,
and the decoder fuses its existing tanh GELU into the first FFN projection.
CPU features use a fixed 400-point mixed-radix FFT
and skip windows wholly inside silent padding. Without cuDNN, encoder
attention uses the bundled tiled FP16 kernel. The default fatbin carries SM75, SM80 (A100), SM89 (L4), SM90, SM100, SM110,
and SM120 machine code plus forward-compatible PTX, matching the main bundle.
The sm89 mode uses its dedicated cubin; portable mode uses PTX 9.2, matching
the main bundle and its CUDA 13.2 driver requirement. Generate all images with
CUDA 13.2:

```sh
zig/pkg/inference/scripts/whisper/regen-kernels.sh --check
```

From `zig/pkg/inference`, build with `zig build -Dcuda=true
-Dcuda-artifacts=fatbin -Doptimize=ReleaseFast`. Use worktree-local build caches.
For numerical regressions, build `test-bin` with those options and
`-Dtest-filter='Whisper CUDA'`, then run `zig-out/bin/antfly-inference-tests`.

Use `antfly inference transcribe MODEL_DIR AUDIO.wav --backend cuda --language en`
to require CUDA explicitly. To isolate mixed-precision differences, set
`ANTFLY_CUDA_WHISPER_F16_WEIGHTS=0` (also retains FP32 cross-attention caches). `ANTFLY_CUDA_WHISPER_CUDNN=0` selects the
bundled attention fallback. Both switches are diagnostic comparisons, not changes
to transcription semantics.

The HTTP benchmark uses the unified `/ai/v1/read` endpoint with `audio_options`
and requires complete recognition before recording latency or WER.

For an end-to-end comparison, start Antfly with
`ANTFLY_INFERENCE_REQUIRED_BACKEND=cuda` and
`ANTFLY_INFERENCE_PREFERRED_BACKEND=cuda` on localhost port 18081. Use a models
directory containing the matching `openai/whisper-small` checkpoint. Start
whisper.cpp's CUDA `whisper-server` on port 18082 with the matching FP16
`ggml-small.bin`, flash attention enabled, `--beam-size 1 --best-of 1`. Use full 30-second encoder context in both servers. Do not use
quantized baseline weights or compare warm requests with process startup. Both
HTTP endpoints retain their default temperature fallback after a low-confidence
greedy pass. The fixture comparison must report any fallback work.

```sh
python3 zig/pkg/inference/scripts/whisper/benchmark.py \
  --audio zig/e2e/inference/testdata/whisper_quality.wav \
  --reference zig/pkg/inference/scripts/whisper/english-reference.txt \
  --trials 30 --warmup 3 --output benchmark.json
```

The reference is `The quick brown fox jumps over the lazy dog.`. Repeat with the
Spanish fixture and an independently checked reference, longer recordings,
silence, and `--timestamps`. The runner alternates server order, excludes warmup,
records every transcript and latency, and requires the lower bound of a paired
95% bootstrap interval for median speedup to exceed one, with WER no worse than
the baseline. Small timing differences are not automatically counted as wins. Retain the GPU name,
driver, source revisions, model hashes, server flags, and placement logs alongside
the report. Bundled SM80 and SM89 images establish artifact coverage for A100 and L4 without
requiring those GPUs for development. GB10 timing results are not measurements
of L4 or A100 performance.


## Design references

- [NVIDIA CUDA memory access guidance](https://docs.nvidia.com/cuda/cuda-programming-guide/02-basics/writing-cuda-kernels.html): neighboring lanes read neighboring head channels. Decoder attention splits keys into 128-position blocks so short prefixes occupy more than one block per head, then merges partial maxima, denominators, and weighted values in FP32.
- [NVIDIA cuDNN attention contract](https://docs.nvidia.com/deeplearning/cudnn/latest/operations/Attention.html): the encoder uses supported noncausal FP16 SDPA with 64-channel heads, preserving FP32 output. Vendor attention remains optional.
- [whisper.cpp](https://github.com/ggml-org/whisper.cpp): development baseline revision `4da27c41cebadb3cbbcd2563e9c7cbc8d84bdcad`. Benchmark its warm HTTP endpoint, with CUDA and flash attention enabled, rather than including process/model startup.

Use Nsight Systems on the real-checkpoint test to inspect kernel execution and
CUDA API time. Pipeline stage timers enqueue asynchronous GPU work and must not
be treated as synchronized per-stage GPU measurements. The initial GB10 profile
identified generic cross attention (57% of GPU time) and a vocabulary-wide serial
suppression scan as bottlenecks; both have dedicated Whisper implementations.

On hybrid CPU hosts, pin both servers to the same CPU set and run the client
on a separate performance core. Record all affinities alongside the results;
the runner records its own affinity. This controls host scheduling differences
without changing either runtime's GPU or decoding settings.
