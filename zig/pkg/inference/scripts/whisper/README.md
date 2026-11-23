# CUDA Whisper qualification

The native Whisper architecture uses shared NCT convolution, dense projection,
F32 attention, normalization and row-selection programs. The qualified default
preserves F32 model matrices, attention and cross-attention caches. Decoder token
selection reads back a compact statistics record; unsupported selection modes
retain the full-logit fallback. There is no separate Whisper CUDA module or
artifact bundle.

Regenerate the shared multiarchitecture artifacts with the pinned CUDA toolkit:

```sh
zig/pkg/inference/scripts/regen-cuda-artifacts.sh --check --all
```

From `zig/pkg/inference`, build with `zig build -Dcuda=true
-Dcuda-artifacts=fatbin -Doptimize=ReleaseFast`. Use worktree-local build caches.
Use `antfly inference transcribe MODEL_DIR AUDIO.wav --backend cuda --language en`
to require CUDA explicitly. Keep precision and attention policy fixed across
benchmark arms. Historical results below describe their recorded source and
settings; they do not qualify later revisions or a different precision policy.

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
- [NVIDIA cuDNN attention contract](https://docs.nvidia.com/deeplearning/cudnn/latest/operations/Attention.html): reduced-precision SDPA is a separate policy whose numerics and performance require qualification.
- [whisper.cpp](https://github.com/ggml-org/whisper.cpp): development baseline revision `4da27c41cebadb3cbbcd2563e9c7cbc8d84bdcad`. Benchmark its warm HTTP endpoint, with CUDA and flash attention enabled, rather than including process/model startup.

Use Nsight Systems on the real-checkpoint test to inspect kernel execution and
CUDA API time. Pipeline stage timers enqueue asynchronous GPU work and must not
be treated as synchronized per-stage GPU measurements. The initial GB10 profile
identified generic cross attention (57% of GPU time) and a vocabulary-wide serial
suppression scan as bottlenecks; both use shared attention and row-selection families.

On hybrid CPU hosts, pin both servers to the same CPU set and run the client
on a separate performance core. Record all affinities alongside the results;
the runner records its own affinity. This controls host scheduling differences
without changing either runtime's GPU or decoding settings.
