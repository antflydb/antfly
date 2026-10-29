# Whisper runtime

How the Whisper transcriber executes, the switches for profiling it, and the
measurements behind the defaults in [Build Voice Dictation](../../guides/voice-dictation.mdx).
Measurements are from an M-series laptop with a ReleaseFast build, as of 2026-10-22.

## Stage timings and execution

Measured on an M-series laptop with a ReleaseFast build, the 2.5 s "quick brown fox" clip breaks down like this by stage on a warm server (milliseconds, whisper-tiny, 12 decoded tokens; the first request on a fresh process also pays a one-time weight upload of roughly 40 ms):

| Encoder / decoder | Mel | Encoder | Prompt block | Decode | Total |
| --- | ---: | ---: | ---: | ---: | ---: |
| Metal / Metal (default) | 5 | 35 | 21 | 28 | 89 |
| CPU / CPU | 5 | 915 | 8 | 30 | 958 |

The mel spectrogram runs as two dense matrix products on the system BLAS (the windowed frames against the DFT basis, then the power spectrum against the filterbank), which replaced a per-frame transform that took 60 ms. The encoder is one Metal frame: the two convolutions run as an im2col unfold plus a matrix multiply, the non-causal attention over 1500 positions uses the flash kernel, and the residual adds are folded into the layer norms. The decoder keeps its self-attention cache and the projected encoder keys resident on the device and runs each token as one command buffer on one compute encoder: Q, K and V are projected from one dispatch straight into the cache rows, every residual add is fused into the following layer norm, cross-attention over the 1500 encoder positions runs as a split-K kernel (also per query row for the prompt block), and the token itself is chosen on the device by a kernel that applies the suppression lists and the timestamp grammar and returns sixteen floats instead of the 51,865-float row. A decode token is about 1.8 ms of GPU time and about 2.2 ms of wall time; the remainder is command-buffer submission and completion latency. An encode-ahead mode (`TERMITE_WHISPER_ENABLE_PIPELINED_DECODE=1`) lets the device choose each token, keep the timestamp grammar and embed the chosen token itself, so the host encodes the next step while the current one runs; it produces the same transcripts but no measurable speedup, because the runtime keeps one command buffer in flight and the host encode was never on the critical path, so it stays off by default. All of it keeps the transcript bit-for-bit identical to the CPU path. The CLI exposes both placements for measurement (`antfly inference transcribe --backend metal --decoder-backend native`); the server keeps everything on one session. Set `TERMITE_SERVER_GENERATE_TIMING=1` to log the breakdown for every `/dictate` request, `TERMITE_WHISPER_METAL_PROFILE=1` to print per-op GPU time for the encoder and each decoder step, `TERMITE_METAL_TRACE_FRAME=all` for per-frame encoder and blit counts, and `TERMITE_METAL_TRACE_ENCODERS=1` to name every encoder transition.

## Quantized checkpoints

A ready-made q8_0 bundle of large-v3-turbo is published as `antflydb/whisper-large-v3-turbo` (`antfly inference pull antflydb/whisper-large-v3-turbo:gguf:Q8_0 --tasks transcribe`). Any other checkpoint can be quantized with the export command, which writes a GGUF bundle the server discovers like any other model directory (copy the source directory's `model_manifest.json` next to `model.gguf`):

```bash
antfly inference export ~/.antfly/inference/models/openai/whisper-large-v3-turbo --target gguf --format q8_0 \
  --output ~/.antfly/inference/models/openai/whisper-large-v3-turbo-q8_0/model.gguf
```

Whisper applies its own policy under the requested format: position tables stay dense, the token table (also the tied output head) and the whole encoder never go below q8_0, and the decoder layers take the format you asked for. A 4-bit token table breaks language detection on the small checkpoints, and q4_0 across all 32 encoder layers of large-v3-turbo produces nonsense while q8_0 is both exact and the fastest quantized encoder on Metal. `--quantize-include` and `--quantize-exclude` still apply on top. Quantized steps run on the same single-encoder, zero-blit decode path as f32, so the gains land where the bytes are: model size, the encoder, and the prompt block. Decode is bound by kernel dispatch rather than weight bandwidth at these sizes, so the per-token cost barely moves. Measured on the same laptop and clip (Metal, warm CLI run, the tied f16 `large-v3-turbo` checkpoint as published):

| Checkpoint | Weights | Size | Encoder | Prompt block | Decode per token | Transcript |
|------------|---------|------|---------|--------------|------------------|------------|
| tiny | f32 | 144 MB | 62 ms | 28 ms | 2.2 ms | reference |
| tiny | q8_0 | 42 MB | 47 ms | 10 ms | 2.5 ms | identical |
| tiny | q4_0 | 39 MB | 45 ms | 9 ms | 2.2 ms | drops a word |
| small | f32 | 922 MB | 290 ms | 86 ms | 9.6 ms | reference |
| small | q8_0 | 257 MB | 106 ms | 28 ms | 9.0 ms | identical |
| small | q4_0 | 203 MB | 115 ms | 29 ms | 8.9 ms | identical |
| large-v3-turbo | f16 | 1543 MB | 600 ms | 52 ms | 6.0 ms | reference |
| large-v3-turbo | q8_0 | 829 MB | 406 ms | 14 ms | 5.4 ms | identical |
| large-v3-turbo | q4_0 | 779 MB | 415 ms | 14 ms | 5.4 ms | identical |

q8_0 is the format to reach for: it is exact on every clip, and the f16 checkpoints run their decoder on the unfused path (the fused Q/K/V projection only takes f32 or quantized weights), so exporting large-v3-turbo to q8_0 also makes its prompt block four times faster. The q4_1, q5_0 and q4_k encoders are correct but two to three times slower than q8_0 because they lack the fast multi-row kernels, which is the other reason for the q8_0 floor. large-v3 and large-v3-turbo take 128 mel bins instead of 80; the front end reads that from the checkpoint config.
