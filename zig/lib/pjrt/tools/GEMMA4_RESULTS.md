# Gemma 4 E4B QAT: native CUDA versus native PJRT CUDA

Warm isolated direct generation, encoded media read through synchronized fixed-budget tokens; no HTTP, cold model load, or compilation in latency; memory includes startup and inference. Native Q4_0/F16 KV versus dense BF16/BF16 KV from identical QAT GGUF sources.

NVIDIA L4 SM89, driver 580.159.03. Three alternating fresh-process rounds, one warmup and three measured calls per case per arm (nine samples). Native additionally primes all requested modalities. No other GPU compute process or build/CPU-oracle job ran during timing.

| Workload | Prompt / output tokens | Native ms | PJRT ms | Native speedup |
|---|---:|---:|---:|---:|
| text_short_256 | 32 / 256 | 3750.44 | 11810.23 | 3.15× |
| page_summary_128 | 419 / 128 | 1973.08 | 6027.42 | 3.05× |
| long2k_summary_64 | 1775 / 64 | 1287.19 | 3191.43 | 2.48× |
| image_64 | 284 / 64 | 1114.06 | 3079.08 | 2.76× |
| audio_1s_64 | 42 / 64 | 1012.07 | 2951.65 | 2.92× |
| audio_12s_64 | 317 / 64 | 1110.96 | 3011.91 | 2.71× |
| audio_30s_64 | 767 / 64 | 1280.63 | 3078.64 | 2.40× |
| video_6s_64 | 465 / 64 | 1147.80 | 3207.64 | 2.79× |

All 144 measured calls returned the complete token budget. Token IDs and per-round medians are retained in the ignored campaign; compact provenance and results are in `gemma4-l4-results.json`.

## Memory

Median of three per-process-tree campaign peaks, sampled every 0.2 s. Includes startup and all text/vision/audio weights requested by the suite.

| Arm | GPU GiB | PSS + GPU GiB |
|---|---:|---:|
| native | 7.65 | 9.60 |
| pjrt | 19.49 | 24.01 |

The runtime representations and weight inventories differ. Combined peaks include streamed BF16 upload buffers at startup; this is not a pure allocator-efficiency comparison.

## Numerical checks

| Arm | CPU-reference logit gate | Matching first-token argmax |
|---|---:|---:|
| native | 5/8 | 8/8 |
| pjrt | 6/8 | 8/8 |

Gate: cosine ≥0.9999 and relative L2 ≤0.02 against the independent official FP32 CPU forward from the same dequantized source GGUF. Native diagnostic readback bypasses device-greedy/replay fast paths. Numerical failures are retained below; the gate was not widened.

- r0-native audio_1s_64: cosine 0.9998950, relative L2 0.01474, max absolute difference 0.4001.
- r0-native audio_30s_64: cosine 0.9998628, relative L2 0.01681, max absolute difference 0.5912.
- r0-native video_6s_64: cosine 0.9998154, relative L2 0.02346, max absolute difference 1.1546.
- r0-pjrt audio_30s_64: cosine 0.9993670, relative L2 0.03706, max absolute difference 1.3242.
- r0-pjrt long2k_summary_64: cosine 0.9998185, relative L2 0.03384, max absolute difference 1.1351.

Measured first-token CPU agreement: native 72/72, PJRT 72/72. Both arms reproduce the same complete token sequence within each case across all nine repeats; this does not imply agreement between the two precisions.

## Validation and limits

- Native worker build succeeded after the PLE correction. The focused native PLE regression passed; the earlier CUDA-enabled inference type check passed 14/14 build steps.
- Four focused Python tests pass, including official-cache parity through sliding wraparound and shared KV. PJRT library checks passed before the campaigns.
- All logit vectors have 262,144 finite components. These are first-step checks, not full-generation parity.

- One retained prompt per case, repeated; synthetic media test geometry, not task quality.
- PJRT uses functional KV buffers through the native borrowed-buffer API, without donation.
- First-step diagnostic logits and argmax checks do not establish whole-generation parity.
- Native diagnostic logits use full readback with device-greedy/replay paths disabled; performance keeps defaults.
- This branch corrects PLE projection-normalization epsilon; the baseline CUDA optimizations were already present on main.

A broad default-suite attempt was not completed after its mock CUDA allocation runner failed; the focused PLE regression was built and run directly.

The native PLE normalization regression passes against the literal scaled-projection formula. A separate L4 fused-kernel check on checkpoint Q6_K rows passes six zero/nonzero-epsilon cases with maximum absolute error below 1.8e-6. Live Metal validation was not run on this Linux host.

Reproduction and artifact preparation: [GEMMA4.md](GEMMA4.md).
