# OCR performance evidence

The [serving-default qualification](serving-defaults-1331.json) records the
GPU-first recurrent dispatch and memory-admission checks added for Grepstein:
381 post-main CUDA-suite tests passed with 21 skips. The earlier matched
host-recurrence rollback checks passed, and both pinned reader checkpoints
passed five private page-tail checks at a 2048-token allowance. The final
real-model rerun was rejected before placement because the shared host had
insufficient free memory; its retry remains pending. Raw DOJ content remains
private. This sample does
not qualify full-corpus accuracy, A100 capacity, or the hosted activation path.
The [release qualification guide](../../../../../../docs/design/inference/grepstein-release-qualification.md)
describes the separate deployment and enrichment gates. Hardware performance
qualification does not control whether supported portable CUDA recurrence runs
on the GPU.

The historical experiment corpus is archived outside the source tree. The
[archive manifest](archive.json) records its source commit, GCS object generation,
SHA-256 and file count. All 9,197 files were compared byte-for-byte with the
tracked checkout before removal, and the uploaded object's checksum was checked.
The archive includes complete reports, settings, hardware snapshots, source and
binary identities, runtime patches, logs, captured drivers and rejected attempts.

Retrieve it with access to Antfly's artifact bucket, verify the recorded SHA-256,
and extract it into a separate evidence directory. Its paths are repository
relative; do not extract historical source files over an active checkout.

```sh
gcloud storage cp "$(python3 -c 'import json; print(json.load(open("zig/pkg/inference/scripts/lightonocr3/results/archive.json"))["uri"])')" /tmp/lightonocr3-history.tar.gz
shasum -a 256 /tmp/lightonocr3-history.tar.gz
mkdir -p /tmp/lightonocr3-evidence
tar -xzf /tmp/lightonocr3-history.tar.gz -C /tmp/lightonocr3-evidence
```

The source and unselected runtime experiments also remain on
[`archive/lightonocr3-experiments-47ccdc89ad`](https://github.com/antflydb/antfly/tree/archive/lightonocr3-experiments-47ccdc89ad).
These are historical qualifications of the identified builds. Cleanup changes
must receive fresh checks before inheriting any performance claim.

| Hardware / model | Native pages/s | Reference pages/s | Evidence in archive |
| --- | ---: | ---: | --- |
| M4 Max, LightOn 0.8B | 4.725–4.736 | vLLM 4.097–4.102 | `results/throughput_engine_m4_max/` |
| M4 Max, LightOn 4B | 1.210–1.211 | vLLM 1.1517–1.1518 | same |
| M4 Max, Florence | 5.306–5.309 | MLX proxy 5.131–5.138 | same |
| DGX Spark, LightOn 0.8B, partial BF16 prefill FFN | 9.613 / 9.975 | vLLM 14.569 | `results/cuda_spark_checkpoint/` |
| DGX Spark, LightOn 4B, partial BF16 prefill FFN | 3.421 / 3.427 | vLLM 4.630 | same |
| DGX Spark, Florence FP16, canonical GEMM | 25.347–25.758 | vLLM 25.340–25.348 | same |

Paths above are under `zig/pkg/inference/scripts/lightonocr3/`. The Metal runs
are paired comparisons. CUDA rows are separate capacity screens, not adjacent
paired runs; the current LightOn comparison is below. All use three synthetic
invoice, receipt and shipping fixtures, not a representative OCR evaluation.
Native accepts encoded images; references may accept decoded PIL images or use
HTTP. Metal Florence uses Q4_K/F16 encoder versus an FP16 MLX proxy. CUDA Florence
uses original FP16. Defaults retain F32 activations and recurrent state; the
partial BF16 options are not complete activation-precision matches. An older
rare CUDA Florence receipt mismatch is not definitively root-caused.

## CUDA follow-up qualification

[Follow-up #1331](https://github.com/antflydb/antfly/pull/1331) fuses the 0.8B
output projection/top-token selection and both vision MLP projections on SM12.1.
The vision kernels preserve the original bias, activation and residual rounding.
They avoid cuBLASLt BF16 bias-epilogue cliffs observed against vLLM's actual Torch
linear path. All raw measurements and settings are retained outside Git.

The current source also adds asynchronous mixed-prefix/decode token readback,
actual-row 4B head algorithm selection, independently admitted event-based
metadata staging, and same-stream reuse of owned deferred allocations. These changes are correctness-qualified. The table below still
identifies the earlier frozen767 build; fresh comparisons of the current879 build
are queued for a quiet GPU window. Earlier contended screens are excluded. No new throughput gain is
claimed for the execution update yet.

| Model | Native pages/s | Actual vLLM pages/s | Native versus reference mean |
| --- | ---: | ---: | ---: |
| LightOn 0.8B | 15.1768 / 15.1975 | 14.4855 / 14.4823 | +4.9% |
| LightOn 4B | 4.6341 / 4.5643 | 4.6563 / 4.6574 | -1.2% |

0.8B exceeds the fresh reference on these fixtures. 4B remains slightly behind;
this PR does not claim that both models beat vLLM. These are isolated DGX Spark
GB10 runs with 256 warmup requests and two 256-request measurements, 128 native
physical slots and at most 128 active vLLM sequences. All native outputs match the
frozen native serial bytes and complete raw vLLM reference text. All rounds use
actual wide four-token chains, unchanged operator byte limits, a 1,024-entry plan
bound and no uncached/scalar fallbacks. No competing GPU process was observed.

Both rows select `ANTFLY_CUDA_LIGHTON_PRECISION_PROFILE=bf16_vision`. Patch embedding,
the final vision merger, decoder projection outputs/residuals and recurrent state
remain F32; existing optional BF16 prefill and F16-query/F32-output paged prefill
remain enabled. This is a partial precision profile. `f32` selects the conservative
vision path. The 0.8B run also uses a 19 GiB temporary-cache budget and norm-weight
cache (`ANTFLY_INFERENCE_CUDA_TEMP_CACHE_MB=19456` and
`ANTFLY_CUDA_ENABLE_LIGHTON_NORM_WEIGHT_CACHE=1`), within unchanged memory limits.
The 4B run retains a 16 GiB cache and disables that norm-weight cache.

Current qualification includes 271 default and 110 wide CUDA tests, both models' F32 vision
checks at batches 1,1,2,3,8,16, and all 118 independent BF16 vision-block cases
bit-for-bit, including 65,536 rows and batch 16. The head also matches independent
IEEE F32 PyTorch logits in 12 real-checkpoint cases with exact token choices and
an unchanged 2e-4 winning-score limit. Readonly inputs, canaries, suppression,
ties, alignment and alias checks remain enforced. The largest vision cases keep
the original 60 GiB memory guard; the harness releases only unused test memory and
its immutable input file-cache pages before checking it. Metal-enabled compilation
and 13 CPU ownership/bounds/readback checks pass; no new Metal GPU qualification is claimed. The 4B head descriptor also passes all
128 actual batch sizes with and without suppression: 256 independent full-logit
checks, exact tokens, readonly inputs and canaries, with maximum error 5.61e-6
against the unchanged 2e-4 limit. All four existing upload rollback combinations,
denied host admission, event reuse and synchronous-download rollback pass. Deferred
reuse also preserves pinned before/after byte copies at an exact physical quota,
unchanged admission charges, exact recurrent-state ownership and poisoned-state
reset. Existing cache/free rollback controls remain; CUDA math and precision are
unchanged.

The [compact manifest](followup-1331.json) records the 79 frozen runtime inputs,
binary, checkpoint/fixture/report hashes, generator versions and content-addressed
evidence locations. The 26.5 MB current qualification bundle contains only selected
runtime source/binary, synthetic correctness checks and ownership evidence. The
separate `reproduction.performance_evidence` bundle retains the frozen767
measurements and exact controls shown above.
It remains on Glowworm and can be retrieved through the already authorized host:

```sh
tailscale ssh glowworm 'cat ARTIFACT_PATH' > qualification879.tar.gz
shasum -a 256 qualification879.tar.gz
```

Replace `ARTIFACT_PATH` with `reproduction.evidence.remote_path` from the manifest
and verify `reproduction.evidence.sha256` before extraction. Extract into a separate
evidence directory. These are three synthetic invoice/receipt/shipping pages;
representative OCR accuracy and broader workloads still need evaluation.

## Reproduce

Keep the fixture definitions in `../corpus/cases.json`, their images, the invoice
under `src/models/testdata/lightonocr3/`, and the pinned checkpoint revisions in
the user guide. Build a ReleaseFast inference test executable with Zig 0.17 and
`-Dtest-filter=LightOnOCR3`; CUDA builds also require `-Dcuda=true`.

- `benchmark_concurrency.py` measures native encoded-image serving. It validates
  every timed output against native serial decoding and accepts a full reference
  report for cross-runtime content checks.
- `benchmark_vllm_metal.py` and `benchmark_vllm_offline.py` run actual installed
  reference engines. Use unique media IDs and disable prefix/processor caches.
- `test_benchmark_vllm_metal.py` checks reference cache-capacity accounting.
- `build_cuda_bf16_prefill.py` reproduces the optional SM12.1 native BF16
  recurrent prefill library with pinned source and artifact fingerprints.
  `check_cuda_bf16_prefill.py` checks its output/state against the independent
  FlashInfer wrapper, short-prefix histories, read-only inputs, canaries and
  insufficient scratch. Default F32 checks retain their original tolerances.
- `build_cuda_prefill_attention.py` builds the optional SM12.1 F16-query,
  F32-output paged prefill extension beside a selected recurrent library.
  `check_cuda_prefill_attention.cpp` includes its native-only build command and
  independently checks causal addressing, permuted physical pages, plan reuse,
  overwritten CPU metadata, workspace bounds and input/output canaries. These
  constructed checks do not constitute a general accuracy evaluation.
- `check_cuda_lighton_bf16_vision.py` invokes the actual serving vision block
  through the qualification-only C ABI below. Its PyTorch oracle uses original
  checkpoint weights and independent Transformers operators. It preserves the
  fixed error bounds and records full checkpoint fingerprints.
- `check_cuda_lighton_attention_inputs.py` loads the serving PTX and checks its
  Q/K normalization, partial M-RoPE and sigmoid against independent Transformers
  operators with real checkpoint norm weights, readonly inputs and output canaries.
- `verify_cuda_florence.sh --schedule-stress` adds the F64 cross-attention oracle
  with delayed warp scheduling, preserving the original numerical tolerance.
- `verify_cuda_{recurrent,attention,activations,vision,florence}.sh` reproduce the
  independent numerical, state, read-only and allocation-canary checks.
- `check_metal_{projections,vision,prefill_attention}.m` retain independent Metal
  differential checks and their generation/compilation commands.

Use each entry point's `--help` and archive report settings for the exact command,
checkpoint/image fingerprints, operator byte limits, precision and warmups of a
historical run. New parity comparisons must report actual cohorts, equal useful
token work, saturation warmups, complete output agreement and fresh reference
measurements. Keep new raw results outside tracked source directories.

For the BF16 vision check, run these commands from the repository root with Zig
0.17. The target below is the qualified DGX Spark target; run the resulting
library and checker on that CUDA host with PyTorch, Transformers, safetensors
and cuDNN available. This helper is not linked into serving binaries.

```sh
mkdir -p zig-out
zig build-lib -dynamic -fPIC -OReleaseFast -lc \
  -target aarch64-linux.7.0...7.0-gnu.2.39 \
  -mcpu cortex_a725+aes+sha2+sha3+sm4-perfmon-spe \
  --dep antfly_platform \
  -Mroot=zig/pkg/inference/src/lighton_vision_bf16_qualification.zig \
  -Mantfly_platform=zig/lib/platform/src/root.zig \
  --cache-dir .zig-cache/local --global-cache-dir .zig-cache/global \
  -femit-bin=zig-out/libvision.so
python3 zig/pkg/inference/scripts/lightonocr3/check_cuda_lighton_bf16_vision.py \
  --library zig-out/libvision.so --library-sha256 "$LIBRARY_SHA256" \
  --model-08 "$MODEL_08" --model-4b "$MODEL_4B" \
  --output-dir "$NEW_REPORT_DIRECTORY"
```

Set `LIBRARY_SHA256` to the built library's SHA-256 and model paths to the original
checkpoint directories. The output directory must be new. Regenerate the checked-in
PTX on a CUDA build host with the pinned NVCC version in the manifest:

```sh
nvcc -ptx -arch=sm_80 -O3 --fmad=false -std=c++17 \
  zig/pkg/inference/src/ops/cuda/prototypes/lighton_vision_bf16.cu \
  -o zig/pkg/inference/src/ops/cuda/lighton_vision_bf16.ptx
python3 - <<'PYGEN'
from pathlib import Path
p = Path("zig/pkg/inference/src/ops/cuda/lighton_vision_bf16.ptx")
p.write_bytes(p.read_bytes().rstrip() + b"\n")
PYGEN
```

For the F32 attention-input kernel, use the pinned NVCC version in the compact
manifest and normalize its final newline before building:

```sh
nvcc --ptx -arch=compute_75 -O3 --fmad=false -std=c++17 \
  zig/pkg/inference/src/ops/cuda/prototypes/lighton_attention_inputs.cu \
  -o zig/pkg/inference/src/ops/cuda/lighton_attention_inputs.ptx
python3 - <<'PYGEN'
from pathlib import Path
p = Path("zig/pkg/inference/src/ops/cuda/lighton_attention_inputs.ptx")
p.write_bytes(p.read_bytes().rstrip() + b"\n")
PYGEN
python3 zig/pkg/inference/scripts/lightonocr3/check_cuda_lighton_attention_inputs.py \
  --runtime-root . --ptx zig/pkg/inference/src/ops/cuda/lighton_attention_inputs.ptx \
  --model-08 "$MODEL_08" --model-4b "$MODEL_4B" --output "$NEW_REPORT_JSON"
```

This check uses the fixed F32 numerical budget. It does not allocate or access KV
or recurrent state, pad inactive request rows, or measure OCR throughput.

The SM12.1 vision up/GELU kernels are generated offline with Triton 3.7.1 and the
pinned CUDA 13.0 libdevice. Regeneration requires no GPU; serving requires neither
Python nor Triton. PTX 9.0 targets the qualified driver. From the repository root:

```sh
TRITON_CACHE_DIR="$PWD/.zig-cache/triton-vision" python3 \
  zig/pkg/inference/src/ops/cuda/prototypes/lighton_vision_gemm.py \
  --libdevice /usr/local/cuda-13.0/nvvm/libdevice/libdevice.10.bc \
  --output zig/pkg/inference/src/ops/cuda
```

The generator asserts the math-library fingerprint, 128 threads, 24 KiB shared
memory and zero global/profile scratch. Keep its generated manifest with external
qualification evidence; it is not needed by serving binaries.

The head and vision down kernels are generated offline with Triton 3.7.1/PTX 9.0;
serving requires neither Python nor Triton. Generate outside tracked source paths:

```sh
TRITON_CACHE_DIR="$PWD/.zig-cache/triton-ocr" python3 \
  zig/pkg/inference/src/ops/cuda/prototypes/lighton_head.py \
  --output .zig-cache/generated-lighton-head
cp .zig-cache/generated-lighton-head/*.ptx zig/pkg/inference/src/ops/cuda/
TRITON_CACHE_DIR="$PWD/.zig-cache/triton-ocr" python3 \
  zig/pkg/inference/src/ops/cuda/prototypes/lighton_vision_down.py \
  --output .zig-cache/generated-lighton-down
cp .zig-cache/generated-lighton-down/*.ptx zig/pkg/inference/src/ops/cuda/
python3 zig/pkg/inference/scripts/lightonocr3/check_cuda_lighton_head.py \
  --model-dir "$MODEL_08" \
  --partial-ptx zig/pkg/inference/src/ops/cuda/lighton_head_top1_partial.ptx \
  --finish-ptx zig/pkg/inference/src/ops/cuda/lighton_head_top1_finish.ptx \
  --output "$NEW_REPORT_JSON"
```

The head uses 256 threads/32 KiB shared memory plus a small reduction; vision down
uses 128 threads/48 KiB. Both generators assert zero global/profile scratch. The
existing fused-vision control selects the original path when disabled;
`ANTFLY_CUDA_DISABLE_LIGHTON_FUSED_HEAD=1` restores dense head projection/argmax.
