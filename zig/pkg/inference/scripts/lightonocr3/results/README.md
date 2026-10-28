# OCR performance evidence

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
paired runs; LightOn CUDA parity remains outstanding. All use three synthetic
invoice, receipt and shipping fixtures, not a representative OCR evaluation.
Native accepts encoded images; references may accept decoded PIL images or use
HTTP. Metal Florence uses Q4_K/F16 encoder versus an FP16 MLX proxy. CUDA Florence
uses original FP16. Defaults retain F32 activations and recurrent state; the
partial BF16 options are not complete activation-precision matches. An older
rare CUDA Florence receipt mismatch is not definitively root-caused.

## CUDA follow-up qualification

[Follow-up #1331](https://github.com/antflydb/antfly/pull/1331) keeps 0.8B vision-block
activations BF16 on SM12.1 and avoids the 4B joined-projection split copies.
It also fuses F32 language attention preparation and projection/residual operations.
The F32 vision output retains the ordinary GEMM and combines bias/residual in one
pass, avoiding the slower large-shape cuBLASLt bias epilogue.
Patch embedding, the final vision merger, decoder projection outputs and recurrent
state remain F32. The 4B preset retains F32 vision because BF16 vision did not
produce a repeatable throughput gain. `ANTFLY_CUDA_LIGHTON_PRECISION_PROFILE=f32`
selects the original vision path.

The current head additionally fuses the vision up projection with GELU on SM12.1,
retaining both the original F32 arithmetic and the BF16 pre-activation rounding.
It uses no global workspace and retains the existing fused-vision rollback.
The new build passes 265 default and 106 wide CUDA checks, both models' original
real-checkpoint checks, and all 118 independent BF16 vision-block cases bit-for-bit.
It also preserves ordinary cache reuse while accounting cached bytes in constant
time, and selects two cold-qualified 0.8B projection descriptors.

The current 0.8B measurement is **14.4186 / 14.6407 pages/s**, with the same output,
capacity and admission gates. It uses a 19 GiB temporary-cache budget and norm-weight
caching (`ANTFLY_INFERENCE_CUDA_TEMP_CACHE_MB=19456` and
`ANTFLY_CUDA_ENABLE_LIGHTON_NORM_WEIGHT_CACHE=1`), within unchanged memory limits.
The prior vLLM mean is 14.4377 pages/s. A fresh reference is pending, so this narrow
lead does not establish a repeatable win. Current 4B timing overlapped another GPU
job and is excluded; its next isolated comparison is pending.

The table below records the preceding snapshot559/565, before this vision-GEMM change:

| Model | Native pages/s | Actual vLLM pages/s | Selected vision |
| --- | ---: | ---: | --- |
| LightOn 0.8B | 14.1538 / 13.9947 | 14.3536 / 14.5218 | BF16 blocks |
| LightOn 4B | 4.3075 / 4.3297 | 4.6198 / 4.6258 | F32 |

These are separate isolated runs on DGX Spark GB10: 256 warmup requests and two
256-request measurements, 128 physical native slots and at most 128 active vLLM
sequences. Native timed outputs match independent native serial bytes and raw vLLM text
on three synthetic pages. Native uses actual wide four-token
chains, unchanged operator byte limits, a 1,024-entry cuBLAS plan bound and zero
uncached/scalar fallbacks. The optional BF16 FFN/recurrent prefill and F16-query,
F32-output paged prefill extensions remain enabled. This is a partial precision
profile. At that preceding snapshot, reaching the vLLM means required about 2.6% more
throughput for 0.8B and 7.0% for 4B. CUDA parity and representative OCR accuracy remain outstanding.

The [compact manifest](followup-1331.json) records source, binary, checkpoint,
fixture and report hashes. The cleaned vision block matches the independent
Transformers/PyTorch cuDNN reference bit-for-bit in all 118 cases across every
checkpoint layer, batches up to 16 and 65,536 rows. Wrong-row, cross-image,
alias and alignment controls are rejected. These checks qualify the matching
cuDNN backend; they do not establish Flash Attention equivalence.

The preceding snapshot's F32 CUDA regression: 275 selected, 261 passed, 14 skips; wide regression:
104 selected, 103 passed, one skip. The new attention-input PTX also passes 40
independent Transformers cases using every checkpoint Q/K norm pair and text/image
coordinates. Maximum error is below 2e-6 against the unchanged 2e-4 bound; inputs
and output canaries remain unchanged. Both original real-checkpoint feature/prompt
checks pass at batch sizes 1, 2, 3, 8 and 16. Metal compilation and seven focused
CPU ownership/admission/profile checks pass; no new Metal GPU speed is claimed.
Raw follow-up reports remain outside the source tree and are not yet published.
Earlier versioned evidence remains in [followup.json](followup.json),
[the wide-cohort bundle](followup-bfd1738430.json) and
[the 64-page bundle](followup-736f721b59.json).

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
