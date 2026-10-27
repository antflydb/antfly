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

The selected `05a8fc248f` runtime on DGX Spark GB10 uses 256 warmup requests
and two 256-request measurements, 128 physical native slots and at most 128
active vLLM sequences. Every native timed output matches independent native
serial bytes and complete normalized actual-vLLM content on three synthetic
fixtures. Decode outputs and recurrent state remain F32; the measured profiles
use optional BF16 FFN/recurrent prefill and F16-query/F32-output paged prefill
attention. These profiles are not a full activation-precision match.

| Model | Native pages/s | vLLM pages/s | Required actual execution |
| --- | ---: | ---: | --- |
| LightOn 0.8B | 12.5125 / 12.4897 | 14.4262 / 14.4847 | Wide four-token chains in each round |
| LightOn 4B | 4.0103 / 4.0152 | 4.6341 / 4.6303 | Wide four-token chains in each round |

The 0.8B run uses vision/prefix limits 16/16; 4B uses 8/16 and 256-row
prefix chunks. Both enable the BF16 normalization-input mirror while retaining
F32 outputs. Operator byte limits are unchanged. Greedy execution reads token
IDs; 4B skips all 426 unused intermediate prefix heads per round. Both isolated
comparisons observed no foreign GPU compute process. LightOn parity and a
representative OCR accuracy evaluation remain outstanding.

All 50 frozen build345 source/config files match the selected commit. CUDA
regression: 259 selected, 245 passed, 14 backend skips; wide: 94 selected,
93 passed, one skip. Independent packet, allocation-denial, admission,
numerical and ownership checks retain their original tolerances. Packed-state
qualification includes both recurrent widths, F32/BF16 convolution snapshots,
poisoned reuse, short prefixes and reset against independent storage. The
latest runtime compiles for Metal and passes focused CPU tests; these are
not new Metal GPU throughput measurements.

Prior CUDA Florence build283 measurements were 25.5421–25.7362 pages/s versus
actual vLLM 25.3656 / 25.3890, with matching greedy/no-repeat policy and outputs
on the same limited fixture set. M5 Metal qualification is in the user guide.

The [archive manifest](followup.json) pins an immutable 77-file baseline bundle
and a minimized 56-file supplemental vision qualification bundle. It records
object generations, sizes, SHA-256 and verified MD5 values. The bundles cover
earlier selected runtimes; build345 and actual-vLLM317 logs remain local and
are excluded from those archived payloads. Binaries, source snapshots, full
reports and experiment logs stay outside tracked source directories. The
[previous wide-cohort bundle](followup-bfd1738430.json) and
[earlier 64-page functional bundle](followup-736f721b59.json) remain available.

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
