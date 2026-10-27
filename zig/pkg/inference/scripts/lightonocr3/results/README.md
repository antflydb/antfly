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

The bounded two-token decoder graph has fresh DGX Spark qualification for both
LightOn models with default decode, optional BF16 prefill FFN, and optional BF16
recurrent prefill. Every timed output agrees with native serial decoding and the
fresh vLLM reference's normalized content on all three fixtures. The six 64-page
runs require positive model-graph replay counters. The CUDA regression groups
pass (240 selected: 226 passed, 14 backend skips; wide group: 76 selected,
75 passed, one skip), including independent admission, ownership and state checks.
These are functional checks, not a sustained vLLM parity result.

Use `cuda_ocr_profile.py --decode-graph` and the native harness's
`--require-lighton-model-graph`. Graph rows follow stable sequence IDs and both
output columns return to the scheduler's original claim order. The public
attention verifier also checks repeated two-token replay, reset, complete KV
contents and unused slots with the original F32 threshold. The
[follow-up archive manifest](followup.json) records the immutable evidence URI,
object generation and SHA-256. Its 80 files include the exact frozen binary,
source, commands, six reports, reference outputs and per-file checksums; ambient
authentication and session variables are excluded.

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
- `verify_cuda_{recurrent,attention,activations,vision,florence}.sh` reproduce the
  independent numerical, state, read-only and allocation-canary checks.
- `check_metal_{projections,vision,prefill_attention}.m` retain independent Metal
  differential checks and their generation/compilation commands.

Use each entry point's `--help` and archive report settings for the exact command,
checkpoint/image fingerprints, operator byte limits, precision and warmups of a
historical run. New parity comparisons must report actual cohorts, equal useful
token work, saturation warmups, complete output agreement and fresh reference
measurements. Keep new raw results outside tracked source directories.
