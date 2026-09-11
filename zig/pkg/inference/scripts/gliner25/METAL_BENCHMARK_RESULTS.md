# Recorded GLiNER2.5 Metal results

The full campaign completed on 2026-09-10 (Pacific) using the
[measurement contract](METAL_BENCHMARK.md). All 60 model/task/baseline
comparisons completed across three fresh-process repetitions. Metal led
Python MPS on the latent and anchorless record cases in all three models
(6 comparisons); Python MPS led on the other 24. Python CPU led in all
30 separate CPU comparisons. Every comparison had all three 95% intervals
on the same side of one.

The 10 short task fixtures, FP32, batch one, and one CPU math thread define
this result. This is a direct-core comparison on one host, with the production
native request lifetime including weight materialization, uploads and cleanup.
It is not a representative throughput corpus, a pure custom-kernel comparison,
or a serving/release qualification. See the contract for the distinct native
production-default and pinned deterministic PyTorch math profiles.

## Machine and validation

- Apple M4, Mac16,12, arm64, 16 GiB RAM, 10 logical CPUs, macOS 26.5.
- AC power and low-power mode off at every recorded measurement boundary.
- UTC start: `2026-09-10T23:54:20.588275+00:00`; finish: `2026-09-11T00:46:13.722561+00:00`.
- Zig 0.16.0, ReleaseFast, Metal enabled; other accelerator backends disabled.
- Pinned Python 3.12.3 and PyTorch 2.9.1; exact dependencies and all three
  original FP32 model identities are recorded in the report.
- 6 global preflight runs and 18 measured worker-pair runs; 5 warmups and
  30 balanced measured pairs per case per repetition.
- Separate verification rechecked 480 validation, 1,800 warmup and 10,800
  measured outputs (13,080 total), all 180 per-repetition distributions and
  bootstrap intervals, and the 60 aggregate rows.
- Exact canonical output decisions and matching cross-arm encoder tokens;
  only confidence allows the fixed absolute tolerance of 5e-4. Eight cases
  additionally match frozen token tensors; classification and JointIE have
  no frozen token tensors and retain their cross-arm token/output checks.
- Source snapshot, binary, model/reference fixtures, pinned upstream and
  dependencies verified. All 48 worker cleanup receipts completed without
  survivors or inspection errors; a final host process scan found no workers.
- Peak sampled combined process-tree RSS: 4,869,685,248 bytes
  (4.54 GiB), below the 8 GiB limit. This is sampled RSS, not a
  transient peak guarantee or an additional GPU allocation total.
- ReleaseFast build passed. All 54 new benchmark tests and 7 existing CPU
  benchmark tests passed, including deadline, descendant cleanup, fallback,
  output mismatch, resource limit and power-change failure cases.

## Evidence identity

These paths name local evidence retained outside Git. The tables below preserve
all published values in the repository; the raw responses remain in the local
campaign directory. Hashes bind this snapshot to that raw evidence. The source
manifest covers 3,676 source/build/script/JSON inputs, including uncommitted
files; Git HEAD alone does not identify the measured implementation.

| Artifact | Location | SHA-256 |
| --- | --- | --- |
| Report | `/private/tmp/gliner25-metal-all-tasks-v2-ac/report.json` | `bfa357af3e7e185ca24b66ef38b7f838be6e3a161d0e916cb1cf4e1a3159a822` |
| Evidence manifest (99 files) | `/private/tmp/gliner25-metal-all-tasks-v2-ac/evidence_manifest.json` | `9bb80b405598069cd33391fa6802973a50e5e6022e4ccdeae9d3694c744c31e7` |
| Verification receipt | `/private/tmp/gliner25-metal-all-tasks-v2-ac-verification.json` | `b947171c967d9185d1812398f9400e7b604b333284fcbb3da4a196d982a4de94` |
| Separate verification script | `/private/tmp/verify_gliner25_metal_campaign_v1.py` | `bef7eb8c7e6dbf82f4cb97140e564ceadc645dbe4f715619310ca95536ef9d77` |
| Source file-set digest | `/private/tmp/gliner25-metal-all-tasks-v2-ac/source_manifest.json` | `e27c0fa34a7cac4e5cfe2ddfba676b385219acf0b6fd4610d5dd766848d38ec7` |
| Native executable | `zig/pkg/inference/zig-out/bin/antfly-inference-gliner25-metal-bench` | `ff056509efc2579e5e314166b80e159dbc390be6ae9f9c1b195d8425781feaa8` |

Source HEAD: `aa44bddd1dd8befb5d0aec8bdb6c304054b89149` plus the source manifest's dirty tree.
Pinned Fastino upstream commit: `3c913c7369301133d3b7699252074c4303ada50e`.

The earlier campaign at `/private/tmp/gliner25-metal-all-tasks-v1` was
interrupted after power changed from battery to AC during its third
multilingual MPS repetition. The power guard invalidated that repetition;
its raw evidence and interruption/cleanup receipts were preserved. Its
measurements are excluded from these AC tables. The complete AC campaign
used the same source, executable, cases and sampling protocol in a new output
directory, with no task exclusions, tolerance changes, outlier trimming, or
result-driven retries.

## Complete comparison tables

Status: **complete**. Batch 1, FP32, one CPU math thread per arm.
Speedup is Python latency / Metal latency; values above 1 favor Metal.
Latencies are medians of the fresh-process repetition medians. Intervals describe each repetition separately.
Sample p95 is descriptive; this report does not qualify serving latency or release readiness.

## Fastino MPS reference

| Model | Request | Metal ms | Python ms | Speedup | 95% intervals by repetition | Result |
| --- | --- | ---: | ---: | ---: | --- | --- |
| small | mixed_tasks | 170.357 | 72.380 | 0.422× | 1: [0.403, 0.434]; 2: [0.404, 0.436]; 3: [0.406, 0.432] | python_faster |
| small | unicode_offsets | 175.326 | 46.470 | 0.276× | 1: [0.315, 0.348]; 2: [0.246, 0.294]; 3: [0.252, 0.300] | python_faster |
| small | entity_attributes | 187.411 | 62.352 | 0.340× | 1: [0.372, 0.422]; 2: [0.322, 0.370]; 3: [0.313, 0.370] | python_faster |
| small | legacy_structure | 161.195 | 41.569 | 0.270× | 1: [0.293, 0.312]; 2: [0.243, 0.279]; 3: [0.238, 0.281] | python_faster |
| small | record_natural | 180.649 | 98.205 | 0.591× | 1: [0.576, 0.615]; 2: [0.558, 0.637]; 3: [0.535, 0.650] | python_faster |
| small | record_latent | 164.240 | 430.537 | 2.609× | 1: [2.326, 2.425]; 2: [2.579, 2.850]; 3: [2.592, 2.876] | metal_faster |
| small | record_anchorless | 171.310 | 446.243 | 2.549× | 1: [2.492, 2.575]; 2: [2.535, 2.587]; 3: [2.578, 2.622] | metal_faster |
| small | enum_field | 174.084 | 59.164 | 0.375× | 1: [0.366, 0.407]; 2: [0.337, 0.386]; 3: [0.341, 0.378] | python_faster |
| small | constrained_classification | 139.362 | 30.605 | 0.233× | 1: [0.245, 0.268]; 2: [0.210, 0.234]; 3: [0.207, 0.237] | python_faster |
| small | joint_ie | 202.714 | 158.960 | 0.775× | 1: [0.761, 0.797]; 2: [0.762, 0.861]; 3: [0.740, 0.847] | python_faster |
| base | mixed_tasks | 329.753 | 94.308 | 0.294× | 1: [0.307, 0.342]; 2: [0.272, 0.343]; 3: [0.261, 0.333] | python_faster |
| base | unicode_offsets | 260.625 | 52.572 | 0.183× | 1: [0.165, 0.212]; 2: [0.179, 0.216]; 3: [0.175, 0.222] | python_faster |
| base | entity_attributes | 305.441 | 75.015 | 0.236× | 1: [0.220, 0.287]; 2: [0.228, 0.284]; 3: [0.214, 0.250] | python_faster |
| base | legacy_structure | 254.722 | 49.357 | 0.185× | 1: [0.170, 0.207]; 2: [0.176, 0.205]; 3: [0.174, 0.200] | python_faster |
| base | record_natural | 304.800 | 112.586 | 0.378× | 1: [0.336, 0.419]; 2: [0.351, 0.433]; 3: [0.351, 0.412] | python_faster |
| base | record_latent | 259.823 | 775.396 | 2.811× | 1: [2.227, 3.011]; 2: [2.306, 3.037]; 3: [2.527, 3.316] | metal_faster |
| base | record_anchorless | 297.805 | 640.922 | 2.039× | 1: [1.825, 1.963]; 2: [1.981, 2.092]; 3: [2.140, 2.749] | metal_faster |
| base | enum_field | 276.153 | 75.458 | 0.274× | 1: [0.221, 0.296]; 2: [0.241, 0.294]; 3: [0.244, 0.306] | python_faster |
| base | constrained_classification | 253.980 | 39.224 | 0.164× | 1: [0.145, 0.171]; 2: [0.147, 0.184]; 3: [0.165, 0.225] | python_faster |
| base | joint_ie | 307.764 | 175.248 | 0.600× | 1: [0.506, 0.582]; 2: [0.546, 0.630]; 3: [0.509, 0.632] | python_faster |
| multi | mixed_tasks | 323.400 | 93.603 | 0.284× | 1: [0.274, 0.301]; 2: [0.263, 0.296]; 3: [0.272, 0.304] | python_faster |
| multi | unicode_offsets | 329.949 | 54.118 | 0.162× | 1: [0.149, 0.171]; 2: [0.147, 0.177]; 3: [0.159, 0.181] | python_faster |
| multi | entity_attributes | 333.620 | 72.603 | 0.212× | 1: [0.192, 0.240]; 2: [0.198, 0.237]; 3: [0.199, 0.234] | python_faster |
| multi | legacy_structure | 318.769 | 48.535 | 0.150× | 1: [0.143, 0.165]; 2: [0.144, 0.164]; 3: [0.145, 0.154] | python_faster |
| multi | record_natural | 319.545 | 110.667 | 0.340× | 1: [0.315, 0.360]; 2: [0.307, 0.347]; 3: [0.327, 0.371] | python_faster |
| multi | record_latent | 305.040 | 1095.593 | 3.616× | 1: [3.136, 3.773]; 2: [3.288, 3.589]; 3: [3.235, 3.810] | metal_faster |
| multi | record_anchorless | 310.789 | 675.550 | 2.163× | 1: [2.067, 2.293]; 2: [1.846, 2.224]; 3: [1.880, 2.339] | metal_faster |
| multi | enum_field | 316.019 | 71.794 | 0.227× | 1: [0.207, 0.251]; 2: [0.204, 0.240]; 3: [0.202, 0.248] | python_faster |
| multi | constrained_classification | 304.661 | 50.220 | 0.167× | 1: [0.143, 0.180]; 2: [0.147, 0.178]; 3: [0.153, 0.181] | python_faster |
| multi | joint_ie | 332.942 | 166.604 | 0.490× | 1: [0.451, 0.530]; 2: [0.444, 0.521]; 3: [0.469, 0.536] | python_faster |

## Fastino CPU reference

| Model | Request | Metal ms | Python ms | Speedup | 95% intervals by repetition | Result |
| --- | --- | ---: | ---: | ---: | --- | --- |
| small | mixed_tasks | 223.159 | 21.563 | 0.098× | 1: [0.095, 0.102]; 2: [0.103, 0.108]; 3: [0.076, 0.092] | python_faster |
| small | unicode_offsets | 205.201 | 19.436 | 0.093× | 1: [0.087, 0.097]; 2: [0.101, 0.108]; 3: [0.077, 0.101] | python_faster |
| small | entity_attributes | 223.813 | 19.846 | 0.090× | 1: [0.082, 0.094]; 2: [0.096, 0.099]; 3: [0.071, 0.091] | python_faster |
| small | legacy_structure | 200.348 | 18.503 | 0.093× | 1: [0.089, 0.097]; 2: [0.099, 0.104]; 3: [0.073, 0.095] | python_faster |
| small | record_natural | 204.056 | 19.015 | 0.092× | 1: [0.089, 0.095]; 2: [0.100, 0.103]; 3: [0.073, 0.096] | python_faster |
| small | record_latent | 206.572 | 19.379 | 0.098× | 1: [0.094, 0.101]; 2: [0.104, 0.108]; 3: [0.085, 0.100] | python_faster |
| small | record_anchorless | 209.511 | 19.817 | 0.093× | 1: [0.086, 0.098]; 2: [0.102, 0.107]; 3: [0.082, 0.104] | python_faster |
| small | enum_field | 218.921 | 19.504 | 0.090× | 1: [0.082, 0.093]; 2: [0.098, 0.104]; 3: [0.070, 0.086] | python_faster |
| small | constrained_classification | 169.526 | 17.181 | 0.098× | 1: [0.089, 0.108]; 2: [0.113, 0.117]; 3: [0.083, 0.097] | python_faster |
| small | joint_ie | 234.012 | 24.287 | 0.105× | 1: [0.100, 0.109]; 2: [0.110, 0.115]; 3: [0.087, 0.102] | python_faster |
| base | mixed_tasks | 348.188 | 55.052 | 0.159× | 1: [0.153, 0.165]; 2: [0.156, 0.167]; 3: [0.165, 0.186] | python_faster |
| base | unicode_offsets | 321.565 | 52.906 | 0.169× | 1: [0.162, 0.174]; 2: [0.156, 0.170]; 3: [0.158, 0.196] | python_faster |
| base | entity_attributes | 333.029 | 51.978 | 0.160× | 1: [0.149, 0.167]; 2: [0.153, 0.161]; 3: [0.160, 0.178] | python_faster |
| base | legacy_structure | 298.726 | 43.553 | 0.159× | 1: [0.144, 0.166]; 2: [0.143, 0.153]; 3: [0.155, 0.169] | python_faster |
| base | record_natural | 309.322 | 45.244 | 0.148× | 1: [0.127, 0.160]; 2: [0.143, 0.155]; 3: [0.152, 0.171] | python_faster |
| base | record_latent | 305.207 | 48.599 | 0.155× | 1: [0.144, 0.160]; 2: [0.150, 0.162]; 3: [0.162, 0.185] | python_faster |
| base | record_anchorless | 315.051 | 48.015 | 0.151× | 1: [0.140, 0.162]; 2: [0.145, 0.156]; 3: [0.157, 0.174] | python_faster |
| base | enum_field | 320.871 | 47.877 | 0.151× | 1: [0.144, 0.161]; 2: [0.141, 0.148]; 3: [0.153, 0.169] | python_faster |
| base | constrained_classification | 280.812 | 46.013 | 0.175× | 1: [0.162, 0.183]; 2: [0.154, 0.165]; 3: [0.167, 0.188] | python_faster |
| base | joint_ie | 354.925 | 56.858 | 0.158× | 1: [0.152, 0.167]; 2: [0.156, 0.164]; 3: [0.168, 0.186] | python_faster |
| multi | mixed_tasks | 361.763 | 66.722 | 0.179× | 1: [0.163, 0.189]; 2: [0.171, 0.188]; 3: [0.149, 0.164] | python_faster |
| multi | unicode_offsets | 339.201 | 62.677 | 0.174× | 1: [0.166, 0.193]; 2: [0.172, 0.191]; 3: [0.147, 0.158] | python_faster |
| multi | entity_attributes | 352.688 | 58.518 | 0.154× | 1: [0.141, 0.166]; 2: [0.153, 0.172]; 3: [0.131, 0.140] | python_faster |
| multi | legacy_structure | 334.417 | 52.638 | 0.147× | 1: [0.140, 0.163]; 2: [0.145, 0.161]; 3: [0.125, 0.140] | python_faster |
| multi | record_natural | 331.258 | 52.705 | 0.158× | 1: [0.142, 0.169]; 2: [0.137, 0.170]; 3: [0.119, 0.129] | python_faster |
| multi | record_latent | 332.428 | 58.111 | 0.162× | 1: [0.156, 0.184]; 2: [0.154, 0.171]; 3: [0.140, 0.151] | python_faster |
| multi | record_anchorless | 337.035 | 55.287 | 0.154× | 1: [0.148, 0.169]; 2: [0.147, 0.164]; 3: [0.124, 0.135] | python_faster |
| multi | enum_field | 345.811 | 55.208 | 0.155× | 1: [0.150, 0.162]; 2: [0.146, 0.167]; 3: [0.130, 0.137] | python_faster |
| multi | constrained_classification | 306.906 | 54.681 | 0.170× | 1: [0.166, 0.180]; 2: [0.163, 0.180]; 3: [0.138, 0.151] | python_faster |
| multi | joint_ie | 361.526 | 65.453 | 0.178× | 1: [0.161, 0.189]; 2: [0.166, 0.186]; 3: [0.145, 0.155] | python_faster |

Full outputs, timings, identities, memory observations and diagnostics accompany report.json.
Native Metal uses production default math, including eligible Apple MPSMatrix operations. PyTorch MPS uses its pinned deterministic FP32 profile. Host decoding and native request uploads are timed.
The MPS and CPU comparisons use separate pairs; their Metal measurements must not be mixed.
