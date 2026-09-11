# GLiNER2.5 FP32 performance results, v2

These measurements describe the frozen build identified below. Subsequent
[PR review fixes](PR_REVIEW_FIXES.md) have separate correctness validation;
the latency campaign has not been rerun for that later source.

The optimized Metal policy passed the original 30-case milestone on an Apple M4
(Mac16,12, 16 GiB, macOS 26.5) on AC power with low-power mode disabled.
All 30 model/task combinations were faster than pinned Fastino/PyTorch MPS in
every one of three fresh-process repetitions. Paired speedups ranged from
2.08x to 50.42x across the 90 case/repetition results; the smallest lower 95%
confidence bound was 1.666x. All six prior latent/anchorless winners remained
wins in every repetition, with a minimum lower bound of 15.731x.

This is the direct-core FP32 comparison in [the v2 protocol](METAL_BENCHMARK_V2.md).
It includes schema compilation, processing, encoder, heads, decoding and temporary
cleanup. Batch size is one, math threads one, with five warmups and thirty balanced
pairs per case/repetition. Confidence intervals use 10,000 paired bootstrap
resamples and seed 20260730. Intervals are per repetition, not simultaneous or
pooled claims. Exact output decisions/order/offsets and actual token packets pass;
the original absolute confidence tolerance remains 5e-4. Loading and protocol I/O
are outside the clock. No reduced precision, host fallback, diagnostic phase
instrumentation, tolerance changes or new math kernels were used.

## Original 30 cases

Each latency cell spans the three per-repetition medians in milliseconds. Speedup
spans the three paired median ratios; the last column is the smallest lower 95%
Python/Metal bound. The milestone requires every bound >=5/6; retaining an earlier
win requires every bound >1.

| Model | Task | Metal ms | Python MPS ms | Paired speedup | Min lower 95% |
| --- | --- | ---: | ---: | ---: | ---: |
| small | mixed_tasks | 18.48–19.52 | 68.51–70.26 | 3.54–3.61x | 2.739x |
| small | unicode_offsets | 14.67–15.32 | 43.43–44.54 | 2.85–3.03x | 2.596x |
| small | entity_attributes | 17.40–17.65 | 57.98–59.84 | 3.17–3.37x | 2.876x |
| small | legacy_structure | 14.27–14.76 | 38.03–40.66 | 2.61–2.64x | 2.480x |
| small | record_natural | 16.10–17.39 | 97.17–98.99 | 5.82–6.11x | 5.164x |
| small | record_latent | 19.80–20.17 | 403.08–424.04 | 20.17–21.05x | 19.688x |
| small | record_anchorless | 21.32–22.10 | 443.44–465.24 | 20.72–20.94x | 20.364x |
| small | enum_field | 17.13–17.59 | 56.57–59.33 | 3.40–3.52x | 2.814x |
| small | constrained_classification | 9.35–9.42 | 25.36–26.82 | 2.67–2.82x | 2.545x |
| small | joint_ie | 18.65–19.74 | 146.78–149.21 | 7.31–7.78x | 5.619x |
| base | mixed_tasks | 20.61–21.68 | 71.56–72.49 | 3.26–3.28x | 3.065x |
| base | unicode_offsets | 19.99–21.28 | 45.35–45.81 | 2.17–2.31x | 2.107x |
| base | entity_attributes | 22.04–22.85 | 62.41–66.17 | 2.80–2.89x | 2.675x |
| base | legacy_structure | 16.95–17.43 | 41.54–42.97 | 2.40–2.46x | 2.332x |
| base | record_natural | 18.15–18.58 | 96.73–97.49 | 5.26–5.33x | 5.186x |
| base | record_latent | 21.21–28.40 | 713.25–748.21 | 26.51–31.78x | 20.991x |
| base | record_anchorless | 36.00–37.79 | 621.23–653.42 | 17.08–17.26x | 16.846x |
| base | enum_field | 19.25–25.14 | 66.75–68.45 | 2.80–3.25x | 1.928x |
| base | constrained_classification | 12.40–12.60 | 28.76–29.62 | 2.26–2.30x | 2.203x |
| base | joint_ie | 19.23–20.03 | 145.29–146.55 | 7.25–7.57x | 6.885x |
| multi | mixed_tasks | 22.92–23.51 | 72.93–73.94 | 3.08–3.17x | 2.967x |
| multi | unicode_offsets | 21.10–22.17 | 45.97–47.66 | 2.13–2.17x | 2.044x |
| multi | entity_attributes | 23.07–24.28 | 63.56–65.04 | 2.63–2.76x | 2.555x |
| multi | legacy_structure | 17.73–17.79 | 41.62–42.52 | 2.30–2.38x | 2.257x |
| multi | record_natural | 18.68–19.23 | 96.42–96.82 | 5.12–5.17x | 4.996x |
| multi | record_latent | 21.46–29.32 | 1064.40–1071.20 | 37.89–50.42x | 29.649x |
| multi | record_anchorless | 37.37–37.90 | 621.35–624.26 | 16.32–16.50x | 15.731x |
| multi | enum_field | 31.47–33.92 | 69.88–72.96 | 2.30–2.60x | 1.666x |
| multi | constrained_classification | 13.69–13.86 | 28.79–31.35 | 2.08–2.12x | 2.032x |
| multi | joint_ie | 19.83–19.87 | 144.77–147.40 | 7.23–7.35x | 6.889x |

## CPU preservation

The fresh native CPU binary passed all 30 cases in every one of three process
repetitions against pinned Fastino/PyTorch CPU. Paired native/Python latency
ratios ranged from 0.5094 to 0.8538 (1.17x–1.96x speedups). Every upper 95%
latency-ratio bound was below 1; the largest was 0.9086044. The original CPU
protocol remains five warmups, thirty balanced pairs and 2,000 bootstrap
resamples per case/repetition. Its 90 intervals were rederived from raw pairs.
The shared build receipt binds this CPU binary and the optimized Metal binary
to the same frozen source. Historical CPU measurements are not used as proof.

## Scaling correctness and measurement limits

The unchanged scaling corpus has 129 cases and 348 batch items across three
models: S128/256/512, B1/2/4, ragged Unicode/JointIE and B8/S128 correctness-only
smoke. All 129 native Metal cases match the saved Python CPU outputs and exact
actual token IDs, masks and shapes. All three native owners stop cleanly with
zero retained model, workspace, transient and pending device allocations.

The separate MPS scaling campaign retains its full 129-row denominator.
Multilingual completed 43 preflight cases, 42 measured cases (126 pairs) and one
B8 correctness-only row. Small and base each exceeded the unchanged 30-second
MPS response deadline on their first expanded latent case; their 86 campaign
rows retain failure/lifecycle evidence. These failures are not speedup values.
The native-only correctness check does not claim an MPS comparison.

Separate small/base supplements each completed the other 25 cases: 24 measured
cases and one B8 correctness-only case. Their exact manifest-approved selection
covers mixed tasks, natural records, Unicode and JointIE; it excludes the two
record families affected by the timed-out session. No input, deadline, threshold,
tolerance or sampling rule changed, and the original 129-row partial report is
unchanged. Across the primary and supplementary evidence, 93 unique cases have
completed CPU/Metal/MPS validation: 90 with descriptive timings and three B8
smokes. The remaining 36 small/base latent/anchorless cases have native-versus-CPU
correctness proof, but no completed MPS timing comparison.

| Model | Measured scaling cases | Descriptive paired speedup range |
| --- | ---: | ---: |
| Small supplement | 24 | 0.94x–31.95x |
| Base supplement | 24 | 1.05x–16.93x |
| Multilingual primary | 42 | 0.86x–53.18x |

Values below 1 mean Metal was slower in that small descriptive sample. These
shape results do not imply that Metal wins every scaling case.

Scaling uses one warmup and three pairs. Its timings are descriptive: no
confidence intervals and no original-30 latency acceptance claim. The failed
three-case smoke is also retained; its JointIE outputs now match both CPU/MPS,
and its native S512/B4 latent result matches CPU, but the MPS latent call timed out.

## Implementation and validation

[Execution design](METAL_OPTIMIZATION.md) describes immutable model residency,
bounded admitted workspaces, command scopes, derived relative-position reuse
and schema-dependent heads. Record decoding now discards complete-key duplicates
before retaining values under unchanged caps. Single-window JointIE defaults to
the versioned source beam profile; explicit native algorithms and independent
windowed global resources remain distinct. OpenAPI and Go/Python/TypeScript SDKs
preserve omitted decoder settings, and qualification binds resolved profiles.

The final decoder/integration suite passed 82 selected tests with no skips,
including all nine independently captured source optimizer cases, allocation
failure/cancellation checks, global window constraints and qualification.
Focused SDK checks passed: Python 44, Go extraction tests, TypeScript 45 plus
type checking. Earlier residency/workspace/control checks and real Metal HTTP,
queued-cancellation and cache-lifecycle tests are preserved separately.

## Independent audit

The final independent audit rederived all 2,700 Metal/MPS pairs and all 90
10,000-resample confidence intervals. It checked 6,540 output responses,
120 validation token pairs (96 also against frozen source captures), all 12
native/MPS stop-and-cleanup proofs, physical ownership counters, and source,
binary, model, input and power identities. Its receipt is
`/private/tmp/gliner25-final-fp32-audit-v2/result-v3/audit.json`, SHA256
`305b9c4a597415a8d2d1953e2fc9872c1b42b42cd505dcd4e70615a715d1ae73`.
The earlier auditor attempt is retained: its command check incorrectly required
an explicit false diagnostics flag. The revised checker accepts only the exact
omitted-default or explicit-false command and still requires actual diagnostics
disabled and absent phase measurements; ten checker regressions pass.

Both supplementary runs were independently audited: all 144 raw pairs,
output/token checks, ownership transitions and graceful shutdowns pass. The
original 129-row report was rehashed unchanged. Receipt:
`/private/tmp/gliner25-final-fp32-supplements-audit-v1/receipt.json`, SHA256
`6424b48af477b72fda83611cb377e54e17b35a4e5c888129fc5d3282b130b3a0`.

The final cross-check explicitly joins the standalone CPU audit to the shared
build receipt and the Metal audit. All 125 recorded process identities were
rechecked as exited. Receipt:
`/private/tmp/gliner25-final-fp32-result-v1/receipt.json`.

## Evidence identity

Both ReleaseFast binaries were built serially through the repository graph with
source snapshots identical before and after. No staging, commit or push was made.

- Source tree SHA256: `ada826e9d8a597cd05a2515658d964f5369e9a9f530f63a122c1fa7b5aad3046`.
- Metal binary SHA256: `93d3d3afe51b15402c2002d39ab5ba585a591bfefc0e89836a0e67bc20efa945`.
- CPU binary SHA256: `965309d2ac6dd36df0b1bc6fe7f6b7b3d63b4b95688bee008f4be429512d32cc`.
- Pinned Fastino commit: `3c913c7369301133d3b7699252074c4303ada50e`.
- Build receipt: `/private/tmp/gliner25-fp32-final-build-v3/receipt.json`.
- Original-30 raw evidence: `/private/tmp/gliner25-final-fp32-metal-v2/`.
- Full scaling evidence: `/private/tmp/gliner25-final-fp32-scaling-v1/`.
- Current CPU preservation: `/private/tmp/gliner25-final-native-cpu-preservation-v1/audit.json`.
- Small/base supplementary scaling: `/private/tmp/gliner25-final-fp32-scaling-{small,base}-supplement-v1/`.
- Native 129-case evidence: `/private/tmp/gliner25-final-fp32-native-scaling-v1/`.
- Decoder tests: `/private/tmp/gliner25-fp32-decoder-tests-v2.log`.

The old [v1 results](METAL_BENCHMARK_RESULTS.md) remain historical. These local
comparisons do not qualify serving load, another device, another precision,
training, or public release. Serving/release qualification flags remain false.
