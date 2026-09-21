# CUDA candidate quality gate

`validate_gemma4_cuda_quality_candidate.py` provides deterministic quality
evidence for opt-in Gemma 4 CUDA candidates whose floating-point evaluation
order may differ from the production baseline. It supplements the existing
exact-token performance qualification; it does not replace that gate and has
no authority to enable, promote, or default a kernel.

## Reviewed suite

The versioned
`scripts/gemma4/fixtures/gemma4_cuda_quality_suite_v1.json` suite contains four
different 2,051-token prompts. Every prompt locks the rendered UTF-8 byte
count, rendered SHA-256, prompt-token count, and canonical prompt-token-ID
SHA-256. Token IDs use the embedded Gemma 4 GGUF BPE with BOS insertion and
special-token parsing; the digest is SHA-256 over the UTF-8 compact JSON integer
array. The prompts cover:

- a long free-form evidence handoff;
- exact evidence extraction with required names, status, and date;
- a bare JSON response with exact keys, types, and values;
- an exact arithmetic/format response.

The suite also locks greedy decoding, F16 KV, a 512-token prefill chunk, a
2,432-token capture capacity, budgets, and raw rendered-chat transport. Both
arms must reproduce every prompt-token contract on every repetition.

The default `exact-v1` profile requires identical generated token IDs and
decoded text, deterministic repeats within each arm, valid output, all semantic
contracts, required candidate routes, required baseline routes, zero forbidden
fallback/rejection counters, and stable content-addressed provenance. This is
the only profile whose successful result is labeled `quality_qualified=true`.
Even then, the result is only evidence for the independent promotion gate.
An alternate suite path is always collect-only even if it defines an exact
profile; only the reviewed in-repository suite and its `exact-v1` policy can
produce `quality_qualified=true`. The v1 suite bytes are pinned by SHA-256 in
the harness; a changed suite requires a reviewed new version and cannot silently
reuse the v1 qualification label.

`bounded-freeform-v1` must be selected by name and is collect-only. Three
structured canaries still require exact output. Only the free-form case may
diverge, and it must satisfy all of these diagnostic bounds:

- no more than one of four cases diverges;
- first divergence is at token 32 or later and after 20% of the common output;
- positional token divergence is at most 90%;
- output lengths differ by at most eight tokens;
- normalized decoded-text similarity is at least 0.60;
- both outputs independently satisfy the full semantic and validity contract.

These bounds help characterize deterministic numerical drift. They are not a
claim of semantic equivalence: bounded evidence is always written with
`quality_qualified=false`, the process exits nonzero, and it cannot default a
route.

## Running the gate

Use an empty output path outside the repository so evidence creation cannot
change the source snapshot being measured:

```sh
python3 zig/pkg/inference/scripts/gemma4/validate_gemma4_cuda_quality_candidate.py \
  --kernel-id <catalog-kernel-id> \
  --model .models/unsloth/gemma-4-E2B-it-qat-GGUF/gemma-4-E2B-it-qat-UD-Q4_K_XL.gguf \
  --output-dir /tmp/gemma4-cuda-quality-exact-v1
```

The candidate must be present in `CANDIDATE_CATALOG` with a typed baseline
value, a fail-closed candidate value, required route counters, and forbidden
fallback/rejection counters. Arbitrary candidate environment overrides are not
accepted. Parent environment variables are scrubbed to a small execution
allowlist; fixed comparison values and the selected catalog gate are then
applied explicitly.

The SM89 Gemma 4 E2B split-K online decode candidate is selected with
`--kernel-id cuda.attention.gqa.decode.splitk_online_sm89`. Its catalog
contract compares `ANTFLY_INFERENCE_CUDA_GQA_DECODE_PROFILE=off` against
`required-splitk-online-sm89` while holding the score-prework baseline at
`required-tiled64`. The locked 2,051-token/300-output case uses F16 KV,
prefill chunk 512, and capture capacity 2,432. Candidate evidence must report
exactly 140 split-K launches (112 HD256 and 28 HD512), no split-K fallback or
forbidden-route counters, and persistent graph replay without capture discards
or capacity skips. These totals are specific to the fresh-process CLI gate:
four host-visible graph-construction decode evaluations each traverse 35 Gemma
layers (28 local HD256 and 7 global HD512); the subsequent 295 captured graph
launches are reported by the independent persistent-replay counter. The
baseline must exercise both tiled64 head-dimension routes.

The warm-server benchmark has a deliberately different evidence scope. It
parses process-lifetime, bounded route telemetry and requires one observation
of each head dimension with no fallback or rejection; it does not treat those
observations as per-launch counters. Its coverage contract therefore remains
unchanged. Both gates supplement—and do not relax—the multi-prompt `exact-v1`
token and decoded-text policy.

For a diagnostic collection only:

```sh
python3 zig/pkg/inference/scripts/gemma4/validate_gemma4_cuda_quality_candidate.py \
  --kernel-id <catalog-kernel-id> \
  --model .models/unsloth/gemma-4-E2B-it-qat-GGUF/gemma-4-E2B-it-qat-UD-Q4_K_XL.gguf \
  --threshold-profile bounded-freeform-v1 \
  --output-dir /tmp/gemma4-cuda-quality-bounded-v1
```

At least two balanced baseline/candidate repetitions are mandatory. The gate
records latency and throughput for diagnosis but never consults them when
forming the quality verdict.

## Evidence and fail-closed behavior

`quality_evidence.json` records each prompt, repetition, execution order,
prompt-token attestation, generated token IDs and text, first divergence,
positional divergence rate, normalized text similarity, semantic checks,
route counters, logs, timing, and exact runtime environment. It also binds:

- the model, measured binary, checked-in SM89 cubin, wrapper, tuning profile,
  candidate catalog, harness, suite, and referenced fixtures by SHA-256;
- the binary's reported embedded cubin identity to the checked-in SM89 bytes;
- the complete Git tracked diff and untracked-content inventory;
- a single NVIDIA L4 with compute capability 8.9;
- pre-run and post-run source/input identities.

Missing counters, missing token records, invalid UTF-8, changed inputs,
artifact-identity mismatches, prompt drift, nondeterminism, output-contract
failures, wrong hardware, and subprocess failures all fail closed.

## Current numerical-quality limitation

The audited generation CLI and server expose prompt IDs, generated IDs, and
decoded text, but not teacher-forced logits or top-k scores. Therefore this
gate does not invent logit maximum/RMS error, top-k overlap, chosen-token
margin, or KL-like metrics. Before any non-exact candidate can be treated as
production-equivalent, a separate reviewed interface must provide
teacher-forced baseline/candidate logits and add conservative, versioned
top-k, margin, distribution-divergence, and multi-fixture quality/perplexity
thresholds. Until then, every non-exact result remains collect-only regardless
of its decoded-text score.
