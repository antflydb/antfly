# Official vLLM engine comparison

The released Memorilla Python inference path executes successfully on GB10 with
its pinned **vLLM 0.9.1, PyTorch 2.7.0 and Transformers 4.53.3**. The official
encoder, reader and decoder reproduce the exact operational-fact failures seen
in our native and Transformers diagnostics. The natural QA subset has the same
accuracy as the independent Transformers control.

A separate warm latency comparison measures **0.819 s native F32 versus
0.823 s vLLM BF16**. This is effectively parity on the fixed workload. The
observed native median is 3.57 ms lower, about 0.4%, with a descriptive paired
case-bootstrap interval crossing zero. It does not establish general speed
superiority or isolate kernel performance.

## Warm question-to-answer latency

Both paths execute two serial passes over the frozen 24-question diagnostic:
48 timed requests each. Both use the same persisted native document vectors,
native system instruction, Stage3 checkpoint, model pair, greedy decoding and
48-token maximum. Both base models and the reader remain resident. Questions
are embedded on every request. The timed path includes question embedding,
16-token memory readout, prompt framing and answer generation. Document
ingestion, corpus selection, startup and the initial warmup are excluded.

| Path | Combined median | Pass 1 | Pass 2 |
| --- | ---: | ---: | ---: |
| Native managed HTTP API, F32 | **0.819280 s** | 0.819915 s | 0.817923 s |
| Official vLLM 0.9.1 engines, BF16 | **0.822848 s** | 0.822561 s | 0.823267 s |

Native includes JSON serialization, loopback HTTP, validation, admission,
artifact checks and request cleanup. vLLM runs in process, with its upstream
CUDA graph settings, no tensor-capture hooks and disabled prefix/output caching.
The timing runner invokes the official encoder once per question, converts its
output through FP16 storage to BF16 reader inputs, then calls the unchanged
`MemoryVLLM.generate`. It uses the known embedding width directly, omitting the
redundant probe in the upstream bulk `embed_texts` helper. Prepared document
tensors stay on CPU until each request, so their GPU transfer is included.

All 48 vLLM prompt-ID sequences match the previous matched native/F32 reference.
All 48 fresh native answers and token sequences match the qualified native
baseline. vLLM matches native answer tokens on 40/48 requests; the arithmetic
profiles differ. Outputs contain 2–6 tokens including EOS, with no truncation.
For the conditional subset of 40 requests with identical output tokens, medians
are 0.775575 s native and 0.787557 s vLLM. That subset is descriptive rather
than a separate representative benchmark.

The vLLM component medians are 0.187951 s for question encoding, 0.012956 s for
reader plus framing, and 0.621358 s for decoder scheduling/generation. Component
medians do not sum to the end-to-end median. A 5,000-replicate paired bootstrap
resamples questions while retaining both passes and both engines. Its 95%
percentile interval for vLLM minus native median is approximately
**[-38, +63] ms**. This is a fixed-workload variability diagnostic, not a
population claim.

These are different serving and arithmetic profiles. Native uses its qualified
F32 weights, activations and KV, CUDA 13-era libraries and disabled TF32. vLLM
uses BF16 weights/activations/KV, PyTorch's CUDA 12.8 libraries and CUDA graphs.
The native binary and all 83 runtime source files are unchanged from the
[qualified native latency result](2026-11-09-native-python-latency.md).
The GPU is NVIDIA GB10, capability 12.1, driver 580.178.04. Engines execute
separately without competing GPU jobs. No long-generation, concurrent-load,
larger-corpus, newer-vLLM or precision-matched kernel comparison is claimed.

## Official inference quality

The quality run calls the unchanged upstream `MemoryVLLM.from_pretrained` and
`MemoryVLLM.answer` for the operational questions. For TriviaQA it uses upstream
embedding, message construction, task instruction/suffix and `generate`, with
the question embedding separate from the decoder-only suffix. It reuses the
loaded engines across both question sets. This run includes document encoding
and diagnostic captures; its wall times are not used in the latency table.

| Workload | Official vLLM | Matched Transformers control |
| --- | ---: | ---: |
| Exact operational facts | 0/22 | 0/22 |
| Missing facts | 2/2 | 2/2 |
| Bounded TriviaQA subset, normalized exact match | 20/32 | 20/32 |

The control uses the same PyTorch 2.7.0/CUDA 12.8 environment, BF16 policy,
models, questions and instructions. vLLM matches its output tokens on 22/24
operational and 30/32 TriviaQA requests: **52/56 total**. All 56 prompt-ID
sequences match. No output is truncated. These are matching accuracy results
with some differing generated tokens, rather than bitwise engine parity.

Both encoder feature sets are finite and closely aligned, but none of the
1,101 compared BF16 feature rows are bitwise identical. Minimum cosine is
0.999585 for the operational set and 0.999229 for TriviaQA; maximum absolute
differences are 0.003540 and 0.005127 respectively. These are BF16 diagnostics,
not native F32 conformance limits. No numerical gate is relaxed, and these
measurements do not identify a unique cause for individual answer differences.

The original helpful-instruction vLLM quality receipt also compares with the
older native F32 receipt, matching 19/24 answer sequences. That comparison
changes both instruction and precision; it is retained as a diagnostic rather
than treated as a matched native parity test. The latency comparison above
instead aligns system instruction and stored document vectors.

The matched PyTorch 2.7 and earlier PyTorch 2.9 BF16 Transformers controls have
the same accuracy but differ on four operational answer sequences. Holding
PyTorch constant avoids attributing those differences to vLLM.

The TriviaQA selection is the previously frozen first 32 document-bound-eligible
rows from revision `c6b0d7eea65659254115ed31dd26869eb6dc0586`, with a single
reference answer per question. This is not the full published benchmark or a
demonstration that every correct answer required memory rather than decoder
prior knowledge. The exact operational final-192 split remains sealed. No
reader candidate, native BF16 profile or production rollout is promoted.

## Build and provenance

Memorilla source remains pinned to
`d8f18891a50bf33c18f04fe5167c22f51e58c9cd`; the released model checkpoint and
base-model locks are unchanged. vLLM 0.9.1's official published wheels target
x86-64, so this ARM64 host uses an isolated source build following its
[source-build documentation](https://docs.vllm.ai/en/v0.9.1/getting_started/installation/gpu.html).
No vLLM or Memorilla implementation patches are applied.

The official PyPI source archive SHA256 is
`c5ad11603f49a1fad05c88debabb8b839780403ce1b51751ec4da4e8a838082c`.
Of 1,761 original implementation files checked, 1,759 remain identical to that
archive. The standard build installs two FlashAttention Python wrappers from its
declared dependency commit `8798f27777fb57f447070301bf33a9f9c607f491`.
Both wrappers also match the published vLLM 0.9.1 wheel byte-for-byte; the x86
wheel is inspected only, never executed. These are standard dependency build
outputs, with no manual implementation patches. Generated `vllm/_version.py`
metadata is excluded from the source comparison; both runtime and distribution
metadata report 0.9.1. Five compiled
CUDA extension hashes are recorded. Compilation targets SM120 with NVIDIA's
verified CUDA 12.8.1 redistributions; the resulting build runs on GB10/SM121.
Python development headers are extracted locally without system installation.
All 157 installed packages pass dependency checking. Existing reference
environments and native code are preserved.

Evidence is under `.benchmark-results/memorilla/vllm-qualification-20261109/`:

- `official-operational/`, `official-triviaqa/`: actual answers, tokens, engine
  settings, source archives and per-request document/query/memory/prompt tensors.
- `transformers-torch270-bf16-operational/` and
  `transformers-torch270-bf16-triviaqa/`: matched independent controls.
- `native-latency/`, `vllm-latency/`, `latency-comparison.json` and
  `latency-bootstrap.json`: fresh timing and output comparisons.
- `validation.json`: completed quality/tensor/archive/runtime validation,
  SHA256 `83c9d91886e39dd216779c3fed935a57f92f4f3c56da87e9445a6bea9b9c5ebf`.
- `build.sh`, `run.sh`, `qualify.sh`, `run_latency.sh`, environment requirements,
  source integrity and import probes: local reproduction and build provenance.

The new comparison tests pass 5/5, and the new Python sources pass formatting
and diff checks. Native checks reuse the unchanged qualified binary plus fresh
48-request output verification; this session does not rerun or claim a new
full-repository native test suite.
