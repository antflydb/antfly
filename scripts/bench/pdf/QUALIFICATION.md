# Layered document qualification

Extend existing gates; do not replace their validators. `qualify.py` executes a
trusted JSON plan and aggregates **only its declared gate scopes**. A green
contract plan is not evidence of model accuracy, native fusion or GPU performance.
Even three represented layers do not prove that the same model/deployment was
tested at every layer. Review the exact scopes and deployment evidence.

## Contract lane

Python 3.11+ and the usual Zig/Go build dependencies are required:

```sh
python3 scripts/bench/pdf/qualify.py \
  --plan scripts/bench/pdf/execution.plan.json \
  --output /tmp/pdf-contract-evidence-001
```

The checked-in plan reuses existing targets for task capabilities, result
cardinality, credentials, cancellation, admission, linked/worker media transport,
distributed proxy routing, PDF rendering and precommit/replay recovery. Targets
run sequentially in Debug with one compiler job each. This bounds runner-level
concurrency; the tests still exercise their own concurrent execution. These
tests intentionally use model-free providers. Their success cannot qualify
serving-time GLiNER2 fusion, Gemma generation, or any other real model.

## Model lanes

Use the family script's exact fixtures, backend selection, precision and thresholds.
The runner checks the script's versioned report schema and boolean verdict, plus
its process exit status. It does **not** recompute numerical acceptance.

| Existing owner | Native evidence binding | Scope caveat |
| --- | --- | --- |
| `qwen3_embedding/qualify_qwen3_embedding_{metal,cuda}.py` | `schema`, `pass` | Oracle, MRL, retrieval, batch parity; now concurrent request isolation too. Endpoint execution is not proof of device-side fusion. |
| `gemma4/benchmark_gemma4_cuda_batching.py` | `antfly.gemma4.cuda_batching.v1`, `passed` | Existing exact-response, scheduler activity, throughput and p95 gates remain unchanged. |
| `qwen3vl/qualify_qwen3vl_reranker_metal.py` | `antfly.qwen3vl.reranker_metal_qualification.v1`, `pass` | Retains multimodal score/preprocessing oracles; not universal reranker batching certification. |
| `gliner2/qualify_gliner2_cuda_hardware.py` | `contract=gliner2_cuda_hardware_qualification_lane/v1`, `pass` | CUDA training/gradient checks do not qualify serving-time extraction/NER fusion. |
| `florence2/verify_florence2_cuda_perf.sh` | Explicit legacy exit-code envelope + full log | Keeps backend/resident-KV/token/timing checks, not an OCR semantic oracle. |
| `clipclap/verify_clipclap_cuda.sh` | Explicit legacy exit-code envelope + full log | Native/CUDA text-vector parity only; image/audio/PDF coverage remains separate. |

Family paths above are relative to `zig/pkg/inference/scripts/`. Existing
precision-specific tolerances are unchanged. Qwen's additional test uses the
existing `batch_gates` cosine threshold against isolated outputs (and the existing
MRL reduction), not a new universal tolerance. It overlaps independently prompted
query/document requests at full/reduced dimensions, reverses arrival order, and
checks exact response-index coverage. Defaults are two rounds, four concurrent
requests; long-context oracles remain in the serial gate. Concurrency is bounded
to at most 16 requests; HTTP timeouts apply to every request. The report explicitly
states that this does not prove native fusion. Family-specific scheduler/backend
evidence is still required for that claim.

Both serial batch and concurrent responses also use the family's existing unit
norm tolerance. Cosine parity alone cannot detect scaled, unnormalized vectors.

Example for an already-running Qwen3 endpoint:

```sh
python3 scripts/bench/pdf/qualify.py \
  --plan scripts/bench/pdf/qwen3-metal.plan.json \
  --output /tmp/qwen-metal-evidence-001 \
  --var binary=/path/to/frozen/antfly --var models=/path/to/frozen/models \
  --var oracle=/path/to/oracle.json --var base_url=http://127.0.0.1:8080 \
  --var model=Qwen/Qwen3-Embedding-0.6B-GGUF --var tier=q8_0
```

Binary/model hashes are **local artifact inventory**, not attestation that a remote
URL served those artifacts or selected that backend. Retain server startup/model
selection logs, remote build/model digests, backend/precision configuration and
topology with the native report before promoting a hardware result. Do not infer
distributed qualification from a loopback run, or CUDA execution from a script's
name. Remote services are user-owned: the runner neither starts nor stops them.

## Document lanes

Prepare the existing Circus corpus and frozen binaries/models as in [README.md](README.md).
`document.plan.json` runs `qualify_documents.py`, which reuses `benchmark.py` and
the existing output signatures. It requires:

- All requested trials to finish indexing, both consumer outputs, and backend
  selection evidence from the existing harness.
- Forced OCR and exactly one physical render per source/page/trial for two
  compatible consumers, including terminal partial windows.
- Each identical consumer must match the primary's text hashes, page geometry,
  artifact counts and published-vector count, not merely match itself across runs.
- Trial byte ranges in the original server log bind events to each indexing run.
  Corpus page ranges and indexed manifest fingerprints define the expected set;
  both physical renders and admission windows must cover it exactly once. Missing,
  overlapping, out-of-range or out-of-trial evidence fails closed. Older benchmark
  results without trial boundaries cannot qualify this gate.
- Both `full_index` and `write` paths at two distinct renderer caps (256/128 MiB
  by default), identical content hashes, page geometry and published-vector counts.
- Successful windows with tracked bytes within the requested cap and actual
  parallelism bounded by requested parallelism. A serial admitted window is valid;
  requested workers alone never prove concurrent rendering.

```sh
python3 scripts/bench/pdf/qualify.py \
  --plan scripts/bench/pdf/document.plan.json \
  --output /tmp/pdf-document-evidence-001 \
  --var binary=/path/to/frozen/antfly --var revision=FULL_BINARY_SOURCE_SHA \
  --var models=/tmp/pdf-assets/models --var work_dir=/tmp/pdf-assets \
  --var circus_dir=/path/to/antfly-circus --var name=document-001
```

This lane intentionally profiles and computes **no speedup**. Tracked render-window
admission is not RSS/GPU peak memory, and low-cap success is not deterministic
allocator-failure injection (covered in the contract lane). Structural parity is
not OCR accuracy. The current benchmark pins Florence/BGE and confirms Metal;
Gemma, ClipClap, extractor and reranker PDF hardware lanes remain unqualified.
The existing `pdf-model-qualification-test` separately probes real remote reader,
generator and embedder contracts, but only checks structural outputs.

For completed-indexing performance, retain the existing alternating-order
`compare.py` and same-binary `render_matrix.py` experiments separately, without
profiling. Their reports now have schemas `antfly.pdf.comparison.v1` and
`antfly.pdf.render_matrix.v1`; the native verdict field is `timing_comparable`.
That verdict establishes comparability, **not that a speedup occurred**. Preserve
first-process and warm-trial timing distributions and failures. Never compare
ratios between different sync levels or memory caps.

`performance.plan.json` runs that existing paired gate. Bind `main_binary`,
`main_revision`, `pr_binary`, `pr_revision`, `models`, `work_dir`, `circus_dir`
and a fresh `name` with `--var`. `compare.py` and `render_matrix.py` accept
`--output` to place native evidence inside the runner's fresh gate directory
while leaving shared corpus/model assets in `--work-dir`.

A valid pressure-driven rematerialization can fail the strict one-render reuse
gate without being a correctness defect. Keep its failure and pressure evidence;
do not remove memory limits or force retention merely to pass the reuse check.

## Plan and evidence contract

Plans are executable configuration: review them like shell scripts. Commands are
argv arrays, not shell strings. `${repo}`, `${run}`, `${python}` and explicit
`--var NAME=VALUE` substitutions do not invoke a shell. Pass credentials through
the existing environment, not command arguments/variables retained in reports.

Each gate declares `id`, `layer` (`model`, `execution`, `document`), `kind`
(`contract`, `hardware`, `performance`), `scope`, `command`, optional `cwd`, and
positive `timeout_seconds`. Hardware/performance gates must name `artifacts`
from the plan's path inventory. Files/directories are hashed before and after
the run. Do not change weights while qualifying; large model trees are deliberately
hashed twice. Build flags/driver/runtime/backend details remain in native evidence.

JSON gates declare `report.path`, `report.schema`, `report.pass_field` and optional
`report.schema_field` (for GLiNER's `contract`). Reports must be newly created
inside `${run}`; stale, missing, unversioned, malformed or non-boolean verdicts
fail closed. Legacy shell/compiled gates must opt into `legacy_exit_code: true`;
the runner retains their entire log and a versioned exit-code envelope, without
fabricating native JSON or additional acceptance criteria.

Every output directory is new. The runner retains the plan, Git revision/dirty
diff and untracked-source fingerprints, host identity, commands, logs, native
report hashes, gate verdicts and artifact inventory. A changed source/artifact
snapshot disqualifies the aggregate. Run from a frozen checkout and put output
outside the checkout. Subprocess timeout/interruption terminates only the owned
process group; failed attempts retain a non-passing summary. Other gates continue
after a failed gate. Native evidence files remain alongside the summary.

`--only execution` can select one layer of a combined plan without requiring
unselected model assets; omitted gates remain `not_run`, so that partial run never
passes the whole plan. A contract-only plan can pass its own scope, but missing
layers remain visible. Combine reviewed gate entries in a plan when they target
the same exact deployment; do not treat three unrelated green lanes as universal
qualification. `limitations` must accompany published results.

## Remaining hardware qualification

This follow-up supplies orchestration and gates, not hardware measurements.
Next run the pinned family artifacts, then qualify true cross-request extraction,
all ClipClap modalities, reranking and chunking with their own result contracts;
add backend-native fusion counters where absent. Run the same model contracts
through independent inference nodes with cancellation/admission recovery and
retain deployment identity. Extend document semantic oracles and accelerator/RSS
measurements before claiming those properties. No placeholder or skipped gate is
evidence of qualification.
