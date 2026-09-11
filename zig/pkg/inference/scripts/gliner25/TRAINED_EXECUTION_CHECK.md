# Native execution of checked trained artifacts

`check_trained_execution.py` is an additive execution proof for a trained,
native-materialized FP32 GLiNER2.5 model. It consumes a successful
[`check_training_merge.py`](TRAINING_MERGE_CHECK.md) report, independently
rechecks its original/training/adapter/job/output identities, and runs the
separate `antfly-inference-gliner25-trained-check` executable. It neither
modifies the existing bundle/MASSIVE/oracle tools nor enables a public model
qualification gate.

The proof scope is `gliner25_trained_artifact_execution/v1`. Its immutable
[`trained_execution_contract_v1.json`](trained_execution_contract_v1.json)
pins the old helper/checker closure, source request bytes, new input-only
fixture, comparison policy and resource defaults/hard ceilings. The fixture
contains ten complete ordered schemas and original texts, with no gold,
expected outputs or tensor state. It is regenerated in tests using the frozen
`schema_for` function and its exact original requests. The input fixture is
5,965 bytes, SHA-256
`d9414f1d7a2138ed0a9211285bcee5f662667747a31b255ca1708f48a7966abd`.

## Proof boundary

Native CPU results are compared separately with all three pinned Python
forms: unmerged adapter, official PEFT merge, and Python-loaded native-produced
model. Each comparison requires identical actual encoder token IDs, selected
labels/values/order/spans, and the existing absolute `5e-4` confidence bound.
Constrained classification also retains strict solver validity/exactness.
All raw native solver objectives and work counters are saved without treating
floating objectives or search work as discrete decisions.

Every reported source span is checked against the immutable original text.
Native codepoint and UTF-8 byte coordinates must agree, including record
anchors and internal occurrence source spans; emitted value text must match
the source substring after the existing whitespace trim. Synthetic terminal
suffixes cannot be clipped into valid spans by this checker. Source-free enum
values remain distinguishable from source mentions.

Metal requires a complete successful CPU report for the identical executable,
trained artifact, input fixture, source reference and arithmetic policy. The
CPU report's raw event bytes and process completion receipt are revalidated,
and its comparisons are independently rederived. Metal must match the CPU's
public metadata as well as the same selected-output/token/confidence rules.
This includes derived values, entity/field dtypes, attribute multiplicity,
record confidence/anchor, typed relation endpoints and solver status.
Request-local device counters and memory/readback ceilings are checked.
Both paths declare `strict_f32_activations_v1` with FP32 weights, activations,
accumulation and heads. Native does not expose full classification probability
maps here; this proof makes no claim about unselected native probabilities.

The Python three-form check is prior evidence about artifact math and loading.
This new check establishes Zig execution only after its own actual model run
passes. Ten authored examples are not a held-out quality or convergence study.

## Commands

Build once from the repository's `zig/` directory through the complete
ReleaseFast dependency graph. A Metal-capable binary must also produce the CPU
reference, so its exact executable hash can match both runs:

```sh
zig build inference-gliner25-trained-check-build \
  -Dmetal=true -Donnx=false -Dcuda=false -Dpjrt=false -j1
```

The output is
`zig/pkg/inference/zig-out/bin/antfly-inference-gliner25-trained-check` relative
to the repository root. Freeze a copy and record its digest before actual
execution. The build, CPU run and Metal run share the serialized model lane
on the development machine. The following command reproduces the small
all-target LoRA check with a fresh output directory; the completed runs are
recorded below.

```sh
GLINER25_EXECUTION_CHECKER=/Users/timkaye/Documents/af/antfly/zig/pkg/inference/scripts/gliner25/check_trained_execution.py
GLINER25_TRAINED_WORKER=/absolute/path/frozen-antfly-inference-gliner25-trained-check
GLINER25_ADAPTER_RUN=/private/tmp/antfly-gliner25-training-runtime-artifact-v1/lora-all-host128-backend384-uninterrupted
GLINER25_MATERIALIZATION=/private/tmp/antfly-gliner25-materialization-v1

PYTHONDONTWRITEBYTECODE=1 python3 "$GLINER25_EXECUTION_CHECKER" \
  --variant small --backend native \
  --source-dir /private/tmp/antfly-gliner25-models/small \
  --adapter-dir "$GLINER25_ADAPTER_RUN/model" --run-dir "$GLINER25_ADAPTER_RUN" \
  --model-dir "$GLINER25_MATERIALIZATION/lora-all-merged" \
  --merge-job-config "$GLINER25_MATERIALIZATION/lora-all.json" \
  --merge-report "$GLINER25_MATERIALIZATION/lora-all-peft-check/report.json" \
  --merge-report-sha256 b2a51eebd9f43c3ddc790dee9a8cf39f6f5c3d5d90b7e726b61d0c7369bd5127 \
  --binary "$GLINER25_TRAINED_WORKER" \
  --output-dir /private/tmp/gliner25-trained-lora-native-v1
```

For the Metal run, keep the exact binary, model, oracle and schema arguments,
select `--backend metal`, and supply:

```text
--native-reference-report /private/tmp/gliner25-trained-lora-native-v1/report.json
--native-reference-sha256 <the approved complete CPU report SHA-256>
--output-dir /private/tmp/gliner25-trained-lora-metal-v1
```

The corresponding DoRA inputs are
`dora-all-host128-backend512-uninterrupted`, `dora-all-merged`, `dora-all.json`
and `dora-all-peft-check/report.json`. Its prior merge-report SHA-256 is
`a7a628ecc1ce5734b7268c7e3e0cec8049a61b4069b3dd55ecf8a6cd1a39be81`.
Every proof needs a new output directory. The driver records and rechecks its
Python executable/environment, `psutil` version, all helper bytes, selected
binary and optional explicit limits file before publishing success.

## Ownership and resource contract

The driver creates exactly one private six-file model copy: weights, four
original sidecars and the merge receipt. Each input is opened as a bounded
regular descriptor, then streamed and hashed through that same descriptor.
Total copy bytes are capped at 2 GiB with an additional 256 MiB free-disk
headroom check. The worker receives only the private directory and input-only
envelopes. It never reopens a mutable original model pathname. Exact artifacts
and envelopes are checked before and after execution; only the driver-owned
scratch is removed after the worker is reaped.

The fixed geometry is batch one, at most 128 words, 512 encoded tokens and
64 boundary queries. Default owner limits are 128 MiB loader metadata,
128 MiB request scratch, 512 MiB encoder device and 128 MiB head device,
plus 8 MiB proposal and 8 MiB scalar-result readbacks. The combined declared
ceiling is 3 GiB. Admission separately counts the model mapping, fixed 8 MiB
setup owner, loader and request owners. Metal additionally charges a complete
FP32 weight-file allowance for the existing owned host weight cache; device
ceilings include upload staging. Request result serialization is included in
its bounded owner before the reported peak is sampled.

An optional `--limits /absolute/path/limits.json` replaces the complete
resource object. Every field, including version, is required and must satisfy
the fixed hard ceilings. No limit changes inference equations, geometry,
request inventory or comparison tolerances. A lower limit can produce an
explicit incomplete resource-denial receipt. CPU and Metal can have separately
recorded resource envelopes while retaining identical artifact/math inputs.

The driver uses a 6 GiB worker RSS guard, 180-second startup, 120-second
request and 600-second total defaults, bounded by the native hard limits.
It retains exact raw JSON lines: reserializing a value such as `1e-8` can
change byte counts. The protocol is exactly ready, ten ordered results and
complete, with at most 4 MiB per event, 64 MiB total events and 8 MiB stderr.
A complete worker must exit successfully within five seconds and emit no
extra bytes. The worker uses its stdin pipe as a parent lifeline and owns an
independent hard deadline. SIGINT/SIGTERM, timeout, malformed output or worker
failure cause bounded termination/reaping; no automatic retry occurs.
Constructor failures retain subprocess ownership before selector registration;
cleanup must establish child exit before removing its private artifact copy.
Streaming artifact copying checks the driver deadline between blocks. The
reused static artifact audits and hashes have fixed input-size ceilings and
are checked between phases; they are not cooperatively interrupted mid-audit.
The hard deadline claim applies to the supervised worker/runtime phase.

`report.json` requires complete execution. Numerical or metadata mismatches
remain complete diagnostic reports with `parity_pass:false` and nonzero CLI
exit. Incomplete/cancelled/resource-denied checks retain `failure.json`,
received `events.jsonl`, stderr and `process.json` as available. The process
receipt reports cleanup and actual peak worker RSS separately from native
allocator counters. All reports retain `qualification:false`.

## Validation status

Seventeen focused Python tests pass without a model or numerical Python import.
They cover full synthetic artifact audits, approved reference hashes, all
three comparison phases, Unicode/source-byte validation, strict solver and
device counters, native-to-Metal metadata differences, bounded private copies,
failure/cancellation/deadline cleanup, duplicate/missing/extra events and
real lightweight child-process framing/timeout/reaping, selector construction
and registration failures, retrying failed constructor cleanup, and retaining
the private artifact when reaping cannot be established. The native worker
build and the four actual trained-model executions below also passed.

```sh
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover \
  -s zig/pkg/inference/scripts/gliner25 -p 'test_trained_execution.py' -v
PYTHONDONTWRITEBYTECODE=1 python3 zig/pkg/inference/scripts/run_model_contract_tests.py gliner25
```

The complete GLiNER2.5 Python contract checkpoint with the verified official
promtool completed **157 tests, all passed with no skips**. Its log is
`/private/tmp/gliner25-contracts-promtool-trained-v5.log`, SHA-256
`62c744adc5684c2ac2754f26c3fa53ad81811f32831c737b00bef4e52e993160`.
This aggregates several independent contract suites; it is not 157 model runs.

## Actual small all-target trained-artifact execution

Both materialized rank-2/alpha-4, zero-dropout adapters covering all 131
encoder and task-head Linear modules passed the fixed ten requests on CPU
and Metal. All four runs used the same 14,611,000-byte ReleaseFast executable:
`/private/tmp/antfly-gliner25-trained-worker-metal-v2/bin/antfly-inference-gliner25-trained-check`,
SHA-256 `3bd30a2b0487bd58929a62a138e565cc47a53c420815908dcb9ad1cac6002517`.
The driver SHA-256 was
`eacc46fe657f644c13491d14064138c68685830395170c1235a514f0def7f6a1`;
the contract SHA-256 was
`a216ef93b6917063994ed0728150688f0d47b9a778fded95b48b010cb8454182`.

| Adapter/backend | Maximum confidence difference versus any Python form | Maximum versus same-artifact CPU | Observed worker RSS |
|---|---:|---:|---:|
| LoRA CPU | `1.3709068298339844e-6` | — | 418,250,752 B |
| LoRA Metal | `1.9669532775878906e-6` | `1.9073486328125e-6` | 547,094,528 B |
| DoRA CPU | `2.86102294921875e-6` | — | 419,561,472 B |
| DoRA Metal | `2.205371856689453e-6` | `1.9669532775878906e-6` | 548,454,400 B |

All 120 comparisons against the three Python forms and all 20 CPU–Metal
comparisons passed exact token IDs, discrete output and original-source
coordinate checks at the unchanged `5e-4` confidence bound. CPU–Metal checks
also preserve the native metadata described above. Each run completed its
12-event protocol with exit zero and reclaimed its one private six-file copy.
RSS observations establish bounded execution here, not a performance result.

| Report | SHA-256 |
|---|---|
| `/private/tmp/gliner25-trained-lora-native-v1/report.json` | `f236489c57c69697a4c31665193b540f324bc5ba30ccc54959b88581f3221887` |
| `/private/tmp/gliner25-trained-lora-metal-v1/report.json` | `8e5db454bc424447f93398d26f4df285572c284da83023d96ea01a532c202e32` |
| `/private/tmp/gliner25-trained-dora-native-v1/report.json` | `8cc39f1b2786974d7253626098b8a5f42bdf9ea363954681389935eeb060ee35` |
| `/private/tmp/gliner25-trained-dora-metal-v1/report.json` | `72add20e41e1241e8866169f5a7ec85c0154488fa66a3424e7b22949bc11cf94` |

A separate read-only audit rehashed all four raw reports, event streams,
process receipts, binary and helpers; rederived every comparison; checked all
four cleanup flags; and revalidated source, training, adapter, job and merged
identities before and after the audit. It imported no numerical runtime.
The new receipt is
`/private/tmp/antfly-gliner25-materialization-v1/trained-execution-validation-v1.json`,
30,869 bytes, SHA-256
`ea3c2fb430d10d68fd5a0db0e5449ebae76008600698868147508db4f4e826eb`.
The audit script is retained beside it. The earlier `validation.json` with
`native_extraction_executed:false` is preserved as a historical checkpoint.

This is evidence for executing these exact two trained FP32 artifacts. It
does not establish arbitrary training-update parity: a separately identified
inactive-adapter batch must follow the pinned trainer's zero-touch versus
`None` behavior, including optimizer effects, and its native correction/oracle
remains open. The prior training recipe and its receipts are unchanged. Other
variants, ranks, head-only trained artifacts, representative quality,
convergence and release qualification require their own evidence.
