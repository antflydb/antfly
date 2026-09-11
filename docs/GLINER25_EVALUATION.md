# GLiNER2.5 held-out qualification contract

The existing ten-request corpus establishes exact artifact and implementation
parity. It is part of development and must never be counted as held-out model
quality. The [evaluation catalog](../zig/pkg/inference/scripts/gliner25/evaluation_catalog.json)
records primary dataset metadata and its exact revisions. Its entries remain
`metadata_only`; the separately locked CrossNER AI corpus below has now been
downloaded, audited, prepared and evaluated with all three original FP32
checkpoints in Python, native CPU and Metal, plus all nine reduced-storage
profiles on native CPU. The measured results below cover one short English
entity task and do not establish production qualification. The historical
CPU and Metal references use different executables; fresh comparisons with
the same executable are tracked separately below.

The executable [evaluation contract](../zig/pkg/inference/scripts/gliner25/evaluation_contract.py)
provides offline file admission, split/leakage checks, requests that omit gold,
and exact-fact metrics. It runs no model and always reports `qualification:
false`; the separate execution driver runs the models. The official scorer
check below passes for the declared exact-span metric. Grouped statistical
intervals, calibration, additional task corpora and release policies remain
required.

## Prepared CrossNER AI holdout

The [corpus manifest](../zig/pkg/inference/scripts/gliner25/crossner_ai_manifest.json)
pins six files (310,171 bytes) from author revision
`2e7ba2a7798c961e3f29fbc51252c5a8d40224bf`, including the original BIO splits,
literal ontology declaration, README and MIT repository license notice. Both
Git blob IDs and SHA-256 hashes were verified. Raw corpus bytes remain in the
local cache, rather than this repository.

The [deterministic adapter](../zig/pkg/inference/scripts/gliner25/prepare_crossner_ai.py)
joins each original BIO token sequence with a single ASCII space and converts
strict BIO labels into exact half-open UTF-8 byte spans. This is an explicit
reconstruction profile; it does not claim to recover original article spacing.
Malformed or orphan continuation labels fail admission. The fixed schema uses
all fourteen author-declared types in declaration order, including `misc`, with
their original spellings and no added descriptions or examples. Every request
includes absent types. Threshold `0.5`, overlap `flat` and strict decoding are
frozen before model execution.

The first audit rejected three exact-text families shared by development and
test. The declared policy retains the entire official test split and removes
the lower-priority development rows `117`, `143` and `262` (zero-based source
indices). It uses normalized text and original split only; gold annotations
do not influence exclusions. Two shared families have annotation disagreements.
The identical test rows `414` and `418` remain two official examples; their
shared family ID must remain one resampling group. `split_audit.json` records
all member IDs, text/gold hashes, differences and exclusions. The resulting
calibration split is derived, not the unchanged official development split.

| Split | Original examples | Retained examples | Retained entities |
| --- | ---: | ---: | ---: |
| Train | 100 | 100 | 532 |
| Calibration | 350 | 347 | 1,541 |
| Test | 431 | 431 | 1,809 |

Test examples contain at most 84 original BIO tokens and 492 UTF-8 bytes, so
this corpus does not qualify long-document behavior. Article IDs are absent;
near-duplicate article membership and pretraining/teacher exposure remain
unknown. The split audit makes no contamination-free claim.

Local preparation is available at
`/private/tmp/antfly-gliner25-eval-data/crossner-ai-locked-v2` and reproduced
byte-for-byte in a second directory. Its lock SHA-256 is
`a3ea7f513501af42db0030eb6557dd845dbcdf39483c7e8f23a2f7e636e4f144`.
The separate [preparation evidence](../zig/pkg/inference/scripts/gliner25/crossner_ai_preparation.json)
binds the adapter, harness, corpus manifest, lock, requests, gold and split audit.
The prior `crossner-ai-locked-v1` directory is an intentionally failed audit
attempt with no complete prepared receipt; it must not be used for evaluation.

To reproduce preparation after fetching the immutable files named in the
manifest from `raw.githubusercontent.com/zliucr/CrossNER/<revision>/<path>`:

```sh
PYTHONDONTWRITEBYTECODE=1 /private/tmp/antfly-gliner25-oracle-venv/bin/python zig/pkg/inference/scripts/gliner25/prepare_crossner_ai.py --corpus-dir /private/tmp/antfly-gliner25-eval-data/crossner-ai-2e7ba2a7798c961e3f29fbc51252c5a8d40224bf --output-dir /private/tmp/antfly-gliner25-eval-data/crossner-ai-new-lock
```

`prediction_facts(request, output, backend)` adapts actual Python or native/Metal
entity output without reading gold. Python codepoint coordinates become UTF-8
bytes; native output must already declare byte coordinates. Full query
coverage, exact source text, finite confidence and duplicate-free occurrences
are required. Metrics include overall and per-type exact occurrence F1 plus
per-type absent-query false-positive rates. The independent official BIO audit
below verifies this named exact-span metric through a lossless per-type
boundary representation. Five focused adapter tests cover Unicode, strict BIO
admission, text-only split decisions, test multiplicity, absent queries, source
corruption and audit-file tampering.

The bounded [execution driver](../zig/pkg/inference/scripts/gliner25/evaluate.py)
is ready for serial model runs. Its separate `--evaluation-fixture` native
worker mode uses the existing bundle-check executable while preserving the
ten-case diagnostic mode. A worker sees no gold; the driver separately proves
that both prepared files reproduce the locked test rows. Original FP32 and
converted files are rehashed before loading and after execution. Source and
bundle receipts, helper/binary hashes, every request identity, and the actual
encoder token IDs are retained in the output evidence.

This first execution profile admits at most 1,024 examples, 128 processor
words, 512 encoded tokens, 64 queries and 1 MiB of text per request. The Python
processor runs with `max_len=None`; explicit preflight rejects excess words or
tokens before encoder execution. The native processor applies matching hard
caps. A typed error still occupies its original denominator position. Missing
results, timeouts, token drift or any failed request prevent a completed metric
report; no failing example is silently omitted. These bounds deliberately
define a short-text evaluation profile, not a long-document gate.

For example, after the parent build lane produces
`zig/zig-out/bin/antfly-inference-gliner25-bundle-check`, run one worker at a
time from the repository root:

```sh
GLINER25_EVAL_DIR=/private/tmp/antfly-gliner25-eval-data/crossner-ai-locked-v2
GLINER25_PYTHON=/private/tmp/antfly-gliner25-oracle-venv/bin/python
PYTHONDONTWRITEBYTECODE=1 "$GLINER25_PYTHON" zig/pkg/inference/scripts/gliner25/evaluate.py run --lock "$GLINER25_EVAL_DIR/lock.json" --prepared-dir "$GLINER25_EVAL_DIR/prepared" --model small --model-dir /private/tmp/antfly-gliner25-models/small --backend python --output-dir /private/tmp/gliner25-crossner-python-small
PYTHONDONTWRITEBYTECODE=1 "$GLINER25_PYTHON" zig/pkg/inference/scripts/gliner25/evaluate.py run --lock "$GLINER25_EVAL_DIR/lock.json" --prepared-dir "$GLINER25_EVAL_DIR/prepared" --model small --model-dir /private/tmp/antfly-gliner25-models/small --backend native --binary zig/zig-out/bin/antfly-inference-gliner25-bundle-check --source-reference-report /private/tmp/gliner25-crossner-python-small/report.json --output-dir /private/tmp/gliner25-crossner-native-small
```

Repeat with the matching `base` or `multi` artifact and its own Python report.
For a converted artifact, point `--model-dir` to its immutable bundle. A Metal
run also requires `--native-reference-report` from that identical source
artifact or converted bundle. A single disposable worker defaults to one CPU
thread, a 6 GiB RSS ceiling, 180-second startup and 125-second response timeout;
the native request itself has a 120-second cooperative deadline and a Metal
hard-cancellation watchdog. The runner records no performance claim.

`responses.jsonl` preserves every raw result/error and exact token sequence.
`predictions.jsonl` contains gold-independent exact facts, and a fully successful
run produces `metrics.json` plus `report.json`. The report includes typed entity
micro F1, a macro average over the fixed fourteen-type ontology, per-type
support/negative-query denominators, and signed per-metric quality loss relative
to original Python FP32. No absolute quality floor is inferred from these
results. The agreed relative degradation limits are assessed separately below.
Original/converted FP32 parity and same-artifact CPU–Metal parity require exact
ordered decisions and unchanged `5e-4` confidence tolerance. Reduced precision
versus source quality is reported separately. Changed confidence can fail
runtime parity even when quality F1 is unchanged. Six synthetic driver tests
exercise these contracts through actual disposable stub subprocesses; no model
execution is implied by their passing result.

## Measured CrossNER AI source FP32 results

All three Python and native CPU runs completed all **431 official test
examples**, with **1,809 gold entities** and zero errors or omitted requests.
The full fourteen-label schema, threshold `0.5`, flat overlap and strict
decoding were fixed before execution. No descriptions or examples were added
to the author's literal type names. The results measure exact typed entity
occurrences in the reconstructed text described above.

| Model | True positives | False positives | False negatives | Precision | Recall | Micro F1 | Macro F1, all 14 types |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Small | 692 | 928 | 1,117 | 42.72% | 38.25% | 40.36% | 41.25% |
| Base | 1,035 | 1,161 | 774 | 47.13% | 57.21% | 51.69% | 55.05% |
| Multi | 837 | 1,008 | 972 | 45.37% | 46.27% | 45.81% | 49.32% |

These are the measured model-quality results for this fixed schema. Native
CPU reproduced Python's exact token sequences and ordered entity decisions
on **1,293 of 1,293 model/example pairs**. Every metric family was identical
between the corresponding backends. Confidence comparison retained the
existing absolute tolerance of `5e-4`:

| Model | Native/Python cases passed | Encoded tokens, minimum–maximum | Maximum confidence difference |
| --- | ---: | ---: | ---: |
| Small | 431/431 | 41–135 | `3.4570694e-6` |
| Base | 431/431 | 41–135 | `6.0796738e-6` |
| Multi | 431/431 | 52–187 | `1.6868114e-5` |

The [measured evidence ledger](../zig/pkg/inference/scripts/gliner25/crossner_ai_execution.json)
records each source model revision, weight and tokenizer hash, six raw report
paths and hashes for original FP32, response/prediction/metric file pins, the exact helper hashes,
and all per-type results. Independent read-only review reproduced the locked
requests and separate gold, reconstructed facts from every saved raw output,
rescored them, and recomputed every stored CPU/Python comparison. All native
runs used binary SHA-256
`0069cdad6b8d3b1a97f00779c3de73d81125766ad832f5fc3cd9d9d076b77334`
and execution-driver SHA-256
`28ba8c41a29b36bdbee8e708e96d73d91f0b6ed91384a19e7d5cad2919e50718`.
Large raw reports remain in the pinned local capture directories.

Exact whole-document entity predictions matched gold on 21, 23 and 20 of 431
examples for small, base and multi respectively. Across the 5,133 document/type
queries with no gold entity of that type, 577, 604 and 497 produced a false
positive (11.24%, 11.77% and 9.68%). The overall zero-entity document count is
only one; its separate 100% false-positive rate is not a substitute for these
per-type denominators. All three models recovered zero of the 181 `misc`
entities. The fixed-ontology quality result includes this label and its errors;
the test findings do not justify changing the locked schema or threshold.

This historical receipt establishes CPU implementation parity for a short
English entity corpus. It does not qualify multilingual text, long documents,
attributes, classifications or their constraints, relations, records, enums,
JointIE or training. No held-out absolute quality floor, statistical ranking
or performance claim is inferred. Reduced precision versus source quality
and the separate original-FP32 Metal extension below retain these same
requests and denominators.

All nine converted native CPU runs also completed all 431
examples without errors, with exact source token sequences and unchanged
request settings. Weights in the named encoder storage profile use float32
activations and accumulation; task heads remain float32. The ledger separately
pins each bundle receipt and recomputes its saved raw-output comparisons and
quality deltas against the original Python FP32 report.

| Model | Encoder storage | Micro F1 | F1 change from FP32, points | Ordered decisions equal to FP32 | Decisions and confidence within `5e-4` |
| --- | --- | ---: | ---: | ---: | ---: |
| Small | FP16 | 40.3381% | −0.0235 | 429/431 | 334/431 |
| Small | Q8_0 | 40.2915% | −0.0701 | 395/431 | 38/431 |
| Small | Q4_0 | 38.2576% | −2.1040 | 120/431 | 21/431 |
| Base | FP16 | 51.6854% | 0 | 428/431 | 357/431 |
| Base | Q8_0 | 51.8334% | +0.1480 | 407/431 | 30/431 |
| Base | Q4_K | 52.0000% | +0.3146 | 185/431 | 0/431 |
| Multi | FP16 | 45.8128% | 0 | 429/431 | 346/431 |
| Multi | Q8_0 | 45.9393% | +0.1265 | 401/431 | 24/431 |
| Multi | Q4_K | 45.3225% | −0.4903 | 158/431 | 4/431 |

Small's per-type F1 changes in percentage points are:

| Type | FP16 | Q8_0 | Q4_0 |
| --- | ---: | ---: | ---: |
| field | 0 | −0.8593 | −1.2167 |
| task | 0 | −0.0500 | +0.0169 |
| product | −0.1792 | −0.2680 | −3.5608 |
| algorithm | 0 | +0.0963 | −2.4457 |
| researcher | 0 | 0 | −3.4655 |
| metrics | 0 | +0.2614 | −0.4422 |
| programlang | 0 | −0.7463 | −3.6000 |
| conference | 0 | +0.1587 | +1.6185 |
| university | 0 | −1.0734 | +2.2599 |
| country | 0 | 0 | +1.3903 |
| person | 0 | +0.2720 | −1.4307 |
| organisation | 0 | +0.3676 | −6.5315 |
| location | 0 | 0 | −5.8458 |
| misc | 0 | 0 | 0 |

The [agreed degradation gates](GLINER25_IMPLEMENTATION.md) allow at most 0.5 F1
percentage points of loss for FP16/Q8 and one point for Q4, by task and declared
slice. **Small Q4_0 fails** the overall gate (−2.1040 points) and eight of the
fourteen type slices. **Small Q8_0 fails** three type slices: field (−0.8593,
207 gold), programlang (−0.7463, 60 gold), and university (−1.0734, 28 gold).
Small FP16 stays within the limit for the overall task and every declared type
on this corpus. These relative gates are distinct from an absolute F1 floor,
which has not been defined. Qualification remains false.

Base and multi's corresponding per-type changes are:

| Type | Base FP16 | Base Q8_0 | Base Q4_K | Multi FP16 | Multi Q8_0 | Multi Q4_K |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| field | 0 | +0.0505 | −1.3741 | 0 | −0.2606 | −0.4261 |
| task | −0.0734 | +0.2561 | +0.5504 | +0.0856 | +0.3046 | +0.9309 |
| product | 0 | +0.3091 | +2.4095 | 0 | +0.2225 | +0.3597 |
| algorithm | 0 | +0.0960 | −1.8785 | 0 | 0 | −2.5826 |
| researcher | 0 | 0 | +2.9762 | 0 | +0.7441 | −1.6769 |
| metrics | 0 | −0.2635 | +1.4537 | 0 | +0.7206 | +1.5182 |
| programlang | 0 | +0.4629 | −1.5552 | 0 | 0 | −4.7619 |
| conference | 0 | 0 | −1.2048 | −0.3224 | −0.3224 | −8.4269 |
| university | 0 | 0 | −3.4632 | 0 | 0 | +1.0766 |
| country | 0 | 0 | −2.0408 | 0 | 0 | +3.4296 |
| person | +0.2413 | 0 | −3.3669 | 0 | −0.3197 | +2.3383 |
| organisation | 0 | +0.6601 | +2.8632 | 0 | 0 | −3.1127 |
| location | 0 | 0 | +0.7905 | 0 | 0 | +1.8170 |
| misc | 0 | 0 | 0 | 0 | 0 | 0 |

All three FP16 profiles, base Q8_0 and multi Q8_0 meet the relative overall and
type-slice limits on this corpus. **Base Q4_K fails seven type slices** despite
its improved overall F1. **Multi Q4_K fails five type slices** despite staying
within the overall one-point limit. Thus small Q8_0 and all three Q4 profiles
fail this held-out relative-quality gate. No absolute quality floor is inferred,
and these corpus-specific results do not qualify any production profile.

The separate [read-only evidence auditor](../zig/pkg/inference/scripts/gliner25/audit_crossner_ai_execution.py)
reproduced all twelve native reports against their three pinned Python source
reports: **5,172 native results**, zero errors or omitted requests, exact token
sequences throughout, and every stored comparison and metric delta verified.
The ledger records all fifteen raw report hashes and each bundle receipt.
The audit runs no model and leaves source reports and execution helpers intact:

```sh
PYTHONDONTWRITEBYTECODE=1 python3 zig/pkg/inference/scripts/gliner25/audit_crossner_ai_execution.py --output /private/tmp/gliner25-crossner-reviewed.json
```

Matching aggregate quality does not establish exact output parity. No threshold,
schema, or model tuning is performed on this locked test. This historical CPU
ledger remains unchanged; the original-FP32 Metal extension uses separate
receipts below. Reduced-precision Metal held-out comparisons remain open.
Qualification remains false.

### Original FP32 Metal extension

The small, base and multi Metal runs each completed all 431 locked requests
with zero errors. Their actual token sequences and ordered decisions match
both Python and the corresponding historical native CPU outputs: **1,293
Metal model/example pairs and 2,586 reference comparisons**. Every one of the
15 overall/type metric families is identical across the three backends for
each model. The quality values therefore remain those in the FP32 table above.
Confidence comparison keeps the original absolute tolerance of `5e-4`.

| Model | Cases matching each reference | Maximum Metal/Python confidence difference | Maximum Metal/historical CPU difference | Peak worker RSS |
| --- | ---: | ---: | ---: | ---: |
| Small | 431/431 | `3.6358833313e-6` | `3.8743019104e-6` | 555,106,304 B |
| Base | 431/431 | `9.1195106506e-6` | `7.5697898865e-6` | 1,823,080,448 B |
| Multi | 431/431 | `1.6510486603e-5` | `1.5139579773e-5` | 2,496,200,704 B |

All three runs recorded the 6 GiB worker RSS ceiling and the unchanged
128-word, 512-encoded-token, 64-query request policy. The request deadline is
120 seconds; the frozen driver defaults are 180 seconds for startup and 125
seconds per response. Those latter defaults are source-defined rather than
serialized in the historical reports. These are resource observations, not
latency or throughput qualification.

Reports are under `/private/tmp/gliner25-crossner-metal-{small,base,multi}-fp32-v1/report.json`:

| Model | Report bytes | Report SHA-256 |
| --- | ---: | --- |
| Small | 414,855 | `aff3ecf1a97aa4d72595f115d3246a3d6c984230b7b38b60f3034c7a3ad8a8d6` |
| Base | 417,118 | `6623f3591370c4191079ca3213bf368baee61d85804e6df0382b6e1832c81ef4` |
| Multi | 417,493 | `645d2aa1de07e037dd06851f2527b196b232b34627dad633828d8919f2d10000` |

The independent read-only replay is
`/private/tmp/gliner25-crossner-metal-all-independent-audit-v3.json`
(25,044 bytes, SHA-256
`b99c6d62525434d1c2f7ccf8509a0559a294bdd76dcad2427f6dab4502788fd3`).
It reconstructs every saved output comparison and metric, verifies all raw
evidence and helper pins, rehashes the fifteen current source files and the
Metal executable, and removes its private scoring snapshots. The earlier
two-model audit remains unchanged.

The original CPU references use executable
`0069cdad6b8d3b1a97f00779c3de73d81125766ad832f5fc3cd9d9d076b77334`;
the Metal reports use
`a43fcdee29931bcb67972ff002f3c1457e25f547cda24a60759cfed4a0f6880d`.
Both declare ReleaseFast and strict FP32 math. These receipts establish
numerical parity for identical artifacts across those historical builds;
they do not establish identical-executable CPU/Metal parity by themselves.

Separate fresh small, base and multi CPU runs with `a43fcdee...` now pass a
retrospective comparison against their saved Metal outputs: **1,293/1,293 exact
token sequences and ordered decisions**, all 15 metric families identical,
and maximum confidence differences `3.8743019104e-6`, `7.5697898865e-6` and
`1.5139579773e-5`, respectively. The comparison rehashes the exact executable,
all three artifacts, all request
and output evidence, and the frozen helpers. It runs no model and leaves
the original Metal reference hashes and historical CPU ledger unchanged.

The fresh CPU reports are
`/private/tmp/gliner25-crossner-native-{small,base,multi}-fp32-a43-v1/report.json`,
with SHA-256 values
`e4ada68e4b13c05719a6a675fbae088e08a9108c293f8d9af22f4bbfcb0ce044`,
`4d74d1cade977ef3e55f9eb4704a3509be71ade2cc4ae6d58fcd3b7866f02d07`
and `2c71554d0a3efbe6a3a7616ada89c29ca6db0d6d0c07f5d3715cc5ace2b96da0`.
The all-three comparison receipt is
`/private/tmp/gliner25-crossner-same-executable-all-audit-v2.json`
(761,528 bytes, SHA-256
`13fafb8c9436d409b6daf12b8542969a7d5244fdea17f045012b7b935db45a0a`).
Its small/base results preserve the prior comparison receipt,
`/private/tmp/gliner25-crossner-same-executable-small-base-audit-v1.json`
(508,124 bytes, SHA-256
`b349817bd8a6136239f67778577cf80e7dc934e628e8e57acc4351a9d64a75c1`).
No performance conclusion follows from these correctness captures.

The additive [FP32 CPU/Metal evidence ledger](../zig/pkg/inference/testdata/gliner25/crossner_fp32_metal_execution_v1.json)
retains all five source-file pins for each model, Python/historical CPU/fresh
CPU/Metal report and raw-evidence digests, the shared executable, helper and
audit identities, all comparison counts and confidence maxima, and metric
equality against the historical full-ontology results. It remains outside the
upstream numerical fixture manifest and preserves the historical execution
ledger unchanged. The new [capture and validation tool](../zig/pkg/inference/scripts/gliner25/capture_crossner_metal_evidence.py)
replays the bounded saved outputs before producing a new ledger, using private
snapshots for the existing scorer and streaming source-file checks. Eight
focused regressions pass, rejecting substituted models/builds, rewritten
historical references, incomplete coverage, relaxed tolerances and unsupported
release claims. Metric digests use canonical key order.

Repository-only validation needs no model or temporary capture files:

```sh
python3 -B zig/pkg/inference/scripts/gliner25/capture_crossner_metal_evidence.py --validate zig/pkg/inference/testdata/gliner25/crossner_fp32_metal_execution_v1.json
```

The compact ledger preserves provenance and summaries. Full raw-output replay
still needs the pinned capture files, artifacts and prepared corpus; restore
those files or run a separately documented model campaign if they have been
removed. Capturing with `--output` requires a new path and never replaces an
existing ledger. This evidence establishes short English entity parity only;
it adds no absolute quality floor or release qualification.

### Independent official BIO metric check

The additive [official scorer audit](../zig/pkg/inference/scripts/gliner25/audit_crossner_bio_scoring.py)
now verifies the declared exact typed-span counts against CrossNER's
[unmodified CoNLL scorer](https://github.com/zliucr/CrossNER/blob/2e7ba2a7798c961e3f29fbc51252c5a8d40224bf/src/conll2002_metrics.py).
Its exact source and MIT notice are retained in the
[test reference](../zig/pkg/inference/testdata/gliner25/crossner_metric_reference/README.md).

Ordinary flat extraction permits overlap between different entity types. The
audit therefore creates one BIO track per type, using the union of gold and
predicted UTF-8 boundaries within each document. It preserves every cross-type
overlap and partial-word mismatch, and never trims, snaps or drops predictions.
The official scorer's TP/FP/FN and precision/recall/F1 must match all 15 declared
metric families. Explicit type/document sentinels prevent cross-row matches.
This proves exact typed-span metric equivalence, not original BIO-token
representability or the original trainer's flattened batching and tag-column
order.

Seven model-free tests pass, including exhaustive flat-span inventories,
cross-type overlap, Unicode boundaries, empty documents, adjacent same-type
entities, invalid spans, report substitution and non-regular input rejection.
The reader admits bounded regular files without blocking on FIFOs, hashes the
exact bytes consumed, and rechecks all inputs. The existing metric implementation
receives only bounded snapshots in a private temporary directory. Report scope,
lock identity, completion and embedded metrics are checked against their
prepared and metric receipts.

The current audit passed all **15 reports / 6,465 scored document instances /
225 metric families**: the twelve historical CPU profiles and all three
original-FP32 Metal runs. Its receipt is
`/private/tmp/gliner25-crossner-official-bio-audit-v4.json` (66,728 bytes,
SHA-256 `67959d10aec3007ba3e3c915419d08a49d67d25da068bea6958a18ad83501b1e`).
The unchanged audited script has SHA-256
`1597e430e846acaa5f45ec7b355bc1f309e350a75c5ecf7f135f0f3b47327b7f`.
All fourteen results in the prior v3 receipt remain identical; that historical
receipt is retained. This inventory excludes the fresh same-executable CPU
captures and stays below the auditor's sixteen-report ceiling.
The initial global-overlap rejection and two review findings are retained in
earlier logs; the current audit preserves all cross-type predictions and rejects
incoherent receipts and FIFO input. No model reruns, quality thresholds or
locked-test changes are part of this metric check.

```sh
PYTHONDONTWRITEBYTECODE=1 python3 zig/pkg/inference/scripts/gliner25/audit_crossner_bio_scoring.py --prepared-dir /private/tmp/antfly-gliner25-eval-data/crossner-ai-locked-v2/prepared --report /private/tmp/gliner25-crossner-native-small-fp32/report.json --output /private/tmp/gliner25-crossner-bio-new-audit.json
```

## Pinned upstream evaluation is incomplete

At commit `3c913c7369301133d3b7699252074c4303ada50e`, the trainer's
[evaluate method](https://github.com/fastino-ai/GLiNER2/blob/3c913c7369301133d3b7699252074c4303ada50e/gliner2/training/trainer.py#L1907)
aggregates losses and proposal diagnostics. Task metrics require the optional
`compute_metrics` callback. The separate
[boundary metrics](https://github.com/fastino-ai/GLiNER2/blob/3c913c7369301133d3b7699252074c4303ada50e/gliner2/training/metrics.py)
measure proposal coverage and exact query spans; they are not a complete
attribute, classification, relation, record or JointIE scorer. Empty support
has F1 zero, and exact spans use sets, which cannot measure repeated record
multiplicity. The pinned multitask test imports `benchmarks.multitask`, but
that package is absent from the pinned Git tree. Its test names do not establish
a usable held-out benchmark.

The upstream [InputExample and split helper](https://github.com/fastino-ai/GLiNER2/blob/3c913c7369301133d3b7699252074c4303ada50e/gliner2/training/data.py#L673)
use string mentions and random example-level splitting. A separate canonical
evaluation layer must retain exact occurrences, complete ontologies, document
families and original split identities. Do not use sanitization that silently
drops an invalid gold item, or construct an inference schema from types found
in the current document's annotations.

## Public corpus candidates and their limits

| Corpus | Useful coverage | Required interpretation |
| --- | --- | --- |
| [CrossNER](https://github.com/zliucr/CrossNER/tree/2e7ba2a7798c961e3f29fbc51252c5a8d40224bf) | Five English target domains and negative entity queries | Preserve the full domain label inventory, including miscellaneous. Token reconstruction is a named text profile; group overlapping CrossRE documents. |
| [MultiCoNER 2](https://registry.opendata.aws/multiconer/) | Twelve languages, fine-grained entities and noisy text | Report language, clean/noisy and type slices. The primary registry declares CC BY 4.0; corpus objects still need byte/version pins. |
| [SemEval-2014 ABSA](https://alt.qcri.org/semeval2014/task4/index.php) | Occurrence-specific aspect spans and sentiment attributes | Keep positive, negative, neutral and conflict. Evaluate predicted spans with their attributes; gold-span sentiment accuracy is a separate diagnostic. The organizer's [distribution terms](https://alt.qcri.org/semeval2014/task4/index.php?id=data-and-tools) need preservation. |
| [MASSIVE 1.1](https://github.com/alexa/massive/tree/f966f21846043aabef9b0f974fa7970027f43738) | Fifty-two locales, sixty intents, fifty-five slot types and genuine intent/scenario constraints | A translation family shares one split. Freeze the intent→scenario mapping before test; predict both tasks. Keep `utt` immutable and validate `annot_utt` span reconstruction. Data [NOTICE](https://github.com/alexa/massive/blob/f966f21846043aabef9b0f974fa7970027f43738/NOTICE.md) distinguishes CC BY 4.0 data from repository code. |
| [GoEmotions](https://github.com/google-research/google-research/tree/08a8d6736475776f42ffac23b2c13111a28e5795/goemotions) | Multilabel classification, empty predictions and taxonomy-derived constraints | Use the rater-agreement splits and all twenty-eight labels. Any higher-level taxonomy task is a declared derived evaluation. Group comments, threads and duplicate text. |
| [CrossRE / Multi-CrossRE](https://github.com/mainlp/CrossRE/tree/a58885fa760559d2dc9e176e730fe731f020d6f3) | Typed directed relations, end-to-end JointIE and translated domains | Gold-entity relation classification differs from predicted-entity extraction. Keep translations/back-translations together and distinguish machine-translated from human-checked slices. The repository's GPL-3.0 license is recorded separately from data rights. |
| [Re-DocRED](https://github.com/tonytan48/Re-DocRED/tree/ccfb54f5ddf5836027c87badda10f6dfc56efaac) | Revised document-level relation annotations and inter-sentence evidence | Map predicted mentions to gold clusters only inside scoring. Cluster relations do not uniquely identify one gold mention pair. The revised corpus improves original DocRED's incomplete annotations; use its own immutable split files. |
| [WikiEvents](https://github.com/raspberryice/gen-arg/tree/253e0889b2377e0f7084cb406cf5d4142ee8a365) | Natural records anchored at explicit event triggers | End-to-end trigger/record extraction differs from argument extraction given gold triggers. Keep full-span and head/coreference metrics distinct. Its S3 corpus URLs need content pins. |
| [Doc2EDAG / ChFinAnn](https://github.com/shun-zheng/Doc2EDAG/tree/da4f4bc0b9a896170d7f220baf3fe85cb535a025) | Chinese trigger-free event tables, latent/anchorless modes and repeated records | Distant supervision and semantic tables do not establish exact occurrence gold. Keep record multiplicity, evaluate each mode separately, and do not invent a natural anchor. |
| [DuEE-Fin](https://github.com/PaddlePaddle/PaddleNLP/blob/3f87dac9b719f75399f92f8bf634ae2ef0611832/slm/examples/information_extraction/DuEE/README.md) | Chinese document events and a source-free enumerated role | The primary baseline explicitly includes enum classification alongside triggers and arguments. Verify access to labeled holdout data. A new grouped split must be called derived; removing triggers is a separate latent/anchorless ablation. |

README/Git revisions pin the source of this design, **not** externally hosted
corpus bytes. The catalog stores metadata-file SHA-256 values. Acquisition must
add immutable per-file pins, dataset notices, deterministic adapter code and
an explicit record of exclusions before a lock can be executed. Do not import
baseline code or execute a remote dataset loading script just to read records.

## Freeze the experiment before evaluating test labels

Maintain an external schema file for each domain/use case. It contains every
declared type, field, relation, attribute, enum and classification label, with
stable ordering, descriptions, instructions and examples. Gold labels may be
absent from a document; the corresponding negative query must still execute.
Document-specific schema selection must follow an input-side use-case rule,
never a label-conditioned rule.

Pin original dataset bytes, normalized JSONL, adapter and official scorer code,
schema/description/example files, split membership and sampling IDs. Thresholds,
temperatures, prompt language, relation caps, overlap policy, record identity,
solver budgets and long-window policy are calibrated on training/development
data and frozen before final test. Hard rules must follow the ontology or an
independent business contract. A relation appearing functional or acyclic in
one gold document does not justify imposing that restriction globally.

Preserve original public train/dev/test splits. Audit source-document IDs,
translations, paraphrases, duplicate normalized text, linked announcements,
threads and any benchmark-development examples across every participating
corpus. The helper rejects cross-split family and NFC/casefold/whitespace
duplicates. Normalization affects this audit only; model input bytes stay
unchanged. Near-duplicate and provenance review remain adapter-level work.
Record pretraining/teacher contamination as **unknown** unless independently
demonstrated: a local split audit cannot prove that a model's synthetic-data
teacher never encountered a public benchmark.

Use full labeled test coverage where practical. If a resource cap requires a
subset, freeze IDs without test-label stratification, keep a denominator ledger
and call it a subset result. The helper admits at most 100,000 normalized records
per lock, 512 MiB per pinned file, 2 MiB per JSONL record/schema and 4,096 exact
facts per document. Larger campaigns need declared shards and one global
family/duplicate audit; shard boundaries are not new train/test boundaries.
Do not omit timeouts, resource errors, invalid spans or infeasible output from
the denominator. Runtime failures block a completed qualification run.

## Local harness contract

A `gliner25_evaluation_lock/v1` JSON file has `status: "locked"`, the pinned
upstream commit and Unicode version, the executable `harness_sha256`, `qualification: false`,
`schema_selection: "fixed_before_test"`, `test_used_for_tuning: false`, fixed
`request_options`, and `offset_unit: "utf8_bytes"`. It lists:

- `source_files`: actual source-corpus file pins.
- `supporting_files`: optional pinned audit ledgers and corpus manifests.
- `adapter_file` and `metric_contract_file`: file pins, accompanied by matching
  `adapter_sha256` and `metric_contract_sha256`.
- `schemas`: IDs, origins (`public_ontology`, `training_only`, or
  `external_business_contract`) and file pins.
- `splits`: `train`, `calibration`, or `test`, each with exact record count and
  a normalized JSONL file pin.
- `metrics`: named exact-fact families with an explicit definition and either
  `set` or `multiset` counting. Record instances require `multiset`.

Every file pin is `{path, size_bytes, sha256}` relative to the lock directory;
paths cannot escape it. Each normalized row contains only
`{id, family_id, language, schema_id, text, gold}`. `gold` maps every declared
metric family to an array of facts, including empty arrays for negative cases.
Exact facts contain no probabilities. A `span` or `anchor` uses half-open UTF-8
byte coordinates; optional `text` must match those exact original bytes.

For example, an entity fact is
`{"type":"organization","span":{"start":8,"end":13,"text":"Acme!"}}`.
An attribute fact adds its group/label to an entity occurrence. A relation fact
contains type and ordered typed endpoint spans. A record fact contains its
structure, optional natural anchor, and canonical field/value/occurrence
content. The adapter must freeze field-value ordering and whether a corpus
supports semantic identity or exact occurrences; JSON object-key order is not
part of exact-fact identity. Repeated identical whole records remain repeated
facts. Public scalar/list output ordering is additionally checked by runtime
parity tests, not erased by a set-based quality scorer.

From the repository root, using the pinned Python environment:

```sh
python zig/pkg/inference/scripts/gliner25/evaluation_contract.py audit --lock /data/suite/lock.json
python zig/pkg/inference/scripts/gliner25/evaluation_contract.py prepare --lock /data/suite/lock.json --output-dir /data/suite/prepared
python zig/pkg/inference/scripts/gliner25/evaluation_contract.py score --prepared-dir /data/suite/prepared --predictions /data/suite/predictions.jsonl --output /data/suite/metrics.json
```

Preparation writes `requests.jsonl` separately from `gold.jsonl`, followed by a
complete receipt with both hashes. The model worker receives only request IDs,
request hashes, immutable text, full schema and frozen options. The dataset
adapter converts actual returned values to exact prediction facts; it must not
read gold to choose endpoints, records or labels. Prediction rows are
`{request_id, request_sha256, metrics}`. Scoring rejects missing, duplicate,
reordered or mismatched requests and changed files. Failed preparation has no
complete receipt. This boundary prevents accidental gold fields from entering
the inference API; it is not a security sandbox for a hostile worker.

The current helper reports micro precision/recall/F1, raw TP/FP/FN, support,
document exact match, and the rate of nonempty predictions on zero-gold
queries. It also retains both zero-gold denominators and false-positive query
counts. Empty support has F1 zero and is never evidence of
coverage. These base metrics do not replace official head/coreference matching,
partial-record assignment, type/language macro metrics, numeric count error,
calibration, constraint validity, or grouped confidence intervals. Those must
be implemented and differential-tested in each frozen scorer before its gate
is evaluated. The six synthetic helper tests validate the contract only.

## Required task metrics and release evidence

Entity quality needs typed exact-span micro and macro F1, absent-query false
positive rate, overlap/length/noise slices and proposal recall without gold
candidate injection. Attribute quality needs end-to-end `(entity occurrence,
attribute group, label)` F1 and per-group multilabel exact match. Conditional
gold-span attribute accuracy is diagnostic, and cannot substitute for missed
entity accounting.

Classification needs task accuracy or multilabel micro/macro F1, subset accuracy,
default-label errors, calibration and strict constraint violations. Preserve
ordinal ordering where declared. JointIE needs typed node F1, directional edge
F1, complete graph exact match and an independent graph validator. Report
feasibility, final search exhaustion, retained-candidate recall and optimizer
scope. A feasible beam result is not an optimality proof.

Records need whole-instance multiset F1, field/role F1 under a deterministic
one-to-one instance matching policy, count errors, enum accuracy, required-field
and exclusivity violations. Report natural, latent and anchorless modes
separately, including repeated equal-valued records. Long documents need full
original-source coordinates, repeated mentions, cross-window relation distance,
cross-window field grouping and globally revalidated constraints. Measure real
word/token distributions; a multi-sentence dataset does not automatically
exercise the greater-than-4096-word path. Synthetic padding is a stress slice,
not evidence of natural long-document quality.

Run published FP32 Python/native parity, strict same-bundle CPU/Metal parity,
reduced-storage quality relative to published FP32, and fine-tuned/adapter
quality as separate comparisons. Every cell binds model/source/adapter/bundle,
tokenizer, schemas, token sequences, build/runtime policy, decoder profile,
corpus and scorer hashes. Confidence tolerance cannot hide changed decisions.
Use paired document-family bootstrap for quality deltas and language/domain
slices; retain counts for rare classes. Freeze per-task quality floors and
non-inferiority margins before opening test results. An unset floor is a
pending gate, not an automatic pass.

The remaining production corpus work is substantial: representative customer
formats and ontologies; all constraint operators with feasible and infeasible
gold; JointIE symmetry/inverse/degree/cycle policies; globally exclusive record
resources; source-free enums and literal/regex conflicts; exact latent and
anchorless occurrence identity; multilingual span attributes; and human-labeled
long documents with cross-window facts. Public-task averages cannot replace
these cases. Corpus qualification also remains separate from latency, memory,
concurrency, cancellation, eviction, soak and supported-platform gates.
