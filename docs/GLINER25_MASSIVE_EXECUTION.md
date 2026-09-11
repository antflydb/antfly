# MASSIVE execution contract

The additive `evaluate_massive11.py` runner admits the ten already locked
[MASSIVE 1.1 profiles](GLINER25_MASSIVE.md), executes three bounded blinded
shards, and publishes metrics only after proving complete ordered coverage
of all 2,974 test requests. It runs independently of the six frozen CrossNER
helpers. Measured execution below covers the published small model's English
entity and intent-classification profiles on Python, native CPU and Metal. Other profiles, model variants,
confidence intervals, performance and release qualification remain separate
requirements; no tuning used these locked test results.

## Immutable admission

`scripts/gliner25/massive11_execution.json` pins the audited preparation,
corpus manifest, adapter, all ten full schemas and metric inventories, three
published model revisions/five-file identities, and every global/shard
request-ID and request-content digest. The runner audits original source
bytes and split identities, reconstructs requests and gold independently,
and compares them to the approved preparation before loading a model. It
does not execute an adapter supplied by an arbitrary lock.

The separate `gliner25_massive11_blinded_execution/v2` worker envelope contains
one shared schema/options object and the original text, source ID, request
ID and request hash for every row in its shard. It contains no gold labels,
gold spans or per-example label shortlist. Sharing the schema makes the
60-constraint profile fit the existing 8 MiB fixture limit. Native and Python
independently verify fixed schema/source/model pins, exact half-open global
range, ordered digests and each reconstructed request hash. Native admission
embeds the approved registry's blinded pins; rehashing a changed envelope
does not authorize new examples or options.

CrossNER v1 retains its parser, 14-label schema, limits, output and six helper
hashes. The ten-case bundle diagnostic mode is unchanged. The new splitter
field belongs to this evaluation envelope; it does not enable a new public
serving API option by itself.

## Fixed inference and resource policy

All profiles retain threshold `0.5`, flat overlap, strict decoding and UTF-8
byte offsets. Ordinary classification uses the model's ordinary calibrated
classification path. Intent/scenario uses pinned `Classifier` compilation,
scoring and decoding with all 60 training-derived implications and
`on_infeasible="raise"`. Native uses the corresponding canonical tasks and
constraints. An infeasible or finally exhausted native search cannot become
a successful output.

| Control | Admitted value |
|---|---:|
| Test requests per profile | 2,974 |
| Shard ranges | `[0,1024)`, `[1024,2048)`, `[2048,2974)` |
| Original UTF-8 text bytes per request | 1 MiB |
| Processor words, including inserted terminal punctuation | 128 |
| Encoded schema plus text tokens | 512 |
| Boundary queries / classification labels | 64 / 78 |
| Exact search nodes / native final beam nodes | 200,000 / 200,000 |
| Beam width / source candidates per task | 16 / 64 |
| Native local assignments / subset visits | 4,096 / 65,536 |
| Output values / output string bytes | 2,048 / 1 MiB |
| Native request allocator ceiling | 512 MiB |
| One encoded worker event / one shard evidence file | 4 MiB / 64 MiB |
| Native cooperative request deadline | 120 seconds |
| Driver response / model-startup deadline, defaults | 125 / 180 seconds |
| Worker RSS ceiling, default | 6 GiB |

Python uses one Torch thread and one inter-op thread, explicit BLAS thread
limits, offline loading and a network-denial guard. Native and Metal use the
same complete ReleaseFast binary and strict FP32 activation/head/accumulation
policy, including reduced encoder artifacts. Python is bounded by the process
RSS/deadline guard; the 512 MiB native allocator is an additional native
request limit, not a claimed Torch allocator limit. The driver kills and
reaps a worker on timeout, excessive RSS/output, interruption or protocol
failure. It never restarts or silently retries an example. Each shard is a
fresh model process, avoiding splitter-dependent caches from another profile.
The Python launcher preserves the absolute virtual-environment interpreter
invocation path. The receipt separately hashes its resolved executable and
records `sys.prefix`, `sys.base_prefix`, bounded `pyvenv.cfg` bytes, relevant
Python environment variables and interpreter flags. The worker verifies that
identity before importing the numerical runtime and again after execution;
all three shards must share it. Resolving a venv's executable symlink for
launch would select the base environment and is rejected by this contract.

Original whitespace profiles remain for all six locales. zh-CN and ja-JP
companions explicitly select `char`, passed to the existing native processor
and Python's pinned `set_word_splitter`. Original `utt` whitespace and UTF-8
source spans remain unchanged. Preflight uses `max_len=None`, rejects overflow,
and checks actual encoder token IDs against that untruncated preparation.
Known Hindi and Japanese char word-limit cases remain in the denominator;
encoded-token failures must be measured rather than inferred from preparation.
A different capacity policy needs a separate declared version before
execution, not a silent retry or a change after viewing test outcomes.

## Commands

Run from the repository root with new output directories. This example uses
multilingual Chinese char; substitute any approved profile/source variant
without changing its schema or options.

```sh
GLINER25_PYTHON=/private/tmp/antfly-gliner25-oracle-venv/bin/python
GLINER25_PREPARED=/private/tmp/antfly-gliner25-eval-data/massive11-locked-v1
GLINER25_RUNNER=zig/pkg/inference/scripts/gliner25/evaluate_massive11.py

PYTHONDONTWRITEBYTECODE=1 "$GLINER25_PYTHON" "$GLINER25_RUNNER" prepare \
  --prepared-root "$GLINER25_PREPARED" --profile entities_zh-CN_char \
  --model multi --output-dir /private/tmp/massive-multi-zh-char-preflight
```

`prepare` loads no model. It emits three compact fixtures and a receipt
pinning their bytes and the current helper closure. Run source FP32 Python
first, one shard at a time:

```sh
for GLINER25_SHARD in 0 1 2; do
  PYTHONDONTWRITEBYTECODE=1 "$GLINER25_PYTHON" "$GLINER25_RUNNER" run-shard \
    --prepared-root "$GLINER25_PREPARED" --profile entities_zh-CN_char \
    --model multi --model-dir /private/tmp/antfly-gliner25-models/multi \
    --backend python --shard "$GLINER25_SHARD" \
    --output-dir "/private/tmp/massive-multi-zh-char-python-$GLINER25_SHARD"
done

PYTHONDONTWRITEBYTECODE=1 "$GLINER25_PYTHON" "$GLINER25_RUNNER" aggregate \
  --prepared-root "$GLINER25_PREPARED" --profile entities_zh-CN_char \
  --shard-report /private/tmp/massive-multi-zh-char-python-0/report.json \
  --shard-report /private/tmp/massive-multi-zh-char-python-1/report.json \
  --shard-report /private/tmp/massive-multi-zh-char-python-2/report.json \
  --output-dir /private/tmp/massive-multi-zh-char-python
```

Build once through the complete repository graph from `zig/`:
`zig build inference-gliner25-bundle-check-build -Doptimize=ReleaseFast -Dmetal=true -Dcuda=false -Donnx=false -Dpjrt=false -j1`.
These explicit flags make the entire shared dependency graph ReleaseFast. On a CPU
host use `-Dmetal=false`; that binary cannot qualify Metal.

For native shards use the same `run-shard` arguments with `--backend native`,
`--binary zig/zig-out/bin/antfly-inference-gliner25-bundle-check`,
and `--source-reference-report /private/tmp/massive-multi-zh-char-python/report.json`.
Aggregate those reports with that same source-reference option. `--model-dir`
may point to original FP32 or an admitted converted bundle. Actual source
files or the complete bundle receipt/files are rehashed before and after
execution.

For Metal shards and their aggregate, use `--backend metal` for execution,
the same source-reference option, and
`--native-reference-report /private/tmp/massive-multi-zh-char-native/report.json`.
The native reference must use identical artifact and executable bytes. Never
reuse native evidence from another bundle or arithmetic policy. Root must
schedule model runs serially on the constrained development machine; these
commands are instructions, not evidence they have run.

## Completion, errors and resumability

Each shard pins envelope, consumed raw response bytes, independently derived
predictions, artifact, executable, splitter, helper closure, resource policy
and reference hashes. Complete transport requires the worker's final artifact
verification and successful exit. Interruption or missing results leave an
incomplete receipt and explicit unprocessed count. Shards contain no metrics.

Aggregation requires exactly three complete shards in order with identical
execution identities. It rederives predictions/comparisons from pinned raw
responses and validates every request ID/hash. Missing, duplicated,
overlapping, reordered, altered or differently configured shards fail.
Resume means supplying completed compatible shards and executing missing
shards into new directories; existing outputs/receipts are never overwritten.

Only complete coverage can publish `report.json`, using a synced temporary
file and atomic exclusive publication. A failed aggregate retains raw staging
evidence and `failure.json`, with no metrics. Complete transport with explicit
per-request errors produces a labeled diagnostic: failed rows supply no
predicted facts, all their gold remains, and all 2,974 documents remain in
every applicable denominator. Original errors remain errors, never empty
successes. Errors or failed required parity make the CLI exit nonzero even
when a complete diagnostic aggregate exists. Every receipt says
`qualification:false`.

Exact token parity is required for reduced artifacts too. Source FP32 native
requires exact selections/spans and unchanged `5e-4` confidence tolerance
against Python. Metal requires that same parity against native execution of
the identical artifact. Reduced-model quality deltas remain separate from
backend numerical parity. Classification metrics include intent accuracy/all
60 label slices, scenario accuracy, joint assignment accuracy and a direct
constraint-satisfaction document rate. Absent labels stay in the inventory.
Raw solver evidence is retained without treating beam feasibility as proof
of global optimality.

Translated/duplicate families and char companions are correlated. Family-aware
confidence intervals and broader task evaluation remain separate work. There
is no threshold/schema/mapping/model selection on the locked test and no
claim about pretraining contamination.

## Focused evidence

`test_massive_eval.py` covers ten schemas/three ranges, Unicode/source-ID
hashes, rehashed tampering, full-ontology outputs, strict constraint witnesses,
exact token/confidence comparisons, consumed-byte provenance, atomic
publication, and fake-worker aggregation/error/cancellation. CI discovers
the tests through:

```sh
PYTHONDONTWRITEBYTECODE=1 python3 zig/pkg/inference/scripts/run_model_contract_tests.py gliner25
```

All 110 GLiNER25 Python contract tests passed, including 19 MASSIVE tests and
a real disposable symlink-venv subprocess with an environment-only import.
The log is `/private/tmp/gliner25-massive-venv-contract-v1.log`, SHA-256
`cea898107373a8830c43b3a18eaaa26347c220a7604092cc681b50d34cc5d4e5`.
The six frozen CrossNER helpers remain byte-identical.

Fixture-only validation of all 30 real shards is under
`/private/tmp/gliner25-massive-v2-preflight-v3/`; `preflight_matrix.json` has
SHA-256 `3a31669ad1806d665ac00571bb97bf7c190d55a8e262778f489bcadfe256616b`.
The largest shared-schema fixture is 318,006 bytes. The fixtures remain
byte-identical to the earlier v1 preflight; the new receipt binds the corrected
Python launcher and current helper closure.

The compiled native v2 worker admitted all ten profiles' three multilingual
model envelopes before the deliberately absent model loader, and rejected a
tampered request. This is 30 admission checks, with no encoder execution:
`/private/tmp/gliner25-massive-native-admission-v3/report.json`, SHA-256
`5ba53347348b26d79d4c64d6531c979d0ba030a81aa60919d11586e2edcb8ec6`.
Its 14,600,280-byte ReleaseFast worker has SHA-256
`a43fcdee29931bcb67972ff002f3c1457e25f547cda24a60759cfed4a0f6880d`.

The separate source-only audit at
`/private/tmp/gliner25-massive-source-schema-v5/report.json`, SHA-256
`e9fd97a98dbd0be943a71779e22a9fc33a602aee03f3dbdef4e03cfaec723421`,
checks all ten actual pinned schema/compiler paths, every one of the 1,080
intent/scenario assignments, and the result facade. It blocks numerical
imports and uses AST-verified package carriers for pure source modules.
It proves neither tokenization nor neural scoring nor decoder search.

The first Python shard attempt is preserved at
`/private/tmp/gliner25-massive-small-en-python-0/`: it processed zero requests
and loaded no model because resolving the venv symlink selected the base
interpreter without the pinned Accelerate/PEFT packages. The corrected
launcher then passed a metadata-only subprocess check in the actual oracle
venv: `/private/tmp/gliner25-massive-python-launch-v1/report.json`, SHA-256
`5b80a2467fc5a566749797d3b72e64e05b095b56e57d1045925ac302c5c573c2`.
The failed output is never reused. The corrected Python and native English
executions below have completed; remaining profile and backend coverage are
separate requirements.

## First complete English source run and coordinate errors

The published small Python model completed all three fixed `entities_en-US`
shards and accounted for all 2,974 requests. Its aggregate retained **2,972
successful results and two source-coordinate errors**. The report is
`/private/tmp/gliner25-massive-small-en-python-v2/report.json`, SHA-256
`0abf84c9fab9d039a665a0bee2d4dd2e48922806dc5e3d38e8699de1e1cbad4f`.
Complete transport does not make this an error-free qualification: the CLI
exits nonzero when the completed diagnostic contains request errors.

At the unchanged full 55-slot schema, threshold 0.5, flat overlap and original
whitespace profile, exact entity totals were 1,171 true positives, 2,765 false
positives and 1,644 false negatives over 2,815 gold spans. Micro precision was
29.7510%, recall 41.5986%, and F1 34.6912%. All 2,974 documents remain in the
denominator, including the two errors and 994 documents without gold spans.
The failed rows contribute no predicted facts and retain all their gold.
These diagnostic results establish neither an absolute quality floor nor a
release pass. No schema, threshold, mapping or model selection used this test.

A separate blinded diagnostic reproduced the two errors without changing the
original reports, prepared requests, gold, helpers or scoring. It is
`/private/tmp/gliner25-massive-small-en-errors-v1/report.json`, SHA-256
`44c9a12094ae0312c3473343ecdd3d6a44f564a99170e760b719126106937818`.

| Original input | Original codepoint length | Pinned Python candidate | Reported span |
|---|---:|---|---|
| `current time in cpt` | 19 | `time_zone: "cpt."` | `[16,20)` |
| `meaning of` | 10 | `definition_word: "meaning of."` | `[0,11)` |

The pinned source [processor](https://github.com/fastino-ai/GLiNER2/blob/3c913c7369301133d3b7699252074c4303ada50e/gliner2/processor.py#L522)
appends terminal punctuation before building records, and the padded batch
stores that modified record text as `original_texts`. The boundary
[entity formatter](https://github.com/fastino-ai/GLiNER2/blob/3c913c7369301133d3b7699252074c4303ada50e/gliner2/models/boundary/engine.py#L283)
checks against word-map length and formats from the modified text. A selected
span ending in a synthetic period can therefore extend beyond the caller's
original input. Both observed candidates violate the frozen original-source
coordinate contract and correctly remain request errors. Clipping their
offsets, trimming their text or relabeling them as empty successes would
change the admitted evaluation and has not been done.

The native processor retains the immutable caller document and separate word
source ranges. A wholly synthetic terminal word has no source range. Ordinary
pipeline formatting and JointIE candidate admission omit a candidate if its
first or last word has no original source; they do not trim a scored mixed
span into an unscored shorter span. A different tokenizer case can incorporate
the synthetic period into an original URL word: that word retains its original
source range, marks `has_synthetic_suffix`, and formats only the original URL
bytes. This is an explicit source-coordinate safety policy, not parity with
the pinned formatter's out-of-document output.

The first native shard completed 1,024 requests with zero errors using the
frozen `a43fcdee...` worker. Its report is
`/private/tmp/gliner25-massive-small-en-native-v2-0/report.json`, SHA-256
`b9570606ef748f414353ac27afd5bb83147a958dfd9445d912c50afebac2e9bb`.
For `current time in cpt`, the raw native response retains only
`time: "current time"` at `[0,12)` and omits the `time_zone` candidate; it does
**not** return a clipped `"cpt"`. The response file SHA-256 is
`119fb39b8c0b4da350ebb22f5aa49f1f70af16567e227cdec9623c34519834f8`.
The complete native aggregate now accounts for **2,974 successful requests,
zero errors**, with all three locked shards exactly once. Its report is
`/private/tmp/gliner25-massive-small-en-native-v2/report.json`, SHA-256
`58dab6aaf0498ec1e7a30f13e765e8f13af3e584175db4a99ea1313498315db0`.
Of the 2,974 comparisons, exactly the two original Python error rows fail;
the other 2,972 match exact token IDs and selected spans/labels, with maximum
aligned confidence difference `6.079673767089844e-6` at the unchanged `5e-4`
bound. Both aggregate required-parity and required-token-parity flags remain
false because an error row cannot establish reference parity.

Native totals are 1,171 true positives, 2,766 false positives and 1,644 false
negatives, giving F1 34.6860% on all 2,974 documents. The additional false
positive is the valid remaining `time` entity in the Python-error row:
the Python error contributes no predictions for that whole request, while
native retains its valid result. This 0.00514 percentage-point difference is
a recorded formatter/error-accounting difference, not a quantization result
or a reason to change thresholds. The known representation difference stays
visible without changing tolerances or excusing source errors.

All three actual Metal shards use the identical source artifact and frozen
`a43fcdee...` executable. The complete aggregate accounts for **2,974 successful
requests and zero errors**. All 2,974 token/decision comparisons against native
pass, with 3,937 aligned confidences and maximum absolute difference
`9.47713851928711e-6` at the unchanged `5e-4` bound. All 56 metric entries,
including the full 55-slot inventory, exactly equal the native report: entity
F1 is 34.6860%, with the same 1,171/2,766/1,644 TP/FP/FN counts.

Against Python, 2,972 requests pass exact token/decision parity and 3,936
aligned confidences have maximum difference `1.0073184967041016e-5`. The two
original coordinate errors remain failed reference rows. The aggregate's
`required_parity` explicitly selects `same_artifact_native` and passes. Its
separate `required_token_parity` includes both source and native references
and remains false because the Python error rows provide no successful token
reference. The aggregate CLI therefore exits one, preserving the unresolved
source comparison instead of turning it into an overall pass.

The Metal aggregate report is
`/private/tmp/gliner25-massive-small-en-metal-v2/report.json` (2,295,854 bytes),
SHA-256 `7ef06510ccc91c34d13a91cbd80733ca1dc6b679d0e4075e027d421fb0541328`.
Its three shard reports, under the corresponding `...metal-v2-{0,1,2}/`
directories, remain separately pinned:

| Shard | Requests | Peak worker RSS | Report SHA-256 |
|---|---:|---:|---|
| 0 | 1,024 | 771,538,944 B | `aa0bd91c54bd3327559a8bd8cca0267f92aca84724e933fb87b6d5d4e33534bf` |
| 1 | 1,024 | 763,445,248 B | `d220f4fdf1c00fe0c1c221b8a6d7e6c6a123bdd447edd5a33ad14111a6d0c237` |
| 2 | 926 | 664,977,408 B | `f9279b186bf7b3a004db6e0bbc4946d29b3249939e8b2dfeb5c4d5ac5b473398` |

An independent read-only audit rehashed the three aggregate reports, all raw
responses/predictions and Metal shard fixtures, then rederived every comparison
and all metric entries using the frozen contracts. It verified complete
ordered coverage, identical worker/artifact/helper identities and preservation
of the two source failures. Its receipt is
`/private/tmp/gliner25-massive-small-en-metal-independent-audit-v1.json`,
SHA-256 `441ceb5be8568ca4175fc0feee38bbd246a62b86682854194d10d1c240c17790`.
The audit executed no model and changed no original report, helper, schema,
threshold or tolerance. This remains evidence for one small English slot
profile; all reports retain `qualification:false`.


## English intent classification checkpoint

The published small FP32 model completed all three locked `intent_en-US`
shards on Python, native CPU and Metal: **2,974 requests, zero errors** on each.
Every request supplied the same ordered **60-label ontology**. Labels were
never selected from the row's gold annotation. Ordinary single-label
classification emits its argmax and confidence even when that confidence is
below the fixed 0.5 threshold; this is the pinned ordinary source behavior.

The additive [complete evidence ledger](../zig/pkg/inference/testdata/gliner25/massive_intent_small_execution_v2.json)
has SHA-256 `022c61abcd2900cf84042684f2a47c37a40adae48fe6dfa1d4e7a776659748d7`.
The earlier [partial checkpoint](../zig/pkg/inference/testdata/gliner25/massive_intent_small_execution_v1.json)
is preserved unchanged; it covered only Metal shards 0 and 1.
Its [auditor](../zig/pkg/inference/scripts/gliner25/audit_massive_intent_execution.py)
recomputed predictions, all 61 metric families, token digests and numerical
comparisons from exact bounded descriptor bytes. It checked the original
five model files and the executable before and after replay, all six complete
Python/CPU shard receipts, all three Metal shards and their complete aggregate, prepared request/gold
pins, and the unchanged 14-file evaluation helper closure. The raw reports
remain necessary for full replay; the compact ledger preserves their identities
and the reviewed result after temporary files are removed. It is separate from
the upstream numerical fixture manifest.

| Comparison | Requests | Exact token sequences and selected labels | Maximum selected-confidence difference |
|---|---:|---:|---:|
| Native CPU / Python | 2,974 | 2,974 | `4.649162292480469e-6` |
| Metal / Python | 2,974 | 2,974 | `4.887580871582031e-6` |
| Metal / native CPU | 2,974 | 2,974 | `6.765127182006836e-6` |

All comparisons retain the `5e-4` absolute confidence bound. CPU and Metal
use the same `a43fcdee...` ReleaseFast binary and artifact. The largest observed
encoded request is 363 tokens, below the unchanged 512-token limit. Peak
sampled worker RSS was 1,283,653,632 bytes for Python, 674,824,192 bytes for CPU,
and 822,525,952 bytes across the Metal shards, each under the recorded
6 GiB guard. All three shards occur exactly once in each complete aggregate.

Python, native CPU and Metal select the correct intent for **1,633 / 2,974
requests (54.9092% accuracy and micro F1)**. All 61 metric families match
exactly, including all 60 per-intent slices. Fixed-ontology macro F1 is
52.5149%. `cooking_query` has zero gold support but retains 47 false positives
in its declared slice. The measured accuracy does not establish an agreed
absolute quality floor or release qualification. These results prompted no
held-out threshold, schema or model tuning.

The actual ordinary APIs expose one selected label and one confidence per
request. They do **not** expose the full 60-probability vector. This evidence
therefore proves selected-label/confidence parity, not equality of unselected
probabilities, their ranking, or probability calibration. It also does not
qualify the separate intent/scenario constrained profile or any non-English
profile.

Seven additive adversarial auditor tests passed. They reject foreign models,
executable/data pins and resource ceilings, wrong request IDs,
extra labels/tasks, nonfinite confidences, changed evidence bytes and false
parity despite wrong tokens; they also preserve absent-label false positives
and below-threshold argmax output. Log:
`/private/tmp/gliner25-massive-intent-audit-tests-v4.log`, SHA-256
`866bdbe94c4a83a5d6b3c1e126f1ec31bb9aaa8aa5d75eb6d41dcdf868763723`.
The earlier test-only expectation error is retained in the v1 log. No model
ran during this audit.
