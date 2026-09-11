# GLiNER2.5 oracle and reference fixtures

The oracle freezes the upstream implementation, Python numerical environment,
and published model contents used for native parity work. A successful capture
means the Python reference ran. It does **not** qualify native inference,
training, quantization, Metal execution, extraction quality, or performance.
Those require separate release evidence for each artifact and backend.

The source pin is [Fastino GLiNER2 commit
3c913c7369301133d3b7699252074c4303ada50e](https://github.com/fastino-ai/GLiNER2/tree/3c913c7369301133d3b7699252074c4303ada50e).
The exact package versions, all required model files, byte sizes, and SHA256
digests are in
[`oracle_manifest.json`](../zig/pkg/inference/scripts/gliner25/oracle_manifest.json).

| Model | Immutable Hugging Face revision |
| --- | --- |
| `fastino/gliner2.5-base-v1` | `72ac19b486cd4557424c8d61114e7530c243e9b0` |
| `fastino/gliner2.5-small-v1` | `cab1bddfd30fda7b803a4691c41f90378a2d517a` |
| `fastino/gliner2.5-multi-v1` | `aaecfe45db1d828c963717054ccb868e8ad1f1d5` |

## Checked-in evidence

[`testdata/gliner25`](../zig/pkg/inference/testdata/gliner25) contains the verbatim
configuration, encoder configuration, and tokenizer configuration for all three
models; `encoder_config/config.json` is flattened to `encoder_config.json` only
in those small configuration fixtures.

`primitives.json` contains upstream shared-pool selection, centered inside prefix
and autograd VJP results, and overlap resolution. Pool cases include two ragged
documents, inactive queries, exact ties, and scores below the finite `-10000`
mask sentinel. All arrays are flattened in the declared layouts. `lengths`
counts **words**, so a document with length 5 has 6 valid boundaries.

`regex.json` pins Python 3.12 / Unicode 15 validator behavior for 390 compiled
pattern/flag configurations, 57 texts and full/match/search modes (66,690
comparisons), plus all Unicode decimal-digit ranges. The generator is
`capture_regex.py`; both files participate in the reference integrity manifest.
The native bounded engine's supported syntax and explicit exclusions are
documented in [`GLINER25_REGEX.md`](GLINER25_REGEX.md).

`token_evidence.json` contains all 30 encoder token sequences extracted from
an already completed native/Python validation report. It covers the direct
classification and JointIE encoder calls that bypassed the original boundary
core tensor hook. This is derived benchmark evidence, not a fresh tensor
capture: the file binds the exact report, driver, native binary, dependency
profile, model files, ordered canonical requests and sequence hashes.
`extract_token_evidence.py` revalidates those identities and the recorded output
parity gates without running a tokenizer or model. Benchmark timing files remain
separate from the tensor oracle; this compact evidence carries its own scope.

`training_losses.json` and `capture_training_losses.py` pin independent native
loss and derivative expectations. Their passing focused tests establish those
loss functions only; they do not qualify full-model training, optimizer state,
adapters, or a training release. `training_targets.json` and
`capture_training_targets.py` add ten target-packing cases from the pinned word
schema transformer and boundary packer without a model or subword-tokenizer
load. Five loss tests and seven target tests passed; full backward and optimizer
training remain separate gates.
`training_selection.json` and `capture_training_selection.py` add 42 pinned
classification/candidate-selection cases. Their six native tests passed. This
qualifies detached selection semantics only; differentiable model training is
still a separate gate.
`training_matching.json` and `capture_training_matching.py` add eighteen
matching cost/assignment cases; all six focused native tests passed.
`training_head.json` and `capture_training_head.py` capture a synthetic stage-one
head with nine outputs, all forty-six parameter VJPs and three input VJPs in
evaluation and training modes. The native seeded graph passed both cases with
explicit inverted dropout masks. The zero-dropout SDPA expansion is checked
against genuine upstream SDPA before capture. These tests establish bounded
head math and adjoints, not optimizer updates or end-to-end fine-tuning.
`training_candidates.json` and `capture_training_candidates.py` extend that
path through live shared-pool scoring at fixed retained spans. Native evaluation
and explicit-mask training passed all fourteen outputs, seventy-two parameter
VJPs and three input VJPs. The fixture records typed absolute boundary indices,
length features and each case's detached inside-centering mean. It neither
injects gold candidates nor differentiates discrete top-k selection.
`training_explicit.json` and `capture_training_explicit.py` cover the distinct
explicit-span scorer: both outputs, twenty-eight parameter VJPs and six input
VJPs passed in evaluation and explicit-mask training modes.
`training_relations.json` and `capture_training_relations.py` cover the live
directional sparse relation scorer: six outputs, twelve parameter VJPs and two
input VJPs passed, including invalid pair slots and a final-logit-only gradient
case. These small fixtures load no checkpoint or encoder.
`training_record_loss.json` and `capture_training_record_loss.py` add ten actual
dense-batch record loss and live-logit gradient cases; all four focused native
tests passed. They preserve global object/field denominators, alternative
occurrence mass, masked assignments and padding semantics. Record-head
loss composition and complete training steps remain separate gates.
`training_records.json` and `capture_training_records.py` cover the actual
batched `forward_groups_dense` record head in all three modes. Both group-mask
cases passed all eighteen parameter and three input VJPs, including the
all-masked candidate pool, live padded assignment rows and anchorless padding.
This establishes record-head forward/backward math; its complete loss and
optimizer integration still requires native end-to-end training-step parity.
`training_encoder/` and `capture_training_encoder.py` pin two tiny DeBERTa
shapes against Transformers 4.55.4. Native train/eval forward and all
thirty-eight parameter VJPs in training mode passed, together with repeated
word/query/classification/relation routing. The consumer does not check
evaluation-mode encoder VJPs.
`training_peft/` and `capture_training_peft.py` pin eight LoRA/DoRA cases against
PEFT 0.17.1, including separate dropout masks for repeated shared-module calls;
every adapter, magnitude and input VJP passed. Both folders hash their exact
library sources and Safetensors payloads.
`training_adamw.json` and `capture_training_adamw.py` pin actual Torch updates
over three parameters, three microbatches and two flushes, including clipping,
partial-window correction and absent versus zero gradients. Native numerical
and mid-window resume tests passed. `training_optimizer_groups.json` executes
the pinned `_create_optimizer` AST with a recording constructor and no Torch
import. All four supported native CPU profiles (full, heads, LoRA, DoRA) passed
its exact membership/LR/decay expectations. The remaining fixture branches
record source behavior only; symbolic CUDA/MPS flags are not hardware evidence.

`training_step/` and `capture_training_step.py` compose the actual tiny source
DeBERTa model, every task loss and AdamW in full, head-only, LoRA and DoRA
profiles. All four native CPU consumers passed exact preprocessing/mentions,
all scalar losses, every trainable gradient's absence/zero/value and both
optimizer flushes, including subsequent native updated-weight forwards.
The zero-dropout capture reproduced byte-for-byte and includes ordered
schemas, original UTF-8 annotations, exact tiny tokenizer artifacts and an
in-memory source resume. All four native profiles also pass durable
fresh-controller mid-window restore and the subsequent source-matched updates.
Native complete-mask validation and reordered-set dropout replay pass.
`training_step_dropout/` and `capture_training_step_dropout.py` separately pin
dropout 0.125 across the encoder, heads and adapters. Two captures reproduced
byte-for-byte, and all four native CPU profiles passed exact preprocessing,
all component losses, every gradient's absence/zero/value, durable
fresh-controller mid-window restore and both AdamW updates. The named mask
contract does not claim equivalent native/Torch random-number streams. These
four files and their generator are enrolled in the integrity inventory.
Complete Metal training and real-checkpoint convergence remain separate gates.

`tiny/` contains deterministic random head weights and a complete CPU/f32 forward
reference, plus ten fixed extraction requests through a tiny upstream model.
Head weights use the checkpoint prefix `boundary_head.`. Boundary-head settings
and tensor shapes are recorded in `capture.json`; weights and tensors use
Safetensors. The fixture includes both the shared scorer and explicit-span
scorer, which have distinct parameter and scoring paths.

`small_reference/` runs the same fixed requests against the published small
checkpoint. Captures include encoded token IDs, token-to-text mappings, encoded
text and query states, boundary marginals, candidate indices/masks/logits, and
public outputs. Requests cover mixed extraction/classification, Unicode,
attributes, legacy structures, natural/latent/anchorless records, enums,
constrained classification with strict infeasibility handling, and JointIE.
This small smoke corpus is a reference, not a task-quality benchmark.

`preprocessing.json` adds heterogeneous batches, descriptions, classification
prompts/examples, hidden attribute routes, enum prefixes, empty input, and both
Unicode word splitters. Its raw schema objects preserve insertion order.
`tasks/` contains independent classification, directional relation/biaffine,
and natural/latent/anchorless record-head weights and forward intermediates.
`tokenizer.json` contains 28 published-tokenizer Unicode cases, including
unknown characters and special-token boundaries; a generated tiny vocabulary
in the tokenizer module runs these semantics without model downloads.
`case_equivalence.json` independently fixes Python's Unicode literal
`re.IGNORECASE` behavior for record enum mentions. This differs from full
casefold: dotted/dotless I compare equal, while `ss` does not match `ß`.
`joint_candidates.json` covers sparse JointIE admission, edge deduplication,
per-type caps, rescued endpoints, and centered utility. `pipeline_cases.json`,
`pipeline_cases_base.json`, and `pipeline_cases_multi.json` adapt ten requests
per variant to Antfly's canonical schema and typed results. All three published
variants have checked-in `*_reference/` captures. The adapted format preserves
all five model-file identities, source/output/request digests, edge confidence,
and typed JointIE endpoint identity. Capture and native runtime qualification
remain separate: consult the implementation status for measured backend gates.

Record scoring cardinality and output dtype are separate contracts. The pinned
public processor omits dtype information while compiling record metadata, so an
unspecified non-anchor field uses `zero_or_more` assignment scoring even when
`dtype="str"` presents one value. Anchors default to `required_one`. Antfly
preserves these defaults; declare `optional_one` or `required_one` explicitly
when scalar softmax assignment is intended. Changing this default alters both
selected records and confidence, and is not numerical drift.

All three published tokenizers use the same ordered normalization sequence:
regex whitespace replacement, Unicode NFC, and right stripping. They do not
use a Precompiled normalizer. Native Unigram normalization uses generated
Unicode 15.0.0 tables, and unknown-character segmentation follows Tokenizers
0.21.4 with float64 path scores and consecutive unknown fusion. The generic
normalized-token offset API returns `null` until alignment tracking is
implemented; GLiNER's processor retains its own original-text word mappings.

SciPy **1.16.3** is installed and pinned for these references. The upstream
assignment helper uses SciPy when available, with a small deterministic cost
perturbation. Its dependency-free fallback can make different choices on ties;
captures using that fallback must declare a separate profile.

Native constrained decoding deliberately corrects upstream symmetric-relation
handling: a derived reverse companion does not conflict with its own source's
undirected-pair uniqueness. Missing slots consume no uniqueness resource;
distinct hypothesis/count slots remain distinct. Derived edges still satisfy
typed endpoints, self-loop rules, endpoint degree bounds, acyclicity, and final
global validation. Such corrections require explicit semantic tests, rather
than being hidden by a numerical tolerance. The native pipeline rejects final
search-budget exhaustion by default. Explicit `best_effort=true` may return an
already valid witness with `status=feasible` and `exhausted=true`; it never turns
cancellation, infeasibility, or exhaustion without a witness into a result. A
completed bounded beam reports a feasible result without claiming an optimum.

`reference_manifest.json` hashes the admitted references and their generators.
New training captures are enrolled after their focused native consumers pass;
the presence of a pending capture outside that manifest does not establish
native coverage.
No model weights or large tokenizer files are checked in. Verify configuration
and reference integrity without installing numerical packages:

```sh
python3 zig/pkg/inference/scripts/gliner25/oracle.py verify-fixtures
python3 zig/pkg/inference/scripts/gliner25/oracle.py verify-references
python3 -m unittest discover -s zig/pkg/inference/scripts/gliner25 -p 'test_*.py' -v
```

## Reproduce the oracle

Use Python **3.12.3** and the exact versions in
[`requirements.txt`](../zig/pkg/inference/scripts/gliner25/requirements.txt).
The checked-in reference ran on macOS arm64, CPU, float32, one Torch thread,
with deterministic algorithms enabled. A different platform can produce
floating-point differences even at the same package versions; retain its
provenance and evaluate appropriate tolerances.

```sh
git clone --no-checkout https://github.com/fastino-ai/GLiNER2.git /private/tmp/antfly-gliner25-upstream
git -C /private/tmp/antfly-gliner25-upstream checkout --detach 3c913c7369301133d3b7699252074c4303ada50e
python3.12 -m venv /private/tmp/antfly-gliner25-oracle-venv
/private/tmp/antfly-gliner25-oracle-venv/bin/pip install -r zig/pkg/inference/scripts/gliner25/requirements.txt
```

The source tree must remain completely clean, including ignored files. Run with
`PYTHONDONTWRITEBYTECODE=1`; do not install the source into its own checkout or
write `__pycache__` files there. The harness validates the actual imported module
paths, including namespace-package paths, so an unrelated installed `gliner2`
cannot silently become the reference. Runtime version mismatches are errors.

Download only the variant needed for the next test. The downloader is explicit,
serial, enforces a per-file byte and time budget, requires HTTPS, checks every
file hash, and publishes the destination only after complete verification.
It never overwrites an existing bundle. `--dry-run` reports the required bytes
without network access.

```sh
python3 zig/pkg/inference/scripts/gliner25/download_model.py --model small --output /private/tmp/antfly-gliner25-models/small --dry-run
python3 zig/pkg/inference/scripts/gliner25/download_model.py --model small --output /private/tmp/antfly-gliner25-models/small
PYTHONDONTWRITEBYTECODE=1 /private/tmp/antfly-gliner25-oracle-venv/bin/python zig/pkg/inference/scripts/gliner25/oracle.py verify --upstream /private/tmp/antfly-gliner25-upstream --model small --model-dir /private/tmp/antfly-gliner25-models/small
```

Capture into **new** output directories:

```sh
PYTHONDONTWRITEBYTECODE=1 /private/tmp/antfly-gliner25-oracle-venv/bin/python zig/pkg/inference/scripts/gliner25/capture_primitives.py --upstream /private/tmp/antfly-gliner25-upstream --output /private/tmp/gliner25-new-primitives
PYTHONDONTWRITEBYTECODE=1 /private/tmp/antfly-gliner25-oracle-venv/bin/python zig/pkg/inference/scripts/gliner25/oracle.py capture-tiny --upstream /private/tmp/antfly-gliner25-upstream --output /private/tmp/gliner25-new-tiny
PYTHONDONTWRITEBYTECODE=1 /private/tmp/antfly-gliner25-oracle-venv/bin/python zig/pkg/inference/scripts/gliner25/oracle.py capture-model --upstream /private/tmp/antfly-gliner25-upstream --model small --model-dir /private/tmp/antfly-gliner25-models/small --output /private/tmp/gliner25-new-small
```

Captures disable network access in Transformers/Hugging Face, use `AutoExtractor`
to dispatch the boundary architecture, and rehash source and model artifacts
after execution. Model-directory extras that can change loader behavior are
rejected. HF cache symlinks are accepted only when their actual contents match.
Missing evidence is an error, never a skipped test or a success result.

Adapt a completed capture without importing Torch or downloading a model:

```sh
PYTHONDONTWRITEBYTECODE=1 python3 zig/pkg/inference/scripts/gliner25/generate_pipeline_cases.py --model base --capture-dir /private/tmp/gliner25-new-base --output /private/tmp/gliner25-base-pipeline.json
```

The generator accepts `small`, `base`, or `multi` and rejects mismatched variant,
model revision, artifact hashes, request identity, capture generator, runtime
profile, and modified captured tensors. Native checkpoint tests use
`ANTFLY_GLINER25_SMALL_MODEL_DIR`, `ANTFLY_GLINER25_BASE_MODEL_DIR`, or
`ANTFLY_GLINER25_MULTI_MODEL_DIR`; they rehash every actual bundle file and check
all ten task outputs. Keep model validation serial on constrained machines.

The harness admits at most 32 requests, 128 words, 512 encoded tokens including
schema, and 64 queries per request. Each tensor artifact is capped at 32 MiB.
Those are capture safety limits, not GLiNER2.5 serving limits. Long-document
preprocessing, chunk merge/global constraints, token budget boundaries, training
gradients, and backend throughput remain independent qualification work.

When changing a pin, dependency, generator, request, or fixture, regenerate the
affected evidence and review the numerical/output differences before updating
`reference_manifest.json`. Do not update hashes merely to suppress a failure.
