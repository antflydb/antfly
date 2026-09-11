# GLiNER2.5 MASSIVE held-out preparation

Ten MASSIVE 1.1 profiles are prepared and independently reproduced without
executing a model. Each retains all 2,974 official test examples. This adds a
reviewable multilingual slot and classification contract; it supplies no model
quality, throughput, calibration or release qualification result.

The [source manifest](../zig/pkg/inference/scripts/gliner25/massive11_manifest.json)
pins the 40,251,390-byte official archive by SHA-256
`4cba5faa11c71437928e17cb1b9b3d8b8e727e7ea363a3a9a8045e19c0491577`,
six locale files and the release license. It also pins README, repository
licenses/notices and the official dataset converter from author revision
`f966f21846043aabef9b0f974fa7970027f43738`. The archive URL has no recorded S3
version ID; its content hash, rather than the URL or multipart ETag, defines
the admitted bytes. The [downloader](../zig/pkg/inference/scripts/gliner25/download_massive11.py)
fetches files serially, limits size/time and extracts only explicitly pinned
regular-file members. It publishes a source directory only after all hashes
pass.

Data is CC BY 4.0; repository code is Apache 2.0. Preserve attribution to
[FitzGerald et al., MASSIVE](https://arxiv.org/abs/2204.08582) and
[Bastianelli et al., SLURP](https://aclanthology.org/2020.emnlp-main.588/).
The author [README](https://github.com/alexa/massive/blob/f966f21846043aabef9b0f974fa7970027f43738/README.md)
and [third-party notice](https://github.com/alexa/massive/blob/f966f21846043aabef9b0f974fa7970027f43738/THIRD-PARTY.md)
remain in each locked source inventory. Raw source stays in the local cache;
workers receive only the prepared text, schema and options.

## Fixed schemas, original text and split identity

The [adapter](../zig/pkg/inference/scripts/gliner25/prepare_massive11.py) verifies
all 99,126 original rows in `en-US`, `de-DE`, `ar-SA`, `hi-IN`, `zh-CN` and
`ja-JP`. Full inventories of 55 slot types, 60 intents and 18 scenarios come
only from the original training partition and are frozen in lexicographic
order. Every entity request includes all 55 labels, including absent types.
The two English classification profiles use all 60 intent labels. The
constrained companion separately predicts an 18-label scenario and applies
60 intent-to-scenario implications derived from training rows. No test label
chooses a request schema, description, example, threshold or constraint.

Each locale has the same original IDs and partitions. A split family joins
translations by source ID and joins different IDs by NFC, Unicode 15 casefold
and whitespace-normalized text equality across all six locales. The declared
priority `test > dev > train` retains every test example and removes whole
lower-priority source-ID families across translations. The audit excludes
2,126 original IDs: 1,838 train and 288 development IDs. Within-test duplicate
examples retain their original multiplicity and share a resampling family.
Exclusions use source identity, partition and text only. They do not inspect
gold labels. The resulting train and calibration sets are derived partitions.

| Split, in every locale | Original examples | Retained examples |
| --- | ---: | ---: |
| Train | 11,514 | 9,676 |
| Calibration | 2,033 | 1,745 |
| Test | 2,974 | 2,974 |

Requests preserve the exact original `utt` bytes. Removing annotation wrappers
from `annot_utt` reproduces that text exactly in English, German, Arabic and
Hindi. Chinese and Japanese annotations sometimes insert ASCII spaces: 10,908
Chinese and 10,482 Japanese rows across all splits require the explicitly
declared ordinal character projection. It allows only U+0020 spacing
differences and maps each non-space character to the identical ordinal
character in `utt`. Gold spans then use exact half-open original UTF-8 byte
coordinates, including internal source spaces. Any other text change, empty
slot, ambiguous malformed annotation or invalid boundary fails preparation.
Repeated values do not use substring search.

The official [Hugging Face converter](https://github.com/alexa/massive/blob/f966f21846043aabef9b0f974fa7970027f43738/scripts/create_hf_dataset.py)
uses token/BIO preparation with special Chinese/Japanese handling. This
contract instead measures original UTF-8 occurrences. It does not claim
equivalence to an official token-level scorer.

## Explicit word-splitter profiles and capacity evidence

All three public checkpoints default to `whitespace`. The pinned
[splitter](https://github.com/fastino-ai/GLiNER2/blob/3c913c7369301133d3b7699252074c4303ada50e/gliner2/processing/word_splitter.py)
defines that default; the model accepts `char` only as a caller choice. Its
[setter documentation](https://github.com/fastino-ai/GLiNER2/blob/3c913c7369301133d3b7699252074c4303ada50e/gliner2/models/base.py)
says public checkpoints were trained with whitespace splitting. Character
splitting can change model quality and is a separate profile, never an
automatic language-dependent normalization.

| Entity profile | Gold occurrences | Maximum processor words | Gold spans unavailable to this splitter |
| --- | ---: | ---: | ---: |
| English, whitespace | 2,815 | 62 | 0 |
| German, whitespace | 2,824 | 58 | 0 |
| Arabic, whitespace | 2,794 | 43 | 0 |
| Hindi, whitespace | 2,820 | 265 | 0 |
| Chinese, whitespace | 2,802 | 12 | 2,759 |
| Japanese, whitespace | 2,794 | 13 | 2,649 |
| Chinese, explicit char | 2,802 | 88 | 0 |
| Japanese, explicit char | 2,794 | 133 | 0 |

Counts use the exact pinned processor regex and automatic terminal punctuation
rule. They establish boundary representability only; they are not prediction
scores. One Hindi test request and one Japanese char test request exceed the
existing evaluation profile's 128-word limit. They remain in the denominator.
Encoded-token and compiled-query admission still require the exact model
tokenizer and execution profile. No token count, capacity pass or successful
inference is inferred from these static word counts.

All profiles freeze threshold `0.5`, overlap `flat`, strict decoding and their
explicit splitter choice. Entity metrics include exact occurrence micro F1
and all 55 type slices. Intent metrics include exact micro F1 and all 60
intent slices; with one complete valid prediction per example, intent micro
F1 equals accuracy. The constrained profile additionally measures exact
scenario, exact intent/scenario pair and constraint satisfaction. Its 64
metric families fit the existing contract; per-scenario slices would require
an explicit contract extension. Finite confidences, complete declared query
coverage and exact source spans are checked without access to gold.

## Reproduction and transport

The local source is `/private/tmp/antfly-gliner25-eval-data/massive11-source`.
Independent complete trees are `massive11-locked-v1` and
`massive11-locked-repeat` under the same cache root. The compact
[evidence receipt](../zig/pkg/inference/scripts/gliner25/massive11_preparation.json)
binds every profile lock, requests, gold, transport manifest, source/adapter
hash and the full repeated file inventory. Source files are hard-linked
within those local trees when possible, then rehashed during admission.

Every test profile has three blinded transport shards of 1,024, 1,024 and 926
requests. A shard retains global request IDs and hashes and contains no gold.
The transport audit rejects gaps, overlaps, reordered or duplicated requests,
wrong lock identity and extra gold fields even when an altered shard's hash
is recomputed. Completion requires all 2,974 global results and an atomic
aggregate report. A shard is not a new dataset or a separately qualified
sample. The additive [execution runner](GLINER25_MASSIVE_EXECUTION.md) now
defines and validates a shared-schema v2 worker envelope with explicit
splitters, fixed resource limits and complete-shard aggregation. Its pure
contract tests do not qualify model outputs. The CrossNER evaluation helpers
remain frozen. The published small English entity and intent profiles now
have measured Python/CPU/Metal results in the [execution evidence](GLINER25_MASSIVE_EXECUTION.md).
The other profiles and model variants remain unqualified.

From the repository root, with a new destination for each preparation:

```sh
GLINER25_PYTHON=/private/tmp/antfly-gliner25-oracle-venv/bin/python
PYTHONDONTWRITEBYTECODE=1 "$GLINER25_PYTHON" zig/pkg/inference/scripts/gliner25/download_massive11.py --output-dir /private/tmp/massive11-new-source
PYTHONDONTWRITEBYTECODE=1 "$GLINER25_PYTHON" zig/pkg/inference/scripts/gliner25/prepare_massive11.py --corpus-dir /private/tmp/massive11-new-source --output-dir /private/tmp/massive11-new-lock
PYTHONDONTWRITEBYTECODE=1 "$GLINER25_PYTHON" zig/pkg/inference/scripts/gliner25/prepare_massive11.py --corpus-dir /private/tmp/massive11-new-source --output-dir /private/tmp/massive11-new-repeat
PYTHONDONTWRITEBYTECODE=1 "$GLINER25_PYTHON" zig/pkg/inference/scripts/gliner25/audit_massive11_preparation.py --prepared-root /private/tmp/massive11-new-lock --repeat-root /private/tmp/massive11-new-repeat --output /private/tmp/massive11-new-evidence.json
```

Fifteen focused tests cover text projection, Unicode/repeated spans,
translation-aware split exclusions, full schema/constraint invariants,
gold-blind output adaptation, source corruption, selected archive extraction
and complete blinded transport identity. These tests and deterministic
preparation do not qualify the model. Pretraining exposure, near duplicates
beyond the declared normalization, deployment representativeness, additional
languages, records, relations, JointIE and long documents remain separate
coverage and qualification work.
