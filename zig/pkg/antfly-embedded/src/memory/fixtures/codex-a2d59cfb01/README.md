# Codex external-judgment fixtures

Copied unchanged from [antflydb/codex at a2d59cfb01](https://github.com/antflydb/codex/tree/a2d59cfb01/codex-rs/antfly/tests/fixtures/knowledge_eval), under that repository's Apache-2.0 license. See [the integration review](https://github.com/antflydb/antfly/pull/1227#issuecomment-6327590919).

Native tests replay the labelled verdicts through service policy. They validate admission, application, distinct pins, typed relationships and supersession; they do not measure a model's accuracy. Hosted inference and credentials stay with the client. The 64-case merge corpus is distinct from the earlier 30-case concept-pair corpus.

Candidate discovery evaluation uses `scripts/bench/memory_discovery.py` against
an isolated space on a server with a revision-qualified managed embedder:

```sh
uv run --project scripts --locked python scripts/bench/memory_discovery.py \
  --url http://127.0.0.1:8080 --model /path/to/qualified-model.json \
  --output /tmp/codex-discovery.json
```

The model file contains `provider`, `model`, `revision` and `dimensions` only.
Authentication, if needed, is read from `ANTFLY_AUTHORIZATION`. The service owns
provider credentials. The evaluator imports 84 names and their fixture
descriptions without synthesizing aliases from the labels. It reports recall
for the 18 synonyms, admission of the 46 labelled non-synonyms, average candidate
counts and top-five pair recall for lexical, ANN and union modes. A pair is
admitted if either endpoint retrieves the other. It retains the model identity,
fixture digest and complete candidate pages, and rejects truncated measurements.
The all-pairs labels are incomplete, so labelled non-synonym admission is not a
claim of overall identity precision. Test vectors establish mechanics only.

The native lexical baseline (84 entity names, first description per name,
no aliases, either-direction pair admission) currently measures **17/18** synonym
pairs found, **17/18** in the first page's top five, **26/46** labelled non-synonyms
admitted, **2.929** candidates per seed, and **zero primary entity scans**. This is
reproduced by the `memory Codex discovery fixture reports the indexed lexical
baseline` policy test. The real-model ANN/union qualification is reported below.

The native C ABI qualification can also load a local managed model directly:

```sh
cd zig
ANTFLY_MEMORY_QUALIFY_MODELS_DIR=/absolute/path/to/models \
ANTFLY_MEMORY_QUALIFY_MODEL=embeddinggemma2 \
ANTFLY_MEMORY_QUALIFY_DIMENSIONS=768 \
ANTFLY_MEMORY_QUALIFY_SCALE=10000 \
zig build memory-capi-test -Dmetal=false -Dcuda=false
```

It discovers and pins the runtime's actual model revision, uses the production
memory C ABI (including provider roles and lease release), reports all three
modes, and checks union recall and the knowledge-graph/KG and Antfly/AntflyDB
pairs. Without the model-directory variable, only this model-quality test skips;
the native vector-index and bounded-call checks still run with 128 seeds.
Set `ANTFLY_MEMORY_QUALIFY_SCALE=10000` for the full scale qualification; the
bounded-call counter and primary-scan assertion exercise the same path.

## Managed model qualification

The local EmbeddingGemma 2 checkpoint is acquired with the repository's pinned
[reference acquisition script](../../../../../inference/scripts/embeddinggemma2/reference.py)
from upstream revision `914f7f89142e33e77833254d9c9b90c3cef7303b`.
Antfly reports qualified model revision
`1de7ce3dfd82958332698f61e9810172ec07d4a8ef7cc0cbd0e25c732aa50d54`.
The qualification uses 768 dimensions and the managed retrieval-query and
retrieval-document roles. Other managed providers follow the same contract;
these accuracy numbers qualify this model only.

The proposed thresholds of 0.80 names / 0.90 views recall 17/18 synonym pairs in
the union (4.298 candidates/seed). Knowledge graph/KG is missed: its two directed
normalized name similarities are 0.777/0.766 and view similarities 0.851/0.865.
Antfly/AntflyDB has normalized name similarities 0.902/0.931 and view similarities
0.909/0.899. The evaluated default view threshold is therefore 0.85; names stay
at 0.80. No pair labels become aliases, and the rendered views are unchanged.
Use `--view-threshold 0.90` with the HTTP evaluator to reproduce the starting
threshold rather than relying on a server's implicit defaults.

The 10k native vector-index qualification measured **20,000 bounded neighbor
calls and zero primary entity scans** for 10,000 seeds. Both native serving
snapshots held 10,002 documents (the two lifecycle entities plus 10,000 scale
entities). The measured pass took 606.568 seconds in a Debug macOS arm64 build;
this establishes the bounded operation count, not production latency. Scale
uses deterministic vectors to isolate mechanics; model-quality evaluation uses
the separate real-model fixture above.

At the evaluated defaults (0.80 names / 0.85 views, 32 neighbors per projection):

| Discovery | Synonyms | Top-five synonyms | Labelled non-synonyms admitted | Candidates/seed |
| --- | ---: | ---: | ---: | ---: |
| Lexical | 17/18 | 17/18 | 26/46 | 2.929 |
| ANN | 18/18 | 18/18 | 38/46 | 4.631 |
| Union | 18/18 | 18/18 | 39/46 | 4.964 |

Both knowledge graph/KG and Antfly/AntflyDB are retrieved without aliases.
ANN admission uses `semantic_discovered`, including when a later lexical page
repeats a candidate; similarity alone does not establish ANN discovery. The
extra non-synonym admissions are candidates for the external judge, not accepted
merges. See [the machine-readable qualification](discovery-embeddinggemma2.json)
for the fixture digest, complete model revision, method and scale counters.

## Stored judge datasets

The same corpus can be uploaded to the native evaluation harness:

```sh
python3 scripts/bench/memory_judges.py --prepare-only --output /tmp/codex-judge-dataset.json
python3 scripts/bench/memory_judges.py --url http://127.0.0.1:8080 \
  --judge /path/to/qualified-judge.json --output /tmp/codex-judge-results.json
```

The upload has 94 identity inputs, 141 typed-predicate checks from the 47 relation
annotations, and 13 binary replacement inputs. It removes fixture gold fields
from judge inputs. Binary non-replacement labels are not fabricated four-way
"independent" annotations. Without `--judge`, the run waits for outside predictions
on its native task/verdict endpoints. Dataset roots, profiles, model/prompt
revisions, fixture digests and reports remain available for reproducible comparisons.
The server owns model credentials; the judge file carries identity/revision only.
