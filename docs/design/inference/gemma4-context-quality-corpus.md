# Gemma4 context quality corpus v2

## Release claim

This corpus measures preservation of deterministic, source-grounded long-context
retrieval and transformation under attention-matching compaction. It does not
establish broad knowledge, safety, open-ended writing quality, or universal task
competence.

The campaign contains two independently frozen plans, one for Gemma4 E2B and
one for E4B. Each plan contains exactly 100 cases for every qualified-context
length and each of the five buckets: `document_qa`, `exact_retrieval`, `code`,
`structured_tool`, and `multiturn`. Source families are independent within each
100-case model/length/bucket cell. The same frozen cases may be used for a
paired E2B/E4B comparison or at another declared length; neither pairing is
evidence of broader semantic source independence.

## Frozen construction order

1. Seal source-family manifests for selection, fitting, calibration, E2B
   evaluation, and E4B evaluation. A family is a clone cluster, not a file hash.
   The manifest records repository, immutable revision, canonical path, content
   hash, normalized hash, license, content type, and declared authorship
   category (`human`, `generated`, or `mixed`). Authorship is provenance to
   report, not a quality gate; synthetic extraction may supplement but must not
   be mislabeled as source-backed evaluation.
2. Publish one deterministic selection seed bound to the source snapshot. Assign
   families before constructing questions and before running either model.
3. Within each model/length/bucket cell, give each evaluation case one primary
   family and zero or more distractor families that no sibling case in that cell
   uses. The same frozen cases may be paired across E2B/E4B or reused at another
   declared length; those comparisons are useful and do not weaken cell-level
   independence.
4. Generate deterministic, answer-only tasks with a single source-backed answer
   span. Use non-model oracles: exact nonce retrieval, table/config lookup,
   bounded pure-code execution, canonical JSON construction, or deterministic
   multiturn state updates.
5. Render and tokenize through the installed model's actual template and
   tokenizer. Export the exact rendered bytes, token IDs, token byte offsets,
   provenance labels, eligibility labels, tokenizer identity, and template
   identity. Do not infer token positions by independently tokenizing substrings.
6. Verify the answer span is byte-identical at its declared byte range in the
   sealed source and occurs uniquely in the rendered prompt. The source byte
   range disambiguates legitimate repeated values in one file. Derive the
   minimal enclosing token interval from the runtime offsets; it must have
   nonzero eligibility labels and lie wholly outside protected prefix and
   recent ranges. Balance case midpoints 25/25/25/25 across compactable
   quartiles in every length/bucket cell.
7. Reach the target length with source-backed distractor passages. Any excerpt
   is selected and sealed before model execution, with truthful source byte
   spans; post-output truncation or case replacement is forbidden.
8. Encode and decode each reference using the same loaded tokenizer. Seal the
   original reference, u32 little-endian token IDs, decoded bytes, tokenizer
   identity, and counts. Decoded bytes must exactly equal the scored reference;
   no implicit Unicode or whitespace normalization may repair a mismatch.
   Runtime quality execution independently repeats this comparison.
9. Assign native/HTTP/stream routes 34/33/33 per 100-case cell, rotating the
   extra case. Exact and attention-matching variants use the same route and
   structured request.
10. Freeze the complete plan and all referenced files before any exact or
    attention-matching output is observed. Evaluation cases never enter
    selection, fitting, calibration, profile construction, or seed search.

## Diagnostic local-repository constructor

`quality_corpus_construct_v1.py` consumes a sealed local source manifest, the
exact referenced bytes, a fixed seed, and a model ID. It never reads inference
outputs, profiles, scores, or thresholds. It deterministically ranks eligible
families and emits a small diagnostic blueprint set. Every blueprint carries
the source path and hash, exact evidence byte range and hash, declared
authorship, non-model oracle, question, reference, intended compactable
quartile, and a deterministic distractor-order seed.

The five diagnostic adapters are deliberately mechanical and explicitly do not
claim comprehensive production bucket coverage:

- `exact_retrieval`: copy one unique UTF-8 source line verbatim;
- `document_qa`: resolve a cited scalar assignment by key;
- `code`: evaluate a cited bounded integer constant expression;
- `structured_tool`: parse a scalar assignment and emit canonical compact JSON;
- `multiturn`: emit a real three-message last-write-wins blueprint using two
  separately cited source lines.

The manifest records `human`, `generated`, or `mixed` as supplied source
provenance. The constructor preserves it and does not infer human authorship.
Selection, fitting, and calibration family lists are rejected if they overlap
the evaluation families before any blueprint is emitted. A practical local
snapshot may use tracked UTF-8 Antfly files, exact workspace byte hashes, and a
conservative `mixed` declaration where authorship is not independently known.

Each question contains the exact source path and half-open byte range; multiturn
blueprints cite both the earlier and replacement ranges. The emitted artifact
sets `diagnostic_only:true` and `production_quality_coverage:false`.

Blueprint creation is intentionally before renderer-aware materialization. The
materializer selects and seals source excerpts and distractor order, renders the
two-part document/question request with the loaded template, and iterates only
on source-backed excerpt lengths until the predeclared context length and
quartile are reached. It then emits the runtime coverage receipt. The strict
preparation checker accepts the case only if the actual runtime offsets yield
the declared minimal evidence-token interval and quartile. No model output may
influence packing, replacement, or selection.

Production construction must replace the diagnostic adapters with predeclared,
bucket-specific case generators and must fail unless each one yields at least
100 distinct families per model/length/bucket cell:

- document QA: schema/heading-to-value relations with the key and value spans
  sealed before execution;
- exact retrieval: exact cited spans with source/distractor collisions checked;
- code: a bounded expression or pure-function grammar evaluated by an
  independent deterministic interpreter;
- structured tool: schema-validated canonical JSON transformations; and
- multiturn: actual ordered request messages and state updates, rather than a
  transcript embedded in a single question string.

The current serving quality request supports a structured document plus one
question. It cannot yet qualify the multiturn bucket. The driver and receipt
schema must preserve an ordered message array and bind its canonical digest
before a 100-case multiturn cell can be constructed or accepted. This is an
explicit production blocker, not a reason to relabel the diagnostic blueprints.

Example invocation after sealing `sources.json` beneath a campaign directory:

```sh
python3 quality_corpus_construct_v1.py \
  --source-manifest /campaign/sources.json --root /campaign \
  --model gemma4-e2b --selection-seed <64-hex> \
  --output /campaign/e2b-blueprints.json
```

## Validation artifacts

`zig/pkg/inference/scripts/gemma4/quality_corpus_prepare_v2.py` is a fail-closed
preparation checker.
It validates:

- exactly 100 independent cases per model/length/bucket cell;
- independent source families within every 100-case model/length/bucket cell;
- hashed selection/fitting/calibration exclusion manifests;
- unique source families, documents, and rendered prompts within each cell;
- recomputed NFC-normalized hashes from the sealed source bytes;
- complete runtime block-coverage receipts binding the structured document and
  question request, actual rendered/tokenized prompt, token offsets,
  tokenizer/template identity, loaded artifact identity, and build provenance;
- source-byte to rendered-byte equality for the answer-bearing span;
- protected-range exclusion and per-cell compactable-quartile balance;
- balanced serving routes; and
- loaded-tokenizer reference text/token/decode round trips.

`baseline_accuracy_report` consumes every predeclared exact result and reports
counts, rates, and failed case IDs for each cell. It records
`acceptance_threshold: null` and forbids case filtering. Production policy may
decide what capability claim requires, but corpus tooling must not derive or
tune an accuracy floor from observed outputs. Existing paired regression and
teacher-forced NLL limits remain separate release contracts.

The executable exposes `validate` and `baseline-report` subcommands. Both read
bounded strict JSON, reject duplicate keys and symlinked top-level inputs, and
emit compact machine-readable JSON. `validate` checks the complete frozen
campaign; `baseline-report` first validates that campaign and then reports all
predeclared exact results for one model.

The companion tests in `zig/pkg/inference/scripts/gemma4/test_quality_corpus_prepare_v2.py` cover a
valid two-model campaign plus protected-range, reference-round-trip, source
overlap, and baseline-report failures. They use reduced case counts only by
overriding test constants; production constants remain five buckets and 100
cases per cell.

## Irreducible gaps before a real corpus exists

- No source snapshot large enough for every required cell has been frozen.
  Repository revision, file, normalized-content, family, and declared clone
  identifiers make the assignment auditable and reject declared/exact reuse;
  they do not prove semantic or near-clone independence. This is a stated limit,
  not a prerequisite for the authorized local-repository evaluation.
- The staged runtime export now retains the full offset array beside the exact
  generation IDs and rendered bytes, but it is not production evidence until
  that source is compiled and a genuine coverage campaign emits artifacts that
  pass the strict loader. Offline retokenization remains insufficient.
- Selection, fitting, and calibration need sealed source-family manifests. The
  present capture artifacts mostly bind document hashes and query partitions,
  so family-level exclusion cannot yet be reconstructed from existing evidence.
- Deterministic task oracles need independent review. The checker proves that
  declared evidence bytes occur uniquely at the declared source and prompt
  positions and that reference text round-trips through the tokenizer. It does
  not prove that a reference is correct or uniquely implied by the source.
- The corpus has no real source cases yet, and no exact or compacted model output
  has been run. The checker proves preparation invariants only.
- Exact-string tasks qualify the stated retrieval/transformation behavior. A
  separate frozen corpus and independently validated scorer are required for
  open-ended semantic quality.
