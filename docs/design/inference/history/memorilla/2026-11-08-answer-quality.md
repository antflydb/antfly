# Memorilla document answer quality evaluation

The native Stage3 pipeline completed the frozen document QA diagnostic, but
answered none of its 22 factual questions correctly. Both missing-fact questions
correctly returned `UNKNOWN`. Keep this Memorilla path disabled for factual QA
rollout until it passes an evaluation on the intended workload. Numerical
conformance and serving lifecycle checks remain separate from answer quality.

## Experiment

The [fixture](../../../../../zig/pkg/inference/scripts/memorilla/fixtures/quality_v1.json)
contains fictional document collections and questions with authored answers.
Ten pairs change a supporting fact while preserving the question and
distractors. Another pair reverses the same documents, and two auxiliary cases
omit the requested fact. The factual cases include direct lookup, two-hop and
three-hop relations, exact codes and competing entities. Fictional facts reduce
the usefulness of pretraining knowledge as a shortcut.

The fixture and [scorer](../../../../../zig/pkg/inference/scripts/memorilla/quality_cases.py)
were frozen before inspecting model answers. The scorer accepts exact answers
and a finite set of affirmative entity-answer forms, and vetoes forbidden facts,
negation and uncertainty. Gold-entity presence alone does not establish
correctness. An independent semantic review found no correct factual
paraphrases hidden by the conservative formatting rules: all 22 factual answers
were substantively wrong.

The [runner](../../../../../zig/pkg/inference/scripts/memorilla/quality_eval.py)
submitted 53 unique raw documents through the qualified native encoder in
batches of at most four, persisted the returned features, then read them back
for the supplied-feature API. Each question received three or four documents;
53 is the total unique text count across the suite. It generated 24 answers with
a 48-token ceiling and repeated the first request after all other cases. Gold
answers and evidence labels never entered embedding or generation requests. Every response was
checked for model and module identities, document count, memory-slot count,
token bounds and usage. No answer reached the generation ceiling.

Execution used the pinned Qwen3-Embedding-4B, released Stage3 reader and
Qwen3-8B composition with F32 arithmetic and F32 KV on NVIDIA GB10 (SM121).
The supervised binary was the same source-bound build as the
[production serving qualification](2026-11-07-production-serving.md):
`a70e25d84d1e3b66a4fdcd44424bbc7fc83ecd4868b151f1715822290375a10a`.
All 60 recorded runtime source hashes remained unchanged.

## Stage3 results

| Diagnostic | Correct | Cases |
| --- | ---: | ---: |
| Direct factual lookup | 0 | 10 |
| Two-hop factual questions | 0 | 8 |
| Three-hop factual questions | 0 | 2 |
| Same documents in reverse order | 0 | 2 |
| All factual cases | 0 | 22 |
| Missing-fact abstention | 2 | 2 |

None of the ten counterfactual pairs had both worlds answered correctly.
Changing the answer to another unsupported name or number does not count as
document grounding. The reordered-document pair returned the same unsupported
name twice, which establishes consistency for that request but not correctness.

Examples of substantive failures:

| Question | Expected in worlds A and B | Stage3 answers in worlds A and B |
| --- | --- | --- |
| Owner of the copper badge at Neral station | Tovik; Mivra | Neral; Neral |
| Unlock code for the Kelmor vault | 7428; 5931 | 7890; 3579 |
| Color of the Selnix indicator | cobalt; amber | Green; Green |
| Courier carrying the crate containing the signal lantern | Veyra; Lethan | Nikolai; Kovak |

The semantic review classified 14 factual responses as unsupported entity
names, two as unsupported numeric codes, four as another item's distractor
fact and two as the wrong entity type. The first and final requests had
identical request and raw response digests. The complete evaluation had no
HTTP or runtime failure and no truncated answers; these failures are answer
quality failures.

## Encoder and checkpoint controls

An independent Transformers eager CPU/F32 check re-encoded the first four raw
documents from the persisted native feature store. All four vectors passed the
unchanged encoder gates (`atol=1e-4`, cosine at least `0.99999`). Their maximum
absolute error was `3.22238e-7`, and minimum cosine was `0.999999702`.
These four inputs all had 16 active tokens including EOS; this adds an actual
ingestion check for that batch rather than new coverage of ragged lengths.

The two counterfactual owner documents produced distinct vectors: their cosine
was approximately `0.866037` in both native and CPU results. This rules out
identical stored features for that pair; it does not prove that the embedding
retains every fact needed for decoding.

A small Stage2 diagnostic reused the same stored features for
`copper_badge_a/b` and `lantern_courier_a/b`. It answered none of those four
questions correctly. This was an exploratory comparison on four fixed cases,
not a full-suite Stage2 score. Its first request also reproduced the same raw
response after the intervening collections.

## Native text controls

The [baseline runner](../../../../../zig/pkg/inference/scripts/memorilla/quality_baseline.py)
uses the same four badge-owner and lantern-courier cases for full-text and
closed-book controls. Literal documents replace the memory slots in the system
turn for the full-text condition; closed-book omits the documents. The system
instruction and user question remain fixed. These are controlled diagnostic
prompts, not upstream benchmark prompts.

Each condition starts a fresh native Qwen3-8B process with the same pinned
decoder weights, configured F32 profile, F32 KV, greedy decoding and 48-token
ceiling. Exact Hugging Face/native prompt token IDs, printed output IDs, native
finish reasons, `32,762,941,440` resident F32 weight bytes and available CUDA
counters are checked. The ordinary CLI omits the complete resident dtype inventory and
capture-gate state, so these controls do not extend the managed serving
qualification.

On the four selected worlds, Stage3 and Stage2 each scored 0/4 with latent
memory. Literal full-text generation scored 4/4. Both counterfactual pairs
followed the changed document fact, including the two-hop lantern/crate/courier
relation. This establishes that the same decoder can answer these selected
questions when given their text; it does not establish full-text accuracy on
the other 18 factual cases.

| Same four selected case worlds | Correct | Cases |
| --- | ---: | ---: |
| Native Stage3 latent memory | 0 | 4 |
| Native Stage2 latent memory | 0 | 4 |
| Native full text | 4 | 4 |
| Native closed-book | 0 | 4 |

All eight ordinary native control processes completed successfully, with exact
HF/native prompt IDs, checked output IDs and unchanged source assets. Each
counterfactual question is identical across its two worlds, so closed-book sees
the same input twice. It abstained on both badge-owner cases and guessed
`Tommy` on both lantern-courier cases. Its 0/4 factual score is expected without
the distinguishing documents; the guesses also show why abstention needs its
own workload metric.

The native timing contract can contain `token_ids: null` and a `stop` finish
reason. The evaluator requires separately printed token IDs and matching
counts; a non-null timing ID list must agree. The first native control's correct
answer was preserved when an initial evaluator assumption about that metadata
failed. Explicit resume revalidated the unchanged model, prompts, arguments and
artifact hashes before reusing that result. The failed summary and source
version remain in the evidence directory.

## Prompt compatibility

The qualified native API currently uses
`Answer the question using the provided memory.` as its system instruction.
The pinned upstream `MemoryVLLM.answer` defaults to
`You are a helpful assistant.` through `DEFAULT_SYSTEM_PROMPT` in
`memorilla/data.py`. Upstream benchmark evaluation additionally selects
task-specific prompts and suffixes. The earlier conditional numerical oracle
used the native instruction, so its passing comparison establishes arithmetic
agreement for that prompt rather than equivalence with the upstream default.

An independent CPU/F32 experiment tested the two badge-owner worlds with both
instructions. It reused the native stored document vectors and held one
CPU-encoded question vector fixed across the four runs. The released reader's
forward matched the independent reader trace bit for bit, and the original
Qwen3-8B vocabulary, F32 KV, greedy decoding and disabled thinking were retained.
Both instructions returned `Neral` in both worlds: 0/2 factual answers correct
for each instruction. All four generations ended on EOS.

The question vector was independently computed on CPU rather than captured from
the native request. This is a controlled prompt comparison within the F32
reference, not a conditional native-query logit comparison. It reproduces the
same wrong answer and shows that replacing this system instruction alone did
not repair these two cases. Aligning the prompt for upstream compatibility is
still appropriate, but should not be presented as an established quality fix.

These results evaluate a bounded F32 serving recipe. They do not reproduce
published Memorilla scores or establish performance under upstream mixed/BF16
inference, other prompts or other workloads.

## Next quality gates

Keep this API opt-in while testing a representative frozen corpus. Include
entity bindings, exact values, multiple supporting documents, distractors and
unanswerable questions. Compare latent memory with full text, retrieval and
closed-book controls using the same held-out questions and explicit source
coverage. Agree on the primary metric and acceptable error rate before viewing
the next experiment's outputs.

Reproduce the released mixed/BF16 inference recipe as a separate reference
experiment before deciding whether the gap calls for precision work or new
adapter training. The current result concerns the qualified F32 profile; it
does not establish the released BF16 pipeline's score. Retain the existing
failed F16 KV and mixed-precision numerical gates without relaxing them.

Use grounded text for these factual cases while the evaluated memory recipe
fails. A new Qwen or Gemma memory module should earn its deployment scope through
the same workload evaluation rather than inherit it from architecture
compatibility or speed.

## Evidence

The fixture digest is
`8e0b90709bca8893e0d5d5a7b8be381f280edaed5200c8296e277e4be4067630`;
the frozen scorer digest is
`c3499c3645d5a42dacdf43c4131e6680a65fa22287016de18a5e5bc6b91cb80b`.
The worktree's ignored `.benchmark-results/memorilla/quality-validation.json`
binds the binary, runtime sources, experiment sources, inputs and final receipts
with SHA256. Supporting paths are relative to `.benchmark-results/memorilla/`:

| Evidence | Receipt |
| --- | --- |
| Raw-document Stage3 answers and complete scores | `quality-e2e/stage3-primary.json` |
| Independent semantic error review | `quality-e2e/stage3-primary-semantic-review.json` |
| Stage2 subset and stored-feature reuse | `quality-e2e/stage2-diagnostic.json` |
| Native full-text and closed-book controls | `quality-native-baseline-v1/summary.json` |
| Exact native request and response replay digests | `quality-e2e/http-observations.json` |
| Independent CPU encoder comparison | `quality-e2e/encoder-check/receipt.json` |
| CPU prompt comparison and full logits | `quality-e2e/prompt-ablation/result.json` |
| Shared inputs and reproduced native outputs | `quality-e2e/prompt-ablation/causal-integrity.json` |
| Evaluation tools, scoring and native metadata regression checks | `quality-evaluation-final-unit-tests.log` |
| Process, source, asset and cache cleanup checks | `quality-e2e/final-cleanup.json` |

All 95 evaluation-tool tests passed. The model processes and supervised workers
were stopped, both loopback listeners closed, and task-specific CUDA caches
removed. The final audit verified the same binary and all 60 runtime source
hashes. Model assets and experiment evidence were retained, and the repository
cache collector completed successfully. No runtime code, model weights,
precision gates or frozen scoring rules changed during this evaluation.

## Reproduction and interpretation

Run the diagnostic against an already-running supervised server with the
qualified encoder and Stage3 profile. The server must allow 48 generated tokens
and the complete framed question input:

```sh
.venv/bin/python zig/pkg/inference/scripts/memorilla/quality_eval.py \
  --base-url http://127.0.0.1:18091 \
  --profile memorilla-stage3 --encoder encoder \
  --output .benchmark-results/memorilla/quality-e2e/new-stage3.json
```

The adjacent feature store preserves raw documents and their native embeddings.
The receipt preserves answers, scores, token IDs, identities, truncation counts
and request/response digests. Failed or missing cases retain their denominator;
partial runs cannot report accuracy only over successful responses.

This small synthetic diagnostic stresses arbitrary names, short relations and
exact values. It does not estimate performance on personalization, long natural
documents or the published datasets. Those workloads require separate frozen
datasets and metrics. The current outcome is sufficient to withhold factual QA
rollout, while leaving the earlier numerical and lifecycle evidence intact.
