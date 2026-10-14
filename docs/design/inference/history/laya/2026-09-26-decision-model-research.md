# Decision-model research for Laya: Jev, Jeeves, and others (2026-09-26 to 2026-10-04)

> Relocated verbatim from `zig/pkg/inference/models/laya/LAYA.md` (lines 861–1019 at commit 3ce5aff4ea) on 2026-10-12. Durable decisions were folded into [LAYA.md](../../../../../zig/pkg/inference/models/laya/LAYA.md) before the move.

### How Jev likely closes this gap (research, 2026-09-26)

TypeSafe has not published Jev's architecture. Everything public, though,
suggests that Jev reaches its accuracy **without** letting the state see the
questions. That is our default packed layout, so the gap above is most likely
about training, not the mask.

- **Isolation is a product promise.** TypeSafe says every question is
  "evaluated in parallel and in isolation against the same state"
  ([launch post](https://typesafe.ai/blog/introducing-system-one-models-and-jev)).
  A question-aware state (`trunk_sees: "questions"`) breaks that promise:
  adding a question can change another question's answer.
- **The reproductions mask the state to itself.** MoJev/Jevre's mask
  ([source](https://github.com/MoLeMo-Lab/mojev),
  [card](https://huggingface.co/di-zhang-fdu/jevre)) allows only
  `state → state` attention inside the state, bidirectional and never toward
  the questions. Its docstring gives the reason: spans that see each other
  lose "independent decisions, exact permutation invariance". The
  [RLCD explainer](https://di-zhang-llm.github.io/blog/what-is-rlcd-the-secret-behind-jev/)
  describes the same visibility.
- **They train at 150-200k rows, in the target layout, from an LLM.**
  - MoJev fine-tunes all of Qwen3.5-0.8B on 205,084 rows for one epoch.
  - [Open-Jev](https://zefan-cai.github.io/open-jev/story/) trains LoRA on
    Qwen 2B-27B over 148,639 rows and reports 85.3% on JevBench (Jev: 86.6%).
  - Causal pretraining never let the state attend forward, so for these
    models a question-blind state is the natural one.
- **Their heads differ from Laya's.**
  - MoJev mean-pools the state, question and candidate spans. It scores each
    candidate by a rank-512 bilinear product of a state-plus-question vector
    and a candidate vector, trained with a Plackett-Luce ranking loss plus a
    Brier calibration loss.
  - Open-Jev puts a scalar head on the last token.
  - Laya uses a two-layer transformer head over option marker tokens.

**Inference (ours).** Laya's released encoder was trained unpacked, with the
state *after* the question and attending to it: the reverse of Jev's order. The
packed fine-tunes ask it to unlearn that dependence in 400-915 cases. More data
helped a little (packed 0.450 → 0.471 going from 400 to 915 cases), but the
reproductions use about 200× more rows.

**Next experiment.** Train the default, question-blind packed layout at scale
on teacher-labelled synthetic cases. This keeps the state cache and question
isolation, and tests the scale hypothesis directly. A MoJev-style pooled
bilinear head is an optional ablation. Teacher throughput sets the cost of this
experiment; see [Teacher throughput](../../../../../zig/pkg/inference/models/laya/LAYA.md#teacher-throughput).

### Lessons from Jeeves (research, 2026-09-29)

[PostHog/jeeves](https://github.com/PostHog/jeeves) is a Jev-like decision
model: Qwen3.5-9B with LoRA (r = 16 on all projections) and a pointer head,
trained with SFT and then reinforcement learning (CISPO) to reason before it
decides. Figures below are the project's own; we have not reproduced them.

| Setting | Accuracy |
| --- | --- |
| Their test split (2,962 items), with reasoning | 0.840 |
| Same split, without reasoning | 0.804 |
| Dev (325 questions), full reasoning (median 3.3 s, p90 17.1 s on one H100) | 0.825 |
| Dev, reasoning only below 0.9 confidence, capped at 768 tokens (median 2.0 s) | 0.806 |
| Dev, no reasoning (about 0.3 s) | 0.775 |
| Test overall, item-weighted, with reasoning (Jev: 0.857) | 0.889 |

What bears on Laya:

- **Their layout is ours, on a causal decoder.** The prompt is `<state> …
  <q> instructions <opt> option </opt> … <decide>`: the state comes first and
  never sees the question.
  - Serving prefills the prefix a state's questions share once and gives
    every question its own copy of that cache (`inference/engine.py`,
    `group`). That is our trunk cache.
  - It works for them because a causal model's state tokens were never
    trained to see what follows them.
  - Our results point the same way. Every layout that keeps the ModernBERT
    state question-blind trails unpacked by about 0.1 or more, even with all
    30 layers fused per question ([Per-question upper layers](../../../../../zig/pkg/inference/models/laya/LAYA.md#scaling-packed-training-on-open-jev-2026-09-27)).
  - Both open reproductions that keep the state question-blind (MoJev and
    Jeeves) start from Qwen decoders.
- **Base model over data volume.** SFT used 19,126 questions (12 public
  datasets plus synthetic policy data), a tenth of MoJev's 205k rows. That
  fits our Open-Jev result, where 33× more data did not move the packed
  encoder.
- **A pointer head.** Each option is scored by a scaled dot product between a
  query projection (256 wide) of the hidden state at `<decide>` and a key
  projection at that option's `</opt>`, then one temperature fitted on dev.
  Laya instead runs a two-layer transformer head over option markers. A
  pointer head is a cheap ablation on our encoder, and it is the natural
  head for a decoder base.
- **Rare tokens as anchors.** `<state>`, `<q>`, `<opt>`, `</opt>` and
  `<decide>` map to unused Qwen tokens (`<|fim_prefix|>`, …). In their
  ablation, plain text such as "State" did worse. Laya inherits upstream's
  plain-text `"choice question: …"` prefix.
- **Confidence-gated escalation.** `nothink_threshold` answers without
  reasoning when the no-reasoning confidence is at least the threshold, and
  reasons otherwise. For Antfly that suggests Laya as the fast path, with low-
  confidence decisions sent to a reasoning model.
- **Smaller points.** They repeat the question after the reasoning block
  (dropping it hurt). They stopped RL at step 402 of 624, because later steps
  over-sharpened the head on a saturated pool. Their shared-prefix prefill is
  the same as our teacher's `--prefill shared`.

**Inference (ours): what this means for Laya and Antenna.**
- A question-blind, cacheable state on a decoder base is the
  best-supported route to Jev-level accuracy with state reuse. For Laya that
  means a small Qwen decision model (0.8B–2B, as MoJev uses) with a pointer
  head, served with the runtime's prefix KV cache.
- Antenna needs an encoder for embeddings, chunking and extraction
  ([ANTENNA.md](../../../../../zig/pkg/inference/models/antenna/ANTENNA.md)), so this does not transfer directly.
  On the encoder, the cheaper open levers were question-first positions, a
  pointer head, and rare-token anchors. The first two do not close the gap,
  and the markers are already special tokens (see
  [Scaling packed training on Open-Jev](../../../../../zig/pkg/inference/models/laya/LAYA.md#scaling-packed-training-on-open-jev-2026-09-27)).
- Antenna's step 8 (a schema-blind trunk with task branches) should expect
  the same accuracy loss unless one of those closes it.

### Other decision models (research, 2026-10-04)

Figures below are each project's own; none has been reproduced here except
where noted.

| Model | Base | Open | How options are scored | State shared across questions? |
| --- | --- | --- | --- | --- |
| [Cloudflare Clef / Clef-flash](https://blog.cloudflare.com/clef-decision-models/) | Qwen 27B / 9B, frozen, LoRA rank 256 | Apache 2.0 | a small transformer head that routes state evidence to each question and scores all options of all questions jointly | no isolation: questions attend to each other |
| [Fastino GLiDE](https://fastino.ai/blog/introducing-glide-the-first-thinking-decision-model) | undisclosed | API only | a fast distribution first, then extra reasoning when the top answer is uncertain | undisclosed |
| [Amazon Strands Decider 2B](https://www.beri.net/article/cloudflare-clef-amazon-strands-decider-open-weight-decision-models-vs-jev-benchmarks-pricing) | Qwen3.5-2B, LoRA rank 16 | Apache 2.0 | a pointer head of about 1M parameters | not stated |
| [OpenDecider-nano](https://huggingface.co/manjunathshiva/opendecider-nano) | Ettin-encoder-400m (ModernBERT architecture), fully fine-tuned | Apache 2.0 (Ettin: MIT) | Laya's scheme: one `[MASK]` per option, an MLP per marker, softmax per question | no: question first, state re-encoded per question |

- **Clef.** Clef-flash runs at a 38.8 ms median against Jev's 524 ms. It wins
  on routing (Banking77 macro-F1 94.2 against 79.7) and loses on judgment
  (When2Call 72.4 against 81.0). Hosted, it costs $0.09 (flash) and $0.24 per
  million input tokens, against Jev's $0.042.
- **GLiDE.** An independent test found a median of about 1 s and tails past
  two minutes, and weaker calibration than Jev: at a 90% confidence cutoff
  it answered 26% of decisions at 84.5% accuracy, against Jev's 70% at
  95.9%.
- **OpenDecider-nano** scores 0.796 on the full typed-decisions test split
  (2,000 decisions), against 0.766 for Laya's typed-decisions checkpoint;
  both were fine-tuned on its train split. Jev's 0.754 is zero-shot. Before
  that fine-tune it was distilled on about 190,000 questions (public
  classification, NLI, reading-comprehension and similar sets, plus
  synthetic business cases). The targets were the averaged distributions of
  Qwen3-235B and DeepSeek V4.1 Flash, each temperature-scaled on held-out
  gold. It took under $30 of GPU time.

**What this means for Laya.**
- **Scale and calibrated teachers, not the base, separate OpenDecider from
  Laya.** It is a 400M ModernBERT-family encoder with Laya's scorer and
  Laya's unpacked layout. Its training set is about 100× our step-0 split,
  and its targets are calibrated teacher distributions. That is the scale
  hypothesis of [How Jev likely closes this gap](#how-jev-likely-closes-this-gap-research-2026-09-26),
  shown for the unpacked layout. Whether scale also closes the packed gap
  is untested; our 64k-row Open-Jev run used rule labels.
- **Isolation is a product choice.** Clef scores questions jointly, so
  `trunk_sees: "questions"` (0.554, packed cost) is a defensible mode.
- **Decoders cost more.** Clef and Strands Decider show the decoder route
  works, but at 2–6× Jev's price per token. An open decoder baseline already
  exists (Strands Decider 2B), so building one is not a priority.
- **Confidence-gated escalation** (GLiDE, Jeeves) fits Laya as a fast path in
  front of a reasoning model, with a hard latency budget.
