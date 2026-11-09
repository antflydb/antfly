# Released latent-only QA diagnostic

The exact operational-fact failures are reproduced by the Python CUDA reference,
including identical native/Python F32 output tokens for all 24 diagnostic
questions. This points to a limitation of the released reader on this workload,
rather than a native execution discrepancy. It does not prove that every
possible shared recipe error has been excluded, or that latent-only Memorilla
fails on all QA workloads.

The Python reference executes the hash-pinned upstream `MemoryModule` with the
released encoder and decoder through Transformers CUDA. It does **not** execute
the original vLLM engine. The separate BF16/upstream-prompt operational control
also fails all 22 factual questions. See the
[operational qualification report](2026-11-09-latent-operational-qualification.md)
for the frozen questions, arithmetic policies and candidate-training results.

The authors explicitly discuss loss of fine factual details and exact surface
forms in their [paper's analysis and limitations](https://arxiv.org/html/2610.21375v3).
That is consistent with these results; it is not proof of the cause of any
individual error.

## Published-data checks

The additional evidence lives in
`.benchmark-results/memorilla/latent-repair-20261109/`. These are bounded
development diagnostics, not reproductions of the full published benchmark or
production qualification. All use released Stage3, Qwen3-Embedding-4B and
Qwen3-8B on GB10, BF16 CUDA eager attention, greedy decoding, a 48-token output
limit, and the task-specific upstream system instruction and question suffix.
The decoder vocabulary remains at its original 151936 rows. Document embeddings
are computed from source texts, without answer labels entering model inputs.

Dataset revision is `c6b0d7eea65659254115ed31dd26869eb6dc0586`.
The existing dataset lock records SHA256-verified immutable source parquet files.
The original sample consists of the first 32 rows satisfying the 1–64 document
bound. **All 32 selected FactKG labels are True.** The reader answers 29/32
correctly on that sample, but this selection is unsuitable for assessing
representative classification performance. The result is retained with that
limitation rather than treated as a successful qualification.

A prospective FactKG sample uses `random.Random(602337).sample(eligible, 32)`, sorted
by original row index. Eligibility depends only on document count. Selection
never depends on the answer. Its source SHA256 is
`91d3570737d003205e36447f9123d48f59742a045cd0c76f0d389f466fb0a428`;
the selected sample has 14 True and 18 False labels. It was generated but not
executed in this diagnostic; no accuracy claim is made for it.

The TriviaQA sample scores **20/32 normalized exact matches**, with a warm
generation median of 0.429 seconds. This confirms that the Python latent-only
path produces correct answers on some natural QA cases while failing the
unfamiliar operational-name/code diagnostic. Its BF16 timing and task differ
from the qualified native F32 latency workload; they are not a speed comparison.
The scorer is case/punctuation/article normalized exact match against the
released single reference answer. It does not relax formatting for alternative
wording or enumerate additional answer aliases.

The completed raw receipts are `factkg32-bf16/result.json` and
`triviaqa32-bf16/result.json`; each preserves answers, selected token IDs,
finish reasons, prompt construction, model identities and archived runner code.
No result from these samples substitutes for the unchanged exact operational
development gate. Natural-question correctness alone also does not establish
that an answer depended on memory rather than the decoder's prior knowledge.

## Runtime preservation

All 83 files in the qualified runtime source snapshot remain unchanged. Snapshot
SHA256 is `2894c69dc65f2d33147f5337b71bf860e55528044f426cec25c31c3a4f087b40`.
The existing native binary remains
`a38c3bc05319821d612f069fa812270e904ed5d63430f3e0b28127be4a014378`.
This diagnostic does not change runtime code, checkpoints, arithmetic gates or
the serving allowlist. The operational final 192 cases remain sealed, and no
candidate is promoted.
