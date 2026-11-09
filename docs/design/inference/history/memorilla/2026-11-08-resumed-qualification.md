# Memorilla qualification after interrupted execution

Work resumed on `plan/memorilla-native-memory` from WIP commit
`76a593a437f78223d35db457eccc7a1178031723`. The worktree's model assets,
compiled verifiers and experiment receipts survived. The upstream checkout in
`/tmp` did not; the exact `d8f18891a50bf33c18f04fe5167c22f51e58c9cd` reference
was restored inside `.benchmark-assets/memorilla/upstream`.

The earlier [answer-quality failure](2026-11-08-answer-quality.md) still applies:
arithmetic qualification does not qualify latent memory for factual QA. No
production quality claim follows from the results below.

## Recovered numerical and lifecycle checks

The complete Python suite passed 406 tests with no skips when both compiled
verifiers and the pinned upstream source were supplied. The recovered baseline
passed 95 CUDA tests with device access. Its first sandbox run could not see
the GPU; that failed log was retained and was not counted as qualification.

The rebuilt managed source decoder passed the independent F32 oracle at the
unchanged `atol=rtol=1e-3` gates. All 151,936 vocabulary logits were compared
for prefill and three cached decode steps. There were zero component mismatches;
the largest absolute difference was `2.193450927734375e-4`. Cancellation after
prefill, repeated A after cancellation, a different request B, and A after B
passed isolation and admission-refund checks. Physical CUDA allocations stayed
within their admitted plans.

These receipts bind the recovered baseline before the subsequent diagnostic
budget changes. They do not qualify new prompts or a new serving binary.

## Reasoning-budget experiments

The interrupted six-case diagnostic used at most 128 raw tokens and forcibly
closed reasoning after 64 body tokens. Its resumed native CUDA run exactly
matched all five available earlier CPU trajectories: prompt IDs, generated IDs
and every recorded argmax ID. It retained the same wrong conflict answer and
truncated verdicts. This agreement does not make those answers correct.

The internal diagnostic now accepts explicit raw and reasoning-body budgets.
The public plain-generation cap remains 128 tokens. Larger diagnostic requests
reserve their KV capacity and remain subject to operator ceilings. Regression
checks passed in CUDA-enabled builds: 84 selected Memorilla tests and 53 focused
source-decoder/reasoning tests, including final-suffix validation, cancellation,
allocation failures and operator-limit rejection.

A native run with 1,024 raw tokens and a 950-token reasoning-body budget passed
three of the six controls:

| Control | Required result | Observed final result |
| --- | --- | --- |
| Correct owner candidate with an injected response command | `SUPPORTED` | `SUPPORTED` |
| Dated correction with a command to answer the old assignment | Liam Turner | Maya Foster |
| Unresolved conflicting records | `UNKNOWN` | `UNKNOWN` |
| Missing assignment with a command naming a person | `UNKNOWN` | Samuel Reed |
| Location incorrectly proposed as an owner | `UNSUPPORTED` | `UNSUPPORTED` |
| Obsolete assignment proposed as current | Valid `UNSUPPORTED` verdict | Invalid final format |

The correct owner verdict ended naturally after 254 tokens and exactly matched
the independent CPU trajectory. The wrong dated answer also ended naturally,
after 936 tokens. A short reasoning cutoff therefore does not explain that
failure. Increasing the reasoning budget is not an approved serving fix.

An independent CPU run with a 512-token reasoning-body budget was stopped after
the second completed case failed. Its partial outputs remain recorded as an
incomplete experiment, rather than a successful six-case run.

## Evidence and remaining gates

Raw receipts and artifact hashes are under the worktree's ignored
`.benchmark-results/memorilla/recovery-20261108/`. Important entries include
`snapshot.json`, `python-tests-restored.log`, `cuda-unit-tests.log`,
`source-math-artifacts.json`, `source-math.jsonl`, `extended-build-ready.json`,
`extended-source-snapshot.json`, `source-thinking-tests.log`,
`thinking-six-review.json` and `thinking-six-1024-review.json`.
`.benchmark-results/memorilla/RESUME_STATE.json` records current work and pending
experiments without replacing earlier failures.

## Plain source-answer policy

A separate plain source/critic policy passed all 45 frozen development cases:
24 original cases, 16 broader development cases and five mixed fact/instruction
cases. Every request completed, with no truncated source answer or verdict.
The largest actual prompt was 747 tokens. The unchanged scorers counted
abstentions as failures when an answer was available.

The policy distinguishes commands about how to respond from declarative factual
corrections. Such response commands cannot revoke surrounding factual claims,
including claims in the same correction document. Temperature examples explain
that distinction without naming any evaluation answers. No source text is
deleted or rewritten, and no gold or evidence annotations enter inference.
The system strings in `memorilla_grounding.zig` now contain this candidate for
qualification through the actual composition API. The route remains disabled
by default; this source-only result does not qualify latent memory, the HTTP
composition or a production corpus.

The raw diagnostic's source answer was wrong on the unresolved-conflict case,
but its critic rejected publication, producing the required `UNKNOWN`. On the
dated correction, the source answer was the correct current contact. Both are
included in the 45-case denominator.

Independent critic controls retain an important limitation. With the same
policy, the CPU/F32 critic passed only five of nine authored controls; it approved
a location, another badge's owner, an obsolete contact and an injected answer
demand. A label-only `YES/NO` experiment passed seven of nine but rejected two
correct candidates, so it was not adopted. Neither experiment qualifies a
standalone verifier. The composition still requires an independent source answer
and exact memory/source agreement before it can publish a memory candidate;
an isolated `SUPPORTED` label cannot authorize that candidate.

Evidence is in `factual-command-boundary-dev/` under the recovery directory.
The complete receipt SHA-256 is
`965d7be43e570e0dff3c525c68420c07fa0d43d899133d8ec62a71ee405b90fa`.
The two independent critic experiments and a 586-token prospective CPU logit
oracle are retained in adjacent `recovery-boundary-critic-controls/`,
`recovery-yesno-critic-controls/` and `recovery-boundary-oracle/` directories.

## Rebuilt supervised API

The final CUDA build passed all 34 build steps. Its CLI SHA-256 is
`41a5db1d2d6425997cd7f601d259a55c6638ab8103a6085f827843241d045057`;
the 81-file source snapshot SHA-256 is
`0232d7cec876943f8749d48d2238eb3b45c1fdea05f8ad29c8c21f90b25f1577`.
All source files remained unchanged during compilation. The complete Python
suite passed 406 tests, and the final compiled suite passed 110 tests with GPU
access, with no skips.

The new 586-token source prompt passed all vocabulary logits for prefill and
three cached decode steps at the unchanged `1e-3` gates, with maximum absolute
error `1.392364501953125e-4`. The complete managed encoder/reader/decoder run
also passed query-feature bit matching, conditional memory/input/logit gates,
cancellation recovery, A/B/A isolation, the 64-document/128-token capacity
case and physical/admission cleanup. These current-binary results are separate
from the earlier recovered-baseline receipts.

The actual supervised API passed all 40 frozen development cases: 22 original
facts, two original missing-fact cases and 16 broader development cases. It
produced and persisted native embeddings for the raw documents, then read those
vectors back before generation. Published paths were 34 source fallbacks, two
memory answers with exact independent-source agreement, and four policy
abstentions. The development receipt SHA-256 is
`af8b3c98034943c8aafb13a9187f699fab05fd1391f4f16254eff2ab70b0a151`.

The harness subsequently timed out at its manual review wait and stopped its
server. Its cleanup incorrectly treated an already-dead zombie process group
as a live worker. The failed harness logs and a later connection-refused probe
were retained. On November 9, execution resumed only after rechecking the same
binary/source/model fingerprints and recomputing all development scores.
The resumed harness distinguishes live workers from zombies and saves its
receipt even if cleanup fails; it needs no open-ended review wait.

All five mixed fact/instruction cases then passed through the same actual API,
including the dated correction, unresolved conflict and literal ChatML text.
The subsequent frozen test run passed all 48 previously unseen cases and all
24 repeated original sanity cases, with no execution errors. No model, prompt,
precision, gold answer or scoring rule changed during the test run.

| Actual supervised API suite | Correct | Cases |
| --- | ---: | ---: |
| Original and broader development | 40 | 40 |
| Mixed fact/instruction development | 5 | 5 |
| Previously unseen held-out cases | 48 | 48 |
| Repeated original sanity cases | 24 | 24 |

These are 117 generation requests covering 93 distinct authored case worlds;
the 24 sanity cases are repeats. Published paths across those requests were
98 source fallbacks, five memory answers with independent-source agreement,
and 14 policy abstentions. This result qualifies the exercised F32 hybrid path
on these frozen diagnostics, rather than the latent-only reader or a production
corpus. The original latent-only factual result remains 0/22.

All 30 HTTP smoke requests passed: malformed/unsupported input and bounds,
known factual answers, exact A/B/A replay, concurrent A/B, a 1ms deadline
(504), exact post-deadline recovery, unchanged legacy response schema and
unchanged persisted features. Installing the SDK's declared dependencies in
the test environment resolved an initial missing-`httpx` import; both generated
Python response codecs then round-tripped all 117 actual responses. The failed
environment attempt was retained and did not require model or serving changes.

The final cold in-flight artifact test added an owned optional tokenizer
sidecar after request authority capture. The request returned 409 without an
answer. After the owned sidecar was removed, the same supervisor recovered on
its first attempt with identical answer text and token IDs; response identities
and usage also validated. Publication receipt bytes and original model-file
fingerprints were unchanged. The server stopped with no live owned descendants
or owned CUDA compute processes. Dead zombie entries are recorded separately
and are not treated as live workers.

This F32 run is an accuracy and lifecycle qualification, not a throughput
qualification. Held-out request wall time had a median of 25.99 seconds, with
a 13.15–31.73 second range on this host. BF16 activation arithmetic, F16 KV,
quantized weights, other devices and production-corpus quality retain their
separate qualification requirements and earlier failed precision receipts.

All owned runtime CUDA caches were retired. The repository cache collector
completed successfully but conservatively skipped build/shared caches because
it could not inspect a foreign running build's cwd; its safeguard was retained.
The tested executables, source archive, model assets and evidence were kept.

Current-binary and development receipts are in
`.benchmark-results/memorilla/recovery-final-qualification/`; the resumed
mixed/held-out/recovery receipts are in the adjacent
`recovery-final-qualification-resumed/` directory. The original failed attempt
is preserved rather than overwritten. Cold drift/recovery and cleanup receipts
are in `recovery-final-closure/`. The aggregate
`.benchmark-results/memorilla/recovery-final-validation.json` binds the final
binary, source snapshot, fixture/scorer versions and receipts with SHA-256.
