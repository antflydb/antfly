# AL: main integration and attention paging ownership

Fetched main: `233fc832c1`. Merge: `ae4ae63ea727d9a14b99bae2801f066f2d4d788d`.
The completed AK CUDA qualification was recorded first in `cafad6c23d`.

## Main integration

The merge retains main's new fast paths while adapting them to the existing
shared owners:

- The generic embedded CUDA program owns all seven paged-attention exports,
  including graph advance and both grouped variants. Main's grouped-CTA
  eligibility and launch geometry remain intact.
- Learned vision positions join the existing typed vision export catalog.
- The recurrent owner retains its optional BF16 prefill library across lazy
  primary-program initialization and releases it even when primary loading
  never completed. Main's packed state and borrowed-weight ownership remain.
- BF16 vision bias fusion writes directly into the caller-owned output. The
  allocating and prepared paths retain one output owner and the same eligible
  GEMM epilogue; unsupported cases use the existing ordinary fallback.
- Main's graph-held KV-lock append operation lives in the shared decode-state
  runtime. Generation continues using the existing runtime aliases rather
  than restoring a duplicate state implementation.

## Attention source follow-up

Relative to the merge commit, three identical graph-progress/live-page guards
in the incoming attention source share one lexical recipe. Suffix writes,
ordinary attention and grouped attention keep their distinct schedules,
reductions, scratch arrays and storage expressions.

This removes **329 maintained CUDA source bytes / four lines**. It is a small
consolidation of a critical paging contract, not another broad reduction.
The complete CUDA 13.0 preprocessed token streams match, and both original and
consolidated sources rebuild the entire checked-in attention PTX byte for byte
with `-arch=compute_75 -O3 --fmad=false -std=c++17`.

PTX SHA256:
`f8bf0ecb66a9cd07545c143c06aecb2cab852ee0b7d24073cdf4237e99cbb482`.
No shipped image changes or new device instructions are required.

## Validation

- Both Metal and CUDA backends compile after conflict resolution.
- Focused host tests: **168 passed / 75 skipped**, with GPU execution disabled.
- The same focused suite compiles with both GPU backends enabled.
- Generated-source freshness: **70 files + 129 native owned regions**.
- The production Metal projection checker passes all numerical, producer/
  consumer, copy, canary, fallback and cancellation checks after the merge.
- A six-test Linux ARM64 CUDA binary compiles for the merged attention,
  recurrent-state, cuDNN vision, fused QKV, compact norm and BF16 bias paths.
  The isolated device run selected those six cases plus 38 imported smoke
  tests: **43 passed / one failed / zero skipped / zero leaked**. The failure
  is recurrent-state recycling's expected resident weight pointer equality.
  The original collector also incorrectly assumed six total tests, ignoring
  imported smoke tests. The failed receipt and binary remain retained.
  [AM](unified-execution-am-outcomes.md) fixes the ownership guard and validates
  each named case explicitly, independently of imported test counts.

The new source recipe's compiled PTX is unchanged, so its device-code
performance is unchanged. No timing claim is made for the host integration
regression suite.

Evidence: `.benchmark-assets/1166/al-campaign/`, including conflict snapshots,
merge test logs, the retained Linux test binary, compiler/source pins and
`attention-verified.json`. The device controller is isolated under
`/home/ajroetker/antfly/.worktrees/1166-al-merge-validation` and waits for a
quiet GPU before running; it changes no other workload.
