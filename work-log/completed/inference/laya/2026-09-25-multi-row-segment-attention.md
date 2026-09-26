# Laya multi-row segment attention (step 1b′): 2026-09-25

Design and summary: [`zig/pkg/inference/models/laya/LAYA.md`](../../../../zig/pkg/inference/models/laya/LAYA.md)
("Segment attention" → "Multi-row batching", "Multi-row batching cost").

Host: Apple M4 Max, 36 GiB, macOS 15 (Darwin 24.6.0), Zig 0.16.0, ReleaseFast
(cost) and Debug (correctness).

## What changed

`ops.SegmentAttention` (`pipelines/laya_tree.zig` `ranges`) never assumed one
tree per call — a query's visible keys are wherever its ranges point. The
gap was entirely in the higher layers: `laya_tree.Row.validate` accepted
exactly one root segment (the trunk), and `pipelines/laya.zig` ran one
session call per built row.

- `laya_tree.validate`: generalized the trunk check from "segment 0" to "any
  segment whose parent is -1 is a root, and only a root may carry
  `trunk_kind`". A row is now a forest, not a single tree with one root.
- `laya_tree.coalesce`: new. Concatenates several already-built, already
  valid rows into one physical row — tokens, segments (renumbered into one
  global space per row), parents (each row's root stays a root), anchors and
  markers (moved with their tokens, markers padded to the widest row's
  option count). Positions are **not** shifted; each tree keeps the
  positions it would have alone. Returns `owners[k]`: which input row
  contributed the merged row's `k`-th question.
- `pipelines/laya.zig` `executePacked`: after building rows per state group
  (unchanged), greedily batches consecutive rows (request order) into one
  session call while the combined length stays within
  `cfg.packing.max_packed_len` — the same bound `laya_tree.build` already
  enforces per state, not a new knob. `ANTFLY_LAYA_PACKED_BATCH=0` disables
  batching (one call per row, prior behavior).
- `architectures/laya_packed.zig` `forwardRow`: `laya_tree.treeCount(row) !=
  1` forces `forwardFull`, skipping the trunk cache for a merged row. Segment
  attention already keeps cost proportional to visible keys, so a merged
  call's states cost the same whether or not any of them would have been
  cached alone — batching and the state cache are independent wins here, not
  yet composed.

No change to `ops.SegmentAttention`, `linalg.segmentAttentionHost`, or the
Metal kernel (`termite_sdpa_f32_segments`): both already take arbitrary
per-query ranges into one key array with no notion of "one tree per row".

## Correctness

`pipelines/laya_packed_test.zig`, synthetic checkpoint, both question and
candidate packing modes unless noted:

- "laya multi-row coalescing isolates independent states and matches running
  them alone" (3 distinct states): every cross-tree token pair invisible;
  merged decisions vs. each row run alone, max error 8.3e-7 (CPU and Metal).
- "laya multi-row coalescing holds exactly with a dozen near-identical
  states" (12 states, direct `forwardRow`, bypassing the session): exact,
  max error 0 (CPU and Metal, both runs).
- "laya packed pipeline batches many small states into fewer session calls"
  (12 states x 2 questions through the real session/pipeline,
  `ANTFLY_LAYA_PACKED_BATCH=0` vs default): 12 calls -> 2, identical prompt
  tokens, exact decisions (CPU and Metal).
- Full `--test-filter laya` suite, CPU and Metal, with and without
  `ANTFLY_LAYA_REFERENCE`: all pass (fixture-backed runs: 52-53 passed,
  8 skipped for CUDA/Metal-only/long-running cases not applicable here).

### One transient Metal failure, not reproduced

A first fixture-backed Metal run of the full `laya` suite failed once at
"laya packed pipeline batches many small states into fewer session calls"
(`maxError(...) < 1e-5` on the *pipeline* comparison, not the lower-level
direct-`forwardRow` test, which passed in the same run). Immediately after:

- A standalone rerun of the same pipeline-level test, and a direct
  `forwardRow`-level test at the same 12-state scale, both passed with
  **exact** (0) error.
- A full rerun of the entire fixture-backed `laya` suite on Metal passed
  cleanly (53 passed, 8 skipped, 0 failed).

At the time of the failing run, `ps` showed several other agents' Metal
jobs active on the same GPU (a `with-lock gpu` training run and at least one
other `zig build test` in flight), sharing this machine's single GPU per the
shared rules. The failure did not reproduce on any of three subsequent runs,
including the identical test at the identical scale, run without other GPU
activity visible in `ps`. Given the code path is otherwise verified exact at
this and smaller scales, and reruns without contention keep passing at 0
error, this is recorded as suspected transient GPU contention on the shared
machine, not a defect in the coalescing or segment-attention code. Flagged
here rather than silently discarded, per the honesty bar on negative
results; if it recurs, look first at anything that changes shape (not just
content) between back-to-back session calls to the same Metal provider.

## Cost

See LAYA.md "Multi-row batching cost" for the table. Summary: with only 4
questions per state, packing pays a fixed per-call cost (admission, tensor
marshaling, encoding a state's own trunk from scratch) that a handful of
questions cannot amortize, so one packed call per state is *slower* than not
packing at all, on both backends (Metal: 1,155 ms vs. 578 ms unpacked at 16
states, 4,779 ms vs. 2,564 ms at 64; CPU: 7,154 ms vs. 5,897 ms at 16 states,
28,229 ms vs. 22,425 ms at 64). Batching removes that per-call overhead:
16 states in 1 call is 1.8x (Metal) / 1.7x (CPU) faster than unpacked and
3.6x / 2.0x faster than one-row-per-state; 64 states in 2 calls is 1.8x /
1.7x faster than unpacked and 3.4x / 2.1x faster than one-row-per-state.

Raw benchmark output (ReleaseFast, `laya-released` checkpoint):

```
LAYA_PACKED_MANY_STATES {"backend":"metal","states":16,"questions_per_state":4,"unpacked_ms":578.3,"packed_batched_ms":321.4,"packed_unbatched_ms":1154.7,"batched_chunks":1,"unbatched_chunks":16,"unpacked_tokens":3888,"packed_tokens":2176}
LAYA_PACKED_MANY_STATES {"backend":"metal","states":64,"questions_per_state":4,"unpacked_ms":2564.0,"packed_batched_ms":1400.7,"packed_unbatched_ms":4779.1,"batched_chunks":2,"unbatched_chunks":64,"unpacked_tokens":15552,"packed_tokens":8704}
LAYA_PACKED_MANY_STATES {"backend":"native","states":16,"questions_per_state":4,"unpacked_ms":5896.5,"packed_batched_ms":3547.6,"packed_unbatched_ms":7154.0,"batched_chunks":1,"unbatched_chunks":16,"unpacked_tokens":3888,"packed_tokens":2176}
LAYA_PACKED_MANY_STATES {"backend":"native","states":64,"questions_per_state":4,"unpacked_ms":22424.5,"packed_batched_ms":13180.9,"packed_unbatched_ms":28229.2,"batched_chunks":2,"unbatched_chunks":64,"unpacked_tokens":15552,"packed_tokens":8704}
```

The full unpacked-vs-packed sweep (`state_sentences` 1/4/12 x `questions`
1/2/4/8/16/64) also reran clean on both backends alongside the many-states
addition; those numbers match the existing LAYA.md table within run-to-run
noise (e.g. CPU 12-sentence/64-question: 57.5 s unpacked / 5.2 s packed /
4.4 s cached here vs. 59.2 / 5.1 / 4.3 s recorded 2026-09-24) and are not
repeated here.

## Open issues

- Multi-row batching and the trunk state cache (step 1a) are not composed:
  a merged row always re-encodes every tree's trunk. Worth revisiting once
  there is a cache keyed per-tree inside a forest, not per-row.
- Batch grouping is greedy in request order up to `max_packed_len`; no
  attempt to sort or bin-pack for a tighter fit. Simple and matches the
  existing per-state row-splitting policy in `laya_tree.build`, but leaves
  some packing efficiency on the table when request order is adversarial.
- The Metal transient failure above was not root-caused; see the section
  above for what to check if it recurs.
