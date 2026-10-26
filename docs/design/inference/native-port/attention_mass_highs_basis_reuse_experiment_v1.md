# Persistent HiGHS basis experiment: bounded design

This is an optional training-only experiment. It does not alter the reviewed cold solver, the constraint-generation policy, or canonical inference code. It carries no equivalence, performance, serving, or production claim.

## Concrete row-layout constraint

The frozen LP builder emits `2N` rows as every selected upper constraint followed by every selected lower constraint. The controller stores selection in a boolean vector and rebuilds selected indices in ascending full-row order each round. When a newly selected full row sorts before an existing row, the next cold model moves existing upper rows and moves the entire lower block.

HiGHS v1.8.0 `Highs_addRows` appends rows; it cannot insert into that canonical ordering. An append-only persistent model therefore has physical order:

1. initial uppers;
2. initial lowers;
3. round-1 new uppers;
4. round-1 new lowers;
5. later round pairs in the same pattern.

The prototype records `(full_row, sign)` for every physical row and scatters `alpha=-upper_dual`, `beta=-lower_dual` through that identity. The mapping prevents a dual-sign or row-identity bug, but it does not make the warm solve algorithmically identical to rebuilding the cold canonical matrix.

## Two possible experiments

**Append-only persistent model** is the highest-value performance candidate. Create and configure one HiGHS handle, pass the initial canonical restricted LP, solve it, append exactly the next round's upper rows followed by lower rows, and solve again. Existing physical row indices stay stable. Record the basis before and after each append, but initially let HiGHS extend its own basis rather than guessing statuses for new rows. Cancellation, the absolute deadline, fresh `1e-10` retry handle, witness checks, and destruction remain mandatory.

**Canonical rebuild with remapped basis** reconstructs the next cold row order and maps prior row statuses by `(full_row, sign)`. New constraints still need basis statuses that form a valid basis, and `Highs_setBasis` changes initialization. This is less likely to save assembly time and does not guarantee the cold pivot path. It should not be the first experiment.

Either strategy can change simplex pivots, iterations, duals, raw weights, pricing magnitudes, selected rows, and later constraints while reaching the same optimum. Exact cold trajectory may therefore be unattainable with a warm basis. Any such difference makes the candidate a new training recipe requiring separate full numerical and quality qualification. Objective equality alone is never acceptance.

## Required candidate interface and fallback

- New explicit identity such as `mass-cg-highs-v180-persistent-basis-v1`.
- The current cold `create/passModel/run/getSolution/destroy` solver remains byte-identical and callable for every round.
- The persistent handle owns all model, callback, basis, and row-map state. One `deinit` destroys it on every terminal, cancellation, timeout, retry, allocation failure, or fallback branch.
- The `1e-10` witness retry always uses a fresh cold handle, matching the frozen rule; it must not inherit the rejected basis.
- Before and after each HiGHS call: check cancellation and the same absolute deadline. Callback interruption and post-solution deadline rejection remain unchanged.
- On any API/status/mapping/witness failure, discard every persistent result and rerun the whole seed through the cold path from its original input. Do not resume from a candidate checkpoint.

## First bounded diagnostic

Use one frozen non-held original-seed `owner14/block2048` matrix. Run cold and persistent candidates in separate guarded processes under 1024 MiB RSS, zero swap growth, 560 seconds internal and 600 seconds external. Record:

- exact per-round selected-before rows, added row IDs/order, retry branch, subset size, iterations, cumulative iterations, objective, `t`, and full violation;
- physical `(full_row, sign)` map and basis-status hashes before/after every append;
- raw weights, row duals mapped to full alpha/beta, original-coordinate witness, stored f32 bias, stored weights and mass;
- HiGHS calls and time, total elapsed, peak RSS, cancellation/deadline events, creates/destroys, and drain status.

For an equivalence result, require the existing exact branch/row/terminal-byte requirements and reviewed reduction bound. If they fail, retain the result only as evidence for a separately qualified recipe. Only after one-seed acceptance should the experiment cover the hardest `owner14/block6144`, both 352/288 profiles, null/warm hints, retry, timeout, cancellation, and rollback.

## What this cannot solve alone

Measured mass work spent 147.495 of 164.284 seconds in 71 HiGHS solves. State reuse targets that term, but no speedup is established. The prototype still covers only mass constraint generation. Output refinement, SVD/tangent/projection, closest-feasible bias LP, key/value stages, independent audit, arbitrary-prompt serving, no-KV-readback ownership, committed savings, and serving latency remain separate work.
