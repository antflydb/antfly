# Laya production hardening review

The review addresses admission, immutable inputs, checkpoint cursors, export
durability, and qualification evidence. The original sustained campaigns remain
sealed under `.benchmark-results/laya-finetuning-20260921/`; revised-binary
checks are retained under `.benchmark-results/laya-production-review-20260921/`.
Status: review complete; the previously measured production profiles remain
qualified after hardening. The final source snapshot and machine-readable
result are retained as `implementation-snapshot.tar.gz` and `review-result.json`.

## Changes

| Finding | Fix |
| --- | --- |
| Export reopened tokenizer files and retained mutable mapped source weights | Snapshot source inputs during admission; retain owned parameters/frozen values and release raw weights before optimizer restore. Export admitted tokenizer bytes and fsync files. Optional tokenizer metadata may be absent. |
| Only the first training batch received geometry admission | Check worst padded attention size, tokens, and options across all splits before allocating a backend or creating output. Reject malformed encoder metadata and non-finite frozen tensors early. |
| Resume accepted impossible optimizer/accumulation positions | Validate the full cursor, including partial epoch flushes, before restoring device state. Validate absolute stop boundaries. |
| Paused runs could return qualification success | Require a completed report; preserve monitoring/timeout failures. Execute copied binary/job inputs and check free space on the output filesystem. |
| Aggregate metrics could hide mismatched evidence | Reconstruct the native run fingerprint; bind source, all splits, replay recipe and record order. Recompute metrics from predictions, reject invalid JSON/metrics, and compare each type against source serving calibration. |
| Torch replay trusted prepared tokens by ID | Re-tokenize each record and retain sequence hashes; reject parity type/shape mismatches. |
| Qualification Python tests lacked CI coverage | Run them in both Zig test lanes and include Laya scripts in CI path selection. |

The review does not change forward, gradient, optimizer, sampling, or scheduler
arithmetic. Existing v1 checkpoint identities remain compatible. Older quality
reports require their sealed manifest when re-scored by the stricter tool.

## Verification

- CPU with system BLAS: 33 passed, six optional backend/artifact skips.
- Portable CPU without Metal or system BLAS: 33 passed, six optional skips.
- Enabled Metal regressions: 16 passed, no skips.
- Python conversion, quality, and process-runner regressions: 18 passed.
- Workflow path-selection regressions: 10 passed.
- All three retained sustained campaigns pass the stricter quality scorer.
- All 400 prepared decisions reproduce their cached token sequences. A one-token
  mutation is rejected before evaluation.

The final binary passes all 201 source and trained-model gradient tensors on
both CPU and Metal at the unchanged tolerance. The new export passes actual
CPU/Metal serving reloads on 20 held-out decisions in batches one and four;
maximum probability errors are `1.133e-6` and `4.173e-7`, below `5e-5`.

A full-size Metal restore from the completed 80-update / 320-microbatch RLCD
checkpoint finishes evaluation, calibration, and export in 392.70 seconds.
Tracked host peak is **16,586,769,643 bytes (15.45 GiB)**, within the unchanged
16 GiB limit. Sampled RSS peaks at 12,947,587,072 bytes. Global swapouts do not
increase; global pageouts increase by 945. This completed-checkpoint run adds
no optimizer updates and does not establish zero-paging training.

Optimizer checkpoint, serving weights, configuration, and tokenizer files are
byte-identical to the previously qualified artifacts. All 160 final prediction
rows and final aggregate/per-type metrics match exactly. The new predictions
also include record IDs. Artifact SHA-256:

- Optimizer: `d23f030b1a30ad6189c10849d288bfadc5d18cee1403cf69c0b51edbdf78f2de`.
- Serving weights: `6e01eab249a54c077874e382dc7308f25686f2328cb052612c71d55a17dcfcfa`.

The first hardened variant retained its raw source snapshot during restore and
failed the 16 GiB budget. That attempt remains in `full-size-resume-evidence/`.
The final implementation frees the raw snapshot after copying admitted
parameters and frozen export values. `full-size-resume-final-evidence/` records
the passing retry; the limit was unchanged.

`quality-bound-commands.json`, `final-model-validation-commands.json`,
`full-size-equivalence.json`, and individual logs retain commands and outcomes.
`prior-seal-verification.json` verifies all 290 original files and five symlinks
without changes. Identical large review artifacts are retained as hardlinks.
Rebuilding after the final test-fixture correction changes only Mach-O UUID,
debug object path/timestamp, and code signature: every runtime section and
dynamic-link metadata block is identical (`cli-section-identity.json`).

The qualification scope remains CPU soft CE with system BLAS and Metal soft
CE/RLCD on the measured M4 Pro profile. The sustained campaigns use the pinned
synthetic offline suite, batch one, accumulation four, seed 42, and sequences
capped at 512 tokens. CPU RLCD, other hardware/recipes, and application traffic
still require their own sustained quality campaigns. Paging was observed; this
is not a zero-swap qualification.
