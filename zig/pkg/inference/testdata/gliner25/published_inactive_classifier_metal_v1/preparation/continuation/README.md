# Published-small Metal classifier continuation v1

Preparation only. The original successful LoRA/DoRA pause processes and all artifacts remain unchanged in `../antfly-gliner25-training-inactive-published-small-metal-v1`. This new directory pins every consumed pause artifact, initial state and helper; it creates fresh resumed and uninterrupted owners/output directories.

The frozen production binary is still9f0d349efa3f33dd29b6c3dd26a12a8bcf2babf2bc78400c2b84d87d58d786f9. Model, dataset path and bytes, seed, targets, dropout, optimizer and scheduling are unchanged. Host128MiB/backend1GiB (metadata64MiB)/combined2GiB and all source/dataset/tape/transfer/token/primitive limits remain unchanged. Outer sampled process-tree RSS3GiB,35s/10s cleanup,1,890s deadline and4MiB each output reuse the byte-identical qualified supervisor. No global guard or physical reserve changes.

Explicit first resumed invocation, owned by root's serial lane:

```sh
PYTHONDONTWRITEBYTECODE=1 /private/tmp/antfly-gliner25-oracle-venv/bin/python /private/tmp/antfly-gliner25-training-inactive-published-small-metal-continuation-v1/continuation.py run --mode lora --phase resumed
```

Then separately execute `--mode lora --phase uninterrupted`. DoRA uses the same two explicit phases. Each new process is exclusive; no retry or overwrite. The driver revalidates the original pause checkpoint and exact resident Controller/state hashes before launching, then checks the consumed executable/config/source/data and helper bytes afterward. Failure/cleanup receipts persist. New runs must complete5microbatches/3updates, preserve raw terms, apply fallback at rows1/3/4 and flush at2/4/5; the final wholly inactive partial window has zero gradient norm but advances every selected slot's Adam count.

Once both new phases pass, compare them with the original pause:

```sh
PYTHONDONTWRITEBYTECODE=1 /private/tmp/antfly-gliner25-oracle-venv/bin/python /private/tmp/antfly-gliner25-training-inactive-published-small-metal-continuation-v1/continuation.py validate --mode lora
```

This requires exact stitched semantic reports/decision fingerprints, final result/state, checkpoint bytes and all four portable export files. Owner peaks, resident allocation bounds and sampled RSS are recorded separately. The paused receipt's older driver remains recoverable and is never rewritten. Offline `validate-phase --mode lora --phase resumed --output NEWPATH` makes additive evidence only.

Six pure/read-only tests pass, including exact old paused-state reconstruction, unchanged resource/config recipe, all-inactive partial flush semantics, same-backend report stitching, process cleanup/argv/RSS rejection and exclusive receipts. They launch no child or model. A completed run will establish published-small classifier-only Metal continuity; it will not establish independent published Python loss/VJP parity, cross-backend numerical parity, full target coverage, long-context training or convergence.
