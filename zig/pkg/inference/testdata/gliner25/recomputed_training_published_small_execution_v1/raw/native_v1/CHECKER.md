# Implemented qualification checker

The root agent has bound the new standalone production executable in `executable.json`. Binding performs file checks only. No model phase has run at this checkpoint. Do not rerun `bind` or replace an existing execution, configuration, output, or receipt. The earlier README and proposal remain immutable historical preparation.

`checker.py` and `run_phase.py` are frozen by the binding. Their exact source bytes are retained in `helper_archive`. The sole supervision dependency is the previously tested `supervision.py`, SHA256 `a937237975be2ed879f62afd285494ccb51a7c1d7cc1dff7160621fb26b87332`; neither its source nor the existing Python environment was modified. No Torch import, model copy, or source archive extraction is used.

The executable is `/private/tmp/antfly-gliner25-runtime-regional-v1/bin/train-gliner25`, 10,944,672 bytes, SHA256 `f482989317fd34c0a8852d5a36d837fb78f6cc57862863f5a0b77981125f02b8`. The binding checks its observed standalone `--help` identity, build receipt `d9eb64e96bcc0fbe3569806cef320f2ad074932ef01fec13395e283f9dac2b4a`, source archive receipt `e5b6ff67b8c2b31c03f78bc0df9d6ecf8fa75863664aa77ef0e39c5bec6a1100`, archived inventory, and source archive `42f90571c1d6bf76c89ba146865637b855e0dc5c5b2302a4e6188cbbf26bfdf1`. Live checkout changes do not reinterpret the frozen binary. The archived selection is not a full external dependency closure.

## First authorized phase

Only the root agent schedules this command after its active build has completed:

```sh
PYTHONDONTWRITEBYTECODE=1 /private/tmp/antfly-gliner25-oracle-venv/bin/python /private/tmp/antfly-gliner25-regional-all-small-native-v1/run_phase.py run --mode lora --phase paused
```

It invokes the standalone executable directly with `lora-paused.json`, shutdown grace 30 seconds, and `--stop-after-microbatches 1`. There is no public-runtime argv prefix. Success requires exactly one microbatch, zero optimizer updates, one accumulated microbatch, no model export, valid independently reconstructed Controller/state hashes, and complete owned-child cleanup.

This profile selects encoder and every task head: 131 modules. The live encoder means **none of its five microbatches should use the global zero-loss fallback**, including the three rows without classification. Classification contributes only on rows zero and two. All optimizer groups use the adapter task learning rate: frozen `gliner_boundary_run.zig:107` selects `groups[1..]` for LoRA/DoRA and line 125 assigns every adapter slot group zero. Exact frozen source hashes and review locations are recorded in `frozen-checker-source-review.json`.

The fixed shared-pool schema predicts 14 dormant modules (explicit-span proposer, pair scorer, and record-only candidate encoder), 115 modules with three final Adam updates, and two classifier modules with two updates. Paused presence is explicit for 117 modules and absent for 14; final presence and accumulators must be cleared. This is a predeclared route check, not measured published-model evidence. Each checkpoint retains all 262 LoRA or 393 DoRA slots.

## Resource and lifecycle contract

The job deadline is 1,800 seconds. The outer supervisor deadline is **1,890 seconds**, retaining the original prepared 90-second margin. Sampled child-tree RSS is capped at 4 GiB; stdout and stderr at 4 MiB each; sampling interval is 50 ms. The unchanged supervisor permits 35 seconds of graceful parent cleanup, then a bounded ten-second kill/reap interval and two-second worker parent-loss interval, tracking process creation identity rather than historical PIDs.

Native host/backend caps are 768 MiB/1 GiB, combined ceiling 3 GiB. Region-plan/Step/caller reservations are 128/128/64 MiB. The explicit resource plan charges source, dataset, job, runtime, and optimizer owners; the actual compiled admission must still pass before the first forward. Allocator admission and sampled RSS are separate measurements. No cap is automatically raised.

The disk guard requires 421,957,884 free bytes: the complete six-phase growth bound of 153,522,428 bytes plus 256 MiB headroom. Checkpoint files are bounded at 16 MiB, headers at 2 MiB, and every payload read at 1 MiB. Tensor payloads are streamed for finiteness, moments, counters, and hashes. No complete checkpoint-to-Python-float list or copied model weights are retained.

Each phase owns a fresh execution directory before preflight. Success and failure leave an immutable process receipt. Source files, authored dataset, configuration, executable, binding, and helpers are checked before and after execution. Atomic JSON publication refuses existing destinations; cleanup only removes its unpublished private temporary file. A failed phase is never retried implicitly.

## Subsequent commands

After reviewing each previous result, the root agent schedules one command at a time:

```sh
PYTHONDONTWRITEBYTECODE=1 /private/tmp/antfly-gliner25-oracle-venv/bin/python /private/tmp/antfly-gliner25-regional-all-small-native-v1/run_phase.py run --mode lora --phase resumed
PYTHONDONTWRITEBYTECODE=1 /private/tmp/antfly-gliner25-oracle-venv/bin/python /private/tmp/antfly-gliner25-regional-all-small-native-v1/run_phase.py run --mode lora --phase uninterrupted
PYTHONDONTWRITEBYTECODE=1 /private/tmp/antfly-gliner25-oracle-venv/bin/python /private/tmp/antfly-gliner25-regional-all-small-native-v1/run_phase.py validate --mode lora
```

Resume resolves only the exact expected state digest from the verified pause into a new configuration. Final validation independently reconstructs Controller/state, layout, target, and adapter parameter digests; checks all saved names/shapes and checkpoint bytes; and requires exact stitched semantic reports and decision fingerprints, final result/checkpoint, and all four export files between resumed and uninterrupted runs. Owner peaks remain separate observations.

DoRA uses the same three `run` phases followed by `validate --mode dora`, and is rejected until the LoRA final validation passes under the same executable binding. Do not combine these commands into an unattended batch.

If a process completed but a checker revision is needed, preserve all old helpers and receipts. The additive offline path starts no model:

```sh
PYTHONDONTWRITEBYTECODE=1 /private/tmp/antfly-gliner25-oracle-venv/bin/python /private/tmp/antfly-gliner25-regional-all-small-native-v1/run_phase.py validate-phase --mode lora --phase paused --receipt-name validation-v2.json
```

## Measured checker evidence

Nine bounded synthetic tests pass in `checker-tests-v1.log`: strict JSON and declared f32 identity, exact integer counter limbs, streaming SafeTensors ranges and mutation detection, descriptor cleanup, full inventory and route counters, None versus present-zero gradients, independent raw state/Controller hashes, active/inactive progress, argv preservation, timeout-plan alignment, and atomic no-overwrite cleanup. These tests do not execute the model or establish published numerical training parity.

```sh
cd /private/tmp/antfly-gliner25-regional-all-small-native-v1
PYTHONDONTWRITEBYTECODE=1 /private/tmp/antfly-gliner25-oracle-venv/bin/python -m unittest -v test_checker
```

Any later successful model result supports within-profile native replay/resume on the authored five rows with source dropout 0.1. It does not establish PyTorch RNG/loss/VJP/update parity, cross-backend updates, long-context quality, convergence, performance, or release-gate approval.
