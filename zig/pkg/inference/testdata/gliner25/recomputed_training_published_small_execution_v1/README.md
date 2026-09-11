# Published-small native regional training

This ledger records six successful production CLI phases for rank-2 LoRA and
DoRA targeting all 131 supported encoder/task-head Linear modules, using
`attention_profile=replay_tiled_v1` and
`activation_profile=layer_recompute_v1`. Each mode ran five short authored
examples for one epoch with accumulation two: pause after microbatch one,
fresh-owner resume, and a separate uninterrupted run. Final microbatch/update
counts are 5/3, with flushes after rows 2, 4 and 5.

Both modes preserve byte-exact final `result.json`, `latest.safetensors`, and
all four adapter-export files across resume/uninterrupted execution. The frozen
independent checker streams checkpoint payloads in at most 1 MiB reads,
reconstructs Controller and state hashes, and validates exact names, shapes,
per-slot counters and presence. Original checkers, wrappers, process receipts,
raw output, consumed jobs and final reports are retained. During packaging,
all twelve final artifact pairs were independently rehashed and compared with
the recorded reports; no training or model load ran.

The executable is the standalone ReleaseFast production training entry, built
with Metal support but executed with `execution=native`. SHA-256:
`92a2816cdf4a3d87838506c3232d6515eadb64ba6976c686b0d3dca8595f15f2`.
Its 2,587-file recorded source selection and archive identities are pinned.
This selection is not a complete external dependency closure. The ordinary
published inventory and initializer were used, with the original five source
file pins and exact authored dataset; no synthetic model/adapter initialization
seam was used.

All encoder targets remain active on entity rows without classification, so
the global zero-loss fallback is false for all five rows. At the first pause,
LoRA has 234 present / 28 absent slots; DoRA has 351 / 42. After final flush,
all accumulators are absent. LoRA's slot-step distribution is 230 at step 3,
4 at step 2, 28 at step 0; DoRA's is 345/6/42 respectively. The archived exact
slot names retain which paths received gradients. Original source dropout is
0.1 and native replay is deterministic; no PyTorch RNG equivalence is claimed.

| Mode | Maximum trainer host bytes | Maximum native backend bytes | Maximum sampled child-tree RSS bytes |
| --- | ---: | ---: | ---: |
| LoRA | 22,672,679 | 21,976,220 | 463,323,136 |
| DoRA | 32,805,432 | 33,032,120 | 486,211,584 |

Each invocation admitted 2,354,931,717 bytes including separate source, dataset,
job, host and backend reservations. Explicit host/backend/combined caps remain
768 MiB / 1 GiB / 3 GiB, the optimizer transaction is 32 MiB, regional-plan/Step
caps are 128 MiB each, and caller scratch is 64 MiB. The sampled process-tree
cap was 4 GiB, job timeout 1,800 seconds, outer timeout 1,890 seconds, and each
output stream cap 4 MiB. RSS is sampled and may double-count shared pages; it
is distinct from allocator peaks and admission. All six phase receipts report
complete owned-process cleanup, no survivors/inspection errors, and exit 0.
These short runs are not a memory or speed benchmark.

The earlier v1 LoRA pause passed, then fresh resume failed cleanly with
`TrainingOptimizerLimitExceeded`. The old restore path reserved the default
64 MiB parser ceiling before inspecting the checkpoint, which exceeded the
32 MiB whole-transaction cap. V2 reserves staged state and the immutable
snapshot first, then clamps parser allowance to the remaining transaction.
All v1/v2 memory caps remain unchanged; the original failure is preserved.
Exact old/new Controller source members and their full diff also retain the
associated declared-cap versus backing-OOM attribution fix.

This is native published-small restart/artifact consistency, not published
Metal regional training, published-model PyTorch VJP/update parity, 512-token
numerical execution, task quality, convergence or release qualification.
The public availability gate remains unchanged.

Run the bounded, model-free archive check:

```sh
PYTHONDONTWRITEBYTECODE=1 python3 verify.py
```

Files larger than 32 KiB use deterministic gzip. `manifest.json` pins both
stored and decoded bytes, original locators and the measured scope. No model
weights, checkpoint tensor payloads, binary or full source archive are copied
here. Full numerical revalidation requires the external pinned artifacts;
`verify.py` checks archived byte identities and report consistency only.
Historical preparation/build receipts remain unchanged and describe their
then-unexecuted state; the later process and final-validation receipts carry
completed-run evidence.
