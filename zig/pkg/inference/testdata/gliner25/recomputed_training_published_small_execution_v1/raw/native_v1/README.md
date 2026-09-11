# Published small all-target regional training preparation

Prepared only. No build, model load, training invocation, or qualification is
represented by these files. The original source, historical jobs and five-row
dataset remain untouched. This profile uses the ordinary production constructor.

The first sequence is native CPU LoRA: pause after microbatch 1, validate that
checkpoint, resume into a fresh output directory, then run uninterrupted and
compare. DoRA follows serially. A newly built production executable must be bound
before any invocation; historical executables predate regional recomputation.

The six `.json`/`.template.json` configurations select
`attention_profile=replay_tiled_v1`, `activation_profile=layer_recompute_v1`, and
`peft.targets=["encoder","all_task_heads"]`. Everything else that affects model
or training semantics comes from the historical classifier job: exact source
identity and dataset, rank 2, alpha 3, adapter dropout 0, seed 257713, one epoch,
batch size 1, accumulation 2, constant schedule, and the same task/gold settings.
The source encoder and head dropout remain 0.1.

The resume templates contain an explicit non-executable sentinel. A prospective
driver must first validate the paused result and checkpoint, resolve the exact
32-byte state pin into a new `*-resumed.json` with exclusive creation, and retain
both template and resolved-byte digests. Reusing any existing output is forbidden.

| Owner or ceiling | Declared bytes |
| --- | ---: |
| Source maximum (actual reservation is file bytes + auxiliary + owner) | 536,870,912 |
| Source auxiliary allowance | 134,217,728 |
| Trainer host | 805,306,368 |
| Native backend | 1,073,741,824 |
| Backend metadata allowance inside backend ceiling | 67,108,864 |
| Job / dataset | 33,554,432 / 4,194,304 |
| Combined process admission ceiling | 3,221,225,472 |
| Regional plan / Step / caller reservations | 134,217,728 / 134,217,728 / 67,108,864 |
| Sampled child-tree RSS supervisor limit | 4,294,967,296 |

The exact outer job formula gives at most **2,453,667,840 bytes** using the source
ceiling. The historical source reservation would give 2,354,931,717 bytes; its
`sizeof(Source)` component is not asserted to be a current ABI measurement.
Production computes the current reservation before reading source payloads.

The regional host floor is 320 MiB. The remaining live compiled graphs, head
bindings/tapes, replay metadata, pending gradient sums and optimizer staging must
pass the production shape plan and enclosing owner limits before forward.
The 1 GiB backend ceiling is an admitted cap, not a claim that the generated
program fits. No cap or global live-memory guard is changed on denial.

`inventory.json` derives the same 131 supported Linear modules as the historical
all-target export: 72 encoder, 43 boundary, 2 classifier, 8 record and 6 relation.
Original tensor count is 334. The adapter inventory is 262 LoRA slots with
889,576 raw FP32 bytes, or 393 DoRA slots with 1,090,340 bytes. The source header
and all four sidecars were checked without reading/copying the weight payload;
the eventual driver and production Source must enforce the full weight hash.

Disk reservations use the current job formula `8MiB + 4 * adapter_payload` per
checkpoint. They also include the exporter header/config/receipt bounds, all
six logs and receipt allowances, and one replacement checkpoint overlap. Total
planned growth is **153,522,428 bytes**, plus unchanged 256 MiB free headroom,
requiring **421,957,884 free bytes** for the whole prospective plan. Source-copy
and full-model-export bytes are both zero. Free space must still be checked
before every phase.

Rows lacking classification annotations are still active through encoder
adapters. Expected global fallback flags are therefore all false. Each real
microbatch preserves positive raw `terms.total` as the optimizer objective.
There are five microbatches, three updates and flushes after rows 2, 4 and 5.
Classifier slots participate in two windows; encoder and enabled optional-head
zero-touch slots participate in three. Dormant boundary-head paths may have
`grad=None` and must not be guessed from numeric zeros.

`qualification_plan.json` specifies the bounded process/checker protocol. The
first worker argv, passed through the pinned supervisor with a 4 GiB sampled
tree RSS cap, is:

```text
NEW_BINARY finetune train gliner25 /private/tmp/antfly-gliner25-regional-all-small-native-v1/lora-paused.json --shutdown-grace-seconds 30 --stop-after-microbatches 1
```

The supervisor remains pinned to psutil 7.1.3, 50 ms samples, 35-second parent
grace, 10-second kill/reap wait, and 4 MiB per output stream. Any failure keeps its
receipt and outputs; there is no implicit retry. Before this command is run,
the additive checker still needs implementation and its focused rejection tests.
It must independently reconstruct checkpoint identity from exact bytes, check
every slot, and compare final checkpoint/export bytes and stitched semantic
reports across fresh-owner resume and uninterrupted execution.

This is not published PyTorch RNG/VJP/update parity, CPU–Metal update equality,
long-context qualification, quality, performance, or release-gate evidence.
