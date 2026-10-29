# AY: Fresh Glowworm qualification and model-kernel migration audit

Production source: `4e90c8f8f6`. Qualification tooling: `23244097b2`.

Glowworm is used to verify the current CUDA implementation on NVIDIA GB10
(compute capability 12.1, driver 580.178.04). Exact local sources are cross-built
with Zig 0.17 ReleaseFast for Linux ARM64; the pinned binary executes in an owned
remote worktree. The remote repository is an older partial snapshot, so it is
not used as the build source. CUDA production/graph source still matches the AU
implementation `e5fd7dc9ff`; 121 CUDA source/image files are pinned. The latest
AW/AX reduction is Metal host/runtime code, which Glowworm cannot execute.

AY adds test-only CUDA-event stream and captured-batch replay modes to the
existing original-versus-candidate projection qualifier. Production kernels,
launch adapters, device images and model dispatch are unchanged. The new source
is benchmark/test tooling and documentation, not a new kernel reduction.

## Correctness

**49 selected tests pass with zero skips.** Besides module reachability entries,
the selected device coverage includes:

- 1,458 explicit native-wire cases across 25 projection adapters;
- 156 projection device profiles, independently encoded arithmetic, canaries,
  read-only operands and two mutable-operand graph replays per profile;
- the same wire/device suites against frozen pre-AU adapters;
- EmbeddingGemma packed column splits, fused vision rotary, fused backbone,
  native BF16 vision across batches/tails and packed consumers;
- LightOn vision MLP, BF16 bias epilogues and compact normalization/QKV, checked
  against their original numerical boundaries and read-only inputs.

The separately built projection/graph fixture reports **41 passed, zero skipped**.
Every captured original/candidate 32-call batch is checked against the device
oracle before its timing cohort. The graph fixture initially exposes an oracle
assumption: float expected values assumed K=256, while performance profiles use
K=1536. The reference now scales by K/256; the existing K=256 device checks are
unchanged. This is a fixture correction, not a production arithmetic fix.

## Performance

Each profile measures one/eight rows, K=1536, N=1025 and KV=257. AA/AB/BA orders
use 64 adjacent pairs and 32 adapter calls per arm. Preset limits are 1.05 for
paired median and four consecutive 16-pair trial medians, and 1.10 for paired
p95. Every completed timing profile, including failures, is retained.

| Metric | Checks within all limits | AA controls within limits | Candidate median ratio range | Maximum candidate p95 ratio |
| --- | ---: | ---: | ---: | ---: |
| Host submission | 150/150 | 50/50 | 0.990989–1.013791 | 1.059880 |
| Direct CUDA-event stream | 126/150 | 38/50 | 0.994485–1.002191 | 1.251291 |
| Captured-batch CUDA-event replay | 124/150 | 39/50 | 0.986224–1.004303 | 2.399484 |

Host submission passes every control, median, trial and p95 check. The two GPU
metrics show no meaningful median slowdown in these profiles, but neither
qualifies p95: the AA controls fail too. All candidate median and trial-median gates pass; one graph AA trial-median
gate also fails.
This is inconclusive GPU-tail evidence, not a demonstrated kernel regression or
a blanket performance-parity claim. Direct stream timestamps include host
submission starvation; captured replay removes within-batch enqueue gaps but
still includes graph scheduling and event-boundary enqueue effects.

A resident model server initially prevents the declared 60-second quiet-start
gate. The user then authorizes a shared-GPU comparison. The original host and
stream assays have already started naturally after a quiet window opens, so
their protocol/samples are preserved. No exclusive machine reservation is
claimed. The graph diagnostic explicitly permits shared GPU use; its start/end
snapshots show another model server holding a CUDA context. These measurements
are not full-model tokens/sec, examples/sec or time-to-first-output results.
They cannot close AV's separate Metal GPU-tail qualification gap.

The first graph run completes all 54 argmax profile/order checks before the
float-output oracle assertion. All 54 are retained, including its two p95
misses. After correcting the reference, an explicit qualification-only adapter
selector measures only the remaining 96 profiles. No completed profile is
repeated, discarded or selected by its performance result. The combined graph
receipt retains both source/binary identities and both protocols. The selector
is not committed to production test tooling.

## Next device-kernel work

[The model-kernel family plan](model-kernel-family-migration.md) recommends
sharing implementations through compile-time semantic/layout/schedule traits,
while keeping tuned specializations and model profiles. The first cohorts are
fused normalization/residuals and packed projection consumers, followed by
additional compatible attention consumers. Existing sharing receives no new
source-reduction credit. The proposed 20% target applies to a selected authored
device-source cohort after charging all new owners and metadata; it is not an
already achieved reduction or a promise of fewer device instructions.

## Evidence and reproduction

The permanent CUDA projection preparer documents the host, direct-event and
captured-batch environment flags. The original frozen-adapter baseline is
`ca07c2846a`. Existing device/replay qualification runs without a benchmark flag.
The graph mode captures original/candidate batches once per profile, validates
their outputs, then times replay with reusable event pairs. No timing event or
graph allocation is inside the measured host-submission interval.

Evidence in `.benchmark-assets/1166/ay-campaign/` includes exact Linux binaries,
qualified fixture source, source/image pins, compiler commands, declared
protocols, original and corrected fixture logs, all raw timing samples,
individual/combined receipts, model-source inventory and cleanup records.
Temporary owned local/remote worktrees are removed after collection; other GPU
processes, worktrees and caches are untouched.
