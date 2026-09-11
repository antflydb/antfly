# Training-attention execution evidence

This additive [ledger](manifest.json) retains the completed CPU v8 and Metal
v2 checkpoints for the dedicated replay-tiled DeBERTa training-attention
implementation. It is separate from the upstream numerical reference manifest.

- CPU v8: 22 selected, 18 passed, four expected GPU skips.
- Metal v1: alignment type error at compilation; no attention test ran. The
  validated bucket view is now taken from the original aligned control input.
- Metal v2: 23 selected, 23 passed, no skips or reported leaks. The source
  comparisons, device allocation/cancellation checks, and tiny full/head
  managed replay/resume tests all passed in this executable.
- Metal width v1: seven selected, seven passed, no skips or reported leaks.
  One test covers 18 CPU–Metal width/tail cases; six encoder graph, binding,
  allocation and replay-region cases also pass.

The nine source cases use batch two, two heads, width four per head, 512
relative rows, sequence lengths seven and 512, and dropout zero, 0.1 and 0.125.
Context and all five Q/K/V/Qr/Kr gradient slices use the unchanged gate
`abs(actual-expected) <= 2e-5 + 3e-5*abs(expected)`. Fully masked queries,
repeated relative buckets and physical high-bit replay counters are included.
The native consumers did not emit per-case maximum errors; a passing gate
must not be presented as a measured zero error.

The tiny managed jobs use a separate authored H4, one-layer model with dropout
zero. Each full/head mode completes six microbatches and four optimizer updates
over two epochs, checks cancellation and declared-versus-backing allocation
failures, and matches exact fresh-owner partial-resume state. This is native
composition and continuity evidence; the projected source cases independently
establish the primitive's numerical gate.

The separate width cases use D64/D128/D256 with two heads, batch one,
S1/S17/S65, R512 and dropout zero/0.1, including ragged and fully masked inputs.
They compare context and all five VJPs against the independently tested CPU
primitive using different 17-by-31 tiles at the same tolerance. These are
CPU–Metal geometry comparisons, not new upstream source captures. The host
owner is capped at 64 MiB; each case proves a device logical-byte bound below
16 MiB and includes complete ownership cleanup. The attempted live identity
observer missed this short run, so its exact executable remains unrecorded.

All native logs, the original supervised source receipt, and its capture
metadata are retained verbatim. The external 40 MiB tensor file remains in
`/private/tmp/gliner25-training-attention-v1` and is bound by exact size/SHA-256.
The Metal v2 [live identity](metal_v2_live_identity.json) records the observed
process creation identity, argv, executable stat/hash and thirteen frozen
source hashes. Its test executable was 49,006,728 bytes, SHA-256
`80db1c2557dd336df188c2a7889ff1e5dd63fc2adffd2e190c6ffa9771d168ee`.
CPU v8 and failed Metal v1 executable identities were not recorded. The source
inventory is scoped, not a complete build dependency closure. The v2 device
test source is archived before the width-coverage additions; the width run's
test and encoder-binding source snapshots are retained separately.

The root ran these commands serially from `zig`; change `-Dmetal=true` to
`false` for the CPU variant:

```sh
ANTFLY_GLINER25_TRAINING_ATTENTION_FIXTURE_DIR=/private/tmp/gliner25-training-attention-v1 \
ZIG_GLOBAL_CACHE_DIR=/private/tmp/antfly-gliner25-zig-cache \
zig build inference-test -Dmetal=true -Dcuda=false -j1 -- \
  --test-filter 'replay DeBERTa training attention' \
  --test-filter 'GLiNER2.5 replay encoder' \
  --test-filter 'resident program replay attention' \
  --test-filter 'deberta training Metal' \
  --test-filter 'native dense view alias metadata failure' \
  --test-filter 'boundary native trainer replay attention'
```

Pure S4096/S16384 graph and device-planner checks prove bounded admission,
not numerical long-context execution. This ledger does not qualify activation
recomputation, published-model replay training, performance, convergence or
release availability. Existing public/default behavior remains unchanged.

The ancillary local contract receipts are separate from the native results:
v8 passes all 203 Python tests in 3.879 seconds, without skips, using the
unchanged pinned oracle interpreter and actual official promtool 3.14.0. The
preceding v7 log retains its 25 import/dependency errors among 201 discovered
tests: the selected `scripts/.venv` interpreter lacked `psutil` and `packaging`.
Choosing the existing pinned interpreter fixed that checkpoint without
dependency changes. Both logs and the exact successful command are recorded
in the manifest; this does not claim remote CI or model execution.
