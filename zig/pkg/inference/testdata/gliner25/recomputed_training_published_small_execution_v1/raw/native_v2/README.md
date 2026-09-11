# Fresh regional all131 native campaign: v2

Prepared only. No executable is bound, no campaign process has started, and no
model or checkpoint is copied into this directory. The preceding v1 campaign
is unchanged: its LoRA pause passed, while fresh resume returned
`TrainingOptimizerLimitExceeded` before updating. `prior_campaign.json` pins
all 37 existing v1 files. Those results do not establish v2 success and no v1
checkpoint will initialize v2.

The new preparation keeps all five published-small source pins, the authored
five-row JSONL, every training/optimizer/dropout option, all 131 Linear targets,
and every memory/disk/time/output cap. Each of the six config files is exactly
the corresponding v1 byte sequence with only the campaign directory changed.
The inventory is byte-identical: 262 LoRA or 393 DoRA slots. The normal production
constructor and `native` execution remain selected with `replay_tiled_v1` and
`layer_recompute_v1`. No synthetic fixture seam is used.

## Corrected restore admission, unchanged caps

The prepared source pins include the corrected restore accounting in
`seeded_gradient_trainer.zig`, SHA256
`ef0af87691ad971fd67d750c4ccce1633ba3352211afd161a2f97817edd42cf6`,
and the current job/export/dataset attribution changes. The restore path first
reserves staged state and the actual immutable checkpoint snapshot. It then
clamps the header parser allowance to the remaining transaction capacity; the
64 MiB default parser ceiling is not charged as an unconditional allocation.
This preparation itself does not qualify that implementation.

The transaction cap stays **32 MiB**. Native staged-state admission is computed
from four adapter payloads plus, per slot, twice the name bytes, four times the
tensor rank, and 1,024 metadata bytes. At the existing conservative checkpoint
file bounds, the remaining parser capacities are:

| Mode | Staged-state formula | Checkpoint file upper bound | Remaining header allowance |
| --- | ---: | ---: | ---: |
| LoRA | 3,872,360 B | 11,946,912 B | 17,735,160 B |
| DoRA | 4,835,850 B | 12,749,968 B | 15,968,614 B |

These are static admission terms, not measured peaks or successful restores.
The enclosing host owner separately charges its descriptor/metadata storage.
Host/backend/combined caps remain 768 MiB/1 GiB/3 GiB; region-plan/Step/caller
reservations remain 128/128/64 MiB. The live physical-memory guard remains
enabled. The outer sampled child-tree RSS cap is 4 GiB, distinct from allocator
admission. The job/outer deadlines remain 1,800/1,890 seconds, with 4 MiB per
output stream and unchanged owned-process cleanup intervals.

The complete six-phase growth bound is still 153,522,428 bytes; with 256 MiB
headroom, the disk guard requires 421,957,884 bytes before every phase. There
are no full-weight exports or model/source copies. No cap is raised implicitly.

## Fresh executable binding

The old standalone executable `f4829893…125f02b8` is explicitly denied, along
with older executables. Root must supply the new production executable's exact
SHA256, its build receipt and observed `--help` identity, the archived source
inventory, and the archive receipt/payload. All twelve prepared formula-source
pins must match that frozen build inventory. Execution revalidates those
archived bytes without requiring the live checkout to remain unchanged.

Use `run_phase.py bind --help` to inspect the file-only binding arguments. No
placeholder artifact path or hash is preselected. Binding writes a new
exclusive `executable.json` and exact helper archive. It starts no model.

After root has reviewed the fresh binding and released the serial build lane,
the first and only initial phase is:

```sh
PYTHONDONTWRITEBYTECODE=1 /private/tmp/antfly-gliner25-oracle-venv/bin/python /private/tmp/antfly-gliner25-regional-all-small-native-v2/run_phase.py run --mode lora --phase paused
```

Root then schedules LoRA `resumed`, `uninterrupted`, and `validate --mode lora`
separately. DoRA remains blocked until LoRA's complete continuity validation
passes under the same new executable. No phase retries in an existing directory.
Resume creates a new config from its immutable template using only the verified
v2 pause's state digest. Failures and original process/helper receipts persist.

The frozen supervision dependency is unchanged:
`a937237975be2ed879f62afd285494ccb51a7c1d7cc1dff7160621fb26b87332`.
The checker retains its 16 MiB checkpoint/2 MiB header/1 MiB chunk bounds, exact
ordered inventory and None-versus-zero counters, independently reconstructed
Controller/state/layout/adapter hashes, pre/post source/data/config/build pins,
atomic output checks and byte-exact final resume comparisons. All adapter slots
use the sole `task_lr` group, including encoder adapters. The fixed live encoder
means all five global fallback flags must be false; classifier participation is
restricted to rows zero and two, and flushes remain [2,4,5].

## Preparation checks

Twelve synthetic tests pass: the nine original bounded checks, plus exact v1-to-v2
path-only relocation and cap preservation, rejection of stale/missing formula
source inventory, and rejection of the old executable before any build lookup.
These checks do not load a model or validate a numerical update.

```sh
cd /private/tmp/antfly-gliner25-regional-all-small-native-v2
PYTHONDONTWRITEBYTECODE=1 /private/tmp/antfly-gliner25-oracle-venv/bin/python -m unittest -v test_checker
PYTHONDONTWRITEBYTECODE=1 /private/tmp/antfly-gliner25-oracle-venv/bin/python run_phase.py inspect
```

`preparation-checkpoint-v2.json` pins the completed preparation, helpers, tests
and read-only inspection. Published-source PyTorch RNG/loss/VJP/update parity,
CPU–Metal update parity, long-context quality, convergence, performance and
release qualification remain outside this campaign's continuity contract.
