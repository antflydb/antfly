# LSM Read/Write Sampled Baseline Evidence (2026-06)

> Relocated verbatim from `zig/pkg/antfly/src/storage/lsm/LSM.md` (lines 349–396 at commit 271838a195) on 2026-09-16 during the documentation cleanup. This is a historical implementation log kept for context; the living design is [`LSM.md`](../../../zig/pkg/antfly/src/storage/lsm/LSM.md), specifically the "Baseline Commands" section. Durable decisions from this log were folded into that document before the move.

### Sampled Baseline Evidence

Collected on 2026-06-02 from this worktree with 3 samples and 20k keys:

- Read command: `zig build lsm-backend-bench && ./zig-out/bin/lsm_backend_bench --samples 3 --keys 20000 --value-size 128 --storage host --cache both > /tmp/lsm-read-current.jsonl`
- Read comparator smoke: `zig build lsm-backend-bench-compare && ./zig-out/bin/lsm_backend_bench_compare --before /tmp/lsm-read-current.jsonl --after /tmp/lsm-read-current.jsonl`
- Cached warm hit path: median `ns/op=702.60`, `read_table_block_loads=6`,
  shared block hit/miss `99994/6`.
- Cached warm full scan: median `ns/op=88.51`, `cursor_block_loads=485`,
  `cursor_block_reuses=199515`, `read_table_block_loads=0`, and cursor
  value borrow/copy `100000/0`.
- Uncached warm full scan: median `ns/op=139.50`, `read_table_block_loads=450`,
  `read_table_block_bytes=798655`, and cursor value borrow/copy `100000/0`.
- Mixed read/write cache mode: median `ns/op=656.63`, bloom negatives
  `56205`, survivor reads/hits/misses/tombstones `60111/60000/111/0`,
  and shared block hit/miss `59986/14`.
- L0-pressure command: `zig build lsm-write-bench && ./zig-out/bin/lsm_write_bench --samples 3 --keys 20000 --batch-size 100 --flush-threshold 100 --storage host --mode default --workload-set l0_pressure > /tmp/lsm-write-l0-current.jsonl`
- L0-pressure comparator smoke: `zig build lsm-write-bench-compare && ./zig-out/bin/lsm_write_bench_compare --before /tmp/lsm-write-l0-current.jsonl --after /tmp/lsm-write-l0-current.jsonl`
- L0-pressure load median after the 2026-06-02 base-level target tuning:
  `ns/op=1449.60`, effective L0 soft/hard `4/8`, foreground write-pressure
  compactions `28`, `l0_runs_after=4`, `compactable_l0_runs_after=0`,
  `level_overflow_runs_after=0`, `level_overflow_bytes_after=0`,
  `wal_retained_bytes_after=0`.
- L0 maintenance median after the same tuning: `ns/op=250.00`,
  compactions `0`, `l0_runs_after=4`, `compactable_l0_runs_after=0`,
  `level_overflow_runs_after=0`, `wal_retained_bytes_after=0`.
- After widening nonzero L0 pressure assist windows to compact up to
  `2 * l0_limit`, the same 3-sample L0-pressure run produced load
  `ns/op=1546.75`, write-pressure compactions `28`, `l0_runs_after=4`,
  `compactable_l0_runs_after=0`, `level_overflow_runs_after=24`, and
  `wal_retained_bytes_after=0`. Follow-up maintenance dropped to
  `ns/op=1504125.00` with `1` compaction.
- Before the base-level target tuning, the same current run still left
  `level_overflow_runs_after=24` and required one follow-up maintenance
  compaction. Raising the default base-level target from 4 runs/128 KiB to
  32 runs/1 MiB removes that immediate L1 overflow while preserving bounded L0
  and zero retained WAL.

The next compaction-policy slice should target the remaining foreground
compaction cost shown by the L0-pressure load phase, while preserving the zero
retained-WAL after-state and bounded maintenance cleanup.

Large-ingest guardrails:

- Run the 50k and 1M dense public/provisioned guardrails after any change that
  touches WAL, flush, compaction, manifest publication, HBC publish, or
  ResourceManager pressure.

