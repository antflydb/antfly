# Full-Text Plan

## Goal

Keep full-text visibility semantics aligned with the LSM path:

- write/sync waits for search visibility, not full compaction
- segment merges run as background maintenance
- explicit force-compaction stays available as an admin/test hammer

## Current Policy

1. `SyncLevel.full_text` means the affected documents are searchable in the
   relevant full-text indexes.
2. Scheduled text merges are background work owned by the text-merge runtime and
   its resource budgets.
3. `runUntilIdle()` drains scheduled text merges as part of idle maintenance.
4. `drainScheduledTextMerges()` is the explicit blocking API for "finish the
   merges that are already scheduled".
5. `forceCompactTextIndexes()` remains the heavy hammer that aggressively
   rewrites text indexes beyond the normal merge scheduler.

## Why

The old shape mixed two different concerns:

- search visibility / replay catch-up
- segment merge compaction

That made `full_text` sync heavier than it needed to be and hid the real cost of
forced compaction in normal maintenance timings.

## Task List

- [x] Stop draining scheduled text merges inside normal sync-level waits.
- [x] Add an explicit `drainScheduledTextMerges()` API.
- [x] Route `runUntilIdle()` through the scheduled-merge drain path.
- [x] Keep `forceCompactTextIndexes()` as a separate explicit admin/test path.
- [x] Move broad callers off forced compaction where the goal is just stable
      maintenance rather than minimal segments.

## Follow-Through

- Add per-index segment-count reporting to `replay_bench.zig`.
- Make forced compaction report or reserve text-merge resources so its memory
  behavior is observable instead of bypassing the normal scheduler path.
- Add a `best_effort` forced-compaction path that stops under resource pressure
  and leaves merge debt scheduled instead of always acting like a blocking
  hammer.
- Consider a first-class chunked full-text generator path instead of piggybacking
  on dense-generator config for `generated_chunked_full_text`.
