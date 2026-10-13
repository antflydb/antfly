# Laya packed serving cost: how the numbers got here (2026-09-24 to 2026-10-03)

> Relocated verbatim from `zig/pkg/inference/models/laya/LAYA.md` (lines 1869–1909 at commit 3ce5aff4ea) on 2026-10-12. Durable decisions were folded into [LAYA.md](../../../../../zig/pkg/inference/models/laya/LAYA.md) before the move.

How the numbers got here, all on the same machine and checkpoint:

- **RoPE on the device:** the first packed Metal implementation rotated RoPE
  on the host. That took 56 device synchronizations per forward, added
  200–800 ms, and lost to the fused baseline everywhere. The device M-RoPE op
  removed the overhead.
- **Dense masks (before step 1b):** 16 questions on a 407-token state took
  342 ms on Metal and 2.76 s on CPU. Segment attention brought that to 200 ms
  and 2.10 s, and the gap grows with row length.
- **State cache before segment attention:** a cached follow-up was 108 ms on
  Metal and 740 ms on CPU. Removing the trunk's zero query rows brought it to
  66 ms and 331 ms.

**Device scoring for packed rows (2026-09-25, restored 2026-10-03).**
Scoring on the device instead of reading the hidden state back saved 2–4% at
16–64 questions (for example 12 sentences, 64 questions: 406.8 → 399.5 ms
packed) and nothing measurable below. It was reverted after intermittent
wrong outputs on Metal, then restored once the cause, a stale embedding-table
cache, was fixed (see below). Either way, the packed Metal path is bound by
encoder GPU work that the fused resident kernels would compute the same way,
so routing packed rows through them (step 1c) is not expected to pay off on
Metal.

**Stale embedding-table cache (fixed 2026-10-03).** The Metal device
scoring failure was not a race. Metal's embedding lookup copied each table
into a device cache keyed by the table's buffer identity (device buffer
handle or host address) and shape. Gathering marker and anchor rows from the
hidden state put activations into that cache. Once the buffer pool handed a
later table of the same shape the same buffer, the lookup returned the
earlier rows. The batching test failed in 3 of 24 Metal runs with a
probability error up to 0.059. A regression test that reuses a pooled table
fails on every run without the fix.

Device tables are now read in place, and the host-address cache only keeps
immutable model storage. With device scoring back on:
- the regression test passes;
- the batching test passes 40 of 40 runs;
- the Laya parity tests pass on Metal against fresh PyTorch fixtures.

The full raw output of every run is in
[`docs/design/inference/history/laya/2026-09-24-tree-packing.md`](../../../../../docs/design/inference/history/laya/2026-09-24-tree-packing.md).
