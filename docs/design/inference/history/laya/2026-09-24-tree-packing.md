# Tree-packed Laya: 2026-09-24 benchmark and test evidence

> Summary of a verbatim log. Full text: [docs/design/inference/history/laya/2026-09-24-tree-packing.md@f3fd4d3](https://github.com/antflydb/antfly/blob/f3fd4d35258791fe16ab927a1e7f9b3af247b074/docs/design/inference/history/laya/2026-09-24-tree-packing.md) (all per-case JSON rows). Design and summary: [`zig/pkg/inference/models/laya/LAYA.md`](../../../../../zig/pkg/inference/models/laya/LAYA.md); narrative in [2026-09-25-serving-cost-history.md](2026-09-25-serving-cost-history.md).

Raw benchmark and test output from the tree-packed Laya work on 2026-09-24. Host: Apple M4 Max, 36 GiB, macOS 15 (Darwin 24.6.0), Zig 0.16.0; checkpoint `convaiinnovations/laya` at `c5d78730f3493e4fe16d61507ef4b78eef7318cf` prepared with `scripts/laya/prepare_laya.py`. The packed session uses `laya.packing = {"mode":"question","max_packed_len":8192}` on the same weights, so only cost is compared; median of five warm requests after one discarded, ReleaseFast test binary.

```bash
ANTFLY_LAYA_PACKED_BENCH=<prepared laya dir> [ANTFLY_LAYA_BACKEND=metal] \
  zig build test -Doptimize=ReleaseFast -- --test-filter "laya packed benchmark"
```

## Decisions
- **Packed RoPE on device**, not rotated on the host — the host version's 56 device synchronizations per forward added 200–800 ms fixed overhead.
- **State cache restricted to CPU, then re-enabled on Metal** after the row-join fix (in-stream flat concat/slice on Metal) and device-resident entries; trunks under 96 tokens are not cached.

## Rejected approaches
- **Host-rotated packed RoPE on Metal** — packed slower than unpacked in most cases (e.g. 1 sentence × 1 question: 249.9 ms packed vs 73.1 ms unpacked).
- **First Metal state cache (host keys/values)** — no speedup because host keys and values are re-uploaded every request; and decisions through a session were wrong (max probability error 0.034 vs the oracle) because the device row concat/gather was not ordered with pending batched command work.
- **Device-resident cache with an unframed branch forward** — cached slower than uncached (e.g. 12 × 16: 448.9 vs 342.2 ms), showing the remaining cost was per-op submission rather than uploads.

## Key measurements (ms; state sentences × questions; tokens unpacked → packed)

| Case | Tokens | Metal unpacked | Metal packed (device RoPE) | Metal cached (GPU cache) | Metal segment attn packed / cached | CPU unpacked (segment attn run) | CPU segment attn packed / cached |
|---|---|---:|---:|---:|---:|---:|---:|
| 1 × 1 | 55 → 56 | 58.5 | 56.5 | 56.5 | 60.7 / 59.5 | 317.8 | 300.8 / 328.5 |
| 4 × 16 | 2491 → 542 | 410.8 | 190.7 | 191.4 | 132.2 / 119.2 | 4248.2 | 1191.2 / 994.7 |
| 12 × 1 | 407 → 408 | 195.5 | 139.2 | 108.5 | 135.1 / 66.2 | 1162.1 | 1144.8 / 331.2 |
| 12 × 16 | 6587 → 798 | 1534.7 | 340.3 | 473.1¹ | 200.0 / 141.2 | 15620.5 | 2102.2 / 1503.3 |
| 12 × 64 | 26363 → 2046 | — | — | — | 472.2 / 382.4 (unpacked 6099.4) | 59179.5 | 5076.6 / 4331.7 |

¹ The uncached companion value in that run (972.6 ms) is an outlier; three earlier runs measured 340–347 ms.

- Segment attention (step 1b) is the last configuration measured; earlier columns are from the same day's preceding steps. CPU run-to-run variation is visible (e.g. 8.6 s vs 10.6 s for eight questions over the longest state).

## Correctness evidence (fixture-backed, native)
- Training forward and all 45 gradient tensors match PyTorch, max absolute error 0.0000023841858.
- Packed training graph vs serving logits: question mode 0.0000032186508, candidate mode 0.0000069886446; export serving vs training max probability error 0.000000059604645.
- One-segment packed encoder reproduces the unpacked encoder exactly (max error 0); packed questions isolated (max error 0.0000027418137 question / 0.0000047683716 candidate) with exact trunk encoding. PyTorch oracle: `one_segment_vs_upstream_max_error` 0.0.

## Step 0: typed-decisions accuracy, 2026-09-24

Serving-path `finetune eval laya`, Metal, s0-eval (152 cases, 760 decisions), overall metrics:

| Model | Packing | Accuracy | Soft CE | ECE | Seconds |
|---|---|---:|---:|---:|---:|
| released | none | 0.3868421052631579 | 1.30795638096163 | 0.1579487887652297 | 53.172325 |
| released | question | 0.3605263157894737 | 1.337135795768793 | 0.132774419063016 | 37.975024 |
| upstream-typed-decisions | none | 0.7539473684210526 | 0.8853601253877356 | 0.19298428155873945 | 75.273512 |
| packed-finetune | question | 0.5736842105263158 | 1.026462118628347 | 0.06338861419966349 | 92.540653 |
| unpacked-finetune | none | 0.5723684210526315 | 1.0072466132111706 | 0.055086075279273485 | 143.770676 |

- Fine-tuning on 2000 records: packed (question) 3343 s, host peak 11167714964 bytes; unpacked 11363 s, host peak 10607865574 bytes.
- Packed training throughput by batch size (median step, steps 3+): bs=1 0.88 decisions/s, bs=4 1.16, bs=8 1.10.
