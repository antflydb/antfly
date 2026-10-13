# Laya base-size encoder (step 2d): 2026-10-03

> Relocated verbatim from `zig/pkg/inference/models/laya/LAYA.md` (lines 1448–1497 at commit 3ce5aff4ea) on 2026-10-12. Durable decisions were folded into [LAYA.md](../../../../../zig/pkg/inference/models/laya/LAYA.md) before the move.

### Base-size encoder (step 2d, 2026-10-03)

Can a ModernBERT-base encoder replace Laya-large (395M) at about 40% of its
size? Two base trunks were tested:
- **ModernBERT-base:** plain `answerdotai/ModernBERT-base`.
- **Antenna-base:** the Antenna trunk (run21), ModernBERT-base feature-distilled
  from gliner2.5-base's encoder ([ANTENNA.md](../../../../../zig/pkg/inference/models/antenna/ANTENNA.md)).

Both start from the released checkpoint's decision settings with a fresh
768-wide head (`scripts/antenna/init_decision_head.py`, `--hf-encoder` or
`--student`). Training is unpacked on `s0-train`, evaluated on `s0-eval`
(760 decisions), RLCD, on resident Metal.

**Documented step-0 recipe** (1 epoch, gradient accumulation 5, encoder rate
2.5e-5, head rate 1e-4):

| Trunk | Seed 42 | Seed 43 | Seed 44 | Mean | Train time | Peak footprint |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Laya-large | 0.599 | 0.628 | 0.637 | **0.621** | 22 min | 26.4 GB |
| Antenna-base | 0.555 | 0.466 | 0.493 | **0.505** | 12 min | 11.5 GB |
| ModernBERT-base | 0.451 | 0.446 | 0.421 | **0.439** | 12 min | 11.5 GB |

Laya-large's seeds are the ones in [Packed vs unpacked at equal budget](../../../../../zig/pkg/inference/models/laya/LAYA.md#packed-vs-unpacked-at-equal-budget-2026-09-26).
Rerunning seed 42 on this machine gave 0.600.

**Three epochs, no accumulation** (15× the updates, otherwise the same):

| Trunk | Seed 42 | Seed 43 |
| --- | ---: | ---: |
| Laya-large | 0.447 | |
| Antenna-base | 0.662 | 0.488 |
| Antenna-base, LoRA rank 16 (base frozen) | 0.563 | |
| ModernBERT-base | 0.453 | 0.467 |

For reference, a head trained on the frozen Antenna trunk scores 0.511 (Open-Jev,
then `s0-train`; see ANTENNA.md).

**Reading.**
- **Gate not met.** Neither base trunk comes within tolerance of Laya-large:
  Antenna-base trails by 0.12 and ModernBERT-base by 0.18.
- **The GLiNER distillation helps decisions.** Antenna-base beats plain
  ModernBERT-base by 0.07 on the documented recipe and by 0.11 on the
  longer one.
- **Fine-tuning the trunk buys little for Antenna.** Averaged over seeds,
  full fine-tunes of Antenna-base (0.505 and 0.575) are close to the
  frozen-trunk head (0.511), which keeps the trunk shared with the GLiNER
  heads.
- **The longer recipe is unstable.** Antenna-base scored 0.662 and 0.488 on
  two seeds, and it drops Laya-large to 0.447. Use the documented recipe for
  comparisons.
