# Laya weight quantization (step 1d): 2026-09-25/26 measurements

> Relocated verbatim from `zig/pkg/inference/models/laya/LAYA.md` (lines 1949–2102 at commit 3ce5aff4ea) on 2026-10-12. Durable decisions were folded into [LAYA.md](../../../../../zig/pkg/inference/models/laya/LAYA.md) before the move.

### Weight quantization (step 1d)

Released `laya` (unpacked) on the 760 step-0 eval decisions through the
serving evaluator, 2026-09-25, Apple M4 Max. Footprint is the process's peak
memory footprint, which includes Metal allocations.

| Weights | Backend | Accuracy | Soft CE | ECE | Peak footprint | Eval time |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| dense | Metal | 0.3868 | 1.3080 | 0.158 | 7.94 GB | 61 s |
| q8_0 | Metal | 0.3842 | 1.3067 | 0.158 | **5.09 GB** | 67 s |
| dense | CPU | 0.3868 | 1.3080 | 0.158 | 2.94 GB | 602 s |
| q8_0 | CPU | 0.3855 | 1.3075 | 0.157 | 3.47 GB | 1,587 s |

On Metal, q8_0 cuts memory by 36% at the same accuracy (two of 760
decisions change) and is about 10% slower. On CPU it was originally a loss:
the native q8_0 path kept prepared layouts beside the quantized bytes, and
its kernels were slower than the dense BLAS path for these shapes. CPU times
overlapped with training runs, so treat them as indicative only. It does not
meet the 5e-5 exact-serving bound and is not meant to; the fixture test
requires identical labels and probabilities within 2e-2
(`pipelines/laya_quantized_test.zig`). The fused resident Metal path reads
dense weights, so a quantized checkpoint runs the generic encoder.

**After the CPU kernel fix (2026-09-26),** same 760 decisions, ReleaseFast,
one job at a time (no other GPU work):

| Weights | Backend | Accuracy | Soft CE | Peak footprint | Eval time |
| --- | --- | ---: | ---: | ---: | ---: |
| dense | Metal | 0.3868 | 1.3080 | 7.92 GB | 48.6 s |
| q8_0 | Metal | 0.3842 | 1.3067 | **5.02 GB** | 48.1 s |
| dense | CPU | 0.3868 | 1.3080 | 1.06 GB | 490.5 s |
| q8_0 | CPU | 0.3842 | 1.3067 | 1.53 GB | 497.4 s |

q8_0 now runs at dense speed on both backends and saves 37% of Metal memory.
The CPU footprint stays above dense because dense weights are read through a
read-only mmap, which the footprint barely counts, while the Q8_0 bytes are
allocated. The earlier table's CPU numbers overlapped other jobs.

Two problems explained the CPU loss:

- **Wrong kernel selected.** `native_compute.zig` already has a
  dequant-once-then-Accelerate-SGEMM fast path for other quantized
  architectures (GLiNER, CLIP/CLAP), gated by weight-name heuristics. Laya's
  ModernBERT-encoder and decision-head linears matched none of them, so every
  Laya Q8_0 linear fell back to the native int8 dot-product kernel, which
  loses to Accelerate's SGEMM (used by the dense path) at Laya's shapes (rows
  in the hundreds to low thousands, `out_dim` 1024-5248).
- **Triple-counted memory.** The native kernel's on-first-use preparation
  (`prepareNativeQuantizedStorage`) keeps three representations of the same
  weight once a Laya linear is touched: the raw compressed bytes, a
  row-major "prepared" copy, and a 4-row panel copy for the int8 kernel.
  Each is close to the size of the compressed weight, so the three together
  land close to the size of the *dense* f32 weight — explaining why the
  table above shows q8_0 CPU footprint (3.47 GB) larger than dense
  (2.94 GB) despite Q8_0 packing to about 1/4 the bytes of f32.

The fix (`ops/native_compute.zig`): a dedicated `shouldUseLayaDequantSgemm`
predicate routes Laya's encoder/head linears through a transient
dequantize-into-scratch-buffer-then-SGEMM path (`layaDequantScratchSgemm`),
reusing the same `dispatchSgemmTransB` call the dense path uses; the scratch
buffer is freed every call, so no persistent dense mirror is kept (unlike the
GLiNER/ClipClap path, which caches dequantized weights up to a 512 MB budget
that would not fit Laya's ~330 M encoder parameters). `loadWeight` skips
`ensurePreparedKBlock` for the same weight names, so the row-major/panel
copies are never built at all: CPU footprint should now be dominated by the
compressed Q8_0 bytes alone (about 1/4 of dense f32) plus one small reused
scratch buffer.

One subtlety cost a wasted first attempt and is worth recording: the natural
predicate to reuse is `models/laya.zig`'s `quantizedLinear`, which is exactly
right for *checkpoint* tensor names (`"encoder.layers.N...."`,
`"head.layers.N...."`). But by the time a weight reaches CPU kernel dispatch,
`session_factory.normalizeWeightKey` has already rewritten those to runtime
keys (`"model.layers.N...."` for the encoder, `"model.head.layers.N...."` for
the head — see `laya_head.weight`). A predicate built on the checkpoint name
space silently never fires at dispatch time. `shouldUseLayaDequantSgemm`
matches the runtime key space instead, with a unit test
(`"laya dequant sgemm predicate matches normalized runtime keys, not
checkpoint names"`) pinning both spellings so this cannot regress silently
again.

**Verification.** `--test-filter "laya"` passes on both CPU and Metal (52 of
60 selected tests; the rest are CUDA-only, benchmark-only, or optional-path
skips), including the existing `pipelines/laya_quantized_test.zig` parity
test (max probability error ~1e-6 on each backend, well inside the 2e-2
gate) and two new `ops/native_compute.zig` tests: the naming-contract test
above, and an end-to-end test that dispatches a Laya-named Q8_0 linear and
checks both that it takes the dequant+SGEMM path (via the dispatch counter)
and that `QuantizedStorage.prepared.ownedBytes()` stays 0 afterward. The
generic `--test-filter "q8_0"` kernel suite (30 tests covering GLiNER,
CLIP/CLAP, and general GGUF Q8_0/Q8_1 decode) is unchanged, since the new
path only activates for Laya's specific runtime weight-name patterns.

**Re-measured after the fix (2026-09-26).** The shared build/GPU lock stayed
held by other agents' training and evaluation runs for most of this session
(one job alone held it 50+ minutes; a mid-session lock-policy change did not
help, since the in-flight holder had already committed to the old
combined-hold behavior). A clean window eventually opened. Time budget did
not allow the full 760-decision set at that point, so this is the first 200
of the 760 step-0 eval decisions (`head -n 200 td/s0-eval.jsonl`), same
checkpoint, ReleaseFast, ordinary Metal/CPU load (no other job running):

| Weights | Backend | Accuracy | Soft CE | Eval time | Max RSS | Peak footprint |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| dense | Metal | 0.415 | 1.3383 | 10.26 s | 1.37 GB | 5.76 GB |
| q8_0 | Metal | 0.415 | 1.3347 | **7.78 s** | 2.22 GB | **3.91 GB** |
| dense | CPU (native) | 0.415 | 1.3383 | 78.32 s | 4.87 GB | 0.97 GB |
| q8_0 | CPU (native) | 0.415 | 1.3347 | **77.33 s** | 5.35 GB | 1.32 GB |

("Max RSS" and "Peak footprint" are `/usr/bin/time -l`'s "maximum resident
set size" and "peak memory footprint" lines; they diverge because the
process mmaps the dense safetensors checkpoint read-only, and macOS's
footprint accounting undercounts clean file-backed pages relative to newly
allocated ones — see below.)

**Timing goal met on both backends.** CPU q8_0 is now on par with dense
(previously 1,587 s vs 602 s on the full set, contended — q8_0 2.6x
*slower*; now 77.33 s vs 78.32 s on this subset, q8_0 marginally *faster*).
Metal q8_0 is 24% faster than dense here, better than the prior table's
"about 10% slower" on the full 760; the difference in backend/subset
between the two measurements (this is a smaller, differently-ordered
subset, and a different codebase revision) means the two Metal numbers are
not directly comparable, but the qualitative result — q8_0 no longer
loses to dense — holds on both backends.

**Footprint: mixed, and worth explaining.** Metal footprint drops 32% (3.91
vs 5.76 GB), close to the prior table's 36% figure and reproducing it
independently. CPU footprint is *higher* for q8_0 (1.32 vs 0.97 GB
"peak footprint"; 5.35 vs 4.87 GB RSS), not lower. The reason is structural,
not a regression from this fix: dense weights are loaded via a read-only
mmap of the safetensors file, and those clean pages barely register in
macOS's footprint/RSS accounting once mapped; `quantizeDenseQ8_0` computes
Q8_0 into a freshly allocated heap buffer, which is real, counted, dirty
memory regardless of how compact it is. This fix still removes the
regression this same table used to show for CPU (footprint *larger* than
dense by keeping three prepared representations of every quantized weight);
what remains is the fixed cost of quantized bytes not being able to share
dense's free mmap ride. A tighter apples-to-apples comparison would need to
isolate weight-storage bytes specifically (e.g. `vmmap`/heap diffing across
the load step) rather than whole-process RSS/footprint, which is left as a
follow-up.

Reproduce with:

```bash
~/bin/zig build -Doptimize=fast --prefix <dir>
/usr/bin/time -l <dir>/bin/antfly-inference finetune eval laya \
  <laya-released-dir> <records.jsonl> --backend native   # dense
ANTFLY_LAYA_WEIGHT_QUANT=q8_0 /usr/bin/time -l <dir>/bin/antfly-inference \
  finetune eval laya <laya-released-dir> <records.jsonl> --backend native
```

The full-760-decision re-run at this fixed revision, and a weight-storage-only
footprint breakdown, are still open (see the work log).
