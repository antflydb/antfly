# BN: Direct-main qualification and smaller readback staging

This follow-up checks the original main-versus-branch performance limit directly,
instead of inferring it from BM's improvement over the previous branch head.
The frozen main executable is the exact BL baseline at `c8d46b3d47`, SHA-256
`7f59d7adc450c05ebdd3ccd4713ed7b6a8f5da925ee94e99d902e72668c8285b`.
Main through the merged `63cf18d980` adds no intervening native kernel changes.
The first candidate is the exact BM final executable, bound to `20bb6685d0`.

## Retained direct-main failure

The fixed four-case protocol uses 16 balanced process arms, 25 warmups and 301
measured iterations per process, with unchanged 1.05 median / 1.10 p95 limits
and byte-identical finite outputs. AA passes all cases. AB and reversed BA both
fail only the single-batch, 16-token MiniLM p95 gate: ratios 2.87252 and 2.75168;
median ratios are 1.00634 and 1.00291. All other cases pass in both orders.
The already-pinned batch-two case improves substantially, but that does not
supersede this newly demonstrated smaller-output failure.

The failing output is 24 KiB, below BM's 32 KiB pinning minimum, so it still uses
the pageable download route. The isolated change lowers generic bulk-pinning
admission to 16 KiB. No model identity or dimensions enter the policy. The
24 KiB output now uses the existing 32 KiB size class; it previously allocated
no bulk pinned buffer. The 4 MiB retention ceiling, growth synchronization,
checked frees, audit admission and allocation-denial fallback are unchanged.

## Safety and boundary checks

The four targeted CUDA tests are extended through the 16/24 KiB admission and
growth boundaries. They pass physically on Glowworm's GB10 (43 filtered suite
tests pass, zero skipped). The exploratory short-output matrix also passes all
supported 4/8/16-token cases in AA/AB/reversed BA. A one-token scout is retained
as an invalid-input result: the existing harness rejects lengths below four
before model loading. This is not a model correctness failure, and its failed
receipts are not rewritten as passing.

The final acceptance matrix includes the original four cases plus 4/8-token
MiniLM inputs, using 16 balanced arms and 301 measured iterations. Reversed BA
changes the arm order while keeping baseline/candidate binary roles fixed;
ratios and exit status therefore retain their ordinary new/old meaning.

The existing Metal dense-pair regression additionally passes with both
`MTL_DEBUG_LAYER=1` and `MTL_SHADER_VALIDATION=1`. Both validation layers report
enabled; all 40 filtered suite tests pass without skips or diagnostics. No Metal
production program changes in this follow-up.

All raw configurations, frozen executables, source patches, invalid-input runs,
failed direct-main comparisons and final acceptance receipts are retained under
`.benchmark-assets/1166/bn-campaign`. The native CUDA program sources and images are unchanged;
this host policy change earns no kernel source or fatbin reduction credit.
Physical L4/A100 qualification remains deferred. GPU/host exclusivity is not
claimed, and no affinity or global clock setting is changed.

## Final acceptance

All six cases pass in AA, AB and reversed BA: 36 timing gates pass, with finite,
byte-identical outputs in all 288 acceptance processes. Limits remain 1.05 for
median time and 1.10 for p95. Candidate/main ratios are:

| Case | AB median / p95 | BA median / p95 |
|---|---:|---:|
| MiniLM batch 1, sequence 4 | 1.00868 / 0.99867 | 0.99951 / 0.98200 |
| MiniLM batch 1, sequence 8 | 0.99918 / 1.00219 | 1.00206 / 1.04578 |
| MiniLM batch 1, sequence 16 | 1.00354 / 0.99852 | 0.99878 / 0.99619 |
| MiniLM batch 1, sequence 128 | 1.00100 / 1.00135 | 0.99991 / 0.99931 |
| MiniLM batch 2, sequence 16 | 0.37877 / 0.34605 | 0.38322 / 0.40673 |
| Q4_K reranker | 1.00118 / 1.00200 | 0.99879 / 1.00170 |

The previously failing 24 KiB output now matches main's warmed p95 in both
orders. The 48 KiB case is 61.7–62.1% lower in median time and 59.3–65.4% lower
at p95. Across the other cases, maximum median/p95 ratios are 1.00868/1.04578.
The AA control's maximum ratios are 1.00115/1.02001.

This establishes controlled GB10 warmed execution parity for the final source;
it does not erase the retained failures of earlier revisions or establish
physical L4/A100 parity. First-use latency is a separate limitation: single-batch
16-token first-warmup times are 1.43–1.78 ms versus main's 0.98–1.00 ms
(1.434–1.822 times, about 0.43–0.80 ms extra). The new path requires first-use
pinning, and these totals also include other cold preparation. No cold-latency
or end-to-end server throughput parity is claimed.

All remote executables match their frozen local hashes, and the production CUDA
source hash matches the qualified prototype input. Archive integrity verifies
552 successful output hashes across the original, scout and acceptance runs;
24 rejected one-token preflight runs remain recorded. License checks pass.
The follow-up changes only the existing CUDA size policy and boundary tests;
there is no new native program, generated owner or model-specific dispatch.
