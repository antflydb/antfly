# AO: shared FP16 row normalization

Baseline: `55edccd11c`.

Ordinary and window-packed CUDA normalization now consume one private lexical
recipe for row statistics, the original warp-merge reduction, affine arithmetic
and FP16 publication. Only the destination row is parameterized. Input row
selection, window geometry and admission remain in their existing kernels.

This removes **1,281 maintained CUDA source bytes / 24 lines** from
`florence_fused.cu`: **10,913 → 9,632 bytes (11.74%)**. The helper, comments and
call sites are counted; no production implementation moves into tooling or
stored expansions. This is a file-specific reduction, not a whole-backend one.

## Acceptance

- CUDA 13.0 V13.0.88, `-ptx -arch=compute_80 -O3`, matching the production
  `scripts/lightonocr3/verify_cuda_florence.sh` recipe.
- All **256,056 expanded tokens**, including pragma directives, match.
- The complete before/after PTX images are byte-identical to the checked-in
  `florence_fused.ptx`; the shipped device program and its performance remain
  unchanged. No additional GPU timing is required for unchanged driver input.
- Generated-source freshness and CUDA artifact source policy pass.

PTX SHA256:
`d4f91e70390ffb1e571526c14370582584cc5c657d316245106ab576d0a0a5f8`.

The first compiler proof inherited `compute_75` from the recurrent script:
its two variants matched each other but not the `compute_80` shipping image.
That failed comparison is retained. The corrected target proves equality to
the actual shipped module, without changing kernel source or retrying timing.

Evidence: `.benchmark-assets/1166/ao-campaign/`, including `verified.json`,
source accounting, the failed wrong-target comparison and the complete
`norm-static-v2.tar.gz` proof. The immutable compiler-only remote snapshot is
`/home/ajroetker/antfly/.worktrees/1166-ao-f16-norm-v2`.
