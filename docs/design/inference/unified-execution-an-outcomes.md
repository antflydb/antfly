# AN: shared recurrent Q/K scan

Baseline: `6da3b7a20a`.

## Maintained source

Fresh-prefix, ragged and prefix-only ragged CUDA recurrence now consume one
private lexical recipe for the padded eight-token Q/K staging, recurrent scan
and final state publication. Each kernel keeps its packet ABI, page/head
mapping, initialization policy, source offsets and prefix-only admission.

The recipe preserves the existing shared-memory padding, barriers, unroll
policies, register arrays, XOR reductions and arithmetic/store order. Expanding
in the caller avoids changing the compiler's function-inlining boundary.

This removes **3,383 maintained CUDA source bytes / 68 lines** from
`lighton_qwen35_recurrent.cu`: **34,369 → 30,986 bytes (9.84%)**. It replaces
three scan bodies with one owner; it is not a 9.84% reduction of the whole CUDA
backend. The new macro and its call sites are included in that accounting.
No implementation is moved into generated files or qualification tooling.

## Acceptance

- CUDA 13.0 V13.0.88 compiles both complete module sources with
  `-ptx -arch=compute_75 -O3 --fmad=false -std=c++17`.
- All **231,021 expanded tokens**, including pragma directives, match.
- Both complete emitted PTX images are byte-identical to the checked-in
  `lighton_recurrent.ptx`. The shipped driver/JIT input is unchanged, so this
  consolidation does not change its device execution performance.
- Generated-source freshness passes for **70 files + 129 native owned regions**.
- CUDA artifact source policy passes.

PTX SHA256:
`f0e2f9a87724fa2a78a45a246e4c6519c48ee213eb38474e21b94bb054c8f324`.

Evidence: `.benchmark-assets/1166/an-campaign/`, including the immutable
before/after sources, compiler command lines, token proof, full PTX archive,
source accounting and `verified.json`. The compiler-only remote snapshot is
`/home/ajroetker/antfly/.worktrees/1166-an-recurrent`; it runs no GPU jobs.
The existing `scripts/lightonocr3/verify_cuda_recurrent.sh` independently
rebuilds and compares the production image before numerical validation.

AM's ownership-fix device qualification remains separately queued behind other
Glowworm work. Its acceptance is not inferred from this source-only proof.
