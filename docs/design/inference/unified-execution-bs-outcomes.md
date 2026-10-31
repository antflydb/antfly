# BS: Static native export families

Six Metal split-attention exports previously maintained their own admission and
stage/merge programs. They now instantiate one split-stage program and one
merge-stage program, with static policies for windowed decoding and global
F16/head-256 decoding. Single-row and batched bindings retain their original
query ownership. All admission predicates, their order, arithmetic, helper
calls and native symbol names remain unchanged. Historical model-specific local
names are retained only to preserve compiler inputs; the new policy owner uses
operation/layout names.

The F32, F16 and BF16 dense scalar/reduction exports now instantiate one Metal
export-pair template. Storage types remain explicit and continue selecting the
same native helper overloads. The distinct BF16 tiled program stays separate.
CUDA's five dense GQA signatures share their complete common parameter prefix,
including the original type spelling and order; optional decode scalars remain
with the appropriate instances.

Two CUDA tensor-attention schedules also share their exact online-softmax phase.
They retain separate static probability-publication expressions: immediate
half rounding into shared memory versus F32 register staging. The later
publication, scratch aliasing barriers, reduction order and native resources
are untouched. The visible-key-window phase now owns its complete conditional
scope, simplifying its two callers without changing emitted source.

## Preservation and accounting

The six original Metal stage/merge bodies and both original CUDA softmax bodies
are frozen in the local receipts. Each descriptor reconstructs its original
body exactly. Both generator freshness checks pass against the complete existing
native owners; none of their generated regions changes.

The full CUDA inference bundle (681,503 bytes), Metal Objective-C owner
(4,565,088 bytes), and CUDA encoder header (68,149 bytes) retain the exact SHA-256
values recorded in [BR](unified-execution-br-outcomes.md). CPU programs and
checked-in CUDA images are unchanged. No new device helper, runtime policy
branch or kernel launch is introduced. This is compiler-input preservation,
not an additional binary-size reduction or a new hardware timing claim. Prior
CPU/Metal/Glowworm performance results remain separately qualified in BP.

Against `5049c307c5a7308b44c8311e3c6f8716ec0ca012`, the complete ledger reports
**37 authored production lines / 5,800 bytes removed**, counting templates,
policies, instance metadata and adapters. All generated regions are unchanged.
Moving code between owners earns no separate reduction credit.

The 18-test Tile suite remains green (cached), both generator freshness checks
pass, and canonical license and Apache dependency-boundary checks pass.
Independent enabled-backend cache-identity qualification also passes. Frozen
bodies, complete before/after compiler-input hashes, build logs and the source
ledger are retained under `.benchmark-assets/1166/bs-campaign`.
