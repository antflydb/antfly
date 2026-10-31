# BT: Paired-projection ownership and dead generated helpers

Two live CUDA helper programs still sat between generated regions in the native
bundle: the Q4/Q8 paired-activation program and the fixed-row FFN program. Their
existing specialization families now own the helper definitions as well as the
exports. No function moves relative to another function; only the generator
begin markers move ahead of their helpers. Editing either generated helper now
participates in normal freshness checking.

The helpers share their dual dot-product and warp-publication phases with the
existing dense/routed projection families. Dense 32-bit indexing and routed
64-bit indexing remain distinct static instances. Full-tile and tail paths,
column/warp counts, accumulation order, activation and scratch publication keep
their original native spelling. Historical device names remain compatibility
contracts; the new source owners and shared phases use operation names.

The remaining F32/F16/BF16 single-row Metal scalar/reduction exports now use one
storage-typed template. Their private 32-lane reduction storage stays distinct
from the multi-row family. The complete Metal Objective-C source and CUDA
encoder header remain byte-identical to the preceding revision.

## Dead-code audit

A lexical inventory of the canonical CUDA bundle and all recursively included
project headers found one generated helper with no reference beyond its own
definition: `termite_q6k_scale_f32`. Its private generator fragment was only
listed in the runtime shared-helper array, with no standalone family consumer.
The fragment and array entry are removed. Live Q6 value, integer and subblock
scale decoders remain in place; their common recipe components are still used.

After removing generator marker lines and precisely that unused definition,
the complete CUDA bundle remains byte-identical to its frozen original. The
inventory also identifies 106 helper definitions outside generated regions in
the bundle itself. That is a scoped lexical inventory, not a duplicate count
or an assertion that those helpers are dead. It excludes definitions in included
headers. Moving these unique primitives alone would not earn source-reduction
credit. This batch does not claim that every native helper has been generated.

## Qualification

Glowworm rebuilds all three canonical artifacts using pinned NVCC 13.2.86 and
the existing generator's architecture/compression settings. The final candidate,
including the dead-helper deletion, reproduces the frozen artifact bytes:

| Artifact | Bytes | SHA-256 |
| --- | ---: | --- |
| Portable PTX | 11,679,367 | `e7ebbb79119fda3e33a04fe36f77b8b59d383bcc5be795c4d547942d3a2c626c` |
| Compressed fatbin | 15,000,488 | `a96d134d0613477953ebb10e005e4d8da109e403a93ee7ffc4bb81dc64fd6df9` |
| SM89 cubin | 6,641,472 | `6808bf313d8d24fbeed4666e971d19025c2ff40a79cfe052ca5d0ee4cf5f493c` |

The fatbin retains native SM75/80/89/90/100/110/120 images plus portable SM75 PTX.
Commands follow the canonical compiler contract; local generator freshness is
checked separately before the hashed source snapshot is uploaded to the owned
qualification checkout. Its remote HEAD is not the candidate revision. Initial
pre-deletion artifact receipts remain separate from the final receipts.

Nineteen selected CUDA renderer/standalone-attention tests pass on the host with
zero skips. The 18-test Tile suite remains green (cached), both generator
freshness checks pass, and final license, Apache boundary and independent
backend cache-identity checks pass. Receipt roots are
`.benchmark-assets/1166/bt-campaign` and its `final` subdirectory.

No new hardware timing result is claimed: CPU programs and checked-in device
images are unchanged, and the rebuilt native images preserve their bytes.
Physical L4/A100 validation remains deferred as previously agreed.

## Accounting

Against `6d537d164f112f7765ec2276b0661f34c14c5f05`, all replacement owners and
metadata together remove **1,470 authored production bytes**, while adding a
net **four authored lines**. Generated regions grow by 136 lines / 5,431 bytes
because the two formerly manual helper definitions acquire generator ownership.
That transfer earns no reduction credit. The size reduction is modest; the
primary result is closing two ownership gaps, removing a verified dead decoder
and retaining exact native images.
