# Integrating the OCR kernel families

Main through `41814339ff` (LightOnOCR3, PR #1250) is integrated with the
unified execution branch. The integration retains upstream's device/model
rollout gates and specialized schedules. It does not promote unqualified
shapes or devices.

## Operation ownership

The five new CUDA programs are exposed as gated-delta recurrence, paged BF16
attention, BF16 activation/norm fusions, vision fusions and encoder/decoder
fusions. Their typed entry manifests replace numeric kernel slots with named
operations. Native export spellings and PTX filenames remain the compatibility
ABI of the existing qualified images; no PTX regeneration or CUDA language
version change is necessary.

One transactional embedded-program owner replaces five loading/unloading
implementations. It publishes handles only after every symbol resolves,
unloads a partially loaded program on failure and permits retry from an empty
owner. This also avoids publishing stale partial Florence handles. A mock-driver
regression covers load failure, partial lookup failure and successful retry
without requiring a GPU.

One inline launch method supplies the common two-dimensional grid,
one-dimensional block, zero dynamic scratch and launch accounting at 22 native
launch sites. Its arguments retain each schedule's thread count and exact
pointer packet. The recurrence module retains its state, admission leases,
short-prefix history semantics, mixed-page handling and buffer release policy.

Fifteen Metal BF16 affine projection entry wrappers share a compile-time
Batch/Columns source family. The existing exact-batch schedules remain 2–4 rows
with two columns, 5–8 rows with four columns, and 9–16 rows with one column.
Their generated Metal text is unchanged. Model rollout policy stays in the
adapters rather than the operation source family.

Paged GQA split attention now shares its numerical staging and reduction tiles
between the existing qualified geometries and the new OCR geometries. The
shared stage owns KV acquisition, score/value work and online accumulation;
the shared reducer owns split statistics and output normalization. The four
entries also share two native ABI families. Profile-specific admission guards
remain explicit. This removes another 7,062 maintained native source bytes;
the entire expanded Metal program is unchanged.

CUDA qualification identity now covers the five module adapters, their shared
owner, all five embedded PTX images and their native producer inputs.

## Merge reconciliation

The canonical decode-state owner carries upstream's batched-step accounting;
the generation module continues to refer to that shared owner. New CUDA paths
use the existing shared output owners and retained tensor views. Allocating
BF16 projections reserve the upstream row-bucket capacity while keeping the
logical tensor shape. Prepared Into projections retain their supplied buffer
and the existing capacity-checked fallback. Fused FP16 bias, cross attention,
convolution and pinned OCR uploads preserve their upstream gates and behavior.
The new F32 row-chunk launch bound is combined with shared typed argument
packing. F16 resident-weight accounting is charged once after successful insert.
Metal hidden-state wrappers retain upstream's external-handle lifetime fix.

A shared Metal compile-options owner preserves both effects of the deprecated
`fastMathEnabled = NO`: safe arithmetic optimization and precise default math
functions. Setting only `mathMode = Safe` leaves the function mode fast on the
current framework. The three new library initializers use the complete policy.
A hardware-independent Objective-C regression compares the owner with the
framework's legacy setter and runs in both Metal qualification targets. A
negative control confirms that the prior Safe-only policy fails that regression.

## Source and equivalence evidence

Receipts are under `.benchmark-assets/1166/ocr-generic/`.

The scoped consolidation removes **10,393 maintained production bytes**, counting
the complete replacement owners, 829 added build-identity bytes and 902 bytes
of precision-check build integration. CUDA module
production text falls from 32,395 to 31,475 bytes; its line count rises from 674
to 693. The rollback regression adds 2,281 test bytes, and the independent Metal
framework regression adds 1,190 test/tool bytes separately. Metal
projection wrappers remove 4,630 physical source bytes. Unrelated upstream
changes and earlier campaigns are excluded from these figures. Combined with
Y's 5,537-byte reduction, these two batches remove 15,930 maintained production
bytes. This does not establish a 20% CUDA/Metal reduction or smaller native
instruction streams.

The complete merged main Metal program (988,879 bytes) and training program
(17,072 bytes) are byte-identical before/after wrapper consolidation. All host
preprocessing tokens match except literal source-line diagnostic tags. Against
upstream, all 45 newly introduced main-library kernels and all three separate
OCR Metal programs are byte-identical. All five CUDA PTX images are unchanged
from upstream. All 29 non-loader CUDA adapter methods match upstream token for
token after expanding the common inline launch and resolving named entries to
their original function slots. The CUDA producer sources are unchanged.

Validation results and remaining device qualification are recorded below as
checks complete. Byte/source equivalence is not a substitute for device or
model qualification of the combined upstream merge.

Local checks pass 150 selected host/Metal tests with one checkpoint-dependent
skip, 54 real Metal compiler checks, all 154 runtime cases, generated-source
freshness and the both-backend product build. The full selected test body also
compiles with both accelerators enabled. Its initial execution retains 53
`CudaUnavailable` failures on the Mac; the subsequent host/Metal execution
explicitly excludes those device-only cases. The rollback regression passes.
The pre-correction performance campaign is retained separately. Qualification
is restarted from the corrected precision snapshot; partial cohorts are not
carried forward. The final model/control campaign is recorded as it completes.
CUDA device qualification waits for the existing Glowworm workload; no new
CUDA performance result is claimed here.
