# BU: Shared row-dot program and Flash value publication

The four/eight-row CUDA linear path and four-row QKV path duplicated a complete
Q4/Q8 row-accumulation and publication program. They now instantiate one native
program with explicit array extent and warp-count policies. The linear schedule
retains four columns/eight warps and `Rows`-sized operand arrays; QKV retains
eight columns/four warps and four-row arrays. Their different grid mapping,
plane selection, scratch shapes and admission remain with their callers.

The shared program includes accumulator initialization, Q8 operand staging and
zero-filled row tails, full/tail Q4 dot paths, ordered warp partials, the block
barrier and final row/column publication. All operations retain their original
native spelling. The two-row implementation remains independent: its existing
qualification note records lower SM100 register use than the array schedule.
This batch makes no new register-use claim about that implementation.

The formerly manual linear helper is now a readable CUDA source owner at
`src/tile/families/native/cuda/q4_q8_rows.cu`, registered at its original position
in the translation unit. One family/region is added, with no function reorder.
The new source enters the checked input manifest and backend cache identity.

Metal Flash prefill also shares its P·V accumulation and full/tail output
publication templates across the head-256 and head-512 schedules. Key-tile
count, shared probability pitch, output owners, head width and launch stride
remain explicit format arguments. Score passes, softmax association, rescaling
policy, scratch offsets and barriers retain their previous implementations.

## Preservation and qualification

The complete Metal Objective-C owner and CUDA encoder header remain
byte-identical to the frozen `86936bc614` originals. Removing only Tile marker
lines makes the complete CUDA bundle byte-identical too. One-off reconstruction
checks compare both row variants and both Flash publication profiles against
their frozen source exactly; generator freshness verifies the complete outputs.

Glowworm rebuilds the canonical artifacts with pinned NVCC 13.2.86 and the
existing architecture/compression contract. Portable PTX (11,679,367 bytes),
compressed fatbin (15,000,488 bytes), and SM89 cubin (6,641,472 bytes) reproduce
their frozen bytes and the SHA-256 values recorded in BT. The owned remote
checkout is overlaid with the exact hashed source snapshot; its HEAD is not
the candidate commit. No new hardware timing or device-image reduction is
claimed, and CPU programs are unchanged.

Nineteen selected Metal renderer/phase tests pass on the host with zero skips.
The 18-test Tile suite remains green (cached), both generator freshness checks
pass, and formatting, canonical license, Apache boundary and independent enabled
backend cache-identity checks pass. Physical L4/A100 validation remains deferred
as previously agreed. Receipts are retained under
`.benchmark-assets/1166/bu-campaign`.

## Accounting and audit limits

Against `86936bc6149837c7a30320e85a80e4e0acb2f8b6`, the complete source ledger
reports **59 authored production lines / 2,771 bytes removed**, charging the new
native owner, shared templates, license header, policies and registration.
Generated regions grow by 119 lines / 4,784 bytes as the existing helper acquires
ownership; that transfer earns no reduction credit.

The bounded clone audit included private numerical helpers as well as exports.
Several apparent large duplicates were already outputs of a single generator,
which do not represent additional authored duplication. Three apparent
single-reference Metal helpers remain live through standalone template
instantiators or macro-supplied predicates and are retained. The lexical audit
is advisory, not a reason to delete programs based on text counts. No additional
proven-dead production program is removed in this batch.
