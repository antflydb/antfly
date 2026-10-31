# BQ: Shared standalone CUDA owners and image producers

Six standalone families still duplicated module loading, entry lookup and
retirement: scalar conversion/clamp, video preprocessing, attention-match
execution and fitting, training arithmetic, and boundary attention. They now
use the shared embedded-program owner. Fixed-image families retain a single
image descriptor; boundary attention retains its existing SM80/SM89/SM120 image
selection through a typed selected-image owner. The fitter reuses its dense
kernel enum as the export table instead of maintaining a second name list.

The common function accessor is explicitly inline. Launch dimensions, shared
memory, parameter packing, synchronization, scratch admission, cooperative
launch policy and numerical bodies remain with their existing schedules.
A normalization receipt verifies 102 function bodies are unchanged after
substituting typed handle access for the old handle fields. All 31 checked-in
CUDA image files preserve their previous SHA-256 values. No new whole-model
performance comparison is claimed for this batch; its kernel images and launch
work are unchanged.

## Generation and operation ownership

The standalone catalog grows from 12 to 17 images. Activation, paged attention,
gated recurrence, vision projection and encoder/decoder families now share ABI
entry tables with their launchers. The catalog records each native producer's
FMA/language policy; these policies are not forced to match across families.

The five existing verification scripts use the common offline regenerator,
with exact family selection so NVCC-only checks need neither Triton nor PyTorch.
All 17 images regenerate byte-identically with pinned NVCC 13.0.88 / Triton
3.7.1. Historical image filenames and exported symbols remain compatibility
contracts. The paged-attention source owner is operation-named, and a duplicate
license banner is removed without changing its surrounding include order.
Original historical qualification receipts retain their original paths.

## Failure ownership

Retained embedded owners preserve a module whose rollback unload fails and
retry retirement before accepting another load. No function table is published
until every lookup succeeds. Several older wrappers previously reset their
state after a failed unload, defeating that retention; they now reset only
after checked retirement succeeds.

The encoder owner also propagates child retirement failures. A failure-injection
test retires one child, fails the next child and verifies that the remaining
child/module handles survive for retry. Optional prefill library retirement
propagates workspace cleanup failures before closing the library, preserving
the existing successful cleanup order. Shutdown remains context-owned; callers
must retain an owner to retry a failed checked retirement.

Narrow test filters previously missed the owner test unless another CUDA test
made its module reachable. Explicit test imports now expose the host-only owner
and layout regressions directly; the CUDA owner import is excluded for WASM.
Initial no-match harness runs are retained separately from the successful runs.

## Qualification and accounting

The frozen CUDA suite passes 196 of 206 selected tests on Glowworm. Ten skip for
SM89-specific behavior, optional cuDNN/prefill libraries or external model
fixtures. Attention-match tests run with their explicit opt-in enabled; the
remaining device-specific case requires SM89. The suite includes the owner
fault-injection regression, video/scalar paths, attention-compaction recovery,
resident operations, projection/recurrence cases and the CUDA DeBERTa five-VJP
reference test. Host-only owner/layout tests pass two selected tests with no
skips. Tile tests (17), offline image-check tests (3), both generator freshness
checks, legal/dependency/license checks, engine-error freshness and independent
backend cache identity checks also pass.

The final byte-identical image check includes the canonical license placement
follow-up. The final WASM test-import guard changes no native test behavior or
serving code. Receipts and frozen binaries are retained under
`.benchmark-assets/1166/bq-campaign`, separately from earlier failures.

Against `82a8b97f58`, the production source ledger reports **10 authored lines /
1,312 bytes removed**, including replacement owners and metadata, with Zig tests
separated. The reduction is modest: most of the benefit is eliminating six
loader implementations and five independent regeneration paths while fixing
failure ownership. A rename or moving entry tables into the catalog earns no
reduction credit. Device-image and numerical-kernel sizes are unchanged.
