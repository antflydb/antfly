# W campaign progress

This follows qualified V commit `25aa23c94f49ca465e6baf9b1f116ea94a740c28`.
That commit is the fixed source-accounting and performance baseline. CPU and
Metal qualification has passed. CUDA images have been freshly regenerated and
match qualified V byte-for-byte. CUDA runtime/model qualification is waiting for
the existing Glowworm workload. This campaign is not yet fully qualified or
complete.

## Source reduction

After charging every replacement owner, helper, and metadata change, maintained
production source falls by **498,547 bytes and 7,666 lines**. CPU contributes
389,145 bytes against 1,926,232 baseline authored production bytes: **20.2024%**.
That conservative CPU total also pays for both shared source-emission utilities;
it receives no credit for the shared build/registry reduction. CUDA and Metal
contribute another 105,704 bytes together, with their shared owners fully charged.
Their individual 20% targets have not been reached.

These are maintenance-source reductions. Generated native expansions remain
checked in, and generated production source grows by 637,438 bytes. Physical
production text consequently grows by 138,891 bytes. This does not establish a
20% reduction in repository size, executable size, or native instructions.

## Implementation

CPU integer dots, Q8/K projections, grouped panels, worker schedules, and
activation lifetimes have static source owners. The owners preserve explicit
register names, acquisition order, unrolling, and stack/heap lifetimes. Unused
private helpers are retired using the Zig AST inventory. Two Q8 range paths use
shared native tile helpers; the single-row path keeps a separate stack frame.

GPU boundary operators and deterministic training attention share mathematical
owners with explicit native spelling policies. CUDA's five E4B FFN geometries
share an AOT owner while retaining their scalar/array states and column-first,
row-first, or packed-weight-first acquisition. Vision BF16 attention, projection
argmax publication, and scalar TurboQuant online attention have native phase
owners. M16/M32 MMA prefill retains each baseline's exact native declarations,
loop idioms, WMMA offsets, and shared-memory symbols.

Generation now batches all 128 native regions by file. It validates marker
uniqueness, overlap, and external helper placement before publishing a staged
file. A seven-region, three-file repair check restores exact bytes; external
helper drift rejects the affected file write.

## Qualification so far

The composed Mac suite passes 708 tests with 28 expected skips. The final
registry/source suite passes 23 tests; qualification tooling passes 24 tests.
Metal product checks, generated-source freshness, runtime shader compilation,
and GLiNER2 Metal parity pass. All 15 Mac model cases, fixed controls, independent
decoder references, and five 120-second soaks pass. Soaks clean allocator-live
bytes to zero. The changed Q8 ranges also pass the independent numerical tests
and the fixed 180-case direct performance campaign.
The x86-64 v2/v3/v4 checks and WebAssembly 32/64 SIMD builds also pass; these
are compile checks, not performance measurements on those architectures.

Both complete Metal programs remain byte-identical to V, and native host
dispatch/binding tokens are unchanged. Seven protected CPU kernel groups retain
their instructions, resources, and required alignment. The final Mac rebuild
also retains every loadable section byte and address of the qualified benchmark;
its 116 differing file bytes are linker UUID, object timestamps, and validated
ad hoc signature hashes. Whole-file hashes differ and are recorded separately.

Eighteen refactored complete CUDA functions preserve actual Clang tokens across
SM75/80/89/90/100/110/120 preprocessing paths. The fresh CUDA 13.2 build also
passes the native gate: all 445 exports retain identical instructions and
resources on all seven targets. Its PTX, fatbin, and SM89 cubin each match V
byte-for-byte. The completed V 21-profile/558-case direct-kernel qualification
therefore carries by complete image identity; this is explicitly reused evidence,
not a new timing run. Original result hashes and available fixture/harness-source
hashes are verified. Eight remote fixture paths were not individually pinned in
V's local fixture manifest and are listed explicitly; the carry does not depend
on claiming a new replay of those packs.

Six additional current direct profiles passed before competing work resumed
during the seventh. Their receipts and the interrupted attempt remain preserved.
The redundant direct-only resume queue was stopped before starting a profile;
the remaining device window is reserved for CUDA runtime/unit/GLiNER checks,
four model cases, and two soaks. Those runtime checks remain pending. The local
combined Metal/CUDA product check and final CUDA source-policy check pass.

The ignored `.benchmark-assets/1166/w-campaign/` directory retains source ledgers,
frozen binaries, source maps, native proofs, complete protocols, and all failed
or interrupted attempts. Earlier CUDA candidates and contention diagnostics
remain archived; later success must not erase those receipts or relax gates.
