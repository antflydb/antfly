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

## Main integration, 2026-10-26

The pending merge incorporates upstream `b0b39178d60c13617ca1f9c79b00f706093da244`
into W checkpoint `e2cde5dd64`. The source-reduction totals and qualification
above describe the pre-merge checkpoint; upstream additions receive no W
reduction credit. The merged product needs its own evidence.

Upstream selected-KV/query-capture state remains in the shared decoder-state
owner. Its task adapter supplies the optional GPU-oracle policy explicitly.
CUDA retains the shared symbol-loading/clearing registry, including explicit
native-name exceptions for repeat attention and staged BF16, hardware gates,
and default-off E4B routes. CUDA function handles have a distinct opaque type
with the same pointer ABI, so reflection identifies functions by type and cannot
load or clear a request cancellation context as a kernel. The driver ABI also
participates in the JIT qualification fingerprint. Flash prefill shares a geometry template specialized
by KV-head count, so E2B and E4B can coexist in one bundle without the first
wrapper fixing both layouts. The JIT input graph includes the newly introduced
native template headers.

The selected Metal scorer initially exposed upstream test and implementation
failures. Test requests now provide their required cancellation context and
publish contiguous-row metadata after writing a native slot directly. The
native scorer checks the full 512-slot attention array: its former 256-slot
bound rejected every slot in the leased paged-KV namespace. The five-format
GPU oracle now checks scores before/after repacking and explicit-position
causal attention, with an assertion that it uses a paged slot. The EOS default
regression lives with its canonical model-definition owner.

Focused CPU/Metal tests pass 221 cases with four expected skips; the typed CUDA
host/registry/ABI regressions pass eight cases, and selected-KV tests pass all 15
without skips. GLiNER2 passes its three enabled CPU/Metal cases, with seven CUDA
cases disabled on this product. All five portable compile checks pass again:
x86-64 v2/v3/v4 and WebAssembly 32/64 SIMD. Seven protected native CPU groups
retain their instructions, resources and required alignment against qualified W;
changed data references resolve to identical bytes.
Both-backend product compilation, 69-file/128-region source freshness, generated
Metal compilation, and Metal runtime correctness checks pass. This Mac has no
offline Metal compiler: the recorded compiler bridge invokes the actual GPU
`newLibraryWithSource` compiler, validates nonempty libraries, and records every
source hash/result. No offline AIR qualification is claimed.

CUDA 13.2.86 regenerated all three merged images in an isolated native ARM
Linux compiler VM. All pinned inputs were verified before and after generation;
the initial incomplete remote-input verification and subsequent repairs are
retained. Baseline image bytes match incoming main exactly. All seven native
targets contain 454 exports, retaining the previous 445 and adding nine upstream
exports. Against qualified W, 442 existing exports retain exact instructions and
resources on every architecture. Two changed exports match upstream exactly.
The remaining HD256 flash-prefill export matches upstream on SM100/110/120;
on SM75/80/89/90 it has scheduling differences with unchanged resource usage
and unchanged instruction counts except eight fewer instructions on SM75.
These differences require device qualification; the compiler VM supplies no
device correctness or latency evidence. All 24 artifact/tool/ABI tests pass.
The remote regeneration attempt stopped only its own process group when other
Glowworm work resumed. CUDA device validation continues to wait for that work.
Fresh CPU/Metal comparisons against upstream use an isolated main worktree with
only the identical benchmark driver and its build registration overlaid; its
production sources remain unchanged. All 13 encoder/reranker cases, 39 primitive
controls, and the native decoder pass against that main baseline. The main Metal
decoder fails all 16 chunked-prefill arms at maximum error 0.000249102; the gate
remains 0.0001. The candidate passes all 16 arms and both independent decoder
oracles. Its complete Metal decoder output is byte-identical to qualified W.
A separate unchanged 32-arm comparison against that correct W binary passes
p50/p95 ratios 1.01023/0.99409. This does not convert the failed main comparison
into a passing one.

One initial short host-encoding control misses p50 at 1.06908, while its longer
model control passes. The predeclared A/A diagnostic passes at 1.01543; the full
three-case B-first and original A-first confirmation cohorts both pass unchanged
5% p50 / 10% p95 gates. Every original arm and the initial miss remain available.
No production performance change or relaxed threshold is credited for these
confirmations. Both decoder references and all five 120-second ownership soaks
pass; tracked allocator live bytes return to zero.
The final CPU/Metal executable retains every loadable section byte and address
of the benchmarked merged executable. A complete-file comparison accounts for
UUIDs, debug object timestamps/cache paths and validated ad hoc signature page
hashes; nothing else differs. This carries those CPU/Metal results across the
CUDA-only type/artifact changes and build scheduler correction.

An isolated, task-owned native ARM Linux VM now boots for compiler-only CUDA
regeneration using the existing hash-verified CUDA 13.2 and Zig packages. It has
two CPUs, 4 GiB RAM and a 20 GiB disk; the original VM/default connection is
unchanged. No device performance evidence will be attributed to this builder.
The task-owned VM was removed after compilation and static review; the original
VM and default connection were preserved.
The CUDA-wide test scope compiles without running device tests. Its dual-backend
macOS build initially peaked at 15,131,361,280 bytes, exceeding the original
14 GiB reservation. The final regenerated-image build peaked at 20,664,074,240
bytes and exceeded the intermediate 16 GiB reservation. Both failures are
retained; the final scheduler reservation is 24 GiB. Single-backend reservations
and runtime/performance gates are unchanged.
Receipts, failed attempts, tool identities and complete inputs live separately
under `.benchmark-assets/1166/w-campaign/merge-main-20261026/`.
