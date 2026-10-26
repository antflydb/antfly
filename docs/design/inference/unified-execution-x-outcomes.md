# X campaign progress

This continues from merged W commit `2ae70fe43d`. Source and qualification
receipts are under `.benchmark-assets/1166/x-campaign/`. The separate CUDA and
Metal 20% maintained-source targets remain open.

## Native source ownership

Nineteen additional Metal ABI families now cover 39 kernel entries, including
multiline BF16 matrix, relative attention, compressed KV, projection and audio
signatures. Together with W, 47 families cover 242 entries. Each signature keeps
its original argument names, address spaces, buffer slots, types and builtins.

Five canonical native attention phases supply staging, accumulator setup, score
rows, online weights and publication to both dense and paged compatibility
schedules. Four numerical fragments also supply specialized flash rendering:
online-state initialization, accumulator initialization, the 32-lane causal
softmax update and final normalization. Kernel admission, KV acquisition,
threadgroup storage and loop schedules remain explicit at their call sites.

The owner emits C string fragments and standalone native template expansions
before native compilation. The generator now checks 69 files and 129 owned
regions. Unknown phase references reject generation. The Metal renderer, phase
owner and compatibility template participate in JIT qualification identity, so
editing these authored inputs invalidates stale qualification.

Against the fixed merged W baseline, maintained production source falls by
**4,531 bytes**, with the complete replacement owner, metadata and helpers
charged. Maintained line count increases by 82. Generated production source
increases by 3,552 bytes, so physical production text falls by 979 bytes.
Inline tests add another 523 bytes separately. These figures establish neither
a 20% GPU reduction nor a smaller native instruction stream.

## Qualification

Both complete Metal programs are byte-identical to the baseline: 922,886 bytes
for the main library and 17,072 for deterministic training attention. Native
host dispatch/binding tokens are also identical. All seven protected CPU groups
retain their instructions, resources and required alignment; changed addresses
resolve to identical data.

The final local source/ownership/selected-KV scope passes 41 tests without skips.
Real Metal compilation checks and all 154 runtime cases pass. GLiNER2 passes
three enabled CPU/Metal cases, with seven CUDA-disabled skips. Both-backend
product checks, source policy and source-generation freshness pass. All 15
model cases and 42 primitive/host controls pass the unchanged latency and
numerical gates, with matching route/resource counters. Both independent
decoder references and all five 120-second ownership soaks pass; tracked live
allocator bytes return to zero. All five portable compile checks (x86-64
v2/v3/v4 and WebAssembly 32/64 SIMD) and 24 qualification-tool tests pass.

CUDA translation units, include closure and all three images are unchanged.
Device qualification remains behind Glowworm's existing workload. The new
source-pinned controller archives each interrupted attempt and restarts only
after another quiet window. It never accepts partial timing cohorts or retries
a test failure. All 21 direct case packs, the direct harness and the qualified
model baseline are pinned in addition to source and helper inputs.
