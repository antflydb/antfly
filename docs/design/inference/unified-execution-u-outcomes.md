# U campaign outcomes

This is the U01–U56 result for issue 1166. Implementation commit c9c340e200 includes origin/main 27011cf1c3b4f0768cbd1578ceb82d8d74e52382 through merge df86c32e32. The U accounting baseline remains b7568c5ffc8daaff4f4c6f0409a746052f71d172; earlier campaigns and later main-only changes receive no credit.

The final source fingerprint is ba6eea8662f24e7330e116ee0681a481638e1887f9cd8558047436cac40558c8 (5,553 inputs). The Mac executable SHA256 is ced424c23ab71999eaba3173aa5e72d6cf02a8b763e0b039fa4e0fb5f8d564f7; CUDA is 8003df012738b90e479d97a6c7827dcb45cdb493009c188fb3af895f213a4899.

The U-only net removes **1,626 production lines / 44,677 bytes**. The consolidation cohorts remove 2,175 lines / 79,091 bytes before separate repair/capability charges; the 29-line / 1,081-byte unused alias retirement is separate. Generated output, tests, prior work and main-only changes are excluded. Metal shader source has zero U delta; CUDA device source adds 287 lines / 12,593 bytes for correctness/capability work. The reductions are in CPU kernels/ownership and Metal/CUDA host orchestration.

Validation passes all 19 supported models, 11 required controls, 31 additional CPU widths, 1,554 paired Metal and 558 CUDA direct cases, seven 120-second soaks, independent decoder references, affected training/failure suites and five portable compile configurations. x86 runtime/performance qualification is not claimed.

CPU primitives use the identical isolated, aligned benchmark on reference and candidate; the original failed harness arms remain available. Other model comparisons use the frozen corrected canonical baseline. The independent decoder maximum error is below 0.000759 at tolerance 0.002. Expected backend-disabled/optional-fixture skips remain explicit. The main portable PTX 9.2 image and CUDA TinyLlama profile remain unsupported on this driver/device profile; pinned training PTX 8.7 loads successfully.

The executable-size ledger is separate from source savings. Canonical corrected baseline / final sizes are 18,884,344 / 18,904,520 bytes on Mac and 132,253,160 / 132,523,760 bytes on CUDA. The benchmark/descriptor/capability additions increase total executable size; protected hot functions retain their qualified instruction streams and resources.

Raw arms, sources, binaries, per-cohort ledgers, consumed-owner evidence, fixture/tool hashes, calibration/oracle repair witnesses and failed prototypes remain under .benchmark-assets/1166/u-campaign/. The register there is packages.json; the combined acceptance record is final-evidence.json.

| ID | Outcome and retained constraints |
| --- | --- |
| U01 | Frozen merged starting closure, corrected binaries, device/tool identities, failures and raw prototype arms. Later main-only paths excluded. |
| U02 | Explicit cache-owned storage versus borrowed tensor and capture aliases; physical leases retired before cache storage. |
| U03 | Resident-key ownership, duplicate admission and every partial construction/teardown failure preserve retained storage. |
| U04 | Laya route assertions use its actual production capability predicate; Spark fallback outputs remain tested. |
| U05 | Public admission error normalized; eviction, budget release and retained resident bytes asserted. |
| U06 | Strict batched cuBLAS route, pinned library/device references, deterministic native SM120 forward/owned gradients and exact resume. Legacy SM8 images retained. |
| U07 | Integer dtype/stride/VJP admission reproduced independently, including beyond-F32-exact values and wide invalid integers. |
| U08 | Metadata/K16/Metal phase prototypes admitted separately; rejected generic accumulator and ABI-only CPU prototypes retained. |
| U09 | One checked reshape resolver with static caller compatibility policies; 100,000 comparisons against frozen bodies. |
| U10 | Existing right-aligned broadcast owner now includes the loose three-input policy; no new materialization. |
| U11 | Existing Metal compressed-format vocabulary owns wire tags and exact row extents; no second enum. |
| U12 | Exact typed-index admission consumed by actual CUDA gather/scatter adapters; legacy float semantics remain separate. |
| U13 | Four I32/I64 F32/BF16 gather exports share narrow typed-load leaves and compatible old traversal. |
| U14 | Two matching scatter/VJP exports preserve duplicate atomics, normalized bounds and device-only execution. |
| U15 | Complete supported GLiNER2 CUDA forward/backward/optimizer/resume cases pass. |
| U16 | One owned-F32 conversion metadata/cleanup owner preserves logical prefixes, all dtype conversions and failed shape cleanup. |
| U17 | Static K16 activation owner retains ordinary versus three-block predicates, vector counts and instruction streams. |
| U18 | Static Q4K MR1/MR2 phase owner retains exact one/two-row accumulator extents and codec schedule. |
| U19 | Shared Q4/Q5 panel16 MR2 raw/prepared-sum phases keep corrections and sum acquisition at old positions. |
| U20 | MR4 panel16 uses matching shared phases with unchanged tails, live planes and register schedule. |
| U21 | Packed-QKV MR1/MR4 publication phases share matching owners; bias and block-loop schedules stay distinct. |
| U22 | Matching Q4/Q5 versus Q6 block4 MR2 assertions/Q8 loads/init/publication shared; codec loops, block8 and SDOT schedules intentionally retained. |
| U23 | Typed two/three-projection Q8 workers keep exact contexts, partitions, callback lifetime and cancellation/drain. |
| U24 | Legacy workers and prepared pair/triple phase body share static arity; original named accumulator planes, scalar ABI leaves and math order retained. |
| U25 | Allocating and Into quantized attention blocks share their exact orchestration; fresh and borrowed output responsibilities remain explicit. |
| U26 | Dense/paged Into paths share matching 59-argument binding and timing publication; providers/pages remain separate native leaves. |
| U27 | Three compressed dense/gated/quantized consumers share full-KV acquisition, borrowed-span inputs and lexical cleanup; different gated/native submission schedules retained. |
| U28 | Matching Layer/RMS norm sample setup/publication shared; norm arithmetic and saved statistics stay local. |
| U29 | F16/BF16 embedding setup uses static storage policy; original widening and native wire unchanged. |
| U30 | Compressed-format/readiness/span binding phases consolidated through existing owners; active-frame/provider cache lifetimes preserved. |
| U31 | Finite/debug observation uses scoped borrowed views; separate fixture mirror ownership repair prevents retained host copies. |
| U32 | Replaced wrappers retired; actual runtime compilation, replay/reset, retained outputs and partial-failure fixtures execute shared owners. |
| U33 | GQA route accounting and compressed paged fresh-output setup consume existing owners; cache mutation/host fallback remain local. |
| U34 | Split-K-online/score-prework share existing paged wire record with original live scalar field addresses and scratch extents. |
| U35 | Q4K ordinary/tiled triple-bias paths share checked launch admission while retaining native dispatch geometry. |
| U36 | Q8 TC/raw launch owner consumes named row/column extents and variant wire policy; separate tile-extent/bias-slot correctness repair covered by 48 F64 cases. |
| U37 | DeBERTa query-tile metadata shared statically; native shared memory and block/grid schedules unchanged. |
| U38 | Existing MTP request owns checked lengths and borrowed-choice classification for Metal/CUDA; encoding, rollback and device choices remain local. |
| U39 | Static donation choice owns both where-consume traversals without changing retained inputs, aliases or error cleanup. |
| U40 | Replaced attention/launch setup retired; measured-negative tuples and mutable address records retained rather than padded/generalized. |
| U41 | CPU/Metal/CUDA ordinary norms share checked dense-row geometry, retaining backend representation and weight policy. |
| U42 | Assigned Metal norms execute existing prepared LayerNorm, mutable GLiNER LayerNorm or SIMD RMSNorm schedules; query never prepares. Resident parameter identity collision repaired separately. |
| U43 | Actual Metal F32 linear Into consumes prepared immutable slots; cold/mutable admission declines and queries allocate nothing. Single-row native schedule retained. |
| U44 | Ordinary softmax/log-softmax row admission shares the existing descriptor; reduction/nonfinite behavior unchanged. |
| U45 | Logical RoPE lowers and executes on CPU/Metal/CUDA assigned outputs, with checked per-invocation positions and real CUDA capture replay. MRoPE/table routes retain distinct indexing. |
| U46 | Four matching Metal activation identities reuse the existing enum statically; exact/approximate GELU, scalar/libm/FMA and SIMD math leaves retained. |
| U47 | Compatible ordinary norm/linear/RoPE adapters consume actual shared Into bodies; no temporary GPU tensor, registry, readback or extra submission. |
| U48 | Independent F64 cases plus query-purity, alias, malformed geometry, repeated replay, fresh inputs, retained outputs and partial-OOM coverage for every new descriptor consumer. |
| U49 | All consumed owners and measured retentions reconciled; U-only net removes 1,626 production lines / 44,677 bytes after charging shared contracts, repair/capability costs and the separate 29-line dead-path retirement. |
| U50 | Final 5,553-input closure and both executable identities frozen, with 4,734 fixture/tool identities, actual shader expansion, all pinned CUDA images and the separate legacy/SM120 training image provenance. |
| U51 | All seven native CUDA targets audited: 438 existing exports retain instructions/resources; six typed exports and the one all-masked attention repair are separately identified. |
| U52 | Seven native hot functions preserve counts/resources and required alignment; four address-only changes resolve to identical data. Independent boundary references and all 39 final CPU controls pass. x86/WASM compile checks make no unavailable-ISA runtime claim. |
| U53 | All 1,554 Metal and 558 CUDA direct cases pass, including numerical, guard and resource gates. Both Metal creation orders use the declared paired estimator. Missing BF16 warmup count and two stale CUDA all-masked oracle outputs are repaired with original failures retained. |
| U54 | All 19 supported models, 11 controls and 31 additional short widths pass the unchanged timing/finite/exact-output gates. CPU/Metal decoder outputs pass independent F64. Forced routes and payload counters match; ONNX encoders improve 353 to 299 through the separately tested cache-identity repair. |
| U55 | Repaired ownership, training, replay, descriptor and failure suites pass across both hosts, including all 10 GLiNER2 cases and six normalization materialization-failure combinations. Seven 120-second soaks pass with zero tracked ownership at teardown, no unexpected driver errors and no new skips. Explicit PTX/decoder negatives remain. |
| U56 | Implementation and reports committed locally; frozen evidence and failed prototypes retained. Scoped cache policy applied only to task worktrees: 22.2 GiB freed on DGX, local cache under budget, two verified obsolete output prefixes removed. No push. |
