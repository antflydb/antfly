# Issue #1166 acceptance map

This maps the original issue checklist to current implementation owners and
executable evidence. It is a review map, not a declaration that every possible
model/backend signature is supported. Unsupported semantics and unqualified
hardware remain explicit. Campaign receipts live in the worktree's ignored
`.benchmark-assets/1166/`; published outcome registers retain their pinned
baselines, failures and hardware limitations.

| Original criterion | Implementation and evidence |
|---|---|
| 1. ONNX control flow and multiple outputs | `zig/lib/onnx/src/ops.zig`: `control flow Loop honors conditions and rejects incomplete semantics`; `convert.zig`: secondary outputs requested before primary. Unsupported dynamic control flow fails explicitly. |
| 2. Fixed-point attention regression | `zig/lib/ml/src/graph/passes/pipeline.zig`: `pipeline exposes attention after primitive softmax recognition`. |
| 3. Representative baselines | BP model matrices cover CPU, Metal and GB10; U–BP outcome registers retain transfer, allocation, residency and selected-route evidence. Completion repeats the final model comparisons. |
| 4. Composed optimizer bindings | `execution/program.zig` and ML pipeline: redirected bindings, eliminated nodes, public/state bindings. |
| 5. Common optimization entry | Imported ONNX and `graph/execution.zig` consume shared optimization/preparation; source format does not select an independent optimizer. |
| 6. Typed session capabilities | `models/session_contract.zig`: resolved manifest/implementation contract; wrapper/session tests preserve capabilities. |
| 7. Shape/cache policy | Imported ONNX shape-cache CPU/Metal/CUDA tests share weights and survive eviction; cached decoder preparation preserves executables across cache growth. |
| 8. Program/prepare/resident contract | `execution/root.zig` exports `ModelProgram`, `prepare`, `Executable`, `BackendContext`; `prepared executable reuses CPU and Metal plans with resident outputs`. |
| 9. Specialized and general execution | `execution/model_candidate.zig` admits whole-model candidates before submission; prepared BERT and physical-frame tests execute general plans. |
| 10. Loading/backend/preparation separation | `architectures/model_definition.zig` owns `LoadedModel`; `backends/compute_context.zig` owns backend construction; `execution/` owns prepared contracts. `session_factory.zig` retains architecture adapters. |
| 11. Serving lifecycle | Model manager delegates model/session construction and resolved capabilities; its serving admission, cancellation and cache lifecycle remain serving responsibilities. |
| 12. Specialized planning interfaces | `ops/kernel_descriptor.zig`, graph planner and `ModelCandidate`; operation/layout/schedule contracts keep model-specific admission in adapters. |
| 13. Accurate CUDA capabilities | `graph/cuda_capabilities.zig` rejects unsupported integer/strided signatures and validates complete arithmetic/layout contracts. |
| 14. Imported ONNX CUDA | Imported ONNX CUDA shared-weight/eviction tests execute supported signatures through common context; unsupported signatures remain rejected. |
| 15. Transfer-aware placement | `execution/placement.zig`, graph partition gates and prepared Metal fanout test; diagnostics expose fallback partitions. |
| 16. Physical memory authority | `execution/physical_buffers.zig`; prepared workspace, aliases/views, auxiliary planes, retained outputs and competing writes execute against assigned storage. |
| 17. Stable CUDA capture addresses | CUDA planned transfers/auxiliary representations, physical capture scopes and stable row snapshots; driver graph replay tests retain argument values and device-position addresses. |
| 18. Stateful execution ownership | Decoder runtime and prepared executable adapters retain KV/recurrent state; request-reset/capture invalidation and state-recycling tests cover lifecycle. Architecture definitions still describe model semantics. |
| 19. Explicit stage residency | Resident session/output contracts, prepared encoder/head paths, resident decoder outputs and stage-lifetime tests. Host materialization remains explicit at requested outputs. |
| 20. Exact output/transfer representation | `ONNX host outputs export directly from a final CPU fallback`; `transferTensor preserves floating storage without numeric conversion`; integer storage tests across devices. |
| 21. Task orchestration | Pipelines delegate prepared/stateful execution through sessions and runtime candidates. Task-specific processing and streaming orchestration remain pipeline responsibilities. |
| 22. BERT training pilot | `architectures/bert_graph.zig`: shared encoder-block finite differences for every activation, full encoder gradients across embeddings/masked attention; modern BERT trunk tests separately preserve its semantics. |
| 23. Qualified retirement | BP dead-path ledger, BK exact export allowlist and all-architecture instruction/resource comparator; BT dead decoder retirement. Qualification executables remain live. |
| 24. Directory consolidation as appropriate | `execution/` is the public preparation/executable boundary. Existing `graph/`, `ops/`, `backends/` and `pipelines/` paths retain implementation/compatibility ownership; wholesale path moves provide no source or performance benefit and are not a second execution engine. |
| 25. Reference/fallback policy | Generic interpreter remains an oracle/debug path. Runtime partition gates and model-candidate post-commit tests reject hidden fallback after submission. |
| 26. Three input formats | Model matrix includes MiniLM safetensors, GGUF and ONNX; packed quantized models use independent declared tolerances rather than assumed identical weights. |
| 27. Device outputs | Independent encoder/decoder/primitive references and CPU/Metal/GB10 correctness receipts; performance comparisons also require finite, byte-identical process outputs against their matching baseline. |
| 28. Shape/storage/state matrix | Typed gather/scatter, view/alias, dynamic ONNX cache, masks, packed projection and decoder prefill/replay suites exercise actual dispatch, tails and guards. |
| 29. Cross-stage ownership | Prepared frames pin published outputs through pool pressure/eviction; staged resident execution outlives publishing sessions; exhaustive allocation failures cover partial publication. |
| 30. No hidden readbacks | Scoped `execution/transfer_audit.zig` cannot weaken an outer strict scope; planned CUDA/Metal execution tests enforce actual transfer admission, with explicit metadata/output exceptions. |
| 31. Actual counters and bytes | `execution/stats.zig` and diagnostics aggregate physical transfers, memory, launches, reuse, cache and fallback counters; outcome registers distinguish measured execution from planning assumptions. |
| 32. Plan agrees with dispatch | Native partition executor, prepared physical execution and device descriptor suites run operations and inspect actual outputs/counters; catalog-only tests do not qualify a route. |
| 33. Specialized/general fallback | Model-candidate pre-submit declines and post-commit errors, runtime partition gates, strict transfer scopes and general prepared execution preserve observable fallback policy. |
| 34. Cancellation/admission/state reset | Prepared prefill cancellation/recovery, request resets, persistent-buffer capture invalidation, independent recurrent admission and retained-weight teardown suites. |
| 35. Performance budgets | Retained randomized AA/AB/BA qualification uses median <=1.05 and p95 <=1.10, finite outputs and declared numerical references before retirement. Failed/contaminated runs remain recorded. |

## Retained distinctions and completion limits

Canonical numerical helpers and local include leaves remain authored owners.
Generated variants still represent independent instructions when their reduction
order, rounding, index width, memory staging, register allocation or supported
geometry differs. In particular, Q4/Q8 named two-row operands retain the measured
32-register schedule rather than the 40-register array alternative.

The completion campaign's ownership inventory is generated from both actual
registries and verified against the emitted regions/files. It is not another
runtime dispatch table. Export compatibility names remain ABI adapters around
operation-named owners.

Review/full CI on the final commit remains a human merge gate. Physical L4/A100
validation is deferred by the user's available-host decision. GB10 qualification
and compile/resource checks do not substitute for physical timings on those
architectures. Historical fatbin savings must not be quoted against a newer
compressed main without a new matching-baseline measurement.
