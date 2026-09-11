# GLiNER2.5 native training contract

The boundary architecture has its own native training path. It supports full,
head-only, LoRA and DoRA modes, CPU or resident Metal execution, gradient
accumulation, durable partial-window resume and portable model/adapter export.
The legacy GLiNER2 span-grid trainer is a different objective.

The reference is Fastino commit
`3c913c7369301133d3b7699252074c4303ada50e`; source, dependency, model and fixture
identities remain pinned. Training accepts floating-point source weights.
Quantized training is excluded; reduced-precision inference after export needs
separate qualification.

Jobs default to `attention_profile: "materialized_v1"` and
`activation_profile: "retained_v1"`. The independent opt-ins are
`replay_tiled_v1` and `layer_recompute_v1`. Both enter durable run identity and
retain bounded host, backend and combined admission. See the
[regional training guide](GLINER25_RECOMPUTED_TRAINING.md) for replay ownership,
work and scratch accounting.

## Reference coverage and limits

| Boundary | Retained verification |
| --- | --- |
| Targets, candidates and assignment | Strict mixed-task/Unicode packing, full declared schemas, detached gold injection and matching, retained gold-capacity failures. |
| Losses and VJPs | Pinned Torch fixtures for boundary, classification, relation and record losses; empty/masked behavior and trainable-gradient presence. |
| Complete training steps | [Full/head/LoRA/DoRA step fixtures](../zig/pkg/inference/scripts/gliner25/TRAIN_STEP_ORACLE.md) plus [controlled dropout](../zig/pkg/inference/scripts/gliner25/TRAIN_STEP_DROPOUT_ORACLE.md), including the exact semantic-pair/mask-transport contract. |
| Inactive adapters | [Source fixtures](../zig/pkg/inference/scripts/gliner25/TRAINING_INACTIVE_ADAPTERS.md) distinguish absent gradients, optional-head zero touches and wholly inactive fallback; native consumers perform their own later updates and resume. |
| Replay attention | [Actual-source forward and five-VJP oracle](../zig/pkg/inference/scripts/gliner25/TRAINING_ATTENTION_ORACLE.md), exact native counter masks and fixed numeric tolerances. |
| Managed jobs | Cancellation/allocation failure, atomic optimizer transactions, epoch-end partial flushes, exact same-backend fresh-owner resume and explicit resource denial. |
| Exports | [Complete tensor and PEFT checks](GLINER25_TRAINING_EXPORT.md), [native materialization](GLINER25_MERGE.md), and [trained-artifact execution](../zig/pkg/inference/scripts/gliner25/TRAINED_EXECUTION_CHECK.md). |

Published-small local jobs exercised task-head and all-131-target CPU adapters,
classifier-only CPU/Metal inactive batches, and all-target regional CPU
LoRA/DoRA restart consistency. Those bounded authored recipes do not establish
published-model PyTorch gradient equality, equality of training updates between
backends, full-context numerics, useful trained quality or convergence. Published
regional Metal and other backbone/rank/target combinations remain open.

Same-backend resume requires exact state and output bytes. Cross-implementation
losses, gradients and adapted arithmetic use their predeclared fixture
tolerances; a byte-identical artifact reload is a separate property. Native
random streams are deterministic across resume, not PyTorch RNG replicas.

The [job guide](GLINER25_TRAINING_JOB.md) defines executable configuration and
limits. The [data guide](GLINER25_TRAINING_DATA.md) and
[four-row preparation tool](../zig/pkg/inference/scripts/gliner25/TRAINING_JOB_FIXTURE.md)
define immutable inputs. Validation-data preflight runs no evaluation.
Campaign logs and copied source/binaries are external run artifacts under the
[fixture policy](../zig/pkg/inference/testdata/gliner25/README.md).

## Implementation sequence

| Stage | Concrete implementation | Required proof |
| --- | --- | --- |
| Losses and supervision primitives | `finetune/gliner_boundary_losses.zig`: masked BCE, asymmetric focal, pair BCE, listwise gold-mass, inside BCE, noisy-OR consistency, abstention, Poisson counts, detached hard negatives, exact/IoU candidate labels, and dense start/end/inside targets | Compare each scalar loss, every input-logit gradient, masks, axis order, and targets against pinned Torch; prove empty/all-masked behavior, limits, cancellation, and allocation cleanup |
| Canonical training data | `finetune/gliner_boundary_targets.zig`: compile immutable document offsets, full declared query schemas, positive/negative annotations, classification labels, record identities/alternatives, and typed relation pairs into bounded target batches | Differential preprocessing and target fixtures for every task; reject malformed labels, ambiguous occurrences, and capacity overflow before a step; preserve the full evaluation schema independently of gold labels |
| Detached candidate and record decisions | `finetune/gliner_boundary_selection.zig` and `gliner_boundary_matching.zig`: query/shared pools, gold injection, exact record target bindings, natural anchors, and bounded Hungarian matching | Pinned proposal membership and cost/assignment fixtures, explicit caller-owned random draws, complete retained gold, immutable binding fingerprints, and allocation/cancellation checks |
| Differentiable boundary forward | Build the boundary, shared-pool, classifier, relation, and record operations on the existing graph/autodiff infrastructure; bind discrete selected indices separately from differentiable re-scoring | Per-operation and per-head gradients against Torch, then all-task loss and every trainable gradient on a tiny real checkpoint |
| Head-only training | Freeze the encoder and enroll the explicitly selected boundary/classifier/relation/record tensors as regular trainables; cache encoder states only under exact model/tokenizer/schema/augmentation identities | Frozen tensor hashes remain unchanged; native versus Torch optimizer updates match; cached and uncached head steps agree |
| Full training | Enroll encoder embeddings, relative embeddings, normalization, projections, and all selected task parameters; add explicit encoder/task optimizer groups and learning rates | Gradient completeness, distinct LR/decay groups, clipping, accumulation, final partial-window flush, nonfinite atomicity, and memory-admitted CPU/Metal steps |
| LoRA and DoRA | Resolve public aliases to exact architecture-specific linear modules; add PEFT-compatible DoRA graph semantics and magnitude optimizer/checkpoint state | Exact selected parameter inventory, shared-weight behavior, initial no-op output, A/B/magnitude gradients, one-step update, adapter import/export, and merged versus unmerged inference |
| Durable training runs | Reuse managed execution, bounded graph caches, optimizer-state persistence, explicit dataset/model/schema hashes, deterministic random-stream state, and atomic artifact publication | Interrupted/resumed and uninterrupted runs produce equivalent next-step inputs, gradients, optimizer counters, final adapters/checkpoints, and inference results |
| Release qualification | Run all three published models on CPU and Metal, then evaluate held-out data with every supported task and adapter mode | Independent Python reference runs, repeated seeds, disjoint validation/test data, per-task quality and calibration, training throughput/peak memory, and failure recovery |

## Existing infrastructure to reuse

`real_autodiff_trainer.zig` owns regular parameters and LoRA parameters,
supports gradient accumulation and clipping, provides compiled device execution
and resident optimizer state, bounds shape-specific graph caching, and saves
optimizer/checkpoint state. `architectures/deberta_graph.zig` provides the
encoder graph. These components should be extended through their existing
interfaces rather than introducing a second optimizer or checkpoint owner.
`seeded_gradient_trainer.zig` now supplies explicit-gradient optimizer groups
and staged update validation while retaining `RealAutodiffTrainer` as the slot
and Adam-state owner. Native CPU is the default; explicit `resident_metal`
Controller updates and managed full/head-only runs have the independently
scoped tiny GPU evidence described above. Managed configuration defaults to
native CPU and accepts explicit `resident_metal` with a Metal-enabled build and
separate metadata/device admission. The resident job option now has the
separate published-small classifier-only LoRA/DoRA CLI continuity evidence
above; other published target families/backbones remain unqualified. The CPU
CLI also has the separate head-only and all-Linear adapter evidence above.
`graph/seeded_training.zig` and `multi_stage_training.zig` retain activations
across detached candidate/relation decisions without replaying the encoder or
dropout. Their tapes bind the parameter epoch and exact input identity.

`gliner2_real_autodiff.zig` supplies useful integration patterns and existing
checkpoint/evaluation workflows, but its `gliner2_total_loss` consumes the older
span and count-embedding architecture. `gliner2_boundary.zig` stores cached
top-layer span-training artifacts with a separate versioned family. Neither
artifact nor objective should be accepted as a GLiNER2.5 training run.

## Loss and selection semantics

The boundary model combines start, end, pair, inside, soft-IoU, reranker-listwise,
proposal-listwise, marginal-consistency, abstention, and count losses, then adds
classification, relation, and record losses. Every published checkpoint's exact
weights, annealing schedules, negative-query sampling, and reduction mode must
be retained. Published profiles use settings such as focal marginals, summed
reduction, and gold-inclusive hard-negative pools; generic BCE defaults are
insufficient.

Candidate indices, hard-negative ranking, gold-injection decisions, and record
Hungarian assignments are detached. Selected proposal logits must be recomputed
through the differentiable path. Detaching those logits would remove proposal
training; differentiating through candidate membership would change the
reference objective.

Record matching also uses a different normalization from its final loss:
Hungarian costs sum list-field BCE terms, while the matched field loss averages
candidate terms, then fields, then matched records. Natural records identify
instances through their anchor candidates; latent and anchorless modes match
gold identities to instance hypotheses. These paths need separate target and
gradient fixtures, including absent fields, alternative occurrences, empty
records, duplicate spans, and insufficient capacity.

The native loss module returns owned scalar losses and gradients in the input's
explicit `[B,Q,C]` or `[B,C,Q]` order. It supports graph/custom-VJP integration
without assuming a particular optimizer owner. Reductions retain two easily
missed source behaviors: negative weights do not alter the normalization
denominator, and generic `global` reduction does not apply an additional query
mask. Its caller must include query validity in the element mask. `sum` and
`per_query` apply the query mask explicitly.

Listwise losses preserve the finite `-1e4` masking sentinel, including its effect
when valid logits are even smaller. Noisy-OR consistency sends gradients to
both candidate and marginal logits and preserves the upper probability clamp.
Poisson counts use `log_input=true` and `full=false`. All-masked and empty
supervision is finite with zero gradients. Invalid active targets, nonfinite
results, budget exhaustion, cancellation, and allocation failure remain typed
errors; they cannot become skipped targets or successful zero-loss steps.

`scripts/gliner25/capture_training_losses.py` imports the actual pinned loss
functions and records high-precision values and gradients from exact float32
inputs. It loads no model and restricts Torch to one CPU thread. Its fixture
records source-file digests and software versions. End-to-end tests must also
cover the production floating-point dtype and backend kernels.

Kernel limits independently bound batch, query and candidate dimensions,
elements per buffer, and work. Empty dimensions do not bypass scratch-allocation
limits. Consistency and dense-target scratch is reclaimed before returning;
only the result vectors remain owned by the caller. Service-level admission
must additionally bound the complete training step across all component losses.

## Adapter and optimizer compatibility traps

The boundary-specific `gliner_boundary_peft_graph.zig` implements LoRA and
DoRA using PEFT-style `[out,in]` weights. DoRA recomputes the live row norm and
detaches it in the graph, preserves bias/scaling semantics, and shares adapter
weights while assigning dropout to each module use. Its pinned PEFT fixture
passed all eight train/eval/dropout cases. The standalone compatibility
`lora.zig` helper differentiates its differently oriented norm and must not
replace this boundary path. Allocation errors propagate through the new graph
and its adapter scratch.

Architecture-aware aliases must include `encoder`, `classification_head`,
`extractive_head`, `relation_head`, `record_head`, and `all_task_heads`. Resolve
them against the exact checkpoint inventory, preserve tied/shared weights, and
reject selectors that unexpectedly match no modules. Pinned trainer defaults
still contain legacy span/count names, so they must not silently define the
native boundary training target set.

Full upstream training separates encoder and task learning rates using the
literal test `"encoder" in name`. Boundary-encoder and candidate-encoder
parameters therefore receive `encoder_lr` too. Head-only freezing excludes
only the top-level encoder subtree. Every source group applies the configured
weight decay, including biases and normalization parameters. In LoRA mode,
every trainable parameter receives `task_lr`, including DoRA magnitude and
separately unfrozen heads. The exact source AST grouping fixture records these
rules; its native consumer passed all four supported CPU run profiles (full,
heads, LoRA and DoRA). Symbolic CUDA/MPS constructor flags and an additional
unfrozen base parameter remain source-only cases.
Checkpoints must preserve group
membership, schedules, per-parameter Adam counters, gradient-accumulation
position, and conditional task-family state. A missing task in a microbatch
must not accidentally decay or advance a parameter that received no gradient.
In particular, PyTorch gradient `None` skips AdamW state and decay, while an
explicit zero gradient participates. The shared-pool path leaves legacy
explicit scorer parameters unused; upstream `_head_touch` intentionally gives
zero gradients to optional relation/record heads. These cases cannot be merged.

## Data, reproducibility, and resource admission

The canonical target compiler accepts exact source offsets first.
String-only imported annotations need an explicit occurrence policy and must
report ambiguity. Record identity and alternative occurrences must survive
lowering; they cannot be flattened into independent field spans. Native entity
attribute supervision requires an explicit annotation contract lowered to the
same hidden query labels used during inference, since the pinned training data
helpers do not expose a dedicated attribute helper.

`gliner_boundary_targets.compileBatch` consumes processor samples, their compiled
schemas, and one annotation object per sample. Each annotation carries the exact
schema fingerprint. Source spans declare UTF-8 bytes, Unicode codepoints, or
UTF-16 code units and must align exactly to original-text word boundaries. The
compiler rejects invalid UTF-8, split surrogate pairs, offsets inside a word,
and references to synthetic punctuation. This deliberately tightens the pinned
character-to-word helper, which can expand a partially aligned source span.

Entity annotations retain their declared type and explicit attribute labels.
Every applicable attribute group must be supplied, including an empty label
list for a negative multilabel group. Extractive annotations are complete for
the declared schema, so omitted entity, attribute, field, or relation labels
are negative targets. Classification has a separate supervision mask: an
omitted task is unsupervised; a present empty label set is supervised negative
and must satisfy its cardinality. Cross-task classification constraints require
complete classification task supervision and a valid gold assignment.

Records have explicit IDs scoped to their declared structure. A field contains
distinct gold values; each value retains its explicit alternative source
occurrences. Declared enum choices have their own identity and point to the
processor's synthetic scoring position. Their canonical choice value is
validated before training. Required fields, duplicate annotations, inconsistent
enum alternatives, and insufficient target capacity are errors. JointIE edges
reference exact typed entity annotations. The compiler requires the complete
gold graph to satisfy endpoint, overlap, degree, symmetry/inverse, and global
graph constraints; a solver cannot drop invalid gold to make it trainable.

The result owns record IDs, grouped mention pairs, target arrays, and schema/text
hashes. Packed `[B,Q,G]` pairs use the same word/query coordinates as the native
processor, including enum prefixes. Stable deduplication preserves all distinct
gold mentions. Dynamic padding uses the observed maximum, with a minimum gold
capacity of one; fixed padding fails if gold exceeds the requested capacity.
Batch size, source bytes, routed widths, annotations, per-query gold, padded
elements, work, and cancellation are bounded before a training step. The next
candidate stage must preserve these identities while keeping discrete proposal
membership and assignment decisions outside the differentiable graph.

`capture_training_targets.py` exercises the actual pinned schema transformer's
word-label path and boundary target packer without loading a model or a subword
tokenizer. Its ten fixtures cover mixed tasks, lowered attributes, enum prefixes,
ragged and fixed padding, natural/latent/anchorless record identity, explicit
alternative occurrences, negative extraction, classification-only samples, both
Unicode splitters, and strict gold-capacity overflow. Full tokenizer and encoder
parity remain separate fixture and runtime checks.

`gliner_boundary_selection` requires an explicit training or evaluation phase.
Probabilistic gold injection consumes caller-owned uniform draws; evaluation
never consumes gold-injection draws and cannot change its proposed spans based
on evaluation answers. Query pools and document pools preserve their separate
recall denominators and layout. The native path retains the pinned gold/query
quota priority bands and finite invalid-score sentinel, but raises a capacity
error if a requested gold injection disappears from the retained pool. A
successful training step must not silently lose supervision because its pool
was too small. The 42-case fixture calls the pinned query selector and actual
document-pool forward implementation without loading a model.

`gliner_boundary_matching.compile` binds each canonical record identity to the
exact retained candidate columns and declared field membership. Every distinct
gold field value must retain at least one of its annotated alternative
occurrences. Its owned target map and fingerprint survive release of the
annotations and candidate inputs. `buildCosts` is a high-precision reference
over float32 logits; the production graph supplies its own float32 costs through
`fromScoredCosts`, which widens those exact values for the Hungarian solver.
Matching costs are detached decisions, not the reported training loss.

Natural records bind explicit anchor candidates. Latent and anchorless modes
match only active instance hypotheses, and every gold record must receive an
assignment. This deliberately corrects the pinned finite-sentinel path, where
an inactive hypothesis can win a gold column when valid costs exceed `1e4`,
then silently discard that gold during filtering. Empty supervision preserves
negative object labels for active non-natural hypotheses. The 18-case matching
fixture records pinned target indicators, float64 reference costs, actual
float32 graph costs, and both resulting assignments; exact ties are evaluated
within the active-hypothesis domain.

The complete declared schema belongs in evaluation input. Inferring the schema
from labels present in the gold document leaks supervision and changes negative
queries. Dataset validation must report and reject invalid records or labels by
default. The pinned source sometimes skips malformed classification shapes or
sanitizes records despite stronger documentation; native strict validation
must be explicit and covered by compatibility tests.

Randomness must be reproducible across resume: schema/task order, data order,
dropout per use site, negative-query sampling, proposal/gold injection, and
optimizer steps all need stable counters or serialized states. Tiny deterministic
fixtures should inject identical masks/selected indices into both runtimes
before broader stochastic equivalence tests.

Admission must account for parameters, gradients, Adam state, activation and
checkpoint recomputation buffers, candidate/record capacities, and device
workspace together. Full training must not use an inference-only memory
estimate. Overflow of gold mention, candidate, relation, or record capacity is
an error by default. This protects supervision integrity and prevents an
apparently successful run from silently training on less data than requested.
