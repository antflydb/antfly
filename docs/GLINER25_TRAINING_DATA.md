# GLiNER2.5 native training data and run contract

The native training library accepts versioned JSONL through
`finetune/gliner_boundary_dataset.zig`. Each line carries an independent, complete
extraction schema and explicit annotations. This is an internal training
interface; public GLiNER2.5 rollout remains gated by
[the implementation qualification plan](GLINER25_IMPLEMENTATION.md).
The version-1 [training job and CLI](GLINER25_TRAINING_JOB.md) now own source
admission, split preflight, native optimization, durable resume and final
export. Their implementation does not itself qualify real-model training.

```json
{"version":1,"id":"meeting-001","text":"Ada met Grace at Acme.","schema":{"entities":["person","company"],"relations":[{"type":"met"}]},"entities":[{"id":"ada","type":"person","span":{"start":0,"end":3}},{"id":"grace","type":"person","span":{"start":8,"end":13}},{"id":"acme","type":"company","span":{"start":17,"end":21}}],"relations":[{"type":"met","head":{"entity":"ada"},"tail":{"entity":"grace"}}]}
```

Annotation names resolve against the compiled schema, including JointIE entity
and relation types. Unknown names, duplicate example/entity IDs, unknown JSON
members, unsupported versions, and malformed endpoint/value alternatives fail.
Schemas are never inferred from an example's positive labels.
JSON nesting is bounded before parsing or recursive schema serialization.
Each sample owns its bounded native regex compiler; the runner owns the matching
context for its cached graph. Their Python 3.12/Unicode 15 semantics are versioned
in the run identity, with execution work and cancellation scoped to each batch.

| Member | Annotation form |
| --- | --- |
| `entities` | `id`, declared `type`, `span`, optional `attributes` containing named `group` and `labels`. |
| `classifications` | Named `task` and an explicit `labels` array. Omitted tasks are unsupervised; a supplied empty array is a supervised negative and must satisfy the task constraints. |
| `records` | Declared structure `type`, record `id`, and named `fields`. Each field has a `values` array. A value has either explicit `occurrences` or a declared enum `choice`. |
| `relations` | Declared `type`, `head` and `tail`. Each endpoint has either an example-local entity ID in `entity`, or an explicit `span`. |

Every span is half-open. Its optional `unit` defaults to `utf8_bytes`; explicit
`unicode_codepoints` and `utf16_codeunits` are supported. Offsets must identify
valid character boundaries in the original text. Tokenizer/target preflight
additionally requires representable word boundaries. No annotation is snapped,
silently dropped or truncated. Several `occurrences` represent alternatives for
one distinct field value; separate `values` represent distinct values.

For example, a record field with an enum may contain
`{"name":"status","values":[{"choice":"paid"}]}`. An extractive field may
contain `{"name":"party","values":[{"occurrences":[{"start":0,"end":3}]}]}`.
Natural anchors are ordinary annotated fields named by the structure schema.
Record mode, multiplicity, cardinality, validators and exclusivity remain schema
properties. Entity, attribute, record and relation annotations are complete for
the declared schema; absent positives in these task families are negatives.

`Dataset.open` reads a bounded, owned snapshot using cancellation checks between
256 KiB reads. Changing the source file after loading cannot change the consumed
examples. The snapshot SHA-256 and the ordered schema fingerprints bind training
resume. CRLF and a final newline are accepted; blank examples fail with their
original line number. Defaults cap the file at 256 MiB, each row at 2 MiB and the
dataset's total live host allocations at 512 MiB. Callers may select different
explicit limits under process admission. The complete snapshot is validated;
invalid or oversized examples do not disappear from the dataset denominator.

`Dataset.preflight` runs actual tokenization and target compilation before the
native runner starts. `requireDisjoint` rejects shared document IDs or exact
UTF-8 text across splits. It does not detect near-duplicates or pretraining
contamination. Calibration/test data must remain independently frozen; this
training loader performs no automatic split selection or threshold tuning.
The job preflights every declared split against the same tokenizer, capacities
and offset rules. It records consumed calibration/test hashes in the run
fingerprint, then releases those datasets before constructing mutable training
state. Only `train_file` reaches the optimizer. No held-out forward pass,
metric, model selection or quality evaluation occurs in this job.

The [four-row fixture](../zig/pkg/inference/scripts/gliner25/TRAINING_JOB_FIXTURE.md)
contains two training and two disjoint validation examples, all with the same
full entity, classification, natural-record and ordinary-relation schema. It
tests source/job plumbing. Its repeated task templates and tiny sample count
cannot establish convergence or representative held-out quality.

`gliner_boundary_native_trainer.Trainer` owns a bounded CPU backend, one optimizer
controller and at most one cached training graph. It borrows the verified source
model/tokenizer and dataset, which must outlive it. Run settings bind artifact
digests, annotations, schema order, batch/accumulation settings, schedules,
dropout protocol, objective settings and the complete adapter layout. Graph
reuse requires matching geometry and ordered schemas. Inactive adapter modules
retain their global optimizer slots and absent-gradient semantics.

`gliner_boundary_training_source.Source` owns aligned immutable snapshots of
all five source files: `model.safetensors`, `config.json`,
`encoder_config/config.json`, `tokenizer.json` and `tokenizer_config.json`.
It hashes and consumes the same opened descriptors, checks the complete
334-tensor FP32 inventory, and retains the tokenizer and tensor views until all
trainer users are drained. `expected_source` can require exact external file
pins; otherwise the receipt records the bytes consumed without authenticating
them against a caller-selected reference. The optimizer makes separate mutable
copies of its selected trainables.

`next` consumes one microbatch or performs an end-of-epoch partial flush; it
returns null at the configured horizon. Failed computation does not advance
optimizer or data counters. A checkpoint includes unfinished accumulation,
presence bits, moments and exact counters. Restore validates the derived dataset
cursor before publishing the staged state. It reads a bounded immutable
checkpoint snapshot through one descriptor and records both its file digest
and the reconstructed canonical optimizer-state digest. An expected state pin
is checked before the fresh owner replaces live state. Native replay uses its versioned
counter RNG; exact PyTorch dropout comparisons use explicit shared masks.

The library enforces separate host/backend ceilings and computes conservative
combined admission including the borrowed model and dataset. The enclosing job
manager reserves the larger of preflight and training phase requirements and
retains the source lease. The CLI creates one admission controller; embedded
callers must pass their shared process admission owner. Public run,
save, restore and position operations reject concurrent/reentrant calls. The
owner must join callers before destruction. Resource limits are admission
policies; quantized training is explicitly unsupported.

The current job selects the native CPU backend and strict FP32 activations.
Passing Metal component or resident Session tests does not enable a GPU job or
qualify a resident optimizer. Numerical tiny-model parity, source-artifact
training, convergence, serving and GPU qualification are separate evidence stages. See
[the training evidence](GLINER25_TRAINING.md) for their current status.
