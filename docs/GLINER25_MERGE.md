# GLiNER2.5 adapter materialization

The versioned merge job materializes an ordinary LoRA or DoRA adapter into a new
FP32 GLiNER2.5 model directory. It accepts the published small, base and multilingual
architectures, requires the complete 334-tensor original inventory, and retains
every original parameter name and shape. It does not enable the public model
qualification gate or establish model quality.

Run through the public inference executable:

```sh
antfly-inference finetune adapter materialize gliner25 /absolute/path/merge.json
```

The standalone package target is `materialize-gliner25-adapter`; from `zig/`,
the shared repository graph exposes `inference-materialize-gliner25-adapter`.
For example, `zig build inference-materialize-gliner25-adapter
-Doptimize=ReleaseFast -Dmetal=false -Dcuda=false -Donnx=false -Dpjrt=false -j1
-- /absolute/path/merge.json` builds and runs the same command. All routes use
the same job and disposable worker. `--shutdown-grace-seconds N` accepts
1–300 seconds and defaults to 30. The first SIGINT or SIGTERM cancels the merge
and allows private staging cleanup; a second signal or the hard deadline forces
termination. The supervisor owns the child PID and reaping and never retries a
failed merge automatically. Timeout exits 124, and an interrupted parent exits
128 plus the signal number. A forcibly terminated process can leave a private
staging directory; operators can remove that directory after verifying the
worker is no longer running.

## Exact input contract

`gliner_boundary_merge_job.Config` contains the following JSON fields. Unknown
fields, duplicate keys, quoted numeric resource limits, negative integers and
unsupported versions are rejected. All paths must be absolute and contain no
empty, `.` or `..` components. The output directory must not already exist.

| Field | Requirement |
| --- | --- |
| `version` | Integer `1`. |
| `source_dir` | Original FP32 extractor directory, including all four consumed sidecars. |
| `adapter_dir` | Directory containing `adapter_config.json` and `adapter_model.safetensors`. |
| `output_dir` | New destination directory. Its parent must already exist. |
| `expected_source` | Full source identity: `backbone`, `precision`, `weight`, and ordered four-element `sidecars`. Precision must be `fp32`. |
| `expected_adapter` | Exact `config`, `weights`, and nullable `receipt` file digests. |
| `schema_sha256` | The 32-byte training schema/data fingerprint bound by the adapter. |
| `source_limits` | Optional explicit source snapshot and tokenizer/parser bounds. |
| `merge_limits` | Optional merge scratch, output, header, receipt and adapter validation bounds. |
| `memory` | Optional process admission and adapter import ownership bounds. |
| `timeout_seconds` | Positive integer, default 1800; hard maximum 86400. |
| `disk_headroom_bytes` | Additional free space to preserve; default 256 MiB. |

A file digest has `size_bytes` and `sha256`, using the existing bundle digest
representation. SHA-256 values are lowercase hexadecimal bytes; schema/target/
parameter fingerprints use the existing 32-byte array representation. Generate
the job from the exact original identity and exported adapter receipt rather
than from model names or Hugging Face card metadata.

`expected_adapter.receipt: null` requires that
`antfly_gliner25_adapter.json` be **absent**. It does not permit an unchecked
receipt. This supports standard PEFT artifacts only when the job supplies the
explicit original source and schema binding. When a receipt is present, both its
exact file digest and its source/config/weight/target/value bindings must match.

The parent and child hash and parse the same bounded configuration snapshot from
one descriptor. The process contract binds its exact size and SHA-256 plus the
selected shutdown grace. Changing the configuration path before the worker
loads it causes a configuration-identity failure. Reformatting otherwise equal
JSON intentionally changes this invocation identity.

## Ownership and resource bounds

The merge receives the existing immutable training `Source` owner. This owner
snapshots and validates the original model and all four sidecars before exposing
aligned FP32 tensor views. The merger never reopens the source pathname or uses
a mutable model mapping. Adapter descriptors are opened together, read through
bounded same-descriptor snapshots, hashed, and imported into a separate immutable
owner. No adapter pathname is consulted after import.

The admitted host reservation includes a fixed 8 MiB configuration setup owner
plus four job owners:

1. The actual five source file sizes plus the source auxiliary allowance.
2. Twice the exact adapter input file sizes plus its auxiliary allowance,
   covering simultaneous raw snapshots and imported tensor values.
3. The declared merge scratch ceiling, covering metadata, verification scratch
   and one adapted output matrix at a time.
4. The small job metadata ceiling.

The setup owner bounds raw configuration bytes, the strict type-validation
prepass, retained parsed configuration and the parent's recovered argument copy.
It survives until the corresponding parent or child invocation finishes. The
job requires this owned `ConfigSnapshot`; an arbitrary parsed configuration plus
an unrelated digest is not an execution input. The worker reports setup
reservation, live heap bytes and peak heap bytes separately from job scratch.
The 8 MiB setup reservation is fixed rather than controlled by JSON fields.

Defaults are 3 GiB combined admission, 4 MiB job metadata, 32 MiB adapter
auxiliary allowance, a 512 MiB maximum adapter owner, and 64 MiB merge scratch.
The source defaults retain the training source's 2 GiB total ceiling and
384 MiB auxiliary allowance. Source reservation uses actual file sizes, not the
entire source ceiling. The process's live-memory admission guard still applies;
an otherwise valid job can be refused under host pressure.

All owners require a reclaiming backing allocator. Freed per-matrix output and
validation records must not be retained in an outer arena. There is no second
whole-model copy and no GPU/model execution during materialization. The result
reports admitted bytes, setup live/peak usage, the source reservation, and
measured adapter/job/merge ownership peaks; these are allocator receipts, not
operating-system peak RSS measurements.

The job checks an output-size upper bound and free disk headroom before loading
model payloads, and checks the prepared bound again before creating staging.
Explicit hard ceilings remain independent of current host capacity. Increasing
a declared limit never bypasses process admission.

## Tensor and publication semantics

Adapter targets are resolved against the original published Linear inventory.
The merger reparses the owned adapter configuration and verifies its actual
tensor values and target fingerprint before using it. A cached caller-supplied
module descriptor cannot silently change merge targets or scaling. LoRA uses
FP32 rank accumulation; DoRA computes the FP32 direction's squared row norm in
FP64 before applying the output-channel magnitude. Bias is never scaled.

The writer streams canonical FP32 tensors into a private mode-0700 staging
directory. A separate streaming pass verifies header names, shapes, dtypes,
contiguous offsets, full payload size and every tensor byte. Adapted matrices
are independently recomputed using the same native formula for this integrity
check. Untouched tensors, biases and all four sidecars are compared byte for
byte with the immutable original snapshot. The verifier holds regular-file
descriptors, checks unexpected trailing bytes, and rejects file metadata changes
during verification.

After files and directories are synced, an atomic no-overwrite rename publishes
the completed payload. Cleanup owns only the private staging directory. A parent
directory sync failure after publication returns
`BoundaryMergePublishedDurabilityUnconfirmed` and retains the completed output.
A signal arriving after publication can similarly leave a complete output even
when the invocation exits as interrupted; inspect its receipt before retrying,
and always use a new destination.

The output contains `model.safetensors`, the exact original four sidecars, and
`antfly_gliner25_merge.json`. Its `gliner_boundary_merge/v1` receipt binds the
original source identity, exact job and adapter file digests, schema/target/
parameter fingerprints, output identity, tensor counts and
`f32_lora_delta_f64_dora_row_norm_v1` math policy. It is an integrity/provenance
record, not a publisher signature.

## Validation status

The focused tests cover synthetic LoRA/DoRA publication and an independent
equation, exact frozen/bias/sidecar bytes, no overwrite, every injected allocation
failure, cancellation at each cooperative checkpoint, mutated input/output
snapshots, strict job parsing and admission, optional receipt presence, and
original-argv command binding. The first native checkpoint passed all 25
selected integration tests with no skips or leaks, including the three merge
tests, three job tests, merge CLI test and three adapter tests. Its log is
`/private/tmp/gliner25-merge-metrics-integration-v1.log`, SHA-256
`0a0b81baffed9696beef213f13fa01dec138e0f7346c1b1ed79fd63712c41c7c`.
The subsequent fixed setup-owner and actual pending-I/O-cancellation checkpoint
passed all 12 selected tests with no skips or leaks:
`/private/tmp/gliner25-merge-setup-cleanup-v2.log`, SHA-256
`687e9a32797e451f4364837b0c39afa2f41bfb3452220653a75cfee957199682`.
Platform original-argv tests have a separate run and must not be inferred from
the native count.
The existing adapter importer/merge tests are
extended to rebuild descriptors from actual owned inputs and exercise an
unaligned caller-owned output buffer.

The published small model now has separate actual LoRA/DoRA materialization
and PEFT 0.18 compatibility evidence below. Its predeclared comparison requires
all 334 FP32 parameters and exact untouched/bias/sidecar bytes; adapted weights use
`abs_error <= 1e-6 + 1e-5 * abs(reference)`. Ten-task upstream merged/unmerged/
native-materialized outputs require exact token IDs and discrete decisions with
the already declared 5e-4 confidence tolerance. Native recomputation integrity
does not imply byte-identical adapted math across different implementations.
See the [independent merge checker contract](../zig/pkg/inference/scripts/gliner25/TRAINING_MERGE_CHECK.md)
for immutable oracle inputs, exact commands and separate checker evidence.

## Actual small all-target materialization

The public supervised command materialized both trained rank-2/alpha-4,
zero-dropout adapters covering all 72 encoder and 59 task-head Linear modules.
These are the four-microbatch/two-update small CPU jobs in the
[training export evidence](GLINER25_TRAINING_EXPORT.md). The materializer was
the 26,014,432-byte ReleaseFast CPU executable with SHA-256
`12e8f2e9723fda9dd1ba6d2317b24ee07e2cc95bcce5baddee557ddd5a5bc279`.
Its source inventory and build identity are recorded by
`/private/tmp/antfly-gliner25-materialization-v1/binary.json`, SHA-256
`6b8ea6c7b86ac2faf1829a017119e5e16fa61050c62131b5dac28ebc0791fc86`.

Both new model files contain 334 canonical FP32 tensors and are 295,567,748
bytes. All 131 adapted matrices, comprising 24,444,168 elements, were compared
with the official PEFT merged weights. All 203 untouched tensors, including
164 biases and 49,437,711 total elements, retained exact source bytes; all
four original sidecars also matched exactly.

| Measured property | LoRA | DoRA |
|---|---:|---:|
| Adapted values violating `1e-6 + 1e-5 * abs(PEFT)` | 0 | 0 |
| Maximum adapted absolute error | `2.9802322387695312e-8` | `7.152557373046875e-7` |
| Maximum confidence/probability difference across all three ten-request comparisons | `1.7881393432617188e-6` | `2.205371856689453e-6` |
| Native materialization scratch peak bytes | 4,970,746 | 4,972,238 |
| Admitted native host bytes, all reported owners | 553,252,969 | 553,693,127 |
| Independent Python checker peak worker RSS bytes | 1,332,756,480 | 1,240,940,544 |
| Independent checker private-copy bytes | 610,758,288 | 610,978,361 |

The tensor rule includes its absolute term near zero; maximum relative error
alone is not a separate pass criterion. Native allocator receipts and Python
worker RSS are different measurements. Each checker completed all 37 protocol
events, released the prior model owner before the next load, and removed its
private scratch after reaping. The runs used a 6 GiB RSS guard and a 768 MiB
private-copy ceiling.

| Artifact under `/private/tmp/antfly-gliner25-materialization-v1/` | SHA-256 |
|---|---|
| `lora-all-merged/model.safetensors` | `86311eaf2fc2c8f0cd42ea6a86e52f1932609157c96fe26f9fac1f8592e56db6` |
| `dora-all-merged/model.safetensors` | `1c8e6569f9beeb4cdb858ab310ae57ea3d64b5a6f07af1f5da3d6918f9bda189` |
| `lora-all-merged/antfly_gliner25_merge.json` | `8a5a3f8dba1d60c5adcec706b762682831fc3cdd96bd86817bb80594f97b1732` |
| `dora-all-merged/antfly_gliner25_merge.json` | `95021e59167461c316df67bff3bd373719c71ed1dbdeca7f3bc89ae25d018546` |
| `lora-all-peft-check/report.json` | `b2a51eebd9f43c3ddc790dee9a8cf39f6f5c3d5d90b7e726b61d0c7369bd5127` |
| `dora-all-peft-check/report.json` | `a7a628ecc1ce5734b7268c7e3e0cec8049a61b4069b3dd55ecf8a6cd1a39be81` |
| `validation.json` | `9af5cae6c0de91478c6e1963a35f1c5db0adfbf52e5be5738763931a4f76d293` |

All three forms—unmerged PEFT adapter, official PEFT merge, and independently
loaded native-produced model—executed through the pinned **Python** loaders.
All ten input token sequences and discrete outputs match in all three
pairwise comparisons, using the unchanged `5e-4` confidence bound. This is
trained-artifact interoperability and numerical equivalence. The subsequent
separate [trained-execution proof](../zig/pkg/inference/scripts/gliner25/TRAINED_EXECUTION_CHECK.md)
also passes Zig CPU and Metal for both materialized small files: 120 exact
token/decision and fixed-confidence comparisons against the three Python
forms, plus 20 CPU–Metal comparisons including native output metadata.
All four runs completed their 12-event protocol, closed the model owners and
removed the private artifact copy. The largest aligned confidence difference
was `2.86102294921875e-6`, within the unchanged `5e-4` bound.
The fixed authored requests do not establish held-out quality, convergence,
other-model/rank support, performance or release qualification.

Independent review rehashed the original/source/adapter/job/output identities,
reconstructed every captured phase from the raw protocol events and rederived
the reported comparisons without loading a model again. Original success
receipts and all frozen checker/helper bytes were retained unchanged.
The new independently rederived receipt is
`/private/tmp/antfly-gliner25-materialization-v1/trained-execution-validation-v1.json`,
SHA-256 `ea3c2fb430d10d68fd5a0db0e5449ebae76008600698868147508db4f4e826eb`.
It preserves the earlier `validation.json` with its historical Python-only
scope. The separate inactive-adapter training-update correctness item remains
open; loading, materialization and inference parity do not prove that behavior.
