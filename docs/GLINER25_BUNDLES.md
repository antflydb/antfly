# GLiNER2.5 native bundles

The boundary architecture uses a complete, single-file GGUF and the original
four configuration/tokenizer sidecars. It does not use the legacy GLiNER2
encoder/head split bundle. Original checkpoint tensor names are preserved.

Build the converter from `zig/`:

```sh
ZIG_GLOBAL_CACHE_DIR=/private/tmp/antfly-gliner25-zig-cache zig build inference-gliner25-convert-build -Dmetal=false -Dcuda=false -j1
```

The delegated build uses ReleaseFast and one build job. From the repository
root, convert an already downloaded, complete checkpoint:

```sh
zig/pkg/inference/zig-out/bin/antfly-inference-gliner25-convert --model-dir /models/gliner2.5-small-v1 --output-dir /models/gliner2.5-small-q8 --precision q8_0
zig/pkg/inference/zig-out/bin/antfly-inference-gliner25-convert --verify-dir /models/gliner2.5-small-q8
```

The output parent must exist. The destination must not exist. Conversion writes
a private sibling directory, validates the finished tensor file, writes its
receipt, syncs files and directories, and publishes with an atomic exclusive
rename. Linux uses the native no-replace rename; macOS uses `RENAME_EXCL`.
Other publication platforms fail explicitly. A competing creator cannot be
overwritten. Failed unpublished conversions remove their own staging directory.
If the final parent durability barrier fails after publication, the complete
destination remains; verify it before retrying with another destination.

| Precision | Small | Base / multilingual | Encoder matrices | All other tensors |
| --- | --- | --- | --- | --- |
| `fp32` | Supported by converter | Supported by converter | FP32 | FP32 |
| `fp16_encoder` | Supported by converter | Supported by converter | FP16 | FP32 |
| `q8_0` | Supported by converter | Supported by converter | Q8_0 | FP32 |
| `q4_0` | Supported by converter | Rejected | Q4_0 | FP32 |
| `q4_k` | Rejected | Supported by converter | Q4_K | FP32 |

The version 1 tensor policy casts or quantizes only the declared encoder linear
matrices and word embedding table. Biases, normalization, relative-position
embeddings, learned boundaries, and every extraction/task head remain FP32.
All 334 tensor names, shapes, byte counts, types, and quantization row alignment
must match the variant inventory. Nonfinite source values and values outside
the supported reduced-precision scale range are rejected. Conversion never
silently skips an unknown or malformed tensor.

`antfly_inference_bundle.json` records architecture/config/policy versions,
backbone, precision, and SHA-256 plus byte length for all five source files and
all five output files. Configuration and tokenizer files are copied verbatim.
The GGUF metadata independently records the source weight hash and profile.
The loader hashes its actual opened GGUF mapping, validates its complete tensor
inventory, and checks the exact metadata bytes consumed by the configuration
and tokenizer parsers. The managed tokenizer loader checks the same bytes it
passes to the tokenizer parser. Original single-file SafeTensors loading also
validates the header of the opened store it uses, rather than a separate copy.

A receipt provides content integrity; it is neither a publisher signature nor
a numerical qualification. Changing the receipt and model together requires a
new external artifact identity and qualification record. In-place mutation of
an actively mapped model is unsupported; publish immutable version directories
and replace the selected model through the existing model lifecycle.

Validation in this implementation includes an actual small FP32 conversion,
roundtrip inventory verification, existing-destination rejection, and rejection
by the real session loader of a same-size modified weight file. All nine reduced
variant/profile combinations have also been converted and verified against the
334-tensor policy. This establishes conversion and integrity behavior, not
task-quality or performance qualification of reduced precision. Public boundary
runtime promotion remains subject to the gates in
[GLINER25_IMPLEMENTATION.md](GLINER25_IMPLEMENTATION.md).

The test that uses a real checkpoint runs only when
`ANTFLY_GLINER25_SMALL_MODEL_DIR` is set. The converted FP32 session/wire parity
test uses `ANTFLY_GLINER25_SMALL_FP32_BUNDLE_DIR`. Missing model variables produce
explicit skips; they do not count as model evidence.

The diagnostic runner exercises the real session loader for a converted
bundle. Build its native version with `zig build inference-gliner25-bundle-check-build
-Dmetal=false -Dcuda=false -j1` from `zig/`, then run from the repository root:

```sh
python zig/pkg/inference/scripts/gliner25/check_bundles.py --backend native --binary zig/pkg/inference/zig-out/bin/antfly-inference-gliner25-bundle-check --bundle /models/gliner2.5-small-q8 --output-dir /tmp/gliner25-small-q8-native
```

Use the pinned oracle environment, which supplies `psutil`. The driver never
downloads or executes a Python model. It checks encoder token IDs against the
compact evidence derived from the completed pinned Fastino/native benchmark,
binds source model/fixture/binary hashes, runs one bundle at a time with a
6 GiB sampled RSS guard, and records exact public ordering, labels, surfaces,
offsets, and confidence differences. Expected outputs are deserialized only in
the independent comparison driver, after inference. The executable verifies
the actual bundle on load and rehashes it again before reporting completion.

For Metal, build with `-Dmetal=true -Dcuda=false -j1` and retain the completed
native report. Use the exact same converted directory:

```sh
python zig/pkg/inference/scripts/gliner25/check_bundles.py --backend metal --binary zig/pkg/inference/zig-out/bin/antfly-inference-gliner25-bundle-check --bundle /models/gliner2.5-small-q8 --native-reference-report /tmp/gliner25-small-q8-native/report.json --output-dir /tmp/gliner25-small-q8-metal
```

Version 2 reports require the runner's `strict_f32_activations_v1` policy and
record separate source-FP32 quality diagnostics and same-bundle backend parity.
The Metal driver rejects an incomplete, old, or different native report before
execution. It binds the native report hash and binary identity, the current
driver contract, all source and converted file pins, precision, model, fixture,
ordered cases and exact token sequences. The CPU and Metal binaries may differ;
their hashes are both retained. Expected labels never enter the executable's
inference contract.

Same-bundle parity requires exact ordered labels, surfaces, spans, typed and
derived relations, record anchors, scalar/list presentation, attribute modes,
and solver status/exhaustion. Confidence comparison keeps the fixed absolute
tolerance `5e-4`, including record confidence absent from the older FP32 fixture
format. Solver work counters and utilities are retained as separate evidence.
A parity failure saves all completed comparisons and exits unsuccessfully.
Source-FP32 changes do not become backend failures when both backends produce
the same converted-model result; they still require held-out quality evaluation.
The nine driver tests use synthetic worker responses and make no model claims.

The first complete diagnostic used ReleaseFast binary SHA-256
`2996fefd7c30b9df144138d97058ff60306b98956d53a9b00c3e88b3919635a6`.
That version used the generic native reduced-linear path, which can quantize
activations. Its reports are historical diagnostics of that arithmetic and are
rejected as references for the corrected strict CPU/Metal comparison.
All ten requests had identical token IDs in every profile. The following
counts include ordering changes as public-output differences:

| Model / storage | Exact ordered decisions / 10 | Also within FP32 confidence tolerance / 10 |
| --- | ---: | ---: |
| Small FP32 | 10 | 10 |
| Small FP16 encoder | 10 | 9 |
| Small Q8_0 | 10 | 6 |
| Small Q4_0 | 7 | 2 |
| Base FP16 encoder | 10 | 9 |
| Base Q8_0 | 9 | 4 |
| Base Q4_K | 7 | 3 |
| Multilingual FP16 encoder | 9 | 9 |
| Multilingual Q8_0 | 9 | 3 |
| Multilingual Q4_K | 7 | 1 |

The FP32 absolute confidence tolerance is `5e-4`; widening it is not a quality
qualification. Observed changes include relation presence, attribute label
counts, and latent record counts, as well as ordering. These ten curated cases
cannot establish the allowed task-quality loss on held-out data. Conversion,
CPU execution, same-bundle CPU/Metal numerical parity, and quality relative to
published FP32 remain distinct release gates. `fp16_encoder` describes weight
storage; the initial strict device path retains FP32 activations and protected
tensors.

## Strict CPU and Metal diagnostic comparison

A fresh version-2 diagnostic run used the same ReleaseFast binary and exact
converted bundles for both backends. All 100 case comparisons passed: the nine
reduced bundles plus small FP32, with ten task requests per bundle. Tokens,
ordered decisions, source spans, attributes, relation endpoint identity, record
metadata and solver status matched; the absolute confidence tolerance stayed
at 5e-4. No timing or held-out quality claim follows from this corpus.

The profile `strict_f32_activations_v1` explicitly disables CPU activation
quantization and uses float activation operands and accumulation for the Metal
reduced-weight paths at every row count. Generic model/backend dispatch remains
unchanged. Older automatic-activation CPU reports cannot establish equivalence
with this profile.

| Artifact | SHA-256 |
| --- | --- |
| Native report `/private/tmp/gliner25-bundle-native-strict-v3/report.json` | `7ce959984c0bc87534736068bca7adeaa6162a4c6ecf5df932da3f48b4d0ceac` |
| Metal report `/private/tmp/gliner25-bundle-metal-strict-v3/report.json` | `a2e87cce22bc0b051a45a28f2ff54d007c160058657ab9409fde2f28a4c9d198` |
| Executed binary | `6abf00a5a2baf74038d3803df75d2a0f9999cf117af4013fe761dfcf24438166` |
| Diagnostic driver | `4c58f89ea38065223696752743aa5db89d4fa6f430fe21765e0d729c21dc8b74` |

Some reduced bundles still differ from their source FP32 output. In particular,
Q4 profiles matched seven of ten source decisions per variant; multilingual
FP16 matched nine. All Q8 profiles matched all ten source decisions under the
strict activation policy, though confidence changes exceeded 5e-4 on several
cases. These counts are diagnostic examples, not accuracy estimates. Promotion
still requires the declared task/slice quality gates and matched end-to-end
performance measurements.
