# Controlled-dropout mixed-task training oracle

The tiny Python capture is complete for full-parameter, head-only, LoRA and
DoRA training with dropout `0.125` in the encoder, boundary/task heads and
adapters. Two complete executions produced byte-identical artifacts. All four
source profiles passed the existing objective, gradient-presence, gold
coverage, two AdamW flushes and exact mid-window resume checks. All four native
CPU consumers also passed every component loss, every gradient's
absence/zero/value, durable fresh-controller mid-window restore and both AdamW
updates. All four complete controlled-dropout GPU consumers also passed the
pinned/native losses and gradients at unchanged tolerances, under the semantic
pair/mask correspondence below. Their per-microbatch weights come from the
capture; they do not prove GPU optimizer updates or resume. No production model
or corpus was loaded, and `qualification` remains false.

[`capture_training_step_dropout.py`](capture_training_step_dropout.py) reuses
the immutable [zero-dropout composition driver](capture_training_step.py),
whose SHA-256 is
`844069a5b1c8139eb809de303562f436dc4502973b9226c8c441511c252c4810`.
It modifies only dropout configuration and dropout operations. The actual
pinned Fastino preprocessing, candidate/relation selection, record matching,
losses, backward, accumulation, clipping, AdamW and resume methods execute
through that unchanged driver. Unknown live dropout sites and implicit
functional training dropout fail capture.

Every microbatch supplies a complete set of named float32 inverted masks,
whose values are exactly zero or float32 `1 / (1 - 0.125)`. Mask bits depend
on site name, microbatch and element index, independently of weights, gold
or predictions. This is an explicit replay contract, not a claim that native
and Torch random-number streams are equivalent.

Each mask declares its intended native name and shape; the consumer validates
the complete set against finalized `Plan.dropoutDescriptors`. The source
classifier scores individual task slices; the fixture maps those rows to
the prepared padded classification-marker order. Its unsupervised classifier
has a complete native mask but no source classifier call. Source relation
training retains padded `[B,R,pair_cap]` proposal rows; the capture assigns
mask rows in the source's compact valid-pair order. Invalid source rows have
zero final-logit cotangents. The native mask capacity is the actual
relation-task count times `pair_cap`. Exact source row metadata accompanies
every mask.

The complete Metal consumer compares selected relation identities and labels
through an exhaustive one-to-one permutation, including every valid pair and
both representations' padding. It does not require identical ranking order
across floating-point implementations. For example, two reversed pairs in the
dropout fixture have mathematical probability products differing by about
`8.3e-9`, and both round to float32 `0.4300095439`. A scalar float32 evaluation
retains exactly the same 240 directed pairs but swaps two source rows; the
zero-dropout fixture has eight reordered rows.

For a mixed controlled-dropout step, a test-only discovery pass records the
backend's permutation. The consumer then transports only the relation hidden
mask rows, so each semantic pair receives its exact source mask, and replays
the step. Every other mask remains unchanged. Replay must preserve the
backend's full ordered decision arrays and detached inside-mean bytes; each
run still seals its actual physical mask/input bytes. Source membership,
labels, all losses and all gradients retain their existing checks and numeric
tolerances. This correction changes neither production ranking nor fixture
bytes. All eight zero/controlled-dropout GPU profile comparisons passed in
`/private/tmp/gliner25-managed-metal-integration-v3.log`, SHA-256
`aa17d7b1439b18324f5caafb5aaf5e2c40492f6b8395aad153d6e94c5fcb39b2`.
The process exited successfully: 15 selected tests passed with zero skips and
zero leaks. This is exact selected-pair membership/label coverage and
backend-local ordered replay, not bit-identical cross-backend ranking order.

Boundary SDPA is expanded only to apply the explicit probability mask. On
the actual `[2,2,18,4]` Q/K/V shape, its zero-dropout forward and all three
input gradients were checked against genuine Torch SDPA; the largest absolute
difference was `1.1920928955078125e-7`. Encoder and adapter dropout use the
actual model modules, including separate content and relative-position calls
through shared query/key projections.

| Profile | Mixed / negative / negative mask counts | Source resume |
| --- | --- | --- |
| Full parameters | 21 / 20 / 20 | Exact |
| Head only | 21 / 20 / 20 | Exact |
| LoRA | 37 / 36 / 36 | Exact |
| DoRA | 37 / 36 / 36 | Exact |

The four files under
[`training_step_dropout`](../../testdata/gliner25/training_step_dropout)
total 10,266,387 bytes. The capture and tensor hashes are:

| File | Bytes | SHA-256 |
| --- | ---: | --- |
| `capture.json` | 2,574,918 | `5b2ce5ceee254002bea98bdf63024395be14cb53328d6d6d89bbb7e9d43d43e7` |
| `tensors.safetensors` | 7,684,320 | `bbc1c28c5fd7ba0925068a6063492ece2392800fe031e61060ae360eb998da60` |

The tokenizer bytes are identical to the existing zero-dropout fixture.
All four files and the generator are enrolled in `reference_manifest.json`
after the four native consumer tests passed. Existing zero-dropout fixture
bytes and frozen evaluation helpers remain unchanged.

With the shared compute lane assigned and a new output destination:

```sh
PYTHONDONTWRITEBYTECODE=1 OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 /private/tmp/antfly-gliner25-oracle-venv/bin/python zig/pkg/inference/scripts/gliner25/capture_training_step_dropout.py --output-dir /private/tmp/gliner25-training-step-dropout-new
```

The successful local copies are
`/private/tmp/gliner25-training-step-dropout-all-v1` and
`/private/tmp/gliner25-training-step-dropout-repeat-v1`. The preceding
`dropout-probe-v1` contains only the initial full-parameter probe and is not
the four-profile fixture. Native evidence was recorded in
`/private/tmp/gliner25-training-source-session-v2.log`: all four controlled-mask
profile tests passed. The subsequent v3 hardware checkpoint also passed all
four controlled-dropout GPU consumers and all four zero-dropout GPU consumers.
Separate tests in that checkpoint establish tiny GPU Controller updates and
managed full/head-only exact resume. Published-model GPU jobs, managed adapter
updates, real-checkpoint convergence and public training-run qualification
remain separate gates. See [the scoped training evidence](../../../../../docs/GLINER25_TRAINING.md).
