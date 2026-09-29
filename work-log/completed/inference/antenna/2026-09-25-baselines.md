# Antenna baselines (step 0), 2026-09-25

Step 0 of the Antenna plan (zig/pkg/inference/models/antenna/ANTENNA.md):
accuracy, calibration and zero-shot NER for the models Antenna is measured
against, and encoder latency for the GLiNER2.5 and Laya encoders. These are
the targets the step 1 pilot has to meet. Nothing here is a qualification.

Machine: Apple M4 Max (36 GiB), macOS 15.6.1. Python 3.12.3, torch 2.9.1,
transformers 4.55.4, upstream GLiNER2 at `3c913c7` (scripts/gliner25/oracle.py).
Zig 0.16.0, ReleaseFast. Branch `antenna/baselines`.

## Models

| Name | Checkpoint | Revision | Weights SHA-256 | Path |
|---|---|---|---|---|
| decide | fastino/GLiNER2.5-Decide (span, DeBERTa-v3-large) | `7ee5da4c2415e32259bcdc0b1a7367c32ce8d6f6` | `40a5a23f…6dc997` | upstream PyTorch, MPS |
| gliner2.5-base | fastino/gliner2.5-base-v1 (boundary, DeBERTa-v3-base) | `72ac19b486cd4557424c8d61114e7530c243e9b0` | `7274094d…dc277a` | upstream PyTorch, MPS |
| gliner2-large | fastino/gliner2-large-v1 (span, DeBERTa-v3-large) | `bf90d758a5d482bbfc276041b8cb7b570e5318e3` | `92a76e84…10638e` | upstream PyTorch, MPS |
| laya | convaiinnovations/laya, released (ModernBERT-large) | `c5d78730f3493e4fe16d61507ef4b78eef7318cf` | `891102d3…cc8d86c` | native serving eval, Metal |
| laya-packed | the question-packed Laya fine-tune `fz0` from LAYA.md step 0 (one epoch on typed-decisions train) | local | `d2d77b4f…3cc67` | native serving eval, Metal |

## Method

Datasets and label names come from `scripts/antenna/antenna_datasets.py`
(pinned revisions and SHA-256s), the same module the pilot trains from.
Every test split is subsampled with seed 20260925: 500 classification
records, 300 NER sentences, and all 2,000 typed-decisions questions. The
reports keep the sampled ids.

- **GLiNER models** (`baselines.py`): each label is scored in one pass with
  upstream's `Classifier` (softmax probability for single-label tasks).
  Typed-decisions questions are single-label with label descriptions, and
  score questions are ordinal. NER uses `batch_extract_entities` at threshold
  0.5, scored as exact span and type micro F1. MPS and CPU agree to 1.5e-6 in
  probabilities and give identical spans.
- **Laya** (`laya_baselines.py`): the same records, converted to Laya choice
  questions and scored by `antfly-inference finetune eval laya` (serving path,
  with the model's calibration). Datasets with more labels than Laya admits
  (20) are not applicable. Typed-decisions questions whose state exceeds 316
  Laya tokens are dropped (160 of 2,000), as in LAYA.md's step-0 evaluation.
  The GLiNER models are also scored on that 1,840-question subset.
- ECE uses 15 equal-width bins of top-label confidence.

"In-domain" and "held-out" refer to the Antenna pilot's training data (it
trains on the train splits of Banking77, AG News, CrossNER AI, literature and
music, and MIT restaurant), not to these models' training.

## Results (seeded subsamples)

Accuracy (classification) or exact span micro F1 (NER).

| Group | Dataset | decide | gliner2.5-base | gliner2-large | laya | laya-packed |
|---|---|---:|---:|---:|---:|---:|
| in-domain | banking77 | 0.730 | 0.724 | 0.702 | n/a | n/a |
| in-domain | ag_news | 0.738 | 0.732 | 0.726 | 0.932 | 0.878 |
| in-domain | crossner_ai | 0.503 | 0.506 | 0.512 |  |  |
| in-domain | crossner_literature | 0.485 | 0.521 | 0.565 |  |  |
| in-domain | crossner_music | 0.616 | 0.681 | 0.667 |  |  |
| in-domain | mit_restaurant | 0.422 | 0.463 | 0.455 |  |  |
| held-out | clinc150 | 0.630 | 0.588 | 0.556 | n/a | n/a |
| held-out | sst5 | 0.412 | 0.442 | 0.486 | 0.476 | 0.214 |
| held-out | typed_decisions | 0.471 | 0.418 | 0.515 | 0.367* | 0.442* |
| held-out | crossner_politics | 0.597 | 0.576 | 0.652 |  |  |
| held-out | crossner_science | 0.492 | 0.550 | 0.549 |  |  |
| held-out | mit_movie | 0.514 | 0.418 | 0.431 |  |  |
| in-domain | mean classification accuracy | 0.734 | 0.728 | 0.714 | 0.932 | 0.878 |
| in-domain | mean NER F1 | 0.506 | 0.543 | 0.550 |  |  |
| held-out | mean classification accuracy | 0.504 | 0.483 | 0.519 | 0.421 | 0.328 |
| held-out | mean NER F1 | 0.534 | 0.515 | 0.544 |  |  |

\* On the 1,840 questions Laya admits; the Laya means cover fewer datasets.

Calibration and secondary metrics:

| Dataset | decide | gliner2.5-base | gliner2-large | laya | laya-packed |
|---|---:|---:|---:|---:|---:|
| banking77 | ECE 0.119, mF1 0.709 | ECE 0.186, mF1 0.705 | ECE 0.219, mF1 0.689 | n/a | n/a |
| clinc150 | ECE 0.122, mF1 0.623 | ECE 0.222, mF1 0.601 | ECE 0.288, mF1 0.634 | n/a | n/a |
| ag_news | ECE 0.195, mF1 0.732 | ECE 0.239, mF1 0.726 | ECE 0.235, mF1 0.708 | ECE 0.034, sCE 0.204 | ECE 0.370, sCE 0.750 |
| sst5 | ECE 0.124, mF1 0.401 | ECE 0.403, mF1 0.370 | ECE 0.463, mF1 0.421 | ECE 0.077, sCE 1.138 | ECE 0.138, sCE 1.479 |
| typed_decisions | ECE 0.106, mF1 0.382, sCE 1.158 | ECE 0.357, mF1 0.268, sCE 2.551 | ECE 0.299, mF1 0.311, sCE 2.821 | ECE 0.179, sCE 1.332 | ECE 0.053, sCE 1.121 |

Typed-decisions on the same 1,840 questions for every model:

| Model | Accuracy | Soft CE |
|---|---:|---:|
| decide | 0.482 | 1.154 |
| gliner2.5-base | 0.412 | 2.559 |
| gliner2-large | 0.509 | 2.860 |
| laya | 0.367 | 1.332 |
| laya-packed | 0.442 | 1.121 |

By question kind (all 2,000 for GLiNER): Decide 0.473 choice, 0.482 noul,
0.461 score; gliner2.5-base 0.340, 0.545, 0.381; gliner2-large 0.435, 0.677,
0.455.

Readings:

- Decide leads intent classification (Banking77 0.730, CLINC150 0.630) and is
  much better calibrated than the other GLiNER checkpoints on every
  classification set (typed-decisions ECE 0.106 vs 0.30–0.36, soft CE 1.16 vs
  2.55–2.86).
- gliner2-large has the best NER (in-domain 0.550, held-out 0.544) and the
  best typed-decisions argmax (0.509 on the shared subset), but is poorly
  calibrated. Decide gives up about 0.03–0.05 NER F1 to it.
- Released Laya is strong on AG News (0.932, ECE 0.034) and weak zero-shot on
  typed-decisions (0.367, matching LAYA.md's 0.387 on its own subset). Its
  model card lists AG News as an evaluation but does not say whether AG News
  was in training, so treat 0.932 as possibly in-domain for Laya.
- The packed Laya fine-tune is the best-calibrated typed-decisions model
  (ECE 0.053, soft CE 1.121), as expected after one epoch on its train split,
  but loses general classification (SST-5 0.214, AG News ECE 0.370).
- For the pilot: in-domain targets are Decide's classification (0.734) and
  gliner2-large's NER (0.550); held-out targets are about 0.50–0.52
  classification and 0.53–0.54 NER, with Decide's calibration as the bar.

## Encoder latency

Encoder only, batch 1, median of seven warm runs after two warmups
(`src/bench/antenna_encoder_timing_test.zig`, ReleaseFast). GLiNER2.5 runs its
serving encoder on a real processor batch (a two-type entity schema plus
repeated English text, so the sequence includes the schema prefix).
Laya runs the generic ModernBERT forward on the session's backend, not its
fused decision kernels (which cover only unpacked rows up to 512 tokens).

| Encoder | Backend | ~64 tokens | ~512 tokens | ~2048 tokens |
|---|---|---:|---:|---:|
| gliner2.5-base (DeBERTa-v3-base, 184M) | CPU | 28.9 ms (65) | 291.8 ms (516) | 3,534.5 ms (2,165) |
| Laya (ModernBERT-large, 395M) | CPU | 310.6 ms | 1,473.0 ms | 12,719.8 ms |
| gliner2.5-base | Metal, `reference_v1` | 224.4 ms (65) | 324.4 ms (516) | 873.4 ms (2,165) |
| Laya | Metal, generic forward | 209.3 ms | 780.8 ms | 4,903.0 ms |

Actual GLiNER token counts are in parentheses (the processor rounds to whole
words). Caveats:

- The two encoders differ in size (base vs large), so this compares the
  deployed baselines, not the architectures at equal size.
- GLiNER2.5 on Metal ran `reference_v1`, which uploads weights per request.
  The served `optimized_v2` path keeps weights resident, but preparing it
  needs the serving process-isolation watchdog, which this offline test does
  not have; it was not timed here.
- Laya's fused Metal decision kernels (LAYA.md: about 58 ms for a 55-token
  decision) are faster than the generic forward timed here at 64 tokens.
- CPU uses the native backend with system BLAS threads.

## Commands

```sh
# datasets (cached under $ANTFLY_ANTENNA_DATA)
python zig/pkg/inference/scripts/antenna/antenna_datasets.py --summary
# GLiNER checkpoints
python zig/pkg/inference/scripts/antenna/baselines.py --model-dir <dir> \
  --model-id <hub id> --revision <sha> --upstream <GLiNER2 at 3c913c7> --output <report.json>
# Laya (binary from `zig build -Doptimize=ReleaseFast` in zig/pkg/inference)
python zig/pkg/inference/scripts/antenna/laya_baselines.py --binary <antfly-inference> \
  --model-dir <laya dir> --output <report.json>
# tables
python zig/pkg/inference/scripts/antenna/baselines_tables.py decide=<report> ... --typed-subset laya
# encoder latency (from zig/pkg/inference)
ANTFLY_ANTENNA_TIMING_GLINER25=<gliner2.5-base dir> ANTFLY_ANTENNA_TIMING_LAYA=<laya dir> \
ANTFLY_ANTENNA_TIMING_BACKEND=native|metal \
  zig build test -Doptimize=ReleaseFast -- --test-filter "antenna encoder timing"
```

Wall time: GLiNER subsample runs took 2.5–5.5 minutes per model on MPS;
Laya runs about 2 minutes per dataset on Metal.

## Caveats

- Subsamples: 500 classification records and 300 NER sentences per dataset
  give roughly ±2–4 points of sampling noise; small differences between
  checkpoints are not significant. A full-split pass (`--full`) was started
  and stopped: Decide's full Banking77 (3,080 records) scored 0.702 accuracy,
  ECE 0.106 (subsample 0.730, 0.119), but full CLINC150 (5,500 records, 151
  labels) ran over 30 minutes on MPS, which put the full pass for all five
  models past the time budget.
- Fastino's "Fast Decisions" figures (Decide 60.1%, Laya 46.6%) are a
  different, vendor-run suite and are not comparable to these numbers.
- Label names are the natural-language forms in `antenna_datasets.py`
  (for example "card arrival", "science and technology"); prompt wording
  moves zero-shot scores by a few points.
- NER text is the dataset tokens joined by single spaces, so detokenization
  artifacts ("model s") are part of the input for every model.
- The packed Laya fine-tune was trained on typed-decisions, so that dataset
  is in-domain for it.
