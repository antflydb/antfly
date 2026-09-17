# GLiNER2.5 CUDA training PR readiness

Status: implementation and CUDA training parity are ready for review. No
commit or push is part of this work. The broad production qualification gate
remains closed until the explicitly listed scope is exercised.

## Closed gates

- Native CUDA training uses an explicit absolute cuBLAS library when requested,
  fails closed on invalid paths or symbols, reports the vendor version, and
  binds that version into the retained-training checkpoint fingerprint.
- The pinned Python CUDA worker reports the cuBLAS version behind its current
  handle. The integrated comparisons use cuBLAS 12.8 on both arms.
- The rebuilt CUDA test binary passes 113 selected tests, with three
  Metal-only skips. The full-model checkpoint test passes in an isolated run;
  its earlier full-suite admission interruption is retained as historical
  evidence and did not require changing guards or assertions.
- Focused Compute Sanitizer memcheck passes the CUDA training cuBLAS,
  clipping, and seeded-trainer cases with zero errors.
- Python training contract tests: 23 passed.
- Four integrated 100-update comparisons pass all final tensor gates:

  | Model/mode | Trainable tensors | Native examples/s | Python examples/s | Native/Python latency | 95% interval |
  | --- | ---: | ---: | ---: | ---: | ---: |
  | small heads, B8 | 136 | 90.16 | 52.75 | 0.590 | 0.579–0.610 |
  | small full, B2 | 334 | 18.77 | 9.03 | 0.482 | 0.477–0.487 |
  | small full, B8 | 334 | 40.01 | 33.84 | 0.850 | 0.838–0.864 |
  | multilingual heads, B8 | 136 | 74.70 | 52.22 | 0.701 | 0.684–0.719 |

  Each interval is the paired log-latency 95% confidence interval; values below
  one favor native. Every row has matching cuBLAS 12.8 identities and no
  parity failures. Weights, gradients, and Adam moments are checked at every
  scheduled state. The benchmark’s separate `performance_qualified` flag stays
  false until the broader release gates below are met.

## Remaining release scope

These are qualification items rather than known implementation defects:

1. Run sustained full and head training for base and multilingual models, and
   qualify LoRA and DoRA after freezing and recording their authoritative
   adapter registration order.
2. Add a separately frozen real-data matrix covering entity, classification,
   relation, record, and joint extraction tasks. Compare held-out quality and
   require the documented 0.5 percentage-point bound.
3. Select the fastest quality-qualified Python CUDA baseline per row, including
   `torch.compile` and FlashDeBERTa where supported. Keep selection and
   measurement samples separate.
4. Repeat correctness and performance on supported Ampere and Hopper devices;
   current physical evidence is the NVIDIA L4 (SM89).
5. Archive the final binaries, source identities, library hashes, benchmark
   reports, sanitizer logs, and fixture manifests outside temporary storage
   before adding any production qualification registry entry.

BF16/FP16 training, CUDA graphs, persistent workspaces, and new cuBLASLt tuning
remain out of scope until the FP32 matrix above is complete. Production
qualification entries must remain absent for untested model, precision, task,
adapter, or hardware combinations.

## Review commands and evidence

The final source passes `git diff --check`. The reproducible benchmark harnesses
are `benchmark_training_cuda.py`, `training_cuda_worker.py`, and the existing
GLiNER2.5 fixtures under `testdata/gliner25`. Current reports are retained in
the local evidence campaign under `/tmp/gliner25-v41-*`; they must be copied to
durable evidence storage as part of release preparation. The working tree is
intentionally uncommitted and unpushed.
