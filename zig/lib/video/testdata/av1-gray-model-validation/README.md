# Monochrome RGB correction qualification

This phase corrects software WebM monochrome SDR preparation on CPU and Metal.
The independent exhaustive oracle covers every 8/10/12-bit code value under both
range declarations. macOS ReleaseSafe passes 246 video checks (two skips) and 49
model policy/lifetime checks. The new oracle also executes on actual Linux ARM64
in the pinned offline image. Linux pretrained model execution is not covered.

`native-cpu.json` completes all corpus vectors and fails the existing gate for the
seven remaining color formats. `metal-envelope.json` also completes all corpus
vectors, but its later cancellation/retry request is denied by memory admission.
The two monochrome vectors pass the unchanged pretrained vector gate in both
reports, independently verified in `qualification.json`. Complete-vector scores
and rankings are derived there from the retained vectors and pinned reference.
Full-corpus retrieval does not pass. Timings on this shared host are diagnostic.

The initial simultaneous Metal request and subsequent automatic-policy retry
both encounter transient capacity denial. The 4 GiB envelope run retains physical
memory checks and also records its later denial. These stress checks remain
failed; a partial vector correction does not establish full-pipeline qualification.
All logs and failed reports remain available. No tolerance is relaxed.

Reproduce with a fresh ReleaseSafe server and pinned model using the commands
from `../av1-model-validation/README.md`. The corrected Metal run adds
`--process-memory-budget-mb 4096`. The harness defaults to automatic policy and
now records this optional operator envelope in controls. Both complete corpus
runs remain failing commands until the other format/stress gaps are addressed.

`qualification.json` records source, fixture, report and binary identities. The
native and automatic-policy report tool hash identifies the parent commit's
harness. The envelope run used the CLI envelope patch before controls gained its
explicit field; its exact patch against that parent is retained and hash-checked.
The updated controls field affects future reports only. Runtime source identities
are recorded separately from measurement-tool revisions.

Next work remains chroma reconstruction and high-depth color quantization,
complete pretrained CPU/Metal/Linux parity, scalable-layer model policy,
full-pipeline admission/cancellation qualification and the full active non-CUDA
scope. The prior nine-format failing receipts remain historical evidence.
