# PDF Render-Control Verification (2026-09)

> Relocated verbatim from `zig/PDF.md` (lines 5259–5344 at commit 271838a195) on 2026-09-16 during the documentation cleanup. This is a historical implementation log kept for context; the living design is [`PDF.md`](../../../zig/PDF.md). Durable decisions from this log were folded into that document before the move.

## History and verification notes

This section retains dated verification evidence for specific hardening work.
It is a record of what was checked and when, not part of the ongoing design
narrative above.

### September 2026 render-control verification

#### Follow-up: precommit sharing and image-row decoding

Precommit document extraction now uses the shared PDF window scheduler through
an invocation-owned execution session. It borrows immutable provider/backend
handles but owns its mutable scheduling, recovery and progress state; it never
installs a scheduler on the live replay runtime. Compatible reader/generator
consumers borrow the same page buffers and retain independent typed outputs.
The publication sink differs: replay uses fenced private durable rows, whereas
precommit uses node-admitted local result staging (64 MiB hard limit) and the
existing atomic primary/artifact commit. Staging pressure declines optional
sharing and preserves the ordinary bounded execution path. No page rasters
survive their consumer window. Source-fingerprint changes invalidate staged
results; forced reprocessing and unchanged-state checks stay with precommit's
ordinary publication path. Reuse binds to full source SHA-256, not the shortened
telemetry fingerprint. Terminal partial batches can share; a consumer that
declined an earlier nonterminal window stays disabled. Page-vector replay
checkpoints are not used by this precommit text-result sink.

The native image decoder has a pull-based row path for 8-bit DeviceGray/RGB/CMYK
images with raw or single-Flate streams, including TIFF/PNG predictors, color
keys, and same-size 8-bit soft masks with optional Matte correction. Color and
mask streams advance together into native-resolution RGBA. PDF bytes are
borrowed; owned decryption input, inflate history, predictor rows and RGBA are
charged to the existing image-tree allocator and aggregate render grant.
There is no full decompressed sample plane or full RGBA soft-mask allocation.
The 64 MiB materialized-stream ceiling remains in force for general streams
and individual rows; geometry bounds cumulative image work. Truncated or
surplus samples, invalid checksums, and cancellation stop decoding. The final
native RGBA result must still fit the hard image working-set limit.

Other codecs, color spaces, packed samples, predictor layouts and differently
sized masks retain their existing guarded paths; this is not a relaxation of
their limits or an automatic reduction in DPI. The formerly rejected academic
page has rendered at requested 150 DPI under the unchanged 256 MiB scratch cap;
the final full service run passes all 51 pages with no OCR failures. Two-consumer
precommit and replay probes each render nine pages exactly once, with equal
retained text/geometry and complete vector coverage for both consumers. Tests
also cover terminal-tail sharing and declined-prefix isolation. See
`scripts/bench/pdf/RESULTS-2026-09-09-STREAMING.md` for final qualification.

The initial source-build ablation exposed two admission defects: reserving every
lane's 128 MiB decode ceiling made parallel rendering impossible under the
default 256 MiB renderer cap, and speculative partial grants below that ceiling
were incorrectly returned as page failures before launching a worker.

The revised design separates hard decode limits from scheduling estimates.
Raster/fork estimates and measured per-document worker peaks choose concurrency;
every allocation still passes through the admitted, shared physical-memory cap.
Underestimated parallel work retries only failed identities serially, even when
no extra grant is available. Successful page buffers retain their owners.

Speculative windows carry bounded retry metadata for scratch-pressure failures.
After the prior window's consumers finish and release its grant, the coordinator
acquires fresh scratch admission and retries only those failed pages once. The
same mechanism serves OCR/generation and visual embedding. It preserves page
geometry, deadlines/cancellation and output credit, retains no invocation-local
callback pointers, and does not retry deterministic decode-limit violations.
Foreground failures do not create another speculative replay cycle.

Source tests and production qualification must verify these changes separately.
The 51-page corpus also includes an approximately 98.7 MB decoded image that
exceeds the unchanged 64 MiB materialized-stream ceiling. The image-row path
above addresses that input without relaxing benchmark gates or reducing DPI.
See `scripts/bench/pdf/` for the retained
failed experiments, render-control matrix and multi-consumer qualification.

Final Metal qualification at production source `29dd963a4` confirms four actual
render workers under the unchanged 256 MiB cap. Two durable-replay consumers
share nine physical page renders (rather than eighteen), retain identical text
and geometry, and publish 25 vectors each without an inference-worker restart.
The larger cohort's prefetch-dependent administration failures are fixed; only
the independently identified oversized decoded stream remains rejected.
These profiled checks establish correctness/reuse, not an elapsed-time ratio.
The separate, unprofiled nine-page comparison against pinned main `98d911a88`
passes all 12 quality-matched trials: warm elapsed is 8.826 s versus 6.574 s
(25.5% lower, 1.342× throughput). Host load remains uncontrolled; see
`scripts/bench/pdf/RESULTS-2026-09-08-RENDER.md` for controls and limitations.

