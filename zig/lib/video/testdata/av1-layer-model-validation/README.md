# Scalable AV1 pretrained presentation preservation

Recorded run results and output filenames below describe the measured historical
epoch. Generated reports/logs were removed from the current tree; see
[qualification and historical recovery](../../../../../docs/plans/native-video-noncuda-audit.md#artifact-policy-and-historical-evidence).
Fixture inputs and independent reference oracles remain checked in.

Model WebM planning and selected replay now share operating point zero with
highest-spatial-layer presentation. Lower layers remain decoded as dependencies;
they do not become extra model sampling pictures. Explicit all-layer output
remains available to library callers. Admission preflight, cache producers and
ordinary inference all use the same model policy.

A packet-preserving WebM corpus adds the pinned two-spatial/three-temporal-layer
AV1 clip to the nine depth/chroma controls. Independent FFmpeg output matches
all 12 top-layer AOM picture hashes; repeated generation reproduces all 12 corpus
artifacts byte-exactly. The regression compares the library's 24-picture timeline
with the model's 12-picture timeline, selected IDs 0/6, admitted pixel count and
independent planar hashes. The complete macOS model suite passes 51 tests.

The old server fails the scalable embedding gate (maximum coordinate error
0.03649, cosine 0.95742) and changes one query's ranking. The corrected server
preserves all ten video vectors, both queries, exact rankings and top-1/5/10
overlap 1.0 on native CPU, Metal and actual Linux ARM64 CPU. Gates are unchanged.
Linux compares native execution with the pinned PyTorch CPU reference generated
on macOS; it is not a fresh Linux PyTorch run. These generated fixtures establish
reference preservation, not end-user relevance or arbitrary scalable conformance.

Metal completes every vector/ranking comparison, then its retry is capacity-denied; its overall stress report remains failed.
CPU/Linux disconnect/retry and ten serial sustained requests complete. Peer
disconnect does not prove cancellation completion. Broader scalable streams,
operating-point switching, display policies, real-stream/fuzz campaigns,
full-pipeline load/performance and the remaining non-CUDA scope stay active.

Reproduce generation with `zig/pkg/inference/scripts/embeddinggemma2/generate_av1_model_corpus.py
--include-scalable --output zig/pkg/inference/scripts/embeddinggemma2/testdata/av1-layers`.
Run `zig/pkg/inference/scripts/embeddinggemma2/video_compare.py` with that corpus, the pinned
checkpoint, CPU/native/Metal backends, `--warmups 0 --iterations 1 --threads 4
--concurrency 1 --sustained-rounds 1`, and `--process-memory-budget-mb 4096`
for owned native servers. Use this `pytorch-cpu.json` as `--reference`.
Linux runs the static `aarch64-linux-musl` server inside the pinned ARM64 Python
image with network disabled and source/checkpoint read-only. No checkpoint or
executable is distributed. Earlier failed sandbox GPU and nonplanar test-oracle
runs remain as historical diagnostics; final tests use the independent planar
oracle. Production layer policy was frozen before either server build; subsequent
edits changed only test hash layout. `qualification.json` binds the evidence.
