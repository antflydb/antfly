# Raw TS model qualification

Recorded run results and output filenames below describe the measured historical
epoch. Generated reports/logs were removed from the current tree; see
[qualification and historical recovery](../../../../../../../docs/plans/native-video-noncuda-audit.md#artifact-policy-and-historical-evidence).
Fixture inputs and independent reference oracles remain checked in.

Two synthetic closed-GOP/reordered B-frame clips are pinned as original MP4 and
native TS copy variants, plus two text queries. Native model tests compare
logical sampling, preparation plans and exact queued F32 patch hashes. These
fixtures do not establish broad real-world TS/codec conformance.

Pinned PyTorch, native CPU, Metal and actual Linux ARM64 preserve vectors,
retrieval rankings/top-k and video prompt-token counts. Per-backend TS/MP4
vectors match exactly. Source, fixture, checkpoint and binary hashes are in
`qualification.json`; weights are not committed. The shared-host single-sample
HTTP timings and four-request concurrency checks are diagnostic evidence only.

`video_compare.py` records/gates endpoint usage: video counts expanded encoder
tokens, while text usage counts lexical tokens. The reference obtains text
usage with `add_special_tokens=False` and separately records encoder tokens.
Earlier exploratory attention-mask-only text gates failed because BOS/EOS were
included in a lexical-usage comparison; video counts were already exact.

`video_transport_http_qualification.py` damages PAT data without repairing its
CRC and verifies preflight HTTP 400 (`INVALID_REQUEST`, `InvalidTransportCrc`)
without embedding publication. Runtime decoder failures use `INVALID_VIDEO`.
The producer failure helper forces resize fallbacks after actual Linux exposed
heap-placement-dependent allocation counts; production allocation is unchanged.

Reproduce with `zig build test-video-model` in the inference package. Use
`install-video-model-tests` for another host, and `bench-server` for pretrained
execution. From the repository root, run `zig/pkg/inference/scripts/embeddinggemma2/video_compare.py`
with this corpus, pinned checkpoint, CPU reference and native/Metal server;
controls and exact binary identities are retained in reports. The Linux image
ran with network disabled, only staged task inputs, binaries and output mounted.
Remaining original non-CUDA scope is active, including broader TS layouts,
SAR/HDR/VFR, real-world pretrained qualification and HLS model orchestration.
