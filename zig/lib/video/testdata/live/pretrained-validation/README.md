# Pretrained live-window reference preservation

The real Sintel fixture is `tools/embeddinggemma2/testdata/live-pretrained`.
It derives six seconds beginning at second 20 of the existing pinned trailer,
scales to 128x72 at 10 fps, and uses single-threaded libx264 with one-second
closed GOPs in fragmented MP4. FFmpeg is only an offline fixture/reference tool.
Fixture metadata pins both compressed output and original source hashes and
records the initialization/media boundaries. Sintel is by the Blender
Foundation, licensed under CC BY 3.0; the existing trailer provenance is retained
in `tools/embeddinggemma2/testdata/retrieval-long`.

Pinned pretrained native CPU, Metal and actual Linux ARM64 run the real
`embedNextLive` adapter over those units. Each backend produces three windows
with source intervals [0.2,2.2), [2.2,4.2), [4.2,6.2) seconds. Every streamed
vector is exactly equal to complete-clip source-time inference, with 246 encoder
tokens per window. Query vectors use the same loaded model. All vector gates,
cosine-score/ranking gates and top-1/top-3 overlap pass against pinned PyTorch
CPU, with exact rankings and maximum score drift below 5e-7 on this fixture.
Pinned PyTorch MPS also completes the same windows/queries and passes F32 vector
gates against its CPU reference. Those completed timing samples are retained.

The executable uses the existing worker supervisor and hard-cancellation
watchdog; process-required protection spans load, inference and close. The
first Metal attempts lacked that request boundary and correctly failed with
`ProcessIsolationRequired`; final runtime reports qualify the protected path.
The Python driver terminates/reaps its process group on timeout and requires
completed reports, every interval, token counts, exact baseline equality and
pinned reference vectors. Negative verifier tests reject incomplete reports,
changed intervals/token counts, baseline drift and query-vector drift.

Reproduce from `zig/pkg/inference`, using the recorded Zig compiler:

```sh
zig build build-embeddinggemma2-live -Doptimize=ReleaseSafe --prefix /tmp/live
zig build build-embeddinggemma2-live -Dtarget=aarch64-linux-musl -Doptimize=ReleaseSafe --prefix /tmp/live-linux
```

From the repository root with the already acquired pinned bundle and pinned
Transformers environment:

```sh
python3 -B tools/embeddinggemma2/video_live_qualification.py --model-dir /path/to/model --backend cpu --output /tmp/oracle.json
python3 -B tools/embeddinggemma2/video_live_qualification.py --model-dir /path/to/model --binary /tmp/live/bin/antfly-embeddinggemma2-live --backend native --reference /tmp/oracle.json --output /tmp/native.json
# Repeat with --backend metal. Execute the Linux binary on actual Linux.
```

This qualifies one real six-second clip and two queries, not broad codec/stream
conformance or end-user relevance. Timings are completed shared-host diagnostics;
file acquisition/model loading, peak memory and sustained load are not measured.
Broader field/dynamic streams, automatic frontiers for them, cross-window
projection reuse and reduced frontier decoding remain in the full active goal.
The JSON receipt binds source, binaries, fixture, checkpoint and oracle scopes.
Weights remain outside the repository. Copied reports remove trailing blank
lines for Git whitespace checks.
