# Native video non-CUDA qualification

The eight requested work groups are implemented within the declared contracts
in [MEDIA.md](../../zig/lib/media/MEDIA.md) and
[VIDEO.md](../../zig/lib/video/VIDEO.md). Runtime codecs are maintained in-tree;
FFmpeg/PyTorch and reference codec tools are qualification-only. Application
relevance belongs to Antfly users; model checks preserve reference vectors,
cosine scores, rankings and top-k sets.

## Results and scope

[The compact results record](../../zig/lib/video/testdata/qualification-summary.json)
identifies the measured checkpoint, final gates, platforms and resource effects.
These are historical measurements of the recorded sources, not a new performance
claim for every subsequent commit.

| Requested work | Implementation and qualification |
|---|---|
| WebM model integration | VP8/VP9/AV1 logical pictures feed batched vision/projector execution. Final Metal WebM checks pass eight vectors and three rankings; CPU and actual Linux reference-preservation runs have separate recorded epochs. |
| Broader streams, fuzzing and recovery | Retained-frame coverage-guided campaigns, corruption/restart tests and independent H.264/VP8/VP9/AV1 goldens. Three Big Buck Bunny scenes across nine streams match all 324 pictures on macOS and Linux; regeneration is exact. Recovery reports gaps; ordinary inference stays strict. |
| AV1 profiles/formats/layers | Profiles 0–2, 8/10/12-bit mono/4:2:0/4:2:2/4:4:4, Annex B and operating-point/layer policies have independent outputs. Both original constrained AV1 model suites pass complete numerical and lifecycle/load gates. |
| Performance and reference ownership | Realistic 480p/1080p codec benchmarks, measured portable SIMD preparation, selected-output materialization and progressive AVC reference sharing. Warm long-clip initialized-buffer bytes fall 99.72%; this is not a shared-host throughput claim. |
| Actual Linux execution | Offline ARM64 Linux executes the full decoder suite: 252 passes, 29 platform skips. Final portable model/admission suite: 54 passes, eight Metal skips. The separately scoped pretrained WebM run passes eight vectors, three rankings and lifecycle/load gates. |
| Sampling/display/windows | Logical AVC fields/partitions/planes, WebM picture identities, VFR source-time selection, explicit windows, rotation/SAR and declared HDR rendering. The 52-second clip and six exact streamed-vs-buffered TS windows exercise long/source-time paths. |
| Resident Metal and scheduling | Device token composition, backbone inputs, pooling/normalization; bounded batched preparation overlaps admitted execution. Shared admission, cancellation, retry and optional-constant reclamation pass device and production gates. |
| Live transport | Inherited DASH addressing/templates/BaseURL, selected representations, periods/live cursors; authenticated HLS master/delta/blocking reload, native AES-CBC, fMP4 part/parent reconciliation and streamed normalized AVC TS. |

All seven final production checks pass: AV1 layers and formats at their original
4096 MiB budgets, the full clip, WebM, depth/chroma, and streamed native CPU/Metal
TS. Maximum Metal coordinate errors are 3.9023e-7 for both constrained AV1 suites,
8.7964e-7 for the full clip, 6.2213e-7 for WebM and 3.7626e-7 for depth/chroma.
Score/ranking/top-k, cancellation/retry, half-close, concurrent completion and
cleanup gates pass without larger reservations, looser tolerances or a change
to the 16-retry limit. Streamed TS preserves all six buffered vectors exactly,
including retained-output retry and verification of 12 acknowledged AUs.

The post-main-merge Metal/model suite passes 66/66 with no skips. The Apache
boundary suite passes 35 checks and verifies 2,235 reachable sources. Final Metal
[build/runtime fingerprints](../../zig/lib/video/testdata/vision-weight-validation/staged-production-provenance.json)
remain separate from earlier Linux pretrained/full-decoder epochs. The latter
source audit found one test-only fixture-path rewrite among 256 files; decoder
runtime was unchanged. The executed Linux pretrained ELF had no interpreter or
DT_NEEDED codec libraries. Linux ARM64 execution does not establish native x86
hardware performance.

Warm 52-second clip initialized bytes fall from 9,695,085,672 to 27,477,096.
Steady owned runtime buffers increase from 632,222,724 to 1,240,134,660 bytes;
four warm requests at concurrency two have zero net live owned bytes/objects.
These observations exclude preparation textures/buffers, decoder surfaces and
opaque driver storage. They do not establish physical residency, bus traffic,
whole-pipeline memory or serving capacity. Earlier failed/control reports remain
failed; no allocator-relief hook ships.

Qualification covers bounded corpora and supported subsets, not arbitrary codec
conformance or origin interoperability. TS LL-HLS parts remain outside the
streamed TS contract. HDR/display tests qualify the declared rendering policy,
not every real-world mastering convention. CUDA is excluded.

## Reproduce

Use the repository-pinned Zig toolchain and the qualification environment pinned
by `tools/embeddinggemma2/reference.py` and `linux-reference.Dockerfile`. From the
repository root:

```sh
mkdir -p .benchmark-results/video
zig build --build-file zig/lib/video/build.zig test-video test-live-windows -Doptimize=ReleaseSafe
zig build --build-file zig/lib/media/build.zig test-media test-media-http -Doptimize=ReleaseSafe
zig build --build-file zig/pkg/inference/build.zig test-video-model -Doptimize=ReleaseSafe
python3 -m unittest discover -s tools/embeddinggemma2 -p 'test_*.py'
```

The Linux [qualification workflow](../../.github/workflows/zig-video-qualification.yml)
acquires the pinned checkpoint, generates independent PyTorch references and
runs native codec/model/recovery/display checks. Its generated reports go into
`.benchmark-results/` and upload as CI artifacts. Cross-compilation alone does
not count as actual Linux execution.

For a local pretrained comparison, build the inference benchmark server, supply
the verified checkpoint and use a checked-in independent oracle:

```sh
python3 tools/embeddinggemma2/video_compare.py --backend metal \
  --binary zig/zig-out/bin/antfly-inference-bench-server \
  --model-dir /path/to/verified/checkpoint \
  --corpus tools/embeddinggemma2/testdata/retrieval/webm-corpus.json \
  --reference zig/lib/video/testdata/webm-validation/pytorch-cpu.json \
  --warmups 0 --iterations 1 --concurrency 2 --sustained-rounds 2 \
  --collect-metal-telemetry --output .benchmark-results/video/webm-metal.json
```

The five final comparison argument sets are retained in
[production-commands.json](../../zig/lib/video/testdata/vision-weight-validation/production-commands.json).
Codec generators, corpus definitions, per-picture goldens, attribution and
independent CPU/MPS references remain alongside the fixtures. Reproduce their
pinned inputs before comparing a new implementation; source/binary epochs are
part of the evidence, not interchangeable labels.

## Artifact policy and historical evidence

Track test inputs, fuzz regression seeds, independent reference oracles,
generators, provenance/attribution and this concise summary. Generate measured
backend vectors, replay outputs, run receipts and logs under ignored
`.benchmark-results/`. Do not check in each run, source snapshot or diagnostic
experiment. No release asset, LFS archive or history rewrite is required.

The complete pre-cleanup evidence remains at
[commit bf9d632d02](https://github.com/antflydb/antfly/tree/bf9d632d025a6b12837997f6aeb86e1a818ec831).
For a specific historical file, recover it without changing the checkout:

```sh
git show bf9d632d02:zig/lib/video/testdata/vision-weight-validation/staged-production-full-clip.json \
  > .benchmark-results/video/historical-full-clip.json
```

Historical fixture READMEs can describe their measured epoch and commands;
removed output files are recovered from that commit or regenerated with their
producer. Their old test counts, source fingerprints and failed outcomes are
not promoted to current evidence by this cleanup.

Fixture layout maintenance keeps independent reference values intact while
sharing exact JSON copies and expected pixel payloads. See the
[maintenance commands](../../zig/lib/video/testdata/README.md#fixture-maintenance).
Live benchmark suites resolve relative input paths and check their hashes.
Reference/table row formatting changes source-file hashes; historical production
measurements and their recorded fingerprints remain bound to the earlier epoch.
