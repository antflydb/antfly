# Owned dynamic live checkpoint qualification

Recorded run results and output filenames below describe the measured historical
epoch. Generated reports/logs were removed from the current tree; see
[qualification and historical recovery](../../../../../../docs/plans/native-video-noncuda-audit.md#artifact-policy-and-historical-evidence).
Fixture inputs and independent reference oracles remain checked in.

The default coordinator now uses the maintained Zig persistent decoder for avc3
streams. Independently admitted parameter checkpoints survive decoder failure.
Probe/prepared/canonical suffix readers own bootstrap NALs in their first payload;
selectors and timestamps remain unchanged. All checkpoint allocation/content
probes precede source eviction and cursor mutation. Failed appends preserve old
checkpoints and incoming ownership, then reconstruct retained preroll on retry.
The full-replay oracle retains the same parameter checkpoints.

Generated dynamic geometry/depth/chroma tests compare actual native pixels from
prepared storage with complete-source decoding before and after eviction. Old
prepared windows remain independently decodable after later commits. A malformed
next unit discards native state while retaining checkpoint bytes/cursors, and a
restored unit retries successfully. Allocation campaigns exhaust construction,
checkpoint replacement and independently decoded pixel allocations.

ReleaseSafe macOS passes 240 video (two skips), 23 live and 49 CPU/Metal model
checks. Actual Linux ARM64 passes 215 video (27 skips), 23 live and 44 model
checks (five Metal skips).

The retained real Sintel fixture copies encoded pictures through offline
h264_mp4toannexb and fragmented avc3 muxing, preserving inline SPS/PPS at IDRs.
Native CPU, Metal and Linux execute six rolling and two sparse intervals in
incremental and replay modes. All vectors/token counts exactly equal both
complete-source and replay baselines. Each policy reconstructs 60 rather than
150 packets. Rolling reconstruction bytes fall 81,888 to 33,683; sparse bytes
fall 86,518 to 33,683. Counts exclude prefix validation and transport reads.

Fresh pinned PyTorch CPU and MPS cover all eight remuxed intervals independently.
Every backend preserves exact rankings and full top-k overlap (1, 3, 5, 8) for
both queries. Maximum coordinate error is below 2.08e-6. Dependencies used by the
reference generator remain offline qualification tools, not native runtime code.

Dynamic cache keys remain conservatively bound to available prefixes. Rolling
reports correctly contain zero hits/eleven stores, and sparse reports zero
hits/four stores. Pair qualification uses the explicit prefix policy:

```sh
python zig/pkg/inference/scripts/embeddinggemma2/video_incremental_qualification.py \
  --incremental zig/lib/video/testdata/live/dynamic-coordinator-validation/native.json \
  --replay zig/lib/video/testdata/live/dynamic-coordinator-validation/native-replay.json \
  --reference zig/lib/video/testdata/live/dynamic-coordinator-validation/pytorch-mps.json \
  --cache-policy prefix --output /tmp/dynamic-agreement.json
```

Repeat for Metal/Linux and the CPU oracle. The default static-IDR policy remains
unchanged; its older source-bound report still passes. The negative gate rejects
the dynamic report as static reuse. Timings are shared-host diagnostics, without
sustained throughput, RSS or dynamic cache benefit claims.

Source, fixture, compiler, checkpoint, executed binary and artifact hashes are in
qualification.json. Configuration-entry compaction, broader mixed/dynamic
pretrained inputs, persistent-owner fuzz and the original non-CUDA scope remain
active. No runtime dependency is added.
