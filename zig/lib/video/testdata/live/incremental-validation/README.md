# Persistent static-AVC live reconstruction

Recorded run results and output filenames below describe the measured historical
epoch. Generated reports/logs were removed from the current tree; see
[qualification and historical recovery](../../../../../../docs/plans/native-video-noncuda-audit.md#artifact-policy-and-historical-evidence).
Fixture inputs and independent reference oracles remain checked in.

The live coordinator defaults to persistent static-configuration AVC metadata
reconstruction. Workspaces, reference pictures, unfinished fields and plane lanes
remain in an admitted owner instead of replaying the retained GOP on every append.
The reconstruction loop is shared with ordinary decoding. Independently owned
logical snapshots survive advancement/eviction. SHA-256 detects prefix/configuration
mutation before reuse; byte placement is normalized. Fallible eviction probes run
before mutation. Post-entry failures discard the decoder owner and retry from
retained preroll while preserving committed cursor, snapshots and incoming unit
ownership. In-band parameter-set streams still use full replay.

Replay metadata now reports other pictures completed in the current packet before
returning an unfinished-selection error. Differential prefix tests exposed the
previous omission for interleaved PAFF assemblies. Tests compare metadata through
unfinished fields, exercise prefix mutation and denied eviction, and exhaustively
fail owner allocations. Shared metadata admission charges actual allocation/growth.

ReleaseSafe passes 234 video + 20 live + 49 CPU/Metal model checks on macOS.
Actual Linux ARM64 passes 209 video + 20 live + 44 model checks, with 27 video and
five Metal skips. The modified ordinary reconstruction path passes 101,170
coverage-guided H.264 fuzz runs. This is not persistent-owner fuzz qualification.

Fresh pretrained native CPU, Metal and actual Linux compare incremental and full
replay on the same real six-second Sintel clip, six rolling intervals and two
sparse intervals. All vectors and token counts exactly match complete-clip and
replay inference. The clipped tail has 124 tokens; other intervals have 246.
Each policy reconstructs 60 rather than 150 packets. Rolling reconstruction bytes
fall 81,132 to 33,431; sparse bytes fall 85,762 to 33,431. Coordinator operations
and cache behavior remain equal (five rolling hits/six stores; zero sparse hits).
These counters exclude prefix hashing and transport reads, and shared-host timings
do not establish sustained throughput or RSS improvements.

Fresh pinned PyTorch CPU and MPS now cover all eight intervals independently,
including the odd rolling windows and the clipped tail. Every backend preserves
exact rankings and full top-k overlap (1, 3, 5, 8) for both queries. Largest vector
coordinate error stays below 2.08e-6. FFmpeg independently selects/decodes source
pictures; checkpoint and reference implementation hashes are verified. Retrieval
measures reference preservation, without end-user relevance labels.

Reproduce the offline oracle and completed pair gate with the pinned environment:

```sh
python zig/pkg/inference/scripts/embeddinggemma2/video_rolling_reference.py \
  --model-dir /path/to/verified/checkpoint --backend mps --output /tmp/oracle.json
python zig/pkg/inference/scripts/embeddinggemma2/video_incremental_qualification.py \
  --incremental zig/lib/video/testdata/live/incremental-validation/native.json \
  --replay zig/lib/video/testdata/live/incremental-validation/native-replay.json \
  --reference /tmp/oracle.json --output /tmp/pair-agreement.json
```

Repeat oracle generation with CPU and pair checks with Metal/Linux reports.
Suites retain exact fragments/window policy; replay suites set
`incremental_validation` to false. Negative checks reject missing/incomplete oracles,
changed sampling policies and an equal-work report even with unchanged embeddings.
The legacy three-window oracle retains its explicitly narrower verification scope.

Initial final builds failed because generated cache objects/libraries were missing
at link time. Retained failure logs identify those errors. Task-specific isolated
caches completed every required gate; no source error is attributed to those logs.

Source, fixture, compiler, executed binary, checkpoint and artifact hashes are in
`qualification.json`. No native runtime dependency is added. Dynamic incremental
configuration/geometry, large sustained pipelines, persistent-owner fuzzing and
broader original non-CUDA requirements remain active.
