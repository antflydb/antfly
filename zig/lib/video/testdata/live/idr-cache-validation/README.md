# IDR-bound reader projection cache qualification

Antfly's maintained Zig path now preserves a selected picture's projection key
when an earlier independent GOP is evicted or retained payloads are repacked.
The hash starts at a completed single-packet IDR with static parameter sets,
includes the track/display configuration, source clocks and coded dependency
bytes through picture completion, and uses relative picture selectors. Physical
byte offsets are excluded. ReaderSelection's full reader/policy binding is
unchanged. Dynamic parameter sets, unknown completion metadata and crossing
assemblies retain conservative prefix dependencies.

ReleaseSafe macOS model gates pass 49 checks. Actual Linux ARM64 model execution
passes 44 checks and skips five Metal checks. New regressions exercise suffix
rebasing, different allocations/offsets, mutation before/after the reset,
in-band configuration fallback and incomplete/crossing metadata. Existing field
completion, display mutation, allocation failure, admission and CPU/Metal cached
producer tests also pass.

Fresh pretrained CPU, Metal and Linux run the real six-second Sintel clip with
six rolling and two sparse intervals. Every output exactly equals its independently
computed complete-clip baseline, including the 124-token clipped tail. Rolling
telemetry records five hits and six stores; the previous source-bound advance
receipt recorded zero hits and eleven stores. Sparse intervals share no selected
pictures and correctly record zero hits. The pinned PyTorch oracle covers three
original two-second intervals; score/ranking/top-k gates apply explicitly to
rolling intervals 0, 2 and 4. No fresh PyTorch oracle is claimed for the others.

Reproduce the completed report gate with the pinned qualification environment:

```sh
python tools/embeddinggemma2/video_rolling_qualification.py \
  --report zig/lib/video/testdata/live/idr-cache-validation/native.json \
  --reference zig/lib/video/testdata/live/pretrained-validation/pytorch.json \
  --output /tmp/rolling-agreement.json
```

Repeat with `metal.json` or `linux.json`. The verifier rejects the prior zero-hit
report and enforces source intervals, epoch/ordinal identities, token counts,
exact live/baseline vectors and expected cache isolation. The retained suites
record how to invoke `antfly-embeddinggemma2-live` with the verified checkpoint;
Linux paths refer to the recorded offline container mounts.

Source, fixture, compiler, checkpoint, executed binary and artifact hashes are
recorded in `qualification.json`. This phase changes inference cache planning,
not codec reconstruction; unchanged codec gates remain in the preceding advance
receipt. Diagnostic timings on a shared host do not establish sustained
throughput, RSS or general stream conformance. The full non-CUDA goal remains
active, including incremental append validation and broader model qualification.
