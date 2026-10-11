# Resident Metal final normalization qualification

The model adapter now pools, truncates to the requested dimension and normalizes
on device, then reads only the returned vector for validation/serialization.
The same primitive serves multimodal groups, resident text and raster cohorts.
Flat text/media requests propagate requested dimensions before readback. The
v4 sampling/rendering recipe remains unchanged; earlier receipts retain their
original source and binary fingerprints.

Actual Metal units cover long 8192-row masked pooling, NaN padding exclusion,
retained row views after parent destruction, dimension truncation, finite scales
1/1e-35/1e35, invalid norms and planned command scopes. Two selected units and
all sixteen video model units pass. Thirty-one Python contract tests pass.

The sampling corpus's ten pinned vectors agree with the updated CPU/Metal
server. Maximum coordinate errors are 4.62e-7 / 3.85e-7; both rankings and all
top-k sets match exactly. Each backend completes eight load requests at
concurrency two and a disconnect/retry. Twenty Metal reduced-dimension cases
(128/256/512/768 across two queries, VFR, PAFF and a long window) agree with the
independent truncated PyTorch vectors; maximum error is 4.87e-7. Restoring the
default dimension on the resident model also passes. The original six pretrained
mixed/video cases pass on both backends, including HTTP 128 dimensions, forced
admission retry and damaged-selected-frame cleanup/retry.

`qualification.json` binds source, artifact and reference hashes. Reports retain
complete vectors, retrieval metrics, dimension agreement and binary identities.
`server-build.log` records the successful isolated-cache production build. The
missing-object failure in the earlier shared cache was superseded by that build.
No isolated throughput or latency speedup is claimed from these shared-host runs.

To reproduce the dimension campaign with the pinned qualification environment
and checkpoint, from the worktree root:

```sh
python tools/embeddinggemma2/video_resident_qualification.py --backend metal \
  --corpus tools/embeddinggemma2/testdata/sampling/corpus.json \
  --model-dir .benchmark-assets/native-video/model \
  --binary zig/pkg/inference/zig-out/bin/antfly-inference-bench-server \
  --reference tools/embeddinggemma2/testdata/sampling/pytorch.json \
  --output /tmp/resident-dimensions.json
```

The sampling and original-video campaigns use `video_compare.py` and
`video_http_oracle.py` with this same binary. Focused compile-time filters are
`-Dtest-filter='embeddinggemma2 masked device mean'` and
`-Dtest-filter='embeddinggemma2 normalized device mean'`; runtime filters alone
do not limit compilation. The successful build uses both
`ZIG_GLOBAL_CACHE_DIR=/private/tmp/native-video-resident-norm-global` and
`--cache-dir /private/tmp/native-video-resident-norm-local` to avoid shared-cache
interference.

Explicit resident-text diagnostic disablement retains its host route. Linux
pretrained reports remain pinned to their preceding CPU binary; this phase's
shared dimension changes have macOS native CPU regression evidence. Broader
codec/display inputs, whole-pipeline scheduling, cross-window frame reuse,
retained-reference copying, isolated performance and streaming inheritance
remain active requirements.
