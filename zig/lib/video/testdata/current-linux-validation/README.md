# Current native video execution on Linux ARM64

The complete library suite executes inside the pinned offline Linux ARM64 VM:
252 passes, 29 platform skips, zero failures (281 selected). The statically linked
`aarch64-linux-musl` binary uses Zig 0.17.0 and ReleaseSafe. The image has networking
disabled, read-only mounts and a bounded 128 MiB writable temporary filesystem.

The gate covers H.264 progressive reference sharing, transactional detach and
allocation-failure ownership; separate-plane, interlaced, dynamic SPS and split
partition reconstruction; VP8/VP9/AV1 independent picture goldens and stateful
ownership; native depth/chroma, AV1 Annex B/scalable metadata; and normalized TS
streaming, source clocks, acknowledgement/retry and cancellation. macOS-only
Metal/VideoToolbox checks are platform skips. The full raw log retains every
selected outcome; this is execution evidence rather than cross-compilation.

`receipt.json` pins the executable, build/run logs and complete linked library
source/test fingerprints. This closes actual-Linux execution for the current
H.264 reference-storage tests. Earlier real-stream differentials and measured
copy/latency reports keep their original binaries and source scopes. This run
contains deterministic fuzz seeds; sustained coverage-guided campaigns are
recorded separately in `../sequence-fuzz-validation/`.
