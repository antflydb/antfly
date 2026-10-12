# Native TS AVC/HLS qualification

Independent FFmpeg/FFprobe fixture bytes, clocks and all 80 decoded NV12 hashes
are checked by native Zig. The HLS HTTP test checks authenticated playlist and
media fetch through the production client into native decoding. Retry, epoch,
held-reader and allocation/admission cleanup tests accompany bounded fuzzing.
The final fuzz report records 101,456 additional runs; its cumulative counts
are 102,382 to 203,838, not a fresh 203,838-run campaign.

`qualification.json` binds source, generated artifacts and installed Linux
binaries. Logs retain commands' test/build results. The media HTTP log is from
the broader seven-test gate; TS HTTP has a separate one-test gate. Synthetic
fixtures do not establish general transport, codec or model conformance.

Reproduce from `zig/lib/media` with `zig build test-media test-media-http`
and `zig build test-media-transport-fuzz --fuzz=100000`; from `zig/lib/video`,
use `zig build test-video test-transport-http`. Installation steps
`install-media-tests`, `install-media-http-tests`, `install-video-tests` and
`install-transport-http-tests` support execution on another host. Set target
`aarch64-linux-musl`, `ReleaseSafe`, and video LLVM as recorded in build logs.
The pinned Linux image ran offline with only test binaries mounted.

No-map HLS uses the video-supplied transport factory. Normalized backing is
configuration plus packet bytes, not serialized MP4. Complete per-segment
PAT/PMT, AVC PES-aligned access units and complete NALs are current restrictions.
The original non-CUDA objective remains active.
