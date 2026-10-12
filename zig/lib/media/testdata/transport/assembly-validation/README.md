# Cross-PES AVC access-unit qualification

Native decoding checks all 160 fixture pictures (80 unique originals plus their
split-PES variants). Four serialized seeds per campaign positively reach demux
and normalization. Native access-unit timestamps match the original pinned
FFprobe timing oracle; every complete compressed AU matches independent FFprobe
hashes and every NV12 picture matches independent FFmpeg decoding.

The repacketizer splits AUD/start-code prefixes, SPS/PPS and coded slices. The
60/20 pictures occupy 480/158 PES records; only PES commencing an AU carry clocks.
FFprobe's parser drops timestamp pairs for 59/19 pictures in these tiny split
fixtures. This limitation is recorded rather than treated as clock agreement.
Native checks verify preserved original anchors and reject clock reuse/orphans.
The generator and fixtures reproduce byte for byte.

Full gates cover CPU/Metal, actual Linux ARM64, authenticated split-PES HTTP,
HLS retry/epoch leases and allocation/admission cleanup. Model producer patches
match original MP4, aligned TS and split TS exactly on both hosts. This phase
does not independently rerun pretrained checkpoint inference for split-layout
files. Earlier raw-TS pretrained receipts retain their own source/binary scope.

Reproduce with media `test-media test-media-http` and
`test-media-transport-fuzz --fuzz=100000`; video `test-video test-transport-http`
and `test-transport-fuzz --fuzz=100000`; inference `test-video-model`.
Linux installation steps and offline image are recorded in qualification.json.
Fuzz report title lines are normalized to avoid Git conflict-marker lint; the
manifest hashes the normalized logs.

SEI/HRD/POC inference for multiple untimed AUs per PES, cross-segment partial-AU
buffering, TS maps, real-world presentation and original non-CUDA scope remain
active. Unsupported timing fails without inventing additional timestamps.
