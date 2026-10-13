# Streaming AVC assembly barriers

Generated fixtures qualify completed-prefix indexing and live delivery while an
unfinished future PAFF picture remains buffered. Adaptive 8-bit 4:2:0 and
interleaved/fractured 10-bit 4:2:2 field prefixes preserve a pending barrier until
the mate arrives. Packet-by-packet published units cannot advance automatic or
explicit watermarks past that barrier. EOF rejects the incomplete assembly
without marking the coordinator finished; delivery and commit retain ownership.
Source-time model sampling excludes unfinished future fields and rejects an
interval crossing their timestamp.

Complete logical indexes also match the ordinary walk for dynamic geometry,
data partitions and separate colour planes. Corrupt NALs and admission failures
remain errors. Missing data partitions are deliberately not converted into a
pending tail: publishing complete partition groups remains required.

Final macOS ReleaseSafe checks pass 213 video tests (two skips), 13 live window
checks and 40 CPU/Metal model checks. Actual Linux ARM64 passes 15 AVC checks,
13 live checks and 36 model checks (four Metal skips). Linux execution uses the
pinned offline/read-only Ubuntu image in qualification.json with read-only
binaries and a bounded 64 MiB temporary filesystem. Raw Linux reports and the
captured macOS gate summary are included. Source, fixture, compiler and Linux
binary hashes bind the scope; older pretrained receipts describe their original
source snapshots and do not qualify this change's pretrained field streams.

Reproduce with Zig 0.17.0:

```sh
# zig/lib/video
zig build test-video test-live-windows -Doptimize=ReleaseSafe --summary all
zig build install-video-tests install-live-window-tests -Dvideo-test-filter='AVC ' -Dtarget=aarch64-linux-musl -Doptimize=ReleaseSafe --prefix /tmp/field-frontier-linux
# zig/pkg/inference
zig build test-video-model -Doptimize=ReleaseSafe --summary all
zig build install-video-model-tests -Dtarget=aarch64-linux-musl -Doptimize=ReleaseSafe --prefix /tmp/field-frontier-linux
```

These gates qualify generated-fixture scheduling/sampling and CPU/Metal policy
checks, not pretrained field/dynamic stream parity, broad codec conformance or
throughput. The full non-CUDA goal remains active. Retained GOP validation still
repeats decoding and requires measured optimization. No codec library or new
transport dependency is introduced.
