# Shared node owner production qualification

Frozen source f9e1e0c37d799fb96868b72a0c5461a7fbfa103f passes all 53 production
build steps with pinned Nix/glibc Zig for GNU AArch64. Seven separately compiled
archives link into the installed executable. The explicit compiler scheduling
budget is 26 GiB on the unchanged 23.43 GiB Docker VM. This proves the explicit
compile gate, not the default build budget. linux-production.json retains the
complete command, compiler provenance, nine critical source hashes and artifact.

The exact artifact passes help and the existing authenticated small-client
harness with a two-slot node scheduler and checkpoint budget 16. Standalone
holds 32 HTTP and 32 pgwire connections, including 16 held cursors and two
extended portals, recovers after release, and exits zero after SIGINT. The
container enforces two CPUs, 2 GiB without extra swap, and 512 PIDs; the child
harness applies two-CPU affinity and 512 FDs. All thresholds pass: control latency
0.0521 s, idle RSS growth 42,639,360 bytes, thread growth 64, FD growth 70,
recovery FD growth zero. Full redacted server and qualification logs are retained.

The runtime runs run_local.py --binary /antfly --manifest /out/manifest.json
--output /out/result.json --policy fixed in the pinned image, with the binary
mounted read-only, source at /source, and fresh writable output at /out.
The runtime JSON records the exact standalone command, configuration, measured
limits and binary/manifest fingerprints.

This proves opt-in owner startup/shutdown across production archives and the
configured small-client behavior. It does not qualify later execution-port or
inference lease bytes, actual inference/background admission, CPU teams,
sustained overload, stalled accepted completion, or automatic defaults.
Defaults remain disabled.
