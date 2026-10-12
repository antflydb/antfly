# HLS master discovery qualification

Source/binary/artifact-bound evidence for bounded master parsing, explicit video
selection and authenticated native fMP4 ingest/reload. This phase does not
complete the full non-CUDA goal. No codec or model execution code changes here.

The portable gate covers constraints, default/named video, duplicate URI and
metadata tie ordering, malformed inputs, pool denial, cancellation and allocation
failures across discovery and two epochs. The production HTTP gate verifies
headers on all reads, selected-playlist relative resources and configuration
changes with an old packet retained. Both gates execute on macOS and actual
Linux ARM64. The pinned public Apple sample additionally qualifies real master
metadata only; its media references are not fetched or redistributed.

Reproduce from `zig/lib/media` with `zig build test-media test-media-http` and
`zig build test-media-hls-fuzz --fuzz=100000` using a fresh local cache. Four seeds
pass through Smith's length-prefixed serialization and reach positive selection.
The fuzz harness caps bytes, variants/renditions, planning storage and control
steps, and asserts zero retained pool usage. The final report starts at zero
runs; other exploratory campaigns are not added to its counts.

`zig build install-media-tests install-media-http-tests
-Dtarget=aarch64-linux-musl -Doptimize=ReleaseSafe -p <output>` installs portable
and production HTTP qualification binaries for another host. Linux execution
used a pinned offline image, read-only root, writable temporary working directory,
no external networking and only the two test binaries mounted read-only.

Logs trim terminal empty lines; the fuzz divider is prefixed to avoid git's
conflict-marker check. Hashes bind this normalized representation. Encrypted,
MPEG-TS, low-latency and separate-track ingestion, broader stream qualification,
scheduling and performance remain within the active goal.
