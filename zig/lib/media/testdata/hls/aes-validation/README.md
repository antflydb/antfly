# Native AES-128 HLS qualification

This source/binary/artifact-bound phase qualifies identity-key AES-128 CBC/PKCS7
resources before native fMP4 ingestion. It does not complete the full non-CUDA
goal or qualify SAMPLE-AES/CBCS/DRM, MPEG-TS or low-latency HLS. Codec/model code
and the v6 embedding recipe are unchanged.

The fixture generator uses independent OpenSSL encryption with public synthetic
keys. Eleven vectors cover empty/single-byte, block/padding/wide-decryption and
4097-byte boundaries. Resource fixtures encrypt existing initialization/media
bytes. Exact plaintext, map declaration lifetime, sequence IVs, key refresh,
reconfiguration, allocation cleanup, cancellation and held packets have positive
and failure gates. Production HTTP covers authentication and two independently
encrypted byte ranges with key rotation. Sensitive owners retain their original
allocation and wipe it before free.

Reproduce fixtures with `python3 zig/lib/media/scripts/generate_hls_aes_fixtures.py`.
From `zig/lib/media`, run `zig build test-media test-media-http`. Fresh separate
reports use `zig build test-media-hls-aes-declaration-fuzz --fuzz=100000` and
`zig build test-media-hls-aes-cipher-fuzz --fuzz=100000`, each with a new local
cache. Smith seed acceptance reaches valid declarations and independent
plaintext before mutation; input size is bounded to 16 KiB. The earlier combined
campaign is not summed into the final counts.

Linux test installation uses `install-media-tests install-media-http-tests`,
`aarch64-linux-musl`, ReleaseSafe, and execution in the pinned offline image with
read-only root, writable temporary working directory, no external network and
only two test binaries mounted. Logs trim trailing empty lines and prefix the
fuzz-report divider for git's conflict-marker check. Metadata records these
normalizations.

OpenSSL is a qualification tool only. Real-world encrypted-stream expansion,
other encryption/transport formats, scheduling and performance remain active.
