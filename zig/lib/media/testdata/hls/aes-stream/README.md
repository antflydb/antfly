# Incremental AES HLS fixture

OpenSSL independently encrypts the six-fragment generated AVC fixture from
`testdata/fragmented.mp4`, excluding its 797-byte initialization. The public
fixture key is bytes 00 through 0f, with sequence-derived IV 10, CBC and PKCS7.
`manifest.json` records command, tool, source/plaintext/ciphertext hashes and sizes.
The source fixture's existing FFprobe provenance applies to the decrypted packets.
Reference tooling is offline only; runtime decryption uses Zig's native AES.
