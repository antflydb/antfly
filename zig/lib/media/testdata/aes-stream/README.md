# Incremental encrypted HLS qualification

Native Zig CBC decrypts into the automatic fMP4 framer before response EOF.
Key/IV bind prefix replay, and fixed cipher workspace participates in shared
admission. The decoder holds the final plaintext block until PKCS7 validation;
transport, padding, cancellation and consumer failure do not commit the resource.
Earlier accepted fragments remain independent and can be verified on retry.

The gate checks eleven independent OpenSSL vectors at all chunk sizes 1–65,
full native packet/timestamp parity, actual key/IV mutation before transport,
key refresh and rotation, ciphertext truncation and invalid padding, consumer
failure and cancellation, workspace denial before entry and every allocation
failure. A real authenticated chunked server truncates the first encrypted
response after one delivery, then holds its retry tail until the first new
callback. Another real Reader test checks encrypted initialization, exact ranged
independent CBC objects and key rotation while preserving held packet leases.

Portable qualification passes 204 media and 16 production HTTP checks on macOS
ARM64 and actual Linux ARM64. Model unit checks pass 42 with three optional
checkpoint skips. The separate actual checkpoint gate compares six normalized
streamed vectors, token counts and source clocks with buffered plaintext HLS
inference. Its first output deliberately fails; the exact retained vector retries
before more HTTP, while the server withholds the final tail until model output.
CPU, Metal and actual Linux ARM64 each complete all six windows with seven
callbacks, three before the withheld tail, five HTTP requests and zero maximum
coordinate difference. Source, binary, checkpoint and raw-log hashes are recorded
in receipt.json.

MacOS model qualification uses Debug; the Linux model binary uses ReleaseSafe.
Linux runs in the pinned native ARM64 VM, with read-only model/binary mounts,
no external network, writable temporary space and authenticated loopback HTTP.
The six-second generated AVC source and OpenSSL fixture provenance do not prove
broader live-origin interoperability or codec conformance. TS/LL-HLS incremental
reconciliation remains open. Runtime needs no external crypto/codec library.
