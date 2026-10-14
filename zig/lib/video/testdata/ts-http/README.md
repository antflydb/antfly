# Native TS HTTP and model streaming qualification

The Reader's codec hook composes incremental native TS/PES extraction and AVC
AU normalization with the shared authenticated HTTP byte transport. Each output
owns its packet snapshot. Retries restart from committed demux/configuration
state, hash and suppress acknowledged AUs, then resume new delivery. Source
configuration and the manifest cursor commit at validated EOF. Consumer failure,
cancellation and allocation failure destroy transient session owners. Taking a
snapshot and acknowledging it are separate operations.

The focused suite checks streamed/buffered packet parity, before-EOF delivery,
prefix mutation rejection, retained snapshots, cancellation and retry, global
ordinals, discontinuity epochs, mixed buffered/streamed lifetime byte limits and
every allocation failure. Allocation movement is forced for deterministic
failure counts. The media/HTTP regression suites also exercise fMP4, AES-CBC,
LL-HLS reconciliation and growing DASH resources through the shared transport.

The configured checkpoint gate serves the six-second `live-stream.ts` fixture
over a real authenticated HTTP connection. Response tails are withheld until
model output occurs. The first output fails; its retained vector must be retried
identically before another HTTP request. Replay suppresses the acknowledged AU
prefix. Six output windows then match the buffered TS route in timestamps,
token counts and vectors. The TS fixture is a packet-copy remux of the existing
fMP4 fixture; `live-stream.json` records its FFmpeg command, hashes and PES starts.

The focused TS suite passes 22/22 on macOS and actual Linux ARM64. The media
and authenticated HTTP suites pass 218/218 and 18/18 on both hosts. Model unit
checks pass 42 tests with five optional checkpoint skips. Configured CPU, Metal
and Linux runs each preserve six vectors exactly, verifying 12 accepted AUs on
retry before new delivery.

`receipt.json` records terminal platform results, source/binary/log hashes and
the pinned checkpoint identity. Configured comparisons use the same backend on
both sides; they qualify streaming reference preservation, rather than a new
cross-backend or PyTorch comparison. The fixture adds no end-user relevance
claim. Runtime parsing, decryption, decoding and inference require no external
codec or crypto library. FFmpeg is used only to generate the checked-in fixture.

TS LL-HLS parts remain unsupported. This gate does not claim arbitrary TS program
or codec support, unbounded retries, durable acknowledgements or automatic
recovery. The fixed session admission envelope is conservative and also covers
the embedded producers' workspace reservations.
