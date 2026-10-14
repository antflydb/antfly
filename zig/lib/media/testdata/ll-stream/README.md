# Incremental LL-HLS qualification

Streaming parts retain accepted-sample commitments and native clocks across
resource failure. Complete parents verify those samples before delivering new
suffixes, including prefix boundaries within repackaged moofs. Each parent
freezes its expected prefix across retry. Unverified observed fragments never
commit to the resource ledger; corrupt truncation cannot poison healthy retry.
Fully duplicate parents consume EOF without duplicate callbacks. Advertised
active resources remain pinned; compatible missing parts can transition to a
complete parent while retaining accepted progress.

The suite checks packet-byte/clock parity for re-fragmented parents, prefix
mutation before new callbacks, frozen retry state, fully duplicate parents,
unfinished-part fallback, additional advertised parts on reload, mixed buffered
parts/streaming parents, independent AES objects and every allocation failure.
Owned-index trimming charges moving overlap and retains exact suffix charges.
FFprobe provenance in ../hls/ll-parent-2s.json records all 60 unchanged native
packet hashes/descriptors and the half-second first-part subset.

Qualification passes 214 media and 17 production HTTP tests on macOS ARM64 and
actual Linux ARM64, plus 42 model checks with four optional checkpoint skips.
The authenticated chunked server holds the parent tail until a verified new
suffix is delivered. Five accepted part samples and fifteen new samples share
its first repackaged parent moof; the whole pipeline emits all 60 exactly once.

The configured checkpoint gate compares six source-time windows against buffered
full-clip HLS inference. It injects first-output failure after parent suffix
acceptance, retries the exact retained vector before more HTTP, and withholds
its retry tail until a new model output runs. CPU, Metal and actual Linux ARM64 each complete six windows, seven callbacks,
three before the withheld tail and six requests, with zero maximum coordinate
difference. Source, binary, fixture, checkpoint and raw-log hashes are in receipt.json. macOS model binaries use Debug; Linux
uses ReleaseSafe, with read-only artifacts and no external network. This is
bounded generated-fixture qualification. Incremental normalized TS remains open.
