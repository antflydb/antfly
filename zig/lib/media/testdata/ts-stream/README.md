# Native incremental TS/PES extraction qualification

`transport_stream.Stream` frames arbitrary input chunks, retains the active PES
only, and publishes borrowed complete payloads under synchronous backpressure.
Native timestamps, untimed continuation flags, offsets and final transactional
state match existing buffered extraction and independent FFprobe fixtures.
The caller copies payloads it retains and reconstructs/reconciles on retry.
State handoff occurs at validated resource EOF, not per-PES acceptance.

Four ordinary/B-frame/split-PES fixtures replay with chunk sizes 1, 187, 188,
189, 8192 and whole-body input. Gates check early callbacks, retained capacity,
no complete-output arrays, all allocation failures, cancellation, consumer
failure, CRC, truncated packets, limits, and fixed/dynamic admission denial.
A real authenticated chunked server cannot send its tail until the first native
PES callback runs. Both the complete 218-test media suite and 18 production HTTP
tests pass on macOS ARM64 and actual Linux ARM64. Source/binary/fixture/log hashes
and commands are recorded in receipt.json.

This qualifies incremental extraction. AVC normalization/duration lookahead,
acknowledged Reader replay/cursors and direct TS model delivery remain open;
existing TS inference still uses the buffered path. No new pretrained-model,
codec-conformance or end-user retrieval-quality claim is added here.
