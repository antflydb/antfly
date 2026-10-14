# Incremental TS AVC normalization qualification

`transport_avc.Stream` consumes borrowed PES records, joins split Annex B data,
maps AUDs to PES clock anchors and uses next-AU DTS duration lookahead. A timed
PES can begin in the preceding AU before its later AUD. Untimed continuation
bytes do not invent clocks. Ambiguous multiple-AU timestamps and missing final
duration reject; legacy PES-aligned streams without AUD retain their contract.
Normalized output owns source bytes, AVCC, metadata, identities and packet index.

The focused gate checks four ordinary/B-frame/split-PES fixtures with one-byte,
188-byte, 8192-byte and whole-body input. Each normalized packet's encoded bytes,
DTS/PTS/duration/sync and AVCC match the buffered adapter. Owned snapshots remain
valid after both producers are destroyed. Additional gates cover cross-PES timing,
legacy no-AUD input, consumer failure and every allocation failure. The failure
harness forces allocation movement so counts are reproducible across heaps.

The focused suite passes 16/16 on macOS ARM64 and actual Linux ARM64.
The separately requested historical broader device regression completed with
272 passing tests and two skips. It began before the final allocation-harness
and no-AUD test changes; it does not qualify the later HTTP integration. Runtime uses
native Zig. The underlying TS payload/decoded-picture fixtures already carry
independent FFprobe/FFmpeg provenance. This phase qualifies AU normalization;
acknowledged HTTP/Reader replay and direct TS model delivery remain open. It adds
no pretrained parity or end-user retrieval-quality claim.
