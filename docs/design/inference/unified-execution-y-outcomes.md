# Y: shared Metal parameter layouts

A typed Zig owner supplies 74 scalar parameter layouts to C host records,
runtime Metal declarations and the standalone paged-attention renderer.
The generated header is checked by the existing source generator (70 files
and 129 native owned regions). Both the owner and generated include are part
of the appropriate Metal qualification/cache input closure.

Compared with X (`1140056e86`), the six implementation paths remove 5,537
maintained production bytes and 163 lines, including the complete replacement
owner and build/codegen integration. Generated production text adds 13,748
bytes, so physical production text grows by 8,211 bytes; inline tests add
another 12 bytes separately. Unrelated main-merge changes are excluded.
This is source ownership consolidation, not an instruction or binary reduction.

Both complete Metal programs remain byte-identical to X (922,886 and 17,072
bytes). Preprocessed host operations and declarations match except literal
source-line arguments used only by optional command-encoder diagnostic tracing.
The standalone paged layout uses the same typed owner, and the external-helper
drift check requires both its native include and binding outside generated
regions. Source freshness validates the generated header as a whole.

Receipts are under `.benchmark-assets/1166/y-campaign/`: the whole-program proof
is `program-identity.json`, scoped accounting is `source-accounting-typed.json`,
and local checks are recorded in `local-checks-typed-schema-v4.json` and its
individual logs. Earlier failed schema attempts are retained separately.
CUDA source and images are unchanged by Y; device qualification remains queued
behind the other Glowworm workload.

Final local qualification passes: 43 selected tests with no skips, 54 real
Metal compiler checks, all 154 runtime cases, three enabled GLiNER2 parity
cases (seven CUDA-disabled skips), both-backend product build, generated-source
freshness and CUDA source policy. No new model latency claim is made for Y.
