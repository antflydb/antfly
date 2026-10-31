# AX: Retire unreachable Metal host bridges

Baseline: `d46a7dc62f`. Implementation: `18f6e141f9`.

Nine superseded C/Zig bridge pairs are removed: **85,824 maintained production
bytes / 1,436 lines**, comprising 79,863 bytes of Metal host bodies and 5,961
bytes of Zig extern declarations. Together with AW, this continuation removes
**97,250 production bytes / 1,646 lines**. Shader arithmetic is unchanged.

This retires duplicate execution entry points rather than adding another owner.
The surviving prepared embeddings, slot-backed FFNs and device-KV decoder
families retain their exact implementations. CPU/CUDA source, generated source,
Metal shader strings and device images are unchanged. No new dispatch,
allocation, kernel, runtime switch or fallback is introduced by AX.

All names below have the prefix `termite_metal_decode_runtime_`:

| Retired bridge | C body bytes |
| --- | ---: |
| `apply_linear_pair_activation_multiply_linear_residual` | 15,514 |
| `apply_gated_ffn_residual_i2_s` | 14,004 |
| `apply_gated_ffn_residual_q8_0` | 13,845 |
| `apply_attention_gated_block_q_slot_device` | 9,797 |
| `apply_rms_norm_i2_s_linear_residual` | 7,596 |
| `apply_attention_gated_block_device` | 7,313 |
| `apply_attention_dense_block_device` | 4,413 |
| `attention_span_device` | 3,698 |
| `embedding_lookup_direct_device` | 3,683 |

## Reachability and preservation proof

Before removal, a tracked repository-wide whole-word symbol search finds exactly
two references per name: its C definition and its Zig extern declaration.
There are no C callers, Zig callers, tests, native headers or symbol lookup
strings naming these entry points. Related live names, including the
`_device_kv_device` variants, are matched as distinct whole-word symbols.
The Metal build compiles the C file directly into the inference module; these
are internal bridges, not published native header contracts.

The retirement receipt retains all original bodies/declarations, their SHA-256
hashes and the original reference locations. Applying only those eighteen
recorded deletions reproduces both candidate files byte for byte. Every
surviving C implementation and Zig declaration is therefore unchanged, including
embedded shader source, shared helper bodies and error handling. No source-size
claim depends on moving code into generated files or tests.

The first receipt check incorrectly used substring absence, which also matched
retained names with longer suffixes. Whole-word matching corrects that proof;
the original eighteen deletions themselves are unchanged.

## Validation and limits

The selected real-device suite reports **52 passed, zero skipped**, including
module reachability entries. It exercises prepared embedding lookup, mapped
BF16 rows, dense/rms-linear tensors, masked attention, single/multi-row Q4_0
and Q8_0 FFNs, planned Q4_K frames, paged gated-block last rows, and the shared
ternary attention/FFN bridges with heap and constant-backed inputs.

The permanent serialized Metal suite also passes:

- 32 ternary FFN profiles / 204 explicit failure checks;
- 256 K-block FFN profiles / 176 failure checks;
- 128 ternary attention profiles / 240 failure checks.

Combined CPU/Metal/CUDA compilation, formatting, and generated freshness
(70 files + 129 native regions) pass. No new timing assay is needed to establish
repository reachability: AX deletes only uncalled bodies and declarations and
changes no live implementation. This is a source retirement result, not a
measured throughput or binary-size gain. AW's separate FFN timing results and
AV's still-open GPU tail miss retain their original scopes.

Evidence is retained in `.benchmark-assets/1166/ax-campaign/`: baseline source,
all retired fragments, retirement and source-accounting receipts, selected test
commands and output, backend build/freshness logs and cache-cleanup records.
