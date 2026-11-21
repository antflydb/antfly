// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
// tile-codegen:begin attention.scalar_phases family (do not edit; run: zig build tile-kernel-codegen -- --write)
#ifndef ANTFLY_CAUSAL_KEY_WINDOW_PHASES
#define ANTFLY_CAUSAL_KEY_WINDOW_PHASES
#define ANTFLY_CAUSAL_KEY_END(TempType, Query, Count, Offset, End) \
    TempType visible = Query - Offset + 1u; \
    End = visible < Count ? visible : Count;
#define ANTFLY_CAUSAL_WINDOW_START(TempType, Query, Window, Offset, Begin, End) \
    TempType window_start_abs = (Query + 1u > Window) ? (Query + 1u - Window) : 0u; \
    if (window_start_abs > Offset) { \
        Begin = window_start_abs - Offset; \
        if (Begin > End) Begin = End; \
    }
#define ANTFLY_CAUSAL_KEY_WINDOW(TempType, Query, Count, Offset, Window, Begin, End) \
    ANTFLY_CAUSAL_KEY_END(TempType, Query, Count, Offset, End) \
    if (Window != 0u) { \
        ANTFLY_CAUSAL_WINDOW_START(TempType, Query, Window, Offset, Begin, End) \
    }
#endif
#ifndef ANTFLY_DECODE_REPLAY_PHASES
#define ANTFLY_DECODE_REPLAY_PHASES
#define ANTFLY_DECODE_REPLAY_KV(Scalars) \
    kv_position_offset = Scalars[4]; \
    query_position_offset = Scalars[1]; \
    kv_seq_len = Scalars[2];
#define ANTFLY_DECODE_REPLAY_TOTAL(Scalars) \
    ANTFLY_DECODE_REPLAY_KV(Scalars) \
    total_sequence_len = Scalars[3];
#endif
// tile-codegen:end attention.scalar_phases family
