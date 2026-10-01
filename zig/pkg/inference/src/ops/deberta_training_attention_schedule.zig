// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Shared bounded attention replay schedule. Backends own buffers and dispatch;
//! ordering, subdivision and the versioned parameter ABI are common.
const device = @import("deberta_training_attention_device.zig");
const Groups = @import("resident_training_groups.zig").Grouped;
pub const Phase = enum(u32) { validate_f32, validate_control, zero, forward, rows, dq, dkdv, relative };
pub const Params = extern struct {
    batch: u32,
    sequence: u32,
    heads: u32,
    dimension: u32,
    relative_rows: u32,
    phase: Phase = .validate_f32,
    begin: u32 = 0,
    count: u32 = 0,
    batch_index: u32 = 0,
    group_count: u32 = 0,
    operand: u32 = 0,
    threads: u32,
    head_index: u32 = 0,
    group_begin: u32 = 0,
    order_begin: u32 = 0,
    order_count: u32 = 0,
    dropout_threshold: u32,
    dropout_scale: f32,
    attention_scale: f32,
    backward: u32,
    dropout_stream: u64,
};

pub fn run(attrs: device.Attrs, admitted: device.Plan, grouped: ?Groups, initial: Params, dispatcher: anytype) !void {
    var params = initial;
    // Validate the whole physical control and every floating input in bounded
    // GPU scans, before any attention kernel can use them. CPU metadata has
    // already passed the same strict control checks before allocation.
    params.phase = .validate_control;
    var begin: usize = 0;
    while (begin < admitted.input_elements[2]) {
        params.begin = @intCast(begin);
        params.count = @intCast(@min(admitted.finite_chunk_elements, admitted.input_elements[2] - begin));
        try dispatcher.dispatch(&params);
        begin += params.count;
    }
    params.phase = .validate_f32;
    for ([_]usize{ 0, 1, 3 }) |operand| {
        params.operand = @intCast(operand);
        begin = 0;
        while (begin < admitted.input_elements[operand]) {
            params.begin = @intCast(begin);
            params.count = @intCast(@min(admitted.finite_chunk_elements, admitted.input_elements[operand] - begin));
            try dispatcher.dispatch(&params);
            begin += params.count;
        }
    }
    if (admitted.backward) {
        params.phase = .zero;
        begin = 0;
        while (begin < admitted.output_elements) {
            params.begin = @intCast(begin);
            params.count = @intCast(@min(admitted.finite_chunk_elements, admitted.output_elements - begin));
            try dispatcher.dispatch(&params);
            begin += params.count;
        }
    }
    const backward_phases = [_]Phase{ .rows, .dq, .dkdv };
    const forward_phases = [_]Phase{.forward};
    const phases: []const Phase = if (admitted.backward) &backward_phases else &forward_phases;
    for (phases) |phase| {
        params.phase = phase;
        begin = 0;
        while (begin < admitted.attention_rows) {
            params.begin = @intCast(begin);
            params.count = @intCast(@min(admitted.row_wave, admitted.attention_rows - begin));
            try dispatcher.dispatch(&params);
            begin += params.count;
        }
    }
    if (grouped) |groups| {
        params.phase = .relative;
        for (0..attrs.batch) |batch| {
            params.batch_index = @intCast(batch);
            begin = 0;
            while (begin < attrs.seq_len) {
                params.begin = @intCast(begin);
                params.count = @intCast(@min(admitted.relative_query_wave, attrs.seq_len - begin));
                for (0..attrs.num_heads) |head| {
                    params.head_index = @intCast(head);
                    if (!admitted.relative_split_groups) {
                        params.group_begin = 0;
                        params.group_count = @intCast(groups.rows.len);
                        params.order_begin = 0;
                        // Both device kernels truncate this to each group length;
                        // this value proves multi-query waves include all keys.
                        params.order_count = @intCast(admitted.bucket_count);
                        try dispatcher.dispatch(&params);
                    } else {
                        for (0..groups.rows.len) |group| {
                            params.group_begin = @intCast(group);
                            params.group_count = 1;
                            const count: usize = @intCast(groups.offsets[group + 1] - groups.offsets[group]);
                            var ordinal: usize = 0;
                            while (ordinal < count) {
                                params.order_begin = @intCast(ordinal);
                                params.order_count = @intCast(@min(admitted.relative_order_wave, count - ordinal));
                                try dispatcher.dispatch(&params);
                                ordinal += params.order_count;
                            }
                        }
                    }
                }
                begin += params.count;
            }
        }
    }
}
