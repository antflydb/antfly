// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Cross-ABI qualification for rounded native candidates. The flat whole-head
//! validator and the packed serving kernels intentionally have independent
//! indexing and reduction implementations; this fixture feeds both the same
//! candidate/query population and checks the actual ABI2 descriptor route.
const std = @import("std");
const build_options = @import("build_options");
const native = @import("context_attention_match.zig");
const attention = @import("context_kernels.zig");
const views = @import("../runtime/kv/context_view.zig");

const kv_heads = 2;
const source_rows = 517;
const query_rows = 3;
const compact_logical_rows = 512;
const compact_rows = 64;
const candidate_rows = 69;
const bias_rows_per_head = compact_rows;

fn compactMultiplicity(head: usize, compact_row: usize) usize {
    return if (head == 0) 8 else if (compact_row % 2 == 0) 4 else 12;
}

fn candidateRow(head: usize, logical: usize) usize {
    if (logical == 0) return 0; // exact prefix
    if (logical <= compact_logical_rows) {
        const zero_based = logical - 1;
        if (head == 0) return 1 + zero_based / 8;
        const pair = zero_based / 16;
        return 1 + pair * 2 + @intFromBool(zero_based % 16 >= 4);
    }
    if (logical <= compact_logical_rows + 2) return 1 + compact_rows + logical - compact_logical_rows - 1; // exact recent
    return 1 + compact_rows + 2 + logical - compact_logical_rows - 3; // exact continuation
}

fn storedKey(head: usize, row: usize, column: usize) f16 {
    const raw = @as(f32, @floatFromInt((row * 13 + head * 7 + column * 3) % 23)) - 11;
    return @floatCast(raw / 64);
}

fn storedValue(head: usize, row: usize, column: usize) f16 {
    const raw = @as(f32, @floatFromInt((row * 17 + head * 11 + column * 5) % 29)) - 14;
    return @floatCast(raw / 16);
}

fn queryValue(consumer: usize, query_head: usize, column: usize) f32 {
    const raw = @as(f32, @floatFromInt((consumer * 19 + query_head * 7 + column * 5) % 31)) - 15;
    return raw / 16;
}

fn fillConsumerQueries(comptime dim: usize, values: []f32, consumer: usize, query_heads: usize) void {
    std.debug.assert(values.len == query_rows * query_heads * dim);
    for (0..query_rows) |query_row| {
        for (0..query_heads) |query_head| {
            for (0..dim) |column|
                values[(query_row * query_heads + query_head) * dim + column] = queryValue(consumer + query_row * 3, query_head, column);
        }
    }
}

fn reference(comptime dim: usize, query: []const f32, head: usize, logical_rows: usize, output: []f64) void {
    std.debug.assert(query.len == dim and output.len == dim);
    var logits: [source_rows]f64 = undefined;
    var maximum = -std.math.inf(f64);
    const scale: f32 = 1.0 / @sqrt(@as(f32, @floatFromInt(dim)));
    for (0..logical_rows) |logical| {
        const row = candidateRow(head, logical);
        var dot: f64 = 0;
        for (0..dim) |column|
            dot += @as(f64, query[column]) * @as(f64, storedKey(head, row, column));
        logits[logical] = dot * @as(f64, scale);
        maximum = @max(maximum, logits[logical]);
    }
    @memset(output, 0);
    var denominator: f64 = 0;
    for (logits[0..logical_rows], 0..) |logit, logical| {
        const weight = @exp(logit - maximum);
        denominator += weight;
        const row = candidateRow(head, logical);
        for (output, 0..) |*value, column|
            value.* += weight * @as(f64, storedValue(head, row, column));
    }
    for (output) |*value| value.* /= denominator;
}

fn expectConsumerReference(comptime dim: usize, queries: []const f32, output: []const f32, query_heads: usize, tolerance: f64) !void {
    const groups = query_heads / kv_heads;
    var expected: [dim]f64 = undefined;
    for (0..query_rows) |query_row| for (0..query_heads) |query_head| {
        const vector = query_row * query_heads + query_head;
        const head = query_head / groups;
        reference(dim, queries[vector * dim ..][0..dim], head, source_rows - query_rows + query_row + 1, &expected);
        for (expected, output[vector * dim ..][0..dim]) |want, got|
            try std.testing.expectApproxEqAbs(want, @as(f64, got), tolerance);
    };
}

fn expectOutputsClose(expected: []const f32, actual: []const f32, tolerance: f32) !void {
    try std.testing.expectEqual(expected.len, actual.len);
    for (expected, actual) |want, got| try std.testing.expectApproxEqAbs(want, got, tolerance);
}

fn fixture(comptime dim: usize) !void {
    const scale: f32 = 1.0 / @sqrt(@as(f32, @floatFromInt(dim)));

    // The flat validator operates one physical head at a time, exactly as the
    // native publication transaction does after assembling a complete head.
    var validation = try native.Kernels.initTracked(null);
    defer validation.deinit();
    var source_keys = try validation.core.buffer(source_rows * dim * @sizeOf(f16));
    defer source_keys.deinit();
    var source_values = try validation.core.buffer(source_rows * dim * @sizeOf(f16));
    defer source_values.deinit();
    var candidate_keys = try validation.core.buffer(candidate_rows * dim * @sizeOf(f16));
    defer candidate_keys.deinit();
    var candidate_values = try validation.core.buffer(candidate_rows * dim * @sizeOf(f16));
    defer candidate_values.deinit();
    var dense_bias = try validation.core.buffer(candidate_rows * @sizeOf(f32));
    defer dense_bias.deinit();
    var head_queries = try validation.core.buffer(5 * dim * @sizeOf(f32));
    defer head_queries.deinit();

    // Actual packed serving buffers are head-major ABI2 arenas. Descriptor
    // buffers are uploaded explicitly, matching PackedContextSession framing.
    var serving = try attention.Kernels.initFlash();
    defer serving.deinit();
    var packed_keys = try serving.core.buffer(kv_heads * candidate_rows * dim * @sizeOf(f16));
    defer packed_keys.deinit();
    var packed_values = try serving.core.buffer(kv_heads * candidate_rows * dim * @sizeOf(f16));
    defer packed_values.deinit();
    var packed_bias = try serving.core.buffer(kv_heads * bias_rows_per_head * @sizeOf(f32));
    defer packed_bias.deinit();
    var consumer_a_queries = try serving.core.buffer(query_rows * 4 * dim * @sizeOf(f32));
    defer consumer_a_queries.deinit();
    var consumer_b_queries = try serving.core.buffer(query_rows * 6 * dim * @sizeOf(f32));
    defer consumer_b_queries.deinit();
    var consumer_a_serial = try serving.core.buffer(query_rows * 4 * dim * @sizeOf(f32));
    defer consumer_a_serial.deinit();
    var consumer_b_serial = try serving.core.buffer(query_rows * 6 * dim * @sizeOf(f32));
    defer consumer_b_serial.deinit();
    var route_output = try serving.core.buffer(query_rows * 6 * dim * @sizeOf(f32));
    defer route_output.deinit();

    const spans = [_]views.Span{
        .{ .physical_start = 0, .row_count = 1, .logical_start = 0, .logical_count = 1 },
        .{ .physical_start = 1, .row_count = compact_rows, .logical_start = 1, .logical_count = compact_logical_rows, .kind = .compacted, .bias_start = 0 },
        .{ .physical_start = 65, .row_count = 2, .logical_start = 513, .logical_count = 2 },
        .{ .physical_start = 67, .row_count = 2, .logical_start = 515, .logical_count = 2 },
        .{ .physical_start = 0, .row_count = 1, .logical_start = 0, .logical_count = 1 },
        .{ .physical_start = 1, .row_count = compact_rows, .logical_start = 1, .logical_count = compact_logical_rows, .kind = .compacted, .bias_start = 0 },
        .{ .physical_start = 65, .row_count = 2, .logical_start = 513, .logical_count = 2 },
        .{ .physical_start = 67, .row_count = 2, .logical_start = 515, .logical_count = 2 },
    };
    const heads = [_]views.Head{
        .{ .kv_start = 0, .capacity_rows = candidate_rows, .bias_start = 0, .bias_rows = bias_rows_per_head, .span_start = 0, .span_count = 4 },
        .{ .kv_start = candidate_rows, .capacity_rows = candidate_rows, .bias_start = bias_rows_per_head, .bias_rows = bias_rows_per_head, .span_start = 4, .span_count = 4 },
    };
    const view = views.View{
        .version = views.abi_version,
        .epoch = 7,
        .logical_tokens = source_rows,
        .capacity_rows = kv_heads * candidate_rows,
        .bias_rows = kv_heads * bias_rows_per_head,
        .kv_heads = kv_heads,
        .head_dim = @intCast(dim),
        .spans = &spans,
        .heads = &heads,
    };
    try std.testing.expectEqual(@as(u32, candidate_rows), try view.validate());
    var descriptor_spans = try serving.core.buffer(spans.len * @sizeOf(views.Span));
    defer descriptor_spans.deinit();
    var descriptor_heads = try serving.core.buffer(heads.len * @sizeOf(views.Head));
    defer descriptor_heads.deinit();
    @memcpy(descriptor_spans.bytes[0 .. spans.len * @sizeOf(views.Span)], std.mem.sliceAsBytes(spans[0..]));
    @memcpy(descriptor_heads.bytes[0 .. heads.len * @sizeOf(views.Head)], std.mem.sliceAsBytes(heads[0..]));

    const packed_k = std.mem.bytesAsSlice(f16, @as([]align(2) u8, @alignCast(packed_keys.bytes[0..packed_keys.logical_bytes])));
    const packed_v = std.mem.bytesAsSlice(f16, @as([]align(2) u8, @alignCast(packed_values.bytes[0..packed_values.logical_bytes])));
    for (0..kv_heads) |head| for (0..candidate_rows) |row| for (0..dim) |column| {
        const offset = (head * candidate_rows + row) * dim + column;
        packed_k[offset] = storedKey(head, row, column);
        packed_v[offset] = storedValue(head, row, column);
    };
    for (0..kv_heads) |head| {
        for (0..compact_rows) |row|
            packed_bias.floats()[head * bias_rows_per_head + row] = @log(@as(f32, @floatFromInt(compactMultiplicity(head, row))));
    }
    fillConsumerQueries(dim, consumer_a_queries.floats(), 0, 4);
    fillConsumerQueries(dim, consumer_b_queries.floats(), 1, 6);

    const validation_request = native.HeadRequest{
        .source_rows = source_rows,
        .candidate_rows = candidate_rows,
        .head_dim = @intCast(dim),
        .validation_rows = 5,
        .validation_group_rows = 1,
        .scale = scale,
        .max_mass_error = 0.001,
        .max_output_rmse = 0.001,
        .max_consumer_output_rmse = 0.001,
        .staging_budget_bytes = 64 * 1024 * 1024,
    };
    for (0..kv_heads) |head| {
        const src_k = std.mem.bytesAsSlice(f16, @as([]align(2) u8, @alignCast(source_keys.bytes[0..source_keys.logical_bytes])));
        const src_v = std.mem.bytesAsSlice(f16, @as([]align(2) u8, @alignCast(source_values.bytes[0..source_values.logical_bytes])));
        const candidate_k = std.mem.bytesAsSlice(f16, @as([]align(2) u8, @alignCast(candidate_keys.bytes[0..candidate_keys.logical_bytes])));
        const candidate_v = std.mem.bytesAsSlice(f16, @as([]align(2) u8, @alignCast(candidate_values.bytes[0..candidate_values.logical_bytes])));
        for (0..candidate_rows) |row| for (0..dim) |column| {
            candidate_k[row * dim + column] = storedKey(head, row, column);
            candidate_v[row * dim + column] = storedValue(head, row, column);
        };
        for (0..source_rows) |logical| for (0..dim) |column| {
            const row = candidateRow(head, logical);
            src_k[logical * dim + column] = storedKey(head, row, column);
            src_v[logical * dim + column] = storedValue(head, row, column);
        };
        @memset(dense_bias.floats(), 0);
        for (0..compact_rows) |row|
            dense_bias.floats()[1 + row] = @log(@as(f32, @floatFromInt(compactMultiplicity(head, row))));
        const q = head_queries.floats();
        const a_first = head * 2;
        const b_first = head * 3;
        const a_last = (query_rows - 1) * 4;
        const b_last = (query_rows - 1) * 6;
        @memcpy(q[0 * dim ..][0..dim], consumer_a_queries.floats()[(a_last + a_first + 0) * dim ..][0..dim]);
        @memcpy(q[1 * dim ..][0..dim], consumer_a_queries.floats()[(a_last + a_first + 1) * dim ..][0..dim]);
        @memcpy(q[2 * dim ..][0..dim], consumer_b_queries.floats()[(b_last + b_first + 0) * dim ..][0..dim]);
        @memcpy(q[3 * dim ..][0..dim], consumer_b_queries.floats()[(b_last + b_first + 1) * dim ..][0..dim]);
        @memcpy(q[4 * dim ..][0..dim], consumer_b_queries.floats()[(b_last + b_first + 2) * dim ..][0..dim]);
        const metrics = try validation.validateHead(validation_request, source_keys.ref(), source_values.ref(), head_queries.ref(), candidate_keys.ref(), candidate_values.ref(), dense_bias.ref());
        try std.testing.expectEqual(@as(u32, 5), metrics.consumer_groups);
        try std.testing.expect(metrics.max_mass_error <= 2e-5);
        try std.testing.expect(metrics.output_rmse <= 2e-5);
        try std.testing.expect(metrics.max_consumer_output_rmse <= 2e-5);
    }

    const descriptors = attention.DescriptorStorage{ .spans = descriptor_spans.ref(), .heads = descriptor_heads.ref() };
    const common = attention.Request{
        .view = view,
        .current_epoch = view.epoch,
        .query_start = source_rows - query_rows,
        .query_rows = query_rows,
        .query_heads = 4,
        .scale = scale,
        .descriptors = descriptors,
    };
    try serving.attention(common, consumer_a_queries.ref(), packed_keys.ref(), packed_values.ref(), packed_bias.ref(), consumer_a_serial.ref());
    var consumer_b_request = common;
    consumer_b_request.query_heads = 6;
    try serving.attention(consumer_b_request, consumer_b_queries.ref(), packed_keys.ref(), packed_values.ref(), packed_bias.ref(), consumer_b_serial.ref());
    try expectConsumerReference(dim, consumer_a_queries.floats(), consumer_a_serial.floats(), 4, 5e-5);
    try expectConsumerReference(dim, consumer_b_queries.floats(), consumer_b_serial.floats(), 6, 5e-5);

    // A deliberately admitted three-way decode exercises the merge kernel;
    // the three-row flash dispatch crosses causal exact-row boundaries.
    inline for (.{ .{ 4, &consumer_a_queries, &consumer_a_serial }, .{ 6, &consumer_b_queries, &consumer_b_serial } }) |case| {
        const query_heads: u32 = case[0];
        const queries = case[1];
        const serial = case[2];
        const split_count: u32 = 3;
        try std.testing.expect(split_count > 1);
        var scratch = try serving.core.buffer(try attention.splitDecodeScratchBytes(query_heads, @intCast(dim), split_count));
        defer scratch.deinit();
        var request = common;
        request.query_heads = query_heads;
        request.query_start = source_rows - 1;
        request.query_rows = 1;
        request.split_decode = .{ .scratch = scratch.ref(), .split_count = split_count };
        const vector_elements = @as(usize, query_heads) * dim;
        const last_row_offset = (query_rows - 1) * vector_elements * @sizeOf(f32);
        const decode_queries = try queries.ref().slice(last_row_offset, vector_elements * @sizeOf(f32));
        const decode_output = try route_output.ref().slice(0, vector_elements * @sizeOf(f32));
        try serving.attention(request, decode_queries, packed_keys.ref(), packed_values.ref(), packed_bias.ref(), decode_output);
        try expectOutputsClose(serial.floats()[(query_rows - 1) * vector_elements ..], route_output.floats()[0..vector_elements], 8e-5);
        request.split_decode = null;
        request.flash_prefill = true;
        request.query_start = source_rows - query_rows;
        request.query_rows = query_rows;
        const flash_output = try route_output.ref().slice(0, query_rows * vector_elements * @sizeOf(f32));
        try serving.attention(request, queries.ref(), packed_keys.ref(), packed_values.ref(), packed_bias.ref(), flash_output);
        try expectOutputsClose(serial.floats(), route_output.floats()[0 .. query_rows * vector_elements], 0.001);
    }
}

test "context native packed parity validates rounded multihead candidate through actual serving ABI" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    try fixture(256);
    try fixture(512);
}
