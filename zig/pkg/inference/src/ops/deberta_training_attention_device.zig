// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Static admission for the dedicated safe-math Metal training-attention V1
//! kernels. This describes logical device bytes and conservative scalar work,
//! including every replay. Driver/compiler memory remains separately admitted
//! by the backend owner. There are no S*S device or threadgroup allocations.
const std = @import("std");
const groups = @import("resident_training_groups.zig");

pub const Attrs = @import("ml").graph.DebertaTrainingAttentionAttrs;
const mib = 1024 * 1024;
const gib = 1024 * mib;

pub const key_tile: u32 = 64;
pub const max_head_dim: u32 = 256;
pub const max_wave_work_items: u64 = 128 * 1024 * 1024;
pub const finite_chunk_elements: u32 = 1024 * 1024;
pub const threadgroup_bytes: usize = (3 * key_tile + 4) * 4;

pub const Limits = struct {
    max_tensor_bytes: usize = gib,
    max_scratch_bytes: usize = 128 * mib,
    max_host_metadata_bytes: usize = 128 * mib,
    max_work_items: u64 = 1 << 40,
};

pub const Plan = struct {
    limits: Limits,
    backward: bool,
    input_elements: [4]usize,
    output_elements: usize,
    output_bytes: usize,
    batch_tokens: usize,
    hidden: usize,
    attention_rows: usize,
    bucket_count: usize,
    row_scratch_bytes: usize,
    group_device_bytes: usize,
    group_upload_bytes: usize,
    /// Includes row scalars, integer descriptors, the largest sequential
    /// private-buffer upload, and the four-byte finite-status allocation.
    scratch_bytes: usize,
    host_metadata_bytes: usize,
    work_items: u64,
    threads: u32,
    key_tile: u32,
    row_wave: u32,
    relative_query_wave: u32,
    relative_order_wave: u32,
    relative_split_groups: bool,
    finite_chunk_elements: u32,
    /// At most one four-byte status observation after each owned, synchronous
    /// command buffer. This never includes activation or gradient readback.
    scalar_readback_bytes: usize,
    grouping_limits: groups.Limits,
};

fn mul(a: usize, b: usize) !usize {
    return std.math.mul(usize, a, b) catch error.ResourceLimitExceeded;
}
fn add(a: usize, b: usize) !usize {
    return std.math.add(usize, a, b) catch error.ResourceLimitExceeded;
}
fn workMul(a: u64, b: u64) !u64 {
    return std.math.mul(u64, a, b) catch error.ResourceLimitExceeded;
}
fn workAdd(a: u64, b: u64) !u64 {
    return std.math.add(u64, a, b) catch error.ResourceLimitExceeded;
}
fn divCeil(a: usize, b: usize) usize {
    return a / b + @intFromBool(a % b != 0);
}

pub fn plan(attrs: Attrs, backward: bool, requested: Limits) !Plan {
    const layout = try attrs.layout();
    if (attrs.head_dim > max_head_dim) return error.UnsupportedDebertaTrainingAttentionProfile;
    if (requested.max_tensor_bytes == 0 or requested.max_scratch_bytes == 0 or
        requested.max_host_metadata_bytes == 0 or requested.max_work_items == 0)
        return error.InvalidDebertaTrainingAttentionLimit;
    const limits = Limits{
        .max_tensor_bytes = @min(requested.max_tensor_bytes, gib),
        .max_scratch_bytes = @min(requested.max_scratch_bytes, 128 * mib),
        .max_host_metadata_bytes = @min(requested.max_host_metadata_bytes, 128 * mib),
        .max_work_items = @min(requested.max_work_items, 1 << 46),
    };
    const tokens = std.math.cast(usize, layout.batch_tokens) orelse return error.ResourceLimitExceeded;
    const hidden = std.math.cast(usize, layout.hidden) orelse return error.ResourceLimitExceeded;
    const out = try mul(tokens, hidden);
    const qkv = try mul(out, 3);
    const relative = try mul(try mul(attrs.relative_rows, 2), hidden);
    const control = std.math.cast(usize, layout.control_elements) orelse return error.ResourceLimitExceeded;
    const output = if (backward) try add(qkv, relative) else out;
    for ([_]usize{ qkv, relative, control, output }) |count| {
        if (count > std.math.maxInt(i32) or try mul(count, 4) > limits.max_tensor_bytes)
            return error.ResourceLimitExceeded;
    }
    const rows = try mul(tokens, attrs.num_heads);
    if (rows > std.math.maxInt(u32)) return error.ResourceLimitExceeded;
    const buckets = try mul(attrs.seq_len, 2) - 1;
    if (buckets > std.math.maxInt(i32)) return error.ResourceLimitExceeded;
    const grouping_limits = groups.Limits{
        .max_indices = @min(buckets, 4 * 1024 * 1024),
        .max_metadata_bytes = limits.max_host_metadata_bytes,
        .max_sort_work = @intCast(@min(limits.max_work_items, 256 * 1024 * 1024)),
    };
    const grouping = if (backward) try groups.plan(buckets, attrs.relative_rows, grouping_limits) else groups.Admission{ .persistent_bytes = 0, .peak_bytes = 0, .sort_work = 0 };
    const group_upload = if (backward) try mul(try add(buckets, 1), 4) else 0;
    const row_bytes = if (backward) try mul(rows, 3 * 4) else 0;
    const scratch = try add(try add(try add(row_bytes, grouping.persistent_bytes), group_upload), 4);
    // A fixed bound covers the small buffer-ref wrappers and dispatch state;
    // the only size-dependent host allocations are the integer grouping.
    const metadata = try add(grouping.peak_bytes, 4096);
    if (scratch > limits.max_scratch_bytes or metadata > limits.max_host_metadata_bytes)
        return error.ResourceLimitExceeded;

    var threads: u32 = key_tile;
    while (threads < attrs.head_dim) threads *= 2;
    const pair_work = try workAdd(try workMul(16, attrs.head_dim), 64);
    const per_row_work = try workAdd(try workMul(attrs.seq_len, pair_work), threads * 4);
    if (per_row_work > max_wave_work_items) return error.ResourceLimitExceeded;
    const row_wave: u32 = @intCast(@min(128, max_wave_work_items / per_row_work));
    // Inverse-bucket traversal visits each relative offset once per query,
    // across all groups. Admit that traversal even for out-of-range diagonals.
    // Heads have disjoint output ranges, so they can be separate dispatches
    // without changing any reduction order or restricting the context size.
    const relative_pair_work = try workAdd(try workMul(13, attrs.head_dim), 64);
    const per_relative_query = try workAdd(
        try workMul(buckets, try workAdd(try workMul(13, attrs.head_dim), 64)),
        try workMul(@min(attrs.relative_rows, buckets), threads * 2 + 8),
    );
    // An unusually large single query falls back to one bucket group and a
    // bounded ordinal segment. It still writes the same private output-owned
    // accumulator in batch/query/key order. This is a dispatch subdivision,
    // never an attention algorithm or host-compute fallback.
    const split_groups = backward and per_relative_query > max_wave_work_items;
    const relative_wave: u32 = if (backward and !split_groups) @intCast(@min(32, max_wave_work_items / per_relative_query)) else 1;
    const order_wave: u32 = @intCast(@min(buckets, (max_wave_work_items - (threads * 2 + 8)) / relative_pair_work));
    const pairs = try workMul(rows, attrs.seq_len);
    const replay_work = try workMul(pairs, try workAdd(try workMul(if (backward) 53 else 10, attrs.head_dim), if (backward) 256 else 64));
    const finite_elements = try add(try add(qkv, relative), if (backward) out else 0);
    var work = try workAdd(replay_work, try workAdd(finite_elements, control));
    work = try workAdd(work, try workMul(output, 2));
    work = try workAdd(work, try workMul(rows, threads * 4));
    if (backward) {
        work = try workAdd(work, grouping.sort_work);
        work = try workAdd(work, try workMul(try workMul(tokens, attrs.num_heads), try workMul(attrs.relative_rows, 2 * @as(u64, attrs.head_dim) + 8)));
        if (split_groups) work = try workAdd(work, try workMul(try workMul(tokens, attrs.num_heads), try workMul(divCeil(buckets, order_wave), 2 * @as(u64, attrs.head_dim) + 8)));
        work = try workAdd(work, try workMul(pairs, 8));
    }
    if (work > limits.max_work_items) return error.ResourceLimitExceeded;
    var commands = try add(divCeil(qkv, finite_chunk_elements), divCeil(relative, finite_chunk_elements));
    commands = try add(commands, divCeil(control, finite_chunk_elements));
    commands = try add(commands, try mul(divCeil(rows, row_wave), if (backward) 3 else 1));
    if (backward) {
        commands = try add(commands, divCeil(out, finite_chunk_elements));
        commands = try add(commands, divCeil(output, finite_chunk_elements));
        const relative_commands = if (split_groups)
            try mul(try mul(tokens, attrs.num_heads), try add(@min(attrs.relative_rows, buckets), divCeil(buckets, order_wave)))
        else
            try mul(try mul(attrs.batch, attrs.num_heads), divCeil(attrs.seq_len, relative_wave));
        commands = try add(commands, relative_commands);
    }
    return .{
        .limits = limits,
        .backward = backward,
        .input_elements = .{ qkv, relative, control, if (backward) out else 0 },
        .output_elements = output,
        .output_bytes = try mul(output, 4),
        .batch_tokens = tokens,
        .hidden = hidden,
        .attention_rows = rows,
        .bucket_count = buckets,
        .row_scratch_bytes = row_bytes,
        .group_device_bytes = grouping.persistent_bytes,
        .group_upload_bytes = group_upload,
        .scratch_bytes = scratch,
        .host_metadata_bytes = metadata,
        .work_items = work,
        .threads = threads,
        .key_tile = key_tile,
        .row_wave = row_wave,
        .relative_query_wave = relative_wave,
        .relative_order_wave = order_wave,
        .relative_split_groups = split_groups,
        .finite_chunk_elements = finite_chunk_elements,
        .scalar_readback_bytes = try mul(commands, 4),
        .grouping_limits = grouping_limits,
    };
}

fn fixture(sequence: u32) Attrs {
    return .{ .batch = 2, .seq_len = sequence, .num_heads = 2, .head_dim = 4, .relative_rows = 512, .dropout_probability = 0.125, .dropout_stream_id = 3 };
}

test "deberta training Metal plan counts all replay work and bounded linear scratch" {
    const a = try plan(fixture(128), true, .{});
    const b = try plan(fixture(256), true, .{});
    const forward = try plan(fixture(256), false, .{});
    try std.testing.expectEqual(@as(usize, 3 * 2 * 2 * 256 * 4), b.row_scratch_bytes);
    try std.testing.expect(b.scratch_bytes < 2 * a.scratch_bytes + 64);
    try std.testing.expect(b.work_items > 3 * a.work_items);
    try std.testing.expect(b.work_items > 4 * forward.work_items);
    try std.testing.expectEqual(@as(usize, 4), forward.scratch_bytes);
    try std.testing.expectEqual(@as(usize, 0), forward.group_device_bytes);
    try std.testing.expect(b.row_wave > 0 and b.relative_query_wave > 0);
    try std.testing.expectEqual(@as(u32, 64), b.threads);
    try std.testing.expect(b.scalar_readback_bytes > 0);
}

test "deberta training Metal plan rejects unsupported dimensions work bytes and scratch before dispatch" {
    var attrs = fixture(17);
    attrs.head_dim = 257;
    try std.testing.expectError(error.UnsupportedDebertaTrainingAttentionProfile, plan(attrs, false, .{}));
    attrs = fixture(17);
    try std.testing.expectError(error.ResourceLimitExceeded, plan(attrs, true, .{ .max_work_items = 1 }));
    try std.testing.expectError(error.ResourceLimitExceeded, plan(attrs, true, .{ .max_tensor_bytes = 1 }));
    try std.testing.expectError(error.ResourceLimitExceeded, plan(attrs, true, .{ .max_scratch_bytes = 1 }));
    try std.testing.expectError(error.ResourceLimitExceeded, plan(attrs, true, .{ .max_host_metadata_bytes = 4095 }));
    try std.testing.expectError(error.InvalidDebertaTrainingAttentionLimit, plan(attrs, true, .{ .max_work_items = 0 }));
    attrs.seq_len = std.math.maxInt(i32);
    try std.testing.expectError(error.ResourceLimitExceeded, plan(attrs, true, .{}));
}

test "deberta training Metal plan admits published long contexts with explicit training work ceiling" {
    for ([_]u32{ 4096, 16384 }) |sequence| {
        const attrs = Attrs{ .batch = 1, .seq_len = sequence, .num_heads = 12, .head_dim = 64, .relative_rows = 512, .dropout_probability = 0.1, .dropout_stream_id = 3 };
        const admitted = try plan(attrs, true, .{ .max_work_items = 1 << 46 });
        try std.testing.expect(admitted.output_bytes <= gib);
        try std.testing.expect(admitted.scratch_bytes < 128 * mib);
        try std.testing.expect(admitted.relative_query_wave > 0);
        try std.testing.expect(!admitted.relative_split_groups);
        if (sequence == 16384) try std.testing.expectError(error.ResourceLimitExceeded, plan(attrs, true, .{}));
    }
    // A single head/query can also be divided without raising the fixed
    // synchronous command work bound or imposing a sequence-length cap.
    const large = Attrs{ .batch = 1, .seq_len = 30000, .num_heads = 1, .head_dim = 256, .relative_rows = 512, .dropout_probability = 0, .dropout_stream_id = 3 };
    const split = try plan(large, true, .{ .max_work_items = 1 << 46 });
    try std.testing.expect(split.relative_split_groups);
    try std.testing.expectEqual(@as(u32, 1), split.relative_query_wave);
    try std.testing.expect(split.relative_order_wave < split.bucket_count);
}
