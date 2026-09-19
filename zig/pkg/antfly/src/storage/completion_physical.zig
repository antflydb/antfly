// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Explicit dynamic fields in the existing physical encodings. Never scan
//! arbitrary user bytes for a matching timestamp or sequence constant.
const std = @import("std");
const compiler = @import("completion_compiler.zig");
const keys = @import("internal_keys.zig");

pub fn bindTimestampKey(selected: ?[]const []const u8, key: []const u8) bool {
    const list = selected orelse return true;
    for (list) |candidate| if (std.mem.eql(u8, candidate, key)) return true;
    return false;
}

fn generation(template: *compiler.Template, index: usize, sample: u64, offset: usize) !void {
    const value = template.operations[index].value;
    if (offset > value.len or value.len - offset < 8) return error.UnsupportedCompletionTemplate;
    const observed = std.mem.readInt(u64, value[offset..][0..8], .big);
    if (observed > sample or sample == 0) return error.UnsupportedCompletionTemplate;
    if (observed == sample) try template.bind(index, .{ .kind = .replay_sequence, .target = .value, .byte_order = .big, .offset = @intCast(offset) });
}

pub fn bindOperation(template: *compiler.Template, index: usize, sequence: u64, timestamp_keys: ?[]const []const u8) !void {
    const op = template.operations[index];
    if (op.kind != .put) return;
    if (keys.isRelationalRowKey(op.key)) {
        const rows = @import("db/algebraic/relational_row_codec.zig");
        _ = try rows.rowWriteTimestampNs(op.value);
        if (bindTimestampKey(timestamp_keys, op.key))
            try template.bind(index, .{ .kind = .commit_timestamp, .target = .value, .byte_order = .little, .offset = rows.completion_timestamp_offset });
        return;
    }
    if (std.mem.eql(u8, op.key, &keys.identity_visibility_summary_key)) {
        if (op.value.len != 40) return error.UnsupportedCompletionTemplate;
        for ([_]usize{ 16, 24, 32 }) |offset| try generation(template, index, sequence, offset);
    } else if (op.key.len == 6 and op.key[0] == keys.identity_namespace and op.key[1] == keys.identity_ordinal_state_kind) {
        if (op.value.len != 25 or op.value[16] > 1) return error.UnsupportedCompletionTemplate;
        try generation(template, index, sequence, 8);
        if (op.value[16] == 1) try generation(template, index, sequence, 17);
    } else if (op.key.len == 6 and op.key[0] == keys.identity_namespace and op.key[1] == keys.identity_visibility_chunk_kind) {
        if (op.value.len < 8 or !std.mem.eql(u8, op.value[0..4], &.{ 1, 0, 0, 0 })) return error.UnsupportedCompletionTemplate;
        const count = std.mem.readInt(u32, op.value[4..8], .big);
        if (count > (op.value.len - 8) / 20 or op.value.len != 8 + @as(usize, count) * 20) return error.UnsupportedCompletionTemplate;
        for (0..count) |i| {
            const offset = 8 + i * 20;
            if (op.value[offset + 2] > 1 or op.value[offset + 3] != 0) return error.UnsupportedCompletionTemplate;
            try generation(template, index, sequence, offset + 4);
            if (op.value[offset + 2] == 1) try generation(template, index, sequence, offset + 12);
        }
    } else if (op.key.len > 3 and op.key[0] == keys.replay_namespace and op.key[1] == 0xff and op.key[2] == keys.artifact_source_revision_kind) {
        if (op.value.len != 8 or std.mem.readInt(u64, op.value[0..8], .big) != sequence) return error.UnsupportedCompletionTemplate;
        try template.bind(index, .{ .kind = .replay_sequence, .target = .value, .byte_order = .big, .offset = 0 });
    }
}

test "workload admission physical completion binds only checked identity generations" {
    const alloc = std.testing.allocator;
    const mutations = @import("completion_mutations.zig");
    var plan = try mutations.Plan.init(alloc, .{ .max_operations = 3, .max_bytes = 256 });
    defer plan.deinit();
    var summary: [40]u8 = @splat(0);
    std.mem.writeInt(u64, summary[0..8], 7, .big); // Count equal to sample stays static.
    std.mem.writeInt(u64, summary[16..24], 7, .big);
    std.mem.writeInt(u64, summary[24..32], 5, .big); // Old generation stays static.
    std.mem.writeInt(u64, summary[32..40], 7, .big);
    try plan.put(&keys.identity_visibility_summary_key, &summary);
    const chunk_key = [_]u8{ keys.identity_namespace, keys.identity_visibility_chunk_kind, 0, 0, 0, 0 };
    var chunk: [28]u8 = @splat(0);
    chunk[0] = 1;
    std.mem.writeInt(u32, chunk[4..8], 1, .big);
    std.mem.writeInt(u64, chunk[12..20], 7, .big);
    try plan.put(&chunk_key, &chunk);
    try plan.put("arbitrary-user-value", &summary);
    var template = try compiler.Template.copy(alloc, &plan);
    defer template.deinit();
    for (0..template.operations.len) |i| try bindOperation(&template, i, 7, &.{});
    try std.testing.expectEqual(@as(usize, 2), template.operations[0].bindings.len);
    try std.testing.expectEqual(@as(u32, 16), template.operations[0].bindings[0].offset);
    try std.testing.expectEqual(@as(u32, 32), template.operations[0].bindings[1].offset);
    try std.testing.expectEqual(@as(usize, 1), template.operations[1].bindings.len);
    try std.testing.expectEqual(@as(u32, 12), template.operations[1].bindings[0].offset);
    try std.testing.expectEqual(@as(usize, 0), template.operations[2].bindings.len);
}

test "workload admission physical completion refuses malformed or future identity fields" {
    const alloc = std.testing.allocator;
    const mutations = @import("completion_mutations.zig");
    var plan = try mutations.Plan.init(alloc, .{ .max_operations = 2, .max_bytes = 256 });
    defer plan.deinit();
    var summary: [40]u8 = @splat(0);
    std.mem.writeInt(u64, summary[16..24], 8, .big);
    try plan.put(&keys.identity_visibility_summary_key, &summary);
    const chunk_key = [_]u8{ keys.identity_namespace, keys.identity_visibility_chunk_kind, 0, 0, 0, 0 };
    try plan.put(&chunk_key, &.{ 1, 0, 0, 0, 0, 0, 0, 1 });
    var template = try compiler.Template.copy(alloc, &plan);
    defer template.deinit();
    try std.testing.expectError(error.UnsupportedCompletionTemplate, bindOperation(&template, 0, 7, &.{}));
    try std.testing.expectError(error.UnsupportedCompletionTemplate, bindOperation(&template, 1, 7, &.{}));
    try std.testing.expect(!bindTimestampKey(&.{"explicit-key"}, "other-key"));
    try std.testing.expect(bindTimestampKey(&.{"explicit-key"}, "explicit-key"));
}
