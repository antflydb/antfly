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

//! Resident token-major pooling shared by task adapters and prepared stages.
//! Masks/row IDs are explicit host metadata; activations stay on their backend.
const std = @import("std");
const ops = @import("../ops/ops.zig");
pub const Kind = enum { mean, cls, max, last };

pub const Value = struct {
    value: ops.CT,
    backend: *const ops.ComputeBackend,
    owns_value: bool,

    pub fn deinit(self: Value) void {
        if (self.owns_value) self.backend.free(self.value);
    }
};

pub fn maskedMean(
    allocator: std.mem.Allocator,
    backend: *const ops.ComputeBackend,
    output: ops.CT,
    mask: []const i32,
    batch: usize,
    seq_len: usize,
    hidden: usize,
) !Value {
    const rows = std.math.mul(usize, batch, seq_len) catch return error.ShapeMismatch;
    if (batch == 0 or seq_len == 0 or hidden == 0 or mask.len != rows) return error.ShapeMismatch;

    // Reuse the admitted batched program before decomposing into primitives.
    // Empty rows keep the general zero-result policy. The existing tuned
    // program has a nonempty-row contract and a bounded batch schedule.
    if (batch <= 64) if (backend.vtable.maskedMeanBatch) |run| fast: {
        for (0..batch) |b| {
            var active = false;
            for (mask[b * seq_len ..][0..seq_len]) |flag| active = active or flag > 0;
            if (!active) break :fast;
        }
        const metadata = try allocator.alloc(i64, rows);
        defer allocator.free(metadata);
        for (mask, metadata) |flag, *value| value.* = @intFromBool(flag > 0);
        if (try run(backend.ptr, output, metadata, batch, seq_len, hidden, hidden, false)) |pooled|
            return .{ .value = pooled, .backend = backend, .owns_value = true };
    };

    const mask_values = try allocator.alloc(f32, batch * seq_len);
    defer allocator.free(mask_values);
    for (mask, 0..) |flag, i| mask_values[i] = if (flag > 0) 1.0 else 0.0;

    const denominators = try allocator.alloc(f32, batch);
    defer allocator.free(denominators);
    for (0..batch) |b| {
        var count: f32 = 0;
        for (mask[b * seq_len ..][0..seq_len]) |flag| if (flag > 0) {
            count += 1;
        };
        denominators[b] = @max(count, 1);
    }

    const mask_shape_i32 = [_]i32{ @intCast(batch), @intCast(seq_len), 1 };
    const output_shape = [_]i64{ @intCast(batch), @intCast(seq_len), @intCast(hidden) };
    const sum_shape = [_]i64{ @intCast(batch), 1, @intCast(hidden) };
    const count_shape = [_]i64{ @intCast(batch), 1, 1 };
    const pooled_shape = [_]i64{ @intCast(batch), @intCast(hidden) };

    const mask_ct = try backend.fromFloat32Shape(mask_values, &mask_shape_i32);
    defer backend.free(mask_ct);
    const zero = try backend.fromFloat32Shape(&.{0}, &.{1});
    defer backend.free(zero);
    const masked_output = try backend.primWhereSelect(mask_ct, output, zero);
    defer backend.free(masked_output);
    const summed = try backend.primReduceSum(masked_output, &.{1}, &output_shape);
    defer backend.free(summed);
    const counts = try backend.fromFloat32Shape(denominators, &.{ @intCast(batch), 1, 1 });
    defer backend.free(counts);
    const count_broadcast = try backend.primBroadcastInDim(counts, &sum_shape, &.{ 0, 1, 2 }, &count_shape);
    defer backend.free(count_broadcast);
    const mean3d = try backend.primDivide(summed, count_broadcast);
    defer backend.free(mean3d);
    const mean2d = try backend.primReshape(mean3d, &pooled_shape);

    return .{ .value = mean2d, .backend = backend, .owns_value = true };
}

pub fn cls(
    allocator: std.mem.Allocator,
    backend: *const ops.ComputeBackend,
    output: ops.CT,
    batch: usize,
    seq_len: usize,
    hidden: usize,
) !Value {
    const flat_rows = std.math.mul(usize, batch, seq_len) catch return error.ShapeMismatch;
    const flat_shape = [_]i64{
        std.math.cast(i64, flat_rows) orelse return error.ShapeMismatch,
        std.math.cast(i64, hidden) orelse return error.ShapeMismatch,
    };
    const flat = try backend.primReshape(output, &flat_shape);
    defer backend.free(flat);
    const row_ids = try allocator.alloc(u32, batch);
    defer allocator.free(row_ids);
    for (row_ids, 0..) |*row_id, b| {
        const row = std.math.mul(usize, b, seq_len) catch return error.ShapeMismatch;
        row_id.* = std.math.cast(u32, row) orelse return error.ShapeMismatch;
    }
    if (try backend.takeRows(flat, row_ids, batch, hidden)) |pooled| {
        return .{ .value = pooled, .backend = backend, .owns_value = true };
    }

    const input_shape = [_]i64{ @intCast(batch), @intCast(seq_len), @intCast(hidden) };
    const starts = [_]i64{ 0, 0, 0 };
    const limits = [_]i64{ @intCast(batch), 1, @intCast(hidden) };
    const strides = [_]i64{ 1, 1, 1 };
    const pooled_shape = [_]i64{ @intCast(batch), @intCast(hidden) };
    const sliced = try backend.primSlice(output, &starts, &limits, &strides, &input_shape);
    defer backend.free(sliced);
    const pooled = try backend.primReshape(sliced, &pooled_shape);
    return .{ .value = pooled, .backend = backend, .owns_value = true };
}

pub fn last(
    allocator: std.mem.Allocator,
    backend: *const ops.ComputeBackend,
    output: ops.CT,
    mask: []const i32,
    batch: usize,
    seq_len: usize,
    hidden: usize,
) !Value {
    const rows = std.math.mul(usize, batch, seq_len) catch return error.ShapeMismatch;
    if (batch == 0 or seq_len == 0 or hidden == 0 or mask.len != rows) return error.ShapeMismatch;

    const row_ids = try allocator.alloc(u32, batch);
    defer allocator.free(row_ids);
    for (0..batch) |b| {
        var last_idx: usize = 0;
        for (0..seq_len) |s| {
            if (mask[b * seq_len + s] > 0) last_idx = s;
        }
        row_ids[b] = @intCast(b * seq_len + last_idx);
    }

    const flat_shape = [_]i64{ @intCast(batch * seq_len), @intCast(hidden) };
    const flat = try backend.primReshape(output, &flat_shape);
    defer backend.free(flat);
    if (try backend.takeRows(flat, row_ids, batch, hidden)) |pooled| {
        return .{ .value = pooled, .backend = backend, .owns_value = true };
    }
    const indices = try allocator.alloc(i32, batch);
    defer allocator.free(indices);
    for (row_ids, indices) |row, *index| index.* = std.math.cast(i32, row) orelse return error.UnsupportedShape;
    const index_tensor = (try backend.fromInt32Shape(indices, &.{@intCast(batch)})) orelse return error.UnsupportedResidentTextPooling;
    defer backend.free(index_tensor);
    const pooled = try backend.primGather(flat, index_tensor, 0, &flat_shape);
    return .{ .value = pooled, .backend = backend, .owns_value = true };
}

pub fn poolOutput(allocator: std.mem.Allocator, backend: *const ops.ComputeBackend, output: ops.CT, mask: []const i32, batch: usize, seq_len: usize, kind: Kind) !Value {
    if (batch == 0 or seq_len == 0) return error.ShapeMismatch;
    const shape = try backend.tensorShape(output, allocator);
    defer allocator.free(shape);
    return poolKnownShape(allocator, backend, output, shape, mask, batch, seq_len, kind);
}

fn poolKnownShape(allocator: std.mem.Allocator, backend: *const ops.ComputeBackend, output: ops.CT, shape: []const i64, mask: []const i32, batch: usize, seq_len: usize, kind: Kind) !Value {
    const rows = std.math.mul(usize, batch, seq_len) catch return error.ShapeMismatch;
    if (shape.len == 2 and shape[0] == @as(i64, @intCast(batch)) and shape[1] > 0)
        return .{ .value = output, .backend = backend, .owns_value = false };
    if (shape.len == 2 and shape[0] == @as(i64, @intCast(rows)) and shape[1] > 0) {
        const dims = [_]i64{ @intCast(batch), @intCast(seq_len), shape[1] };
        const reshaped = try backend.primReshape(output, &dims);
        defer backend.free(reshaped);
        return poolSequence(allocator, backend, reshaped, mask, batch, seq_len, @intCast(shape[1]), kind);
    }
    if (shape.len != 3) return error.UnexpectedOutputShape;
    if (shape[0] != @as(i64, @intCast(batch)) or shape[1] != @as(i64, @intCast(seq_len)) or shape[2] <= 0) return error.ShapeMismatch;
    return poolSequence(allocator, backend, output, mask, batch, seq_len, @intCast(shape[2]), kind);
}

fn poolSequence(allocator: std.mem.Allocator, backend: *const ops.ComputeBackend, output: ops.CT, mask: []const i32, batch: usize, seq_len: usize, hidden: usize, kind: Kind) !Value {
    return switch (kind) {
        .mean => maskedMean(allocator, backend, output, mask, batch, seq_len, hidden),
        .cls => cls(allocator, backend, output, batch, seq_len, hidden),
        .last => last(allocator, backend, output, mask, batch, seq_len, hidden),
        .max => maskedMax(allocator, backend, output, mask, batch, seq_len, hidden),
    };
}

pub fn maskedMax(allocator: std.mem.Allocator, backend: *const ops.ComputeBackend, output: ops.CT, mask: []const i32, batch: usize, seq_len: usize, hidden: usize) !Value {
    const rows = std.math.mul(usize, batch, seq_len) catch return error.ShapeMismatch;
    if (batch == 0 or seq_len == 0 or hidden == 0 or mask.len != rows) return error.ShapeMismatch;
    const mask_values = try allocator.alloc(f32, rows);
    defer allocator.free(mask_values);
    for (mask, mask_values) |flag, *value| value.* = if (flag > 0) 1 else 0;
    const mask_tensor = try backend.fromFloat32Shape(mask_values, &.{ @intCast(batch), @intCast(seq_len), 1 });
    defer backend.free(mask_tensor);
    const empty = try backend.fromFloat32Shape(&.{-std.math.inf(f32)}, &.{1});
    defer backend.free(empty);
    const selected = try backend.primWhereSelect(mask_tensor, output, empty);
    defer backend.free(selected);
    const dims = [_]i64{ @intCast(batch), @intCast(seq_len), @intCast(hidden) };
    const pooled3d = try backend.primReduceMax(selected, &.{1}, &dims);
    defer backend.free(pooled3d);
    const pooled = try backend.primReshape(pooled3d, &.{ @intCast(batch), @intCast(hidden) });
    return .{ .value = pooled, .backend = backend, .owns_value = true };
}

fn tryFusedMeanNormalize(allocator: std.mem.Allocator, backend: *const ops.ComputeBackend, output: ops.CT, mask: ?[]const i32, batch: usize, sequence: usize, hidden: usize) !?Value {
    if (backend.vtable.regions.sequence_encoder.poolNormalize == null) return null;
    const rows = try std.math.mul(usize, batch, sequence);
    if (mask) |values| if (values.len != rows) return error.ShapeMismatch;
    const metadata = try allocator.alloc(i64, rows);
    defer allocator.free(metadata);
    if (mask) |values| {
        for (values, metadata) |flag, *value| value.* = @intFromBool(flag > 0);
    } else @memset(metadata, 1);
    const value = (try backend.maskedMeanNormalize(&.{ .hidden = output, .attention_mask = metadata, .batch = batch, .seq_len = sequence, .hidden_size = hidden, .normalize = true })) orelse return null;
    return .{ .value = value, .backend = backend, .owns_value = true };
}

fn isTokenShape(shape: []const i64, batch: usize, sequence: usize) bool {
    const b = std.math.cast(i64, batch) orelse return false;
    const seq = std.math.cast(i64, sequence) orelse return false;
    const rows = std.math.mul(i64, b, seq) catch return false;
    return (shape.len == 2 and shape[0] == rows and shape[1] > 0) or
        (shape.len == 3 and shape[0] == b and shape[1] == seq and shape[2] > 0);
}

/// Pool and normalize in one device batch, then materialize only the requested
/// final rows. Encoder outputs and the actual backend owner remain borrowed.
pub fn finishHostRows(allocator: std.mem.Allocator, backend: *const ops.ComputeBackend, output: ops.CT, mask: []const i32, batch: usize, sequence: usize, kind: Kind, normalize: bool) ![][]f32 {
    if (batch == 0 or sequence == 0) return error.ShapeMismatch;
    const source_shape = try backend.tensorShape(output, allocator);
    defer allocator.free(source_shape);
    const has_pool = !(source_shape.len == 2 and source_shape[0] == (std.math.cast(i64, batch) orelse return error.ShapeMismatch));
    // Prepared heads may already return normalized final rows. An empty tail
    // must not create a command batch or disturb the prepared owner's state.
    var submission = try @import("submission.zig").Scope.beginIfNeeded(backend, has_pool or normalize);
    defer submission.deinit();
    if (submission.foreign) return error.ForeignCommandSubmission;
    var normalized_in_pool = false;
    const pooled = pooled: {
        if (kind == .mean and normalize and has_pool and isTokenShape(source_shape, batch, sequence)) {
            if (try tryFusedMeanNormalize(allocator, backend, output, mask, batch, sequence, @intCast(source_shape[source_shape.len - 1]))) |value| {
                normalized_in_pool = true;
                break :pooled value;
            }
        }
        break :pooled try poolKnownShape(allocator, backend, output, source_shape, mask, batch, sequence, kind);
    };
    defer pooled.deinit();
    const shape = try backend.tensorShape(pooled.value, allocator);
    defer allocator.free(shape);
    if (shape.len != 2 or shape[0] != (std.math.cast(i64, batch) orelse return error.ShapeMismatch) or shape[1] <= 0) return error.UnexpectedOutputShape;
    const width: usize = @intCast(shape[1]);
    const value = if (normalize and !normalized_in_pool) normalized: {
        // A single-row mean is the identity. Reuse the same fused row program
        // to normalize CLS/last/max results without a unit-weight tensor.
        if (try tryFusedMeanNormalize(allocator, backend, pooled.value, null, batch, 1, width)) |result|
            break :normalized result.value;
        break :normalized try @import("../graph/resident_ops.zig").l2NormalizeLastDim(allocator, backend, pooled.value, shape);
    } else pooled.value;
    defer if (normalize and !normalized_in_pool) backend.free(value);
    try submission.finish();
    const host = try backend.toFloat32(value, allocator);
    defer allocator.free(host);
    if (host.len != try std.math.mul(usize, batch, width)) return error.ShapeMismatch;
    const rows = try allocator.alloc([]f32, batch);
    var initialized: usize = 0;
    errdefer {
        for (rows[0..initialized]) |row| allocator.free(row);
        allocator.free(rows);
    }
    for (rows, 0..) |*row, index| {
        row.* = try allocator.dupe(f32, host[index * width ..][0..width]);
        initialized += 1;
    }
    return rows;
}

fn checkPoolingBackend(kind: @import("../backends/backends.zig").BackendType) !void {
    const a = std.testing.allocator;
    var context = try @import("../backends/compute_context.zig").BackendContext.init(a, kind, std.testing.io);
    defer context.deinit(a);
    const backend = context.computeBackend();
    const nan = std.math.nan(f32);
    const values = [_]f32{ -2, -2, nan, nan, -4, 6, nan, nan, 3, -3, nan, nan, nan, nan, nan, nan, nan, nan };
    const mask = [_]i32{ 1, 0, 1, 0, 1, 0, 0, 0, 0 };
    const input = try backend.ensureDeviceResidentOwned(try backend.fromFloat32Shape(&values, &.{ 3, 3, 2 }));
    defer backend.free(input);
    const audit_mod = @import("transfer_audit.zig");
    for ([_]Kind{ .mean, .cls, .max, .last }) |mode| {
        var audit = audit_mod.Audit{ .forbid_device_host_access = true };
        const pooled = blk: {
            try audit_mod.begin(&audit);
            defer audit_mod.end();
            break :blk try poolOutput(a, &backend, input, &mask, 3, 3, mode);
        };
        defer pooled.deinit();
        try std.testing.expectEqual(@as(u64, 0), audit.download_calls);
        try std.testing.expectEqual(@as(u64, 0), audit.host_accesses);
        const expected: [6]f32 = switch (mode) {
            .mean => .{ -3, 2, 3, -3, 0, 0 },
            .cls => .{ -2, -2, nan, nan, nan, nan },
            .max => .{ -2, 6, 3, -3, -std.math.inf(f32), -std.math.inf(f32) },
            .last => .{ -4, 6, 3, -3, nan, nan },
        };
        const result = try backend.toFloat32(pooled.value, a);
        defer a.free(result);
        try std.testing.expectEqual(@as(usize, 6), result.len);
        for (expected, result) |wanted, actual| {
            if (std.math.isNan(wanted)) {
                try std.testing.expect(std.math.isNan(actual));
            } else try std.testing.expectEqual(wanted, actual);
        }
        var final_audit = audit_mod.Audit{};
        const final_rows = blk: {
            try audit_mod.begin(&final_audit);
            defer audit_mod.end();
            break :blk try finishHostRows(a, &backend, input, &mask, 3, 3, mode, false);
        };
        defer {
            for (final_rows) |row| a.free(row);
            a.free(final_rows);
        }
        for (final_rows, 0..) |row, b| for (row, 0..) |actual, h| {
            const wanted = expected[b * 2 + h];
            if (std.math.isNan(wanted)) try std.testing.expect(std.math.isNan(actual)) else try std.testing.expectEqual(wanted, actual);
        };
        if (kind != .native) {
            try std.testing.expectEqual(@as(u64, 1), final_audit.download_calls);
            try std.testing.expectEqual(@as(u64, 6 * @sizeOf(f32)), final_audit.download_bytes);
        }
    }
    const finite_input = try backend.ensureDeviceResidentOwned(try backend.fromFloat32Shape(&.{ 3, 0, 0, 4, nan, nan, 0, 0, 0, 0, 0, 0 }, &.{ 2, 3, 2 }));
    defer backend.free(finite_input);
    const finite_mask = [_]i32{ 1, 1, 0, 1, 1, 1 };
    var primitive_table = backend.vtable.*;
    primitive_table.regions.sequence_encoder.poolNormalize = null;
    const primitive_backend = ops.ComputeBackend{ .ptr = backend.ptr, .vtable = &primitive_table };
    for ([_]*const ops.ComputeBackend{ &backend, &primitive_backend }) |selected| {
        for ([_]Kind{ .mean, .cls, .max, .last }) |mode| {
            const fused_before = selected.debugTimingSnapshot().provider.metal_runtime_nomic_bert_pool_normalize_successes;
            const final = try finishHostRows(a, selected, finite_input, &finite_mask, 2, 3, mode, true);
            if (kind == .metal and selected.vtable.regions.sequence_encoder.poolNormalize != null)
                try std.testing.expect(selected.debugTimingSnapshot().provider.metal_runtime_nomic_bert_pool_normalize_successes > fused_before);
            defer {
                for (final) |row| a.free(row);
                a.free(final);
            }
            const wanted: [2]f32 = switch (mode) {
                .mean, .max => .{ 0.6, 0.8 },
                .cls => .{ 1, 0 },
                .last => .{ 0, 1 },
            };
            for (wanted, final[0]) |expected, actual| try std.testing.expectApproxEqAbs(expected, actual, 2e-6);
            try std.testing.expectEqualSlices(f32, &.{ 0, 0 }, final[1]);
        }
    }

    // A final host request must not flush or cancel an unrelated caller's
    // active command batch. Its owner remains responsible for retirement.
    var foreign_submission = try @import("submission.zig").Scope.begin(&backend);
    if (foreign_submission.owns) {
        try std.testing.expectError(error.ForeignCommandSubmission, finishHostRows(a, &backend, input, &mask, 3, 3, .mean, false));
        try std.testing.expect(backend.vtable.regions.commands.hasActiveFrame.?(backend.ptr));
    }
    foreign_submission.deinit();

    // Cold constant preparation and normalization participate in the same
    // batch; only the final BxH matrix may cross the device boundary.
    var normalized_final_audit = audit_mod.Audit{};
    const normalized_rows = blk: {
        try audit_mod.begin(&normalized_final_audit);
        defer audit_mod.end();
        break :blk try finishHostRows(a, &backend, input, &mask, 3, 3, .mean, true);
    };
    defer {
        for (normalized_rows) |row| a.free(row);
        a.free(normalized_rows);
    }
    try std.testing.expectApproxEqAbs(-3 / @sqrt(@as(f32, 13)), normalized_rows[0][0], 2e-6);
    try std.testing.expectApproxEqAbs(2 / @sqrt(@as(f32, 13)), normalized_rows[0][1], 2e-6);
    try std.testing.expectApproxEqAbs(3 / @sqrt(@as(f32, 18)), normalized_rows[1][0], 2e-6);
    try std.testing.expectApproxEqAbs(-3 / @sqrt(@as(f32, 18)), normalized_rows[1][1], 2e-6);
    try std.testing.expectEqualSlices(f32, &.{ 0, 0 }, normalized_rows[2]);
    if (kind != .native) {
        try std.testing.expectEqual(@as(u64, 1), normalized_final_audit.download_calls);
        try std.testing.expectEqual(@as(u64, 6 * @sizeOf(f32)), normalized_final_audit.download_bytes);
    }

    // The final normalized matrix remains resident, including a zero row.
    var normalization_audit = audit_mod.Audit{ .forbid_device_host_access = true };
    const normalized = blk: {
        try audit_mod.begin(&normalization_audit);
        defer audit_mod.end();
        const mean = try maskedMean(a, &backend, input, &mask, 3, 3, 2);
        defer mean.deinit();
        break :blk try @import("../graph/resident_ops.zig").l2NormalizeLastDim(a, &backend, mean.value, &.{ 3, 2 });
    };
    defer backend.free(normalized);
    try std.testing.expectEqual(@as(u64, 0), normalization_audit.download_calls);
    try std.testing.expectEqual(@as(u64, 0), normalization_audit.host_accesses);
    const normalized_host = try backend.toFloat32(normalized, a);
    defer a.free(normalized_host);
    const inverse13 = 1 / @sqrt(@as(f32, 13));
    const inverse18 = 1 / @sqrt(@as(f32, 18));
    for ([_]f32{ -3 * inverse13, 2 * inverse13, 3 * inverse18, -3 * inverse18, 0, 0 }, normalized_host) |wanted, actual|
        try std.testing.expectApproxEqAbs(wanted, actual, 2e-6);

    // Nonempty rows admit the existing batched program where available.
    // Compare its real dispatch to forced primitive decomposition, including
    // masked NaNs and negative mask entries (which are excluded).
    const nonempty_input = try backend.ensureDeviceResidentOwned(try backend.fromFloat32Shape(values[0..12], &.{ 2, 3, 2 }));
    defer backend.free(nonempty_input);
    const nonempty_mask = [_]i32{ 1, -1, 1, 0, 1, 0 };
    var general_table = backend.vtable.*;
    general_table.maskedMeanBatch = null;
    const general = ops.ComputeBackend{ .ptr = backend.ptr, .vtable = &general_table };
    for ([_]*const ops.ComputeBackend{ &backend, &general }) |selected| {
        var mean_audit = audit_mod.Audit{ .forbid_device_host_access = true };
        const mean = blk: {
            try audit_mod.begin(&mean_audit);
            defer audit_mod.end();
            break :blk try maskedMean(a, selected, nonempty_input, &nonempty_mask, 2, 3, 2);
        };
        defer mean.deinit();
        try std.testing.expectEqual(@as(u64, 0), mean_audit.download_calls);
        try std.testing.expectEqual(@as(u64, 0), mean_audit.host_accesses);
        const host = try selected.toFloat32(mean.value, a);
        defer a.free(host);
        try std.testing.expectEqualSlices(f32, &.{ -3, 2, 3, -3 }, host);
    }
    // Exercise the exact integer gather fallback without a hidden readback.
    var table = backend.vtable.*;
    table.takeRows = null;
    const fallback = ops.ComputeBackend{ .ptr = backend.ptr, .vtable = &table };
    var audit = audit_mod.Audit{ .forbid_device_host_access = true };
    const pooled = blk: {
        try audit_mod.begin(&audit);
        defer audit_mod.end();
        break :blk try last(a, &fallback, input, &mask, 3, 3, 2);
    };
    defer pooled.deinit();
    try std.testing.expectEqual(@as(u64, 0), audit.download_calls);
    try std.testing.expectEqual(@as(u64, 0), audit.host_accesses);
    const result = try fallback.toFloat32(pooled.value, a);
    defer a.free(result);
    try std.testing.expectEqualSlices(f32, &.{ -4, 6, 3, -3 }, result[0..4]);
    try checkGenericWidths(a, &backend);
}

fn checkGenericWidths(a: std.mem.Allocator, backend: *const ops.ComputeBackend) !void {
    const mask = [_]i32{ 1, 0, 1, 0, 1, 1 };
    for ([_]usize{ 3, 257, 1025 }) |width| {
        const source = try a.alloc(f32, 6 * width);
        defer a.free(source);
        for (source, 0..) |*v, index| v.* = @floatFromInt(@as(i32, @intCast((index * 13) % 31)) - 15);
        const input = try backend.ensureDeviceResidentOwned(try backend.fromFloat32Shape(source, &.{ 2, 3, @intCast(width) }));
        defer backend.free(input);
        for ([_]Kind{ .mean, .cls, .max, .last }) |mode| {
            const actual = try finishHostRows(a, backend, input, &mask, 2, 3, mode, true);
            defer {
                for (actual) |row| a.free(row);
                a.free(actual);
            }
            const expected = try a.alloc(f32, width);
            defer a.free(expected);
            for (0..2) |b| {
                @memset(expected, if (mode == .max) -std.math.inf(f32) else 0);
                if (mode == .cls or mode == .last) {
                    const selected = b * 3 + @as(usize, if (mode == .cls) 0 else 2);
                    @memcpy(expected, source[selected * width ..][0..width]);
                } else {
                    for (0..3) |token| {
                        if (mask[b * 3 + token] <= 0) continue;
                        for (expected, source[(b * 3 + token) * width ..][0..width]) |*dst, value|
                            if (mode == .max) {
                                dst.* = @max(dst.*, value);
                            } else {
                                dst.* += value;
                            };
                    }
                    if (mode == .mean) for (expected) |*value| {
                        value.* /= 2;
                    };
                }
                var square: f32 = 0;
                for (expected) |value| square += value * value;
                const norm = @sqrt(square);
                if (norm > 0) for (expected) |*value| {
                    value.* /= norm;
                };
                for (expected, actual[b]) |wanted, value| try std.testing.expectApproxEqAbs(wanted, value, 2e-6);
            }
        }
    }
}

test "resident sequence pooling CPU handles masks empty rows and exact gather without activation reads" {
    if (comptime !@import("build_options").enable_native) return error.SkipZigTest;
    try checkPoolingBackend(.native);
}

test "resident sequence pooling Metal handles masks empty rows and exact gather without activation reads" {
    if (comptime !@import("build_options").enable_metal) return error.SkipZigTest;
    try checkPoolingBackend(.metal);
}

test "resident sequence pooling CUDA handles masks empty rows and exact gather without activation reads" {
    if (comptime !@import("build_options").enable_cuda) return error.SkipZigTest;
    try checkPoolingBackend(.cuda);
}
