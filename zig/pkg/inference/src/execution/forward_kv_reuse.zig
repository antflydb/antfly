// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Immutable donor operands for one full-sequence forward. Paged runtimes own
//! their existing state; this scope adds no cross-request cache or new arithmetic.
const std = @import("std");
const ops = @import("../ops/ops.zig");
const contracts = @import("../graph/backend_contracts.zig");
const Cleanup = @import("cleanup_context.zig").Context;

pub const Scope = struct {
    allocator: std.mem.Allocator,
    backend: *const ops.ComputeBackend,
    cleanup: ?*const Cleanup,
    entries: []?Entry,
    const Entry = struct { source_key: ops.CT, source_value: ops.CT, held: contracts.ForwardKvReuse.Pair };

    pub fn init(a: std.mem.Allocator, backend: *const ops.ComputeBackend, layers: usize, cleanup: ?*const Cleanup) !Scope {
        const entries = try a.alloc(?Entry, layers);
        @memset(entries, null);
        return .{ .allocator = a, .backend = backend, .cleanup = cleanup, .entries = entries };
    }
    fn release(self: *Scope, value: ops.CT) void {
        if (self.cleanup) |cleanup| self.backend.freeWithCleanup(value, cleanup.*) else self.backend.free(value);
    }
    pub fn deinit(self: *Scope) void {
        for (self.entries) |entry| if (entry) |held| {
            self.release(held.held.key);
            self.release(held.held.value);
        };
        self.allocator.free(self.entries);
    }
    pub fn context(self: *Scope) contracts.ForwardKvReuse {
        return .{ .ptr = self, .resolve_fn = resolve };
    }
    fn resolve(raw: *anyopaque, layer: usize, read_only: bool, key: ops.CT, value: ops.CT) !contracts.ForwardKvReuse.Pair {
        const self: *Scope = @ptrCast(@alignCast(raw));
        if (layer >= self.entries.len) return error.InvalidForwardKvLayer;
        if (read_only) return if (self.entries[layer]) |entry| entry.held else error.MissingForwardKvDonor;
        if (self.entries[layer]) |entry| {
            // A declined fused dispatch may retry the same operands via GQA.
            if (entry.source_key != key or entry.source_value != value) return error.DuplicateForwardKvDonor;
            return .{ .key = key, .value = value };
        }
        const held_key = (try self.backend.retainImmutableTensor(key, self.allocator)) orelse return error.ForwardKvRetentionUnsupported;
        errdefer self.release(held_key);
        const held_value = (try self.backend.retainImmutableTensor(value, self.allocator)) orelse return error.ForwardKvRetentionUnsupported;
        self.entries[layer] = .{ .source_key = key, .source_value = value, .held = .{ .key = held_key, .value = held_value } };
        return .{ .key = key, .value = value };
    }
};

test "forward KV reuse preserves causal donor values after original tensors retire" {
    const a = std.testing.allocator;
    const native = @import("../ops/native_compute.zig");
    var weights = native.WeightStore{ .allocator = a, .resident_weights = .{}, .lazy_weights = .{} };
    var compute = native.NativeCompute.init(a, &weights, null);
    defer compute.deinit();
    const cb = compute.computeBackend();
    var scope = try Scope.init(a, &cb, 2, null);
    defer scope.deinit();
    const q = try cb.fromFloat32Shape(&.{ 0, 0, 0, 0 }, &.{ 2, 2 });
    defer cb.free(q);
    var context = ops.AttentionContext{ .total_sequence_len = 2, .query_sequence_len = 2, .kv_sequence_len = 2, .layer_index = 0, .forward_kv_reuse = scope.context() };
    {
        const k = try cb.fromFloat32Shape(&.{ 0, 0, 0, 0 }, &.{ 2, 2 });
        defer cb.free(k);
        const v = try cb.fromFloat32Shape(&.{ 2, 4, 6, 8 }, &.{ 2, 2 });
        defer cb.free(v);
        const donor = try cb.gqaPagedAttention(q, k, v, null, context, 1, 1, 1, 2);
        cb.free(donor);
    }
    context.skip_kv_write = true;
    const output = try cb.gqaPagedAttention(q, q, q, null, context, 1, 1, 1, 2);
    defer cb.free(output);
    const actual = try cb.toFloat32(output, a);
    defer a.free(actual);
    for ([_]f32{ 2, 4, 4, 6 }, actual) |want, got| try std.testing.expectApproxEqAbs(want, got, 1e-6);
    context.layer_index = 1;
    try std.testing.expectError(error.MissingForwardKvDonor, cb.gqaPagedAttention(q, q, q, null, context, 1, 1, 1, 2));
    context.mode = .paged_decode;
    try std.testing.expectError(error.InvalidForwardKvMode, cb.gqaPagedAttention(q, q, q, null, context, 1, 1, 1, 2));
}
