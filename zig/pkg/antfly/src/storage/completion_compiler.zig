// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Bounded compiler workspace. The overlay has no forwarding write callback:
//! its only backing capability is a caller-owned stable read transaction.
//! Templates own bytes and explicit patch locations, not completion resources.
const std = @import("std");
const erased = @import("backend_erased.zig");
const types = @import("backend_types.zig");
const mutations = @import("completion_mutations.zig");
pub const slot = @import("lsm_backend/completion_slot.zig");
const Allocator = std.mem.Allocator;

pub const Options = struct {
    /// Caller owns this immutable snapshot and its lifetime. The DB apply lock
    /// must cover compilation through real prepare and fence installation.
    snapshot: *erased.ReadTxn,
    scratch_bytes: usize = 1024 * 1024,
    plan_limits: mutations.Limits = .{ .max_operations = 128, .max_bytes = 128 * 1024 },
    timestamp: u64,
    /// Only the current CJ2 binary sequence field is accepted. Arbitrary
    /// offsets cannot turn a matching constant into a dynamic field.
    replay_payload_sequence_offset: ?u32 = null,
};
pub const Template = struct {
    allocator: Allocator,
    plan: mutations.Plan,
    operations: []slot.Operation,
    binding_storage: []slot.Binding,
    binding_count: usize = 0,

    pub fn copy(alloc: Allocator, source: *const mutations.Plan) !Template {
        var plan = try mutations.Plan.init(alloc, .{ .max_operations = @max(1, source.count), .max_bytes = @max(1, source.used_bytes) });
        errdefer plan.deinit();
        for (source.operations()) |op| switch (op.kind) {
            .put => try plan.put(op.key, op.value),
            .delete => try plan.delete(op.key),
        };
        const operations = try alloc.alloc(slot.Operation, source.count);
        errdefer alloc.free(operations);
        const bindings = try alloc.alloc(slot.Binding, std.math.mul(usize, source.count, 4) catch return error.CompletionPlanCapacityExceeded);
        for (plan.operations(), operations) |op, *out| out.* = .{
            .kind = if (op.kind == .put) .put else .delete,
            .key = op.key,
            .value = op.value,
        };
        return .{ .allocator = alloc, .plan = plan, .operations = operations, .binding_storage = bindings };
    }
    /// Add bindings in operation order; a single operation's bindings remain
    /// contiguous. No mutation of bytes is performed by this module.
    pub fn bind(self: *Template, index: usize, binding: slot.Binding) !void {
        if (self.binding_count == self.binding_storage.len) return error.CompletionPlanCapacityExceeded;
        const op = &self.operations[index];
        const target = switch (binding.target) {
            .key => op.key,
            .value => op.value,
        };
        if (binding.offset > target.len or target.len - binding.offset < 8) return error.UnsupportedCompletionTemplate;
        const first = self.binding_count - op.bindings.len;
        self.binding_storage[self.binding_count] = binding;
        self.binding_count += 1;
        op.bindings = self.binding_storage[first..self.binding_count];
    }
    pub fn deinit(self: *Template) void {
        self.allocator.free(self.binding_storage);
        self.allocator.free(self.operations);
        self.plan.deinit();
        self.* = undefined;
    }
};
pub const CompiledTemplates = struct {
    prepare: Template,
    commit: Template,
    abort: Template,
    intent_revision: u64,
    /// These are the compiler's sample inputs, not future authoritative values.
    timestamp: u64,
    replay_sequence: ?u64,
    pub fn deinit(self: *CompiledTemplates) void {
        self.prepare.deinit();
        self.commit.deinit();
        self.abort.deinit();
        self.* = undefined;
    }
};

pub const Overlay = struct {
    snapshot: *erased.ReadTxn,
    log: mutations.Plan,
    committed_count: usize = 0,
    committed_bytes: usize = 0,
    batch_open: bool = false,
    pub fn init(alloc: Allocator, snapshot: *erased.ReadTxn, limits: mutations.Limits) !Overlay {
        return .{ .snapshot = snapshot, .log = try mutations.Plan.init(alloc, limits) };
    }
    pub fn deinit(self: *Overlay) void {
        std.debug.assert(!self.batch_open);
        self.log.deinit();
    }
    pub fn store(self: *Overlay) erased.Store {
        return .{ .allocator = self.log.alloc, .ptr = self, .vtable = &store_vtable };
    }
    fn cast(ptr: *anyopaque) *Overlay {
        return @ptrCast(@alignCast(ptr));
    }
    fn get(ptr: *anyopaque, key: []const u8) anyerror![]const u8 {
        const self = cast(ptr);
        var i = self.log.count;
        while (i != 0) {
            i -= 1;
            const op = self.log.operations()[i];
            if (std.mem.eql(u8, op.key, key)) return if (op.kind == .delete) error.NotFound else op.value;
        }
        return self.snapshot.get(key);
    }
    fn noDeinit(_: Allocator, _: *anyopaque) void {}
    fn capabilities(_: *anyopaque) types.Capabilities {
        return .{ .ordered_ranges = false, .cursors = false };
    }
    fn serialization(_: *anyopaque) anyerror!types.WriteSerialization {
        return .{ .acquired_by_begin = true };
    }
    fn beginRead(alloc: Allocator, ptr: *anyopaque) anyerror!erased.ReadTxn {
        return .{ .allocator = alloc, .ptr = ptr, .vtable = &read_vtable };
    }
    fn cursor(_: Allocator, _: *anyopaque) anyerror!erased.Cursor {
        return error.UnsupportedCompletionTemplateScan;
    }
    fn beginWrite(_: Allocator, _: *anyopaque) anyerror!erased.WriteTxn {
        return error.UnsupportedCompletionTemplateWrite;
    }
    fn beginBatch(alloc: Allocator, ptr: *anyopaque) anyerror!erased.Batch {
        const self = cast(ptr);
        if (self.batch_open) return error.UnsupportedCompletionTemplateNesting;
        self.batch_open = true;
        return .{ .allocator = alloc, .ptr = ptr, .vtable = &batch_vtable };
    }
    fn abort(_: Allocator, ptr: *anyopaque) void {
        const self = cast(ptr);
        std.debug.assert(self.batch_open);
        self.log.count = self.committed_count;
        self.log.used_bytes = self.committed_bytes;
        self.batch_open = false;
    }
    fn commit(_: Allocator, ptr: *anyopaque) anyerror!void {
        const self = cast(ptr);
        std.debug.assert(self.batch_open);
        self.committed_count = self.log.count;
        self.committed_bytes = self.log.used_bytes;
        self.batch_open = false;
    }
    fn put(ptr: *anyopaque, key: []const u8, value: []const u8) anyerror!void {
        const self = cast(ptr);
        std.debug.assert(self.batch_open);
        try self.log.put(key, value);
    }
    fn delete(ptr: *anyopaque, key: []const u8) anyerror!void {
        const self = cast(ptr);
        std.debug.assert(self.batch_open);
        try self.log.delete(key);
    }
    const read_vtable: erased.ReadTxn.VTable = .{ .abort = noDeinit, .get = get, .open_cursor = cursor };
    const batch_vtable: erased.Batch.VTable = .{ .abort = abort, .commit = commit, .get = get, .put = put, .delete = delete };
    const store_vtable: erased.Store.VTable = .{ .deinit = noDeinit, .capabilities = capabilities, .begin_read = beginRead, .begin_write = beginWrite, .begin_batch = beginBatch, .write_serialization = serialization };
};
