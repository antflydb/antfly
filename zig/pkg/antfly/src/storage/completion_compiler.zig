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
    /// The DB's final batch planner supplies complete logical mutations. Expand
    /// DocStore columnar metadata into point operations before sealing them.
    physical_mutations: bool = false,
    allow_named_participants: bool = false,
    /// Retained consensus completion binds its own authoritative term/index.
    /// This does not enable participant acknowledgement or outbox mutations.
    allow_raft_marker: bool = false,
    /// Null preserves the legacy timestamp binding. Physical plans enumerate
    /// only rows whose TTL timestamp came from the future commit clock.
    timestamp_keys: ?[]const []const u8 = null,
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
        const bindings = try alloc.alloc(slot.Binding, std.math.mul(usize, source.count, 128) catch return error.CompletionPlanCapacityExceeded);
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

/// Exercise the same DocStore row/columnar expansion as publication, while the
/// only writable capability is a bounded mutation log. Payload externalization
/// is deliberately absent: a caller must prove all artifacts are inline.
pub const PhysicalSink = struct {
    baseline: *erased.Batch,
    plan: *mutations.Plan,
    columns_invalidated: bool = false,
    columnar_mutation: ?@import("internal_keys.zig").ColumnarMutationToken = null,

    pub fn runtime(self: *PhysicalSink, alloc: Allocator) erased.Batch {
        return .{ .allocator = alloc, .ptr = self, .vtable = &vtable };
    }
    pub fn writer(self: *PhysicalSink, alloc: Allocator, batch: *erased.Batch) @import("docstore.zig").DocStore.Batch.BatchTxn {
        return .{ .alloc = alloc, .runtime = batch, .columns_invalidated = &self.columns_invalidated, .columnar_mutation = &self.columnar_mutation };
    }
    fn cast(raw: *anyopaque) *PhysicalSink {
        return @ptrCast(@alignCast(raw));
    }
    fn get(raw: *anyopaque, key: []const u8) anyerror![]const u8 {
        const self = cast(raw);
        var remaining = self.plan.count;
        while (remaining != 0) {
            remaining -= 1;
            const op = self.plan.operations()[remaining];
            if (std.mem.eql(u8, op.key, key)) return if (op.kind == .put) op.value else error.NotFound;
        }
        return self.baseline.get(key);
    }
    fn put(raw: *anyopaque, key: []const u8, value: []const u8) anyerror!void {
        try cast(raw).plan.put(key, value);
    }
    fn delete(raw: *anyopaque, key: []const u8) anyerror!void {
        try cast(raw).plan.delete(key);
    }
    fn abort(_: Allocator, _: *anyopaque) void {}
    fn commit(_: Allocator, _: *anyopaque) anyerror!void {
        return error.UnsupportedCompletionTemplateWrite;
    }
    const vtable: erased.Batch.VTable = .{ .abort = abort, .commit = commit, .get = get, .put = put, .delete = delete };
};
pub const CompiledTemplates = struct {
    prepare: Template,
    commit: Template,
    abort: Template,
    intent_revision: u64,
    /// These are the compiler's sample inputs, not future authoritative values.
    timestamp: u64,
    replay_sequence: ?u64,
    /// Actual point reads against the immutable base, including absent keys.
    /// Replicated admission must cover these dependencies as well as writes.
    baseline_reads: [][]const u8,
    pub fn deinit(self: *CompiledTemplates) void {
        for (self.baseline_reads) |key| self.prepare.allocator.free(key);
        self.prepare.allocator.free(self.baseline_reads);
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
    baseline_reads: std.ArrayListUnmanaged([]const u8) = .empty,
    baseline_read_bytes: usize = 0,
    pub fn init(alloc: Allocator, snapshot: *erased.ReadTxn, limits: mutations.Limits) !Overlay {
        return .{ .snapshot = snapshot, .log = try mutations.Plan.init(alloc, limits) };
    }
    pub fn deinit(self: *Overlay) void {
        std.debug.assert(!self.batch_open);
        for (self.baseline_reads.items) |key| self.log.alloc.free(key);
        self.baseline_reads.deinit(self.log.alloc);
        self.log.deinit();
    }
    pub fn copyBaselineReads(self: *const Overlay, alloc: Allocator) ![][]const u8 {
        const keys = try alloc.alloc([]const u8, self.baseline_reads.items.len);
        var copied: usize = 0;
        errdefer {
            for (keys[0..copied]) |key| alloc.free(key);
            alloc.free(keys);
        }
        for (self.baseline_reads.items, keys) |key, *out| {
            out.* = try alloc.dupe(u8, key);
            copied += 1;
        }
        return keys;
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
        for (self.baseline_reads.items) |existing| if (std.mem.eql(u8, existing, key)) return self.snapshot.get(key);
        if (self.baseline_reads.items.len == 512 or key.len > 256 * 1024 -| self.baseline_read_bytes)
            return error.CompletionPlanCapacityExceeded;
        const owned = try self.log.alloc.dupe(u8, key);
        self.baseline_reads.append(self.log.alloc, owned) catch |err| {
            self.log.alloc.free(owned);
            return err;
        };
        self.baseline_read_bytes += key.len;
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
