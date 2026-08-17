// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0.

//! Transport-neutral metadata node lifecycle operations.

const std = @import("std");
const operation = @import("../api/operation.zig");
const raft_reconciler = @import("../raft/reconciler.zig");
const metadata_api = @import("api.zig");
const table_manager = @import("table_manager.zig");

pub const Source = struct {
    ptr: *anyopaque,
    vtable: *const VTable,
    supports_upsert_node: bool = false,
    supports_upsert_store: bool = false,
    supports_report_store_status: bool = false,
    supports_request_shutdown: bool = false,
    supports_cancel_shutdown: bool = false,
    supports_finalize_shutdown: bool = false,

    pub const VTable = struct {
        snapshot: *const fn (*anyopaque) anyerror!metadata_api.AdminSnapshot,
        free_snapshot: *const fn (*anyopaque, *metadata_api.AdminSnapshot) void,
        upsert_node: ?*const fn (*anyopaque, std.mem.Allocator, table_manager.NodeRecord) anyerror!void = null,
        upsert_store: ?*const fn (*anyopaque, std.mem.Allocator, table_manager.StoreRecord) anyerror!void = null,
        report_store_status: ?*const fn (*anyopaque, std.mem.Allocator, table_manager.StoreStatusReport) anyerror!void = null,
        request_shutdown: ?*const fn (*anyopaque, u64) anyerror!void = null,
        cancel_shutdown: ?*const fn (*anyopaque, u64) anyerror!void = null,
        finalize_shutdown: ?*const fn (*anyopaque, u64) anyerror!void = null,
        trigger_reallocate: ?*const fn (*anyopaque) anyerror!void = null,
    };
};

/// Owns parsed registration records until an operation transfers each record
/// to its source. Source callbacks take ownership when invoked, including when
/// they return an error.
pub const Registration = struct {
    node: ?table_manager.NodeRecord,
    store: ?table_manager.StoreRecord = null,

    pub fn deinit(self: *Registration, alloc: std.mem.Allocator) void {
        if (self.node) |record| table_manager.freeNode(alloc, record);
        if (self.store) |record| table_manager.freeStore(alloc, record);
        self.* = undefined;
    }
};

/// Owns a status report until it is transferred to the source callback.
pub const StatusReport = struct {
    value: ?table_manager.StoreStatusReport,

    pub fn deinit(self: *StatusReport, alloc: std.mem.Allocator) void {
        if (self.value) |report| freeStoreStatusReport(alloc, report);
        self.* = undefined;
    }
};

pub const Operations = struct {
    source: Source,

    pub fn register(self: Operations, alloc: std.mem.Allocator, ctx: operation.RequestContext, registration: *Registration) !void {
        try ctx.ensureActive();
        const node = registration.node orelse return error.InvalidArgument;
        if (node.node_id == 0) return error.InvalidArgument;
        if (registration.store) |store| {
            if (store.store_id == 0 or store.node_id != node.node_id or store.store_id != node.node_id)
                return error.StoreIdentityMismatch;
            if (!self.source.supports_upsert_store) return error.UnsupportedOperation;
        }
        if (!self.source.supports_upsert_node) return error.UnsupportedOperation;
        const upsert_node = self.source.vtable.upsert_node.?;

        var snapshot = try self.source.vtable.snapshot(self.source.ptr);
        defer self.source.vtable.free_snapshot(self.source.ptr, &snapshot);
        try preserveLifecycleAndDrainIntent(alloc, &snapshot, registration);

        const owned_node = registration.node.?;
        registration.node = null;
        try upsert_node(self.source.ptr, alloc, owned_node);
        if (registration.store) |owned_store| {
            registration.store = null;
            try self.source.vtable.upsert_store.?(self.source.ptr, alloc, owned_store);
        }
    }

    pub fn reportStatus(self: Operations, alloc: std.mem.Allocator, ctx: operation.RequestContext, report: *StatusReport) !void {
        try ctx.ensureActive();
        if (!self.source.supports_report_store_status) return error.UnsupportedOperation;
        const callback = self.source.vtable.report_store_status.?;
        const value = report.value orelse return error.InvalidArgument;
        if (value.store_id == 0) return error.InvalidArgument;
        report.value = null;
        try callback(self.source.ptr, alloc, value);
    }

    pub fn requestShutdown(self: Operations, alloc: std.mem.Allocator, ctx: operation.RequestContext, node_id: u64) !void {
        try ctx.ensureActive();
        if (node_id == 0) return error.InvalidArgument;
        if (self.source.supports_request_shutdown) {
            const callback = self.source.vtable.request_shutdown.?;
            if (callback(self.source.ptr, node_id)) |_| {
                try self.triggerReallocateIfSupported();
                return;
            } else |err| switch (err) {
                error.UnsupportedOperation => {},
                else => return err,
            }
        }

        if (!self.source.supports_upsert_node or !self.source.supports_upsert_store) return error.UnsupportedOperation;
        const upsert_node = self.source.vtable.upsert_node.?;
        const upsert_store = self.source.vtable.upsert_store.?;
        var snapshot = try self.source.vtable.snapshot(self.source.ptr);
        defer self.source.vtable.free_snapshot(self.source.ptr, &snapshot);

        var changed = false;
        var node_found = false;
        for (snapshot.nodes) |node| {
            if (node.node_id != node_id) continue;
            node_found = true;
            // Shutdown is monotonic unless the operator explicitly cancels it.
            // In particular, an idempotent request retry must not demote a
            // pending finalization back to draining and lose its completion
            // intent.
            if (!table_manager.nodeLifecycleActive(node.lifecycle)) break;
            var updated = try table_manager.cloneNode(alloc, node);
            var updated_owned = true;
            errdefer if (updated_owned) table_manager.freeNode(alloc, updated);
            const draining = try alloc.dupe(u8, table_manager.node_lifecycle_draining);
            alloc.free(updated.lifecycle);
            updated.lifecycle = draining;
            updated_owned = false;
            try upsert_node(self.source.ptr, alloc, updated);
            changed = true;
            break;
        }
        if (!node_found) {
            const role = try alloc.dupe(u8, "data");
            var role_owned = true;
            errdefer if (role_owned) alloc.free(role);
            const lifecycle = try alloc.dupe(u8, table_manager.node_lifecycle_draining);
            var lifecycle_owned = true;
            errdefer if (lifecycle_owned) alloc.free(lifecycle);
            role_owned = false;
            lifecycle_owned = false;
            try upsert_node(self.source.ptr, alloc, .{
                .node_id = node_id,
                .role = role,
                .lifecycle = lifecycle,
            });
            changed = true;
        }
        for (snapshot.stores) |store| {
            if (store.node_id != node_id or store.drain_requested) continue;
            var updated = try table_manager.cloneStore(alloc, store);
            var updated_owned = true;
            errdefer if (updated_owned) table_manager.freeStore(alloc, updated);
            updated.drain_requested = true;
            updated_owned = false;
            try upsert_store(self.source.ptr, alloc, updated);
            changed = true;
        }
        if (changed) try self.triggerReallocateIfSupported();
    }

    pub fn cancelShutdown(self: Operations, alloc: std.mem.Allocator, ctx: operation.RequestContext, node_id: u64) !void {
        try ctx.ensureActive();
        if (node_id == 0) return error.InvalidArgument;
        if (self.source.supports_cancel_shutdown) {
            const callback = self.source.vtable.cancel_shutdown.?;
            if (callback(self.source.ptr, node_id)) |_| {
                try self.triggerReallocateIfSupported();
                return;
            } else |err| switch (err) {
                error.UnsupportedOperation => {},
                else => return err,
            }
        }

        if (!self.source.supports_upsert_node or !self.source.supports_upsert_store) return error.UnsupportedOperation;
        const upsert_node = self.source.vtable.upsert_node.?;
        const upsert_store = self.source.vtable.upsert_store.?;
        var snapshot = try self.source.vtable.snapshot(self.source.ptr);
        defer self.source.vtable.free_snapshot(self.source.ptr, &snapshot);
        var changed = false;
        for (snapshot.nodes) |node| {
            if (node.node_id != node_id) continue;
            if (table_manager.nodeLifecycleActive(node.lifecycle)) break;
            var updated = try table_manager.cloneNode(alloc, node);
            var updated_owned = true;
            errdefer if (updated_owned) table_manager.freeNode(alloc, updated);
            const active = try alloc.dupe(u8, table_manager.node_lifecycle_active);
            alloc.free(updated.lifecycle);
            updated.lifecycle = active;
            updated_owned = false;
            try upsert_node(self.source.ptr, alloc, updated);
            changed = true;
            break;
        }
        for (snapshot.stores) |store| {
            if (store.node_id != node_id or !store.drain_requested) continue;
            var updated = try table_manager.cloneStore(alloc, store);
            var updated_owned = true;
            errdefer if (updated_owned) table_manager.freeStore(alloc, updated);
            updated.drain_requested = false;
            updated_owned = false;
            try upsert_store(self.source.ptr, alloc, updated);
            changed = true;
        }
        if (changed) try self.triggerReallocateIfSupported();
    }

    pub fn finalizeShutdown(self: Operations, ctx: operation.RequestContext, node_id: u64) !void {
        try ctx.ensureActive();
        if (node_id == 0) return error.InvalidArgument;
        if (!self.source.supports_finalize_shutdown) return error.UnsupportedOperation;
        const callback = self.source.vtable.finalize_shutdown.?;
        var snapshot = try self.source.vtable.snapshot(self.source.ptr);
        defer self.source.vtable.free_snapshot(self.source.ptr, &snapshot);
        if (nodeFinalizeUnsafe(&snapshot, node_id)) return error.ActiveNodeFinalizeRejected;
        try callback(self.source.ptr, node_id);
        try self.triggerReallocateIfSupported();
    }

    fn triggerReallocateIfSupported(self: Operations) !void {
        const callback = self.source.vtable.trigger_reallocate orelse return;
        callback(self.source.ptr) catch |err| switch (err) {
            error.UnsupportedOperation => {},
            else => return err,
        };
    }
};

fn preserveLifecycleAndDrainIntent(alloc: std.mem.Allocator, snapshot: *const metadata_api.AdminSnapshot, registration: *Registration) !void {
    var node = &registration.node.?;
    if (table_manager.nodeLifecycleActive(node.lifecycle)) {
        for (snapshot.nodes) |existing| {
            if (existing.node_id != node.node_id) continue;
            if (!table_manager.nodeLifecycleActive(existing.lifecycle)) {
                const lifecycle = try alloc.dupe(u8, existing.lifecycle);
                alloc.free(node.lifecycle);
                node.lifecycle = lifecycle;
            }
            break;
        }
    }
    if (registration.store) |*store| {
        if (store.drain_requested) return;
        for (snapshot.nodes) |existing| {
            if (existing.node_id != store.node_id) continue;
            if (!table_manager.nodeLifecycleActive(existing.lifecycle)) {
                store.drain_requested = true;
                return;
            }
            break;
        }
        for (snapshot.stores) |existing| {
            if (existing.store_id != store.store_id) continue;
            store.drain_requested = existing.drain_requested;
            return;
        }
    }
}

fn nodeFinalizeUnsafe(snapshot: *const metadata_api.AdminSnapshot, node_id: u64) bool {
    var draining_node = false;
    for (snapshot.nodes) |node| {
        if (node.node_id != node_id) continue;
        if (table_manager.nodeLifecycleActive(node.lifecycle)) return true;
        draining_node = true;
        break;
    }
    for (snapshot.placement_intents) |intent| {
        if (intent.record.local_node_id == node_id) return true;
    }
    for (snapshot.stores) |store| {
        if (store.node_id != node_id) continue;
        if (!draining_node and !store.drain_requested) return true;
        if (table_manager.storeHasTerminationDebt(store)) return true;
    }
    return false;
}

pub fn freeStoreStatusReport(alloc: std.mem.Allocator, report: table_manager.StoreStatusReport) void {
    alloc.free(report.health_class);
    table_manager.freeGroupStatuses(alloc, report.group_statuses);
    table_manager.freeRuntimeGroupStatusReports(alloc, report.runtime_statuses);
}

test "metadata node operations stop canceled shutdown before source mutation" {
    const FakeSource = struct {
        calls: usize = 0,
        fn snapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCall;
        }
        fn freeSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
        fn request(ptr: *anyopaque, _: u64) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
        }
    };
    var source = FakeSource{};
    const ops = Operations{ .source = .{ .ptr = &source, .vtable = &.{
        .snapshot = FakeSource.snapshot,
        .free_snapshot = FakeSource.freeSnapshot,
        .request_shutdown = FakeSource.request,
    }, .supports_request_shutdown = true } };
    var canceled = std.atomic.Value(bool).init(true);
    try std.testing.expectError(error.Canceled, ops.requestShutdown(std.testing.allocator, .{
        .cancellation = operation.CancellationToken.fromAtomic(&canceled),
    }, 9));
    try std.testing.expectEqual(@as(usize, 0), source.calls);
}

test "metadata node operations preserve pending finalization on shutdown retry" {
    const FakeSource = struct {
        nodes: [1]table_manager.NodeRecord = .{.{
            .node_id = 9,
            .lifecycle = table_manager.node_lifecycle_finalizing,
        }},
        stores: [1]table_manager.StoreRecord = .{.{
            .store_id = 9,
            .node_id = 9,
            .drain_requested = false,
        }},
        node_upserts: usize = 0,
        store_upserts: usize = 0,

        fn snapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = &.{},
                .ranges = &.{},
                .nodes = self.nodes[0..],
                .stores = self.stores[0..],
                .placement_intents = &.{},
                .split_transitions = &.{},
                .merge_transitions = &.{},
            };
        }

        fn freeSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}

        fn upsertNode(ptr: *anyopaque, alloc: std.mem.Allocator, record: table_manager.NodeRecord) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.node_upserts += 1;
            table_manager.freeNode(alloc, record);
        }

        fn upsertStore(ptr: *anyopaque, alloc: std.mem.Allocator, record: table_manager.StoreRecord) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.store_upserts += 1;
            try std.testing.expect(record.drain_requested);
            table_manager.freeStore(alloc, record);
        }
    };

    var source = FakeSource{};
    const ops = Operations{ .source = .{
        .ptr = &source,
        .vtable = &.{
            .snapshot = FakeSource.snapshot,
            .free_snapshot = FakeSource.freeSnapshot,
            .upsert_node = FakeSource.upsertNode,
            .upsert_store = FakeSource.upsertStore,
        },
        .supports_upsert_node = true,
        .supports_upsert_store = true,
    } };

    try ops.requestShutdown(std.testing.allocator, .{}, 9);
    try std.testing.expectEqual(@as(usize, 0), source.node_upserts);
    try std.testing.expectEqual(@as(usize, 1), source.store_upserts);
}

test "metadata node operations reject finalization while termination debt remains" {
    var placements = [_]raft_reconciler.PlacementIntent{.{
        .record = .{ .group_id = 101, .replica_id = 1, .local_node_id = 9 },
        .store_id = 9,
    }};
    var group_statuses = [_]table_manager.GroupStatusReport{.{ .group_id = 101 }};
    var runtime_statuses = [_]table_manager.RuntimeGroupStatusReport{.{ .group_id = 101, .node_id = 9, .store_id = 9 }};
    var stores = [_]table_manager.StoreRecord{.{
        .store_id = 9,
        .node_id = 9,
        .drain_requested = true,
        .group_statuses = group_statuses[0..],
        .runtime_statuses = runtime_statuses[0..],
    }};
    var nodes = [_]table_manager.NodeRecord{.{
        .node_id = 9,
        .lifecycle = table_manager.node_lifecycle_draining,
    }};
    var snapshot: metadata_api.AdminSnapshot = .{
        .status = .{ .metadata_group_id = 1, .metrics = .{} },
        .tables = &.{},
        .ranges = &.{},
        .nodes = nodes[0..],
        .stores = stores[0..],
        .placement_intents = placements[0..],
        .split_transitions = &.{},
        .merge_transitions = &.{},
    };

    try std.testing.expect(nodeFinalizeUnsafe(&snapshot, 9));
    snapshot.placement_intents = &.{};
    try std.testing.expect(nodeFinalizeUnsafe(&snapshot, 9));
    stores[0].group_statuses = &.{};
    try std.testing.expect(nodeFinalizeUnsafe(&snapshot, 9));
    stores[0].runtime_statuses = &.{};
    try std.testing.expect(!nodeFinalizeUnsafe(&snapshot, 9));

    // A retained observation explicitly owned by another store is not debt
    // for this store in either shutdown status or mutation admission.
    runtime_statuses[0] = .{ .group_id = 102, .node_id = 9, .store_id = 10 };
    stores[0].runtime_statuses = runtime_statuses[0..];
    try std.testing.expect(!nodeFinalizeUnsafe(&snapshot, 9));
}
