// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");
const managed_host = @import("managed_host.zig");
const metadata_view = @import("metadata_view.zig");
const service = @import("service.zig");
const reconciler = @import("reconciler.zig");

pub const MetadataUpdateSource = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        drain_updates: *const fn (ptr: *anyopaque, alloc: std.mem.Allocator, max_updates: usize) anyerror![]metadata_view.MetadataUpdate,
        free_updates: ?*const fn (ptr: *anyopaque, alloc: std.mem.Allocator, updates: []metadata_view.MetadataUpdate) void = null,
    };

    pub fn drainUpdates(self: MetadataUpdateSource, alloc: std.mem.Allocator, max_updates: usize) ![]metadata_view.MetadataUpdate {
        return try self.vtable.drain_updates(self.ptr, alloc, max_updates);
    }

    pub fn freeUpdates(self: MetadataUpdateSource, alloc: std.mem.Allocator, updates: []metadata_view.MetadataUpdate) void {
        if (self.vtable.free_updates) |free_updates| {
            free_updates(self.ptr, alloc, updates);
            return;
        }
        for (updates) |*update| update.deinit(alloc);
        alloc.free(updates);
    }
};

pub const MetadataUpdateSink = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        submit_update: *const fn (ptr: *anyopaque, update: metadata_view.MetadataUpdate) anyerror!void,
        submit_batch: ?*const fn (ptr: *anyopaque, updates: []const metadata_view.MetadataUpdate) anyerror!void = null,
    };

    pub fn submit(self: MetadataUpdateSink, update: metadata_view.MetadataUpdate) !void {
        return try self.vtable.submit_update(self.ptr, update);
    }

    pub fn submitBatch(self: MetadataUpdateSink, updates: []const metadata_view.MetadataUpdate) !void {
        if (self.vtable.submit_batch) |submit_batch| {
            return try submit_batch(self.ptr, updates);
        }
        for (updates) |update| try self.submit(update);
    }
};

pub const RuntimeLoopConfig = struct {
    max_updates_per_step: usize = 64,
};

pub const RuntimeStepResult = struct {
    drained_updates: usize = 0,
    reconcile: reconciler.ReconcileResult = .{},
    runtime: managed_host.ManagedSyncResult = .{
        .reconcile = .{},
        .runtime = .{},
    },
};

pub const MemoryUpdateSource = struct {
    alloc: std.mem.Allocator,
    pending: std.ArrayListUnmanaged(metadata_view.MetadataUpdate) = .empty,

    pub fn init(alloc: std.mem.Allocator) MemoryUpdateSource {
        return .{ .alloc = alloc };
    }

    pub fn deinit(self: *MemoryUpdateSource) void {
        for (self.pending.items) |*update| update.deinit(self.alloc);
        self.pending.deinit(self.alloc);
        self.* = undefined;
    }

    pub fn source(self: *MemoryUpdateSource) MetadataUpdateSource {
        return .{
            .ptr = self,
            .vtable = &.{
                .drain_updates = drainUpdates,
                .free_updates = freeUpdates,
            },
        };
    }

    pub fn sink(self: *MemoryUpdateSource) MetadataUpdateSink {
        return .{
            .ptr = self,
            .vtable = &.{
                .submit_update = submitUpdate,
                .submit_batch = submitBatchUpdates,
            },
        };
    }

    pub fn push(self: *MemoryUpdateSource, update: metadata_view.MetadataUpdate) !void {
        try self.pending.append(self.alloc, try update.clone(self.alloc));
    }

    pub fn pushBatch(self: *MemoryUpdateSource, updates: []const metadata_view.MetadataUpdate) !void {
        for (updates) |update| try self.push(update);
    }

    fn drainUpdates(ptr: *anyopaque, alloc: std.mem.Allocator, max_updates: usize) ![]metadata_view.MetadataUpdate {
        const self: *MemoryUpdateSource = @ptrCast(@alignCast(ptr));
        const take = @min(max_updates, self.pending.items.len);
        const out = try alloc.alloc(metadata_view.MetadataUpdate, take);
        errdefer alloc.free(out);
        for (self.pending.items[0..take], 0..) |update, i| out[i] = try update.clone(alloc);
        for (self.pending.items[0..take]) |*update| update.deinit(self.alloc);
        if (take < self.pending.items.len) {
            std.mem.copyForwards(metadata_view.MetadataUpdate, self.pending.items[0 .. self.pending.items.len - take], self.pending.items[take..]);
        }
        self.pending.items.len -= take;
        return out;
    }

    fn freeUpdates(_: *anyopaque, alloc: std.mem.Allocator, updates: []metadata_view.MetadataUpdate) void {
        for (updates) |*update| update.deinit(alloc);
        alloc.free(updates);
    }

    fn submitUpdate(ptr: *anyopaque, update: metadata_view.MetadataUpdate) !void {
        const self: *MemoryUpdateSource = @ptrCast(@alignCast(ptr));
        try self.push(update);
    }

    fn submitBatchUpdates(ptr: *anyopaque, updates: []const metadata_view.MetadataUpdate) !void {
        const self: *MemoryUpdateSource = @ptrCast(@alignCast(ptr));
        try self.pushBatch(updates);
    }
};

pub const ManagedHostRuntime = struct {
    alloc: std.mem.Allocator,
    cfg: RuntimeLoopConfig,
    update_source: MetadataUpdateSource,
    svc: service.ManagedHostService,

    pub fn init(
        alloc: std.mem.Allocator,
        host_cfg: managed_host.ManagedHostConfig,
        host_deps: managed_host.ManagedHostDeps,
        svc_cfg: service.ManagedServiceConfig,
        svc_deps: service.ManagedServiceDeps,
        update_source: MetadataUpdateSource,
        cfg: RuntimeLoopConfig,
    ) !ManagedHostRuntime {
        return .{
            .alloc = alloc,
            .cfg = cfg,
            .update_source = update_source,
            .svc = try service.ManagedHostService.init(alloc, host_cfg, host_deps, svc_cfg, svc_deps),
        };
    }

    pub fn deinit(self: *ManagedHostRuntime) void {
        self.svc.deinit();
        self.* = undefined;
    }

    pub fn stepOnce(self: *ManagedHostRuntime) !RuntimeStepResult {
        const updates = try self.update_source.drainUpdates(self.alloc, self.cfg.max_updates_per_step);
        defer self.update_source.freeUpdates(self.alloc, updates);

        if (updates.len > 0) try self.svc.submitBatch(updates);

        var result = RuntimeStepResult{ .drained_updates = updates.len };
        if (self.svc.pending_updates.items.len > 0) {
            result.runtime = try self.svc.syncPending();
            result.reconcile = result.runtime.reconcile;
        } else {
            try self.svc.runRound();
        }
        return result;
    }
};

pub const ManagedHttpHostRuntime = struct {
    alloc: std.mem.Allocator,
    cfg: RuntimeLoopConfig,
    update_source: MetadataUpdateSource,
    svc: service.ManagedHttpHostService,

    pub fn init(
        alloc: std.mem.Allocator,
        host_cfg: managed_host.ManagedHttpHostConfig,
        host_deps: managed_host.ManagedHttpHostDeps,
        svc_cfg: service.ManagedServiceConfig,
        svc_deps: service.ManagedServiceDeps,
        update_source: MetadataUpdateSource,
        cfg: RuntimeLoopConfig,
    ) !ManagedHttpHostRuntime {
        return .{
            .alloc = alloc,
            .cfg = cfg,
            .update_source = update_source,
            .svc = try service.ManagedHttpHostService.init(alloc, host_cfg, host_deps, svc_cfg, svc_deps),
        };
    }

    pub fn deinit(self: *ManagedHttpHostRuntime) void {
        self.svc.deinit();
        self.* = undefined;
    }

    pub fn start(self: *ManagedHttpHostRuntime) !void {
        try self.svc.start();
    }

    pub fn stop(self: *ManagedHttpHostRuntime) void {
        self.svc.stop();
    }

    pub fn baseUri(self: *ManagedHttpHostRuntime, alloc: std.mem.Allocator) ![]u8 {
        return try self.svc.baseUri(alloc);
    }

    pub fn stepOnce(self: *ManagedHttpHostRuntime) !RuntimeStepResult {
        const updates = try self.update_source.drainUpdates(self.alloc, self.cfg.max_updates_per_step);
        defer self.update_source.freeUpdates(self.alloc, updates);

        if (updates.len > 0) try self.svc.submitBatch(updates);

        var result = RuntimeStepResult{ .drained_updates = updates.len };
        if (self.svc.pending_updates.items.len > 0) {
            result.runtime = try self.svc.syncPending();
            result.reconcile = result.runtime.reconcile;
        } else {
            try self.svc.runRound();
        }
        return result;
    }
};

test "managed host runtime deterministically drains metadata updates" {
    const raft_engine = @import("raft_engine");
    const catalog = @import("catalog.zig");
    const host_mod = @import("host.zig");

    const Factory = struct {
        alloc: std.mem.Allocator,
        store: *raft_engine.core.MemoryStorage,

        fn iface(self: *@This()) host_mod.ReplicaDescriptorFactory {
            return .{
                .ptr = self,
                .vtable = &.{
                    .build_descriptor = buildDescriptor,
                    .free_descriptor = freeDescriptor,
                },
            };
        }

        fn buildDescriptor(ptr: *anyopaque, record: catalog.ReplicaRecord) !raft_engine.runtime.ReplicaDescriptor {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const peers = try self.alloc.dupe(raft_engine.core.types.NodeId, &[_]raft_engine.core.types.NodeId{record.local_node_id});
            return .{
                .group = .{
                    .group_id = record.group_id,
                    .local_node_id = record.local_node_id,
                    .raft_config = .{
                        .id = record.local_node_id,
                        .group_id = record.group_id,
                        .peers = peers[0..],
                        .election_tick = 5,
                        .heartbeat_tick = 1,
                        .pre_vote = false,
                    },
                    .storage = self.store.storage(),
                },
                .bootstrap = .persisted,
            };
        }

        fn freeDescriptor(ptr: *anyopaque, alloc: std.mem.Allocator, desc: *raft_engine.runtime.ReplicaDescriptor) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = alloc;
            self.alloc.free(desc.group.raft_config.peers);
        }
    };

    var source = MemoryUpdateSource.init(std.testing.allocator);
    defer source.deinit();
    try source.push(.{
        .replica_intent = .{
            .upsert = .{
                .record = .{
                    .group_id = 901,
                    .replica_id = 1,
                    .local_node_id = 1,
                },
                .peer_node_ids = &.{},
            },
        },
    });

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };
    var runtime = try ManagedHostRuntime.init(
        std.testing.allocator,
        .{ .host = .{ .local_node_id = 1 } },
        .{ .host = .{ .descriptor_factory = factory.iface() } },
        .{},
        .{},
        source.source(),
        .{},
    );
    defer runtime.deinit();

    const result = try runtime.stepOnce();
    try std.testing.expectEqual(@as(usize, 1), result.drained_updates);
    try std.testing.expectEqual(@as(usize, 1), result.reconcile.ensured);
    try std.testing.expectEqual(@as(usize, 0), source.pending.items.len);
    try std.testing.expectEqual(@as(usize, 1), runtime.svc.metrics.applied_updates);
}

test "metadata update sink feeds deterministic source queue" {
    var source = MemoryUpdateSource.init(std.testing.allocator);
    defer source.deinit();

    const sink = source.sink();
    try sink.submit(.{
        .replica_intent = .{
            .upsert = .{
                .record = .{
                    .group_id = 910,
                    .replica_id = 2,
                    .local_node_id = 3,
                },
                .peer_node_ids = &.{ 3, 4 },
            },
        },
    });

    const drained = try source.source().drainUpdates(std.testing.allocator, 16);
    defer source.source().freeUpdates(std.testing.allocator, drained);

    try std.testing.expectEqual(@as(usize, 1), drained.len);
    try std.testing.expectEqual(@as(u64, 910), drained[0].replica_intent.upsert.record.group_id);
}

test "runtime loop module compiles" {
    _ = MetadataUpdateSource;
    _ = MetadataUpdateSink;
    _ = RuntimeLoopConfig;
    _ = RuntimeStepResult;
    _ = MemoryUpdateSource;
    _ = ManagedHostRuntime;
    _ = ManagedHttpHostRuntime;
}
