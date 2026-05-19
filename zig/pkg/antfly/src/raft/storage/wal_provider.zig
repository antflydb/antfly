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
const raft_engine = @import("raft_engine");
const catalog = @import("../catalog.zig");
const host = @import("../host.zig");
const state_machine = @import("../state_machine/mod.zig");
const storage_mod = @import("mod.zig");
const wal_replica_state = @import("wal_replica_state.zig");

pub const WalReplicaProviderConfig = struct {
    root_dir: []const u8,
    state: wal_replica_state.WalReplicaStateConfig = .{},
    flush_on_deinit: bool = true,
};

pub const WalReplicaProvider = struct {
    alloc: std.mem.Allocator,
    cfg: WalReplicaProviderConfig,
    root_dir: []u8,
    base_factory: host.ReplicaDescriptorFactory,
    states: std.AutoHashMapUnmanaged(u64, *wal_replica_state.WalReplicaState) = .empty,

    pub fn init(
        alloc: std.mem.Allocator,
        cfg: WalReplicaProviderConfig,
        base_factory: host.ReplicaDescriptorFactory,
    ) !WalReplicaProvider {
        return .{
            .alloc = alloc,
            .cfg = cfg,
            .root_dir = try alloc.dupe(u8, cfg.root_dir),
            .base_factory = base_factory,
        };
    }

    pub fn deinit(self: *WalReplicaProvider) void {
        var it = self.states.valueIterator();
        while (it.next()) |state| {
            if (self.cfg.flush_on_deinit) state.*.flushForShutdown() catch {};
            state.*.deinit();
            self.alloc.destroy(state.*);
        }
        self.states.deinit(self.alloc);
        self.alloc.free(self.root_dir);
        self.* = undefined;
    }

    pub fn flushForShutdown(self: *WalReplicaProvider) !void {
        var it = self.states.valueIterator();
        while (it.next()) |state| try state.*.flushForShutdown();
    }

    pub fn descriptorFactory(self: *WalReplicaProvider) host.ReplicaDescriptorFactory {
        return .{
            .ptr = self,
            .vtable = &.{
                .build_descriptor = buildDescriptor,
                .free_descriptor = freeDescriptor,
            },
        };
    }

    pub fn runtimeHooks(self: *WalReplicaProvider) host.RuntimeHooks {
        return .{
            .group_storage = .{
                .ptr = self,
                .vtable = &.{
                    .persist_ready = persistReady,
                },
            },
        };
    }

    pub fn appliedIndexSink(self: *WalReplicaProvider) state_machine.AppliedIndexSink {
        return .{
            .ptr = self,
            .vtable = &.{
                .set_applied_index = setAppliedIndex,
            },
        };
    }

    pub fn stateForGroup(self: *WalReplicaProvider, group_id: u64) ?*wal_replica_state.WalReplicaState {
        return self.states.get(group_id);
    }

    fn buildDescriptor(ptr: *anyopaque, record: catalog.ReplicaRecord) !raft_engine.runtime.ReplicaDescriptor {
        const self: *WalReplicaProvider = @ptrCast(@alignCast(ptr));
        var desc = try self.base_factory.buildDescriptor(record);
        errdefer self.base_factory.freeDescriptor(self.alloc, &desc);
        const state = try self.ensureState(record);
        try state.seedConfStateIfEmpty(desc.group.raft_config.peers);
        desc.group.storage = state.storage();
        desc.group.raft_config.applied = state.appliedIndex();
        return desc;
    }

    fn freeDescriptor(ptr: *anyopaque, alloc: std.mem.Allocator, desc: *raft_engine.runtime.ReplicaDescriptor) void {
        const self: *WalReplicaProvider = @ptrCast(@alignCast(ptr));
        self.base_factory.freeDescriptor(alloc, desc);
    }

    fn persistReady(ptr: *anyopaque, group_id: u64, ready: raft_engine.core.Ready) !void {
        const self: *WalReplicaProvider = @ptrCast(@alignCast(ptr));
        const state = self.states.get(group_id) orelse return error.UnknownGroup;
        try state.groupStorage().persistReady(group_id, ready);
    }

    fn setAppliedIndex(
        ptr: *anyopaque,
        group_id: raft_engine.core.types.GroupId,
        index: raft_engine.core.types.Index,
    ) !void {
        const self: *WalReplicaProvider = @ptrCast(@alignCast(ptr));
        const state = self.states.get(group_id) orelse return error.UnknownGroup;
        try state.setAppliedIndex(index);
    }

    fn ensureState(self: *WalReplicaProvider, record: catalog.ReplicaRecord) !*wal_replica_state.WalReplicaState {
        if (self.states.get(record.group_id)) |state| return state;

        var layout = try storage_mod.ReplicaPathLayout.initForReplica(self.alloc, self.root_dir, record.group_id, record.replica_id);
        defer layout.deinit(self.alloc);

        const state = try self.alloc.create(wal_replica_state.WalReplicaState);
        errdefer self.alloc.destroy(state);
        state.* = try wal_replica_state.WalReplicaState.init(self.alloc, layout, self.cfg.state);
        errdefer state.deinit();

        try self.states.put(self.alloc, record.group_id, state);
        return state;
    }
};

test "wal replica provider wires host through WAL-backed local state" {
    const BaseFactory = struct {
        alloc: std.mem.Allocator,
        dummy_store: *raft_engine.core.MemoryStorage,

        fn iface(self: *@This()) host.ReplicaDescriptorFactory {
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
            const peers = try self.alloc.dupe(u64, &.{record.local_node_id});
            return .{
                .group = .{
                    .group_id = record.group_id,
                    .local_node_id = record.local_node_id,
                    .raft_config = .{
                        .id = record.local_node_id,
                        .group_id = record.group_id,
                        .peers = peers,
                        .election_tick = 5,
                        .heartbeat_tick = 1,
                        .pre_vote = false,
                        .check_quorum = true,
                    },
                    .storage = self.dummy_store.storage(),
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

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/wal-provider", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    var dummy_store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer dummy_store.deinit();
    var base_factory = BaseFactory{ .alloc = std.testing.allocator, .dummy_store = &dummy_store };
    {
        var provider = try WalReplicaProvider.init(std.testing.allocator, .{ .root_dir = root }, base_factory.iface());
        defer provider.deinit();

        var local_host = host.Host.init(std.testing.allocator, .{ .local_node_id = 1 }, .{
            .descriptor_factory = provider.descriptorFactory(),
            .runtime_hooks = provider.runtimeHooks(),
        });
        defer local_host.deinit();

        _ = try local_host.ensureReplica(.{
            .group_id = 501,
            .replica_id = 1,
            .local_node_id = 1,
            .bootstrap_mode = .persisted,
        });
        try local_host.campaignGroup(501);
        _ = try local_host.runRound(1, 1);
        try local_host.propose(501, "wal-backed");
        _ = try local_host.runRound(1, 1);

        const state = provider.stateForGroup(501) orelse return error.MissingState;
        try std.testing.expect((try state.storage().lastIndex()) >= 1);
        var initial_state = try state.storage().initialState(std.testing.allocator);
        defer initial_state.deinit(std.testing.allocator);
        try std.testing.expectEqualSlices(u64, &.{1}, initial_state.conf_state.voters);
    }

    {
        var provider = try WalReplicaProvider.init(std.testing.allocator, .{ .root_dir = root }, base_factory.iface());
        defer provider.deinit();

        var local_host = host.Host.init(std.testing.allocator, .{ .local_node_id = 1 }, .{
            .descriptor_factory = provider.descriptorFactory(),
            .runtime_hooks = provider.runtimeHooks(),
        });
        defer local_host.deinit();

        _ = try local_host.ensureReplica(.{
            .group_id = 501,
            .replica_id = 1,
            .local_node_id = 1,
            .bootstrap_mode = .persisted,
        });

        const state = provider.stateForGroup(501) orelse return error.MissingState;
        try std.testing.expect((try state.storage().lastIndex()) >= 1);
    }
}
