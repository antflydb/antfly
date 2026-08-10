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

//! Storage-free consumer for the compiled data-Raft apply/projection owner.

const std = @import("std");
const abi = @import("kernel_owner_abi");

pub const RaftApplyStoreConfig = struct {
    root_dir: []const u8,
    no_sync: bool = false,
    read_only: bool = false,
    context: ?*anyopaque = null,
};

pub const AppliedDataBatch = struct {
    commit_index: u64,
    entry_count: usize,
    normal_entry_count: usize,
    admin_entry_count: usize,
    last_entry_term: u64,
    last_entry_index: u64,
};

pub const RaftApplyStore = struct {
    handle: ?*anyopaque,

    pub fn init(_: std.mem.Allocator, cfg: RaftApplyStoreConfig) !RaftApplyStore {
        var handle: ?*anyopaque = null;
        try statusToError(abi.antfly_data_apply_store_open(&.{
            .no_sync = @intFromBool(cfg.no_sync),
            .read_only = @intFromBool(cfg.read_only),
            .context = cfg.context,
            .root_dir = .fromSlice(cfg.root_dir),
        }, &handle));
        return .{ .handle = handle orelse return error.StorageKernelFailure };
    }

    pub fn deinit(self: *RaftApplyStore) void {
        abi.antfly_data_apply_store_close(self.handle);
        self.* = undefined;
    }

    pub fn applyBatch(self: *RaftApplyStore, group_id: u64, commit_index: u64, entries: []const u8) !void {
        try statusToError(abi.antfly_data_apply_store_apply_batch(self.handle, &.{
            .group_id = group_id,
            .commit_index = commit_index,
            .entries = .fromSlice(entries),
        }));
    }

    pub fn buildSnapshot(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64) ![]u8 {
        var owned: abi.OwnedBytes = .{};
        try statusToError(abi.antfly_data_apply_store_build_snapshot(self.handle, &.{
            .group_id = group_id,
        }, &owned));
        defer abi.antfly_storage_owner_buffer_destroy(&owned);
        return try alloc.dupe(u8, owned.slice());
    }

    pub fn installSnapshot(
        self: *RaftApplyStore,
        group_id: u64,
        commit_index: u64,
        snapshot: []const u8,
    ) !void {
        try statusToError(abi.antfly_data_apply_store_install_snapshot(self.handle, &.{
            .group_id = group_id,
            .commit_index = commit_index,
            .snapshot = .fromSlice(snapshot),
        }));
    }

    pub fn latestBatch(self: *RaftApplyStore, group_id: u64) !?AppliedDataBatch {
        var result: abi.DataApplyLatestResult = .{};
        try statusToError(abi.antfly_data_apply_store_latest(self.handle, &.{
            .group_id = group_id,
        }, &result));
        if (result.version != abi.abi_version) return error.InvalidAbiVersion;
        if (result.present == 0) return null;
        return .{
            .commit_index = result.commit_index,
            .entry_count = @intCast(result.entry_count),
            .normal_entry_count = @intCast(result.normal_entry_count),
            .admin_entry_count = @intCast(result.admin_entry_count),
            .last_entry_term = result.last_entry_term,
            .last_entry_index = result.last_entry_index,
        };
    }

    pub fn retainActiveGroups(self: *RaftApplyStore, group_ids: []const u64) !void {
        const request = groupsRequest(group_ids);
        try statusToError(abi.antfly_data_apply_store_retain_groups(self.handle, &request));
    }

    pub fn beginActiveGroupTransition(self: *RaftApplyStore, group_ids: []const u64) !ActiveGroupTransition {
        var handle: ?*anyopaque = null;
        const request = groupsRequest(group_ids);
        try statusToError(abi.antfly_data_apply_store_begin_group_transition(
            self.handle,
            &request,
            &handle,
        ));
        return .{ .handle = handle orelse return error.StorageKernelFailure };
    }

    pub const ActiveGroupTransition = struct {
        handle: ?*anyopaque,
        active: bool = true,

        pub fn commit(self: *@This()) void {
            std.debug.assert(self.active);
            const status = abi.antfly_data_apply_store_commit_group_transition(self.handle);
            std.debug.assert(status == .ok);
            self.active = false;
        }

        pub fn abort(self: *@This()) void {
            if (!self.active) return;
            const status = abi.antfly_data_apply_store_abort_group_transition(self.handle);
            std.debug.assert(status == .ok);
            self.active = false;
        }

        pub fn deinit(self: *@This()) void {
            if (self.active) self.abort();
            abi.antfly_data_apply_store_destroy_group_transition(self.handle);
            self.* = undefined;
        }
    };
};

fn groupsRequest(group_ids: []const u64) abi.DataApplyGroupsRequest {
    return .{
        .group_ids = if (group_ids.len == 0) null else group_ids.ptr,
        .group_count = @intCast(group_ids.len),
    };
}

fn statusToError(status: abi.Status) !void {
    return switch (status) {
        .ok => {},
        .invalid_abi => error.InvalidAbiVersion,
        .invalid_argument => error.InvalidArgument,
        .not_found => error.NotFound,
        .busy => error.StorageOwnerBusy,
        .read_only => error.ReadOnly,
        .out_of_memory => error.OutOfMemory,
        .corrupted => error.Corrupted,
        else => error.StorageKernelFailure,
    };
}
