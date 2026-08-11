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
const error_identity = @import("kernel_error_identity");
pub const projection_wire = @import("data_raft_projection_wire.zig");

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

    pub fn prepareSnapshot(self: *RaftApplyStore, group_id: u64, applied_index: u64) !?PreparedSnapshot {
        var handle: ?*anyopaque = null;
        try statusToError(abi.antfly_data_apply_store_prepare_snapshot(self.handle, &.{
            .group_id = group_id,
            .applied_index = applied_index,
        }, &handle));
        return if (handle) |value| .{ .handle = value } else null;
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

    pub fn latestBatchForTransition(self: *RaftApplyStore, group_id: u64) !?AppliedDataBatch {
        var result: abi.DataApplyLatestResult = .{};
        try statusToError(abi.antfly_data_apply_store_latest_for_transition(self.handle, &.{
            .group_id = group_id,
        }, &result));
        return try decodeLatest(result);
    }

    pub fn observeSplitControl(
        self: *RaftApplyStore,
        alloc: std.mem.Allocator,
        group_id: u64,
    ) !projection_wire.SplitControlObservation {
        var response = try self.projection(.{ .kind = .observe_split_control, .group_id = group_id });
        defer abi.antfly_storage_owner_buffer_destroy(&response);
        return try projection_wire.decodeSplitControlAlloc(alloc, response.slice());
    }

    pub fn currentSplitState(
        self: *RaftApplyStore,
        alloc: std.mem.Allocator,
        group_id: u64,
    ) !?projection_wire.SplitState {
        var observation = try self.observeSplitControl(alloc, group_id);
        defer observation.deinit(alloc);
        const state = observation.state;
        observation.state = null;
        return state;
    }

    pub fn currentSplitDeltaSequence(
        self: *RaftApplyStore,
        alloc: std.mem.Allocator,
        group_id: u64,
    ) !u64 {
        var observation = try self.observeSplitControl(alloc, group_id);
        defer observation.deinit(alloc);
        return observation.delta_sequence;
    }

    pub fn currentSplitAcknowledgement(
        self: *RaftApplyStore,
        alloc: std.mem.Allocator,
        group_id: u64,
    ) !?projection_wire.SplitAcknowledgement {
        var observation = try self.observeSplitControl(alloc, group_id);
        defer observation.deinit(alloc);
        return observation.acknowledgement;
    }

    pub fn currentRange(
        self: *RaftApplyStore,
        alloc: std.mem.Allocator,
        group_id: u64,
    ) !projection_wire.ByteRange {
        var response = try self.projection(.{ .kind = .current_range, .group_id = group_id });
        defer abi.antfly_storage_owner_buffer_destroy(&response);
        return try projection_wire.decodeRangeAlloc(alloc, response.slice());
    }

    pub fn groupStatePageInRange(
        self: *RaftApplyStore,
        alloc: std.mem.Allocator,
        group_id: u64,
        byte_range: anytype,
        after_key: ?[]const u8,
        max_entries: usize,
        max_bytes: usize,
    ) !projection_wire.GroupStatePage {
        var response = try self.projection(.{
            .kind = .group_state_page,
            .group_id = group_id,
            .max_entries = @intCast(max_entries),
            .max_bytes = @intCast(max_bytes),
            .range_start = .fromSlice(byte_range.start),
            .range_end = .fromSlice(byte_range.end),
            .after_key = .fromSlice(after_key orelse ""),
        });
        defer abi.antfly_storage_owner_buffer_destroy(&response);
        return try projection_wire.decodeGroupStatePageAlloc(alloc, response.slice());
    }

    pub fn listSplitDeltasPage(
        self: *RaftApplyStore,
        alloc: std.mem.Allocator,
        group_id: u64,
        after_sequence: u64,
        through_sequence: u64,
        max_entries: usize,
        max_bytes: usize,
    ) !projection_wire.SplitDeltas {
        var response = try self.projection(.{
            .kind = .split_deltas_page,
            .group_id = group_id,
            .after_sequence = after_sequence,
            .through_sequence = through_sequence,
            .max_entries = @intCast(max_entries),
            .max_bytes = @intCast(max_bytes),
        });
        defer abi.antfly_storage_owner_buffer_destroy(&response);
        return try projection_wire.decodeSplitDeltasAlloc(alloc, response.slice());
    }

    pub fn captureVerifiedSplitHandoffMetadataAtRootIncarnation(
        self: *RaftApplyStore,
        alloc: std.mem.Allocator,
        group_id: u64,
        expected: AppliedDataBatch,
        root_incarnation: u128,
    ) !?projection_wire.SplitHandoffMetadata {
        var request = abi.DataApplyProjectionRequest{
            .kind = .capture_verified_handoff_metadata,
            .group_id = group_id,
            .expected = encodeLatest(expected),
        };
        std.mem.writeInt(u128, &request.root_incarnation_le, root_incarnation, .little);
        var response = self.projection(request) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        defer abi.antfly_storage_owner_buffer_destroy(&response);
        return try projection_wire.decodeHandoffMetadataAlloc(alloc, response.slice());
    }

    pub const ReconcileResult = union(enum) {
        advanced,
        reconciled,
        handoff: projection_wire.SplitHandoffMetadata,

        pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
            switch (self.*) {
                .handoff => |*value| value.deinit(alloc),
                else => {},
            }
            self.* = undefined;
        }
    };

    /// Reconciles against an opaque resident table owner. The owner handle is
    /// borrowed only for this synchronous, page-bounded kernel operation.
    pub fn reconcileAuthoritativeOwner(
        self: *RaftApplyStore,
        alloc: std.mem.Allocator,
        owner_handle: ?*anyopaque,
        group_id: u64,
        expected: ?AppliedDataBatch,
        capture_handoff: bool,
        max_page_entries: usize,
        max_page_bytes: usize,
    ) !ReconcileResult {
        var result: abi.DataApplyReconcileResult = .{};
        try statusToError(abi.antfly_data_apply_store_reconcile_owner(
            self.handle,
            owner_handle,
            &.{
                .capture_handoff = @intFromBool(capture_handoff),
                .group_id = group_id,
                .max_page_entries = @intCast(max_page_entries),
                .max_page_bytes = @intCast(max_page_bytes),
                .expected = if (expected) |value| encodeLatest(value) else .{},
            },
            &result,
        ));
        defer abi.antfly_storage_owner_buffer_destroy(&result.handoff_metadata);
        if (result.version != abi.abi_version) return error.InvalidAbiVersion;
        return switch (result.state) {
            .advanced => .advanced,
            .reconciled => .reconciled,
            .handoff => .{ .handoff = try projection_wire.decodeHandoffMetadataAlloc(
                alloc,
                result.handoff_metadata.slice(),
            ) },
        };
    }

    fn projection(
        self: *RaftApplyStore,
        request: abi.DataApplyProjectionRequest,
    ) !abi.OwnedBytes {
        var result: abi.OwnedBytes = .{};
        try statusToError(abi.antfly_data_apply_store_projection(self.handle, &request, &result));
        return result;
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

    pub const PreparedSnapshot = struct {
        handle: ?*anyopaque,

        pub const MaterializedFile = struct {
            path: []u8,
            size: u64,

            pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
                std.Io.Dir.cwd().deleteFile(std.Options.debug_io, self.path) catch {};
                alloc.free(self.path);
                self.* = undefined;
            }
        };

        pub fn materializeFile(self: *PreparedSnapshot, alloc: std.mem.Allocator) !MaterializedFile {
            var result: abi.DataApplyPreparedSnapshotResult = .{};
            try statusToError(abi.antfly_data_apply_prepared_snapshot_materialize(self.handle, &result));
            defer abi.antfly_storage_owner_buffer_destroy(&result.path);
            if (result.version != abi.abi_version) return error.InvalidAbiVersion;
            const path = alloc.dupe(u8, result.path.slice()) catch |err| {
                std.Io.Dir.cwd().deleteFile(std.Options.debug_io, result.path.slice()) catch {};
                return err;
            };
            return .{ .path = path, .size = result.size };
        }

        pub fn cancel(self: *PreparedSnapshot) void {
            const status = abi.antfly_data_apply_prepared_snapshot_cancel(self.handle);
            std.debug.assert(status == .ok);
        }

        pub fn deinit(self: *PreparedSnapshot) void {
            abi.antfly_data_apply_prepared_snapshot_destroy(self.handle);
            self.* = undefined;
        }
    };
};

fn decodeLatest(result: abi.DataApplyLatestResult) !?AppliedDataBatch {
    if (result.version != abi.abi_version) return error.InvalidAbiVersion;
    if (result.present == 0) return null;
    return .{
        .commit_index = result.commit_index,
        .entry_count = std.math.cast(usize, result.entry_count) orelse return error.InvalidArgument,
        .normal_entry_count = std.math.cast(usize, result.normal_entry_count) orelse return error.InvalidArgument,
        .admin_entry_count = std.math.cast(usize, result.admin_entry_count) orelse return error.InvalidArgument,
        .last_entry_term = result.last_entry_term,
        .last_entry_index = result.last_entry_index,
    };
}

fn encodeLatest(value: AppliedDataBatch) abi.DataApplyLatestResult {
    return .{
        .present = 1,
        .commit_index = value.commit_index,
        .entry_count = @intCast(value.entry_count),
        .normal_entry_count = @intCast(value.normal_entry_count),
        .admin_entry_count = @intCast(value.admin_entry_count),
        .last_entry_term = value.last_entry_term,
        .last_entry_index = value.last_entry_index,
    };
}

fn groupsRequest(group_ids: []const u64) abi.DataApplyGroupsRequest {
    return .{
        .group_ids = if (group_ids.len == 0) null else group_ids.ptr,
        .group_count = @intCast(group_ids.len),
    };
}

fn statusToError(status: abi.Status) !void {
    return error_identity.statusToError(status);
}
