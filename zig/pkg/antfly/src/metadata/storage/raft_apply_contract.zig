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

//! Storage-free metadata apply-store contract shared by the concrete kernel
//! owner and its opaque client. Keep backend types and implementation tests out
//! of this module so consumers do not regain the physical storage graph.

const std = @import("std");
const metadata = @import("../domain.zig");
const metadata_incarnation = @import("../incarnation.zig");
const metadata_table_manager = @import("../table_manager.zig");
const topology_protocol = @import("../topology_protocol.zig");

pub const AppliedMetadataBatch = struct {
    commit_index: u64,
    entries_bytes: []const u8,
};

pub const TableTransitionFence = struct {
    generation: u64 = 0,
    active_count: u32 = 0,
    range_membership: topology_protocol.RangeMembershipAccumulator = .{},

    pub fn active(self: @This()) bool {
        return self.active_count != 0;
    }

    pub fn membership(self: @This(), table_id: u64) topology_protocol.RangeMembership {
        return self.range_membership.finish(table_id);
    }
};

pub const TableRestoreAdmission = struct {
    expected_transition_generation: u64,
    incarnation_generation: u64,
    already_applied: bool,
};

pub const TableDropProjection = struct {
    table: metadata.TableRecord,
    fence: TableTransitionFence,
    extension_owned: bool,
    range_group_ids: []u64,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.range_group_ids);
        metadata_table_manager.freeTable(alloc, self.table);
        self.* = undefined;
    }
};

pub const CatalogProjectionSnapshot = struct {
    metadata_incarnation: ?metadata_incarnation.MetadataClusterIncarnation,
    catalog_revision: u64,
    tables: []metadata.TableRecord,
    ranges: []metadata.RangeRecord,
};

pub const CatalogCursor = struct {
    metadata_incarnation: ?metadata_incarnation.MetadataClusterIncarnation,
    revision: u64,
};

pub const ProjectionSignalKind = enum {
    metadata_incarnation,
    table,
    range,
    store,
    placement_intent,
    reconcile_lease,
    shuffle_join_lease,
    split_transition,
    merge_transition,
    schema_progress,
    restore_progress,
    restore_job,
    replication_source_status,
};

pub const ProjectionSignal = struct {
    kind: ProjectionSignalKind,
    metadata_group_id: u64,
    table_name: ?[]const u8 = null,
    table_id: u64 = 0,
    group_id: u64 = 0,
    store_id: u64 = 0,
    node_id: u64 = 0,
};

pub const ProjectionListener = struct {
    ptr: *anyopaque,
    vtable: *const VTable,
    /// When set, the apply store brackets the durable commit and synchronous
    /// notification for matching projection changes with this listener's
    /// barrier callbacks. Correctness-sensitive consumers use this to
    /// serialize a short external publication step with the authoritative
    /// projection commit; ordinary listeners remain notification-only.
    commit_barrier_kind: ?ProjectionSignalKind = null,

    pub const VTable = struct {
        on_projection_signal: *const fn (ptr: *anyopaque, signal: ProjectionSignal) void,
        before_projection_commit: ?*const fn (ptr: *anyopaque) void = null,
        after_projection_commit: ?*const fn (ptr: *anyopaque) void = null,
    };

    pub fn onProjectionSignal(self: ProjectionListener, signal: ProjectionSignal) void {
        self.vtable.on_projection_signal(self.ptr, signal);
    }

    pub fn beginCommitBarrier(self: ProjectionListener) void {
        if (self.vtable.before_projection_commit) |begin| begin(self.ptr);
    }

    pub fn endCommitBarrier(self: ProjectionListener) void {
        if (self.vtable.after_projection_commit) |end| end(self.ptr);
    }

    pub fn validate(self: ProjectionListener) !void {
        const configured = self.commit_barrier_kind != null;
        if ((self.vtable.before_projection_commit != null) != configured or
            (self.vtable.after_projection_commit != null) != configured)
            return error.InvalidProjectionCommitBarrier;
    }
};

pub const CommittedKeySignal = struct {
    metadata_group_id: u64,
    key: []const u8,
};

pub const CommittedKeyListener = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        matches_key: *const fn (ptr: *anyopaque, signal: CommittedKeySignal) bool,
        on_committed_key: *const fn (ptr: *anyopaque, signal: CommittedKeySignal) void,
    };

    pub fn onCommittedKey(self: CommittedKeyListener, signal: CommittedKeySignal) void {
        if (!self.vtable.matches_key(self.ptr, signal)) return;
        self.vtable.on_committed_key(self.ptr, signal);
    }
};
