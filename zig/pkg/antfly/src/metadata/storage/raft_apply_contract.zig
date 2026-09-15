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

pub const AppliedMetadataCheckpoint = struct {
    commit_index: u64,
    input_kind: enum(u8) { committed_entries = 0, snapshot = 1 },
    input_bytes: u64,

    pub fn fromInput(commit_index: u64, kind: @FieldType(@This(), "input_kind"), bytes: []const u8) @This() {
        return .{ .commit_index = commit_index, .input_kind = kind, .input_bytes = bytes.len };
    }
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
    /// False only when existing report payloads and observation clocks are unchanged.
    store_reports_changed: bool = true,
    /// Runtime references change group facts/clocks but retain runtime pages.
    store_runtime_changed: bool = true,
    /// Borrowed until the synchronous listener returns; null invalidates all groups.
    store_group_ids: ?[]const u64 = null,
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

/// Process-local token used to detach and drain one registered callback pair.
pub const LifecycleListenerRegistration = struct { id: u64 };

const system_catalog = @import("../../system_catalog/domain.zig");

pub const TableTopologyMutation = union(enum) {
    create: struct {
        expected_transition_generation: u64,
        table: metadata.TableRecord,
        ranges: []const metadata.RangeRecord,
    },
    drop: struct {
        table_id: u64,
        expected_name: []const u8,
        expected_transition_generation: u64,
        range_contract: union(enum) {
            /// Fixed-size membership proof used by topology protocol v2.
            membership: topology_protocol.RangeMembership,
            /// Decode-only compatibility for v1 entries already present in a
            /// Raft log during a rolling binary upgrade.
            legacy_group_ids: []const u64,
        },
    },
};

pub const SystemCatalogCommand = struct {
    version: u16 = 1,
    expected_revision: u64,
    mutation: system_catalog.Mutation,
    topology: ?TableTopologyMutation = null,
    placement_update: ?struct { expected: metadata.TableRecord, replacement: metadata.TableRecord } = null,
};

pub const CatalogAdmission = struct { meta: system_catalog.Meta, placement_policy: system_catalog.PlacementPolicy = .{} };

const store_report_update = @import("../store_report_update.zig");
pub const CatalogProjectionRequest = union(enum) {
    read_store: struct { store_id: u64, reports: bool },
    read_store_group_facts: u64,
    read_store_report_targets: struct { store_id: u64, group_ids: []const u64, full: bool, include_runtime: bool },
    catalog_read: system_catalog.Read,
    catalog_export: void,
    catalog_list_tables: system_catalog.TableList,
    catalog_meta: void,
    catalog_admission: system_catalog.Mutation,
    catalog_prepare: SystemCatalogCommand,
    catalog_resolve_table: system_catalog.Target,
    catalog_resolve_identity: system_catalog.Target,
    catalog_resolve_many: system_catalog.ResolveMany,
    catalog_query_definition: []const u8,
    catalog_write_validation: []const u8,
    catalog_write_validation_revision: void,
    topology_activation: void,
    report_cursor: u64,
    read_control_stores: []const u64,
    report_baseline_progress: @import("../store_report_baseline.zig").ProgressQuery,
    report_baseline_fragment_admission: @import("../store_report_baseline.zig").Request,
    catalog_snapshot: void,
};
