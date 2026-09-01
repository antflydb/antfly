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

const metadata = @import("../domain.zig");
const metadata_incarnation = @import("../incarnation.zig");

pub const AppliedMetadataBatch = struct {
    commit_index: u64,
    entries_bytes: []const u8,
};

pub const TableTransitionFence = struct {
    generation: u64 = 0,
    active_count: u32 = 0,

    pub fn active(self: @This()) bool {
        return self.active_count != 0;
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

    pub const VTable = struct {
        on_projection_signal: *const fn (ptr: *anyopaque, signal: ProjectionSignal) void,
    };

    pub fn onProjectionSignal(self: ProjectionListener, signal: ProjectionSignal) void {
        self.vtable.on_projection_signal(self.ptr, signal);
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
