// Copyright 2026 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

const std = @import("std");
const core = @import("../core/mod.zig");

/// An immutable, bounded-memory snapshot payload. Implementations own the
/// backing resource and must support concurrent cancellation-safe reads until
/// `deinit` is called.
pub const SnapshotArtifact = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        len: *const fn (ptr: *anyopaque) u64,
        write_to: *const fn (ptr: *anyopaque, writer: *std.Io.Writer) anyerror!void,
        read_all: *const fn (ptr: *anyopaque, alloc: std.mem.Allocator) anyerror![]u8,
        deinit: *const fn (ptr: *anyopaque) void,
    };

    pub fn len(self: SnapshotArtifact) u64 {
        return self.vtable.len(self.ptr);
    }

    pub fn writeTo(self: SnapshotArtifact, writer: *std.Io.Writer) !void {
        return try self.vtable.write_to(self.ptr, writer);
    }

    pub fn readAll(self: SnapshotArtifact, alloc: std.mem.Allocator) ![]u8 {
        return try self.vtable.read_all(self.ptr, alloc);
    }

    pub fn deinit(self: SnapshotArtifact) void {
        self.vtable.deinit(self.ptr);
    }
};

pub const SnapshotMaterialization = union(enum) {
    bytes: []u8,
    artifact: SnapshotArtifact,

    pub fn len(self: SnapshotMaterialization) u64 {
        return switch (self) {
            .bytes => |bytes| bytes.len,
            .artifact => |artifact| artifact.len(),
        };
    }

    pub fn deinit(self: *SnapshotMaterialization, alloc: std.mem.Allocator) void {
        switch (self.*) {
            .bytes => |bytes| if (bytes.len > 0) alloc.free(bytes),
            .artifact => |artifact| artifact.deinit(),
        }
        self.* = undefined;
    }
};

pub const ReadyPersistenceDiagnostics = struct {
    skipped_no_durable_state: bool = false,
    used_batch: bool = false,
    used_group_storage: bool = false,
    storage_apply_elapsed_ns: u64 = 0,
    encode_elapsed_ns: u64 = 0,
    wal_append_elapsed_ns: u64 = 0,
    wal_wait_elapsed_ns: u64 = 0,
    wal_coalesce_elapsed_ns: u64 = 0,
    wal_txn_open_elapsed_ns: u64 = 0,
    wal_put_elapsed_ns: u64 = 0,
    wal_commit_elapsed_ns: u64 = 0,
    wal_physical_commits: u64 = 0,
    encoded_bytes: u64 = 0,
    delta_records_since_checkpoint: u64 = 0,
    delta_bytes_since_checkpoint: u64 = 0,
};

// GroupStorage owns raft-log durability for one hosted group.
// Implementations must consume the passed slices synchronously and must not retain them.
pub const GroupStorage = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        persist_ready: *const fn (ptr: *anyopaque, group_id: core.types.GroupId, ready: core.Ready) anyerror!void,
        compact_snapshot: *const fn (ptr: *anyopaque, group_id: core.types.GroupId, snapshot: core.types.Snapshot) anyerror!void,
        compact_snapshot_artifact: ?*const fn (
            ptr: *anyopaque,
            group_id: core.types.GroupId,
            metadata: core.types.SnapshotMetadata,
            artifact: SnapshotArtifact,
        ) anyerror!void = null,
        persist_ready_diagnostics: ?*const fn (
            ptr: *anyopaque,
            group_id: core.types.GroupId,
            ready: core.Ready,
            diagnostics: *ReadyPersistenceDiagnostics,
        ) anyerror!void = null,
    };

    pub fn persistReady(self: GroupStorage, group_id: core.types.GroupId, ready: core.Ready) !void {
        return try self.vtable.persist_ready(self.ptr, group_id, ready);
    }

    pub fn compactSnapshot(self: GroupStorage, group_id: core.types.GroupId, snapshot: core.types.Snapshot) !void {
        return try self.vtable.compact_snapshot(self.ptr, group_id, snapshot);
    }

    pub fn compactSnapshotArtifact(
        self: GroupStorage,
        alloc: std.mem.Allocator,
        group_id: core.types.GroupId,
        metadata: core.types.SnapshotMetadata,
        artifact: SnapshotArtifact,
    ) !void {
        if (self.vtable.compact_snapshot_artifact) |compact| {
            return try compact(self.ptr, group_id, metadata, artifact);
        }
        const data = try artifact.readAll(alloc);
        defer alloc.free(data);
        return try self.compactSnapshot(group_id, .{ .metadata = metadata, .data = data });
    }

    pub fn persistReadyWithDiagnostics(
        self: GroupStorage,
        group_id: core.types.GroupId,
        ready: core.Ready,
        diagnostics: ?*ReadyPersistenceDiagnostics,
    ) !void {
        if (diagnostics) |diag| {
            if (self.vtable.persist_ready_diagnostics) |persist_ready_diagnostics| {
                return try persist_ready_diagnostics(self.ptr, group_id, ready, diag);
            }
        }
        return try self.persistReady(group_id, ready);
    }
};

// PersistBatch lets the host batch raft-log durability work across groups.
// Implementations must consume the passed slices synchronously and must not retain them.
pub const PersistBatch = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        persist_ready: *const fn (ptr: *anyopaque, group_id: core.types.GroupId, ready: core.Ready) anyerror!void,
        persist_ready_diagnostics: ?*const fn (
            ptr: *anyopaque,
            group_id: core.types.GroupId,
            ready: core.Ready,
            diagnostics: *ReadyPersistenceDiagnostics,
        ) anyerror!void = null,
        finish: *const fn (ptr: *anyopaque) anyerror!void,
    };

    pub fn persistReady(self: PersistBatch, group_id: core.types.GroupId, ready: core.Ready) !void {
        return try self.vtable.persist_ready(self.ptr, group_id, ready);
    }

    pub fn persistReadyWithDiagnostics(
        self: PersistBatch,
        group_id: core.types.GroupId,
        ready: core.Ready,
        diagnostics: ?*ReadyPersistenceDiagnostics,
    ) !void {
        if (diagnostics) |diag| {
            if (self.vtable.persist_ready_diagnostics) |persist_ready_diagnostics| {
                return try persist_ready_diagnostics(self.ptr, group_id, ready, diag);
            }
        }
        return try self.persistReady(group_id, ready);
    }

    pub fn finish(self: PersistBatch) !void {
        return try self.vtable.finish(self.ptr);
    }
};

// DiskBatcher creates host-round persistence batches.
pub const DiskBatcher = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        begin_batch: *const fn (ptr: *anyopaque) anyerror!PersistBatch,
    };

    pub fn beginBatch(self: DiskBatcher) !PersistBatch {
        return try self.vtable.begin_batch(self.ptr);
    }
};

/// A point-in-time state-machine view prepared on the Raft host thread and
/// materialized by the snapshot worker. Implementations must capture the view
/// synchronously in `prepare_snapshot`. `cancel` may race with `materialize`
/// and must return promptly without calling back into MultiRaft;
/// and `deinit` may run on either the worker or shutdown thread, so both must be
/// thread-safe. The state-machine owner must outlive every source it returns;
/// MultiRaft enforces that by joining its snapshot worker on shutdown.
pub const SnapshotSource = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        materialize: *const fn (ptr: *anyopaque, alloc: std.mem.Allocator) anyerror!SnapshotMaterialization,
        cancel: ?*const fn (ptr: *anyopaque) void = null,
        deinit: *const fn (ptr: *anyopaque) void,
    };

    pub fn materialize(self: SnapshotSource, alloc: std.mem.Allocator) !SnapshotMaterialization {
        return try self.vtable.materialize(self.ptr, alloc);
    }

    pub fn cancel(self: SnapshotSource) void {
        const cancel_fn = self.vtable.cancel orelse return;
        cancel_fn(self.ptr);
    }

    pub fn deinit(self: SnapshotSource) void {
        self.vtable.deinit(self.ptr);
    }
};

// StateMachine owns apply-side effects for one hosted group.
// Implementations must consume the slices synchronously and must not retain them.
pub const StateMachine = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        prepare_snapshot: ?*const fn (
            ptr: *anyopaque,
            group_id: core.types.GroupId,
            applied_index: core.types.Index,
        ) anyerror!?SnapshotSource = null,
        build_snapshot: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: core.types.GroupId,
        ) anyerror!?[]u8 = null,
        apply_ready: *const fn (
            ptr: *anyopaque,
            group_id: core.types.GroupId,
            snapshot: ?core.types.Snapshot,
            committed_entries: []const core.Entry,
            read_states: []const core.ReadState,
        ) anyerror!void,
    };

    pub fn prepareSnapshot(
        self: StateMachine,
        group_id: core.types.GroupId,
        applied_index: core.types.Index,
    ) !?SnapshotSource {
        const prepare = self.vtable.prepare_snapshot orelse return null;
        return try prepare(self.ptr, group_id, applied_index);
    }

    pub fn buildSnapshot(self: StateMachine, alloc: std.mem.Allocator, group_id: core.types.GroupId) !?[]u8 {
        const build = self.vtable.build_snapshot orelse return null;
        return try build(self.ptr, alloc, group_id);
    }

    pub fn applyReady(
        self: StateMachine,
        group_id: core.types.GroupId,
        snapshot: ?core.types.Snapshot,
        committed_entries: []const core.Entry,
        read_states: []const core.ReadState,
    ) !void {
        return try self.vtable.apply_ready(self.ptr, group_id, snapshot, committed_entries, read_states);
    }
};

// ApplyQueue lets the host enqueue apply work and then drain it once per host round.
// Implementations must consume the passed slices synchronously and must not retain them.
pub const ApplyQueue = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        enqueue_apply: *const fn (
            ptr: *anyopaque,
            group_id: core.types.GroupId,
            snapshot: ?core.types.Snapshot,
            committed_entries: []const core.Entry,
            read_states: []const core.ReadState,
        ) anyerror!void,
        drain: *const fn (ptr: *anyopaque) anyerror!void,
        abort: *const fn (ptr: *anyopaque) void,
    };

    pub fn enqueueApply(
        self: ApplyQueue,
        group_id: core.types.GroupId,
        snapshot: ?core.types.Snapshot,
        committed_entries: []const core.Entry,
        read_states: []const core.ReadState,
    ) !void {
        return try self.vtable.enqueue_apply(self.ptr, group_id, snapshot, committed_entries, read_states);
    }

    pub fn drain(self: ApplyQueue) !void {
        return try self.vtable.drain(self.ptr);
    }

    pub fn abort(self: ApplyQueue) void {
        self.vtable.abort(self.ptr);
    }
};
