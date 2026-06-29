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

const core = @import("../core/mod.zig");

pub const ReadyPersistenceDiagnostics = struct {
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

// StateMachine owns apply-side effects for one hosted group.
// Implementations must consume the slices synchronously and must not retain them.
pub const StateMachine = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        apply_ready: *const fn (
            ptr: *anyopaque,
            group_id: core.types.GroupId,
            committed_entries: []const core.Entry,
            read_states: []const core.ReadState,
        ) anyerror!void,
    };

    pub fn applyReady(
        self: StateMachine,
        group_id: core.types.GroupId,
        committed_entries: []const core.Entry,
        read_states: []const core.ReadState,
    ) !void {
        return try self.vtable.apply_ready(self.ptr, group_id, committed_entries, read_states);
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
            committed_entries: []const core.Entry,
            read_states: []const core.ReadState,
        ) anyerror!void,
        drain: *const fn (ptr: *anyopaque) anyerror!void,
    };

    pub fn enqueueApply(
        self: ApplyQueue,
        group_id: core.types.GroupId,
        committed_entries: []const core.Entry,
        read_states: []const core.ReadState,
    ) !void {
        return try self.vtable.enqueue_apply(self.ptr, group_id, committed_entries, read_states);
    }

    pub fn drain(self: ApplyQueue) !void {
        return try self.vtable.drain(self.ptr);
    }
};
