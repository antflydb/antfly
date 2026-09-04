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

//! Control-facing two-phase snapshot publication contract. Physical staging
//! and namespace exchange stay behind an opaque compiled-kernel handle while
//! catalog validation and generation admission remain in distributed control.

const descriptor_contract = @import("../storage/kernel_owner_descriptor.zig");
const backup_contract = @import("backup_contract.zig");

pub const PrepareRequest = struct {
    path: []const u8,
    table_name: []const u8,
    group_id: u64,
    lsm_root_generation: u64,
    identity: descriptor_contract.Identity,
    schema_json: []const u8,
    indexes_json: []const u8,
    encoded_snapshot: []const u8,
};

pub const RestoreRequest = struct {
    path: []const u8,
    table_name: []const u8,
    group_id: u64,
    lsm_root_generation: u64,
    identity: ?descriptor_contract.Identity,
    backup_root: []const u8,
    artifact_backup_id: []const u8,
    source_identity: []const u8,
    manifest: *const backup_contract.TableBackupManifest,
    shard: *const backup_contract.ShardSnapshot,
};

pub const Source = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        retire_group_for_publication: *const fn (
            ptr: *anyopaque,
            group_id: u64,
            table_name: []const u8,
        ) anyerror!void,
        prepare: *const fn (
            ptr: *anyopaque,
            request: PrepareRequest,
        ) anyerror!*anyopaque,
        prepare_restore: *const fn (
            ptr: *anyopaque,
            request: RestoreRequest,
        ) anyerror!RestorePreparation,
        reconcile_restore: *const fn (
            ptr: *anyopaque,
            request: RestoreRequest,
        ) anyerror!void,
        repair_published_restore: *const fn (
            ptr: *anyopaque,
            request: RestoreRequest,
        ) anyerror!void,
        promote: *const fn (ptr: *anyopaque, snapshot: *anyopaque) anyerror!void,
        publish_prepared: *const fn (
            ptr: *anyopaque,
            snapshot: *anyopaque,
        ) anyerror!bool,
        commit: *const fn (ptr: *anyopaque, snapshot: *anyopaque) anyerror!void,
        rollback: *const fn (ptr: *anyopaque, snapshot: *anyopaque) anyerror!void,
        destroy: *const fn (ptr: *anyopaque, snapshot: *anyopaque) void,
    };

    pub fn retireGroupForPublication(self: Source, group_id: u64, table_name: []const u8) !void {
        return try self.vtable.retire_group_for_publication(self.ptr, group_id, table_name);
    }

    pub fn prepare(self: Source, request: PrepareRequest) !Prepared {
        return .{
            .source = self,
            .handle = try self.vtable.prepare(self.ptr, request),
        };
    }

    pub fn prepareRestore(self: Source, request: RestoreRequest) !RestorePreparation {
        return try self.vtable.prepare_restore(self.ptr, request);
    }

    pub fn reconcileRestore(self: Source, request: RestoreRequest) !void {
        try self.vtable.reconcile_restore(self.ptr, request);
    }

    pub fn repairPublishedRestore(self: Source, request: RestoreRequest) !void {
        try self.vtable.repair_published_restore(self.ptr, request);
    }
};

pub const RestorePreparation = union(enum) {
    prepared: Prepared,
    already_imported,
};

pub const Prepared = struct {
    source: Source,
    handle: *anyopaque,
    active: bool = true,

    pub fn promote(self: *Prepared) !void {
        try self.source.vtable.promote(self.source.ptr, self.handle);
    }

    pub fn publishPrepared(self: *Prepared) !bool {
        return try self.source.vtable.publish_prepared(self.source.ptr, self.handle);
    }

    pub fn commit(self: *Prepared) !void {
        return try self.source.vtable.commit(self.source.ptr, self.handle);
    }

    pub fn rollback(self: *Prepared) !void {
        return try self.source.vtable.rollback(self.source.ptr, self.handle);
    }

    pub fn deinit(self: *Prepared) void {
        if (!self.active) return;
        self.source.vtable.destroy(self.source.ptr, self.handle);
        self.active = false;
    }
};
