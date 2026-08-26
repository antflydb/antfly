// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0.

//! Transport-neutral metadata administration mutation operations.

const std = @import("std");
const operation = @import("../api/operation.zig");
const metadata_api = @import("api.zig");
const metadata_table_manager = @import("table_manager.zig");

pub const Source = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        validate_publication: *const fn (*anyopaque, metadata_api.CatalogPublicationContract) anyerror!bool,
        validate_table_publication: *const fn (*anyopaque, metadata_api.CatalogTablePublicationContract) anyerror!bool,
        trigger_reallocate: *const fn (*anyopaque) anyerror!void,
        upsert_schema_progress: *const fn (*anyopaque, std.mem.Allocator, metadata_table_manager.SchemaProgressRecord) anyerror!void,
        upsert_restore_progress: *const fn (*anyopaque, std.mem.Allocator, metadata_table_manager.RestoreProgressRecord) anyerror!void,
        remove_restore_progress: *const fn (*anyopaque, u64, u64, u64) anyerror!void,
    };
};

pub const Operations = struct {
    source: Source,

    pub fn validatePublication(
        self: Operations,
        request: operation.RequestContext,
        contract: metadata_api.CatalogPublicationContract,
    ) !bool {
        try request.ensureActive();
        return self.source.vtable.validate_publication(self.source.ptr, contract);
    }

    pub fn validateTablePublication(
        self: Operations,
        request: operation.RequestContext,
        contract: metadata_api.CatalogTablePublicationContract,
    ) !bool {
        try request.ensureActive();
        return self.source.vtable.validate_table_publication(self.source.ptr, contract);
    }

    pub fn triggerReallocate(self: Operations, request: operation.RequestContext) !void {
        try request.ensureActive();
        try self.source.vtable.trigger_reallocate(self.source.ptr);
    }

    pub fn upsertSchemaProgress(
        self: Operations,
        alloc: std.mem.Allocator,
        request: operation.RequestContext,
        record: metadata_table_manager.SchemaProgressRecord,
    ) !void {
        try request.ensureActive();
        try self.source.vtable.upsert_schema_progress(self.source.ptr, alloc, record);
    }

    pub fn upsertRestoreProgress(
        self: Operations,
        alloc: std.mem.Allocator,
        request: operation.RequestContext,
        record: metadata_table_manager.RestoreProgressRecord,
    ) !void {
        try request.ensureActive();
        try metadata_table_manager.validateRestoreProgressRecord(record);
        try self.source.vtable.upsert_restore_progress(self.source.ptr, alloc, record);
    }

    pub fn removeRestoreProgress(
        self: Operations,
        request: operation.RequestContext,
        table_id: u64,
        node_id: u64,
        group_id: u64,
    ) !void {
        try request.ensureActive();
        try self.source.vtable.remove_restore_progress(self.source.ptr, table_id, node_id, group_id);
    }
};

test "metadata admin mutations reject canceled or invalid restore progress before reaching their source" {
    const FakeSource = struct {
        calls: usize = 0,

        fn validatePublication(ptr: *anyopaque, _: metadata_api.CatalogPublicationContract) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            return true;
        }

        fn validateTablePublication(ptr: *anyopaque, _: metadata_api.CatalogTablePublicationContract) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            return true;
        }

        fn triggerReallocate(ptr: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
        }

        fn upsertSchemaProgress(ptr: *anyopaque, _: std.mem.Allocator, _: metadata_table_manager.SchemaProgressRecord) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
        }

        fn upsertRestoreProgress(ptr: *anyopaque, _: std.mem.Allocator, _: metadata_table_manager.RestoreProgressRecord) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
        }

        fn removeRestoreProgress(ptr: *anyopaque, _: u64, _: u64, _: u64) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
        }
    };

    var source = FakeSource{};
    const operations = Operations{ .source = .{
        .ptr = &source,
        .vtable = &.{
            .validate_publication = FakeSource.validatePublication,
            .validate_table_publication = FakeSource.validateTablePublication,
            .trigger_reallocate = FakeSource.triggerReallocate,
            .upsert_schema_progress = FakeSource.upsertSchemaProgress,
            .upsert_restore_progress = FakeSource.upsertRestoreProgress,
            .remove_restore_progress = FakeSource.removeRestoreProgress,
        },
    } };
    var canceled = std.atomic.Value(bool).init(true);
    try std.testing.expectError(error.Canceled, operations.triggerReallocate(.{
        .cancellation = operation.CancellationToken.fromAtomic(&canceled),
    }));
    try std.testing.expectError(error.Canceled, operations.removeRestoreProgress(.{
        .cancellation = operation.CancellationToken.fromAtomic(&canceled),
    }, 1, 2, 3));
    try std.testing.expectError(
        error.InvalidRestoreProgressRecord,
        operations.upsertRestoreProgress(std.testing.allocator, .{}, .{
            .table_id = 0,
            .node_id = 2,
            .group_id = 3,
            .backup_id = "backup-3",
        }),
    );
    try std.testing.expectEqual(@as(usize, 0), source.calls);
}
