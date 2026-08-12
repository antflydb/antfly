// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0.

//! Transport-neutral metadata administration mutation operations.

const operation = @import("../api/operation.zig");
const metadata_api = @import("api.zig");

pub const Source = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        validate_publication: *const fn (*anyopaque, metadata_api.CatalogPublicationContract) anyerror!bool,
        validate_table_publication: *const fn (*anyopaque, metadata_api.CatalogTablePublicationContract) anyerror!bool,
        trigger_reallocate: *const fn (*anyopaque) anyerror!void,
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
};

test "metadata admin mutations reject canceled work before reaching their source" {
    const std = @import("std");
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
    };

    var source = FakeSource{};
    const operations = Operations{ .source = .{
        .ptr = &source,
        .vtable = &.{
            .validate_publication = FakeSource.validatePublication,
            .validate_table_publication = FakeSource.validateTablePublication,
            .trigger_reallocate = FakeSource.triggerReallocate,
        },
    } };
    var canceled = std.atomic.Value(bool).init(true);
    try std.testing.expectError(error.Canceled, operations.triggerReallocate(.{
        .cancellation = operation.CancellationToken.fromAtomic(&canceled),
    }));
    try std.testing.expectEqual(@as(usize, 0), source.calls);
}
