// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0.

//! Transport-neutral metadata table lifecycle operations.

const std = @import("std");
const operation = @import("../api/operation.zig");
const tables_api = @import("../api/tables.zig");
const table_manager = @import("table_manager.zig");

pub const Source = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        create_table: *const fn (*anyopaque, std.mem.Allocator, []const u8, tables_api.CreateTableRequest) anyerror!void,
        replace_definition: *const fn (*anyopaque, table_manager.TableRecord, table_manager.TableRecord) anyerror!void,
        drop_table: *const fn (*anyopaque, std.mem.Allocator, []const u8) anyerror!void,
        update_schema: *const fn (*anyopaque, std.mem.Allocator, []const u8, []const u8) anyerror!void,
        create_index: *const fn (*anyopaque, std.mem.Allocator, []const u8, []const u8, []const u8) anyerror!void,
        drop_index: *const fn (*anyopaque, std.mem.Allocator, []const u8, []const u8) anyerror!void,
        put_enrichment: *const fn (*anyopaque, std.mem.Allocator, []const u8, []const u8, []const u8) anyerror!void,
        delete_enrichment: *const fn (*anyopaque, std.mem.Allocator, []const u8, []const u8) anyerror!void,
    };
};

pub const Operations = struct {
    source: Source,

    pub fn create(self: Operations, alloc: std.mem.Allocator, ctx: operation.RequestContext, table_name: []const u8, request: tables_api.CreateTableRequest) !void {
        try validateNameAndContext(ctx, table_name);
        try self.source.vtable.create_table(self.source.ptr, alloc, table_name, request);
    }

    pub fn replaceDefinition(self: Operations, ctx: operation.RequestContext, table_name: []const u8, expected: table_manager.TableRecord, replacement: table_manager.TableRecord) !void {
        try validateNameAndContext(ctx, table_name);
        if (!std.mem.eql(u8, expected.name, table_name)) return error.ExpectedTableNameMismatch;
        if (!std.mem.eql(u8, replacement.name, table_name)) return error.TableNameMismatch;
        try self.source.vtable.replace_definition(self.source.ptr, expected, replacement);
    }

    pub fn drop(self: Operations, alloc: std.mem.Allocator, ctx: operation.RequestContext, table_name: []const u8) !void {
        try validateNameAndContext(ctx, table_name);
        try self.source.vtable.drop_table(self.source.ptr, alloc, table_name);
    }

    pub fn updateSchema(self: Operations, alloc: std.mem.Allocator, ctx: operation.RequestContext, table_name: []const u8, schema_json: []const u8) !void {
        try validateNameAndContext(ctx, table_name);
        try self.source.vtable.update_schema(self.source.ptr, alloc, table_name, schema_json);
    }

    pub fn createIndex(self: Operations, alloc: std.mem.Allocator, ctx: operation.RequestContext, table_name: []const u8, index_name: []const u8, index_json: []const u8) !void {
        try validateNameAndContext(ctx, table_name);
        if (index_name.len == 0) return error.InvalidArgument;
        try self.source.vtable.create_index(self.source.ptr, alloc, table_name, index_name, index_json);
    }

    pub fn dropIndex(self: Operations, alloc: std.mem.Allocator, ctx: operation.RequestContext, table_name: []const u8, index_name: []const u8) !void {
        try validateNameAndContext(ctx, table_name);
        if (index_name.len == 0) return error.InvalidArgument;
        try self.source.vtable.drop_index(self.source.ptr, alloc, table_name, index_name);
    }

    pub fn putEnrichment(self: Operations, alloc: std.mem.Allocator, ctx: operation.RequestContext, table_name: []const u8, enrichment_name: []const u8, enrichment_json: []const u8) !void {
        try validateNameAndContext(ctx, table_name);
        if (enrichment_name.len == 0) return error.InvalidArgument;
        try self.source.vtable.put_enrichment(self.source.ptr, alloc, table_name, enrichment_name, enrichment_json);
    }

    pub fn deleteEnrichment(self: Operations, alloc: std.mem.Allocator, ctx: operation.RequestContext, table_name: []const u8, enrichment_name: []const u8) !void {
        try validateNameAndContext(ctx, table_name);
        if (enrichment_name.len == 0) return error.InvalidArgument;
        try self.source.vtable.delete_enrichment(self.source.ptr, alloc, table_name, enrichment_name);
    }
};

fn validateNameAndContext(ctx: operation.RequestContext, name: []const u8) !void {
    try ctx.ensureActive();
    if (name.len == 0) return error.InvalidArgument;
}

test "metadata table operations enforce cancellation before source calls" {
    const Fake = struct {
        calls: usize = 0,
        fn unsupportedCreate(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: tables_api.CreateTableRequest) !void {
            return error.UnsupportedOperation;
        }
        fn unsupportedReplace(_: *anyopaque, _: table_manager.TableRecord, _: table_manager.TableRecord) !void {
            return error.UnsupportedOperation;
        }
        fn drop(ptr: *anyopaque, _: std.mem.Allocator, _: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
        }
        fn unsupportedSchema(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8) !void {
            return error.UnsupportedOperation;
        }
        fn unsupportedIndex(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: []const u8) !void {
            return error.UnsupportedOperation;
        }
        fn unsupportedDropIndex(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8) !void {
            return error.UnsupportedOperation;
        }
    };
    var source = Fake{};
    const ops = Operations{ .source = .{ .ptr = &source, .vtable = &.{
        .create_table = Fake.unsupportedCreate,
        .replace_definition = Fake.unsupportedReplace,
        .drop_table = Fake.drop,
        .update_schema = Fake.unsupportedSchema,
        .create_index = Fake.unsupportedIndex,
        .drop_index = Fake.unsupportedDropIndex,
        .put_enrichment = Fake.unsupportedIndex,
        .delete_enrichment = Fake.unsupportedDropIndex,
    } } };
    var canceled = std.atomic.Value(bool).init(true);
    try std.testing.expectError(error.Canceled, ops.drop(std.testing.allocator, .{
        .cancellation = operation.CancellationToken.fromAtomic(&canceled),
    }, "docs"));
    try std.testing.expectEqual(@as(usize, 0), source.calls);
}
