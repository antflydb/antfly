// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0.

//! Transport-neutral result contracts for contextual protocol operations.

const std = @import("std");

pub const Method = enum {
    get,
    post,
    put,
    delete,
};

pub const McpApplicationOperation = union(enum) {
    list_tables,
    create_table: TableBody,
    drop_table: Table,
    describe_table: Table,
    list_indexes: Table,
    create_index: TableIndexBody,
    drop_index: TableIndex,
    get_document: DocumentRead,
    sample_documents: TableBody,
    query: TableBody,
    backup: TableBody,
    restore: TableBody,
    batch: TableBody,

    pub const Table = struct { table_name: []const u8 };
    pub const TableBody = struct { table_name: []const u8, body: []const u8 };
    pub const TableIndex = struct { table_name: []const u8, index_name: []const u8 };
    pub const TableIndexBody = struct { table_name: []const u8, index_name: []const u8, body: []const u8 };
    pub const DocumentRead = struct { table_name: []const u8, key: []const u8, fields: ?[]const u8 = null };
};

pub const Header = struct {
    name: []u8,
    value: []u8,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.name);
        alloc.free(self.value);
        self.* = undefined;
    }
};

pub const OwnedResponse = struct {
    status: u16 = 200,
    content_type: []const u8,
    body: []u8,
    public_cors: bool = false,
    headers: []Header = &.{},

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        for (self.headers) |*header| header.deinit(alloc);
        if (self.headers.len > 0) alloc.free(self.headers);
        alloc.free(self.body);
        self.* = undefined;
    }
};

pub fn json(body: []u8, public_cors: bool) OwnedResponse {
    return .{
        .content_type = "application/json",
        .body = body,
        .public_cors = public_cors,
    };
}

pub fn bytes(content_type: []const u8, body: []u8) OwnedResponse {
    return .{
        .content_type = content_type,
        .body = body,
    };
}

pub fn textAlloc(alloc: std.mem.Allocator, status: u16, body: []const u8) !OwnedResponse {
    return .{
        .status = status,
        .content_type = "text/plain",
        .body = try alloc.dupe(u8, body),
    };
}

test "owned contextual response releases its body" {
    const alloc = std.testing.allocator;
    var response = json(try alloc.dupe(u8, "{}"), false);
    response.deinit(alloc);
}
