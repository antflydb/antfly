// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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
    /// Present when content_type is dynamically owned rather than static.
    owned_content_type: ?[]u8 = null,
    public_cors: bool = false,
    headers: []Header = &.{},

    pub fn clone(self: @This(), alloc: std.mem.Allocator) !@This() {
        const headers = try alloc.alloc(Header, self.headers.len);
        errdefer alloc.free(headers);
        var initialized: usize = 0;
        errdefer for (headers[0..initialized]) |*header| header.deinit(alloc);
        for (self.headers, headers) |source, *target| {
            const name = try alloc.dupe(u8, source.name);
            errdefer alloc.free(name);
            target.* = .{ .name = name, .value = try alloc.dupe(u8, source.value) };
            initialized += 1;
        }
        const owned_type = if (self.owned_content_type != null) try alloc.dupe(u8, self.content_type) else null;
        errdefer if (owned_type) |value| alloc.free(value);
        return .{
            .status = self.status,
            .content_type = owned_type orelse self.content_type,
            .owned_content_type = owned_type,
            .body = try alloc.dupe(u8, self.body),
            .public_cors = self.public_cors,
            .headers = headers,
        };
    }
    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        for (self.headers) |*header| header.deinit(alloc);
        if (self.headers.len > 0) alloc.free(self.headers);
        alloc.free(self.body);
        if (self.owned_content_type) |value| alloc.free(value);
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

pub fn jsonWithStatus(status: u16, body: []u8, public_cors: bool) OwnedResponse {
    var response = json(body, public_cors);
    response.status = status;
    return response;
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

pub fn jsonErrorAlloc(alloc: std.mem.Allocator, status: u16, message: []const u8) !OwnedResponse {
    return .{
        .status = status,
        .content_type = "application/json",
        .body = try std.fmt.allocPrint(alloc, "{{\"error\":{f}}}", .{std.json.fmt(message, .{})}),
    };
}

test "owned contextual response releases its body" {
    const alloc = std.testing.allocator;
    var response = json(try alloc.dupe(u8, "{}"), false);
    response.deinit(alloc);
}

test "retained owner native controls contextual response cloning retains dynamic content type" {
    const a = std.testing.allocator;
    const content_type = try a.dupe(u8, "application/problem+json");
    var original: OwnedResponse = .{ .content_type = content_type, .owned_content_type = content_type, .body = try a.dupe(u8, "{}") };
    var copy = try original.clone(a);
    defer copy.deinit(a);
    original.deinit(a);
    try std.testing.expectEqualStrings("application/problem+json", copy.content_type);
    try std.testing.expectEqualStrings("{}", copy.body);
}
