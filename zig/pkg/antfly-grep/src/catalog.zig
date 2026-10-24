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

//! Portable source catalogs. Authentication belongs to the selected adapter;
//! catalog reader identities and filters are never an authorization boundary.
const std = @import("std");
const A = std.mem.Allocator;
const indexed = @import("indexed.zig");
pub const Source = struct {
    id: []const u8,
    alias: ?[]const u8 = null,
    table: []const u8,
    document_field: []const u8,
    filter: std.json.Value,
    access_id: ?[]const u8 = null,
    status: []const u8,
    valid_until: ?i64 = null,
};
pub const Catalog = struct {
    version: u32,
    endpoint: []const u8,
    sources: []const Source,
    principals: []const []const u8 = &.{},
};
pub fn table(source: Source) !indexed.Table {
    if (source.filter != .object or !std.mem.eql(u8, source.document_field, "grep")) return error.InvalidSourceCatalog;
    if (source.id.len == 0 or source.id.len > 512 or std.mem.indexOfAny(u8, source.id, "\x00\r\n") != null or source.table.len == 0 or std.mem.indexOfAny(u8, source.table, "\x00\r\n") != null or std.mem.eql(u8, source.table, ".") or std.mem.eql(u8, source.table, "..")) return error.InvalidSourceCatalog;
    if (source.access_id) |id| {
        if (id.len == 0 or id.len > 128) return error.InvalidSourceCatalog;
        for (id) |char| if (!std.ascii.isAlphanumeric(char) and char != '_' and char != '-') return error.InvalidSourceCatalog;
    }
    if (!std.mem.eql(u8, source.status, "searchable") and !std.mem.eql(u8, source.status, "pending") and !std.mem.eql(u8, source.status, "syncing") and !std.mem.eql(u8, source.status, "stale")) return error.InvalidSourceCatalog;
    return .{ .name = source.id, .alias = source.alias, .table = source.table, .source_id = source.id, .document_field = source.document_field, .source_filter = source.filter, .access_id = source.access_id, .status = source.status, .valid_until = source.valid_until };
}
pub fn load(a: A, io: std.Io, path: []const u8) ![]const u8 {
    const bytes = try std.Io.Dir.cwd().readFileAlloc(io, path, a, .limited(32 * 1024 * 1024));
    defer a.free(bytes);
    var memory: @import("antfly_regex").Budget = .{ .backing = a, .limit = 64 * 1024 * 1024 };
    var arena = std.heap.ArenaAllocator.init(memory.allocator());
    defer arena.deinit();
    const state = arena.allocator();
    const catalog = try std.json.parseFromSliceLeaky(Catalog, state, bytes, .{});
    if (catalog.version != 2 or catalog.sources.len == 0 or catalog.sources.len > 1000 or catalog.principals.len > 262144) return error.InvalidSourceCatalog;
    try @import("providers.zig").validateCustom(catalog.endpoint);
    var tables: std.ArrayList(indexed.Table) = .empty;
    for (catalog.sources) |source| try tables.append(state, try table(source));
    return std.json.Stringify.valueAlloc(a, .{ .version = @as(u32, 1), .endpoint = catalog.endpoint, .tables = tables.items, .principals = catalog.principals }, .{});
}
