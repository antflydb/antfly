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

/// Discover ordinary catalog-table rows through the caller's data-plane identity.
/// Server authentication/RLS controls which catalog and content rows are visible.
pub fn discover(a: A, io: std.Io, endpoint: []const u8, catalog_table: []const u8, credential: ?[]const u8, principals: []const []const u8, timeout_ms: u64) ![]const u8 {
    try @import("providers.zig").validateCustom(endpoint);
    if (catalog_table.len == 0 or std.mem.eql(u8, catalog_table, ".") or std.mem.eql(u8, catalog_table, "..")) return error.InvalidSourceCatalog;
    const httpx = @import("httpx");
    var memory: @import("antfly_regex").Budget = .{ .backing = a, .limit = 64 * 1024 * 1024 };
    var arena = std.heap.ArenaAllocator.init(memory.allocator());
    defer arena.deinit();
    const state = arena.allocator();
    const escaped = try indexed.escape(state, catalog_table);
    const url = try std.fmt.allocPrint(state, "{s}/tables/{s}/query", .{ std.mem.trimEnd(u8, endpoint, "/"), escaped });
    const auth = if (credential) |token| try std.fmt.allocPrint(state, "Bearer {s}", .{token}) else null;
    var headers: std.ArrayList([2][]const u8) = .empty;
    try headers.append(state, .{ "Content-Type", "application/json" });
    if (auth) |value| try headers.append(state, .{ "Authorization", value });
    var http = httpx.Client.init(a, io);
    defer http.deinit();
    var sources: std.ArrayList(Source) = .empty;
    var snapshot: ?[]const u8 = null;
    var previous: ?[]const u8 = null;
    var wire_bytes: usize = 0;
    for (0..1024) |_| {
        const body = try std.json.Stringify.valueAlloc(state, .{ .query = .{ .equals = "source_catalog", .field = "doc_type" }, .fields = [_][]const u8{ "version", "doc_type", "source", "document_version", "published_revision" }, .limit = @as(u32, 64), .order_by = [_]struct { field: []const u8, desc: bool }{.{ .field = "_id", .desc = false }}, .remote_snapshot = snapshot, .search_after = if (previous) |id| @as(?[1][]const u8, .{id}) else null }, .{ .emit_null_optional_fields = false });
        var response = try http.post(url, .{ .borrowed_body = body, .headers = headers.items, .timeout_ms = timeout_ms, .max_response_size = 1024 * 1024, .max_retries = 0, .follow_redirects = false, .cookies_enabled = false });
        defer response.deinit();
        if (!response.ok()) return error.SourceCatalogUnavailable;
        const bytes = response.body orelse return error.InvalidSourceCatalog;
        wire_bytes += bytes.len;
        if (wire_bytes > 32 * 1024 * 1024) return error.SourceCatalogBudgetExceeded;
        const Record = struct { version: u32, doc_type: []const u8, source: Source, document_version: u32, published_revision: []const u8 = "" };
        const Hit = struct { _id: []const u8, _source: Record };
        const Envelope = struct { responses: []const struct { status: u32, remote_snapshot: []const u8, @"error": ?std.json.Value = null, hits: struct { hits: []const Hit } } };
        const page = try std.json.parseFromSliceLeaky(Envelope, state, bytes, .{ .ignore_unknown_fields = true, .allocate = .alloc_always });
        if (page.responses.len != 1 or page.responses[0].status != 200 or page.responses[0].remote_snapshot.len == 0 or page.responses[0].@"error" != null) return error.InvalidSourceCatalog;
        const result = page.responses[0];
        if (snapshot) |pinned| {
            if (!std.mem.eql(u8, pinned, result.remote_snapshot)) return error.SourceCatalogChanged;
        } else snapshot = result.remote_snapshot;
        if (result.hits.hits.len > 64) return error.InvalidSourceCatalog;
        for (result.hits.hits) |hit| {
            if (hit._source.version != 1 or hit._source.document_version != 1 or !std.mem.eql(u8, hit._source.doc_type, "source_catalog") or !std.mem.eql(u8, hit._id, hit._source.source.id)) return error.InvalidSourceCatalog;
            if (previous) |id| if (std.mem.order(u8, id, hit._id) != .lt) return error.InvalidSourceCatalog;
            if (std.mem.eql(u8, hit._source.source.status, "searchable") and (hit._source.source.valid_until == null or hit._source.published_revision.len == 0)) return error.InvalidSourceCatalog;
            previous = hit._id;
            _ = try table(hit._source.source);
            try sources.append(state, hit._source.source);
            if (sources.items.len > 1000) return error.SourceCatalogBudgetExceeded;
        }
        if (result.hits.hits.len < 64) {
            if (sources.items.len == 0) return error.NoConnectedSources;
            var tables: std.ArrayList(indexed.Table) = .empty;
            for (sources.items) |source| try tables.append(state, try table(source));
            return std.json.Stringify.valueAlloc(a, .{ .version = @as(u32, 1), .endpoint = endpoint, .tables = tables.items, .principals = principals }, .{});
        }
    }
    return error.SourceCatalogBudgetExceeded;
}
