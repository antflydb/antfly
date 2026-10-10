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

const std = @import("std");
const Namespace = @import("doc_identity_namespace.zig").Namespace;
pub const ttl_ms: u64 = 300_000;
pub const max_ttl_ms: u64 = std.time.ms_per_hour;
/// Original committed key cover. Group IDs identify logical fanout slots;
/// namespaces identify immutable physical generations, independently of owners.
pub const Range = struct {
    group_id: u64,
    namespace: Namespace,
    /// Physical snapshot identity, independent of preserved document IDs.
    /// Legacy covers used the public request ID plus a unique namespace.
    generation_id: ?[]const u8 = null,
    start_key: []const u8,
    end_key: ?[]const u8 = null,
};
pub const Recipe = struct {
    schema_json: []const u8,
    read_schema_json: []const u8,
    indexes_json: []const u8,
};
pub fn validateCover(table_id: u64, cover: []const Range) !void {
    if (cover.len == 0 or cover.len > 4096 or cover[0].start_key.len != 0 or cover[cover.len - 1].end_key != null) return error.CatalogGenerationChanged;
    for (cover, 0..) |range, i| {
        if (range.group_id == 0 or range.namespace.table_id != table_id) return error.CatalogGenerationChanged;
        if (range.generation_id) |id| try validateId(id);
        for (cover[0..i]) |previous| {
            if (previous.group_id == range.group_id) return error.CatalogGenerationChanged;
            if (previous.generation_id != null and range.generation_id != null) {
                if (std.mem.eql(u8, previous.generation_id.?, range.generation_id.?)) return error.CatalogGenerationChanged;
            } else if (previous.namespace.eql(range.namespace)) return error.CatalogGenerationChanged;
        }
        if (range.end_key) |end| if (std.mem.order(u8, range.start_key, end) != .lt) return error.CatalogGenerationChanged;
        if (i != 0) {
            const end = cover[i - 1].end_key orelse return error.CatalogGenerationChanged;
            if (!std.mem.eql(u8, end, range.start_key)) return error.CatalogGenerationChanged;
        }
    }
}
fn validateId(id: []const u8) !void {
    if (id.len != 64) return error.InvalidQueryRequest;
    for (id) |byte| if (!((byte >= '0' and byte <= '9') or (byte >= 'a' and byte <= 'f'))) return error.InvalidQueryRequest;
}
/// Internal subrequests may carry a cut either at the top level or inside the
/// retrieval envelope. Do not recapture a resume cover on its current carrier.
pub fn bindBodyAlloc(a: std.mem.Allocator, body: []const u8, group_id: u64) !?[]u8 {
    var parsed = try std.json.parseFromSlice(std.json.Value, a, body, .{});
    defer parsed.deinit();
    if (!try bindValue(parsed.arena.allocator(), &parsed.value, group_id)) return null;
    return try std.json.Stringify.valueAlloc(a, parsed.value, .{});
}
fn bindValue(a: std.mem.Allocator, value: *std.json.Value, group_id: u64) !bool {
    if (value.* != .object) return false;
    if (value.object.getPtr("_native_cut")) |input| {
        const bytes = try std.json.Stringify.valueAlloc(a, input.*, .{});
        const cut = try std.json.parseFromSliceLeaky(Request, a, bytes, .{});
        if (!cut.create or cut.cover.len == 0) return false;
        const selected = try cut.forGroup(group_id);
        input.* = try std.json.parseFromSliceLeaky(std.json.Value, a, try std.json.Stringify.valueAlloc(a, selected, .{}), .{});
        return true;
    }
    if (value.object.getPtr("query_request")) |query| return bindValue(a, query, group_id);
    return false;
}
pub const Request = struct {
    id: []const u8,
    table_id: u64,
    expires_ms: u64,
    create: bool = false,
    /// Authenticated original generation identity. Execution may move to a
    /// replacement range, but physical document IDs remain in this namespace.
    origin: ?Namespace = null,
    timeout_ms: ?u64 = null,
    cover: []const Range = &.{},
    recipe: ?Recipe = null,
    /// Bind only at a catalog-fenced physical owner. A resume coordinator
    /// retains the whole cover; its virtual owners bind each original range.
    pub fn forGroup(self: Request, group_id: u64) !Request {
        if (self.cover.len == 0) return self;
        for (self.cover) |range| if (range.group_id == group_id) {
            var selected = self;
            selected.id = range.generation_id orelse self.id;
            selected.origin = range.namespace;
            selected.cover = &.{};
            selected.recipe = null;
            return selected;
        };
        return error.CatalogGenerationChanged;
    }
    pub fn namespace(self: Request, owner: Namespace) !Namespace {
        if (owner.table_id != self.table_id) return error.CatalogGenerationChanged;
        const original = self.origin orelse owner;
        if (original.table_id != self.table_id or (self.create and !original.eql(owner))) return error.CatalogGenerationChanged;
        if (self.create and self.cover.len != 0) {
            for (self.cover) |range| if (range.namespace.eql(owner)) return original;
            return error.CatalogGenerationChanged;
        }
        return original;
    }
    /// Each original generation gets its own virtual/local cache root when
    /// multiple pre-split ranges execute on the same replacement owner.
    pub fn cacheId(self: Request, a: std.mem.Allocator, owner: Namespace) ![]u8 {
        const original = try self.namespace(owner);
        if (original.eql(owner)) return a.dupe(u8, self.id);
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("native-query-origin-cache-v1");
        hash.update(self.id);
        var number: [8]u8 = undefined;
        for ([_]u64{ original.table_id, original.shard_id, original.range_id }) |value| {
            std.mem.writeInt(u64, &number, value, .big);
            hash.update(&number);
        }
        var digest: [32]u8 = undefined;
        hash.final(&digest);
        return a.dupe(u8, &std.fmt.bytesToHex(digest, .lower));
    }
    pub fn forDeadline(self: Request, deadline_ns: ?u64) Request {
        var request = self;
        if (deadline_ns) |deadline| request.timeout_ms = (deadline -| @import("antfly_platform").time.monotonicNs()) / std.time.ns_per_ms;
        return request;
    }
    pub fn validate(self: Request, now: u64) !void {
        if (self.id.len != 64 or self.table_id == 0) return error.InvalidQueryRequest;
        if (self.cover.len != 0) {
            try validateCover(self.table_id, self.cover);
            if (self.recipe == null) return error.CatalogGenerationChanged;
        }
        if (self.origin) |origin| if (origin.table_id != self.table_id) return error.CatalogGenerationChanged;
        try validateId(self.id);
        if (self.expires_ms <= now or self.expires_ms > now +| max_ttl_ms) return error.CatalogGenerationChanged;
    }
};

test "native query origins retain identities across repartition and isolate cache roots" {
    const a = std.testing.allocator;
    const original: Namespace = .{ .table_id = 7, .shard_id = 10, .range_id = 11 };
    const replacement: Namespace = .{ .table_id = 7, .shard_id = 20, .range_id = 21 };
    const id: [64]u8 = @splat('a');
    var request: Request = .{ .id = &id, .table_id = 7, .expires_ms = 1000, .origin = original };
    try request.validate(1);
    try std.testing.expect((try request.namespace(replacement)).eql(original));
    const moved = try request.cacheId(a, replacement);
    defer a.free(moved);
    const local = try request.cacheId(a, original);
    defer a.free(local);
    try std.testing.expectEqualStrings(&id, local);
    try std.testing.expect(!std.mem.eql(u8, moved, local));
    request.origin = .{ .table_id = 7, .shard_id = 30, .range_id = 31 };
    const other = try request.cacheId(a, replacement);
    defer a.free(other);
    try std.testing.expect(!std.mem.eql(u8, moved, other));
    request.create = true;
    try std.testing.expectError(error.CatalogGenerationChanged, request.namespace(replacement));
    request.create = false;
    request.origin = .{ .table_id = 8 };
    try std.testing.expectError(error.CatalogGenerationChanged, request.validate(1));
    try std.testing.expectError(error.CatalogGenerationChanged, request.namespace(replacement));
}
