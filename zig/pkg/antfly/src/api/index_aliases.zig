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
const local = @import("antfly_local_sources");
const catalog = local.api_index_metadata.aliases;
const metadata = @import("../metadata/table_manager.zig");
const A = std.mem.Allocator;

pub fn execute(server: anytype, a: A, table: []const u8, name: []const u8, body: ?[]const u8, context: local.api_operation.RequestContext) ![]u8 {
    try context.ensureActive();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const before = (try server.loadOwnedTableRecord(scratch, table)) orelse return error.TableNotFound;
    const root = try std.json.parseFromSliceLeaky(std.json.Value, scratch, before.indexes_json, .{});
    if (body == null) {
        const aliases = root.object.get(catalog.key) orelse return error.IndexAliasNotFound;
        const resource = aliases.object.get(name) orelse return error.IndexAliasNotFound;
        return std.json.Stringify.valueAlloc(a, resource, .{});
    }
    const request = std.json.parseFromSliceLeaky(catalog.Request, scratch, body.?, .{ .allocate = .alloc_always }) catch return error.InvalidIndexAliasRequest;
    if (try catalog.replay(scratch, root, name, request)) |committed|
        return std.json.Stringify.valueAlloc(a, committed, .{});
    if (comptime @hasDecl(@TypeOf(server.*), "ensureIndexAliasAdmission")) try server.ensureIndexAliasAdmission(context);
    const relational = @import("relational_index_mutation.zig");
    if (try relational.contains(scratch, before.schema_json, name)) return error.IndexAliasNameConflict;
    if (try relational.contains(scratch, before.schema_json, request.index)) return error.UnsupportedOperation;
    var readiness: std.json.Value = .null;
    {
        const status_bytes = try server.indexAliasTargetResource(scratch, before, request.index, context);
        const status = try std.json.parseFromSliceLeaky(std.json.Value, scratch, status_bytes, .{});
        const aggregate = status.object.get("status") orelse return error.IndexAliasTargetNotReady;
        readiness = aggregate.object.get("readiness") orelse return error.IndexAliasTargetNotReady;
        // Runtime status was collected separately; reject if its catalog cut
        // changed instead of attaching a new generation's proof to an old one.
        const after = (try server.loadOwnedTableRecord(scratch, table)) orelse return error.TableGenerationChanged;
        if (!metadata.tableDefinitionsEqual(before, after)) return error.TableGenerationChanged;
    }
    const next = try catalog.put(scratch, before.indexes_json, name, request, readiness, @import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms);
    const updated = try std.json.parseFromSliceLeaky(std.json.Value, scratch, next, .{});
    // Allocate the success observation before entering the commit boundary.
    const response = try std.json.Stringify.valueAlloc(a, updated.object.get(catalog.key).?.object.get(name).?, .{});
    errdefer a.free(response);
    {
        var replacement = before;
        replacement.indexes_json = next;
        try context.ensureActive();
        try server.source.replaceTableDefinition(before, replacement);
    }
    return response;
}

test "index alias commits one CAS and reconciles a retry after rollback" {
    const Fake = struct {
        const Self = @This();
        const Source = struct {
            owner: *Self,
            pub fn replaceTableDefinition(self: @This(), expected: metadata.TableRecord, replacement: metadata.TableRecord) !void {
                if (!metadata.tableDefinitionsEqual(self.owner.table, expected)) return error.TableGenerationChanged;
                self.owner.table = try metadata.cloneTable(self.owner.a, replacement);
                self.owner.commits += 1;
            }
        };
        a: A,
        source: Source = undefined,
        table: metadata.TableRecord = .{ .table_id = 1, .name = "docs", .indexes_json = "{\"a\":{\"type\":\"embeddings\"},\"b\":{\"type\":\"embeddings\"}}" },
        commits: usize = 0,
        ready: bool = true,
        race: bool = false,
        pub fn loadOwnedTableRecord(self: *Self, a: A, _: []const u8) !?metadata.TableRecord {
            return try metadata.cloneTable(a, self.table);
        }
        pub fn indexAliasTargetResource(self: *Self, a: A, _: metadata.TableRecord, _: []const u8, _: local.api_operation.RequestContext) ![]u8 {
            if (self.race) self.table.description = "changed during readiness collection";
            return std.json.Stringify.valueAlloc(a, .{ .status = .{ .readiness = .{ .queryable = self.ready, .complete = self.ready } } }, .{});
        }
    };
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var fake: Fake = .{ .a = a };
    fake.source = .{ .owner = &fake };
    const create = "{\"index\":\"a\",\"idempotency_key\":\"1\"}";
    const swap = "{\"index\":\"b\",\"expected_index\":\"a\",\"idempotency_key\":\"2\"}";
    _ = try execute(&fake, a, "docs", "live", create, .{});
    const result = try execute(&fake, a, "docs", "live", swap, .{});
    _ = try execute(&fake, a, "docs", "live", "{\"index\":\"a\",\"expected_index\":\"b\",\"idempotency_key\":\"3\"}", .{});
    fake.ready = false;
    const retried = try execute(&fake, a, "docs", "live", swap, .{});
    try std.testing.expectEqualStrings(result, retried);
    try std.testing.expectEqual(@as(usize, 3), fake.commits);
    try std.testing.expectError(error.IndexAliasTargetNotReady, execute(&fake, a, "docs", "live", "{\"index\":\"b\",\"expected_index\":\"a\",\"idempotency_key\":\"4\"}", .{}));
    fake.ready = true;
    fake.race = true;
    try std.testing.expectError(error.TableGenerationChanged, execute(&fake, a, "docs", "live", "{\"index\":\"b\",\"expected_index\":\"a\",\"idempotency_key\":\"4\"}", .{}));
    try std.testing.expectEqual(@as(usize, 3), fake.commits);
}

test "index alias query pins routing before embedding across a concurrent swap" {
    const query_api = local.api_query;
    const types = local.storage_db_types;
    const Resolver = struct {
        current: []const u8,
        next: []const u8,
        pub fn bind(ptr: *anyopaque, a: A, _: []const u8, body: []const u8) !?[]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            const scratch = arena.allocator();
            const root = try std.json.parseFromSliceLeaky(std.json.Value, scratch, self.current, .{});
            var query = try std.json.parseFromSliceLeaky(std.json.Value, scratch, body, .{});
            try catalog.bindQuery(scratch, root, &query);
            return try std.json.Stringify.valueAlloc(a, query, .{});
        }
        pub fn embed(ptr: *anyopaque, a: A, _: []const u8, index: []const u8, _: []const u8, _: ?[]const u8, k: u32) !types.DenseKnnQuery {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            // Model A's computation completes after the live alias moved to B.
            self.current = self.next;
            const vector = try a.dupe(f32, if (std.mem.eql(u8, index, "a")) &.{ 1, 0 } else &.{ 0, 1 });
            return .{ .vector = vector, .k = k };
        }
    };
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const base = "{\"a\":{\"type\":\"embeddings\"},\"b\":{\"type\":\"embeddings\"}}";
    const ready = try std.json.parseFromSliceLeaky(std.json.Value, scratch, "{\"complete\":true}", .{});
    const first = try catalog.put(scratch, base, "live", .{ .index = "a", .idempotency_key = "1" }, ready, 1);
    const second = try catalog.put(scratch, first, "live", .{ .index = "b", .expected_index = "a", .idempotency_key = "2" }, ready, 2);
    var resolver: Resolver = .{ .current = first, .next = second };
    const iface: query_api.SemanticResolver = .{ .ptr = &resolver, .vtable = &.{ .bind_index_aliases = Resolver.bind, .resolve_dense_query = Resolver.embed } };
    var old = try query_api.parsePublicQueryRequest(a, iface, "docs", "{\"indexes\":[\"live\"],\"semantic_search\":\"hello\"}");
    defer old.deinit(a);
    var new = try query_api.parsePublicQueryRequest(a, iface, "docs", "{\"indexes\":[\"live\"],\"semantic_search\":\"hello\"}");
    defer new.deinit(a);
    try std.testing.expectEqualStrings("a", old.req.dense_queries[0].index_name);
    try std.testing.expectEqualStrings("b", new.req.dense_queries[0].index_name);
    try std.testing.expectEqual(@as(f32, 1), old.req.dense_queries[0].query.vector[0]);
    try std.testing.expectEqual(@as(f32, 1), new.req.dense_queries[0].query.vector[1]);
    var response = try query_api.encodeQueryResponses(a, "docs", old.req, .{}, .{ .alloc = a, .hits = &.{}, .total_hits = 0 });
    defer response.deinit(a);
    var parsed = try std.json.parseFromSlice(std.json.Value, a, response.json, .{});
    defer parsed.deinit();
    const names = parsed.value.object.get("responses").?.array.items[0].object.get("resolved_indexes").?;
    try std.testing.expectEqualStrings("a", names.array.items[0].string);
}

test "index alias state and retry receipts survive table wire and backup restore" {
    const backups = @import("backups.zig");
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const ready = try std.json.parseFromSliceLeaky(std.json.Value, scratch, "{\"complete\":true}", .{});
    const bytes = try catalog.put(scratch, "{\"text\":{\"type\":\"full_text\"}}", "live", .{ .index = "text", .idempotency_key = "1" }, ready, 1);
    const table: metadata.TableRecord = .{ .table_id = 1, .name = "docs", .indexes_json = bytes };
    const wire = try std.json.Stringify.valueAlloc(scratch, table, .{});
    const decoded = try std.json.parseFromSliceLeaky(metadata.TableRecord, scratch, wire, .{});
    try std.testing.expectEqualStrings(bytes, decoded.indexes_json);
    var manifest = try backups.createManifest(a, "daily", .portable, &table, &.{.{ .group_id = 11, .start_key = "", .snapshot_path = "docs.afb", .artifact_size_bytes = 1, .artifact_sha256 = "0000000000000000000000000000000000000000000000000000000000000000" }});
    defer manifest.deinit(a);
    const restored = try backups.deriveRestoreTableRecord(a, "restored", "file:///backup", &manifest);
    defer metadata.freeTable(a, restored);
    const root = try std.json.parseFromSliceLeaky(std.json.Value, scratch, restored.indexes_json, .{});
    try std.testing.expectEqualStrings("text", try catalog.resolve(root, "live"));
    const replayed = (try catalog.replay(scratch, root, "live", .{ .index = "text", .idempotency_key = "1" })).?;
    try std.testing.expectEqual(@as(u64, 1), replayed.swapped_at_ms);
}
