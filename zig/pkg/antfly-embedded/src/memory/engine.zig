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

//! Engine adapters for the single native memory policy. Table callbacks route
//! through existing Raft/transaction ownership; embedded uses the native DB.
const std = @import("std");
pub const service = @import("service.zig");
const reads_mod = @import("../api/table_read_source.zig");
const writes_mod = @import("../api/table_write_source.zig");
const db_types = @import("../storage/db/types.zig");
const A = std.mem.Allocator;

// Memory policy rows may be objects, scalars, or arrays. Storage adapters own
// one versioned object envelope so routed NDJSON never flattens policy values.
fn encodeRow(a: A, json: []const u8) ![]const u8 {
    return std.fmt.allocPrint(a, "{{\"memory_record_version\":1,\"value\":{s}}}", .{json});
}
pub fn decodeRow(a: A, json: []const u8) ![]const u8 {
    var fields: [2]?[]const u8 = undefined;
    @import("antfly-json").objectFields(a, json, &.{ "memory_record_version", "value" }, &fields) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return error.MemoryCorruptState,
    };
    if (!std.mem.eql(u8, fields[0] orelse return error.MemoryCorruptState, "1")) return error.MemoryCorruptState;
    return fields[1] orelse error.MemoryCorruptState;
}

fn encodeChange(a: A, path: []const u8, json: []const u8) ![]const u8 {
    if (std.mem.startsWith(u8, path, "assertion-recall/")) {
        const projection = try std.json.parseFromSliceLeaky(service.types.RecallProjection, a, json, .{});
        var output: std.Io.Writer.Allocating = .init(a);
        try output.writer.print("{{\"memory_record_version\":1,\"value\":{s}", .{json});
        for (projection.text_indexes) |name| {
            try output.writer.writeByte(',');
            try std.json.Stringify.value(name, .{}, &output.writer);
            try output.writer.writeByte(':');
            try std.json.Stringify.value(try std.fmt.allocPrint(a, "{s}\n{s}", .{ projection.text, projection.filter_terms }), .{}, &output.writer);
        }
        if (projection.embedding) |vector| {
            try output.writer.writeAll(",\"_embeddings\":{");
            for (projection.vector_indexes, 0..) |name, i| {
                if (i != 0) try output.writer.writeByte(',');
                try std.json.Stringify.value(name, .{}, &output.writer);
                try output.writer.writeByte(':');
                try std.json.Stringify.value(vector, .{}, &output.writer);
            }
            try output.writer.writeByte('}');
        }
        try output.writer.writeByte('}');
        return output.toOwnedSlice();
    }
    if (!std.mem.startsWith(u8, path, "entity-semantic/")) return encodeRow(a, json);
    const projection = try std.json.parseFromSliceLeaky(service.types.EntityProjection, a, json, .{});
    const rest = path["entity-semantic/".len..];
    const slash = std.mem.indexOfScalar(u8, rest, '/') orelse return error.MemoryCorruptState;
    var embeddings: std.json.ObjectMap = .empty;
    try embeddings.put(a, rest[0..slash], try std.json.parseFromSliceLeaky(std.json.Value, a, try std.json.Stringify.valueAlloc(a, projection.embedding, .{}), .{}));
    return std.fmt.allocPrint(a, "{{\"memory_record_version\":1,\"value\":{s},\"_embeddings\":{s}}}", .{ json, try std.json.Stringify.valueAlloc(a, std.json.Value{ .object = embeddings }, .{}) });
}
fn vectorIndexConfig(a: A, dims: u32) ![]const u8 {
    return std.json.Stringify.valueAlloc(a, .{ .field = "embedding", .dims = dims, .metric = "cosine", .external = true }, .{});
}
fn hydrationRequest(keys: []const []const u8) !db_types.SearchRequest {
    if (keys.len == 0 or keys.len > 64) return error.InvalidMemoryRequest;
    return .{ .filter_doc_ids = keys, .filter_doc_ids_positive = true, .limit = @intCast(keys.len), .include_stored = true, .document_authority_mode = .lookup };
}
fn neighborRequest(input: service.NeighborRequest) !db_types.SearchRequest {
    if (input.limit == 0 or input.limit > 64 or !std.mem.startsWith(u8, input.index, "memory_")) return error.InvalidMemoryRequest;
    return .{ .index_name = input.index, .query = .{ .dense_knn = .{ .vector = input.vector, .k = input.limit } }, .limit = input.limit, .include_stored = false };
}

fn assertionSearchRequest(a: A, input: service.RecallSearchRequest) !db_types.SearchRequest {
    _ = try searchLanes(input);
    if (input.text_indexes.len > 1 or input.vector_indexes.len > 1) return error.InvalidMemoryRequest;
    var text: std.ArrayList(db_types.NamedFullTextQuery) = .empty;
    var dense: std.ArrayList(db_types.NamedDenseQuery) = .empty;
    for (input.text_indexes) |name| {
        if (!std.mem.startsWith(u8, name, "memory_recall_text_")) return error.InvalidMemoryRequest;
        if (input.query.len != 0) try text.append(a, .{ .name = "$full_text_results", .index_name = name, .query = .{ .match = .{ .field = name, .text = input.query } } });
    }
    if (input.embedding) |vector| for (input.vector_indexes) |name| {
        if (!std.mem.startsWith(u8, name, "memory_recall_vector_")) return error.InvalidMemoryRequest;
        try dense.append(a, .{ .name = name, .index_name = name, .query = .{ .vector = vector, .k = input.offset + input.limit } });
    };
    if (text.items.len + dense.items.len == 0) return error.InvalidMemoryRequest;
    if (!input.filters.empty() and input.text_indexes.len != 1) return error.InvalidMemoryRequest;
    const filter = if (input.filters.empty()) "" else try @import("recall_filters.zig").query(a, input.text_indexes[0], input.filters);
    return .{ .full_text_queries = text.items, .dense_queries = dense.items, .primary_text_index_name = if (filter.len > 0) input.text_indexes[0] else null, .filter_query_json = filter, .merge_config = .{ .window_size = input.offset + input.limit }, .limit = input.offset + input.limit, .include_stored = false };
}

fn searchLanes(input: service.RecallSearchRequest) !usize {
    if (input.limit == 0 or input.limit > 4096 or input.offset > 4096 - input.limit or input.text_indexes.len > 2 or input.vector_indexes.len > 2) return error.InvalidMemoryRequest;
    if (!input.filters.empty() and input.embedding != null and input.vector_indexes.len > input.text_indexes.len) return error.InvalidMemoryRequest;
    // Each audience is an independent bounded retrieval lane. The owner wire
    // admits one named full-text result per request; do not depend on native
    // multi-query support that is unavailable across a compiled-owner boundary.
    return @max(@as(usize, 1), @max(input.text_indexes.len, if (input.embedding != null) input.vector_indexes.len else 0));
}
fn laneRequest(input: service.RecallSearchRequest, lane: usize) service.RecallSearchRequest {
    var result = input;
    result.text_indexes = if (lane < input.text_indexes.len) input.text_indexes[lane..][0..1] else &.{};
    result.vector_indexes = if (input.embedding != null and lane < input.vector_indexes.len) input.vector_indexes[lane..][0..1] else &.{};
    return result;
}
fn searchPage(a: A, input: service.RecallSearchRequest, hits: []service.Neighbor, truncated: bool) !service.RecallSearchPage {
    std.mem.sort(service.Neighbor, hits, {}, struct {
        fn less(_: void, l: service.Neighbor, r: service.Neighbor) bool {
            return if (l.similarity == r.similarity) std.mem.lessThan(u8, l.key, r.key) else l.similarity > r.similarity;
        }
    }.less);
    const start = @min(input.offset, hits.len);
    const end = @min(start + input.limit, hits.len);
    const keys = try a.alloc([]const u8, end - start);
    for (hits[start..end], keys) |hit, *key| key.* = hit.key;
    return .{ .keys = keys, .more = truncated or end < hits.len };
}

pub const IndexCatalog = struct {
    ptr: *anyopaque,
    create: *const fn (*anyopaque, A, []const u8, []const u8, []const u8) anyerror!void,
    drop: *const fn (*anyopaque, A, []const u8, []const u8) anyerror!void,
};

pub const Routed = struct {
    table: []const u8,
    reads: reads_mod.TableReadSource,
    writes: writes_mod.TableWriteSource,
    /// Server-owned tables publish index DDL through their authoritative catalog.
    index_catalog: ?IndexCatalog = null,
    context: service.Context,
    resource_ptr: ?*anyopaque = null,
    resource_fn: ?*const fn (*anyopaque, A, service.types.provenance.Resource) anyerror!service.types.provenance.Snapshot = null,
    request: @import("../api/operation.zig").RequestContext = .{},

    pub fn backend(self: *Routed) service.Backend {
        return .{ .ptr = self, .random_bytes = if (self.request.fanout_io != null or self.request.deadline_io != null) randomBytes else null, .get = get, .get_many = getMany, .scan = scan, .commit = commit, .resource = if (self.resource_fn != null) resource else null, .neighbors = neighbors, .ensure_vector_index = ensureVectorIndex, .drop_index = dropIndex, .ensure_text_index = ensureTextIndex, .search_assertions = searchAssertions };
    }
    fn randomBytes(ptr: *anyopaque, bytes: []u8) !void {
        const self: *Routed = @ptrCast(@alignCast(ptr));
        const borrow = self.request.fanout_io orelse self.request.deadline_io orelse return error.MemoryCapabilityUnavailable;
        var receiver = try borrow.receive();
        @import("antfly_platform").entropy.fill(receiver.io(), bytes) catch return error.MemoryCapabilityUnavailable;
    }
    fn createIndex(self: *Routed, a: A, name: []const u8, json: []const u8) !void {
        if (self.index_catalog) |catalog| return catalog.create(catalog.ptr, a, self.table, name, json);
        try self.writes.createIndex(a, self.table, name, json) orelse return error.MemoryCapabilityUnavailable;
    }
    fn deleteIndex(self: *Routed, a: A, name: []const u8) !void {
        if (self.index_catalog) |catalog| return catalog.drop(catalog.ptr, a, self.table, name);
        try self.writes.dropIndex(a, self.table, name) orelse return error.MemoryCapabilityUnavailable;
    }
    fn dropIndex(ptr: *anyopaque, a: A, name: []const u8) !void {
        const self: *Routed = @ptrCast(@alignCast(ptr));
        try self.context.requireGrant(.admin);
        self.deleteIndex(a, name) catch |err| switch (err) {
            error.IndexNotFound => return,
            else => return err,
        };
    }
    fn ensureTextIndex(ptr: *anyopaque, a: A, name: []const u8) !void {
        const self: *Routed = @ptrCast(@alignCast(ptr));
        try self.context.requireGrant(.admin);
        self.createIndex(a, name, try std.json.Stringify.valueAlloc(a, .{ .type = "full_text", .field = name }, .{})) catch |err| switch (err) {
            error.IndexAlreadyExists => return,
            else => return err,
        };
    }
    fn searchAssertions(ptr: *anyopaque, a: A, input: service.RecallSearchRequest) !service.RecallSearchPage {
        const self: *Routed = @ptrCast(@alignCast(ptr));
        try self.context.check();
        var hits: std.ArrayList(service.Neighbor) = .empty;
        var truncated = false;
        for (0..try searchLanes(input)) |lane| {
            var request = try assertionSearchRequest(a, laneRequest(input, lane));
            request.execution_deadline_ns = self.request.deadline_ns;
            request.cancellation = self.request.cancellation;
            var response = (try self.reads.query(a, self.table, request, .read_index)) orelse return error.MemorySpaceNotFound;
            defer response.deinit(a);
            const Wire = struct { responses: []const struct { hits: struct { hits: []const struct { _id: []const u8, _score: f64 } } } };
            const value = try std.json.parseFromSliceLeaky(Wire, a, response.json, .{ .ignore_unknown_fields = true });
            if (value.responses.len != 1 or value.responses[0].hits.hits.len > request.limit) return error.MemoryCorruptState;
            const rows = value.responses[0].hits.hits;
            truncated = truncated or rows.len == request.limit;
            for (rows) |row| try hits.append(a, .{ .key = try a.dupe(u8, row._id), .similarity = row._score });
        }
        return searchPage(a, input, hits.items, truncated);
    }
    fn ensureVectorIndex(ptr: *anyopaque, a: A, name: []const u8, dims: u32) !void {
        const self: *Routed = @ptrCast(@alignCast(ptr));
        try self.context.requireGrant(.admin);
        self.createIndex(a, name, try std.json.Stringify.valueAlloc(a, .{ .type = "embeddings", .external = true, .dimension = dims, .distance_metric = "cosine" }, .{})) catch |err| switch (err) {
            error.IndexAlreadyExists => return,
            else => return err,
        };
    }
    fn neighbors(ptr: *anyopaque, a: A, input: service.NeighborRequest) ![]const service.Neighbor {
        const self: *Routed = @ptrCast(@alignCast(ptr));
        try self.context.check();
        var request = try neighborRequest(input);
        request.execution_deadline_ns = self.request.deadline_ns;
        request.cancellation = self.request.cancellation;
        var response = (try self.reads.query(a, self.table, request, .read_index)) orelse return error.MemoryCapabilityUnavailable;
        defer response.deinit(a);
        const Wire = struct { responses: []const struct { hits: struct { hits: []const struct { _id: []const u8, _score: f64 } } } };
        const value = try std.json.parseFromSliceLeaky(Wire, a, response.json, .{ .ignore_unknown_fields = true });
        if (value.responses.len != 1) return error.MemoryCorruptState;
        const hits = value.responses[0].hits.hits;
        if (hits.len > input.limit) return error.MemoryCorruptState;
        const result = try a.alloc(service.Neighbor, hits.len);
        // Parsed strings may borrow the response buffer released on return.
        for (hits, result) |hit, *out| out.* = .{ .key = try a.dupe(u8, hit._id), .similarity = hit._score };
        return result;
    }
    fn resource(ptr: *anyopaque, a: A, reference: service.types.provenance.Resource) !service.types.provenance.Snapshot {
        const self: *Routed = @ptrCast(@alignCast(ptr));
        return self.resource_fn.?(self.resource_ptr.?, a, reference);
    }
    fn getMany(ptr: *anyopaque, a: A, keys: []const []const u8) ![]const ?service.Row {
        const self: *Routed = @ptrCast(@alignCast(ptr));
        try self.context.check();
        var request = try hydrationRequest(keys);
        request.execution_deadline_ns = self.request.deadline_ns;
        request.cancellation = self.request.cancellation;
        var documents = (try self.reads.hydrateDocuments(a, self.table, request, .read_index)) orelse return error.MemorySpaceNotFound;
        defer documents.deinit();
        const rows = try a.alloc(?service.Row, keys.len);
        for (documents.rows, keys, rows) |document, key, *out| out.* = if (document) |json| .{ .key = key, .json = try a.dupe(u8, try decodeRow(a, json)) } else null;
        return rows;
    }
    fn get(ptr: *anyopaque, a: A, key: []const u8) !?service.Row {
        const self: *Routed = @ptrCast(@alignCast(ptr));
        try self.context.check();
        const row = (try self.reads.lookup(a, self.table, key, .{ .execution_deadline_ns = self.request.deadline_ns, .execution_io = self.request.deadline_io, .cancellation = self.request.cancellation }, .read_index)) orelse return null;
        return .{ .key = key, .json = try decodeRow(a, row.json), .version = row.version, .digest = row.expected_content_digest };
    }
    fn scan(ptr: *anyopaque, a: A, prefix: []const u8, cursor: ?[]const u8, limit: u32) !service.Page {
        const self: *Routed = @ptrCast(@alignCast(ptr));
        try self.context.check();
        try validateCursor(prefix, cursor);
        const end = try prefixEnd(a, prefix);
        const response = (try self.reads.scan(a, self.table, cursor orelse prefix, end, .{ .execution_deadline_ns = self.request.deadline_ns, .cancellation = self.request.cancellation, .include_documents = true, .inclusive_from = cursor == null, .exclusive_to = true, .limit = limit + 1 }, .read_index)) orelse return error.MemorySpaceNotFound;
        var rows: std.ArrayList(service.Row) = .empty;
        var lines = std.mem.tokenizeScalar(u8, response.ndjson, '\n');
        while (lines.next()) |line| {
            var fields: [2]?[]const u8 = undefined;
            try @import("antfly-json").objectFields(a, line, &.{ "_id", "_source" }, &fields);
            const id = try std.json.parseFromSliceLeaky([]const u8, a, fields[0] orelse return error.MemoryCorruptState, .{});
            if (!std.mem.startsWith(u8, id, prefix)) return error.MemoryCorruptState;
            try rows.append(a, .{ .key = id, .json = try decodeRow(a, fields[1] orelse line) });
        }
        const count = @min(rows.items.len, limit);
        return .{ .rows = rows.items[0..count], .next = if (rows.items.len > count) rows.items[count - 1].key else null };
    }
    fn commit(ptr: *anyopaque, a: A, expected: ?service.Row, changes: []const service.Change) !void {
        const self: *Routed = @ptrCast(@alignCast(ptr));
        try self.context.check();
        var writes: std.ArrayList(db_types.TransactionWrite) = .empty;
        var deletes: std.ArrayList([]const u8) = .empty;
        for (changes) |change| {
            if (change.json) |json| try writes.append(a, .{ .key = change.key, .value = try encodeChange(a, change.key, json) }) else try deletes.append(a, change.key);
        }
        const result = (try self.writes.commitTransactionWithCancellation(a, &.{.{ .table_name = self.table, .writes = writes.items, .deletes = deletes.items, .predicates = &.{.{ .key = "control", .expected_version = if (expected) |row| row.version else 0, .expected_content_digest = if (expected) |row| row.digest else null, .unique_absence = expected == null }} }}, if (hasProjection(changes)) .full_index else .write, self.request.cancellation)) orelse return error.MemoryCapabilityUnavailable;
        switch (result) {
            .conflict => return error.MemoryRevisionConflict,
            .committed => |committed| if (committed.propagation_pending or committed.visibility_pending) return error.MemoryCommitPending,
        }
    }
};

pub fn Native(comptime DB: type) type {
    return struct {
        db: *DB,
        context: service.Context,
        resource_ptr: ?*anyopaque = null,
        resource_fn: ?*const fn (*anyopaque, A, service.types.provenance.Resource) anyerror!service.types.provenance.Snapshot = null,
        const Self = @This();
        pub fn backend(self: *Self) service.Backend {
            return .{ .ptr = self, .random_bytes = if (self.db.backend_runtime.io() != null) randomBytes else null, .get = get, .get_many = getMany, .scan = scan, .commit = commit, .resource = if (self.resource_fn != null) resource else null, .neighbors = neighbors, .ensure_vector_index = ensureVectorIndex, .drop_index = dropIndex, .ensure_text_index = ensureTextIndex, .search_assertions = searchAssertions };
        }
        fn randomBytes(ptr: *anyopaque, bytes: []u8) !void {
            const self: *Self = @ptrCast(@alignCast(ptr));
            @import("antfly_platform").entropy.fill(self.db.backend_runtime.io() orelse return error.MemoryCapabilityUnavailable, bytes) catch return error.MemoryCapabilityUnavailable;
        }
        fn dropIndex(ptr: *anyopaque, _: A, name: []const u8) !void {
            const self: *Self = @ptrCast(@alignCast(ptr));
            try self.context.requireGrant(.admin);
            _ = try self.db.deleteIndex(name);
        }
        fn ensureTextIndex(ptr: *anyopaque, a: A, name: []const u8) !void {
            const self: *Self = @ptrCast(@alignCast(ptr));
            try self.context.requireGrant(.admin);
            self.db.addIndex(.{ .name = name, .kind = .full_text, .config_json = try std.json.Stringify.valueAlloc(a, .{ .field = name }, .{}) }) catch |err| switch (err) {
                error.IndexAlreadyExists => return,
                else => return err,
            };
        }
        fn searchAssertions(ptr: *anyopaque, a: A, input: service.RecallSearchRequest) !service.RecallSearchPage {
            const self: *Self = @ptrCast(@alignCast(ptr));
            try self.context.check();
            var hits: std.ArrayList(service.Neighbor) = .empty;
            var truncated = false;
            for (0..try searchLanes(input)) |lane| {
                const request = try assertionSearchRequest(a, laneRequest(input, lane));
                var response = try self.db.search(a, request);
                defer response.deinit();
                if (response.hits.len > request.limit) return error.MemoryCorruptState;
                truncated = truncated or response.hits.len == request.limit;
                for (response.hits) |hit| try hits.append(a, .{ .key = try a.dupe(u8, hit.id), .similarity = hit.score orelse return error.MemoryCorruptState });
            }
            return searchPage(a, input, hits.items, truncated);
        }
        fn ensureVectorIndex(ptr: *anyopaque, a: A, name: []const u8, dims: u32) !void {
            const self: *Self = @ptrCast(@alignCast(ptr));
            try self.context.requireGrant(.admin);
            self.db.addIndex(.{ .name = name, .kind = .dense_vector, .config_json = try vectorIndexConfig(a, dims) }) catch |err| switch (err) {
                error.IndexAlreadyExists => return,
                else => return err,
            };
        }
        fn neighbors(ptr: *anyopaque, a: A, input: service.NeighborRequest) ![]const service.Neighbor {
            const self: *Self = @ptrCast(@alignCast(ptr));
            try self.context.check();
            var response = try self.db.search(a, try neighborRequest(input));
            defer response.deinit();
            if (response.hits.len > input.limit) return error.MemoryCorruptState;
            const result = try a.alloc(service.Neighbor, response.hits.len);
            for (response.hits, result) |hit, *out| out.* = .{ .key = try a.dupe(u8, hit.id), .similarity = hit.score orelse return error.MemoryCorruptState };
            return result;
        }
        fn resource(ptr: *anyopaque, a: A, reference: service.types.provenance.Resource) !service.types.provenance.Snapshot {
            const self: *Self = @ptrCast(@alignCast(ptr));
            return self.resource_fn.?(self.resource_ptr.?, a, reference);
        }
        fn getMany(ptr: *anyopaque, a: A, keys: []const []const u8) ![]const ?service.Row {
            const self: *Self = @ptrCast(@alignCast(ptr));
            try self.context.check();
            var response = try self.db.search(a, try hydrationRequest(keys));
            defer response.deinit();
            const rows = try a.alloc(?service.Row, keys.len);
            @memset(rows, null);
            for (response.hits) |hit| {
                const json = hit.stored_data orelse return error.MemoryCorruptState;
                var found = false;
                for (keys, rows) |key, *out| if (std.mem.eql(u8, key, hit.id)) {
                    out.* = .{ .key = key, .json = try a.dupe(u8, try decodeRow(a, json)) };
                    found = true;
                };
                if (!found) return error.MemoryCorruptState;
            }
            return rows;
        }
        fn get(ptr: *anyopaque, a: A, key: []const u8) !?service.Row {
            const self: *Self = @ptrCast(@alignCast(ptr));
            try self.context.check();
            const result = (try self.db.getDocument(a, key, .{})) orelse return null;
            return .{ .key = key, .json = try decodeRow(a, result.json), .version = result.version orelse try self.db.getTimestamp(a, key), .digest = result.expected_content_digest };
        }
        fn scan(ptr: *anyopaque, a: A, prefix: []const u8, cursor: ?[]const u8, limit: u32) !service.Page {
            const self: *Self = @ptrCast(@alignCast(ptr));
            try self.context.check();
            try validateCursor(prefix, cursor);
            const result = try self.db.scan(a, cursor orelse prefix, try prefixEnd(a, prefix), .{ .include_documents = true, .inclusive_from = cursor == null, .exclusive_to = true, .limit = limit + 1 });
            const rows = try a.alloc(service.Row, result.documents.len);
            for (result.documents, rows) |document, *row| row.* = .{ .key = document.id, .json = try decodeRow(a, document.json) };
            const count = @min(rows.len, limit);
            return .{ .rows = rows[0..count], .next = if (rows.len > count) rows[count - 1].key else null };
        }
        fn commit(ptr: *anyopaque, a: A, expected: ?service.Row, changes: []const service.Change) !void {
            const self: *Self = @ptrCast(@alignCast(ptr));
            try self.context.check();
            var writes: std.ArrayList(db_types.BatchWrite) = .empty;
            var deletes: std.ArrayList([]const u8) = .empty;
            for (changes) |change| {
                if (change.json) |json| try writes.append(a, .{ .key = change.key, .value = try encodeChange(a, change.key, json) }) else try deletes.append(a, change.key);
            }
            self.db.batch(.{ .writes = writes.items, .deletes = deletes.items, .sync_level = if (hasProjection(changes)) .full_index else .write, .predicates = &.{.{ .key = "control", .expected_version = if (expected) |row| row.version else 0, .expected_content_digest = if (expected) |row| row.digest else null, .unique_absence = expected == null }} }) catch |err| switch (err) {
                error.VersionConflict, error.IntentConflict => return error.MemoryRevisionConflict,
                else => return err,
            };
        }
    };
}

fn hasProjection(changes: []const service.Change) bool {
    for (changes) |change| if (std.mem.startsWith(u8, change.key, "entity-semantic/") or std.mem.startsWith(u8, change.key, "assertion-recall/") or std.mem.eql(u8, change.key, "entity-semantic-index") or std.mem.eql(u8, change.key, "assertion-recall-index")) return true;
    return false;
}
fn validateCursor(prefix: []const u8, cursor: ?[]const u8) !void {
    if (cursor) |value| if (!std.mem.startsWith(u8, value, prefix)) return error.InvalidMemoryCursor;
}
fn prefixEnd(a: A, prefix: []const u8) ![]const u8 {
    if (prefix.len == 0) return "\xff";
    const end = try a.dupe(u8, prefix);
    end[end.len - 1] += 1;
    return end;
}

test "capi native memory audience lanes retain the compiled owner text identity" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const input: service.RecallSearchRequest = .{ .text_indexes = &.{ "memory_recall_text_team", "memory_recall_text_private" }, .query = "Deployment", .limit = 8 };
    try std.testing.expectEqual(@as(usize, 2), try searchLanes(input));
    for (0..try searchLanes(input)) |lane| {
        const request = try assertionSearchRequest(a, laneRequest(input, lane));
        const wire = try @import("../api/query_execution_contract.zig").encodeStorageKernelQueryRequest(a, request);
        const json = try std.json.parseFromSliceLeaky(std.json.Value, a, wire, .{});
        try std.testing.expectEqualStrings(input.text_indexes[lane], json.object.get("full_text_index").?.string);
    }
    var oversized = input;
    oversized.offset = std.math.maxInt(u32);
    try std.testing.expectError(error.InvalidMemoryRequest, searchLanes(oversized));
    oversized = input;
    oversized.text_indexes = &.{ "memory_recall_text_a", "memory_recall_text_b", "memory_recall_text_c" };
    try std.testing.expectError(error.InvalidMemoryRequest, searchLanes(oversized));
}

test "capi native memory routed index DDL uses catalog authority" {
    const Counts = struct {
        creates: usize = 0,
        drops: usize = 0,
        fn create(ptr: *anyopaque, _: A, _: []const u8, _: []const u8, _: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.creates += 1;
        }
        fn drop(ptr: *anyopaque, _: A, _: []const u8, _: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.drops += 1;
        }
        fn source(self: *@This()) IndexCatalog {
            return .{ .ptr = self, .create = create, .drop = drop };
        }
    };
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var catalog: Counts = .{};
    var adapter: Routed = .{ .table = "memory_space", .reads = undefined, .writes = undefined, .index_catalog = catalog.source(), .context = .{ .principal = "admin", .admin = true, .now_ms = 1 } };
    try Routed.ensureTextIndex(&adapter, a, "memory_recall_text_test");
    try Routed.ensureVectorIndex(&adapter, a, "memory_recall_vector_test", 3);
    try Routed.dropIndex(&adapter, a, "memory_recall_text_test");
    try std.testing.expectEqual(@as(usize, 2), catalog.creates);
    try std.testing.expectEqual(@as(usize, 1), catalog.drops);
}
