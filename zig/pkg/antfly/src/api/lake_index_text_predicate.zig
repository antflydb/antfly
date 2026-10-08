// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Exact indexed metadata predicates in a pinned native text snapshot.
const std = @import("std");
const local = @import("antfly_local_sources");
const rows = @import("lake_index_sql_rows.zig");
const corpus = @import("lake_index_native_text.zig");
const A = std.mem.Allocator;
const Bitmap = local.encoding_roaring.RoaringBitmap;
const Result = local.storage_db_query_search_exec.IndexedTextPredicate;
const Condition = local.sql_catalog.Condition;

/// File-local ascending identities are attested by native text metadata v7.
/// This directory is O(files + segments), never O(rows).
pub const Identities = struct {
    const Span = struct { lower: u32, upper: u32 };
    files: std.StringHashMapUnmanaged(Span) = .empty,
    offsets: []const u32,
    pub fn init(a: A, root: corpus.Root, snapshot: *const local.index.IndexSnapshot) !Identities {
        if (root.version != corpus.metadata_version or (root.file_groups.len == 0 and snapshot.segments.len != 0)) return error.InvalidNativeLakeTextCorpus;
        const offsets = try a.alloc(u32, snapshot.segments.len + 1);
        offsets[0] = 0;
        for (snapshot.segments, 0..) |segment, i| offsets[i + 1] = try std.math.add(u32, offsets[i], segment.reader.doc_count);
        var result: Identities = .{ .offsets = offsets };
        var segment: usize = 0;
        for (root.file_groups) |group| {
            const start = segment;
            segment += group.segments.len;
            if (segment > snapshot.segments.len) return error.InvalidNativeLakeTextCorpus;
            const entry = try result.files.getOrPut(a, try a.dupe(u8, group.file.id));
            if (entry.found_existing) return error.InvalidNativeLakeTextCorpus;
            entry.value_ptr.* = .{ .lower = offsets[start], .upper = offsets[segment] };
        }
        if (segment != snapshot.segments.len) return error.InvalidNativeLakeTextCorpus;
        return result;
    }
    fn id(self: Identities, a: A, snapshot: *const local.index.IndexSnapshot, doc: u32) ![]const u8 {
        var lower: usize = 0;
        var upper = snapshot.segments.len;
        while (lower < upper) {
            const mid = lower + (upper - lower) / 2;
            if (self.offsets[mid + 1] <= doc) lower = mid + 1 else upper = mid;
        }
        if (lower == snapshot.segments.len) return error.InvalidNativeLakeTextCorpus;
        return (try snapshot.segments[lower].reader.storedIdScoped(a, doc - self.offsets[lower])) orelse error.InvalidNativeLakeTextCorpus;
    }
    fn findForward(self: Identities, scratch: *std.heap.ArenaAllocator, snapshot: *const local.index.IndexSnapshot, position: *u32, file: []const u8, key: []const u8) !?u32 {
        const span = self.files.get(file) orelse return error.ExternalLakeSnapshotMismatch;
        position.* = @max(position.*, span.lower);
        while (position.* < span.upper) {
            _ = scratch.reset(.retain_capacity);
            const candidate = try self.id(scratch.allocator(), snapshot, position.*);
            switch (std.mem.order(u8, candidate, key)) {
                .lt => position.* += 1,
                .gt => return null,
                .eq => {
                    const found = position.*;
                    position.* += 1;
                    return found;
                },
            }
        }
        return null;
    }
    fn find(self: Identities, scratch: *std.heap.ArenaAllocator, snapshot: *const local.index.IndexSnapshot, file: []const u8, key: []const u8) !?u32 {
        const span = self.files.get(file) orelse return error.ExternalLakeSnapshotMismatch;
        var lower = span.lower;
        var upper = span.upper;
        while (lower < upper) {
            _ = scratch.reset(.retain_capacity);
            const mid = lower + (upper - lower) / 2;
            const candidate = try self.id(scratch.allocator(), snapshot, mid);
            switch (std.mem.order(u8, candidate, key)) {
                .lt => lower = mid + 1,
                .gt => upper = mid,
                .eq => return mid,
            }
        }
        return null;
    }
};

pub const Resolver = struct {
    server: *@import("http_server.zig").ApiHttpServer,
    table: local.sql_catalog.Table,
    source: *local.serverless_query_lake_serving.ServingSource,
    context: local.api_operation.RequestContext,
    identities: Identities,
    snapshot: *const local.index.IndexSnapshot,
    private_digests: *const std.StringHashMapUnmanaged([]const u8),

    pub fn resolve(self: Resolver, a: A, json: []const u8) !?Result {
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const parsed = try std.json.parseFromSlice(std.json.Value, arena.allocator(), json, .{});
        return self.value(a, parsed.value, 0, true);
    }
    fn value(self: Resolver, a: A, input: std.json.Value, depth: usize, allow_scan: bool) anyerror!?Result {
        if (depth > 64 or input != .object or input.object.count() != 1) return null;
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        var conditions: std.ArrayList(Condition) = .empty;
        const scalar_conditions = try collectConditions(arena.allocator(), input, &conditions, depth);
        if (scalar_conditions) {
            if (try self.index(a, conditions.items, false)) |bitmap| return .{ .bitmap = bitmap };
        }
        // Separate indexes may enforce individual conjuncts. An unresolved
        // child makes this whole predicate residual; OR/NOT never use supersets.
        if (input.object.get("conjuncts")) |children| {
            if (try self.resolveChildren(a, children, true, depth)) |result| return result;
        }
        if (input.object.get("disjuncts")) |children| return self.resolveChildren(a, children, false, depth);
        if (input.object.get("bool")) |boolean| {
            if (boolean != .object) return null;
            var conjuncts: std.array_list.Managed(std.json.Value) = .init(arena.allocator());
            var fields = boolean.object.iterator();
            while (fields.next()) |entry| {
                if (!std.mem.eql(u8, entry.key_ptr.*, "must") and !std.mem.eql(u8, entry.key_ptr.*, "filter")) return null;
                if (entry.value_ptr.* != .array) return null;
                try conjuncts.appendSlice(entry.value_ptr.array.items);
            }
            if (try self.resolveChildren(a, .{ .array = conjuncts }, true, depth)) |result| return result;
        }
        if (allow_scan and scalar_conditions) {
            if (try self.index(a, conditions.items, true)) |bitmap| return .{ .bitmap = bitmap };
        }
        return null;
    }
    fn resolveChildren(self: Resolver, a: A, input: std.json.Value, conjunction: bool, depth: usize) !?Result {
        if (input != .array or input.array.items.len == 0) return null;
        var result: ?Result = null;
        errdefer if (result) |*predicate| predicate.bitmap.deinit();
        var exact = true;
        for (input.array.items) |child| {
            var next = (try self.value(a, child, depth + 1, false)) orelse {
                if (conjunction) {
                    exact = false;
                    continue;
                }
                if (result) |*predicate| predicate.bitmap.deinit();
                return null;
            };
            if (!conjunction and !next.exact) {
                next.bitmap.deinit();
                if (result) |*predicate| predicate.bitmap.deinit();
                return null;
            }
            exact = exact and next.exact;
            if (result) |*predicate| {
                defer next.bitmap.deinit();
                if (conjunction) predicate.bitmap.andWith(&next.bitmap) else try predicate.bitmap.orWith(&next.bitmap);
            } else result = next;
        }
        if (result) |*predicate| predicate.exact = exact;
        return result;
    }
    fn index(self: Resolver, a: A, conditions: []const Condition, scan: bool) !?Bitmap {
        var predicate = (if (scan) try rows.tryOpenPredicateScan(a, self.table, conditions, self.context, self.source) else try rows.tryOpenPredicate(a, self.server, self.table, conditions, self.context, self.source)) orelse return null;
        defer predicate.deinit();
        var result = Bitmap.init(a);
        errdefer result.deinit();
        const broad = predicate.estimatedRows() > self.snapshot.liveDocCount() / 16;
        var positions: std.StringHashMapUnmanaged(u32) = .empty;
        defer {
            var keys = positions.keyIterator();
            while (keys.next()) |key| a.free(key.*);
            positions.deinit(a);
        }
        var window = std.heap.ArenaAllocator.init(a);
        defer window.deinit();
        var scratch = std.heap.ArenaAllocator.init(a);
        defer scratch.deinit();
        while (true) {
            try self.context.ensureActive();
            _ = window.reset(.retain_capacity);
            const refs = if (broad) try predicate.nextPhysical(window.allocator(), 1024) else try predicate.next(window.allocator(), 1024);
            if (refs.len == 0) break;
            for (refs) |ref| {
                if (ref != .external) return error.InvalidNativeLakeRowIndex;
                const row = ref.external;
                const digest = self.private_digests.get(row.file_id) orelse return error.ExternalLakeSnapshotMismatch;
                var buffer: [96]u8 = undefined;
                const key = try std.fmt.bufPrint(&buffer, "lake2:{s}:{x:0>8}:{x:0>16}", .{ digest, row.row_group_ordinal, row.row_ordinal });
                const doc = if (broad) blk: {
                    if (!positions.contains(row.file_id)) {
                        const owned_file = try a.dupe(u8, row.file_id);
                        errdefer a.free(owned_file);
                        try positions.put(a, owned_file, 0);
                    }
                    break :blk try self.identities.findForward(&scratch, self.snapshot, positions.getPtr(row.file_id).?, row.file_id, key);
                } else try self.identities.find(&scratch, self.snapshot, row.file_id, key);
                if (doc) |number| try result.add(number);
            }
        }
        return result;
    }
};

// Accept only exact scalar comparisons on top-level relational columns.
// Unknown grammar stays with the shared, authoritative residual evaluator.
fn column(value: std.json.Value) ?[]const u8 {
    if (value != .string or !std.mem.startsWith(u8, value.string, "/")) return null;
    const name = value.string[1..];
    if (name.len == 0 or std.mem.indexOfAny(u8, name, "/~") != null) return null;
    return name;
}
fn scalar(value: std.json.Value) bool {
    return switch (value) {
        .string, .integer, .float, .bool => true,
        else => false,
    };
}
fn collectConditions(a: A, input: std.json.Value, out: *std.ArrayList(Condition), depth: usize) !bool {
    if (depth > 64 or input != .object or input.object.count() != 1) return false;
    if (input.object.get("conjuncts")) |children| {
        if (children != .array or children.array.items.len == 0) return false;
        for (children.array.items) |child| if (!try collectConditions(a, child, out, depth + 1)) return false;
        return true;
    }
    if (input.object.get("bool")) |boolean| {
        if (boolean != .object or boolean.object.count() == 0) return false;
        var fields = boolean.object.iterator();
        while (fields.next()) |entry| {
            if (!std.mem.eql(u8, entry.key_ptr.*, "must") and !std.mem.eql(u8, entry.key_ptr.*, "filter")) return false;
            if (entry.value_ptr.* != .array or entry.value_ptr.array.items.len == 0) return false;
            for (entry.value_ptr.array.items) |child| if (!try collectConditions(a, child, out, depth + 1)) return false;
        }
        return true;
    }
    if (input.object.get("term")) |term| {
        if (term != .object or term.object.count() != 2) return false;
        const name = column(term.object.get("path") orelse return false) orelse return false;
        const value = term.object.get("value") orelse return false;
        if (!scalar(value)) return false;
        try out.append(a, .{ .column = name, .op = .eq, .value = value });
        return true;
    }
    if (input.object.get("range")) |range| {
        if (range != .object) return false;
        const name = column(range.object.get("path") orelse return false) orelse return false;
        var it = range.object.iterator();
        var bounds: usize = 0;
        while (it.next()) |entry| {
            if (std.mem.eql(u8, entry.key_ptr.*, "path")) continue;
            const op: Condition.Op = if (std.mem.eql(u8, entry.key_ptr.*, "gt")) .gt else if (std.mem.eql(u8, entry.key_ptr.*, "gte")) .gte else if (std.mem.eql(u8, entry.key_ptr.*, "lt")) .lt else if (std.mem.eql(u8, entry.key_ptr.*, "lte")) .lte else return false;
            if (!scalar(entry.value_ptr.*)) return false;
            try out.append(a, .{ .column = name, .op = op, .value = entry.value_ptr.* });
            bounds += 1;
        }
        return bounds != 0;
    }
    return false;
}

test "external lake indexed predicate parser preserves exact scalar bounds and rejects unsupported grammar" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const parsed = try std.json.parseFromSlice(std.json.Value, a,
        \\{ "conjuncts": [{"term":{"path":"/kind","value":"story"}}, {"range":{"path":"/created_at","gte":42,"lt":100}}] }
    , .{});
    var conditions: std.ArrayList(Condition) = .empty;
    try std.testing.expect(try collectConditions(a, parsed.value, &conditions, 0));
    try std.testing.expectEqual(@as(usize, 3), conditions.items.len);
    try std.testing.expectEqualStrings("kind", conditions.items[0].column);
    const unsupported = try std.json.parseFromSlice(std.json.Value, a,
        \\{"term":{"path":"/nested/value","value":1}}
    , .{});
    try std.testing.expect(!try collectConditions(a, unsupported.value, &conditions, 0));
}
