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

//! A text snapshot may cover part, never implicitly all, of a source row.
//! Keep ordinary fields on the shared Parquet path and bind stored values to
//! both the authenticated field coverage and the query's native row identity.
const std = @import("std");
const local = @import("antfly_local_sources");
const A = std.mem.Allocator;
pub const Plan = struct { parquet: []const []const u8, sidecar: []const []const u8 };
pub fn plan(a: A, wanted: []const []const u8, available: []const []const u8) !Plan {
    var coverage: std.StringHashMapUnmanaged(void) = .empty;
    defer coverage.deinit(a);
    for (available) |field| try coverage.put(a, field, {});
    var parquet: std.ArrayList([]const u8) = .empty;
    errdefer parquet.deinit(a);
    var sidecar: std.ArrayList([]const u8) = .empty;
    errdefer sidecar.deinit(a);
    for (wanted) |field| {
        const covered = coverage.contains(field);
        if (covered) try sidecar.append(a, field) else try parquet.append(a, field);
    }
    const source_fields = try parquet.toOwnedSlice(a);
    errdefer a.free(source_fields);
    return .{ .parquet = source_fields, .sidecar = try sidecar.toOwnedSlice(a) };
}
pub fn appendStored(a: A, snapshot: *const local.index.IndexSnapshot, doc: u32, expected_key: []const u8, fields: []const []const u8, target: *std.json.Value) !void {
    const stored = (try snapshot.storedDocDecompressed(a, doc)) orelse return error.InvalidNativeLakeTextCorpus;
    defer a.free(stored.data);
    try appendStoredValue(a, stored.id, stored.data, expected_key, fields, target);
}

/// One scope per result batch, not per hit or server. Reuses decompression for
/// nearby/interleaved ranked hits without retaining unbounded decoded bodies.
pub const Hydrator = struct {
    const Cursor = @import("antfly_local_sources").section_doc_values.Cursor;
    const Column = struct { reader: *const local.segment.SegmentReader, name: []u8, cursor: Cursor, used: u64 };
    blocks: local.index.IndexSnapshot.StoredDocBlockCache,
    columns: [4]?Column = @splat(null),
    tick: u64 = 0,
    column_values: usize = 0,
    column_opens: usize = 0,

    pub fn init(a: A) Hydrator {
        return .{ .blocks = .init(a, 1024 * 1024) };
    }

    pub fn deinit(self: *Hydrator) void {
        for (&self.columns) |*slot| if (slot.*) |*value| {
            value.cursor.deinit();
            self.blocks.allocator.free(value.name);
        };
        self.blocks.deinit();
        self.* = undefined;
    }

    fn column(self: *Hydrator, reader: *const local.segment.SegmentReader, name: []const u8) !?*Cursor {
        self.tick +|= 1;
        var oldest: usize = 0;
        for (&self.columns, 0..) |*slot, i| {
            if (slot.*) |*value| {
                if (value.reader == reader and std.mem.eql(u8, value.name, name)) {
                    value.used = self.tick;
                    return &value.cursor;
                }
                if (self.columns[oldest] != null and value.used < self.columns[oldest].?.used) oldest = i;
            } else oldest = i;
        }
        const a = self.blocks.allocator;
        var scoped = (try reader.storedColumnScoped(a, name)) orelse return null;
        errdefer scoped.deinit();
        const owned = try a.dupe(u8, name);
        if (self.columns[oldest]) |*previous| {
            previous.cursor.deinit();
            a.free(previous.name);
        }
        self.column_opens += 1;
        self.columns[oldest] = .{ .reader = reader, .name = owned, .cursor = Cursor.init(scoped), .used = self.tick };
        return &self.columns[oldest].?.cursor;
    }
    pub const batch_limit = 64;
    /// Process one field across a bounded, ordinal-sorted winner permutation.
    /// Result positions stay unchanged and a wide projection needs no wider
    /// cursor cache. Each legacy row is parsed once for the complete projection.
    pub fn appendBatch(self: *Hydrator, a: A, snapshot: *const local.index.IndexSnapshot, docs: []const u32, expected_keys: []const []const u8, fields: []const []const u8, targets: []?std.json.Value) !void {
        if (docs.len > batch_limit or docs.len != targets.len or docs.len != expected_keys.len) return error.InvalidNativeLakeTextCorpus;
        var scratch = std.heap.ArenaAllocator.init(a);
        defer scratch.deinit();
        var legacy: [batch_limit]bool = @splat(false);
        var permutation: [batch_limit]usize = undefined;
        for (docs, 0..) |doc, i| {
            permutation[i] = i;
            _ = scratch.reset(.retain_capacity);
            const resolved = snapshot.resolveDocId(doc) orelse return error.InvalidNativeLakeTextCorpus;
            const segment = try snapshot.readSegment(resolved.seg_idx);
            const id = (try segment.reader.storedIdScoped(scratch.allocator(), resolved.local_id)) orelse return error.InvalidNativeLakeTextCorpus;
            if (!std.mem.eql(u8, id, expected_keys[i])) return error.ExternalLakeSnapshotMismatch;
            if (targets[i] == null or targets[i].? != .object) return error.InvalidNativeLakeTextCorpus;
            const columnar = block: {
                for (segment.reader.fields) |field| for (field.sections) |section| {
                    if (section.section_type == .columnar_stored) break :block true;
                };
                break :block false;
            };
            if (!columnar) {
                legacy[i] = true;
                const stored = (try snapshot.storedDocWithBlockCache(&self.blocks, doc)) orelse return error.InvalidNativeLakeTextCorpus;
                try appendStoredValue(a, stored.id, stored.data, expected_keys[i], fields, &targets[i].?);
            }
        }
        std.mem.sort(usize, permutation[0..docs.len], docs, struct {
            fn less(ordinals: []const u32, l: usize, r: usize) bool {
                return ordinals[l] < ordinals[r];
            }
        }.less);
        for (fields) |field| for (permutation[0..docs.len]) |i| {
            if (legacy[i]) continue;
            _ = scratch.reset(.retain_capacity);
            const resolved = snapshot.resolveDocId(docs[i]).?;
            const segment = try snapshot.readSegment(resolved.seg_idx);
            var tail: ?[]const u8 = null;
            var cursor = try self.column(&segment.reader, field);
            if (cursor == null) if (std.mem.indexOfScalar(u8, field, '.')) |dot| {
                tail = field[dot + 1 ..];
                cursor = try self.column(&segment.reader, field[0..dot]);
            };
            if (if (cursor) |value| try value.get(resolved.local_id) else null) |encoded| {
                const root = try local.storage_typed_json.decodeBinary(scratch.allocator(), encoded);
                const value = if (tail) |suffix| local.api_json_helpers.extractJsonPathValue(root, suffix) orelse return error.InvalidNativeLakeTextCorpus else root;
                const target = &targets[i].?;
                if (target.object.contains(field)) return error.InvalidNativeLakeTextCorpus;
                const key = try a.dupe(u8, field);
                errdefer a.free(key);
                var owned = try local.storage_db_types.cloneJsonValue(a, value);
                errdefer local.storage_db_types.deinitJsonValue(a, &owned);
                try target.object.put(a, key, owned);
                self.column_values += 1;
            } else {
                const stored = (try snapshot.storedDocWithBlockCache(&self.blocks, docs[i])) orelse return error.InvalidNativeLakeTextCorpus;
                try appendStoredValue(a, stored.id, stored.data, expected_keys[i], &.{field}, &targets[i].?);
            }
        };
    }
    pub fn append(self: *Hydrator, a: A, snapshot: *const local.index.IndexSnapshot, doc: u32, expected_key: []const u8, fields: []const []const u8, target: *std.json.Value) !void {
        var targets = [_]?std.json.Value{target.*};
        defer target.* = targets[0].?;
        try self.appendBatch(a, snapshot, &.{doc}, &.{expected_key}, fields, &targets);
    }
};

fn appendStoredValue(a: A, id: []const u8, data: []const u8, expected_key: []const u8, fields: []const []const u8, target: *std.json.Value) !void {
    if (!std.mem.eql(u8, id, expected_key)) return error.ExternalLakeSnapshotMismatch;
    var parsed = try std.json.parseFromSlice(std.json.Value, a, data, .{});
    defer parsed.deinit();
    if (parsed.value != .object or target.* != .object) return error.InvalidNativeLakeTextCorpus;
    for (fields) |field| {
        if (target.object.contains(field)) return error.InvalidNativeLakeTextCorpus;
        const value = local.api_json_helpers.extractJsonPathValue(parsed.value, field) orelse return error.InvalidNativeLakeTextCorpus;
        const key = try a.dupe(u8, field);
        errdefer a.free(key);
        var owned = try local.storage_db_types.cloneJsonValue(a, value);
        errdefer local.storage_db_types.deinitJsonValue(a, &owned);
        try target.object.put(a, key, owned);
    }
}

test "external lake bounded window profiles sidecar coverage keeps unrelated source fields on Parquet" {
    const a = std.testing.allocator;
    const projection = try plan(a, &.{ "label", "body", "nested.text", "nested", "counter" }, &.{ "body", "nested.text" });
    defer a.free(projection.parquet);
    defer a.free(projection.sidecar);
    try std.testing.expectEqualSlices([]const u8, &.{ "label", "nested", "counter" }, projection.parquet);
    try std.testing.expectEqualSlices([]const u8, &.{ "body", "nested.text" }, projection.sidecar);
}

test "external lake bounded window profiles sidecar hydration binds identities and preserves typed fields under allocation faults" {
    const a = std.testing.allocator;
    const encoded = (try local.storage_db_document_mapper.buildTextSegmentFromDocuments(a, &.{.{ .key = "row", .value = "{\"body\":\"a needle\",\"nested\":{\"text\":\"detail\"},\"counter\":9007199254740993}" }}, .{}, null)).?;
    defer a.free(encoded);
    var writer = try local.index.IndexWriter.init(a);
    defer writer.deinit();
    try writer.addSegmentWithId(1, encoded);
    const Run = struct {
        fn run(alloc: A, snapshot: *const local.index.IndexSnapshot) !void {
            var hydrator = Hydrator.init(alloc);
            defer hydrator.deinit();
            var row: std.json.Value = .{ .object = .empty };
            defer local.storage_db_types.deinitJsonValue(alloc, &row);
            try hydrator.append(alloc, snapshot, 0, "row", &.{ "body", "nested.text", "counter" }, &row);
            try std.testing.expectEqualStrings("a needle", row.object.get("body").?.string);
            try std.testing.expectEqualStrings("detail", row.object.get("nested.text").?.string);
            try std.testing.expectEqual(@as(i64, 9007199254740993), row.object.get("counter").?.integer);
        }
    };
    try Run.run(a, writer.snapshot());
    // Heap remaps depend on surrounding allocation addresses. Disable them
    // so each exhaustive fault-injection run has the same allocation count.
    var stable = std.testing.FailingAllocator.init(a, .{ .resize_fail_index = 0 });
    try std.testing.checkAllAllocationFailures(stable.allocator(), Run.run, .{writer.snapshot()});
    var row: std.json.Value = .{ .object = .empty };
    defer local.storage_db_types.deinitJsonValue(a, &row);
    try std.testing.expectError(error.ExternalLakeSnapshotMismatch, appendStored(a, writer.snapshot(), 0, "different-generation-row", &.{"body"}, &row));
    try std.testing.expectError(error.InvalidNativeLakeTextCorpus, appendStored(a, writer.snapshot(), 0, "row", &.{"missing"}, &row));
}

test "external lake bounded window profiles ranked sidecar hydration reuses bounded decoded blocks across segments" {
    const a = std.testing.allocator;
    const first = (try local.storage_db_document_mapper.buildTextSegmentFromDocuments(a, &.{
        .{ .key = "one", .value = "{\"body\":\"first needle\"}" },
        .{ .key = "two", .value = "{\"body\":\"second needle\"}" },
    }, .{}, null)).?;
    defer a.free(first);
    const second = (try local.storage_db_document_mapper.buildTextSegmentFromDocuments(a, &.{
        .{ .key = "three", .value = "{\"body\":\"third needle\"}" },
    }, .{}, null)).?;
    defer a.free(second);
    var writer = try local.index.IndexWriter.init(a);
    defer writer.deinit();
    try writer.addSegmentWithId(1, first);
    try writer.addSegmentWithId(2, second);
    const Run = struct {
        fn run(alloc: A, snapshot: *const local.index.IndexSnapshot, repetitions: usize) !void {
            var hydrator = Hydrator.init(alloc);
            defer hydrator.deinit();
            // Alternate segment/block owners and preserve non-physical order.
            const docs = [_]u32{ 2, 1, 0, 2, 0, 1 };
            const keys = [_][]const u8{ "one", "two", "three" };
            const bodies = [_][]const u8{ "first needle", "second needle", "third needle" };
            for (0..repetitions) |_| for (docs) |doc| {
                var row: std.json.Value = .{ .object = .empty };
                defer local.storage_db_types.deinitJsonValue(alloc, &row);
                try hydrator.append(alloc, snapshot, doc, keys[doc], &.{"body"}, &row);
                try std.testing.expectEqualStrings(bodies[doc], row.object.get("body").?.string);
            };
            try std.testing.expectEqual(@as(usize, 2), hydrator.blocks.decode_count);
            try std.testing.expect(hydrator.blocks.live_bytes <= 1024 * 1024);
            var row: std.json.Value = .{ .object = .empty };
            defer local.storage_db_types.deinitJsonValue(alloc, &row);
            try std.testing.expectError(error.ExternalLakeSnapshotMismatch, hydrator.append(alloc, snapshot, 0, "three", &.{"body"}, &row));
            try std.testing.expectEqual(@as(usize, 0), row.object.count());
            try std.testing.expectError(error.InvalidNativeLakeTextCorpus, hydrator.append(alloc, snapshot, 99, "missing", &.{"body"}, &row));
        }
    };
    try Run.run(a, writer.snapshot(), 16);
    // Heap remaps may succeed or fall back depending on surrounding allocation
    // addresses. Disable them so exhaustive failure indexes are reproducible.
    var stable = std.testing.FailingAllocator.init(a, .{ .resize_fail_index = 0 });
    // One full interleaving covers all ownership paths; repeated cache hits
    // above exercise reuse without quadratically growing the fault sweep.
    try std.testing.checkAllAllocationFailures(stable.allocator(), Run.run, .{ writer.snapshot(), 1 });
}

test "external lake bounded window profiles typed source columns project without whole rows and survive both compaction orders" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const sa = arena.allocator();
    const payload = try sa.alloc(u8, 256 * 1024);
    @memset(payload, 'x');
    var docs: [4]local.introducer.TextDocument = undefined;
    for (&docs, [_][]const u8{ "a", "c", "b", "d" }) |*doc, id| {
        var row: std.json.ObjectMap = .empty;
        try row.put(sa, "body", .{ .string = id });
        try row.put(sa, "counter", .{ .integer = 9007199254740993 });
        try row.put(sa, "flag", .{ .bool = true });
        try row.put(sa, "bucket", .{ .integer = 37 });
        try row.put(sa, "payload", .{ .string = payload });
        var nested: std.json.ObjectMap = .empty;
        try nested.put(sa, "text", .{ .string = id });
        try row.put(sa, "nested", .{ .object = nested });
        const root: std.json.Value = .{ .object = row };
        doc.* = .{ .id = id, .stored_data = try std.json.Stringify.valueAlloc(sa, root, .{}), .stored_source = root, .text_fields = &.{} };
    }
    const sort = [_]local.segment.SegmentIndexSortField{.{ .field = "_id" }};
    const first = try local.introducer.buildSegmentFromTextWithAnalysisOptions(a, docs[0..2], &local.search_analysis.default_analyzer, .{}, .{ .store_source_columns = true, .index_sort = &sort });
    defer a.free(first);
    const second = try local.introducer.buildSegmentFromTextWithAnalysisOptions(a, docs[2..4], &local.search_analysis.default_analyzer, .{}, .{ .store_source_columns = true, .index_sort = &sort });
    defer a.free(second);
    var r1 = try local.segment.SegmentReader.init(a, first);
    defer r1.deinit();
    var r2 = try local.segment.SegmentReader.init(a, second);
    defer r2.deinit();
    var deleted = local.encoding_roaring.RoaringBitmap.init(a);
    defer deleted.deinit();
    try deleted.add(1);
    for ([_]bool{ false, true }) |ordered| {
        const merged = try local.segment.mergeSegmentInputsWithOptions(a, &.{ .{ .reader = &r1, .deleted = deleted }, .{ .reader = &r2 } }, .{ .index_sort = if (ordered) &sort else &.{} });
        defer a.free(merged);
        var writer = try local.index.IndexWriter.init(a);
        defer writer.deinit();
        try writer.addSegmentWithId(1, merged);
        const snapshot = writer.snapshot();
        const Run = struct {
            fn run(alloc: A, snap: *const local.index.IndexSnapshot) !void {
                var hydrator = Hydrator.init(alloc);
                defer hydrator.deinit();
                for ([_]u32{ 2, 0, 1 }) |doc| {
                    var scratch = std.heap.ArenaAllocator.init(alloc);
                    defer scratch.deinit();
                    const key = (try snap.storedIdScoped(scratch.allocator(), doc)).?;
                    var row: std.json.Value = .{ .object = .empty };
                    defer local.storage_db_types.deinitJsonValue(alloc, &row);
                    try hydrator.append(alloc, snap, doc, key, &.{ "body", "counter", "nested.text" }, &row);
                    try std.testing.expectEqualStrings(key, row.object.get("body").?.string);
                    try std.testing.expectEqualStrings(key, row.object.get("nested.text").?.string);
                    try std.testing.expectEqual(@as(i64, 9007199254740993), row.object.get("counter").?.integer);
                }
                try std.testing.expectEqual(@as(usize, 9), hydrator.column_values);
                try std.testing.expectEqual(@as(usize, 0), hydrator.blocks.decode_count);
                for (hydrator.columns) |column| if (column) |value| try std.testing.expect(!std.mem.eql(u8, value.name, "payload"));
            }
        };
        try Run.run(a, snapshot);
        const Batch = struct {
            fn run(alloc: A, snap: *const local.index.IndexSnapshot) !void {
                var hydrator = Hydrator.init(alloc);
                defer hydrator.deinit();
                var scratch = std.heap.ArenaAllocator.init(alloc);
                defer scratch.deinit();
                const ordinals = [_]u32{ 2, 0, 1 };
                var keys: [3][]const u8 = undefined;
                for (ordinals, &keys) |doc, *key| key.* = (try snap.storedIdScoped(scratch.allocator(), doc)).?;
                var targets: [3]?std.json.Value = @splat(std.json.Value{ .object = .empty });
                defer for (&targets) |*value| if (value.*) |*row| local.storage_db_types.deinitJsonValue(alloc, row);
                try hydrator.appendBatch(alloc, snap, &ordinals, &keys, &.{ "body", "counter", "nested.text", "flag", "bucket" }, &targets);
                for (targets, keys) |row, key| {
                    try std.testing.expectEqualStrings(key, row.?.object.get("body").?.string);
                    try std.testing.expectEqualStrings(key, row.?.object.get("nested.text").?.string);
                    try std.testing.expect(row.?.object.get("flag").?.bool);
                    try std.testing.expectEqual(@as(i64, 37), row.?.object.get("bucket").?.integer);
                }
                try std.testing.expectEqual(@as(usize, 5), hydrator.column_opens);
                try std.testing.expectEqual(@as(usize, 15), hydrator.column_values);
                try std.testing.expectEqual(@as(usize, 0), hydrator.blocks.decode_count);
            }
        };
        try Batch.run(a, snapshot);
        var batch_stable = std.testing.FailingAllocator.init(a, .{ .resize_fail_index = 0 });
        try std.testing.checkAllAllocationFailures(batch_stable.allocator(), Batch.run, .{snapshot});
        var stable = std.testing.FailingAllocator.init(a, .{ .resize_fail_index = 0 });
        try std.testing.checkAllAllocationFailures(stable.allocator(), Run.run, .{snapshot});
        const full = (try snapshot.storedDocDecompressed(a, 0)).?;
        defer a.free(full.data);
        const parsed = try std.json.parseFromSlice(std.json.Value, a, full.data, .{});
        defer parsed.deinit();
        try std.testing.expectEqual(@as(usize, payload.len), parsed.value.object.get("payload").?.string.len);
        const reader = try snapshot.readSegment(0);
        try std.testing.expectEqual(@as(?u32, 5), try reader.reader.storedDocLength(0));
    }
}
