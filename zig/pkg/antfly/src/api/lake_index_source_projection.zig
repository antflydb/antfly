// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

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
    blocks: local.index.IndexSnapshot.StoredDocBlockCache,

    pub fn init(a: A) Hydrator {
        return .{ .blocks = .init(a, 1024 * 1024) };
    }

    pub fn deinit(self: *Hydrator) void {
        self.blocks.deinit();
        self.* = undefined;
    }

    pub fn append(self: *Hydrator, a: A, snapshot: *const local.index.IndexSnapshot, doc: u32, expected_key: []const u8, fields: []const []const u8, target: *std.json.Value) !void {
        const stored = (try snapshot.storedDocWithBlockCache(&self.blocks, doc)) orelse return error.InvalidNativeLakeTextCorpus;
        // Parse/clone before another get can invalidate borrowed block bytes.
        try appendStoredValue(a, stored.id, stored.data, expected_key, fields, target);
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

test "external lake sidecar coverage keeps unrelated source fields on Parquet" {
    const a = std.testing.allocator;
    const projection = try plan(a, &.{ "label", "body", "nested.text", "nested", "counter" }, &.{ "body", "nested.text" });
    defer a.free(projection.parquet);
    defer a.free(projection.sidecar);
    try std.testing.expectEqualSlices([]const u8, &.{ "label", "nested", "counter" }, projection.parquet);
    try std.testing.expectEqualSlices([]const u8, &.{ "body", "nested.text" }, projection.sidecar);
}

test "external lake sidecar hydration binds identities and preserves typed fields under allocation faults" {
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
    try std.testing.checkAllAllocationFailures(a, Run.run, .{writer.snapshot()});
    var row: std.json.Value = .{ .object = .empty };
    defer local.storage_db_types.deinitJsonValue(a, &row);
    try std.testing.expectError(error.ExternalLakeSnapshotMismatch, appendStored(a, writer.snapshot(), 0, "different-generation-row", &.{"body"}, &row));
    try std.testing.expectError(error.InvalidNativeLakeTextCorpus, appendStored(a, writer.snapshot(), 0, "row", &.{"missing"}, &row));
}

test "external lake ranked sidecar hydration reuses bounded decoded blocks across segments" {
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
