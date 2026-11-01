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

//! Authenticated document -> term/frequency evidence. Native encoders emit
//! postings directly into a bounded shared-run sort; sparse retractions seek
//! changed documents and mutate only reached term-statistics paths.
const std = @import("std");
const local = @import("antfly_local_sources");
const stats = @import("lake_index_text_statistics.zig");
const tree = @import("../serverless/graph_segment/page_tree.zig");
const Pages = @import("../serverless/graph_segment/page_store.zig").PageStore;
const Mutations = @import("lake_index_page_mutations.zig").Mutations;
const stores = @import("../serverless/artifacts/store.zig");
const A = std.mem.Allocator;
const Token = @import("antfly_cancellation").CancellationToken;
pub fn publish(a: A, store: *stores.ArtifactStore, sort: *local.sql_spill.Sort, token: Token) !?tree.Ref {
    const Source = struct {
        sort: *local.sql_spill.Sort,
        scratch: std.heap.ArenaAllocator,
        pub fn next(self: *@This()) !?tree.Cursor.Record {
            _ = self.scratch.reset(.free_all);
            const row = (try self.sort.next(self.scratch.allocator())) orelse return null;
            if (row.keys.len != 1 or row.values.len != 1 or row.keys[0].value != .string or row.values[0].value != .string or row.values[0].value.string.len != 4) return error.InvalidNativeLakeTextStatistics;
            return .{ .key = row.keys[0].value.string, .value = row.values[0].value.string };
        }
    };
    var source: Source = .{ .sort = sort, .scratch = .init(a) };
    defer source.scratch.deinit();
    var reads: u64 = 0;
    var writes: u64 = 0;
    const scope = store.upload_scope.?;
    var pages: Pages = .{ .domain = scope.domain, .attempt = scope.attempt, .artifacts = store, .cancellation = token, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes, .streaming = true };
    pages.admitPublication();
    return tree.buildSortedWithOptions(a, pages.store(), &source, .{ .target_bytes = tree.bulk_target_page_bytes });
}
pub const Metrics = struct { documents: u64 = 0, entries: u64 = 0, terms: u64 = 0 };
pub fn retract(a: A, store: *stores.ArtifactStore, manager: *local.sql_spill.Manager, forward: ?tree.Ref, summary: ?tree.Ref, deleted: *const local.encoding_roaring.RoaringBitmap, lengths: *std.StringHashMapUnmanaged(u64), token: Token) !?tree.Ref {
    return retractMeasured(a, store, manager, forward, summary, deleted, lengths, token, null);
}
pub fn retractMeasured(a: A, store: *stores.ArtifactStore, manager: *local.sql_spill.Manager, forward: ?tree.Ref, summary: ?tree.Ref, deleted: *const local.encoding_roaring.RoaringBitmap, lengths: *std.StringHashMapUnmanaged(u64), token: Token, metrics: ?*Metrics) !?tree.Ref {
    var reads: u64 = 0;
    var writes: u64 = 0;
    const scope = store.upload_scope.?;
    var pages: Pages = .{ .domain = scope.domain, .attempt = scope.attempt, .artifacts = store, .cancellation = token, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes, .streaming = true };
    pages.admitPublication();
    var cache: tree.Cache = .{ .decoded = true, .alloc = local.sql_memory_budget.reclaimingAllocator(a), .underlying = pages.store() };
    defer cache.deinit();
    const storage = cache.store();
    var counts: std.StringHashMapUnmanaged(u32) = .empty;
    defer {
        var it = counts.keyIterator();
        while (it.next()) |key| a.free(key.*);
        counts.deinit(a);
    }
    var docs = deleted.iterator();
    while (docs.next()) |doc| {
        if (metrics) |value| value.documents += 1;
        try token.check();
        var lower: [4]u8 = undefined;
        std.mem.writeInt(u32, &lower, doc, .big);
        var upper: [4]u8 = undefined;
        if (doc != std.math.maxInt(u32)) std.mem.writeInt(u32, &upper, doc + 1, .big);
        var cursor = try tree.Cursor.init(a, storage, forward, &lower, if (doc == std.math.maxInt(u32)) null else &upper);
        defer cursor.deinit();
        while (try cursor.next()) |row| {
            if (metrics) |value| value.entries += 1;
            if (row.key.len < 8 or row.value.len != 4) return error.InvalidNativeLakeTextStatistics;
            const key = row.key[4..];
            const size = std.mem.readInt(u32, key[0..4], .big);
            if (size > key.len - 4) return error.InvalidNativeLakeTextStatistics;
            const frequency = std.mem.readInt(u32, row.value[0..4], .big);
            if (frequency == 0) return error.InvalidNativeLakeTextStatistics;
            const existing = try counts.getOrPut(a, key);
            if (!existing.found_existing) {
                existing.key_ptr.* = a.dupe(u8, key) catch |err| {
                    _ = counts.remove(key);
                    return err;
                };
                existing.value_ptr.* = 0;
            }
            existing.value_ptr.* = try std.math.add(u32, existing.value_ptr.*, 1);
            const field = key[4..][0..size];
            const length = try lengths.getOrPut(a, field);
            if (!length.found_existing) {
                length.key_ptr.* = a.dupe(u8, field) catch |err| {
                    _ = lengths.remove(field);
                    return err;
                };
                length.value_ptr.* = 0;
            }
            length.value_ptr.* = try std.math.add(u64, length.value_ptr.*, frequency);
        }
    }
    var changes = Mutations.init(a, manager, storage);
    defer changes.deinit();
    const terms = try a.alloc([]const u8, counts.count());
    defer a.free(terms);
    var it = counts.keyIterator();
    var n: usize = 0;
    while (it.next()) |term| {
        terms[n] = term.*;
        n += 1;
    }
    std.mem.sort([]const u8, terms, {}, struct {
        fn less(_: void, l: []const u8, r: []const u8) bool {
            return std.mem.order(u8, l, r) == .lt;
        }
    }.less);
    var begin: usize = 0;
    while (begin < terms.len) {
        var scratch = std.heap.ArenaAllocator.init(local.sql_memory_budget.reclaimingAllocator(a));
        defer scratch.deinit();
        var end = begin;
        var bytes: usize = 0;
        while (end < terms.len and end - begin < 64) : (end += 1) {
            if (end != begin and bytes + terms[end].len > 512 * 1024) break;
            bytes += terms[end].len;
        }
        const found = try tree.lookupMany(scratch.allocator(), storage, summary, terms[begin..end]);
        for (found, terms[begin..end]) |entry, term| {
            if (metrics) |value| value.terms += 1;
            const row = entry orelse return error.InvalidNativeLakeTextStatistics;
            var value = try stats.Summary.decode(row.value);
            value.frequency = try std.math.sub(u32, value.frequency, counts.get(term).?);
            var encoded: [stats.Summary.max_bytes]u8 = undefined;
            try changes.addRecord(term, if (value.frequency == 0) null else value.encode(&encoded));
        }
        begin = end;
    }
    return changes.finish(summary);
}

test "external lake forward text retractions seek changed terms and match complete statistics" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const out = arena.allocator();
    var dir = try local.common_test_directory.TestDirectory.init("forward-text-retractions");
    defer dir.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, dir.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    store.upload_scope = try stores.UploadScope.forPublication(@splat(7), 1, std.testing.io);
    var marker: u8 = 0;
    var manager: local.sql_spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &marker, .checkpoint = struct {
        fn check(_: *anyopaque) !void {}
    }.check, .async_writes = false };
    defer manager.deinit();
    const count = 128;
    var docs: [count]local.introducer.TextDocument = undefined;
    for (&docs, 0..) |*doc, i| doc.* = .{
        .id = try std.fmt.allocPrint(out, "{d:0>4}", .{i}),
        .stored_data = "{}",
        .text_fields = try out.dupe(local.introducer.TextField, &.{.{ .field_name = "body", .text = try std.fmt.allocPrint(out, "common common unique{c}{c}", .{ @as(u8, @intCast('a' + i / 26)), @as(u8, @intCast('a' + i % 26)) }) }}),
    };
    const staged = try @import("lake_index_segment_spill.zig").LocalSegment.empty(a, &manager, &store);
    defer staged.release();
    staged.captureStatistics(&manager);
    var sink = staged.sink();
    try local.storage_db_document_mapper.writeTextSegmentFromProjectionBatchToSink(a, .{ .docs = &docs }, .{}, .{}, &sink);
    try staged.seal();
    const captured = staged.statistics.?;
    const before = try captured.publishConsuming(&store, .none, std.testing.io, stats.NoContributions{});
    const forward = try captured.publishForward(&store, .none);
    try std.testing.expect(!captured.forward_disabled and forward != null);
    var reader = try local.segment.SegmentReader.initSource(a, staged.source());
    defer reader.deinit();
    var deleted: local.encoding_roaring.RoaringBitmap = .init(a);
    defer deleted.deinit();
    try deleted.add(7);
    var lengths: std.StringHashMapUnmanaged(u64) = .empty;
    defer {
        var fields = lengths.keyIterator();
        while (fields.next()) |field| a.free(field.*);
        lengths.deinit(a);
    }
    var metrics: Metrics = .{};
    const after = try retractMeasured(a, &store, &manager, forward, before, &deleted, &lengths, .none, &metrics);
    try std.testing.expectEqual(@as(u64, 1), metrics.documents);
    try std.testing.expectEqual(@as(u64, 2), metrics.entries);
    try std.testing.expectEqual(@as(u64, 2), metrics.terms);
    try std.testing.expectEqual(@as(?u64, 3), lengths.get("body"));
    var expected_lengths: std.StringHashMapUnmanaged(u64) = .empty;
    defer expected_lengths.deinit(out);
    const expected = try stats.publishReaderFiltered(out, &store, &reader, .none, std.testing.io, stats.NoContributions{}, &deleted, &expected_lengths);
    var reads: u64 = 1024 * 1024;
    var writes: u64 = 0;
    var pages: Pages = .{ .domain = store.upload_scope.?.domain, .artifacts = &store, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes };
    try std.testing.expect(try tree.equivalent(a, pages.store(), expected, after, .{}));
    // The native merge supplies the authenticated row mapping. Reuse must
    // preserve frequencies through deletion and arbitrary document reordering.
    const oracle = try @import("lake_index_segment_spill.zig").LocalSegment.empty(a, &manager, &store);
    defer oracle.release();
    oracle.captureStatistics(&manager);
    var mapped = try local.segment.MergeSourceMap.init(a, &.{reader.doc_count});
    defer mapped.deinit();
    var sink2 = oracle.sink();
    try local.segment.writeMergedSegmentToSinkWithOptions(a, &sink2, &.{.{ .reader = &reader, .deleted = deleted }}, .{ .input_order = .arbitrary, .index_sort = &.{.{ .field = "_id", .desc = true }}, .source_map = &mapped });
    try mapped.finishOutput(count - 1);
    try oracle.seal();
    const oracle_forward = try oracle.statistics.?.publishForward(&store, .none);
    for ([_]stats.Captured.ForwardInput{ .{ .captured = captured }, .{ .tree = forward } }) |input| {
        const reused = try stats.Captured.create(a, &manager);
        defer reused.deinit();
        try reused.reuseForward(&store, input, 0, &mapped, .none);
        try std.testing.expectEqual(@as(u64, (count - 1) * 2), reused.reused_forward_entries);
        const rewritten = try reused.publishForward(&store, .none);
        reads = 8 * 1024 * 1024;
        try std.testing.expect(try tree.equivalent(a, pages.store(), oracle_forward, rewritten, .{}));
        try std.testing.expect(reused.forward_budget.peak <= reused.forward_budget.limit);
    }
    // Append maps preserve ordering across producer boundaries, including a
    // reused source sort that was rewound after the preceding arbitrary merge.
    var append = try local.segment.MergeSourceMap.init(a, &.{ count, count });
    defer append.deinit();
    try append.recordRange(0, 0, count, 0);
    try append.recordRange(1, 0, count, count);
    try append.finishOutput(count * 2);
    const ordered = try stats.Captured.create(a, &manager);
    defer ordered.deinit();
    ordered.orderedForward();
    const sorted = try stats.Captured.create(a, &manager);
    defer sorted.deinit();
    for (0..2) |index| {
        try ordered.reuseForward(&store, .{ .captured = captured }, index, &append, .none);
        try sorted.reuseForward(&store, .{ .tree = forward }, index, &append, .none);
    }
    const merge_rows = manager.merged_rows;
    const ordered_root = try ordered.publishForward(&store, .none);
    try std.testing.expectEqual(merge_rows, manager.merged_rows);
    try std.testing.expect(ordered.forward_sort.?.presorted);
    const sorted_root = try sorted.publishForward(&store, .none);
    reads = 8 * 1024 * 1024;
    try std.testing.expect(try tree.equivalent(a, pages.store(), sorted_root, ordered_root, .{}));
    // Reusing the same output document range violates the ordered contract.
    try std.testing.expectError(error.InvalidNativeLakeTextStatistics, ordered.reuseForward(&store, .{ .tree = forward }, 0, &append, .none));
    const FailureCase = struct {
        fn run(failing: A, backend: *stores.ArtifactStore, evidence: ?tree.Ref, original: ?tree.Ref, retired: *const local.encoding_roaring.RoaringBitmap) !void {
            var failure_token: u8 = 0;
            var owner: local.sql_spill.Manager = .{ .alloc = failing, .io = std.testing.io, .context = &failure_token, .checkpoint = struct {
                fn check(_: *anyopaque) !void {}
            }.check, .async_writes = false };
            defer owner.deinit();
            var removed: std.StringHashMapUnmanaged(u64) = .empty;
            defer {
                var fields = removed.keyIterator();
                while (fields.next()) |field| failing.free(field.*);
                removed.deinit(failing);
            }
            _ = try retract(failing, backend, &owner, evidence, original, retired, &removed, .none);
        }
    };
    var probe = std.testing.FailingAllocator.init(a, .{});
    try FailureCase.run(probe.allocator(), &store, forward, before, &deleted);
    // Content-addressed writes can allocate less once the store is warm.
    // Still induce every observed failure position and verify exact cleanup.
    for (0..probe.alloc_index) |fail_index| {
        var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = fail_index });
        if (FailureCase.run(failing.allocator(), &store, forward, before, &deleted)) |_| {
            try std.testing.expect(!failing.has_induced_failure);
        } else |err| if (err != error.OutOfMemory) return err;
        try std.testing.expectEqual(failing.allocated_bytes, failing.freed_bytes);
    }
}
