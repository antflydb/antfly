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

//! Extend archive membership without asking its physical-row evaluator to
//! resolve WAL document numbers. Archive evaluation remains lazy and bounded.
const std = @import("std");
const local = @import("antfly_local_sources");
const A = std.mem.Allocator;
const Bitmap = local.encoding_roaring.RoaringBitmap;
const Predicate = local.storage_db_query_search_exec.IndexedTextPredicate;
const Context = local.serverless_query_lake_read_context.Context;

/// Transfers predicate and recent only on success. The archive occupies
/// [0, archive_docs); recent numbers belong to the appended immutable segments.
pub fn append(a: A, predicate: *Predicate, archive_docs: u32, recent: *Bitmap, context: Context) !void {
    try context.ensureActive();
    if (predicate.producer == null) {
        try predicate.bitmap.orWith(recent);
        return;
    }
    const owner = try a.create(Owner);
    owner.* = .{ .a = a, .archive = predicate.*, .archive_docs = archive_docs, .recent = recent.*, .context = context };
    recent.* = Bitmap.init(a);
    predicate.* = .{
        .bitmap = Bitmap.init(a),
        .exact = owner.archive.exact,
        .producer = .{
            .ptr = owner,
            .produce = Owner.produce,
            .contains = Owner.contains,
            .contains_many = Owner.containsMany,
            // The archive's materialized/borrowed bitmap is not complete membership
            // for the joined snapshot. Never expose it through these shortcuts.
        },
        .owner = owner,
        .close = Owner.close,
    };
}

const Owner = struct {
    a: A,
    archive: Predicate,
    archive_docs: u32,
    recent: Bitmap,
    context: Context,

    fn close(raw: *anyopaque) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.archive.deinit();
        self.recent.deinit();
        self.a.destroy(self);
    }
    fn contains(raw: *anyopaque, a: A, doc: u32) !bool {
        var allowed: [1]bool = undefined;
        try containsMany(raw, a, &.{doc}, &allowed);
        return allowed[0];
    }
    fn containsMany(raw: *anyopaque, a: A, docs: []const u32, allowed: []bool) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (docs.len != allowed.len) return error.InvalidQueryRequest;
        try self.context.ensureActive();
        // Preserve the archive provider's batched membership contract and keep
        // transient storage independent of the incoming candidate population.
        var first: usize = 0;
        while (first < docs.len) {
            try self.context.ensureActive();
            const count = @min(128, docs.len - first);
            var archive_docs: [128]u32 = undefined;
            var positions: [128]usize = undefined;
            var matches: [128]bool = undefined;
            var n: usize = 0;
            for (docs[first..][0..count], first..) |doc, i| {
                if (doc < self.archive_docs) {
                    archive_docs[n] = doc;
                    positions[n] = i;
                    n += 1;
                } else allowed[i] = self.recent.contains(doc);
            }
            const producer = self.archive.producer.?;
            if (n != 0) {
                if (producer.contains_many) |check| {
                    try check(producer.ptr, a, archive_docs[0..n], matches[0..n]);
                } else for (archive_docs[0..n], matches[0..n]) |doc, *keep| {
                    if (producer.contains) |check| {
                        keep.* = try check(producer.ptr, a, doc);
                    } else {
                        var selected = try producer.produce(producer.ptr, a, doc, 1, null);
                        defer selected.deinit();
                        keep.* = selected.contains(0);
                    }
                }
                for (positions[0..n], matches[0..n]) |i, keep| allowed[i] = keep;
            }
            first += count;
        }
    }
    fn produce(raw: *anyopaque, a: A, offset: u32, count: u32, candidates: ?*const Bitmap) !Bitmap {
        const self: *@This() = @ptrCast(@alignCast(raw));
        try self.context.ensureActive();
        var result = try self.recent.sliceRebased(a, offset, @as(u64, offset) + count);
        errdefer result.deinit();
        if (candidates) |selected| result.andWith(selected);
        if (offset < self.archive_docs and count != 0) {
            const archive_count = @min(count, self.archive_docs - offset);
            var selected = if (candidates) |value| try value.sliceRebased(a, 0, archive_count) else null;
            defer if (selected) |*value| value.deinit();
            if (selected == null or selected.?.cardinality() != 0) {
                const producer = self.archive.producer.?;
                var archive = try producer.produce(producer.ptr, a, offset, archive_count, if (selected) |*value| value else null);
                defer archive.deinit();
                try result.orWith(&archive);
            }
        }
        return result;
    }
};

test "external lake recent predicate retains lazy archive membership and bounds" {
    const a = std.testing.allocator;
    const Fake = struct {
        closed: bool = false,
        calls: usize = 0,
        fn close(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.closed = true;
        }
        fn produce(raw: *anyopaque, alloc: A, offset: u32, count: u32, candidates: ?*const Bitmap) !Bitmap {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            if (@as(u64, offset) + count > 8) return error.InvalidQueryRequest;
            var result = Bitmap.init(alloc);
            errdefer result.deinit();
            for ([_]u32{ 1, 4 }) |doc| if (doc >= offset and doc - offset < count) {
                if (candidates == null or candidates.?.contains(doc - offset)) try result.add(doc - offset);
            };
            return result;
        }
    };
    var fake: Fake = .{};
    var predicate: Predicate = .{ .bitmap = Bitmap.init(a), .producer = .{ .ptr = &fake, .produce = Fake.produce }, .owner = &fake, .close = Fake.close };
    var predicate_owned = true;
    defer if (predicate_owned) predicate.deinit();
    var recent = Bitmap.init(a);
    defer recent.deinit();
    try recent.add(8);
    try recent.add(10);
    try append(a, &predicate, 8, &recent, .{});
    try std.testing.expectEqual(@as(usize, 0), fake.calls);
    const producer = predicate.producer.?;
    try std.testing.expect(producer.materialized == null and producer.borrow == null);
    var allowed: [7]bool = undefined;
    try producer.contains_many.?(producer.ptr, a, &.{ 10, 4, 8, 0, 9, 1, 8 }, &allowed);
    try std.testing.expectEqualSlices(bool, &.{ true, true, true, false, false, true, true }, &allowed);
    var candidates = Bitmap.init(a);
    defer candidates.deinit();
    for ([_]u32{ 0, 3, 4, 5, 6 }) |doc| try candidates.add(doc);
    var selected = try producer.produce(producer.ptr, a, 4, 7, &candidates);
    defer selected.deinit();
    try std.testing.expectEqual(@as(u64, 3), selected.cardinality());
    for ([_]u32{ 0, 4, 6 }) |doc| try std.testing.expect(selected.contains(doc));
    var all = try producer.produce(producer.ptr, a, 0, 11, null);
    defer all.deinit();
    try std.testing.expectEqual(@as(u64, 4), all.cardinality());
    try std.testing.expect(try producer.contains.?(producer.ptr, a, 10));
    const owner: *Owner = @ptrCast(@alignCast(predicate.owner.?));
    owner.context.deadline_ns = 1;
    try std.testing.expectError(error.DeadlineExceeded, producer.produce(producer.ptr, a, 8, 3, null));
    try std.testing.expectError(error.DeadlineExceeded, producer.contains.?(producer.ptr, a, 10));
    predicate.deinit();
    predicate_owned = false;
    try std.testing.expect(fake.closed);
}

test "external lake recent predicate eager and canceled admission preserve ownership" {
    const a = std.testing.allocator;
    var predicate: Predicate = .{ .bitmap = Bitmap.init(a) };
    defer predicate.deinit();
    try predicate.bitmap.add(1);
    var recent = Bitmap.init(a);
    defer recent.deinit();
    try recent.add(8);
    try std.testing.expectError(error.DeadlineExceeded, append(a, &predicate, 8, &recent, .{ .deadline_ns = 1 }));
    try std.testing.expectEqual(@as(u64, 1), predicate.bitmap.cardinality());
    try std.testing.expectEqual(@as(u64, 1), recent.cardinality());
    try append(a, &predicate, 8, &recent, .{});
    try std.testing.expect(predicate.producer == null);
    try std.testing.expectEqual(@as(u64, 2), predicate.bitmap.cardinality());
    try std.testing.expect(predicate.bitmap.contains(1) and predicate.bitmap.contains(8));
}
