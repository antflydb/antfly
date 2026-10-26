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

//! A bounded recent-write view, independent of full namespace materialization.
//! The caller pins its published generation and captures one WAL cutoff. This
//! view owns only the latest tail mutation per identity. Tombstones suppress
//! published candidates; fresh inserts remain eligible for merge/refill.
const std = @import("std");
const A = std.mem.Allocator;
const wal = @import("../wal/types.zig");
const codec = @import("../api/codec.zig");
const Mutation = @import("../api/types.zig").DocumentMutation;

pub const Overlay = struct {
    alloc: A,
    by_id: std.StringHashMapUnmanaged(Mutation) = .empty,
    ordered: []const []const u8 = &.{},
    cutoff: u64,
    bytes: usize = 0,
    max_bytes: usize = 0,
    resolver: ?BodyResolver = null,
    pub const BodyResolver = struct { ptr: *anyopaque, resolve: *const fn (*anyopaque, A, []const u8) anyerror![]u8 };
    pub const Lookup = union(enum) { unchanged, deleted, present: []const u8 };

    pub fn init(alloc: A, records: []const wal.Record, published_lsn: u64, cutoff: u64, max_bytes: usize) !Overlay {
        return initWithResolver(alloc, records, published_lsn, cutoff, max_bytes, null);
    }
    pub fn initWithResolver(alloc: A, records: []const wal.Record, published_lsn: u64, cutoff: u64, max_bytes: usize, resolver: ?BodyResolver) !Overlay {
        if (cutoff < published_lsn or records.len != cutoff - published_lsn) return error.InvalidWal;
        var self = Overlay{ .alloc = alloc, .cutoff = cutoff, .max_bytes = max_bytes, .resolver = resolver };
        errdefer self.deinit();
        for (records, 0..) |record, i| {
            if (record.lsn != published_lsn + i + 1) return error.InvalidWal;
            // Charge every admitted mutation before decoding. A chain of
            // updates cannot hide unbounded parsing/allocator work.
            self.bytes = std.math.add(usize, self.bytes, record.payload.len + @sizeOf(Mutation) + 64) catch return error.WalReadBudgetExceeded;
            if (self.bytes > max_bytes) return error.WalReadBudgetExceeded;
            var mutation = try codec.decodeMutationAlloc(alloc, record.payload);
            errdefer mutation.deinit(alloc);
            const old = try self.by_id.getOrPut(alloc, mutation.doc_id);
            if (old.found_existing) {
                // Keep the map's existing owned key stable while replacing its
                // value. The incoming duplicate identity is not retained.
                const key = old.value_ptr.doc_id;
                alloc.free(mutation.doc_id);
                if (old.value_ptr.body) |body| alloc.free(body);
                mutation.doc_id = key;
            }
            old.value_ptr.* = mutation;
        }
        const ordered = try alloc.alloc([]const u8, self.by_id.count());
        self.ordered = ordered;
        var iterator = self.by_id.keyIterator();
        var i: usize = 0;
        while (iterator.next()) |key| : (i += 1) ordered[i] = key.*;
        std.mem.sort([]const u8, ordered, {}, struct {
            fn less(_: void, l: []const u8, r: []const u8) bool {
                return std.mem.order(u8, l, r) == .lt;
            }
        }.less);
        return self;
    }
    pub fn deinit(self: *Overlay) void {
        self.alloc.free(self.ordered);
        var it = self.by_id.valueIterator();
        while (it.next()) |mutation| mutation.deinit(self.alloc);
        self.by_id.deinit(self.alloc);
        self.* = undefined;
    }
    pub fn lookup(self: *Overlay, id: []const u8) !Lookup {
        const mutation = self.by_id.getPtr(id) orelse return .unchanged;
        if (mutation.kind == .delete) return .deleted;
        if (mutation.kind == .checkpoint or mutation.kind == .enriched) {
            const raw = mutation.body orelse return error.InvalidDocumentBody;
            const resolved = if (mutation.kind == .enriched) resolved: {
                const body = try @import("../build/document_facts.zig").parseEnrichedBody(raw);
                break :resolved try self.alloc.dupe(u8, body.body);
            } else resolved: {
                const resolver = self.resolver orelse return error.WalBodyResolverRequired;
                break :resolved try resolver.resolve(resolver.ptr, self.alloc, raw);
            };
            errdefer self.alloc.free(resolved);
            const bytes = std.math.add(usize, self.bytes, resolved.len) catch return error.WalReadBudgetExceeded;
            if (bytes > self.max_bytes) return error.WalReadBudgetExceeded;
            self.alloc.free(raw);
            mutation.body = resolved;
            mutation.kind = .upsert;
            self.bytes = bytes;
        }
        return .{ .present = mutation.body orelse "" };
    }

    pub const Document = struct { doc_id: []const u8, body: []const u8 };
    /// Cursor is already positioned strictly after the caller's resume key.
    /// next returns borrowed bytes valid until the next call. It must yield
    /// identities in ascending order from the pinned published generation.
    pub const BaseCursor = struct {
        ptr: *anyopaque,
        next: *const fn (*anyopaque) anyerror!?Document,
    };
    pub const Page = struct {
        documents: []Document,
        pub fn deinit(self: *Page, alloc: A) void {
            for (self.documents) |doc| {
                alloc.free(doc.doc_id);
                alloc.free(doc.body);
            }
            alloc.free(self.documents);
        }
    };
    pub const ListOptions = struct {
        max_scanned: usize = 16384,
        cancellation: ?@import("objectstore").CancellationToken = null,
    };
    /// Merge before filtering/limiting, so deleted/updated candidates cannot
    /// consume result slots. An application predicate must inspect the current
    /// body (including current authorization), not stale indexed metadata.
    pub fn list(self: *Overlay, cursor: BaseCursor, after: []const u8, limit: usize, max_output_bytes: usize, predicate: anytype, options: ListOptions) !Page {
        if (limit == 0 or limit > 4096) return error.InvalidPageSize;
        var out: std.ArrayListUnmanaged(Document) = .empty;
        errdefer {
            for (out.items) |doc| {
                self.alloc.free(doc.doc_id);
                self.alloc.free(doc.body);
            }
            out.deinit(self.alloc);
        }
        var index: usize = 0;
        while (index < self.ordered.len and std.mem.order(u8, self.ordered[index], after) != .gt) index += 1;
        var base = try cursor.next(cursor.ptr);
        var bytes: usize = 0;
        var scanned: usize = 0;
        while (out.items.len < limit) {
            if (options.cancellation) |token| try token.check();
            if (scanned >= options.max_scanned) return error.QueryCandidateBudgetExceeded;
            scanned += 1;
            const key = if (index < self.ordered.len) self.ordered[index] else null;
            if (base == null and key == null) break;
            const use_base = base != null and (key == null or std.mem.order(u8, base.?.doc_id, key.?) == .lt);
            var candidate: ?Document = null;
            var advance_base = false;
            if (use_base) {
                if (std.mem.order(u8, base.?.doc_id, after) != .gt) return error.InvalidOverlayBaseOrder;
                candidate = base;
                advance_base = true;
            } else {
                const id = key.?;
                if (base != null and std.mem.eql(u8, base.?.doc_id, id)) advance_base = true;
                switch (try self.lookup(id)) {
                    .present => |body| candidate = .{ .doc_id = id, .body = body },
                    .deleted => {},
                    .unchanged => unreachable,
                }
                index += 1;
            }
            if (candidate) |doc| if (try predicate.matches(doc)) {
                bytes = std.math.add(usize, bytes, doc.doc_id.len + doc.body.len + @sizeOf(Document)) catch return error.WalReadBudgetExceeded;
                if (bytes > max_output_bytes) return error.WalReadBudgetExceeded;
                const id = try self.alloc.dupe(u8, doc.doc_id);
                errdefer self.alloc.free(id);
                const body = try self.alloc.dupe(u8, doc.body);
                errdefer self.alloc.free(body);
                try out.append(self.alloc, .{ .doc_id = id, .body = body });
            };
            if (advance_base and out.items.len < limit) base = try cursor.next(cursor.ptr);
        }
        return .{ .documents = try out.toOwnedSlice(self.alloc) };
    }
};

test "serverless live overlay refills pages, admits new matches and suppresses deletes and stale metadata" {
    const a = std.testing.allocator;
    var records: [4]wal.Record = undefined;
    const mutations = [_]Mutation{
        .{ .kind = .delete, .doc_id = "a" },
        .{ .kind = .upsert, .doc_id = "b", .body = "denied" },
        .{ .kind = .upsert, .doc_id = "c", .body = "match" },
        .{ .kind = .upsert, .doc_id = "c", .body = "allowed" },
    };
    for (&records, mutations, 0..) |*record, mutation, i| record.* = .{ .lsn = i + 11, .timestamp_ns = 0, .payload = try codec.encodeMutationAlloc(a, mutation) };
    defer for (records) |record| a.free(record.payload);
    var overlay = try Overlay.init(a, &records, 10, 14, 4096);
    defer overlay.deinit();
    const Base = struct {
        index: usize = 0,
        fn next(raw: *anyopaque) !?Overlay.Document {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const docs = [_]Overlay.Document{ .{ .doc_id = "a", .body = "allowed" }, .{ .doc_id = "b", .body = "allowed" }, .{ .doc_id = "d", .body = "allowed" } };
            if (self.index == docs.len) return null;
            defer self.index += 1;
            return docs[self.index];
        }
    };
    const Predicate = struct {
        pub fn matches(_: @This(), doc: Overlay.Document) !bool {
            return std.mem.eql(u8, doc.body, "allowed");
        }
    };
    var base = Base{};
    var page = try overlay.list(.{ .ptr = &base, .next = Base.next }, "", 2, 4096, Predicate{}, .{});
    defer page.deinit(a);
    try std.testing.expectEqual(@as(usize, 2), page.documents.len);
    try std.testing.expectEqualStrings("c", page.documents[0].doc_id);
    try std.testing.expectEqualStrings("d", page.documents[1].doc_id);
    try std.testing.expect(try overlay.lookup("a") == .deleted);
    try std.testing.expectError(error.WalReadBudgetExceeded, Overlay.init(a, &records, 10, 14, 1));
    try std.testing.expectError(error.InvalidWal, Overlay.init(a, &records, 10, 15, 4096));
}

test "serverless live overlay releases every partial allocation" {
    const Run = struct {
        fn run(a: A) !void {
            const source = std.testing.allocator;
            var items: [3]wal.Record = undefined;
            const input = [_]Mutation{ .{ .kind = .upsert, .doc_id = "a", .body = "one" }, .{ .kind = .upsert, .doc_id = "a", .body = "two" }, .{ .kind = .delete, .doc_id = "b" } };
            for (&items, input, 0..) |*record, mutation, i| record.* = .{ .lsn = i + 1, .timestamp_ns = 0, .payload = try codec.encodeMutationAlloc(source, mutation) };
            defer for (items) |record| source.free(record.payload);
            var overlay = try Overlay.init(a, &items, 0, 3, 4096);
            defer overlay.deinit();
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Run.run, .{});
}

test "serverless live overlay resolves derived bodies lazily once within its admitted byte budget" {
    const Run = struct {
        fn resolve(raw: *anyopaque, a: A, _: []const u8) ![]u8 {
            const count: *usize = @ptrCast(@alignCast(raw));
            count.* += 1;
            return a.dupe(u8, "resolved");
        }
        fn run(a: A) !void {
            const enriched = try @import("../build/document_facts.zig").encodeEnrichedBodyAlloc(a, "public", 1000);
            defer a.free(enriched);
            const checkpoint = try codec.encodeMutationAlloc(a, .{ .kind = .checkpoint, .doc_id = "a", .body = "internal" });
            defer a.free(checkpoint);
            const derived = try codec.encodeMutationAlloc(a, .{ .kind = .enriched, .doc_id = "b", .body = enriched });
            defer a.free(derived);
            const records = [_]wal.Record{ .{ .lsn = 1, .timestamp_ns = 0, .payload = checkpoint }, .{ .lsn = 2, .timestamp_ns = 0, .payload = derived } };
            var calls: usize = 0;
            var overlay = try Overlay.initWithResolver(a, &records, 0, 2, 4096, .{ .ptr = &calls, .resolve = resolve });
            defer overlay.deinit();
            try std.testing.expectEqual(@as(usize, 0), calls);
            try std.testing.expectEqual(@as(usize, 2), overlay.by_id.count());
            try std.testing.expectEqualStrings("public", (try overlay.lookup("b")).present);
            try std.testing.expectEqual(@as(usize, 0), calls);
            try std.testing.expectEqualStrings("resolved", (try overlay.lookup("a")).present);
            try std.testing.expectEqualStrings("resolved", (try overlay.lookup("a")).present);
            try std.testing.expectEqual(@as(usize, 1), calls);
        }
    };
    try Run.run(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Run.run, .{});
}
