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

//! Merge selected logical chunks with existing vector scopes. Including old
//! outputs makes empty/shrinking generations retire vectors instead of merely
//! ceasing to produce them. Only one key per input is retained.
const std = @import("std");
const keys = @import("../internal_keys.zig");
const logical = @import("artifact_chunk_cursor.zig");

pub const Budget = @import("artifact_scan_budget.zig").Budget;

pub const Step = union(enum) {
    member: []const u8,
    /// Inclusive physical resume position. No accepted member was skipped.
    yielded: []const u8,
    /// Encoded merge-input positions, including empty/ignored logical scopes.
    logical_yielded: []const u8,
    end,
};

pub fn Cursor(comptime Txn: type) type {
    return struct {
        const Self = @This();
        alloc: std.mem.Allocator,
        chunks: logical.Cursor(Txn),
        outputs: Txn.CursorAdapter,
        prefix: []u8,
        embedding: []const u8,
        chunk: ?[]u8 = null,
        output: ?[]u8 = null,
        output_entry: ?@import("../backend_erased.zig").Entry = null,
        started: bool = false,
        capture_window_hints: bool = false,
        output_has_windows: bool = false,
        member_has_windows: bool = false,
        result: ?[]u8 = null,
        logical_resume: ?[]u8 = null,

        pub fn open(alloc: std.mem.Allocator, txn: *Txn, document: []const u8, producer: []const u8, embedding: []const u8) !Self {
            var chunks = try logical.Cursor(Txn).openNamed(alloc, txn, document, producer);
            errdefer chunks.close();
            var outputs = try txn.openPhysicalCursorAdapter();
            errdefer outputs.close();
            const prefix = try keys.artifactNamedPrefixAlloc(alloc, document, "chunk", producer);
            return .{ .alloc = alloc, .chunks = chunks, .outputs = outputs, .prefix = prefix, .embedding = embedding };
        }

        pub fn enableWindowHints(self: *Self) void {
            self.capture_window_hints = true;
            self.outputs.setKeysOnly();
        }

        pub fn close(self: *Self) void {
            self.chunks.close();
            self.outputs.close();
            self.alloc.free(self.prefix);
            if (self.chunk) |value| self.alloc.free(value);
            if (self.output) |value| self.alloc.free(value);
            if (self.result) |value| self.alloc.free(value);
            if (self.logical_resume) |value| self.alloc.free(value);
            self.* = undefined;
        }

        pub fn seekAfter(self: *Self, key: []const u8) !void {
            try self.chunks.seekAfter(key);
            // The prefix successor skips the whole member subtree, including
            // binary embedding names and all consumer outputs.
            const upper = try keys.nextPrefixAlloc(self.alloc, key);
            defer if (upper) |value| self.alloc.free(value);
            self.output_entry = if (upper) |value| try self.outputs.seekAtOrAfter(value) else null;
            self.started = true;
            if (self.chunk) |value| self.alloc.free(value);
            self.chunk = null;
            if (self.output) |value| self.alloc.free(value);
            self.output = null;
        }

        pub fn resumePhysical(self: *Self, floor: []const u8) !void {
            if (!std.mem.startsWith(u8, floor, self.prefix) or floor.len == self.prefix.len) return error.InvalidBatchRequest;
            if (!self.started) {
                self.output_entry = try self.outputs.seekAtOrAfter(floor);
                self.started = true;
            } else if (self.output_entry) |row| if (std.mem.order(u8, row.key, floor) == .lt) {
                self.output_entry = try self.outputs.seekAtOrAfter(floor);
            };
        }

        pub fn resumeLogical(self: *Self, raw: []const u8) !void {
            try self.chunks.resumeFrom(raw);
        }

        pub fn checkpointLogicalAlloc(self: *const Self, alloc: std.mem.Allocator) ![]u8 {
            if (self.chunk != null) return self.chunks.checkpointBeforeReturnedAlloc(alloc);
            return self.chunks.checkpointAlloc(alloc);
        }

        /// Returned key is borrowed until the next call. The caller owns and
        /// closes the shared snapshot before invoking any external provider.
        pub fn next(self: *Self) !?[]const u8 {
            var budget: Budget = .{ .max_visits = std.math.maxInt(usize), .max_bytes = std.math.maxInt(usize) };
            return switch (try self.poll(&budget)) {
                .member => |key| key,
                .end => null,
                .yielded, .logical_yielded => unreachable,
            };
        }

        /// Physical tail inspection shares a page budget across calls. The
        /// returned resume position includes ignored/unrelated rows, so a
        /// page with no members still makes restartable forward progress.
        pub fn poll(self: *Self, budget: *Budget) !Step {
            if (self.result) |value| self.alloc.free(value);
            self.result = null;
            if (self.logical_resume) |value| self.alloc.free(value);
            self.logical_resume = null;
            if (!self.started) {
                self.output_entry = try self.outputs.seekAtOrAfter(self.prefix);
                self.started = true;
            }
            if (self.chunk == null) switch (try self.chunks.poll(budget)) {
                .row => |row| {
                    self.chunk = try self.alloc.dupe(u8, row.key);
                },
                .end => {},
                .yielded => {
                    self.logical_resume = try self.chunks.checkpointAlloc(self.alloc);
                    return .{ .logical_yielded = self.logical_resume.? };
                },
            };
            var physical_visits: usize = 0;
            while (self.output == null) {
                const row = self.output_entry orelse break;
                if (!std.mem.startsWith(u8, row.key, self.prefix)) {
                    self.output_entry = null;
                    break;
                }
                // A selected chunk already contributes this scope. Do not
                // scan the entire remaining physical tail looking for an
                // output greater than it; that would repeat at every page.
                if (self.chunk) |chunk| if (std.mem.order(u8, row.key, chunk) != .lt) break;
                // One physical step remains admissible after logical prefetch
                // consumed the budget. Otherwise every retry would refetch
                // the same member and yield at the same output forever.
                if (budget.exhausted() and physical_visits != 0) return .{ .yielded = row.key };
                physical_visits += 1;
                budget.visits +|= 1;
                budget.bytes +|= row.key.len +| row.value.len;
                if (keys.isDerivedEmbeddingArtifactKey(row.key) and keys.matchesDerivedEmbeddingArtifactName(row.key, self.embedding)) {
                    const base = (try keys.derivedEmbeddingBaseKeyAlloc(self.alloc, row.key)).?;
                    defer self.alloc.free(base);
                    const parent = keys.mediaWindowParent(base);
                    self.output = try self.alloc.dupe(u8, parent);
                    self.output_has_windows = self.capture_window_hints and parent.len != base.len;
                }
                self.output_entry = try self.outputs.next();
            }
            if (self.chunk == null and self.output == null) return .end;
            const order = if (self.chunk == null) std.math.Order.gt else if (self.output == null) std.math.Order.lt else std.mem.order(u8, self.chunk.?, self.output.?);
            self.member_has_windows = order != .lt and self.output_has_windows;
            if (order != .gt) {
                self.result = self.chunk;
                self.chunk = null;
                if (order == .eq) {
                    self.alloc.free(self.output.?);
                    self.output = null;
                }
            } else {
                self.result = self.output;
                self.output = null;
            }
            const member = self.result.?;
            // Use the existing ordered output scan to prove absence. Most
            // text members have at most their scalar row to step over.
            var visits: usize = 0;
            while (self.output_entry) |row| {
                if (!std.mem.startsWith(u8, row.key, member)) break;
                if (self.capture_window_hints) {
                    // A selected member must still advance when its budget
                    // expires. Unknown is conservative: the caller will
                    // capture the exact child set if this member needs a write.
                    if (!budget.exhausted() and !self.member_has_windows) {
                        budget.visit(row.key.len);
                        const window = row.key.len > member.len and row.key[member.len] == keys.media_window_kind;
                        self.member_has_windows = window;
                        visits += 1;
                        if (!window and visits < 3) {
                            self.output_entry = try self.outputs.next();
                            continue;
                        }
                        if (!window and row.key.len > member.len and row.key[member.len] < keys.media_window_kind) {
                            if (!budget.exhausted()) {
                                const lower = try keys.mediaWindowChildKeyAlloc(self.alloc, member, 0);
                                defer self.alloc.free(lower);
                                const found = try self.outputs.seekAtOrAfter(lower);
                                budget.visit(if (found) |value| value.key.len else lower.len);
                                self.member_has_windows = if (found) |value| std.mem.startsWith(u8, value.key, lower[0 .. lower.len - @sizeOf(u32)]) else false;
                                if (!self.member_has_windows) {
                                    self.output_entry = found;
                                    break;
                                }
                            } else self.member_has_windows = true;
                        }
                    } else self.member_has_windows = true;
                }
                const upper = try keys.nextPrefixAlloc(self.alloc, member);
                defer if (upper) |value| self.alloc.free(value);
                self.output_entry = if (upper) |value| try self.outputs.seekAtOrAfter(value) else null;
                if (self.capture_window_hints) budget.visit(if (self.output_entry) |value| value.key.len else member.len);
                break;
            }
            budget.visits +|= 1;
            budget.bytes +|= self.result.?.len;
            return .{ .member = self.result.? };
        }
    };
}

test "model aware media window hint lookahead charges its budget and still advances" {
    const a = std.testing.allocator;
    const backend_erased = @import("../backend_erased.zig");
    var backend = @import("../mem_backend.zig").Backend.init(a, .{});
    defer backend.close();
    var store = try backend.runtimeStore(a, .{ .name = "window-hint-budget" });
    defer store.deinit();
    var erased = try backend_erased.storeFrom(a, store);
    defer erased.deinit();
    const member = try keys.chunkArtifactKeyAlloc(a, "doc", "text", 0);
    defer a.free(member);
    const scalar = try keys.derivedEmbeddingArtifactKeyAlloc(a, member, "model");
    defer a.free(scalar);
    {
        var write = try erased.beginWrite();
        errdefer write.abort();
        try write.put(member, "{\"body\":\"text\"}");
        try write.put(scalar, "vector");
        try write.commit();
    }
    var read = try erased.beginRead();
    defer read.abort();
    var borrowed: logical.BorrowedRead = .{ .read = &read };
    const C = Cursor(logical.BorrowedRead);
    var plain = try C.open(a, &borrowed, "doc", "text", "model");
    defer plain.close();
    var hinted = try C.open(a, &borrowed, "doc", "text", "model");
    defer hinted.close();
    hinted.enableWindowHints();
    var plain_budget: Budget = .{};
    var budget: Budget = .{};
    try std.testing.expectEqualStrings(member, (try plain.poll(&plain_budget)).member);
    try std.testing.expectEqualStrings(member, (try hinted.poll(&budget)).member);
    try std.testing.expect(!hinted.member_has_windows);
    try std.testing.expectEqual(plain_budget.visits + 2, budget.visits);
    try std.testing.expectEqual(plain_budget.bytes + member.len + scalar.len, budget.bytes);

    var limited = try C.open(a, &borrowed, "doc", "text", "model");
    defer limited.close();
    limited.enableWindowHints();
    budget = .{ .max_visits = 1, .max_bytes = 1 };
    try std.testing.expectEqualStrings(member, (try limited.poll(&budget)).member);
    try std.testing.expect(limited.member_has_windows); // Exact child capture is required when lookahead cannot certify absence.
    try std.testing.expectEqual(@as(usize, 3), budget.visits); // Logical member, mandatory advance, returned member.
    budget = .{};
    try std.testing.expect((try limited.poll(&budget)) == .end);
}
