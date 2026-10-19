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

//! Bounded lexical summary index. Keys own paths, never source generations.
//! Changed-path updates adjust logical totals without visiting unrelated files.
const std = @import("std");
const A = std.mem.Allocator;
pub fn Index(comptime Summary: type) type {
    return struct {
        const Self = @This();
        const Flags = packed struct(u8) { searched: bool, matched: bool, skipped: bool, invalid: bool, exact: bool, visible: bool, reserved: u2 = 0 };
        const Logical = struct {
            bytes: usize,
            matches: usize,
            occurrences: usize,
            flags: Flags,
            fn from(s: Summary, emitted: bool) ?Logical {
                if (s.files > 1 or s.files_with_matches > 1 or s.skipped_paths > 1 or s.invalid_utf8_files > 1 or !s.complete or s.errors != 0) return null;
                return .{ .bytes = s.bytes, .matches = s.matches, .occurrences = s.occurrences, .flags = .{ .searched = s.files == 1, .matched = s.files_with_matches == 1, .skipped = s.skipped_paths == 1, .invalid = s.invalid_utf8_files == 1, .exact = s.occurrences_exact, .visible = emitted } };
            }
        };
        a: A,
        io: std.Io,
        budget: @import("antfly_regex").Budget,
        files: std.StringHashMapUnmanaged(Logical) = .empty,
        aggregate: Summary = .{},
        inexact: usize = 0,
        revision: ?[64]u8 = null,
        healthy: bool = true,
        mutex: std.Io.Mutex = .init,
        pub const limit = 16 * 1024 * 1024;
        pub fn init(a: A, io: std.Io) Self {
            return .{ .a = a, .io = io, .budget = .{ .backing = a, .limit = limit } };
        }
        pub fn deinit(self: *Self) void {
            var keys = self.files.keyIterator();
            while (keys.next()) |key| self.budget.allocator().free(key.*);
            self.files.deinit(self.budget.allocator());
            self.files = .empty;
        }
        pub fn clear(self: *Self) void {
            self.deinit();
            self.aggregate = .{};
            self.inexact = 0;
            self.revision = null;
            self.healthy = true;
        }
        fn adjust(self: *Self, value: Logical, add: bool) void {
            const values = .{ value.bytes, value.matches, value.occurrences, @intFromBool(value.flags.searched), @intFromBool(value.flags.matched), @intFromBool(value.flags.skipped), @intFromBool(value.flags.invalid) };
            inline for (.{ "bytes", "matches", "occurrences", "files", "files_with_matches", "skipped_paths", "invalid_utf8_files" }, 0..) |name, i| {
                if (add) @field(self.aggregate, name) += values[i] else @field(self.aggregate, name) -= values[i];
            }
            if (!value.flags.exact) {
                if (add) self.inexact += 1 else self.inexact -= 1;
            }
            self.aggregate.occurrences_exact = self.inexact == 0;
        }
        pub fn matched(self: *const Self, path: []const u8) bool {
            return if (self.files.get(path)) |value| value.flags.matched else false;
        }
        pub fn visible(self: *const Self, path: []const u8) bool {
            return if (self.files.get(path)) |value| value.flags.visible else false;
        }
        pub fn remove(self: *Self, path: []const u8) void {
            if (self.files.fetchRemove(path)) |old| {
                self.adjust(old.value, false);
                self.budget.allocator().free(old.key);
            }
        }
        pub fn record(self: *Self, path: []const u8, summary: Summary, emitted: bool) void {
            self.mutex.lockUncancelable(self.io);
            defer self.mutex.unlock(self.io);
            if (!self.healthy or self.files.contains(path)) return;
            const logical = Logical.from(summary, emitted) orelse {
                self.healthy = false;
                return;
            };
            const owned = self.budget.allocator().dupe(u8, path) catch {
                self.healthy = false;
                return;
            };
            self.files.put(self.budget.allocator(), owned, logical) catch {
                self.budget.allocator().free(owned);
                self.healthy = false;
                return;
            };
            self.adjust(logical, true);
        }
    };
}

const TestSummary = struct { files: usize = 0, bytes: usize = 0, matches: usize = 0, occurrences: usize = 0, files_with_matches: usize = 0, skipped_paths: usize = 0, invalid_utf8_files: usize = 0, occurrences_exact: bool = true, complete: bool = true, errors: usize = 0 };
test "lexical summary index repairs aggregates and bounds failed admission" {
    const I = Index(TestSummary);
    var index = I.init(std.testing.allocator, std.testing.io);
    defer index.deinit();
    index.record("a", .{ .files = 1, .bytes = 10, .matches = 2, .occurrences = 2, .files_with_matches = 1, .occurrences_exact = false }, true);
    index.record("b", .{ .files = 1, .bytes = 5 }, true);
    try std.testing.expect(index.matched("a"));
    try std.testing.expect(!index.matched("b"));
    try std.testing.expect(index.visible("b"));
    try std.testing.expectEqual(@as(usize, 4 * @sizeOf(usize)), @sizeOf(I.Logical));
    try std.testing.expectEqual(@as(usize, 15), index.aggregate.bytes);
    index.remove("a");
    try std.testing.expect(index.aggregate.occurrences_exact);
    try std.testing.expectEqual(@as(usize, 0), index.aggregate.matches);
    index.clear();
    try std.testing.expectEqual(@as(usize, 0), index.budget.live);
    index.budget.limit = 1;
    index.record("failed", .{ .files = 1 }, false);
    try std.testing.expect(!index.healthy);
    index.clear();
    try std.testing.expectEqual(@as(usize, 0), index.budget.live);
}
