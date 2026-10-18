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

//! Exact bounded materialized file outcomes and an ordered score index. Source
//! bytes remain generation-owned. Admission loss invalidates the entire index;
//! callers retain ordinary search as the conservative fallback.
const std = @import("std");
const A = std.mem.Allocator;
const Times = @import("reuse_cache.zig").Times;
pub const Member = struct { path: []const u8, tag: [32]u8 };
pub const Delivery = struct { token: [64]u8, revision: [64]u8, members: []Member, by_path: std.StringHashMapUnmanaged([32]u8) = .empty };
pub fn Index(comptime Summary: type, comptime Evaluation: type) type {
    return struct {
        const Self = @This();
        const Key = struct { score: f64, path: []const u8 };
        const Tree = std.Treap(Key, struct {
            fn compare(a: Key, b: Key) std.math.Order {
                if (a.score != b.score) return if (a.score > b.score) .lt else .gt;
                return std.mem.order(u8, a.path, b.path);
            }
        }.compare);
        const TimeKey = struct { value: i64, path: []const u8 };
        const TimeTree = std.Treap(TimeKey, struct {
            fn compare(a: TimeKey, b: TimeKey) std.math.Order {
                if (a.value != b.value) return std.math.order(a.value, b.value);
                return std.mem.order(u8, a.path, b.path);
            }
        }.compare);
        pub const File = struct {
            arena: std.heap.ArenaAllocator,
            path: []const u8,
            revision: [64]u8,
            tag: [32]u8,
            evaluation: Evaluation,
            summary: Summary = .{},
            score: f64 = 0,
            ranked: bool = false,
            times: Times,
            oldest_link: TimeTree.Node = undefined,
            newest_link: TimeTree.Node = undefined,
            excluded_epoch: u64 = 0,
            link: Tree.Node = undefined,
        };
        a: A,
        io: std.Io,
        budget: @import("antfly_regex").Budget,
        files: std.StringHashMapUnmanaged(*File) = .empty,
        tree: Tree = .{},
        aggregate: Summary = .{},
        oldest: TimeTree = .{},
        newest: TimeTree = .{},
        validity_epoch: u64 = 0,
        revision: ?[64]u8 = null,
        healthy: bool = true,
        qualifying: usize = 0,
        mutex: std.Io.Mutex = .init,
        pub const limit = 4 * 1024 * 1024;
        pub fn init(a: A, io: std.Io) Self {
            return .{ .a = a, .io = io, .budget = .{ .backing = a, .limit = limit } };
        }
        pub fn deinit(self: *Self) void {
            var it = self.files.valueIterator();
            while (it.next()) |file| self.destroy(file.*);
            self.files.deinit(self.budget.allocator());
            self.files = .empty;
        }
        pub fn clear(self: *Self) void {
            self.deinit();
            self.tree = .{};
            self.qualifying = 0;
            self.aggregate = .{};
            self.oldest = .{};
            self.newest = .{};
            self.validity_epoch = 0;
            self.revision = null;
            self.healthy = true;
        }
        fn destroy(self: *Self, file: *File) void {
            file.arena.deinit();
            self.budget.allocator().destroy(file);
        }
        pub fn fail(self: *Self) void {
            self.mutex.lockUncancelable(self.io);
            defer self.mutex.unlock(self.io);
            self.healthy = false;
        }
        pub fn capture(self: *Self, path: []const u8, revision: [64]u8, value: Evaluation, times: Times) ?*File {
            self.mutex.lockUncancelable(self.io);
            defer self.mutex.unlock(self.io);
            if (!self.healthy) return null;
            return self.make(path, revision, value, times) catch {
                self.healthy = false;
                return null;
            };
        }
        fn make(self: *Self, path: []const u8, revision: [64]u8, value: Evaluation, times: Times) !*File {
            const a = self.budget.allocator();
            const file = try a.create(File);
            errdefer a.destroy(file);
            file.* = .{ .arena = .init(a), .path = undefined, .revision = revision, .tag = undefined, .evaluation = undefined, .times = times };
            errdefer file.arena.deinit();
            file.path = try file.arena.allocator().dupe(u8, path);
            file.evaluation = try @import("reuse_cache.zig").retainedClone(Evaluation, file.arena.allocator(), value, 0);
            self.io.random(&file.tag);
            return file;
        }
        fn adjust(self: *Self, summary: Summary, add: bool) void {
            inline for (@typeInfo(Summary).@"struct".field_names) |name| {
                if (@TypeOf(@field(summary, name)) == bool) {
                    if (add) @field(self.aggregate, name) = @field(self.aggregate, name) and @field(summary, name);
                } else if (add) @field(self.aggregate, name) += @field(summary, name) else @field(self.aggregate, name) -= @field(summary, name);
            }
        }
        fn detach(self: *Self, file: *File) void {
            if (file.times.oldest != std.math.maxInt(i64)) {
                var oldest = self.oldest.getEntryForExisting(&file.oldest_link);
                oldest.set(null);
                var newest = self.newest.getEntryForExisting(&file.newest_link);
                newest.set(null);
            }
            self.adjust(file.summary, false);
            if (file.ranked) {
                var entry = self.tree.getEntryForExisting(&file.link);
                entry.set(null);
                self.qualifying -= 1;
            }
        }
        pub fn remove(self: *Self, path: []const u8) void {
            if (self.files.fetchRemove(path)) |old| {
                self.detach(old.value);
                self.destroy(old.value);
            }
        }
        pub fn commit(self: *Self, pending: ?*File, summary: Summary, score: f64) void {
            self.mutex.lockUncancelable(self.io);
            defer self.mutex.unlock(self.io);
            const file = pending orelse {
                self.healthy = false;
                return;
            };
            if (!self.healthy or !summary.complete or summary.errors != 0) {
                self.destroy(file);
                self.healthy = false;
                return;
            }
            const entry = self.files.getOrPut(self.budget.allocator(), file.path) catch {
                self.destroy(file);
                self.healthy = false;
                return;
            };
            if (entry.found_existing) {
                self.detach(entry.value_ptr.*);
                self.destroy(entry.value_ptr.*);
            }
            entry.key_ptr.* = file.path;
            entry.value_ptr.* = file;
            file.summary = summary;
            file.score = score;
            if (file.times.oldest != std.math.maxInt(i64)) {
                var oldest = self.oldest.getEntryFor(.{ .value = file.times.oldest, .path = file.path });
                oldest.set(&file.oldest_link);
                var newest = self.newest.getEntryFor(.{ .value = file.times.newest, .path = file.path });
                newest.set(&file.newest_link);
            }
            self.adjust(summary, true);
            file.ranked = summary.files_with_matches != 0;
            if (file.ranked) {
                var ranked = self.tree.getEntryFor(.{ .score = score, .path = file.path });
                ranked.set(&file.link);
                self.qualifying += 1;
            }
        }
        pub fn first(self: *Self) ?*File {
            return if (self.tree.getMin()) |node| @fieldParentPtr("link", node) else null;
        }
        pub fn next(file: *File) ?*File {
            return if (file.link.next()) |node| @fieldParentPtr("link", node) else null;
        }
        pub fn evidenceTimes(self: *Self) Times {
            const first_node = self.oldest.getMin() orelse return .{};
            const last_node = self.newest.getMax().?;
            const first_file: *File = @fieldParentPtr("oldest_link", first_node);
            const last_file: *File = @fieldParentPtr("newest_link", last_node);
            return .{ .oldest = first_file.times.oldest, .newest = last_file.times.newest };
        }
        /// Check the surviving outcomes before mutating the index. Dirty or
        /// deleted files are reevaluated/retired, so their old expiry cannot
        /// force rebuilding unrelated fresh outcomes. No query timestamps slide.
        pub fn validAfter(self: *Self, changes: []const []const u8, ttl: u64) bool {
            if (!self.healthy or self.revision == null or self.validity_epoch == std.math.maxInt(u64)) return false;
            self.validity_epoch += 1;
            for (changes) |path| if (self.files.get(path)) |file| {
                file.excluded_epoch = self.validity_epoch;
            };
            var oldest = self.oldest.getMin();
            while (oldest) |node| {
                const file: *File = @fieldParentPtr("oldest_link", node);
                if (file.excluded_epoch != self.validity_epoch) break;
                oldest = node.next();
            }
            var newest = self.newest.getMax();
            while (newest) |node| {
                const file: *File = @fieldParentPtr("newest_link", node);
                if (file.excluded_epoch != self.validity_epoch) break;
                newest = node.prev();
            }
            const first_file: *File = @fieldParentPtr("oldest_link", oldest orelse return false);
            const last_file: *File = @fieldParentPtr("newest_link", newest orelse return false);
            const times: Times = .{ .oldest = first_file.times.oldest, .newest = last_file.times.newest };
            return times.valid(std.Io.Clock.real.now(self.io).toSeconds(), ttl);
        }
        pub fn valid(self: *Self, ttl: u64) bool {
            return self.healthy and self.revision != null and self.evidenceTimes().valid(std.Io.Clock.real.now(self.io).toSeconds(), ttl);
        }
    };
}

const TestSummary = struct { files: usize = 0, files_with_matches: usize = 0, matches: usize = 0, errors: usize = 0, complete: bool = true };
const TestEvaluation = struct { data: []const u8 };
test "exact score index retains rejected files and promotes outside top k on deletion" {
    const I = Index(TestSummary, TestEvaluation);
    var index = I.init(std.testing.allocator, std.testing.io);
    defer index.deinit();
    var times: Times = .{};
    times.add(std.Io.Clock.real.now(std.testing.io).toSeconds());
    index.commit(index.capture("a", @splat('a'), .{ .data = "score" }, times), .{ .files = 1, .files_with_matches = 1, .matches = 1 }, 0.9);
    index.commit(index.capture("b", @splat('b'), .{ .data = "score" }, times), .{ .files = 1, .files_with_matches = 1, .matches = 1 }, 0.7);
    index.commit(index.capture("c", @splat('c'), .{ .data = "negative" }, times), .{ .files = 1 }, 0.0);
    try std.testing.expectEqualStrings("a", index.first().?.path);
    try std.testing.expectEqual(@as(usize, 3), index.files.count());
    index.remove("a");
    try std.testing.expectEqualStrings("b", index.first().?.path);
    try std.testing.expectEqual(@as(usize, 2), index.aggregate.files);
    try std.testing.expectEqual(@as(usize, 1), index.qualifying);
    const previous_tag = index.files.get("c").?.tag;
    index.commit(index.capture("c", @splat('d'), .{ .data = "new score" }, times), .{ .files = 1, .files_with_matches = 1, .matches = 1 }, 0.95);
    try std.testing.expectEqualStrings("c", index.first().?.path);
    try std.testing.expect(!std.mem.eql(u8, &previous_tag, &index.first().?.tag));
    const oversized = try std.testing.allocator.alloc(u8, I.limit + 1);
    defer std.testing.allocator.free(oversized);
    try std.testing.expect(index.capture("too large", @splat('x'), .{ .data = oversized }, times) == null);
    try std.testing.expect(!index.healthy);
    index.clear();
    try std.testing.expectEqual(@as(usize, 0), index.budget.live);
    try std.testing.expectEqual(@as(usize, 0), index.qualifying);
}

test "active evidence removes deleted and replaced expiry witnesses including rejected files" {
    const I = Index(TestSummary, TestEvaluation);
    var index = I.init(std.testing.allocator, std.testing.io);
    defer index.deinit();
    const now = std.Io.Clock.real.now(std.testing.io).toSeconds();
    var stale: Times = .{};
    stale.add(now - 1000);
    var fresh: Times = .{};
    fresh.add(now);
    index.commit(index.capture("old loser", @splat('a'), .{ .data = "negative" }, stale), .{ .files = 1 }, 0);
    index.commit(index.capture("winner", @splat('b'), .{ .data = "positive" }, fresh), .{ .files = 1, .files_with_matches = 1 }, 1);
    index.revision = @splat('x');
    try std.testing.expect(!index.valid(100));
    index.remove("old loser");
    try std.testing.expect(index.valid(100));
    index.commit(index.capture("winner", @splat('c'), .{ .data = "negative" }, stale), .{ .files = 1 }, 0);
    try std.testing.expect(!index.valid(100));
    index.commit(index.capture("winner", @splat('d'), .{ .data = "negative" }, fresh), .{ .files = 1 }, 0);
    try std.testing.expect(index.valid(100));
    var future: Times = .{};
    future.add(now + 1000);
    index.commit(index.capture("future loser", @splat('e'), .{ .data = "negative" }, future), .{ .files = 1 }, 0);
    try std.testing.expect(!index.valid(100));
    index.remove("future loser");
    try std.testing.expect(index.valid(100));
    index.remove("winner");
    try std.testing.expect(!index.valid(100));
    try std.testing.expect(index.oldest.getMin() == null and index.newest.getMax() == null);
}

test "pending removals and replacements cannot expire surviving fresh outcomes" {
    const I = Index(TestSummary, TestEvaluation);
    var index = I.init(std.testing.allocator, std.testing.io);
    defer index.deinit();
    const now = std.Io.Clock.real.now(std.testing.io).toSeconds();
    var stale: Times = .{};
    stale.add(now - 1000);
    var fresh: Times = .{};
    fresh.add(now);
    index.commit(index.capture("removed", @splat('a'), .{ .data = "rejected" }, stale), .{ .files = 1 }, 0);
    index.commit(index.capture("survivor", @splat('b'), .{ .data = "match" }, fresh), .{ .files = 1, .files_with_matches = 1 }, 1);
    index.revision = @splat('x');
    try std.testing.expect(!index.valid(100));
    try std.testing.expect(index.validAfter(&.{"removed"}, 100));
    try std.testing.expect(!index.validAfter(&.{"survivor"}, 100));
    try std.testing.expectEqual(@as(usize, 2), index.files.count());
    try std.testing.expectEqual(@as(usize, 2), index.aggregate.files);
    try std.testing.expect(!index.valid(100));
}
