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

const std = @import("std");
const types = @import("types.zig");
const Observation = struct {
    activity: types.EmbeddingActivityStats,
    observed_at_ms: u64,
};
/// Readiness-neutral samples, fenced by index generation and receipt age.
pub const Owner = struct {
    mutex: std.atomic.Mutex = .unlocked,
    cache: std.StringHashMapUnmanaged(Observation) = .empty,
    pub fn clear(self: *Owner, alloc: std.mem.Allocator) void {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        var keys = self.cache.keyIterator();
        while (keys.next()) |key| alloc.free(@constCast(key.*));
        self.cache.clearRetainingCapacity();
    }
    pub fn deinit(self: *Owner, alloc: std.mem.Allocator) void {
        self.clear(alloc);
        self.cache.deinit(alloc);
    }
    fn isEmbeddingActivityIndex(item: types.DBIndexStats) bool {
        return item.kind == .dense_vector or item.kind == .sparse_vector;
    }

    fn statusContainsEmbeddingActivityIndex(indexes: []const types.DBIndexStats, name: []const u8) bool {
        for (indexes) |item| {
            if (isEmbeddingActivityIndex(item) and std.mem.eql(u8, item.name, name)) return true;
        }
        return false;
    }

    pub fn observe(
        self: *Owner,
        alloc: std.mem.Allocator,
        indexes: []types.DBIndexStats,
        runtime: anytype,
        now_ms: u64,
    ) void {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();

        for (indexes) |*item| {
            if (!isEmbeddingActivityIndex(item.*)) continue;
            const activity = runtime.indexEmbeddingActivity(item.name);
            item.embedding_activity_observed = true;
            item.embedding_activity_sample_fresh = true;
            item.embedding_activity = activity;

            if (self.cache.getPtr(item.name)) |cached| {
                cached.* = .{ .activity = activity, .observed_at_ms = now_ms };
                continue;
            }
            const owned_name = alloc.dupe(u8, item.name) catch continue;
            self.cache.putNoClobber(
                alloc,
                owned_name,
                .{ .activity = activity, .observed_at_ms = now_ms },
            ) catch {
                alloc.free(owned_name);
                continue;
            };
        }

        // Successful lifecycle access is authoritative for the current index
        // set. Prune removed names so repeated DDL cannot grow telemetry state.
        var entries = self.cache.iterator();
        while (entries.next()) |entry| {
            if (statusContainsEmbeddingActivityIndex(indexes, entry.key_ptr.*)) continue;
            const owned_name = entry.key_ptr.*;
            self.cache.removeByPtr(entry.key_ptr);
            alloc.free(owned_name);
        }
    }

    pub fn retain(self: *Owner, alloc: std.mem.Allocator, indexes: []types.DBIndexStats, now_ms: u64) void {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();

        for (indexes) |*item| {
            if (!isEmbeddingActivityIndex(item.*)) continue;
            item.embedding_activity_observed = false;
            item.embedding_activity_sample_fresh = false;
            item.embedding_activity = .{};

            const cached = self.cache.get(item.name) orelse continue;
            const identity_matches = cached.activity.index_generation == item.coverage_generation;
            const receipt_fresh = cached.observed_at_ms != 0 and
                now_ms -| cached.observed_at_ms < types.embedding_activity_retention_ms;
            if (cached.activity.epoch != 0 and
                cached.activity.sample_sequence != 0 and
                identity_matches and
                receipt_fresh)
            {
                item.embedding_activity_observed = true;
                item.embedding_activity = cached.activity;
                continue;
            }

            if (self.cache.fetchRemove(item.name)) |removed| {
                alloc.free(removed.key);
            }
        }
    }
};
fn lockAtomic(mutex: *std.atomic.Mutex) void {
    while (!mutex.tryLock()) std.atomic.spinLoopHint();
}

test "embedding activity cache retains exact generation and prunes stale samples and removed names" {
    const Fake = struct {
        fn indexEmbeddingActivity(_: *@This(), _: []const u8) types.EmbeddingActivityStats {
            return .{ .epoch = 1, .sample_sequence = 1, .index_generation = 7 };
        }
    };
    var runtime: Fake = .{};
    var owner: Owner = .{};
    defer owner.deinit(std.testing.allocator);
    var indexes = [_]types.DBIndexStats{.{ .name = "idx", .kind = .dense_vector, .coverage_generation = 7 }};
    owner.observe(std.testing.allocator, &indexes, &runtime, 100);
    owner.retain(std.testing.allocator, &indexes, 101);
    try std.testing.expect(indexes[0].embedding_activity_observed);
    try std.testing.expect(!indexes[0].embedding_activity_sample_fresh);
    indexes[0].coverage_generation = 8;
    owner.retain(std.testing.allocator, &indexes, 101);
    try std.testing.expect(!indexes[0].embedding_activity_observed);
    try std.testing.expectEqual(@as(u32, 0), owner.cache.count());
    indexes[0].coverage_generation = 7;
    owner.observe(std.testing.allocator, &indexes, &runtime, 100);
    owner.retain(std.testing.allocator, &indexes, 100 + types.embedding_activity_retention_ms);
    try std.testing.expect(!indexes[0].embedding_activity_observed);
    owner.observe(std.testing.allocator, &indexes, &runtime, 100);
    owner.observe(std.testing.allocator, &.{}, &runtime, 101);
    try std.testing.expectEqual(@as(u32, 0), owner.cache.count());
}
