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
const search = @import("antfly_vectorindex").search_types;

pub const Filter = struct {
    pub const DirectMode = enum { graph, exact, bounds };

    ids: search.RequestFilterState,
    request: search.SearchRequest,

    pub fn init(alloc: std.mem.Allocator, request: search.SearchRequest) !Filter {
        return .{ .ids = try search.RequestFilterState.init(alloc, request), .request = request };
    }

    pub fn deinit(self: *Filter, alloc: std.mem.Allocator) void {
        self.ids.deinit(alloc);
    }

    pub fn active(self: *const Filter) bool {
        return !self.ids.isTrivial() or self.request.key_predicate != null or self.request.filter_prefix.len != 0 or
            self.request.distance_over != null or self.request.distance_under != null;
    }

    pub fn allows(self: *const Filter, id: u64, metadata: []const u8) !bool {
        if (self.ids.rejects(id) or !std.mem.startsWith(u8, metadata, self.request.filter_prefix)) return false;
        if (self.request.key_predicate) |predicate| return predicate.allows(predicate.ptr, metadata);
        return true;
    }

    pub fn distanceAllowed(self: *const Filter, dist: f32) bool {
        if (self.request.distance_over) |threshold| if (!(dist > threshold)) return false;
        if (self.request.distance_under) |threshold| if (!(dist < threshold)) return false;
        return true;
    }

    pub fn cancelled(self: *const Filter) bool {
        return if (self.request.cancellation) |token| token.isCancelled() else false;
    }

    pub fn eligibleCount(self: *const Filter) ?u32 {
        if (self.ids.include.count() == 0 or self.request.key_predicate != null or self.request.filter_prefix.len != 0) return null;
        if (self.ids.exclude.count() == 0) return self.ids.include.count();
        var count: u32 = 0;
        var ids = self.ids.include.keyIterator();
        while (ids.next()) |id| {
            if (!self.ids.exclude.contains(id.*)) count += 1;
        }
        return count;
    }

    /// Small sets are exact. A larger, sparse RaBitQ allowlist can avoid
    /// walking unrelated nodes by scoring its codes before loading vectors.
    pub fn directMode(self: *const Filter, dims: usize, live: usize, transient: bool, rabitq: bool) DirectMode {
        const count: usize = self.ids.include.count();
        if (count == 0) return .graph;
        const width = @max(dims, 1);
        const exact_limit: usize = if (transient) 16 else 64;
        if (count <= @min(exact_limit, (64 * 1024) / width)) return .exact;
        if (rabitq and count *| 8 <= live and count <= @min(1024, (1024 * 1024) / width)) return .bounds;
        if (!transient and count *| 32 <= live and count <= @min(256, (256 * 1024) / width)) return .exact;
        return .graph;
    }
};

test "sparse allowlists use bounded direct work on either side of the exact cutoff" {
    const alloc = std.testing.allocator;
    var ids: [65]u64 = undefined;
    for (&ids, 0..) |*id, i| id.* = i;
    for ([_]usize{ 16, 17, 64, 65 }) |count| {
        var filter = try Filter.init(alloc, .{ .query = &.{}, .k = 10, .filter_ids = ids[0..count] });
        defer filter.deinit(alloc);
        try std.testing.expectEqual(if (count <= 64) Filter.DirectMode.exact else .bounds, filter.directMode(768, 1_000_000, false, true));
        try std.testing.expectEqual(if (count <= 16) Filter.DirectMode.exact else .bounds, filter.directMode(768, 1_000_000, true, true));
        if (count == 65) try std.testing.expectEqual(Filter.DirectMode.graph, filter.directMode(768, 300, false, true));
    }
}
