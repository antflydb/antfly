// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Bounded durable merge forest for maintenance deltas. Local sorts contain one
//! batch only; authenticated runs survive checkpoints and merge in binary levels.
const std = @import("std");
const tree = @import("../serverless/graph_segment/page_tree.zig");
const A = std.mem.Allocator;
pub const Run = struct { page: tree.Ref, level: u8 };
pub const Policy = enum { sum, contribution, latest };
pub const Forest = struct {
    a: A,
    store: tree.Store,
    policy: Policy,
    levels: [32]?tree.Ref = @splat(null),
    pub fn restore(self: *Forest, runs: []const Run) !void {
        if (runs.len > self.levels.len) return error.InvalidNativeLakeDeltaRun;
        for (runs) |run| {
            if (run.level >= self.levels.len or self.levels[run.level] != null) return error.InvalidNativeLakeDeltaRun;
            try run.page.validate();
            self.levels[run.level] = run.page;
        }
    }
    pub fn snapshot(self: *const Forest, a: A) ![]const Run {
        var result: std.ArrayList(Run) = .empty;
        errdefer result.deinit(a);
        for (self.levels, 0..) |page, level| if (page) |ref| try result.append(a, .{ .page = ref, .level = @intCast(level) });
        return result.toOwnedSlice(a);
    }
    pub fn empty(self: *const Forest) bool {
        for (self.levels) |page| if (page != null) return false;
        return true;
    }
    pub fn add(self: *Forest, source: anytype) !void {
        var incoming = try tree.buildSorted(self.a, self.store, source);
        var level: usize = 0;
        // Commit the new directory only after every page is durable. Failure
        // leaves the caller's previously checkpointed forest usable.
        var next = self.levels;
        while (incoming) |page| : (level += 1) {
            if (level == next.len) return error.NativeLakeDeltaRunLimitExceeded;
            const old = next[level] orelse {
                next[level] = page;
                break;
            };
            incoming = try self.merge(old, page);
            next[level] = null;
        }
        self.levels = next;
    }
    pub fn finish(self: *Forest) !?tree.Ref {
        var result: ?tree.Ref = null;
        // Higher levels are older. Latest-write policy must retain chronology.
        var level: usize = self.levels.len;
        while (level != 0) {
            level -= 1;
            if (self.levels[level]) |page| result = if (result) |old| try self.merge(old, page) else page;
        }
        return result;
    }
    fn merge(self: *Forest, old: tree.Ref, fresh: tree.Ref) !?tree.Ref {
        var left = try tree.Cursor.init(self.a, self.store, old, "", null);
        defer left.deinit();
        var right = try tree.Cursor.init(self.a, self.store, fresh, "", null);
        defer right.deinit();
        const Merge = struct {
            left: *tree.Cursor,
            right: *tree.Cursor,
            policy: Policy,
            l: ?tree.Cursor.Record = null,
            r: ?tree.Cursor.Record = null,
            advance_left: bool = true,
            advance_right: bool = true,
            value: [8 + @import("lake_index_text_statistics.zig").Summary.max_bytes]u8 = undefined,
            pub fn next(self_: *@This()) !?tree.Cursor.Record {
                while (true) {
                    if (self_.advance_left) self_.l = try self_.left.next();
                    if (self_.advance_right) self_.r = try self_.right.next();
                    self_.advance_left = false;
                    self_.advance_right = false;
                    if (self_.l == null and self_.r == null) return null;
                    const order: std.math.Order = if (self_.l == null) .gt else if (self_.r == null) .lt else std.mem.order(u8, self_.l.?.key, self_.r.?.key);
                    self_.advance_left = order != .gt;
                    self_.advance_right = order != .lt;
                    if (order != .eq) return if (order == .lt) self_.l else self_.r;
                    if (self_.policy == .latest) return self_.r;
                    const l = self_.l.?;
                    const r = self_.r.?;
                    if (l.value.len < 8 or r.value.len < 8) return error.InvalidNativeLakeDeltaRun;
                    const sum = std.math.add(i64, std.mem.readInt(i64, l.value[0..8], .big), std.mem.readInt(i64, r.value[0..8], .big)) catch return error.InvalidNativeLakeDeltaRun;
                    if (sum == 0) continue;
                    std.mem.writeInt(i64, self_.value[0..8], sum, .big);
                    const payload = if (self_.policy == .contribution) (if (std.mem.readInt(i64, r.value[0..8], .big) > 0) r.value[8..] else l.value[8..]) else "";
                    if (payload.len > self_.value.len - 8) return error.InvalidNativeLakeDeltaRun;
                    @memcpy(self_.value[8..][0..payload.len], payload);
                    return .{ .key = r.key, .value = self_.value[0 .. 8 + payload.len] };
                }
            }
        };
        var source: Merge = .{ .left = &left, .right = &right, .policy = self.policy };
        return tree.buildSorted(self.a, self.store, &source);
    }
};
