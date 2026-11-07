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

//! Bounded, execution-owned operator measurements. Times are inclusive and
//! cannot be summed across parent/child operators. Physical storage I/O is not
//! inferred from rows emitted through the SQL cursor boundary.
const std = @import("std");
const binding = @import("relation_binding.zig");
const catalog = @import("catalog.zig");
pub const Node = struct {
    id: usize,
    node_type: []const u8,
    relation: ?[]const u8 = null,
    metadata_rows: ?u64 = null,
    calls: u64 = 0,
    rows: u64 = 0,
    exhausted: bool = false,
    failed: bool = false,
    elapsed_ns: ?u64 = null,
};
pub const Profile = struct {
    allocator: std.mem.Allocator,
    io: ?std.Io,
    timing: bool = true,
    summary: bool = true,
    buffers: bool = true,
    wal: bool = false,
    /// Timing OFF must avoid clock reads at every pull, not only hide output.
    clock_reads: u64 = 0,
    nodes: std.ArrayList(Node) = .empty,
    identities: std.AutoHashMapUnmanaged(*const binding.Node, usize) = .empty,
    /// Missing when the backend has no execution clock.
    elapsed_ns: ?u64 = null,
    retained_peak_bytes: ?usize = null,
    spill_written_bytes: ?u64 = null,
    result_rows: usize = 0,
    nodes_omitted: bool = false,
    pub fn register(self: *Profile, node: *const binding.Node, scans: []const catalog.StatementScan) !?usize {
        if (self.identities.get(node)) |id| return id;
        if (self.nodes.items.len == 8192) {
            self.nodes_omitted = true;
            return null;
        }
        const id = self.nodes.items.len;
        const relation = if (node.operation == .scan and node.operation.scan.index < scans.len) scans[node.operation.scan.index].table.physical_name else null;
        try self.nodes.append(self.allocator, .{ .id = id, .node_type = @tagName(node.operation), .relation = relation, .elapsed_ns = if (self.io != null and self.timing) 0 else null });
        try self.identities.put(self.allocator, node, id);
        return id;
    }
    pub fn registerStage(self: *Profile, kind: []const u8, relation_name: ?[]const u8) !?usize {
        if (self.nodes.items.len == 8192) {
            self.nodes_omitted = true;
            return null;
        }
        const id = self.nodes.items.len;
        try self.nodes.append(self.allocator, .{ .id = id, .node_type = kind, .relation = relation_name, .elapsed_ns = if (self.io != null and self.timing) 0 else null });
        return id;
    }
    pub fn registerScan(self: *Profile, table: catalog.Table) !?usize {
        if (self.nodes.items.len == 8192) {
            self.nodes_omitted = true;
            return null;
        }
        const id = self.nodes.items.len;
        try self.nodes.append(self.allocator, .{ .id = id, .node_type = "scan_boundary", .relation = table.physical_name, .elapsed_ns = if (self.io != null and self.timing) 0 else null });
        return id;
    }
    pub fn wallNow(self: *Profile) ?std.Io.Timestamp {
        const io = self.io orelse return null;
        self.clock_reads +|= 1;
        return std.Io.Clock.awake.now(io);
    }
    pub fn wallDuration(self: *Profile, start: ?std.Io.Timestamp) ?u64 {
        return if (start) |s| @intCast(@max(0, s.durationTo(self.wallNow().?).toNanoseconds())) else null;
    }
    pub fn now(self: *Profile) ?std.Io.Timestamp {
        return if (self.timing) self.wallNow() else null;
    }
    pub fn duration(self: *Profile, start: ?std.Io.Timestamp) ?u64 {
        return if (self.timing) self.wallDuration(start) else null;
    }
    pub fn record(self: *Profile, id: ?usize, start: ?std.Io.Timestamp, rows: usize, exhausted: bool, failed: bool) void {
        const index = id orelse return;
        const node = &self.nodes.items[index];
        node.calls +|= 1;
        node.rows +|= rows;
        node.exhausted = node.exhausted or exhausted;
        node.failed = node.failed or failed;
        if (self.duration(start)) |ns| node.elapsed_ns = (node.elapsed_ns orelse 0) +| ns;
    }
    pub fn jsonStringify(self: Profile, jw: anytype) !void {
        try jw.write(.{
            .version = @as(u32, 2),
            .scope = "coordinator_sql_operators",
            .coverage = "root execution and SQL pull boundaries; provider and parallel internals excluded",
            .timing = if (self.timing) "inclusive; overlapping operators must not be summed" else "disabled",
            .summary = self.summary,
            .elapsed_ns = if (self.summary) self.elapsed_ns else null,
            .retained_peak_bytes = if (self.buffers) self.retained_peak_bytes else null,
            .spill_written_bytes = if (self.buffers) self.spill_written_bytes else null,
            .result_rows = self.result_rows,
            .storage_scanned_rows = @as(?u64, null),
            .storage_read_bytes = @as(?u64, null),
            .buffers = if (self.buffers) .{ .scope = "coordinator SQL memory and temporary spill bytes", .retained_peak_bytes = self.retained_peak_bytes, .spill_written_bytes = self.spill_written_bytes, .storage_read_bytes = @as(?u64, null), .storage_cache_hit_bytes = @as(?u64, null) } else null,
            .wal = if (self.wal) .{ .scope = "durable provider journal", .records = @as(?u64, null), .bytes = @as(?u64, null), .coverage = "not reported by this execution provider" } else null,
            .nodes_omitted = self.nodes_omitted,
            .nodes = self.nodes.items,
        });
    }
};

test "SQL EXPLAIN timing off eliminates per pull clock reads independently of summary" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var profile: Profile = .{ .allocator = arena.allocator(), .io = std.testing.io, .timing = false };
    const root = try profile.registerStage("scan", null);
    const start = profile.wallNow();
    for (0..10000) |_| profile.record(root, profile.now(), 1, false, false);
    profile.elapsed_ns = profile.wallDuration(start);
    try std.testing.expectEqual(@as(u64, 2), profile.clock_reads);
    try std.testing.expectEqual(@as(u64, 10000), profile.nodes.items[0].rows);
    try std.testing.expect(profile.nodes.items[0].elapsed_ns == null);
    try std.testing.expect(profile.elapsed_ns != null);
}
