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

//! Context-dependency host operations as one JSON call.
//!
//! A distributed host (the data server) serves a group's DB with five
//! capabilities: propose a replicated command to the group, report whether
//! this replica leads it, deliver a change notice to every group of a
//! dependent table, read other tables (and other shards of this table), and
//! report the table's identity and inbound dependents. Expressing them as one
//! `(op, request JSON) -> response JSON` call keeps the storage-owner ABI to a
//! single function pointer and lets direct (non-ABI) hosts use the same
//! adapter.

const std = @import("std");
const Allocator = std.mem.Allocator;
const context_dependency_db = @import("context_dependency_db.zig");
const reader_mod = @import("context_source_db_reader.zig");
const resolution = @import("enrichment/context_resolution.zig");
const command_mod = @import("enrichment/context_dependency_command.zig");

const wire = @import("context_host_wire.zig");
pub const Op = wire.Op;
pub const WirePage = wire.WirePage;
pub const encodePageAlloc = wire.encodePageAlloc;

pub const Call = struct {
    ptr: *anyopaque,
    /// Response bytes are owned by `alloc`.
    call_fn: *const fn (ptr: *anyopaque, alloc: Allocator, op: Op, request: []const u8) anyerror![]u8,

    fn call(self: Call, alloc: Allocator, op: Op, request: []const u8) ![]u8 {
        return self.call_fn(self.ptr, alloc, op, request);
    }
};

/// Adapter state; must outlive every DB it is installed on.
pub const Host = struct {
    call: Call,

    pub fn replication(self: *Host) context_dependency_db.Replication {
        return .{ .ptr = self, .propose_fn = propose, .is_owner_fn = isLeader };
    }

    pub fn dispatcher(self: *Host) context_dependency_db.Dispatcher {
        return .{ .ptr = self, .deliver = deliver };
    }

    pub fn readSource(self: *Host) reader_mod.ReadSource {
        return .{ .ptr = self, .lookup_fn = lookup, .query_fn = query, .request_fn = request, .scan_fn = scan, .edges_fn = edges, .artifacts_fn = artifacts };
    }

    pub fn topology(self: *Host) Topology {
        return .{ .ptr = self, .fetch_fn = fetchTopology };
    }

    fn propose(ptr: *anyopaque, alloc: Allocator, command: command_mod.Command) anyerror!void {
        const self: *Host = @ptrCast(@alignCast(ptr));
        const encoded = try std.json.Stringify.valueAlloc(alloc, command, .{});
        defer alloc.free(encoded);
        alloc.free(try self.call.call(alloc, .propose, encoded));
    }

    fn isLeader(ptr: *anyopaque) bool {
        const self: *Host = @ptrCast(@alignCast(ptr));
        const out = self.call.call(std.heap.page_allocator, .is_leader, "") catch return false;
        defer std.heap.page_allocator.free(out);
        return std.mem.eql(u8, out, "1");
    }

    fn deliver(ptr: *anyopaque, alloc: Allocator, dependent: []const u8, notice: []const u8) anyerror!void {
        const self: *Host = @ptrCast(@alignCast(ptr));
        const encoded = try std.json.Stringify.valueAlloc(alloc, .{ .dependent = dependent, .notice = notice }, .{});
        defer alloc.free(encoded);
        alloc.free(try self.call.call(alloc, .deliver, encoded));
    }

    fn lookup(ptr: *anyopaque, alloc: Allocator, table_id: []const u8, key: []const u8) anyerror!?[]u8 {
        const self: *Host = @ptrCast(@alignCast(ptr));
        const encoded = try std.json.Stringify.valueAlloc(alloc, .{ .table = table_id, .key = key }, .{});
        defer alloc.free(encoded);
        const out = try self.call.call(alloc, .lookup, encoded);
        defer alloc.free(out);
        const parsed = try std.json.parseFromSlice(struct { value: ?[]const u8 = null }, alloc, out, .{ .ignore_unknown_fields = true });
        defer parsed.deinit();
        return if (parsed.value.value) |v| try alloc.dupe(u8, v) else null;
    }

    fn query(ptr: *anyopaque, alloc: Allocator, table_id: []const u8, envelope: []const u8, offset: u32, limit: u32) anyerror!resolution.Page {
        const self: *Host = @ptrCast(@alignCast(ptr));
        const encoded = try std.json.Stringify.valueAlloc(alloc, .{ .table = table_id, .envelope = envelope, .offset = offset, .limit = limit }, .{});
        defer alloc.free(encoded);
        return pageFrom(alloc, try self.call.call(alloc, .query, encoded));
    }

    fn request(ptr: *anyopaque, alloc: Allocator, table_id: []const u8, request_json: []const u8, k: u32) anyerror!resolution.Page {
        const self: *Host = @ptrCast(@alignCast(ptr));
        const encoded = try std.json.Stringify.valueAlloc(alloc, .{ .table = table_id, .request = request_json, .k = k }, .{});
        defer alloc.free(encoded);
        return pageFrom(alloc, try self.call.call(alloc, .request, encoded));
    }

    fn scan(ptr: *anyopaque, alloc: Allocator, table_id: []const u8, envelope: []const u8, after: []const u8, limit: u32) anyerror!resolution.Page {
        const self: *Host = @ptrCast(@alignCast(ptr));
        const encoded = try std.json.Stringify.valueAlloc(alloc, .{ .table = table_id, .envelope = envelope, .after = after, .limit = limit }, .{});
        defer alloc.free(encoded);
        return pageFrom(alloc, try self.call.call(alloc, .scan, encoded));
    }

    fn edges(ptr: *anyopaque, alloc: Allocator, table_id: []const u8, graph_index: []const u8, node: []const u8, types: []const []const u8, direction: @import("enrichment/context_sources.zig").GraphSpec.Direction, options: resolution.EdgeOptions) anyerror![]resolution.Edge {
        const self: *Host = @ptrCast(@alignCast(ptr));
        const encoded = try std.json.Stringify.valueAlloc(alloc, .{ .table = table_id, .graph_index = graph_index, .node = node, .edge_types = types, .direction = direction, .limit = options.limit, .include_generated = options.include_generated }, .{});
        defer alloc.free(encoded);
        const out = try self.call.call(alloc, .edges, encoded);
        defer alloc.free(out);
        const parsed = try std.json.parseFromSlice([]const struct { edge_type: []const u8, neighbor: []const u8, outgoing: bool }, alloc, out, .{ .ignore_unknown_fields = true });
        defer parsed.deinit();
        const list = try alloc.alloc(resolution.Edge, parsed.value.len);
        var filled: usize = 0;
        errdefer {
            resolution.freeEdges(alloc, list[0..filled]);
        }
        for (parsed.value, list) |src, *dst| {
            const t = try alloc.dupe(u8, src.edge_type);
            errdefer alloc.free(t);
            dst.* = .{ .edge_type = t, .neighbor = try alloc.dupe(u8, src.neighbor), .outgoing = src.outgoing };
            filled += 1;
        }
        return list;
    }

    fn artifacts(ptr: *anyopaque, alloc: Allocator, table_id: []const u8, key: []const u8, names: []const []const u8) anyerror![]u8 {
        const self: *Host = @ptrCast(@alignCast(ptr));
        const encoded = try std.json.Stringify.valueAlloc(alloc, .{ .table = table_id, .key = key, .names = names }, .{});
        defer alloc.free(encoded);
        return self.call.call(alloc, .artifacts, encoded);
    }

    fn fetchTopology(ptr: *anyopaque, alloc: Allocator) anyerror![]u8 {
        const self: *Host = @ptrCast(@alignCast(ptr));
        return self.call.call(alloc, .topology, "");
    }
};

/// Source of this table's identity and inbound dependents (JSON
/// `{"table_id","inbound":[...]}`), fetched by the group leader and
/// replicated so every replica stages notices from the same topology.
pub const Topology = struct {
    ptr: *anyopaque,
    fetch_fn: *const fn (ptr: *anyopaque, alloc: Allocator) anyerror![]u8,

    pub fn fetch(self: Topology, alloc: Allocator) ![]u8 {
        return self.fetch_fn(self.ptr, alloc);
    }
};

fn pageFrom(alloc: Allocator, owned: []u8) !resolution.Page {
    defer alloc.free(owned);
    const parsed = try std.json.parseFromSlice(WirePage, alloc, owned, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    const rows = try alloc.alloc(resolution.Row, parsed.value.rows.len);
    var filled: usize = 0;
    errdefer {
        for (rows[0..filled]) |row| {
            alloc.free(row.key);
            alloc.free(row.value);
        }
        alloc.free(rows);
    }
    for (parsed.value.rows, rows) |src, *dst| {
        const key = try alloc.dupe(u8, src.key);
        errdefer alloc.free(key);
        dst.* = .{ .key = key, .value = try alloc.dupe(u8, src.value) };
        filled += 1;
    }
    return .{ .rows = rows, .exhausted = parsed.value.exhausted };
}

test "json host adapter round trips pages and lookups" {
    const alloc = std.testing.allocator;
    const Fake = struct {
        proposed: usize = 0,
        fn call(ptr: *anyopaque, a: Allocator, op: Op, req: []const u8) anyerror![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return switch (op) {
                .propose => blk: {
                    self.proposed += 1;
                    break :blk a.dupe(u8, "");
                },
                .is_leader => a.dupe(u8, "1"),
                .lookup => if (std.mem.indexOf(u8, req, "\"k1\"") != null) a.dupe(u8, "{\"value\":\"{\\\"x\\\":1}\"}") else a.dupe(u8, "{\"value\":null}"),
                .query, .request, .scan => encodePageAlloc(a, &.{.{ .key = @constCast("r1"), .value = @constCast("{\"t\":\"a\"}") }}, true),
                .edges => a.dupe(u8, "[{\"edge_type\":\"cites\",\"neighbor\":\"n2\",\"outgoing\":true}]"),
                .artifacts => a.dupe(u8, "{\"tag\":\"x\"}"),
                .topology => a.dupe(u8, "{\"table_id\":\"srv:1\",\"inbound\":[]}"),
                .deliver => a.dupe(u8, ""),
            };
        }
    };
    var fake = Fake{};
    var host = Host{ .call = .{ .ptr = &fake, .call_fn = Fake.call } };
    try std.testing.expect(host.replication().isOwner());
    try host.replication().propose(alloc, .{ .inbox = &.{"n"} });
    try std.testing.expectEqual(@as(usize, 1), fake.proposed);
    const source = host.readSource();
    const hit = (try source.lookup_fn(source.ptr, alloc, "srv:2", "k1")).?;
    defer alloc.free(hit);
    try std.testing.expectEqualStrings("{\"x\":1}", hit);
    try std.testing.expect((try source.lookup_fn(source.ptr, alloc, "srv:2", "k2")) == null);
    var page = try source.query_fn(source.ptr, alloc, "srv:2", "{}", 0, 10);
    defer page.deinit(alloc);
    try std.testing.expectEqualStrings("r1", page.rows[0].key);
    var scanned = try source.scan_fn(source.ptr, alloc, "srv:2", "{}", "r0", 10);
    defer scanned.deinit(alloc);
    try std.testing.expectEqualStrings("r1", scanned.rows[0].key);
    const edges = try source.edges_fn(source.ptr, alloc, "srv:2", "refs", "n1", &.{}, .out, .{ .limit = 10 });
    defer resolution.freeEdges(alloc, edges);
    try std.testing.expectEqualStrings("n2", edges[0].neighbor);
    const values = try source.artifacts_fn(source.ptr, alloc, "srv:2", "n1", &.{"tag"});
    defer alloc.free(values);
    try std.testing.expectEqualStrings("{\"tag\":\"x\"}", values);
    const topo = try host.topology().fetch(alloc);
    defer alloc.free(topo);
    try std.testing.expect(std.mem.indexOf(u8, topo, "srv:1") != null);
}
