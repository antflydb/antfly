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

//! Durable compressed radix indexes over historically owned proposal keys.
//! Compressed labels keep insertion proportional to key length, with at most
//! two nodes per distinct key. Physical keys contain only fixed-size digests.
const std = @import("std");
const records = @import("resolver_authority.zig");
const A = std.mem.Allocator;
pub fn encodedKey(a: A, raw: []const u8, reverse: bool) ![]const u8 {
    const bytes = try a.dupe(u8, raw);
    if (reverse) std.mem.reverse(u8, bytes);
    return std.fmt.allocPrint(a, "{x}", .{bytes});
}
pub const Node = struct { refs: u64, terminal: ?[]const u8 = null };
pub const Edge = struct { label: []const u8, child: []const u8 };
pub const Cut = struct { id: []const u8, refs: u64 };
pub const Index = struct {
    a: A,
    backend: records.Backend,
    root: []const u8,
    fn digest(self: Index, raw: []const u8) ![]const u8 {
        return records.routes.digest(self.a, raw);
    }
    fn path(self: Index, kind: []const u8, id: []const u8) ![]const u8 {
        return std.fmt.allocPrint(self.a, "{s}{s}/{s}", .{ self.root, kind, id });
    }
    fn node(self: Index, id: []const u8) !?Node {
        const raw = try self.backend.get(self.backend.ptr, self.a, try self.path("node", id)) orelse return null;
        const value = try std.json.parseFromSliceLeaky(Node, self.a, raw, .{ .allocate = .alloc_always });
        if (value.refs == 0) return error.InvalidCatalogRecord;
        return value;
    }
    fn putNode(self: Index, id: []const u8, value: Node) !void {
        try self.backend.put(self.backend.ptr, self.a, try self.path("node", id), try std.json.Stringify.valueAlloc(self.a, value, .{}));
    }
    fn edgePath(self: Index, id: []const u8, byte: u8) ![]const u8 {
        return std.fmt.allocPrint(self.a, "{s}edge/{s}/{x:0>2}", .{ self.root, id, byte });
    }
    fn edge(self: Index, id: []const u8, byte: u8) !?Edge {
        const raw = try self.backend.get(self.backend.ptr, self.a, try self.edgePath(id, byte)) orelse return null;
        return try std.json.parseFromSliceLeaky(Edge, self.a, raw, .{ .allocate = .alloc_always });
    }
    fn putEdge(self: Index, parent: []const u8, value: Edge) !void {
        if (value.label.len == 0) return error.InvalidCatalogRecord;
        try self.backend.put(self.backend.ptr, self.a, try self.edgePath(parent, value.label[0]), try std.json.Stringify.valueAlloc(self.a, value, .{}));
    }
    pub fn insert(self: Index, key: []const u8, original: []const u8) !void {
        var id = try self.digest("");
        var offset: usize = 0;
        var ancestors: std.ArrayList([]const u8) = .empty;
        while (true) {
            const current = (try self.node(id)) orelse Node{ .refs = 0 };
            if (offset == key.len) {
                if (current.terminal != null) return;
                try self.putNode(id, .{ .refs = try std.math.add(u64, current.refs, 1), .terminal = original });
                break;
            }
            const link = try self.edge(id, key[offset]);
            if (link == null) {
                const child = try self.digest(key);
                try self.putNode(child, .{ .refs = 1, .terminal = original });
                try self.putEdge(id, .{ .label = key[offset..], .child = child });
                try self.putNode(id, .{ .refs = try std.math.add(u64, current.refs, 1), .terminal = current.terminal });
                break;
            }
            const existing = link.?;
            const remaining = key[offset..];
            var common: usize = 0;
            while (common < remaining.len and common < existing.label.len and remaining[common] == existing.label[common]) : (common += 1) {}
            if (common == existing.label.len) {
                try ancestors.append(self.a, id);
                id = existing.child;
                offset += common;
                continue;
            }
            if (common == 0) return error.InvalidCatalogRecord;
            const old_child = (try self.node(existing.child)) orelse return error.InvalidCatalogRecord;
            const split = try self.digest(key[0 .. offset + common]);
            try self.putEdge(split, .{ .label = existing.label[common..], .child = existing.child });
            const is_terminal = common == remaining.len;
            try self.putNode(split, .{ .refs = try std.math.add(u64, old_child.refs, 1), .terminal = if (is_terminal) original else null });
            if (!is_terminal) {
                const child = try self.digest(key);
                try self.putNode(child, .{ .refs = 1, .terminal = original });
                try self.putEdge(split, .{ .label = remaining[common..], .child = child });
            }
            try self.putEdge(id, .{ .label = existing.label[0..common], .child = split });
            try self.putNode(id, .{ .refs = try std.math.add(u64, current.refs, 1), .terminal = current.terminal });
            break;
        }
        for (ancestors.items) |ancestor| {
            var parent = (try self.node(ancestor)) orelse return error.InvalidCatalogRecord;
            parent.refs = try std.math.add(u64, parent.refs, 1);
            try self.putNode(ancestor, parent);
        }
    }
    pub fn prefix(self: Index, key: []const u8) !?Cut {
        var id = try self.digest("");
        var offset: usize = 0;
        while (offset < key.len) {
            const link = (try self.edge(id, key[offset])) orelse return null;
            const remaining = key[offset..];
            const count = @min(remaining.len, link.label.len);
            if (!std.mem.eql(u8, remaining[0..count], link.label[0..count])) return null;
            offset += count;
            id = link.child;
        }
        const value = (try self.node(id)) orelse return null;
        return .{ .id = id, .refs = value.refs };
    }
    pub fn matches(self: Index, cut: Cut, template: []const u8) !bool {
        var pending: std.ArrayList([]const u8) = .empty;
        try pending.append(self.a, cut.id);
        var offset: usize = 0;
        var seen: std.StringHashMapUnmanaged(void) = .empty;
        while (offset < pending.items.len) : (offset += 1) {
            const id = pending.items[offset];
            if ((try seen.getOrPut(self.a, id)).found_existing) return error.InvalidCatalogRecord;
            if (seen.count() > 16384) return error.ResolverCurationLimitExceeded;
            const value = (try self.node(id)) orelse return error.InvalidCatalogRecord;
            if (value.terminal) |key| if (@import("resolver_namespace.zig").keyInNamespace(template, key)) return true;
            const edge_prefix = try std.fmt.allocPrint(self.a, "{s}edge/{s}/", .{ self.root, id });
            var after: ?[]const u8 = null;
            while (true) {
                const page = try self.backend.scan(self.backend.ptr, self.a, edge_prefix, after, 128);
                for (page.rows) |row| {
                    const child = try std.json.parseFromSliceLeaky(Edge, self.a, row.value, .{ .allocate = .alloc_always });
                    try pending.append(self.a, child.child);
                }
                after = page.next;
                if (after == null) break;
            }
        }
        return false;
    }
};
