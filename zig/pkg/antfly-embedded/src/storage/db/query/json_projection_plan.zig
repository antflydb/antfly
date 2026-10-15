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
const Allocator = std.mem.Allocator;

/// Immutable selector trie. Preparation allocates spans, never path maps per
/// source object. Numeric and fanout selectors are merged once at compilation.
pub const Selector = struct {
    fields: std.StringHashMapUnmanaged(*Selector) = .empty,
    indices: std.AutoHashMapUnmanaged(usize, *Selector) = .empty,
    array_items: std.AutoHashMapUnmanaged(usize, *Selector) = .empty,
    fanout: ?*Selector = null,
    prepared: bool = false,
};
pub const Path = struct {
    parts: []const []const u8,
    // Single-path selectors preserve ordered replacement without rebuilding
    // suffix tries for each row or nested fanout. Borrowed from the plan owner.
    selector: ?*const Selector = null,
};

pub fn index(part: []const u8) ?usize {
    if (part.len == 0 or std.mem.indexOfNone(u8, part, "0123456789") != null) return null;
    return std.fmt.parseInt(usize, part, 10) catch null;
}

// Bound both input size and synthetic union work. These limits apply before
// materializing a document and fail as an invalid request, never a stack fault.
pub const max_components = 65536;
pub const max_path_depth = 1024;
const max_nodes = 131072;
const max_steps = 1024 * 1024;
const Compiler = struct {
    a: Allocator,
    work: Allocator,
    nodes: usize = 0,
    steps: usize = 0,
    const Task = union(enum) { prepare: *Selector, derive: *Selector, finish: *Selector };
    fn step(self: *@This()) !void {
        if (self.steps == max_steps) return error.InvalidQueryRequest;
        self.steps += 1;
    }
    fn node(self: *@This()) !*Selector {
        if (self.nodes == max_nodes) return error.InvalidQueryRequest;
        self.nodes += 1;
        const value = try self.a.create(Selector);
        value.* = .{};
        return value;
    }
    // Build only the union's fields. Preparation is scheduled separately so
    // numeric aliases and nested fanout never recurse through native frames.
    fn merge(self: *@This(), left: *Selector, right: *Selector) !*Selector {
        const Pair = struct { left: *Selector, right: *Selector, result: *Selector };
        if (left == right or right.fields.count() == 0) return left;
        if (left.fields.count() == 0) return right;
        const result = try self.node();
        var pending: std.ArrayListUnmanaged(Pair) = .empty;
        defer pending.deinit(self.work);
        try pending.append(self.work, .{ .left = left, .right = right, .result = result });
        while (pending.pop()) |pair| {
            var it = pair.left.fields.iterator();
            while (it.next()) |entry| {
                try self.step();
                try pair.result.fields.put(self.a, entry.key_ptr.*, entry.value_ptr.*);
            }
            it = pair.right.fields.iterator();
            while (it.next()) |entry| {
                try self.step();
                const found = try pair.result.fields.getOrPut(self.a, entry.key_ptr.*);
                const r = entry.value_ptr.*;
                if (!found.found_existing) {
                    found.value_ptr.* = r;
                    continue;
                }
                const l = found.value_ptr.*;
                if (l == r or r.fields.count() == 0) continue;
                if (l.fields.count() == 0) {
                    found.value_ptr.* = r;
                    continue;
                }
                const child = try self.node();
                found.value_ptr.* = child;
                try pending.append(self.work, .{ .left = l, .right = r, .result = child });
            }
        }
        return result;
    }
    fn prepare(self: *@This(), root: *Selector) !void {
        var tasks: std.ArrayListUnmanaged(Task) = .empty;
        defer tasks.deinit(self.work);
        try tasks.append(self.work, .{ .prepare = root });
        while (tasks.pop()) |task| {
            try self.step();
            switch (task) {
                .finish => |value| value.prepared = true,
                .prepare => |value| {
                    if (value.prepared) continue;
                    try tasks.append(self.work, .{ .derive = value });
                    var it = value.fields.iterator();
                    while (it.next()) |entry| try tasks.append(self.work, .{ .prepare = entry.value_ptr.* });
                },
                .derive => |value| {
                    var it = value.fields.iterator();
                    while (it.next()) |entry| {
                        try self.step();
                        const child = entry.value_ptr.*;
                        if (index(entry.key_ptr.*)) |i| {
                            const found = try value.indices.getOrPut(self.a, i);
                            found.value_ptr.* = if (found.found_existing) try self.merge(found.value_ptr.*, child) else child;
                        } else {
                            if (value.fanout == null) value.fanout = try self.node();
                            // Fanout is used only for object array items.
                            try value.fanout.?.fields.put(self.a, entry.key_ptr.*, child);
                        }
                    }
                    var indices = value.indices.iterator();
                    while (indices.next()) |entry| {
                        const child = if (value.fanout) |fanout| try self.merge(entry.value_ptr.*, fanout) else entry.value_ptr.*;
                        try value.array_items.put(self.a, entry.key_ptr.*, child);
                    }
                    try tasks.append(self.work, .{ .finish = value });
                    var items = value.array_items.iterator();
                    while (items.next()) |entry| try tasks.append(self.work, .{ .prepare = entry.value_ptr.* });
                },
            }
        }
    }
};
pub fn compile(a: Allocator, paths: []const Path) !*Selector {
    var components: usize = 0;
    var bytes: usize = 0;
    for (paths) |path| {
        if (path.parts.len > max_path_depth or path.parts.len > max_components -| components) return error.InvalidQueryRequest;
        components += path.parts.len;
        for (path.parts) |part| {
            if (part.len > max_steps -| bytes) return error.InvalidQueryRequest;
            bytes += part.len;
        }
    }
    var compiler = Compiler{ .a = a, .work = @import("antfly_platform").allocator.reclaimingScratch(a) };
    const root = try compiler.node();
    for (paths) |path| {
        var current = root;
        for (path.parts) |part| {
            const entry = try current.fields.getOrPut(a, part);
            if (!entry.found_existing) {
                entry.key_ptr.* = try a.dupe(u8, part);
                entry.value_ptr.* = try compiler.node();
            }
            current = entry.value_ptr.*;
        }
    }
    try compiler.prepare(root);
    return root;
}
