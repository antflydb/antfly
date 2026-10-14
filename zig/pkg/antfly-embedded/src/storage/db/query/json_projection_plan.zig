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
pub const Path = struct { parts: []const []const u8 };

pub fn index(part: []const u8) ?usize {
    if (part.len == 0 or std.mem.indexOfNone(u8, part, "0123456789") != null) return null;
    return std.fmt.parseInt(usize, part, 10) catch null;
}

pub fn compile(a: Allocator, paths: []const Path) !*Selector {
    const root = try new(a);
    for (paths) |path| {
        var current = root;
        for (path.parts) |part| {
            const entry = try current.fields.getOrPut(a, part);
            if (!entry.found_existing) {
                entry.key_ptr.* = try a.dupe(u8, part);
                entry.value_ptr.* = try new(a);
            }
            current = entry.value_ptr.*;
        }
    }
    try prepare(a, root);
    return root;
}
fn new(a: Allocator) !*Selector {
    const node = try a.create(Selector);
    node.* = .{};
    return node;
}
fn merge(a: Allocator, left: *Selector, right: *Selector) anyerror!*Selector {
    if (left == right) return left;
    if (left.fields.count() == 0) return right;
    if (right.fields.count() == 0) return left;
    const result = try new(a);
    var it = left.fields.iterator();
    while (it.next()) |entry| try result.fields.put(a, entry.key_ptr.*, entry.value_ptr.*);
    it = right.fields.iterator();
    while (it.next()) |entry| {
        const found = try result.fields.getOrPut(a, entry.key_ptr.*);
        if (found.found_existing) found.value_ptr.* = try merge(a, found.value_ptr.*, entry.value_ptr.*) else found.value_ptr.* = entry.value_ptr.*;
    }
    try prepare(a, result);
    return result;
}
fn prepare(a: Allocator, node: *Selector) anyerror!void {
    if (node.prepared) return;
    var it = node.fields.iterator();
    while (it.next()) |entry| {
        const child = entry.value_ptr.*;
        try prepare(a, child);
        if (index(entry.key_ptr.*)) |i| {
            const found = try node.indices.getOrPut(a, i);
            if (found.found_existing) found.value_ptr.* = try merge(a, found.value_ptr.*, child) else found.value_ptr.* = child;
        } else {
            if (node.fanout == null) node.fanout = try new(a);
            // Fanout is only applied to object array items, so these nodes do
            // not require a recursively synthesized fanout of themselves.
            try node.fanout.?.fields.put(a, entry.key_ptr.*, child);
        }
    }
    var indices = node.indices.iterator();
    while (indices.next()) |entry| {
        const child = if (node.fanout) |fanout| try merge(a, entry.value_ptr.*, fanout) else entry.value_ptr.*;
        try node.array_items.put(a, entry.key_ptr.*, child);
    }
    node.prepared = true;
}
