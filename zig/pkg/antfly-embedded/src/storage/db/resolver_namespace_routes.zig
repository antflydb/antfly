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

//! Shared durable namespace-prefix routing for Lite and metadata consumers.
const std = @import("std");
const A = std.mem.Allocator;
const RouteNode = struct { refs: u64 = 0, terminal: u64 = 0 };
pub fn digest(a: A, raw: []const u8) ![]const u8 {
    var hash: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(raw, &hash, .{});
    return a.dupe(u8, &std.fmt.bytesToHex(hash, .lower));
}
pub fn update(a: A, txn: anytype, root: []const u8, template: []const u8, add: bool) !void {
    const static_prefix = template[0 .. std.mem.indexOf(u8, template, "{{") orelse template.len];
    const edge = try std.fmt.allocPrint(a, "{s}route/{s}/{s}", .{ root, try digest(a, static_prefix), try digest(a, template) });
    const exists = if (txn.get(edge)) |_| true else |err| if (err == error.NotFound) false else return err;
    if (exists == add) return;
    for (0..static_prefix.len + 1) |length| {
        const path = try std.fmt.allocPrint(a, "{s}node/{s}", .{ root, try digest(a, static_prefix[0..length]) });
        var node = if (txn.get(path)) |raw| try std.json.parseFromSliceLeaky(RouteNode, a, raw, .{}) else |err| if (err == error.NotFound and add) RouteNode{} else return err;
        if (add) {
            node.refs = try std.math.add(u64, node.refs, 1);
            if (length == static_prefix.len) node.terminal = try std.math.add(u64, node.terminal, 1);
        } else {
            node.refs = try std.math.sub(u64, node.refs, 1);
            if (length == static_prefix.len) node.terminal = try std.math.sub(u64, node.terminal, 1);
        }
        if (node.refs == 0) try txn.delete(path) else try txn.put(path, try std.json.Stringify.valueAlloc(a, node, .{}));
    }
    if (add) try txn.put(edge, try std.json.Stringify.valueAlloc(a, template, .{})) else try txn.delete(edge);
}
pub fn matching(a: A, txn: anytype, root: []const u8, key: []const u8) ![][]const u8 {
    var result: std.ArrayList([]const u8) = .empty;
    for (0..key.len + 1) |length| {
        const prefix_digest = try digest(a, key[0..length]);
        const node_raw = txn.get(try std.fmt.allocPrint(a, "{s}node/{s}", .{ root, prefix_digest })) catch |err| switch (err) {
            error.NotFound => break,
            else => return err,
        };
        const node = try std.json.parseFromSliceLeaky(RouteNode, a, node_raw, .{});
        if (node.terminal == 0) continue;
        const bucket = try std.fmt.allocPrint(a, "{s}route/{s}/", .{ root, prefix_digest });
        var cursor = try txn.openCursor();
        defer cursor.close();
        var row = cursor.seekAtOrAfter(bucket) catch |err| if (err == error.NotFound) null else return err;
        while (row) |entry| {
            if (!std.mem.startsWith(u8, entry.key, bucket)) break;
            const template = try std.json.parseFromSliceLeaky([]const u8, a, entry.value, .{ .allocate = .alloc_always });
            if (@import("resolver_namespace.zig").keyInNamespace(template, key)) try result.append(a, template);
            row = cursor.next() catch |err| if (err == error.NotFound) null else return err;
        }
    }
    return result.items;
}
