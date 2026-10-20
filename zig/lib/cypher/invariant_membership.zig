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

//! An owned invariant RHS. Nullable structures keep ordinary three-valued
//! comparisons; a collision-checked index serves the null-free equality domain.
const std = @import("std");
const v = @import("value.zig");
const w = @import("value_work.zig");
pub const Membership = struct {
    value: v.Value,
    index: w.Index = .{},
    uncertain: std.ArrayList(v.Value) = .empty,
    has_null: bool = false,
    pub fn init(a: std.mem.Allocator, backing: std.mem.Allocator, bytes: *?*v.BytePool, metadata: *w.Work, input: v.Value) !Membership {
        var scratch = std.heap.ArenaAllocator.init(backing);
        defer scratch.deinit();
        // Equality keys omit entity payloads, share immutable DAG allocations,
        // and pool wide strings outside the geometrically growing arena.
        var context: v.CloneContext = .{ .a = a, .scratch = scratch.allocator(), .identity_only = true, .byte_pool = bytes, .byte_allocator = backing };
        var self: Membership = .{ .value = try context.clone(input) };
        if (self.value != .list) return self;
        for (self.value.list, 0..) |item, i| {
            const info = try metadata.info(item);
            if (item == .null) self.has_null = true else if (info.unknown) {
                try self.uncertain.append(a, item);
            } else try self.index.insert(a, info.hash, i);
        }
        _ = try metadata.info(self.value);
        return self;
    }
    pub fn contains(self: *const Membership, work: *w.Work, left: v.Value) !v.Value {
        if (self.value == .null) return .null;
        if (self.value != .list) return error.CypherTypeMismatch;
        if (self.value.list.len == 0) return .{ .boolean = false };
        if (left == .null) return .null;
        const info = try work.info(left);
        var unknown = self.has_null;
        if (info.unknown) {
            // A nullable probe can be unknown against an otherwise null-free
            // key with a different hash. Do not use grouping NULL equality.
            for (self.value.list) |item| {
                if (try work.equal(left, item)) |same| {
                    if (same) return .{ .boolean = true };
                } else unknown = true;
            }
        } else {
            var candidate = self.index.head(info.hash);
            while (candidate) |position| {
                const link = self.index.links.items[position];
                if ((try work.equal(left, self.value.list[link.index])) == true) return .{ .boolean = true };
                candidate = link.next;
            }
            for (self.uncertain.items) |item| {
                if (try work.equal(left, item)) |same| {
                    if (same) return .{ .boolean = true };
                } else unknown = true;
            }
        }
        return if (unknown) .null else .{ .boolean = false };
    }
};
