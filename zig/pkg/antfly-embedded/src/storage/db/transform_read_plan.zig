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

//! Request-only planning for transform bases. No DB, executor or schema owner.
//! Keys are borrowed from the request; the plan owns only its containers.
const std = @import("std");

pub const Plan = struct {
    established: std.StringHashMapUnmanaged(void) = .empty,
    durable_bases: std.ArrayListUnmanaged([]const u8) = .empty,

    pub fn deinit(self: *Plan, alloc: std.mem.Allocator) void {
        self.established.deinit(alloc);
        self.durable_bases.deinit(alloc);
        self.* = .{};
    }

    /// Writes and deletes establish a request-local base, including absence.
    pub fn establish(self: *Plan, alloc: std.mem.Allocator, key: []const u8) !void {
        try self.established.put(alloc, key, {});
    }

    /// Only the first transform of an otherwise untouched key needs a pinned
    /// durable read. Planning cannot read or mutate storage.
    pub fn transform(self: *Plan, alloc: std.mem.Allocator, key: []const u8) !void {
        if (self.established.contains(key)) return;
        // Reserve both containers before publishing either change. The plan
        // remains usable if an allocation fails.
        try self.established.ensureUnusedCapacity(alloc, 1);
        try self.durable_bases.ensureUnusedCapacity(alloc, 1);
        self.established.putAssumeCapacity(key, {});
        self.durable_bases.appendAssumeCapacity(key);
    }
};

test "transform plan reads each durable base once and preserves request order" {
    const alloc = std.testing.allocator;
    var plan: Plan = .{};
    defer plan.deinit(alloc);
    try plan.establish(alloc, "written");
    try plan.establish(alloc, "deleted");
    for ([_][]const u8{ "second", "written", "first", "deleted", "second" }) |key|
        try plan.transform(alloc, key);
    try std.testing.expectEqual(@as(usize, 2), plan.durable_bases.items.len);
    try std.testing.expectEqualStrings("second", plan.durable_bases.items[0]);
    try std.testing.expectEqualStrings("first", plan.durable_bases.items[1]);
}

test "transform plan releases every failed allocation" {
    const Check = struct {
        fn run(alloc: std.mem.Allocator) !void {
            var plan: Plan = .{};
            defer plan.deinit(alloc);
            try plan.establish(alloc, "written");
            for ([_][]const u8{ "a", "b", "a", "written", "c" }) |key|
                try plan.transform(alloc, key);
            try std.testing.expectEqual(@as(usize, 3), plan.durable_bases.items.len);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Check.run, .{});
}
