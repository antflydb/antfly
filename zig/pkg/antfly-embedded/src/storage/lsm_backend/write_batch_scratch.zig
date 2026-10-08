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

/// Reuse small writer batches without retaining an exceptional batch's peak.
pub const Scratch = struct {
    pub const max_retained_keys = 1024;
    keys: std.ArrayListUnmanaged([]const u8) = .empty,
    indexes: std.ArrayListUnmanaged(usize) = .empty,
    values: std.ArrayListUnmanaged(?[]const u8) = .empty,
    pub fn prepareKeys(self: *Scratch, a: std.mem.Allocator, n: usize) !void {
        try self.keys.ensureTotalCapacityPrecise(a, n);
        try self.indexes.ensureTotalCapacityPrecise(a, n);
        self.keys.items.len = n;
        self.indexes.items.len = n;
    }
    pub fn prepareValues(self: *Scratch, a: std.mem.Allocator, n: usize) !void {
        try self.values.ensureTotalCapacityPrecise(a, n);
        self.values.items.len = n;
    }
    pub fn prepare(self: *Scratch, a: std.mem.Allocator, n: usize) !void {
        try self.prepareKeys(a, n);
        try self.prepareValues(a, n);
    }
    pub fn deinit(self: *Scratch, a: std.mem.Allocator) void {
        self.keys.deinit(a);
        self.indexes.deinit(a);
        self.values.deinit(a);
        self.* = .{};
    }
};

test "lsm writer batch scratch reuses allocations and unwinds failures" {
    const Fixture = struct {
        fn run(a: std.mem.Allocator) !void {
            var scratch: Scratch = .{};
            defer scratch.deinit(a);
            try scratch.prepare(a, 16);
            const keys = scratch.keys.items.ptr;
            const indexes = scratch.indexes.items.ptr;
            const values = scratch.values.items.ptr;
            try scratch.prepare(a, 16);
            try std.testing.expect(keys == scratch.keys.items.ptr);
            try std.testing.expect(indexes == scratch.indexes.items.ptr);
            try std.testing.expect(values == scratch.values.items.ptr);
            try scratch.prepare(a, 32);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}
