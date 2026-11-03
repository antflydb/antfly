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

//! Physical bounded document window, shared by owner-side ranking/hydration.
const std = @import("std");
const Source = @import("read_session_source_types").types;
pub const Implementation = struct {
    pub const max_bytes: usize = 8 * 1024 * 1024;
    arena: std.heap.ArenaAllocator,
    scope: ?Source.backend_erased.ReadScope = null,
    bytes: usize = 0,
    retirements: usize = 0,
    pub fn init(alloc: std.mem.Allocator) @This() {
        return .{ .arena = .init(Source.platform.allocator.reclaimingScratch(alloc)) };
    }
    pub fn retire(self: *@This()) void {
        if (self.scope) |*scope| {
            scope.close();
            self.scope = null;
            self.retirements += 1;
        }
        _ = self.arena.reset(.free_all);
        self.bytes = 0;
    }
    pub fn deinit(self: *@This()) void {
        self.retire();
        self.arena.deinit();
    }
    pub fn get(self: *@This(), txn: *Source.store.DocStore.Txn, physical: []const u8, expected: ?u64) ![]const u8 {
        if (self.bytes > 0 and (expected == null or expected.? > max_bytes -| self.bytes)) self.retire();
        if (self.scope == null) self.scope = try txn.openReadScope(self.arena.allocator());
        const raw = try self.scope.?.get(physical);
        self.bytes +|= raw.len;
        return raw;
    }
    pub fn getMany(self: *@This(), txn: *Source.store.DocStore.Txn, physical: []const []const u8, output: []?[]const u8) !void {
        if (physical.len > 64 or output.len != physical.len) return error.InvalidBatch;
        self.retire();
        self.scope = try txn.openReadScope(self.arena.allocator());
        self.bytes = try readMany(&self.scope.?, physical, output);
    }
    pub fn readMany(scope: *Source.backend_erased.ReadScope, physical: []const []const u8, output: []?[]const u8) !usize {
        if (physical.len > 64 or output.len != physical.len) return error.InvalidBatch;
        const Entry = struct {
            key: []const u8,
            row: usize,
            value_index: usize = 0,
            pub fn less(_: void, left: @This(), right: @This()) bool {
                return std.mem.order(u8, left.key, right.key) == .lt;
            }
        };
        var entries: [64]Entry = undefined;
        for (physical, 0..) |key, i| entries[i] = .{ .key = key, .row = i };
        std.mem.sort(Entry, entries[0..physical.len], {}, Entry.less);
        var keys: [64][]const u8 = undefined;
        var values: [64]?[]const u8 = undefined;
        var count: usize = 0;
        for (entries[0..physical.len]) |*entry| {
            if (count == 0 or !std.mem.eql(u8, keys[count - 1], entry.key)) {
                keys[count] = entry.key;
                count += 1;
            }
            entry.value_index = count - 1;
        }
        try scope.getManySorted(keys[0..count], values[0..count]);
        for (entries[0..physical.len]) |entry| output[entry.row] = values[entry.value_index];
        // Duplicate keys borrow the same retained value. Account actual scope
        // payload once, independently of caller order or repeated rows.
        var bytes: usize = 0;
        for (values[0..count]) |value| if (value) |raw| {
            bytes +|= raw.len;
        };
        return bytes;
    }

    pub fn finishRow(self: *@This(), uncached: bool) void {
        if (uncached or self.bytes >= max_bytes) self.retire();
    }
};
