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

//! Deterministic logical diagnostic artifacts emitted by scenarios.

const std = @import("std");
const ids = @import("id.zig");

pub const Record = struct {
    choice_prefix: usize,
    name: []const u8,
    bytes: []const u8,
    digest: u64,
};

pub const Sink = struct {
    allocator: std.mem.Allocator,
    choice_prefix: usize,
    records: std.ArrayListUnmanaged(Record) = .empty,

    pub fn init(allocator: std.mem.Allocator, choice_prefix: usize) Sink {
        return .{ .allocator = allocator, .choice_prefix = choice_prefix };
    }

    pub fn deinit(self: *Sink) void {
        for (self.records.items) |record| {
            self.allocator.free(record.name);
            self.allocator.free(record.bytes);
        }
        self.records.deinit(self.allocator);
        self.* = undefined;
    }

    pub fn add(self: *Sink, name: []const u8, bytes: []const u8) !void {
        if (name.len == 0) return error.EmptyCollectorName;
        for (self.records.items) |record| if (std.mem.eql(u8, record.name, name))
            return error.DuplicateCollectorName;
        const owned_name = try self.allocator.dupe(u8, name);
        errdefer self.allocator.free(owned_name);
        const owned_bytes = try self.allocator.dupe(u8, bytes);
        errdefer self.allocator.free(owned_bytes);
        try self.records.append(self.allocator, .{
            .choice_prefix = self.choice_prefix,
            .name = owned_name,
            .bytes = owned_bytes,
            .digest = ids.digest(bytes),
        });
    }

    pub fn canonicalize(self: *Sink) void {
        std.mem.sort(Record, self.records.items, {}, struct {
            fn lessThan(_: void, left: Record, right: Record) bool {
                return std.mem.order(u8, left.name, right.name) == .lt;
            }
        }.lessThan);
    }

    pub fn renderAlloc(self: *const Sink, allocator: std.mem.Allocator) ![]u8 {
        return std.json.Stringify.valueAlloc(allocator, self.records.items, .{ .whitespace = .indent_2 });
    }
};

test "collector artifacts are owned canonical and prefix tagged" {
    var sink = Sink.init(std.testing.allocator, 7);
    defer sink.deinit();
    try sink.add("tasks", "two");
    try sink.add("files", "one");
    sink.canonicalize();
    try std.testing.expectEqualStrings("files", sink.records.items[0].name);
    try std.testing.expectEqual(@as(usize, 7), sink.records.items[0].choice_prefix);
    try std.testing.expectError(error.DuplicateCollectorName, sink.add("files", "again"));
}
