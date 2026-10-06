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

//! Column-addressable blocks and disk directories under one statement quota.
const std = @import("std");
const spill = @import("spill.zig");
const disk = @import("disk_rows.zig");
const operators = @import("operators.zig");
const Datum = @import("scalar.zig").Datum;
const A = std.mem.Allocator;
pub const Store = struct {
    a: A,
    file: spill.File,
    directory: disk.Integers,
    locations: disk.Integers,
    ordinals: disk.Integers,
    width: usize,
    pending: std.ArrayList(operators.Row) = .empty,
    arena: std.heap.ArenaAllocator,
    bytes: usize = 0,
    cache: [4]Entry,
    tick: u64 = 0,
    decodes: usize = 0,
    const Entry = struct { arena: std.heap.ArenaAllocator, offset: ?usize = null, values: []const Datum = &.{}, touched: u64 = 0 };
    pub fn init(a: A, manager: *spill.Manager, width: usize) !Store {
        var file = try manager.create();
        errdefer file.close();
        var directory = try disk.Integers.init(manager);
        errdefer directory.deinit();
        var locations = try disk.Integers.init(manager);
        errdefer locations.deinit();
        const ordinals = try disk.Integers.init(manager);
        var self: Store = .{ .a = a, .file = file, .directory = directory, .locations = locations, .ordinals = ordinals, .width = width, .arena = .init(a), .cache = undefined };
        for (&self.cache) |*entry| entry.* = .{ .arena = .init(a) };
        return self;
    }
    pub fn deinit(self: *Store) void {
        for (&self.cache) |*entry| entry.arena.deinit();
        self.pending.deinit(self.a);
        self.arena.deinit();
        self.ordinals.deinit();
        self.locations.deinit();
        self.directory.deinit();
        self.file.close();
    }
    pub fn append(self: *Store, row: operators.Row) !void {
        if (row.values.len != self.width) return error.InvalidSqlSpill;
        var bytes: usize = @sizeOf(operators.Row) + row.values.len * @sizeOf(Datum);
        for (row.values) |value| bytes +|= try operators.datumBytes(value);
        if (self.pending.items.len != 0 and (self.pending.items.len >= 64 or bytes > @min(64 * 1024, self.file.manager.max_record_bytes / 4) -| self.bytes)) try self.flush();
        const a = self.arena.allocator();
        const values = try a.alloc(Datum, self.width);
        for (row.values, values) |value, *out| out.* = try operators.cloneDatum(a, value);
        try self.pending.append(self.a, .{ .values = values, .keys = &.{}, .ordinal = row.ordinal });
        self.bytes +|= bytes;
    }
    pub fn flush(self: *Store) !void {
        if (self.pending.items.len == 0) return;
        const base = self.directory.len;
        const cells = try self.a.alloc(Datum, self.pending.items.len);
        defer self.a.free(cells);
        for (0..self.width) |column| {
            for (self.pending.items, cells) |row, *out| out.* = row.values[column];
            const offset = try self.file.append(.{ .values = cells, .keys = &.{}, .ordinal = base }, spill.none);
            try self.directory.append(@intCast(offset));
        }
        for (self.pending.items, 0..) |row, lane| {
            try self.locations.append(base);
            try self.locations.append(lane);
            try self.ordinals.append(@intCast(row.ordinal));
        }
        self.pending.clearRetainingCapacity();
        _ = self.arena.reset(.free_all);
        self.bytes = 0;
    }
    pub fn cell(self: *Store, row: usize, column: usize) !Datum {
        try self.flush();
        if (column >= self.width or row >= self.ordinals.len) return error.InvalidSqlSpill;
        const base = try self.locations.at(row * 2);
        const lane = try self.locations.at(row * 2 + 1);
        const offset = try self.directory.at(base + column);
        self.tick +%= 1;
        var oldest = &self.cache[0];
        for (&self.cache) |*entry| {
            if (entry.offset == offset) {
                entry.touched = self.tick;
                if (lane >= entry.values.len) return error.InvalidSqlSpill;
                return entry.values[lane];
            }
            if (entry.offset == null or entry.touched < oldest.touched) oldest = entry;
        }
        oldest.offset = null;
        _ = oldest.arena.reset(.free_all);
        const decoded = try self.file.read(oldest.arena.allocator(), offset);
        if (decoded.row.ordinal != base or lane >= decoded.row.values.len) return error.InvalidSqlSpill;
        oldest.values = decoded.row.values;
        oldest.offset = offset;
        oldest.touched = self.tick;
        self.decodes += 1;
        return oldest.values[lane];
    }
};
