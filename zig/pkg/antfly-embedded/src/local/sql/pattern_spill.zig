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

//! Statement-owned, reusable pattern sets. Each evaluation reads one pattern.
const std = @import("std");
const scalar = @import("scalar.zig");
const spill = @import("spill.zig");
pub const Set = struct {
    interface: scalar.PatternSet,
    file: *spill.File,
    start: ?u64 = null,
    end: u64 = 0,
    pub fn create(manager: *spill.Manager) !*Set {
        const self = try manager.alloc.create(Set);
        errdefer manager.alloc.destroy(self);
        self.start = null;
        self.end = 0;
        if (manager.pattern_file == null) {
            const file = try manager.alloc.create(spill.File);
            errdefer manager.alloc.destroy(file);
            file.* = try manager.create();
            manager.pattern_file = file;
        }
        self.file = manager.pattern_file.?;
        self.interface = .{ .ptr = self, .count = 0, .next = next, .close = close };
        try manager.registerPattern(&self.interface);
        return self;
    }
    pub fn append(self: *Set, value: scalar.Datum) !void {
        if (!value.sql_null and value.value != .string) return error.SqlTypeMismatch;
        if (self.start == null) self.start = self.file.size;
        if (self.interface.count != 0 and self.end != self.file.size) return error.InvalidSqlSpill;
        _ = try self.file.append(.{ .values = &.{value}, .keys = &.{}, .ordinal = self.interface.count }, spill.none);
        self.end = self.file.size;
        self.interface.count += 1;
    }
    fn next(raw: *anyopaque, a: std.mem.Allocator, offset: *u64) !?scalar.Datum {
        const self: *Set = @ptrCast(@alignCast(raw));
        const start = self.start orelse return null;
        if (offset.* == self.end - start) return null;
        if (offset.* > self.end - start) return error.InvalidSqlSpill;
        const record = try self.file.read(a, start + offset.*);
        if (record.following > self.end) return error.InvalidSqlSpill;
        offset.* = record.following - start;
        if (record.row.values.len != 1) return error.InvalidSqlSpill;
        return record.row.values[0];
    }
    fn close(raw: *anyopaque) void {
        const self: *Set = @ptrCast(@alignCast(raw));
        const a = self.file.manager.alloc;
        a.destroy(self);
    }
};
