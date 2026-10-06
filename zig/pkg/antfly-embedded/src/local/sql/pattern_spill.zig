// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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
