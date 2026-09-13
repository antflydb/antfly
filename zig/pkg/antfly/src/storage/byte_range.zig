// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: ELv2
const std = @import("std");

pub const ByteRange = struct {
    start: []const u8, // inclusive, empty = -inf
    end: []const u8, // exclusive, empty = +inf

    /// Check if key is within [start, end).
    pub fn contains(self: ByteRange, key: []const u8) bool {
        // start <= key
        if (self.start.len > 0) {
            if (std.mem.order(u8, key, self.start) == .lt) return false;
        }
        // key < end
        if (self.end.len > 0) {
            if (std.mem.order(u8, key, self.end) != .lt) return false;
        }
        return true;
    }
    /// Release a range whose nonempty bounds were allocated by the caller.
    pub fn deinit(self: *ByteRange, alloc: std.mem.Allocator) void {
        if (self.start.len > 0) alloc.free(self.start);
        if (self.end.len > 0) alloc.free(self.end);
        self.* = undefined;
    }
};
