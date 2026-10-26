// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Shared AOT native-source emission; target policy belongs to each owner.
const std = @import("std");

pub const Emitter = struct {
    allocator: std.mem.Allocator,
    bytes: std.ArrayList(u8) = .empty,
    pub fn raw(self: *Emitter, text: []const u8) !void {
        try self.bytes.appendSlice(self.allocator, text);
    }
    pub fn add(self: *Emitter, comptime fmt: []const u8, args: anytype) !void {
        const text = try std.fmt.allocPrint(self.allocator, fmt, args);
        defer self.allocator.free(text);
        try self.raw(text);
    }
};
