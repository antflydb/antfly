// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Immutable, authenticated retained-frame view. Validation happens once;
//! ordinal boundaries permit exact durable-cursor resumption without scanning
//! consumed effects. The owner must retain immutable bytes until deinit.
const std = @import("std");
const retained = @import("retained_effects.zig");

pub const Frame = struct {
    reader: retained.Reader,
    offsets: []u32,
    alloc: std.mem.Allocator,

    pub fn init(alloc: std.mem.Allocator, bytes: []const u8, sequence: u64) !Frame {
        var reader = try retained.Reader.init(bytes, sequence);
        const offsets = try alloc.alloc(u32, @as(usize, reader.remaining) + 1);
        errdefer alloc.free(offsets);
        for (offsets[0 .. offsets.len - 1]) |*offset| {
            offset.* = @intCast(reader.pos);
            _ = (try reader.next()) orelse return error.RetainedEffectsCorrupt;
        }
        offsets[offsets.len - 1] = @intCast(reader.pos);
        _ = try reader.next();
        reader.pos = 16;
        reader.remaining = @intCast(offsets.len - 1);
        return .{ .reader = reader, .offsets = offsets, .alloc = alloc };
    }

    pub fn deinit(self: *Frame) void {
        self.alloc.free(self.offsets);
        self.* = undefined;
    }

    /// Both offset and remaining count are replicated progress. Direct ordinal
    /// addressing validates their correspondence in O(1), without trusting an
    /// arbitrary byte offset or reparsing the consumed prefix.
    pub fn readerAt(self: *const Frame, offset: u32, remaining: u32) !retained.Reader {
        if (offset == 0) {
            if (remaining != 0) return error.InvalidRestoreStagingRecord;
            return self.reader;
        }
        if (remaining > self.reader.remaining) return error.InvalidRestoreStagingRecord;
        const ordinal = self.reader.remaining - remaining;
        if (self.offsets[ordinal] != offset) return error.InvalidRestoreStagingRecord;
        var reader = self.reader;
        reader.pos = offset;
        reader.remaining = remaining;
        return reader;
    }
};
