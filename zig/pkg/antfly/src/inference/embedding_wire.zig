// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: LicenseRef-Elastic-2.0

//! Shared linked-provider metadata contract. Binary bytes travel separately;
//! planning counts the same JSON representation emitted by the provider ABI.
const std = @import("std");

pub const Options = struct {
    model: []const u8 = "",
    task_type: ?[]const u8 = null,
    instruction: ?[]const u8 = null,
};

pub fn Request(comptime Part: type) type {
    return struct {
        model: []const u8,
        parts: []const Part,
        attachment_count: usize,
        task_type: ?[]const u8,
        instruction: ?[]const u8,
    };
}

pub fn metadataPart(part: anytype) @TypeOf(part) {
    return switch (part) {
        .binary => |binary| .{ .binary = .{ .mime_type = binary.mime_type, .data = &.{} } },
        else => part,
    };
}

fn jsonSize(value: anytype) !usize {
    var buffer: [256]u8 = undefined;
    var counter = std.Io.Writer.Discarding.init(&buffer);
    try std.json.Stringify.value(value, .{}, &counter.writer);
    return std.math.cast(usize, counter.fullCount()) orelse error.BodyTooLarge;
}

/// Every text/URL/MIME is scanned once, without copying binary payload bytes.
pub const Sizer = struct {
    base_bytes: usize,
    parts_bytes: usize = 0,
    count: usize = 0,
    attachment_count: usize = 0,

    pub fn init(comptime Part: type, options: Options) !Sizer {
        return .{ .base_bytes = try jsonSize(Request(Part){
            .model = options.model,
            .parts = &.{},
            .attachment_count = 0,
            .task_type = options.task_type,
            .instruction = options.instruction,
        }) };
    }

    pub fn append(self: *Sizer, part: anytype) !usize {
        const bytes = try jsonSize(metadataPart(part));
        self.parts_bytes = std.math.add(usize, self.parts_bytes, bytes) catch return error.BodyTooLarge;
        if (self.count > 0) self.parts_bytes = std.math.add(usize, self.parts_bytes, 1) catch return error.BodyTooLarge;
        self.count += 1;
        if (part == .binary) self.attachment_count += 1;
        var digits: usize = 1;
        var remaining = self.attachment_count;
        while (remaining >= 10) : (remaining /= 10) digits += 1;
        const total = std.math.add(usize, self.base_bytes, self.parts_bytes) catch return error.BodyTooLarge;
        return std.math.add(usize, total, digits - 1) catch error.BodyTooLarge;
    }
};
