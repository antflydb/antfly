// Copyright 2026 Antfly, Inc.
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

pub const Error = error{
    InvalidCff,
    TruncatedCff,
    MissingTable,
    UnsupportedCff,
    InvalidGlyphIndex,
};

const ParseError = Error || std.mem.Allocator.Error;

pub const GlyphPoint = @import("sfnt.zig").GlyphPoint;
pub const GlyphContour = @import("sfnt.zig").GlyphContour;
pub const GlyphOutline = @import("sfnt.zig").GlyphOutline;

const Index = struct {
    count: u16,
    off_size: u8,
    data_offset: usize,
    offsets_offset: usize,

    fn getObject(self: Index, bytes: []const u8, index: usize) Error![]const u8 {
        if (index >= self.count) return error.InvalidGlyphIndex;
        const start_off = try readIndexOffset(bytes, self.offsets_offset, self.off_size, index);
        const end_off = try readIndexOffset(bytes, self.offsets_offset, self.off_size, index + 1);
        if (end_off < start_off or start_off == 0 or end_off == 0) return error.InvalidCff;
        const start = self.data_offset + start_off - 1;
        const end = self.data_offset + end_off - 1;
        if (end > bytes.len or end < start) return error.TruncatedCff;
        return bytes[start..end];
    }
};

pub const Font = struct {
    bytes: []const u8,
    charstrings: Index,
    charset: []u16,
    global_subrs: Index,
    local_subrs: ?Index,
    fd_array: ?[]FontDict,
    fd_select: ?[]u16,

    pub fn init(alloc: std.mem.Allocator, bytes: []const u8) ParseError!Font {
        if (bytes.len < 4) return error.TruncatedCff;
        const header_size = bytes[2];
        if (header_size > bytes.len) return error.TruncatedCff;

        var cursor: usize = header_size;
        const name_index = try parseIndex(bytes, cursor);
        cursor = try indexEndOffset(bytes, name_index);
        const top_dict_index = try parseIndex(bytes, cursor);
        if (top_dict_index.count == 0) return error.InvalidCff;
        cursor = try indexEndOffset(bytes, top_dict_index);
        const string_index = try parseIndex(bytes, cursor);
        cursor = try indexEndOffset(bytes, string_index);
        const global_subrs = try parseIndex(bytes, cursor);

        const top_dict = try top_dict_index.getObject(bytes, 0);
        const charstrings_offset = try parseTopDictOffset(top_dict, .{ .primary = 17 });
        const charset_offset = parseTopDictOffset(top_dict, .{ .primary = 15 }) catch 0;
        const private = parseTopDictPrivate(top_dict) catch null;
        const fd_array_offset = parseTopDictOffset(top_dict, .{ .primary = 12, .escaped = 36 }) catch 0;
        const fd_select_offset = parseTopDictOffset(top_dict, .{ .primary = 12, .escaped = 37 }) catch 0;
        if (charstrings_offset >= bytes.len) return error.TruncatedCff;

        const charstrings = try parseIndex(bytes, charstrings_offset);
        const glyph_count = charstrings.count;
        const charset = try parseCharsetAlloc(alloc, bytes, charset_offset, glyph_count);
        errdefer alloc.free(charset);

        const local_subrs = if (private) |priv|
            try parseLocalSubrs(bytes, priv.size, priv.offset)
        else
            null;
        const fd_array = if (fd_array_offset != 0)
            try parseFontDictArrayAlloc(alloc, bytes, fd_array_offset)
        else
            null;
        errdefer if (fd_array) |slice| alloc.free(slice);
        const fd_select = if (fd_select_offset != 0)
            try parseFDSelectAlloc(alloc, bytes, fd_select_offset, glyph_count)
        else
            null;
        errdefer if (fd_select) |slice| alloc.free(slice);

        return .{
            .bytes = bytes,
            .charstrings = charstrings,
            .charset = charset,
            .global_subrs = global_subrs,
            .local_subrs = local_subrs,
            .fd_array = fd_array,
            .fd_select = fd_select,
        };
    }

    pub fn deinit(self: *Font, alloc: std.mem.Allocator) void {
        alloc.free(self.charset);
        if (self.fd_array) |slice| alloc.free(slice);
        if (self.fd_select) |slice| alloc.free(slice);
        self.* = undefined;
    }

    pub fn glyphOutlineAlloc(self: Font, alloc: std.mem.Allocator, glyph_index: u16) ParseError!?GlyphOutline {
        if (glyph_index >= self.charstrings.count) return error.InvalidGlyphIndex;
        const program = try self.charstrings.getObject(self.bytes, glyph_index);
        if (program.len == 0) return null;

        var contours = std.ArrayList(GlyphContour).empty;
        errdefer {
            for (contours.items) |*contour| contour.deinit(alloc);
            contours.deinit(alloc);
        }
        var current = std.ArrayList(GlyphPoint).empty;
        defer current.deinit(alloc);
        var stack = std.ArrayList(f64).empty;
        defer stack.deinit(alloc);

        var x: f64 = 0;
        var y: f64 = 0;
        var width_seen = false;
        var hint_count: usize = 0;
        var transient: [32]f64 = @as([32]f64, @splat(0));
        try self.executeCharStringAlloc(alloc, program, self.localSubrsForGlyph(glyph_index), &stack, &current, &contours, &x, &y, &width_seen, &hint_count, &transient, 0);

        if (contours.items.len == 0) return null;
        return .{
            .contours = try contours.toOwnedSlice(alloc),
            .x_min = 0,
            .y_min = 0,
            .x_max = 0,
            .y_max = 0,
        };
    }

    fn executeCharStringAlloc(
        self: Font,
        alloc: std.mem.Allocator,
        program: []const u8,
        active_local_subrs: ?Index,
        stack: *std.ArrayList(f64),
        current: *std.ArrayList(GlyphPoint),
        contours: *std.ArrayList(GlyphContour),
        x: *f64,
        y: *f64,
        width_seen: *bool,
        hint_count: *usize,
        transient: *[32]f64,
        subr_depth: u8,
    ) ParseError!void {
        if (subr_depth > 16) return error.UnsupportedCff;
        var i: usize = 0;
        while (i < program.len) {
            const b0 = program[i];
            i += 1;
            switch (b0) {
                1, 3, 18, 23 => try consumeStemHints(stack, width_seen, hint_count),
                4 => {
                    if (!width_seen.* and stack.items.len > 1) _ = stack.orderedRemove(0);
                    if (stack.items.len < 1) return error.InvalidCff;
                    if (current.items.len > 0) try flushContour(alloc, contours, current);
                    y.* += stack.items[0];
                    try current.append(alloc, .{ .x = x.*, .y = y.*, .on_curve = true });
                    stack.clearRetainingCapacity();
                    width_seen.* = true;
                },
                5 => {
                    if ((stack.items.len % 2) != 0) return error.InvalidCff;
                    var s: usize = 0;
                    while (s + 1 < stack.items.len) : (s += 2) {
                        x.* += stack.items[s];
                        y.* += stack.items[s + 1];
                        try current.append(alloc, .{ .x = x.*, .y = y.*, .on_curve = true });
                    }
                    stack.clearRetainingCapacity();
                    width_seen.* = true;
                },
                6 => try executeAlternatingLines(alloc, stack, current, x, y, true),
                7 => try executeAlternatingLines(alloc, stack, current, x, y, false),
                8 => try executeRrcurveto(alloc, stack, current, x, y),
                10 => {
                    if (active_local_subrs == null or stack.items.len == 0) return error.UnsupportedCff;
                    const subr_index = try subroutineIndex(stack.orderedRemove(stack.items.len - 1), active_local_subrs.?.count);
                    const subr = try active_local_subrs.?.getObject(self.bytes, subr_index);
                    try self.executeCharStringAlloc(alloc, subr, active_local_subrs, stack, current, contours, x, y, width_seen, hint_count, transient, subr_depth + 1);
                },
                11 => return,
                12 => {
                    if (i >= program.len) return error.TruncatedCff;
                    const escaped = program[i];
                    i += 1;
                    switch (escaped) {
                        3 => try binaryStackOp(alloc, stack, andFloat),
                        4 => try binaryStackOp(alloc, stack, orFloat),
                        5 => try unaryStackOp(stack, notFloat),
                        9 => try unaryStackOp(stack, absFloat),
                        10 => try binaryStackOp(alloc, stack, addFloat),
                        11 => try binaryStackOp(alloc, stack, subFloat),
                        12 => try binaryStackOp(alloc, stack, divFloat),
                        14 => try unaryStackOp(stack, negFloat),
                        15 => try binaryStackOp(alloc, stack, eqFloat),
                        18 => {
                            if (stack.items.len == 0) return error.InvalidCff;
                            _ = stack.pop();
                        },
                        20 => try putTransient(stack, transient),
                        21 => try getTransient(alloc, stack, transient),
                        22 => try ifelseStack(alloc, stack),
                        23 => try stack.append(alloc, 0.5),
                        24 => try binaryStackOp(alloc, stack, mulFloat),
                        26 => try unaryStackOp(stack, sqrtFloat),
                        27 => try dupStack(alloc, stack),
                        28 => try exchStack(stack),
                        29 => try indexStack(alloc, stack),
                        30 => try rollStack(alloc, stack),
                        34 => try executeHflex(alloc, stack, current, x, y),
                        35 => try executeFlex(alloc, stack, current, x, y),
                        36 => try executeHflex1(alloc, stack, current, x, y),
                        37 => try executeFlex1(alloc, stack, current, x, y),
                        else => return error.UnsupportedCff,
                    }
                    width_seen.* = true;
                },
                14 => {
                    if (current.items.len > 0) try flushContour(alloc, contours, current);
                    return;
                },
                21 => {
                    if (!width_seen.* and stack.items.len > 2) _ = stack.orderedRemove(0);
                    if (stack.items.len < 2) return error.InvalidCff;
                    if (current.items.len > 0) try flushContour(alloc, contours, current);
                    x.* += stack.items[0];
                    y.* += stack.items[1];
                    try current.append(alloc, .{ .x = x.*, .y = y.*, .on_curve = true });
                    stack.clearRetainingCapacity();
                    width_seen.* = true;
                },
                22 => {
                    if (!width_seen.* and stack.items.len > 1) _ = stack.orderedRemove(0);
                    if (stack.items.len < 1) return error.InvalidCff;
                    if (current.items.len > 0) try flushContour(alloc, contours, current);
                    x.* += stack.items[0];
                    try current.append(alloc, .{ .x = x.*, .y = y.*, .on_curve = true });
                    stack.clearRetainingCapacity();
                    width_seen.* = true;
                },
                24 => try executeRcurveline(alloc, stack, current, x, y),
                25 => try executeRlinecurve(alloc, stack, current, x, y),
                26 => try executeVvcurveto(alloc, stack, current, x, y),
                27 => try executeHhcurveto(alloc, stack, current, x, y),
                28 => {
                    if (i + 2 > program.len) return error.TruncatedCff;
                    const value: i16 = @bitCast((@as(u16, program[i]) << 8) | program[i + 1]);
                    i += 2;
                    try stack.append(alloc, @floatFromInt(value));
                },
                29 => {
                    if (stack.items.len == 0 or self.global_subrs.count == 0) return error.UnsupportedCff;
                    const subr_index = try subroutineIndex(stack.orderedRemove(stack.items.len - 1), self.global_subrs.count);
                    const subr = try self.global_subrs.getObject(self.bytes, subr_index);
                    try self.executeCharStringAlloc(alloc, subr, active_local_subrs, stack, current, contours, x, y, width_seen, hint_count, transient, subr_depth + 1);
                },
                30 => try executeVhcurveto(alloc, stack, current, x, y),
                31 => try executeHvcurveto(alloc, stack, current, x, y),
                19, 20 => {
                    try consumeStemHints(stack, width_seen, hint_count);
                    const mask_bytes = (hint_count.* + 7) / 8;
                    if (i + mask_bytes > program.len) return error.TruncatedCff;
                    i += mask_bytes;
                },
                32...246 => try stack.append(alloc, @floatFromInt(@as(i32, b0) - 139)),
                247...250 => {
                    if (i >= program.len) return error.TruncatedCff;
                    const value = (@as(i32, b0) - 247) * 256 + program[i] + 108;
                    i += 1;
                    try stack.append(alloc, @floatFromInt(value));
                },
                251...254 => {
                    if (i >= program.len) return error.TruncatedCff;
                    const value = -((@as(i32, b0) - 251) * 256) - program[i] - 108;
                    i += 1;
                    try stack.append(alloc, @floatFromInt(value));
                },
                255 => {
                    if (i + 4 > program.len) return error.TruncatedCff;
                    const raw = (@as(u32, program[i]) << 24) |
                        (@as(u32, program[i + 1]) << 16) |
                        (@as(u32, program[i + 2]) << 8) |
                        @as(u32, program[i + 3]);
                    i += 4;
                    const value: i32 = @bitCast(raw);
                    try stack.append(alloc, @as(f64, @floatFromInt(value)) / 65536.0);
                },
                else => return error.UnsupportedCff,
            }
            width_seen.* = true;
        }
    }

    fn localSubrsForGlyph(self: Font, glyph_index: u16) ?Index {
        if (self.fd_array) |fd_array| {
            const fd_select = self.fd_select orelse return self.local_subrs;
            if (glyph_index >= fd_select.len) return self.local_subrs;
            const fd_idx = fd_select[glyph_index];
            if (fd_idx >= fd_array.len) return self.local_subrs;
            return fd_array[fd_idx].local_subrs;
        }
        return self.local_subrs;
    }
};

const FontDict = struct {
    local_subrs: ?Index,
};

fn parseIndex(bytes: []const u8, offset: usize) Error!Index {
    if (offset + 2 > bytes.len) return error.TruncatedCff;
    const count = readU16(bytes, offset);
    if (count == 0) {
        return .{ .count = 0, .off_size = 0, .data_offset = offset + 2, .offsets_offset = offset + 2 };
    }
    if (offset + 3 > bytes.len) return error.TruncatedCff;
    const off_size = bytes[offset + 2];
    if (off_size == 0 or off_size > 4) return error.InvalidCff;
    const offsets_offset = offset + 3;
    const data_offset = offsets_offset + (@as(usize, count) + 1) * off_size;
    if (data_offset > bytes.len) return error.TruncatedCff;
    return .{
        .count = count,
        .off_size = off_size,
        .data_offset = data_offset,
        .offsets_offset = offsets_offset,
    };
}

fn indexEndOffset(bytes: []const u8, index: Index) Error!usize {
    if (index.count == 0) return index.data_offset;
    const end_off = try readIndexOffset(bytes, index.offsets_offset, index.off_size, index.count);
    return index.data_offset + end_off - 1;
}

fn readIndexOffset(bytes: []const u8, offsets_offset: usize, off_size: u8, index: usize) Error!usize {
    const start = offsets_offset + index * off_size;
    const end = start + off_size;
    if (end > bytes.len) return error.TruncatedCff;
    var value: usize = 0;
    var i: usize = start;
    while (i < end) : (i += 1) value = (value << 8) | bytes[i];
    return value;
}

const DictOperator = struct {
    primary: u8,
    escaped: ?u8 = null,
};

fn parseTopDictOffset(dict_bytes: []const u8, target_operator: DictOperator) Error!usize {
    var operands: [16]f64 = undefined;
    var operands_len: usize = 0;
    var i: usize = 0;
    while (i < dict_bytes.len) {
        const b0 = dict_bytes[i];
        i += 1;
        switch (b0) {
            0...21 => {
                const matched = if (b0 == 12) blk: {
                    if (i >= dict_bytes.len) return error.TruncatedCff;
                    const escaped = dict_bytes[i];
                    i += 1;
                    break :blk target_operator.primary == 12 and target_operator.escaped == escaped;
                } else blk: {
                    break :blk target_operator.primary == b0 and target_operator.escaped == null;
                };
                if (matched) {
                    if (operands_len == 0) return error.InvalidCff;
                    return @intFromFloat(operands[operands_len - 1]);
                }
                operands_len = 0;
            },
            else => {
                const operand = try parseDictOperand(dict_bytes, &i, b0);
                if (operands_len >= operands.len) return error.UnsupportedCff;
                operands[operands_len] = operand;
                operands_len += 1;
            },
        }
    }
    return error.MissingTable;
}

fn parseDictOperand(bytes: []const u8, cursor: *usize, b0: u8) Error!f64 {
    switch (b0) {
        28 => {
            if (cursor.* + 2 > bytes.len) return error.TruncatedCff;
            const value: i16 = @bitCast((@as(u16, bytes[cursor.*]) << 8) | bytes[cursor.* + 1]);
            cursor.* += 2;
            return @floatFromInt(value);
        },
        29 => {
            if (cursor.* + 4 > bytes.len) return error.TruncatedCff;
            const raw = (@as(u32, bytes[cursor.*]) << 24) |
                (@as(u32, bytes[cursor.* + 1]) << 16) |
                (@as(u32, bytes[cursor.* + 2]) << 8) |
                @as(u32, bytes[cursor.* + 3]);
            cursor.* += 4;
            const value: i32 = @bitCast(raw);
            return @floatFromInt(value);
        },
        30 => return try parseDictReal(bytes, cursor),
        32...246 => return @floatFromInt(@as(i32, b0) - 139),
        247...250 => {
            if (cursor.* >= bytes.len) return error.TruncatedCff;
            const value = (@as(i32, b0) - 247) * 256 + bytes[cursor.*] + 108;
            cursor.* += 1;
            return @floatFromInt(value);
        },
        251...254 => {
            if (cursor.* >= bytes.len) return error.TruncatedCff;
            const value = -((@as(i32, b0) - 251) * 256) - bytes[cursor.*] - 108;
            cursor.* += 1;
            return @floatFromInt(value);
        },
        else => return error.InvalidCff,
    }
}

fn parseDictReal(bytes: []const u8, cursor: *usize) Error!f64 {
    var buf: [64]u8 = undefined;
    var len: usize = 0;
    while (true) {
        if (cursor.* >= bytes.len) return error.TruncatedCff;
        const byte = bytes[cursor.*];
        cursor.* += 1;
        const nibbles = [_]u8{ byte >> 4, byte & 0x0f };
        for (nibbles) |nibble| {
            switch (nibble) {
                0x0...0x9 => {
                    if (len >= buf.len) return error.UnsupportedCff;
                    buf[len] = '0' + @as(u8, nibble);
                    len += 1;
                },
                0xa => {
                    if (len >= buf.len) return error.UnsupportedCff;
                    buf[len] = '.';
                    len += 1;
                },
                0xb => {
                    if (len >= buf.len) return error.UnsupportedCff;
                    buf[len] = 'e';
                    len += 1;
                },
                0xc => {
                    if (len + 2 >= buf.len) return error.UnsupportedCff;
                    buf[len] = 'e';
                    buf[len + 1] = '-';
                    len += 2;
                },
                0xe => {
                    if (len >= buf.len) return error.UnsupportedCff;
                    buf[len] = '-';
                    len += 1;
                },
                0xf => return std.fmt.parseFloat(f64, buf[0..len]) catch error.InvalidCff,
                else => return error.InvalidCff,
            }
        }
    }
}

const PrivateDictRef = struct {
    size: usize,
    offset: usize,
};

fn parseTopDictPrivate(dict_bytes: []const u8) Error!PrivateDictRef {
    var operands: [16]f64 = undefined;
    var operands_len: usize = 0;
    var i: usize = 0;
    while (i < dict_bytes.len) {
        const b0 = dict_bytes[i];
        i += 1;
        switch (b0) {
            0...21 => {
                const matched = if (b0 == 12) blk: {
                    if (i >= dict_bytes.len) return error.TruncatedCff;
                    i += 1;
                    break :blk false;
                } else b0 == 18;
                if (matched) {
                    if (operands_len < 2) return error.InvalidCff;
                    return .{
                        .size = @intFromFloat(operands[operands_len - 2]),
                        .offset = @intFromFloat(operands[operands_len - 1]),
                    };
                }
                operands_len = 0;
            },
            else => {
                const operand = try parseDictOperand(dict_bytes, &i, b0);
                if (operands_len >= operands.len) return error.UnsupportedCff;
                operands[operands_len] = operand;
                operands_len += 1;
            },
        }
    }
    return error.MissingTable;
}

fn parseLocalSubrs(bytes: []const u8, private_size: usize, private_offset: usize) Error!?Index {
    if (private_offset + private_size > bytes.len) return error.TruncatedCff;
    const dict_bytes = bytes[private_offset .. private_offset + private_size];
    const subrs_rel = parseTopDictOffset(dict_bytes, .{ .primary = 19 }) catch return null;
    const subrs_abs = private_offset + subrs_rel;
    return try parseIndex(bytes, subrs_abs);
}

fn parseFontDictArrayAlloc(alloc: std.mem.Allocator, bytes: []const u8, offset: usize) ParseError![]FontDict {
    const fd_index = try parseIndex(bytes, offset);
    const out = try alloc.alloc(FontDict, fd_index.count);
    errdefer alloc.free(out);
    for (out, 0..) |*entry, i| {
        const dict_bytes = try fd_index.getObject(bytes, i);
        const private = parseTopDictPrivate(dict_bytes) catch null;
        entry.* = .{
            .local_subrs = if (private) |priv|
                try parseLocalSubrs(bytes, priv.size, priv.offset)
            else
                null,
        };
    }
    return out;
}

fn parseFDSelectAlloc(alloc: std.mem.Allocator, bytes: []const u8, offset: usize, glyph_count: u16) ParseError![]u16 {
    const out = try alloc.alloc(u16, glyph_count);
    errdefer alloc.free(out);
    if (glyph_count == 0) return out;
    if (offset >= bytes.len) return error.TruncatedCff;
    const format = bytes[offset];
    var cursor = offset + 1;
    switch (format) {
        0 => {
            if (cursor + glyph_count > bytes.len) return error.TruncatedCff;
            for (out, 0..) |*entry, i| entry.* = bytes[cursor + i];
        },
        3 => {
            if (cursor + 2 > bytes.len) return error.TruncatedCff;
            const range_count = readU16(bytes, cursor);
            cursor += 2;
            if (range_count == 0) return error.InvalidCff;
            var written: usize = 0;
            var first: u16 = 0;
            var fd: u8 = 0;
            var r: usize = 0;
            while (r < range_count) : (r += 1) {
                if (cursor + 3 > bytes.len) return error.TruncatedCff;
                first = readU16(bytes, cursor);
                fd = bytes[cursor + 2];
                cursor += 3;
                if (r == 0 and first != 0) return error.InvalidCff;
                if (cursor + 2 > bytes.len) return error.TruncatedCff;
                const next_first = readU16(bytes, cursor);
                if (next_first < first or next_first > glyph_count) return error.InvalidCff;
                var g = first;
                while (g < next_first) : (g += 1) {
                    out[g] = fd;
                    written += 1;
                }
            }
            if (written != glyph_count) return error.InvalidCff;
        },
        else => return error.UnsupportedCff,
    }
    return out;
}

fn subroutineBias(count: u16) i32 {
    return if (count < 1240) 107 else if (count < 33900) 1131 else 32768;
}

fn subroutineIndex(operand: f64, count: u16) Error!usize {
    const index = @as(i32, @intFromFloat(operand)) + subroutineBias(count);
    if (index < 0 or index >= count) return error.InvalidGlyphIndex;
    return @intCast(index);
}

fn executeAlternatingLines(
    alloc: std.mem.Allocator,
    stack: *std.ArrayList(f64),
    current: *std.ArrayList(GlyphPoint),
    x: *f64,
    y: *f64,
    start_horizontal: bool,
) !void {
    var horizontal = start_horizontal;
    for (stack.items) |delta| {
        if (horizontal) x.* += delta else y.* += delta;
        try current.append(alloc, .{ .x = x.*, .y = y.*, .on_curve = true });
        horizontal = !horizontal;
    }
    stack.clearRetainingCapacity();
}

fn executeRrcurveto(
    alloc: std.mem.Allocator,
    stack: *std.ArrayList(f64),
    current: *std.ArrayList(GlyphPoint),
    x: *f64,
    y: *f64,
) !void {
    if ((stack.items.len % 6) != 0) return error.InvalidCff;
    var s: usize = 0;
    while (s + 5 < stack.items.len) : (s += 6) {
        const p0 = [2]f64{ x.*, y.* };
        const c1 = [2]f64{ x.* + stack.items[s], y.* + stack.items[s + 1] };
        const c2 = [2]f64{ c1[0] + stack.items[s + 2], c1[1] + stack.items[s + 3] };
        x.* = c2[0] + stack.items[s + 4];
        y.* = c2[1] + stack.items[s + 5];
        try appendCubicFlattenedAlloc(alloc, current, p0, c1, c2, .{ x.*, y.* }, 8);
    }
    stack.clearRetainingCapacity();
}

fn executeRcurveline(
    alloc: std.mem.Allocator,
    stack: *std.ArrayList(f64),
    current: *std.ArrayList(GlyphPoint),
    x: *f64,
    y: *f64,
) !void {
    if (stack.items.len < 8) return error.InvalidCff;
    var s: usize = 0;
    while (s + 7 < stack.items.len) : (s += 6) {
        if (stack.items.len - s == 2) break;
        const p0 = [2]f64{ x.*, y.* };
        const c1 = [2]f64{ x.* + stack.items[s], y.* + stack.items[s + 1] };
        const c2 = [2]f64{ c1[0] + stack.items[s + 2], c1[1] + stack.items[s + 3] };
        x.* = c2[0] + stack.items[s + 4];
        y.* = c2[1] + stack.items[s + 5];
        try appendCubicFlattenedAlloc(alloc, current, p0, c1, c2, .{ x.*, y.* }, 8);
    }
    if (stack.items.len - s != 2) return error.InvalidCff;
    x.* += stack.items[s];
    y.* += stack.items[s + 1];
    try current.append(alloc, .{ .x = x.*, .y = y.*, .on_curve = true });
    stack.clearRetainingCapacity();
}

fn executeRlinecurve(
    alloc: std.mem.Allocator,
    stack: *std.ArrayList(f64),
    current: *std.ArrayList(GlyphPoint),
    x: *f64,
    y: *f64,
) !void {
    if (stack.items.len < 8) return error.InvalidCff;
    var s: usize = 0;
    while (stack.items.len - s > 6) : (s += 2) {
        x.* += stack.items[s];
        y.* += stack.items[s + 1];
        try current.append(alloc, .{ .x = x.*, .y = y.*, .on_curve = true });
    }
    if (stack.items.len - s != 6) return error.InvalidCff;
    const p0 = [2]f64{ x.*, y.* };
    const c1 = [2]f64{ x.* + stack.items[s], y.* + stack.items[s + 1] };
    const c2 = [2]f64{ c1[0] + stack.items[s + 2], c1[1] + stack.items[s + 3] };
    x.* = c2[0] + stack.items[s + 4];
    y.* = c2[1] + stack.items[s + 5];
    try appendCubicFlattenedAlloc(alloc, current, p0, c1, c2, .{ x.*, y.* }, 8);
    stack.clearRetainingCapacity();
}

fn executeVvcurveto(
    alloc: std.mem.Allocator,
    stack: *std.ArrayList(f64),
    current: *std.ArrayList(GlyphPoint),
    x: *f64,
    y: *f64,
) !void {
    var s: usize = 0;
    var dx1: f64 = 0;
    if ((stack.items.len % 4) == 1) {
        dx1 = stack.items[0];
        s = 1;
    }
    while (s + 3 < stack.items.len) : (s += 4) {
        const p0 = [2]f64{ x.*, y.* };
        const c1 = [2]f64{ x.* + dx1, y.* + stack.items[s] };
        const c2 = [2]f64{ c1[0] + stack.items[s + 1], c1[1] + stack.items[s + 2] };
        x.* = c2[0];
        y.* = c2[1] + stack.items[s + 3];
        try appendCubicFlattenedAlloc(alloc, current, p0, c1, c2, .{ x.*, y.* }, 8);
        dx1 = 0;
    }
    if (s != stack.items.len) return error.InvalidCff;
    stack.clearRetainingCapacity();
}

fn executeHhcurveto(
    alloc: std.mem.Allocator,
    stack: *std.ArrayList(f64),
    current: *std.ArrayList(GlyphPoint),
    x: *f64,
    y: *f64,
) !void {
    var s: usize = 0;
    var dy1: f64 = 0;
    if ((stack.items.len % 4) == 1) {
        dy1 = stack.items[0];
        s = 1;
    }
    while (s + 3 < stack.items.len) : (s += 4) {
        const p0 = [2]f64{ x.*, y.* };
        const c1 = [2]f64{ x.* + stack.items[s], y.* + dy1 };
        const c2 = [2]f64{ c1[0] + stack.items[s + 1], c1[1] + stack.items[s + 2] };
        x.* = c2[0] + stack.items[s + 3];
        y.* = c2[1];
        try appendCubicFlattenedAlloc(alloc, current, p0, c1, c2, .{ x.*, y.* }, 8);
        dy1 = 0;
    }
    if (s != stack.items.len) return error.InvalidCff;
    stack.clearRetainingCapacity();
}

fn executeHvcurveto(
    alloc: std.mem.Allocator,
    stack: *std.ArrayList(f64),
    current: *std.ArrayList(GlyphPoint),
    x: *f64,
    y: *f64,
) !void {
    var s: usize = 0;
    var horizontal = true;
    while (stack.items.len - s >= 4) {
        const remaining = stack.items.len - s;
        const p0 = [2]f64{ x.*, y.* };
        var c1: [2]f64 = undefined;
        var c2: [2]f64 = undefined;
        var endp: [2]f64 = undefined;
        if (horizontal) {
            c1 = .{ x.* + stack.items[s], y.* };
            c2 = .{ c1[0] + stack.items[s + 1], c1[1] + stack.items[s + 2] };
            endp = .{ c2[0], c2[1] + stack.items[s + 3] };
            if (remaining == 5) endp[0] += stack.items[s + 4];
        } else {
            c1 = .{ x.*, y.* + stack.items[s] };
            c2 = .{ c1[0] + stack.items[s + 1], c1[1] + stack.items[s + 2] };
            endp = .{ c2[0] + stack.items[s + 3], c2[1] };
            if (remaining == 5) endp[1] += stack.items[s + 4];
        }
        x.* = endp[0];
        y.* = endp[1];
        try appendCubicFlattenedAlloc(alloc, current, p0, c1, c2, endp, 8);
        s += if (remaining == 5) 5 else 4;
        horizontal = !horizontal;
    }
    if (s != stack.items.len) return error.InvalidCff;
    stack.clearRetainingCapacity();
}

fn executeVhcurveto(
    alloc: std.mem.Allocator,
    stack: *std.ArrayList(f64),
    current: *std.ArrayList(GlyphPoint),
    x: *f64,
    y: *f64,
) !void {
    var s: usize = 0;
    var horizontal = false;
    while (stack.items.len - s >= 4) {
        const remaining = stack.items.len - s;
        const p0 = [2]f64{ x.*, y.* };
        var c1: [2]f64 = undefined;
        var c2: [2]f64 = undefined;
        var endp: [2]f64 = undefined;
        if (horizontal) {
            c1 = .{ x.* + stack.items[s], y.* };
            c2 = .{ c1[0] + stack.items[s + 1], c1[1] + stack.items[s + 2] };
            endp = .{ c2[0], c2[1] + stack.items[s + 3] };
            if (remaining == 5) endp[0] += stack.items[s + 4];
        } else {
            c1 = .{ x.*, y.* + stack.items[s] };
            c2 = .{ c1[0] + stack.items[s + 1], c1[1] + stack.items[s + 2] };
            endp = .{ c2[0] + stack.items[s + 3], c2[1] };
            if (remaining == 5) endp[1] += stack.items[s + 4];
        }
        x.* = endp[0];
        y.* = endp[1];
        try appendCubicFlattenedAlloc(alloc, current, p0, c1, c2, endp, 8);
        s += if (remaining == 5) 5 else 4;
        horizontal = !horizontal;
    }
    if (s != stack.items.len) return error.InvalidCff;
    stack.clearRetainingCapacity();
}

fn executeHflex(
    alloc: std.mem.Allocator,
    stack: *std.ArrayList(f64),
    current: *std.ArrayList(GlyphPoint),
    x: *f64,
    y: *f64,
) !void {
    if (stack.items.len != 7) return error.InvalidCff;
    const p0 = [2]f64{ x.*, y.* };
    const c1 = .{ x.* + stack.items[0], y.* };
    const c2 = .{ c1[0] + stack.items[1], c1[1] + stack.items[2] };
    const mid = .{ c2[0] + stack.items[3], c2[1] };
    try appendCubicFlattenedAlloc(alloc, current, p0, c1, c2, mid, 8);
    const c3 = .{ mid[0] + stack.items[4], mid[1] };
    const c4 = .{ c3[0] + stack.items[5], c3[1] - stack.items[2] };
    x.* = c4[0] + stack.items[6];
    try appendCubicFlattenedAlloc(alloc, current, mid, c3, c4, .{ x.*, y.* }, 8);
    stack.clearRetainingCapacity();
}

fn executeFlex(
    alloc: std.mem.Allocator,
    stack: *std.ArrayList(f64),
    current: *std.ArrayList(GlyphPoint),
    x: *f64,
    y: *f64,
) !void {
    if (stack.items.len != 13) return error.InvalidCff;
    const p0 = [2]f64{ x.*, y.* };
    const c1 = .{ x.* + stack.items[0], y.* + stack.items[1] };
    const c2 = .{ c1[0] + stack.items[2], c1[1] + stack.items[3] };
    const mid = .{ c2[0] + stack.items[4], c2[1] + stack.items[5] };
    try appendCubicFlattenedAlloc(alloc, current, p0, c1, c2, mid, 8);
    const c3 = .{ mid[0] + stack.items[6], mid[1] + stack.items[7] };
    const c4 = .{ c3[0] + stack.items[8], c3[1] + stack.items[9] };
    x.* = c4[0] + stack.items[10];
    y.* = c4[1] + stack.items[11];
    try appendCubicFlattenedAlloc(alloc, current, mid, c3, c4, .{ x.*, y.* }, 8);
    _ = stack.items[12];
    stack.clearRetainingCapacity();
}

fn executeHflex1(
    alloc: std.mem.Allocator,
    stack: *std.ArrayList(f64),
    current: *std.ArrayList(GlyphPoint),
    x: *f64,
    y: *f64,
) !void {
    if (stack.items.len != 9) return error.InvalidCff;
    const p0 = [2]f64{ x.*, y.* };
    const c1 = .{ x.* + stack.items[0], y.* + stack.items[1] };
    const c2 = .{ c1[0] + stack.items[2], c1[1] + stack.items[3] };
    const mid = .{ c2[0] + stack.items[4], c2[1] };
    try appendCubicFlattenedAlloc(alloc, current, p0, c1, c2, mid, 8);
    const c3 = .{ mid[0] + stack.items[5], mid[1] };
    const c4 = .{ c3[0] + stack.items[6], c3[1] + stack.items[7] };
    x.* = c4[0] + stack.items[8];
    try appendCubicFlattenedAlloc(alloc, current, mid, c3, c4, .{ x.*, y.* }, 8);
    stack.clearRetainingCapacity();
}

fn executeFlex1(
    alloc: std.mem.Allocator,
    stack: *std.ArrayList(f64),
    current: *std.ArrayList(GlyphPoint),
    x: *f64,
    y: *f64,
) !void {
    if (stack.items.len != 11) return error.InvalidCff;
    const dx_sum = stack.items[0] + stack.items[2] + stack.items[4] + stack.items[6] + stack.items[8];
    const dy_sum = stack.items[1] + stack.items[3] + stack.items[5] + stack.items[7] + stack.items[9];
    const p0 = [2]f64{ x.*, y.* };
    const c1 = .{ x.* + stack.items[0], y.* + stack.items[1] };
    const c2 = .{ c1[0] + stack.items[2], c1[1] + stack.items[3] };
    const mid = .{ c2[0] + stack.items[4], c2[1] + stack.items[5] };
    try appendCubicFlattenedAlloc(alloc, current, p0, c1, c2, mid, 8);
    const c3 = .{ mid[0] + stack.items[6], mid[1] + stack.items[7] };
    const c4 = .{ c3[0] + stack.items[8], c3[1] + stack.items[9] };
    if (@abs(dx_sum) > @abs(dy_sum)) {
        x.* = c4[0] + stack.items[10];
        y.* = c4[1];
    } else {
        x.* = c4[0];
        y.* = c4[1] + stack.items[10];
    }
    try appendCubicFlattenedAlloc(alloc, current, mid, c3, c4, .{ x.*, y.* }, 8);
    stack.clearRetainingCapacity();
}

fn parseCharsetAlloc(alloc: std.mem.Allocator, bytes: []const u8, charset_offset: usize, glyph_count: u16) ParseError![]u16 {
    if (glyph_count == 0) return try alloc.alloc(u16, 0);
    const out = try alloc.alloc(u16, glyph_count);
    errdefer alloc.free(out);
    out[0] = 0;
    if (glyph_count == 1) return out;
    if (charset_offset == 0) {
        var i: usize = 1;
        while (i < glyph_count) : (i += 1) out[i] = @intCast(i);
        return out;
    }
    if (charset_offset >= bytes.len) return error.TruncatedCff;
    const format = bytes[charset_offset];
    var cursor = charset_offset + 1;
    switch (format) {
        0 => {
            var i: usize = 1;
            while (i < glyph_count) : (i += 1) {
                if (cursor + 2 > bytes.len) return error.TruncatedCff;
                out[i] = readU16(bytes, cursor);
                cursor += 2;
            }
        },
        1 => {
            var i: usize = 1;
            while (i < glyph_count) {
                if (cursor + 3 > bytes.len) return error.TruncatedCff;
                const first = readU16(bytes, cursor);
                const left = bytes[cursor + 2];
                cursor += 3;
                var j: usize = 0;
                while (j <= left and i < glyph_count) : (j += 1) {
                    out[i] = @intCast(first + j);
                    i += 1;
                }
            }
            if (i != glyph_count) return error.InvalidCff;
        },
        2 => {
            var i: usize = 1;
            while (i < glyph_count) {
                if (cursor + 4 > bytes.len) return error.TruncatedCff;
                const first = readU16(bytes, cursor);
                const left = readU16(bytes, cursor + 2);
                cursor += 4;
                var j: usize = 0;
                while (j <= left and i < glyph_count) : (j += 1) {
                    out[i] = @intCast(first + j);
                    i += 1;
                }
            }
            if (i != glyph_count) return error.InvalidCff;
        },
        else => return error.UnsupportedCff,
    }
    return out;
}

fn flushContour(alloc: std.mem.Allocator, contours: *std.ArrayList(GlyphContour), current: *std.ArrayList(GlyphPoint)) !void {
    if (current.items.len == 0) return;
    while (current.items.len > 1 and pointsAlmostEqual(current.items[0], current.items[current.items.len - 1])) {
        _ = current.pop();
    }
    if (current.items.len < 2) {
        current.clearRetainingCapacity();
        return;
    }
    try contours.append(alloc, .{ .points = try alloc.dupe(GlyphPoint, current.items) });
    current.clearRetainingCapacity();
}

fn appendCubicFlattenedAlloc(
    alloc: std.mem.Allocator,
    out: *std.ArrayList(GlyphPoint),
    p0: [2]f64,
    c1: [2]f64,
    c2: [2]f64,
    p3: [2]f64,
    steps: usize,
) !void {
    var step: usize = 1;
    while (step <= steps) : (step += 1) {
        const t = @as(f64, @floatFromInt(step)) / @as(f64, @floatFromInt(steps));
        const omt = 1.0 - t;
        try out.append(alloc, .{
            .x = omt * omt * omt * p0[0] + 3.0 * omt * omt * t * c1[0] + 3.0 * omt * t * t * c2[0] + t * t * t * p3[0],
            .y = omt * omt * omt * p0[1] + 3.0 * omt * omt * t * c1[1] + 3.0 * omt * t * t * c2[1] + t * t * t * p3[1],
            .on_curve = true,
        });
    }
}

fn pointsAlmostEqual(a: GlyphPoint, b: GlyphPoint) bool {
    return @abs(a.x - b.x) < 0.0001 and @abs(a.y - b.y) < 0.0001;
}

fn readU16(bytes: []const u8, offset: usize) u16 {
    return (@as(u16, bytes[offset]) << 8) | bytes[offset + 1];
}

fn consumeStemHints(stack: *std.ArrayList(f64), width_seen: *bool, hint_count: *usize) Error!void {
    var values = stack.items.len;
    if (!width_seen.* and values % 2 == 1) {
        values -= 1;
        width_seen.* = true;
    }
    if (values % 2 != 0) return error.InvalidCff;
    hint_count.* += values / 2;
    stack.clearRetainingCapacity();
    width_seen.* = true;
}

fn unaryStackOp(stack: *std.ArrayList(f64), comptime op: fn (f64) f64) Error!void {
    if (stack.items.len == 0) return error.InvalidCff;
    stack.items[stack.items.len - 1] = op(stack.items[stack.items.len - 1]);
}

fn binaryStackOp(alloc: std.mem.Allocator, stack: *std.ArrayList(f64), comptime op: fn (f64, f64) f64) ParseError!void {
    if (stack.items.len < 2) return error.InvalidCff;
    const b = stack.pop().?;
    const a = stack.pop().?;
    try stack.append(alloc, op(a, b));
}

fn dupStack(alloc: std.mem.Allocator, stack: *std.ArrayList(f64)) ParseError!void {
    if (stack.items.len == 0) return error.InvalidCff;
    try stack.append(alloc, stack.items[stack.items.len - 1]);
}

fn exchStack(stack: *std.ArrayList(f64)) Error!void {
    if (stack.items.len < 2) return error.InvalidCff;
    const last = stack.items.len - 1;
    const prev = last - 1;
    const tmp = stack.items[prev];
    stack.items[prev] = stack.items[last];
    stack.items[last] = tmp;
}

fn indexStack(alloc: std.mem.Allocator, stack: *std.ArrayList(f64)) ParseError!void {
    if (stack.items.len == 0) return error.InvalidCff;
    const idx_value = stack.pop().?;
    const idx: usize = if (idxValueToSigned(idx_value) < 0) 0 else @intCast(idxValueToSigned(idx_value));
    if (stack.items.len == 0) return error.InvalidCff;
    const from_top = if (idx >= stack.items.len) stack.items.len - 1 else idx;
    const value = stack.items[stack.items.len - 1 - from_top];
    try stack.append(alloc, value);
}

fn rollStack(alloc: std.mem.Allocator, stack: *std.ArrayList(f64)) ParseError!void {
    if (stack.items.len < 2) return error.InvalidCff;
    const j_value = stack.pop().?;
    const n_value = stack.pop().?;
    const n_signed = idxValueToSigned(n_value);
    if (n_signed < 0) return error.InvalidCff;
    const n: usize = @intCast(n_signed);
    if (n == 0 or n > stack.items.len) return error.InvalidCff;
    var j = idxValueToSigned(j_value);
    const n_i: isize = @intCast(n);
    j = @mod(j, n_i);
    if (j < 0) j += n_i;
    if (j == 0) return;

    const start = stack.items.len - n;
    const scratch = try alloc.alloc(f64, n);
    defer alloc.free(scratch);
    @memcpy(scratch, stack.items[start..]);
    var idx: usize = 0;
    while (idx < n) : (idx += 1) {
        const src = (idx + n - @as(usize, @intCast(j))) % n;
        stack.items[start + idx] = scratch[src];
    }
}

fn putTransient(stack: *std.ArrayList(f64), transient: *[32]f64) Error!void {
    if (stack.items.len < 2) return error.InvalidCff;
    const idx_value = stack.pop().?;
    const value = stack.pop().?;
    const idx_signed = idxValueToSigned(idx_value);
    if (idx_signed < 0 or idx_signed >= transient.len) return error.InvalidCff;
    transient[@intCast(idx_signed)] = value;
}

fn getTransient(alloc: std.mem.Allocator, stack: *std.ArrayList(f64), transient: *const [32]f64) ParseError!void {
    if (stack.items.len == 0) return error.InvalidCff;
    const idx_value = stack.pop().?;
    const idx_signed = idxValueToSigned(idx_value);
    if (idx_signed < 0 or idx_signed >= transient.len) return error.InvalidCff;
    try stack.append(alloc, transient[@intCast(idx_signed)]);
}

fn ifelseStack(alloc: std.mem.Allocator, stack: *std.ArrayList(f64)) ParseError!void {
    if (stack.items.len < 4) return error.InvalidCff;
    const s2 = stack.pop().?;
    const s1 = stack.pop().?;
    const v2 = stack.pop().?;
    const v1 = stack.pop().?;
    const chosen = if (v1 <= v2) s1 else s2;
    try stack.append(alloc, chosen);
}

fn idxValueToSigned(value: f64) isize {
    return @intFromFloat(@round(value));
}

fn absFloat(v: f64) f64 {
    return @abs(v);
}

fn addFloat(a: f64, b: f64) f64 {
    return a + b;
}

fn andFloat(a: f64, b: f64) f64 {
    return if (a != 0 and b != 0) 1 else 0;
}

fn orFloat(a: f64, b: f64) f64 {
    return if (a != 0 or b != 0) 1 else 0;
}

fn eqFloat(a: f64, b: f64) f64 {
    return if (@abs(a - b) < 0.000001) 1 else 0;
}

fn subFloat(a: f64, b: f64) f64 {
    return a - b;
}

fn divFloat(a: f64, b: f64) f64 {
    return a / b;
}

fn mulFloat(a: f64, b: f64) f64 {
    return a * b;
}

fn negFloat(v: f64) f64 {
    return -v;
}

fn notFloat(v: f64) f64 {
    return if (v == 0) 1 else 0;
}

fn sqrtFloat(v: f64) f64 {
    return std.math.sqrt(v);
}

test "cff parses simple charstring outline" {
    const alloc = std.testing.allocator;
    const cff_bytes =
        &[_]u8{
            1,   0,   4,   1,
            0,   1,   1,   1,
            5,   'T', 'e', 's',
            't', 0,   1,   1,
            1,   5,   190, 15,
            165, 17,  0,   0,
            0,   0,   0,   2,
            1,   1,   2,   20,
            14,  139, 139, 21,
            247, 124, 139, 5,
            251, 124, 250, 124,
            5,   251, 124, 251,
            124, 5,   14,  0,
            0,   1,
        };

    var font = try Font.init(alloc, cff_bytes);
    defer font.deinit(alloc);

    var outline = (try font.glyphOutlineAlloc(alloc, 1)).?;
    defer outline.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), outline.contours.len);
    try std.testing.expect(outline.contours[0].points.len >= 3);
}

test "cff executes local and global subroutines with arithmetic operators" {
    const alloc = std.testing.allocator;
    const global_subr_index =
        [_]u8{
            0,   1,   1, 1,  5,
            139, 189, 5, 11,
        };
    const local_subr_index =
        [_]u8{
            0,   1,   1, 1,  5,
            189, 139, 5, 11,
        };

    var bytes = std.ArrayList(u8).empty;
    defer bytes.deinit(alloc);
    try bytes.appendSlice(alloc, &global_subr_index);
    const local_offset = bytes.items.len;
    try bytes.appendSlice(alloc, &local_subr_index);

    const global_subrs = try parseIndex(bytes.items, 0);
    const local_subrs = try parseIndex(bytes.items, local_offset);
    const font = Font{
        .bytes = bytes.items,
        .charstrings = global_subrs,
        .charset = &.{},
        .global_subrs = global_subrs,
        .local_subrs = local_subrs,
        .fd_array = null,
        .fd_select = null,
    };

    const program =
        [_]u8{
            159, 169, 12, 10,
            199, 141, 12, 12,
            21,  32,  29, 32,
            10,  14,
        };

    var contours = std.ArrayList(GlyphContour).empty;
    defer {
        for (contours.items) |*contour| contour.deinit(alloc);
        contours.deinit(alloc);
    }
    var current = std.ArrayList(GlyphPoint).empty;
    defer current.deinit(alloc);
    var stack = std.ArrayList(f64).empty;
    defer stack.deinit(alloc);
    var x: f64 = 0;
    var y: f64 = 0;
    var width_seen = false;
    var hint_count: usize = 0;
    var transient: [32]f64 = @as([32]f64, @splat(0));
    try font.executeCharStringAlloc(alloc, &program, font.local_subrs, &stack, &current, &contours, &x, &y, &width_seen, &hint_count, &transient, 0);

    try std.testing.expectEqual(@as(usize, 1), contours.items.len);
    const points = contours.items[0].points;
    try std.testing.expectEqual(@as(usize, 3), points.len);
    try std.testing.expectApproxEqAbs(@as(f64, 50), points[0].x, 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 30), points[0].y, 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 50), points[1].x, 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 80), points[1].y, 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 100), points[2].x, 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 80), points[2].y, 0.001);
}

test "cff hintmask skips mask bytes and continues outline parsing" {
    const alloc = std.testing.allocator;
    const font = Font{
        .bytes = &.{},
        .charstrings = .{ .count = 0, .off_size = 0, .data_offset = 0, .offsets_offset = 0 },
        .charset = &.{},
        .global_subrs = .{ .count = 0, .off_size = 0, .data_offset = 0, .offsets_offset = 0 },
        .local_subrs = null,
        .fd_array = null,
        .fd_select = null,
    };
    const program =
        [_]u8{
            139, 149, 1,
            19,  0,   139,
            139, 21,  189,
            139, 5,   14,
        };

    var contours = std.ArrayList(GlyphContour).empty;
    defer {
        for (contours.items) |*contour| contour.deinit(alloc);
        contours.deinit(alloc);
    }
    var current = std.ArrayList(GlyphPoint).empty;
    defer current.deinit(alloc);
    var stack = std.ArrayList(f64).empty;
    defer stack.deinit(alloc);
    var x: f64 = 0;
    var y: f64 = 0;
    var width_seen = false;
    var hint_count: usize = 0;
    var transient: [32]f64 = @as([32]f64, @splat(0));
    try font.executeCharStringAlloc(alloc, &program, font.local_subrs, &stack, &current, &contours, &x, &y, &width_seen, &hint_count, &transient, 0);

    try std.testing.expectEqual(@as(usize, 1), hint_count);
    try std.testing.expectEqual(@as(usize, 1), contours.items.len);
    try std.testing.expectApproxEqAbs(@as(f64, 50), contours.items[0].points[1].x, 0.001);
}

test "cff supports transient and logical Type2 operators" {
    const alloc = std.testing.allocator;
    const font = Font{
        .bytes = &.{},
        .charstrings = .{ .count = 0, .off_size = 0, .data_offset = 0, .offsets_offset = 0 },
        .charset = &.{},
        .global_subrs = .{ .count = 0, .off_size = 0, .data_offset = 0, .offsets_offset = 0 },
        .local_subrs = null,
        .fd_array = null,
        .fd_select = null,
    };
    const program =
        [_]u8{
            149, 139, 12,  20,
            139, 12,  21,  149,
            12,  15,  12,  18,
            139, 12,  5,   12,
            18,  149, 139, 12,
            3,   12,  18,  139,
            149, 12,  4,   12,
            18,  149, 141, 151,
            161, 12,  22,  12,
            23,  255, 0,   1,
            0,   0,   12,  10,
            21,  189, 139, 5,
            14,
        };

    var contours = std.ArrayList(GlyphContour).empty;
    defer {
        for (contours.items) |*contour| contour.deinit(alloc);
        contours.deinit(alloc);
    }
    var current = std.ArrayList(GlyphPoint).empty;
    defer current.deinit(alloc);
    var stack = std.ArrayList(f64).empty;
    defer stack.deinit(alloc);
    var x: f64 = 0;
    var y: f64 = 0;
    var width_seen = false;
    var hint_count: usize = 0;
    var transient: [32]f64 = @as([32]f64, @splat(0));
    try font.executeCharStringAlloc(alloc, &program, font.local_subrs, &stack, &current, &contours, &x, &y, &width_seen, &hint_count, &transient, 0);

    try std.testing.expectEqual(@as(usize, 1), contours.items.len);
    try std.testing.expectApproxEqAbs(@as(f64, 22), contours.items[0].points[0].x, 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 1.5), contours.items[0].points[0].y, 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 72), contours.items[0].points[1].x, 0.001);
}

test "cff parses dict real operands" {
    const dict =
        [_]u8{
            30, 0x1a, 0x2f, // 1.2
            17,
            30, 0x2b, 0x1f, // 2e1
            15,
        };
    try std.testing.expectEqual(@as(usize, 1), try parseTopDictOffset(&dict, .{ .primary = 17 }));
    try std.testing.expectEqual(@as(usize, 20), try parseTopDictOffset(&dict, .{ .primary = 15 }));
}

test "cff parses charset format 1" {
    const alloc = std.testing.allocator;
    const bytes =
        [_]u8{
            0,
            1,
            0,
            10,
            1,
            0,
            20,
            0,
        };
    const charset = try parseCharsetAlloc(alloc, &bytes, 1, 4);
    defer alloc.free(charset);
    try std.testing.expectEqualSlices(u16, &.{ 0, 10, 11, 20 }, charset);
}

test "cff parses charset format 2" {
    const alloc = std.testing.allocator;
    const bytes =
        [_]u8{
            0,
            2,
            0,
            30,
            0,
            2,
            0,
            40,
            0,
            0,
        };
    const charset = try parseCharsetAlloc(alloc, &bytes, 1, 5);
    defer alloc.free(charset);
    try std.testing.expectEqualSlices(u16, &.{ 0, 30, 31, 32, 40 }, charset);
}

test "cff parses fdselect format 0" {
    const alloc = std.testing.allocator;
    const bytes =
        [_]u8{
            0,
            2,
            1,
            0,
        };
    const fd_select = try parseFDSelectAlloc(alloc, &bytes, 0, 3);
    defer alloc.free(fd_select);
    try std.testing.expectEqualSlices(u16, &.{ 2, 1, 0 }, fd_select);
}

test "cff parses fdselect format 3" {
    const alloc = std.testing.allocator;
    const bytes =
        [_]u8{
            3,
            0,
            2,
            0,
            0,
            1,
            0,
            2,
            3,
            0,
            5,
        };
    const fd_select = try parseFDSelectAlloc(alloc, &bytes, 0, 5);
    defer alloc.free(fd_select);
    try std.testing.expectEqualSlices(u16, &.{ 1, 1, 3, 3, 3 }, fd_select);
}

test "cff selects per-fd local subroutines" {
    const alloc = std.testing.allocator;
    const charstrings_bytes =
        [_]u8{
            0,   2,   1,  1,  7,  13,
            139, 139, 21, 32, 10, 14,
            139, 139, 21, 32, 10, 14,
        };
    const local0_bytes =
        [_]u8{
            0,   1,   1, 1,  5,
            189, 139, 5, 11,
        };
    const local1_bytes =
        [_]u8{
            0,   1,   1, 1,  5,
            139, 189, 5, 11,
        };

    var bytes = std.ArrayList(u8).empty;
    defer bytes.deinit(alloc);
    try bytes.appendSlice(alloc, &charstrings_bytes);
    const local0_offset = bytes.items.len;
    try bytes.appendSlice(alloc, &local0_bytes);
    const local1_offset = bytes.items.len;
    try bytes.appendSlice(alloc, &local1_bytes);

    var charset = [_]u16{ 0, 1 };
    var fd_array = [_]FontDict{
        .{ .local_subrs = try parseIndex(bytes.items, local0_offset) },
        .{ .local_subrs = try parseIndex(bytes.items, local1_offset) },
    };
    var fd_select = [_]u16{ 0, 1 };

    const font = Font{
        .bytes = bytes.items,
        .charstrings = try parseIndex(bytes.items, 0),
        .charset = charset[0..],
        .global_subrs = .{ .count = 0, .off_size = 0, .data_offset = 0, .offsets_offset = 0 },
        .local_subrs = null,
        .fd_array = fd_array[0..],
        .fd_select = fd_select[0..],
    };

    var outline0 = (try font.glyphOutlineAlloc(alloc, 0)).?;
    defer outline0.deinit(alloc);
    var outline1 = (try font.glyphOutlineAlloc(alloc, 1)).?;
    defer outline1.deinit(alloc);

    try std.testing.expectApproxEqAbs(@as(f64, 50), outline0.contours[0].points[1].x, 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 0), outline0.contours[0].points[1].y, 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 0), outline1.contours[0].points[1].x, 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 50), outline1.contours[0].points[1].y, 0.001);
}
