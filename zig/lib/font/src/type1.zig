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

pub const GlyphPoint = @import("sfnt.zig").GlyphPoint;
pub const GlyphContour = @import("sfnt.zig").GlyphContour;
pub const GlyphOutline = @import("sfnt.zig").GlyphOutline;

pub const Error = error{
    InvalidType1,
    TruncatedType1,
    UnsupportedType1,
    InvalidSubroutine,
};

const ParseError = Error || std.mem.Allocator.Error;

pub const SeacComponents = struct {
    asb: f64,
    adx: f64,
    ady: f64,
    bchar: u8,
    achar: u8,
};

pub fn decryptCharStringAlloc(alloc: std.mem.Allocator, encrypted: []const u8, lenIV: usize) ParseError![]u8 {
    return try decryptWithSeedAlloc(alloc, encrypted, 4330, lenIV);
}

pub fn decryptEexecAlloc(alloc: std.mem.Allocator, encrypted: []const u8) ParseError![]u8 {
    return try decryptWithSeedAlloc(alloc, encrypted, 55665, 4);
}

fn decryptWithSeedAlloc(alloc: std.mem.Allocator, encrypted: []const u8, seed: u16, skip: usize) ParseError![]u8 {
    var out = try alloc.alloc(u8, encrypted.len);
    errdefer alloc.free(out);
    var r: u16 = seed;
    for (encrypted, 0..) |cipher, i| {
        const plain = cipher ^ @as(u8, @truncate(r >> 8));
        r = @truncate((@as(u32, cipher) + r) * 52845 + 22719);
        out[i] = plain;
    }
    if (skip >= out.len) return try alloc.alloc(u8, 0);
    const sliced = try alloc.dupe(u8, out[skip..]);
    alloc.free(out);
    return sliced;
}

pub fn glyphOutlineAlloc(alloc: std.mem.Allocator, charstring: []const u8, local_subrs: ?[]const []const u8) ParseError!?GlyphOutline {
    if (charstring.len == 0) return null;

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
    try executeCharStringAlloc(alloc, charstring, local_subrs, &stack, &current, &contours, &x, &y, &width_seen, null, 0);

    if (contours.items.len == 0) return null;
    return .{
        .contours = try contours.toOwnedSlice(alloc),
        .x_min = 0,
        .y_min = 0,
        .x_max = 0,
        .y_max = 0,
    };
}

pub fn seacComponentsAlloc(alloc: std.mem.Allocator, charstring: []const u8, local_subrs: ?[]const []const u8) ParseError!?SeacComponents {
    if (charstring.len == 0) return null;

    var stack = std.ArrayList(f64).empty;
    defer stack.deinit(alloc);
    var current = std.ArrayList(GlyphPoint).empty;
    defer current.deinit(alloc);
    var contours = std.ArrayList(GlyphContour).empty;
    defer {
        for (contours.items) |*contour| contour.deinit(alloc);
        contours.deinit(alloc);
    }
    var x: f64 = 0;
    var y: f64 = 0;
    var width_seen = false;
    var out: ?SeacComponents = null;
    try executeCharStringAlloc(
        alloc,
        charstring,
        local_subrs,
        &stack,
        &current,
        &contours,
        &x,
        &y,
        &width_seen,
        &out,
        0,
    );
    return out;
}

fn executeCharStringAlloc(
    alloc: std.mem.Allocator,
    program: []const u8,
    local_subrs: ?[]const []const u8,
    stack: *std.ArrayList(f64),
    current: *std.ArrayList(GlyphPoint),
    contours: *std.ArrayList(GlyphContour),
    x: *f64,
    y: *f64,
    width_seen: *bool,
    seac_out: ?*?SeacComponents,
    depth: u8,
) ParseError!void {
    if (depth > 16) return error.UnsupportedType1;
    var i: usize = 0;
    while (i < program.len) {
        const b0 = program[i];
        i += 1;
        switch (b0) {
            1, 3 => {
                stack.clearRetainingCapacity();
                width_seen.* = true;
            },
            4 => {
                if (stack.items.len < 1) return error.InvalidType1;
                y.* += stack.items[stack.items.len - 1];
                if (current.items.len > 0) try flushContour(alloc, contours, current);
                try current.append(alloc, .{ .x = x.*, .y = y.*, .on_curve = true });
                stack.clearRetainingCapacity();
                width_seen.* = true;
            },
            5 => {
                if ((stack.items.len % 2) != 0) return error.InvalidType1;
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
            9 => {
                if (current.items.len > 0) try flushContour(alloc, contours, current);
                stack.clearRetainingCapacity();
            },
            10 => {
                if (local_subrs == null or stack.items.len == 0) return error.UnsupportedType1;
                const raw_idx = @as(i32, @intFromFloat(stack.pop().?));
                if (raw_idx < 0 or raw_idx >= local_subrs.?.len) return error.InvalidSubroutine;
                try executeCharStringAlloc(alloc, local_subrs.?[@intCast(raw_idx)], local_subrs, stack, current, contours, x, y, width_seen, seac_out, depth + 1);
            },
            11 => return,
            12 => {
                if (i >= program.len) return error.TruncatedType1;
                const escaped = program[i];
                i += 1;
                switch (escaped) {
                    7 => {
                        if (stack.items.len < 4) return error.InvalidType1;
                        x.* = stack.items[0];
                        y.* = stack.items[1];
                        stack.clearRetainingCapacity();
                        if (current.items.len > 0) try flushContour(alloc, contours, current);
                        try current.append(alloc, .{ .x = x.*, .y = y.*, .on_curve = true });
                        width_seen.* = true;
                    },
                    6 => {
                        if (stack.items.len < 5) return error.InvalidType1;
                        if (seac_out) |out| {
                            out.* = .{
                                .asb = stack.items[stack.items.len - 5],
                                .adx = stack.items[stack.items.len - 4],
                                .ady = stack.items[stack.items.len - 3],
                                .bchar = @intFromFloat(stack.items[stack.items.len - 2]),
                                .achar = @intFromFloat(stack.items[stack.items.len - 1]),
                            };
                            stack.clearRetainingCapacity();
                            if (current.items.len > 0) try flushContour(alloc, contours, current);
                            return;
                        }
                        return error.UnsupportedType1;
                    },
                    12 => {
                        if (stack.items.len < 2) return error.InvalidType1;
                        const b = stack.pop().?;
                        const a = stack.pop().?;
                        try stack.append(alloc, a / b);
                    },
                    33 => {
                        if (stack.items.len < 2) return error.InvalidType1;
                        x.* = stack.items[stack.items.len - 2];
                        y.* = stack.items[stack.items.len - 1];
                        stack.clearRetainingCapacity();
                        if (current.items.len > 0) try flushContour(alloc, contours, current);
                        try current.append(alloc, .{ .x = x.*, .y = y.*, .on_curve = true });
                    },
                    else => return error.UnsupportedType1,
                }
            },
            13 => {
                if (stack.items.len < 2) return error.InvalidType1;
                x.* = stack.items[0];
                y.* = 0;
                stack.clearRetainingCapacity();
                if (current.items.len > 0) try flushContour(alloc, contours, current);
                try current.append(alloc, .{ .x = x.*, .y = y.*, .on_curve = true });
                width_seen.* = true;
            },
            14 => {
                if (current.items.len > 0) try flushContour(alloc, contours, current);
                return;
            },
            21 => {
                if (stack.items.len < 2) return error.InvalidType1;
                x.* += stack.items[stack.items.len - 2];
                y.* += stack.items[stack.items.len - 1];
                if (current.items.len > 0) try flushContour(alloc, contours, current);
                try current.append(alloc, .{ .x = x.*, .y = y.*, .on_curve = true });
                stack.clearRetainingCapacity();
                width_seen.* = true;
            },
            22 => {
                if (stack.items.len < 1) return error.InvalidType1;
                x.* += stack.items[stack.items.len - 1];
                if (current.items.len > 0) try flushContour(alloc, contours, current);
                try current.append(alloc, .{ .x = x.*, .y = y.*, .on_curve = true });
                stack.clearRetainingCapacity();
                width_seen.* = true;
            },
            30 => try executeVhcurveto(alloc, stack, current, x, y),
            31 => try executeHvcurveto(alloc, stack, current, x, y),
            32...246 => try stack.append(alloc, @floatFromInt(@as(i32, b0) - 139)),
            247...250 => {
                if (i >= program.len) return error.TruncatedType1;
                const value = (@as(i32, b0) - 247) * 256 + program[i] + 108;
                i += 1;
                try stack.append(alloc, @floatFromInt(value));
            },
            251...254 => {
                if (i >= program.len) return error.TruncatedType1;
                const value = -((@as(i32, b0) - 251) * 256) - program[i] - 108;
                i += 1;
                try stack.append(alloc, @floatFromInt(value));
            },
            255 => {
                if (i + 4 > program.len) return error.TruncatedType1;
                const raw = (@as(u32, program[i]) << 24) |
                    (@as(u32, program[i + 1]) << 16) |
                    (@as(u32, program[i + 2]) << 8) |
                    @as(u32, program[i + 3]);
                i += 4;
                const value: i32 = @bitCast(raw);
                try stack.append(alloc, @floatFromInt(value));
            },
            else => return error.UnsupportedType1,
        }
    }
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
    if ((stack.items.len % 6) != 0) return error.InvalidType1;
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
        } else {
            c1 = .{ x.*, y.* + stack.items[s] };
            c2 = .{ c1[0] + stack.items[s + 1], c1[1] + stack.items[s + 2] };
            endp = .{ c2[0] + stack.items[s + 3], c2[1] };
        }
        if (remaining != 4) return error.InvalidType1;
        x.* = endp[0];
        y.* = endp[1];
        try appendCubicFlattenedAlloc(alloc, current, p0, c1, c2, endp, 8);
        s += 4;
        horizontal = !horizontal;
    }
    if (s != stack.items.len) return error.InvalidType1;
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
        } else {
            c1 = .{ x.*, y.* + stack.items[s] };
            c2 = .{ c1[0] + stack.items[s + 1], c1[1] + stack.items[s + 2] };
            endp = .{ c2[0] + stack.items[s + 3], c2[1] };
        }
        if (remaining != 4) return error.InvalidType1;
        x.* = endp[0];
        y.* = endp[1];
        try appendCubicFlattenedAlloc(alloc, current, p0, c1, c2, endp, 8);
        s += 4;
        horizontal = !horizontal;
    }
    if (s != stack.items.len) return error.InvalidType1;
    stack.clearRetainingCapacity();
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

test "type1 parses simple charstring outline" {
    const alloc = std.testing.allocator;
    const charstring = [_]u8{
        139, 139, 21,
        247, 124, 139,
        5,   251, 124,
        250, 124, 5,
        251, 124, 251,
        124, 5,   14,
    };

    var outline = (try glyphOutlineAlloc(alloc, &charstring, null)).?;
    defer outline.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), outline.contours.len);
    try std.testing.expect(outline.contours[0].points.len >= 3);
}

test "type1 executes local subroutine outline" {
    const alloc = std.testing.allocator;
    const subr = [_]u8{ 189, 139, 5, 11 };
    const charstring = [_]u8{
        139, 139, 21,
        139, 10,  14,
    };
    const local_subrs = [_][]const u8{&subr};

    var outline = (try glyphOutlineAlloc(alloc, &charstring, local_subrs[0..])).?;
    defer outline.deinit(alloc);
    try std.testing.expectApproxEqAbs(@as(f64, 50), outline.contours[0].points[1].x, 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 0), outline.contours[0].points[1].y, 0.001);
}

test "type1 decrypts charstring with lenIV" {
    const alloc = std.testing.allocator;
    const plain = [_]u8{ 0, 0, 0, 0, 139, 139, 21, 14 };
    var encrypted = try alloc.alloc(u8, plain.len);
    defer alloc.free(encrypted);
    var r: u16 = 4330;
    for (plain, 0..) |value, i| {
        const cipher = value ^ @as(u8, @truncate(r >> 8));
        encrypted[i] = cipher;
        r = @truncate((@as(u32, cipher) + r) * 52845 + 22719);
    }
    const decrypted = try decryptCharStringAlloc(alloc, encrypted, 4);
    defer alloc.free(decrypted);
    try std.testing.expectEqualSlices(u8, plain[4..], decrypted);
}

test "type1 extracts seac components" {
    const alloc = std.testing.allocator;
    const charstring = [_]u8{
        139, // asb = 0
        247, 92, // adx = 200
        247, 192, // ady = 300
        204, // bchar = 65
        185, // achar = 46
        12,
        6,
        14,
    };
    const seac = (try seacComponentsAlloc(alloc, &charstring, null)).?;
    try std.testing.expectEqual(@as(f64, 0), seac.asb);
    try std.testing.expectEqual(@as(f64, 200), seac.adx);
    try std.testing.expectEqual(@as(f64, 300), seac.ady);
    try std.testing.expectEqual(@as(u8, 65), seac.bchar);
    try std.testing.expectEqual(@as(u8, 46), seac.achar);
}
