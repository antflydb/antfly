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
    GlyphOutlineTooComplex,
};

const ParseError = Error || std.mem.Allocator.Error;

pub const OutlineLimits = struct {
    max_operations: usize = std.math.maxInt(usize),
    /// Optional caller-owned page/request meter shared across glyphs.
    remaining_operations: ?*usize = null,
};

const FlexState = struct {
    active: bool = false,
    start: [2]f64 = .{ 0, 0 },
    points: [7][2]f64 = undefined,
    point_count: usize = 0,

    fn begin(self: *FlexState, x: f64, y: f64) Error!void {
        if (self.active or !std.math.isFinite(x) or !std.math.isFinite(y)) return error.InvalidType1;
        self.* = .{
            .active = true,
            .start = .{ x, y },
        };
    }

    fn record(self: *FlexState, x: f64, y: f64) Error!void {
        if (!self.active or self.point_count >= self.points.len or !std.math.isFinite(x) or !std.math.isFinite(y))
            return error.InvalidType1;
        self.points[self.point_count] = .{ x, y };
        self.point_count += 1;
    }

    fn finish(
        self: *FlexState,
        alloc: std.mem.Allocator,
        current: *std.ArrayList(GlyphPoint),
        x: *f64,
        y: *f64,
        arguments: []const f64,
    ) ParseError!void {
        if (!self.active or self.point_count != self.points.len or arguments.len != 3)
            return error.InvalidType1;
        for (arguments) |argument| if (!std.math.isFinite(argument)) return error.InvalidType1;
        if (current.items.len == 0) {
            try current.append(alloc, .{ .x = self.start[0], .y = self.start[1], .on_curve = true });
        }
        const current_point = current.items[current.items.len - 1];
        if (!pointsAlmostEqual(current_point, .{ .x = self.start[0], .y = self.start[1], .on_curve = true }))
            return error.InvalidType1;

        try appendCubicFlattenedAlloc(alloc, current, self.start, self.points[1], self.points[2], self.points[3], 8);
        try appendCubicFlattenedAlloc(alloc, current, self.points[3], self.points[4], self.points[5], self.points[6], 8);
        x.* = arguments[1];
        y.* = arguments[2];
        self.* = .{};
    }
};

fn exactIntFromFloat(comptime T: type, value: f64) Error!T {
    if (!std.math.isFinite(value) or
        @trunc(value) != value or
        value < @as(f64, @floatFromInt(std.math.minInt(T))) or
        value > @as(f64, @floatFromInt(std.math.maxInt(T))))
    {
        return error.InvalidType1;
    }
    return @intFromFloat(value);
}

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
    return glyphOutlineAllocLimited(alloc, charstring, local_subrs, .{});
}

pub fn glyphOutlineAllocLimited(alloc: std.mem.Allocator, charstring: []const u8, local_subrs: ?[]const []const u8, limits: OutlineLimits) ParseError!?GlyphOutline {
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
    var othersubr_results = std.ArrayList(f64).empty;
    defer othersubr_results.deinit(alloc);
    var flex = FlexState{};

    var x: f64 = 0;
    var y: f64 = 0;
    var width_seen = false;
    const shared_limit = if (limits.remaining_operations) |remaining| remaining.* else std.math.maxInt(usize);
    var remaining_operations = @min(limits.max_operations, shared_limit);
    const initial_operations = remaining_operations;
    defer if (limits.remaining_operations) |remaining| {
        remaining.* -|= initial_operations - remaining_operations;
    };
    try executeCharStringAlloc(alloc, charstring, local_subrs, &stack, &othersubr_results, &flex, &current, &contours, &x, &y, &width_seen, null, &remaining_operations, 0);

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
    return seacComponentsAllocLimited(alloc, charstring, local_subrs, .{});
}

pub fn seacComponentsAllocLimited(alloc: std.mem.Allocator, charstring: []const u8, local_subrs: ?[]const []const u8, limits: OutlineLimits) ParseError!?SeacComponents {
    if (charstring.len == 0) return null;

    var stack = std.ArrayList(f64).empty;
    defer stack.deinit(alloc);
    var othersubr_results = std.ArrayList(f64).empty;
    defer othersubr_results.deinit(alloc);
    var flex = FlexState{};
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
    const shared_limit = if (limits.remaining_operations) |remaining| remaining.* else std.math.maxInt(usize);
    var remaining_operations = @min(limits.max_operations, shared_limit);
    const initial_operations = remaining_operations;
    defer if (limits.remaining_operations) |remaining| {
        remaining.* -|= initial_operations - remaining_operations;
    };
    try executeCharStringAlloc(
        alloc,
        charstring,
        local_subrs,
        &stack,
        &othersubr_results,
        &flex,
        &current,
        &contours,
        &x,
        &y,
        &width_seen,
        &out,
        &remaining_operations,
        0,
    );
    return out;
}

fn executeCharStringAlloc(
    alloc: std.mem.Allocator,
    program: []const u8,
    local_subrs: ?[]const []const u8,
    stack: *std.ArrayList(f64),
    othersubr_results: *std.ArrayList(f64),
    flex: *FlexState,
    current: *std.ArrayList(GlyphPoint),
    contours: *std.ArrayList(GlyphContour),
    x: *f64,
    y: *f64,
    width_seen: *bool,
    seac_out: ?*?SeacComponents,
    remaining_operations: *usize,
    depth: u8,
) ParseError!void {
    if (depth > 16) return error.UnsupportedType1;
    var i: usize = 0;
    while (i < program.len) {
        if (remaining_operations.* == 0) return error.GlyphOutlineTooComplex;
        remaining_operations.* -= 1;
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
                if (!flex.active) {
                    if (current.items.len > 0) try flushContour(alloc, contours, current);
                    try current.append(alloc, .{ .x = x.*, .y = y.*, .on_curve = true });
                }
                stack.clearRetainingCapacity();
                width_seen.* = true;
            },
            5 => {
                if (flex.active) return error.InvalidType1;
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
            6 => if (flex.active) return error.InvalidType1 else try executeAlternatingLines(alloc, stack, current, x, y, true),
            7 => if (flex.active) return error.InvalidType1 else try executeAlternatingLines(alloc, stack, current, x, y, false),
            8 => if (flex.active) return error.InvalidType1 else try executeRrcurveto(alloc, stack, current, x, y),
            9 => {
                if (flex.active) return error.InvalidType1;
                if (current.items.len > 0) try flushContour(alloc, contours, current);
                stack.clearRetainingCapacity();
            },
            10 => {
                if (local_subrs == null or stack.items.len == 0) return error.UnsupportedType1;
                const raw_idx = try exactIntFromFloat(i32, stack.pop().?);
                if (raw_idx < 0 or raw_idx >= local_subrs.?.len) return error.InvalidSubroutine;
                try executeCharStringAlloc(alloc, local_subrs.?[@intCast(raw_idx)], local_subrs, stack, othersubr_results, flex, current, contours, x, y, width_seen, seac_out, remaining_operations, depth + 1);
            },
            11 => return,
            12 => {
                if (i >= program.len) return error.TruncatedType1;
                const escaped = program[i];
                i += 1;
                switch (escaped) {
                    0, 1, 2 => {
                        // dotsection, vstem3, and hstem3 only affect the Type 1
                        // rasterizer's hinting state, not outline geometry.
                        stack.clearRetainingCapacity();
                    },
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
                                .bchar = try exactIntFromFloat(u8, stack.items[stack.items.len - 2]),
                                .achar = try exactIntFromFloat(u8, stack.items[stack.items.len - 1]),
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
                    16 => {
                        // callothersubr bridges to a PostScript procedure. The
                        // standard flex and hint-replacement procedures may
                        // return values consumed by the following `pop`
                        // operators. Preserve those values while keeping the
                        // geometric moves in the charstring authoritative.
                        if (stack.items.len < 2) return error.InvalidType1;
                        const subr_number_f = stack.pop().?;
                        const argument_count_f = stack.pop().?;
                        const subr_number = try exactIntFromFloat(i32, subr_number_f);
                        if (!std.math.isFinite(argument_count_f) or
                            @trunc(argument_count_f) != argument_count_f or
                            argument_count_f < 0 or
                            argument_count_f > @as(f64, @floatFromInt(stack.items.len)))
                        {
                            return error.InvalidType1;
                        }
                        const argument_count: usize = @intFromFloat(argument_count_f);
                        const first = stack.items.len - argument_count;
                        const arguments = stack.items[first..];
                        othersubr_results.clearRetainingCapacity();
                        switch (subr_number) {
                            // Flex termination emits the two source Beziers and
                            // returns the absolute final point to the customary
                            // `pop pop setcurrentpoint` tail.
                            0 => {
                                try flex.finish(alloc, current, x, y, arguments);
                                try othersubr_results.appendSlice(alloc, &.{ arguments[2], arguments[1] });
                            },
                            1 => {
                                if (argument_count != 0) return error.InvalidType1;
                                try flex.begin(x.*, y.*);
                            },
                            2 => {
                                if (argument_count != 0) return error.InvalidType1;
                                try flex.record(x.*, y.*);
                            },
                            // Hint replacement returns its sole argument.
                            3 => {
                                if (argument_count != 1) return error.InvalidType1;
                                try othersubr_results.append(alloc, arguments[0]);
                            },
                            // Font-defined OtherSubrs execute arbitrary
                            // PostScript. Never fabricate their results: doing
                            // so produces plausible but incorrect outlines.
                            else => return error.UnsupportedType1,
                        }
                        stack.shrinkRetainingCapacity(first);
                    },
                    17 => {
                        const value = othersubr_results.pop() orelse return error.InvalidType1;
                        try stack.append(alloc, value);
                    },
                    33 => {
                        if (flex.active) return error.InvalidType1;
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
                if (flex.active) return error.InvalidType1;
                if (current.items.len > 0) try flushContour(alloc, contours, current);
                return;
            },
            21 => {
                if (stack.items.len < 2) return error.InvalidType1;
                x.* += stack.items[stack.items.len - 2];
                y.* += stack.items[stack.items.len - 1];
                if (!flex.active) {
                    if (current.items.len > 0) try flushContour(alloc, contours, current);
                    try current.append(alloc, .{ .x = x.*, .y = y.*, .on_curve = true });
                }
                stack.clearRetainingCapacity();
                width_seen.* = true;
            },
            22 => {
                if (stack.items.len < 1) return error.InvalidType1;
                x.* += stack.items[stack.items.len - 1];
                if (!flex.active) {
                    if (current.items.len > 0) try flushContour(alloc, contours, current);
                    try current.append(alloc, .{ .x = x.*, .y = y.*, .on_curve = true });
                }
                stack.clearRetainingCapacity();
                width_seen.* = true;
            },
            30 => if (flex.active) return error.InvalidType1 else try executeVhcurveto(alloc, stack, current, x, y),
            31 => if (flex.active) return error.InvalidType1 else try executeHvcurveto(alloc, stack, current, x, y),
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

test "type1 accepts standard othersubr and pop operators" {
    const alloc = std.testing.allocator;
    const charstring = [_]u8{
        139, 139, 21, // 0 0 rmoveto
        189, 139, 5, // 50 0 rlineto
        149, 140, 142, 12, 16, // 10 1 3 callothersubr
        12, 17, 159, 12, 33, // pop 20 setcurrentpoint
        139, 189, 5, 14, // 0 50 rlineto endchar
    };

    var outline = (try glyphOutlineAlloc(alloc, &charstring, null)).?;
    defer outline.deinit(alloc);
    try std.testing.expect(outline.contours.len >= 1);
}

test "type1 rejects non-integral and out-of-range othersubr operands" {
    const alloc = std.testing.allocator;
    // 0 2147483647 (2 4 div) div => subroutine number 4294967294.
    const out_of_range = [_]u8{
        139, 255, 0x7f, 0xff, 0xff, 0xff, 141, 143, 12, 12, 12, 12, 12, 16, 14,
    };
    try std.testing.expectError(error.InvalidType1, glyphOutlineAlloc(alloc, &out_of_range, null));

    // Fractional argument counts are invalid rather than implicitly truncated.
    const fractional_count = [_]u8{ 141, 143, 12, 12, 139, 12, 16, 14 };
    try std.testing.expectError(error.InvalidType1, glyphOutlineAlloc(alloc, &fractional_count, null));
}

test "type1 rejects custom OtherSubrs that require PostScript execution" {
    const alloc = std.testing.allocator;
    const custom_othersubr = [_]u8{
        139, 139, 21, // 0 0 rmoveto
        149, 159, 141, 143, 12, 16, // 10 20 2 4 callothersubr
        12, 17, 12, 17, 12, 33, // pop pop setcurrentpoint
        139, 189, 5, 14, // 0 50 rlineto endchar
    };
    try std.testing.expectError(error.UnsupportedType1, glyphOutlineAlloc(alloc, &custom_othersubr, null));
}

test "type1 enforces a shared charstring operation limit" {
    const charstring = [_]u8{ 139, 139, 21, 149, 139, 5, 14 };
    try std.testing.expectError(error.GlyphOutlineTooComplex, glyphOutlineAllocLimited(std.testing.allocator, &charstring, null, .{ .max_operations = 3 }));
}

test "type1 charges caller-owned operation meter across glyphs" {
    const alloc = std.testing.allocator;
    const charstring = [_]u8{ 139, 139, 21, 149, 139, 5, 14 };
    var remaining: usize = 8;
    if (try glyphOutlineAllocLimited(alloc, &charstring, null, .{ .remaining_operations = &remaining })) |outline_value| {
        var outline = outline_value;
        outline.deinit(alloc);
    }
    try std.testing.expect(remaining < 8);
    try std.testing.expectError(error.GlyphOutlineTooComplex, glyphOutlineAllocLimited(alloc, &charstring, null, .{
        .remaining_operations = &remaining,
    }));
    try std.testing.expectEqual(@as(usize, 0), remaining);
}

test "type1 rejects empty OtherSubr pop" {
    const alloc = std.testing.allocator;
    const empty_pop = [_]u8{ 12, 17, 14 };
    try std.testing.expectError(error.InvalidType1, glyphOutlineAlloc(alloc, &empty_pop, null));
}

test "type1 reconstructs standard Flex curves" {
    const alloc = std.testing.allocator;
    const charstring = [_]u8{
        239, 129, 21, // 100 -10 rmoveto
        139, 140, 12, 16, // 0 1 callothersubr
        189, 139, 21, 139, 141, 12, 16, // 50 0 rmoveto; 0 2 callothersubr
        104, 139, 21, 139, 141, 12, 16, // -35 0 rmoveto; record
        149, 149, 21, 139, 141, 12, 16, // 10 10 rmoveto; record
        164, 139, 21, 139, 141, 12, 16, // 25 0 rmoveto; record
        164, 139, 21, 139, 141, 12, 16, // 25 0 rmoveto; record
        149, 129, 21, 139, 141, 12, 16, // 10 -10 rmoveto; record
        154, 139, 21, 139, 141, 12, 16, // 15 0 rmoveto; record
        189, 247, 92, 129, 142, 139, 12, 16, // 50 200 -10 3 0 callothersubr
        12, 17, 12, 17, 12, 33, // pop pop setcurrentpoint
        14,
    };

    var outline = (try glyphOutlineAlloc(alloc, &charstring, null)).?;
    defer outline.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), outline.contours.len);
    try std.testing.expectEqual(@as(usize, 17), outline.contours[0].points.len);
    try std.testing.expectApproxEqAbs(@as(f64, 100), outline.contours[0].points[0].x, 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 150), outline.contours[0].points[8].x, 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 0), outline.contours[0].points[8].y, 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 200), outline.contours[0].points[16].x, 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, -10), outline.contours[0].points[16].y, 0.001);
}

test "type1 rejects unsafe subroutine and seac integer operands" {
    const alloc = std.testing.allocator;
    const return_subr = [_]u8{11};
    const local_subrs = [_][]const u8{&return_subr};

    // 0 0 div produces NaN. Conversion must return a parse error rather than
    // reaching Zig's safety panic for a non-finite float-to-integer cast.
    const non_finite_callsubr = [_]u8{ 139, 139, 12, 12, 10, 14 };
    try std.testing.expectError(error.InvalidType1, glyphOutlineAlloc(alloc, &non_finite_callsubr, &local_subrs));

    // bchar is 256, outside the byte range required by seac.
    const out_of_range_seac = [_]u8{ 139, 139, 139, 247, 148, 139, 12, 6, 14 };
    try std.testing.expectError(error.InvalidType1, seacComponentsAlloc(alloc, &out_of_range_seac, null));
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
