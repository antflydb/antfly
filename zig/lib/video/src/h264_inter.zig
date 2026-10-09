// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
const layout = @import("h264_layout.zig");
const motion = @import("h264_motion.zig");
const weights = @import("h264_weights.zig");
const Syntax = @import("h264_entropy.zig").Syntax;
const Part = struct { x: usize, y: usize, width: usize, height: usize, group: usize, mask: u2 = 1, direct: bool = false };
fn readMotion(syntax: *Syntax, motions: []const motion.Motion, x: i32, y: i32, require_decoded: bool, top: bool) ?motion.Motion {
    const point = layout.cellSample(syntax.meta, syntax.width, syntax.chroma_format, syntax.paired, syntax.field, syntax.parity, 0, x * 4, y * 4 + (if (top) @as(i32, 3) else 0)) orelse return null;
    const meta = syntax.meta[point.mb];
    if (meta.kind == 255 or meta.slice_id != syntax.slice_id) return null;
    var m = motions[point.index];
    if ((require_decoded and !m.decoded) or m.reference == -2) return null;
    if (syntax.meta[point.mb].field != syntax.field) {
        if (syntax.field) {
            m.vector.y = @divTrunc(m.vector.y, 2);
            m.difference.y = @divTrunc(m.difference.y, 2);
            if (m.reference >= 0) m.reference *= 2;
        } else {
            m.vector.y *= 2;
            m.difference.y *= 2;
            if (m.reference >= 0) m.reference = @divTrunc(m.reference, 2);
        }
    }
    return m;
}
fn candidate(syntax: *Syntax, motions: []const motion.Motion, x: i32, y: i32, top: bool) ?motion.Motion {
    return readMotion(syntax, motions, x, y, true, top);
}
fn neighbor(syntax: *Syntax, motions: []const motion.Motion, stride: usize, x: usize, y: usize, top: bool) motion.Motion {
    _ = stride;
    return readMotion(syntax, motions, @as(i32, @intCast(x)) - @as(i32, @intFromBool(!top)), @as(i32, @intCast(y)) - @as(i32, @intFromBool(top)), false, top) orelse .{};
}
fn median(a: i32, b: i32, c: i32) i32 {
    return a + b + c - @min(a, @min(b, c)) - @max(a, @max(b, c));
}
fn predictor(syntax: *Syntax, motions: []const motion.Motion, stride: usize, x: usize, y: usize, width: usize, height: usize, reference: i8, skipped: bool) motion.Vector {
    _ = stride;
    const ix: i32 = @intCast(x);
    const iy: i32 = @intCast(y);
    const a = candidate(syntax, motions, ix - 1, iy, false);
    const b = candidate(syntax, motions, ix, iy - 1, true);
    const c = candidate(syntax, motions, ix + @as(i32, @intCast(width)), iy - 1, true) orelse candidate(syntax, motions, ix - 1, iy - 1, true);
    if (skipped and (a == null or b == null or (a.?.reference == 0 and a.?.vector.x == 0 and a.?.vector.y == 0) or (b.?.reference == 0 and b.?.vector.x == 0 and b.?.vector.y == 0))) return .{};
    if (width == 4 and height == 2) {
        const preferred = if (y % 4 == 0) b else a;
        if (preferred) |m| if (m.reference == reference) return m.vector;
    }
    if (width == 2 and height == 4) {
        const preferred = if (x % 4 == 0) a else c;
        if (preferred) |m| if (m.reference == reference) return m.vector;
    }
    if (b == null and c == null) return if (a) |m| m.vector else .{};
    var matched: usize = 0;
    var selected = motion.Vector{};
    for ([_]?motion.Motion{ a, b, c }) |value| if (value) |m| if (m.reference == reference) {
        matched += 1;
        selected = m.vector;
    };
    if (matched == 1) return selected;
    const av = if (a) |m| m.vector else motion.Vector{};
    const bv = if (b) |m| m.vector else motion.Vector{};
    const cv = if (c) |m| m.vector else motion.Vector{};
    return .{ .x = median(av.x, bv.x, cv.x), .y = median(av.y, bv.y, cv.y) };
}
fn fill(syntax: *Syntax, motions: []motion.Motion, stride: usize, x: usize, y: usize, part: Part, value: motion.Motion) void {
    _ = stride;
    for (0..part.height) |row| for (0..part.width) |column| {
        motions[syntax.cellIndex(0, x + part.x + column, y + part.y + row)] = value;
    };
}
fn colocated(syntax: *Syntax, state: anytype, stride: usize, mx: usize, my: usize, part: Part, direct8: bool) !struct { value: motion.Motion, id: u32, poc: i32 } {
    const pic = state.pictures[try state.referenceIndex(1, 0)];
    const x = if (direct8) part.x / 2 * 3 else part.x;
    const y = if (direct8) part.y / 2 * 3 else part.y;
    const point = layout.cell(pic.meta, stride * 4, 1, pic.paired, syntax.field, try state.referenceParity(1, 0), 0, @intCast(mx + x), @intCast(my + y)) orelse return error.MalformedVideoPacket;
    const index = point.index;
    const list: usize = if (pic.motions[0][index].reference >= 0) 0 else 1;
    var m = pic.motions[list][index];
    if (pic.meta[point.mb].field != syntax.field) {
        m.vector.y = if (syntax.field) @divTrunc(m.vector.y, 2) else m.vector.y * 2;
    }
    const id = if (syntax.field) m.identity / 4 * 4 + 2 + (if (m.identity % 4 >= 2) m.identity % 2 else syntax.parity) else m.identity / 4 * 4;
    return .{ .value = m, .id = if (m.reference >= 0) @as(u32, @intCast(id)) else std.math.maxInt(u32), .poc = if (syntax.field) pic.field_poc[try state.referenceParity(1, 0)] else pic.poc };
}
fn direct(syntax: *Syntax, state: anytype, motions: [2][]motion.Motion, stride: usize, mx: usize, my: usize, part: Part, spatial: bool, direct8: bool, refs: [2]i8, predictors: [2]motion.Vector) !void {
    const col = try colocated(syntax, state, stride, mx, my, part, direct8);
    var selected = refs;
    var vectors = predictors;
    if (spatial) {
        const zero = !(try state.referenceLong(1, 0)) and col.value.reference == 0 and @abs(col.value.vector.x) <= 1 and @abs(col.value.vector.y) <= 1;
        for (0..2) |list| if (selected[list] == 0 and zero) {
            vectors[list] = .{};
        };
    } else {
        selected = .{ 0, 0 };
        vectors = .{ .{}, .{} };
        if (col.value.reference >= 0) {
            var found = false;
            for (0..state.referenceCount(0)) |i| if (try identity(state, 0, @intCast(i)) == col.id) {
                selected[0] = @intCast(i);
                found = true;
                break;
            };
            if (!found) return error.MissingVideoReference;
            const scale = if ((try state.referenceLong(0, @intCast(selected[0])))) @as(i32, 256) else weights.distance(currentPoc(state), try referencePoc(state, 0, @intCast(selected[0])), col.poc);
            vectors[0] = .{ .x = (scale * col.value.vector.x + 128) >> 8, .y = (scale * col.value.vector.y + 128) >> 8 };
            vectors[1] = .{ .x = vectors[0].x - col.value.vector.x, .y = vectors[0].y - col.value.vector.y };
        }
    }
    for (0..2) |list| fill(syntax, motions[list], stride, mx, my, part, .{ .decoded = true, .identity = try identity(state, list, selected[list]), .direct = true, .reference = selected[list], .vector = vectors[list] });
}
fn compensate(syntax: *Syntax, state: anytype, planes: anytype, motions: [2][]motion.Motion, width: usize, height: usize, mx: usize, my: usize, part: Part, bit_depth: u8, chroma_format: u8) !void {
    const m = [2]motion.Motion{ motions[0][syntax.cellIndex(0, mx + part.x, my + part.y)], motions[1][syntax.cellIndex(0, mx + part.x, my + part.y)] };
    const Sample = layout.Sample(@TypeOf(planes[0]));
    var references: [2][3]layout.View(Sample) = undefined;
    for (0..2) |list| if (m[list].reference >= 0) {
        const reference: usize = @intCast(m[list].reference);
        const data = try state.planes(list, reference, width, height);
        for (0..3) |p| references[list][p] = .{ .data = data[p], .width = width / (if (p == 0 or chroma_format == 3) @as(usize, 1) else 2), .height = height / (if (p == 0 or chroma_format != 1) @as(usize, 1) else 2) / (if (state.field_mode) @as(usize, 2) else 1), .field = state.field_mode, .parity = try state.referenceParity(list, reference) };
    };
    if (m[0].reference < 0 and m[1].reference < 0) return error.MissingVideoReference;
    for (0..3) |p| {
        const divisor: usize = if (p == 0 or chroma_format == 3) 1 else 2;
        const stride = width / divisor;
        const sub_y: usize = if (p == 0 or chroma_format >= 2) 1 else 2;
        for (0..part.height * 4 / sub_y) |row| for (0..part.width * 4 / divisor) |column| {
            const x = (mx + part.x) * 4 / divisor + column;
            const y = (my + part.y) * 4 / sub_y + row;
            var values: [2]Sample = .{ 0, 0 };
            for (0..2) |list| if (m[list].reference >= 0) {
                values[list] = motion.pixel(references[list][p], stride, x, y, .{ .x = m[list].vector.x, .y = m[list].vector.y * (if (p != 0 and chroma_format == 2) @as(i32, 2) else 1) + (if (p != 0 and chroma_format == 1 and state.field_mode) 2 * (@as(i32, @intCast(state.field_parity)) - @as(i32, @intCast(references[list][p].parity))) else 0) }, p != 0 and chroma_format != 3, bit_depth);
            };
            var value: Sample = undefined;
            if (m[0].reference >= 0 and m[1].reference >= 0) {
                value = switch (state.weight_mode) {
                    .none => @intCast((@as(u32, values[0]) + values[1] + 1) / 2),
                    .explicit => weights.pairDepth(values[0], values[1], state.weights[0][@as(usize, @intCast(m[0].reference)) / (if (state.field_mode and !state.field_picture) @as(usize, 2) else 1)][p], state.weights[1][@as(usize, @intCast(m[1].reference)) / (if (state.field_mode and !state.field_picture) @as(usize, 2) else 1)][p], bit_depth),
                    .implicit => if ((try state.referenceLong(0, @intCast(m[0].reference))) or (try state.referenceLong(1, @intCast(m[1].reference)))) @intCast((@as(u32, values[0]) + values[1] + 1) / 2) else weights.implicitDepth(values[0], values[1], currentPoc(state), try referencePoc(state, 0, @intCast(m[0].reference)), try referencePoc(state, 1, @intCast(m[1].reference)), bit_depth),
                };
            } else {
                const list: usize = if (m[0].reference >= 0) 0 else 1;
                value = if (state.weight_mode == .explicit) weights.singleDepth(values[list], state.weights[list][@as(usize, @intCast(m[list].reference)) / (if (state.field_mode and !state.field_picture) @as(usize, 2) else 1)][p], bit_depth) else values[list];
            }
            layout.put(planes[p], y * stride + x, value);
        };
    }
}
fn identity(state: anytype, list: usize, reference: i8) !u32 {
    if (reference < 0) return std.math.maxInt(u32);
    const index = try state.referenceIndex(list, @intCast(reference));
    return state.pictures[index].id * 4 + (if (state.field_mode) @as(u32, @intCast(2 + try state.referenceParity(list, @intCast(reference)))) else 0);
}
fn currentPoc(state: anytype) i32 {
    return if (state.field_mode) state.current_field_poc[state.field_parity] else state.current_poc;
}
fn referencePoc(state: anytype, list: usize, reference: usize) !i32 {
    const pic = state.pictures[try state.referenceIndex(list, reference)];
    return if (state.field_mode) pic.field_poc[try state.referenceParity(list, reference)] else pic.poc;
}

pub fn predict(syntax: *Syntax, state: anytype, planes: anytype, motions: [2][]motion.Motion, width: usize, height: usize, kind: u32, active: [2]usize, skipped: bool, spatial: bool, direct8: bool, bit_depth: u8, chroma_format: u8) !bool {
    const b = syntax.slice_type == 1;
    const mx = syntax.x;
    const my = syntax.y;
    const stride = width / 4;
    var parts: [16]Part = undefined;
    var count: usize = 0;
    var groups: usize = 1;
    var allow8 = true;
    if (b and (kind == 0 or skipped)) {
        const size: usize = if (direct8) 2 else 1;
        allow8 = direct8;
        for (0..4 / size) |row| for (0..4 / size) |column| {
            parts[count] = .{ .x = column * size, .y = row * size, .width = size, .height = size, .group = 0, .mask = 3, .direct = true };
            count += 1;
        };
        syntax.meta[syntax.mb].direct = true;
    } else if ((!b and kind < 3) or (b and kind < 22)) {
        const split = if (b) kind >= 4 else kind != 0;
        const vertical = if (b) kind % 2 == 1 else kind == 2;
        groups = if (split) 2 else 1;
        const masks = [_][2]u2{ .{ 1, 1 }, .{ 2, 2 }, .{ 1, 2 }, .{ 2, 1 }, .{ 1, 3 }, .{ 2, 3 }, .{ 3, 1 }, .{ 3, 2 }, .{ 3, 3 } };
        for (0..groups) |i| {
            parts[i] = .{ .x = if (split and vertical) i * 2 else 0, .y = if (split and !vertical) i * 2 else 0, .width = if (split and vertical) 2 else 4, .height = if (split and !vertical) 2 else 4, .group = i, .mask = if (!b) 1 else if (!split) @intCast(kind) else masks[(kind - 4) / 2][i] };
        }
        count = groups;
    } else if ((!b and (kind == 3 or kind == 4)) or (b and kind == 22)) {
        groups = 4;
        for (0..4) |g| {
            const sub = try syntax.subkind();
            const is_direct = b and sub == 0;
            const mask: u2 = if (!b) 1 else if (sub <= 3) @intCast(@max(sub, 1)) else if (sub < 6 or sub == 10) 1 else if (sub < 8 or sub == 11) 2 else 3;
            const sw: usize = if (is_direct) (if (direct8) @as(usize, 2) else 1) else if (!b) (if (sub >= 2) @as(usize, 1) else 2) else if (sub >= 10 or (sub >= 4 and sub % 2 == 1)) 1 else 2;
            const sh: usize = if (is_direct) sw else if (!b) (if (sub == 1 or sub == 3) @as(usize, 1) else 2) else if (sub >= 10 or (sub >= 4 and sub % 2 == 0)) 1 else 2;
            if (sw < 2 or sh < 2) allow8 = false;
            for (0..2 / sh) |row| for (0..2 / sw) |column| {
                parts[count] = .{ .x = g % 2 * 2 + column * sw, .y = g / 2 * 2 + row * sh, .width = sw, .height = sh, .group = g, .mask = mask, .direct = is_direct };
                count += 1;
            };
        }
    } else return error.MalformedVideoPacket;
    var direct_refs: [2]i8 = .{ -1, -1 };
    var direct_predictions: [2]motion.Vector = .{ .{}, .{} };
    if (b and spatial) {
        for (0..2) |list| {
            const a = neighbor(syntax, motions[list], stride, mx, my, false);
            const top = neighbor(syntax, motions[list], stride, mx, my, true);
            const c = candidate(syntax, motions[list], @intCast(mx + 4), @as(i32, @intCast(my)) - 1, true) orelse candidate(syntax, motions[list], @as(i32, @intCast(mx)) - 1, @as(i32, @intCast(my)) - 1, true) orelse motion.Motion{};
            for ([_]motion.Motion{ a, top, c }) |m| if (m.decoded and m.reference >= 0 and (direct_refs[list] < 0 or m.reference < direct_refs[list])) {
                direct_refs[list] = m.reference;
            };
            if (direct_refs[list] >= 0) direct_predictions[list] = predictor(syntax, motions[list], stride, mx, my, 4, 4, direct_refs[list], false);
        }
        if (direct_refs[0] < 0 and direct_refs[1] < 0) direct_refs = .{ 0, 0 };
    }
    var references: [2][4]i8 = @splat(@splat(-1));
    for (0..if (b) @as(usize, 2) else 1) |list| for (0..groups) |g| {
        var first: ?Part = null;
        for (parts[0..count]) |part| if (part.group == g) {
            first = part;
            break;
        };
        const part = first.?;
        if (part.direct or part.mask & (@as(u2, 1) << @as(u1, @intCast(list))) == 0) continue;
        const bx = mx + part.x;
        const by = my + part.y;
        const left = neighbor(syntax, motions[list], stride, bx, by, false);
        const top = neighbor(syntax, motions[list], stride, bx, by, true);
        references[list][g] = if (skipped or (!b and kind == 4)) 0 else try syntax.reference(active[list], if (left.direct) -1 else left.reference, if (top.direct) -1 else top.reference);
        for (parts[0..count]) |p| if (p.group == g) {
            for (0..p.height) |row| for (0..p.width) |column| {
                motions[list][syntax.cellIndex(0, mx + p.x + column, my + p.y + row)].reference = references[list][g];
            };
        };
    };
    // Direct motion is inferred before parsing neighbouring MVD contexts.
    for (parts[0..count]) |part| if (part.direct) {
        try direct(syntax, state, motions, stride, mx, my, part, spatial, direct8, direct_refs, direct_predictions);
    };
    for (0..if (b) @as(usize, 2) else 1) |list| for (parts[0..count]) |part| {
        if (part.direct) continue;
        if (references[list][part.group] < 0) {
            fill(syntax, motions[list], stride, mx, my, part, .{ .decoded = true });
            continue;
        }
        const bx = mx + part.x;
        const by = my + part.y;
        const left = neighbor(syntax, motions[list], stride, bx, by, false);
        const top = neighbor(syntax, motions[list], stride, bx, by, true);
        const prediction = predictor(syntax, motions[list], stride, bx, by, part.width, part.height, references[list][part.group], skipped);
        const dx = if (skipped) 0 else try syntax.mvd(0, @as(u32, if (left.reference >= 0) @abs(left.difference.x) else 0) + if (top.reference >= 0) @abs(top.difference.x) else 0);
        const dy = if (skipped) 0 else try syntax.mvd(1, @as(u32, if (left.reference >= 0) @abs(left.difference.y) else 0) + if (top.reference >= 0) @abs(top.difference.y) else 0);
        const mv = motion.Vector{ .x = prediction.x + dx, .y = prediction.y + dy };
        if (mv.x < -32768 or mv.x > 32767 or mv.y < -32768 or mv.y > 32767) return error.MalformedVideoPacket;
        fill(syntax, motions[list], stride, mx, my, part, .{ .identity = try identity(state, list, references[list][part.group]), .decoded = true, .reference = references[list][part.group], .vector = mv, .difference = .{ .x = dx, .y = dy } });
    };
    if (!b) for (parts[0..count]) |part| {
        fill(syntax, motions[1], stride, mx, my, part, .{ .decoded = true });
    };
    for (parts[0..count]) |part| try compensate(syntax, state, planes, motions, width, height, mx, my, part, bit_depth, chroma_format);
    return allow8;
}
