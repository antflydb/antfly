// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
const motion = @import("h264_motion.zig");
const weights = @import("h264_weights.zig");
const Syntax = @import("h264_entropy.zig").Syntax;
const State = @import("h264_references.zig").State;
const Part = struct { x: usize, y: usize, width: usize, height: usize, group: usize, mask: u2 = 1, direct: bool = false };
fn neighbor(motions: []const motion.Motion, stride: usize, x: usize, y: usize, top: bool) motion.Motion {
    if (if (top) y == 0 else x == 0) return .{};
    return motions[(y - @as(usize, @intFromBool(top))) * stride + x - @as(usize, @intFromBool(!top))];
}
fn fill(motions: []motion.Motion, stride: usize, x: usize, y: usize, part: Part, value: motion.Motion) void {
    for (0..part.height) |row| for (0..part.width) |column| {
        motions[(y + part.y + row) * stride + x + part.x + column] = value;
    };
}
fn colocated(state: *State, stride: usize, mx: usize, my: usize, part: Part, direct8: bool) !struct { value: motion.Motion, id: u32, poc: i32 } {
    if (state.list_count == 0 or state.list1[0] >= state.count) return error.MissingVideoReference;
    const pic = state.pictures[state.list1[0]];
    const x = if (direct8) part.x / 2 * 3 else part.x;
    const y = if (direct8) part.y / 2 * 3 else part.y;
    const index = (my + y) * stride + mx + x;
    const list: usize = if (pic.motions[0][index].reference >= 0) 0 else 1;
    const m = pic.motions[list][index];
    return .{ .value = m, .id = if (m.reference >= 0) pic.list_ids[list][@intCast(m.reference)] else std.math.maxInt(u32), .poc = pic.poc };
}
fn direct(state: *State, motions: [2][]motion.Motion, stride: usize, mx: usize, my: usize, part: Part, spatial: bool, direct8: bool, refs: [2]i8, predictors: [2]motion.Vector) !void {
    const col = try colocated(state, stride, mx, my, part, direct8);
    var selected = refs;
    var vectors = predictors;
    if (spatial) {
        const zero = state.pictures[state.list1[0]].long_term == null and col.value.reference == 0 and @abs(col.value.vector.x) <= 1 and @abs(col.value.vector.y) <= 1;
        for (0..2) |list| if (selected[list] == 0 and zero) {
            vectors[list] = .{};
        };
    } else {
        selected = .{ 0, 0 };
        vectors = .{ .{}, .{} };
        if (col.value.reference >= 0) {
            var found = false;
            for (0..state.list_count) |i| if (state.list0[i] < state.count and state.pictures[state.list0[i]].id == col.id) {
                selected[0] = @intCast(i);
                found = true;
                break;
            };
            if (!found) return error.MissingVideoReference;
            const first = state.pictures[state.list0[@intCast(selected[0])]];
            const scale = if (first.long_term != null) @as(i32, 256) else weights.distance(state.current_poc, first.poc, col.poc);
            vectors[0] = .{ .x = (scale * col.value.vector.x + 128) >> 8, .y = (scale * col.value.vector.y + 128) >> 8 };
            vectors[1] = .{ .x = vectors[0].x - col.value.vector.x, .y = vectors[0].y - col.value.vector.y };
        }
    }
    for (0..2) |list| fill(motions[list], stride, mx, my, part, .{ .decoded = true, .direct = true, .reference = selected[list], .vector = vectors[list] });
}
fn compensate(state: *State, planes: [3][]u8, motions: [2][]motion.Motion, width: usize, height: usize, mx: usize, my: usize, part: Part) !void {
    const m = [2]motion.Motion{ motions[0][(my + part.y) * (width / 4) + mx + part.x], motions[1][(my + part.y) * (width / 4) + mx + part.x] };
    var references: [2][3][]const u8 = undefined;
    for (0..2) |list| if (m[list].reference >= 0) {
        references[list] = try state.planes(list, @intCast(m[list].reference), width, height);
    };
    if (m[0].reference < 0 and m[1].reference < 0) return error.MissingVideoReference;
    for (0..3) |p| {
        const divisor: usize = if (p == 0) 1 else 2;
        const stride = width / divisor;
        for (0..part.height * 4 / divisor) |row| for (0..part.width * 4 / divisor) |column| {
            const x = (mx + part.x) * 4 / divisor + column;
            const y = (my + part.y) * 4 / divisor + row;
            var values: [2]u8 = .{ 0, 0 };
            for (0..2) |list| if (m[list].reference >= 0) {
                values[list] = motion.pixel(references[list][p], stride, x, y, m[list].vector, p != 0);
            };
            var value: u8 = undefined;
            if (m[0].reference >= 0 and m[1].reference >= 0) {
                value = switch (state.weight_mode) {
                    .none => @intCast((@as(u16, values[0]) + values[1] + 1) / 2),
                    .explicit => weights.pair(values[0], values[1], state.weights[0][@intCast(m[0].reference)][p], state.weights[1][@intCast(m[1].reference)][p]),
                    .implicit => if (state.pictures[state.list0[@intCast(m[0].reference)]].long_term != null or state.pictures[state.list1[@intCast(m[1].reference)]].long_term != null) @intCast((@as(u16, values[0]) + values[1] + 1) / 2) else weights.implicit(values[0], values[1], state.current_poc, state.pictures[state.list0[@intCast(m[0].reference)]].poc, state.pictures[state.list1[@intCast(m[1].reference)]].poc),
                };
            } else {
                const list: usize = if (m[0].reference >= 0) 0 else 1;
                value = if (state.weight_mode == .explicit) weights.single(values[list], state.weights[list][@intCast(m[list].reference)][p]) else values[list];
            }
            planes[p][y * stride + x] = value;
        };
    }
}
pub fn predict(syntax: *Syntax, state: *State, planes: [3][]u8, motions: [2][]motion.Motion, width: usize, height: usize, kind: u32, active: [2]usize, skipped: bool, spatial: bool, direct8: bool) !bool {
    const b = syntax.slice_type == 1;
    const mx = syntax.mb % (width / 16) * 4;
    const my = syntax.mb / (width / 16) * 4;
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
            const a = neighbor(motions[list], stride, mx, my, false);
            const top = neighbor(motions[list], stride, mx, my, true);
            const c = motion.available(motions[list], stride, @intCast(mx + 4), @as(i32, @intCast(my)) - 1) orelse motion.available(motions[list], stride, @as(i32, @intCast(mx)) - 1, @as(i32, @intCast(my)) - 1) orelse motion.Motion{};
            for ([_]motion.Motion{ a, top, c }) |m| if (m.decoded and m.reference >= 0 and (direct_refs[list] < 0 or m.reference < direct_refs[list])) {
                direct_refs[list] = m.reference;
            };
            if (direct_refs[list] >= 0) direct_predictions[list] = motion.predictor(motions[list], stride, mx, my, 4, 4, direct_refs[list], false);
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
        const left = neighbor(motions[list], stride, bx, by, false);
        const top = neighbor(motions[list], stride, bx, by, true);
        references[list][g] = if (skipped or (!b and kind == 4)) 0 else try syntax.reference(active[list], if (left.direct) -1 else left.reference, if (top.direct) -1 else top.reference);
        for (parts[0..count]) |p| if (p.group == g) {
            for (0..p.height) |row| for (0..p.width) |column| {
                motions[list][(my + p.y + row) * stride + mx + p.x + column].reference = references[list][g];
            };
        };
    };
    // Direct motion is inferred before parsing neighbouring MVD contexts.
    for (parts[0..count]) |part| if (part.direct) {
        try direct(state, motions, stride, mx, my, part, spatial, direct8, direct_refs, direct_predictions);
    };
    for (0..if (b) @as(usize, 2) else 1) |list| for (parts[0..count]) |part| {
        if (part.direct) continue;
        if (references[list][part.group] < 0) {
            fill(motions[list], stride, mx, my, part, .{ .decoded = true });
            continue;
        }
        const bx = mx + part.x;
        const by = my + part.y;
        const left = neighbor(motions[list], stride, bx, by, false);
        const top = neighbor(motions[list], stride, bx, by, true);
        const prediction = motion.predictor(motions[list], stride, bx, by, part.width, part.height, references[list][part.group], skipped);
        const dx = if (skipped) 0 else try syntax.mvd(0, @as(u32, if (left.reference >= 0) @abs(left.difference.x) else 0) + if (top.reference >= 0) @abs(top.difference.x) else 0);
        const dy = if (skipped) 0 else try syntax.mvd(1, @as(u32, if (left.reference >= 0) @abs(left.difference.y) else 0) + if (top.reference >= 0) @abs(top.difference.y) else 0);
        const mv = motion.Vector{ .x = prediction.x + dx, .y = prediction.y + dy };
        if (mv.x < -32768 or mv.x > 32767 or mv.y < -32768 or mv.y > 32767) return error.MalformedVideoPacket;
        fill(motions[list], stride, mx, my, part, .{ .decoded = true, .reference = references[list][part.group], .vector = mv, .difference = .{ .x = dx, .y = dy } });
    };
    if (!b) for (parts[0..count]) |part| {
        fill(motions[1], stride, mx, my, part, .{ .decoded = true });
    };
    for (parts[0..count]) |part| try compensate(state, planes, motions, width, height, mx, my, part);
    return allow8;
}
