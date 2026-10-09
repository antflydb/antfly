// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Progressive luma/chroma filtering, H.264 clause 8.7, using residual,
//! intra and reference-identity/motion boundary strengths for I/P/B pictures.
const std = @import("std");
const layout = @import("h264_layout.zig");
const media = @import("antfly_media");
const alpha = [_]i32{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 4, 5, 6, 7, 8, 9, 10, 12, 13, 15, 17, 20, 22, 25, 28, 32, 36, 40, 45, 50, 56, 63, 71, 80, 90, 101, 113, 127, 144, 162, 182, 203, 226, 255, 255 };
const beta = [_]i32{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 2, 2, 3, 3, 3, 3, 4, 4, 4, 6, 6, 7, 7, 8, 8, 9, 9, 10, 10, 11, 11, 12, 12, 13, 13, 14, 14, 15, 15, 16, 16, 17, 17, 18, 18 };
const tc1 = [_]i32{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 4, 4, 4, 5, 6, 6, 7, 8, 9, 10, 11, 13 };
const tc2 = [_]i32{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 4, 4, 5, 5, 6, 7, 8, 8, 10, 11, 12, 13, 15, 17 };
const tc3 = [_]i32{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 4, 4, 4, 5, 6, 6, 7, 8, 9, 10, 11, 13, 14, 16, 18, 20, 23, 25 };
fn byte(comptime Sample: type, v: i32, bit_depth: u8) Sample {
    return @intCast(std.math.clamp(v, 0, (@as(i32, 1) << @as(u5, @intCast(bit_depth))) - 1));
}
fn edge(plane: anytype, q: usize, step: usize, strong: bool, chroma: bool, a: i32, b: i32, tc: i32, bit_depth: u8) void {
    const Sample = layout.Sample(@TypeOf(plane));

    const p0: i32 = layout.get(plane, q - step);
    const p1: i32 = layout.get(plane, q - 2 * step);
    const q0: i32 = layout.get(plane, q);
    const q1: i32 = layout.get(plane, q + step);
    if (@abs(p0 - q0) >= a or @abs(p1 - p0) >= b or @abs(q1 - q0) >= b) return;
    if (strong) {
        if (chroma) {
            layout.put(plane, q - step, byte(Sample, (2 * p1 + p0 + q1 + 2) >> 2, bit_depth));
            layout.put(plane, q, byte(Sample, (2 * q1 + q0 + p1 + 2) >> 2, bit_depth));
            return;
        }
        const p2: i32 = layout.get(plane, q - 3 * step);
        const q2: i32 = layout.get(plane, q + 2 * step);
        const small = @abs(p0 - q0) < (a >> 2) + 2;
        if (small and @abs(p2 - p0) < b) {
            const p3: i32 = layout.get(plane, q - 4 * step);
            layout.put(plane, q - step, byte(Sample, (p2 + 2 * p1 + 2 * p0 + 2 * q0 + q1 + 4) >> 3, bit_depth));
            layout.put(plane, q - 2 * step, byte(Sample, (p2 + p1 + p0 + q0 + 2) >> 2, bit_depth));
            layout.put(plane, q - 3 * step, byte(Sample, (2 * p3 + 3 * p2 + p1 + p0 + q0 + 4) >> 3, bit_depth));
        } else layout.put(plane, q - step, byte(Sample, (2 * p1 + p0 + q1 + 2) >> 2, bit_depth));
        if (small and @abs(q2 - q0) < b) {
            const q3: i32 = layout.get(plane, q + 3 * step);
            layout.put(plane, q, byte(Sample, (q2 + 2 * q1 + 2 * q0 + 2 * p0 + p1 + 4) >> 3, bit_depth));
            layout.put(plane, q + step, byte(Sample, (q2 + q1 + q0 + p0 + 2) >> 2, bit_depth));
            layout.put(plane, q + 2 * step, byte(Sample, (2 * q3 + 3 * q2 + q1 + q0 + p0 + 4) >> 3, bit_depth));
        } else layout.put(plane, q, byte(Sample, (2 * q1 + q0 + p1 + 2) >> 2, bit_depth));
        return;
    }
    var clipping = tc + @as(i32, if (chroma) 1 else 0);
    if (!chroma) {
        const p2: i32 = layout.get(plane, q - 3 * step);
        const q2: i32 = layout.get(plane, q + 2 * step);
        const average = (p0 + q0 + 1) >> 1;
        if (@abs(p2 - p0) < b) {
            clipping += 1;
            layout.put(plane, q - 2 * step, byte(Sample, p1 + std.math.clamp((p2 + average - 2 * p1) >> 1, -tc, tc), bit_depth));
        }
        if (@abs(q2 - q0) < b) {
            clipping += 1;
            layout.put(plane, q + step, byte(Sample, q1 + std.math.clamp((q2 + average - 2 * q1) >> 1, -tc, tc), bit_depth));
        }
    }
    const delta = std.math.clamp(((q0 - p0) * 4 + p1 - q1 + 4) >> 3, -clipping, clipping);
    layout.put(plane, q - step, byte(Sample, p0 + delta, bit_depth));
    layout.put(plane, q, byte(Sample, q0 - delta, bit_depth));
}
fn chromaQp(qp: i32, offset: i32, depth_offset: i32) i32 {
    const index = std.math.clamp(qp + offset, -depth_offset, 51);
    return if (index < 30) index else ([_]i32{ 29, 30, 31, 32, 32, 33, 34, 34, 35, 35, 36, 36, 37, 37, 37, 38, 38, 38, 39, 39, 39, 39 })[@as(usize, @intCast(index - 30))];
}
pub fn picture(planes: anytype, width: usize, qps: []const u8, meta: []const @import("h264_entropy.zig").Meta, counts: [3][]u8, motions: [2][]@import("h264_motion.zig").Motion, chroma_offsets: [2]i32, bit_depth: u8, chroma_format: u8, paired: bool, control: media.source.Control) !void {
    const Sample = std.meta.Child(@TypeOf(planes[0]));
    const mb_width = width / 16;
    for (0..qps.len) |address| {
        const mb = if (paired) layout.raster(address, mb_width) else address;
        const qp = qps[mb];
        const field = meta[mb].field;
        const parity = mb / mb_width % 2;
        try control.check();
        if (meta[mb].kind == 255 or meta[mb].filter == 1) continue;
        for (0..2) |direction| for (0..3) |p| {
            const chroma = p != 0 and chroma_format != 3;
            const sx: usize = if (chroma) 2 else 1;
            const sy: usize = if (chroma and chroma_format == 1) 2 else 1;
            const size_x = 16 / sx;
            const size_y = 16 / sy;
            const stride = width / sx;
            const x = mb % mb_width * size_x;
            const y = mb / mb_width / (if (field) @as(usize, 2) else 1) * size_y;
            const view = layout.View(Sample){ .data = planes[p], .width = stride, .height = planes[p].len / stride / (if (field) @as(usize, 2) else 1), .field = field, .parity = parity };
            const edge_size = if (direction == 0) size_x else size_y;
            const length = if (direction == 0) size_y else size_x;
            var e: usize = 0;
            while (e < edge_size) : (e += 4) {
                if (!chroma and meta[mb].transform8 and e % 8 != 0) continue;
                if (e == 0 and (if (direction == 0) x == 0 else y == 0)) continue;
                // A frame macroblock below a field pair has two top edges,
                // each filtered with a field stride and its own neighbour QP.
                const first_y = if (field) y * sy * 2 + parity else y * sy;
                const above = if (direction == 1 and e == 0) layout.physicalCell(meta, width, paired, x * sx, first_y - (if (field) @as(usize, 2) else 1)).mb else mb;
                const mixed_top = direction == 1 and e == 0 and !field and meta[above].field;
                for (0..if (mixed_top) @as(usize, 2) else 1) |edge_parity| for (0..length) |i| {
                    const qx = x * sx + (if (direction == 0) e * sx else i * sx);
                    const ly = y * sy + (if (direction == 0) i * sy else e * sy);
                    const qy = (if (field) ly * 2 + parity else ly) + edge_parity;
                    const step_y: usize = if (field or mixed_top) 2 else 1;
                    const qc = layout.physicalCell(meta, width, paired, qx, qy);
                    const pc = layout.physicalCell(meta, width, paired, qx - (if (direction == 0) @as(usize, 1) else 0), qy - (if (direction == 1) step_y else 0));
                    const neighbor = pc.mb;
                    if (e == 0 and meta[mb].filter == 2 and meta[mb].slice_id != meta[neighbor].slice_id) continue;
                    const offset: i32 = 6 * @as(i32, bit_depth - 8);
                    const chroma_offset = chroma_offsets[if (p == 2) 1 else 0];
                    const current_q: i32 = if (p != 0) chromaQp(@as(i32, qp) - offset, chroma_offset, offset) else @as(i32, qp) - offset;
                    const adjacent_q: i32 = if (p != 0) chromaQp(@as(i32, qps[neighbor]) - offset, chroma_offset, offset) else @as(i32, qps[neighbor]) - offset;
                    const average = (current_q + adjacent_q + 1) >> 1;
                    const ai: usize = @intCast(std.math.clamp(average + meta[mb].alpha, 0, 51));
                    const bi: usize = @intCast(std.math.clamp(average + meta[mb].beta, 0, 51));
                    const intra = meta[mb].kind <= 25 or meta[neighbor].kind <= 25;
                    const strength: usize = if (intra) (if (e == 0 and (direction == 0 or (!field and !meta[neighbor].field))) @as(usize, 4) else 3) else if (counts[0][pc.index] != 0 or counts[0][qc.index] != 0) 2 else if (field != meta[neighbor].field or different(motions, pc.index, qc.index, if (field) @as(u32, 2) else 4)) 1 else 0;
                    if (strength == 0) continue;
                    const clipping = if (strength == 1) tc1[ai] else if (strength == 2) tc2[ai] else tc3[ai];
                    if (mixed_top) {
                        const physical_y = y + edge_parity;
                        const field_view = layout.View(Sample){ .data = planes[p], .width = stride, .height = planes[p].len / stride / 2, .field = true, .parity = physical_y % 2 };
                        edge(field_view, physical_y / 2 * stride + x + i, stride, strength == 4, chroma, alpha[ai] << @as(u5, @intCast(bit_depth - 8)), beta[bi] << @as(u5, @intCast(bit_depth - 8)), clipping << @as(u5, @intCast(bit_depth - 8)), bit_depth);
                    } else {
                        const q = if (direction == 0) (y + i) * stride + x + e else (y + e) * stride + x + i;
                        edge(view, q, if (direction == 0) 1 else stride, strength == 4, chroma, alpha[ai] << @as(u5, @intCast(bit_depth - 8)), beta[bi] << @as(u5, @intCast(bit_depth - 8)), clipping << @as(u5, @intCast(bit_depth - 8)), bit_depth);
                    }
                };
            }
        };
    }
}

fn different(motions: [2][]@import("h264_motion.zig").Motion, pb: usize, qb: usize, vertical_threshold: u32) bool {
    var ids: [2][2]u32 = @splat(@splat(std.math.maxInt(u32)));
    for (0..2) |side| for (0..2) |list| {
        const m = motions[list][if (side == 0) pb else qb];
        ids[side][list] = m.identity;
    };
    for (0..2) |swap| {
        var same = true;
        for (0..2) |list| {
            const other = list ^ swap;
            if (ids[0][list] != ids[1][other]) {
                same = false;
                break;
            }
            if (ids[0][list] != std.math.maxInt(u32)) {
                const p = motions[list][pb].vector;
                const q = motions[other][qb].vector;
                if (@abs(p.x - q.x) >= 4 or @abs(p.y - q.y) >= vertical_threshold) {
                    same = false;
                    break;
                }
            }
        }
        if (same) return false;
    }
    return true;
}

test "video H264 deblocking chroma QP retains negative high depth QPY" {
    try std.testing.expectEqual(@as(i32, 2), chromaQp(-4, 6, 24));
    try std.testing.expectEqual(@as(i32, -12), chromaQp(-10, -2, 12));
    try std.testing.expectEqual(@as(i32, 39), chromaQp(49, 12, 36));
}
