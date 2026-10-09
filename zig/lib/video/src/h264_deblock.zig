// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Progressive luma/chroma filtering, H.264 clause 8.7, using residual,
//! intra and reference-identity/motion boundary strengths for I/P/B pictures.
const std = @import("std");
const media = @import("antfly_media");
const alpha = [_]i32{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 4, 5, 6, 7, 8, 9, 10, 12, 13, 15, 17, 20, 22, 25, 28, 32, 36, 40, 45, 50, 56, 63, 71, 80, 90, 101, 113, 127, 144, 162, 182, 203, 226, 255, 255 };
const beta = [_]i32{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 2, 2, 3, 3, 3, 3, 4, 4, 4, 6, 6, 7, 7, 8, 8, 9, 9, 10, 10, 11, 11, 12, 12, 13, 13, 14, 14, 15, 15, 16, 16, 17, 17, 18, 18 };
const tc1 = [_]i32{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 4, 4, 4, 5, 6, 6, 7, 8, 9, 10, 11, 13 };
const tc2 = [_]i32{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 4, 4, 5, 5, 6, 7, 8, 8, 10, 11, 12, 13, 15, 17 };
const tc3 = [_]i32{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 4, 4, 4, 5, 6, 6, 7, 8, 9, 10, 11, 13, 14, 16, 18, 20, 23, 25 };
fn byte(v: i32) u8 {
    return @intCast(std.math.clamp(v, 0, 255));
}
fn edge(plane: []u8, q: usize, step: usize, strong: bool, chroma: bool, a: i32, b: i32, tc: i32) void {
    const p0: i32 = plane[q - step];
    const p1: i32 = plane[q - 2 * step];
    const q0: i32 = plane[q];
    const q1: i32 = plane[q + step];
    if (@abs(p0 - q0) >= a or @abs(p1 - p0) >= b or @abs(q1 - q0) >= b) return;
    if (strong) {
        if (chroma) {
            plane[q - step] = byte((2 * p1 + p0 + q1 + 2) >> 2);
            plane[q] = byte((2 * q1 + q0 + p1 + 2) >> 2);
            return;
        }
        const p2: i32 = plane[q - 3 * step];
        const q2: i32 = plane[q + 2 * step];
        const small = @abs(p0 - q0) < (a >> 2) + 2;
        if (small and @abs(p2 - p0) < b) {
            const p3: i32 = plane[q - 4 * step];
            plane[q - step] = byte((p2 + 2 * p1 + 2 * p0 + 2 * q0 + q1 + 4) >> 3);
            plane[q - 2 * step] = byte((p2 + p1 + p0 + q0 + 2) >> 2);
            plane[q - 3 * step] = byte((2 * p3 + 3 * p2 + p1 + p0 + q0 + 4) >> 3);
        } else plane[q - step] = byte((2 * p1 + p0 + q1 + 2) >> 2);
        if (small and @abs(q2 - q0) < b) {
            const q3: i32 = plane[q + 3 * step];
            plane[q] = byte((q2 + 2 * q1 + 2 * q0 + 2 * p0 + p1 + 4) >> 3);
            plane[q + step] = byte((q2 + q1 + q0 + p0 + 2) >> 2);
            plane[q + 2 * step] = byte((2 * q3 + 3 * q2 + q1 + q0 + p0 + 4) >> 3);
        } else plane[q] = byte((2 * q1 + q0 + p1 + 2) >> 2);
        return;
    }
    var clipping = tc + @as(i32, if (chroma) 1 else 0);
    if (!chroma) {
        const p2: i32 = plane[q - 3 * step];
        const q2: i32 = plane[q + 2 * step];
        const average = (p0 + q0 + 1) >> 1;
        if (@abs(p2 - p0) < b) {
            clipping += 1;
            plane[q - 2 * step] = byte(p1 + std.math.clamp((p2 + average - 2 * p1) >> 1, -tc, tc));
        }
        if (@abs(q2 - q0) < b) {
            clipping += 1;
            plane[q + step] = byte(q1 + std.math.clamp((q2 + average - 2 * q1) >> 1, -tc, tc));
        }
    }
    const delta = std.math.clamp(((q0 - p0) * 4 + p1 - q1 + 4) >> 3, -clipping, clipping);
    plane[q - step] = byte(p0 + delta);
    plane[q] = byte(q0 - delta);
}
fn chromaQp(qp: u8, offset: i32) i32 {
    const index: usize = @intCast(std.math.clamp(@as(i32, qp) + offset, 0, 51));
    return if (index < 30) @intCast(index) else ([_]i32{ 29, 30, 31, 32, 32, 33, 34, 34, 35, 35, 36, 36, 37, 37, 37, 38, 38, 38, 39, 39, 39, 39 })[index - 30];
}
pub fn picture(planes: [3][]u8, width: usize, qps: []const u8, meta: []const @import("h264_entropy.zig").Meta, counts: []const u8, motions: [2][]@import("h264_motion.zig").Motion, references: *@import("h264_references.zig").State, chroma_offset: i32, alpha_offset: i32, beta_offset: i32, control: media.source.Control) !void {
    const mb_width = width / 16;
    for (qps, 0..) |qp, mb| {
        try control.check();
        for (0..2) |direction| for (0..3) |p| {
            const chroma = p != 0;
            const size: usize = if (chroma) 8 else 16;
            const stride = if (chroma) width / 2 else width;
            const x = mb % mb_width * size;
            const y = mb / mb_width * size;
            var e: usize = 0;
            while (e < size) : (e += 4) {
                if (!chroma and meta[mb].transform8 and e % 8 != 0) continue;
                if (e == 0 and (if (direction == 0) x == 0 else y == 0)) continue;
                const neighbor = if (e != 0) mb else if (direction == 0) mb - 1 else mb - mb_width;
                const current_q: i32 = if (chroma) chromaQp(qp, chroma_offset) else qp;
                const adjacent_q: i32 = if (chroma) chromaQp(qps[neighbor], chroma_offset) else qps[neighbor];
                const average = (current_q + adjacent_q + 1) >> 1;
                const ai: usize = @intCast(std.math.clamp(average + alpha_offset, 0, 51));
                const bi: usize = @intCast(std.math.clamp(average + beta_offset, 0, 51));
                for (0..size) |i| {
                    const q = if (direction == 0) (y + i) * stride + x + e else (y + e) * stride + x + i;
                    const block_stride = width / 4;
                    const bx = mb % mb_width * 4 + (if (direction == 0) e / (if (chroma) @as(usize, 2) else 4) else i / (if (chroma) @as(usize, 2) else 4));
                    const by = mb / mb_width * 4 + (if (direction == 0) i / (if (chroma) @as(usize, 2) else 4) else e / (if (chroma) @as(usize, 2) else 4));
                    const qb = by * block_stride + bx;
                    const pb = qb - (if (direction == 0) @as(usize, 1) else block_stride);
                    const intra = meta[mb].kind <= 25 or meta[neighbor].kind <= 25;

                    const strength: usize = if (intra) (if (e == 0) @as(usize, 4) else 3) else if (counts[pb] != 0 or counts[qb] != 0) 2 else if (different(motions, references, pb, qb)) 1 else 0;
                    if (strength == 0) continue;
                    const clipping = if (strength == 1) tc1[ai] else if (strength == 2) tc2[ai] else tc3[ai];
                    edge(planes[p], q, if (direction == 0) 1 else stride, strength == 4, chroma, alpha[ai], beta[bi], clipping);
                }
            }
        };
    }
}

fn different(motions: [2][]@import("h264_motion.zig").Motion, state: *@import("h264_references.zig").State, pb: usize, qb: usize) bool {
    var ids: [2][2]u32 = @splat(@splat(std.math.maxInt(u32)));
    for (0..2) |side| for (0..2) |list| {
        const m = motions[list][if (side == 0) pb else qb];
        if (m.reference >= 0 and m.reference < state.list_count) {
            const index = (if (list == 0) state.list0 else state.list1)[@intCast(m.reference)];
            if (index < state.count) ids[side][list] = state.pictures[index].id;
        }
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
                if (@abs(p.x - q.x) >= 4 or @abs(p.y - q.y) >= 4) {
                    same = false;
                    break;
                }
            }
        }
        if (same) return false;
    }
    return true;
}
