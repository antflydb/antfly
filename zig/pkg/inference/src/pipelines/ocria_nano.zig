// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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

//! OCRia-nano-1 (llamaindex/OCRia-nano-1): a line detector, a script router
//! and four CTC line recognizers (Latin, CJK, RTL, world scripts).
//!
//! The models are plain ONNX graphs run on the native importer. Everything
//! around them is ported from LiteParse's `ocr/ocria` module (Apache-2.0,
//! https://github.com/run-llama/liteparse), the reference the models were
//! qualified with: OpenCV-exact resizes and perspective crops, the
//! connected-component line boxes, collinear merging, column-gutter splits,
//! sideways-page retry, page-level script routing with the low-confidence
//! rescue, homoglyph folding and the junk filter. Faint-token recovery and
//! the adaptive second detection pass (both off or optional upstream) are not
//! ported, and RTL readings use a reduced bidi reorder (see visualToLogical).

const std = @import("std");
const backends = @import("../backends/backends.zig");
const image = @import("image.zig");
const Preparation = @import("preprocessing_memory.zig").Scope;
const InferenceExecutionControl = @import("../execution_control.zig").InferenceExecutionControl;

pub const model_type = "ocria_nano";

/// Script groups in router-output order. Group 0 (Latin) is required; the
/// others fall back to it when their recognizer is not installed.
pub const groups = [_][]const u8{ "latin", "cjk", "rtl", "world" };
pub const group_count = groups.len;

/// A line quad: top-left, top-right, bottom-right, bottom-left along the text.
pub const Quad = [4][2]f32;

pub const Recognizer = struct {
    session: backends.Session,
    /// Class `k + 1` decodes to `charset[k]`; class 0 is the CTC blank.
    charset: [][]u8,
    /// Exported in visual order: RTL readings are reordered to logical order.
    visual_rtl: bool = false,

    pub fn deinit(self: *Recognizer, allocator: std.mem.Allocator) void {
        freeCharset(allocator, self.charset);
        self.session.close();
    }
};

pub const Options = struct {
    det_long_side: usize = 1600,
    /// Probability threshold; `det.meta.json` `thresh` overrides it.
    det_thresh: f32 = 0.5,
    det_min_score: f32 = 0.6,
    /// Collinear merge gap in line heights; `det.meta.json` `merge` overrides it.
    det_merge: f32 = 0.6,
    gutter: bool = true,
    orient: bool = true,
    rescue: bool = true,
    junk: bool = true,
};

/// Sessions and sidecars beyond the detector. The detector session is owned
/// by the enclosing multistage pipeline.
pub const Stages = struct {
    router: backends.Session,
    recognizers: [group_count]?Recognizer,
    options: Options = .{},

    pub fn deinit(self: *Stages, allocator: std.mem.Allocator) void {
        for (&self.recognizers) |*maybe| if (maybe.*) |*rec| {
            rec.deinit(allocator);
            maybe.* = null;
        };
        self.router.close();
    }

    fn recognizer(self: *const Stages, group: usize) *const Recognizer {
        if (self.recognizers[group]) |*rec| return rec;
        return &self.recognizers[0].?;
    }
};

/// One recognized line in source-image pixels.
pub const Line = struct {
    text: []u8,
    confidence: f32,
    quad: Quad,
};

// ── Charsets and metadata sidecars ──────────────────────────────────

/// `rec_<group>.charset.json`: a JSON array of strings.
pub fn parseCharset(allocator: std.mem.Allocator, bytes: []const u8) ![][]u8 {
    const parsed = std.json.parseFromSlice(std.json.Value, allocator, bytes, .{}) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return error.InvalidCharset,
    };
    defer parsed.deinit();
    if (parsed.value != .array or parsed.value.array.items.len == 0) return error.InvalidCharset;
    const items = parsed.value.array.items;
    var out = try allocator.alloc([]u8, items.len);
    var filled: usize = 0;
    errdefer {
        for (out[0..filled]) |s| allocator.free(s);
        allocator.free(out);
    }
    for (items) |item| {
        if (item != .string) return error.InvalidCharset;
        out[filled] = try allocator.dupe(u8, item.string);
        filled += 1;
    }
    return out;
}

pub fn freeCharset(allocator: std.mem.Allocator, charset: [][]u8) void {
    for (charset) |s| allocator.free(s);
    allocator.free(charset);
}

pub const DetectorMeta = struct { thresh: ?f32 = null, merge: ?f32 = null };

/// `det.meta.json`: `{"thresh": .., "merge": ..}`, both optional.
pub fn parseDetectorMeta(allocator: std.mem.Allocator, bytes: []const u8) DetectorMeta {
    const parsed = std.json.parseFromSlice(std.json.Value, allocator, bytes, .{}) catch return .{};
    defer parsed.deinit();
    if (parsed.value != .object) return .{};
    return .{
        .thresh = jsonFloat(parsed.value.object.get("thresh")),
        .merge = jsonFloat(parsed.value.object.get("merge")),
    };
}

/// `rec_<group>.meta.json`: `{"visual_rtl": bool}`.
pub fn parseVisualRtl(allocator: std.mem.Allocator, bytes: []const u8) bool {
    const parsed = std.json.parseFromSlice(std.json.Value, allocator, bytes, .{}) catch return false;
    defer parsed.deinit();
    if (parsed.value != .object) return false;
    const value = parsed.value.object.get("visual_rtl") orelse return false;
    return value == .bool and value.bool;
}

fn jsonFloat(value: ?std.json.Value) ?f32 {
    const v = value orelse return null;
    return switch (v) {
        .float => |f| if (std.math.isFinite(f) and f > 0 and f < 1e6) @floatCast(f) else null,
        .integer => |i| if (i > 0 and i < 1_000_000) @floatFromInt(i) else null,
        else => null,
    };
}

// ── Rasters and OpenCV-exact image operations ───────────────────────

pub const Raster = struct {
    w: usize,
    h: usize,
    ch: usize,
    data: []u8,

    pub fn init(allocator: std.mem.Allocator, w: usize, h: usize, ch: usize) !Raster {
        const data = try allocator.alloc(u8, try std.math.mul(usize, try std.math.mul(usize, w, h), ch));
        @memset(data, 0);
        return .{ .w = w, .h = h, .ch = ch, .data = data };
    }

    pub fn deinit(self: Raster, allocator: std.mem.Allocator) void {
        allocator.free(self.data);
    }

    inline fn at(self: Raster, x: usize, y: usize, c: usize) u8 {
        return self.data[(y * self.w + x) * self.ch + c];
    }
};

/// numpy/OpenCV round half to even.
fn roundEven(v: f64) f64 {
    const r = @round(v);
    if (@abs(v - @trunc(v)) == 0.5) return 2.0 * @round(v / 2.0);
    return r;
}

fn cvRound(v: f64) i64 {
    return @intFromFloat(roundEven(v));
}

fn satShort(v: f32) i32 {
    return @intCast(std.math.clamp(cvRound(v), std.math.minInt(i16), std.math.maxInt(i16)));
}

/// `cv2.cvtColor(.., COLOR_RGB2GRAY)`: 14-bit fixed-point BT.601 weights.
pub fn toGray(allocator: std.mem.Allocator, img: Raster) !Raster {
    const out = try Raster.init(allocator, img.w, img.h, 1);
    if (img.ch == 1) {
        @memcpy(out.data, img.data);
        return out;
    }
    for (out.data, 0..) |*o, i| {
        const p = img.data[i * img.ch ..][0..3];
        const v: u32 = @as(u32, p[0]) * 4899 + @as(u32, p[1]) * 9617 + @as(u32, p[2]) * 1868 + (1 << 13);
        o.* = @intCast(v >> 14);
    }
    return out;
}

/// `img.mean(axis=2).astype(np.uint8)`.
fn meanGray(allocator: std.mem.Allocator, img: Raster) !Raster {
    const out = try Raster.init(allocator, img.w, img.h, 1);
    for (out.data, 0..) |*o, i| {
        var sum: u32 = 0;
        for (img.data[i * img.ch ..][0..img.ch]) |v| sum += v;
        o.* = @intCast(sum / @as(u32, @intCast(img.ch)));
    }
    return out;
}

/// `np.rot90(img, k)`: k quarter turns counter-clockwise.
pub fn rot90(allocator: std.mem.Allocator, img: Raster, k: u32) !Raster {
    switch (k % 4) {
        0 => {
            const out = try Raster.init(allocator, img.w, img.h, img.ch);
            @memcpy(out.data, img.data);
            return out;
        },
        1 => {
            var out = try Raster.init(allocator, img.h, img.w, img.ch);
            for (0..out.h) |i| for (0..out.w) |j| {
                const sx = img.w - 1 - i;
                const sy = j;
                for (0..img.ch) |c| out.data[(i * out.w + j) * img.ch + c] = img.at(sx, sy, c);
            };
            return out;
        },
        2 => {
            var out = try Raster.init(allocator, img.w, img.h, img.ch);
            for (0..img.h) |y| for (0..img.w) |x| {
                for (0..img.ch) |c| out.data[(y * img.w + x) * img.ch + c] = img.at(img.w - 1 - x, img.h - 1 - y, c);
            };
            return out;
        },
        else => {
            const once = try rot90(allocator, img, 1);
            defer once.deinit(allocator);
            return rot90(allocator, once, 2);
        },
    }
}

const resize_bits = 11;
const resize_one: i32 = 1 << resize_bits;

const LinearTap = struct { sx: usize, a0: i32, a1: i32 };

fn linearTaps(allocator: std.mem.Allocator, src: usize, dst: usize, area: bool) ![]LinearTap {
    const taps = try allocator.alloc(LinearTap, dst);
    const inv_scale = @as(f64, @floatFromInt(dst)) / @as(f64, @floatFromInt(src));
    const scale = 1.0 / inv_scale;
    for (taps, 0..) |*tap, d| {
        const df: f64 = @floatFromInt(d);
        var sx: i64 = undefined;
        var f: f32 = undefined;
        if (area) {
            sx = @intFromFloat(@floor(df * scale));
            const ff: f32 = @floatCast((df + 1.0) - @as(f64, @floatFromInt(sx + 1)) * inv_scale);
            f = if (ff <= 0) 0 else ff - @floor(ff);
        } else {
            const fx: f32 = @floatCast((df + 0.5) * scale - 0.5);
            const fl = @floor(fx);
            sx = @intFromFloat(fl);
            f = fx - fl;
        }
        if (sx < 0) {
            sx = 0;
            f = 0;
        }
        if (sx >= @as(i64, @intCast(src)) - 1) {
            sx = @as(i64, @intCast(src)) - 1;
            f = 0;
        }
        tap.* = .{
            .sx = @intCast(sx),
            .a0 = satShort((1.0 - f) * @as(f32, @floatFromInt(resize_one))),
            .a1 = satShort(f * @as(f32, @floatFromInt(resize_one))),
        };
    }
    return taps;
}

fn resizeLinearTaps(allocator: std.mem.Allocator, img: Raster, w: usize, h: usize, area: bool) !Raster {
    const xt = try linearTaps(allocator, img.w, w, area);
    defer allocator.free(xt);
    const yt = try linearTaps(allocator, img.h, h, area);
    defer allocator.free(yt);
    const ch = img.ch;
    const row_len = w * ch;
    // Only the source rows the vertical taps read need a horizontal pass.
    const rows = try allocator.alloc(i32, img.h * row_len);
    defer allocator.free(rows);
    const done = try allocator.alloc(bool, img.h);
    defer allocator.free(done);
    @memset(done, false);
    var out = try Raster.init(allocator, w, h, ch);
    errdefer out.deinit(allocator);
    for (yt, 0..) |ytap, dy| {
        const sy0 = ytap.sx;
        const sy1 = @min(ytap.sx + 1, img.h - 1);
        for ([_]usize{ sy0, sy1 }) |sy| {
            if (done[sy]) continue;
            done[sy] = true;
            const r = rows[sy * row_len ..][0..row_len];
            for (xt, 0..) |tap, dx| for (0..ch) |c| {
                const s0: i32 = img.at(tap.sx, sy, c);
                r[dx * ch + c] = if (tap.sx + 1 < img.w)
                    s0 * tap.a0 + @as(i32, img.at(tap.sx + 1, sy, c)) * tap.a1
                else
                    s0 * resize_one;
            };
        }
        const r0 = rows[sy0 * row_len ..][0..row_len];
        const r1 = rows[sy1 * row_len ..][0..row_len];
        const o = out.data[dy * row_len ..][0..row_len];
        for (o, 0..) |*px, i| {
            const v = (@as(i64, r0[i]) * ytap.a0 + @as(i64, r1[i]) * ytap.a1 + (1 << 21)) >> 22;
            px.* = @intCast(std.math.clamp(v, 0, 255));
        }
    }
    return out;
}

/// `cv2.resize(.., interpolation=INTER_LINEAR)`.
pub fn resizeLinear(allocator: std.mem.Allocator, img: Raster, w: usize, h: usize) !Raster {
    if (w == img.w and h == img.h) return rot90(allocator, img, 0);
    return resizeLinearTaps(allocator, img, w, h, false);
}

const AreaTap = struct { d: usize, s: usize, alpha: f32 };

/// OpenCV `computeResizeAreaTab`, flattened in output order.
fn areaTab(allocator: std.mem.Allocator, src: usize, dst: usize, scale: f64) ![]AreaTap {
    var taps = std.ArrayListUnmanaged(AreaTap).empty;
    errdefer taps.deinit(allocator);
    const srcf: f64 = @floatFromInt(src);
    for (0..dst) |d| {
        const fsx1 = @as(f64, @floatFromInt(d)) * scale;
        const fsx2 = fsx1 + scale;
        const cell = @min(scale, srcf - fsx1);
        var sx2: i64 = @intFromFloat(@floor(fsx2));
        sx2 = @min(sx2, @as(i64, @intCast(src)) - 1);
        const sx1: i64 = @min(@as(i64, @intFromFloat(@ceil(fsx1))), sx2);
        if (@as(f64, @floatFromInt(sx1)) - fsx1 > 1e-3)
            try taps.append(allocator, .{ .d = d, .s = @intCast(sx1 - 1), .alpha = @floatCast((@as(f64, @floatFromInt(sx1)) - fsx1) / cell) });
        var sx = sx1;
        while (sx < sx2) : (sx += 1)
            try taps.append(allocator, .{ .d = d, .s = @intCast(sx), .alpha = @floatCast(1.0 / cell) });
        const tail = fsx2 - @as(f64, @floatFromInt(sx2));
        if (tail > 1e-3)
            try taps.append(allocator, .{ .d = d, .s = @intCast(sx2), .alpha = @floatCast(@min(@min(tail, 1.0), cell) / cell) });
    }
    return taps.toOwnedSlice(allocator);
}

/// `cv2.resize(.., interpolation=INTER_AREA)`.
pub fn resizeArea(allocator: std.mem.Allocator, img: Raster, w: usize, h: usize) !Raster {
    if (w == img.w and h == img.h) return rot90(allocator, img, 0);
    const scale_x = @as(f64, @floatFromInt(img.w)) / @as(f64, @floatFromInt(w));
    const scale_y = @as(f64, @floatFromInt(img.h)) / @as(f64, @floatFromInt(h));
    if (scale_x < 1.0 or scale_y < 1.0) return resizeLinearTaps(allocator, img, w, h, true);
    const ch = img.ch;
    const isx = @round(scale_x);
    const isy = @round(scale_y);
    var out = try Raster.init(allocator, w, h, ch);
    errdefer out.deinit(allocator);
    if (@abs(scale_x - isx) < std.math.floatEps(f64) and @abs(scale_y - isy) < std.math.floatEps(f64)) {
        const kx: usize = @intFromFloat(isx);
        const ky: usize = @intFromFloat(isy);
        const area: u32 = @intCast(kx * ky);
        const inv_area: f32 = 1.0 / @as(f32, @floatFromInt(area));
        for (0..h) |dy| for (0..w) |dx| for (0..ch) |c| {
            var s: u32 = 0;
            for (0..ky) |yy| for (0..kx) |xx| {
                s += img.at(dx * kx + xx, dy * ky + yy, c);
            };
            const v: u32 = if (area == 4) (s + 2) >> 2 else @intCast(cvRound(@as(f64, @floatFromInt(s)) * @as(f64, inv_area)));
            out.data[(dy * w + dx) * ch + c] = @intCast(@min(v, 255));
        };
        return out;
    }
    const xt = try areaTab(allocator, img.w, w, scale_x);
    defer allocator.free(xt);
    const yt = try areaTab(allocator, img.h, h, scale_y);
    defer allocator.free(yt);
    const row_len = img.w * ch;
    const buf = try allocator.alloc(f32, w * ch);
    defer allocator.free(buf);
    const sum = try allocator.alloc(f32, w * ch);
    defer allocator.free(sum);
    var buf_row: usize = std.math.maxInt(usize);
    var yi: usize = 0;
    for (0..h) |dy| {
        @memset(sum, 0);
        while (yi < yt.len and yt[yi].d == dy) : (yi += 1) {
            const sy = yt[yi].s;
            const beta = yt[yi].alpha;
            if (buf_row != sy) {
                @memset(buf, 0);
                const row = img.data[sy * row_len ..][0..row_len];
                for (xt) |tap| for (0..ch) |c| {
                    buf[tap.d * ch + c] += @as(f32, @floatFromInt(row[tap.s * ch + c])) * tap.alpha;
                };
                buf_row = sy;
            }
            for (sum, buf) |*s, b| s.* += b * beta;
        }
        for (out.data[dy * w * ch ..][0 .. w * ch], sum) |*o, s| o.* = @intCast(std.math.clamp(cvRound(s), 0, 255));
    }
    return out;
}

/// `cv2.getPerspectiveTransform(src, dst)`.
fn perspectiveTransform(src: Quad, dst: Quad) [9]f64 {
    var a: [8][9]f64 = undefined;
    for (0..4) |i| {
        const x: f64 = src[i][0];
        const y: f64 = src[i][1];
        const u: f64 = dst[i][0];
        const v: f64 = dst[i][1];
        a[i] = .{ x, y, 1, 0, 0, 0, -x * u, -y * u, u };
        a[i + 4] = .{ 0, 0, 0, x, y, 1, -x * v, -y * v, v };
    }
    for (0..8) |col| {
        var piv = col;
        for (col + 1..8) |r| if (@abs(a[r][col]) > @abs(a[piv][col])) {
            piv = r;
        };
        std.mem.swap([9]f64, &a[col], &a[piv]);
        const d = a[col][col];
        if (@abs(d) < 1e-300) continue;
        for (0..8) |r| {
            if (r == col) continue;
            const f = a[r][col] / d;
            if (f != 0) for (col..9) |k| {
                a[r][k] -= f * a[col][k];
            };
        }
    }
    var m: [9]f64 = undefined;
    for (0..8) |i| m[i] = if (a[i][i] != 0) a[i][8] / a[i][i] else 0;
    m[8] = 1;
    return m;
}

fn invert3(m: [9]f64) [9]f64 {
    const det = m[0] * (m[4] * m[8] - m[5] * m[7]) - m[1] * (m[3] * m[8] - m[5] * m[6]) + m[2] * (m[3] * m[7] - m[4] * m[6]);
    if (det == 0) return @splat(0);
    const d = 1.0 / det;
    return .{
        (m[4] * m[8] - m[5] * m[7]) * d,
        (m[2] * m[7] - m[1] * m[8]) * d,
        (m[1] * m[5] - m[2] * m[4]) * d,
        (m[5] * m[6] - m[3] * m[8]) * d,
        (m[0] * m[8] - m[2] * m[6]) * d,
        (m[2] * m[3] - m[0] * m[5]) * d,
        (m[3] * m[7] - m[4] * m[6]) * d,
        (m[1] * m[6] - m[0] * m[7]) * d,
        (m[0] * m[4] - m[1] * m[3]) * d,
    };
}

const inter_bits = 5;
const inter_tab: i64 = 1 << inter_bits;
const remap_scale: i32 = 1 << 15;

fn cubicCoeffs(x: f32) [4]f32 {
    const A: f32 = -0.75;
    const c0 = ((A * (x + 1) - 5 * A) * (x + 1) + 8 * A) * (x + 1) - 4 * A;
    const c1 = ((A + 2) * x - (A + 3)) * x * x + 1;
    const c2 = ((A + 2) * (1 - x) - (A + 3)) * (1 - x) * (1 - x) + 1;
    return .{ c0, c1, c2, 1 - c0 - c1 - c2 };
}

/// OpenCV `initInterTab2D(INTER_CUBIC)` fixed-point weights.
pub const BicubicTable = struct {
    entries: [inter_tab * inter_tab][16]i32,

    pub fn init(self: *BicubicTable) void {
        var one_d: [inter_tab][4]f32 = undefined;
        for (&one_d, 0..) |*c, i| c.* = cubicCoeffs(@as(f32, @floatFromInt(i)) / @as(f32, @floatFromInt(inter_tab)));
        for (0..inter_tab) |iy| for (0..inter_tab) |ix| {
            var t: [16]i32 = undefined;
            var isum: i32 = 0;
            for (0..4) |i| for (0..4) |j| {
                t[i * 4 + j] = satShort(one_d[iy][i] * one_d[ix][j] * @as(f32, @floatFromInt(remap_scale)));
                isum += t[i * 4 + j];
            };
            if (isum != remap_scale) {
                const diff = isum - remap_scale;
                var mk = [2]usize{ 2, 2 };
                var big = [2]usize{ 2, 2 };
                for (2..4) |k1| for (2..4) |k2| {
                    if (t[k1 * 4 + k2] < t[mk[0] * 4 + mk[1]]) {
                        mk = .{ k1, k2 };
                    } else if (t[k1 * 4 + k2] > t[big[0] * 4 + big[1]]) {
                        big = .{ k1, k2 };
                    }
                };
                if (diff < 0) t[big[0] * 4 + big[1]] -= diff else t[mk[0] * 4 + mk[1]] -= diff;
            }
            self.entries[iy * inter_tab + ix] = t;
        };
    }
};

/// `cv2.warpPerspective(img, m, (w, h), flags=INTER_CUBIC, borderMode=BORDER_REPLICATE)`.
fn warpPerspectiveCubic(allocator: std.mem.Allocator, img: Raster, m: [9]f64, w: usize, h: usize, tab: *const BicubicTable) !Raster {
    const inv = invert3(m);
    const ch = img.ch;
    var out = try Raster.init(allocator, w, h, ch);
    const sw: i64 = @intCast(img.w);
    const sh: i64 = @intCast(img.h);
    for (0..h) |y| for (0..w) |x| {
        const xf: f64 = @floatFromInt(x);
        const yf: f64 = @floatFromInt(y);
        const x0 = inv[0] * xf + inv[1] * yf + inv[2];
        const y0 = inv[3] * xf + inv[4] * yf + inv[5];
        var ww = inv[6] * xf + inv[7] * yf + inv[8];
        ww = if (ww != 0) @as(f64, @floatFromInt(inter_tab)) / ww else 0;
        const lo: f64 = @floatFromInt(std.math.minInt(i32));
        const hi: f64 = @floatFromInt(std.math.maxInt(i32));
        const fx = std.math.clamp(x0 * ww, lo, hi);
        const fy = std.math.clamp(y0 * ww, lo, hi);
        const xi = cvRound(fx);
        const yi = cvRound(fy);
        const sx = xi >> inter_bits;
        const sy = yi >> inter_bits;
        const a: usize = @intCast((yi & (inter_tab - 1)) * inter_tab + (xi & (inter_tab - 1)));
        const wt = &tab.entries[a];
        var cols: [4]usize = undefined;
        for (&cols, 0..) |*col, j| col.* = @intCast(std.math.clamp(sx - 1 + @as(i64, @intCast(j)), 0, sw - 1));
        var sum: [4]i64 = @splat(0);
        for (0..4) |i| {
            const yy: usize = @intCast(std.math.clamp(sy - 1 + @as(i64, @intCast(i)), 0, sh - 1));
            const row = img.data[yy * img.w * ch ..];
            for (cols, 0..) |xx, j| {
                const wv: i64 = wt[i * 4 + j];
                for (0..ch) |c| sum[c] += @as(i64, row[xx * ch + c]) * wv;
            }
        }
        for (0..ch) |c| out.data[(y * w + x) * ch + c] = @intCast(std.math.clamp((sum[c] + (1 << 14)) >> 15, 0, 255));
    };
    return out;
}

fn dist(a: [2]f32, b: [2]f32) f32 {
    return @sqrt((a[0] - b[0]) * (a[0] - b[0]) + (a[1] - b[1]) * (a[1] - b[1]));
}

/// Perspective-crop a (possibly rotated) quad; tall boxes are turned level.
fn cropQuad(allocator: std.mem.Allocator, img: Raster, q: Quad, tab: *const BicubicTable) !Raster {
    const w: usize = @max(@as(usize, @intFromFloat(@max(dist(q[0], q[1]), dist(q[2], q[3])))), 2);
    const h: usize = @max(@as(usize, @intFromFloat(@max(dist(q[0], q[3]), dist(q[1], q[2])))), 2);
    const wf: f32 = @floatFromInt(w);
    const hf: f32 = @floatFromInt(h);
    const m = perspectiveTransform(q, .{ .{ 0, 0 }, .{ wf, 0 }, .{ wf, hf }, .{ 0, hf } });
    const c = try warpPerspectiveCubic(allocator, img, m, w, h, tab);
    if (hf / wf >= 1.5) {
        defer c.deinit(allocator);
        return rot90(allocator, c, 1);
    }
    return c;
}

// ── Connected components and rotated rectangles ─────────────────────

const Component = struct { x0: usize, y0: usize, x1: usize, y1: usize, start: usize, len: usize };

const ComponentSet = struct {
    comps: []Component,
    pixels: [][2]u32,

    fn deinit(self: ComponentSet, allocator: std.mem.Allocator) void {
        allocator.free(self.comps);
        allocator.free(self.pixels);
    }
};

/// 4-connected components, labelled in raster order of their first pixel.
fn components4(allocator: std.mem.Allocator, mask: []const bool, w: usize, h: usize) !ComponentSet {
    const seen = try allocator.alloc(bool, w * h);
    defer allocator.free(seen);
    @memset(seen, false);
    var comps = std.ArrayListUnmanaged(Component).empty;
    errdefer comps.deinit(allocator);
    var pixels = std.ArrayListUnmanaged([2]u32).empty;
    errdefer pixels.deinit(allocator);
    var stack = std.ArrayListUnmanaged(usize).empty;
    defer stack.deinit(allocator);
    for (0..w * h) |start| {
        if (!mask[start] or seen[start]) continue;
        seen[start] = true;
        try stack.append(allocator, start);
        var comp = Component{ .x0 = std.math.maxInt(usize), .y0 = std.math.maxInt(usize), .x1 = 0, .y1 = 0, .start = pixels.items.len, .len = 0 };
        while (stack.pop()) |p| {
            const x = p % w;
            const y = p / w;
            comp.x0 = @min(comp.x0, x);
            comp.y0 = @min(comp.y0, y);
            comp.x1 = @max(comp.x1, x);
            comp.y1 = @max(comp.y1, y);
            try pixels.append(allocator, .{ @intCast(x), @intCast(y) });
            const neighbours = [_]?usize{
                if (x > 0) p - 1 else null,
                if (x + 1 < w) p + 1 else null,
                if (y > 0) p - w else null,
                if (y + 1 < h) p + w else null,
            };
            for (neighbours) |maybe| if (maybe) |q| if (mask[q] and !seen[q]) {
                seen[q] = true;
                try stack.append(allocator, q);
            };
        }
        comp.len = pixels.items.len - comp.start;
        try comps.append(allocator, comp);
    }
    return .{ .comps = try comps.toOwnedSlice(allocator), .pixels = try pixels.toOwnedSlice(allocator) };
}

const RotRect = struct { cx: f32, cy: f32, w: f32, h: f32, angle: f32 };

fn cross(o: [2]f64, a: [2]f64, b: [2]f64) f64 {
    return (a[0] - o[0]) * (b[1] - o[1]) - (a[1] - o[1]) * (b[0] - o[0]);
}

/// `cv2.minAreaRect` over a convex hull (monotone chain).
fn minAreaRect(allocator: std.mem.Allocator, pts_in: []const [2]f32) !RotRect {
    const p = try allocator.alloc([2]f64, pts_in.len);
    defer allocator.free(p);
    for (p, pts_in) |*d, s| d.* = .{ s[0], s[1] };
    std.mem.sort([2]f64, p, {}, struct {
        fn lt(_: void, a: [2]f64, b: [2]f64) bool {
            return a[0] < b[0] or (a[0] == b[0] and a[1] < b[1]);
        }
    }.lt);
    var n: usize = 0;
    for (p) |q| {
        if (n > 0 and p[n - 1][0] == q[0] and p[n - 1][1] == q[1]) continue;
        p[n] = q;
        n += 1;
    }
    const pts = p[0..n];
    if (pts.len <= 1) return .{ .cx = if (n == 1) @floatCast(pts[0][0]) else 0, .cy = if (n == 1) @floatCast(pts[0][1]) else 0, .w = 0, .h = 0, .angle = 0 };
    var hull = try allocator.alloc([2]f64, pts.len * 2);
    defer allocator.free(hull);
    var k: usize = 0;
    for (pts) |q| {
        while (k >= 2 and cross(hull[k - 2], hull[k - 1], q) <= 0) k -= 1;
        hull[k] = q;
        k += 1;
    }
    const lower = k + 1;
    var i: usize = pts.len - 1;
    while (i > 0) {
        i -= 1;
        const q = pts[i];
        while (k >= lower and cross(hull[k - 2], hull[k - 1], q) <= 0) k -= 1;
        hull[k] = q;
        k += 1;
    }
    k -= 1;
    const hv = hull[0..k];
    var best_area: f64 = std.math.inf(f64);
    var best: ?RotRect = null;
    for (hv, 0..) |a, idx| {
        const b = hv[(idx + 1) % hv.len];
        const dx = b[0] - a[0];
        const dy = b[1] - a[1];
        const len = std.math.hypot(dx, dy);
        if (len == 0) continue;
        const ux = dx / len;
        const uy = dy / len;
        const vx = -uy;
        const vy = ux;
        var u_0 = std.math.inf(f64);
        var u_1 = -std.math.inf(f64);
        var v_0 = std.math.inf(f64);
        var v_1 = -std.math.inf(f64);
        for (hv) |q| {
            const pu = q[0] * ux + q[1] * uy;
            const pv = q[0] * vx + q[1] * vy;
            u_0 = @min(u_0, pu);
            u_1 = @max(u_1, pu);
            v_0 = @min(v_0, pv);
            v_1 = @max(v_1, pv);
        }
        const area = (u_1 - u_0) * (v_1 - v_0);
        if (best == null or area < best_area) {
            best_area = area;
            const cu = (u_0 + u_1) / 2;
            const cv = (v_0 + v_1) / 2;
            best = .{
                .cx = @floatCast(cu * ux + cv * vx),
                .cy = @floatCast(cu * uy + cv * vy),
                .w = @floatCast(u_1 - u_0),
                .h = @floatCast(v_1 - v_0),
                .angle = @floatCast(std.math.radiansToDegrees(std.math.atan2(uy, ux))),
            };
        }
    }
    return best orelse .{ .cx = @floatCast(pts[0][0]), .cy = @floatCast(pts[0][1]), .w = 0, .h = 0, .angle = 0 };
}

/// `cv2.boxPoints`.
fn boxPoints(r: RotRect) Quad {
    const a = std.math.degreesToRadians(@as(f64, r.angle));
    const b: f32 = @floatCast(@cos(a) * 0.5);
    const c: f32 = @floatCast(@sin(a) * 0.5);
    const p0 = [2]f32{ r.cx - c * r.h - b * r.w, r.cy + b * r.h - c * r.w };
    const p1 = [2]f32{ r.cx + c * r.h - b * r.w, r.cy - b * r.h - c * r.w };
    return .{ p0, p1, .{ 2 * r.cx - p0[0], 2 * r.cy - p0[1] }, .{ 2 * r.cx - p1[0], 2 * r.cy - p1[1] } };
}

fn norm(v: [2]f32) f32 {
    return @sqrt(v[0] * v[0] + v[1] * v[1]);
}

/// Corners as [top-left, top-right, bottom-right, bottom-left] along the long axis.
pub fn orderQuad(q: Quad) Quad {
    const c = [2]f32{ (q[0][0] + q[1][0] + q[2][0] + q[3][0]) / 4, (q[0][1] + q[1][1] + q[2][1] + q[3][1]) / 4 };
    const e1 = [2]f32{ q[1][0] - q[0][0], q[1][1] - q[0][1] };
    const e2 = [2]f32{ q[2][0] - q[1][0], q[2][1] - q[1][1] };
    var u = if (norm(e1) >= norm(e2)) e1 else e2;
    const n = norm(u) + 1e-6;
    u = .{ u[0] / n, u[1] / n };
    if (u[0] < 0) u = .{ -u[0], -u[1] };
    const v = [2]f32{ -u[1], u[0] };
    var a: [4]f32 = undefined;
    var b: [4]f32 = undefined;
    for (q, 0..) |p, i| {
        a[i] = (p[0] - c[0]) * u[0] + (p[1] - c[1]) * u[1];
        b[i] = (p[0] - c[0]) * v[0] + (p[1] - c[1]) * v[1];
    }
    var tl: usize = 0;
    var tr: usize = 0;
    var br: usize = 0;
    var bl: usize = 0;
    for (1..4) |i| {
        if (a[i] + b[i] < a[tl] + b[tl]) tl = i;
        if (a[i] - b[i] > a[tr] - b[tr]) tr = i;
        if (a[i] + b[i] > a[br] + b[br]) br = i;
        if (a[i] - b[i] < a[bl] - b[bl]) bl = i;
    }
    return .{ q[tl], q[tr], q[br], q[bl] };
}

fn quadCenter(q: Quad) [2]f32 {
    return .{ (q[0][0] + q[1][0] + q[2][0] + q[3][0]) / 4, (q[0][1] + q[1][1] + q[2][1] + q[3][1]) / 4 };
}

/// Join near-horizontal line quads that continue one another.
pub fn mergeCollinear(allocator: std.mem.Allocator, boxes: []const Quad, gap: f32) ![]Quad {
    const max_tilt: f32 = 0.35;
    const max_perp: f32 = 0.35;
    const min_h_ratio: f32 = 0.6;
    const max_dangle: f32 = 0.07;
    const n = boxes.len;
    if (n < 2) return allocator.dupe(Quad, boxes);
    const c = try allocator.alloc([2]f32, n);
    defer allocator.free(c);
    const u = try allocator.alloc([2]f32, n);
    defer allocator.free(u);
    const v = try allocator.alloc([2]f32, n);
    defer allocator.free(v);
    const len = try allocator.alloc(f32, n);
    defer allocator.free(len);
    const hgt = try allocator.alloc(f32, n);
    defer allocator.free(hgt);
    const flat = try allocator.alloc(bool, n);
    defer allocator.free(flat);
    for (boxes, 0..) |q, i| {
        c[i] = quadCenter(q);
        const d = [2]f32{ q[1][0] - q[0][0], q[1][1] - q[0][1] };
        const l = @max(norm(d), 1e-3);
        u[i] = .{ d[0] / l, d[1] / l };
        v[i] = .{ -u[i][1], u[i][0] };
        len[i] = l;
        hgt[i] = @max(norm(.{ q[3][0] - q[0][0], q[3][1] - q[0][1] }), 1e-3);
        flat[i] = @abs(u[i][1]) <= max_tilt and len[i] >= 0.5 * hgt[i];
    }
    const cos_min = 1.0 - max_dangle * max_dangle / 2.0;
    const right = try allocator.alloc(isize, n);
    defer allocator.free(right);
    const left_gap = try allocator.alloc(f32, n);
    defer allocator.free(left_gap);
    const left_idx = try allocator.alloc(isize, n);
    defer allocator.free(left_idx);
    @memset(left_gap, std.math.inf(f32));
    @memset(left_idx, -1);
    for (0..n) |a| {
        var best_g = std.math.inf(f32);
        var best: isize = -1;
        if (flat[a]) for (0..n) |b| {
            if (a == b or !flat[b]) continue;
            const d = [2]f32{ c[b][0] - c[a][0], c[b][1] - c[a][1] };
            const along = d[0] * u[a][0] + d[1] * u[a][1];
            const perp = @abs(d[0] * v[a][0] + d[1] * v[a][1]);
            const hm = (hgt[a] + hgt[b]) / 2;
            const g = along - (len[a] + len[b]) / 2;
            const ok = along > 0 and
                @min(hgt[a], hgt[b]) / @max(hgt[a], hgt[b]) >= min_h_ratio and
                @abs(u[a][0] * u[b][0] + u[a][1] * u[b][1]) >= cos_min and
                perp <= max_perp * hm and
                g <= gap * hm and
                g >= -0.5 * @min(len[a], len[b]);
            if (ok) {
                if (g < best_g) {
                    best_g = g;
                    best = @intCast(b);
                }
                if (g < left_gap[b]) {
                    left_gap[b] = g;
                    left_idx[b] = @intCast(a);
                }
            }
        };
        right[a] = best;
    }
    const nxt = try allocator.alloc(isize, n);
    defer allocator.free(nxt);
    const has_prev = try allocator.alloc(bool, n);
    defer allocator.free(has_prev);
    const seen = try allocator.alloc(bool, n);
    defer allocator.free(seen);
    @memset(has_prev, false);
    @memset(seen, false);
    for (0..n) |a| {
        const b = right[a];
        nxt[a] = if (b >= 0 and left_idx[@intCast(b)] == @as(isize, @intCast(a))) b else -1;
    }
    for (nxt) |b| if (b >= 0) {
        has_prev[@intCast(b)] = true;
    };
    var out = std.ArrayListUnmanaged(Quad).empty;
    errdefer out.deinit(allocator);
    var chain = std.ArrayListUnmanaged(usize).empty;
    defer chain.deinit(allocator);
    for (0..n) |a| {
        if (has_prev[a] or seen[a]) continue;
        chain.clearRetainingCapacity();
        try chain.append(allocator, a);
        seen[a] = true;
        while (true) {
            const nx = nxt[chain.items[chain.items.len - 1]];
            if (nx < 0 or seen[@intCast(nx)]) break;
            try chain.append(allocator, @intCast(nx));
            seen[@intCast(nx)] = true;
        }
        if (chain.items.len == 1) {
            try out.append(allocator, boxes[a]);
            continue;
        }
        var k = chain.items[0];
        for (chain.items[1..]) |i| if (len[i] > len[k]) {
            k = i;
        };
        var ca = [2]f32{ 0, 0 };
        var np: f32 = 0;
        for (chain.items) |i| for (boxes[i]) |p| {
            ca[0] += p[0];
            ca[1] += p[1];
            np += 1;
        };
        ca = .{ ca[0] / np, ca[1] / np };
        const ua = u[k];
        const va = v[k];
        var a0 = std.math.inf(f32);
        var a1 = -std.math.inf(f32);
        var b0 = std.math.inf(f32);
        var b1 = -std.math.inf(f32);
        for (chain.items) |i| for (boxes[i]) |p| {
            const dx = p[0] - ca[0];
            const dy = p[1] - ca[1];
            const pa = dx * ua[0] + dy * ua[1];
            const pb = dx * va[0] + dy * va[1];
            a0 = @min(a0, pa);
            a1 = @max(a1, pa);
            b0 = @min(b0, pb);
            b1 = @max(b1, pb);
        };
        const at = struct {
            fn f(cc: [2]f32, uu: [2]f32, vv: [2]f32, s: f32, t: f32) [2]f32 {
                return .{ cc[0] + uu[0] * s + vv[0] * t, cc[1] + uu[1] * s + vv[1] * t };
            }
        }.f;
        try out.append(allocator, orderQuad(.{ at(ca, ua, va, a0, b0), at(ca, ua, va, a1, b0), at(ca, ua, va, a1, b1), at(ca, ua, va, a0, b1) }));
    }
    for (0..n) |i| if (!seen[i]) try out.append(allocator, boxes[i]);
    return out.toOwnedSlice(allocator);
}

fn minY(q: Quad) f32 {
    return @min(@min(q[0][1], q[1][1]), @min(q[2][1], q[3][1]));
}

fn minX(q: Quad) f32 {
    return @min(@min(q[0][0], q[1][0]), @min(q[2][0], q[3][0]));
}

const Extent = struct { x0: f64, y0: f64, x1: f64, y1: f64 };

fn extent(q: Quad) Extent {
    var e = Extent{ .x0 = std.math.inf(f64), .y0 = std.math.inf(f64), .x1 = -std.math.inf(f64), .y1 = -std.math.inf(f64) };
    for (q) |p| {
        const x: f64 = p[0];
        const y: f64 = p[1];
        e.x0 = @min(e.x0, x);
        e.y0 = @min(e.y0, y);
        e.x1 = @max(e.x1, x);
        e.y1 = @max(e.y1, y);
    }
    return e;
}

// ── Model execution ─────────────────────────────────────────────────

const Output = struct {
    shape: []i64,
    f32s: ?[]f32 = null,
    i64s: ?[]i64 = null,

    fn deinit(self: Output, allocator: std.mem.Allocator) void {
        allocator.free(self.shape);
        if (self.f32s) |v| allocator.free(v);
        if (self.i64s) |v| allocator.free(v);
    }
};

/// One page's execution context: `backing` is the session output allocator,
/// `work` the admitted page allocator every intermediate comes from.
pub const Env = struct {
    backing: std.mem.Allocator,
    work: std.mem.Allocator,
    retained: *Preparation,
    control: ?InferenceExecutionControl,
    tab: *const BicubicTable,

    /// Run `session` on one f32 input of `shape`, filled by `fill(ctx, dst)`.
    /// Returns the named outputs copied into `work`.
    fn run(
        self: *const Env,
        session: backends.Session,
        shape: []const i64,
        ctx: anytype,
        comptime fill: fn (@TypeOf(ctx), []f32) void,
        comptime names: []const []const u8,
    ) ![names.len]Output {
        const outputs = blk: {
            var preparation = Preparation.child(self.backing, session.withAdmissionControl(self.control), self.retained);
            defer preparation.deinit();
            const scratch = try preparation.allocator();
            var count: usize = 1;
            for (shape) |d| count *= @intCast(d);
            const input = scratch.alloc(f32, count) catch |err| return preparation.failure(err);
            defer scratch.free(input);
            fill(ctx, input);
            try preparation.trim();
            const input_name = if (session.inputInfo().len > 0) session.inputInfo()[0].name else "x";
            var tensor = try preparation.tensor(f32, input_name, shape, input);
            defer tensor.deinit();
            break :blk try preparation.run(session, &.{tensor}, self.backing, self.control);
        };
        defer {
            for (outputs) |*t| t.deinit();
            self.backing.free(outputs);
        }
        var result: [names.len]Output = undefined;
        var filled: usize = 0;
        errdefer for (result[0..filled]) |o| o.deinit(self.work);
        inline for (names, 0..) |name, slot| {
            const t = findOutput(outputs, name, slot) orelse return error.MissingModelOutput;
            result[slot] = try copyOutput(self.work, t);
            filled += 1;
        }
        return result;
    }
};

fn findOutput(outputs: []backends.Tensor, name: []const u8, slot: usize) ?*backends.Tensor {
    for (outputs) |*t| if (std.mem.eql(u8, t.name, name)) return t;
    return if (slot < outputs.len) &outputs[slot] else null;
}

fn copyOutput(allocator: std.mem.Allocator, t: *const backends.Tensor) !Output {
    const shape = try allocator.dupe(i64, t.shape);
    errdefer allocator.free(shape);
    var count: usize = 1;
    for (shape) |d| count *= std.math.cast(usize, d) orelse return error.InvalidModelOutput;
    switch (t.dtype) {
        .f32 => {
            if (t.data.len < count * 4) return error.InvalidModelOutput;
            const values = try allocator.alloc(f32, count);
            for (values, 0..) |*v, i| v.* = @bitCast(std.mem.readInt(u32, t.data[i * 4 ..][0..4], .little));
            return .{ .shape = shape, .f32s = values };
        },
        .i64 => {
            if (t.data.len < count * 8) return error.InvalidModelOutput;
            const values = try allocator.alloc(i64, count);
            for (values, 0..) |*v, i| v.* = std.mem.readInt(i64, t.data[i * 8 ..][0..8], .little);
            return .{ .shape = shape, .i64s = values };
        },
        .i32 => {
            if (t.data.len < count * 4) return error.InvalidModelOutput;
            const values = try allocator.alloc(i64, count);
            for (values, 0..) |*v, i| v.* = std.mem.readInt(i32, t.data[i * 4 ..][0..4], .little);
            return .{ .shape = shape, .i64s = values };
        },
        else => return error.InvalidModelOutput,
    }
}

// ── Detector ────────────────────────────────────────────────────────

/// Kernel shrink ratio the detector targets were built with.
const shrink: f32 = 0.3;

fn detect(env: *const Env, detector: backends.Session, opts: Options, img: Raster) ![]Quad {
    const a = env.work;
    const big_h: f64 = @floatFromInt(img.h);
    const big_w: f64 = @floatFromInt(img.w);
    const s = @min(@as(f64, @floatFromInt(opts.det_long_side)) / @max(big_h, big_w), 1.0);
    const h: usize = @max(32, @as(usize, @intFromFloat(roundEven(big_h * s / 32.0))) * 32);
    const w: usize = @max(32, @as(usize, @intFromFloat(roundEven(big_w * s / 32.0))) * 32);
    const sx = @as(f64, @floatFromInt(w)) / big_w;
    const sy = @as(f64, @floatFromInt(h)) / big_h;
    const small = try resizeArea(a, img, w, h);
    defer small.deinit(a);

    const Fill = struct {
        fn fill(src: *const Raster, dst: []f32) void {
            const plane = src.w * src.h;
            for (0..3) |c| for (0..plane) |i| {
                dst[c * plane + i] = @as(f32, @floatFromInt(src.data[i * 3 + c])) / 127.5 - 1.0;
            };
        }
    };
    const shape = [_]i64{ 1, 3, @intCast(h), @intCast(w) };
    const outs = try env.run(detector, &shape, &small, Fill.fill, &.{"prob"});
    defer outs[0].deinit(a);
    const prob = outs[0].f32s orelse return error.InvalidModelOutput;
    if (outs[0].shape.len != 4) return error.InvalidModelOutput;
    const ph: usize = @intCast(outs[0].shape[2]);
    const pw: usize = @intCast(outs[0].shape[3]);
    if (ph == 0 or pw == 0 or prob.len < ph * pw) return error.InvalidModelOutput;
    const stride: f32 = @floatFromInt(h / ph);

    const mask = try a.alloc(bool, ph * pw);
    defer a.free(mask);
    for (mask, prob[0 .. ph * pw]) |*m, p| m.* = p > opts.det_thresh;
    const set = try components4(a, mask, pw, ph);
    defer set.deinit(a);

    var boxes = std.ArrayListUnmanaged(Quad).empty;
    defer boxes.deinit(a);
    var pts = std.ArrayListUnmanaged([2]f32).empty;
    defer pts.deinit(a);
    var span = std.ArrayListUnmanaged([2]usize).empty;
    defer span.deinit(a);
    for (set.comps) |comp| {
        if (comp.len < 2) continue;
        const pix = set.pixels[comp.start..][0..comp.len];
        var score: f64 = 0;
        for (pix) |p| score += prob[p[1] * pw + p[0]];
        score /= @floatFromInt(pix.len);
        if (@as(f32, @floatCast(score)) < opts.det_min_score) continue;
        const rows = comp.y1 - comp.y0 + 1;
        try span.resize(a, rows);
        for (span.items) |*r| r.* = .{ std.math.maxInt(usize), 0 };
        for (pix) |p| {
            const r = &span.items[p[1] - comp.y0];
            r[0] = @min(r[0], p[0]);
            r[1] = @max(r[1], p[0]);
        }
        pts.clearRetainingCapacity();
        for (span.items, 0..) |r, i| {
            if (r[0] == std.math.maxInt(usize)) continue;
            const y: f32 = @floatFromInt(comp.y0 + i);
            const x0: f32 = @floatFromInt(r[0]);
            const x1: f32 = @as(f32, @floatFromInt(r[1])) + 1;
            for ([_][2]f32{ .{ x0, y }, .{ x0, y + 1 }, .{ x1, y }, .{ x1, y + 1 } }) |pt|
                try pts.append(a, .{ pt[0] * stride, pt[1] * stride });
        }
        var r = try minAreaRect(a, pts.items);
        const w_is_horizontal = @abs(@mod(r.angle + 90.0, 180.0) - 90.0) < 45.0;
        const kernel_h = if (w_is_horizontal) r.h else r.w;
        const line_h = kernel_h / (1.0 - 2.0 * shrink);
        const d = shrink * line_h;
        r.w += 2 * d;
        r.h += 2 * d;
        var quad = boxPoints(r);
        for (&quad) |*p| {
            p[0] = std.math.clamp(@as(f32, @floatCast(@as(f64, p[0]) / sx)), 0, @as(f32, @floatFromInt(img.w)));
            p[1] = std.math.clamp(@as(f32, @floatCast(@as(f64, p[1]) / sy)), 0, @as(f32, @floatFromInt(img.h)));
        }
        try boxes.append(a, orderQuad(quad));
    }
    const merged = if (opts.det_merge > 0) try mergeCollinear(a, boxes.items, opts.det_merge) else try a.dupe(Quad, boxes.items);
    // Row band of 8 px, then left edge; stable like Python's sort.
    std.sort.insertion(Quad, merged, {}, struct {
        fn lt(_: void, l: Quad, r: Quad) bool {
            const lb = roundEven(@as(f64, minY(l)) / 8.0);
            const rb = roundEven(@as(f64, minY(r)) / 8.0);
            if (lb != rb) return lb < rb;
            return minX(l) < minX(r);
        }
    }.lt);
    return merged;
}

// ── Recognizers and router ──────────────────────────────────────────

/// Recognizer input height.
const rec_h = 32;
const rec_stride = 4;
const rec_max_w = 1280;
const rec_bucket_w = 128;
const rec_batch = 32;
const router_w = 512;
const router_batch = 64;

/// Grey line image at height 32, width scaled with the aspect ratio.
fn prepLine(allocator: std.mem.Allocator, crop: Raster, min_w: usize, max_w: usize) !Raster {
    const g = try toGray(allocator, crop);
    defer g.deinit(allocator);
    const scaled = roundEven(@as(f64, @floatFromInt(g.w)) * rec_h / @as(f64, @floatFromInt(@max(g.h, 1))));
    const nw: usize = std.math.clamp(@as(usize, @intFromFloat(@max(scaled, 0))), min_w, max_w);
    return if (g.h > rec_h) resizeArea(allocator, g, nw, rec_h) else resizeLinear(allocator, g, nw, rec_h);
}

pub const Reading = struct { text: []u8, conf: f32 };

fn freeReadings(allocator: std.mem.Allocator, readings: []Reading) void {
    for (readings) |r| allocator.free(r.text);
    allocator.free(readings);
}

/// (text, confidence) per crop. Lines are batched by width (sorted, 32 per
/// batch, padded to a multiple of 128 px) as the reference does: a line's
/// output depends on the padded width it was run at.
fn recognize(env: *const Env, rec: *const Recognizer, crops: []const Raster) ![]Reading {
    const a = env.work;
    const lines = try a.alloc(Raster, crops.len);
    var prepared: usize = 0;
    defer {
        for (lines[0..prepared]) |l| l.deinit(a);
        a.free(lines);
    }
    for (crops, 0..) |c, i| {
        lines[i] = try prepLine(a, c, rec_stride * 2, rec_max_w);
        prepared += 1;
    }
    const order = try a.alloc(usize, lines.len);
    defer a.free(order);
    for (order, 0..) |*o, i| o.* = i;
    std.sort.insertion(usize, order, lines, struct {
        fn lt(ls: []const Raster, l: usize, r: usize) bool {
            return ls[l].w < ls[r].w;
        }
    }.lt);
    const out = try a.alloc(Reading, lines.len);
    for (out) |*r| r.* = .{ .text = &.{}, .conf = 0 };
    errdefer freeReadings(a, out);
    var start: usize = 0;
    while (start < order.len) : (start += rec_batch) {
        const idx = order[start..@min(start + rec_batch, order.len)];
        var maxw: usize = 1;
        for (idx) |i| maxw = @max(maxw, lines[i].w);
        const w = std.math.divCeil(usize, maxw, rec_bucket_w) catch unreachable;
        const width = w * rec_bucket_w;
        const Ctx = struct { lines: []const Raster, idx: []const usize, width: usize };
        const Fill = struct {
            fn fill(ctx: Ctx, dst: []f32) void {
                @memset(dst, 1.0);
                for (ctx.idx, 0..) |i, j| {
                    const l = ctx.lines[i];
                    for (0..rec_h) |y| for (0..l.w) |x| {
                        dst[(j * rec_h + y) * ctx.width + x] = @floatCast(@as(f64, @floatFromInt(l.data[y * l.w + x])) / 127.5 - 1.0);
                    };
                }
            }
        };
        const shape = [_]i64{ @intCast(idx.len), 1, rec_h, @intCast(width) };
        const outs = try env.run(rec.session, &shape, Ctx{ .lines = lines, .idx = idx, .width = width }, Fill.fill, &.{ "ids", "conf" });
        defer for (outs) |o| o.deinit(a);
        // The native interpreter may hand integer outputs back as f32.
        const ids_f32 = if (outs[0].i64s == null) outs[0].f32s orelse return error.InvalidModelOutput else null;
        const ids = outs[0].i64s orelse try a.alloc(i64, ids_f32.?.len);
        defer if (outs[0].i64s == null) a.free(ids);
        if (ids_f32) |values| for (ids, values) |*dst, v| {
            if (!std.math.isFinite(v)) return error.InvalidModelOutput;
            dst.* = @intFromFloat(@round(v));
        };
        const conf = outs[1].f32s orelse return error.InvalidModelOutput;
        if (outs[0].shape.len != 2 or outs[0].shape[0] != idx.len) return error.InvalidModelOutput;
        const t_all: usize = @intCast(outs[0].shape[1]);
        if (t_all == 0 or ids.len < idx.len * t_all or conf.len < idx.len * t_all) return error.InvalidModelOutput;
        const stride = @max(width / t_all, 1);
        var text = std.ArrayListUnmanaged(u8).empty;
        defer text.deinit(a);
        for (idx, 0..) |i, j| {
            const t = @min(lines[i].w / stride, t_all);
            text.clearRetainingCapacity();
            var sum: f64 = 0;
            var n: usize = 0;
            var prev: i64 = 0;
            for (0..t) |k| {
                const c = ids[j * t_all + k];
                if (c != 0 and c != prev) {
                    if (c > 0 and @as(usize, @intCast(c - 1)) < rec.charset.len)
                        try text.appendSlice(a, rec.charset[@intCast(c - 1)]);
                    sum += conf[j * t_all + k];
                    n += 1;
                }
                prev = c;
            }
            var reading = try a.dupe(u8, text.items);
            if (rec.visual_rtl and hasRtl(reading)) {
                const logical = visualToLogical(a, reading) catch |err| {
                    a.free(reading);
                    return err;
                };
                a.free(reading);
                reading = logical;
            }
            out[i] = .{ .text = reading, .conf = if (n == 0) 0 else @floatCast(sum / @as(f64, @floatFromInt(n))) };
        }
    }
    return out;
}

/// Recognize the crops at `idx` with `rec`.
fn recognizeSubset(env: *const Env, rec: *const Recognizer, crops: []const Raster, idx: []const usize) ![]Reading {
    const sub = try env.work.alloc(Raster, idx.len);
    defer env.work.free(sub);
    for (sub, idx) |*s, i| s.* = crops[i];
    return recognize(env, rec, sub);
}

const Route = struct {
    groups: []usize,
    flip: bool,
    share: [group_count]f64,
};

fn softmax(comptime n: usize, z: []const f32) [n]f32 {
    var m = -std.math.inf(f32);
    for (z[0..n]) |v| m = @max(m, v);
    var e: [n]f32 = undefined;
    var s: f32 = 0;
    for (&e, z[0..n]) |*o, v| {
        o.* = @exp(v - m);
        s += o.*;
    }
    for (&e) |*o| o.* /= s;
    return e;
}

fn argmax(v: []const f64) usize {
    var best: usize = 0;
    for (1..v.len) |i| if (v[i] > v[best]) {
        best = i;
    };
    return best;
}

/// Page-level routing: every line takes the page's dominant group unless it
/// is long enough to judge and confidently another; Latin unless another
/// group holds a clear majority of the line width.
fn route(env: *const Env, router: backends.Session, crops: []const Raster) !Route {
    const a = env.work;
    const override: f32 = 0.9;
    const min_aspect: f64 = 4.0;
    const flip_min_lines = 3;
    const flip_vote: f64 = 0.7;
    const other_share: f64 = 0.65;
    const gp = try a.alloc([group_count]f32, crops.len);
    defer a.free(gp);
    const fp = try a.alloc(f32, crops.len);
    defer a.free(fp);
    var start: usize = 0;
    while (start < crops.len) : (start += router_batch) {
        const part = crops[start..@min(start + router_batch, crops.len)];
        const preps = try a.alloc(Raster, part.len);
        var n_prep: usize = 0;
        defer {
            for (preps[0..n_prep]) |p| p.deinit(a);
            a.free(preps);
        }
        for (part, 0..) |c, i| {
            preps[i] = try prepLine(a, c, 8, std.math.maxInt(usize));
            n_prep += 1;
        }
        const Fill = struct {
            fn fill(ps: []const Raster, dst: []f32) void {
                for (ps, 0..) |g, j| for (0..rec_h) |y| for (0..router_w) |x| {
                    dst[(j * rec_h + y) * router_w + x] = @as(f32, @floatFromInt(g.data[y * g.w + x % g.w])) / 127.5 - 1.0;
                };
            }
        };
        const shape = [_]i64{ @intCast(part.len), 1, rec_h, router_w };
        const outs = try env.run(router, &shape, @as([]const Raster, preps), Fill.fill, &.{ "group", "flip" });
        defer for (outs) |o| o.deinit(a);
        const g = outs[0].f32s orelse return error.InvalidModelOutput;
        const f = outs[1].f32s orelse return error.InvalidModelOutput;
        if (g.len < part.len * group_count or f.len < part.len * 2) return error.InvalidModelOutput;
        for (0..part.len) |j| {
            gp[start + j] = softmax(group_count, g[j * group_count ..]);
            fp[start + j] = softmax(2, f[j * 2 ..])[1];
        }
    }
    var share: [group_count]f64 = @splat(0);
    var wsum: f64 = 0;
    for (gp, crops) |p, c| {
        const wi: f64 = @floatFromInt(c.w);
        wsum += wi;
        for (0..group_count) |k| share[k] += @as(f64, p[k]) * wi;
    }
    for (&share) |*s| s.* /= wsum;
    var dom = argmax(&share);
    if (dom != 0 and share[dom] <= other_share) dom = 0;
    const out_groups = try a.alloc(usize, crops.len);
    for (gp, crops, out_groups) |p, c, *o| {
        var pv: [group_count]f64 = undefined;
        var pmax: f32 = -std.math.inf(f32);
        for (p, 0..) |v, k| {
            pv[k] = v;
            pmax = @max(pmax, v);
        }
        const own = argmax(&pv);
        o.* = if (own != dom and pmax > override and @as(f64, @floatFromInt(c.w)) >= min_aspect * @as(f64, @floatFromInt(c.h))) own else dom;
    }
    var vote: f64 = 0;
    for (fp, crops) |f, c| vote += @as(f64, f) * @as(f64, @floatFromInt(c.w));
    return .{ .groups = out_groups, .flip = crops.len >= flip_min_lines and vote > flip_vote * wsum, .share = share };
}

// ── Page-level steps ────────────────────────────────────────────────

fn longLines(boxes: []const Quad) usize {
    var n: usize = 0;
    for (boxes) |q| {
        const e = extent(q);
        const dx = e.x1 - e.x0;
        const dy = e.y1 - e.y0;
        if (dx >= 30 and dx >= 3 * dy) n += 1;
    }
    return n;
}

fn looksSideways(boxes: []const Quad) bool {
    return boxes.len >= 15 and @as(f64, @floatFromInt(longLines(boxes))) < 0.25 * @as(f64, @floatFromInt(boxes.len));
}

/// The quad in the original image, corners in reading order.
fn readingFrame(q: Quad, flip: bool, sideways: bool, w0: usize) Quad {
    var p = q;
    if (flip) p = .{ p[2], p[3], p[0], p[1] };
    if (sideways) for (&p) |*pt| {
        pt.* = .{ @floatCast(@as(f64, @floatFromInt(w0)) - 1.0 - pt[1]), pt[0] };
    };
    return p;
}

/// Otsu's threshold: the first level maximising the between-class variance.
fn otsu(hist: *const [256]f64, total: f64) u8 {
    const n = @max(total, 1.0);
    var w: f64 = 0;
    var mu: f64 = 0;
    var cum: [256][2]f64 = undefined;
    for (0..256) |i| {
        w += hist[i] / n;
        mu += hist[i] / n * @as(f64, @floatFromInt(i));
        cum[i] = .{ w, mu };
    }
    const mu_t = cum[255][1];
    var best_s = -std.math.inf(f64);
    var best: u8 = 0;
    for (cum, 0..) |cw, i| {
        const d = mu_t * cw[0] - cw[1];
        const s = d * d / (cw[0] * (1 - cw[0]));
        if (std.math.isFinite(s) and s > best_s) {
            best_s = s;
            best = @intCast(i);
        }
    }
    return best;
}

const Gray = struct {
    g: Raster,
    fn px(self: Gray, x: i64, y: i64) u8 {
        return self.g.data[@as(usize, @intCast(y)) * self.g.w + @as(usize, @intCast(x))];
    }
};

/// Columns of a printed vertical rule crossing the line box, as a mask over
/// `[xa, xb)`.
fn ruleColumns(allocator: std.mem.Allocator, gray: Gray, xa: i64, xb: i64, y0: f64, y1: f64, thr: u8) ![]bool {
    const gw: i64 = @intCast(gray.g.w);
    const gh: i64 = @intCast(gray.g.h);
    const h = y1 - y0;
    const band: i64 = @intFromFloat(@ceil(0.35 * h));
    const top: i64 = @intFromFloat(y0);
    const bot: i64 = @intFromFloat(@ceil(y1));
    const Share = struct {
        fn f(gr: Gray, gw_: i64, gh_: i64, x: i64, ya_in: i64, yb_in: i64, t: u8) f64 {
            const ya = std.math.clamp(ya_in, 0, gh_);
            const yb = std.math.clamp(yb_in, 0, gh_);
            if (yb <= ya or x < 0 or x >= gw_) return 0;
            var n: usize = 0;
            var y = ya;
            while (y < yb) : (y += 1) if (gr.px(x, y) < t) {
                n += 1;
            };
            return @as(f64, @floatFromInt(n)) / @as(f64, @floatFromInt(yb - ya));
        }
    };
    const width: usize = @intCast(xb - xa);
    const solid = try allocator.alloc(bool, width);
    defer allocator.free(solid);
    for (solid, 0..) |*s, i| {
        const x = xa + @as(i64, @intCast(i));
        s.* = Share.f(gray, gw, gh, x, top, bot, thr) >= 0.9 and
            Share.f(gray, gw, gh, x, top - band, top, thr) >= 0.8 and
            Share.f(gray, gw, gh, x, bot, bot + band, thr) >= 0.8;
    }
    const out = try allocator.alloc(bool, width);
    @memset(out, false);
    const max_w: i64 = @max(@as(i64, @intFromFloat(@ceil(0.15 * h))), 3);
    const flank: i64 = @max(@as(i64, @intFromFloat(@ceil(0.25 * h))), 3);
    const pad: i64 = @max(@as(i64, @intFromFloat(@ceil(0.06 * h))), 2);
    var i: usize = 0;
    while (i < width) {
        if (!solid[i]) {
            i += 1;
            continue;
        }
        const s = i;
        while (i < width and solid[i]) i += 1;
        const rs = xa + @as(i64, @intCast(s));
        const re = xa + @as(i64, @intCast(i));
        const blank = struct {
            fn f(gr: Gray, gw_: i64, gh_: i64, from: i64, to: i64, t_: i64, b_: i64, t: u8) bool {
                var n: i64 = 0;
                var x = from;
                while (x < to) : (x += 1) if (Share.f(gr, gw_, gh_, x, t_, b_, t) < 0.1) {
                    n += 1;
                };
                return n * 2 >= to - from;
            }
        }.f;
        if (@as(i64, @intCast(i - s)) <= max_w and blank(gray, gw, gh, rs - flank, rs, top, bot, thr) and blank(gray, gw, gh, re, re + flank, top, bot, thr)) {
            var x = rs - pad;
            while (x < re + pad) : (x += 1) if (x >= xa and x < xb) {
                out[@intCast(x - xa)] = true;
            };
        }
    }
    return out;
}

const gutter_min: f64 = 0.4;

/// Cut near-horizontal boxes at blank vertical runs that line up with other
/// boxes' edges and stay clear in the neighbouring lines.
fn splitGutters(allocator: std.mem.Allocator, img: Raster, boxes: []const Quad) ![]Quad {
    if (boxes.len < 3) return allocator.dupe(Quad, boxes);
    const gray = Gray{ .g = try meanGray(allocator, img) };
    defer gray.g.deinit(allocator);
    const gw: i64 = @intCast(gray.g.w);
    const gh: i64 = @intCast(gray.g.h);
    const ext = try allocator.alloc(Extent, boxes.len);
    defer allocator.free(ext);
    for (ext, boxes) |*e, q| e.* = extent(q);
    var out = std.ArrayListUnmanaged(Quad).empty;
    errdefer out.deinit(allocator);
    var idx = std.ArrayListUnmanaged(i64).empty;
    defer idx.deinit(allocator);
    var cuts = std.ArrayListUnmanaged([2]f64).empty;
    defer cuts.deinit(allocator);
    for (boxes, 0..) |q, i| {
        const e0 = ext[i];
        const x0 = e0.x0;
        const y0 = e0.y0;
        const x1 = e0.x1;
        const y1 = e0.y1;
        const h = y1 - y0;
        const tilt = @abs(@as(f64, q[1][1]) - @as(f64, q[0][1]));
        if (h < 6 or x1 - x0 < 4 * h or tilt > 0.25 * h) {
            try out.append(allocator, q);
            continue;
        }
        const ya = @min(@max(@as(i64, @intFromFloat(y0 + 0.15 * h)), 0), gh);
        const yb = std.math.clamp(@as(i64, @intFromFloat(@ceil(y1 - 0.15 * h))), 0, gh);
        const xa = @max(@as(i64, @intFromFloat(x0)), 0);
        const xb = @min(@as(i64, @intFromFloat(@ceil(x1))), gw);
        if (yb <= ya or xb <= xa) {
            try out.append(allocator, q);
            continue;
        }
        var lo: u8 = 255;
        var hi: u8 = 0;
        var hist: [256]f64 = @splat(0);
        var total: f64 = 0;
        var y = ya;
        while (y < yb) : (y += 1) {
            var x = xa;
            while (x < xb) : (x += 1) {
                const v = gray.px(x, y);
                lo = @min(lo, v);
                hi = @max(hi, v);
                hist[v] += 1;
                total += 1;
            }
        }
        if (@as(i32, hi) - @as(i32, lo) < 60) {
            try out.append(allocator, q);
            continue;
        }
        const thr = otsu(&hist, total);
        const rule = try ruleColumns(allocator, gray, xa, xb, y0, y1, thr);
        defer allocator.free(rule);
        const isRule = struct {
            fn f(r: []const bool, base: i64, x: i64) bool {
                const off = x - base;
                return off >= 0 and off < r.len and r[@intCast(off)];
            }
        }.f;
        idx.clearRetainingCapacity();
        var x = xa;
        while (x < xb) : (x += 1) {
            if (isRule(rule, xa, x)) continue;
            var yy = ya;
            const inked = while (yy < yb) : (yy += 1) {
                if (gray.px(x, yy) < thr) break true;
            } else false;
            if (inked) try idx.append(allocator, x - xa);
        }
        if (idx.items.len < 2) {
            try out.append(allocator, q);
            continue;
        }
        const cy = (y0 + y1) / 2;
        const tol = 0.6 * h;
        cuts.clearRetainingCapacity();
        for (0..idx.items.len - 1) |g| {
            if (@as(f64, @floatFromInt(idx.items[g + 1] - idx.items[g])) <= gutter_min * h) continue;
            const ga: f64 = @floatFromInt(xa + idx.items[g] + 1);
            const gb: f64 = @floatFromInt(xa + idx.items[g + 1]);
            var left: usize = 0;
            var right: usize = 0;
            for (ext, 0..) |e, j| {
                if (j == i) continue;
                const eh = e.y1 - e.y0;
                if (!(@abs((e.y0 + e.y1) / 2 - cy) < 25 * h and 0.5 * h < eh and eh < 2 * h)) continue;
                if (@abs(e.x0 - gb) < tol) left += 1;
                if (@abs(e.x1 - ga) < tol) right += 1;
            }
            if (left < 1 or right < 1 or left + right < 3 or ga - x0 < 3 * h) continue;
            const m: f64 = @floatFromInt(@max(@as(i64, @intFromFloat((gb - ga) * 0.25)), 1));
            const sa: i64 = @intFromFloat(ga + m);
            const sb: i64 = @intFromFloat(gb - m);
            if (sb <= sa) continue;
            var ok: usize = 0;
            var bad: usize = 0;
            for (ext, 0..) |e, j| {
                const eh = e.y1 - e.y0;
                if (j == i or @abs((e.y0 + e.y1) / 2 - cy) > 4 * h or !(0.5 * h < eh and eh < 2 * h)) continue;
                if (e.x1 <= @as(f64, @floatFromInt(sa)) or e.x0 >= @as(f64, @floatFromInt(sb))) {
                    ok += 1;
                    continue;
                }
                const ra = std.math.clamp(@as(i64, @intFromFloat(e.y0 + 0.15 * eh)), 0, gh);
                const rb = std.math.clamp(@as(i64, @intFromFloat(@ceil(e.y1 - 0.15 * eh))), 0, gh);
                const ca = std.math.clamp(sa, 0, gw);
                const cb = std.math.clamp(sb, 0, gw);
                var inked = false;
                var ry = ra;
                outer: while (ry < rb) : (ry += 1) {
                    var cx = ca;
                    while (cx < cb) : (cx += 1) if (!isRule(rule, xa, cx) and gray.px(cx, ry) < thr) {
                        inked = true;
                        break :outer;
                    };
                }
                if (inked) bad += 1 else ok += 1;
            }
            if (!(ok >= 2 and (bad == 0 or (bad == 1 and ok >= 3)))) continue;
            try cuts.append(allocator, .{ ga, gb });
        }
        if (cuts.items.len == 0) {
            try out.append(allocator, q);
            continue;
        }
        const pad = 0.15 * h;
        var edges = std.ArrayListUnmanaged(f64).empty;
        defer edges.deinit(allocator);
        try edges.append(allocator, x0);
        for (cuts.items) |cut| {
            try edges.append(allocator, cut[0] + pad);
            try edges.append(allocator, cut[1] - pad);
        }
        try edges.append(allocator, x1);
        var k: usize = 0;
        while (k + 1 < edges.items.len) : (k += 2) {
            const s = edges.items[k];
            const e = edges.items[k + 1];
            if (e - s > 0.5 * h) {
                const sf: f32 = @floatCast(s);
                const ef: f32 = @floatCast(e);
                const yaf: f32 = @floatCast(y0);
                const ybf: f32 = @floatCast(y1);
                try out.append(allocator, .{ .{ sf, yaf }, .{ ef, yaf }, .{ ef, ybf }, .{ sf, ybf } });
            }
        }
    }
    return out.toOwnedSlice(allocator);
}

fn majority(gs: []const usize) usize {
    var n: [group_count]usize = @splat(0);
    for (gs) |g| n[g] += 1;
    var best: usize = 0;
    for (1..group_count) |i| if (n[i] > n[best]) {
        best = i;
    };
    return best;
}

fn mean(values: []const f32) f64 {
    if (values.len == 0) return std.math.nan(f64);
    var s: f64 = 0;
    for (values) |v| s += v;
    return s / @as(f64, @floatFromInt(values.len));
}

const PageReads = struct { reads: []Reading, flip: bool };

/// Readings per box with routing, rescue and off-script re-reads.
fn readPage(env: *const Env, stages: *const Stages, img: Raster, boxes: []const Quad) !PageReads {
    const a = env.work;
    var crops = try a.alloc(Raster, boxes.len);
    var n_crops: usize = 0;
    defer {
        for (crops[0..n_crops]) |c| c.deinit(a);
        a.free(crops);
    }
    for (boxes, 0..) |q, i| {
        crops[i] = try cropQuad(a, img, q, env.tab);
        n_crops += 1;
    }
    const r = try route(env, stages.router, crops);
    const gs = r.groups;
    defer a.free(gs);
    if (r.flip) for (crops) |*c| {
        const turned = try rot90(a, c.*, 2);
        c.deinit(a);
        c.* = turned;
    };
    const n = crops.len;
    const out = try a.alloc(Reading, n);
    for (out) |*o| o.* = .{ .text = &.{}, .conf = 0 };
    errdefer freeReadings(a, out);

    var idx = std.ArrayListUnmanaged(usize).empty;
    defer idx.deinit(a);
    const replace = struct {
        fn f(al: std.mem.Allocator, slot: *Reading, next: Reading) !void {
            al.free(slot.text);
            slot.* = .{ .text = try al.dupe(u8, next.text), .conf = next.conf };
        }
    }.f;

    for (0..group_count) |gi| {
        idx.clearRetainingCapacity();
        for (0..n) |i| if (gs[i] == gi) try idx.append(a, i);
        if (idx.items.len == 0) continue;
        const res = try recognizeSubset(env, stages.recognizer(gi), crops, idx.items);
        defer freeReadings(a, res);
        for (idx.items, res) |i, rd| try replace(a, &out[i], rd);
    }

    var dom = majority(gs);
    const keep_conf: f32 = 0.75;
    if (stages.options.rescue and dom == 0) {
        const rescue_conf: f64 = 0.75;
        const rescue_margin: f64 = 0.15;
        const rescue_share: f64 = 0.2;
        idx.clearRetainingCapacity();
        for (0..n) |i| if (gs[i] == dom) try idx.append(a, i);
        var any_share = false;
        for (r.share[1..]) |s| any_share = any_share or s >= rescue_share;
        if (idx.items.len > 0 and any_share) {
            const confs = try a.alloc(f32, idx.items.len);
            defer a.free(confs);
            for (confs, idx.items) |*c, i| c.* = out[i].conf;
            const lat_conf = mean(confs);
            if (lat_conf < rescue_conf) {
                const sample = idx.items[0..@min(idx.items.len, 40)];
                var best: ?usize = null;
                var best_conf = lat_conf + rescue_margin;
                for (1..group_count) |gi| {
                    if (r.share[gi] < rescue_share) continue;
                    const res = try recognizeSubset(env, stages.recognizer(gi), crops, sample);
                    defer freeReadings(a, res);
                    const cs = try a.alloc(f32, res.len);
                    defer a.free(cs);
                    for (cs, res) |*c, rd| c.* = rd.conf;
                    const c = mean(cs);
                    if (c >= best_conf) {
                        best = gi;
                        best_conf = c;
                    }
                }
                if (best) |gi| {
                    const res = try recognizeSubset(env, stages.recognizer(gi), crops, idx.items);
                    defer freeReadings(a, res);
                    for (idx.items, res) |i, rd| {
                        if (!(out[i].conf >= keep_conf and out[i].conf >= rd.conf)) {
                            try replace(a, &out[i], rd);
                            gs[i] = gi;
                        }
                    }
                    dom = gi;
                }
            }
        }
        if (dom == 0) {
            const line_margin: f32 = 0.1;
            const line_share: f64 = 0.15;
            const line_conf: f32 = 0.8;
            for (1..group_count) |gi| {
                if (r.share[gi] < line_share) continue;
                idx.clearRetainingCapacity();
                for (0..n) |i| if (gs[i] == 0) try idx.append(a, i);
                if (idx.items.len == 0) break;
                const res = try recognizeSubset(env, stages.recognizer(gi), crops, idx.items);
                defer freeReadings(a, res);
                for (idx.items, res) |i, rd| {
                    const sc = scriptChars(gi, rd.text);
                    if (rd.conf >= out[i].conf + line_margin and rd.conf >= line_conf and sc >= 3 and sc > latinLetters(rd.text)) {
                        try replace(a, &out[i], rd);
                        gs[i] = gi;
                    }
                }
            }
        }
    }

    // Lines routed away from the page's script are read with the page's
    // recognizer too; the off-script reading must win clearly.
    idx.clearRetainingCapacity();
    for (0..n) |i| if (gs[i] != dom) try idx.append(a, i);
    if (idx.items.len > 0) {
        const res = try recognizeSubset(env, stages.recognizer(dom), crops, idx.items);
        defer freeReadings(a, res);
        for (idx.items, res) |i, rd| {
            const sc = scriptChars(gs[i], out[i].text);
            const routed_ok = out[i].conf >= keep_conf and sc >= 2 and sc > latinLetters(out[i].text);
            if (!routed_ok and (out[i].conf < rd.conf + 0.1 or sc < 3)) try replace(a, &out[i], rd);
        }
    }
    for (out) |*o| {
        const cleaned = try cleanLatin(a, o.text);
        a.free(o.text);
        o.text = cleaned;
    }
    return .{ .reads = out, .flip = r.flip };
}

/// OCR one decoded page. Lines are returned in detector order.
pub fn runPage(env: *const Env, detector: backends.Session, stages: *const Stages, decoded: image.Image) ![]Line {
    const a = env.work;
    const src_ch: usize = decoded.channels;
    if (src_ch == 0 or src_ch == 2 or src_ch > 4) return error.UnsupportedImageChannels;
    var img0 = Raster{ .w = decoded.width, .h = decoded.height, .ch = 3, .data = undefined };
    img0.data = try a.alloc(u8, img0.w * img0.h * 3);
    defer img0.deinit(a);
    if (decoded.data.len < img0.w * img0.h * src_ch) return error.InvalidImage;
    for (0..img0.w * img0.h) |i| {
        const px = decoded.data[i * src_ch ..][0..src_ch];
        img0.data[i * 3 ..][0..3].* = if (src_ch == 1) .{ px[0], px[0], px[0] } else .{ px[0], px[1], px[2] };
    }

    const opts = stages.options;
    var boxes = try detect(env, detector, opts, img0);
    defer a.free(boxes);
    var sideways = false;
    var turned: ?Raster = null;
    defer if (turned) |t| t.deinit(a);
    if (opts.orient and looksSideways(boxes)) {
        const t = try rot90(a, img0, 1);
        turned = t;
        const tboxes = try detect(env, detector, opts, t);
        if (longLines(tboxes) > 2 * longLines(boxes) + 5) {
            a.free(boxes);
            boxes = tboxes;
            sideways = true;
        } else a.free(tboxes);
    }
    const img = if (sideways) turned.? else img0;
    if (opts.gutter) {
        const split = try splitGutters(a, img, boxes);
        a.free(boxes);
        boxes = split;
    }
    if (env.control) |active| try active.update(.executing, 0, @intCast(boxes.len));
    if (boxes.len == 0) return a.alloc(Line, 0);
    const page = try readPage(env, stages, img, boxes);
    defer {
        for (page.reads) |rd| a.free(rd.text);
        a.free(page.reads);
    }
    var lines = std.ArrayListUnmanaged(Line).empty;
    errdefer {
        for (lines.items) |l| a.free(l.text);
        lines.deinit(a);
    }
    for (boxes, page.reads) |q, rd| {
        if (std.mem.trim(u8, rd.text, " \t\r\n").len == 0) continue;
        if (opts.junk and isJunk(rd.text, rd.conf)) continue;
        const pts = if (opts.orient) readingFrame(q, page.flip, sideways, img0.w) else q;
        try lines.append(a, .{ .text = try a.dupe(u8, rd.text), .confidence = rd.conf, .quad = pts });
    }
    return lines.toOwnedSlice(a);
}

pub fn quadBBox(q: Quad) [4]f64 {
    const e = extent(q);
    return .{ @trunc(e.x0), @trunc(e.y0), @trunc(e.x1), @trunc(e.y1) };
}

// ── Script classes and text filters ─────────────────────────────────

const Utf8Iter = struct {
    bytes: []const u8,
    i: usize = 0,

    fn next(self: *Utf8Iter) ?u21 {
        if (self.i >= self.bytes.len) return null;
        const n = std.unicode.utf8ByteSequenceLength(self.bytes[self.i]) catch {
            self.i += 1;
            return 0xfffd;
        };
        if (self.i + n > self.bytes.len) {
            self.i = self.bytes.len;
            return 0xfffd;
        }
        const cp = std.unicode.utf8Decode(self.bytes[self.i..][0..n]) catch 0xfffd;
        self.i += n;
        return cp;
    }
};

fn inRange(c: u21, lo: u21, hi: u21) bool {
    return c >= lo and c <= hi;
}

fn isAsciiAlpha(c: u21) bool {
    return (c >= 'a' and c <= 'z') or (c >= 'A' and c <= 'Z');
}

fn isDigitLike(c: u21) bool {
    if (c >= '0' and c <= '9') return true;
    for (digit_zeros) |z| if (c >= z and c < z + 10) return true;
    // Superscripts, fractions and other numeric forms count as numeric.
    return c == 0xb2 or c == 0xb3 or c == 0xb9 or inRange(c, 0xbc, 0xbe) or inRange(c, 0x2070, 0x2079) or
        inRange(c, 0x2080, 0x2089) or inRange(c, 0x2150, 0x2189) or inRange(c, 0x2460, 0x249b) or inRange(c, 0xff10, 0xff19);
}

/// An approximation of Unicode `Alphabetic` sufficient for the filters below:
/// letters (and the combining signs alphabetic scripts write with) in the
/// blocks the recognizers emit, excluding punctuation, symbols and digits.
fn isAlphabetic(c: u21) bool {
    if (c < 0x80) return isAsciiAlpha(c);
    if (c == 0xaa or c == 0xb5 or c == 0xba) return true;
    if (inRange(c, 0xc0, 0x24f)) return c != 0xd7 and c != 0xf7;
    if (inRange(c, 0x250, 0x2af)) return true; // IPA
    if (inRange(c, 0x300, 0x36f)) return false; // combining diacritics
    if (inRange(c, 0x370, 0x3ff)) return c != 0x375 and c != 0x37e and c != 0x384 and c != 0x385 and c != 0x387;
    if (inRange(c, 0x400, 0x52f)) return !inRange(c, 0x482, 0x489);
    if (inRange(c, 0x531, 0x587)) return !inRange(c, 0x55a, 0x55f);
    if (inRange(c, 0x5b0, 0x5f2)) return c != 0x5be and c != 0x5c0 and c != 0x5c3 and c != 0x5c6;
    if (inRange(c, 0x600, 0x6ff)) return !isDigitLike(c) and !(inRange(c, 0x600, 0x60f) or c == 0x61b or inRange(c, 0x61d, 0x61f) or inRange(c, 0x66a, 0x66d) or c == 0x6d4 or c == 0x6dd or c == 0x6de or c == 0x6e9);
    if (inRange(c, 0x700, 0x8ff)) return true;
    if (inRange(c, 0x900, 0xdff)) return !isDigitLike(c) and c != 0x964 and c != 0x965 and c != 0x970;
    if (inRange(c, 0xe00, 0xeff)) return !isDigitLike(c) and c != 0xe3f and c != 0xe4f and c != 0xe5a and c != 0xe5b;
    if (inRange(c, 0xf00, 0xfff)) return inRange(c, 0xf40, 0xfbc) or c == 0xf00;
    if (inRange(c, 0x1000, 0x109f)) return !isDigitLike(c) and !inRange(c, 0x104a, 0x104f);
    if (inRange(c, 0x10a0, 0x10ff)) return c != 0x10fb;
    if (inRange(c, 0x1100, 0x11ff)) return true;
    if (inRange(c, 0x1200, 0x139f)) return !inRange(c, 0x1360, 0x137c);
    if (inRange(c, 0x13a0, 0x13ff)) return true;
    if (inRange(c, 0x1780, 0x17ff)) return !isDigitLike(c) and !inRange(c, 0x17d4, 0x17db);
    if (inRange(c, 0x1e00, 0x1fff)) return true;
    if (inRange(c, 0x2c00, 0x2dff)) return true;
    if (inRange(c, 0x3040, 0x30ff)) return c != 0x30a0 and c != 0x30fb;
    if (inRange(c, 0x3105, 0x318f)) return true;
    if (inRange(c, 0x31f0, 0x31ff)) return true;
    if (inRange(c, 0x3400, 0x4dbf)) return true;
    if (inRange(c, 0x4e00, 0x9fff)) return true;
    if (inRange(c, 0xa000, 0xa4cf)) return true;
    if (inRange(c, 0xa640, 0xa69f) or inRange(c, 0xa720, 0xa7ff)) return true;
    if (inRange(c, 0xac00, 0xd7af)) return true;
    if (inRange(c, 0xf900, 0xfaff)) return true;
    if (inRange(c, 0xfb00, 0xfdff)) return c != 0xfd3e and c != 0xfd3f;
    if (inRange(c, 0xfe70, 0xfefc)) return true;
    if (inRange(c, 0xff21, 0xff3a) or inRange(c, 0xff41, 0xff5a) or inRange(c, 0xff66, 0xffdc)) return true;
    if (inRange(c, 0x20000, 0x3134f)) return true;
    return false;
}

fn isAlphanumeric(c: u21) bool {
    return isAlphabetic(c) or isDigitLike(c);
}

fn isUpper(c: u21) bool {
    if (c >= 'A' and c <= 'Z') return true;
    if (inRange(c, 0xc0, 0xde)) return c != 0xd7;
    if (inRange(c, 0x100, 0x17f)) return c % 2 == 0 and c != 0x138;
    if (inRange(c, 0x391, 0x3ab)) return c != 0x3a2;
    if (inRange(c, 0x400, 0x42f)) return true;
    if (inRange(c, 0x460, 0x4ff)) return c % 2 == 0;
    if (inRange(c, 0x531, 0x556)) return true;
    return false;
}

fn isLower(c: u21) bool {
    if (c >= 'a' and c <= 'z') return true;
    if (inRange(c, 0xdf, 0xff)) return c != 0xf7;
    if (inRange(c, 0x100, 0x17f)) return c % 2 == 1 or c == 0x138;
    if (inRange(c, 0x3ac, 0x3ce)) return true;
    if (inRange(c, 0x430, 0x45f)) return true;
    if (inRange(c, 0x460, 0x4ff)) return c % 2 == 1;
    if (inRange(c, 0x561, 0x587)) return true;
    return false;
}

fn isWhitespace(c: u21) bool {
    return c == ' ' or (c >= 0x09 and c <= 0x0d) or c == 0x85 or c == 0xa0 or c == 0x1680 or
        inRange(c, 0x2000, 0x200a) or c == 0x2028 or c == 0x2029 or c == 0x202f or c == 0x205f or c == 0x3000;
}

fn isLatinLetter(c: u21) bool {
    return isAsciiAlpha(c) or (inRange(c, 0xc0, 0x24f) and isAlphabetic(c));
}

fn isCyrillic(c: u21) bool {
    return inRange(c, 0x400, 0x4ff);
}

fn isGreekOrCyrillic(c: u21) bool {
    return inRange(c, 0x370, 0x4ff);
}

/// Characters that show a reading really is in `group`'s script.
fn isScriptChar(group: usize, c: u21) bool {
    return switch (group) {
        0 => isAsciiAlpha(c) or inRange(c, 0xc0, 0x24f) or inRange(c, 0x370, 0x4ff),
        1 => inRange(c, 0x3040, 0x30ff) or inRange(c, 0x3400, 0x9fff) or inRange(c, 0xac00, 0xd7af) or inRange(c, 0xf900, 0xfaff),
        2 => inRange(c, 0x590, 0x8ff) or inRange(c, 0xfb1d, 0xfdff) or inRange(c, 0xfe70, 0xfeff),
        else => inRange(c, 0x900, 0xdff) or inRange(c, 0xe00, 0xeff) or inRange(c, 0xf00, 0x109f) or
            inRange(c, 0x10a0, 0x10ff) or inRange(c, 0x530, 0x58f) or inRange(c, 0x1200, 0x139f) or inRange(c, 0x1780, 0x17ff),
    };
}

fn scriptChars(group: usize, text: []const u8) usize {
    var it = Utf8Iter{ .bytes = text };
    var n: usize = 0;
    while (it.next()) |c| if (isScriptChar(group, c)) {
        n += 1;
    };
    return n;
}

fn latinLetters(text: []const u8) usize {
    var it = Utf8Iter{ .bytes = text };
    var n: usize = 0;
    while (it.next()) |c| if (isLatinLetter(c)) {
        n += 1;
    };
    return n;
}

const homo_from = "аеорсухкіјѕԁԛԝАВЕКМНОРСТХІЈЅΑΒΕΖΗΙΚΜΝΟΡΤΥΧοαν" ++ "ՍսՕօոհցզաԼՏนทรอบ๐ບນຣ໐၀ဝ";
const homo_to = "aeopcyxkijsdqwABEKMHOPCTXIJSABEZHIKMNOPTYXoav" ++ "UuOonhgqwLSunsououusooo";
const cyr_from = "aceopxyiABCEHKMOPTXYIΑΒΓΕΗΚΜΟΠΡΤΧαο";
const cyr_to = "асеорхуіАВСЕНКМОРТХУІАВГЕНКМОПРТХао";

const digit_zeros = [_]u21{
    0x0660, 0x06f0, 0x0966, 0x09e6, 0x0a66, 0x0ae6, 0x0b66, 0x0be6, 0x0c66, 0x0ce6, 0x0d66, 0x0de6,
    0x0e50, 0x0ed0, 0x0f20, 0x1040, 0x1090, 0x17e0,
};

fn mapChar(comptime from: []const u8, comptime to: []const u8, c: u21) ?u21 {
    const pairs = comptime blk: {
        @setEvalBranchQuota(100_000);
        var out: []const [2]u21 = &.{};
        var fi = std.unicode.Utf8View.initComptime(from).iterator();
        var ti = std.unicode.Utf8View.initComptime(to).iterator();
        while (fi.nextCodepoint()) |f| {
            const t = ti.nextCodepoint() orelse @compileError("homoglyph tables differ in length");
            out = out ++ [_][2]u21{.{ f, t }};
        }
        if (ti.nextCodepoint() != null) @compileError("homoglyph tables differ in length");
        break :blk out;
    };
    for (pairs) |p| if (p[0] == c) return p[1];
    return null;
}

fn latinLookalike(c: u21, upper: bool) ?u21 {
    if (mapChar(homo_from, homo_to, c)) |m| return m;
    return switch (c) {
        'т' => if (upper) 'T' else 't',
        'н' => if (upper) 'H' else 'h',
        'м' => if (upper) 'M' else 'm',
        'п', 'л' => 'n',
        'г' => 'r',
        else => null,
    };
}

fn appendCodepoint(allocator: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), c: u21) !void {
    var buf: [4]u8 = undefined;
    const n = std.unicode.utf8Encode(c, &buf) catch std.unicode.utf8Encode(0xfffd, &buf) catch unreachable;
    try out.appendSlice(allocator, buf[0..n]);
}

/// Fold other scripts' lookalike characters in Latin lines (and Latin
/// lookalikes in Cyrillic words); other scripts' digits become ASCII in
/// Latin lines. Lines mostly in another script are left alone.
pub fn cleanLatin(allocator: std.mem.Allocator, text: []const u8) ![]u8 {
    var lat: usize = 0;
    var oth: usize = 0;
    var cyr_total: usize = 0;
    {
        var it = Utf8Iter{ .bytes = text };
        while (it.next()) |c| {
            if (isLatinLetter(c)) lat += 1 else if (isAlphabetic(c)) oth += 1;
            if (isCyrillic(c)) cyr_total += 1;
        }
    }
    const latin_line = lat > 0 and lat >= 2 * oth;
    const cyr_line = cyr_total > lat;
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(allocator);
    var words = std.ArrayListUnmanaged([]const u8).empty;
    defer words.deinit(allocator);
    // Split on every whitespace code point (Rust `split(char::is_whitespace)`).
    {
        var it = Utf8Iter{ .bytes = text };
        var start: usize = 0;
        var pos: usize = 0;
        while (it.next()) |c| {
            if (isWhitespace(c)) {
                try words.append(allocator, text[start..pos]);
                start = it.i;
            }
            pos = it.i;
        }
        try words.append(allocator, text[start..]);
    }
    for (words.items, 0..) |word, k| {
        if (k > 0) try out.append(allocator, ' ');
        var wl: usize = 0;
        var wo: usize = 0;
        var up: usize = 0;
        var low: usize = 0;
        var cg: usize = 0;
        var cyr: usize = 0;
        var lookalikes_only = true;
        var it = Utf8Iter{ .bytes = word };
        while (it.next()) |c| {
            if (isLatinLetter(c)) wl += 1 else if (isAlphabetic(c)) wo += 1;
            if (isUpper(c)) up += 1;
            if (isLower(c)) low += 1;
            if (isGreekOrCyrillic(c)) cg += 1;
            if (isCyrillic(c)) cyr += 1;
            if (isAlphabetic(c) and !isCyrillic(c) and mapChar(cyr_from, cyr_to, c) == null) lookalikes_only = false;
        }
        const upper = up > low;
        var all_lookalike = wo > 0;
        if (all_lookalike) {
            it = .{ .bytes = word };
            while (it.next()) |c| {
                if (!isAlphabetic(c) or isLatinLetter(c)) continue;
                if (latinLookalike(c, upper) == null or inRange(c, 0x370, 0x3ff)) {
                    all_lookalike = false;
                    break;
                }
            }
        }
        const fold_cg = cg > 0 and wl >= cg;
        const fold = latin_line and wo > 0 and (wl >= wo or all_lookalike);
        const to_cyr = (cyr >= 2 and cyr > wl + (cg - cyr)) or (cyr >= 1 and cyr_line and lookalikes_only);
        it = .{ .bytes = word };
        while (it.next()) |c0| {
            var c = c0;
            if (to_cyr) {
                c = mapChar(cyr_from, cyr_to, c) orelse c;
            } else if (fold or (fold_cg and isGreekOrCyrillic(c))) {
                c = latinLookalike(c, upper) orelse c;
            }
            var digit: ?u21 = null;
            for (digit_zeros) |z| if (c >= z and c < z + 10) {
                digit = z;
            };
            if (digit != null and latin_line) {
                try out.append(allocator, @intCast('0' + (c - digit.?)));
            } else try appendCodepoint(allocator, &out, c);
        }
    }
    return out.toOwnedSlice(allocator);
}

/// A detector false positive on a picture: no letter or digit, or 1-2 unsure characters.
pub fn isJunk(text: []const u8, conf: f32) bool {
    var it = Utf8Iter{ .bytes = text };
    var word: usize = 0;
    while (it.next()) |c| if (isAlphanumeric(c)) {
        word += 1;
    };
    return word == 0 or (word <= 2 and conf < 0.8);
}

fn isRtlChar(c: u21) bool {
    return inRange(c, 0x590, 0x8ff) or inRange(c, 0xfb1d, 0xfeff);
}

fn hasRtl(text: []const u8) bool {
    var it = Utf8Iter{ .bytes = text };
    while (it.next()) |c| if (isRtlChar(c)) return true;
    return false;
}

fn mirror(c: u21) u21 {
    return switch (c) {
        '(' => ')',
        ')' => '(',
        '[' => ']',
        ']' => '[',
        '{' => '}',
        '}' => '{',
        '<' => '>',
        '>' => '<',
        0xab => 0xbb,
        0xbb => 0xab,
        else => c,
    };
}

/// Reorder a visual-order RTL reading into logical order with an RTL
/// paragraph level, as python-bidi `get_display(s, base_dir="R")` does for
/// one line. This is a reduced bidi algorithm: strong RTL characters take
/// level 1; Latin letters and digits take level 2; neutrals between two
/// level-2 characters join them and otherwise take the paragraph level.
/// Explicit embeddings and Arabic-number subtleties are not modelled.
pub fn visualToLogical(allocator: std.mem.Allocator, text: []const u8) ![]u8 {
    var cps = std.ArrayListUnmanaged(u21).empty;
    defer cps.deinit(allocator);
    var it = Utf8Iter{ .bytes = text };
    while (it.next()) |c| try cps.append(allocator, c);
    const n = cps.items.len;
    const levels = try allocator.alloc(u8, n);
    defer allocator.free(levels);
    const Kind = enum { r, l, n };
    const kinds = try allocator.alloc(Kind, n);
    defer allocator.free(kinds);
    for (cps.items, kinds) |c, *k| k.* = if (isRtlChar(c) and !isDigitLike(c)) .r else if (isAlphanumeric(c)) .l else .n;
    var i: usize = 0;
    while (i < n) {
        if (kinds[i] != .n) {
            levels[i] = if (kinds[i] == .r) 1 else 2;
            i += 1;
            continue;
        }
        var j = i;
        while (j < n and kinds[j] == .n) j += 1;
        const before: Kind = if (i > 0) kinds[i - 1] else .r;
        const after: Kind = if (j < n) kinds[j] else .r;
        const level: u8 = if (before == .l and after == .l) 2 else 1;
        @memset(levels[i..j], level);
        i = j;
    }
    // Reverse runs at level >= 2, then the whole line (level >= 1).
    i = 0;
    while (i < n) {
        if (levels[i] < 2) {
            i += 1;
            continue;
        }
        var j = i;
        while (j < n and levels[j] >= 2) j += 1;
        std.mem.reverse(u21, cps.items[i..j]);
        std.mem.reverse(u8, levels[i..j]);
        i = j;
    }
    std.mem.reverse(u21, cps.items);
    std.mem.reverse(u8, levels);
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(allocator);
    for (cps.items, levels) |c, lv| try appendCodepoint(allocator, &out, if (lv % 2 == 1) mirror(c) else c);
    return out.toOwnedSlice(allocator);
}

// ── Tests ───────────────────────────────────────────────────────────

test "ocria round half to even matches numpy" {
    try std.testing.expectEqual(@as(f64, 2), roundEven(2.5));
    try std.testing.expectEqual(@as(f64, 4), roundEven(3.5));
    try std.testing.expectEqual(@as(f64, -2), roundEven(-2.5));
    try std.testing.expectEqual(@as(f64, 3), roundEven(2.6));
}

test "ocria rot90 matches numpy" {
    const a = std.testing.allocator;
    var data = [_]u8{ 1, 2, 3, 4, 5, 6 };
    const img = Raster{ .w = 3, .h = 2, .ch = 1, .data = &data };
    const r1 = try rot90(a, img, 1);
    defer r1.deinit(a);
    try std.testing.expectEqualSlices(u8, &.{ 3, 6, 2, 5, 1, 4 }, r1.data);
    const r2 = try rot90(a, img, 2);
    defer r2.deinit(a);
    try std.testing.expectEqualSlices(u8, &.{ 6, 5, 4, 3, 2, 1 }, r2.data);
}

test "ocria bicubic weights sum to one" {
    const tab = try std.testing.allocator.create(BicubicTable);
    defer std.testing.allocator.destroy(tab);
    tab.init();
    for (tab.entries) |t| {
        var s: i32 = 0;
        for (t) |v| s += v;
        try std.testing.expectEqual(remap_scale, s);
    }
}

test "ocria min area rect of an axis box" {
    const r = try minAreaRect(std.testing.allocator, &.{ .{ 0, 0 }, .{ 10, 0 }, .{ 10, 4 }, .{ 0, 4 } });
    try std.testing.expectApproxEqAbs(@as(f32, 5), r.cx, 1e-4);
    try std.testing.expectApproxEqAbs(@as(f32, 2), r.cy, 1e-4);
    try std.testing.expectApproxEqAbs(@as(f32, 10), @max(r.w, r.h), 1e-4);
    try std.testing.expectApproxEqAbs(@as(f32, 4), @min(r.w, r.h), 1e-4);
}

test "ocria area resize averages integer blocks" {
    const a = std.testing.allocator;
    var data = [_]u8{ 0, 4, 8, 12, 0, 4, 8, 12 };
    const img = Raster{ .w = 4, .h = 2, .ch = 1, .data = &data };
    const out = try resizeArea(a, img, 2, 1);
    defer out.deinit(a);
    try std.testing.expectEqualSlices(u8, &.{ 2, 10 }, out.data);
}

test "ocria merges two collinear line pieces" {
    const a = std.testing.allocator;
    const boxes = [_]Quad{
        .{ .{ 0, 0 }, .{ 40, 0 }, .{ 40, 10 }, .{ 0, 10 } },
        .{ .{ 44, 0 }, .{ 90, 0 }, .{ 90, 10 }, .{ 44, 10 } },
    };
    const merged = try mergeCollinear(a, &boxes, 0.6);
    defer a.free(merged);
    try std.testing.expectEqual(@as(usize, 1), merged.len);
    const e = extent(merged[0]);
    try std.testing.expectApproxEqAbs(@as(f64, 0), e.x0, 1e-3);
    try std.testing.expectApproxEqAbs(@as(f64, 90), e.x1, 1e-3);
}

test "ocria charset parses a JSON array" {
    const a = std.testing.allocator;
    const cs = try parseCharset(a, "[\" \", \"a\", \"\\u00e9\"]");
    defer freeCharset(a, cs);
    try std.testing.expectEqual(@as(usize, 3), cs.len);
    try std.testing.expectEqualStrings("é", cs[2]);
    try std.testing.expectError(error.InvalidCharset, parseCharset(a, "{}"));
    const meta = parseDetectorMeta(a, "{\"thresh\": 0.55}");
    try std.testing.expectApproxEqAbs(@as(f32, 0.55), meta.thresh.?, 1e-6);
    try std.testing.expect(meta.merge == null);
    try std.testing.expect(parseVisualRtl(a, "{\"visual_rtl\": true}"));
}

fn expectClean(input: []const u8, expected: []const u8) !void {
    const out = try cleanLatin(std.testing.allocator, input);
    defer std.testing.allocator.free(out);
    try std.testing.expectEqualStrings(expected, out);
}

test "ocria clean latin folds lookalikes only in latin context" {
    try expectClean("pt6а-42 Привет", "pt6a-42 Привет");
    try expectClean("REPՍBLQՍE FRANÇAISE", "REPUBLQUE FRANÇAISE");
    try expectClean("C๐mmunicatoทร Security", "Communicatons Security");
    try expectClean("В - Linear 1- Center", "B - Linear 1- Center");
    try expectClean("KLAR 08 (UA०८ SHIP TERM", "KLAR 08 (UA08 SHIP TERM");
    try expectClean("ZQX272003тOKEN OнMS", "ZQX272003TOKEN OHMS");
    try expectClean("airborлe пoise to гeduce", "airborne noise to reduce");
    try expectClean("aрганізацый ФOPYMA ПяТAГΑ", "арганізацый ФОРУМА ПяТАГА");
    try expectClean("significant at α = 0.05 level", "significant at α = 0.05 level");
    try expectClean("Привет мир, как дела", "Привет мир, как дела");
    try expectClean("३ मार्च २०२४ को", "३ मार्च २०२४ को");
    try expectClean("Հայաստանի Հանրապետություն", "Հայաստանի Հանրապետություն");
}

test "ocria junk filter" {
    try std.testing.expect(isJunk("|", 0.99));
    try std.testing.expect(isJunk("ab", 0.5));
    try std.testing.expect(!isJunk("ab", 0.9));
    try std.testing.expect(!isJunk("hello", 0.3));
}

test "ocria visual to logical keeps latin runs in order" {
    const a = std.testing.allocator;
    // Visual "םולש" (as rendered) is logical "שלום".
    const out = try visualToLogical(a, "םולש");
    defer a.free(out);
    try std.testing.expectEqualStrings("שלום", out);
    const mixed = try visualToLogical(a, "ABC 12 םולש");
    defer a.free(mixed);
    try std.testing.expectEqualStrings("שלום ABC 12", mixed);
}

test "ocria reading frame maps a quarter turn back" {
    const q = Quad{ .{ 0, 0 }, .{ 10, 0 }, .{ 10, 2 }, .{ 0, 2 } };
    const r = readingFrame(q, false, true, 100);
    try std.testing.expectEqual([2]f32{ 99, 0 }, r[0]);
    try std.testing.expectEqual([2]f32{ 99, 10 }, r[1]);
}
