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
const F32x4 = @Vector(4, f32);
const pcm_scale = 4.5 / 32768.0;

pub const HybridState = struct {
    overlap: [32][18]f32 = [_][18]f32{[_]f32{0} ** 18} ** 32,
};

pub const QmfState = struct {
    values: [15 * 64]f32 = [_]f32{0} ** (15 * 64),
};

pub const SynthesisState = struct {
    hybrid: HybridState = .{},
    qmf: QmfState = .{},
    v: [1024]f32 = [_]f32{0} ** 1024,
};

const enwindow = [_]i32{
    0,      -1,     -1,     -1,     -1,     -1,     -1,     -2,
    -2,     -2,     -2,     -3,     -3,     -4,     -4,     -5,
    -5,     -6,     -7,     -7,     -8,     -9,     -10,    -11,
    -13,    -14,    -16,    -17,    -19,    -21,    -24,    -26,
    -29,    -31,    -35,    -38,    -41,    -45,    -49,    -53,
    -58,    -63,    -68,    -73,    -79,    -85,    -91,    -97,
    -104,   -111,   -117,   -125,   -132,   -139,   -147,   -154,
    -161,   -169,   -176,   -183,   -190,   -196,   -202,   -208,
    213,    218,    222,    225,    227,    228,    228,    227,
    224,    221,    215,    208,    200,    189,    177,    163,
    146,    127,    106,    83,     57,     29,     -2,     -36,
    -72,    -111,   -153,   -197,   -244,   -294,   -347,   -401,
    -459,   -519,   -581,   -645,   -711,   -779,   -848,   -919,
    -991,   -1064,  -1137,  -1210,  -1283,  -1356,  -1428,  -1498,
    -1567,  -1634,  -1698,  -1759,  -1817,  -1870,  -1919,  -1962,
    -2001,  -2032,  -2057,  -2075,  -2085,  -2087,  -2080,  -2063,
    2037,   2000,   1952,   1893,   1822,   1739,   1644,   1535,
    1414,   1280,   1131,   970,    794,    605,    402,    185,
    -45,    -288,   -545,   -814,   -1095,  -1388,  -1692,  -2006,
    -2330,  -2663,  -3004,  -3351,  -3705,  -4063,  -4425,  -4788,
    -5153,  -5517,  -5879,  -6237,  -6589,  -6935,  -7271,  -7597,
    -7910,  -8209,  -8491,  -8755,  -8998,  -9219,  -9416,  -9585,
    -9727,  -9838,  -9916,  -9959,  -9966,  -9935,  -9863,  -9750,
    -9592,  -9389,  -9139,  -8840,  -8492,  -8092,  -7640,  -7134,
    6574,   5959,   5288,   4561,   3776,   2935,   2037,   1082,
    70,     -998,   -2122,  -3300,  -4533,  -5818,  -7154,  -8540,
    -9975,  -11455, -12980, -14548, -16155, -17799, -19478, -21189,
    -22929, -24694, -26482, -28289, -30112, -31947, -33791, -35640,
    -37489, -39336, -41176, -43006, -44821, -46617, -48390, -50137,
    -51853, -53534, -55178, -56778, -58333, -59838, -61289, -62684,
    -64019, -65290, -66494, -67629, -68692, -69679, -70590, -71420,
    -72169, -72835, -73415, -73908, -74313, -74630, -74856, -74992,
    75038,
};

pub fn overlapAddGranule(
    state: *HybridState,
    hybrid_blocks: []const f32,
    subband_samples: []f32,
) !void {
    if (hybrid_blocks.len != 32 * 36 or subband_samples.len != 32 * 18) {
        return error.InvalidDimensions;
    }

    for (0..32) |subband| {
        const block = hybrid_blocks[subband * 36 ..][0..36];
        const out = subband_samples[subband * 18 ..][0..18];

        for (0..18) |i| {
            out[i] = block[i] + state.overlap[subband][i];
            state.overlap[subband][i] = block[18 + i];
        }
    }
}

pub fn synthesizeSlot(
    state: *SynthesisState,
    subband_samples: []const f32,
    pcm_out: []f32,
) !void {
    if (subband_samples.len != 32 or pcm_out.len != 32) return error.InvalidDimensions;

    @memmove(state.v[64..], state.v[0 .. state.v.len - 64]);
    synthesisMatrix64(subband_samples, state.v[0..64]);
    applyWindow(state.v[0..], pcm_out);
}

pub fn synthesizeFrame(
    state: *SynthesisState,
    subband_samples: []const f32,
    pcm_out: []f32,
) !void {
    return synthesizeFrameMono(&state.qmf, subband_samples, pcm_out);
}

pub fn synthesizeFrameMono(
    qmf_state: *QmfState,
    subband_samples: []const f32,
    pcm_out: []f32,
) !void {
    if (subband_samples.len != 32 * 18 or pcm_out.len != 32 * 18) {
        return error.InvalidDimensions;
    }

    var left: [32 * 18]f32 = undefined;
    @memcpy(left[0..], subband_samples);
    dctIIMonoInPlace(left[0..], 18);

    var lins: [33 * 64]f32 = [_]f32{0} ** (33 * 64);
    @memcpy(lins[0 .. 15 * 64], qmf_state.values[0..]);

    var slot: usize = 0;
    while (slot < 18) : (slot += 2) {
        synthesizePair(left[0..], left[0..], slot, 1, pcm_out[slot * 32 ..][0..64], lins[slot * 64 ..]);
    }

    for (0..15 * 64) |index| {
        if ((index & 1) == 0) {
            qmf_state.values[index] = lins[18 * 64 + index];
        }
    }
}

pub fn synthesizeFrameStereo(
    qmf_state: *QmfState,
    left_subband_samples: []const f32,
    right_subband_samples: []const f32,
    pcm_out: []f32,
) !void {
    if (left_subband_samples.len != 32 * 18 or right_subband_samples.len != 32 * 18 or pcm_out.len != 32 * 18 * 2) {
        return error.InvalidDimensions;
    }

    var left: [32 * 18]f32 = undefined;
    var right: [32 * 18]f32 = undefined;
    @memcpy(left[0..], left_subband_samples);
    @memcpy(right[0..], right_subband_samples);
    dctIIMonoInPlace(left[0..], 18);
    dctIIMonoInPlace(right[0..], 18);

    var lins: [33 * 64]f32 = [_]f32{0} ** (33 * 64);
    @memcpy(lins[0 .. 15 * 64], qmf_state.values[0..]);

    var slot: usize = 0;
    while (slot < 18) : (slot += 2) {
        synthesizePair(left[0..], right[0..], slot, 2, pcm_out[slot * 32 * 2 ..][0 .. 64 * 2], lins[slot * 64 ..]);
    }

    @memcpy(qmf_state.values[0..], lins[18 * 64 ..][0 .. 15 * 64]);
}

fn dctIIMonoInPlace(grbuf: []f32, n: usize) void {
    const g_sec = [_]f32{
        10.19000816, 0.50060302, 0.50241929,
        3.40760851,  0.50547093, 0.52249861,
        2.05778098,  0.51544732, 0.56694406,
        1.48416460,  0.53104258, 0.64682180,
        1.16943991,  0.55310392, 0.78815460,
        0.97256821,  0.58293498, 1.06067765,
        0.83934963,  0.62250412, 1.72244716,
        0.74453628,  0.67480832, 5.10114861,
    };

    var k: usize = 0;
    while (k < n) : (k += 1) {
        var t: [4][8]f32 = undefined;

        var i: usize = 0;
        while (i < 8) : (i += 1) {
            const x0 = grbuf[slotSubbandIndex(k, i)];
            const x1 = grbuf[slotSubbandIndex(k, 15 - i)];
            const x2 = grbuf[slotSubbandIndex(k, 16 + i)];
            const x3 = grbuf[slotSubbandIndex(k, 31 - i)];
            const t0 = x0 + x3;
            const t1 = x1 + x2;
            const t2 = (x1 - x2) * g_sec[3 * i + 0];
            const t3 = (x0 - x3) * g_sec[3 * i + 1];
            t[0][i] = t0 + t1;
            t[1][i] = (t0 - t1) * g_sec[3 * i + 2];
            t[2][i] = t3 + t2;
            t[3][i] = (t3 - t2) * g_sec[3 * i + 2];
        }

        i = 0;
        while (i < 4) : (i += 1) {
            var x0 = t[i][0];
            var x1 = t[i][1];
            var x2 = t[i][2];
            var x3 = t[i][3];
            var x4 = t[i][4];
            var x5 = t[i][5];
            var x6 = t[i][6];
            var x7 = t[i][7];

            const xt = x0 - x7;
            x0 += x7;
            x7 = x1 - x6;
            x1 += x6;
            x6 = x2 - x5;
            x2 += x5;
            x5 = x3 - x4;
            x3 += x4;
            x4 = x0 - x3;
            x0 += x3;
            x3 = x1 - x2;
            x1 += x2;
            t[i][0] = x0 + x1;
            t[i][4] = (x0 - x1) * 0.70710677;
            x5 = x5 + x6;
            x6 = (x6 + x7) * 0.70710677;
            x7 = x7 + xt;
            x3 = (x3 + x4) * 0.70710677;
            x5 -= x7 * 0.198912367;
            x7 += x5 * 0.382683432;
            x5 -= x7 * 0.198912367;
            x0 = xt - x6;
            const xt2 = xt + x6;
            t[i][1] = (xt2 + x7) * 0.50979561;
            t[i][2] = (x4 + x3) * 0.54119611;
            t[i][3] = (x0 - x5) * 0.60134488;
            t[i][5] = (x0 + x5) * 0.89997619;
            t[i][6] = (x4 - x3) * 1.30656302;
            t[i][7] = (xt2 - x7) * 2.56291556;
        }

        i = 0;
        while (i < 7) : (i += 1) {
            const subband = i * 4;
            grbuf[slotSubbandIndex(k, subband + 0)] = t[0][i];
            grbuf[slotSubbandIndex(k, subband + 1)] = t[2][i] + t[3][i] + t[3][i + 1];
            grbuf[slotSubbandIndex(k, subband + 2)] = t[1][i] + t[1][i + 1];
            grbuf[slotSubbandIndex(k, subband + 3)] = t[2][i + 1] + t[3][i] + t[3][i + 1];
        }
        grbuf[slotSubbandIndex(k, 28)] = t[0][7];
        grbuf[slotSubbandIndex(k, 29)] = t[2][7] + t[3][7];
        grbuf[slotSubbandIndex(k, 30)] = t[1][7];
        grbuf[slotSubbandIndex(k, 31)] = t[3][7];
    }
}

fn synthesizePair(
    left: []const f32,
    right: []const f32,
    slot: usize,
    channels: usize,
    pcm_out: []f32,
    lins_base: []f32,
) void {
    std.debug.assert(channels == 1 or channels == 2);
    std.debug.assert(pcm_out.len == 64 * channels);
    std.debug.assert(lins_base.len >= 17 * 64);
    const win = comptime buildSynthWin();
    const zlin_start: isize = 15 * 64;
    var zlin = lins_base[15 * 64 ..];

    zlin[4 * 15] = left[slotSubbandIndex(slot, 16)];
    zlin[4 * 15 + 1] = right[slotSubbandIndex(slot, 16)];
    zlin[4 * 15 + 2] = left[slotSubbandIndex(slot, 0)];
    zlin[4 * 15 + 3] = right[slotSubbandIndex(slot, 0)];

    zlin[4 * 31] = left[slotSubbandIndex(slot + 1, 16)];
    zlin[4 * 31 + 1] = right[slotSubbandIndex(slot + 1, 16)];
    zlin[4 * 31 + 2] = left[slotSubbandIndex(slot + 1, 0)];
    zlin[4 * 31 + 3] = right[slotSubbandIndex(slot + 1, 0)];

    if (channels == 2) {
        synthesizePairEndpoints(pcm_out[1..], channels, lins_base[4 * 15 + 1 ..]);
        synthesizePairEndpoints(pcm_out[32 * channels + 1 ..], channels, lins_base[4 * 15 + 64 + 1 ..]);
    }
    synthesizePairEndpoints(pcm_out, channels, lins_base[4 * 15 ..]);
    synthesizePairEndpoints(pcm_out[32 * channels ..], channels, lins_base[4 * 15 + 64 ..]);

    var i: usize = 15;
    var w_index: usize = 0;
    while (i > 0) {
        i -= 1;
        zlin[4 * i] = left[slotSubbandIndex(slot, 31 - i)];
        zlin[4 * i + 1] = right[slotSubbandIndex(slot, 31 - i)];
        zlin[4 * i + 2] = left[slotSubbandIndex(slot + 1, 31 - i)];
        zlin[4 * i + 3] = right[slotSubbandIndex(slot + 1, 31 - i)];
        zlin[4 * (i + 16)] = left[slotSubbandIndex(slot + 1, 1 + i)];
        zlin[4 * (i + 16) + 1] = right[slotSubbandIndex(slot + 1, 1 + i)];
        lins_base[14 * 64 + 4 * i + 2] = left[slotSubbandIndex(slot, 1 + i)];
        lins_base[14 * 64 + 4 * i + 3] = right[slotSubbandIndex(slot, 1 + i)];

        var a: F32x4 = undefined;
        var b: F32x4 = undefined;
        var k: usize = 0;
        while (k < 8) : (k += 1) {
            const w0 = win[w_index];
            const w1 = win[w_index + 1];
            w_index += 2;
            const vz = lins_base[@intCast(zlin_start + @as(isize, @intCast(4 * i)) - @as(isize, @intCast(k * 64)))..];
            const vy = lins_base[@intCast(zlin_start + @as(isize, @intCast(4 * i)) - @as(isize, @intCast((15 - k) * 64)))..];
            const vz_vec: F32x4 = vz[0..4].*;
            const vy_vec: F32x4 = vy[0..4].*;
            const w0_vec: F32x4 = @splat(w0);
            const w1_vec: F32x4 = @splat(w1);
            const bz = (vz_vec * w1_vec) + (vy_vec * w0_vec);
            const delta = if ((k & 1) == 0)
                (vz_vec * w0_vec) - (vy_vec * w1_vec)
            else
                (vy_vec * w1_vec) - (vz_vec * w0_vec);
            if (k == 0) {
                b = bz;
                a = delta;
            } else {
                b += bz;
                a += delta;
            }
        }

        writePairLanes(pcm_out, channels, 15 - i, 17 + i, 47 - i, 49 + i, a, b);
    }
}

fn synthesizePairEndpoints(dst: []f32, channels: usize, z: []const f32) void {
    var a = (z[14 * 64] - z[0]) * 29;
    a += (z[1 * 64] + z[13 * 64]) * 213;
    a += (z[12 * 64] - z[2 * 64]) * 459;
    a += (z[3 * 64] + z[11 * 64]) * 2037;
    a += (z[10 * 64] - z[4 * 64]) * 5153;
    a += (z[5 * 64] + z[9 * 64]) * 6574;
    a += (z[8 * 64] - z[6 * 64]) * 37489;
    a += z[7 * 64] * 75038;
    dst[0] = clampPcmSample(a * pcm_scale);

    a = z[14 * 64 + 2] * 104;
    a += z[12 * 64 + 2] * 1567;
    a += z[10 * 64 + 2] * 9727;
    a += z[8 * 64 + 2] * 64019;
    a += z[6 * 64 + 2] * -9975;
    a += z[4 * 64 + 2] * -45;
    a += z[2 * 64 + 2] * 146;
    a += z[2] * -5;
    dst[channels * 16] = clampPcmSample(a * pcm_scale);
}

fn writePairLanes(
    pcm_out: []f32,
    channels: usize,
    left_a_index: usize,
    left_b_index: usize,
    left_c_index: usize,
    left_d_index: usize,
    a: F32x4,
    b: F32x4,
) void {
    pcm_out[left_a_index * channels] = clampPcmSample(a[0] * pcm_scale);
    pcm_out[left_b_index * channels] = clampPcmSample(b[0] * pcm_scale);
    pcm_out[left_c_index * channels] = clampPcmSample(a[2] * pcm_scale);
    pcm_out[left_d_index * channels] = clampPcmSample(b[2] * pcm_scale);

    if (channels == 2) {
        pcm_out[left_a_index * channels + 1] = clampPcmSample(a[1] * pcm_scale);
        pcm_out[left_b_index * channels + 1] = clampPcmSample(b[1] * pcm_scale);
        pcm_out[left_c_index * channels + 1] = clampPcmSample(a[3] * pcm_scale);
        pcm_out[left_d_index * channels + 1] = clampPcmSample(b[3] * pcm_scale);
    }
}

fn clampPcmSample(sample: f32) f32 {
    return @min(@as(f32, 1.0), @max(@as(f32, -1.0), sample));
}

fn slotSubbandIndex(slot: usize, subband: usize) usize {
    return subband * 18 + slot;
}

fn buildSynthWin() [15 * 16]f32 {
    return .{
        -1, 26, -31, 208, 218, 401,  -519,  2063, 2000, 4788, -5517, 7134, 5959,  35640, -39336, 74992,
        -1, 24, -35, 202, 222, 347,  -581,  2080, 1952, 4425, -5879, 7640, 5288,  33791, -41176, 74856,
        -1, 21, -38, 196, 225, 294,  -645,  2087, 1893, 4063, -6237, 8092, 4561,  31947, -43006, 74630,
        -1, 19, -41, 190, 227, 244,  -711,  2085, 1822, 3705, -6589, 8492, 3776,  30112, -44821, 74313,
        -1, 17, -45, 183, 228, 197,  -779,  2075, 1739, 3351, -6935, 8840, 2935,  28289, -46617, 73908,
        -1, 16, -49, 176, 228, 153,  -848,  2057, 1644, 3004, -7271, 9139, 2037,  26482, -48390, 73415,
        -2, 14, -53, 169, 227, 111,  -919,  2032, 1535, 2663, -7597, 9389, 1082,  24694, -50137, 72835,
        -2, 13, -58, 161, 224, 72,   -991,  2001, 1414, 2330, -7910, 9592, 70,    22929, -51853, 72169,
        -2, 11, -63, 154, 221, 36,   -1064, 1962, 1280, 2006, -8209, 9750, -998,  21189, -53534, 71420,
        -2, 10, -68, 147, 215, 2,    -1137, 1919, 1131, 1692, -8491, 9863, -2122, 19478, -55178, 70590,
        -3, 9,  -73, 139, 208, -29,  -1210, 1870, 970,  1388, -8755, 9935, -3300, 17799, -56778, 69679,
        -3, 8,  -79, 132, 200, -57,  -1283, 1817, 794,  1095, -8998, 9966, -4533, 16155, -58333, 68692,
        -4, 7,  -85, 125, 189, -83,  -1356, 1759, 605,  814,  -9219, 9959, -5818, 14548, -59838, 67629,
        -4, 7,  -91, 117, 177, -106, -1428, 1698, 402,  545,  -9416, 9916, -7154, 12980, -61289, 66494,
        -5, 6,  -97, 111, 163, -127, -1498, 1634, 185,  288,  -9585, 9838, -8540, 11455, -62684, 65290,
    };
}

fn synthesisMatrix64(input: []const f32, output: []f32) void {
    const pi = std.math.pi;
    std.debug.assert(output.len == 64);

    for (0..64) |i| {
        var sum: f32 = 0;
        for (0..32) |n| {
            const angle = (pi / 64.0) *
                @as(f32, @floatFromInt((16 + i) * (2 * n + 1)));
            sum += input[n] * @cos(angle);
        }
        output[i] = sum;
    }
}

fn applyWindow(v: []const f32, pcm_out: []f32) void {
    std.debug.assert(v.len >= 1024);
    std.debug.assert(pcm_out.len == 32);

    const window = comptime buildWindow();
    var u: [512]f32 = undefined;

    for (0..8) |block| {
        const v_base = block * 128;
        const u_base = block * 64;
        @memcpy(u[u_base..][0..32], v[v_base..][0..32]);
        @memcpy(u[u_base + 32 ..][0..32], v[v_base + 96 ..][0..32]);
    }

    for (0..32) |sample_index| {
        var sum: f32 = 0;
        for (0..16) |phase| {
            const index = sample_index + phase * 32;
            sum += u[index] * window[index];
        }
        pcm_out[sample_index] = sum;
    }
}

fn buildWindow() [512]f32 {
    var window: [512]f32 = [_]f32{0} ** 512;
    // The synthesis window table is stored in 16.16-style fixed-point form:
    // its peak entry is ~75038, which maps to the expected ~1.145 window gain
    // only when normalized by 2^16. Dividing by 2^31 collapses decoded PCM
    // energy toward zero and breaks external MP3 parity.
    const scale = 1.0 / 65536.0;

    for (0..257) |i| {
        var value = @as(f32, @floatFromInt(enwindow[i])) * scale;
        window[i] = value;
        if ((i & 63) != 0) value = -value;
        if (i != 0) window[512 - i] = value;
    }

    return window;
}

test "overlap add rejects invalid dimensions" {
    var state = HybridState{};
    var out: [32 * 18]f32 = [_]f32{0} ** (32 * 18);
    const blocks = [_]f32{0} ** (32 * 36);
    var short_out = [_]f32{ 0, 1 };

    try std.testing.expectError(error.InvalidDimensions, overlapAddGranule(&state, &.{ 0, 1 }, out[0..]));
    try std.testing.expectError(error.InvalidDimensions, overlapAddGranule(&state, blocks[0..], short_out[0..]));
}

test "overlap add copies first half and stores second half" {
    var state = HybridState{};
    var blocks: [32 * 36]f32 = undefined;
    for (&blocks, 0..) |*sample, i| sample.* = @floatFromInt(i);

    var out: [32 * 18]f32 = undefined;
    try overlapAddGranule(&state, blocks[0..], out[0..]);

    for (0..32) |subband| {
        for (0..18) |i| {
            try std.testing.expectEqual(blocks[subband * 36 + i], out[subband * 18 + i]);
            try std.testing.expectEqual(blocks[subband * 36 + 18 + i], state.overlap[subband][i]);
        }
    }
}

test "overlap add carries previous tail into next granule" {
    var state = HybridState{};

    const first_blocks = [_]f32{1} ** (32 * 36);
    var first_out: [32 * 18]f32 = undefined;
    try overlapAddGranule(&state, first_blocks[0..], first_out[0..]);

    const second_blocks = [_]f32{2} ** (32 * 36);
    var second_out: [32 * 18]f32 = undefined;
    try overlapAddGranule(&state, second_blocks[0..], second_out[0..]);

    for (first_out) |sample| try std.testing.expectEqual(@as(f32, 1), sample);
    for (second_out) |sample| try std.testing.expectEqual(@as(f32, 3), sample);
    for (state.overlap) |tail| {
        for (tail) |sample| try std.testing.expectEqual(@as(f32, 2), sample);
    }
}

test "synthesize slot rejects invalid dimensions" {
    var state = SynthesisState{};
    var out: [32]f32 = [_]f32{0} ** 32;
    const input = [_]f32{0} ** 32;
    var short_out = [_]f32{ 0, 1 };

    try std.testing.expectError(error.InvalidDimensions, synthesizeSlot(&state, &.{ 0, 1 }, out[0..]));
    try std.testing.expectError(error.InvalidDimensions, synthesizeSlot(&state, input[0..], short_out[0..]));
}

test "synthesize slot keeps zero input at zero" {
    var state = SynthesisState{};
    const input = [_]f32{0} ** 32;
    var out: [32]f32 = [_]f32{1} ** 32;

    try synthesizeSlot(&state, input[0..], out[0..]);
    for (out) |sample| try std.testing.expectApproxEqAbs(@as(f32, 0), sample, 1e-7);
}

test "synthesize frame emits nonzero pcm for subband impulse" {
    var state = SynthesisState{};
    var input: [32 * 18]f32 = [_]f32{0} ** (32 * 18);
    input[0] = 1;

    var out: [32 * 18]f32 = [_]f32{0} ** (32 * 18);
    try synthesizeFrame(&state, input[0..], out[0..]);

    var energy: f32 = 0;
    for (out) |sample| energy += @abs(sample);
    try std.testing.expect(energy > 0);
}

test "synthesis slot shifts synthesis history without underflow" {
    var state = SynthesisState{};
    var input = [_]f32{0} ** 32;
    input[0] = 1;
    var out: [32]f32 = [_]f32{0} ** 32;

    try synthesizeSlot(&state, input[0..], out[0..]);
    var first_head: [64]f32 = undefined;
    @memcpy(first_head[0..], state.v[0..64]);
    try synthesizeSlot(&state, input[0..], out[0..]);
    try std.testing.expectEqualSlices(f32, first_head[0..], state.v[64..128]);
}
