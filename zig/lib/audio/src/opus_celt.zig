//! CELT layer of the Opus decoder (RFC 6716 section 4.3), ported from the
//! libopus float reference decoder. The port keeps the reference control
//! flow (celt_decode_with_ec, quant_all_bands, alg_unquant, the allocation
//! search, anti-collapse, the backward MDCT with TDAC unfolding, the pitch
//! postfilter and the de-emphasis) so the output matches libopus sample for
//! sample on the RFC test vectors. Only the standard 48 kHz / 960-sample mode
//! is supported, which is the only mode an Opus bitstream can use.

const std = @import("std");
const opus = @import("opus.zig");

const RangeDecoder = opus.RangeDecoder;

pub const nb_ebands: usize = 21;
pub const short_mdct_size: usize = 120;
pub const max_lm: u2 = 3;
pub const overlap: usize = 120;
pub const decode_buffer_size: usize = 2048;
const combfilter_minperiod: i32 = 15;
const bitres: u5 = 3;
const max_fine_bits: i32 = 8;
const fine_offset: i32 = 21;
const qtheta_offset: i32 = 4;
const qtheta_offset_twophase: i32 = 16;
const alloc_steps: u5 = 6;
const log_max_pseudo: usize = 6;
const spread_normal: i32 = 2;
const spread_aggressive: i32 = 3;
const preemph_coef: f32 = 0.85000061;
const sig_scale: f32 = 1.0 / 32768.0;
const very_small: f32 = 1e-30;
const epsilon: f32 = 1e-15;

pub const e_bands = [22]i32{ 0, 1, 2, 3, 4, 5, 6, 7, 8, 10, 12, 14, 16, 20, 24, 28, 34, 40, 48, 60, 78, 100 };
const log_n = [21]i32{ 0, 0, 0, 0, 0, 0, 0, 0, 8, 8, 8, 8, 16, 16, 16, 21, 21, 24, 29, 34, 36 };
const e_means = [21]f32{
    6.4375, 6.25,   5.75,  5.3125, 5.0625,
    4.8125, 4.5,    4.375, 4.875,  4.6875,
    4.5625, 4.4375, 4.875, 4.625,  4.3125,
    4.5,    4.375,  4.625, 4.75,   4.4375,
    3.75,
};
const tf_select_table = [4][8]i8{
    .{ 0, -1, 0, -1, 0, -1, 0, -1 },
    .{ 0, -1, 0, -2, 1, 0, 1, -1 },
    .{ 0, -2, 0, -3, 2, 0, 1, -1 },
    .{ 0, -2, 0, -3, 3, 0, 1, -1 },
};
const log2_frac_table = [24]i32{ 0, 8, 13, 16, 19, 21, 23, 24, 26, 27, 28, 29, 30, 31, 32, 32, 33, 34, 34, 35, 36, 36, 37, 37 };
const spread_icdf = [_]u8{ 25, 23, 2, 0 };
const trim_icdf = [_]u8{ 126, 124, 119, 109, 87, 41, 19, 9, 4, 2, 0 };
const tapset_icdf = [_]u8{ 2, 1, 0 };
const small_energy_icdf = [_]u8{ 2, 1, 0 };
const pred_coef = [4]f32{ 29440.0 / 32768.0, 26112.0 / 32768.0, 21248.0 / 32768.0, 16384.0 / 32768.0 };
const beta_coef = [4]f32{ 30147.0 / 32768.0, 22282.0 / 32768.0, 12124.0 / 32768.0, 6554.0 / 32768.0 };
const beta_intra: f32 = 4915.0 / 32768.0;
const ordery_table = [30]usize{
    1,  0,
    3,  0,
    2,  1,
    7,  0,
    4,  3,
    6,  1,
    5,  2,
    15, 0,
    8,  7,
    12, 3,
    11, 4,
    14, 1,
    9,  6,
    13, 2,
    10, 5,
};
const bit_interleave_table = [16]u32{ 0, 1, 1, 1, 2, 3, 3, 3, 2, 3, 3, 3, 2, 3, 3, 3 };
const bit_deinterleave_table = [16]u32{
    0x00, 0x03, 0x0C, 0x0F, 0x30, 0x33, 0x3C, 0x3F,
    0xC0, 0xC3, 0xCC, 0xCF, 0xF0, 0xF3, 0xFC, 0xFF,
};
const e_prob_model = [4][2][42]u8{
    .{
        .{ 72, 127, 65, 129, 66, 128, 65, 128, 64, 128, 62, 128, 64, 128, 64, 128, 92, 78, 92, 79, 92, 78, 90, 79, 116, 41, 115, 40, 114, 40, 132, 26, 132, 26, 145, 17, 161, 12, 176, 10, 177, 11 },
        .{ 24, 179, 48, 138, 54, 135, 54, 132, 53, 134, 56, 133, 55, 132, 55, 132, 61, 114, 70, 96, 74, 88, 75, 88, 87, 74, 89, 66, 91, 67, 100, 59, 108, 50, 120, 40, 122, 37, 97, 43, 78, 50 },
    },
    .{
        .{ 83, 78, 84, 81, 88, 75, 86, 74, 87, 71, 90, 73, 93, 74, 93, 74, 109, 40, 114, 36, 117, 34, 117, 34, 143, 17, 145, 18, 146, 19, 162, 12, 165, 10, 178, 7, 189, 6, 190, 8, 177, 9 },
        .{ 23, 178, 54, 115, 63, 102, 66, 98, 69, 99, 74, 89, 71, 91, 73, 91, 78, 89, 86, 80, 92, 66, 93, 64, 102, 59, 103, 60, 104, 60, 117, 52, 123, 44, 138, 35, 133, 31, 97, 38, 77, 45 },
    },
    .{
        .{ 61, 90, 93, 60, 105, 42, 107, 41, 110, 45, 116, 38, 113, 38, 112, 38, 124, 26, 132, 27, 136, 19, 140, 20, 155, 14, 159, 16, 158, 18, 170, 13, 177, 10, 187, 8, 192, 6, 175, 9, 159, 10 },
        .{ 21, 178, 59, 110, 71, 86, 75, 85, 84, 83, 91, 66, 88, 73, 87, 72, 92, 75, 98, 72, 105, 58, 107, 54, 115, 52, 114, 55, 112, 56, 129, 51, 132, 40, 150, 33, 140, 29, 98, 35, 77, 42 },
    },
    .{
        .{ 42, 121, 96, 66, 108, 43, 111, 40, 117, 44, 123, 32, 120, 36, 119, 33, 127, 33, 134, 34, 139, 21, 147, 23, 152, 20, 158, 25, 154, 26, 166, 21, 173, 16, 184, 13, 184, 10, 150, 13, 139, 15 },
        .{ 22, 178, 63, 114, 74, 82, 84, 83, 92, 82, 103, 62, 96, 72, 96, 67, 101, 73, 107, 72, 113, 55, 118, 52, 125, 52, 118, 52, 117, 55, 135, 49, 137, 39, 157, 32, 145, 29, 97, 33, 77, 40 },
    },
};

const band_allocation = [11][21]u8{
    .{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 },
    .{ 90, 80, 75, 69, 63, 56, 49, 40, 34, 29, 20, 18, 10, 0, 0, 0, 0, 0, 0, 0, 0 },
    .{ 110, 100, 90, 84, 78, 71, 65, 58, 51, 45, 39, 32, 26, 20, 12, 0, 0, 0, 0, 0, 0 },
    .{ 118, 110, 103, 93, 86, 80, 75, 70, 65, 59, 53, 47, 40, 31, 23, 15, 4, 0, 0, 0, 0 },
    .{ 126, 119, 112, 104, 95, 89, 83, 78, 72, 66, 60, 54, 47, 39, 32, 25, 17, 12, 1, 0, 0 },
    .{ 134, 127, 120, 114, 103, 97, 91, 85, 78, 72, 66, 60, 54, 47, 41, 35, 29, 23, 16, 10, 1 },
    .{ 144, 137, 130, 124, 113, 107, 101, 95, 88, 82, 76, 70, 64, 57, 51, 45, 39, 33, 26, 15, 1 },
    .{ 152, 145, 138, 132, 123, 117, 111, 105, 98, 92, 86, 80, 74, 67, 61, 55, 49, 43, 36, 20, 1 },
    .{ 162, 155, 148, 142, 133, 127, 121, 115, 108, 102, 96, 90, 84, 77, 71, 65, 59, 53, 46, 30, 1 },
    .{ 172, 165, 158, 152, 143, 137, 131, 125, 118, 112, 106, 100, 94, 87, 81, 75, 69, 63, 56, 45, 20 },
    .{ 200, 200, 200, 200, 200, 200, 200, 200, 198, 193, 188, 183, 178, 173, 168, 163, 158, 153, 148, 129, 104 },
};

const exp2_table8 = [8]i32{ 16384, 17866, 19483, 21247, 23170, 25267, 27554, 30048 };
const comb_gains = [3][3]f32{
    .{ 0.3066406250, 0.2170410156, 0.1296386719 },
    .{ 0.4638671875, 0.2680664062, 0.0 },
    .{ 0.7998046875, 0.1000976562, 0.0 },
};

const max_frame: usize = short_mdct_size << max_lm;
const max_n4: usize = (2 * max_frame) / 4;
const max_band_n: usize = @intCast((e_bands[21] - e_bands[20]) << max_lm);

const Complex = struct { re: f32, im: f32 };

/// Tables shared by every decoder: forward-FFT twiddles for the N/4 point
/// transforms, the MDCT pre/post rotation for every block size and the
/// overlap window.
const Tables = struct {
    twiddle: [max_n4]Complex,
    trig: [4][2 * max_n4]f32,
    window: [overlap]f32,

    fn build() Tables {
        var t: Tables = undefined;
        for (0..max_n4) |k| {
            const angle = -2.0 * std.math.pi * @as(f64, @floatFromInt(k)) / @as(f64, @floatFromInt(max_n4));
            t.twiddle[k] = .{ .re = @floatCast(@cos(angle)), .im = @floatCast(@sin(angle)) };
        }
        for (0..4) |shift| {
            const n = (2 * max_frame) >> @intCast(shift);
            const n4 = n / 4;
            @memset(&t.trig[shift], 0);
            for (0..n4) |i| {
                const angle = 2.0 * std.math.pi * (@as(f64, @floatFromInt(i)) + 0.125) / @as(f64, @floatFromInt(n));
                t.trig[shift][2 * i] = @floatCast(-@sin(angle));
                t.trig[shift][2 * i + 1] = @floatCast(@cos(angle));
            }
        }
        for (0..overlap) |i| {
            const phase = (@as(f64, @floatFromInt(i)) + 0.5) / @as(f64, @floatFromInt(overlap));
            const inner = @sin(0.5 * std.math.pi * phase);
            t.window[i] = @floatCast(@sin(0.5 * std.math.pi * inner * inner));
        }
        return t;
    }
};

var tables_lock: std.atomic.Mutex = .unlocked;
var tables_ready: bool = false;
var tables: Tables = undefined;

fn sharedTables() *const Tables {
    while (!tables_lock.tryLock()) std.atomic.spinLoopHint();
    defer tables_lock.unlock();
    if (!tables_ready) {
        tables = Tables.build();
        tables_ready = true;
    }
    return &tables;
}

pub fn window() *const [overlap]f32 {
    return &sharedTables().window;
}

pub const Decoder = struct {
    channels: u8,
    start: usize = 0,
    end: usize = nb_ebands,
    decode_mem: [2][decode_buffer_size + overlap]f32,
    old_band_e: [2 * nb_ebands]f32,
    old_log_e: [2 * nb_ebands]f32,
    old_log_e2: [2 * nb_ebands]f32,
    background_log_e: [2 * nb_ebands]f32,
    preemph_mem: [2]f32,
    postfilter_period: i32,
    postfilter_period_old: i32,
    postfilter_gain: f32,
    postfilter_gain_old: f32,
    postfilter_tapset: u8,
    postfilter_tapset_old: u8,
    rng: u32,
    loss_duration: i32,

    pub fn init(channels: u8) !Decoder {
        if (channels != 1 and channels != 2) return error.UnsupportedAudioFormat;
        var d: Decoder = undefined;
        d.channels = channels;
        d.start = 0;
        d.end = nb_ebands;
        d.reset();
        return d;
    }

    pub fn reset(self: *Decoder) void {
        for (&self.decode_mem) |*mem| @memset(mem, 0);
        @memset(&self.old_band_e, 0);
        @memset(&self.old_log_e, -28.0);
        @memset(&self.old_log_e2, -28.0);
        @memset(&self.background_log_e, 0);
        self.preemph_mem = .{ 0, 0 };
        self.postfilter_period = 0;
        self.postfilter_period_old = 0;
        self.postfilter_gain = 0;
        self.postfilter_gain_old = 0;
        self.postfilter_tapset = 0;
        self.postfilter_tapset_old = 0;
        self.rng = 0;
        self.loss_duration = 0;
    }

    /// Decodes one CELT frame of `frame_size` samples per channel from the
    /// range decoder `dec` (already positioned past any SILK data) and
    /// writes interleaved float PCM to `pcm`. `len` is the byte length of
    /// the whole frame the range decoder was initialised over.
    pub fn decodeFrame(self: *Decoder, len: usize, dec: *RangeDecoder, frame_size: usize, pcm: []f32, stream_channels: u8) !void {
        if (stream_channels != 1 and stream_channels != 2) return error.UnsupportedAudioFormat;
        const c_count: usize = stream_channels;
        const cc_count: usize = self.channels;
        var lm: u2 = 0;
        while (true) : (lm += 1) {
            if (short_mdct_size << lm == frame_size) break;
            if (lm == max_lm) return error.UnsupportedAudioFormat;
        }
        const m: usize = @as(usize, 1) << lm;
        const n: usize = m * short_mdct_size;
        if (pcm.len < n * cc_count) return error.UnsupportedAudioFormat;
        if (len > 1275) return error.UnsupportedAudioFormat;
        const start = self.start;
        const end = self.end;
        if (start >= end or end > nb_ebands) return error.UnsupportedAudioFormat;
        const eff_end = end;
        const t = sharedTables();

        const old_band_e = &self.old_band_e;
        const old_log_e = &self.old_log_e;
        const old_log_e2 = &self.old_log_e2;

        if (c_count == 1) {
            for (0..nb_ebands) |i| old_band_e[i] = @max(old_band_e[i], old_band_e[nb_ebands + i]);
        }

        var total_bits: i32 = @intCast(len * 8);
        var tell: i32 = @intCast(dec.tell());
        var silence = false;
        if (tell >= total_bits) {
            silence = true;
        } else if (tell == 1) {
            silence = (try dec.decodeBitLogp(15)) != 0;
        }
        if (silence) {
            tell = @intCast(len * 8);
            dec.nbits_total += @intCast(tell - @as(i32, @intCast(dec.tell())));
        }

        var postfilter_gain: f32 = 0;
        var postfilter_pitch: i32 = 0;
        var postfilter_tapset: u8 = 0;
        if (start == 0 and tell + 16 <= total_bits) {
            if ((try dec.decodeBitLogp(1)) != 0) {
                const octave: u5 = @intCast(try dec.decodeUint(6));
                postfilter_pitch = (@as(i32, 16) << octave) + @as(i32, @intCast(try dec.readRawBits(4 + @as(usize, octave)))) - 1;
                const qg: i32 = @intCast(try dec.readRawBits(3));
                if (@as(i32, @intCast(dec.tell())) + 2 <= total_bits) {
                    postfilter_tapset = try dec.decodeIcdfBits(&tapset_icdf, 2);
                }
                postfilter_gain = 0.09375 * @as(f32, @floatFromInt(qg + 1));
            }
            tell = @intCast(dec.tell());
        }

        var is_transient = false;
        if (lm > 0 and tell + 3 <= total_bits) {
            is_transient = (try dec.decodeBitLogp(3)) != 0;
            tell = @intCast(dec.tell());
        }
        const short_blocks: usize = if (is_transient) m else 0;

        const intra_ener = if (tell + 3 <= total_bits) (try dec.decodeBitLogp(3)) != 0 else false;

        try unquantCoarseEnergy(start, end, old_band_e, intra_ener, dec, len, c_count, lm);

        var tf_res: [nb_ebands]i32 = [_]i32{0} ** nb_ebands;
        try tfDecode(start, end, is_transient, &tf_res, lm, dec, len);

        tell = @intCast(dec.tell());
        var spread_decision: i32 = spread_normal;
        if (tell + 4 <= total_bits) spread_decision = try dec.decodeIcdfBits(&spread_icdf, 5);

        var cap: [nb_ebands]i32 = undefined;
        for (0..nb_ebands) |i| cap[i] = bandCap(lm, c_count, i);

        var offsets: [nb_ebands]i32 = [_]i32{0} ** nb_ebands;
        var dynalloc_logp: u5 = 6;
        total_bits <<= bitres;
        var tell_frac: i32 = @intCast(dec.tellFrac());
        for (start..end) |i| {
            const width: i32 = @as(i32, @intCast(c_count)) * (e_bands[i + 1] - e_bands[i]) << lm;
            const quanta: i32 = @min(width << bitres, @max(@as(i32, 6) << bitres, width));
            var dynalloc_loop_logp = dynalloc_logp;
            var boost: i32 = 0;
            while (tell_frac + (@as(i32, dynalloc_loop_logp) << bitres) < total_bits and boost < cap[i]) {
                const flag = try dec.decodeBitLogp(dynalloc_loop_logp);
                tell_frac = @intCast(dec.tellFrac());
                if (flag == 0) break;
                boost += quanta;
                total_bits -= quanta;
                dynalloc_loop_logp = 1;
            }
            offsets[i] = boost;
            if (boost > 0) dynalloc_logp = @max(2, dynalloc_logp - 1);
        }

        var alloc_trim: i32 = 5;
        if (tell_frac + (@as(i32, 6) << bitres) <= total_bits) alloc_trim = try dec.decodeIcdfBits(&trim_icdf, 7);

        var bits: i32 = (@as(i32, @intCast(len * 8)) << bitres) - @as(i32, @intCast(dec.tellFrac())) - 1;
        const anti_collapse_rsv: i32 = if (is_transient and lm >= 2 and bits >= (@as(i32, lm) + 2) << bitres) @as(i32, 1) << bitres else 0;
        bits -= anti_collapse_rsv;

        var pulses: [nb_ebands]i32 = [_]i32{0} ** nb_ebands;
        var fine_quant: [nb_ebands]i32 = [_]i32{0} ** nb_ebands;
        var fine_priority: [nb_ebands]i32 = [_]i32{0} ** nb_ebands;
        var intensity: i32 = 0;
        var dual_stereo: i32 = 0;
        var balance: i32 = 0;
        const coded_bands = try computeAllocation(start, end, &offsets, &cap, alloc_trim, &intensity, &dual_stereo, bits, &balance, &pulses, &fine_quant, &fine_priority, c_count, lm, dec);

        try unquantFineEnergy(start, end, old_band_e, &fine_quant, dec, len, c_count);

        // Shift the decode memory.
        for (0..cc_count) |c| {
            const mem = &self.decode_mem[c];
            std.mem.copyForwards(f32, mem[0 .. decode_buffer_size - n + overlap], mem[n .. decode_buffer_size + overlap]);
        }

        var x_storage: [2 * max_frame]f32 = undefined;
        const x_all = x_storage[0 .. c_count * n];
        var collapse_masks: [2 * nb_ebands]u8 = [_]u8{0} ** (2 * nb_ebands);
        var seed = self.rng;
        try quantAllBands(
            start,
            end,
            x_all[0..n],
            if (c_count == 2) x_all[n .. 2 * n] else null,
            &collapse_masks,
            &pulses,
            short_blocks,
            spread_decision,
            dual_stereo,
            intensity,
            &tf_res,
            @as(i32, @intCast(len)) * (@as(i32, 8) << bitres) - anti_collapse_rsv,
            balance,
            dec,
            lm,
            coded_bands,
            &seed,
        );

        var anti_collapse_on = false;
        if (anti_collapse_rsv > 0) anti_collapse_on = (try dec.readRawBits(1)) != 0;

        unquantEnergyFinalise(start, end, old_band_e, &fine_quant, &fine_priority, @as(i32, @intCast(len * 8)) - @as(i32, @intCast(dec.tell())), dec, c_count) catch {};

        if (anti_collapse_on) {
            antiCollapse(x_all, &collapse_masks, lm, c_count, n, start, end, old_band_e, old_log_e, old_log_e2, &pulses, seed);
        }

        if (silence) {
            for (0..c_count * nb_ebands) |i| old_band_e[i] = -28.0;
        }

        // Synthesis into the decode memory (celt_synthesis): a mono stream
        // feeding a stereo decoder is duplicated, a stereo stream feeding a
        // mono decoder is downmixed in the frequency domain.
        {
            const b_count: usize = if (is_transient) m else 1;
            const nb: usize = if (is_transient) short_mdct_size else n;
            const shift: usize = if (is_transient) max_lm else max_lm - lm;
            var freq: [max_frame]f32 = undefined;
            var freq2: [max_frame]f32 = undefined;
            if (cc_count == 1 and c_count == 2) {
                denormaliseBands(x_all[0..n], freq[0..n], old_band_e[0..nb_ebands], start, eff_end, m, silence);
                denormaliseBands(x_all[n .. 2 * n], freq2[0..n], old_band_e[nb_ebands .. 2 * nb_ebands], start, eff_end, m, silence);
                for (0..n) |i| freq[i] = 0.5 * freq[i] + 0.5 * freq2[i];
                const out_syn = self.decode_mem[0][decode_buffer_size - n .. decode_buffer_size + overlap];
                for (0..b_count) |b| mdctBackward(t, freq[0..n], b, b_count, out_syn[nb * b ..], shift);
            } else {
                for (0..cc_count) |c| {
                    const src: usize = if (c_count == 1) 0 else c;
                    const out_syn = self.decode_mem[c][decode_buffer_size - n .. decode_buffer_size + overlap];
                    denormaliseBands(x_all[src * n .. (src + 1) * n], freq[0..n], old_band_e[src * nb_ebands .. (src + 1) * nb_ebands], start, eff_end, m, silence);
                    for (0..b_count) |b| mdctBackward(t, freq[0..n], b, b_count, out_syn[nb * b ..], shift);
                }
            }
        }

        for (0..cc_count) |c| {
            self.postfilter_period = @max(self.postfilter_period, combfilter_minperiod);
            self.postfilter_period_old = @max(self.postfilter_period_old, combfilter_minperiod);
            const base = decode_buffer_size - n;
            combFilter(&self.decode_mem[c], base, self.postfilter_period_old, self.postfilter_period, short_mdct_size, self.postfilter_gain_old, self.postfilter_gain, self.postfilter_tapset_old, self.postfilter_tapset, &t.window);
            if (lm != 0) {
                combFilter(&self.decode_mem[c], base + short_mdct_size, self.postfilter_period, postfilter_pitch, n - short_mdct_size, self.postfilter_gain, postfilter_gain, self.postfilter_tapset, postfilter_tapset, &t.window);
            }
        }
        self.postfilter_period_old = self.postfilter_period;
        self.postfilter_gain_old = self.postfilter_gain;
        self.postfilter_tapset_old = self.postfilter_tapset;
        self.postfilter_period = postfilter_pitch;
        self.postfilter_gain = postfilter_gain;
        self.postfilter_tapset = postfilter_tapset;
        if (lm != 0) {
            self.postfilter_period_old = self.postfilter_period;
            self.postfilter_gain_old = self.postfilter_gain;
            self.postfilter_tapset_old = self.postfilter_tapset;
        }

        if (c_count == 1) @memcpy(old_band_e[nb_ebands .. 2 * nb_ebands], old_band_e[0..nb_ebands]);

        if (!is_transient) {
            @memcpy(old_log_e2, old_log_e);
            @memcpy(old_log_e, old_band_e);
        } else {
            for (0..2 * nb_ebands) |i| old_log_e[i] = @min(old_log_e[i], old_band_e[i]);
        }
        const max_background_increase: f32 = @as(f32, @floatFromInt(@min(160, self.loss_duration + @as(i32, @intCast(m))))) * 0.001;
        for (0..2 * nb_ebands) |i| {
            self.background_log_e[i] = @min(self.background_log_e[i] + max_background_increase, old_band_e[i]);
        }
        for (0..2) |c| {
            for (0..start) |i| {
                old_band_e[c * nb_ebands + i] = 0;
                old_log_e[c * nb_ebands + i] = -28.0;
                old_log_e2[c * nb_ebands + i] = -28.0;
            }
            for (end..nb_ebands) |i| {
                old_band_e[c * nb_ebands + i] = 0;
                old_log_e[c * nb_ebands + i] = -28.0;
                old_log_e2[c * nb_ebands + i] = -28.0;
            }
        }
        self.rng = dec.rng;

        // De-emphasis to interleaved float PCM.
        for (0..cc_count) |c| {
            const x = self.decode_mem[c][decode_buffer_size - n ..];
            var mem = self.preemph_mem[c];
            for (0..n) |j| {
                const tmp = x[j] + very_small + mem;
                mem = preemph_coef * tmp;
                pcm[j * cc_count + c] = tmp * sig_scale;
            }
            self.preemph_mem[c] = mem;
        }
        self.loss_duration = 0;

        if (dec.tell() > 8 * len) return error.UnsupportedAudioFormat;
    }
};

// ---------------------------------------------------------------------------
// Energy

fn laplaceGetFreq1(fs0: i32, decay: i32) i32 {
    const ft = 32768 - 32 - fs0;
    return @intCast((@as(i64, ft) * @as(i64, 16384 - decay)) >> 15);
}

fn laplaceDecode(dec: *RangeDecoder, fs_in: i32, decay: i32) !i32 {
    var fs = fs_in;
    var val: i32 = 0;
    const fm: i32 = @intCast(try dec.getFrequency32(32768));
    var fl: i32 = 0;
    if (fm >= fs) {
        val += 1;
        fl = fs;
        fs = laplaceGetFreq1(fs, decay) + 1;
        while (fs > 1 and fm >= fl + 2 * fs) {
            fs *= 2;
            fl += fs;
            fs = @intCast((@as(i64, fs - 2) * @as(i64, decay)) >> 15);
            fs += 1;
            val += 1;
        }
        if (fs <= 1) {
            const di = (fm - fl) >> 1;
            val += di;
            fl += 2 * di;
        }
        if (fm < fl + fs) {
            val = -val;
        } else {
            fl += fs;
        }
    }
    if (!(fl < 32768 and fs > 0 and fl <= fm and fm < @min(fl + fs, 32768))) return error.UnsupportedAudioFormat;
    try dec.update32(@intCast(fl), @intCast(@min(fl + fs, 32768)), 32768);
    return val;
}

fn unquantCoarseEnergy(start: usize, end: usize, old_e: *[2 * nb_ebands]f32, intra: bool, dec: *RangeDecoder, len: usize, c_count: usize, lm: u2) !void {
    const prob_model = &e_prob_model[lm][@intFromBool(intra)];
    var prev = [2]f32{ 0, 0 };
    const coef: f32 = if (intra) 0 else pred_coef[lm];
    const beta: f32 = if (intra) beta_intra else beta_coef[lm];
    const budget: i32 = @intCast(len * 8);
    for (start..end) |i| {
        for (0..c_count) |c| {
            const tell: i32 = @intCast(dec.tell());
            var qi: i32 = 0;
            if (budget - tell >= 15) {
                const pi: usize = 2 * @as(usize, @min(i, 20));
                qi = try laplaceDecode(dec, @as(i32, prob_model[pi]) << 7, @as(i32, prob_model[pi + 1]) << 6);
            } else if (budget - tell >= 2) {
                const s: i32 = try dec.decodeIcdfBits(&small_energy_icdf, 2);
                qi = (s >> 1) ^ -(s & 1);
            } else if (budget - tell >= 1) {
                qi = -@as(i32, try dec.decodeBitLogp(1));
            } else {
                qi = -1;
            }
            const q: f32 = @floatFromInt(qi);
            const idx = i + c * nb_ebands;
            old_e[idx] = @max(-9.0, old_e[idx]);
            old_e[idx] = coef * old_e[idx] + prev[c] + q;
            prev[c] = prev[c] + q - beta * q;
        }
    }
}

fn unquantFineEnergy(start: usize, end: usize, old_e: *[2 * nb_ebands]f32, fine_quant: *const [nb_ebands]i32, dec: *RangeDecoder, len: usize, c_count: usize) !void {
    for (start..end) |i| {
        if (fine_quant[i] <= 0) continue;
        if (@as(i32, @intCast(dec.tell())) + @as(i32, @intCast(c_count)) * fine_quant[i] > @as(i32, @intCast(len * 8))) continue;
        const extra: u5 = @intCast(fine_quant[i]);
        for (0..c_count) |c| {
            const q2: f32 = @floatFromInt(try dec.readRawBits(extra));
            const offset = (q2 + 0.5) * @as(f32, @floatFromInt(@as(i32, 1) << (14 - extra))) * (1.0 / 16384.0) - 0.5;
            old_e[i + c * nb_ebands] += offset;
        }
    }
}

fn unquantEnergyFinalise(start: usize, end: usize, old_e: *[2 * nb_ebands]f32, fine_quant: *const [nb_ebands]i32, fine_priority: *const [nb_ebands]i32, bits_left_in: i32, dec: *RangeDecoder, c_count: usize) !void {
    var bits_left = bits_left_in;
    for (0..2) |prio| {
        var i = start;
        while (i < end and bits_left >= @as(i32, @intCast(c_count))) : (i += 1) {
            if (fine_quant[i] >= max_fine_bits or fine_priority[i] != @as(i32, @intCast(prio))) continue;
            for (0..c_count) |c| {
                const q2: f32 = @floatFromInt(try dec.readRawBits(1));
                const shift: u5 = @intCast(14 - fine_quant[i] - 1);
                const offset = (q2 - 0.5) * @as(f32, @floatFromInt(@as(i32, 1) << shift)) * (1.0 / 16384.0);
                old_e[i + c * nb_ebands] += offset;
                bits_left -= 1;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Transient / time-frequency decisions

fn tfDecode(start: usize, end: usize, is_transient: bool, tf_res: *[nb_ebands]i32, lm: u2, dec: *RangeDecoder, len: usize) !void {
    var budget: i32 = @intCast(len * 8);
    var tell: i32 = @intCast(dec.tell());
    var logp: u5 = if (is_transient) 2 else 4;
    const tf_select_rsv = lm > 0 and tell + @as(i32, logp) + 1 <= budget;
    budget -= @intFromBool(tf_select_rsv);
    var tf_changed: i32 = 0;
    var curr: i32 = 0;
    for (start..end) |i| {
        if (tell + @as(i32, logp) <= budget) {
            curr ^= @as(i32, try dec.decodeBitLogp(logp));
            tell = @intCast(dec.tell());
            tf_changed |= curr;
        }
        tf_res[i] = curr;
        logp = if (is_transient) 4 else 5;
    }
    var tf_select: usize = 0;
    const t_idx: usize = 4 * @as(usize, @intFromBool(is_transient));
    if (tf_select_rsv and tf_select_table[lm][t_idx + @as(usize, @intCast(tf_changed))] != tf_select_table[lm][t_idx + 2 + @as(usize, @intCast(tf_changed))]) {
        tf_select = try dec.decodeBitLogp(1);
    }
    for (start..end) |i| {
        tf_res[i] = tf_select_table[lm][t_idx + 2 * tf_select + @as(usize, @intCast(tf_res[i]))];
    }
}

// ---------------------------------------------------------------------------
// Bit allocation

// Pulse cache of the standard mode, copied from libopus static_modes_float.h.
const cache_index50 = [105]i16{
    -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  0,   0,   0,   0,   41,  41,  41,  82,  82,  123, 164, 200, 222,
    0,   0,   0,   0,   0,   0,   0,   0,   41,  41,  41,  41,  123, 123, 123, 164, 164, 240, 266, 283, 295,
    41,  41,  41,  41,  41,  41,  41,  41,  123, 123, 123, 123, 240, 240, 240, 266, 266, 305, 318, 328, 336,
    123, 123, 123, 123, 123, 123, 123, 123, 240, 240, 240, 240, 305, 305, 305, 318, 318, 343, 351, 358, 364,
    240, 240, 240, 240, 240, 240, 240, 240, 305, 305, 305, 305, 343, 343, 343, 351, 351, 370, 376, 382, 387,
};
const cache_bits50 = [392]u8{
    40,  7,   7,   7,   7,   7,   7,   7,   7,   7,   7,   7,   7,   7,   7,   7,   7,   7,   7,   7,   7,   7,   7,   7,   7,   7,   7,   7,
    7,   7,   7,   7,   7,   7,   7,   7,   7,   7,   7,   7,   7,   40,  15,  23,  28,  31,  34,  36,  38,  39,  41,  42,  43,  44,  45,  46,
    47,  47,  49,  50,  51,  52,  53,  54,  55,  55,  57,  58,  59,  60,  61,  62,  63,  63,  65,  66,  67,  68,  69,  70,  71,  71,  40,  20,
    33,  41,  48,  53,  57,  61,  64,  66,  69,  71,  73,  75,  76,  78,  80,  82,  85,  87,  89,  91,  92,  94,  96,  98,  101, 103, 105, 107,
    108, 110, 112, 114, 117, 119, 121, 123, 124, 126, 128, 40,  23,  39,  51,  60,  67,  73,  79,  83,  87,  91,  94,  97,  100, 102, 105, 107,
    111, 115, 118, 121, 124, 126, 129, 131, 135, 139, 142, 145, 148, 150, 153, 155, 159, 163, 166, 169, 172, 174, 177, 179, 35,  28,  49,  65,
    78,  89,  99,  107, 114, 120, 126, 132, 136, 141, 145, 149, 153, 159, 165, 171, 176, 180, 185, 189, 192, 199, 205, 211, 216, 220, 225, 229,
    232, 239, 245, 251, 21,  33,  58,  79,  97,  112, 125, 137, 148, 157, 166, 174, 182, 189, 195, 201, 207, 217, 227, 235, 243, 251, 17,  35,
    63,  86,  106, 123, 139, 152, 165, 177, 187, 197, 206, 214, 222, 230, 237, 250, 25,  31,  55,  75,  91,  105, 117, 128, 138, 146, 154, 161,
    168, 174, 180, 185, 190, 200, 208, 215, 222, 229, 235, 240, 245, 255, 16,  36,  65,  89,  110, 128, 144, 159, 173, 185, 196, 207, 217, 226,
    234, 242, 250, 11,  41,  74,  103, 128, 151, 172, 191, 209, 225, 241, 255, 9,   43,  79,  110, 138, 163, 186, 207, 227, 246, 12,  39,  71,
    99,  123, 144, 164, 182, 198, 214, 228, 241, 253, 9,   44,  81,  113, 142, 168, 192, 214, 235, 255, 7,   49,  90,  127, 160, 191, 220, 247,
    6,   51,  95,  134, 170, 203, 234, 7,   47,  87,  123, 155, 184, 212, 237, 6,   52,  97,  137, 174, 208, 240, 5,   57,  106, 151, 192, 231,
    5,   59,  111, 158, 202, 243, 5,   55,  103, 147, 187, 224, 5,   60,  113, 161, 206, 248, 4,   65,  122, 175, 224, 4,   67,  127, 182, 234,
};
const cache_caps50 = [168]u8{
    224, 224, 224, 224, 224, 224, 224, 224, 160, 160, 160, 160, 185, 185, 185, 178, 178, 168, 134, 61, 37,
    224, 224, 224, 224, 224, 224, 224, 224, 240, 240, 240, 240, 207, 207, 207, 198, 198, 183, 144, 66, 40,
    160, 160, 160, 160, 160, 160, 160, 160, 185, 185, 185, 185, 193, 193, 193, 183, 183, 172, 138, 64, 38,
    240, 240, 240, 240, 240, 240, 240, 240, 207, 207, 207, 207, 204, 204, 204, 193, 193, 180, 143, 66, 40,
    185, 185, 185, 185, 185, 185, 185, 185, 193, 193, 193, 193, 193, 193, 193, 183, 183, 172, 138, 65, 39,
    207, 207, 207, 207, 207, 207, 207, 207, 204, 204, 204, 204, 201, 201, 201, 188, 188, 176, 141, 66, 40,
    193, 193, 193, 193, 193, 193, 193, 193, 193, 193, 193, 193, 194, 194, 194, 184, 184, 173, 139, 65, 39,
    204, 204, 204, 204, 204, 204, 204, 204, 201, 201, 201, 201, 198, 198, 198, 187, 187, 175, 140, 66, 40,
};

fn cacheOffset(lm_plus_one: usize, band: usize) ?usize {
    const idx = cache_index50[lm_plus_one * nb_ebands + band];
    if (idx < 0) return null;
    return @intCast(idx);
}

fn cacheCount(lm_plus_one: usize, band: usize) i32 {
    const off = cacheOffset(lm_plus_one, band) orelse return 0;
    return cache_bits50[off];
}

fn cacheBits(lm_plus_one: usize, band: usize, k: usize) i32 {
    const off = cacheOffset(lm_plus_one, band) orelse return 0;
    return cache_bits50[off + k];
}

fn bandCap(lm: u2, c_count: usize, band: usize) i32 {
    const n: i32 = (e_bands[band + 1] - e_bands[band]) << lm;
    const cap: i32 = cache_caps50[(2 * @as(usize, lm) + (c_count - 1)) * nb_ebands + band];
    return ((cap + 64) * @as(i32, @intCast(c_count)) * n) >> 2;
}

fn bits2Pulses(band: usize, lm: i32, bits_in: i32) i32 {
    const lm_plus_one: usize = @intCast(lm + 1);
    var lo: i32 = 0;
    var hi: i32 = cacheCount(lm_plus_one, band);
    const bits = bits_in - 1;
    for (0..log_max_pseudo) |_| {
        const mid = (lo + hi + 1) >> 1;
        if (cacheBits(lm_plus_one, band, @intCast(mid)) >= bits) {
            hi = mid;
        } else {
            lo = mid;
        }
    }
    const lo_bits: i32 = if (lo == 0) -1 else cacheBits(lm_plus_one, band, @intCast(lo));
    if (bits - lo_bits <= cacheBits(lm_plus_one, band, @intCast(hi)) - bits) return lo;
    return hi;
}

fn pulses2Bits(band: usize, lm: i32, pulses: i32) i32 {
    if (pulses == 0) return 0;
    return cacheBits(@intCast(lm + 1), band, @intCast(pulses)) + 1;
}

fn getPulses(i: i32) i32 {
    if (i < 8) return i;
    return (8 + (i & 7)) << @intCast((i >> 3) - 1);
}

fn interpBits2Pulses(
    start: usize,
    end: usize,
    skip_start: usize,
    bits1: *const [nb_ebands]i32,
    bits2: *const [nb_ebands]i32,
    thresh: *const [nb_ebands]i32,
    cap: *const [nb_ebands]i32,
    total_in: i32,
    balance_out: *i32,
    skip_rsv: i32,
    intensity: *i32,
    intensity_rsv_in: i32,
    dual_stereo: *i32,
    dual_stereo_rsv_in: i32,
    bits: *[nb_ebands]i32,
    ebits: *[nb_ebands]i32,
    fine_priority: *[nb_ebands]i32,
    c_count: usize,
    lm: u2,
    dec: *RangeDecoder,
) !usize {
    var total = total_in;
    var intensity_rsv = intensity_rsv_in;
    var dual_stereo_rsv = dual_stereo_rsv_in;
    const c: i32 = @intCast(c_count);
    const alloc_floor: i32 = c << bitres;
    const stereo: u1 = @intFromBool(c_count > 1);
    const log_m: i32 = @as(i32, lm) << bitres;
    var lo: i32 = 0;
    var hi: i32 = @as(i32, 1) << alloc_steps;
    var psum: i32 = 0;
    for (0..alloc_steps) |_| {
        const mid = (lo + hi) >> 1;
        psum = 0;
        var done = false;
        var j = end;
        while (j > start) {
            j -= 1;
            const tmp = bits1[j] + ((mid * bits2[j]) >> alloc_steps);
            if (tmp >= thresh[j] or done) {
                done = true;
                psum += @min(tmp, cap[j]);
            } else if (tmp >= alloc_floor) {
                psum += alloc_floor;
            }
        }
        if (psum > total) hi = mid else lo = mid;
    }
    psum = 0;
    var done = false;
    var j = end;
    while (j > start) {
        j -= 1;
        var tmp = bits1[j] + ((lo * bits2[j]) >> alloc_steps);
        if (tmp < thresh[j] and !done) {
            tmp = if (tmp >= alloc_floor) alloc_floor else 0;
        } else {
            done = true;
        }
        tmp = @min(tmp, cap[j]);
        bits[j] = tmp;
        psum += tmp;
    }

    var coded_bands = end;
    while (true) : (coded_bands -= 1) {
        j = coded_bands - 1;
        if (j <= skip_start) {
            total += skip_rsv;
            break;
        }
        var left = total - psum;
        const span = e_bands[coded_bands] - e_bands[start];
        const percoeff = @divTrunc(left, span);
        left -= span * percoeff;
        const rem = @max(left - (e_bands[j] - e_bands[start]), 0);
        const band_width = e_bands[coded_bands] - e_bands[j];
        var band_bits = bits[j] + percoeff * band_width + rem;
        if (band_bits >= @max(thresh[j], alloc_floor + (@as(i32, 1) << bitres))) {
            if ((try dec.decodeBitLogp(1)) != 0) break;
            psum += @as(i32, 1) << bitres;
            band_bits -= @as(i32, 1) << bitres;
        }
        psum -= bits[j] + intensity_rsv;
        if (intensity_rsv > 0) intensity_rsv = log2_frac_table[j - start];
        psum += intensity_rsv;
        if (band_bits >= alloc_floor) {
            psum += alloc_floor;
            bits[j] = alloc_floor;
        } else {
            bits[j] = 0;
        }
    }
    if (coded_bands <= start) return error.UnsupportedAudioFormat;

    if (intensity_rsv > 0) {
        intensity.* = @as(i32, @intCast(start)) + @as(i32, @intCast(try dec.decodeUint(@intCast(coded_bands + 1 - start))));
    } else {
        intensity.* = 0;
    }
    if (intensity.* <= @as(i32, @intCast(start))) {
        total += dual_stereo_rsv;
        dual_stereo_rsv = 0;
    }
    if (dual_stereo_rsv > 0) {
        dual_stereo.* = try dec.decodeBitLogp(1);
    } else {
        dual_stereo.* = 0;
    }

    var left = total - psum;
    const span = e_bands[coded_bands] - e_bands[start];
    const percoeff = @divTrunc(left, span);
    left -= span * percoeff;
    for (start..coded_bands) |jj| bits[jj] += percoeff * (e_bands[jj + 1] - e_bands[jj]);
    for (start..coded_bands) |jj| {
        const tmp = @min(left, e_bands[jj + 1] - e_bands[jj]);
        bits[jj] += tmp;
        left -= tmp;
    }

    var balance: i32 = 0;
    for (start..coded_bands) |jj| {
        const n0 = e_bands[jj + 1] - e_bands[jj];
        const n = n0 << lm;
        const bit = bits[jj] + balance;
        var excess: i32 = 0;
        if (n > 1) {
            excess = @max(bit - cap[jj], 0);
            bits[jj] = bit - excess;
            const den: i32 = c * n + @as(i32, @intFromBool(c_count == 2 and n > 2 and dual_stereo.* == 0 and @as(i32, @intCast(jj)) < intensity.*));
            const nclogn = den * (log_n[jj] + log_m);
            var offset = (nclogn >> 1) - den * fine_offset;
            if (n == 2) offset += (den << bitres) >> 2;
            if (bits[jj] + offset < (den * 2) << bitres) {
                offset += nclogn >> 2;
            } else if (bits[jj] + offset < (den * 3) << bitres) {
                offset += nclogn >> 3;
            }
            ebits[jj] = @max(0, bits[jj] + offset + (den << (bitres - 1)));
            ebits[jj] = @divTrunc(ebits[jj], den) >> bitres;
            if (c * ebits[jj] > (bits[jj] >> bitres)) ebits[jj] = (bits[jj] >> stereo) >> bitres;
            ebits[jj] = @min(ebits[jj], max_fine_bits);
            fine_priority[jj] = @intFromBool(ebits[jj] * (den << bitres) >= bits[jj] + offset);
            bits[jj] -= (c * ebits[jj]) << bitres;
        } else {
            excess = @max(0, bit - (c << bitres));
            bits[jj] = bit - excess;
            ebits[jj] = 0;
            fine_priority[jj] = 1;
        }
        if (excess > 0) {
            const extra_fine = @min(excess >> (stereo + bitres), max_fine_bits - ebits[jj]);
            ebits[jj] += extra_fine;
            const extra_bits = (extra_fine * c) << bitres;
            fine_priority[jj] = @intFromBool(extra_bits >= excess - balance);
            excess -= extra_bits;
        }
        balance = excess;
    }
    balance_out.* = balance;
    for (coded_bands..end) |jj| {
        ebits[jj] = (bits[jj] >> stereo) >> bitres;
        bits[jj] = 0;
        fine_priority[jj] = @intFromBool(ebits[jj] < 1);
    }
    return coded_bands;
}

fn computeAllocation(
    start: usize,
    end: usize,
    offsets: *const [nb_ebands]i32,
    cap: *const [nb_ebands]i32,
    alloc_trim: i32,
    intensity: *i32,
    dual_stereo: *i32,
    total_in: i32,
    balance: *i32,
    pulses: *[nb_ebands]i32,
    ebits: *[nb_ebands]i32,
    fine_priority: *[nb_ebands]i32,
    c_count: usize,
    lm: u2,
    dec: *RangeDecoder,
) !usize {
    const c: i32 = @intCast(c_count);
    var total: i32 = @max(total_in, 0);
    var skip_start = start;
    const skip_rsv: i32 = if (total >= @as(i32, 1) << bitres) @as(i32, 1) << bitres else 0;
    total -= skip_rsv;
    var intensity_rsv: i32 = 0;
    var dual_stereo_rsv: i32 = 0;
    if (c_count == 2) {
        intensity_rsv = log2_frac_table[end - start];
        if (intensity_rsv > total) {
            intensity_rsv = 0;
        } else {
            total -= intensity_rsv;
            dual_stereo_rsv = if (total >= @as(i32, 1) << bitres) @as(i32, 1) << bitres else 0;
            total -= dual_stereo_rsv;
        }
    }
    var bits1: [nb_ebands]i32 = [_]i32{0} ** nb_ebands;
    var bits2: [nb_ebands]i32 = [_]i32{0} ** nb_ebands;
    var thresh: [nb_ebands]i32 = [_]i32{0} ** nb_ebands;
    var trim_offset: [nb_ebands]i32 = [_]i32{0} ** nb_ebands;
    for (start..end) |j| {
        const width = e_bands[j + 1] - e_bands[j];
        thresh[j] = @max(c << bitres, ((3 * width) << lm << bitres) >> 4);
        trim_offset[j] = (c * width * (alloc_trim - 5 - @as(i32, lm)) * (@as(i32, @intCast(end)) - @as(i32, @intCast(j)) - 1) * (@as(i32, 1) << (@as(u5, lm) + bitres))) >> 6;
        if (width << lm == 1) trim_offset[j] -= c << bitres;
    }
    const alloc = &band_allocation;
    var lo: i32 = 1;
    var hi: i32 = @as(i32, @intCast(alloc.len)) - 1;
    while (lo <= hi) {
        var done = false;
        var psum: i32 = 0;
        const mid = (lo + hi) >> 1;
        var j = end;
        while (j > start) {
            j -= 1;
            const width = e_bands[j + 1] - e_bands[j];
            var bitsj: i32 = (c * width * @as(i32, alloc[@intCast(mid)][j]) << lm) >> 2;
            if (bitsj > 0) bitsj = @max(0, bitsj + trim_offset[j]);
            bitsj += offsets[j];
            if (bitsj >= thresh[j] or done) {
                done = true;
                psum += @min(bitsj, cap[j]);
            } else if (bitsj >= c << bitres) {
                psum += c << bitres;
            }
        }
        if (psum > total) hi = mid - 1 else lo = mid + 1;
    }
    hi = lo;
    lo -= 1;
    for (start..end) |j| {
        const width = e_bands[j + 1] - e_bands[j];
        var bits1j: i32 = (c * width * @as(i32, alloc[@intCast(lo)][j]) << lm) >> 2;
        var bits2j: i32 = if (hi >= @as(i32, @intCast(alloc.len))) cap[j] else (c * width * @as(i32, alloc[@intCast(hi)][j]) << lm) >> 2;
        if (bits1j > 0) bits1j = @max(0, bits1j + trim_offset[j]);
        if (bits2j > 0) bits2j = @max(0, bits2j + trim_offset[j]);
        if (lo > 0) bits1j += offsets[j];
        bits2j += offsets[j];
        if (offsets[j] > 0) skip_start = j;
        bits2j = @max(0, bits2j - bits1j);
        bits1[j] = bits1j;
        bits2[j] = bits2j;
    }
    return interpBits2Pulses(start, end, skip_start, &bits1, &bits2, &thresh, cap, total, balance, skip_rsv, intensity, intensity_rsv, dual_stereo, dual_stereo_rsv, pulses, ebits, fine_priority, c_count, lm, dec);
}

// ---------------------------------------------------------------------------
// PVQ

fn ncwrsUrow(n: usize, k: usize, u: []u32) u32 {
    const len = k + 2;
    u[0] = 0;
    u[1] = 1;
    var kk: usize = 2;
    while (kk < len) : (kk += 1) u[kk] = @intCast((kk << 1) - 1);
    var i: usize = 2;
    while (i < n) : (i += 1) unext(u[1..], k + 1, 1);
    return u[k] +% u[k + 1];
}

fn unext(ui: []u32, len: usize, ui0_in: u32) void {
    var ui0 = ui0_in;
    var j: usize = 1;
    while (j < len) : (j += 1) {
        const ui1 = ui[j] +% ui[j - 1] +% ui0;
        ui[j - 1] = ui0;
        ui0 = ui1;
    }
    ui[j - 1] = ui0;
}

fn uprev(ui: []u32, n: usize, ui0_in: u32) void {
    var ui0 = ui0_in;
    var j: usize = 1;
    while (j < n) : (j += 1) {
        const ui1 = ui[j] -% ui[j - 1] -% ui0;
        ui[j - 1] = ui0;
        ui0 = ui1;
    }
    ui[j - 1] = ui0;
}

fn cwrsi(n: usize, k_in: usize, i_in: u32, y: []i32, u: []u32) f32 {
    var k = k_in;
    var i = i_in;
    var yy: f32 = 0;
    for (0..n) |j| {
        var p = u[k + 1];
        const s: bool = i >= p;
        if (s) i -= p;
        var yj: i32 = @intCast(k);
        p = u[k];
        while (p > i) {
            k -= 1;
            p = u[k];
        }
        i -= p;
        yj -= @intCast(k);
        const val: i32 = if (s) -yj else yj;
        y[j] = val;
        yy += @as(f32, @floatFromInt(val)) * @as(f32, @floatFromInt(val));
        uprev(u, k + 2, 0);
    }
    return yy;
}

fn decodePulses(y: []i32, n: usize, k: usize, dec: *RangeDecoder) !f32 {
    var u: [130]u32 = undefined;
    if (k + 2 > u.len) return error.UnsupportedAudioFormat;
    const ft = ncwrsUrow(n, k, u[0 .. k + 2]);
    const idx = try dec.decodeUint(ft);
    return cwrsi(n, k, idx, y, u[0 .. k + 2]);
}

fn expRotation1(x: []f32, len: usize, stride: usize, c: f32, s: f32) void {
    const ms = -s;
    var i: usize = 0;
    while (i + stride < len) : (i += 1) {
        const x1 = x[i];
        const x2 = x[i + stride];
        x[i + stride] = c * x2 + s * x1;
        x[i] = c * x1 + ms * x2;
    }
    if (len < 2 * stride + 1) return;
    var idx: isize = @intCast(len - 2 * stride - 1);
    while (idx >= 0) : (idx -= 1) {
        const j: usize = @intCast(idx);
        const x1 = x[j];
        const x2 = x[j + stride];
        x[j + stride] = c * x2 + s * x1;
        x[j] = c * x1 + ms * x2;
    }
}

fn expRotation(x: []f32, len_in: usize, dir: i32, stride: usize, k: usize, spread: i32) void {
    const spread_factor = [3]usize{ 15, 10, 5 };
    if (2 * k >= len_in or spread == 0) return;
    const factor = spread_factor[@intCast(spread - 1)];
    const gain: f32 = @as(f32, @floatFromInt(len_in)) / @as(f32, @floatFromInt(len_in + factor * k));
    const theta: f32 = 0.5 * gain * gain;
    const c: f32 = @cos(0.5 * std.math.pi * theta);
    const s: f32 = @cos(0.5 * std.math.pi * (1.0 - theta));
    var stride2: usize = 0;
    if (len_in >= 8 * stride) {
        stride2 = 1;
        while ((stride2 * stride2 + stride2) * stride + (stride >> 2) < len_in) stride2 += 1;
    }
    const len = len_in / stride;
    for (0..stride) |i| {
        const seg = x[i * len .. (i + 1) * len];
        if (dir < 0) {
            if (stride2 != 0) expRotation1(seg, len, stride2, s, c);
            expRotation1(seg, len, 1, c, s);
        } else {
            expRotation1(seg, len, 1, c, -s);
            if (stride2 != 0) expRotation1(seg, len, stride2, s, -c);
        }
    }
}

fn normaliseResidual(iy: []const i32, x: []f32, n: usize, ryy: f32, gain: f32) void {
    const g = gain / @sqrt(ryy);
    for (0..n) |i| x[i] = @as(f32, @floatFromInt(iy[i])) * g;
}

fn extractCollapseMask(iy: []const i32, n: usize, b: usize) u32 {
    if (b <= 1) return 1;
    const n0 = n / b;
    var mask: u32 = 0;
    for (0..b) |i| {
        var tmp: i32 = 0;
        for (0..n0) |j| tmp |= iy[i * n0 + j];
        if (tmp != 0) mask |= @as(u32, 1) << @intCast(i);
    }
    return mask;
}

fn algUnquant(x: []f32, n: usize, k: usize, spread: i32, b: usize, dec: *RangeDecoder, gain: f32) !u32 {
    if (k == 0 or n < 2 or n > max_band_n) return error.UnsupportedAudioFormat;
    var iy: [max_band_n]i32 = undefined;
    const ryy = try decodePulses(iy[0..n], n, k, dec);
    normaliseResidual(iy[0..n], x, n, ryy, gain);
    expRotation(x, n, -1, b, k, spread);
    return extractCollapseMask(iy[0..n], n, b);
}

fn renormaliseVector(x: []f32, n: usize, gain: f32) void {
    var e: f32 = epsilon;
    for (0..n) |i| e += x[i] * x[i];
    const g = gain / @sqrt(e);
    for (0..n) |i| x[i] *= g;
}

// ---------------------------------------------------------------------------
// Band shaping helpers

fn lcgRand(seed: u32) u32 {
    return 1664525 *% seed +% 1013904223;
}

fn fracMul16(a: i32, b: i32) i32 {
    const a16: i32 = @as(i16, @truncate(a));
    const b16: i32 = @as(i16, @truncate(b));
    return (16384 + a16 * b16) >> 15;
}

fn bitexactCos(x: i32) i32 {
    const tmp = (4096 + x * x) >> 13;
    var x2 = tmp;
    x2 = (32767 - x2) + fracMul16(x2, (-7651 + fracMul16(x2, (8277 + fracMul16(-626, x2)))));
    return 1 + x2;
}

fn ilog32(v: u32) i32 {
    if (v == 0) return 0;
    return @intCast(32 - @clz(v));
}

fn bitexactLog2Tan(isin_in: i32, icos_in: i32) i32 {
    const lc = ilog32(@intCast(icos_in));
    const ls = ilog32(@intCast(isin_in));
    const icos = icos_in << @intCast(15 - lc);
    const isin = isin_in << @intCast(15 - ls);
    return (ls - lc) * (1 << 11) + fracMul16(isin, fracMul16(isin, -2597) + 7932) - fracMul16(icos, fracMul16(icos, -2597) + 7932);
}

fn isqrt32(val_in: u32) u32 {
    var val = val_in;
    var g: u32 = 0;
    const bshift: u5 = @intCast((ilog32(val) - 1) >> 1);
    var b: u32 = @as(u32, 1) << bshift;
    var shift = bshift;
    while (true) {
        const t = ((g << 1) + b) << shift;
        if (t <= val) {
            g += b;
            val -= t;
        }
        b >>= 1;
        if (b == 0) break;
        shift -= 1;
    }
    return g;
}

fn haar1(x: []f32, n0_in: usize, stride: usize) void {
    const n0 = n0_in >> 1;
    for (0..stride) |i| {
        for (0..n0) |j| {
            const tmp1 = 0.70710678 * x[stride * 2 * j + i];
            const tmp2 = 0.70710678 * x[stride * (2 * j + 1) + i];
            x[stride * 2 * j + i] = tmp1 + tmp2;
            x[stride * (2 * j + 1) + i] = tmp1 - tmp2;
        }
    }
}

fn deinterleaveHadamard(x: []f32, n0: usize, stride: usize, hadamard: bool) void {
    var tmp: [max_band_n]f32 = undefined;
    const n = n0 * stride;
    if (hadamard) {
        const ordery = ordery_table[stride - 2 ..];
        for (0..stride) |i| {
            for (0..n0) |j| tmp[ordery[i] * n0 + j] = x[j * stride + i];
        }
    } else {
        for (0..stride) |i| {
            for (0..n0) |j| tmp[i * n0 + j] = x[j * stride + i];
        }
    }
    @memcpy(x[0..n], tmp[0..n]);
}

fn interleaveHadamard(x: []f32, n0: usize, stride: usize, hadamard: bool) void {
    var tmp: [max_band_n]f32 = undefined;
    const n = n0 * stride;
    if (hadamard) {
        const ordery = ordery_table[stride - 2 ..];
        for (0..stride) |i| {
            for (0..n0) |j| tmp[j * stride + i] = x[ordery[i] * n0 + j];
        }
    } else {
        for (0..stride) |i| {
            for (0..n0) |j| tmp[j * stride + i] = x[i * n0 + j];
        }
    }
    @memcpy(x[0..n], tmp[0..n]);
}

fn computeQn(n: i32, b: i32, offset: i32, pulse_cap: i32, stereo: bool) i32 {
    var n2 = 2 * n - 1;
    if (stereo and n == 2) n2 -= 1;
    var qb = @divTrunc(b + n2 * offset, n2);
    qb = @min(b - pulse_cap - (@as(i32, 4) << bitres), qb);
    qb = @min(@as(i32, 8) << bitres, qb);
    var qn: i32 = 1;
    if (qb >= (@as(i32, 1) << bitres) >> 1) {
        qn = exp2_table8[@intCast(qb & 0x7)] >> @intCast(14 - (qb >> bitres));
        qn = ((qn + 1) >> 1) << 1;
    }
    return qn;
}

fn stereoMerge(x: []f32, y: []f32, mid: f32, n: usize) void {
    var xp: f32 = 0;
    var side: f32 = 0;
    for (0..n) |j| {
        xp += y[j] * x[j];
        side += y[j] * y[j];
    }
    xp *= mid;
    const el = mid * mid + side - 2 * xp;
    const er = mid * mid + side + 2 * xp;
    if (er < 6e-4 or el < 6e-4) {
        @memcpy(y[0..n], x[0..n]);
        return;
    }
    const lgain = 1.0 / @sqrt(el);
    const rgain = 1.0 / @sqrt(er);
    for (0..n) |j| {
        const l = mid * x[j];
        const r = y[j];
        x[j] = lgain * (l - r);
        y[j] = rgain * (l + r);
    }
}

const BandCtx = struct {
    dec: *RangeDecoder,
    i: usize,
    intensity: i32,
    spread: i32,
    tf_change: i32,
    remaining_bits: i32,
    seed: u32,
    avoid_split_noise: bool,
};

const SplitCtx = struct {
    inv: bool,
    imid: i32,
    iside: i32,
    delta: i32,
    itheta: i32,
    qalloc: i32,
};

fn computeTheta(ctx: *BandCtx, sctx: *SplitCtx, n: i32, b: *i32, b_blocks: usize, b0: usize, lm: i32, stereo: bool, fill: *u32) !void {
    const dec = ctx.dec;
    const i = ctx.i;
    const pulse_cap = log_n[i] + lm * (@as(i32, 1) << bitres);
    const offset = (pulse_cap >> 1) - (if (stereo and n == 2) qtheta_offset_twophase else qtheta_offset);
    var qn = computeQn(n, b.*, offset, pulse_cap, stereo);
    if (stereo and @as(i32, @intCast(i)) >= ctx.intensity) qn = 1;
    const tell: i32 = @intCast(dec.tellFrac());
    var itheta: i32 = 0;
    var inv = false;
    if (qn != 1) {
        if (stereo and n > 2) {
            const p0: i32 = 3;
            const x0: i32 = @divTrunc(qn, 2);
            const ft: i32 = p0 * (x0 + 1) + x0;
            const fs: i32 = @intCast(try dec.getFrequency32(@intCast(ft)));
            var x: i32 = 0;
            if (fs < (x0 + 1) * p0) {
                x = @divTrunc(fs, p0);
            } else {
                x = x0 + 1 + (fs - (x0 + 1) * p0);
            }
            const fl: i32 = if (x <= x0) p0 * x else (x - 1 - x0) + (x0 + 1) * p0;
            const fh: i32 = if (x <= x0) p0 * (x + 1) else (x - x0) + (x0 + 1) * p0;
            try dec.update32(@intCast(fl), @intCast(fh), @intCast(ft));
            itheta = x;
        } else if (b0 > 1 or stereo) {
            itheta = @intCast(try dec.decodeUint(@intCast(qn + 1)));
        } else {
            const half = qn >> 1;
            const ft: i32 = (half + 1) * (half + 1);
            const fm: i32 = @intCast(try dec.getFrequency32(@intCast(ft)));
            var fl: i32 = 0;
            var fs: i32 = 1;
            if (fm < ((half * (half + 1)) >> 1)) {
                itheta = (@as(i32, @intCast(isqrt32(@intCast(8 * fm + 1)))) - 1) >> 1;
                fs = itheta + 1;
                fl = (itheta * (itheta + 1)) >> 1;
            } else {
                itheta = (2 * (qn + 1) - @as(i32, @intCast(isqrt32(@intCast(8 * (ft - fm - 1) + 1))))) >> 1;
                fs = qn + 1 - itheta;
                fl = ft - (((qn + 1 - itheta) * (qn + 2 - itheta)) >> 1);
            }
            try dec.update32(@intCast(fl), @intCast(fl + fs), @intCast(ft));
        }
        if (itheta < 0) return error.UnsupportedAudioFormat;
        itheta = @divTrunc(itheta * 16384, qn);
    } else if (stereo) {
        if (b.* > @as(i32, 2) << bitres and ctx.remaining_bits > @as(i32, 2) << bitres) {
            inv = (try dec.decodeBitLogp(2)) != 0;
        }
        itheta = 0;
    }
    const qalloc: i32 = @as(i32, @intCast(dec.tellFrac())) - tell;
    b.* -= qalloc;

    var imid: i32 = 0;
    var iside: i32 = 0;
    var delta: i32 = 0;
    if (itheta == 0) {
        imid = 32767;
        iside = 0;
        fill.* &= (@as(u32, 1) << @intCast(b_blocks)) - 1;
        delta = -16384;
    } else if (itheta == 16384) {
        imid = 0;
        iside = 32767;
        fill.* &= ((@as(u32, 1) << @intCast(b_blocks)) - 1) << @intCast(b_blocks);
        delta = 16384;
    } else {
        imid = bitexactCos(itheta);
        iside = bitexactCos(16384 - itheta);
        delta = fracMul16((n - 1) << 7, bitexactLog2Tan(iside, imid));
    }
    sctx.* = .{ .inv = inv, .imid = imid, .iside = iside, .delta = delta, .itheta = itheta, .qalloc = qalloc };
}

fn quantBandN1(ctx: *BandCtx, x: []f32, y: ?[]f32, lowband_out: ?[]f32) !u32 {
    const dec = ctx.dec;
    const count: usize = if (y != null) 2 else 1;
    for (0..count) |c| {
        const target = if (c == 0) x else y.?;
        var sign: u32 = 0;
        if (ctx.remaining_bits >= @as(i32, 1) << bitres) {
            sign = try dec.readRawBits(1);
            ctx.remaining_bits -= @as(i32, 1) << bitres;
        }
        target[0] = if (sign != 0) -1.0 else 1.0;
    }
    if (lowband_out) |lo| lo[0] = x[0];
    return 1;
}

fn quantPartition(ctx: *BandCtx, x: []f32, n_in: usize, b_in: i32, b_blocks_in: usize, lowband: ?[]f32, lm_in: i32, gain: f32, fill_in: u32) !u32 {
    var n = n_in;
    var b = b_in;
    var b_blocks = b_blocks_in;
    var lm = lm_in;
    var fill = fill_in;
    const dec = ctx.dec;
    const i = ctx.i;
    const b0 = b_blocks;
    var cm: u32 = 0;

    const lm_plus_one: usize = @intCast(lm + 1);
    const cache_top = cacheBits(lm_plus_one, i, @intCast(cacheCount(lm_plus_one, i)));
    if (lm != -1 and b > cache_top + 12 and n > 2) {
        n >>= 1;
        const y = x[n .. 2 * n];
        lm -= 1;
        if (b_blocks == 1) fill = (fill & 1) | (fill << 1);
        b_blocks = (b_blocks + 1) >> 1;
        var sctx: SplitCtx = undefined;
        try computeTheta(ctx, &sctx, @intCast(n), &b, b_blocks, b0, lm, false, &fill);
        const imid = sctx.imid;
        const iside = sctx.iside;
        var delta = sctx.delta;
        const itheta = sctx.itheta;
        const qalloc = sctx.qalloc;
        const mid: f32 = (1.0 / 32768.0) * @as(f32, @floatFromInt(imid));
        const side: f32 = (1.0 / 32768.0) * @as(f32, @floatFromInt(iside));

        if (b0 > 1 and (itheta & 0x3fff) != 0) {
            if (itheta > 8192) {
                delta -= delta >> @intCast(4 - lm);
            } else {
                delta = @min(0, delta + ((@as(i32, @intCast(n)) << bitres) >> @intCast(5 - lm)));
            }
        }
        var mbits: i32 = @max(0, @min(b, @divTrunc(b - delta, 2)));
        var sbits: i32 = b - mbits;
        ctx.remaining_bits -= qalloc;

        const next_lowband2: ?[]f32 = if (lowband) |lo| lo[n..] else null;
        var rebalance = ctx.remaining_bits;
        if (mbits >= sbits) {
            cm = try quantPartition(ctx, x, n, mbits, b_blocks, lowband, lm, gain * mid, fill);
            rebalance = mbits - (rebalance - ctx.remaining_bits);
            if (rebalance > @as(i32, 3) << bitres and itheta != 0) sbits += rebalance - (@as(i32, 3) << bitres);
            cm |= (try quantPartition(ctx, y, n, sbits, b_blocks, next_lowband2, lm, gain * side, fill >> @intCast(b_blocks))) << @intCast(b0 >> 1);
        } else {
            cm = (try quantPartition(ctx, y, n, sbits, b_blocks, next_lowband2, lm, gain * side, fill >> @intCast(b_blocks))) << @intCast(b0 >> 1);
            rebalance = sbits - (rebalance - ctx.remaining_bits);
            if (rebalance > @as(i32, 3) << bitres and itheta != 16384) mbits += rebalance - (@as(i32, 3) << bitres);
            cm |= try quantPartition(ctx, x, n, mbits, b_blocks, lowband, lm, gain * mid, fill);
        }
    } else {
        var q = bits2Pulses(i, lm, b);
        var curr_bits = pulses2Bits(i, lm, q);
        ctx.remaining_bits -= curr_bits;
        while (ctx.remaining_bits < 0 and q > 0) {
            ctx.remaining_bits += curr_bits;
            q -= 1;
            curr_bits = pulses2Bits(i, lm, q);
            ctx.remaining_bits -= curr_bits;
        }
        if (q != 0) {
            const k: usize = @intCast(getPulses(q));
            cm = try algUnquant(x, n, k, ctx.spread, b_blocks, dec, gain);
        } else {
            const cm_mask: u32 = (@as(u32, 1) << @intCast(b_blocks)) - 1;
            fill &= cm_mask;
            if (fill == 0) {
                @memset(x[0..n], 0);
            } else {
                if (lowband) |lo| {
                    for (0..n) |j| {
                        ctx.seed = lcgRand(ctx.seed);
                        const tmp: f32 = if ((ctx.seed & 0x8000) != 0) 1.0 / 256.0 else -1.0 / 256.0;
                        x[j] = lo[j] + tmp;
                    }
                    cm = fill;
                } else {
                    for (0..n) |j| {
                        ctx.seed = lcgRand(ctx.seed);
                        x[j] = @floatFromInt(@as(i32, @bitCast(ctx.seed)) >> 20);
                    }
                    cm = cm_mask;
                }
                renormaliseVector(x, n, gain);
            }
        }
    }
    return cm;
}

fn quantBand(ctx: *BandCtx, x: []f32, n: usize, b: i32, b_blocks_in: usize, lowband_in: ?[]f32, lm: i32, lowband_out: ?[]f32, gain: f32, lowband_scratch: ?[]f32, fill_in: u32) !u32 {
    var b_blocks = b_blocks_in;
    var lowband = lowband_in;
    var fill = fill_in;
    const n0 = n;
    var n_b = n;
    var b0 = b_blocks;
    var time_divide: usize = 0;
    var recombine: usize = 0;
    const long_blocks = b0 == 1;
    var tf_change = ctx.tf_change;
    n_b = n_b / b_blocks;

    if (n == 1) return quantBandN1(ctx, x, null, lowband_out);

    if (tf_change > 0) recombine = @intCast(tf_change);
    if (lowband_scratch != null and lowband != null and (recombine != 0 or ((n_b & 1) == 0 and tf_change < 0) or b0 > 1)) {
        @memcpy(lowband_scratch.?[0..n], lowband.?[0..n]);
        lowband = lowband_scratch.?;
    }
    for (0..recombine) |k| {
        if (lowband) |lo| haar1(lo, n >> @intCast(k), @as(usize, 1) << @intCast(k));
        fill = bit_interleave_table[fill & 0xF] | (bit_interleave_table[(fill >> 4) & 0xF] << 2);
    }
    b_blocks >>= @intCast(recombine);
    n_b <<= @intCast(recombine);

    while ((n_b & 1) == 0 and tf_change < 0) {
        if (lowband) |lo| haar1(lo, n_b, b_blocks);
        fill |= fill << @intCast(b_blocks);
        b_blocks <<= 1;
        n_b >>= 1;
        time_divide += 1;
        tf_change += 1;
    }
    b0 = b_blocks;
    const n_b0 = n_b;

    if (b0 > 1) {
        if (lowband) |lo| deinterleaveHadamard(lo, n_b >> @intCast(recombine), b0 << @intCast(recombine), long_blocks);
    }

    var cm = try quantPartition(ctx, x, n, b, b_blocks, lowband, lm, gain, fill);

    if (b0 > 1) interleaveHadamard(x, n_b >> @intCast(recombine), b0 << @intCast(recombine), long_blocks);
    n_b = n_b0;
    b_blocks = b0;
    for (0..time_divide) |_| {
        b_blocks >>= 1;
        n_b <<= 1;
        cm |= cm >> @intCast(b_blocks);
        haar1(x, n_b, b_blocks);
    }
    for (0..recombine) |k| {
        cm = bit_deinterleave_table[cm & 0xF];
        haar1(x, n0 >> @intCast(k), @as(usize, 1) << @intCast(k));
    }
    b_blocks <<= @intCast(recombine);

    if (lowband_out) |lo| {
        const scale: f32 = @sqrt(@as(f32, @floatFromInt(n0)));
        for (0..n0) |j| lo[j] = scale * x[j];
    }
    cm &= (@as(u32, 1) << @intCast(b_blocks)) - 1;
    return cm;
}

fn quantBandStereo(ctx: *BandCtx, x: []f32, y: []f32, n: usize, b_in: i32, b_blocks: usize, lowband: ?[]f32, lm: i32, lowband_out: ?[]f32, lowband_scratch: ?[]f32, fill_in: u32) !u32 {
    var b = b_in;
    var fill = fill_in;
    const dec = ctx.dec;
    if (n == 1) return quantBandN1(ctx, x, y, lowband_out);
    const orig_fill = fill;
    var sctx: SplitCtx = undefined;
    try computeTheta(ctx, &sctx, @intCast(n), &b, b_blocks, b_blocks, lm, true, &fill);
    const inv = sctx.inv;
    const imid = sctx.imid;
    const iside = sctx.iside;
    const delta = sctx.delta;
    const itheta = sctx.itheta;
    const qalloc = sctx.qalloc;
    const mid: f32 = (1.0 / 32768.0) * @as(f32, @floatFromInt(imid));
    const side: f32 = (1.0 / 32768.0) * @as(f32, @floatFromInt(iside));
    var cm: u32 = 0;

    if (n == 2) {
        var mbits = b;
        var sbits: i32 = 0;
        if (itheta != 0 and itheta != 16384) sbits = @as(i32, 1) << bitres;
        mbits -= sbits;
        const c = itheta > 8192;
        ctx.remaining_bits -= qalloc + sbits;
        const x2 = if (c) y else x;
        const y2 = if (c) x else y;
        var sign: i32 = 0;
        if (sbits != 0) sign = @intCast(try dec.readRawBits(1));
        sign = 1 - 2 * sign;
        cm = try quantBand(ctx, x2, n, mbits, b_blocks, lowband, lm, lowband_out, 1.0, lowband_scratch, orig_fill);
        const sign_f: f32 = @floatFromInt(sign);
        y2[0] = -sign_f * x2[1];
        y2[1] = sign_f * x2[0];
        x[0] = mid * x[0];
        x[1] = mid * x[1];
        y[0] = side * y[0];
        y[1] = side * y[1];
        var tmp = x[0];
        x[0] = tmp - y[0];
        y[0] = tmp + y[0];
        tmp = x[1];
        x[1] = tmp - y[1];
        y[1] = tmp + y[1];
    } else {
        var mbits: i32 = @max(0, @min(b, @divTrunc(b - delta, 2)));
        var sbits: i32 = b - mbits;
        ctx.remaining_bits -= qalloc;
        var rebalance = ctx.remaining_bits;
        if (mbits >= sbits) {
            cm = try quantBand(ctx, x, n, mbits, b_blocks, lowband, lm, lowband_out, 1.0, lowband_scratch, fill);
            rebalance = mbits - (rebalance - ctx.remaining_bits);
            if (rebalance > @as(i32, 3) << bitres and itheta != 0) sbits += rebalance - (@as(i32, 3) << bitres);
            cm |= try quantBand(ctx, y, n, sbits, b_blocks, null, lm, null, side, null, fill >> @intCast(b_blocks));
        } else {
            cm = try quantBand(ctx, y, n, sbits, b_blocks, null, lm, null, side, null, fill >> @intCast(b_blocks));
            rebalance = sbits - (rebalance - ctx.remaining_bits);
            if (rebalance > @as(i32, 3) << bitres and itheta != 16384) mbits += rebalance - (@as(i32, 3) << bitres);
            cm |= try quantBand(ctx, x, n, mbits, b_blocks, lowband, lm, lowband_out, 1.0, lowband_scratch, fill);
        }
    }
    if (n != 2) stereoMerge(x, y, mid, n);
    if (inv) {
        for (0..n) |j| y[j] = -y[j];
    }
    return cm;
}

fn specialHybridFolding(norm: []f32, norm2: ?[]f32, start: usize, m: usize, dual_stereo: bool) void {
    const n1: usize = m * @as(usize, @intCast(e_bands[start + 1] - e_bands[start]));
    const n2: usize = m * @as(usize, @intCast(e_bands[start + 2] - e_bands[start + 1]));
    if (n2 <= n1) return;
    std.mem.copyForwards(f32, norm[n1..n2], norm[2 * n1 - n2 .. n1]);
    if (dual_stereo) {
        if (norm2) |n2s| std.mem.copyForwards(f32, n2s[n1..n2], n2s[2 * n1 - n2 .. n1]);
    }
}

fn quantAllBands(
    start: usize,
    end: usize,
    x_all: []f32,
    y_all: ?[]f32,
    collapse_masks: *[2 * nb_ebands]u8,
    pulses: *const [nb_ebands]i32,
    short_blocks: usize,
    spread: i32,
    dual_stereo_in: i32,
    intensity: i32,
    tf_res: *const [nb_ebands]i32,
    total_bits: i32,
    balance_in: i32,
    dec: *RangeDecoder,
    lm: u2,
    coded_bands: usize,
    seed: *u32,
) !void {
    var balance = balance_in;
    var dual_stereo = dual_stereo_in != 0;
    const m: usize = @as(usize, 1) << lm;
    const b_blocks: usize = if (short_blocks != 0) m else 1;
    const c_count: usize = if (y_all != null) 2 else 1;
    const norm_offset: usize = m * @as(usize, @intCast(e_bands[start]));
    const norm_len: usize = m * @as(usize, @intCast(e_bands[nb_ebands - 1])) - norm_offset;
    var norm_storage: [2 * 8 * 78]f32 = undefined;
    const norm = norm_storage[0..norm_len];
    const norm2 = norm_storage[norm_len .. 2 * norm_len];
    var lowband_offset: usize = 0;
    var update_lowband = true;
    const scratch_base: usize = m * @as(usize, @intCast(e_bands[nb_ebands - 1]));

    var ctx = BandCtx{
        .dec = dec,
        .i = 0,
        .intensity = intensity,
        .spread = spread,
        .tf_change = 0,
        .remaining_bits = 0,
        .seed = seed.*,
        .avoid_split_noise = b_blocks > 1,
    };

    for (start..end) |i| {
        ctx.i = i;
        const last = i == end - 1;
        const band_start: usize = m * @as(usize, @intCast(e_bands[i]));
        const band_end: usize = m * @as(usize, @intCast(e_bands[i + 1]));
        const n = band_end - band_start;
        const x = x_all[band_start..band_end];
        const y: ?[]f32 = if (y_all) |ya| ya[band_start..band_end] else null;
        const tell: i32 = @intCast(dec.tellFrac());
        if (i != start) balance -= tell;
        const remaining_bits = total_bits - tell - 1;
        ctx.remaining_bits = remaining_bits;
        var b: i32 = 0;
        if (i <= coded_bands - 1) {
            const curr_balance = @divTrunc(balance, @as(i32, @intCast(@min(3, coded_bands - i))));
            b = @max(0, @min(16383, @min(remaining_bits + 1, pulses[i] + curr_balance)));
        }

        if ((band_start >= n + norm_offset or i == start + 1) and (update_lowband or lowband_offset == 0)) lowband_offset = i;
        if (i == start + 1) specialHybridFolding(norm, if (c_count == 2) norm2 else null, start, m, dual_stereo);

        const tf_change = tf_res[i];
        ctx.tf_change = tf_change;
        var lowband_scratch: ?[]f32 = x_all[scratch_base..];
        if (last) lowband_scratch = null;

        var x_cm: u32 = 0;
        var y_cm: u32 = 0;
        var effective_lowband: ?usize = null;
        if (lowband_offset != 0 and (spread != spread_aggressive or b_blocks > 1 or tf_change < 0)) {
            const lo_start: usize = m * @as(usize, @intCast(e_bands[lowband_offset]));
            const eff: usize = if (lo_start >= norm_offset + n) lo_start - norm_offset - n else 0;
            effective_lowband = eff;
            var fold_start = lowband_offset;
            while (true) {
                fold_start -= 1;
                if (!(m * @as(usize, @intCast(e_bands[fold_start])) > eff + norm_offset)) break;
            }
            var fold_end = lowband_offset - 1;
            while (true) {
                fold_end += 1;
                if (!(m * @as(usize, @intCast(e_bands[fold_end])) < eff + norm_offset + n)) break;
            }
            var fold_i = fold_start;
            while (fold_i < fold_end) : (fold_i += 1) {
                x_cm |= collapse_masks[fold_i * c_count + 0];
                y_cm |= collapse_masks[fold_i * c_count + c_count - 1];
            }
        } else {
            x_cm = (@as(u32, 1) << @intCast(b_blocks)) - 1;
            y_cm = x_cm;
        }

        if (dual_stereo and @as(i32, @intCast(i)) == intensity) {
            dual_stereo = false;
            for (0..band_start - norm_offset) |j| norm[j] = 0.5 * (norm[j] + norm2[j]);
        }
        const lowband_x: ?[]f32 = if (effective_lowband) |eff| norm[eff..] else null;
        const lowband_y: ?[]f32 = if (effective_lowband) |eff| norm2[eff..] else null;
        const lowband_out_x: ?[]f32 = if (last) null else norm[band_start - norm_offset ..];
        const lowband_out_y: ?[]f32 = if (last) null else norm2[band_start - norm_offset ..];
        if (dual_stereo) {
            x_cm = try quantBand(&ctx, x, n, @divTrunc(b, 2), b_blocks, lowband_x, lm, lowband_out_x, 1.0, lowband_scratch, x_cm);
            y_cm = try quantBand(&ctx, y.?, n, @divTrunc(b, 2), b_blocks, lowband_y, lm, lowband_out_y, 1.0, lowband_scratch, y_cm);
        } else {
            if (y) |yy| {
                x_cm = try quantBandStereo(&ctx, x, yy, n, b, b_blocks, lowband_x, lm, lowband_out_x, lowband_scratch, x_cm | y_cm);
            } else {
                x_cm = try quantBand(&ctx, x, n, b, b_blocks, lowband_x, lm, lowband_out_x, 1.0, lowband_scratch, x_cm | y_cm);
            }
            y_cm = x_cm;
        }
        collapse_masks[i * c_count + 0] = @truncate(x_cm);
        collapse_masks[i * c_count + c_count - 1] = @truncate(y_cm);
        balance += pulses[i] + tell;
        update_lowband = b > @as(i32, @intCast(n)) << bitres;
        ctx.avoid_split_noise = false;
    }
    seed.* = ctx.seed;
}

fn antiCollapse(x_all: []f32, collapse_masks: *const [2 * nb_ebands]u8, lm: u2, c_count: usize, size: usize, start: usize, end: usize, log_e: *const [2 * nb_ebands]f32, prev1_log_e: *const [2 * nb_ebands]f32, prev2_log_e: *const [2 * nb_ebands]f32, pulses: *const [nb_ebands]i32, seed_in: u32) void {
    var seed = seed_in;
    for (start..end) |i| {
        const n0: usize = @intCast(e_bands[i + 1] - e_bands[i]);
        const depth: i32 = @divTrunc(1 + pulses[i], @as(i32, @intCast(n0))) >> lm;
        const thresh: f32 = 0.5 * @exp2(-0.125 * @as(f32, @floatFromInt(depth)));
        const sqrt_1: f32 = 1.0 / @sqrt(@as(f32, @floatFromInt(n0 << lm)));
        for (0..c_count) |c| {
            var prev1 = prev1_log_e[c * nb_ebands + i];
            var prev2 = prev2_log_e[c * nb_ebands + i];
            if (c_count == 1) {
                prev1 = @max(prev1, prev1_log_e[nb_ebands + i]);
                prev2 = @max(prev2, prev2_log_e[nb_ebands + i]);
            }
            var ediff = log_e[c * nb_ebands + i] - @min(prev1, prev2);
            ediff = @max(0, ediff);
            var r: f32 = 2.0 * @exp2(-ediff);
            if (lm == 3) r *= 1.41421356;
            r = @min(thresh, r);
            r = r * sqrt_1;
            const band_off = c * size + (@as(usize, @intCast(e_bands[i])) << lm);
            const x = x_all[band_off .. band_off + (n0 << lm)];
            var renormalize = false;
            for (0..@as(usize, 1) << lm) |k| {
                if ((collapse_masks[i * c_count + c] & (@as(u8, 1) << @intCast(k))) == 0) {
                    for (0..n0) |j| {
                        seed = lcgRand(seed);
                        x[(j << lm) + k] = if ((seed & 0x8000) != 0) r else -r;
                    }
                    renormalize = true;
                }
            }
            if (renormalize) renormaliseVector(x, n0 << lm, 1.0);
        }
    }
}

fn denormaliseBands(x: []const f32, freq: []f32, band_log_e: []const f32, start_in: usize, end_in: usize, m: usize, silence: bool) void {
    const n = freq.len;
    var start = start_in;
    var end = end_in;
    var bound: usize = m * @as(usize, @intCast(e_bands[end]));
    if (silence) {
        bound = 0;
        start = 0;
        end = 0;
    }
    const first: usize = m * @as(usize, @intCast(e_bands[start]));
    @memset(freq[0..first], 0);
    for (start..end) |i| {
        const j0: usize = m * @as(usize, @intCast(e_bands[i]));
        const j1: usize = m * @as(usize, @intCast(e_bands[i + 1]));
        const lg = band_log_e[i] + e_means[i];
        const g: f32 = @exp2(@min(32.0, lg));
        for (j0..j1) |j| freq[j] = x[j] * g;
    }
    @memset(freq[bound..n], 0);
}

// ---------------------------------------------------------------------------
// Inverse MDCT

fn fftForward(out: []Complex, in: []const Complex, in_stride: usize, n: usize, tw: *const [max_n4]Complex) void {
    if (n == 15) {
        for (0..15) |k| {
            var acc = Complex{ .re = 0, .im = 0 };
            for (0..15) |j| {
                const w = tw[((j * k) % 15) * (max_n4 / 15)];
                const v = in[j * in_stride];
                acc.re += v.re * w.re - v.im * w.im;
                acc.im += v.re * w.im + v.im * w.re;
            }
            out[k] = acc;
        }
        return;
    }
    const half = n / 2;
    fftForward(out[0..half], in, in_stride * 2, half, tw);
    fftForward(out[half..n], in[in_stride..], in_stride * 2, half, tw);
    const step = max_n4 / n;
    for (0..half) |k| {
        const w = tw[k * step];
        const o = out[half + k];
        const t = Complex{ .re = o.re * w.re - o.im * w.im, .im = o.re * w.im + o.im * w.re };
        const e = out[k];
        out[k] = .{ .re = e.re + t.re, .im = e.im + t.im };
        out[half + k] = .{ .re = e.re - t.re, .im = e.im - t.im };
    }
}

/// clt_mdct_backward: `in` is the interleaved spectrum (coefficient k of
/// block `block` lives at in[block + stride*k]); the result overlaps into
/// `out[0..overlap/2 + N/2)` exactly like libopus so consecutive blocks
/// unfold with TDAC in place.
fn mdctBackward(t: *const Tables, in: []const f32, block: usize, stride: usize, out: []f32, shift: usize) void {
    const n = (2 * max_frame) >> @intCast(shift);
    const n2 = n >> 1;
    const n4 = n >> 2;
    const trig = &t.trig[shift];

    var pre: [max_n4]Complex = undefined;
    for (0..n4) |i| {
        const x1 = in[block + stride * 2 * i];
        const x2 = in[block + stride * (n2 - 1 - 2 * i)];
        const yr = x2 * trig[2 * i + 1] + x1 * trig[2 * i];
        const yi = x1 * trig[2 * i + 1] - x2 * trig[2 * i];
        pre[i] = .{ .re = yi, .im = yr };
    }
    var z: [max_n4]Complex = undefined;
    fftForward(z[0..n4], pre[0..n4], 1, n4, &t.twiddle);

    const half_overlap = overlap >> 1;
    var i: usize = 0;
    while (i < (n4 + 1) >> 1) : (i += 1) {
        const a = z[i];
        const b = z[n4 - 1 - i];
        var re = a.im;
        var im = a.re;
        var t0 = trig[2 * i];
        var t1 = trig[2 * i + 1];
        const yr0 = re * t1 + im * t0;
        const yi0 = re * t0 - im * t1;
        re = b.im;
        im = b.re;
        t0 = trig[2 * (n4 - i - 1)];
        t1 = trig[2 * (n4 - i - 1) + 1];
        const yr1 = re * t1 + im * t0;
        const yi1 = re * t0 - im * t1;
        // yp0[0] = yr0; yp1[1] = yi0; yp1[0] = yr1; yp0[1] = yi1
        out[half_overlap + 2 * i] = yr0;
        out[half_overlap + 2 * i + 1] = yi1;
        out[half_overlap + n2 - 2 - 2 * i] = yr1;
        out[half_overlap + n2 - 2 - 2 * i + 1] = yi0;
    }

    for (0..half_overlap) |k| {
        const x1 = out[overlap - 1 - k];
        const x2 = out[k];
        out[k] = x2 * t.window[overlap - 1 - k] - x1 * t.window[k];
        out[overlap - 1 - k] = x2 * t.window[k] + x1 * t.window[overlap - 1 - k];
    }
}

// ---------------------------------------------------------------------------
// Pitch postfilter

fn combFilter(mem: []f32, base: usize, t0_in: i32, t1_in: i32, n: usize, g0: f32, g1: f32, tapset0: u8, tapset1: u8, win: *const [overlap]f32) void {
    if (g0 == 0 and g1 == 0) return;
    const t0: usize = @intCast(@max(t0_in, combfilter_minperiod));
    const t1: usize = @intCast(@max(t1_in, combfilter_minperiod));
    const g00 = g0 * comb_gains[tapset0][0];
    const g01 = g0 * comb_gains[tapset0][1];
    const g02 = g0 * comb_gains[tapset0][2];
    const g10 = g1 * comb_gains[tapset1][0];
    const g11 = g1 * comb_gains[tapset1][1];
    const g12 = g1 * comb_gains[tapset1][2];
    var ov: usize = overlap;
    if (g0 == g1 and t0 == t1 and tapset0 == tapset1) ov = 0;
    const x = mem[base..];
    var i: usize = 0;
    while (i < ov and i < n) : (i += 1) {
        const f = win[i] * win[i];
        const p0 = i + base - t0;
        const p1 = i + base - t1;
        x[i] = x[i] +
            (1.0 - f) * g00 * mem[p0] +
            (1.0 - f) * g01 * (mem[p0 + 1] + mem[p0 - 1]) +
            (1.0 - f) * g02 * (mem[p0 + 2] + mem[p0 - 2]) +
            f * g10 * mem[p1] +
            f * g11 * (mem[p1 + 1] + mem[p1 - 1]) +
            f * g12 * (mem[p1 + 2] + mem[p1 - 2]);
    }
    if (g1 == 0) return;
    while (i < n) : (i += 1) {
        const p1 = i + base - t1;
        x[i] = x[i] + g10 * mem[p1] + g11 * (mem[p1 + 1] + mem[p1 - 1]) + g12 * (mem[p1 + 2] + mem[p1 - 2]);
    }
}

/// smooth_fade from opus_decoder.c: crossfades `in1` into `in2` over one
/// 2.5 ms window (used around redundant frames at mode transitions).
pub fn smoothFade(in1: []const f32, in2: []const f32, out: []f32, len: usize, c_count: usize) void {
    const win = window();
    for (0..len) |i| {
        const w = win[i] * win[i];
        for (0..c_count) |c| {
            out[i * c_count + c] = w * in2[i * c_count + c] + (1 - w) * in1[i * c_count + c];
        }
    }
}

test "mdct backward produces a bounded sinusoid for a single coefficient" {
    const t = sharedTables();
    for ([_]usize{ 0, 1, 2, 3 }) |shift| {
        const n2 = ((2 * max_frame) >> @intCast(shift)) / 2;
        var in: [max_frame]f32 = [_]f32{0} ** max_frame;
        in[7] = 1.0;
        var out: [max_frame + overlap]f32 = [_]f32{0} ** (max_frame + overlap);
        mdctBackward(t, in[0..n2], 0, 1, out[0..], shift);
        var energy: f64 = 0;
        for (out[0 .. overlap / 2 + n2]) |v| {
            try std.testing.expect(std.math.isFinite(v));
            try std.testing.expect(@abs(v) <= 1.0001);
            energy += @as(f64, v) * v;
        }
        try std.testing.expect(energy > 0.25 * @as(f64, @floatFromInt(n2)));
    }
}
