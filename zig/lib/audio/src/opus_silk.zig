//! SILK layer of the Opus decoder (RFC 6716 section 4.2), ported from the
//! libopus fixed-point reference decoder: side-information decoding, the
//! shell pulse coder, NLSF and LTP dequantisation, the LPC/LTP synthesis
//! core, mid/side stereo reconstruction and the 48 kHz resampler. Packet
//! loss concealment and comfort noise are not implemented: every frame is
//! decoded from bits, which is all a file decoder needs.

const std = @import("std");
const opus = @import("opus.zig");
const t = @import("opus_silk_ref_tables.zig");

const RangeDecoder = opus.RangeDecoder;

const max_frame_length: usize = 320;
const max_lpc_order: usize = 16;
const min_lpc_order: usize = 10;
const ltp_order: usize = 5;
const max_nb_subfr: usize = 4;
const max_sub_frame_length: usize = 80;
const ltp_mem_length_ms: i32 = 20;
const sub_frame_length_ms: i32 = 5;
const shell_codec_frame_length: usize = 16;
const log2_shell_codec_frame_length: u5 = 4;
const max_nb_shell_blocks: usize = max_frame_length / shell_codec_frame_length;
const silk_max_pulses: i32 = 16;
const n_rate_levels: usize = 10;
const nlsf_quant_max_amplitude: i32 = 4;
const quant_level_adjust_q10: i32 = 80;
const max_lpc_stabilize_iterations: usize = 16;
const n_levels_qgain: i32 = 64;
const min_qgain_db: i32 = 2;
const max_qgain_db: i32 = 88;
const max_delta_gain_quant: i32 = 36;
const min_delta_gain_quant: i32 = -4;
const pe_min_lag_ms: i32 = 2;
const pe_max_lag_ms: i32 = 18;
const stereo_interp_len_ms: i32 = 8;
const stereo_quant_sub_steps: f64 = 5;
const rand_multiplier: i32 = 196314165;
const rand_increment: i32 = 907633515;
const resampler_order_fir_12: usize = 8;
const resampler_max_batch_size_ms: i32 = 10;

const type_no_voice_activity: i32 = 0;
const type_unvoiced: i32 = 1;
const type_voiced: i32 = 2;

const code_independently: i32 = 0;
const code_independently_no_ltp_scaling: i32 = 1;
const code_conditionally: i32 = 2;

const gain_offset: i32 = ((min_qgain_db * 128) / 6 + 16 * 128);
const gain_inv_scale_q16: i32 = @divTrunc(65536 * (((max_qgain_db - min_qgain_db) * 128) / 6), n_levels_qgain - 1);

fn fixConst(comptime c: f64, comptime q: u6) i32 {
    return @intFromFloat(c * @as(f64, @floatFromInt(@as(i64, 1) << q)) + 0.5);
}

// ---------------------------------------------------------------------------
// Fixed-point helpers (silk/macros.h, SigProc_FIX.h, Inlines.h)

inline fn lsh(a: i32, s: u5) i32 {
    return @bitCast(@as(u32, @bitCast(a)) << s);
}

inline fn rsh(a: i32, s: u5) i32 {
    return a >> s;
}

inline fn i16v(a: i32) i32 {
    return @as(i16, @truncate(a));
}

inline fn smulwb(a: i32, b: i32) i32 {
    return @truncate((@as(i64, a) * @as(i64, i16v(b))) >> 16);
}

inline fn smlawb(a: i32, b: i32, c: i32) i32 {
    return a +% smulwb(b, c);
}

inline fn smulwt(a: i32, b: i32) i32 {
    return @truncate((@as(i64, a) * @as(i64, b >> 16)) >> 16);
}

inline fn smulbb(a: i32, b: i32) i32 {
    return i16v(a) *% i16v(b);
}

inline fn smlabb(a: i32, b: i32, c: i32) i32 {
    return a +% smulbb(b, c);
}

inline fn smulww(a: i32, b: i32) i32 {
    return @truncate((@as(i64, a) * @as(i64, b)) >> 16);
}

inline fn smlaww(a: i32, b: i32, c: i32) i32 {
    return a +% smulww(b, c);
}

inline fn smmul(a: i32, b: i32) i32 {
    return @truncate((@as(i64, a) * @as(i64, b)) >> 32);
}

inline fn rshiftRound(a: i32, s: u5) i32 {
    if (s == 1) return (a >> 1) + (a & 1);
    return ((a >> (s - 1)) + 1) >> 1;
}

inline fn rshiftRound64(a: i64, s: u6) i64 {
    if (s == 1) return (a >> 1) + (a & 1);
    return ((a >> (s - 1)) + 1) >> 1;
}

inline fn sat16(a: i32) i32 {
    return @max(-32768, @min(32767, a));
}

inline fn clz32(a: i32) i32 {
    return @intCast(@clz(@as(u32, @bitCast(a))));
}

inline fn clzAbs(a: i32) i32 {
    return @intCast(@clz(@abs(a)));
}

fn ror32(a: i32, rot: i32) i32 {
    const x: u32 = @bitCast(a);
    if (rot == 0) return a;
    if (rot < 0) {
        const m: u5 = @intCast(-rot);
        return @bitCast((x << m) | (x >> @intCast(32 - @as(i32, m))));
    }
    const r: u5 = @intCast(rot);
    return @bitCast((x << @intCast(32 - @as(i32, r))) | (x >> r));
}

fn clzFrac(in: i32, lz: *i32, frac_q7: *i32) void {
    const lzeros = clz32(in);
    lz.* = lzeros;
    frac_q7.* = ror32(in, 24 - lzeros) & 0x7f;
}

fn lin2log(in_lin: i32) i32 {
    var lz: i32 = 0;
    var frac_q7: i32 = 0;
    clzFrac(in_lin, &lz, &frac_q7);
    return smlawb(frac_q7, frac_q7 *% (128 - frac_q7), 179) +% lsh(31 - lz, 7);
}

fn log2lin(in_log_q7: i32) i32 {
    if (in_log_q7 < 0) return 0;
    if (in_log_q7 >= 3967) return std.math.maxInt(i32);
    var out: i32 = lsh(1, @intCast(rsh(in_log_q7, 7)));
    const frac_q7 = in_log_q7 & 0x7F;
    if (in_log_q7 < 2048) {
        out = out +% rsh(out *% smlawb(frac_q7, smulbb(frac_q7, 128 - frac_q7), -174), 7);
    } else {
        out = out +% rsh(out, 7) *% smlawb(frac_q7, smulbb(frac_q7, 128 - frac_q7), -174);
    }
    return out;
}

fn div32VarQ(a32: i32, b32: i32, qres: i32) i32 {
    const a_headrm: u5 = @intCast(clzAbs(a32) - 1);
    const a32_nrm0 = lsh(a32, a_headrm);
    const b_headrm: u5 = @intCast(clzAbs(b32) - 1);
    const b32_nrm = lsh(b32, b_headrm);
    const b32_inv = @divTrunc(@as(i32, std.math.maxInt(i32) >> 2), rsh(b32_nrm, 16));
    var result = smulwb(a32_nrm0, b32_inv);
    const a32_nrm = a32_nrm0 -% lsh(smmul(b32_nrm, result), 3);
    result = smlawb(result, a32_nrm, b32_inv);
    const lshift: i32 = 29 + @as(i32, a_headrm) - @as(i32, b_headrm) - qres;
    if (lshift < 0) return result <<| @as(u5, @intCast(-lshift));
    if (lshift < 32) return rsh(result, @intCast(lshift));
    return 0;
}

fn inverse32VarQ(b32: i32, qres: i32) i32 {
    const b_headrm: u5 = @intCast(clzAbs(b32) - 1);
    const b32_nrm = lsh(b32, b_headrm);
    const b32_inv = @divTrunc(@as(i32, std.math.maxInt(i32) >> 2), rsh(b32_nrm, 16));
    var result = lsh(b32_inv, 16);
    const err_q32 = lsh((@as(i32, 1) << 29) -% smulwb(b32_nrm, b32_inv), 3);
    result = smlaww(result, err_q32, b32_inv);
    const lshift: i32 = 61 - @as(i32, b_headrm) - qres;
    if (lshift <= 0) return result <<| @as(u5, @intCast(-lshift));
    if (lshift < 32) return rsh(result, @intCast(lshift));
    return 0;
}

fn silkRand(seed: i32) i32 {
    return rand_increment +% seed *% rand_multiplier;
}

// ---------------------------------------------------------------------------
// Codebooks

const NlsfCb = struct {
    n_vectors: usize,
    order: usize,
    quant_step_size_q16: i32,
    cb1_nlsf_q8: []const u8,
    cb1_wght_q9: []const i16,
    cb1_icdf: []const u8,
    pred_q8: []const u8,
    ec_sel: []const u8,
    ec_icdf: []const u8,
    delta_min_q15: []const i16,
};

const nlsf_cb_nb_mb = NlsfCb{
    .n_vectors = 32,
    .order = 10,
    .quant_step_size_q16 = fixConst(0.18, 16),
    .cb1_nlsf_q8 = &t.silk_NLSF_CB1_NB_MB_Q8,
    .cb1_wght_q9 = &t.silk_NLSF_CB1_Wght_Q9,
    .cb1_icdf = &t.silk_NLSF_CB1_iCDF_NB_MB,
    .pred_q8 = &t.silk_NLSF_PRED_NB_MB_Q8,
    .ec_sel = &t.silk_NLSF_CB2_SELECT_NB_MB,
    .ec_icdf = &t.silk_NLSF_CB2_iCDF_NB_MB,
    .delta_min_q15 = &t.silk_NLSF_DELTA_MIN_NB_MB_Q15,
};

const nlsf_cb_wb = NlsfCb{
    .n_vectors = 32,
    .order = 16,
    .quant_step_size_q16 = fixConst(0.15, 16),
    .cb1_nlsf_q8 = &t.silk_NLSF_CB1_WB_Q8,
    .cb1_wght_q9 = &t.silk_NLSF_CB1_WB_Wght_Q9,
    .cb1_icdf = &t.silk_NLSF_CB1_iCDF_WB,
    .pred_q8 = &t.silk_NLSF_PRED_WB_Q8,
    .ec_sel = &t.silk_NLSF_CB2_SELECT_WB,
    .ec_icdf = &t.silk_NLSF_CB2_iCDF_WB,
    .delta_min_q15 = &t.silk_NLSF_DELTA_MIN_WB_Q15,
};

const ltp_gain_icdf_ptrs = [3][]const u8{ &t.silk_LTP_gain_iCDF_0, &t.silk_LTP_gain_iCDF_1, &t.silk_LTP_gain_iCDF_2 };
const ltp_vq_ptrs_q7 = [3][]const i8{ &t.silk_LTP_gain_vq_0, &t.silk_LTP_gain_vq_1, &t.silk_LTP_gain_vq_2 };
const lbrr_flags_icdf_ptr = [2][]const u8{ &t.silk_LBRR_flags_2_iCDF, &t.silk_LBRR_flags_3_iCDF };
const resampler_up2_hq_0 = [3]i32{ 1746, 14986, 39083 - 65536 };
const resampler_up2_hq_1 = [3]i32{ 6854, 25769, 55542 - 65536 };

// ---------------------------------------------------------------------------
// Resampler (silk/resampler.c, IIR_FIR + up2_HQ), decoder direction only

const Resampler = struct {
    s_iir: [6]i32 = [_]i32{0} ** 6,
    s_fir: [resampler_order_fir_12]i16 = [_]i16{0} ** resampler_order_fir_12,
    delay_buf: [96]i16 = [_]i16{0} ** 96,
    copy_only: bool = true,
    batch_size: usize = 0,
    inv_ratio_q16: i32 = 0,
    fs_in_khz: usize = 0,
    fs_out_khz: usize = 0,
    input_delay: usize = 0,

    fn init(fs_hz_in: i32, fs_hz_out: i32) !Resampler {
        const delay_matrix_dec = [3][6]u8{
            .{ 4, 0, 2, 0, 0, 0 },
            .{ 0, 9, 4, 7, 4, 4 },
            .{ 0, 3, 12, 7, 7, 7 },
        };
        var s = Resampler{};
        if (fs_hz_in != 8000 and fs_hz_in != 12000 and fs_hz_in != 16000) return error.UnsupportedAudioFormat;
        if (fs_hz_out != 8000 and fs_hz_out != 12000 and fs_hz_out != 16000 and fs_hz_out != 24000 and fs_hz_out != 48000) return error.UnsupportedAudioFormat;
        s.input_delay = delay_matrix_dec[rateId(fs_hz_in)][rateId(fs_hz_out)];
        s.fs_in_khz = @intCast(@divTrunc(fs_hz_in, 1000));
        s.fs_out_khz = @intCast(@divTrunc(fs_hz_out, 1000));
        s.batch_size = s.fs_in_khz * @as(usize, @intCast(resampler_max_batch_size_ms));
        var up2x: u5 = 0;
        if (fs_hz_out > fs_hz_in) {
            if (fs_hz_out == fs_hz_in * 2) return error.UnsupportedAudioFormat; // 2x path unused for 48 kHz output
            s.copy_only = false;
            up2x = 1;
        } else if (fs_hz_out < fs_hz_in) {
            return error.UnsupportedAudioFormat;
        } else {
            s.copy_only = true;
        }
        s.inv_ratio_q16 = lsh(@divTrunc(lsh(fs_hz_in, 14 + up2x), fs_hz_out), 2);
        while (smulww(s.inv_ratio_q16, fs_hz_out) < lsh(fs_hz_in, up2x)) s.inv_ratio_q16 += 1;
        return s;
    }

    fn rateId(r: i32) usize {
        const a = (r >> 12) - @as(i32, @intFromBool(r > 16000));
        const b = a >> @as(u5, @intCast(@intFromBool(r > 24000)));
        return @intCast(@min(5, b - 1));
    }

    fn process(self: *Resampler, out: []i16, in: []const i16, in_len: usize) void {
        const n_samples = self.fs_in_khz - self.input_delay;
        @memcpy(self.delay_buf[self.input_delay .. self.input_delay + n_samples], in[0..n_samples]);
        if (self.copy_only) {
            @memcpy(out[0..self.fs_in_khz], self.delay_buf[0..self.fs_in_khz]);
            @memcpy(out[self.fs_out_khz .. self.fs_out_khz + in_len - self.fs_in_khz], in[n_samples..in_len]);
        } else {
            var delay_copy: [96]i16 = self.delay_buf;
            self.iirFir(out, delay_copy[0..self.fs_in_khz], self.fs_in_khz);
            self.iirFir(out[self.fs_out_khz..], in[n_samples..], in_len - self.fs_in_khz);
        }
        @memcpy(self.delay_buf[0..self.input_delay], in[in_len - self.input_delay .. in_len]);
    }

    fn iirFir(self: *Resampler, out_in: []i16, in_in: []const i16, in_len_in: usize) void {
        var buf: [2 * 160 + resampler_order_fir_12]i16 = undefined;
        @memcpy(buf[0..resampler_order_fir_12], &self.s_fir);
        var out = out_in;
        var in = in_in;
        var in_len = in_len_in;
        var n_samples_in: usize = 0;
        while (true) {
            n_samples_in = @min(in_len, self.batch_size);
            up2Hq(&self.s_iir, buf[resampler_order_fir_12..], in, n_samples_in);
            const max_index_q16: i32 = @intCast(n_samples_in << 17);
            const produced = interpolate(out, &buf, max_index_q16, self.inv_ratio_q16);
            out = out[produced..];
            in = in[n_samples_in..];
            in_len -= n_samples_in;
            if (in_len > 0) {
                std.mem.copyForwards(i16, buf[0..resampler_order_fir_12], buf[n_samples_in << 1 .. (n_samples_in << 1) + resampler_order_fir_12]);
            } else break;
        }
        @memcpy(&self.s_fir, buf[n_samples_in << 1 .. (n_samples_in << 1) + resampler_order_fir_12]);
    }

    fn interpolate(out: []i16, buf: []const i16, max_index_q16: i32, index_increment_q16: i32) usize {
        var produced: usize = 0;
        var index_q16: i32 = 0;
        while (index_q16 < max_index_q16) : (index_q16 += index_increment_q16) {
            const table_index: usize = @intCast(smulwb(index_q16 & 0xFFFF, 12));
            const b = buf[@intCast(index_q16 >> 16)..];
            const fir = &t.silk_resampler_frac_FIR_12;
            var res_q15 = smulbb(b[0], fir[table_index * 4 + 0]);
            res_q15 = smlabb(res_q15, b[1], fir[table_index * 4 + 1]);
            res_q15 = smlabb(res_q15, b[2], fir[table_index * 4 + 2]);
            res_q15 = smlabb(res_q15, b[3], fir[table_index * 4 + 3]);
            res_q15 = smlabb(res_q15, b[4], fir[(11 - table_index) * 4 + 3]);
            res_q15 = smlabb(res_q15, b[5], fir[(11 - table_index) * 4 + 2]);
            res_q15 = smlabb(res_q15, b[6], fir[(11 - table_index) * 4 + 1]);
            res_q15 = smlabb(res_q15, b[7], fir[(11 - table_index) * 4 + 0]);
            out[produced] = @intCast(sat16(rshiftRound(res_q15, 15)));
            produced += 1;
        }
        return produced;
    }

    fn up2Hq(s: *[6]i32, out: []i16, in: []const i16, len: usize) void {
        for (0..len) |k| {
            const in32 = lsh(in[k], 10);
            var y = in32 -% s[0];
            var x = smulwb(y, resampler_up2_hq_0[0]);
            var out32_1 = s[0] +% x;
            s[0] = in32 +% x;
            y = out32_1 -% s[1];
            x = smulwb(y, resampler_up2_hq_0[1]);
            var out32_2 = s[1] +% x;
            s[1] = out32_1 +% x;
            y = out32_2 -% s[2];
            x = smlawb(y, y, resampler_up2_hq_0[2]);
            out32_1 = s[2] +% x;
            s[2] = out32_2 +% x;
            out[2 * k] = @intCast(sat16(rshiftRound(out32_1, 10)));

            y = in32 -% s[3];
            x = smulwb(y, resampler_up2_hq_1[0]);
            out32_1 = s[3] +% x;
            s[3] = in32 +% x;
            y = out32_1 -% s[4];
            x = smulwb(y, resampler_up2_hq_1[1]);
            out32_2 = s[4] +% x;
            s[4] = out32_1 +% x;
            y = out32_2 -% s[5];
            x = smlawb(y, y, resampler_up2_hq_1[2]);
            out32_1 = s[5] +% x;
            s[5] = out32_2 +% x;
            out[2 * k + 1] = @intCast(sat16(rshiftRound(out32_1, 10)));
        }
    }
};

// ---------------------------------------------------------------------------
// Decoder state

const SideInfoIndices = struct {
    gains_indices: [max_nb_subfr]i8 = [_]i8{0} ** max_nb_subfr,
    ltp_index: [max_nb_subfr]i8 = [_]i8{0} ** max_nb_subfr,
    nlsf_indices: [max_lpc_order + 1]i8 = [_]i8{0} ** (max_lpc_order + 1),
    lag_index: i32 = 0,
    contour_index: i32 = 0,
    signal_type: i32 = 0,
    quant_offset_type: i32 = 0,
    nlsf_interp_coef_q2: i32 = 0,
    per_index: i32 = 0,
    ltp_scale_index: i32 = 0,
    seed: i32 = 0,
};

const DecoderControl = struct {
    pitch_l: [max_nb_subfr]i32 = [_]i32{0} ** max_nb_subfr,
    gains_q16: [max_nb_subfr]i32 = [_]i32{0} ** max_nb_subfr,
    pred_coef_q12: [2][max_lpc_order]i16 = [_][max_lpc_order]i16{[_]i16{0} ** max_lpc_order} ** 2,
    ltp_coef_q14: [ltp_order * max_nb_subfr]i16 = [_]i16{0} ** (ltp_order * max_nb_subfr),
    ltp_scale_q14: i32 = 0,
};

const ChannelState = struct {
    prev_gain_q16: i32 = 65536,
    exc_q14: [max_frame_length]i32 = [_]i32{0} ** max_frame_length,
    slpc_q14_buf: [max_lpc_order]i32 = [_]i32{0} ** max_lpc_order,
    out_buf: [max_frame_length + 2 * max_sub_frame_length]i16 = [_]i16{0} ** (max_frame_length + 2 * max_sub_frame_length),
    lag_prev: i32 = 0,
    last_gain_index: i32 = 0,
    fs_khz: i32 = 0,
    fs_api_hz: i32 = 0,
    nb_subfr: usize = 0,
    frame_length: usize = 0,
    subfr_length: usize = 0,
    ltp_mem_length: usize = 0,
    lpc_order: usize = 0,
    prev_nlsf_q15: [max_lpc_order]i16 = [_]i16{0} ** max_lpc_order,
    first_frame_after_reset: bool = true,
    pitch_lag_low_bits_icdf: []const u8 = &t.silk_uniform8_iCDF,
    pitch_contour_icdf: []const u8 = &t.silk_pitch_contour_iCDF,
    n_frames_decoded: usize = 0,
    n_frames_per_packet: usize = 0,
    ec_prev_signal_type: i32 = 0,
    ec_prev_lag_index: i32 = 0,
    vad_flags: [3]bool = .{ false, false, false },
    lbrr_flag: bool = false,
    lbrr_flags: [3]bool = .{ false, false, false },
    resampler: Resampler = .{},
    nlsf_cb: *const NlsfCb = &nlsf_cb_wb,
    indices: SideInfoIndices = .{},
    loss_cnt: i32 = 0,
    prev_signal_type: i32 = 0,

    fn reset(self: *ChannelState) void {
        self.* = .{};
    }

    fn setFs(self: *ChannelState, fs_khz: i32, fs_api_hz: i32) !void {
        self.subfr_length = @intCast(sub_frame_length_ms * fs_khz);
        const frame_length = self.nb_subfr * self.subfr_length;
        if (self.fs_khz != fs_khz or self.fs_api_hz != fs_api_hz) {
            self.resampler = try Resampler.init(fs_khz * 1000, fs_api_hz);
            self.fs_api_hz = fs_api_hz;
        }
        if (self.fs_khz != fs_khz or frame_length != self.frame_length) {
            if (fs_khz == 8) {
                self.pitch_contour_icdf = if (self.nb_subfr == max_nb_subfr) &t.silk_pitch_contour_NB_iCDF else &t.silk_pitch_contour_10_ms_NB_iCDF;
            } else {
                self.pitch_contour_icdf = if (self.nb_subfr == max_nb_subfr) &t.silk_pitch_contour_iCDF else &t.silk_pitch_contour_10_ms_iCDF;
            }
            if (self.fs_khz != fs_khz) {
                self.ltp_mem_length = @intCast(ltp_mem_length_ms * fs_khz);
                if (fs_khz == 8 or fs_khz == 12) {
                    self.lpc_order = min_lpc_order;
                    self.nlsf_cb = &nlsf_cb_nb_mb;
                } else {
                    self.lpc_order = max_lpc_order;
                    self.nlsf_cb = &nlsf_cb_wb;
                }
                self.pitch_lag_low_bits_icdf = switch (fs_khz) {
                    16 => &t.silk_uniform8_iCDF,
                    12 => &t.silk_uniform6_iCDF,
                    8 => &t.silk_uniform4_iCDF,
                    else => return error.UnsupportedAudioFormat,
                };
                self.first_frame_after_reset = true;
                self.lag_prev = 100;
                self.last_gain_index = 10;
                self.prev_signal_type = type_no_voice_activity;
                @memset(&self.out_buf, 0);
                @memset(&self.slpc_q14_buf, 0);
            }
            self.fs_khz = fs_khz;
            self.frame_length = frame_length;
        }
    }
};

const StereoState = struct {
    pred_prev_q13: [2]i32 = .{ 0, 0 },
    s_mid: [2]i16 = .{ 0, 0 },
    s_side: [2]i16 = .{ 0, 0 },
};

pub const Control = struct {
    n_channels_api: u8,
    n_channels_internal: u8,
    api_sample_rate: i32 = 48_000,
    internal_sample_rate: i32,
    payload_size_ms: i32,
};

pub const Decoder = struct {
    channel: [2]ChannelState = .{ .{}, .{} },
    stereo: StereoState = .{},
    n_channels_api: u8 = 0,
    n_channels_internal: u8 = 0,
    prev_decode_only_middle: bool = false,

    pub fn init() Decoder {
        return .{};
    }

    /// silk_ResetDecoder: clears the per-channel and stereo state but keeps
    /// the remembered channel configuration.
    pub fn reset(self: *Decoder) void {
        for (&self.channel) |*c| c.reset();
        self.stereo = .{};
        self.prev_decode_only_middle = false;
    }

    /// silk_Decode for one internal frame: decodes the next 10 or 20 ms
    /// SILK frame from `dec` and writes interleaved float PCM at the API
    /// rate to `out`. Returns the number of samples per channel written.
    pub fn decode(self: *Decoder, ctl: Control, new_packet: bool, dec: *RangeDecoder, out: []f32) !usize {
        const nci: usize = ctl.n_channels_internal;
        const nca: usize = ctl.n_channels_api;
        if (nci == 0 or nci > 2 or nca == 0 or nca > 2) return error.UnsupportedAudioFormat;
        const cs = &self.channel;

        if (new_packet) {
            for (0..nci) |n| cs[n].n_frames_decoded = 0;
        }
        if (nci > self.n_channels_internal) cs[1].reset();
        const stereo_to_mono = nci == 1 and self.n_channels_internal == 2 and ctl.internal_sample_rate == 1000 * cs[0].fs_khz;

        if (cs[0].n_frames_decoded == 0) {
            for (0..nci) |n| {
                switch (ctl.payload_size_ms) {
                    0, 10 => {
                        cs[n].n_frames_per_packet = 1;
                        cs[n].nb_subfr = 2;
                    },
                    20 => {
                        cs[n].n_frames_per_packet = 1;
                        cs[n].nb_subfr = 4;
                    },
                    40 => {
                        cs[n].n_frames_per_packet = 2;
                        cs[n].nb_subfr = 4;
                    },
                    60 => {
                        cs[n].n_frames_per_packet = 3;
                        cs[n].nb_subfr = 4;
                    },
                    else => return error.UnsupportedAudioFormat,
                }
                const fs_khz_dec = (ctl.internal_sample_rate >> 10) + 1;
                if (fs_khz_dec != 8 and fs_khz_dec != 12 and fs_khz_dec != 16) return error.UnsupportedAudioFormat;
                try cs[n].setFs(fs_khz_dec, ctl.api_sample_rate);
            }
        }

        if (nca == 2 and nci == 2 and (self.n_channels_api == 1 or self.n_channels_internal == 1)) {
            self.stereo.pred_prev_q13 = .{ 0, 0 };
            self.stereo.s_side = .{ 0, 0 };
            cs[1].resampler = cs[0].resampler;
        }
        self.n_channels_api = @intCast(nca);
        self.n_channels_internal = @intCast(nci);

        if (cs[0].n_frames_decoded == 0) {
            for (0..nci) |n| {
                for (0..cs[n].n_frames_per_packet) |i| cs[n].vad_flags[i] = (try dec.decodeBitLogp(1)) != 0;
                cs[n].lbrr_flag = (try dec.decodeBitLogp(1)) != 0;
            }
            for (0..nci) |n| {
                cs[n].lbrr_flags = .{ false, false, false };
                if (cs[n].lbrr_flag) {
                    if (cs[n].n_frames_per_packet == 1) {
                        cs[n].lbrr_flags[0] = true;
                    } else {
                        const lbrr_symbol: i32 = @as(i32, try dec.decodeIcdf(lbrr_flags_icdf_ptr[cs[n].n_frames_per_packet - 2])) + 1;
                        for (0..cs[n].n_frames_per_packet) |i| cs[n].lbrr_flags[i] = ((lbrr_symbol >> @intCast(i)) & 1) != 0;
                    }
                }
            }
            // Skip over the LBRR (in-band FEC) frames.
            for (0..cs[0].n_frames_per_packet) |i| {
                for (0..nci) |n| {
                    if (cs[n].lbrr_flags[i]) {
                        var pulses: [max_frame_length]i16 = undefined;
                        if (nci == 2 and n == 0) {
                            var ms_pred_q13: [2]i32 = .{ 0, 0 };
                            try stereoDecodePred(dec, &ms_pred_q13);
                            if (!cs[1].lbrr_flags[i]) _ = try stereoDecodeMidOnly(dec);
                        }
                        const cond_coding: i32 = if (i > 0 and cs[n].lbrr_flags[i - 1]) code_conditionally else code_independently;
                        try decodeIndices(&cs[n], dec, i, true, cond_coding);
                        try decodePulses(dec, &pulses, cs[n].indices.signal_type, cs[n].indices.quant_offset_type, cs[n].frame_length);
                    }
                }
            }
        }

        var ms_pred_q13: [2]i32 = .{ 0, 0 };
        var decode_only_middle = false;
        if (nci == 2) {
            try stereoDecodePred(dec, &ms_pred_q13);
            if (!cs[1].vad_flags[cs[0].n_frames_decoded]) {
                decode_only_middle = try stereoDecodeMidOnly(dec);
            }
        }
        if (nci == 2 and !decode_only_middle and self.prev_decode_only_middle) {
            @memset(&cs[1].out_buf, 0);
            @memset(&cs[1].slpc_q14_buf, 0);
            cs[1].lag_prev = 100;
            cs[1].last_gain_index = 10;
            cs[1].prev_signal_type = type_no_voice_activity;
            cs[1].first_frame_after_reset = true;
        }

        var samples_out1: [2][max_frame_length + 2]i16 = undefined;
        const has_side = !decode_only_middle;
        var n_samples_out_dec: usize = cs[0].frame_length;
        for (0..nci) |n| {
            if (n == 0 or has_side) {
                const frame_index: i32 = @as(i32, @intCast(cs[0].n_frames_decoded)) - @as(i32, @intCast(n));
                var cond_coding: i32 = code_conditionally;
                if (frame_index <= 0) {
                    cond_coding = code_independently;
                } else if (n > 0 and self.prev_decode_only_middle) {
                    cond_coding = code_independently_no_ltp_scaling;
                }
                n_samples_out_dec = try decodeFrame(&cs[n], dec, samples_out1[n][2..], cond_coding);
            } else {
                @memset(samples_out1[n][2 .. 2 + n_samples_out_dec], 0);
            }
            cs[n].n_frames_decoded += 1;
        }

        if (nca == 2 and nci == 2) {
            stereoMsToLr(&self.stereo, &samples_out1[0], &samples_out1[1], &ms_pred_q13, cs[0].fs_khz, n_samples_out_dec);
        } else {
            samples_out1[0][0] = self.stereo.s_mid[0];
            samples_out1[0][1] = self.stereo.s_mid[1];
            self.stereo.s_mid[0] = samples_out1[0][n_samples_out_dec];
            self.stereo.s_mid[1] = samples_out1[0][n_samples_out_dec + 1];
        }

        const n_samples_out: usize = @intCast(@divTrunc(@as(i32, @intCast(n_samples_out_dec)) * ctl.api_sample_rate, cs[0].fs_khz * 1000));
        if (out.len < n_samples_out * nca) return error.UnsupportedAudioFormat;
        var resample_out: [max_frame_length * 6]i16 = undefined;
        for (0..@min(nca, nci)) |n| {
            cs[n].resampler.process(resample_out[0..n_samples_out], samples_out1[n][1..], n_samples_out_dec);
            if (nca == 2) {
                for (0..n_samples_out) |i| out[n + 2 * i] = @as(f32, @floatFromInt(resample_out[i])) * (1.0 / 32768.0);
            } else {
                for (0..n_samples_out) |i| out[i] = @as(f32, @floatFromInt(resample_out[i])) * (1.0 / 32768.0);
            }
        }
        if (nca == 2 and nci == 1) {
            if (stereo_to_mono) {
                cs[1].resampler.process(resample_out[0..n_samples_out], samples_out1[0][1..], n_samples_out_dec);
                for (0..n_samples_out) |i| out[1 + 2 * i] = @as(f32, @floatFromInt(resample_out[i])) * (1.0 / 32768.0);
            } else {
                for (0..n_samples_out) |i| out[1 + 2 * i] = out[2 * i];
            }
        }
        self.prev_decode_only_middle = decode_only_middle;
        return n_samples_out;
    }
};

// ---------------------------------------------------------------------------
// Frame decoding

fn decodeFrame(ps: *ChannelState, dec: *RangeDecoder, out: []i16, cond_coding: i32) !usize {
    const l = ps.frame_length;
    var ctl = DecoderControl{};
    var pulses: [max_frame_length]i16 = [_]i16{0} ** max_frame_length;
    try decodeIndices(ps, dec, ps.n_frames_decoded, false, cond_coding);
    try decodePulses(dec, &pulses, ps.indices.signal_type, ps.indices.quant_offset_type, l);
    try decodeParameters(ps, &ctl, cond_coding);
    decodeCore(ps, &ctl, out[0..l], &pulses);
    const mv_len = ps.ltp_mem_length - l;
    std.mem.copyForwards(i16, ps.out_buf[0..mv_len], ps.out_buf[l .. l + mv_len]);
    @memcpy(ps.out_buf[mv_len .. mv_len + l], out[0..l]);
    ps.loss_cnt = 0;
    ps.prev_signal_type = ps.indices.signal_type;
    ps.first_frame_after_reset = false;
    ps.lag_prev = ctl.pitch_l[ps.nb_subfr - 1];
    return l;
}

fn decodeIndices(ps: *ChannelState, dec: *RangeDecoder, frame_index: usize, decode_lbrr: bool, cond_coding: i32) !void {
    var ix: i32 = 0;
    if (decode_lbrr or ps.vad_flags[frame_index]) {
        ix = @as(i32, try dec.decodeIcdf(&t.silk_type_offset_VAD_iCDF)) + 2;
    } else {
        ix = try dec.decodeIcdf(&t.silk_type_offset_no_VAD_iCDF);
    }
    ps.indices.signal_type = ix >> 1;
    ps.indices.quant_offset_type = ix & 1;

    if (cond_coding == code_conditionally) {
        ps.indices.gains_indices[0] = @intCast(try dec.decodeIcdf(&t.silk_delta_gain_iCDF));
    } else {
        const g: i32 = @as(i32, try dec.decodeIcdf(t.silk_gain_iCDF[@as(usize, @intCast(ps.indices.signal_type)) * 8 ..][0..8])) << 3;
        ps.indices.gains_indices[0] = @intCast(g + @as(i32, try dec.decodeIcdf(&t.silk_uniform8_iCDF)));
    }
    for (1..ps.nb_subfr) |i| ps.indices.gains_indices[i] = @intCast(try dec.decodeIcdf(&t.silk_delta_gain_iCDF));

    const cb = ps.nlsf_cb;
    const cb1_start: usize = @as(usize, @intCast(ps.indices.signal_type >> 1)) * cb.n_vectors;
    ps.indices.nlsf_indices[0] = @intCast(try dec.decodeIcdf(cb.cb1_icdf[cb1_start .. cb1_start + cb.n_vectors]));
    var ec_ix: [max_lpc_order]usize = undefined;
    var pred_q8: [max_lpc_order]u8 = undefined;
    nlsfUnpack(&ec_ix, &pred_q8, cb, @intCast(ps.indices.nlsf_indices[0]));
    for (0..cb.order) |i| {
        var v: i32 = try dec.decodeIcdf(cb.ec_icdf[ec_ix[i]..]);
        if (v == 0) {
            v -= try dec.decodeIcdf(&t.silk_NLSF_EXT_iCDF);
        } else if (v == 2 * nlsf_quant_max_amplitude) {
            v += try dec.decodeIcdf(&t.silk_NLSF_EXT_iCDF);
        }
        ps.indices.nlsf_indices[i + 1] = @intCast(v - nlsf_quant_max_amplitude);
    }
    if (ps.nb_subfr == max_nb_subfr) {
        ps.indices.nlsf_interp_coef_q2 = try dec.decodeIcdf(&t.silk_NLSF_interpolation_factor_iCDF);
    } else {
        ps.indices.nlsf_interp_coef_q2 = 4;
    }

    if (ps.indices.signal_type == type_voiced) {
        var decode_absolute_lag_index = true;
        if (cond_coding == code_conditionally and ps.ec_prev_signal_type == type_voiced) {
            var delta_lag_index: i32 = try dec.decodeIcdf(&t.silk_pitch_delta_iCDF);
            if (delta_lag_index > 0) {
                delta_lag_index -= 9;
                ps.indices.lag_index = ps.ec_prev_lag_index + delta_lag_index;
                decode_absolute_lag_index = false;
            }
        }
        if (decode_absolute_lag_index) {
            ps.indices.lag_index = @as(i32, try dec.decodeIcdf(&t.silk_pitch_lag_iCDF)) * (ps.fs_khz >> 1);
            ps.indices.lag_index += try dec.decodeIcdf(ps.pitch_lag_low_bits_icdf);
        }
        ps.ec_prev_lag_index = ps.indices.lag_index;
        ps.indices.contour_index = try dec.decodeIcdf(ps.pitch_contour_icdf);
        ps.indices.per_index = try dec.decodeIcdf(&t.silk_LTP_per_index_iCDF);
        for (0..ps.nb_subfr) |k| {
            ps.indices.ltp_index[k] = @intCast(try dec.decodeIcdf(ltp_gain_icdf_ptrs[@intCast(ps.indices.per_index)]));
        }
        if (cond_coding == code_independently) {
            ps.indices.ltp_scale_index = try dec.decodeIcdf(&t.silk_LTPscale_iCDF);
        } else {
            ps.indices.ltp_scale_index = 0;
        }
    }
    ps.ec_prev_signal_type = ps.indices.signal_type;
    ps.indices.seed = try dec.decodeIcdf(&t.silk_uniform4_iCDF);
}

fn nlsfUnpack(ec_ix: *[max_lpc_order]usize, pred_q8: *[max_lpc_order]u8, cb: *const NlsfCb, cb1_index: usize) void {
    var sel = cb.ec_sel[cb1_index * cb.order / 2 ..];
    var i: usize = 0;
    while (i < cb.order) : (i += 2) {
        const entry: usize = sel[0];
        sel = sel[1..];
        ec_ix[i] = ((entry >> 1) & 7) * @as(usize, @intCast(2 * nlsf_quant_max_amplitude + 1));
        pred_q8[i] = cb.pred_q8[i + (entry & 1) * (cb.order - 1)];
        ec_ix[i + 1] = ((entry >> 5) & 7) * @as(usize, @intCast(2 * nlsf_quant_max_amplitude + 1));
        pred_q8[i + 1] = cb.pred_q8[i + ((entry >> 4) & 1) * (cb.order - 1) + 1];
    }
}

fn decodePulses(dec: *RangeDecoder, pulses: *[max_frame_length]i16, signal_type: i32, quant_offset_type: i32, frame_length: usize) !void {
    const rate_level_index: usize = try dec.decodeIcdf(t.silk_rate_levels_iCDF[@as(usize, @intCast(signal_type >> 1)) * 9 ..][0..9]);
    var iter = frame_length >> log2_shell_codec_frame_length;
    if (iter * shell_codec_frame_length < frame_length) iter += 1;
    var sum_pulses: [max_nb_shell_blocks]i32 = [_]i32{0} ** max_nb_shell_blocks;
    var n_lshifts: [max_nb_shell_blocks]i32 = [_]i32{0} ** max_nb_shell_blocks;
    const cdf = t.silk_pulses_per_block_iCDF[rate_level_index * 18 ..][0..18];
    for (0..iter) |i| {
        n_lshifts[i] = 0;
        sum_pulses[i] = try dec.decodeIcdf(cdf);
        while (sum_pulses[i] == silk_max_pulses + 1) {
            n_lshifts[i] += 1;
            const extra: usize = @intFromBool(n_lshifts[i] == 10);
            sum_pulses[i] = try dec.decodeIcdf(t.silk_pulses_per_block_iCDF[(n_rate_levels - 1) * 18 + extra ..][0 .. 18 - extra]);
        }
    }
    for (0..iter) |i| {
        const block = pulses[i * shell_codec_frame_length ..][0..shell_codec_frame_length];
        if (sum_pulses[i] > 0) {
            try shellDecoder(block, dec, sum_pulses[i]);
        } else {
            @memset(block, 0);
        }
    }
    for (0..iter) |i| {
        if (n_lshifts[i] > 0) {
            const n_ls = n_lshifts[i];
            const block = pulses[i * shell_codec_frame_length ..][0..shell_codec_frame_length];
            for (0..shell_codec_frame_length) |k| {
                var abs_q: i32 = block[k];
                for (0..@intCast(n_ls)) |_| {
                    abs_q = lsh(abs_q, 1);
                    abs_q += try dec.decodeIcdf(&t.silk_lsb_iCDF);
                }
                block[k] = @intCast(abs_q);
            }
            sum_pulses[i] |= n_ls << 5;
        }
    }
    try decodeSigns(dec, pulses, frame_length, signal_type, quant_offset_type, &sum_pulses);
}

fn decodeSplit(dec: *RangeDecoder, p: i32, table: []const u8) !struct { i16, i16 } {
    if (p > 0) {
        const c1: i16 = try dec.decodeIcdf(table[t.silk_shell_code_table_offsets[@intCast(p)]..]);
        return .{ c1, @as(i16, @intCast(p)) - c1 };
    }
    return .{ 0, 0 };
}

fn shellDecoder(pulses0: *[16]i16, dec: *RangeDecoder, pulses4: i32) !void {
    var pulses3: [2]i16 = undefined;
    var pulses2: [4]i16 = undefined;
    var pulses1: [8]i16 = undefined;
    pulses3[0], pulses3[1] = try decodeSplit(dec, pulses4, &t.silk_shell_code_table3);
    pulses2[0], pulses2[1] = try decodeSplit(dec, pulses3[0], &t.silk_shell_code_table2);
    pulses1[0], pulses1[1] = try decodeSplit(dec, pulses2[0], &t.silk_shell_code_table1);
    pulses0[0], pulses0[1] = try decodeSplit(dec, pulses1[0], &t.silk_shell_code_table0);
    pulses0[2], pulses0[3] = try decodeSplit(dec, pulses1[1], &t.silk_shell_code_table0);
    pulses1[2], pulses1[3] = try decodeSplit(dec, pulses2[1], &t.silk_shell_code_table1);
    pulses0[4], pulses0[5] = try decodeSplit(dec, pulses1[2], &t.silk_shell_code_table0);
    pulses0[6], pulses0[7] = try decodeSplit(dec, pulses1[3], &t.silk_shell_code_table0);
    pulses2[2], pulses2[3] = try decodeSplit(dec, pulses3[1], &t.silk_shell_code_table2);
    pulses1[4], pulses1[5] = try decodeSplit(dec, pulses2[2], &t.silk_shell_code_table1);
    pulses0[8], pulses0[9] = try decodeSplit(dec, pulses1[4], &t.silk_shell_code_table0);
    pulses0[10], pulses0[11] = try decodeSplit(dec, pulses1[5], &t.silk_shell_code_table0);
    pulses1[6], pulses1[7] = try decodeSplit(dec, pulses2[3], &t.silk_shell_code_table1);
    pulses0[12], pulses0[13] = try decodeSplit(dec, pulses1[6], &t.silk_shell_code_table0);
    pulses0[14], pulses0[15] = try decodeSplit(dec, pulses1[7], &t.silk_shell_code_table0);
}

fn decodeSigns(dec: *RangeDecoder, pulses: *[max_frame_length]i16, length_in: usize, signal_type: i32, quant_offset_type: i32, sum_pulses: *const [max_nb_shell_blocks]i32) !void {
    var icdf: [2]u8 = .{ 0, 0 };
    const base: usize = @intCast(7 * (quant_offset_type + (signal_type << 1)));
    const icdf_ptr = t.silk_sign_iCDF[base..];
    const length = (length_in + shell_codec_frame_length / 2) >> log2_shell_codec_frame_length;
    for (0..length) |i| {
        const p = sum_pulses[i];
        if (p > 0) {
            icdf[0] = icdf_ptr[@intCast(@min(p & 0x1F, 6))];
            const q = pulses[i * shell_codec_frame_length ..][0..shell_codec_frame_length];
            for (0..shell_codec_frame_length) |j| {
                if (q[j] > 0) {
                    const s: i32 = (@as(i32, try dec.decodeIcdf(&icdf)) << 1) - 1;
                    q[j] = @intCast(@as(i32, q[j]) * s);
                }
            }
        }
    }
}

fn gainsDequant(gain_q16: *[max_nb_subfr]i32, ind: *const [max_nb_subfr]i8, prev_ind: *i32, conditional: bool, nb_subfr: usize) void {
    for (0..nb_subfr) |k| {
        if (k == 0 and !conditional) {
            prev_ind.* = @max(@as(i32, ind[k]), prev_ind.* - 16);
        } else {
            const ind_tmp: i32 = @as(i32, ind[k]) + min_delta_gain_quant;
            const double_step_size_threshold = 2 * max_delta_gain_quant - n_levels_qgain + prev_ind.*;
            if (ind_tmp > double_step_size_threshold) {
                prev_ind.* += lsh(ind_tmp, 1) - double_step_size_threshold;
            } else {
                prev_ind.* += ind_tmp;
            }
        }
        prev_ind.* = @max(0, @min(n_levels_qgain - 1, prev_ind.*));
        gain_q16[k] = log2lin(@min(smulwb(gain_inv_scale_q16, prev_ind.*) + gain_offset, 3967));
    }
}

fn nlsfResidualDequant(x_q10: *[max_lpc_order]i16, indices: []const i8, pred_coef_q8: *const [max_lpc_order]u8, quant_step_size_q16: i32, order: usize) void {
    var out_q10: i32 = 0;
    var i: usize = order;
    while (i > 0) {
        i -= 1;
        const pred_q10 = rsh(smulbb(out_q10, pred_coef_q8[i]), 8);
        out_q10 = lsh(indices[i], 10);
        if (out_q10 > 0) {
            out_q10 -= fixConst(0.1, 10);
        } else if (out_q10 < 0) {
            out_q10 += fixConst(0.1, 10);
        }
        out_q10 = smlawb(pred_q10, out_q10, quant_step_size_q16);
        x_q10[i] = @intCast(out_q10);
    }
}

fn nlsfDecode(nlsf_q15: *[max_lpc_order]i16, nlsf_indices: *const [max_lpc_order + 1]i8, cb: *const NlsfCb) void {
    var pred_q8: [max_lpc_order]u8 = undefined;
    var ec_ix: [max_lpc_order]usize = undefined;
    var res_q10: [max_lpc_order]i16 = undefined;
    const cb1: usize = @intCast(nlsf_indices[0]);
    nlsfUnpack(&ec_ix, &pred_q8, cb, cb1);
    nlsfResidualDequant(&res_q10, nlsf_indices[1..], &pred_q8, cb.quant_step_size_q16, cb.order);
    const cb_element = cb.cb1_nlsf_q8[cb1 * cb.order ..];
    const cb_wght_q9 = cb.cb1_wght_q9[cb1 * cb.order ..];
    for (0..cb.order) |i| {
        const tmp: i32 = @divTrunc(lsh(res_q10[i], 14), @as(i32, cb_wght_q9[i])) +% lsh(cb_element[i], 7);
        nlsf_q15[i] = @intCast(@max(0, @min(32767, tmp)));
    }
    nlsfStabilize(nlsf_q15, cb.delta_min_q15, cb.order);
}

fn nlsfStabilize(nlsf_q15: *[max_lpc_order]i16, delta_min_q15: []const i16, l: usize) void {
    const max_loops = 20;
    var loops: usize = 0;
    while (loops < max_loops) : (loops += 1) {
        var min_diff_q15: i32 = @as(i32, nlsf_q15[0]) - delta_min_q15[0];
        var idx: usize = 0;
        for (1..l) |i| {
            const diff_q15: i32 = @as(i32, nlsf_q15[i]) - (@as(i32, nlsf_q15[i - 1]) + delta_min_q15[i]);
            if (diff_q15 < min_diff_q15) {
                min_diff_q15 = diff_q15;
                idx = i;
            }
        }
        const last_diff: i32 = (1 << 15) - (@as(i32, nlsf_q15[l - 1]) + delta_min_q15[l]);
        if (last_diff < min_diff_q15) {
            min_diff_q15 = last_diff;
            idx = l;
        }
        if (min_diff_q15 >= 0) return;
        if (idx == 0) {
            nlsf_q15[0] = delta_min_q15[0];
        } else if (idx == l) {
            nlsf_q15[l - 1] = @intCast((1 << 15) - @as(i32, delta_min_q15[l]));
        } else {
            var min_center_q15: i32 = 0;
            for (0..idx) |k| min_center_q15 += delta_min_q15[k];
            min_center_q15 += rsh(delta_min_q15[idx], 1);
            var max_center_q15: i32 = 1 << 15;
            var k: usize = l;
            while (k > idx) : (k -= 1) max_center_q15 -= delta_min_q15[k];
            max_center_q15 -= rsh(delta_min_q15[idx], 1);
            const center = @max(min_center_q15, @min(max_center_q15, rshiftRound(@as(i32, nlsf_q15[idx - 1]) + @as(i32, nlsf_q15[idx]), 1)));
            nlsf_q15[idx - 1] = @intCast(center - rsh(delta_min_q15[idx], 1));
            nlsf_q15[idx] = @intCast(@as(i32, nlsf_q15[idx - 1]) + delta_min_q15[idx]);
        }
    }
    if (loops == max_loops) {
        std.mem.sort(i16, nlsf_q15[0..l], {}, std.sort.asc(i16));
        nlsf_q15[0] = @intCast(@max(@as(i32, nlsf_q15[0]), @as(i32, delta_min_q15[0])));
        for (1..l) |i| {
            nlsf_q15[i] = @intCast(@max(@as(i32, nlsf_q15[i]), sat16(@as(i32, nlsf_q15[i - 1]) + delta_min_q15[i])));
        }
        nlsf_q15[l - 1] = @intCast(@min(@as(i32, nlsf_q15[l - 1]), (1 << 15) - @as(i32, delta_min_q15[l])));
        var i: usize = l - 1;
        while (i > 0) {
            i -= 1;
            nlsf_q15[i] = @intCast(@min(@as(i32, nlsf_q15[i]), @as(i32, nlsf_q15[i + 1]) - delta_min_q15[i + 1]));
        }
    }
}

fn nlsf2aFindPoly(out: []i32, c_lsf: []const i32, dd: usize) void {
    const qa: u5 = 16;
    out[0] = lsh(1, qa);
    out[1] = -c_lsf[0];
    for (1..dd) |k| {
        const ftmp = c_lsf[2 * k];
        out[k + 1] = lsh(out[k - 1], 1) -% @as(i32, @truncate(rshiftRound64(@as(i64, ftmp) * @as(i64, out[k]), qa)));
        var n: usize = k;
        while (n > 1) : (n -= 1) {
            out[n] = out[n] +% out[n - 2] -% @as(i32, @truncate(rshiftRound64(@as(i64, ftmp) * @as(i64, out[n - 1]), qa)));
        }
        out[1] -%= ftmp;
    }
}

fn nlsf2a(a_q12: *[max_lpc_order]i16, nlsf: *const [max_lpc_order]i16, d: usize) void {
    const qa: u5 = 16;
    const ordering16 = [16]u8{ 0, 15, 8, 7, 4, 11, 12, 3, 2, 13, 10, 5, 6, 9, 14, 1 };
    const ordering10 = [10]u8{ 0, 9, 6, 3, 4, 5, 8, 1, 2, 7 };
    const ordering: []const u8 = if (d == 16) &ordering16 else &ordering10;
    var cos_lsf_qa: [max_lpc_order]i32 = undefined;
    for (0..d) |k| {
        const f_int: i32 = rsh(nlsf[k], 15 - 7);
        const f_frac: i32 = @as(i32, nlsf[k]) - lsh(f_int, 15 - 7);
        const cos_val: i32 = t.silk_LSFCosTab_FIX_Q12[@intCast(f_int)];
        const delta: i32 = @as(i32, t.silk_LSFCosTab_FIX_Q12[@intCast(f_int + 1)]) - cos_val;
        cos_lsf_qa[ordering[k]] = rshiftRound(lsh(cos_val, 8) + delta * f_frac, 20 - qa);
    }
    const dd = d >> 1;
    var p: [max_lpc_order / 2 + 1]i32 = undefined;
    var q: [max_lpc_order / 2 + 1]i32 = undefined;
    nlsf2aFindPoly(&p, cos_lsf_qa[0..], dd);
    nlsf2aFindPoly(&q, cos_lsf_qa[1..], dd);
    var a32_qa1: [max_lpc_order]i32 = undefined;
    for (0..dd) |k| {
        const ptmp = p[k + 1] +% p[k];
        const qtmp = q[k + 1] -% q[k];
        a32_qa1[k] = -qtmp -% ptmp;
        a32_qa1[d - k - 1] = qtmp -% ptmp;
    }
    lpcFit(a_q12, &a32_qa1, 12, qa + 1, d);
    var i: usize = 0;
    while (lpcInversePredGain(a_q12, d) == 0 and i < max_lpc_stabilize_iterations) : (i += 1) {
        bwexpander32(&a32_qa1, d, 65536 - lsh(2, @intCast(i)));
        for (0..d) |k| a_q12[k] = @intCast(rshiftRound(a32_qa1[k], qa + 1 - 12));
    }
}

fn lpcFit(a_qout: *[max_lpc_order]i16, a_qin: *[max_lpc_order]i32, qout: u5, qin: u5, d: usize) void {
    var idx: usize = 0;
    var i: usize = 0;
    while (i < 10) : (i += 1) {
        var maxabs: i32 = 0;
        for (0..d) |k| {
            const absval: i32 = @intCast(@abs(a_qin[k]));
            if (absval > maxabs) {
                maxabs = absval;
                idx = k;
            }
        }
        maxabs = rshiftRound(maxabs, qin - qout);
        if (maxabs > 32767) {
            maxabs = @min(maxabs, 163838);
            const chirp_q16 = fixConst(0.999, 16) - @divTrunc(lsh(maxabs - 32767, 14), rsh(maxabs * @as(i32, @intCast(idx + 1)), 2));
            bwexpander32(a_qin, d, chirp_q16);
        } else break;
    }
    if (i == 10) {
        for (0..d) |k| {
            a_qout[k] = @intCast(sat16(rshiftRound(a_qin[k], qin - qout)));
            a_qin[k] = lsh(a_qout[k], qin - qout);
        }
    } else {
        for (0..d) |k| a_qout[k] = @intCast(rshiftRound(a_qin[k], qin - qout));
    }
}

fn bwexpander32(ar: *[max_lpc_order]i32, d: usize, chirp_q16_in: i32) void {
    var chirp_q16 = chirp_q16_in;
    const chirp_minus_one_q16 = chirp_q16 - 65536;
    for (0..d - 1) |i| {
        ar[i] = smulww(chirp_q16, ar[i]);
        chirp_q16 += rshiftRound(chirp_q16 *% chirp_minus_one_q16, 16);
    }
    ar[d - 1] = smulww(chirp_q16, ar[d - 1]);
}

fn lpcInversePredGain(a_q12: *const [max_lpc_order]i16, order: usize) i32 {
    const qa: u5 = 24;
    const a_limit: i32 = fixConst(0.99975, 24);
    var a_qa: [max_lpc_order]i32 = undefined;
    var dc_resp: i32 = 0;
    for (0..order) |k| {
        dc_resp += a_q12[k];
        a_qa[k] = lsh(a_q12[k], qa - 12);
    }
    if (dc_resp >= 4096) return 0;

    var inv_gain_q30: i32 = 1 << 30;
    var k: usize = order - 1;
    while (k > 0) : (k -= 1) {
        if (a_qa[k] > a_limit or a_qa[k] < -a_limit) return 0;
        const rc_q31 = -lsh(a_qa[k], 31 - qa);
        const rc_mult1_q30 = (@as(i32, 1) << 30) -% smmul(rc_q31, rc_q31);
        inv_gain_q30 = lsh(smmul(inv_gain_q30, rc_mult1_q30), 2);
        if (inv_gain_q30 < fixConst(1.0 / 1e4, 30)) return 0;
        const mult2q: i32 = 32 - clzAbs(rc_mult1_q30);
        const rc_mult2 = inverse32VarQ(rc_mult1_q30, mult2q + 30);
        for (0..(k + 1) >> 1) |n| {
            const tmp1 = a_qa[n];
            const tmp2 = a_qa[k - n - 1];
            const m1: i32 = @truncate(rshiftRound64(@as(i64, tmp2) * @as(i64, rc_q31), 31));
            var tmp64 = rshiftRound64(@as(i64, tmp1 -| m1) * @as(i64, rc_mult2), @intCast(mult2q));
            if (tmp64 > std.math.maxInt(i32) or tmp64 < std.math.minInt(i32)) return 0;
            a_qa[n] = @intCast(tmp64);
            const m2: i32 = @truncate(rshiftRound64(@as(i64, tmp1) * @as(i64, rc_q31), 31));
            tmp64 = rshiftRound64(@as(i64, tmp2 -| m2) * @as(i64, rc_mult2), @intCast(mult2q));
            if (tmp64 > std.math.maxInt(i32) or tmp64 < std.math.minInt(i32)) return 0;
            a_qa[k - n - 1] = @intCast(tmp64);
        }
    }
    if (a_qa[0] > a_limit or a_qa[0] < -a_limit) return 0;
    const rc_q31 = -lsh(a_qa[0], 31 - qa);
    const rc_mult1_q30 = (@as(i32, 1) << 30) -% smmul(rc_q31, rc_q31);
    inv_gain_q30 = lsh(smmul(inv_gain_q30, rc_mult1_q30), 2);
    if (inv_gain_q30 < fixConst(1.0 / 1e4, 30)) return 0;
    return inv_gain_q30;
}

fn decodePitch(lag_index: i32, contour_index: i32, pitch_lags: *[max_nb_subfr]i32, fs_khz: i32, nb_subfr: usize) void {
    var lag_cb: []const i8 = undefined;
    var cbk_size: usize = 0;
    if (fs_khz == 8) {
        if (nb_subfr == max_nb_subfr) {
            lag_cb = &t.silk_CB_lags_stage2;
            cbk_size = 11;
        } else {
            lag_cb = &t.silk_CB_lags_stage2_10_ms;
            cbk_size = 3;
        }
    } else {
        if (nb_subfr == max_nb_subfr) {
            lag_cb = &t.silk_CB_lags_stage3;
            cbk_size = 34;
        } else {
            lag_cb = &t.silk_CB_lags_stage3_10_ms;
            cbk_size = 12;
        }
    }
    const min_lag = pe_min_lag_ms * fs_khz;
    const max_lag = pe_max_lag_ms * fs_khz;
    const lag = min_lag + lag_index;
    for (0..nb_subfr) |k| {
        const v = lag + lag_cb[k * cbk_size + @as(usize, @intCast(contour_index))];
        pitch_lags[k] = @max(min_lag, @min(max_lag, v));
    }
}

fn decodeParameters(ps: *ChannelState, ctl: *DecoderControl, cond_coding: i32) !void {
    var nlsf_q15: [max_lpc_order]i16 = undefined;
    var nlsf0_q15: [max_lpc_order]i16 = undefined;
    gainsDequant(&ctl.gains_q16, &ps.indices.gains_indices, &ps.last_gain_index, cond_coding == code_conditionally, ps.nb_subfr);
    nlsfDecode(&nlsf_q15, &ps.indices.nlsf_indices, ps.nlsf_cb);
    nlsf2a(&ctl.pred_coef_q12[1], &nlsf_q15, ps.lpc_order);
    if (ps.first_frame_after_reset) ps.indices.nlsf_interp_coef_q2 = 4;
    if (ps.indices.nlsf_interp_coef_q2 < 4) {
        for (0..ps.lpc_order) |i| {
            nlsf0_q15[i] = @intCast(@as(i32, ps.prev_nlsf_q15[i]) + rsh(ps.indices.nlsf_interp_coef_q2 * (@as(i32, nlsf_q15[i]) - ps.prev_nlsf_q15[i]), 2));
        }
        nlsf2a(&ctl.pred_coef_q12[0], &nlsf0_q15, ps.lpc_order);
    } else {
        ctl.pred_coef_q12[0] = ctl.pred_coef_q12[1];
    }
    ps.prev_nlsf_q15 = nlsf_q15;

    if (ps.indices.signal_type == type_voiced) {
        decodePitch(ps.indices.lag_index, ps.indices.contour_index, &ctl.pitch_l, ps.fs_khz, ps.nb_subfr);
        const cbk = ltp_vq_ptrs_q7[@intCast(ps.indices.per_index)];
        for (0..ps.nb_subfr) |k| {
            const ix: usize = @intCast(ps.indices.ltp_index[k]);
            for (0..ltp_order) |i| ctl.ltp_coef_q14[k * ltp_order + i] = @intCast(lsh(cbk[ix * ltp_order + i], 7));
        }
        ctl.ltp_scale_q14 = t.silk_LTPScales_table_Q14[@intCast(ps.indices.ltp_scale_index)];
    } else {
        @memset(&ctl.pitch_l, 0);
        @memset(&ctl.ltp_coef_q14, 0);
        ps.indices.per_index = 0;
        ctl.ltp_scale_q14 = 0;
    }
}

fn lpcAnalysisFilter(out: []i16, in: []const i16, b: []const i16, len: usize, d: usize) void {
    var ix: usize = d;
    while (ix < len) : (ix += 1) {
        const p = in[0..ix];
        var out32_q12 = smulbb(p[ix - 1], b[0]);
        out32_q12 = smlabb(out32_q12, p[ix - 2], b[1]);
        out32_q12 = smlabb(out32_q12, p[ix - 3], b[2]);
        out32_q12 = smlabb(out32_q12, p[ix - 4], b[3]);
        out32_q12 = smlabb(out32_q12, p[ix - 5], b[4]);
        out32_q12 = smlabb(out32_q12, p[ix - 6], b[5]);
        var j: usize = 6;
        while (j < d) : (j += 2) {
            out32_q12 = smlabb(out32_q12, p[ix - 1 - j], b[j]);
            out32_q12 = smlabb(out32_q12, p[ix - 2 - j], b[j + 1]);
        }
        out32_q12 = lsh(in[ix], 12) -% out32_q12;
        out[ix] = @intCast(sat16(rshiftRound(out32_q12, 12)));
    }
    @memset(out[0..d], 0);
}

fn decodeCore(ps: *ChannelState, ctl: *DecoderControl, xq: []i16, pulses: *const [max_frame_length]i16) void {
    var sltp: [max_frame_length]i16 = undefined;
    var sltp_q15: [2 * max_frame_length]i32 = undefined;
    var res_q14: [max_sub_frame_length]i32 = undefined;
    var slpc_q14: [max_sub_frame_length + max_lpc_order]i32 = undefined;
    const offset_q10: i32 = t.silk_Quantization_Offsets_Q10[@as(usize, @intCast(ps.indices.signal_type >> 1)) * 2 + @as(usize, @intCast(ps.indices.quant_offset_type))];
    const nlsf_interpolation_flag = ps.indices.nlsf_interp_coef_q2 < 4;

    var rand_seed = ps.indices.seed;
    for (0..ps.frame_length) |i| {
        rand_seed = silkRand(rand_seed);
        var e = lsh(pulses[i], 14);
        if (e > 0) {
            e -= quant_level_adjust_q10 << 4;
        } else if (e < 0) {
            e += quant_level_adjust_q10 << 4;
        }
        e += offset_q10 << 4;
        if (rand_seed < 0) e = -e;
        ps.exc_q14[i] = e;
        rand_seed = rand_seed +% pulses[i];
    }

    @memcpy(slpc_q14[0..max_lpc_order], &ps.slpc_q14_buf);
    var exc_off: usize = 0;
    var xq_off: usize = 0;
    var sltp_buf_idx = ps.ltp_mem_length;
    var lag: i32 = 0;
    for (0..ps.nb_subfr) |k| {
        var pres_q14: []i32 = res_q14[0..ps.subfr_length];
        const a_q12: []const i16 = ctl.pred_coef_q12[k >> 1][0..];
        var a_q12_tmp: [max_lpc_order]i16 = undefined;
        @memcpy(a_q12_tmp[0..ps.lpc_order], a_q12[0..ps.lpc_order]);
        const b_q14 = ctl.ltp_coef_q14[k * ltp_order ..][0..ltp_order];
        const signal_type = ps.indices.signal_type;
        const gain_q10 = rsh(ctl.gains_q16[k], 6);
        var inv_gain_q31 = inverse32VarQ(ctl.gains_q16[k], 47);
        var gain_adj_q16: i32 = 1 << 16;
        if (ctl.gains_q16[k] != ps.prev_gain_q16) {
            gain_adj_q16 = div32VarQ(ps.prev_gain_q16, ctl.gains_q16[k], 16);
            for (0..max_lpc_order) |i| slpc_q14[i] = smulww(gain_adj_q16, slpc_q14[i]);
        }
        ps.prev_gain_q16 = ctl.gains_q16[k];

        if (signal_type == type_voiced) {
            lag = ctl.pitch_l[k];
            if (k == 0 or (k == 2 and nlsf_interpolation_flag)) {
                const start_idx: usize = ps.ltp_mem_length - @as(usize, @intCast(lag)) - ps.lpc_order - ltp_order / 2;
                if (k == 2) {
                    @memcpy(ps.out_buf[ps.ltp_mem_length .. ps.ltp_mem_length + 2 * ps.subfr_length], xq[0 .. 2 * ps.subfr_length]);
                }
                lpcAnalysisFilter(sltp[start_idx..], ps.out_buf[start_idx + k * ps.subfr_length ..], a_q12, ps.ltp_mem_length - start_idx, ps.lpc_order);
                if (k == 0) {
                    inv_gain_q31 = lsh(smulwb(inv_gain_q31, ctl.ltp_scale_q14), 2);
                }
                for (0..@as(usize, @intCast(lag)) + ltp_order / 2) |i| {
                    sltp_q15[sltp_buf_idx - i - 1] = smulwb(inv_gain_q31, sltp[ps.ltp_mem_length - i - 1]);
                }
            } else if (gain_adj_q16 != (1 << 16)) {
                for (0..@as(usize, @intCast(lag)) + ltp_order / 2) |i| {
                    sltp_q15[sltp_buf_idx - i - 1] = smulww(gain_adj_q16, sltp_q15[sltp_buf_idx - i - 1]);
                }
            }
        }

        if (signal_type == type_voiced) {
            var pred_lag_idx: usize = sltp_buf_idx - @as(usize, @intCast(lag)) + ltp_order / 2;
            for (0..ps.subfr_length) |i| {
                var ltp_pred_q13: i32 = 2;
                ltp_pred_q13 = smlawb(ltp_pred_q13, sltp_q15[pred_lag_idx], b_q14[0]);
                ltp_pred_q13 = smlawb(ltp_pred_q13, sltp_q15[pred_lag_idx - 1], b_q14[1]);
                ltp_pred_q13 = smlawb(ltp_pred_q13, sltp_q15[pred_lag_idx - 2], b_q14[2]);
                ltp_pred_q13 = smlawb(ltp_pred_q13, sltp_q15[pred_lag_idx - 3], b_q14[3]);
                ltp_pred_q13 = smlawb(ltp_pred_q13, sltp_q15[pred_lag_idx - 4], b_q14[4]);
                pred_lag_idx += 1;
                res_q14[i] = ps.exc_q14[exc_off + i] +% lsh(ltp_pred_q13, 1);
                sltp_q15[sltp_buf_idx] = lsh(res_q14[i], 1);
                sltp_buf_idx += 1;
            }
        } else {
            pres_q14 = ps.exc_q14[exc_off .. exc_off + ps.subfr_length];
        }

        for (0..ps.subfr_length) |i| {
            var lpc_pred_q10: i32 = @intCast(ps.lpc_order >> 1);
            const h = slpc_q14[0 .. max_lpc_order + i];
            lpc_pred_q10 = smlawb(lpc_pred_q10, h[h.len - 1], a_q12_tmp[0]);
            lpc_pred_q10 = smlawb(lpc_pred_q10, h[h.len - 2], a_q12_tmp[1]);
            lpc_pred_q10 = smlawb(lpc_pred_q10, h[h.len - 3], a_q12_tmp[2]);
            lpc_pred_q10 = smlawb(lpc_pred_q10, h[h.len - 4], a_q12_tmp[3]);
            lpc_pred_q10 = smlawb(lpc_pred_q10, h[h.len - 5], a_q12_tmp[4]);
            lpc_pred_q10 = smlawb(lpc_pred_q10, h[h.len - 6], a_q12_tmp[5]);
            lpc_pred_q10 = smlawb(lpc_pred_q10, h[h.len - 7], a_q12_tmp[6]);
            lpc_pred_q10 = smlawb(lpc_pred_q10, h[h.len - 8], a_q12_tmp[7]);
            lpc_pred_q10 = smlawb(lpc_pred_q10, h[h.len - 9], a_q12_tmp[8]);
            lpc_pred_q10 = smlawb(lpc_pred_q10, h[h.len - 10], a_q12_tmp[9]);
            if (ps.lpc_order == 16) {
                lpc_pred_q10 = smlawb(lpc_pred_q10, h[h.len - 11], a_q12_tmp[10]);
                lpc_pred_q10 = smlawb(lpc_pred_q10, h[h.len - 12], a_q12_tmp[11]);
                lpc_pred_q10 = smlawb(lpc_pred_q10, h[h.len - 13], a_q12_tmp[12]);
                lpc_pred_q10 = smlawb(lpc_pred_q10, h[h.len - 14], a_q12_tmp[13]);
                lpc_pred_q10 = smlawb(lpc_pred_q10, h[h.len - 15], a_q12_tmp[14]);
                lpc_pred_q10 = smlawb(lpc_pred_q10, h[h.len - 16], a_q12_tmp[15]);
            }
            slpc_q14[max_lpc_order + i] = pres_q14[i] +| (lpc_pred_q10 <<| 4);
            xq[xq_off + i] = @intCast(sat16(rshiftRound(smulww(slpc_q14[max_lpc_order + i], gain_q10), 8)));
        }
        std.mem.copyForwards(i32, slpc_q14[0..max_lpc_order], slpc_q14[ps.subfr_length .. ps.subfr_length + max_lpc_order]);
        exc_off += ps.subfr_length;
        xq_off += ps.subfr_length;
    }
    @memcpy(&ps.slpc_q14_buf, slpc_q14[0..max_lpc_order]);
}

// ---------------------------------------------------------------------------
// Stereo

fn stereoDecodePred(dec: *RangeDecoder, pred_q13: *[2]i32) !void {
    var ix: [2][3]i32 = undefined;
    const n: i32 = try dec.decodeIcdf(&t.silk_stereo_pred_joint_iCDF);
    ix[0][2] = @divTrunc(n, 5);
    ix[1][2] = n - 5 * ix[0][2];
    for (0..2) |c| {
        ix[c][0] = try dec.decodeIcdf(&t.silk_uniform3_iCDF);
        ix[c][1] = try dec.decodeIcdf(&t.silk_uniform5_iCDF);
    }
    for (0..2) |c| {
        ix[c][0] += 3 * ix[c][2];
        const low_q13: i32 = t.silk_stereo_pred_quant_Q13[@intCast(ix[c][0])];
        const step_q13 = smulwb(@as(i32, t.silk_stereo_pred_quant_Q13[@intCast(ix[c][0] + 1)]) - low_q13, fixConst(0.5 / stereo_quant_sub_steps, 16));
        pred_q13[c] = smlabb(low_q13, step_q13, 2 * ix[c][1] + 1);
    }
    pred_q13[0] -= pred_q13[1];
}

fn stereoDecodeMidOnly(dec: *RangeDecoder) !bool {
    return (try dec.decodeIcdf(&t.silk_stereo_only_code_mid_iCDF)) != 0;
}

fn stereoMsToLr(state: *StereoState, x1: *[max_frame_length + 2]i16, x2: *[max_frame_length + 2]i16, pred_q13: *const [2]i32, fs_khz: i32, frame_length: usize) void {
    x1[0] = state.s_mid[0];
    x1[1] = state.s_mid[1];
    x2[0] = state.s_side[0];
    x2[1] = state.s_side[1];
    state.s_mid = .{ x1[frame_length], x1[frame_length + 1] };
    state.s_side = .{ x2[frame_length], x2[frame_length + 1] };

    var pred0_q13 = state.pred_prev_q13[0];
    var pred1_q13 = state.pred_prev_q13[1];
    const interp_len: usize = @intCast(stereo_interp_len_ms * fs_khz);
    const denom_q16 = @divTrunc(@as(i32, 1) << 16, stereo_interp_len_ms * fs_khz);
    const delta0_q13 = rshiftRound(smulbb(pred_q13[0] - state.pred_prev_q13[0], denom_q16), 16);
    const delta1_q13 = rshiftRound(smulbb(pred_q13[1] - state.pred_prev_q13[1], denom_q16), 16);
    for (0..interp_len) |n| {
        pred0_q13 += delta0_q13;
        pred1_q13 += delta1_q13;
        var sum = lsh((@as(i32, x1[n]) + @as(i32, x1[n + 2])) +% lsh(x1[n + 1], 1), 9);
        sum = smlawb(lsh(x2[n + 1], 8), sum, pred0_q13);
        sum = smlawb(sum, lsh(x1[n + 1], 11), pred1_q13);
        x2[n + 1] = @intCast(sat16(rshiftRound(sum, 8)));
    }
    pred0_q13 = pred_q13[0];
    pred1_q13 = pred_q13[1];
    for (interp_len..frame_length) |n| {
        var sum = lsh((@as(i32, x1[n]) + @as(i32, x1[n + 2])) +% lsh(x1[n + 1], 1), 9);
        sum = smlawb(lsh(x2[n + 1], 8), sum, pred0_q13);
        sum = smlawb(sum, lsh(x1[n + 1], 11), pred1_q13);
        x2[n + 1] = @intCast(sat16(rshiftRound(sum, 8)));
    }
    state.pred_prev_q13[0] = pred_q13[0];
    state.pred_prev_q13[1] = pred_q13[1];
    for (0..frame_length) |n| {
        const sum = @as(i32, x1[n + 1]) + @as(i32, x2[n + 1]);
        const diff = @as(i32, x1[n + 1]) - @as(i32, x2[n + 1]);
        x1[n + 1] = @intCast(sat16(sum));
        x2[n + 1] = @intCast(sat16(diff));
    }
}

test "fixed point helpers match the reference definitions" {
    try std.testing.expectEqual(@as(i32, 65536), log2lin(lin2log(65536)));
    try std.testing.expectEqual(@as(i32, 2048), lin2log(65536));
    try std.testing.expect(@abs(div32VarQ(1000, 1000, 16) - (1 << 16)) <= 1);
    try std.testing.expectEqual(@as(i32, 0), clz32(-1));
    try std.testing.expectEqual(@as(i32, 32), clz32(0));
}
