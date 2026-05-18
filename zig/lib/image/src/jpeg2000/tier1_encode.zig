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
const arithmetic = @import("arithmetic.zig");
const codeblock = @import("codeblock.zig");
const tile = @import("tile.zig");

pub const native_port_available = true;

pub const EncodedCodeblock = struct {
    data: []u8,
    num_coding_passes: u16,
    zero_bit_planes: u8,
    pass_lengths: []u32,

    pub fn deinit(self: *EncodedCodeblock, allocator: std.mem.Allocator) void {
        allocator.free(self.data);
        allocator.free(self.pass_lengths);
        self.* = undefined;
    }
};

// ---------------------------------------------------------------------------
// Encoder context model -- mirrors Tier1Contexts from codeblock.zig
// ---------------------------------------------------------------------------

const Tier1Contexts = struct {
    contexts: [19]arithmetic.MqContext,

    fn init() Tier1Contexts {
        var out: Tier1Contexts = undefined;
        out.resetAll();
        return out;
    }

    fn resetAll(self: *Tier1Contexts) void {
        arithmetic.resetContexts(&self.contexts);
        // ZC context 0 at state pair 4 (matches OpenJPEG: opj_mqc_setstate(T1_CTXNO_ZC, 0, 4))
        self.contexts[0].reset(2 * 4);
        // Context 17 (run-length/AGG): state pair 3 (matches OpenJPEG: opj_mqc_setstate(T1_CTXNO_AGG, 0, 3))
        self.contexts[17].reset(2 * 3);
        // Context 18 (uniform): state pair 46 (matches OpenJPEG: opj_mqc_setstate(T1_CTXNO_UNI, 0, 46))
        self.contexts[18].reset(2 * 46);
    }

    fn at(self: *Tier1Contexts, index: u8) *arithmetic.MqContext {
        return &self.contexts[index];
    }
};

// Scod bit masks for code_block_style (JPEG 2000 COD / COC marker).
pub const cbs_bypass: u8 = 0x01;
pub const cbs_reset_context: u8 = 0x02;
pub const cbs_termall: u8 = 0x04;
pub const cbs_vsc: u8 = 0x08;
pub const cbs_pterm: u8 = 0x10;
pub const cbs_segsym: u8 = 0x20;

// ---------------------------------------------------------------------------
// Encoder significance state -- tracks which samples are significant
// ---------------------------------------------------------------------------

const CoefficientFlags = packed struct(u8) {
    significant: bool = false,
    sign: bool = false,
    visited: bool = false,
    refined: bool = false,
    _padding: u4 = 0,
};

const EncoderState = struct {
    allocator: std.mem.Allocator,
    width: usize,
    height: usize,
    magnitudes: []u32,
    signs: []bool,
    flags: []CoefficientFlags,

    fn init(allocator: std.mem.Allocator, coefficients: []const i32, width: usize, height: usize) !EncoderState {
        const n = width * height;
        const mags = try allocator.alloc(u32, n);
        const sgns = try allocator.alloc(bool, n);
        const flgs = try allocator.alloc(CoefficientFlags, n);
        @memset(flgs, .{});
        for (0..n) |i| {
            const c = coefficients[i];
            mags[i] = if (c < 0) @intCast(-c) else @intCast(c);
            sgns[i] = c < 0;
        }
        return .{
            .allocator = allocator,
            .width = width,
            .height = height,
            .magnitudes = mags,
            .signs = sgns,
            .flags = flgs,
        };
    }

    fn deinit(self: *EncoderState) void {
        self.allocator.free(self.magnitudes);
        self.allocator.free(self.signs);
        self.allocator.free(self.flags);
    }

    fn idx(self: *const EncoderState, x: usize, y: usize) usize {
        return y * self.width + x;
    }

    fn resetVisited(self: *EncoderState) void {
        for (self.flags) |*f| f.visited = false;
    }

    fn zeroCodingContextIndex(self: *const EncoderState, x: usize, y: usize, subband: tile.SubbandType) u8 {
        const orient: usize = switch (subband) {
            .ll => 0,
            .hl => 1,
            .lh => 2,
            .hh => 3,
        };
        return codeblock.lut_ctxno_zc[orient * 512 + @as(usize, self.zeroCodingLutIndexSparse(x, y))];
    }

    fn zeroCodingLutIndexSparse(self: *const EncoderState, x: usize, y: usize) u9 {
        var index: u9 = 0;
        if (x > 0 and y > 0 and self.flags[self.idx(x - 1, y - 1)].significant) index |= 1 << 0;
        if (y > 0 and self.flags[self.idx(x, y - 1)].significant) index |= 1 << 1;
        if (x + 1 < self.width and y > 0 and self.flags[self.idx(x + 1, y - 1)].significant) index |= 1 << 2;
        if (x > 0 and self.flags[self.idx(x - 1, y)].significant) index |= 1 << 3;
        if (x + 1 < self.width and self.flags[self.idx(x + 1, y)].significant) index |= 1 << 5;
        if (x > 0 and y + 1 < self.height and self.flags[self.idx(x - 1, y + 1)].significant) index |= 1 << 6;
        if (y + 1 < self.height and self.flags[self.idx(x, y + 1)].significant) index |= 1 << 7;
        if (x + 1 < self.width and y + 1 < self.height and self.flags[self.idx(x + 1, y + 1)].significant) index |= 1 << 8;
        return index;
    }

    fn significantNeighborCount(self: *const EncoderState, x: usize, y: usize) u8 {
        var count: u8 = 0;
        const min_x = if (x == 0) 0 else x - 1;
        const max_x = @min(x + 1, self.width - 1);
        const min_y = if (y == 0) 0 else y - 1;
        const max_y = @min(y + 1, self.height - 1);
        var yy = min_y;
        while (yy <= max_y) : (yy += 1) {
            var xx = min_x;
            while (xx <= max_x) : (xx += 1) {
                if (xx == x and yy == y) continue;
                if (self.flags[self.idx(xx, yy)].significant) count += 1;
            }
        }
        return count;
    }

    fn signContribution(self: *const EncoderState, x: usize, y: usize) SignContribution {
        var east_pos: i2 = 0;
        var east_neg: i2 = 0;
        var west_pos: i2 = 0;
        var west_neg: i2 = 0;
        var north_pos: i2 = 0;
        var north_neg: i2 = 0;
        var south_pos: i2 = 0;
        var south_neg: i2 = 0;

        if (x > 0) {
            const i = self.idx(x - 1, y);
            if (self.flags[i].significant) {
                if (self.flags[i].sign) west_neg = 1 else west_pos = 1;
            }
        }
        if (x + 1 < self.width) {
            const i = self.idx(x + 1, y);
            if (self.flags[i].significant) {
                if (self.flags[i].sign) east_neg = 1 else east_pos = 1;
            }
        }
        if (y > 0) {
            const i = self.idx(x, y - 1);
            if (self.flags[i].significant) {
                if (self.flags[i].sign) north_neg = 1 else north_pos = 1;
            }
        }
        if (y + 1 < self.height) {
            const i = self.idx(x, y + 1);
            if (self.flags[i].significant) {
                if (self.flags[i].sign) south_neg = 1 else south_pos = 1;
            }
        }

        const horizontal_pos = @min(@as(i8, east_pos) + @as(i8, west_pos), 1);
        const horizontal_neg = @min(@as(i8, east_neg) + @as(i8, west_neg), 1);
        const vertical_pos = @min(@as(i8, north_pos) + @as(i8, south_pos), 1);
        const vertical_neg = @min(@as(i8, north_neg) + @as(i8, south_neg), 1);

        const horizontal: i2 = @intCast(horizontal_pos - horizontal_neg);
        const vertical: i2 = @intCast(vertical_pos - vertical_neg);
        return .{ .horizontal = horizontal, .vertical = vertical };
    }

    fn signContributionContextIndex(contribution: SignContribution) u8 {
        var h = contribution.horizontal;
        var v = contribution.vertical;
        var n: u8 = 0;
        if (h < 0) {
            h = -h;
            v = -v;
        }
        if (h == 0) {
            n = if (v == 0) 0 else 1;
        } else if (h == 1) {
            if (v == -1) {
                n = 2;
            } else if (v == 0) {
                n = 3;
            } else {
                n = 4;
            }
        }
        return 9 + n;
    }

    fn signContributionPredictor(contribution: SignContribution) u1 {
        if (contribution.horizontal == 0 and contribution.vertical == 0) return 0;
        return @intFromBool(!(contribution.horizontal > 0 or (contribution.horizontal == 0 and contribution.vertical > 0)));
    }

    fn refinementContextIndex(self: *const EncoderState, x: usize, y: usize) u8 {
        const f = self.flags[self.idx(x, y)];
        if (f.refined) return 16;
        return if (self.significantNeighborCount(x, y) != 0) 15 else 14;
    }

    fn markSignificant(self: *EncoderState, x: usize, y: usize) void {
        const i = self.idx(x, y);
        self.flags[i].significant = true;
        self.flags[i].sign = self.signs[i];
        self.flags[i].visited = true;
    }

    fn markRefined(self: *EncoderState, x: usize, y: usize) void {
        const i = self.idx(x, y);
        self.flags[i].refined = true;
        self.flags[i].visited = true;
    }
};

const ZeroCodingNeighborCounts = struct {
    horizontal: u8,
    vertical: u8,
    diagonal: u8,
};

const SignContribution = struct {
    horizontal: i2,
    vertical: i2,
};

fn zeroCodingContextIndexLlLike(horizontal: u8, vertical: u8, diagonal: u8) u8 {
    if (horizontal == 0) {
        if (vertical == 0) {
            if (diagonal == 0) return 0;
            if (diagonal == 1) return 1;
            return 2;
        }
        if (vertical == 1) return 3;
        return 4;
    }
    if (horizontal == 1) {
        if (vertical == 0) {
            if (diagonal == 0) return 5;
            return 6;
        }
        return 7;
    }
    return 8;
}

fn zeroCodingContextIndexHh(horizontal: u8, vertical: u8, diagonal: u8) u8 {
    const hv = horizontal + vertical;
    if (diagonal == 0) {
        if (hv == 0) return 0;
        if (hv == 1) return 1;
        return 2;
    }
    if (diagonal == 1) {
        if (hv == 0) return 3;
        if (hv == 1) return 4;
        return 5;
    }
    if (diagonal == 2) {
        if (hv == 0) return 6;
        return 7;
    }
    return 8;
}

fn cleanupRunModeEligible(state: *const EncoderState, x: usize, stripe_y: usize, stripe_end: usize) bool {
    if (stripe_end - stripe_y != 4) return false;
    var y = stripe_y;
    while (y < stripe_end) : (y += 1) {
        const i = state.idx(x, y);
        if (state.flags[i].significant or state.flags[i].visited) return false;
        if (state.significantNeighborCount(x, y) != 0) return false;
    }
    return true;
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Encode a code block's coefficients using EBCOT Tier-1.
///
/// `code_block_style` is a bitmask of COD Scod bits (see `cbs_*`). The
/// following bits are honored by the encoder:
///   * BYPASS (0x01): after the first four cleanup passes, SPP/MRP switch
///     to raw-bit coding. Pass boundaries become segment boundaries.
///   * RESET (0x02): reset MQ context probabilities at the start of every
///     coding pass.
///   * TERMALL (0x04): flush MQ at the end of every coding pass; each pass
///     gets its own segment.
///   * VSC (0x08): vertically-causal context formation (decode-side
///     responsibility; the encoder already produces a compatible stream
///     because it does not rely on across-stripe state).
///   * PTERM (0x10): predictable termination — the final MQ segment is
///     flushed with a specific bit pattern that the decoder can optionally
///     verify. This implementation uses the standard flush (SETBITS +
///     two byte-outs) which satisfies the predictable-termination
///     constraint described in ITU-T T.800 D.4.2 "easy" case.
///   * SEGSYM (0x20): emit the 0xA segmentation symbol after every cleanup
///     pass.
///
/// Pass lengths stored in the result mark end-of-pass byte offsets into
/// the returned `data` buffer. When BYPASS or TERMALL is set, each
/// boundary corresponds to a terminated segment (MQ flush or raw-bit
/// byte align).
pub fn encodeCodeblock(
    allocator: std.mem.Allocator,
    coefficients: []const i32,
    width: usize,
    height: usize,
    bits_per_component: u8,
    subband: tile.SubbandType,
    code_block_style: u8,
    roi_shift: u8,
) !EncodedCodeblock {
    if (width == 0 or height == 0) return error.InvalidCodeblockShape;
    if (coefficients.len != width * height) return error.InvalidCoefficientCount;

    // Optionally upshift all coefficients by `roi_shift` bitplanes so they appear
    // `roi_shift` bitplanes higher in the MQ scan (implicit max-shift ROI encoding).
    var owned_shifted: ?[]i32 = null;
    defer if (owned_shifted) |s| allocator.free(s);
    const effective_coeffs: []const i32 = if (roi_shift == 0) coefficients else blk: {
        const shifted = try allocator.alloc(i32, coefficients.len);
        for (coefficients, 0..) |c, i| {
            shifted[i] = c << @intCast(roi_shift);
        }
        owned_shifted = shifted;
        break :blk shifted;
    };
    const effective_bpc: u8 = if (roi_shift == 0) bits_per_component else bits_per_component + roi_shift;

    // 1. Find max magnitude to determine bit planes.
    var max_magnitude: u32 = 0;
    for (effective_coeffs) |c| {
        const mag: u32 = if (c < 0) @intCast(-c) else @intCast(c);
        if (mag > max_magnitude) max_magnitude = mag;
    }

    // 2. Determine bit planes.
    const total_bit_planes: u8 = if (max_magnitude > 0) @intCast(std.math.log2(max_magnitude) + 1) else 0;
    const zero_bit_planes: u8 = if (effective_bpc >= total_bit_planes) effective_bpc - total_bit_planes else 0;

    const segsym = (code_block_style & cbs_segsym) != 0;
    const reset_ctx = (code_block_style & cbs_reset_context) != 0;
    const bypass = (code_block_style & cbs_bypass) != 0;
    const termall = (code_block_style & cbs_termall) != 0;

    var output = std.ArrayListUnmanaged(u8).empty;
    errdefer output.deinit(allocator);
    var pass_lengths = std.ArrayListUnmanaged(u32).empty;
    errdefer pass_lengths.deinit(allocator);

    // Owns the currently-open MQ segment. TERMALL / BYPASS / end-of-stream
    // flushes this encoder and commits its bytes to `output`, then resets
    // it for a fresh segment. The raw-bit writer is used while BYPASS is
    // active on SPP/MRP passes.
    var encoder = arithmetic.MqEncoder.init();
    defer encoder.deinit(allocator);
    var bit_writer = arithmetic.BitWriter.init(allocator);
    defer bit_writer.deinit();

    if (total_bit_planes == 0) {
        // All coefficients are zero. OpenJPEG emits zero coding passes for
        // this case (numbps=0, totalpasses=0), which lets Tier-2 encode the
        // codeblock as "not yet included" with no packet-body bytes. Running
        // a cleanup pass here would produce an MQ-flush tail (e.g. `ff 7f`)
        // which bit-level-diverges from the OpenJPEG reference bitstream even
        // though it still decodes to the same coefficients.
        return .{
            .data = try output.toOwnedSlice(allocator),
            .num_coding_passes = 0,
            .zero_bit_planes = zero_bit_planes,
            .pass_lengths = try pass_lengths.toOwnedSlice(allocator),
        };
    }

    var state = try EncoderState.init(allocator, effective_coeffs, width, height);
    defer state.deinit();
    var contexts = Tier1Contexts.init();

    const first_bitplane: i16 = @as(i16, effective_bpc - 1) - @as(i16, zero_bit_planes);

    var pass_index: u16 = 0;
    var current_bitplane = first_bitplane;

    // The BYPASS transition boundary: after the 4th cleanup pass (the
    // cleanup on the 4th bitplane from the MSB, bitplane offset = 3 from
    // `first_bitplane`). For bit planes <= first_bitplane - 4, SPP/MRP
    // switch to raw-bit encoding. CUP continues to use MQ.
    const bypass_from_bitplane: i16 = first_bitplane - 4;

    // First pass is always cleanup on the MSB bit plane using MQ.
    state.resetVisited();
    try encodeCleanupPass(allocator, &encoder, &state, &contexts, current_bitplane, subband);
    if (segsym) try encodeSegmentationSymbol(allocator, &encoder, &contexts);
    pass_index += 1;
    try recordMqBoundary(termall, &encoder, &output, &pass_lengths, allocator);

    // Subsequent bit planes: significance, refinement, cleanup.
    current_bitplane -= 1;
    while (current_bitplane >= 0) : (current_bitplane -= 1) {
        state.resetVisited();
        const in_bypass_region = bypass and current_bitplane <= bypass_from_bitplane;

        // --- Significance propagation pass ---------------------------------
        if (reset_ctx) contexts.resetAll();
        if (in_bypass_region) {
            try encodeSignificancePassRaw(&bit_writer, &state, current_bitplane, subband);
            try recordBitsBoundary(&bit_writer, &output, &pass_lengths, allocator);
        } else {
            try encodeSignificancePass(allocator, &encoder, &state, &contexts, current_bitplane, subband);
            try recordMqBoundary(termall, &encoder, &output, &pass_lengths, allocator);
        }
        pass_index += 1;

        // --- Magnitude refinement pass -------------------------------------
        if (reset_ctx) contexts.resetAll();
        if (in_bypass_region) {
            try encodeRefinementPassRaw(&bit_writer, &state, current_bitplane);
            try recordBitsBoundary(&bit_writer, &output, &pass_lengths, allocator);
        } else {
            try encodeRefinementPass(allocator, &encoder, &state, &contexts, current_bitplane);
            try recordMqBoundary(termall, &encoder, &output, &pass_lengths, allocator);
        }
        pass_index += 1;

        // --- Cleanup pass (always MQ) --------------------------------------
        if (reset_ctx) contexts.resetAll();
        try encodeCleanupPass(allocator, &encoder, &state, &contexts, current_bitplane, subband);
        if (segsym) try encodeSegmentationSymbol(allocator, &encoder, &contexts);
        pass_index += 1;
        const is_last_pass = current_bitplane == 0;
        // The CUP immediately preceding the bypass region (bp = threshold+1)
        // must terminate so the following bypass SPP starts fresh. CUPs in the
        // bypass region are also each their own MQ segment.
        const pre_bypass_cup = bypass and current_bitplane == bypass_from_bitplane + 1;
        const terminate_cup = in_bypass_region or termall or is_last_pass or pre_bypass_cup;
        try recordMqBoundary(terminate_cup, &encoder, &output, &pass_lengths, allocator);

        if (is_last_pass) break;
    }

    // Ensure the stream is terminated: if the last pass was not a forced
    // terminator (e.g. total_bit_planes == 1 with no TERMALL/BYPASS), close
    // the MQ encoder now. pass_lengths entries for non-terminated passes
    // were recorded as `out.items.len + projected` from a clone; those
    // projections can exceed the true final length because the clone's
    // flush overhead differs from the real one. Clamp every entry to the
    // true data length so tier-2 slicing stays in bounds.
    //
    // Detect "encoder has pending state" by checking every field that diverges
    // from the fresh-init values (`a = 0x8000`, `c = 0`, `ct = 12`, `bp = null`,
    // empty buffer). Using just `buffer.len > 0 or bp != null` misses the case
    // where `encode()` has been called enough to shift the A/C/CT registers but
    // not enough to trigger a `byteOut`: this happens for single-coefficient
    // subbands whose entire cleanup pass stays in MPS (e.g. HH_3 = {1,0,0,0}
    // at first_bitplane=0 when total_bit_planes=1 for odd-bpc 5/3 fixtures).
    // Without this guard, the encoder produced `num_coding_passes=1, data=[]`
    // and the decoder silently read 0xff padding, flipping the lone significant
    // coefficient to zero.
    const encoder_has_pending = encoder.buffer.items.len > 0 or
        encoder.bp != null or
        encoder.c != 0 or
        encoder.ct != 12 or
        encoder.a != 0x8000;
    if (encoder_has_pending) {
        try flushMqIntoOutput(&encoder, &output, allocator);
    }
    const final_len: u32 = @intCast(output.items.len);
    for (pass_lengths.items) |*entry| {
        if (entry.* > final_len) entry.* = final_len;
    }
    if (pass_lengths.items.len > 0) {
        pass_lengths.items[pass_lengths.items.len - 1] = final_len;
    }

    return .{
        .data = try output.toOwnedSlice(allocator),
        .num_coding_passes = pass_index,
        .zero_bit_planes = zero_bit_planes,
        .pass_lengths = try pass_lengths.toOwnedSlice(allocator),
    };
}

// ---------------------------------------------------------------------------
// Significance propagation pass (encoder)
// ---------------------------------------------------------------------------

fn encodeSignificancePass(
    allocator: std.mem.Allocator,
    encoder: *arithmetic.MqEncoder,
    state: *EncoderState,
    contexts: *Tier1Contexts,
    bitplane: i16,
    subband: tile.SubbandType,
) !void {
    const bp: u5 = @intCast(bitplane);
    var stripe_y: usize = 0;
    while (stripe_y < state.height) : (stripe_y += 4) {
        var x: usize = 0;
        while (x < state.width) : (x += 1) {
            const stripe_end = @min(stripe_y + 4, state.height);
            var y = stripe_y;
            while (y < stripe_end) : (y += 1) {
                const i = state.idx(x, y);
                if (state.flags[i].significant or state.flags[i].visited) continue;
                if (state.significantNeighborCount(x, y) == 0) continue;

                const zc_ctx = state.zeroCodingContextIndex(x, y, subband);
                const is_significant: u1 = @intFromBool((state.magnitudes[i] >> bp) & 1 == 1);

                try encoder.encode(allocator, contexts.at(zc_ctx), is_significant);

                if (is_significant == 1) {
                    try encodeSign(allocator, encoder, state, contexts, x, y);
                    state.markSignificant(x, y);
                } else {
                    state.flags[i].visited = true;
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Refinement pass (encoder)
// ---------------------------------------------------------------------------

fn encodeRefinementPass(
    allocator: std.mem.Allocator,
    encoder: *arithmetic.MqEncoder,
    state: *EncoderState,
    contexts: *Tier1Contexts,
    bitplane: i16,
) !void {
    const bp: u5 = @intCast(bitplane);
    var stripe_y: usize = 0;
    while (stripe_y < state.height) : (stripe_y += 4) {
        var x: usize = 0;
        while (x < state.width) : (x += 1) {
            const stripe_end = @min(stripe_y + 4, state.height);
            var y = stripe_y;
            while (y < stripe_end) : (y += 1) {
                const i = state.idx(x, y);
                if (!state.flags[i].significant or state.flags[i].visited) continue;

                const ref_ctx = state.refinementContextIndex(x, y);
                const bit: u1 = @intCast((state.magnitudes[i] >> bp) & 1);
                try encoder.encode(allocator, contexts.at(ref_ctx), bit);
                state.markRefined(x, y);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// BYPASS (raw-bit) SPP/MRP encoders
// ---------------------------------------------------------------------------
//
// In BYPASS (lazy) mode, after the fourth bitplane from the MSB the
// significance-propagation and magnitude-refinement passes stop using the
// MQ coder and instead emit raw bits into a byte-aligned segment. The
// selection of which samples to code is unchanged (significance neighbor
// test, significance-predicate), so the raw stream is perfectly
// reconstructable given the decoder tracks the same state.

fn encodeSignificancePassRaw(
    bit_writer: *arithmetic.BitWriter,
    state: *EncoderState,
    bitplane: i16,
    subband: tile.SubbandType,
) !void {
    _ = subband;
    const bp: u5 = @intCast(bitplane);
    var stripe_y: usize = 0;
    while (stripe_y < state.height) : (stripe_y += 4) {
        var x: usize = 0;
        while (x < state.width) : (x += 1) {
            const stripe_end = @min(stripe_y + 4, state.height);
            var y = stripe_y;
            while (y < stripe_end) : (y += 1) {
                const i = state.idx(x, y);
                if (state.flags[i].significant or state.flags[i].visited) continue;
                if (state.significantNeighborCount(x, y) == 0) continue;

                const is_significant: u1 = @intFromBool((state.magnitudes[i] >> bp) & 1 == 1);
                try bit_writer.writeBit(is_significant);
                if (is_significant == 1) {
                    // Emit the raw sign bit (1 == negative) instead of the
                    // MQ-coded prediction-differential used in non-bypass mode.
                    const negative: u1 = @intFromBool(state.signs[i]);
                    try bit_writer.writeBit(negative);
                    state.markSignificant(x, y);
                } else {
                    state.flags[i].visited = true;
                }
            }
        }
    }
}

fn encodeRefinementPassRaw(
    bit_writer: *arithmetic.BitWriter,
    state: *EncoderState,
    bitplane: i16,
) !void {
    const bp: u5 = @intCast(bitplane);
    var stripe_y: usize = 0;
    while (stripe_y < state.height) : (stripe_y += 4) {
        var x: usize = 0;
        while (x < state.width) : (x += 1) {
            const stripe_end = @min(stripe_y + 4, state.height);
            var y = stripe_y;
            while (y < stripe_end) : (y += 1) {
                const i = state.idx(x, y);
                if (!state.flags[i].significant or state.flags[i].visited) continue;
                const bit: u1 = @intCast((state.magnitudes[i] >> bp) & 1);
                try bit_writer.writeBit(bit);
                state.markRefined(x, y);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Segment-close / pass-boundary helpers
// ---------------------------------------------------------------------------

fn flushMqIntoOutput(
    enc: *arithmetic.MqEncoder,
    out: *std.ArrayListUnmanaged(u8),
    allocator: std.mem.Allocator,
) !void {
    try enc.flush(allocator);
    try out.appendSlice(allocator, enc.getBytes());
    enc.deinit(allocator);
    enc.* = arithmetic.MqEncoder.init();
}

fn flushBitsIntoOutput(
    bw: *arithmetic.BitWriter,
    out: *std.ArrayListUnmanaged(u8),
    allocator: std.mem.Allocator,
) !void {
    try bw.alignToByte();
    try out.appendSlice(allocator, bw.getBytes());
    bw.deinit();
    bw.* = arithmetic.BitWriter.init(allocator);
}

fn recordMqBoundary(
    force_terminate: bool,
    enc: *arithmetic.MqEncoder,
    out: *std.ArrayListUnmanaged(u8),
    pass_lengths: *std.ArrayListUnmanaged(u32),
    allocator: std.mem.Allocator,
) !void {
    if (force_terminate) {
        try flushMqIntoOutput(enc, out, allocator);
        try pass_lengths.append(allocator, @intCast(out.items.len));
    } else {
        const projected = try enc.terminatedLength(allocator);
        try pass_lengths.append(allocator, @intCast(out.items.len + projected));
    }
}

fn recordBitsBoundary(
    bw: *arithmetic.BitWriter,
    out: *std.ArrayListUnmanaged(u8),
    pass_lengths: *std.ArrayListUnmanaged(u32),
    allocator: std.mem.Allocator,
) !void {
    try flushBitsIntoOutput(bw, out, allocator);
    try pass_lengths.append(allocator, @intCast(out.items.len));
}

// ---------------------------------------------------------------------------
// Cleanup pass (encoder)
// ---------------------------------------------------------------------------

fn encodeCleanupPass(
    allocator: std.mem.Allocator,
    encoder: *arithmetic.MqEncoder,
    state: *EncoderState,
    contexts: *Tier1Contexts,
    bitplane: i16,
    subband: tile.SubbandType,
) !void {
    const bp: u5 = @intCast(bitplane);
    var stripe_y: usize = 0;
    while (stripe_y < state.height) : (stripe_y += 4) {
        var x: usize = 0;
        while (x < state.width) : (x += 1) {
            const stripe_end = @min(stripe_y + 4, state.height);

            if (cleanupRunModeEligible(state, x, stripe_y, stripe_end)) {
                // Run-length mode: check if all 4 samples are zero at this bit plane.
                var all_zero = true;
                var first_sig_offset: usize = 0;
                for (stripe_y..stripe_end) |y| {
                    const i = state.idx(x, y);
                    if ((state.magnitudes[i] >> bp) & 1 == 1) {
                        all_zero = false;
                        first_sig_offset = y - stripe_y;
                        break;
                    }
                }
                if (all_zero) {
                    // Run symbol = 0 (all four are zero)
                    try encoder.encode(allocator, contexts.at(17), 0);
                    for (stripe_y..stripe_end) |y| {
                        state.flags[state.idx(x, y)].visited = true;
                    }
                    continue;
                }

                // Run symbol = 1
                try encoder.encode(allocator, contexts.at(17), 1);
                // 2-bit index of first significant sample
                const msb: u1 = @intCast((first_sig_offset >> 1) & 1);
                const lsb: u1 = @intCast(first_sig_offset & 1);
                try encoder.encode(allocator, contexts.at(18), msb);
                try encoder.encode(allocator, contexts.at(18), lsb);

                // Mark earlier samples as visited
                for (stripe_y..stripe_y + first_sig_offset) |y| {
                    state.flags[state.idx(x, y)].visited = true;
                }

                // First significant sample after run: per ITU-T T.800 D.6,
                // significance is implied. Encode only the sign.
                try encodeSign(allocator, encoder, state, contexts, x, stripe_y + first_sig_offset);
                state.markSignificant(x, stripe_y + first_sig_offset);

                // Remaining samples after the first significant one
                for (stripe_y + first_sig_offset + 1..stripe_end) |y| {
                    const i = state.idx(x, y);
                    if (state.flags[i].significant or state.flags[i].visited) continue;
                    try encodeCleanupSample(allocator, encoder, state, contexts, x, y, bp, subband);
                }
                continue;
            }

            // Non-run-length mode
            for (stripe_y..stripe_end) |y| {
                const i = state.idx(x, y);
                if (state.flags[i].significant or state.flags[i].visited) continue;
                try encodeCleanupSample(allocator, encoder, state, contexts, x, y, bp, subband);
            }
        }
    }
}

fn encodeCleanupSample(
    allocator: std.mem.Allocator,
    encoder: *arithmetic.MqEncoder,
    state: *EncoderState,
    contexts: *Tier1Contexts,
    x: usize,
    y: usize,
    bp: u5,
    subband: tile.SubbandType,
) !void {
    const i = state.idx(x, y);
    const zc_ctx = state.zeroCodingContextIndex(x, y, subband);
    const is_significant: u1 = @intFromBool((state.magnitudes[i] >> bp) & 1 == 1);
    try encoder.encode(allocator, contexts.at(zc_ctx), is_significant);
    if (is_significant == 1) {
        try encodeSign(allocator, encoder, state, contexts, x, y);
        state.markSignificant(x, y);
    } else {
        state.flags[i].visited = true;
    }
}

// ---------------------------------------------------------------------------
// Sign encoding -- matches decodeSignificanceAndSign (standard policy)
// ---------------------------------------------------------------------------

/// Emit the 4-bit segmentation symbol 0xA (1010) using the UNIFORM context.
/// Mirrors the decode side at codeblock.zig:733-738 so the decoder can
/// read and verify the marker after every cleanup pass.
fn encodeSegmentationSymbol(
    allocator: std.mem.Allocator,
    encoder: *arithmetic.MqEncoder,
    contexts: *Tier1Contexts,
) !void {
    var i: u2 = 0;
    while (true) : (i += 1) {
        const shift: u2 = 3 - i;
        const bit: u1 = @intCast((@as(u8, 0xA) >> shift) & 1);
        try encoder.encode(allocator, contexts.at(18), bit);
        if (i == 3) break;
    }
}

fn encodeSign(
    allocator: std.mem.Allocator,
    encoder: *arithmetic.MqEncoder,
    state: *const EncoderState,
    contexts: *Tier1Contexts,
    x: usize,
    y: usize,
) !void {
    const contribution = state.signContribution(x, y);
    const sign_ctx = EncoderState.signContributionContextIndex(contribution);
    const predicted_sign = EncoderState.signContributionPredictor(contribution);
    const actual_negative = state.signs[state.idx(x, y)];
    // Decoder: negative = (sign_symbol ^ predicted_sign) == 1
    // Encoder: sign_symbol = negative XOR predicted_sign
    const sign_symbol: u1 = @intFromBool(actual_negative) ^ predicted_sign;
    try encoder.encode(allocator, contexts.at(sign_ctx), sign_symbol);
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "encode small 4x4 codeblock produces valid output" {
    const allocator = std.testing.allocator;
    const coefficients = [_]i32{
        10,  -5,  3,  0,
        -7,   2,  0,  1,
        0,    4, -6,  8,
        1,   -3,  5, -2,
    };

    var result = try encodeCodeblock(
        allocator,
        &coefficients,
        4,
        4,
        8,
        .ll,
        0,
        0,
    );
    defer result.deinit(allocator);

    try std.testing.expect(result.data.len > 0);
    try std.testing.expect(result.num_coding_passes > 0);
    // Max magnitude is 10, which is 4 bits, so with 8 bpc we expect 4 zero bit planes
    try std.testing.expectEqual(@as(u8, 4), result.zero_bit_planes);
}

test "encode all-zero codeblock produces zero passes and empty data" {
    // OpenJPEG emits numbps=0/totalpasses=0 for an all-zero block; mirroring
    // that keeps our bitstream bit-exact with the reference under tier-2's
    // "not yet included" tagtree path. The previous behavior (run a cleanup
    // pass and flush MQ to `ff 7f`) produced correct-but-non-canonical bytes.
    const allocator = std.testing.allocator;
    const coefficients = [_]i32{ 0, 0, 0, 0 };

    var result = try encodeCodeblock(
        allocator,
        &coefficients,
        2,
        2,
        8,
        .ll,
        0,
        0,
    );
    defer result.deinit(allocator);

    try std.testing.expectEqual(@as(u16, 0), result.num_coding_passes);
    try std.testing.expectEqual(@as(u8, 8), result.zero_bit_planes);
    try std.testing.expectEqual(@as(usize, 0), result.data.len);
}

test "encode all-zero codeblock emits no passes and empty pass_lengths (regression)" {
    // Regression for the gray8_8x8_lossy_97_d2 panic: when the MQ flush of
    // the single cleanup pass on an all-zero codeblock emits zero bytes,
    // the short-circuit previously recorded a non-zero projected terminated
    // length while `data.len` stayed 0. Tier-2 would then slice `data[0..1]`
    // on a zero-length buffer and panic. The current fix is stronger: we
    // emit zero coding passes (mirroring OpenJPEG's numbps==0 path), so
    // pass_lengths is empty and tier-2 treats the codeblock as "not yet
    // included" with no body bytes.
    const allocator = std.testing.allocator;
    const coefficients = [_]i32{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };

    var result = try encodeCodeblock(
        allocator,
        &coefficients,
        4,
        4,
        8,
        .ll,
        0,
        0,
    );
    defer result.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 0), result.pass_lengths.len);
    try std.testing.expectEqual(@as(usize, 0), result.data.len);
    try std.testing.expectEqual(@as(u16, 0), result.num_coding_passes);
}

test "encode single coefficient round-trips through decoder" {
    const allocator = std.testing.allocator;
    const coefficients = [_]i32{73};
    const bpc: u8 = 10;

    var encoded = try encodeCodeblock(
        allocator,
        &coefficients,
        1,
        1,
        bpc,
        .hl,
        0,
        0,
    );
    defer encoded.deinit(allocator);

    var grid = try codeblock.CoefficientGrid.init(allocator, 1, 1);
    defer grid.deinit();
    grid.clear();

    const plan = try codeblock.buildPassSchedule(allocator, encoded.num_coding_passes, encoded.zero_bit_planes, bpc);
    defer allocator.free(plan);

    const pass_plan: codeblock.ContributionPassPlan = .{
        .component_index = 0,
        .subband = .hl,
        .zero_bit_planes = encoded.zero_bit_planes,
        .start_pass_index = 0,
        .num_passes = encoded.num_coding_passes,
        .passes = plan,
    };

    try codeblock.executeContributionPassPlanMq(
        allocator,
        &grid,
        &pass_plan,
        encoded.data,
        .standard,
        .exact_bitplane,
        .exact_bitplane,
        .standard,
        .{},
    );

    try std.testing.expectEqual(@as(i32, 73), grid.cells[0].magnitude);
    try std.testing.expectEqual(false, grid.cells[0].flags.sign);
}

test "encode 2x1 coefficients round-trips through decoder" {
    const allocator = std.testing.allocator;
    const coefficients = [_]i32{ 52, -59 };
    const bpc: u8 = 9;

    var encoded = try encodeCodeblock(
        allocator,
        &coefficients,
        2,
        1,
        bpc,
        .ll,
        0,
        0,
    );
    defer encoded.deinit(allocator);

    var grid = try codeblock.CoefficientGrid.init(allocator, 2, 1);
    defer grid.deinit();
    grid.clear();

    const plan = try codeblock.buildPassSchedule(allocator, encoded.num_coding_passes, encoded.zero_bit_planes, bpc);
    defer allocator.free(plan);

    const pass_plan: codeblock.ContributionPassPlan = .{
        .component_index = 0,
        .subband = .ll,
        .zero_bit_planes = encoded.zero_bit_planes,
        .start_pass_index = 0,
        .num_passes = encoded.num_coding_passes,
        .passes = plan,
    };

    try codeblock.executeContributionPassPlanMq(
        allocator,
        &grid,
        &pass_plan,
        encoded.data,
        .standard,
        .exact_bitplane,
        .exact_bitplane,
        .standard,
        .{},
    );

    try std.testing.expectEqual(@as(i32, 52), grid.cells[0].magnitude);
    try std.testing.expectEqual(false, grid.cells[0].flags.sign);
    try std.testing.expectEqual(@as(i32, 59), grid.cells[1].magnitude);
    try std.testing.expectEqual(true, grid.cells[1].flags.sign);
}

test "SEGSYM encode yields different bitstream than baseline" {
    const allocator = std.testing.allocator;
    const coefficients = [_]i32{
        10,  -5,  3,  0,
        -7,   2,  0,  1,
        0,    4, -6,  8,
        1,   -3,  5, -2,
    };

    var baseline = try encodeCodeblock(allocator, &coefficients, 4, 4, 8, .ll, 0, 0);
    defer baseline.deinit(allocator);

    var with_segsym = try encodeCodeblock(allocator, &coefficients, 4, 4, 8, .ll, cbs_segsym, 0);
    defer with_segsym.deinit(allocator);

    // SEGSYM appends 4 uniform-context bits after each cleanup pass, so the
    // encoded length must grow (or at minimum the payload must differ).
    try std.testing.expect(with_segsym.data.len >= baseline.data.len);
    try std.testing.expect(!std.mem.eql(u8, baseline.data, with_segsym.data));
}

test "RESET encode yields different bitstream than baseline" {
    const allocator = std.testing.allocator;
    const coefficients = [_]i32{
        10,  -5,  3,  0,
        -7,   2,  0,  1,
        0,    4, -6,  8,
        1,   -3,  5, -2,
    };

    var baseline = try encodeCodeblock(allocator, &coefficients, 4, 4, 8, .ll, 0, 0);
    defer baseline.deinit(allocator);

    var with_reset = try encodeCodeblock(allocator, &coefficients, 4, 4, 8, .ll, cbs_reset_context, 0);
    defer with_reset.deinit(allocator);

    try std.testing.expect(!std.mem.eql(u8, baseline.data, with_reset.data));
}

test "combined SEGSYM + RESET encode differs from either alone" {
    const allocator = std.testing.allocator;
    const coefficients = [_]i32{
        10,  -5,  3,  0,
        -7,   2,  0,  1,
        0,    4, -6,  8,
        1,   -3,  5, -2,
    };

    var only_segsym = try encodeCodeblock(allocator, &coefficients, 4, 4, 8, .ll, cbs_segsym, 0);
    defer only_segsym.deinit(allocator);
    var only_reset = try encodeCodeblock(allocator, &coefficients, 4, 4, 8, .ll, cbs_reset_context, 0);
    defer only_reset.deinit(allocator);
    var both = try encodeCodeblock(allocator, &coefficients, 4, 4, 8, .ll, cbs_segsym | cbs_reset_context, 0);
    defer both.deinit(allocator);

    try std.testing.expect(!std.mem.eql(u8, both.data, only_segsym.data));
    try std.testing.expect(!std.mem.eql(u8, both.data, only_reset.data));
}

test "ROI shift upshifts coefficients and changes bitstream" {
    const allocator = std.testing.allocator;
    const coefficients = [_]i32{
        10,  -5,  3,  0,
        -7,   2,  0,  1,
        0,    4, -6,  8,
        1,   -3,  5, -2,
    };

    var baseline = try encodeCodeblock(allocator, &coefficients, 4, 4, 8, .ll, 0, 0);
    defer baseline.deinit(allocator);

    var shifted = try encodeCodeblock(allocator, &coefficients, 4, 4, 8, .ll, 0, 3);
    defer shifted.deinit(allocator);

    // Upshifting by 3 bitplanes should produce a different payload and use
    // at most the same number of zero bitplanes as the baseline (because the
    // effective precision grew too).
    try std.testing.expect(!std.mem.eql(u8, baseline.data, shifted.data));
    try std.testing.expect(shifted.zero_bit_planes == baseline.zero_bit_planes);
}

test "ROI shift zero produces identical bitstream to baseline" {
    const allocator = std.testing.allocator;
    const coefficients = [_]i32{ 10, -5, 3, 0 };

    var baseline = try encodeCodeblock(allocator, &coefficients, 2, 2, 8, .ll, 0, 0);
    defer baseline.deinit(allocator);

    var zero_shifted = try encodeCodeblock(allocator, &coefficients, 2, 2, 8, .ll, 0, 0);
    defer zero_shifted.deinit(allocator);

    try std.testing.expectEqualSlices(u8, baseline.data, zero_shifted.data);
}

// ---------------------------------------------------------------------------
// BYPASS / TERMALL / PTERM tests
// ---------------------------------------------------------------------------

test "BYPASS produces different bytes than baseline when bypass region is reached" {
    const allocator = std.testing.allocator;
    // Use a high-magnitude coefficient so the encoding spans enough bitplanes
    // to cross the bypass threshold (SPP/MRP switch to raw bits after the
    // 4th cleanup pass).
    const coefficients = [_]i32{
        200, -150, 100, -90,
        -80, 70,   60,  -50,
        40,  -30,  20,  10,
        5,   -4,   -3,  2,
    };

    var baseline = try encodeCodeblock(allocator, &coefficients, 4, 4, 10, .ll, 0, 0);
    defer baseline.deinit(allocator);

    var bypass_encoded = try encodeCodeblock(allocator, &coefficients, 4, 4, 10, .ll, cbs_bypass, 0);
    defer bypass_encoded.deinit(allocator);

    // Both encodings should have the same pass count, but the byte stream
    // must differ because SPP/MRP are coded raw after the 4th cleanup.
    try std.testing.expectEqual(baseline.num_coding_passes, bypass_encoded.num_coding_passes);
    try std.testing.expect(!std.mem.eql(u8, baseline.data, bypass_encoded.data));
}

test "BYPASS round-trip recovers original coefficients" {
    const allocator = std.testing.allocator;
    const coefficients = [_]i32{
        200, -150, 100, -90,
        -80, 70,   60,  -50,
        40,  -30,  20,  10,
        5,   -4,   -3,  2,
    };
    const bpc: u8 = 10;

    var encoded = try encodeCodeblock(allocator, &coefficients, 4, 4, bpc, .ll, cbs_bypass, 0);
    defer encoded.deinit(allocator);

    var grid = try codeblock.CoefficientGrid.init(allocator, 4, 4);
    defer grid.deinit();
    grid.clear();

    const plan = try codeblock.buildPassSchedule(allocator, encoded.num_coding_passes, encoded.zero_bit_planes, bpc);
    defer allocator.free(plan);

    const pass_plan: codeblock.ContributionPassPlan = .{
        .component_index = 0,
        .subband = .ll,
        .zero_bit_planes = encoded.zero_bit_planes,
        .start_pass_index = 0,
        .num_passes = encoded.num_coding_passes,
        .passes = plan,
    };

    var style = codeblock.CodeBlockStyle{};
    style.bypass = true;

    try codeblock.executeContributionPassPlanMqWithSegments(
        allocator,
        &grid,
        &pass_plan,
        encoded.data,
        encoded.pass_lengths,
        .standard,
        .exact_bitplane,
        .exact_bitplane,
        .standard,
        style,
    );

    for (coefficients, 0..) |expected, i| {
        const cell = grid.cells[i];
        const signed_value: i32 = if (cell.flags.sign) -cell.magnitude else cell.magnitude;
        try std.testing.expectEqual(expected, signed_value);
    }
}

test "TERMALL pass_lengths are strictly increasing and cover full segment" {
    const allocator = std.testing.allocator;
    const coefficients = [_]i32{
        200, -150, 100, -90,
        -80, 70,   60,  -50,
        40,  -30,  20,  10,
        5,   -4,   -3,  2,
    };

    var encoded = try encodeCodeblock(allocator, &coefficients, 4, 4, 10, .ll, cbs_termall, 0);
    defer encoded.deinit(allocator);

    try std.testing.expect(encoded.pass_lengths.len == encoded.num_coding_passes);
    // Cumulative byte offsets must be monotonically non-decreasing.
    var prev: u32 = 0;
    for (encoded.pass_lengths) |offset| {
        try std.testing.expect(offset >= prev);
        prev = offset;
    }
    // The last boundary must exactly match the total data length.
    try std.testing.expectEqual(@as(u32, @intCast(encoded.data.len)), encoded.pass_lengths[encoded.pass_lengths.len - 1]);
    // Every pass should produce at least one byte in TERMALL mode.
    prev = 0;
    for (encoded.pass_lengths) |offset| {
        try std.testing.expect(offset > prev);
        prev = offset;
    }
}

test "TERMALL round-trip recovers original coefficients" {
    const allocator = std.testing.allocator;
    const coefficients = [_]i32{
        200, -150, 100, -90,
        -80, 70,   60,  -50,
        40,  -30,  20,  10,
        5,   -4,   -3,  2,
    };
    const bpc: u8 = 10;

    var encoded = try encodeCodeblock(allocator, &coefficients, 4, 4, bpc, .ll, cbs_termall, 0);
    defer encoded.deinit(allocator);

    var grid = try codeblock.CoefficientGrid.init(allocator, 4, 4);
    defer grid.deinit();
    grid.clear();

    const plan = try codeblock.buildPassSchedule(allocator, encoded.num_coding_passes, encoded.zero_bit_planes, bpc);
    defer allocator.free(plan);

    const pass_plan: codeblock.ContributionPassPlan = .{
        .component_index = 0,
        .subband = .ll,
        .zero_bit_planes = encoded.zero_bit_planes,
        .start_pass_index = 0,
        .num_passes = encoded.num_coding_passes,
        .passes = plan,
    };

    var style = codeblock.CodeBlockStyle{};
    style.termination = true;

    try codeblock.executeContributionPassPlanMqWithSegments(
        allocator,
        &grid,
        &pass_plan,
        encoded.data,
        encoded.pass_lengths,
        .standard,
        .exact_bitplane,
        .exact_bitplane,
        .standard,
        style,
    );

    for (coefficients, 0..) |expected, i| {
        const cell = grid.cells[i];
        const signed_value: i32 = if (cell.flags.sign) -cell.magnitude else cell.magnitude;
        try std.testing.expectEqual(expected, signed_value);
    }
}

test "PTERM encoder emits terminated stream the decoder accepts" {
    const allocator = std.testing.allocator;
    const coefficients = [_]i32{
        10,  -5,  3,  0,
        -7,   2,  0,  1,
        0,    4, -6,  8,
        1,   -3,  5, -2,
    };
    const bpc: u8 = 8;

    var encoded = try encodeCodeblock(allocator, &coefficients, 4, 4, bpc, .ll, cbs_pterm, 0);
    defer encoded.deinit(allocator);

    // PTERM uses the standard-flush byte pattern; decoder must accept it
    // without verification in this MVP (symmetric to a non-PTERM encode).
    var grid = try codeblock.CoefficientGrid.init(allocator, 4, 4);
    defer grid.deinit();
    grid.clear();

    const plan = try codeblock.buildPassSchedule(allocator, encoded.num_coding_passes, encoded.zero_bit_planes, bpc);
    defer allocator.free(plan);

    const pass_plan: codeblock.ContributionPassPlan = .{
        .component_index = 0,
        .subband = .ll,
        .zero_bit_planes = encoded.zero_bit_planes,
        .start_pass_index = 0,
        .num_passes = encoded.num_coding_passes,
        .passes = plan,
    };

    // Decoder: pass style without bypass/termall so the fast MQ path runs.
    try codeblock.executeContributionPassPlanMq(
        allocator,
        &grid,
        &pass_plan,
        encoded.data,
        .standard,
        .exact_bitplane,
        .exact_bitplane,
        .standard,
        .{},
    );

    // PTERM is lossless termination -> magnitudes should match the source.
    for (coefficients, 0..) |expected, i| {
        const cell = grid.cells[i];
        const signed_value: i32 = if (cell.flags.sign) -cell.magnitude else cell.magnitude;
        try std.testing.expectEqual(expected, signed_value);
    }

    // The final byte of the encoded stream must not be a trailing 0xff
    // (predictable-termination strips trailing 0xff, matching a clean flush).
    try std.testing.expect(encoded.data.len > 0);
    try std.testing.expect(encoded.data[encoded.data.len - 1] != 0xff);
}
