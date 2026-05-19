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

pub const native_port_available = true;

const MqTransition = struct {
    qeval: u16,
    mps: u1,
    next_mps: u8,
    next_lps: u8,
};

const mqc_states = [_]MqTransition{
    .{ .qeval = 0x5601, .mps = 0, .next_mps = 2, .next_lps = 3 },
    .{ .qeval = 0x5601, .mps = 1, .next_mps = 3, .next_lps = 2 },
    .{ .qeval = 0x3401, .mps = 0, .next_mps = 4, .next_lps = 12 },
    .{ .qeval = 0x3401, .mps = 1, .next_mps = 5, .next_lps = 13 },
    .{ .qeval = 0x1801, .mps = 0, .next_mps = 6, .next_lps = 18 },
    .{ .qeval = 0x1801, .mps = 1, .next_mps = 7, .next_lps = 19 },
    .{ .qeval = 0x0ac1, .mps = 0, .next_mps = 8, .next_lps = 24 },
    .{ .qeval = 0x0ac1, .mps = 1, .next_mps = 9, .next_lps = 25 },
    .{ .qeval = 0x0521, .mps = 0, .next_mps = 10, .next_lps = 58 },
    .{ .qeval = 0x0521, .mps = 1, .next_mps = 11, .next_lps = 59 },
    .{ .qeval = 0x0221, .mps = 0, .next_mps = 76, .next_lps = 66 },
    .{ .qeval = 0x0221, .mps = 1, .next_mps = 77, .next_lps = 67 },
    .{ .qeval = 0x5601, .mps = 0, .next_mps = 14, .next_lps = 13 },
    .{ .qeval = 0x5601, .mps = 1, .next_mps = 15, .next_lps = 12 },
    .{ .qeval = 0x5401, .mps = 0, .next_mps = 16, .next_lps = 28 },
    .{ .qeval = 0x5401, .mps = 1, .next_mps = 17, .next_lps = 29 },
    .{ .qeval = 0x4801, .mps = 0, .next_mps = 18, .next_lps = 28 },
    .{ .qeval = 0x4801, .mps = 1, .next_mps = 19, .next_lps = 29 },
    .{ .qeval = 0x3801, .mps = 0, .next_mps = 20, .next_lps = 28 },
    .{ .qeval = 0x3801, .mps = 1, .next_mps = 21, .next_lps = 29 },
    .{ .qeval = 0x3001, .mps = 0, .next_mps = 22, .next_lps = 34 },
    .{ .qeval = 0x3001, .mps = 1, .next_mps = 23, .next_lps = 35 },
    .{ .qeval = 0x2401, .mps = 0, .next_mps = 24, .next_lps = 36 },
    .{ .qeval = 0x2401, .mps = 1, .next_mps = 25, .next_lps = 37 },
    .{ .qeval = 0x1c01, .mps = 0, .next_mps = 26, .next_lps = 40 },
    .{ .qeval = 0x1c01, .mps = 1, .next_mps = 27, .next_lps = 41 },
    .{ .qeval = 0x1601, .mps = 0, .next_mps = 58, .next_lps = 42 },
    .{ .qeval = 0x1601, .mps = 1, .next_mps = 59, .next_lps = 43 },
    .{ .qeval = 0x5601, .mps = 0, .next_mps = 30, .next_lps = 29 },
    .{ .qeval = 0x5601, .mps = 1, .next_mps = 31, .next_lps = 28 },
    .{ .qeval = 0x5401, .mps = 0, .next_mps = 32, .next_lps = 28 },
    .{ .qeval = 0x5401, .mps = 1, .next_mps = 33, .next_lps = 29 },
    .{ .qeval = 0x5101, .mps = 0, .next_mps = 34, .next_lps = 30 },
    .{ .qeval = 0x5101, .mps = 1, .next_mps = 35, .next_lps = 31 },
    .{ .qeval = 0x4801, .mps = 0, .next_mps = 36, .next_lps = 32 },
    .{ .qeval = 0x4801, .mps = 1, .next_mps = 37, .next_lps = 33 },
    .{ .qeval = 0x3801, .mps = 0, .next_mps = 38, .next_lps = 34 },
    .{ .qeval = 0x3801, .mps = 1, .next_mps = 39, .next_lps = 35 },
    .{ .qeval = 0x3401, .mps = 0, .next_mps = 40, .next_lps = 36 },
    .{ .qeval = 0x3401, .mps = 1, .next_mps = 41, .next_lps = 37 },
    .{ .qeval = 0x3001, .mps = 0, .next_mps = 42, .next_lps = 38 },
    .{ .qeval = 0x3001, .mps = 1, .next_mps = 43, .next_lps = 39 },
    .{ .qeval = 0x2801, .mps = 0, .next_mps = 44, .next_lps = 38 },
    .{ .qeval = 0x2801, .mps = 1, .next_mps = 45, .next_lps = 39 },
    .{ .qeval = 0x2401, .mps = 0, .next_mps = 46, .next_lps = 40 },
    .{ .qeval = 0x2401, .mps = 1, .next_mps = 47, .next_lps = 41 },
    .{ .qeval = 0x2201, .mps = 0, .next_mps = 48, .next_lps = 42 },
    .{ .qeval = 0x2201, .mps = 1, .next_mps = 49, .next_lps = 43 },
    .{ .qeval = 0x1c01, .mps = 0, .next_mps = 50, .next_lps = 44 },
    .{ .qeval = 0x1c01, .mps = 1, .next_mps = 51, .next_lps = 45 },
    .{ .qeval = 0x1801, .mps = 0, .next_mps = 52, .next_lps = 46 },
    .{ .qeval = 0x1801, .mps = 1, .next_mps = 53, .next_lps = 47 },
    .{ .qeval = 0x1601, .mps = 0, .next_mps = 54, .next_lps = 48 },
    .{ .qeval = 0x1601, .mps = 1, .next_mps = 55, .next_lps = 49 },
    .{ .qeval = 0x1401, .mps = 0, .next_mps = 56, .next_lps = 50 },
    .{ .qeval = 0x1401, .mps = 1, .next_mps = 57, .next_lps = 51 },
    .{ .qeval = 0x1201, .mps = 0, .next_mps = 58, .next_lps = 52 },
    .{ .qeval = 0x1201, .mps = 1, .next_mps = 59, .next_lps = 53 },
    .{ .qeval = 0x1101, .mps = 0, .next_mps = 60, .next_lps = 54 },
    .{ .qeval = 0x1101, .mps = 1, .next_mps = 61, .next_lps = 55 },
    .{ .qeval = 0x0ac1, .mps = 0, .next_mps = 62, .next_lps = 56 },
    .{ .qeval = 0x0ac1, .mps = 1, .next_mps = 63, .next_lps = 57 },
    .{ .qeval = 0x09c1, .mps = 0, .next_mps = 64, .next_lps = 58 },
    .{ .qeval = 0x09c1, .mps = 1, .next_mps = 65, .next_lps = 59 },
    .{ .qeval = 0x08a1, .mps = 0, .next_mps = 66, .next_lps = 60 },
    .{ .qeval = 0x08a1, .mps = 1, .next_mps = 67, .next_lps = 61 },
    .{ .qeval = 0x0521, .mps = 0, .next_mps = 68, .next_lps = 62 },
    .{ .qeval = 0x0521, .mps = 1, .next_mps = 69, .next_lps = 63 },
    .{ .qeval = 0x0441, .mps = 0, .next_mps = 70, .next_lps = 64 },
    .{ .qeval = 0x0441, .mps = 1, .next_mps = 71, .next_lps = 65 },
    .{ .qeval = 0x02a1, .mps = 0, .next_mps = 72, .next_lps = 66 },
    .{ .qeval = 0x02a1, .mps = 1, .next_mps = 73, .next_lps = 67 },
    .{ .qeval = 0x0221, .mps = 0, .next_mps = 74, .next_lps = 68 },
    .{ .qeval = 0x0221, .mps = 1, .next_mps = 75, .next_lps = 69 },
    .{ .qeval = 0x0141, .mps = 0, .next_mps = 76, .next_lps = 70 },
    .{ .qeval = 0x0141, .mps = 1, .next_mps = 77, .next_lps = 71 },
    .{ .qeval = 0x0111, .mps = 0, .next_mps = 78, .next_lps = 72 },
    .{ .qeval = 0x0111, .mps = 1, .next_mps = 79, .next_lps = 73 },
    .{ .qeval = 0x0085, .mps = 0, .next_mps = 80, .next_lps = 74 },
    .{ .qeval = 0x0085, .mps = 1, .next_mps = 81, .next_lps = 75 },
    .{ .qeval = 0x0049, .mps = 0, .next_mps = 82, .next_lps = 76 },
    .{ .qeval = 0x0049, .mps = 1, .next_mps = 83, .next_lps = 77 },
    .{ .qeval = 0x0025, .mps = 0, .next_mps = 84, .next_lps = 78 },
    .{ .qeval = 0x0025, .mps = 1, .next_mps = 85, .next_lps = 79 },
    .{ .qeval = 0x0015, .mps = 0, .next_mps = 86, .next_lps = 80 },
    .{ .qeval = 0x0015, .mps = 1, .next_mps = 87, .next_lps = 81 },
    .{ .qeval = 0x0009, .mps = 0, .next_mps = 88, .next_lps = 82 },
    .{ .qeval = 0x0009, .mps = 1, .next_mps = 89, .next_lps = 83 },
    .{ .qeval = 0x0005, .mps = 0, .next_mps = 90, .next_lps = 84 },
    .{ .qeval = 0x0005, .mps = 1, .next_mps = 91, .next_lps = 85 },
    .{ .qeval = 0x0001, .mps = 0, .next_mps = 90, .next_lps = 86 },
    .{ .qeval = 0x0001, .mps = 1, .next_mps = 91, .next_lps = 87 },
    .{ .qeval = 0x5601, .mps = 0, .next_mps = 92, .next_lps = 92 },
    .{ .qeval = 0x5601, .mps = 1, .next_mps = 93, .next_lps = 93 },
};

pub const MqContext = struct {
    state_index: u8 = 0,

    pub fn reset(self: *MqContext, state_index: u8) void {
        self.state_index = state_index;
    }

    pub fn mps(self: *const MqContext) u1 {
        return mqc_states[self.state_index].mps;
    }

    pub fn qeval(self: *const MqContext) u16 {
        return mqc_states[self.state_index].qeval;
    }
};

/// Reset a slice of MqContexts to state 0 (default initial state).
/// Callers that require special initial states (e.g. ZC, AGG, UNI) must
/// re-apply those after calling this helper.
pub fn resetContexts(contexts: []MqContext) void {
    for (contexts) |*ctx| ctx.reset(0);
}

pub const MqDecoder = struct {
    bytes: []const u8,
    bp: usize = 0,
    a: u32 = 0x8000,
    c: u32 = 0,
    ct: u8 = 0,

    pub fn init(bytes: []const u8) MqDecoder {
        var decoder: MqDecoder = .{ .bytes = bytes };
        decoder.c = if (bytes.len == 0) 0xff << 16 else @as(u32, bytes[0]) << 16;
        decoder.byteIn();
        decoder.c <<= 7;
        decoder.ct -%= 7;
        decoder.a = 0x8000;
        return decoder;
    }

    fn byteAt(self: *const MqDecoder, index: usize) u8 {
        return if (index < self.bytes.len) self.bytes[index] else 0xff;
    }

    fn byteIn(self: *MqDecoder) void {
        const current = self.byteAt(self.bp);
        const next = self.byteAt(self.bp + 1);
        if (current == 0xff) {
            if (next > 0x8f) {
                self.c += 0xff00;
                self.ct = 8;
            } else {
                self.bp += 1;
                self.c += @as(u32, next) << 9;
                self.ct = 7;
            }
        } else {
            self.bp += 1;
            self.c += @as(u32, next) << 8;
            self.ct = 8;
        }
    }

    fn renormD(self: *MqDecoder) void {
        while (self.a < 0x8000) {
            if (self.ct == 0) self.byteIn();
            self.a <<= 1;
            self.c <<= 1;
            self.ct -%= 1;
        }
    }

    pub fn decode(self: *MqDecoder, context: *MqContext) u1 {
        const state = mqc_states[context.state_index];
        const a = self.a - state.qeval;
        const chigh = self.c >> 16;
        if (chigh < state.qeval) {
            const symbol: u1 = if (a < state.qeval) state.mps else state.mps ^ 1;
            context.state_index = if (a < state.qeval) state.next_mps else state.next_lps;
            self.a = state.qeval;
            self.renormD();
            return symbol;
        }

        self.c -= @as(u32, state.qeval) << 16;
        if ((a & 0x8000) == 0) {
            const symbol: u1 = if (a < state.qeval) state.mps ^ 1 else state.mps;
            context.state_index = if (a < state.qeval) state.next_lps else state.next_mps;
            self.a = a;
            self.renormD();
            return symbol;
        }

        self.a = a;
        return state.mps;
    }

    pub fn debugState(self: *const MqDecoder) struct { a: u32, c: u32, ct: u8, bp: usize } {
        return .{
            .a = self.a,
            .c = self.c,
            .ct = self.ct,
            .bp = self.bp,
        };
    }
};

pub const MqEncoder = struct {
    buffer: std.ArrayListUnmanaged(u8) = .empty,
    a: u32 = 0x8000,
    c: u32 = 0,
    ct: u8 = 12,
    /// Index of the most recently written byte, or null before first output.
    bp: ?usize = null,

    pub fn init() MqEncoder {
        return .{};
    }

    pub fn deinit(self: *MqEncoder, allocator: std.mem.Allocator) void {
        self.buffer.deinit(allocator);
    }

    pub fn clone(self: *const MqEncoder, allocator: std.mem.Allocator) !MqEncoder {
        var copy = MqEncoder{
            .a = self.a,
            .c = self.c,
            .ct = self.ct,
            .bp = self.bp,
        };
        try copy.buffer.appendSlice(allocator, self.buffer.items);
        return copy;
    }

    /// Encode a single binary symbol using the given context.
    /// Derived from the CODEMPS / CODELPS procedures in ITU-T T.800 Annex C,
    /// with the conditional-exchange paths adapted to match the MqDecoder above.
    pub fn encode(self: *MqEncoder, allocator: std.mem.Allocator, context: *MqContext, symbol: u1) !void {
        const state = mqc_states[context.state_index];
        const qe = @as(u32, state.qeval);
        self.a -= qe;

        if (symbol == state.mps) {
            // CODEMPS
            if ((self.a & 0x8000) == 0) {
                // Renormalization needed
                if (self.a < qe) {
                    // Conditional exchange: MPS interval is smaller
                    self.a = qe;
                } else {
                    self.c += qe;
                }
                context.state_index = state.next_mps;
                try self.renormalize(allocator);
            } else {
                self.c += qe;
            }
        } else {
            // CODELPS
            if (self.a < qe) {
                // Conditional exchange: c += qe, keep a
                self.c += qe;
            } else {
                // No exchange: a = qe
                self.a = qe;
            }
            context.state_index = state.next_lps;
            try self.renormalize(allocator);
        }
    }

    fn renormalize(self: *MqEncoder, allocator: std.mem.Allocator) !void {
        while (self.a < 0x8000) {
            self.a <<= 1;
            self.c <<= 1;
            self.ct -= 1;
            if (self.ct == 0) {
                try self.byteOut(allocator);
            }
        }
    }

    /// Output a byte from the C register, handling bit-stuffing and carry
    /// propagation per ITU-T T.800 Annex C / OpenJPEG mqc_byteout.
    fn byteOut(self: *MqEncoder, allocator: std.mem.Allocator) !void {
        const prev_is_ff = self.bp != null and self.buffer.items[self.bp.?] == 0xff;
        if (prev_is_ff) {
            // Bit-stuffing after 0xff: output only 7 bits.
            const byte: u8 = @intCast((self.c >> 20) & 0x7f);
            self.c &= 0xfffff;
            try self.buffer.append(allocator, byte);
            self.bp = self.buffer.items.len - 1;
            self.ct = 7;
        } else if ((self.c & 0x8000000) == 0) {
            // No carry: normal 8-bit output.
            const byte: u8 = @intCast((self.c >> 19) & 0xff);
            self.c &= 0x7ffff;
            try self.buffer.append(allocator, byte);
            self.bp = self.buffer.items.len - 1;
            self.ct = 8;
        } else {
            // Carry bit set: propagate into previous byte.
            if (self.bp != null) {
                self.buffer.items[self.bp.?] +%= 1;
            }
            self.c &= 0x7ffffff;
            // Check if carry caused previous byte to become 0xff.
            if (self.bp != null and self.buffer.items[self.bp.?] == 0xff) {
                // Must bit-stuff: output only 7 bits.
                const byte: u8 = @intCast((self.c >> 20) & 0x7f);
                self.c &= 0xfffff;
                try self.buffer.append(allocator, byte);
                self.bp = self.buffer.items.len - 1;
                self.ct = 7;
            } else {
                // Normal 8-bit output after carry.
                const byte: u8 = @intCast((self.c >> 19) & 0xff);
                self.c &= 0x7ffff;
                try self.buffer.append(allocator, byte);
                self.bp = self.buffer.items.len - 1;
                self.ct = 8;
            }
        }
    }

    /// Flush the encoder, writing final bytes to terminate the code value.
    pub fn flush(self: *MqEncoder, allocator: std.mem.Allocator) !void {
        self.setbits();
        self.c <<= @intCast(self.ct);
        try self.byteOut(allocator);
        self.c <<= @intCast(self.ct);
        try self.byteOut(allocator);
        // Strip trailing 0xff bytes; the decoder does not need them.
        while (self.buffer.items.len > 0 and self.buffer.items[self.buffer.items.len - 1] == 0xff) {
            _ = self.buffer.pop();
        }
    }

    /// Round C upward so that the bits remaining after byte-out produce a
    /// valid termination point for the decoder (ITU-T T.800 C.2.9 SETBITS).
    fn setbits(self: *MqEncoder) void {
        const temp = self.c + self.a;
        self.c |= 0xffff;
        if (self.c >= temp) {
            self.c -= 0x8000;
        }
    }

    /// Return the encoded bytes. Caller does NOT own the memory.
    pub fn getBytes(self: *const MqEncoder) []const u8 {
        return self.buffer.items;
    }

    pub fn terminatedLength(self: *const MqEncoder, allocator: std.mem.Allocator) !u32 {
        var copy = try self.clone(allocator);
        defer copy.deinit(allocator);
        try copy.flush(allocator);
        return @intCast(copy.buffer.items.len);
    }
};

pub const BitReader = struct {
    bytes: []const u8,
    byte_index: usize = 0,
    bit_index: u3 = 0,
    bits_read: usize = 0,

    pub fn init(bytes: []const u8) BitReader {
        return .{ .bytes = bytes };
    }

    pub fn readBit(self: *BitReader) !u1 {
        if (self.byte_index >= self.bytes.len) return error.EndOfBitstream;
        const value: u1 = @intCast((self.bytes[self.byte_index] >> (7 - self.bit_index)) & 0x01);
        self.bits_read += 1;
        if (self.bit_index == 7) {
            self.byte_index += 1;
            self.bit_index = 0;
            if (self.byte_index < self.bytes.len and self.bytes[self.byte_index - 1] == 0xff and self.bytes[self.byte_index] == 0x00) {
                self.byte_index += 1;
            }
        } else {
            self.bit_index += 1;
        }
        return value;
    }

    pub fn readBits(self: *BitReader, count: u8) !u32 {
        if (count > 32) return error.InvalidBitCount;
        var value: u32 = 0;
        var remaining = count;
        while (remaining > 0) : (remaining -= 1) {
            value = (value << 1) | @as(u32, try self.readBit());
        }
        return value;
    }

    pub fn alignToByte(self: *BitReader) void {
        if (self.bit_index == 0) return;
        const remaining: usize = 8 - @as(usize, self.bit_index);
        self.bits_read += remaining;
        self.byte_index += 1;
        self.bit_index = 0;
        if (self.byte_index < self.bytes.len and self.bytes[self.byte_index - 1] == 0xff and self.bytes[self.byte_index] == 0x00) {
            self.byte_index += 1;
        }
    }

    pub fn consumedBytes(self: *const BitReader) usize {
        return self.byte_index + @intFromBool(self.bit_index != 0);
    }
};

/// Raw-bit writer for BYPASS (lazy) mode in Tier-1: emits bits directly into a byte
/// stream with the same 0xff00 stuffing rule as the MQ encoder output, so callers
/// can splice raw-bit and MQ segments together into one codeword stream.
pub const BitWriter = struct {
    bytes: std.ArrayListUnmanaged(u8) = .empty,
    allocator: std.mem.Allocator,
    bit_index: u3 = 0,

    pub fn init(allocator: std.mem.Allocator) BitWriter {
        return .{ .allocator = allocator };
    }

    pub fn deinit(self: *BitWriter) void {
        self.bytes.deinit(self.allocator);
    }

    pub fn writeBit(self: *BitWriter, bit: u1) !void {
        if (self.bit_index == 0) try self.bytes.append(self.allocator, 0);
        const byte_ptr = &self.bytes.items[self.bytes.items.len - 1];
        const shift: u3 = 7 - self.bit_index;
        byte_ptr.* |= @as(u8, bit) << shift;
        self.bit_index = self.bit_index +% 1;
        if (self.bit_index == 0) {
            // Byte full; if we just wrote 0xff, stuff a zero byte with MSB reserved (0x00).
            if (byte_ptr.* == 0xff) try self.bytes.append(self.allocator, 0);
        }
    }

    pub fn writeBits(self: *BitWriter, value: u32, count: u8) !void {
        var i: u8 = count;
        while (i > 0) {
            i -= 1;
            const bit: u1 = @intCast((value >> @intCast(i)) & 0x01);
            try self.writeBit(bit);
        }
    }

    /// Flush any pending bits into a whole byte boundary. Leaves the trailing byte intact.
    pub fn alignToByte(self: *BitWriter) !void {
        if (self.bit_index != 0) {
            self.bit_index = 0;
            // If the last byte is 0xff we still need the stuffed zero on byte boundary.
            if (self.bytes.items.len > 0 and self.bytes.items[self.bytes.items.len - 1] == 0xff) {
                try self.bytes.append(self.allocator, 0);
            }
        }
    }

    pub fn getBytes(self: *const BitWriter) []const u8 {
        return self.bytes.items;
    }
};

pub const PacketHeaderBitReader = struct {
    bytes: []const u8,
    byte_index: usize = 0,
    bit_index: u3 = 0,
    current_limit: u4 = 8,
    bits_read: usize = 0,

    pub fn init(bytes: []const u8) PacketHeaderBitReader {
        return .{ .bytes = bytes };
    }

    pub fn readBit(self: *PacketHeaderBitReader) !u1 {
        if (self.byte_index >= self.bytes.len) return error.EndOfBitstream;
        const bit_position: u3 = if (self.currentLimit() == 7)
            @as(u3, 6) - self.bit_index
        else
            @as(u3, 7) - self.bit_index;
        const value: u1 = @intCast((self.bytes[self.byte_index] >> bit_position) & 0x01);
        self.bits_read += 1;
        self.advancePosition();
        return value;
    }

    pub fn readBits(self: *PacketHeaderBitReader, count: u8) !u32 {
        if (count > 32) return error.InvalidBitCount;
        var value: u32 = 0;
        var remaining = count;
        while (remaining > 0) : (remaining -= 1) {
            value = (value << 1) | @as(u32, try self.readBit());
        }
        return value;
    }

    pub fn alignToByte(self: *PacketHeaderBitReader) void {
        if (self.bit_index == 0 and self.currentLimit() != 7) return;
        const remaining: usize = @as(usize, self.currentLimit()) - @as(usize, self.bit_index);
        self.bits_read += remaining;
        self.byte_index += 1;
        self.bit_index = 0;
        self.current_limit = self.nextLimitForByte(self.byte_index);
    }

    pub fn consumedBytes(self: *const PacketHeaderBitReader) usize {
        return self.byte_index + @intFromBool(self.bit_index != 0);
    }

    fn currentLimit(self: *const PacketHeaderBitReader) u4 {
        return if (self.byte_index == 0) 8 else self.current_limit;
    }

    fn advancePosition(self: *PacketHeaderBitReader) void {
        const limit = self.currentLimit();
        if (@as(u4, self.bit_index) + 1 >= limit) {
            self.byte_index += 1;
            self.bit_index = 0;
            self.current_limit = self.nextLimitForByte(self.byte_index);
        } else {
            self.bit_index += 1;
        }
    }

    fn nextLimitForByte(self: *const PacketHeaderBitReader, byte_index: usize) u4 {
        if (byte_index == 0 or byte_index >= self.bytes.len) return 8;
        return if (self.bytes[byte_index - 1] == 0xff) 7 else 8;
    }
};

test "bit reader reads across stuffed 0xff00 sequence" {
    var reader = BitReader.init(&.{ 0b10110011, 0xff, 0x00, 0b01010101 });
    try std.testing.expectEqual(@as(u32, 0b10110011), try reader.readBits(8));
    try std.testing.expectEqual(@as(u32, 0xff), try reader.readBits(8));
    try std.testing.expectEqual(@as(u32, 0b01010101), try reader.readBits(8));
    try std.testing.expectEqual(@as(usize, 4), reader.consumedBytes());
}

test "bit reader aligns to next byte boundary" {
    var reader = BitReader.init(&.{ 0b11110000, 0b01010101 });
    try std.testing.expectEqual(@as(u32, 0b1111), try reader.readBits(4));
    reader.alignToByte();
    try std.testing.expectEqual(@as(u32, 0b01010101), try reader.readBits(8));
}

test "packet header bit reader skips stuffed msb after 0xff" {
    var reader = PacketHeaderBitReader.init(&.{ 0xff, 0x7f, 0x80 });
    try std.testing.expectEqual(@as(u32, 0xff), try reader.readBits(8));
    try std.testing.expectEqual(@as(u32, 0x7f), try reader.readBits(7));
    try std.testing.expectEqual(@as(u32, 1), try reader.readBits(1));
}

test "packet header align consumes pending stuffed byte after 0xff" {
    var reader = PacketHeaderBitReader.init(&.{ 0xff, 0x00, 0xff, 0x92 });
    try std.testing.expectEqual(@as(u32, 0xff), try reader.readBits(8));
    reader.alignToByte();
    try std.testing.expectEqual(@as(usize, 2), reader.consumedBytes());
}

test "mq context table exposes expected initial state" {
    var ctx: MqContext = .{};
    try std.testing.expectEqual(@as(u16, 0x5601), ctx.qeval());
    try std.testing.expectEqual(@as(u1, 0), ctx.mps());
    ctx.reset(1);
    try std.testing.expectEqual(@as(u1, 1), ctx.mps());
}

test "mq decoder can consume bounded synthetic stream" {
    var decoder = MqDecoder.init(&.{ 0x6a, 0x3f, 0x92, 0xff, 0x80, 0x11 });
    var ctx: MqContext = .{};
    var ones: usize = 0;
    for (0..16) |_| {
        ones += decoder.decode(&ctx);
    }
    try std.testing.expect(ones <= 16);
    try std.testing.expect(decoder.a >= 0x8000);
}

test "mq encoder round-trips with decoder" {
    const allocator = std.testing.allocator;
    const symbols = [_]u1{ 1, 0, 0, 1, 1, 0, 1, 0, 0, 0, 1, 1 };

    // Encode.
    var enc_ctx: MqContext = .{};
    var encoder = MqEncoder.init();
    defer encoder.deinit(allocator);

    for (symbols) |sym| {
        try encoder.encode(allocator, &enc_ctx, sym);
    }
    try encoder.flush(allocator);

    const encoded = encoder.getBytes();
    try std.testing.expect(encoded.len > 0);

    // Decode.
    var dec_ctx: MqContext = .{};
    var decoder = MqDecoder.init(encoded);

    for (symbols) |expected| {
        const decoded = decoder.decode(&dec_ctx);
        try std.testing.expectEqual(expected, decoded);
    }
}

test "mq encoder round-trips all-zero symbols" {
    const allocator = std.testing.allocator;

    var enc_ctx: MqContext = .{};
    var encoder = MqEncoder.init();
    defer encoder.deinit(allocator);

    for (0..20) |_| {
        try encoder.encode(allocator, &enc_ctx, 0);
    }
    try encoder.flush(allocator);

    var dec_ctx: MqContext = .{};
    var decoder = MqDecoder.init(encoder.getBytes());

    for (0..20) |_| {
        try std.testing.expectEqual(@as(u1, 0), decoder.decode(&dec_ctx));
    }
}

test "mq encoder round-trips all-one symbols" {
    const allocator = std.testing.allocator;

    var enc_ctx: MqContext = .{};
    var encoder = MqEncoder.init();
    defer encoder.deinit(allocator);

    for (0..20) |_| {
        try encoder.encode(allocator, &enc_ctx, 1);
    }
    try encoder.flush(allocator);

    var dec_ctx: MqContext = .{};
    var decoder = MqDecoder.init(encoder.getBytes());

    for (0..20) |_| {
        try std.testing.expectEqual(@as(u1, 1), decoder.decode(&dec_ctx));
    }
}

test "mq encoder round-trips with multiple contexts" {
    const allocator = std.testing.allocator;
    const symbols = [_]u1{ 1, 0, 1, 1, 0, 0, 1, 0, 1, 0, 0, 1, 0, 1, 1, 0 };
    // Alternate between two contexts.
    var enc_ctx0: MqContext = .{};
    var enc_ctx1: MqContext = .{};
    var encoder = MqEncoder.init();
    defer encoder.deinit(allocator);

    for (symbols, 0..) |sym, i| {
        const ctx = if (i % 2 == 0) &enc_ctx0 else &enc_ctx1;
        try encoder.encode(allocator, ctx, sym);
    }
    try encoder.flush(allocator);

    var dec_ctx0: MqContext = .{};
    var dec_ctx1: MqContext = .{};
    var decoder = MqDecoder.init(encoder.getBytes());

    for (symbols, 0..) |expected, i| {
        const ctx = if (i % 2 == 0) &dec_ctx0 else &dec_ctx1;
        try std.testing.expectEqual(expected, decoder.decode(ctx));
    }
}

test "bit writer round-trips individual bits" {
    const allocator = std.testing.allocator;
    var writer = BitWriter.init(allocator);
    defer writer.deinit();

    const pattern = [_]u1{ 1, 0, 1, 1, 0, 0, 1, 0, 1, 1, 1, 0, 0, 0, 1, 0, 1, 0, 1 };
    for (pattern) |bit| try writer.writeBit(bit);
    try writer.alignToByte();

    var reader = BitReader.init(writer.getBytes());
    for (pattern) |bit| {
        try std.testing.expectEqual(bit, try reader.readBit());
    }
}

test "bit writer writeBits matches readBits across multiple widths" {
    const allocator = std.testing.allocator;
    var writer = BitWriter.init(allocator);
    defer writer.deinit();

    try writer.writeBits(0xA5, 8);
    try writer.writeBits(0x07, 3);
    try writer.writeBits(0xDEAD, 16);
    try writer.writeBits(0x1, 1);
    try writer.alignToByte();

    var reader = BitReader.init(writer.getBytes());
    try std.testing.expectEqual(@as(u32, 0xA5), try reader.readBits(8));
    try std.testing.expectEqual(@as(u32, 0x07), try reader.readBits(3));
    try std.testing.expectEqual(@as(u32, 0xDEAD), try reader.readBits(16));
    try std.testing.expectEqual(@as(u32, 0x1), try reader.readBits(1));
}

test "bit writer applies 0xff stuffing rule readable by BitReader" {
    const allocator = std.testing.allocator;
    var writer = BitWriter.init(allocator);
    defer writer.deinit();

    // Emit 0xff, 0xff, then pattern bits — this exercises the stuffed 0x00
    // byte inserted after every 0xff, which BitReader must skip.
    try writer.writeBits(0xff, 8);
    try writer.writeBits(0xff, 8);
    try writer.writeBits(0x55, 8);
    try writer.alignToByte();

    // The emitted bytes must include stuffed zeros after each 0xff.
    const bytes = writer.getBytes();
    try std.testing.expect(bytes.len >= 5);

    var reader = BitReader.init(bytes);
    try std.testing.expectEqual(@as(u32, 0xff), try reader.readBits(8));
    try std.testing.expectEqual(@as(u32, 0xff), try reader.readBits(8));
    try std.testing.expectEqual(@as(u32, 0x55), try reader.readBits(8));
}
