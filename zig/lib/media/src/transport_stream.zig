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

//! Bounded MPEG-2 TS single-program AVC PES extraction. No pixel decoding.
const std = @import("std");
const source = @import("source.zig");
const admission = @import("admission.zig");
pub const Limits = struct {
    max_bytes: usize = 32 * 1024 * 1024,
    max_packets: usize = 8192,
    max_pes_bytes: usize = 16 * 1024 * 1024,
};
pub const Packet = struct {
    offset: usize,
    size: usize,
    pts: i64,
    dts: i64,
    /// PTS/DTS apply only to the first access unit commencing in this PES.
    /// Untimed continuation PES records carry no inferred clock (numeric zeros).
    has_timestamp: bool = true,
};
const Continuity = struct {
    counter: ?u4 = null,
    start: bool = false,
    previous: [184]u8 = undefined,
    length: usize = 0,
    fn accept(self: *Continuity, counter: u4, start: bool, payload: []const u8, discontinuity: bool) !bool {
        if (payload.len > self.previous.len) return error.MalformedTransportStream;
        if (discontinuity) self.counter = null;
        if (self.counter) |last| {
            if (counter == last) {
                if (start != self.start or payload.len != self.length or !std.mem.eql(u8, payload, self.previous[0..self.length])) return error.TransportContinuityError;
                return false; // Exact duplicate payload; do not append it twice.
            }
            if (counter != last +% 1) return error.TransportContinuityError;
        }
        self.counter = counter;
        self.start = start;
        self.length = payload.len;
        @memcpy(self.previous[0..payload.len], payload);
        return true;
    }
};
pub const State = struct {
    program: ?u16 = null,
    pmt_pid: ?u16 = null,
    video_pid: ?u16 = null,
    pcr_pid: ?u16 = null,
    pat_continuity: Continuity = .{},
    pmt_continuity: Continuity = .{},
    video_continuity: Continuity = .{},
    last_dts: ?i64 = null,
};
pub const Demux = struct {
    allocator: std.mem.Allocator,
    bytes: []u8,
    packets: []Packet,
    /// Caller commits only after downstream ingest succeeds. Errors leave its
    /// supplied state unchanged, including continuity and rollover clocks.
    next_state: State,
    reservation: admission.Token = .{},
    pub fn deinit(self: *Demux) void {
        self.allocator.free(self.bytes);
        self.allocator.free(self.packets);
        self.reservation.deinit();
        self.* = undefined;
    }
};
pub fn isTransportStream(bytes: []const u8) bool {
    return bytes.len >= 188 and bytes.len % 188 == 0 and bytes[0] == 0x47 and (bytes.len == 188 or bytes[188] == 0x47);
}
fn crc(bytes: []const u8) u32 {
    var value: u32 = 0xffffffff;
    for (bytes) |byte| {
        value ^= @as(u32, byte) << 24;
        for (0..8) |_| value = if (value & 0x80000000 != 0) (value << 1) ^ 0x04c11db7 else value << 1;
    }
    return value;
}
const Section = struct {
    bytes: [1024]u8 = undefined,
    length: usize = 0,
    expected: ?usize = null,
    fn append(self: *Section, input: []const u8, used: *usize) !?[]const u8 {
        while (used.* < input.len) {
            if (self.length == 0 and input[used.*] == 0xff) {
                used.* = input.len;
                return null;
            }
            if (self.length == self.bytes.len) return error.MalformedTransportStream;
            self.bytes[self.length] = input[used.*];
            self.length += 1;
            used.* += 1;
            if (self.length == 3) {
                const size = (@as(usize, self.bytes[1] & 15) << 8) | self.bytes[2];
                if (size < 9 or size > 1021 or self.bytes[1] & 0xf0 != 0xb0) return error.MalformedTransportStream;
                self.expected = size + 3;
            }
            if (self.expected != null and self.length == self.expected.?) {
                const result = self.bytes[0..self.length];
                self.length = 0;
                self.expected = null;
                if (crc(result) != 0) return error.InvalidTransportCrc;
                return result;
            }
        }
        return null;
    }
};
const Table = struct {
    identity: ?u16 = null,
    version: u5 = 0,
    last: u8 = 0,
    seen: [4]u64 = @splat(0),
    fn accept(self: *Table, bytes: []const u8) !void {
        const identity = std.mem.readInt(u16, bytes[3..5], .big);
        const version: u5 = @intCast((bytes[5] >> 1) & 31);
        if (self.identity == null or self.identity.? != identity or self.version != version) {
            self.* = .{ .identity = identity, .version = version, .last = bytes[7] };
        } else if (self.last != bytes[7]) return error.MalformedTransportStream;
        const number: usize = bytes[6];
        self.seen[number / 64] |= @as(u64, 1) << @intCast(number % 64);
    }
    fn complete(self: Table) bool {
        if (self.identity == null) return false;
        for (0..@as(usize, self.last) + 1) |number| {
            if (self.seen[number / 64] & (@as(u64, 1) << @intCast(number % 64)) == 0) return false;
        }
        return true;
    }
};
/// Borrowed payload valid only during its callback. Copy it for retention.
/// Continuation PES records retain has_timestamp=false; no clocks are invented.
pub const Pes = struct { bytes: []const u8, packet: Packet, ordinal: usize };
pub const Consumer = struct { context: *anyopaque, accept: *const fn (*anyopaque, Pes) anyerror!void };
const Parser = struct {
    allocator: std.mem.Allocator,
    limits: Limits,
    control: source.Control,
    state: State,
    pat: Section = .{},
    pmt: Section = .{},
    pat_table: Table = .{},
    pmt_table: Table = .{},
    pes: std.ArrayList(u8) = .empty,
    pes_started: bool = false,
    output: std.ArrayList(u8) = .empty,
    packets: std.ArrayList(Packet) = .empty,
    consumer: ?Consumer = null,
    published_packets: usize = 0,
    published_bytes: usize = 0,
    fn section(self: *Parser, bytes: []const u8, pat: bool) !void {
        try self.control.check();
        if (bytes.len < 12 or bytes[5] & 0xc0 != 0xc0) return error.MalformedTransportStream;
        if (bytes[5] & 1 == 0) return; // A future table does not configure this stream.
        if (bytes[6] > bytes[7]) return error.MalformedTransportStream;
        if (pat) {
            if (bytes[0] != 0 or (bytes.len - 12) % 4 != 0) return error.MalformedTransportStream;
            var cursor: usize = 8;
            while (cursor < bytes.len - 4) : (cursor += 4) {
                const program = std.mem.readInt(u16, bytes[cursor..][0..2], .big);
                if (bytes[cursor + 2] & 0xe0 != 0xe0) return error.MalformedTransportStream;
                const pid = (@as(u16, bytes[cursor + 2] & 31) << 8) | bytes[cursor + 3];
                if (program == 0) continue; // Network information, not a media program.
                if (pid == 0 or pid == 0x1fff) return error.MalformedTransportStream;
                if (self.state.program) |previous| if (program != previous) return error.UnsupportedTransportProgram;
                if (self.state.pmt_pid) |previous| if (pid != previous) return error.SourceVersionChanged;
                self.state.program = program;
                self.state.pmt_pid = pid;
            }
            try self.pat_table.accept(bytes);
        } else {
            if (bytes[0] != 2 or bytes.len < 16 or std.mem.readInt(u16, bytes[3..5], .big) != self.state.program.?) return error.MalformedTransportStream;
            if (bytes[8] & 0xe0 != 0xe0 or bytes[10] & 0xf0 != 0xf0) return error.MalformedTransportStream;
            const pcr_pid = (@as(u16, bytes[8] & 31) << 8) | bytes[9];
            const program_info = (@as(usize, bytes[10] & 15) << 8) | bytes[11];
            var cursor: usize = 12 + program_info;
            if (cursor > bytes.len - 4) return error.MalformedTransportStream;
            var video: ?u16 = null;
            while (cursor < bytes.len - 4) {
                if (bytes.len - 4 - cursor < 5 or bytes[cursor + 1] & 0xe0 != 0xe0 or bytes[cursor + 3] & 0xf0 != 0xf0) return error.MalformedTransportStream;
                const pid = (@as(u16, bytes[cursor + 1] & 31) << 8) | bytes[cursor + 2];
                const length = (@as(usize, bytes[cursor + 3] & 15) << 8) | bytes[cursor + 4];
                if (bytes[cursor] == 0x1b) {
                    if (pid == 0 or pid == 0x1fff or video != null) return error.UnsupportedTransportProgram;
                    video = pid;
                }
                cursor += 5;
                if (length > bytes.len - 4 - cursor) return error.MalformedTransportStream;
                cursor += length;
            }
            if (video) |pid| {
                if (self.state.video_pid) |previous| if (pid != previous) return error.SourceVersionChanged;
                self.state.video_pid = pid;
            }
            self.state.pcr_pid = pcr_pid;
            try self.pmt_table.accept(bytes);
        }
    }
    fn psi(self: *Parser, assembler: *Section, payload: []const u8, start: bool, pat: bool) !void {
        var cursor: usize = 0;
        if (start) {
            if (payload.len == 0 or payload[0] > payload.len - 1) return error.MalformedTransportStream;
            const pointer: usize = payload[0];
            if (assembler.length != 0) {
                var used: usize = 0;
                if (try assembler.append(payload[1 .. 1 + pointer], &used)) |completed| try self.section(completed, pat);
                if (assembler.length != 0) return error.TruncatedTransportSection;
            }
            cursor = 1 + pointer;
        } else if (assembler.length == 0) return error.TruncatedTransportSection;
        while (cursor < payload.len) {
            if (try assembler.append(payload, &cursor)) |completed| try self.section(completed, pat) else break;
        }
    }
    fn finishPes(self: *Parser) !void {
        if (!self.pes_started) return;
        try self.control.check();
        const bytes = self.pes.items;
        if (bytes.len < 9 or !std.mem.eql(u8, bytes[0..3], &.{ 0, 0, 1 }) or bytes[3] < 0xe0 or bytes[3] > 0xef or bytes[6] & 0xc0 != 0x80) return error.MalformedTransportPes;
        if (bytes[6] & 0x30 != 0) return error.UnsupportedEncryptedMedia;
        const declared = std.mem.readInt(u16, bytes[4..6], .big);
        const end: usize = if (declared == 0) bytes.len else 6 + @as(usize, declared);
        if (end > bytes.len) return error.TruncatedTransportPes;
        for (bytes[end..]) |byte| if (byte != 0xff) return error.MalformedTransportPes;
        const header = @as(usize, bytes[8]) + 9;
        if (header > end) return error.TruncatedTransportPes;
        const flags = bytes[7] >> 6;
        if (flags == 1) return error.MalformedTransportPes;
        const timed = flags != 0;
        var pts: i64 = 0;
        var dts: i64 = 0;
        if (timed) {
            if (bytes[8] < (if (flags == 3) @as(u8, 10) else 5)) return error.MalformedTransportPes;
            const pts_raw = try timestamp(bytes[9..14], if (flags == 3) 3 else 2);
            const dts_raw = if (flags == 3) try timestamp(bytes[14..19], 1) else pts_raw;
            dts = try unwrap(dts_raw, self.state.last_dts);
            if (self.state.last_dts) |last| if (dts < last) return error.TransportTimestampRegression;
            pts = std.math.add(i64, dts, delta(pts_raw, dts_raw)) catch return error.ResourceLimitExceeded;
        }
        if (header == end) return error.MalformedTransportPes;
        const count = if (self.consumer != null) self.published_packets else self.packets.items.len;
        const offset = if (self.consumer != null) self.published_bytes else self.output.items.len;
        if (count >= self.limits.max_packets or end - header > self.limits.max_bytes -| offset) return error.ResourceLimitExceeded;
        const packet_info = Packet{ .offset = offset, .size = end - header, .dts = dts, .pts = pts, .has_timestamp = timed };
        if (self.consumer) |consumer| {
            try consumer.accept(consumer.context, .{ .bytes = bytes[header..end], .packet = packet_info, .ordinal = count });
            self.published_packets += 1;
            self.published_bytes += end - header;
        } else {
            try self.packets.append(self.allocator, packet_info);
            try self.output.appendSlice(self.allocator, bytes[header..end]);
        }
        if (timed) self.state.last_dts = dts;
        self.pes.clearRetainingCapacity();
        self.pes_started = false;
    }
    fn packet(self: *Parser, bytes: *const [188]u8) !void {
        try self.control.check();
        if (bytes[0] != 0x47 or bytes[1] & 0x80 != 0) return error.MalformedTransportStream;
        const pid = (@as(u16, bytes[1] & 31) << 8) | bytes[2];
        const start = bytes[1] & 0x40 != 0;
        const mode = (bytes[3] >> 4) & 3;
        if (mode == 0) return error.MalformedTransportStream;
        var cursor: usize = 4;
        var discontinuity = false;
        if (mode & 2 != 0) {
            const length: usize = bytes[4];
            if (length > 183 or (mode == 2 and length != 183) or (mode == 3 and length > 182)) return error.MalformedTransportStream;
            if (length != 0) {
                const flags = bytes[5];
                discontinuity = flags & 0x80 != 0;
                var required: usize = 1;
                if (flags & 0x10 != 0) required += 6;
                if (flags & 8 != 0) required += 6;
                if (flags & 4 != 0) required += 1;
                if (required > length) return error.MalformedTransportStream;
            }
            cursor += 1 + length;
        }
        if (mode & 1 == 0 or pid == 0x1fff) return;
        const selected = pid == 0 or (self.state.pmt_pid != null and pid == self.state.pmt_pid.?) or (self.state.video_pid != null and pid == self.state.video_pid.?);
        if (!selected) return; // Other PSI/audio/private streams have no video payload.
        if (bytes[3] & 0xc0 != 0) return error.UnsupportedEncryptedMedia;
        const payload = bytes[cursor..];
        const continuity = if (pid == 0) &self.state.pat_continuity else if (self.state.pmt_pid != null and pid == self.state.pmt_pid.?) &self.state.pmt_continuity else &self.state.video_continuity;
        if (!try continuity.accept(@intCast(bytes[3] & 15), start, payload, discontinuity)) return;
        if (pid == 0) return self.psi(&self.pat, payload, start, true);
        if (self.state.pmt_pid != null and pid == self.state.pmt_pid.?) return self.psi(&self.pmt, payload, start, false);
        if (!self.pat_table.complete() or !self.pmt_table.complete()) return error.MissingTransportTables;
        if (discontinuity and self.pes_started) return error.TransportContinuityError;
        if (start) {
            try self.finishPes();
            self.pes_started = true;
        }
        if (!self.pes_started) return error.TruncatedTransportPes;
        if (payload.len > self.limits.max_pes_bytes -| self.pes.items.len) return error.ResourceLimitExceeded;
        try self.pes.appendSlice(self.allocator, payload);
    }
};

/// Native 188-byte packet framing with bounded active-PES storage. One consumer,
/// synchronous callback backpressure, and no implicit retry. Entered failures
/// are terminal and release all admitted workspace. Accepted payloads may have
/// escaped; reconstruct from the original State and reconcile them on retry.
/// A successful finish returns the transactional state for the next resource.
pub const Stream = struct {
    parser: Parser,
    workspace: admission.TrackedAllocator,
    fixed: admission.Token = .{},
    carry: [188]u8 = undefined,
    carry_len: usize = 0,
    input_bytes: usize = 0,
    largest_pes_capacity: usize = 0,
    ended: bool = false,
    active: bool = false,
    pub fn init(a: std.mem.Allocator, state: State, limits: Limits, control: source.Control, pool: ?*admission.Pool, consumer: Consumer) !Stream {
        try control.check();
        if ((state.pmt_pid != null and state.program == null) or (state.video_pid != null and state.pmt_pid == null)) return error.InvalidTransportState;
        if (limits.max_bytes == 0 or limits.max_packets == 0 or limits.max_pes_bytes == 0) return error.ResourceLimitExceeded;
        var fixed = if (pool) |p| try p.acquire(.{ .host_bytes = @sizeOf(Stream) }) else admission.Token{};
        errdefer fixed.deinit();
        var token = if (pool) |p| try p.acquire(.{}) else admission.Token{};
        errdefer token.deinit();
        // ArrayList may temporarily own old/new moving-growth buffers. Charges
        // follow actual allocations, rather than reserving the PES ceiling.
        const bound = std.math.add(usize, std.math.mul(usize, limits.max_pes_bytes, 3) catch return error.ResourceLimitExceeded, 4096) catch return error.ResourceLimitExceeded;
        return .{ .parser = .{ .allocator = a, .limits = limits, .control = control, .state = state, .consumer = consumer }, .workspace = .{ .backing = a, .limit = bound, .reservation = token }, .fixed = fixed };
    }
    pub fn deinit(self: *Stream) void {
        const a = self.workspace.allocator();
        self.parser.pes.deinit(a);
        self.parser.pes = .empty;
        self.parser.output.deinit(a);
        self.parser.output = .empty;
        self.parser.packets.deinit(a);
        self.parser.packets = .empty;
        std.debug.assert(self.workspace.live == 0);
        self.workspace.reservation.deinit();
        self.fixed.deinit();
        self.ended = true;
    }
    fn failure(self: *Stream, err: anyerror) anyerror {
        const result = if (err == error.OutOfMemory and self.workspace.denied != null) self.workspace.denied.? else err;
        self.deinit();
        return result;
    }
    pub fn push(self: *Stream, bytes: []const u8) !void {
        if (self.ended or self.active) return error.InvalidTransportState;
        self.active = true;
        defer self.active = false;
        self.parser.allocator = self.workspace.allocator();
        self.pushEntered(bytes) catch |err| return self.failure(err);
    }
    fn pushEntered(self: *Stream, bytes: []const u8) !void {
        try self.parser.control.check();
        if (bytes.len > self.parser.limits.max_bytes -| self.input_bytes) return error.ResourceLimitExceeded;
        self.input_bytes += bytes.len;
        var offset: usize = 0;
        while (offset < bytes.len) {
            try self.parser.control.check();
            if (self.carry_len == 0 and bytes.len - offset >= 188) {
                try self.parser.packet(bytes[offset..][0..188]);
                self.largest_pes_capacity = @max(self.largest_pes_capacity, self.parser.pes.capacity);
                offset += 188;
                continue;
            }
            const count = @min(self.carry.len - self.carry_len, bytes.len - offset);
            @memcpy(self.carry[self.carry_len..][0..count], bytes[offset..][0..count]);
            self.carry_len += count;
            offset += count;
            if (self.carry_len != self.carry.len) continue;
            try self.parser.packet(&self.carry);
            self.largest_pes_capacity = @max(self.largest_pes_capacity, self.parser.pes.capacity);
            self.carry_len = 0;
        }
    }
    pub fn finish(self: *Stream) !State {
        if (self.ended or self.active) return error.InvalidTransportState;
        self.active = true;
        defer self.active = false;
        self.parser.allocator = self.workspace.allocator();
        const state = self.finishEntered() catch |err| return self.failure(err);
        self.deinit();
        return state;
    }
    fn finishEntered(self: *Stream) !State {
        try self.parser.control.check();
        if (self.input_bytes == 0 or self.carry_len != 0) return error.MalformedTransportStream;
        if (self.parser.pat.length != 0 or self.parser.pmt.length != 0) return error.TruncatedTransportSection;
        try self.parser.finishPes();
        if (!self.parser.pat_table.complete() or !self.parser.pmt_table.complete()) return error.MissingTransportTables;
        if (self.parser.state.video_pid == null) return error.UnsupportedVideoCodec;
        if (self.parser.published_packets == 0) return error.EmptyVideo;
        try self.parser.control.check();
        return self.parser.state;
    }
};

fn timestamp(bytes: []const u8, prefix: u8) !u64 {
    if (bytes.len != 5 or bytes[0] >> 4 != prefix or bytes[0] & 1 == 0 or bytes[2] & 1 == 0 or bytes[4] & 1 == 0) return error.MalformedTransportPes;
    return (@as(u64, (bytes[0] >> 1) & 7) << 30) | (@as(u64, bytes[1]) << 22) | (@as(u64, bytes[2] >> 1) << 15) | (@as(u64, bytes[3]) << 7) | (bytes[4] >> 1);
}
fn delta(value: u64, previous: u64) i64 {
    const wrap: i64 = 1 << 33;
    var result = @as(i64, @intCast(value)) - @as(i64, @intCast(previous));
    if (result < -wrap / 2) result += wrap else if (result > wrap / 2) result -= wrap;
    return result;
}
fn unwrap(value: u64, previous: ?i64) !i64 {
    const last = previous orelse return @intCast(value);
    return std.math.add(i64, last, delta(value, @intCast(@mod(last, @as(i64, 1 << 33))))) catch error.ResourceLimitExceeded;
}
pub fn demux(a: std.mem.Allocator, bytes: []const u8, state: State, limits: Limits, control: source.Control, pool: ?*admission.Pool) !Demux {
    try control.check();
    if (bytes.len > limits.max_bytes) return error.ResourceLimitExceeded;
    if (!isTransportStream(bytes)) return error.MalformedTransportStream;
    if ((state.pmt_pid != null and state.program == null) or
        (state.video_pid != null and state.pmt_pid == null)) return error.InvalidTransportState;
    const count = @min(limits.max_packets, bytes.len / 188);
    const bound = std.math.add(usize, std.math.mul(usize, bytes.len, 6) catch return error.ResourceLimitExceeded, std.math.mul(usize, count, 2 * @sizeOf(Packet)) catch return error.ResourceLimitExceeded) catch return error.ResourceLimitExceeded;
    var reservation = if (pool) |p| try p.acquire(.{ .host_bytes = bound }) else admission.Token{};
    errdefer reservation.deinit();
    var parser = Parser{ .allocator = a, .limits = limits, .state = state, .control = control };
    defer parser.pes.deinit(a);
    defer parser.output.deinit(a);
    defer parser.packets.deinit(a);
    var offset: usize = 0;
    while (offset < bytes.len) : (offset += 188) try parser.packet(bytes[offset..][0..188]);
    if (parser.pat.length != 0 or parser.pmt.length != 0) return error.TruncatedTransportSection;
    try parser.finishPes();
    if (!parser.pat_table.complete() or !parser.pmt_table.complete()) return error.MissingTransportTables;
    if (parser.state.video_pid == null) return error.UnsupportedVideoCodec;
    if (parser.packets.items.len == 0) return error.EmptyVideo;
    const output = try parser.output.toOwnedSlice(a);
    errdefer a.free(output);
    const packets = try parser.packets.toOwnedSlice(a);
    errdefer a.free(packets);
    try control.check();
    try reservation.resize(.{ .host_bytes = output.len + packets.len * @sizeOf(Packet) });
    return .{ .allocator = a, .bytes = output, .packets = packets, .next_state = parser.state, .reservation = reservation };
}

test "transport AVC PES payload clocks and order match independent FFprobe" {
    const Oracle = struct {
        packets: []const struct { offset: usize, size: usize, pts: i64, dts: i64, duration: u32, sync: bool, sha256: []const u8 },
        width: u32,
        height: u32,
        time_base: []const u8,
        decoded_sha256: []const []const u8,
    };
    inline for (.{ "closed", "bframes" }) |name| {
        var result = try demux(std.testing.allocator, @embedFile("../testdata/transport/" ++ name ++ ".ts"), .{}, .{}, .{}, null);
        defer result.deinit();
        const oracle = try std.json.parseFromSlice(Oracle, std.testing.allocator, @embedFile("../testdata/transport/" ++ name ++ ".json"), .{});
        defer oracle.deinit();
        try std.testing.expectEqual(oracle.value.packets.len, result.packets.len);
        try std.testing.expectEqualSlices(u8, @embedFile("../testdata/transport/" ++ name ++ "-pes.bin"), result.bytes);
        for (result.packets, oracle.value.packets) |packet, expected| {
            try std.testing.expectEqual(expected.offset, packet.offset);
            try std.testing.expectEqual(expected.size, packet.size);
            try std.testing.expectEqual(expected.pts, packet.pts);
            try std.testing.expectEqual(expected.dts, packet.dts);
        }
    }
}
fn allocationRun(backing: std.mem.Allocator, pool: *admission.Pool) !void {
    // ArrayList resize success depends on heap placement. Force its allocation
    // fallback so the outer failure campaign has deterministic allocation counts.
    var no_resize = std.testing.FailingAllocator.init(backing, .{ .resize_fail_index = 0 });
    const a = no_resize.allocator();
    defer std.debug.assert(std.meta.eql(pool.snapshot(), admission.Resources{}));
    var result = try demux(a, @embedFile("../testdata/transport/closed.ts"), .{}, .{}, .{}, pool);
    defer result.deinit();
    try std.testing.expect(result.packets.len > 0 and pool.snapshot().host_bytes > 0);
}
test "transport extraction allocation failures and cancellation release all owners" {
    var pool = admission.Pool{ .limits = .{ .host_bytes = 128 * 1024 * 1024 } };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, allocationRun, .{&pool});
    const Cancel = struct {
        calls: usize = 0,
        fn check(raw: ?*const anyopaque) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(@constCast(raw.?)));
            self.calls += 1;
            if (self.calls == 32) return error.Cancelled;
        }
    };
    var cancel = Cancel{};
    try std.testing.expectError(error.Cancelled, demux(std.testing.allocator, @embedFile("../testdata/transport/closed.ts"), .{}, .{}, .{ .context = &cancel, .check_fn = Cancel.check }, &pool));
    try std.testing.expectEqual(admission.Resources{}, pool.snapshot());
    var denied = admission.Pool{ .limits = .{ .host_bytes = 1 } };
    try std.testing.expectError(error.SharedAdmissionExceeded, demux(std.testing.allocator, @embedFile("../testdata/transport/closed.ts"), .{}, .{}, .{}, &denied));
    try std.testing.expectEqual(admission.Resources{}, denied.snapshot());
}
test "transport CRC continuity truncation and timestamp rollover are explicit" {
    const a = std.testing.allocator;
    const bytes = @embedFile("../testdata/transport/closed.ts");
    try std.testing.expectError(error.MalformedTransportStream, demux(a, bytes[0 .. bytes.len - 1], .{}, .{}, .{}, null));
    const damaged = try a.dupe(u8, bytes);
    defer a.free(damaged);
    // Locate PAT, alter its table payload without changing its stored CRC.
    var offset: usize = 0;
    while (offset < damaged.len) : (offset += 188) {
        if (damaged[offset + 1] & 31 == 0 and damaged[offset + 2] == 0) {
            const payload = 4 + (if (damaged[offset + 3] & 0x20 != 0) @as(usize, damaged[offset + 4]) + 1 else 0);
            damaged[offset + payload + 5] ^= 1;
            break;
        }
    }
    try std.testing.expectError(error.InvalidTransportCrc, demux(a, damaged, .{}, .{}, .{}, null));
    try std.testing.expectEqual(@as(i64, (1 << 33) + 20), try unwrap(20, (1 << 33) - 10));
    try std.testing.expectEqual(@as(i64, -20), delta((1 << 33) - 10, 10));
    var continuity = Continuity{};
    try std.testing.expect(try continuity.accept(15, true, "payload", false));
    try std.testing.expect(!try continuity.accept(15, true, "payload", false));
    try std.testing.expect(try continuity.accept(0, false, "next", false));
    try std.testing.expectError(error.TransportContinuityError, continuity.accept(2, false, "gap", false));
    try std.testing.expectError(error.TransportContinuityError, continuity.accept(0, false, "changed", false));
}

test "transport invalid caller state fails without optional dereference" {
    try std.testing.expectError(error.InvalidTransportState, demux(std.testing.allocator, @embedFile("../testdata/transport/closed.ts"), .{ .video_pid = 256 }, .{}, .{}, null));
}

fn fuzzSeed(comptime bytes: []const u8) []const u8 {
    return comptime blk: {
        var encoded: [4 + bytes.len]u8 = undefined;
        std.mem.writeInt(u32, encoded[0..4], bytes.len, .little);
        @memcpy(encoded[4..], bytes);
        const frozen = encoded;
        break :blk &frozen;
    };
}
const fuzz_seeds = .{
    fuzzSeed(@embedFile("../testdata/transport/closed.ts")),
    fuzzSeed(@embedFile("../testdata/transport/bframes.ts")),
    fuzzSeed(@embedFile("../testdata/transport/closed-split.ts")),
    fuzzSeed(@embedFile("../testdata/transport/bframes-split.ts")),
};
test "transport fuzz accepted seeds retain admitted packet bounds" {
    inline for (fuzz_seeds) |seed| {
        var smith = std.testing.Smith{ .in = seed };
        var storage: [256 * 1024]u8 = undefined;
        const length = smith.sliceWithHash(&storage, 0x54530001);
        const bytes = storage[0..length];
        var pool = admission.Pool{ .limits = .{ .host_bytes = 16 * 1024 * 1024 } };
        var result = try demux(std.testing.allocator, bytes, .{}, .{}, .{}, &pool);
        try std.testing.expect(result.packets.len > 0);
        result.deinit();
        try std.testing.expectEqual(admission.Resources{}, pool.snapshot());
    }
}
test "transport fuzz bounded PSI PES and continuity" {
    const Harness = struct {
        fn fuzz(_: void, smith: *std.testing.Smith) !void {
            var storage: [256 * 1024]u8 = undefined;
            const length = smith.sliceWithHash(&storage, 0x54530001);
            const bytes = storage[0..length];
            var pool = admission.Pool{ .limits = .{ .host_bytes = 4 * 1024 * 1024 } };
            defer std.debug.assert(std.meta.eql(pool.snapshot(), admission.Resources{}));
            var result = demux(std.testing.allocator, bytes, .{}, .{ .max_bytes = 256 * 1024, .max_packets = 1024, .max_pes_bytes = 128 * 1024 }, .{}, &pool) catch return;
            defer result.deinit();
            for (result.packets) |packet| {
                try std.testing.expect(packet.offset <= result.bytes.len);
                try std.testing.expect(packet.size <= result.bytes.len - packet.offset);
            }
        }
    };
    try std.testing.fuzz({}, Harness.fuzz, .{ .corpus = &fuzz_seeds });
}

test "transport table completeness includes every numbered section" {
    var table = Table{};
    var section_bytes = [_]u8{ 0, 0, 0, 0, 1, 0xc1, 1, 2 };
    try table.accept(&section_bytes);
    try std.testing.expect(!table.complete());
    section_bytes[6] = 0;
    try table.accept(&section_bytes);
    try std.testing.expect(!table.complete());
    section_bytes[6] = 2;
    try table.accept(&section_bytes);
    try std.testing.expect(table.complete());
    section_bytes[5] = 0xc3;
    try table.accept(&section_bytes);
    try std.testing.expect(!table.complete());
    section_bytes[7] = 1;
    try std.testing.expectError(error.MalformedTransportStream, table.accept(&section_bytes));
}

test "transport untimed continuation PES preserve original payload and timestamp anchors" {
    const a = std.testing.allocator;
    inline for (.{ "closed", "bframes" }) |name| {
        var original = try demux(a, @embedFile("../testdata/transport/" ++ name ++ ".ts"), .{}, .{}, .{}, null);
        defer original.deinit();
        var split = try demux(a, @embedFile("../testdata/transport/" ++ name ++ "-split.ts"), .{}, .{}, .{}, null);
        defer split.deinit();
        try std.testing.expectEqualSlices(u8, original.bytes, split.bytes);
        try std.testing.expect(split.packets.len > original.packets.len);
        var clock: usize = 0;
        for (split.packets) |packet| {
            if (packet.has_timestamp) {
                try std.testing.expect(clock < original.packets.len);
                const expected = original.packets[clock];
                try std.testing.expectEqual(expected.offset, packet.offset);
                try std.testing.expectEqual(expected.pts, packet.pts);
                try std.testing.expectEqual(expected.dts, packet.dts);
                clock += 1;
            } else {
                try std.testing.expectEqual(@as(i64, 0), packet.pts);
                try std.testing.expectEqual(@as(i64, 0), packet.dts);
            }
        }
        try std.testing.expectEqual(original.packets.len, clock);
        try std.testing.expectEqual(original.next_state.last_dts, split.next_state.last_dts);
    }
}

test "incremental transport PES byte clocks and transactional state match buffered independent fixtures at every packet boundary" {
    const a = std.testing.allocator;
    const Capture = struct {
        expected: *Demux,
        count: usize = 0,
        before_eof: usize = 0,
        finishing: bool = false,
        fn accept(raw: *anyopaque, pes: Pes) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const packet = self.expected.packets[self.count];
            try std.testing.expectEqual(self.count, pes.ordinal);
            try std.testing.expectEqualDeep(packet, pes.packet);
            try std.testing.expectEqualSlices(u8, self.expected.bytes[packet.offset..][0..packet.size], pes.bytes);
            if (!self.finishing) self.before_eof += 1;
            self.count += 1;
        }
    };
    inline for (.{ "closed", "bframes", "closed-split", "bframes-split" }) |name| {
        const bytes = @embedFile("../testdata/transport/" ++ name ++ ".ts");
        var expected = try demux(a, bytes, .{}, .{}, .{}, null);
        defer expected.deinit();
        for ([_]usize{ 1, 187, 188, 189, 8192, bytes.len }) |chunk| {
            var pool = admission.Pool{ .limits = .{ .host_bytes = 1024 * 1024 } };
            var capture = Capture{ .expected = &expected };
            var stream = try Stream.init(a, .{}, .{}, .{}, &pool, .{ .context = &capture, .accept = Capture.accept });
            defer stream.deinit();
            var offset: usize = 0;
            while (offset < bytes.len) {
                const end = @min(bytes.len, offset + chunk);
                try stream.push(bytes[offset..end]);
                offset = end;
            }
            try std.testing.expect(capture.before_eof > 0);
            try std.testing.expectEqual(@as(usize, 0), stream.parser.output.capacity);
            try std.testing.expectEqual(@as(usize, 0), stream.parser.packets.capacity);
            try std.testing.expect(stream.largest_pes_capacity < bytes.len);
            capture.finishing = true;
            const state = try stream.finish();
            try std.testing.expectEqualDeep(expected.next_state, state);
            try std.testing.expectEqual(expected.packets.len, capture.count);
            try std.testing.expectEqual(admission.Resources{}, pool.snapshot());
            try std.testing.expectError(error.InvalidTransportState, stream.push(""));
        }
    }
}
fn streamingAllocations(backing: std.mem.Allocator) !void {
    // Keep moving-growth failure counts deterministic across heap placement.
    var no_resize = std.testing.FailingAllocator.init(backing, .{ .resize_fail_index = 0 });
    const a = no_resize.allocator();
    const Discard = struct {
        fn accept(_: *anyopaque, _: Pes) !void {}
    };
    var dummy: u8 = 0;
    var pool = admission.Pool{ .limits = .{ .host_bytes = 1024 * 1024 } };
    defer std.debug.assert(std.meta.eql(pool.snapshot(), admission.Resources{}));
    var stream = try Stream.init(a, .{}, .{}, .{}, &pool, .{ .context = &dummy, .accept = Discard.accept });
    defer stream.deinit();
    const bytes = @embedFile("../testdata/transport/closed-split.ts");
    var offset: usize = 0;
    while (offset < bytes.len) {
        const end = @min(bytes.len, offset + 189);
        try stream.push(bytes[offset..end]);
        offset = end;
    }
    _ = try stream.finish();
}
test "incremental transport allocation failure drains actual PES growth and fixed admission" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, streamingAllocations, .{});
}
test "incremental transport cancellation consumer failure truncated packets and limits release owners and prohibit reentry" {
    const a = std.testing.allocator;
    const bytes = @embedFile("../testdata/transport/closed.ts");
    const Gate = struct {
        cancel: bool = false,
        fail: bool = false,
        count: usize = 0,
        fn check(raw: ?*const anyopaque) !void {
            if (@as(*const @This(), @ptrCast(@alignCast(raw.?))).cancel) return error.Cancelled;
        }
        fn accept(raw: *anyopaque, _: Pes) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.fail) return error.ConsumerFailure;
            self.count += 1;
        }
    };
    var gate = Gate{};
    var pool = admission.Pool{ .limits = .{ .host_bytes = 1024 * 1024 } };
    var stream = try Stream.init(a, .{}, .{}, .{ .context = &gate, .check_fn = Gate.check }, &pool, .{ .context = &gate, .accept = Gate.accept });
    defer stream.deinit();
    try stream.push(bytes[0..188]);
    gate.cancel = true;
    try std.testing.expectError(error.Cancelled, stream.push(bytes[188..]));
    try std.testing.expectEqual(admission.Resources{}, pool.snapshot());
    gate.cancel = false;
    gate.fail = true;
    stream = try Stream.init(a, .{}, .{}, .{}, &pool, .{ .context = &gate, .accept = Gate.accept });
    try std.testing.expectError(error.ConsumerFailure, stream.push(bytes));
    try std.testing.expectEqual(admission.Resources{}, pool.snapshot());
    gate.fail = false;
    stream = try Stream.init(a, .{}, .{}, .{}, &pool, .{ .context = &gate, .accept = Gate.accept });
    try stream.push(bytes[0 .. bytes.len - 1]);
    try std.testing.expectError(error.MalformedTransportStream, stream.finish());
    try std.testing.expectEqual(admission.Resources{}, pool.snapshot());
    stream = try Stream.init(a, .{}, .{ .max_packets = 1 }, .{}, &pool, .{ .context = &gate, .accept = Gate.accept });
    try std.testing.expectError(error.ResourceLimitExceeded, stream.push(bytes));
    try std.testing.expectEqual(admission.Resources{}, pool.snapshot());
    try std.testing.expectError(error.InvalidTransportState, stream.finish());
    var denied = admission.Pool{ .limits = .{ .host_bytes = @sizeOf(Stream) - 1 } };
    try std.testing.expectError(error.SharedAdmissionExceeded, Stream.init(a, .{}, .{}, .{}, &denied, .{ .context = &gate, .accept = Gate.accept }));
    try std.testing.expectEqual(admission.Resources{}, denied.snapshot());
}

test "incremental transport CRC continuity and dynamic growth denial fail before corrupt payload delivery" {
    const a = std.testing.allocator;
    const bytes = @embedFile("../testdata/transport/closed.ts");
    const Discard = struct {
        count: usize = 0,
        fn accept(raw: *anyopaque, _: Pes) !void {
            @as(*@This(), @ptrCast(@alignCast(raw))).count += 1;
        }
    };
    var capture = Discard{};
    var pool = admission.Pool{ .limits = .{ .host_bytes = 1024 * 1024 } };
    const damaged = try a.dupe(u8, bytes);
    defer a.free(damaged);
    var offset: usize = 0;
    while (offset < damaged.len) : (offset += 188) {
        if (damaged[offset + 1] & 31 == 0 and damaged[offset + 2] == 0) {
            const payload = 4 + (if (damaged[offset + 3] & 0x20 != 0) @as(usize, damaged[offset + 4]) + 1 else 0);
            damaged[offset + payload + 5] ^= 1;
            break;
        }
    }
    var stream = try Stream.init(a, .{}, .{}, .{}, &pool, .{ .context = &capture, .accept = Discard.accept });
    defer stream.deinit();
    try std.testing.expectError(error.InvalidTransportCrc, stream.push(damaged));
    try std.testing.expectEqual(@as(usize, 0), capture.count);
    try std.testing.expectEqual(admission.Resources{}, pool.snapshot());
    // Permit the fixed parser, but deny even the first dynamic PES allocation.
    var constrained = admission.Pool{ .limits = .{ .host_bytes = @sizeOf(Stream) } };
    stream = try Stream.init(a, .{}, .{}, .{}, &constrained, .{ .context = &capture, .accept = Discard.accept });
    try std.testing.expectError(error.SharedAdmissionExceeded, stream.push(bytes));
    try std.testing.expectEqual(@as(usize, 0), capture.count);
    try std.testing.expectEqual(admission.Resources{}, constrained.snapshot());
}
