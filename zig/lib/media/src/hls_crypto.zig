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

//! Whole-resource HLS AES-128 CBC using Zig's native AES implementation.
const std = @import("std");
const attributes = @import("hls_attributes.zig");
const source = @import("source.zig");
pub const Key = struct {
    uri: []const u8,
    iv: ?[16]u8 = null,
    pub fn mediaIv(self: Key, sequence: u64) [16]u8 {
        if (self.iv) |value| return value;
        var value: [16]u8 = @splat(0);
        std.mem.writeInt(u64, value[8..16], sequence, .big);
        return value;
    }
};
pub const Declaration = struct { key: ?Key, min_version: u64 = 1, supported: bool = true };
pub fn same(left: ?Key, right: ?Key) bool {
    const l = left orelse return right == null;
    const r = right orelse return false;
    return std.mem.eql(u8, l.uri, r.uri) and std.meta.eql(l.iv, r.iv);
}
pub fn parseKey(text: []const u8, max_uri_bytes: usize) !Declaration {
    const list = try attributes.parse(text);
    const method = list.get("METHOD") orelse return error.MalformedHls;
    if (method.quoted) return error.MalformedHls;
    if (std.mem.eql(u8, method.value, "NONE")) {
        if (list.len != 1) return error.MalformedHls;
        return .{ .key = null };
    }
    const uri = (try list.string("URI")) orelse return error.MalformedHls;
    if (uri.len > max_uri_bytes) return error.ResourceLimitExceeded;
    for (uri) |c| if (c <= 0x20 or c == 0x7f or c == '"') return error.MalformedHls;
    var declaration = Declaration{ .key = .{ .uri = uri } };
    if (list.string("KEYFORMAT") catch return error.MalformedHls) |format| {
        declaration.supported = std.mem.eql(u8, format, "identity");
        declaration.min_version = 5;
    }
    if (try list.string("KEYFORMATVERSIONS")) |versions| {
        var supported = false;
        var parts = std.mem.splitScalar(u8, versions, '/');
        while (parts.next()) |part| {
            const version = try attributes.uint(part);
            if (version == 0) return error.MalformedHls;
            supported = supported or version == 1;
        }
        if (declaration.supported and !supported) return error.UnsupportedEncryptedMedia;
        declaration.min_version = 5;
    }
    if (list.get("IV")) |item| {
        if (item.quoted or item.value.len < 3 or item.value.len > 34 or (!std.mem.startsWith(u8, item.value, "0x") and !std.mem.startsWith(u8, item.value, "0X"))) return error.MalformedHls;
        for (item.value[2..]) |c| if (!((c >= '0' and c <= '9') or (c >= 'A' and c <= 'F') or (c >= 'a' and c <= 'f'))) return error.MalformedHls;
        const number = std.fmt.parseInt(u128, item.value[2..], 16) catch return error.MalformedHls;
        var iv: [16]u8 = undefined;
        std.mem.writeInt(u128, &iv, number, .big);
        declaration.key.?.iv = iv;
        declaration.min_version = @max(declaration.min_version, 2);
    }
    if (declaration.supported and !std.mem.eql(u8, method.value, "AES-128")) return error.UnsupportedEncryptedMedia;
    return declaration;
}
/// Destructively decrypt an owned resource. Return the unpadded prefix; the
/// caller retains/frees the full allocation and wipes it on every exit path.
pub fn decrypt(bytes: []u8, key: *const [16]u8, iv: [16]u8, control: source.Control) ![]u8 {
    try control.check();
    if (bytes.len == 0 or bytes.len % 16 != 0) return error.InvalidHlsCiphertext;
    var ctx = std.crypto.core.aes.Aes128.initDec(key.*);
    defer std.crypto.secureZero(u8, std.mem.asBytes(&ctx));
    var previous = iv;
    var offset: usize = 0;
    while (offset + 64 <= bytes.len) : (offset += 64) {
        if (offset % 4096 == 0) try control.check();
        const cipher = bytes[offset..][0..64].*;
        var plain: [64]u8 = undefined;
        defer std.crypto.secureZero(u8, &plain);
        ctx.decryptWide(4, &plain, &cipher);
        inline for (0..4) |block| {
            const before = if (block == 0) previous else cipher[(block - 1) * 16 ..][0..16].*;
            const a: @Vector(16, u8) = plain[block * 16 ..][0..16].*;
            const b: @Vector(16, u8) = before;
            bytes[offset + block * 16 ..][0..16].* = a ^ b;
        }
        previous = cipher[48..64].*;
    }
    while (offset < bytes.len) : (offset += 16) {
        const cipher = bytes[offset..][0..16].*;
        var plain: [16]u8 = undefined;
        defer std.crypto.secureZero(u8, &plain);
        ctx.decrypt(&plain, &cipher);
        for (plain, previous, 0..) |a, b, index| bytes[offset + index] = a ^ b;
        previous = cipher;
    }
    try control.check();
    const padding: usize = bytes[bytes.len - 1];
    if (padding == 0 or padding > 16) return error.InvalidHlsCiphertext;
    var invalid: u8 = 0;
    for (bytes[bytes.len - padding ..]) |byte| invalid |= byte ^ @as(u8, @intCast(padding));
    if (invalid != 0) return error.InvalidHlsCiphertext;
    return bytes[0 .. bytes.len - padding];
}

/// Bounded CBC body decoder. Retains one final plaintext block until PKCS7 is
/// validated at EOF, plus an incomplete ciphertext block. Callback slices are
/// transient and must be copied/consumed before returning. Errors terminalize
/// and erase the key schedule; retry requires a fresh decoder and prefix replay.
pub const Stream = struct {
    /// Conservative fixed stack/key-schedule envelope charged by HTTP admission.
    pub const workspace_bytes: usize = 16 * 1024;
    ctx: std.crypto.core.aes.AesDecryptCtx(std.crypto.core.aes.Aes128),
    previous: [16]u8,
    carry: [16]u8 = @splat(0),
    carry_len: usize = 0,
    pending: [16]u8 = @splat(0),
    has_pending: bool = false,
    control: source.Control,
    ended: bool = false,
    active: bool = false,
    pub fn init(key: *const [16]u8, iv: [16]u8, control: source.Control) Stream {
        return .{ .ctx = std.crypto.core.aes.Aes128.initDec(key.*), .previous = iv, .control = control };
    }
    pub fn deinit(self: *Stream) void {
        std.crypto.secureZero(u8, std.mem.asBytes(self));
        self.ended = true;
    }
    pub fn push(self: *Stream, bytes: []const u8, context: anytype, comptime write: anytype) !void {
        if (self.ended or self.active) return error.InvalidHlsCipherState;
        self.active = true;
        defer self.active = false;
        errdefer self.deinit();
        try self.control.check();
        var output: [8192]u8 = undefined;
        defer std.crypto.secureZero(u8, &output);
        var used: usize = 0;
        var offset: usize = 0;
        var next_check: usize = 4096;
        while (offset < bytes.len) {
            if (offset >= next_check) {
                try self.control.check();
                next_check = offset +| 4096;
            }
            if (used + 64 > output.len) {
                try write(context, output[0..used]);
                used = 0;
            }
            if (self.carry_len == 0 and bytes.len - offset >= 64) {
                const cipher = bytes[offset..][0..64].*;
                var plain: [64]u8 = undefined;
                defer std.crypto.secureZero(u8, &plain);
                self.ctx.decryptWide(4, &plain, &cipher);
                inline for (0..4) |block| {
                    const before = if (block == 0) self.previous else cipher[(block - 1) * 16 ..][0..16].*;
                    const left: @Vector(16, u8) = plain[block * 16 ..][0..16].*;
                    const right: @Vector(16, u8) = before;
                    plain[block * 16 ..][0..16].* = left ^ right;
                }
                if (self.has_pending) {
                    @memcpy(output[used..][0..16], &self.pending);
                    used += 16;
                }
                @memcpy(output[used..][0..48], plain[0..48]);
                used += 48;
                self.pending = plain[48..64].*;
                self.has_pending = true;
                self.previous = cipher[48..64].*;
                offset += 64;
            } else {
                const count = @min(16 - self.carry_len, bytes.len - offset);
                @memcpy(self.carry[self.carry_len..][0..count], bytes[offset..][0..count]);
                self.carry_len += count;
                offset += count;
                if (self.carry_len != 16) continue;
                var plain: [16]u8 = undefined;
                defer std.crypto.secureZero(u8, &plain);
                self.ctx.decrypt(&plain, &self.carry);
                if (self.has_pending) {
                    @memcpy(output[used..][0..16], &self.pending);
                    used += 16;
                }
                const left: @Vector(16, u8) = plain;
                const right: @Vector(16, u8) = self.previous;
                self.pending = left ^ right;
                self.previous = self.carry;
                self.has_pending = true;
                self.carry_len = 0;
            }
        }
        if (used != 0) try write(context, output[0..used]);
        try self.control.check();
    }
    pub fn finish(self: *Stream, context: anytype, comptime write: anytype) !void {
        if (self.ended or self.active) return error.InvalidHlsCipherState;
        self.active = true;
        defer self.deinit();
        try self.control.check();
        if (self.carry_len != 0 or !self.has_pending) return error.InvalidHlsCiphertext;
        const padding: usize = self.pending[15];
        if (padding == 0 or padding > 16) return error.InvalidHlsCiphertext;
        var invalid: u8 = 0;
        for (self.pending[16 - padding ..]) |byte| invalid |= byte ^ @as(u8, @intCast(padding));
        if (invalid != 0) return error.InvalidHlsCiphertext;
        if (padding != 16) try write(context, self.pending[0 .. 16 - padding]);
        try self.control.check();
    }
};

test "HLS streaming CBC matches independent OpenSSL vectors for every chunk boundary" {
    const a = std.testing.allocator;
    const fixture = @embedFile("../testdata/hls/aes/vectors.bin");
    const Capture = struct {
        bytes: std.ArrayList(u8) = .empty,
        fn write(self: *@This(), bytes: []const u8) !void {
            try self.bytes.appendSlice(std.testing.allocator, bytes);
        }
    };
    var key: [16]u8 = undefined;
    var iv: [16]u8 = undefined;
    for (0..16) |i| {
        key[i] = @intCast(i);
        iv[i] = @intCast(15 - i);
    }
    var cursor: usize = 0;
    while (cursor < fixture.len) {
        const length = std.mem.readInt(u32, fixture[cursor..][0..4], .little);
        const cipher_length = std.mem.readInt(u32, fixture[cursor + 4 ..][0..4], .little);
        cursor += 8;
        const cipher = fixture[cursor..][0..cipher_length];
        for (1..66) |chunk| {
            var stream = Stream.init(&key, iv, .{});
            defer stream.deinit();
            var capture = Capture{};
            defer capture.bytes.deinit(a);
            var offset: usize = 0;
            while (offset < cipher.len) {
                const end = @min(cipher.len, offset + chunk);
                try stream.push(cipher[offset..end], &capture, Capture.write);
                offset = end;
            }
            try std.testing.expect(capture.bytes.items.len <= cipher.len - 16);
            try stream.finish(&capture, Capture.write);
            try std.testing.expectEqual(@as(usize, length), capture.bytes.items.len);
            for (capture.bytes.items, 0..) |value, i| try std.testing.expectEqual(@as(u8, @intCast((i * 37 + 11) % 256)), value);
            try std.testing.expectError(error.InvalidHlsCipherState, stream.push("", &capture, Capture.write));
        }
        cursor += cipher_length;
    }
}
test "HLS streaming CBC terminalizes cancellation consumer failure truncated blocks and invalid padding" {
    const fixture = @embedFile("../testdata/hls/aes/media10.bin");
    var key: [16]u8 = undefined;
    for (0..16) |i| key[i] = @intCast(i);
    var iv: [16]u8 = @splat(0);
    iv[15] = 10;
    const Gate = struct {
        cancel: bool = false,
        fail: bool = false,
        bytes: usize = 0,
        fn check(raw: ?*const anyopaque) !void {
            if (@as(*const @This(), @ptrCast(@alignCast(raw.?))).cancel) return error.Cancelled;
        }
        fn write(self: *@This(), bytes: []const u8) !void {
            if (self.fail) return error.ConsumerFailure;
            self.bytes += bytes.len;
        }
    };
    var gate = Gate{};
    var stream = Stream.init(&key, iv, .{ .context = &gate, .check_fn = Gate.check });
    defer stream.deinit();
    try stream.push(fixture[0..32], &gate, Gate.write);
    gate.cancel = true;
    try std.testing.expectError(error.Cancelled, stream.push(fixture[32..], &gate, Gate.write));
    try std.testing.expect(stream.ended);
    gate.cancel = false;
    gate.fail = true;
    stream = Stream.init(&key, iv, .{});
    try std.testing.expectError(error.ConsumerFailure, stream.push(fixture, &gate, Gate.write));
    try std.testing.expect(stream.ended);
    gate.fail = false;
    stream = Stream.init(&key, iv, .{});
    try stream.push(fixture[0 .. fixture.len - 1], &gate, Gate.write);
    try std.testing.expectError(error.InvalidHlsCiphertext, stream.finish(&gate, Gate.write));
    const damaged = try std.testing.allocator.dupe(u8, fixture);
    defer std.testing.allocator.free(damaged);
    damaged[damaged.len - 17] ^= 128;
    stream = Stream.init(&key, iv, .{});
    try stream.push(damaged, &gate, Gate.write);
    try std.testing.expectError(error.InvalidHlsCiphertext, stream.finish(&gate, Gate.write));
}

test "HLS AES CBC matches independent OpenSSL vectors across block SIMD and padding boundaries" {
    const a = std.testing.allocator;
    const fixture = @embedFile("../testdata/hls/aes/vectors.bin");
    var cursor: usize = 0;
    var count: usize = 0;
    var key: [16]u8 = undefined;
    var iv: [16]u8 = undefined;
    for (0..16) |i| {
        key[i] = @intCast(i);
        iv[i] = @intCast(15 - i);
    }
    while (cursor < fixture.len) {
        const length = std.mem.readInt(u32, fixture[cursor..][0..4], .little);
        const cipher_length = std.mem.readInt(u32, fixture[cursor + 4 ..][0..4], .little);
        cursor += 8;
        const bytes = try a.dupe(u8, fixture[cursor..][0..cipher_length]);
        defer a.free(bytes);
        const plain = try decrypt(bytes, &key, iv, .{});
        try std.testing.expectEqual(@as(usize, length), plain.len);
        for (plain, 0..) |value, i| try std.testing.expectEqual(@as(u8, @intCast((i * 37 + 11) % 256)), value);
        cursor += cipher_length;
        count += 1;
    }
    try std.testing.expectEqual(@as(usize, 11), count);
}
test "HLS AES malformed ciphertext padding and mid-resource cancellation never succeed" {
    var key: [16]u8 = @splat(0);
    const iv: [16]u8 = @splat(0);
    var short: [15]u8 = @splat(0);
    try std.testing.expectError(error.InvalidHlsCiphertext, decrypt(&short, &key, iv, .{}));
    try std.testing.expectError(error.InvalidHlsCiphertext, decrypt(short[0..0], &key, iv, .{}));
    const fixture = @embedFile("../testdata/hls/aes/media10.bin");
    var damaged = try std.testing.allocator.dupe(u8, fixture);
    defer std.testing.allocator.free(damaged);
    for (0..16) |i| key[i] = @intCast(i);
    var expected_iv: [16]u8 = @splat(0);
    expected_iv[15] = 10;
    // Flip the previous cipher block to deterministically invalidate PKCS7.
    damaged[damaged.len - 17] ^= 128;
    try std.testing.expectError(error.InvalidHlsCiphertext, decrypt(damaged, &key, expected_iv, .{}));
    const Cancel = struct {
        calls: usize = 0,
        fn check(raw: ?*const anyopaque) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(@constCast(raw.?)));
            self.calls += 1;
            if (self.calls == 3) return error.Cancelled;
        }
    };
    var cancel = Cancel{};
    @memcpy(damaged, fixture);
    try std.testing.expectError(error.Cancelled, decrypt(damaged, &key, expected_iv, .{ .context = &cancel, .check_fn = Cancel.check }));
    try std.testing.expectEqual(@as(usize, 3), cancel.calls);
}
test "HLS AES identity declaration parses key format versions and big endian IV" {
    const declaration = try parseKey("METHOD=AES-128,URI=\"key.bin\",KEYFORMAT=\"identity\",KEYFORMATVERSIONS=\"1/2\",IV=0XABC", 4096);
    try std.testing.expectEqual(@as(u64, 5), declaration.min_version);
    const key = declaration.key.?;
    try std.testing.expectEqualStrings("key.bin", key.uri);
    try std.testing.expectEqual(@as(u8, 10), key.iv.?[14]);
    try std.testing.expectEqual(@as(u8, 188), key.iv.?[15]);
    const implicit = (try parseKey("METHOD=AES-128,URI=\"key\"", 4096)).key.?;
    const sequence_iv = implicit.mediaIv(0x0102030405060708);
    try std.testing.expectEqual(@as(u64, 0), std.mem.readInt(u64, sequence_iv[0..8], .big));
    try std.testing.expectEqual(@as(u64, 0x0102030405060708), std.mem.readInt(u64, sequence_iv[8..16], .big));
    try std.testing.expect((try parseKey("METHOD=NONE", 4096)).key == null);
    inline for (.{ "METHOD=NONE,URI=\"key\"", "METHOD=AES-128", "METHOD=AES-128,URI=\"key\",IV=\"0x1\"", "METHOD=AES-128,URI=\"key\",IV=0x", "METHOD=AES-128,URI=\"key\",IV=0xZ", "METHOD=AES-128,URI=\"key\",IV=0x100000000000000000000000000000000", "METHOD=AES-128,URI=\"key\",KEYFORMATVERSIONS=\"0\"" }) |text| try std.testing.expectError(error.MalformedHls, parseKey(text, 4096));
    try std.testing.expectError(error.UnsupportedEncryptedMedia, parseKey("METHOD=SAMPLE-AES,URI=\"key\"", 4096));
    try std.testing.expect(!(try parseKey("METHOD=AES-128,URI=\"key\",KEYFORMAT=\"drm\"", 4096)).supported);
    try std.testing.expectError(error.UnsupportedEncryptedMedia, parseKey("METHOD=AES-128,URI=\"key\",KEYFORMATVERSIONS=\"2\"", 4096));
}

fn cipherSeed() []const u8 {
    return comptime blk: {
        const cipher = @embedFile("../testdata/hls/aes/media10.bin");
        var encoded: [4 + cipher.len]u8 = undefined;
        std.mem.writeInt(u32, encoded[0..4], cipher.len, .little);
        @memcpy(encoded[4..], cipher);
        const frozen = encoded;
        break :blk &frozen;
    };
}
test "HLS AES fuzz CBC padding and bounded native decryption" {
    const Harness = struct {
        fn fuzz(_: void, smith: *std.testing.Smith) !void {
            var bytes: [16 * 1024]u8 = undefined;
            const length = smith.sliceWithHash(&bytes, 0x484c5342);
            var key: [16]u8 = undefined;
            for (0..16) |i| key[i] = @intCast(i);
            var iv: [16]u8 = @splat(0);
            iv[15] = 10;
            const plain = decrypt(bytes[0..length], &key, iv, .{}) catch return;
            try std.testing.expect(plain.len < length and length - plain.len <= 16);
        }
    };
    try std.testing.fuzz({}, Harness.fuzz, .{ .corpus = &.{cipherSeed()} });
}
test "HLS AES fuzz cipher seed reaches independent plaintext through Smith" {
    const seed = cipherSeed();
    var smith = std.testing.Smith{ .in = seed };
    var bytes: [16 * 1024]u8 = undefined;
    const length = smith.sliceWithHash(&bytes, 0x484c5342);
    try std.testing.expectEqual(seed.len - 4, length);
    var key: [16]u8 = undefined;
    for (0..16) |i| key[i] = @intCast(i);
    var iv: [16]u8 = @splat(0);
    iv[15] = 10;
    const plain = try decrypt(bytes[0..length], &key, iv, .{});
    const source_bytes = @embedFile("../testdata/fragmented.mp4");
    var cursor: usize = 0;
    while (cursor < source_bytes.len) {
        const box = try @import("isobmff.zig").readBox(source_bytes, cursor);
        if (box.typ == @import("isobmff.zig").fourcc("moof")) break;
        cursor = box.end;
    }
    try std.testing.expectEqualSlices(u8, source_bytes[cursor..11206], plain);
}
