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

//! Frame inline-image data before resuming content operators. Never search
//! binary pixels for an EI-looking token: raw rows have an exact length and
//! supported outer filters have a codec end marker. Unframeable encodings fail
//! closed so pixel bytes cannot become text, resources, or marked content.
const std = @import("std");
const syntax = @import("pdf_syntax");

fn space(byte: u8) bool {
    return byte == 0 or byte == 9 or byte == 10 or byte == 12 or byte == 13 or byte == 32;
}
fn boundary(byte: u8) bool {
    return space(byte) or std.mem.indexOfScalar(u8, "()<>[]{}/%", byte) != null;
}
fn named(name: []const u8, short: []const u8, full: []const u8) bool {
    return std.mem.eql(u8, name, short) or std.mem.eql(u8, name, full);
}
fn componentCount(value: syntax.Object) ?usize {
    if (value == .name) {
        if (named(value.name, "G", "DeviceGray")) return 1;
        if (named(value.name, "RGB", "DeviceRGB")) return 3;
        if (named(value.name, "CMYK", "DeviceCMYK")) return 4;
    } else if (value == .array and value.array.len > 0) {
        const name = value.array[0].asName() orelse return null;
        if (named(name, "I", "Indexed")) return 1;
    }
    return null;
}
fn first(value: *const syntax.Object) ?*const syntax.Object {
    if (value.* == .array) return if (value.array.len > 0) &value.array[0] else null;
    return value;
}

/// Called after BI. Only the small header is parsed; raw samples, JPEG, LZW,
/// and byte-oriented filter bodies are skipped without allocating pixels.
/// Flate framing uses a fixed history window and bounded, cancellable discard.
pub fn skip(scanner: *syntax.Scanner, cancellation: anytype, max_flate_bytes: usize) !void {
    try cancellation.check();
    var width: ?usize = null;
    var height: ?usize = null;
    var bits: ?usize = null;
    var components: ?usize = null;
    var image_mask = false;
    var filter: ?syntax.Object = null;
    defer if (filter) |*object| object.deinit(scanner.alloc);
    var parameters: ?syntax.Object = null;
    defer if (parameters) |*object| object.deinit(scanner.alloc);
    var header_entries: usize = 0;
    while (true) {
        try cancellation.check();
        var key = try scanner.readLexeme();
        defer syntax.Scanner.freeLexeme(scanner.alloc, &key);
        if (key == .keyword and std.mem.eql(u8, key.keyword, "ID")) break;
        if (key != .name or header_entries >= 64) return error.InvalidPdfInlineImage;
        header_entries += 1;
        var value = try scanner.readObject();
        defer value.deinit(scanner.alloc);
        if (named(key.name, "W", "Width")) {
            const number = value.asInteger() orelse return error.InvalidPdfInlineImage;
            if (number <= 0) return error.InvalidPdfInlineImage;
            width = std.math.cast(usize, number) orelse return error.InvalidPdfInlineImage;
        } else if (named(key.name, "H", "Height")) {
            const number = value.asInteger() orelse return error.InvalidPdfInlineImage;
            if (number <= 0) return error.InvalidPdfInlineImage;
            height = std.math.cast(usize, number) orelse return error.InvalidPdfInlineImage;
        } else if (named(key.name, "BPC", "BitsPerComponent")) {
            const number = value.asInteger() orelse return error.InvalidPdfInlineImage;
            if (number != 1 and number != 2 and number != 4 and number != 8 and number != 16) return error.InvalidPdfInlineImage;
            bits = @intCast(number);
        } else if (named(key.name, "CS", "ColorSpace")) {
            components = componentCount(value);
        } else if (named(key.name, "IM", "ImageMask")) {
            if (value != .boolean) return error.InvalidPdfInlineImage;
            image_mask = value.boolean;
        } else if (named(key.name, "F", "Filter")) {
            if (filter) |*old| old.deinit(scanner.alloc);
            filter = value;
            value = .null;
        } else if (named(key.name, "DP", "DecodeParms")) {
            if (parameters) |*old| old.deinit(scanner.alloc);
            parameters = value;
            value = .null;
        }
    }
    if (width == null or height == null or (image_mask and bits != null and bits.? != 1)) return error.InvalidPdfInlineImage;
    // ID consumes one whitespace separator, with CRLF treated as one EOL.
    // Remaining whitespace is image data, including whitespace-valued pixels.
    if (scanner.pos >= scanner.bytes.len or !space(scanner.bytes[scanner.pos])) return error.InvalidPdfInlineImage;
    const separator = scanner.bytes[scanner.pos];
    scanner.pos += 1;
    if (separator == 13 and scanner.pos < scanner.bytes.len and scanner.bytes[scanner.pos] == 10) scanner.pos += 1;
    const data = scanner.bytes[scanner.pos..];
    const outer = if (filter) |*object| first(object) else null;
    const length = if (outer) |object| blk: {
        const name = object.asName() orelse return error.UnsupportedPdfInlineImage;
        if (named(name, "AHx", "ASCIIHexDecode")) break :blk try terminated(data, ">", cancellation);
        if (named(name, "A85", "ASCII85Decode")) break :blk try terminated(data, "~>", cancellation);
        if (named(name, "RL", "RunLengthDecode")) break :blk try runLengthEnd(data, cancellation);
        if (named(name, "DCT", "DCTDecode")) break :blk try jpegEnd(data, cancellation);
        if (named(name, "LZW", "LZWDecode")) {
            var early_change: usize = 1;
            if (parameters) |*params| if (first(params)) |param| if (param.get("EarlyChange")) |entry| {
                const number = entry.asInteger() orelse return error.InvalidPdfInlineImage;
                if (number != 0 and number != 1) return error.InvalidPdfInlineImage;
                early_change = @intCast(number);
            };
            break :blk try lzwEnd(data, early_change, cancellation);
        }
        if (named(name, "Fl", "FlateDecode")) break :blk try flateEnd(data, max_flate_bytes, cancellation);
        return error.UnsupportedPdfInlineImage;
    } else blk: {
        const channels = if (image_mask) 1 else components orelse return error.UnsupportedPdfInlineImage;
        const sample_bits = if (image_mask) 1 else bits orelse return error.InvalidPdfInlineImage;
        const samples = std.math.mul(usize, width.?, channels) catch return error.InvalidPdfInlineImage;
        const row_bits = std.math.mul(usize, samples, sample_bits) catch return error.InvalidPdfInlineImage;
        const padded = std.math.add(usize, row_bits, 7) catch return error.InvalidPdfInlineImage;
        break :blk std.math.mul(usize, padded / 8, height.?) catch return error.InvalidPdfInlineImage;
    };
    if (length > data.len) return error.InvalidPdfInlineImage;
    scanner.pos += length;
    var end = scanner.pos;
    while (end < scanner.bytes.len and space(scanner.bytes[end])) : (end += 1) {
        if ((end - scanner.pos) % 4096 == 0) try cancellation.check();
    }
    if (end == scanner.pos and (length == 0 or !space(data[length - 1]))) return error.InvalidPdfInlineImage;
    if (end + 2 > scanner.bytes.len or !std.mem.eql(u8, scanner.bytes[end..][0..2], "EI") or
        (end + 2 < scanner.bytes.len and !boundary(scanner.bytes[end + 2]))) return error.InvalidPdfInlineImage;
    scanner.pos = end + 2;
}

fn terminated(data: []const u8, marker: []const u8, cancellation: anytype) !usize {
    var index: usize = 0;
    while (index + marker.len <= data.len) : (index += 1) {
        if (index % 4096 == 0) try cancellation.check();
        if (std.mem.eql(u8, data[index..][0..marker.len], marker)) return index + marker.len;
    }
    return error.InvalidPdfInlineImage;
}
fn runLengthEnd(data: []const u8, cancellation: anytype) !usize {
    var pos: usize = 0;
    var packets: usize = 0;
    while (pos < data.len) {
        if (packets % 1024 == 0) try cancellation.check();
        packets += 1;
        const control = data[pos];
        pos += 1;
        if (control == 128) return pos;
        const bytes: usize = if (control < 128) @as(usize, control) + 1 else 1;
        if (bytes > data.len - pos) return error.InvalidPdfInlineImage;
        pos += bytes;
    }
    return error.InvalidPdfInlineImage;
}
fn jpegEnd(data: []const u8, cancellation: anytype) !usize {
    if (data.len < 2 or data[0] != 0xff or data[1] != 0xd8) return error.InvalidPdfInlineImage;
    var pos: usize = 2;
    var work: usize = 0;
    while (pos < data.len) {
        if (work % 1024 == 0) try cancellation.check();
        work +|= 1;
        if (data[pos] != 0xff) {
            pos += 1;
            continue;
        }
        pos += 1;
        while (pos < data.len and data[pos] == 0xff) : (pos += 1) {
            if (pos % 4096 == 0) try cancellation.check();
        }
        if (pos >= data.len) return error.InvalidPdfInlineImage;
        const marker = data[pos];
        pos += 1;
        if (marker == 0xd9) return pos;
        if (marker == 0 or marker == 1 or (marker >= 0xd0 and marker <= 0xd7)) continue;
        if (pos + 2 > data.len) return error.InvalidPdfInlineImage;
        const length: usize = std.mem.readInt(u16, data[pos..][0..2], .big);
        if (length < 2 or length > data.len - pos) return error.InvalidPdfInlineImage;
        pos += length;
    }
    return error.InvalidPdfInlineImage;
}
fn lzwEnd(data: []const u8, early_change: usize, cancellation: anytype) !usize {
    var bit: usize = 0;
    var width: u4 = 9;
    var next: usize = 258;
    var previous = false;
    var count: usize = 0;
    while (bit / 8 < data.len) {
        if (count % 1024 == 0) try cancellation.check();
        count += 1;
        var code: usize = 0;
        for (0..width) |_| {
            if (bit / 8 >= data.len) return error.InvalidPdfInlineImage;
            code = (code << 1) | ((data[bit / 8] >> @as(u3, @intCast(7 - bit % 8))) & 1);
            bit += 1;
        }
        if (code == 257) return (bit + 7) / 8;
        if (code == 256) {
            width = 9;
            next = 258;
            previous = false;
            continue;
        }
        if (code > next or (!previous and code >= 256)) return error.InvalidPdfInlineImage;
        if (previous and next < 4096) {
            next += 1;
            if (width < 12 and next + early_change >= (@as(usize, 1) << width)) width += 1;
        }
        previous = true;
    }
    return error.InvalidPdfInlineImage;
}
// Poll encoded input too: a stream of empty Deflate blocks can do substantial
// work without producing an output byte for the outer discard loop to poll.
fn FramingInput(comptime Control: type) type {
    return struct {
        reader: std.Io.Reader,
        data: []const u8,
        supplied: usize = 0,
        cancellation: Control,
        failure: ?anyerror = null,

        fn stream(r: *std.Io.Reader, _: *std.Io.Writer, _: std.Io.Limit) std.Io.Reader.StreamError!usize {
            const self: *@This() = @alignCast(@fieldParentPtr("reader", r));
            self.cancellation.check() catch |err| {
                self.failure = err;
                return error.ReadFailed;
            };
            if (self.supplied == self.data.len) return error.EndOfStream;
            const remaining = r.end - r.seek;
            @memmove(r.buffer[0..remaining], r.buffer[r.seek..r.end]);
            const count = @min(r.buffer.len - remaining, self.data.len - self.supplied);
            @memcpy(r.buffer[remaining..][0..count], self.data[self.supplied..][0..count]);
            self.supplied += count;
            r.seek = 0;
            r.end = remaining + count;
            return 0;
        }
    };
}

fn flateEnd(data: []const u8, maximum: usize, cancellation: anytype) !usize {
    var encoded_buffer: [8192]u8 = undefined;
    var input = FramingInput(@TypeOf(cancellation)){
        .reader = .{ .vtable = &.{ .stream = FramingInput(@TypeOf(cancellation)).stream }, .buffer = &encoded_buffer, .seek = 0, .end = 0 },
        .data = data,
        .cancellation = cancellation,
    };
    var history: [std.compress.flate.max_window_len]u8 = undefined;
    var decoder: std.compress.flate.Decompress = .init(&input.reader, .zlib, &history);
    var buffer: [8192]u8 = undefined;
    var total: usize = 0;
    while (true) {
        try cancellation.check();
        const count = decoder.reader.readSliceShort(&buffer) catch {
            if (input.failure) |err| return err;
            return error.InvalidPdfInlineImage;
        };
        if (count == 0) break;
        if (count > maximum -| total) return error.DecodedStreamTooLarge;
        total += count;
    }
    return input.supplied - (input.reader.end - input.reader.seek);
}

const NoCancellation = struct {
    pub fn check(_: @This()) !void {}
};
fn expectFrame(alloc: std.mem.Allocator, header: []const u8, data: []const u8) !void {
    const content = try std.fmt.allocPrint(alloc, "{s} ID\r\n{s} EI EMC", .{ header, data });
    defer alloc.free(content);
    var scanner = syntax.Scanner.init(alloc, content);
    defer scanner.deinit();
    try skip(&scanner, NoCancellation{}, 1024 * 1024);
    var token = try scanner.readLexeme();
    defer syntax.Scanner.freeLexeme(alloc, &token);
    try std.testing.expect(token == .keyword);
    try std.testing.expectEqualStrings("EMC", token.keyword);
}

test "inline image framing treats raw rows and filter data as opaque" {
    const a = std.testing.allocator;
    const pixels = " EI EMC BT /Fake 10 Tf (PIXELS) Tj ET /Ghost Do ";
    const raw_header = try std.fmt.allocPrint(a, "/W {d} /H 1 /BPC 8 /CS /G", .{pixels.len});
    defer a.free(raw_header);
    try expectFrame(a, raw_header, pixels);
    try expectFrame(a, "/Width 3 /Height 2 /ImageMask true", "\r\n"); // Row padding, whitespace pixels.
    try expectFrame(a, "/W 1 /H 1 /F [/A85 /Fl]", " EI EMC BT ~>");
    try expectFrame(a, "/W 1 /H 1 /F /AHx", "454920454d43>");
    const rl = try std.fmt.allocPrint(a, "{c}{s}{c}", .{ @as(u8, pixels.len - 1), pixels, @as(u8, 128) });
    defer a.free(rl);
    try expectFrame(a, "/W 1 /H 1 /Filter /RunLengthDecode", rl);
    try expectFrame(a, "/W 10 /H 1 /F /LZW", &.{ 0x80, 0x0b, 0x60, 0x50, 0x22, 0x0c, 0x0c, 0x85, 0x01 });
    // APP payload can contain both fake EI and an EOI-looking byte pair.
    try expectFrame(a, "/W 1 /H 1 /F /DCT", "\xff\xd8\xff\xe1\x00\x09 EI \xff\xd9!\xff\xda\x00\x02x\xff\x00\xd9\xff\xd9");
}

test "inline Flate framing bounds decoded work and does not consume later operators" {
    const a = std.testing.allocator;
    var output = try std.Io.Writer.Allocating.initCapacity(a, 65536);
    defer output.deinit();
    var history: [std.compress.flate.max_window_len]u8 = undefined;
    var compressor = try std.compress.flate.Compress.init(&output.writer, &history, .zlib, .default);
    const pixels = " EI EMC BT (PIXELS) Tj ET";
    try compressor.writer.writeAll(pixels);
    try compressor.finish();
    try expectFrame(a, "/W 1 /H 1 /F /Fl", output.written());
    const content = try std.fmt.allocPrint(a, "/W 1 /H 1 /F /Fl ID {s} EI", .{output.written()});
    defer a.free(content);
    var scanner = syntax.Scanner.init(a, content);
    defer scanner.deinit();
    try std.testing.expectError(error.DecodedStreamTooLarge, skip(&scanner, NoCancellation{}, pixels.len - 1));
}

test "inline image framing rejects ambiguous unknown and truncated encodings" {
    const a = std.testing.allocator;
    for ([_][]const u8{
        "/W 4 /H 1 /BPC 8 /CS /G ID x EI", // EI bytes are part of a truncated row, not a terminator.
        "/W 1 /H 1 /F /AHx ID 00 EI",
        "/W 1 /H 1 /F /RL ID \x05abc EI",
        "/W 1 /H 1 /F /LZW ID \x80 EI",
        "/W 1 /H 1 /F /DCT ID \xff\xd8 EI",
        "/W 1 /H 1 /BPC 8 /CS /G ID x EIevil",
    }) |content| {
        var scanner = syntax.Scanner.init(a, content);
        defer scanner.deinit();
        try std.testing.expectError(error.InvalidPdfInlineImage, skip(&scanner, NoCancellation{}, 1024));
    }
    for ([_][]const u8{
        "/W 1 /H 1 /BPC 8 /CS /Unknown ID EMC BT (PIXELS) Tj ET EI",
        "/W 1 /H 1 /F /Unknown ID EMC BT (PIXELS) Tj ET EI",
        "/W 1 /H 1 /F /CCF /DP << /EndOfBlock false >> ID EMC EI",
    }) |content| {
        var scanner = syntax.Scanner.init(a, content);
        defer scanner.deinit();
        try std.testing.expectError(error.UnsupportedPdfInlineImage, skip(&scanner, NoCancellation{}, 1024));
    }
}

test "inline image headers own allocations and framing remains cancellable" {
    const Check = struct {
        fn run(a: std.mem.Allocator) !void {
            // Make allocation counts independent of whether the backing heap
            // can grow a parser buffer in place on this particular attempt.
            var moving = std.testing.FailingAllocator.init(a, .{ .resize_fail_index = 0 });
            try expectFrame(moving.allocator(), "/W 1 /H 1 /F [/A85 /Fl] /DP [null << /Predictor 12 >>]", "z~>");
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Check.run, .{});
    const Cancel = struct {
        calls: *usize,
        pub fn check(self: @This()) !void {
            self.calls.* += 1;
            if (self.calls.* >= 10) return error.Canceled;
        }
    };
    const zeros: [65536]u8 = @splat('0');
    const content = "/W 1 /H 1 /F /AHx ID " ++ zeros ++ "> EI";
    var scanner = syntax.Scanner.init(std.testing.allocator, content);
    defer scanner.deinit();
    var calls: usize = 0;
    try std.testing.expectError(error.Canceled, skip(&scanner, Cancel{ .calls = &calls }, 1024));
}

test "inline Flate cancellation polls encoded blocks even without decoded output" {
    const a = std.testing.allocator;
    const block_count = 4096;
    const frame_len = 2 + block_count * 5 + 5 + 4;
    const bytes = try a.alloc(u8, frame_len + " EI BT".len);
    defer a.free(bytes);
    @memcpy(bytes[0..2], &[_]u8{ 0x78, 0x01 });
    for (0..block_count) |index| @memcpy(bytes[2 + index * 5 ..][0..5], &[_]u8{ 0x00, 0x00, 0x00, 0xff, 0xff });
    @memcpy(bytes[2 + block_count * 5 ..][0..9], &[_]u8{ 0x01, 0x00, 0x00, 0xff, 0xff, 0x00, 0x00, 0x00, 0x01 });
    @memcpy(bytes[frame_len..], " EI BT");
    try std.testing.expectEqual(@as(usize, frame_len), try flateEnd(bytes, 0, NoCancellation{}));
    const Cancel = struct {
        calls: *usize,
        pub fn check(self: @This()) !void {
            self.calls.* += 1;
            if (self.calls.* >= 3) return error.Canceled;
        }
    };
    var calls: usize = 0;
    try std.testing.expectError(error.Canceled, flateEnd(bytes, 0, Cancel{ .calls = &calls }));
}
