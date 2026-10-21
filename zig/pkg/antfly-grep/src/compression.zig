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

//! Native monorepo compressed stream adapter.
const std = @import("std");
const A = std.mem.Allocator;
const ZstdDecoder = @import("antfly_compress").ZstdDecoder;
pub const Kind = enum { gzip, xz, zstd, bzip2, lz4, lzma, compress, lzip, brotli };
pub fn kind(path: []const u8) ?Kind {
    const suffixes = .{ .{ ".br", Kind.brotli }, .{ ".gz", Kind.gzip }, .{ ".tgz", Kind.gzip }, .{ ".xz", Kind.xz }, .{ ".txz", Kind.xz }, .{ ".zst", Kind.zstd }, .{ ".zstd", Kind.zstd }, .{ ".bz2", Kind.bzip2 }, .{ ".bz", Kind.bzip2 }, .{ ".tbz2", Kind.bzip2 }, .{ ".lz4", Kind.lz4 }, .{ ".lzma", Kind.lzma }, .{ ".Z", Kind.compress }, .{ ".lz", Kind.lzip } };
    inline for (suffixes) |entry| if (std.mem.endsWith(u8, path, entry[0])) return entry[1];
    return null;
}
pub fn decode(a: A, data: []const u8, format: Kind, limit: usize) ![]u8 {
    return decodeOptions(a, data, format, limit, .{});
}
pub fn decodeOptions(a: A, data: []const u8, format: Kind, limit: usize, options: @import("antfly_compress").brotli.Options) ![]u8 {
    const native = @import("antfly_compress");
    switch (format) {
        .zstd => return native.zstd.decodeOptions(a, data, limit, .{ .dictionary = options.raw_dictionary, .max_window_bytes = options.max_window_bytes }),
        .bzip2 => return native.bzip2.decode(a, data, limit),
        .lz4 => return native.lz4.decodeOptions(a, data, limit, .{ .dictionary = options.raw_dictionary }),
        .compress => return native.lzw.decode(a, data, limit),
        else => {},
    }
    var input = std.Io.Reader.fixed(data);
    if (format == .gzip) {
        var buffer: [65536]u8 = undefined;
        var decoder = Reader.init(a, &input, .gzip, &buffer, limit);
        defer decoder.deinit();
        return decoder.reader.allocRemaining(a, .limited(limit)) catch |err| return decoder.err orelse err;
    }
    var output: std.Io.Writer.Allocating = .init(a);
    errdefer output.deinit();
    switch (format) {
        .brotli => try native.brotli.decodeToOptions(a, &input, &output.writer, limit, options),
        .lzma => try native.lzma.standaloneTo(a, &input, &output.writer, limit, options.max_window_bytes),
        .xz => try native.xz.decodeTo(a, &input, &output.writer, limit, options.max_window_bytes),
        .lzip => try native.lzma.lzipTo(a, &input, &output.writer, limit, options.max_window_bytes),
        else => unreachable,
    }
    return output.toOwnedSlice();
}

// Expose exactly one Zstandard frame to the decoder. Frame boundaries are
// discovered from block lengths without buffering their compressed payloads.
const ZstdFrameReader = struct {
    source: *std.Io.Reader,
    reader: std.Io.Reader,
    phase: enum { header, block, payload, trailer, done } = .header,
    remaining: usize,
    last: bool = false,
    has_checksum: bool,
    expected_checksum: ?u32 = null,
    pending: [4]u8 = undefined,
    pending_len: usize = 0,
    pending_pos: usize = 0,
    err: ?anyerror = null,
    fn pull(self: *ZstdFrameReader, writer: *std.Io.Writer, limit: std.Io.Limit) !usize {
        while (true) {
            if (self.pending_pos < self.pending_len) {
                const n = limit.minInt(self.pending_len - self.pending_pos);
                try writer.writeAll(self.pending[self.pending_pos..][0..n]);
                self.pending_pos += n;
                return n;
            }
            switch (self.phase) {
                .header, .payload => {
                    if (self.remaining == 0) {
                        self.phase = if (self.phase == .header or !self.last) .block else if (self.has_checksum) .trailer else .done;
                        continue;
                    }
                    const n = try self.source.stream(writer, .limited(limit.minInt(self.remaining)));
                    self.remaining -= n;
                    return n;
                },
                .block => {
                    const header = try self.source.takeArray(3);
                    @memcpy(self.pending[0..3], header);
                    self.pending_len = 3;
                    self.pending_pos = 0;
                    const bits = @as(u32, header[0]) | (@as(u32, header[1]) << 8) | (@as(u32, header[2]) << 16);
                    const kind_id = (bits >> 1) & 3;
                    if (kind_id == 3 or bits >> 3 > 128 * 1024) return error.InvalidZstd;
                    self.last = bits & 1 != 0;
                    self.remaining = if (kind_id == 1) 1 else bits >> 3;
                    self.phase = .payload;
                },
                .trailer => {
                    self.pending = (try self.source.takeArray(4)).*;
                    self.pending_pos = 0;
                    self.pending_len = 4;
                    self.expected_checksum = std.mem.readInt(u32, &self.pending, .little);
                    self.phase = .done;
                },
                .done => return error.CleanEnd,
            }
        }
    }
    fn stream(r: *std.Io.Reader, w: *std.Io.Writer, limit: std.Io.Limit) std.Io.Reader.StreamError!usize {
        const self: *ZstdFrameReader = @alignCast(@fieldParentPtr("reader", r));
        return self.pull(w, limit) catch |err| {
            if (err == error.CleanEnd) return error.EndOfStream;
            self.err = if (err == error.EndOfStream) error.TruncatedCompressedStream else err;
            return error.ReadFailed;
        };
    }
};

/// Pull-based decoder for codecs exposed through native std.Io.Reader APIs.
/// Concatenated members share one output stream and retain only codec windows.
pub const Reader = struct {
    a: A,
    source: *std.Io.Reader,
    reader: std.Io.Reader,
    format: Kind,
    active: ?union(enum) {
        gzip: std.compress.flate.Decompress,
        zstd: ZstdDecoder,
    } = null,
    window: []u8 = &.{},
    compressed_window: []u8 = &.{},
    zstd_frame: ?ZstdFrameReader = null,
    xxhash: std.hash.XxHash64 = .init(0),
    dictionary: ?*ZstdDecoder.Dictionary = null,
    dictionary_bytes: []const u8 = &.{},
    max_window_bytes: usize = 64 * 1024 * 1024,
    crc: @import("antfly_hash").Crc32 = .init(),
    member_size: u64 = 0,
    total: usize = 0,
    limit: usize,
    err: ?anyerror = null,
    pub fn supported(format: Kind) bool {
        return format == .gzip or format == .zstd;
    }
    pub fn init(a: A, source: *std.Io.Reader, format: Kind, buffer: []u8, limit: usize) Reader {
        std.debug.assert(supported(format));
        return .{ .a = a, .source = source, .format = format, .limit = limit, .reader = .{ .vtable = &.{ .stream = stream }, .buffer = buffer, .seek = 0, .end = 0 } };
    }
    pub fn deinit(self: *Reader) void {
        self.endMember();
        self.a.free(self.window);
        self.a.free(self.compressed_window);
        if (self.dictionary) |dictionary| dictionary.deinit(self.a);
    }
    fn endMember(self: *Reader) void {
        if (self.active) |*active| switch (active.*) {
            .gzip, .zstd => {},
        };
        self.active = null;
    }
    fn beginMember(self: *Reader) !bool {
        _ = self.source.peekByte() catch |err| switch (err) {
            error.EndOfStream => return false,
            else => return err,
        };
        switch (self.format) {
            .gzip => {
                if (self.window.len == 0) self.window = try self.a.alloc(u8, std.compress.flate.max_window_len);
                self.active = .{ .gzip = .init(self.source, .gzip, self.window) };
            },
            .zstd => {
                const Frame = ZstdDecoder.Frame;
                if (self.dictionary_bytes.len > 0 and self.dictionary == null) self.dictionary = try ZstdDecoder.Dictionary.init(self.a, self.dictionary_bytes);
                while (true) {
                    const magic = std.mem.readInt(u32, try self.source.peekArray(4), .little);
                    if (magic & 0xfffffff0 != 0x184d2a50) break;
                    self.source.toss(4);
                    const length = try self.source.takeInt(u32, .little);
                    try self.source.discardAll(length);
                    _ = self.source.peekByte() catch |err| switch (err) {
                        error.EndOfStream => return false,
                        else => return err,
                    };
                }
                _ = self.source.peek(18) catch |err| switch (err) {
                    error.EndOfStream => &.{},
                    else => return err,
                };
                var preview = std.Io.Reader.fixed(self.source.buffered());
                if (try preview.takeInt(u32, .little) != 0xfd2fb528) return error.InvalidZstd;
                const header = try Frame.Zstandard.Header.decode(&preview);
                if (header.descriptor.unused) return error.InvalidZstd;
                const declared_window = header.windowSize() orelse return error.InvalidZstd;
                if (declared_window > self.max_window_bytes) return error.WindowOversize;
                const dictionary_window = if (self.dictionary) |dictionary| dictionary.content.len else 0;
                const window_len = @max(declared_window, dictionary_window);
                if (window_len > std.math.maxInt(u32)) return error.WindowOversize;
                const capacity: usize = @intCast(@max(1024, window_len) + std.compress.zstd.block_size_max);
                if (self.window.len < capacity) {
                    self.a.free(self.window);
                    self.window = &.{};
                    self.window = try self.a.alloc(u8, capacity);
                }
                if (self.compressed_window.len == 0) self.compressed_window = try self.a.alloc(u8, 256 * 1024);
                self.zstd_frame = .{ .source = self.source, .remaining = preview.seek, .has_checksum = header.descriptor.content_checksum_flag, .reader = .{ .vtable = &.{ .stream = ZstdFrameReader.stream }, .buffer = self.compressed_window, .seek = 0, .end = 0 } };
                self.active = .{ .zstd = .init(&self.zstd_frame.?.reader, self.window, .{ .window_len = @intCast(@max(1024, window_len)), .dictionary = self.dictionary }) };
                self.xxhash = .init(0);
            },
            else => unreachable,
        }
        self.crc = .init();
        self.member_size = 0;
        return true;
    }
    fn decoder(self: *Reader) *std.Io.Reader {
        return switch (self.active.?) {
            inline else => |*d| &d.reader,
        };
    }
    fn decoderError(self: *Reader) ?anyerror {
        return switch (self.active.?) {
            inline else => |*d| d.err,
        };
    }
    fn finishMember(self: *Reader) !void {
        if (self.decoderError()) |err| return if (err == error.EndOfStream) error.TruncatedCompressedStream else err;
        if (self.active.? == .gzip) {
            if (self.active.?.gzip.state != .end) return error.TruncatedCompressedStream;
            const metadata = self.active.?.gzip.container_metadata.gzip;
            if (metadata.crc != self.crc.final() or metadata.count != @as(u32, @truncate(self.member_size))) return error.WrongGzipChecksum;
        }
        if (self.format == .zstd) {
            if (self.zstd_frame.?.err) |err| return err;
            if (self.zstd_frame.?.expected_checksum) |expected| if (expected != @as(u32, @truncate(self.xxhash.final()))) return error.ChecksumFailure;
        }
        self.endMember();
    }
    fn pull(self: *Reader, writer: *std.Io.Writer, limit: std.Io.Limit) !usize {
        while (true) {
            if (self.active == null and !try self.beginMember()) return error.CleanEnd;
            const input = self.decoder();
            _ = input.peekByte() catch |err| switch (err) {
                error.EndOfStream => {
                    try self.finishMember();
                    continue;
                },
                else => return self.decoderError() orelse err,
            };
            const bytes = input.buffered();
            const n = limit.minInt(bytes.len);
            if (n > self.limit - self.total) return error.StreamTooLong;
            try writer.writeAll(bytes[0..n]);
            if (self.format == .gzip) self.crc.update(bytes[0..n]);
            if (self.format == .zstd) self.xxhash.update(bytes[0..n]);
            self.member_size += n;
            self.total += n;
            input.toss(n);
            return n;
        }
    }
    fn stream(r: *std.Io.Reader, w: *std.Io.Writer, limit: std.Io.Limit) std.Io.Reader.StreamError!usize {
        const self: *Reader = @alignCast(@fieldParentPtr("reader", r));
        return self.pull(w, limit) catch |err| {
            if (err == error.CleanEnd) return error.EndOfStream;
            self.err = if (err == error.EndOfStream) error.TruncatedCompressedStream else err;
            return error.ReadFailed;
        };
    }
};

test "truncated compressed headers cannot become clean EOF" {
    var input = std.Io.Reader.fixed("not gzip");
    var buffer: [65536]u8 = undefined;
    var decoder = Reader.init(std.testing.allocator, &input, .gzip, &buffer, 1024);
    defer decoder.deinit();
    try std.testing.expectError(error.ReadFailed, decoder.reader.peekByte());
    try std.testing.expect(decoder.err != null);
}
