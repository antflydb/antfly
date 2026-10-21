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

//! Bounded bridge from native push decoders to the scanner's pull interface.
//! The decoder observes backpressure and cancellation at each output buffer.
const std = @import("std");
const A = std.mem.Allocator;
pub const Options = struct { brotli_dictionary: ?*const @import("antfly_compress").brotli.SharedDictionary = null, dictionary: []const u8 = &.{}, max_window_bytes: usize = 64 * 1024 * 1024 };
pub const Decode = *const fn (A, *std.Io.Reader, *std.Io.Writer, usize, Options) anyerror!void;
pub const Stream = struct {
    a: A,
    io: std.Io,
    input: *std.Io.Reader,
    decode: Decode,
    limit: usize,
    options: Options,
    reader: std.Io.Reader,
    ring: [256 * 1024]u8 = undefined,
    head: usize = 0,
    count: usize = 0,
    done: bool = false,
    canceled: bool = false,
    mutex: std.Io.Mutex = .init,
    readable: std.Io.Condition = .init,
    writable: std.Io.Condition = .init,
    err: ?anyerror = null,
    thread: std.Thread,
    pub fn start(a: A, io: std.Io, input: *std.Io.Reader, decode: Decode, buffer: []u8, limit: usize, options: Options) !*Stream {
        const self = try a.create(Stream);
        errdefer a.destroy(self);
        self.* = .{ .a = a, .io = io, .input = input, .decode = decode, .limit = limit, .options = options, .thread = undefined, .reader = .{ .vtable = &.{ .stream = pull }, .buffer = buffer, .seek = 0, .end = 0 } };
        self.thread = try std.Thread.spawn(.{}, produce, .{self});
        return self;
    }
    pub fn deinit(self: *Stream) void {
        self.mutex.lockUncancelable(self.io);
        self.canceled = true;
        self.writable.broadcast(self.io);
        self.mutex.unlock(self.io);
        self.thread.join();
        self.a.destroy(self);
    }
    const Sink = struct {
        owner: *Stream,
        writer: std.Io.Writer,
        fn drain(w: *std.Io.Writer, slices: []const []const u8, splat: usize) std.Io.Writer.Error!usize {
            const self: *Sink = @alignCast(@fieldParentPtr("writer", w));
            try self.owner.push(w.buffer[0..w.end]);
            w.end = 0;
            var total: usize = 0;
            for (slices[0 .. slices.len - 1]) |bytes| {
                try self.owner.push(bytes);
                total += bytes.len;
            }
            for (0..splat) |_| {
                const bytes = slices[slices.len - 1];
                try self.owner.push(bytes);
                total += bytes.len;
            }
            return total;
        }
    };
    fn produce(self: *Stream) void {
        var buffer: [65536]u8 = undefined;
        var sink = Sink{ .owner = self, .writer = .{ .vtable = &.{ .drain = Sink.drain }, .buffer = &buffer } };
        var failure: ?anyerror = null;
        self.decode(self.a, self.input, &sink.writer, self.limit, self.options) catch |err| {
            failure = err;
        };
        sink.writer.flush() catch |err| {
            if (failure == null) failure = err;
        };
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        self.err = failure;
        self.done = true;
        self.readable.broadcast(self.io);
    }
    fn push(self: *Stream, bytes: []const u8) std.Io.Writer.Error!void {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        var offset: usize = 0;
        while (offset < bytes.len) {
            while (self.count == self.ring.len and !self.canceled) self.writable.waitUncancelable(self.io, &self.mutex);
            if (self.canceled) return error.WriteFailed;
            const tail = (self.head + self.count) % self.ring.len;
            const n = @min(bytes.len - offset, @min(self.ring.len - self.count, self.ring.len - tail));
            @memcpy(self.ring[tail..][0..n], bytes[offset..][0..n]);
            self.count += n;
            offset += n;
            self.readable.signal(self.io);
        }
    }
    fn pull(r: *std.Io.Reader, w: *std.Io.Writer, limit: std.Io.Limit) std.Io.Reader.StreamError!usize {
        const self: *Stream = @alignCast(@fieldParentPtr("reader", r));
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        while (self.count == 0 and !self.done) self.readable.waitUncancelable(self.io, &self.mutex);
        if (self.count == 0) return if (self.err != null) error.ReadFailed else error.EndOfStream;
        const n = limit.minInt(@min(self.count, self.ring.len - self.head));
        try w.writeAll(self.ring[self.head..][0..n]);
        self.head = (self.head + n) % self.ring.len;
        self.count -= n;
        self.writable.signal(self.io);
        return n;
    }
};
pub fn bzip2(a: A, input: *std.Io.Reader, output: *std.Io.Writer, limit: usize, _: Options) !void {
    return @import("antfly_compress").bzip2.decodeTo(a, input, output, limit);
}

pub fn lzw(a: A, input: *std.Io.Reader, output: *std.Io.Writer, limit: usize, _: Options) !void {
    return @import("antfly_compress").lzw.decodeTo(a, input, output, limit);
}

pub fn brotli(a: A, input: *std.Io.Reader, output: *std.Io.Writer, limit: usize, options: Options) !void {
    return @import("antfly_compress").brotli.decodeToOptions(a, input, output, limit, .{ .raw_dictionary = options.dictionary, .prepared_dictionary = options.brotli_dictionary, .max_window_bytes = options.max_window_bytes });
}

pub fn lz4(a: A, input: *std.Io.Reader, output: *std.Io.Writer, limit: usize, options: Options) !void {
    return @import("antfly_compress").lz4.decodeToOptions(a, input, output, limit, .{ .dictionary = options.dictionary });
}

pub fn lzma(a: A, input: *std.Io.Reader, output: *std.Io.Writer, limit: usize, options: Options) !void {
    return @import("antfly_compress").lzma.standaloneTo(a, input, output, limit, options.max_window_bytes);
}
pub fn lzip(a: A, input: *std.Io.Reader, output: *std.Io.Writer, limit: usize, options: Options) !void {
    return @import("antfly_compress").lzma.lzipTo(a, input, output, limit, options.max_window_bytes);
}
pub fn xz(a: A, input: *std.Io.Reader, output: *std.Io.Writer, limit: usize, options: Options) !void {
    return @import("antfly_compress").xz.decodeTo(a, input, output, limit, options.max_window_bytes);
}
