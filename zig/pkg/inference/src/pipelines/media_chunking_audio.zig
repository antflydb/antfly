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

//! Window providers retain one PCM window while preserving original sample clocks.
const std = @import("std");
const audio = @import("audio.zig");
const media = @import("antfly_media");
pub const Lease = struct {
    pcm: audio.PcmAudio,
    owned: ?[]f32 = null,
    pub fn deinit(self: *Lease, a: std.mem.Allocator) void {
        if (self.owned) |samples| a.free(samples);
    }
};
pub const Provider = struct {
    context: *anyopaque,
    samples: usize,
    sample_rate: u32,
    read: *const fn (*anyopaque, std.mem.Allocator, usize, usize) anyerror!Lease,
};
pub const Pcm = struct {
    pcm: audio.PcmAudio,
    pub fn provider(self: *@This()) Provider {
        return .{ .context = self, .samples = self.pcm.samples.len, .sample_rate = self.pcm.sample_rate, .read = read };
    }
    fn read(raw: *anyopaque, _: std.mem.Allocator, first: usize, last: usize) !Lease {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (first > last or last > self.pcm.samples.len) return error.InvalidAudioWindow;
        return .{ .pcm = .{ .samples = self.pcm.samples[first..last], .sample_rate = self.pcm.sample_rate } };
    }
};
pub const Wav = struct {
    parsed: audio.wav.ParsedData,
    pub fn provider(self: *@This()) Provider {
        return .{ .context = self, .samples = self.parsed.data.len / self.frameBytes(), .sample_rate = self.parsed.format.sample_rate, .read = read };
    }
    fn frameBytes(self: @This()) usize {
        return @as(usize, self.parsed.format.bits_per_sample / 8) * self.parsed.channels;
    }
    fn read(raw: *anyopaque, a: std.mem.Allocator, first: usize, last: usize) !Lease {
        const self: *@This() = @ptrCast(@alignCast(raw));
        const bytes = self.frameBytes();
        if (first > last or last > self.parsed.data.len / bytes) return error.InvalidAudioWindow;
        var window = self.parsed;
        window.data = self.parsed.data[first * bytes .. last * bytes];
        const decoded = try audio.wav.decodeMonoParsed(a, window);
        return .{ .pcm = .{ .samples = decoded.samples, .sample_rate = decoded.format.sample_rate }, .owned = decoded.samples };
    }
};
pub const SourceWav = struct {
    layout: audio.wav.SourceLayout,
    source: *media.source.Source,
    pub fn provider(self: *@This()) !Provider {
        return .{ .context = self, .samples = std.math.cast(usize, self.layout.frames()) orelse return error.ResourceLimitExceeded, .sample_rate = self.layout.format.sample_rate, .read = read };
    }
    fn read(raw: *anyopaque, a: std.mem.Allocator, first: usize, last: usize) !Lease {
        const self: *@This() = @ptrCast(@alignCast(raw));
        const samples = try self.layout.readMono(a, self.source, first, last);
        return .{ .pcm = .{ .samples = samples, .sample_rate = self.layout.format.sample_rate }, .owned = samples };
    }
};

/// A two-block decode queue overlaps codec work with embedding. The consumer
/// retains only its current window plus overlap; source PCM never accumulates.
pub const Stream = struct {
    a: std.mem.Allocator,
    bytes: []const u8,
    control: @import("../execution_control.zig").InferenceExecutionControl,
    io: std.Io,
    info: audio.streaming.Info,
    storage: [2][]f32 = undefined,
    queue: std.Io.Queue([]f32),
    thread: ?std.Thread = null,
    failure: ?anyerror = null,
    buffer: std.ArrayList(f32) = .empty,
    base: usize = 0,
    received: usize = 0,
    pub fn start(a: std.mem.Allocator, bytes: []const u8, control: @import("../execution_control.zig").InferenceExecutionControl) !?*Stream {
        if (comptime @import("builtin").single_threaded or @import("builtin").cpu.arch.isWasm() or @import("builtin").os.tag == .freestanding) return null;
        if (!try audio.streaming.supported(a, bytes)) return null;
        const check = control.imageWorkControl();
        const info = audio.streaming.inspect(a, bytes, .{ .context = check.context, .check_fn = check.check_fn }) catch |err| {
            if (err == error.UnsupportedStreamingAudioProfile or err == error.UnsupportedAudioFormat) return null;
            return err;
        };
        if (info.samples == 0 or info.sample_rate == 0) return error.UnsupportedAudioFormat;
        const self = try a.create(Stream);
        errdefer a.destroy(self);
        self.* = .{ .a = a, .bytes = bytes, .control = control, .io = control.io orelse std.Options.debug_io, .info = info, .queue = undefined };
        self.queue = .init(&self.storage);
        self.thread = try std.Thread.spawn(.{}, run, .{self});
        return self;
    }
    fn run(self: *Stream) void {
        const check = self.control.imageWorkControl();
        audio.streaming.visit(self.a, self.bytes, .{ .context = check.context, .check_fn = check.check_fn }, .{ .context = self, .write = push }) catch |err| {
            self.failure = err;
        };
        self.queue.close(self.io);
    }
    fn push(raw: *anyopaque, samples: []const f32, rate: u32, channels: u8) !void {
        const self: *Stream = @ptrCast(@alignCast(raw));
        if (channels != 1 or rate != self.info.sample_rate) return error.UnsupportedAudioFormat;
        const owned = try self.a.dupe(f32, samples);
        errdefer self.a.free(owned);
        try self.queue.putOneUncancelable(self.io, owned);
    }
    fn join(self: *Stream) void {
        if (self.thread) |thread| {
            thread.join();
            self.thread = null;
        }
    }
    pub fn deinit(self: *Stream) void {
        self.queue.close(self.io);
        self.join();
        var item: [1][]f32 = undefined;
        while (true) {
            _ = self.queue.getUncancelable(self.io, &item, 1) catch break;
            self.a.free(item[0]);
        }
        self.buffer.deinit(self.a);
        const a = self.a;
        a.destroy(self);
    }
    pub fn finish(self: *Stream) !void {
        var total = self.received;
        var item: [1][]f32 = undefined;
        while (true) {
            _ = self.queue.getUncancelable(self.io, &item, 1) catch break;
            total = try std.math.add(usize, total, item[0].len);
            self.a.free(item[0]);
        }
        self.join();
        if (self.failure) |err| return err;
        if (total != self.info.samples) return error.IncompleteAudioStream;
    }
    pub fn provider(self: *Stream) Provider {
        return .{ .context = self, .samples = self.info.samples, .sample_rate = self.info.sample_rate, .read = read };
    }
    fn read(raw: *anyopaque, _: std.mem.Allocator, first: usize, last: usize) !Lease {
        const self: *Stream = @ptrCast(@alignCast(raw));
        try self.control.check();
        if (first < self.base or last < first or last > self.info.samples) return error.InvalidAudioWindow;
        const discard = @min(first - self.base, self.buffer.items.len);
        std.mem.copyForwards(f32, self.buffer.items[0 .. self.buffer.items.len - discard], self.buffer.items[discard..]);
        self.buffer.items.len -= discard;
        self.base += discard;
        while (self.received < last) {
            var item: [1][]f32 = undefined;
            _ = self.queue.getUncancelable(self.io, &item, 1) catch {
                self.join();
                return self.failure orelse error.IncompleteAudioStream;
            };
            defer self.a.free(item[0]);
            const block_start = self.received;
            self.received = try std.math.add(usize, self.received, item[0].len);
            if (self.received > self.info.samples) return error.InvalidAudioWindow;
            if (self.received <= first) {
                self.base = self.received;
                continue;
            }
            const skip = first -| block_start;
            if (self.buffer.items.len == 0) self.base = block_start + skip;
            try self.buffer.appendSlice(self.a, item[0][skip..]);
        }
        if (first < self.base or last - self.base > self.buffer.items.len) return error.IncompleteAudioStream;
        return .{ .pcm = .{ .samples = self.buffer.items[first - self.base .. last - self.base], .sample_rate = self.info.sample_rate } };
    }
};
