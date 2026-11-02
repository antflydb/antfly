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
pub const Fork = struct {
    provider: Provider,
    context: *anyopaque,
    destroy: *const fn (*anyopaque) void,
    pub fn deinit(self: Fork) void {
        self.destroy(self.context);
    }
};
pub const Provider = struct {
    context: *anyopaque,
    samples: usize,
    /// Unknown-length streams return a shorter final lease and publish EOF here.
    rewind: ?*const fn (*anyopaque) anyerror!void = null,
    fork: ?*const fn (*anyopaque, std.mem.Allocator) anyerror!Fork = null,
    final_samples: ?*const fn (*anyopaque) ?usize = null,
    probe: ?*const fn (*anyopaque, usize, usize) anyerror!?usize = null,
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
    eof: bool = false,
    producer_rate: u32 = 0,
    pub fn start(a: std.mem.Allocator, bytes: []const u8, control: @import("../execution_control.zig").InferenceExecutionControl) !?*Stream {
        if (comptime @import("builtin").single_threaded or @import("builtin").cpu.arch.isWasm() or @import("builtin").os.tag == .freestanding) return null;
        if (!try audio.streaming.supported(a, bytes)) return null;
        const self = try a.create(Stream);
        self.* = .{ .a = a, .bytes = bytes, .control = control, .io = control.io orelse std.Options.debug_io, .info = .{ .samples = 0, .sample_rate = 0 }, .queue = undefined };
        self.queue = .init(&self.storage);
        self.thread = std.Thread.spawn(.{}, run, .{self}) catch |err| {
            a.destroy(self);
            return err;
        };
        errdefer self.deinit();
        // The first block establishes the actual decoder output clock (including
        // implicit SBR), without a separate duration/PCM validation traversal.
        var first: [1][]f32 = undefined;
        _ = self.queue.getUncancelable(self.io, &first, 1) catch {
            self.join();
            const err = self.failure orelse error.UnsupportedAudioFormat;
            if (err == error.UnsupportedStreamingAudioProfile or err == error.UnsupportedAudioFormat) {
                self.deinit();
                return null;
            }
            return err;
        };
        defer a.free(first[0]);
        self.info.sample_rate = self.producer_rate;
        self.info.samples = @intCast(@min(std.math.maxInt(usize), @divFloor(@as(u128, std.math.maxInt(i64)) * self.info.sample_rate, std.time.ns_per_s)));
        try self.buffer.appendSlice(a, first[0]);
        self.received = first[0].len;
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
        if (channels != 1) return error.UnsupportedAudioFormat;
        if (self.producer_rate == 0) self.producer_rate = rate else if (rate != self.producer_rate) return error.UnsupportedAudioFormat;
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
        if (self.eof and total != self.received) return error.IncompleteAudioStream;
        self.received = total;
        self.eof = true;
    }
    /// Analysis and chunk execution are separate bounded decode passes.
    fn rewind(raw: *anyopaque) !void {
        if (comptime @import("builtin").single_threaded or @import("builtin").cpu.arch.isWasm() or @import("builtin").os.tag == .freestanding) return error.UnsupportedPlatform;
        const self: *Stream = @ptrCast(@alignCast(raw));
        self.queue.close(self.io);
        self.join();
        var item: [1][]f32 = undefined;
        while (true) {
            _ = self.queue.getUncancelable(self.io, &item, 1) catch break;
            self.a.free(item[0]);
        }
        self.buffer.clearRetainingCapacity();
        self.base = 0;
        self.received = 0;
        self.eof = false;
        self.failure = null;
        self.producer_rate = 0;
        self.queue = .init(&self.storage);
        self.thread = try std.Thread.spawn(.{}, run, .{self});
        _ = self.queue.getUncancelable(self.io, &item, 1) catch {
            self.join();
            return self.failure orelse error.UnsupportedAudioFormat;
        };
        defer self.a.free(item[0]);
        if (self.producer_rate != self.info.sample_rate) return error.UnsupportedAudioFormat;
        try self.buffer.appendSlice(self.a, item[0]);
        self.received = item[0].len;
    }
    fn fork(raw: *anyopaque, a: std.mem.Allocator) !Fork {
        const self: *Stream = @ptrCast(@alignCast(raw));
        const cloned = (try Stream.start(a, self.bytes, self.control)) orelse return error.UnsupportedAudioFormat;
        return .{ .provider = cloned.provider(), .context = cloned, .destroy = destroyFork };
    }
    fn destroyFork(raw: *anyopaque) void {
        const self: *Stream = @ptrCast(@alignCast(raw));
        self.deinit();
    }
    pub fn provider(self: *Stream) Provider {
        return .{ .context = self, .samples = self.info.samples, .final_samples = finalSamples, .probe = probe, .rewind = rewind, .fork = fork, .sample_rate = self.info.sample_rate, .read = read };
    }
    fn probe(raw: *anyopaque, first: usize, last: usize) !?usize {
        const self: *Stream = @ptrCast(@alignCast(raw));
        _ = try read(raw, self.a, first, @min(last, self.info.samples));
        return finalSamples(raw);
    }
    fn finalSamples(raw: *anyopaque) ?usize {
        const self: *Stream = @ptrCast(@alignCast(raw));
        return if (self.eof) self.received else null;
    }
    fn read(raw: *anyopaque, _: std.mem.Allocator, first: usize, last: usize) !Lease {
        const self: *Stream = @ptrCast(@alignCast(raw));
        try self.control.check();
        if (first < self.base or last < first or last > self.info.samples) return error.InvalidAudioWindow;
        const discard = @min(first - self.base, self.buffer.items.len);
        std.mem.copyForwards(f32, self.buffer.items[0 .. self.buffer.items.len - discard], self.buffer.items[discard..]);
        self.buffer.items.len -= discard;
        self.base += discard;
        while (self.received < last and !self.eof) {
            var item: [1][]f32 = undefined;
            _ = self.queue.getUncancelable(self.io, &item, 1) catch {
                self.join();
                if (self.failure) |err| return err;
                self.eof = true;
                break;
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
        if (self.eof and first >= self.received) return .{ .pcm = .{ .samples = &.{}, .sample_rate = self.info.sample_rate } };
        const stop = @min(last, self.received);
        if (first < self.base or stop - self.base > self.buffer.items.len) return error.IncompleteAudioStream;
        return .{ .pcm = .{ .samples = self.buffer.items[first - self.base .. stop - self.base], .sample_rate = self.info.sample_rate } };
    }
};

test "model aware media speaker analysis rewinds streaming PCM to the original sample clock" {
    const a = std.testing.allocator;
    const bytes = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, "../../lib/audio/testdata/tone.mp3", a, .limited(1024 * 1024));
    defer a.free(bytes);
    const stream = (try Stream.start(a, bytes, .{})) orelse return error.SkipZigTest;
    defer stream.deinit();
    const provider = stream.provider();
    const prefix = try provider.read(provider.context, a, 0, 1024);
    const expected = try a.dupe(f32, prefix.pcm.samples);
    defer a.free(expected);
    var first: usize = 0;
    while (true) {
        const lease = try provider.read(provider.context, a, first, first + 4096);
        if (lease.pcm.samples.len == 0) break;
        first += lease.pcm.samples.len;
    }
    try std.testing.expect(provider.final_samples.?(provider.context).? > 1024);
    try provider.rewind.?(provider.context);
    try std.testing.expect(provider.final_samples.?(provider.context) == null);
    const replayed = try provider.read(provider.context, a, 0, 1024);
    try std.testing.expectEqualSlices(f32, expected, replayed.pcm.samples);
    try std.testing.expectEqual(provider.sample_rate, replayed.pcm.sample_rate);
}

test "model aware media incremental analysis forks streaming decode without consuming chunk PCM" {
    const a = std.testing.allocator;
    const bytes = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, "../../lib/audio/testdata/tone.mp3", a, .limited(1024 * 1024));
    defer a.free(bytes);
    const stream = (try Stream.start(a, bytes, .{})) orelse return error.SkipZigTest;
    defer stream.deinit();
    const main = stream.provider();
    const analysis = try main.fork.?(main.context, a);
    defer analysis.deinit();
    const ahead = try analysis.provider.read(analysis.provider.context, a, 0, 8192);
    const expected = try a.dupe(f32, ahead.pcm.samples);
    defer a.free(expected);
    try std.testing.expect(expected.len > 4096);
    var position = expected.len;
    while (true) {
        const lease = try analysis.provider.read(analysis.provider.context, a, position, position + 4096);
        if (lease.pcm.samples.len == 0) break;
        position += lease.pcm.samples.len;
    }
    const first = try main.read(main.context, a, 0, 4096);
    try std.testing.expectEqualSlices(f32, expected[0..4096], first.pcm.samples);
    const next = try main.read(main.context, a, 4096, expected.len);
    try std.testing.expectEqualSlices(f32, expected[4096..], next.pcm.samples);
}
