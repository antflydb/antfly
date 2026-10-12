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

//! Completed model-facing WebM decode/preparation, without inference or readback.
const std = @import("std");
const media = @import("antfly_media");
const video = @import("mod.zig");
extern fn av_benchmark_device_create() ?*anyopaque;
extern fn av_benchmark_device_destroy(*anyopaque) void;
extern fn av_benchmark_peak_rss() u64;
const Mode = enum { decode, cpu, metal, hash, dump };
const Context = struct {
    a: std.mem.Allocator,
    io: std.Io,
    out: *std.Io.Writer,
    mode: Mode,
    pool: *media.admission.Pool,
    metal: ?*video.preparation.Metal,
    size: u32,
    step: usize,
    pictures: usize = 0,
    prepared: usize = 0,
    output_copy_bytes: u64 = 0,
    decode_peak: usize = 0,
    preparation_ns: u64 = 0,
    gpu_ns: u64 = 0,
    gpu_timed_commands: usize = 0,
    coefficient_upload_bytes: u64 = 0,
    native_upload_bytes: u64 = 0,
    device_bytes: u64 = 0,
    checksum: u64 = 0,
    fn accept(self: *Context, frame: *const video.codecs.Frame) !void {
        self.decode_peak = @max(self.decode_peak, frame.decode_high_water);
        self.output_copy_bytes += frame.bytes.len;
        self.checksum +%= frame.bytes[0];
        const id = self.pictures;
        self.pictures += 1;
        if (self.mode == .dump) {
            if (id == self.step) try self.out.writeAll(frame.bytes);
            return;
        }
        if (self.mode == .hash) {
            var digest: [32]u8 = undefined;
            std.crypto.hash.sha2.Sha256.hash(frame.bytes, &digest, .{});
            const hex = std.fmt.bytesToHex(digest, .lower);
            try self.out.print("{{\"kind\":\"picture\",\"index\":{d},\"width\":{d},\"height\":{d},\"depth\":{d},\"chroma\":{d},\"sha256\":\"{s}\"}}\n", .{ id, frame.width, frame.height, frame.bit_depth, frame.chroma_format, hex });
            return;
        }
        if (self.mode == .decode or id % self.step != 0) return;
        if (frame.color.transfer == 16 or frame.color.transfer == 18) return error.UnsupportedVideoColor;
        const matrix: video.preparation.Matrix = switch (frame.color.matrix) {
            1 => .bt709,
            2, 5, 6 => .bt601,
            9 => .bt2020_ncl,
            else => return error.UnsupportedVideoColor,
        };
        const options = video.preparation.Options{ .width = self.size, .height = self.size, .matrix = matrix, .torchvision = true, .admission_pool = self.pool };
        const start = now(self.io);
        if (self.metal) |owner| {
            var prepared = try owner.submitHost(self.a, frame.host(), options, .{});
            defer prepared.deinit();
            try prepared.wait(self.io, .{});
            const seconds = try prepared.gpuSeconds();
            if (seconds > 0) self.gpu_timed_commands += 1;
            self.gpu_ns += @intFromFloat(@max(seconds, 0) * 1e9);
            self.coefficient_upload_bytes += prepared.coefficient_staging_bytes;
            self.native_upload_bytes += prepared.native_staging_bytes;
            self.device_bytes = @max(self.device_bytes, owner.deviceAllocatedBytes());
        } else {
            const patches = try video.preparation.referenceHost(self.a, frame.host(), options, .{});
            defer self.a.free(patches);
            std.mem.doNotOptimizeAway(patches.ptr);
            self.checksum +%= @as(u32, @bitCast(patches[0]));
        }
        self.preparation_ns += now(self.io) - start;
        self.prepared += 1;
    }
};
fn pass(context: *Context, reader: *media.webm.Reader) !struct { reference_copy_bytes: ?u64, retained_reference_bytes: ?usize } {
    const a = context.a;
    if (reader.packets.len > 8192) return error.ResourceLimitExceeded;
    if (reader.track.codec == .vp8) {
        const session = try video.vp8.Session.init(a, reader, .{ .admission_pool = context.pool, .max_decode_bytes = 256 * 1024 * 1024 });
        defer session.deinit();
        while (session.next_index < reader.packets.len) {
            var decoded = (try session.next()) orelse continue;
            defer decoded.deinit();
            const p = decoded.planes;
            var frame = try @import("codecs/frame.zig").copy(a, context.pool, p.width, p.height, 8, 1, .{ .matrix = 6 }, .{ p.y.ptr, p.u.ptr, p.v.ptr }, .{ p.y_stride, p.uv_stride, p.uv_stride }, 128 * 1024 * 1024);
            defer frame.deinit();
            frame.decode_high_water = decoded.decode_high_water;
            try context.accept(&frame);
            if (context.mode == .dump and context.pictures > context.step) break;
        }
        return .{ .reference_copy_bytes = session.decoder.reference_copy_bytes, .retained_reference_bytes = session.decoder.retainedBytes() };
    }
    const session = try video.codecs.Session.create(a, if (reader.track.codec == .vp9) .vp9 else .av1, .{ .admission_pool = context.pool });
    defer session.destroy();
    for (reader.packets, 0..) |packet, id| {
        var batch = try session.decodeWebmPacket(reader, id);
        defer batch.deinit();
        if (!packet.invisible) for (batch.frames) |*frame| {
            try context.accept(frame);
            if (context.mode == .dump and context.pictures > context.step) break;
        };
        if (context.mode == .dump and context.pictures > context.step) break;
    }
    return .{ .reference_copy_bytes = null, .retained_reference_bytes = null };
}
pub fn main(init: std.process.Init) !void {
    const a = init.gpa;
    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, a);
    defer args.deinit();
    _ = args.skip();
    const path = args.next() orelse return error.InvalidBenchmarkArguments;
    const mode = std.meta.stringToEnum(Mode, args.next() orelse "decode") orelse return error.InvalidBenchmarkArguments;
    const iterations = try std.fmt.parseInt(usize, args.next() orelse "7", 10);
    const size = try std.fmt.parseInt(u32, args.next() orelse "384", 10);
    const step = try std.fmt.parseInt(usize, args.next() orelse "24", 10);
    if (args.next() != null or iterations < 2 or iterations > 100 or size == 0 or size > 2048 or (step == 0 and mode != .dump)) return error.InvalidBenchmarkArguments;
    var buffer: [8192]u8 = undefined;
    var writer = std.Io.File.stdout().writer(init.io, &buffer);
    var pool = media.admission.Pool{ .limits = .{ .host_bytes = 1024 * 1024 * 1024, .device_bytes = 1024 * 1024 * 1024, .commands = 1 } };
    defer std.debug.assert(std.meta.eql(pool.snapshot(), media.admission.Resources{}));
    const file = try std.Io.Dir.cwd().openFile(init.io, path, .{});
    defer file.close(init.io);
    const bytes = (try file.stat(init.io)).size;
    if (bytes == 0 or bytes > 1024 * 1024 * 1024) return error.ResourceLimitExceeded;
    var provider = media.source.FileRange{ .file = file, .io = init.io };
    var input = media.source.Source{ .allocator = a, .identity = path, .storage = .{ .range = .{ .context = &provider, .read_at = media.source.FileRange.readAt, .length = bytes } }, .admission_pool = &pool, .limits = .{ .max_total_bytes = 16 * 1024 * 1024 * 1024 } };
    const index_start = now(init.io);
    var reader = try media.webm.Reader.init(a, &input, .{});
    defer reader.deinit();
    const index_ns = now(init.io) - index_start;
    var device: ?*anyopaque = null;
    var metal: ?video.preparation.Metal = null;
    const setup_start = now(init.io);
    if (mode == .metal) {
        if (comptime @import("builtin").os.tag != .macos) return error.UnsupportedVideoBackend;
        device = av_benchmark_device_create() orelse return error.MetalPreparationUnavailable;
        errdefer av_benchmark_device_destroy(device.?);
        metal = try video.preparation.Metal.init(device.?);
        errdefer metal.?.deinit();
        try metal.?.admit(&pool, 16 * 1024 * 1024);
    }
    defer {
        if (metal) |*owner| owner.deinit();
        if (device) |handle| av_benchmark_device_destroy(handle);
    }
    const setup_ns = now(init.io) - setup_start;
    var samples: [99]u64 = undefined;
    var pictures: usize = 0;
    for (0..if (mode == .hash or mode == .dump) @as(usize, 1) else iterations) |iteration| {
        var context = Context{ .a = a, .io = init.io, .out = &writer.interface, .mode = mode, .pool = &pool, .metal = if (metal) |*owner| owner else null, .size = size, .step = step };
        const start = now(init.io);
        const stats = try pass(&context, &reader);
        if (mode == .dump and context.pictures <= step) return error.InvalidPacketIndex;
        const completed = now(init.io) - start;
        pictures = context.pictures;
        if (iteration > 0) samples[iteration - 1] = completed;
        if (mode != .hash and mode != .dump) try writer.interface.print("{{\"kind\":\"iteration\",\"codec\":\"{s}\",\"backend\":\"{s}\",\"state\":\"{s}\",\"iteration\":{d},\"completed_ns\":{d},\"coded_packets\":{d},\"visible_pictures\":{d},\"prepared_pictures\":{d},\"preparation_ns\":{d},\"gpu_execution_ns\":{d},\"gpu_timed_commands\":{d},\"decode_live_peak_bytes\":{d},\"output_copy_bytes\":{d},\"vp8_reference_copy_bytes\":{?d},\"vp8_retained_reference_bytes\":{?d},\"device_allocated_bytes\":{d},\"process_peak_rss_bytes\":{d},\"coefficient_upload_bytes\":{d},\"native_upload_bytes\":{d},\"checksum\":{d}}}\n", .{ @tagName(reader.track.codec), @tagName(mode), if (iteration == 0) "first-pass" else "warm", iteration, completed, reader.packets.len, context.pictures, context.prepared, context.preparation_ns, context.gpu_ns, context.gpu_timed_commands, context.decode_peak, context.output_copy_bytes, stats.reference_copy_bytes, stats.retained_reference_bytes, context.device_bytes, av_benchmark_peak_rss(), context.coefficient_upload_bytes, context.native_upload_bytes, context.checksum });
        try writer.interface.flush();
    }
    if (mode != .hash and mode != .dump) {
        const warm = samples[0 .. iterations - 1];
        std.mem.sort(u64, warm, {}, std.sort.asc(u64));
        const p50 = warm[(warm.len - 1) / 2];
        const p95 = warm[(warm.len * 95 + 99) / 100 - 1];
        try writer.interface.print("{{\"kind\":\"summary\",\"source_bytes\":{d},\"width\":{d},\"height\":{d},\"duration_seconds\":{?d},\"index_ns\":{d},\"setup_ns\":{d},\"warm_samples\":{d},\"warm_p50_ns\":{d},\"warm_p95_ns\":{d},\"visible_pictures_per_second_p50\":{d}}}\n", .{ bytes, reader.track.width, reader.track.height, reader.duration_seconds, index_ns, setup_ns, warm.len, p50, p95, @as(f64, @floatFromInt(pictures)) * 1e9 / @as(f64, @floatFromInt(@max(p50, 1))) });
        try writer.interface.flush();
    }
}
fn now(io: std.Io) u64 {
    return @intCast(std.Io.Clock.awake.now(io).nanoseconds);
}
