// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! External-corpus differential hashes and deterministic bounded mutation runner.
const std = @import("std");
const media = @import("antfly_media");
const video = @import("mod.zig");
const Guard = struct {
    checks: usize = 0,
    fn check(context: ?*const anyopaque) !void {
        const self: *Guard = @ptrCast(@alignCast(@constCast(context.?)));
        self.checks += 1;
        if (self.checks > 20_000) return error.DeadlineExceeded;
    }
};
pub fn main(init: std.process.Init) !void {
    const a = init.gpa;
    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, a);
    defer args.deinit();
    _ = args.skip();
    const mode = args.next() orelse return error.InvalidArguments;
    const path = args.next() orelse return error.InvalidArguments;
    const count = if (args.next()) |value| try std.fmt.parseInt(usize, value, 10) else 1000;
    if (args.next() != null or count > 1_000_000) return error.InvalidArguments;
    const file = try std.Io.Dir.cwd().openFile(init.io, path, .{});
    defer file.close(init.io);
    const size = (try file.stat(init.io)).size;
    if (size == 0 or size > 64 * 1024 * 1024) return error.ResourceLimitExceeded;
    const original = try a.alloc(u8, @intCast(size));
    defer a.free(original);
    var buffer: [8192]u8 = undefined;
    var input = file.reader(init.io, &buffer);
    try input.interface.readSliceAll(original);
    var out_buffer: [8192]u8 = undefined;
    var out = std.Io.File.stdout().writer(init.io, &out_buffer);
    if (std.mem.eql(u8, mode, "hash") or std.mem.eql(u8, mode, "dump")) {
        var src = media.source.Source{ .allocator = a, .identity = path, .storage = .{ .borrowed = original }, .limits = .{ .max_total_bytes = 4 * 1024 * 1024 * 1024 } };
        var reader = try media.mp4.Reader.init(a, &src, .{});
        defer reader.deinit();
        if (reader.packets.len > 20_000) return error.ResourceLimitExceeded;
        const indexes = try a.alloc(usize, reader.packets.len);
        defer a.free(indexes);
        for (indexes, 0..) |*index, i| index.* = i;
        const Capture = struct {
            reader: *media.mp4.Reader,
            writer: *std.Io.Writer,
            dump: bool,
            fn publish(context: *anyopaque, slot: usize, frame: *const video.h264.Frame) !void {
                const self: *@This() = @ptrCast(@alignCast(context));
                if (self.dump) {
                    try self.writer.writeAll(frame.nv12);
                    return;
                }
                var hash: [32]u8 = undefined;
                std.crypto.hash.sha2.Sha256.hash(frame.nv12, &hash, .{});
                const y_bytes = @as(usize, frame.width) * frame.height * (if (frame.bit_depth > 8) @as(usize, 2) else 1);
                var y_hash: [32]u8 = undefined;
                var uv_hash: [32]u8 = undefined;
                std.crypto.hash.sha2.Sha256.hash(frame.nv12[0..y_bytes], &y_hash, .{});
                std.crypto.hash.sha2.Sha256.hash(frame.nv12[y_bytes..], &uv_hash, .{});
                try self.writer.print("{{\"index\":{d},\"media_pts\":{d},\"pts\":{d},\"width\":{d},\"height\":{d},\"bit_depth\":{d},\"chroma_format\":{d},\"sha256\":\"{s}\",\"y_sha256\":\"{s}\",\"uv_sha256\":\"{s}\"}}\n", .{ slot, self.reader.packets[slot].media_pts, frame.pts, frame.width, frame.height, frame.bit_depth, frame.chroma_format, std.fmt.bytesToHex(hash, .lower), std.fmt.bytesToHex(y_hash, .lower), std.fmt.bytesToHex(uv_hash, .lower) });
            }
        };
        var capture = Capture{ .reader = &reader, .writer = &out.interface, .dump = std.mem.eql(u8, mode, "dump") };
        _ = try video.h264.decodeSelected(a, &reader, indexes, .{ .max_dependency_packets = @max(reader.packets.len, 1), .max_decode_bytes = 512 * 1024 * 1024 }, &capture, Capture.publish);
    } else if (std.mem.eql(u8, mode, "mutate")) {
        var prng = std.Random.DefaultPrng.init(0xA17F1);
        const random = prng.random();
        const bytes = try a.alloc(u8, original.len);
        defer a.free(bytes);
        var accepted: usize = 0;
        for (0..count) |iteration| {
            @memcpy(bytes, original);
            const length = if (iteration % 4 == 0) random.uintLessThan(usize, original.len + 1) else bytes.len;
            for (0..1 + iteration % 8) |_| {
                const position = random.uintLessThan(usize, bytes.len);
                bytes[position] ^= @as(u8, 1) << random.int(u3);
            }
            var guard = Guard{};
            var src = media.source.Source{ .allocator = a, .identity = "mutated", .storage = .{ .borrowed = bytes[0..length] }, .control = .{ .context = &guard, .check_fn = Guard.check }, .limits = .{ .max_total_bytes = 32 * 1024 * 1024 } };
            var reader = media.mp4.Reader.init(a, &src, .{ .max_samples = 256, .max_index_bytes = 1024 * 1024 }) catch continue;
            defer reader.deinit();
            if (reader.packets.len == 0) continue;
            var frame = video.h264.decodeFrame(a, &reader, random.uintLessThan(usize, reader.packets.len), .{ .max_pixels = 1024 * 1024, .max_packet_bytes = 1024 * 1024, .max_decode_bytes = 64 * 1024 * 1024, .max_dependency_packets = 32, .max_slices = 64, .max_parameter_packets = 256, .max_parameter_bytes = 1024 * 1024 }) catch continue;
            frame.deinit();
            accepted += 1;
        }
        try out.interface.print("{{\"kind\":\"bounded-mutation\",\"seed\":{d},\"iterations\":{d},\"accepted\":{d}}}\n", .{ @as(u64, 0xA17F1), count, accepted });
    } else return error.InvalidArguments;
    try out.interface.flush();
}
