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

const std = @import("std");
const webp = @import("antfly_webp");
const Worker = struct {
    bytes: []const u8,
    image: ?webp.DecodedImage = null,
    failure: ?anyerror = null,
    fn run(self: *Worker) void {
        self.image = webp.decodeRgbaLimited(std.heap.c_allocator, self.bytes, .{ .max_pixels = 16 * 1024 * 1024, .max_rgba_bytes = 64 * 1024 * 1024 }) catch |err| {
            self.failure = err;
            return;
        };
    }
};
fn now(io: std.Io) i128 {
    return std.Io.Clock.awake.now(io).nanoseconds;
}
pub fn main(init: std.process.Init) !void {
    const a = init.gpa;
    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, a);
    defer args.deinit();
    _ = args.next();
    const path = args.next() orelse return error.InvalidArguments;
    const iterations = try std.fmt.parseInt(usize, args.next() orelse "20", 10);
    const batch = try std.fmt.parseInt(usize, args.next() orelse "1", 10);
    const warmups = try std.fmt.parseInt(usize, args.next() orelse "2", 10);
    if (iterations == 0 or iterations > 10000 or batch == 0 or batch > 4 or warmups > 100 or args.next() != null) return error.InvalidArguments;
    const bytes = try std.Io.Dir.cwd().readFileAlloc(init.io, path, a, .limited(64 * 1024 * 1024));
    defer a.free(bytes);
    const samples = try a.alloc(f64, iterations);
    defer a.free(samples);
    var workers: [4]Worker = undefined;
    var threads: [4]std.Thread = undefined;
    var pixel_hash: [32]u8 = undefined;
    var width: u32 = 0;
    var height: u32 = 0;
    for (0..iterations + warmups) |iteration| {
        const start = now(init.io);
        var spawned: usize = 0;
        defer {
            for (threads[0..spawned]) |thread| thread.join();
            for (workers[0..batch]) |worker| if (worker.image) |image| std.heap.c_allocator.free(image.rgba);
        }
        for (workers[0..batch]) |*worker| worker.* = .{ .bytes = bytes };
        for (workers[0..batch], 0..) |*worker, i| {
            threads[i] = try std.Thread.spawn(.{}, Worker.run, .{worker});
            spawned += 1;
        }
        for (threads[0..spawned]) |thread| thread.join();
        spawned = 0;
        const elapsed = @as(f64, @floatFromInt(now(init.io) - start)) / 1e9;
        for (workers[0..batch]) |worker| if (worker.failure) |err| return err;
        if (iteration >= warmups) samples[iteration - warmups] = elapsed;
        // Hash verification is outside the timed decode wave.
        for (workers[0..batch], 0..) |worker, index| {
            const image = worker.image.?;
            var hash: [32]u8 = undefined;
            std.crypto.hash.sha2.Sha256.hash(image.rgba, &hash, .{});
            if (iteration == 0 and index == 0) {
                pixel_hash = hash;
                width = image.width;
                height = image.height;
            }
            if (!std.mem.eql(u8, &hash, &pixel_hash) or image.width != width or image.height != height) return error.PixelParityFailed;
        }
    }
    const sorted = try a.dupe(f64, samples);
    defer a.free(sorted);
    std.mem.sort(f64, sorted, {}, std.sort.asc(f64));
    const p50 = sorted[(iterations - 1) / 2];
    const p95 = sorted[(iterations * 95 + 99) / 100 - 1];
    var input_hash: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &input_hash, .{});
    const input_hex = std.fmt.bytesToHex(input_hash, .lower);
    const rgba_hex = std.fmt.bytesToHex(pixel_hash, .lower);
    const output = try std.json.Stringify.valueAlloc(a, .{
        .input = path,
        .input_sha256 = input_hex[0..],
        .rgba_sha256 = rgba_hex[0..],
        .width = width,
        .height = height,
        .batch = batch,
        .warmups = warmups,
        .iterations = iterations,
        .p50_seconds = p50,
        .p95_seconds = p95,
        .images_per_second = @as(f64, @floatFromInt(batch)) / p50,
        .samples_seconds = samples,
    }, .{});
    defer a.free(output);
    std.debug.print("{s}\n", .{output});
}
