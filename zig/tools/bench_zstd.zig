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

//! Bounded-batch or full-corpus construction and held-out recall probe.
//! Pass `full` in place of the batch size for normalized squared-L2 inputs.
//! An optional trailing argument selects the leaf capacity (default 128).
//! The read-only float32 fixture is external vector ownership; its bytes are
//! reported separately from build workspace. This excludes HTTP and WAL costs.
const std = @import("std");
const Native = @import("antfly_compress").ZstdDecoder;
const A = std.mem.Allocator;
fn decode(comptime native: bool, a: A, frame: []const u8, window: []u8, limit: usize) ![]u8 {
    var input: std.Io.Reader = .fixed(frame);
    var decoder: if (native) Native else std.compress.zstd.Decompress = .init(&input, window, .{});
    return decoder.reader.allocRemaining(a, .limited(limit));
}
fn trial(comptime native: bool, io: std.Io, a: A, frame: []const u8, expected: []const u8, iterations: usize) !i96 {
    const window = try a.alloc(u8, std.compress.zstd.default_window_len + std.compress.zstd.block_size_max);
    defer a.free(window);
    const started = std.Io.Clock.awake.now(io).nanoseconds;
    for (0..iterations) |_| {
        const result = try decode(native, a, frame, window, expected.len + 1);
        defer a.free(result);
        if (!std.mem.eql(u8, expected, result)) return error.InvalidBenchmarkOutput;
    }
    return std.Io.Clock.awake.now(io).nanoseconds - started;
}
pub fn main(init: std.process.Init) !void {
    const a = std.heap.smp_allocator;
    const args = try init.minimal.args.toSlice(init.arena.allocator());
    if (args.len < 4 or args.len > 5) return error.ExpectedFrameIterationsMaximumOutputBytesAndOptionalTrials;
    const iterations = try std.fmt.parseInt(usize, args[2], 10);
    const limit = try std.fmt.parseInt(usize, args[3], 10);
    const trials = if (args.len == 5) try std.fmt.parseInt(usize, args[4], 10) else 5;
    if (iterations == 0 or trials == 0 or limit == 0 or limit > 64 * 1024 * 1024) return error.InvalidBenchmarkOptions;
    const frame = try std.Io.Dir.cwd().readFileAlloc(init.io, args[1], a, .limited(64 * 1024 * 1024));
    defer a.free(frame);
    const window = try a.alloc(u8, std.compress.zstd.default_window_len + std.compress.zstd.block_size_max);
    defer a.free(window);
    const expected = try decode(false, a, frame, window, limit + 1);
    defer a.free(expected);
    if (expected.len == 0 or expected.len > limit) return error.InvalidBenchmarkOutput;
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(expected, &digest, .{});
    const hex = std.fmt.bytesToHex(digest, .lower);
    for (0..trials) |i| {
        var native_ns: i96 = undefined;
        var reference_ns: i96 = undefined;
        if (i % 2 == 0) {
            native_ns = try trial(true, init.io, a, frame, expected, iterations);
            reference_ns = try trial(false, init.io, a, frame, expected, iterations);
        } else {
            reference_ns = try trial(false, init.io, a, frame, expected, iterations);
            native_ns = try trial(true, init.io, a, frame, expected, iterations);
        }
        const report = try std.json.Stringify.valueAlloc(a, .{ .trial = i, .iterations = iterations, .decoded_bytes = try std.math.mul(usize, expected.len, iterations), .encoded_frame_bytes = frame.len, .native_first = i % 2 == 0, .native_ns = native_ns, .reference_ns = reference_ns, .sha256 = hex[0..] }, .{});
        defer a.free(report);
        std.debug.print("ZSTD_PREFIX {s}\n", .{report});
    }
}
