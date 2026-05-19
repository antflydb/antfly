// Copyright 2026 Antfly, Inc.
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
const antfly = @import("antfly-zig");

const svb = antfly.streamvbyte;
const platform_time = antfly.platform_time;

const Config = struct {
    values: usize = 100_000,
    encode_iters: usize = 2_000,
    decode_iters: usize = 20_000,
    seed: u64 = 0x5156_4201,
};

fn monotonicNs() u64 {
    return platform_time.monotonicNs();
}

fn parseArgs(io: std.Io, args_in: std.process.Args) !Config {
    var cfg = Config{};
    var args = std.process.Args.Iterator.init(args_in);
    _ = args.skip();

    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--values")) {
            cfg.values = try std.fmt.parseInt(usize, args.next() orelse return error.MissingArgument, 10);
        } else if (std.mem.eql(u8, arg, "--encode-iters")) {
            cfg.encode_iters = try std.fmt.parseInt(usize, args.next() orelse return error.MissingArgument, 10);
        } else if (std.mem.eql(u8, arg, "--decode-iters")) {
            cfg.decode_iters = try std.fmt.parseInt(usize, args.next() orelse return error.MissingArgument, 10);
        } else if (std.mem.eql(u8, arg, "--seed")) {
            cfg.seed = try std.fmt.parseInt(u64, args.next() orelse return error.MissingArgument, 0);
        } else if (std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "-h")) {
            var stdout_buf: [1024]u8 = undefined;
            var out = std.Io.File.stdout().writerStreaming(io, &stdout_buf);
            try out.interface.writeAll(
                \\usage: search_benchmark_codec_bench [--values N] [--encode-iters N] [--decode-iters N] [--seed N]
                \\
            );
            try out.interface.flush();
            std.process.exit(0);
        } else {
            return error.UnknownArgument;
        }
    }

    if (cfg.values == 0 or cfg.encode_iters == 0 or cfg.decode_iters == 0) return error.InvalidArgument;
    return cfg;
}

fn fillBenchmarkValues(values: []u32, seed: u64) void {
    var rng = std.Random.DefaultPrng.init(seed);
    const random = rng.random();

    for (values, 0..) |*value, i| {
        value.* = switch (i % 8) {
            0, 1, 2 => random.int(u8),
            3, 4 => random.int(u16),
            5, 6 => random.intRangeAtMost(u32, 0, 0x00ff_ffff),
            else => random.int(u32),
        };
    }
}

fn checksum(values: []const u32) u64 {
    var sum: u64 = 0;
    for (values) |value| {
        sum = sum *% 1_099_511_628_211 +% value;
    }
    return sum;
}

pub fn main(init: std.process.Init) !void {
    var gpa_state: std.heap.DebugAllocator(.{}) = .init;
    defer _ = gpa_state.deinit();
    const alloc = gpa_state.allocator();

    const cfg = try parseArgs(init.io, init.minimal.args);
    var stdout_buf: [4096]u8 = undefined;
    var out = std.Io.File.stdout().writerStreaming(init.io, &stdout_buf);

    const values = try alloc.alloc(u32, cfg.values);
    defer alloc.free(values);
    fillBenchmarkValues(values, cfg.seed);

    const control_buf = try alloc.alloc(u8, svb.encodedControlLen(cfg.values));
    defer alloc.free(control_buf);
    const data_buf = try alloc.alloc(u8, svb.encodedDataCapacity(cfg.values));
    defer alloc.free(data_buf);

    for (0..16) |_| {
        _ = try svb.encodeInto(control_buf, data_buf, values);
    }

    var encoded_len_sum: usize = 0;
    const encode_start = monotonicNs();
    for (0..cfg.encode_iters) |_| {
        const encoded = try svb.encodeInto(control_buf, data_buf, values);
        encoded_len_sum +%= encoded.control_len + encoded.data_len;
        std.mem.doNotOptimizeAway(data_buf.ptr);
    }
    const encode_elapsed = monotonicNs() - encode_start;

    const encoded = try svb.encodeInto(control_buf, data_buf, values);
    const control = control_buf[0..encoded.control_len];
    const data = data_buf[0..encoded.data_len];

    const decoded = try alloc.alloc(u32, cfg.values);
    defer alloc.free(decoded);

    var decoded_count_sum: usize = 0;
    const decode_start = monotonicNs();
    for (0..cfg.decode_iters) |_| {
        const result = svb.decodeInto(control, data, decoded);
        decoded_count_sum +%= result.decoded;
        std.mem.doNotOptimizeAway(decoded.ptr);
    }
    const decode_elapsed = monotonicNs() - decode_start;

    const original_checksum = checksum(values);
    const decoded_checksum = checksum(decoded);
    if (original_checksum != decoded_checksum) return error.DecodeMismatch;

    const encoded_bytes = control.len + data.len;
    const raw_bytes = cfg.values * @sizeOf(u32);
    const encode_ns_per_value = @as(f64, @floatFromInt(encode_elapsed)) /
        @as(f64, @floatFromInt(cfg.encode_iters * cfg.values));
    const decode_ns_per_value = @as(f64, @floatFromInt(decode_elapsed)) /
        @as(f64, @floatFromInt(cfg.decode_iters * cfg.values));
    const compression = @as(f64, @floatFromInt(raw_bytes)) / @as(f64, @floatFromInt(encoded_bytes));

    try out.interface.print(
        \\{{"bench":"streamvbyte","values":{d},"encode_iters":{d},"decode_iters":{d},"raw_bytes":{d},"encoded_bytes":{d},"compression":{d:.4},"encode_ns_per_value":{d:.4},"decode_ns_per_value":{d:.4},"checksum":{d},"encoded_len_guard":{d},"decoded_count_guard":{d}}}
        \\
    , .{
        cfg.values,
        cfg.encode_iters,
        cfg.decode_iters,
        raw_bytes,
        encoded_bytes,
        compression,
        encode_ns_per_value,
        decode_ns_per_value,
        original_checksum,
        encoded_len_sum,
        decoded_count_sum,
    });
    try out.interface.flush();
}
