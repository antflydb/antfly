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

//! Tiny CLI that wraps gguf.quant_codec.dequantizeToFloat32 for use from
//! out-of-process test harnesses (e.g. the WebGPU shader E2E test).
//!
//! Protocol: argv[1] = format name (matches KnownTensorType, e.g. "MXFP4").
//! stdin: raw quantized bytes (size must be a whole number of blocks).
//! stdout: f32 little-endian bytes — block_count * values_per_block elements.

const std = @import("std");
const tensor_types = @import("gguf/tensor_types.zig");
const codec = @import("gguf/quant_codec.zig");

pub fn main(init: std.process.Init) !void {
    const allocator = std.heap.c_allocator;
    const io = init.io;

    var args_iter = std.process.Args.Iterator.init(init.minimal.args);
    _ = args_iter.next(); // skip argv[0]
    const fmt_name = args_iter.next() orelse {
        std.debug.print("usage: dequant_cli <format>\n", .{});
        std.process.exit(2);
    };

    const fmt_tag = std.meta.stringToEnum(tensor_types.KnownTensorType, fmt_name) orelse {
        std.debug.print("unknown format: {s}\n", .{fmt_name});
        std.process.exit(2);
    };
    const ttype = tensor_types.TensorType{ .known = fmt_tag };

    const block_values = tensor_types.valuesPerBlock(ttype) orelse {
        std.debug.print("no block size for {s}\n", .{fmt_name});
        std.process.exit(2);
    };
    const block_bytes = tensor_types.bytesPerBlock(ttype) orelse {
        std.debug.print("no byte size for {s}\n", .{fmt_name});
        std.process.exit(2);
    };

    var raw: std.ArrayList(u8) = .empty;
    defer raw.deinit(allocator);

    var stdin_buf: [8192]u8 = undefined;
    var stdin_reader = std.Io.File.stdin().reader(io, &stdin_buf);
    const stdin = &stdin_reader.interface;
    try stdin.appendRemainingUnlimited(allocator, &raw);

    if (raw.items.len % block_bytes != 0) {
        std.debug.print(
            "input size {d} not a multiple of block size {d}\n",
            .{ raw.items.len, block_bytes },
        );
        std.process.exit(2);
    }
    const blocks = raw.items.len / block_bytes;
    const out_len = blocks * block_values;

    const output = try allocator.alloc(f32, out_len);
    defer allocator.free(output);
    try codec.dequantizeToFloat32(ttype, raw.items, output);

    var stdout_buf: [8192]u8 = undefined;
    var stdout_writer = std.Io.File.stdout().writer(io, &stdout_buf);
    const stdout = &stdout_writer.interface;
    try stdout.writeAll(std.mem.sliceAsBytes(output));
    try stdout.flush();
}
