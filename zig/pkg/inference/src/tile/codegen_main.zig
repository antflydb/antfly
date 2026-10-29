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

//! `zig build tile-kernel-codegen [-- --write]`: check (default) or rewrite
//! the checked-in sources rendered from tile kernels. Paths are relative to
//! the inference package root.

const std = @import("std");
const gpu = @import("gpu.zig");
const whisper = @import("whisper_logits.zig");

const File = struct { path: []const u8, data: []const u8 };

const files = [_]File{
    .{ .path = "src/ops/metal/generated/tile_whisper_logits_partials.metal", .data = gpu.renderMetal(whisper.kernel) },
    .{ .path = "src/ops/metal/generated/tile_whisper_logits_partials.msl.h", .data = gpu.renderMetalHeader(whisper.kernel, "antfly_tile_whisper_logits_partials_msl") },
    .{ .path = "src/ops/cuda/generated/tile_whisper_logits.cu", .data = gpu.renderCuda(whisper.kernel) },
};

pub fn main(init: std.process.Init) !void {
    const io = init.io;
    const allocator = init.arena.allocator();
    const args = try init.minimal.args.toSlice(allocator);
    var write = false;
    for (args[1..]) |arg| {
        if (std.mem.eql(u8, arg, "--write")) {
            write = true;
        } else if (!std.mem.eql(u8, arg, "--check")) {
            std.debug.print("usage: tile-kernel-codegen [--check|--write]\n", .{});
            return error.InvalidArguments;
        }
    }
    const cwd = std.Io.Dir.cwd();
    var stale: usize = 0;
    for (files) |f| {
        if (write) {
            try cwd.writeFile(io, .{ .sub_path = f.path, .data = f.data });
            std.debug.print("wrote {s}\n", .{f.path});
            continue;
        }
        const actual = cwd.readFileAlloc(io, f.path, allocator, .limited(4 * 1024 * 1024)) catch |err| {
            std.debug.print("{s}: {s}\n", .{ f.path, @errorName(err) });
            stale += 1;
            continue;
        };
        if (!std.mem.eql(u8, actual, f.data)) {
            std.debug.print("{s}: stale\n", .{f.path});
            stale += 1;
        }
    }
    if (stale != 0) {
        std.debug.print("{d} tile kernel source(s) out of date; run `zig build tile-kernel-codegen -- --write`\n", .{stale});
        return error.StaleGeneratedSources;
    }
}
