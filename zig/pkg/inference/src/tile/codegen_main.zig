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
//!
//! `-- --emit-opencl <path>` writes the OpenCL test dialect of the Whisper
//! model kernels (not checked in) for scripts/tile/run_whisper_opencl.py.

const std = @import("std");
const gpu = @import("gpu.zig");
const whisper = @import("whisper_logits.zig");
const gpu_model = @import("gpu_model.zig");
const whisper_model = @import("whisper_model.zig");

const File = struct { path: []const u8, data: []const u8 };

const files = [_]File{
    .{ .path = "src/ops/metal/generated/tile_whisper_logits_partials.metal", .data = gpu.renderMetal(whisper.kernel) },
    .{ .path = "src/ops/metal/generated/tile_whisper_logits_partials.msl.h", .data = gpu.renderMetalHeader(whisper.kernel, "antfly_tile_whisper_logits_partials_msl") },
    .{ .path = "src/ops/cuda/generated/tile_whisper_logits.cu", .data = gpu.renderCuda(whisper.kernel) },
    .{ .path = "src/ops/metal/generated/tile_whisper_model.metal", .data = gpu_model.renderModule(whisper_model.kernels, .metal, "src/tile/whisper_model.zig") },
    .{ .path = "src/ops/cuda/generated/tile_whisper_model.cu", .data = gpu_model.renderModule(whisper_model.kernels, .cuda, "src/tile/whisper_model.zig") },
};

/// Generated regions inside hand-maintained files, by family.
const regions = @import("families.zig").regions;

const opencl_source = gpu_model.renderModule(whisper_model.kernels, .opencl, "src/tile/whisper_model.zig");

pub fn main(init: std.process.Init) !void {
    const io = init.io;
    const allocator = init.arena.allocator();
    const args = try init.minimal.args.toSlice(allocator);
    var write = false;
    const cwd = std.Io.Dir.cwd();
    var i: usize = 1;
    while (i < args.len) : (i += 1) {
        const arg = args[i];
        if (std.mem.eql(u8, arg, "--write")) {
            write = true;
        } else if (std.mem.eql(u8, arg, "--emit-opencl") and i + 1 < args.len) {
            i += 1;
            try cwd.writeFile(io, .{ .sub_path = args[i], .data = opencl_source });
            std.debug.print("wrote {s}\n", .{args[i]});
            return;
        } else if (!std.mem.eql(u8, arg, "--check")) {
            std.debug.print("usage: tile-kernel-codegen [--check|--write|--emit-opencl <path>]\n", .{});
            return error.InvalidArguments;
        }
    }
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
    for (regions) |r| {
        const contents = cwd.readFileAlloc(io, r.path, allocator, .limited(64 * 1024 * 1024)) catch |err| {
            std.debug.print("{s}: {s}\n", .{ r.path, @errorName(err) });
            stale += 1;
            continue;
        };
        const begin = std.mem.indexOf(u8, contents, r.begin) orelse {
            std.debug.print("{s}: missing marker {s}", .{ r.path, r.begin });
            stale += 1;
            continue;
        };
        const body_start = begin + r.begin.len;
        const body_end = std.mem.indexOfPos(u8, contents, body_start, r.end) orelse {
            std.debug.print("{s}: missing marker {s}", .{ r.path, r.end });
            stale += 1;
            continue;
        };
        if (std.mem.eql(u8, contents[body_start..body_end], r.data)) continue;
        if (write) {
            const updated = try std.mem.concat(allocator, u8, &.{ contents[0..body_start], r.data, contents[body_end..] });
            try cwd.writeFile(io, .{ .sub_path = r.path, .data = updated });
            std.debug.print("wrote region in {s}\n", .{r.path});
        } else {
            std.debug.print("{s}: generated region stale\n", .{r.path});
            stale += 1;
        }
    }
    if (stale != 0) {
        std.debug.print("{d} tile kernel source(s) out of date; run `zig build tile-kernel-codegen -- --write`\n", .{stale});
        return error.StaleGeneratedSources;
    }
}
