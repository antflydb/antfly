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

pub const files = [_]File{
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
        } else if (std.mem.eql(u8, arg, "--dump-regions") and i + 1 < args.len) {
            i += 1;
            try cwd.createDirPath(io, args[i]);
            var manifest = std.ArrayListUnmanaged(u8).empty;
            for (regions, 0..) |region, index| {
                const file = try std.fmt.allocPrint(allocator, "{s}/{d}.source", .{ args[i], index });
                try cwd.writeFile(io, .{ .sub_path = file, .data = region.data });
                const entry = try std.fmt.allocPrint(allocator, "{d}\t{s}\t{s}", .{ index, region.path, region.begin });
                try manifest.appendSlice(allocator, entry);
            }
            const file = try std.fmt.allocPrint(allocator, "{s}/regions.tsv", .{args[i]});
            try cwd.writeFile(io, .{ .sub_path = file, .data = manifest.items });
            return;
        } else if (std.mem.eql(u8, arg, "--inventory") and i + 1 < args.len) {
            i += 1;
            const data = try @import("inventory.zig").render(allocator, &regions, &.{});
            try cwd.writeFile(io, .{ .sub_path = args[i], .data = data });
            return;
        } else if (std.mem.eql(u8, arg, "--emit-opencl") and i + 1 < args.len) {
            i += 1;
            try cwd.writeFile(io, .{ .sub_path = args[i], .data = opencl_source });
            std.debug.print("wrote {s}\n", .{args[i]});
            return;
        } else if (!std.mem.eql(u8, arg, "--check")) {
            std.debug.print("usage: tile-kernel-codegen [--check|--write|--inventory <path>|--dump-regions <dir>|--emit-opencl <path>]\n", .{});
            return error.InvalidArguments;
        }
    }
    var stale: usize = 0;
    for ([_][]const u8{ @import("region.zig").metal_kernels_m, @import("region.zig").cuda_bundle }) |path| {
        const contents = try cwd.readFileAlloc(io, path, allocator, .limited(16 * 1024 * 1024));
        try @import("native_sources.zig").audit(contents);
    }

    // Keep enabled-backend cache inputs complete without configure-time I/O.
    var directory = try cwd.openDir(io, "src/tile", .{ .iterate = true });
    defer directory.close(io);
    var walker = try directory.walk(allocator);
    defer walker.deinit();
    var inputs = std.ArrayListUnmanaged([]const u8).empty;
    while (try walker.next(io)) |entry| {
        if (entry.kind != .file or !(std.mem.endsWith(u8, entry.path, ".zig") or std.mem.endsWith(u8, entry.path, ".cu") or std.mem.endsWith(u8, entry.path, ".metal") or std.mem.endsWith(u8, entry.path, ".m")) or
            std.mem.eql(u8, entry.path, "source_inputs.zig")) continue;
        if (std.mem.startsWith(u8, entry.path, "families/native/") and !@import("region.zig").operationFamilyName(std.fs.path.stem(entry.basename))) {
            std.debug.print("native source owner must use an operation/layout name: {s}\n", .{entry.path});
            return error.ModelSpecificNativeOwner;
        }
        try inputs.append(allocator, try std.fmt.allocPrint(allocator, "src/tile/{s}", .{entry.path}));
    }
    std.mem.sort([]const u8, inputs.items, {}, struct {
        fn lessThan(_: void, a: []const u8, b: []const u8) bool {
            return std.mem.lessThan(u8, a, b);
        }
    }.lessThan);
    var manifest = std.ArrayListUnmanaged(u8).empty;
    try manifest.appendSlice(allocator, @import("source_boundary.zig").apache_header ++
        "//! Generated by tile-kernel-codegen. Build configuration does not scan directories.\n" ++
        "pub const paths = [_][]const u8{\n");
    for (inputs.items) |path| {
        try manifest.appendSlice(allocator, try std.fmt.allocPrint(allocator, "    \"{s}\",\n", .{path}));
    }
    try manifest.appendSlice(allocator, "};\n");
    const manifest_path = "src/tile/source_inputs.zig";
    if (write) {
        try cwd.writeFile(io, .{ .sub_path = manifest_path, .data = manifest.items });
    } else {
        const actual = cwd.readFileAlloc(io, manifest_path, allocator, .limited(1024 * 1024)) catch "";
        if (!std.mem.eql(u8, actual, manifest.items)) {
            std.debug.print("{s}: stale\n", .{manifest_path});
            stale += 1;
        }
    }
    const standalone_path = "src/ops/cuda/generated/standalone_programs.json";
    const standalone_data = try std.json.Stringify.valueAlloc(allocator, @import("cuda_standalone.zig").profiles, .{ .whitespace = .indent_2 });
    const standalone_output = try std.mem.concat(allocator, u8, &.{ standalone_data, "\n" });
    if (write) {
        try cwd.writeFile(io, .{ .sub_path = standalone_path, .data = standalone_output });
    } else {
        const actual = cwd.readFileAlloc(io, standalone_path, allocator, .limited(1024 * 1024)) catch "";
        if (!std.mem.eql(u8, actual, standalone_output)) {
            std.debug.print("{s}: stale\n", .{standalone_path});
            stale += 1;
        }
    }
    // Literal embed paths keep the legal/dependency graph statically auditable.
    var images = std.ArrayListUnmanaged(u8).empty;
    try images.appendSlice(allocator, @import("source_boundary.zig").apache_header ++
        "//! Generated by tile-kernel-codegen from cuda_standalone.zig.\n" ++
        "pub const images = struct {\n");
    for (@import("cuda_standalone.zig").profiles) |spec| {
        try images.appendSlice(allocator, try std.fmt.allocPrint(allocator, "    pub const @\"{s}\" = @embedFile(\"../{s}\") ++ \"\\x00\";\n", .{ spec.file, spec.file }));
    }
    try images.appendSlice(allocator, "};\n");
    const images_path = "src/ops/cuda/generated/standalone_images.zig";
    if (write) {
        try cwd.writeFile(io, .{ .sub_path = images_path, .data = images.items });
    } else {
        const actual = cwd.readFileAlloc(io, images_path, allocator, .limited(1024 * 1024)) catch "";
        if (!std.mem.eql(u8, actual, images.items)) {
            std.debug.print("{s}: stale\n", .{images_path});
            stale += 1;
        }
    }
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
        if (std.mem.count(u8, contents, r.begin) != 1 or std.mem.count(u8, contents, r.end) != 1)
            return error.DuplicateRegionBoundary;
        const body_start = begin + r.begin.len;
        const body_end = std.mem.indexOfPos(u8, contents, body_start, r.end) orelse {
            std.debug.print("{s}: missing marker {s}", .{ r.path, r.end });
            stale += 1;
            continue;
        };
        for ([_][]const u8{ "// tile-codegen:begin", "// tile-codegen:end", "// quant-kernel-codegen:begin", "// quant-kernel-codegen:end" }) |marker| {
            if (std.mem.indexOf(u8, contents[body_start..body_end], marker) != null)
                return error.OverlappingSourceOwners;
        }
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
