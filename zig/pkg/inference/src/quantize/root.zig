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

const onnx_variant = @import("onnx_variant.zig");
const gguf_export = @import("gguf_export.zig");
const safetensors_export = @import("safetensors_export.zig");
const options = @import("options.zig");

const print = std.debug.print;
const Allocator = std.mem.Allocator;

pub const Target = options.Target;
pub const Options = options.Options;

test {
    _ = onnx_variant;
    _ = gguf_export;
    _ = safetensors_export;
    _ = options;
}

pub fn main(allocator: Allocator, io: std.Io, args: []const []const u8) !void {
    return mainWithCommand(allocator, io, args, "quantize");
}

pub fn mainWithCommand(allocator: Allocator, io: std.Io, args: []const []const u8, command_name: []const u8) !void {
    const opts = parseArgs(args) catch |err| {
        printUsage(command_name);
        return err;
    };

    switch (opts.target) {
        .onnx => return onnx_variant.run(allocator, io, opts),
        .gguf => return gguf_export.run(allocator, io, opts),
        .safetensors => return safetensors_export.run(allocator, io, opts),
    }
}

fn printUsage(command_name: []const u8) void {
    print(
        \\usage: termite {s} <model-dir> [--target onnx|gguf|safetensors] [--format <format>] [--output <path>] [options]
        \\
        \\ONNX target options:
        \\  --format q8_0              Create an ONNX external-data Q8_0 variant (default)
        \\  --min-elements <n>         Minimum tensor element count to quantize (default: 1024)
        \\
        \\GGUF target options:
        \\  --format none|q1_0|q2_k|q3_k|q4_0|q4_1|q5_0|q5_1|q4_k|q5_k|q6_k|q8_k|q8_0|q8_1
        \\  --quantize <q-format>      Alias for --format
        \\  --output <dir-or-path>      For ClipClap, write the same single-repo artifact layout into this directory
        \\  --quantize-include <csv-prefixes>
        \\  --quantize-exclude <csv-prefixes>
        \\  --projector-output <path>
        \\  --projector-format auto|termite|clip
        \\  --dry-run
        \\
        \\Safetensors target options:
        \\  --format dense|native|q8_0  Export dense tensor bytes (q8_0 is accepted as the current default alias)
        \\  --dry-run
        \\
    , .{command_name});
}

fn parseArgs(args: []const []const u8) !Options {
    if (args.len == 0) return error.MissingModelDir;
    var opts = Options{ .model_dir = args[0] };
    var i: usize = 1;
    while (i < args.len) : (i += 1) {
        if (std.mem.eql(u8, args[i], "--target") and i + 1 < args.len) {
            opts.target = parseTarget(args[i + 1]) orelse return error.UnknownArgument;
            i += 1;
        } else if (std.mem.eql(u8, args[i], "--format") and i + 1 < args.len) {
            opts.format = args[i + 1];
            i += 1;
        } else if (std.mem.eql(u8, args[i], "--quantize") and i + 1 < args.len) {
            opts.format = args[i + 1];
            i += 1;
        } else if (std.mem.eql(u8, args[i], "--output") and i + 1 < args.len) {
            opts.output_path = args[i + 1];
            i += 1;
        } else if (std.mem.eql(u8, args[i], "--min-elements") and i + 1 < args.len) {
            opts.min_elements = try std.fmt.parseInt(usize, args[i + 1], 10);
            i += 1;
        } else if (std.mem.eql(u8, args[i], "--quantize-include") and i + 1 < args.len) {
            opts.quantize_include_prefixes = args[i + 1];
            i += 1;
        } else if (std.mem.eql(u8, args[i], "--quantize-exclude") and i + 1 < args.len) {
            opts.quantize_exclude_prefixes = args[i + 1];
            i += 1;
        } else if (std.mem.eql(u8, args[i], "--projector-output") and i + 1 < args.len) {
            opts.projector_output_path = args[i + 1];
            i += 1;
        } else if (std.mem.eql(u8, args[i], "--projector-format") and i + 1 < args.len) {
            opts.projector_format = args[i + 1];
            i += 1;
        } else if (std.mem.eql(u8, args[i], "--dry-run")) {
            opts.dry_run = true;
        } else {
            return error.UnknownArgument;
        }
    }
    return opts;
}

fn parseTarget(value: []const u8) ?Target {
    if (std.ascii.eqlIgnoreCase(value, "onnx")) return .onnx;
    if (std.ascii.eqlIgnoreCase(value, "gguf")) return .gguf;
    if (std.ascii.eqlIgnoreCase(value, "safetensors")) return .safetensors;
    return null;
}

test "parseArgs accepts gguf target and quantize alias" {
    const opts = try parseArgs(&.{
        "/tmp/model",
        "--target",
        "gguf",
        "--quantize",
        "q4_k",
        "--quantize-include",
        "blk.",
        "--dry-run",
    });
    try std.testing.expectEqual(Target.gguf, opts.target);
    try std.testing.expectEqualStrings("q4_k", opts.format);
    try std.testing.expectEqualStrings("blk.", opts.quantize_include_prefixes.?);
    try std.testing.expect(opts.dry_run);
}

test "parseArgs accepts safetensors target" {
    const opts = try parseArgs(&.{
        "/tmp/model",
        "--target",
        "safetensors",
        "--format",
        "dense",
        "--dry-run",
    });
    try std.testing.expectEqual(Target.safetensors, opts.target);
    try std.testing.expectEqualStrings("dense", opts.format);
    try std.testing.expect(opts.dry_run);
}
