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

const native_export_gguf = @import("../native_export_gguf.zig");
const manifest_mod = @import("../models/manifest.zig");
const constants = @import("constants.zig");
const options = @import("options.zig");

const Allocator = std.mem.Allocator;

pub fn run(allocator: Allocator, io: std.Io, opts: options.Options) !void {
    if (opts.min_elements != constants.default_min_elements) return error.InvalidArguments;

    var argv = std.ArrayListUnmanaged([]const u8).empty;
    defer argv.deinit(allocator);

    try argv.append(allocator, opts.model_dir);
    if (opts.output_path) |output| {
        try argv.appendSlice(allocator, &.{ "--output", output });
        try runExportGguf(allocator, io, opts, argv.items);
    } else if (try isClipclapModelDir(allocator, opts.model_dir)) {
        try argv.appendSlice(allocator, &.{ "--output", opts.model_dir });
        try runExportGguf(allocator, io, opts, argv.items);
    } else {
        const output = try defaultGgufPath(allocator, opts.model_dir, opts.format);
        defer allocator.free(output);
        try argv.appendSlice(allocator, &.{ "--output", output });
        try runExportGguf(allocator, io, opts, argv.items);
    }
}

fn isClipclapModelDir(allocator: Allocator, model_dir: []const u8) !bool {
    var manifest = try manifest_mod.loadFromDir(allocator, model_dir);
    defer manifest.deinit();
    return std.mem.eql(u8, manifest.config_model_arch, "clipclap") or
        std.mem.eql(u8, manifest.inference_bundle_family, "clipclap_gguf_bundle/v1");
}

fn runExportGguf(allocator: Allocator, io: std.Io, opts: options.Options, base_args: []const []const u8) !void {
    var argv = std.ArrayListUnmanaged([]const u8).empty;
    defer argv.deinit(allocator);
    try argv.appendSlice(allocator, base_args);

    try argv.appendSlice(allocator, &.{ "--quantize", opts.format });
    if (opts.quantize_include_prefixes) |value| try argv.appendSlice(allocator, &.{ "--quantize-include", value });
    if (opts.quantize_exclude_prefixes) |value| try argv.appendSlice(allocator, &.{ "--quantize-exclude", value });
    if (opts.projector_output_path) |value| try argv.appendSlice(allocator, &.{ "--projector-output", value });
    if (opts.projector_format) |value| try argv.appendSlice(allocator, &.{ "--projector-format", value });
    if (opts.dry_run) try argv.append(allocator, "--dry-run");

    try native_export_gguf.main(allocator, io, argv.items);
}

pub fn defaultGgufPath(allocator: Allocator, source_dir: []const u8, format: []const u8) ![]u8 {
    const trimmed = trimRightSlash(source_dir);
    const base = std.fs.path.basename(trimmed);
    const parent = std.fs.path.dirname(trimmed) orelse ".";
    const filename = try std.fmt.allocPrint(allocator, "{s}-{s}.gguf", .{ base, format });
    defer allocator.free(filename);
    return std.fs.path.join(allocator, &.{ parent, filename });
}

fn trimRightSlash(value: []const u8) []const u8 {
    var end = value.len;
    while (end > 0 and value[end - 1] == '/') : (end -= 1) {}
    return value[0..end];
}

test "defaultGgufPath appends quantization format as file suffix" {
    const out = try defaultGgufPath(std.testing.allocator, "/tmp/models/antflydb/clipclap", "q4_k");
    defer std.testing.allocator.free(out);
    try std.testing.expectEqualStrings("/tmp/models/antflydb/clipclap-q4_k.gguf", out);
}
