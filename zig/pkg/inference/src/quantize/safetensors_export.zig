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

const native_export_safetensors = @import("../native_export_safetensors.zig");
const constants = @import("constants.zig");
const options = @import("options.zig");

const Allocator = std.mem.Allocator;

pub fn run(allocator: Allocator, io: std.Io, opts: options.Options) !void {
    if (!std.mem.eql(u8, opts.format, constants.default_format) and
        !std.mem.eql(u8, opts.format, "native") and
        !std.mem.eql(u8, opts.format, "dense"))
    {
        return error.UnsupportedQuantizationFormat;
    }
    if (opts.min_elements != constants.default_min_elements or
        opts.quantize_include_prefixes != null or
        opts.quantize_exclude_prefixes != null or
        opts.projector_output_path != null or
        opts.projector_format != null)
    {
        return error.InvalidArguments;
    }

    var argv = std.ArrayListUnmanaged([]const u8).empty;
    defer argv.deinit(allocator);

    try argv.append(allocator, opts.model_dir);
    if (opts.output_path) |output| {
        try argv.appendSlice(allocator, &.{ "--output", output });
    }
    if (opts.dry_run) try argv.append(allocator, "--dry-run");

    try native_export_safetensors.main(allocator, io, argv.items);
}

pub fn defaultSafetensorsPath(allocator: Allocator, source_dir: []const u8) ![]u8 {
    const trimmed = trimRightSlash(source_dir);
    const base = std.fs.path.basename(trimmed);
    const parent = std.fs.path.dirname(trimmed) orelse ".";
    const filename = try std.fmt.allocPrint(allocator, "{s}.safetensors", .{base});
    defer allocator.free(filename);
    return std.fs.path.join(allocator, &.{ parent, filename });
}

fn trimRightSlash(value: []const u8) []const u8 {
    var end = value.len;
    while (end > 0 and value[end - 1] == '/') : (end -= 1) {}
    return value[0..end];
}

test "defaultSafetensorsPath writes sibling safetensors file" {
    const out = try defaultSafetensorsPath(std.testing.allocator, "/tmp/models/antflydb/clipclap");
    defer std.testing.allocator.free(out);
    try std.testing.expectEqualStrings("/tmp/models/antflydb/clipclap.safetensors", out);
}
