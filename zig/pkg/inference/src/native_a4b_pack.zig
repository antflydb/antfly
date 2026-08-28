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
const compat = @import("io/compat.zig");
const session_factory = @import("architectures/session_factory.zig");
const prepared_pack = @import("ops/cuda/a4b_prepared_pack.zig");

const Options = struct {
    model_path: []const u8,
    output_path: ?[]const u8 = null,
    shards: u8 = 4,
    verify: bool = false,
};

fn parse(args: []const []const u8) !Options {
    if (args.len == 0) return error.MissingModelPath;
    var options = Options{ .model_path = args[0] };
    var index: usize = 1;
    while (index < args.len) : (index += 1) {
        if (std.mem.eql(u8, args[index], "--output")) {
            index += 1;
            if (index >= args.len) return error.MissingOutputPath;
            options.output_path = args[index];
        } else if (std.mem.eql(u8, args[index], "--shards")) {
            index += 1;
            if (index >= args.len) return error.MissingShardCount;
            options.shards = try std.fmt.parseInt(u8, args[index], 10);
            if (options.shards == 0 or options.shards > prepared_pack.max_shards)
                return error.InvalidShardCount;
        } else if (std.mem.eql(u8, args[index], "--verify")) {
            options.verify = true;
        } else if (std.mem.eql(u8, args[index], "--help") or std.mem.eql(u8, args[index], "-h")) {
            return error.HelpRequested;
        } else {
            return error.UnknownArgument;
        }
    }
    return options;
}

fn usage() void {
    std.debug.print(
        \\Usage: antfly-inference a4b-pack <model-dir> [--output <dir>] [--shards <1..8>] [--verify]
        \\
        \\Creates a versioned, pre-sharded CUDA expert pack. The output is
        \\<model-dir>/a4b-cuda-pack-v1 unless --output is specified. Creation
        \\is exclusive and atomic; an existing output is never overwritten.
        \\--verify reads every installed default-pack shard and checks SHA-256.
        \\
    , .{});
}

pub fn main(allocator: std.mem.Allocator, io: std.Io, args: []const []const u8) !void {
    const options = parse(args) catch |err| {
        usage();
        if (err == error.HelpRequested) return;
        return err;
    };
    if (options.verify) {
        if (options.output_path != null) return error.VerifyCustomOutputUnsupported;
        const report = try session_factory.verifyCudaA4bPreparedPack(allocator, options.model_path);
        var stdout_buffer: [1024]u8 = undefined;
        var stdout = std.Io.File.stdout().writer(io, &stdout_buffer);
        try stdout.interface.print(
            "a4b prepared pack verified shards={d} sources={d} bytes={d}\n",
            .{ report.shard_count, report.source_count, report.total_source_bytes },
        );
        try stdout.interface.flush();
        return;
    }
    const owned_output = if (options.output_path == null)
        try std.fs.path.join(allocator, &.{ options.model_path, prepared_pack.default_directory_name })
    else
        null;
    defer if (owned_output) |path| allocator.free(path);
    const output_path = options.output_path orelse owned_output.?;
    const report = try session_factory.writeCudaA4bPreparedPack(
        allocator,
        io,
        options.model_path,
        output_path,
        options.shards,
    );
    var stdout_buffer: [1024]u8 = undefined;
    var stdout = std.Io.File.stdout().writer(io, &stdout_buffer);
    try stdout.interface.print(
        "a4b prepared pack complete output={s} shards={d} sources={d} bytes={d}\n",
        .{ output_path, report.shard_count, report.source_count, report.total_source_bytes },
    );
    try stdout.interface.flush();
}

test "A4B prepared pack CLI parses bounded shard count" {
    const options = try parse(&.{ "model", "--output", "pack", "--shards", "8" });
    try std.testing.expectEqualStrings("model", options.model_path);
    try std.testing.expectEqualStrings("pack", options.output_path.?);
    try std.testing.expectEqual(@as(u8, 8), options.shards);
    try std.testing.expect((try parse(&.{ "model", "--verify" })).verify);
    try std.testing.expectError(error.InvalidShardCount, parse(&.{ "model", "--shards", "9" }));
}
