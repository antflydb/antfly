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
const inference = @import("inference_internal");
const readers_mod = inference.readers;

/// Exercise production loading, preprocessing, native inference and decoding
/// without linking the Antfly server or using an external inference runtime.
pub fn main(init: std.process.Init) !void {
    const allocator = std.heap.page_allocator;
    var args = std.process.Args.Iterator.init(init.minimal.args);
    _ = args.next();
    const model_dir = args.next() orelse return error.MissingModelDirectory;
    const max_tokens = try std.fmt.parseInt(usize, args.next() orelse return error.MissingMaxTokens, 10);
    if (max_tokens == 0) return error.InvalidMaxTokens;
    var image_path = args.next() orelse return error.MissingImage;

    var sessions = inference.backends.SessionManager.initWithIo(allocator, init.io);
    sessions.preferred_backends = &.{inference.backends.BackendType.native};
    sessions.required_backend = .native;
    sessions.required_backend_invalid = false;
    var models = inference.server.model_manager.ModelManager.init(allocator, sessions);
    defer models.deinit();

    std.log.info("loading native reader: {s}", .{model_dir});
    const load_start = nowNs();
    var reader = try readers_mod.LoadedReader.loadFromDir(allocator, model_dir, &models.session_manager, &models);
    defer reader.deinit();
    const load_ns = nowNs() - load_start;
    std.log.info("reader loaded in {d:.3} seconds", .{@as(f64, @floatFromInt(load_ns)) / std.time.ns_per_s});

    var output_buffer: [4096]u8 = undefined;
    var output_file = std.Io.File.stdout().writer(init.io, &output_buffer);
    const output = &output_file.interface;
    while (true) {
        const bytes = try std.Io.Dir.cwd().readFileAlloc(init.io, image_path, allocator, .limited(64 * 1024 * 1024));
        defer allocator.free(bytes);
        std.log.info("reading image: {s}", .{image_path});
        const read_start = nowNs();
        var result = try reader.read(bytes, .{ .max_tokens = max_tokens });
        defer result.deinit();
        const read_ns = nowNs() - read_start;
        const row = try std.json.Stringify.valueAlloc(allocator, .{
            .model = model_dir,
            .image = image_path,
            .backend = "native",
            .load_s = @as(f64, @floatFromInt(load_ns)) / std.time.ns_per_s,
            .inference_s = @as(f64, @floatFromInt(read_ns)) / std.time.ns_per_s,
            .text = result.text,
            .regions = result.regions,
        }, .{});
        defer allocator.free(row);
        try output.writeAll(row);
        try output.writeByte('\n');
        try output.flush();
        image_path = args.next() orelse break;
    }
}

fn nowNs() u64 {
    var ts: std.posix.timespec = undefined;
    switch (std.posix.errno(std.posix.system.clock_gettime(std.posix.CLOCK.MONOTONIC, &ts))) {
        .SUCCESS => return @intCast(@as(i128, ts.sec) * std.time.ns_per_s + ts.nsec),
        else => return 0,
    }
}
