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

//! Structured decision and label-set CLI shares serving validation/execution.
//!
//! Several `--request` files run against one warm node, in order, one JSON
//! response per line. `--repeat N` (N > 1) re-runs the whole list N times,
//! interleaving the requests, and reports per-request latency (median and
//! minimum over the repeats, the first pass included) on stderr.
const std = @import("std");
const Node = @import("server/server.zig").Node;
const file = @import("util/c_file.zig");
const backend_choice = @import("native_backend_choice.zig");

pub fn main(a: std.mem.Allocator, io: std.Io, args: []const []const u8, extract: bool) !void {
    const usage = "usage: antfly inference {s} <models-dir> --request <json-file> [--request <json-file> ...] [--backend native|metal|auto] [--repeat N]\n";
    if (args.len < 3) {
        std.debug.print(usage, .{if (extract) "extract" else "decisions"});
        return error.InvalidArguments;
    }
    var backend: backend_choice.Choice = .auto;
    var repeat: usize = 1;
    var paths: std.ArrayList([]const u8) = .empty;
    defer paths.deinit(a);
    var i: usize = 1;
    while (i < args.len) : (i += 2) {
        if (i + 1 >= args.len) {
            std.debug.print(usage, .{if (extract) "extract" else "decisions"});
            return error.InvalidArguments;
        }
        if (std.mem.eql(u8, args[i], "--request")) {
            try paths.append(a, args[i + 1]);
        } else if (std.mem.eql(u8, args[i], "--backend")) {
            backend = backend_choice.parse(args[i + 1]) orelse return error.InvalidBackend;
        } else if (std.mem.eql(u8, args[i], "--repeat")) {
            repeat = std.fmt.parseInt(usize, args[i + 1], 10) catch return error.InvalidArguments;
            if (repeat == 0) return error.InvalidArguments;
        } else {
            std.debug.print(usage, .{if (extract) "extract" else "decisions"});
            return error.InvalidArguments;
        }
    }
    if (paths.items.len == 0) {
        std.debug.print(usage, .{if (extract) "extract" else "decisions"});
        return error.InvalidArguments;
    }
    try backend_choice.validate(backend);
    const requests = try a.alloc([]u8, paths.items.len);
    var loaded: usize = 0;
    defer {
        for (requests[0..loaded]) |request| a.free(request);
        a.free(requests);
    }
    for (paths.items, requests) |path, *request| {
        request.* = try file.readFileMax(a, path, 16 * 1024 * 1024);
        loaded += 1;
    }
    var node = try Node.init(a, .{ .models_dir = args[0], .allow_unknown_models = true, .process_termination_available = true });
    defer node.deinit();
    try node.attachIo(io);
    backend_choice.configureSessionPreference(&node.session_manager, backend);
    backend_choice.configureSessionPreference(&node.model_manager.session_manager, backend);
    var buffer: [4096]u8 = undefined;
    var stdout = std.Io.File.stdout().writer(io, &buffer);
    const timings = try a.alloc(u64, requests.len * repeat);
    defer a.free(timings);
    for (0..repeat) |pass| for (requests, 0..) |request, index| {
        const started = std.Io.Timestamp.now(io, .awake);
        if (extract) {
            var response = try node.extractV2DirectJsonWithControl(a, request, null);
            defer response.deinit();
            timings[index * repeat + pass] = elapsedNs(io, started);
            if (pass == 0) try writeLine(&stdout.interface, response.json);
        } else {
            const response = try node.decideDirectJsonWithControl(a, request, null);
            defer a.free(response);
            timings[index * repeat + pass] = elapsedNs(io, started);
            if (pass == 0) try writeLine(&stdout.interface, response);
        }
    };
    try stdout.interface.flush();
    if (repeat > 1) for (paths.items, 0..) |path, index| {
        const samples = timings[index * repeat ..][0..repeat];
        std.mem.sort(u64, samples, {}, std.sort.asc(u64));
        const median = if (repeat % 2 == 1) samples[repeat / 2] else (samples[repeat / 2 - 1] + samples[repeat / 2]) / 2;
        std.debug.print("latency {s}: median {d:.1} ms, min {d:.1} ms over {d} runs\n", .{ path, toMs(median), toMs(samples[0]), repeat });
    };
}

fn writeLine(writer: *std.Io.Writer, bytes: []const u8) !void {
    try writer.writeAll(bytes);
    try writer.writeAll("\n");
}

fn elapsedNs(io: std.Io, started: std.Io.Timestamp) u64 {
    const nanos = std.Io.Timestamp.durationTo(started, std.Io.Timestamp.now(io, .awake)).nanoseconds;
    return if (nanos <= 0) 0 else @intCast(nanos);
}

fn toMs(ns: u64) f64 {
    return @as(f64, @floatFromInt(ns)) / std.time.ns_per_ms;
}
