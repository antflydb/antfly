// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Structured decision and label-set CLI shares serving validation/execution.
const std = @import("std");
const Node = @import("server/server.zig").Node;
const file = @import("util/c_file.zig");
const backend_choice = @import("native_backend_choice.zig");

pub fn main(a: std.mem.Allocator, io: std.Io, args: []const []const u8, extract: bool) !void {
    if (args.len < 3 or !std.mem.eql(u8, args[1], "--request")) {
        std.debug.print("usage: antfly inference {s} <models-dir> --request <json-file> [--backend native|metal|auto]\n", .{if (extract) "extract" else "decisions"});
        return error.InvalidArguments;
    }
    var backend: backend_choice.Choice = .auto;
    if (args.len > 3) {
        if (args.len != 5 or !std.mem.eql(u8, args[3], "--backend")) return error.InvalidArguments;
        backend = backend_choice.parse(args[4]) orelse return error.InvalidBackend;
    }
    try backend_choice.validate(backend);
    const request = try file.readFileMax(a, args[2], 16 * 1024 * 1024);
    defer a.free(request);
    var node = try Node.init(a, .{ .models_dir = args[0], .allow_unknown_models = true, .process_termination_available = true });
    defer node.deinit();
    try node.attachIo(io);
    backend_choice.configureSessionPreference(&node.session_manager, backend);
    backend_choice.configureSessionPreference(&node.model_manager.session_manager, backend);
    var buffer: [4096]u8 = undefined;
    var stdout = std.Io.File.stdout().writer(io, &buffer);
    if (extract) {
        var response = try node.extractV2DirectJsonWithControl(a, request, null);
        defer response.deinit();
        try stdout.interface.writeAll(response.json);
    } else {
        const response = try node.decideDirectJsonWithControl(a, request, null);
        defer a.free(response);
        try stdout.interface.writeAll(response);
    }
    try stdout.interface.writeAll("\n");
    try stdout.interface.flush();
}
