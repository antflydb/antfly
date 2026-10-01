// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Focused executable entry point for measuring and packaging server runtimes.

const std = @import("std");
const bridge = @import("runtime_bridge.zig");
const role_options = @import("runtime_artifact_options");
const structlog = @import("structlog");
comptime {
    if (role_options.role == .inference)
        @compileError("inference artifacts must use runtime_inference_main.zig");
}

extern fn antfly_runtime_cli(context: *const bridge.Context) callconv(.c) c_int;
extern fn antfly_runtime_data(context: *const bridge.Context) callconv(.c) c_int;
extern fn antfly_runtime_inference(context: *const bridge.Context) callconv(.c) c_int;
extern fn antfly_runtime_metadata(context: *const bridge.Context) callconv(.c) c_int;
extern fn antfly_runtime_standalone(context: *const bridge.Context) callconv(.c) c_int;

pub const std_options: std.Options = .{
    .logFn = structlog.logFn,
};

pub fn main(init: std.process.Init) void {
    mainImpl(init) catch |err| {
        const message = switch (err) {
            error.FileNotFound => "required file was not found; check the configured path",
            error.AddressInUse => "listen address is already in use",
            error.InvalidCharacter, error.InvalidArguments => "invalid command-line value; run with --help",
            else => "startup failed; see the preceding diagnostic for details",
        };
        std.debug.print("antfly {s}: {s}\n", .{ @tagName(role_options.role), message });
        std.process.exit(1);
    };
}

fn mainImpl(init: std.process.Init) anyerror!void {
    structlog.init(.{ .formatter = .json, .level = .info });

    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, init.gpa);
    defer args.deinit();
    _ = args.next();

    var command: []const u8 = if (comptime role_options.role == .cli)
        args.next() orelse return error.InvalidArguments
    else
        @tagName(role_options.role);
    var argument_views: std.ArrayListUnmanaged(bridge.Bytes) = .empty;
    defer argument_views.deinit(init.gpa);
    while (args.next()) |arg| try argument_views.append(init.gpa, .init(arg));

    // The standalone artifact re-executes itself with the same private worker
    // invocation as the full CLI. Dispatch across the existing inference ABI
    // before standalone parsing; this worker is supervised by its RPC pipes.
    const worker_invocation = if (comptime role_options.role == .standalone)
        argument_views.items.len == 2 and
            std.mem.eql(u8, argument_views.items[0].slice(), "inference") and
            std.mem.eql(u8, argument_views.items[1].slice(), "_worker")
    else
        false;
    if (worker_invocation) command = "inference";
    const runtime_arguments = if (worker_invocation) argument_views.items[1..] else argument_views.items;

    const environment_names = init.environ_map.keys();
    const environment_values = init.environ_map.values();
    std.debug.assert(environment_names.len == environment_values.len);
    const environment = try init.gpa.alloc(bridge.EnvironmentEntry, environment_names.len);
    defer init.gpa.free(environment);
    for (environment, environment_names, environment_values) |*entry, name, value| {
        entry.* = .{ .name = .init(name), .value = .init(value) };
    }

    const context = bridge.Context{
        .command = .init(command),
        .arguments_ptr = if (runtime_arguments.len == 0) null else runtime_arguments.ptr,
        .arguments_len = runtime_arguments.len,
        .environment_ptr = if (environment.len == 0) null else environment.ptr,
        .environment_len = environment.len,
    };
    const code = switch (role_options.role) {
        .cli => antfly_runtime_cli(&context),
        .data => antfly_runtime_data(&context),
        .inference => unreachable,
        .metadata => antfly_runtime_metadata(&context),
        .standalone => if (worker_invocation) antfly_runtime_inference(&context) else antfly_runtime_standalone(&context),
    };
    if (code != 0) std.process.exit(@intCast(code));
}
