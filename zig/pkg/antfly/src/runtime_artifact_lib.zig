// Copyright 2026 Antfly, Inc.
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

//! One independently code-generated server runtime, linked into the final
//! Antfly executable through a narrow C ABI entry point.

const builtin = @import("builtin");
const std = @import("std");
const platform = @import("antfly_platform");
const bridge = @import("runtime_bridge.zig");
const role_options = @import("runtime_artifact_options");
const standalone_inference_bridge = @import("standalone/inference_bridge.zig");

const runtime = switch (role_options.role) {
    .cli => @import("cli_runtime.zig"),
    .data => @import("data/runtime.zig"),
    .inference => @import("inference_runtime/runtime.zig"),
    .metadata => @import("metadata/runtime.zig"),
    .standalone => @import("standalone/runtime.zig"),
};
const standalone_runtime = if (role_options.role == .inference)
    @import("standalone/runtime.zig")
else
    struct {};

// The user-manager storage adapter deliberately imports these through the
// compilation root so it shares their exact Zig type identity.
pub const lsm_backend = @import("storage/lsm_backend/mod.zig");
pub const storage_backend_erased = @import("storage/backend_erased.zig");

fn runtimeEntry(context: *const bridge.Context) callconv(.c) c_int {
    const init: *const std.process.Init = @ptrCast(@alignCast(context.init));
    const args: *std.process.Args.Iterator = @ptrCast(@alignCast(context.args));
    const command = context.command_ptr[0..context.command_len];
    const argv0 = if (role_options.role == .cli)
        command
    else
        "antfly";

    runtime.runFromIterator(runtimeInit(init.*), argv0, args) catch |err| {
        const message = switch (err) {
            error.FileNotFound => "required file was not found; check the configured path",
            error.AddressInUse => "listen address is already in use",
            error.InvalidCharacter, error.InvalidArguments => "invalid command-line value; run with --help",
            else => "startup failed; see the preceding diagnostic for details",
        };
        std.debug.print("antfly {s}: {s}\n", .{ @tagName(role_options.role), message });
        return 1;
    };
    return 0;
}

comptime {
    @export(&runtimeEntry, .{ .name = "antfly_runtime_" ++ @tagName(role_options.role) });
    if (role_options.role == .inference) {
        @export(&standaloneInferenceCreate, .{ .name = "antfly_standalone_inference_create" });
        @export(&standaloneInferenceConfigure, .{ .name = "antfly_standalone_inference_configure" });
        @export(&standaloneInferenceProvider, .{ .name = "antfly_standalone_inference_provider" });
        @export(&standaloneInferenceRegisterRoutes, .{ .name = "antfly_standalone_inference_register_routes" });
        @export(&standaloneInferenceDestroy, .{ .name = "antfly_standalone_inference_destroy" });
    }
}

fn standaloneInferenceCreate(context: *const standalone_inference_bridge.CreateContext) callconv(.c) c_int {
    context.out_handle.* = standalone_runtime.linkedInferenceCreate(context) catch |err| {
        return reportStandaloneInferenceFailure("create", err);
    };
    return 0;
}

fn standaloneInferenceConfigure(context: *const standalone_inference_bridge.ConfigureContext) callconv(.c) c_int {
    standalone_runtime.linkedInferenceConfigure(context) catch |err| {
        return reportStandaloneInferenceFailure("configure", err);
    };
    return 0;
}

fn standaloneInferenceProvider(context: *const standalone_inference_bridge.ProviderContext) callconv(.c) void {
    standalone_runtime.linkedInferenceProvider(context);
}

fn standaloneInferenceRegisterRoutes(context: *const standalone_inference_bridge.RoutesContext) callconv(.c) c_int {
    standalone_runtime.linkedInferenceRegisterRoutes(context) catch |err| {
        return reportStandaloneInferenceFailure("register_routes", err);
    };
    return 0;
}

fn standaloneInferenceDestroy(handle: *anyopaque) callconv(.c) void {
    standalone_runtime.linkedInferenceDestroy(handle);
}

fn reportStandaloneInferenceFailure(comptime operation: []const u8, err: anyerror) c_int {
    std.log.err("standalone inference bridge failed operation={s} err={}", .{ operation, err });
    return 1;
}

fn runtimeInit(init: std.process.Init) std.process.Init {
    return .{
        .minimal = init.minimal,
        .arena = init.arena,
        .gpa = runtimeAllocator(init),
        .io = init.io,
        .environ_map = init.environ_map,
        .preopens = init.preopens,
    };
}

fn runtimeAllocator(init: std.process.Init) std.mem.Allocator {
    const fallback = if (!builtin.single_threaded) std.heap.smp_allocator else init.gpa;
    return platform.allocator.processAllocator(fallback);
}
