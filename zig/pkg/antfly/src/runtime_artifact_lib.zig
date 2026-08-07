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

//! One independently code-generated runtime unit, linked into the final Antfly
//! executable through narrow C ABI entry points. A unit may own several roles
//! when they intentionally share a large dependency graph.

const builtin = @import("builtin");
const std = @import("std");
const platform = @import("antfly_platform");
const bridge = @import("runtime_bridge.zig");
const unit_options = @import("runtime_library_options");
const standalone_inference_bridge = @import("standalone/inference_bridge.zig");
const restore_staging_exports = if (unit_options.unit == .application)
    @import("standalone/restore_staging_exports.zig")
else
    struct {};
const api_kernel_exports = if (unit_options.unit == .application)
    @import("api/kernel_exports.zig")
else
    struct {};

const cli_runtime = if (unit_options.unit == .application) @import("cli_runtime.zig") else struct {};
const data_runtime = if (unit_options.unit == .application) @import("data/runtime.zig") else struct {};
const metadata_runtime = if (unit_options.unit == .application) @import("metadata/runtime.zig") else struct {};
const serverless_runtime = if (unit_options.unit == .application) @import("cmd/serverless.zig") else struct {};
const inference_runtime = if (unit_options.unit == .inference) @import("inference_runtime/runtime.zig") else struct {};
const standalone_runtime = if (unit_options.unit == .application) @import("standalone/runtime.zig") else struct {};
// Lite's non-server commands share storage types with standalone, while
// `lite serve` directly enters that runtime. Co-locating them prevents the
// focused CLI library from compiling standalone and inference again.
const lite_runtime = if (unit_options.unit == .application)
    @import("cmd/lite.zig")
else
    struct {};
const standalone_inference_host = if (unit_options.unit == .inference)
    @import("standalone/inference_host.zig")
else
    struct {};

// The user-manager storage adapter deliberately imports these through the
// compilation root so it shares their exact Zig type identity.
pub const lsm_backend = @import("storage/lsm_backend/mod.zig");
pub const storage_backend_erased = @import("storage/backend_erased.zig");

fn runtimeEntry(
    context: *const bridge.Context,
    comptime role_name: []const u8,
    comptime run: fn (std.process.Init, []const u8, *std.process.Args.Iterator) anyerror!void,
) c_int {
    const init: *const std.process.Init = @ptrCast(@alignCast(context.init));
    const args: *std.process.Args.Iterator = @ptrCast(@alignCast(context.args));
    const command = context.command_ptr[0..context.command_len];

    run(runtimeInit(init.*), command, args) catch |err| {
        const message = switch (err) {
            error.FileNotFound => "required file was not found; check the configured path",
            error.AddressInUse => "listen address is already in use",
            error.InvalidCharacter, error.InvalidArguments => "invalid command-line value; run with --help",
            else => "startup failed; see the preceding diagnostic for details",
        };
        std.debug.print("antfly {s}: {s}\n", .{ role_name, message });
        return 1;
    };
    return 0;
}

fn runCli(
    init: std.process.Init,
    command: []const u8,
    args: *std.process.Args.Iterator,
) !void {
    return cli_runtime.runFromIterator(init, command, args);
}

fn runData(init: std.process.Init, _: []const u8, args: *std.process.Args.Iterator) !void {
    return data_runtime.runFromIterator(init, "antfly", args);
}

fn runMetadata(init: std.process.Init, _: []const u8, args: *std.process.Args.Iterator) !void {
    return metadata_runtime.runFromIterator(init, "antfly", args);
}

fn runServerless(init: std.process.Init, _: []const u8, args: *std.process.Args.Iterator) !void {
    return serverless_runtime.runFromIterator(init, "antfly", args);
}

fn runInference(init: std.process.Init, _: []const u8, args: *std.process.Args.Iterator) !void {
    return inference_runtime.runFromIterator(init, "antfly", args);
}

fn runStandalone(init: std.process.Init, command: []const u8, args: *std.process.Args.Iterator) !void {
    if (std.mem.eql(u8, command, "lite")) return lite_runtime.runFromIterator(init, "antfly", args);
    return standalone_runtime.runFromIterator(init, "antfly", args);
}

fn cliEntry(context: *const bridge.Context) callconv(.c) c_int {
    return runtimeEntry(context, "cli", runCli);
}

fn dataEntry(context: *const bridge.Context) callconv(.c) c_int {
    return runtimeEntry(context, "data", runData);
}

fn metadataEntry(context: *const bridge.Context) callconv(.c) c_int {
    return runtimeEntry(context, "metadata", runMetadata);
}

fn serverlessEntry(context: *const bridge.Context) callconv(.c) c_int {
    return runtimeEntry(context, "serverless", runServerless);
}

fn inferenceEntry(context: *const bridge.Context) callconv(.c) c_int {
    return runtimeEntry(context, "inference", runInference);
}

fn standaloneEntry(context: *const bridge.Context) callconv(.c) c_int {
    return runtimeEntry(context, "standalone", runStandalone);
}

comptime {
    switch (unit_options.unit) {
        .application => {
            @export(&api_kernel_exports.create, .{ .name = "antfly_api_kernel_create" });
            @export(&api_kernel_exports.destroy, .{ .name = "antfly_api_kernel_destroy" });
            @export(&api_kernel_exports.requestStats, .{ .name = "antfly_api_kernel_request_stats" });
            @export(&api_kernel_exports.setProvider, .{ .name = "antfly_api_kernel_set_provider" });
            @export(&api_kernel_exports.setHAExecutor, .{ .name = "antfly_api_kernel_set_ha_executor" });
            @export(&api_kernel_exports.executor, .{ .name = "antfly_api_kernel_executor" });
            @export(&api_kernel_exports.streamingExecutor, .{ .name = "antfly_api_kernel_streaming_executor" });
            @export(&api_kernel_exports.attachRuntimeRestoreStore, .{ .name = "antfly_api_kernel_attach_runtime_restore_store" });
            @export(&api_kernel_exports.attachReplicatedRestoreStore, .{ .name = "antfly_api_kernel_attach_replicated_restore_store" });
            @export(&api_kernel_exports.resumeRestoreJobs, .{ .name = "antfly_api_kernel_resume_restore_jobs" });
            @export(&api_kernel_exports.pollRestoreJobs, .{ .name = "antfly_api_kernel_poll_restore_jobs" });
            @export(&api_kernel_exports.prepareRestoreLeadership, .{ .name = "antfly_api_kernel_prepare_restore_leadership" });
            @export(&api_kernel_exports.scheduleSessionMaintenance, .{ .name = "antfly_api_kernel_schedule_session_maintenance" });
            @export(&api_kernel_exports.storageMaintenanceActive, .{ .name = "antfly_api_kernel_storage_maintenance_active" });
            @export(&api_kernel_exports.handle, .{ .name = "antfly_api_kernel_handle" });
            @export(&api_kernel_exports.handleInternal, .{ .name = "antfly_api_kernel_handle_internal" });
            @export(&api_kernel_exports.handlerCreate, .{ .name = "antfly_api_kernel_handler_create" });
            @export(&api_kernel_exports.handlerInit, .{ .name = "antfly_api_kernel_handler_init" });
            @export(&api_kernel_exports.handlerStats, .{ .name = "antfly_api_kernel_handler_stats" });
            @export(&api_kernel_exports.handlerRegisterRoutes, .{ .name = "antfly_api_kernel_handler_register_routes" });
            @export(&api_kernel_exports.handlerDestroy, .{ .name = "antfly_api_kernel_handler_destroy" });
            @export(&cliEntry, .{ .name = "antfly_runtime_cli" });
            @export(&dataEntry, .{ .name = "antfly_runtime_data" });
            @export(&metadataEntry, .{ .name = "antfly_runtime_metadata" });
            @export(&serverlessEntry, .{ .name = "antfly_runtime_serverless" });
            @export(&standaloneEntry, .{ .name = "antfly_runtime_standalone" });
            @export(&restore_staging_exports.create, .{ .name = "antfly_restore_staging_create" });
            @export(&restore_staging_exports.destroy, .{ .name = "antfly_restore_staging_destroy" });
        },
        .inference => {
            @export(&inferenceEntry, .{ .name = "antfly_runtime_inference" });
            @export(&standaloneInferenceCreate, .{ .name = "antfly_standalone_inference_create" });
            @export(&standaloneInferenceConfigure, .{ .name = "antfly_standalone_inference_configure" });
            @export(&standaloneInferenceProvider, .{ .name = "antfly_standalone_inference_provider" });
            @export(&standaloneInferenceRegisterRoutes, .{ .name = "antfly_standalone_inference_register_routes" });
            @export(&standaloneInferenceDestroy, .{ .name = "antfly_standalone_inference_destroy" });
        },
    }
}

fn standaloneInferenceCreate(context: *const standalone_inference_bridge.CreateContext) callconv(.c) c_int {
    context.out_handle.* = standalone_inference_host.linkedInferenceCreate(context) catch |err| {
        return reportStandaloneInferenceFailure("create", err);
    };
    return 0;
}

fn standaloneInferenceConfigure(context: *const standalone_inference_bridge.ConfigureContext) callconv(.c) c_int {
    standalone_inference_host.linkedInferenceConfigure(context) catch |err| {
        return reportStandaloneInferenceFailure("configure", err);
    };
    return 0;
}

fn standaloneInferenceProvider(context: *const standalone_inference_bridge.ProviderContext) callconv(.c) void {
    standalone_inference_host.linkedInferenceProvider(context);
}

fn standaloneInferenceRegisterRoutes(context: *const standalone_inference_bridge.RoutesContext) callconv(.c) c_int {
    standalone_inference_host.linkedInferenceRegisterRoutes(context) catch |err| {
        return reportStandaloneInferenceFailure("register_routes", err);
    };
    return 0;
}

fn standaloneInferenceDestroy(handle: *anyopaque) callconv(.c) void {
    standalone_inference_host.linkedInferenceDestroy(handle);
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
