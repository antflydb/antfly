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
//! executable through narrow C ABI entry points.

const std = @import("std");
const platform = @import("antfly_platform");
const structlog = @import("structlog");
const bridge = @import("runtime_bridge.zig");
const unit_options = @import("runtime_library_options");
const owns_application_runtime = unit_options.unit == .application or unit_options.unit == .distributed;
const owns_api_kernel = unit_options.unit == .application or unit_options.unit == .api_kernel;
const standalone_inference_bridge = @import("standalone/inference_bridge.zig");
const restore_staging_exports = if (owns_application_runtime)
    @import("standalone/restore_staging_exports.zig")
else
    struct {};

const cli_runtime = if (unit_options.unit == .cli) @import("cli_runtime.zig") else struct {};
const data_runtime = if (owns_application_runtime) @import("data/runtime.zig") else struct {};
const graph_metric_maintenance_runtime = if (owns_application_runtime) @import("cmd/graph_metric_maintenance.zig") else struct {};
const ha_runtime = if (owns_application_runtime) @import("cmd/ha.zig") else struct {};
const metadata_runtime = if (owns_application_runtime) @import("metadata/runtime.zig") else struct {};
const standalone_runtime = if (owns_application_runtime) @import("standalone/runtime.zig") else struct {};
const lite_runtime = if (owns_application_runtime) @import("cmd/lite.zig") else struct {};
const serverless_runtime = if (owns_application_runtime) @import("cmd/serverless.zig") else struct {};
const api_kernel_exports = if (owns_api_kernel) @import("api/kernel_exports.zig") else struct {};
const storage_kernel_exports = if (unit_options.unit == .distributed) @import("capi/db.zig") else struct {};
const inference_runtime_entry = if (unit_options.unit == .inference) @import("inference_runtime/runtime.zig") else struct {};
const standalone_inference_host = if (unit_options.unit == .inference)
    @import("standalone/inference_host.zig")
else
    struct {};

pub const build_options = @import("build_options");
pub const aggregation = @import("search/aggregation.zig");
pub const admin = @import("admin/mod.zig");
pub const backup_codec = @import("storage/backup_codec.zig");
pub const common = @import("common/mod.zig");
pub const data = @import("data/mod.zig");
pub const db = @import("storage/db/mod.zig");
pub const graph = @import("graph/graph.zig");
pub const geo = @import("search/geo.zig");
pub const graph_pattern = @import("graph/pattern.zig");
pub const graph_query = @import("graph/query.zig");
pub const hbc = @import("storage/hbc_adapter.zig");
pub const ha = @import("storage/ha/mod.zig");
pub const inference_runtime = if (unit_options.unit == .inference) @import("inference_runtime/runtime.zig") else struct {};
pub const lite = @import("storage/lite/mod.zig");
pub const metadata = @import("metadata/mod.zig");
pub const metadata_api = @import("metadata/api.zig");
pub const platform_clock = @import("antfly_platform").clock;
pub const platform_time = @import("antfly_platform").time;
pub const paths = @import("graph/paths.zig");
pub const portable_backup = @import("storage/portable_backup.zig");
pub const public_api = @import("api/mod.zig");
pub const raft = @import("raft/mod.zig");
pub const schema = @import("storage/schema.zig");
pub const serverless = @import("serverless/mod.zig");
pub const standalone = @import("standalone/mod.zig");
pub const table_schema = @import("schema/mod.zig");
pub const transactions = @import("storage/transactions.zig");
pub const traversal = @import("graph/traversal.zig");
pub const lmdb_engine = @import("lmdb_engine");
pub const lsm_backend = @import("storage/lsm_backend/mod.zig");
pub const storage_backend_erased = @import("storage/backend_erased.zig");

pub const std_options: std.Options = .{
    .logFn = structlog.logFn,
};

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
    const fallback = if (!@import("builtin").single_threaded) std.heap.smp_allocator else init.gpa;
    return platform.allocator.processAllocator(fallback);
}

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

fn runCli(init: std.process.Init, command: []const u8, args: *std.process.Args.Iterator) !void {
    return cli_runtime.runFromIterator(init, command, args);
}

fn runData(init: std.process.Init, _: []const u8, args: *std.process.Args.Iterator) !void {
    return data_runtime.runFromIterator(init, "antfly", args);
}

fn runGraphMetricMaintenance(init: std.process.Init, _: []const u8, args: *std.process.Args.Iterator) !void {
    return graph_metric_maintenance_runtime.runFromIterator(init, "antfly", args);
}

fn runHa(init: std.process.Init, _: []const u8, args: *std.process.Args.Iterator) !void {
    return ha_runtime.runFromIterator(init, "antfly", args);
}

fn runMetadata(init: std.process.Init, _: []const u8, args: *std.process.Args.Iterator) !void {
    return metadata_runtime.runFromIterator(init, "antfly", args);
}

fn runServerless(init: std.process.Init, _: []const u8, args: *std.process.Args.Iterator) !void {
    return serverless_runtime.runFromIterator(init, "antfly", args);
}

fn runInference(init: std.process.Init, _: []const u8, args: *std.process.Args.Iterator) !void {
    return inference_runtime_entry.runFromIterator(init, "antfly", args);
}

fn runStandalone(init: std.process.Init, command: []const u8, args: *std.process.Args.Iterator) !void {
    if (std.mem.eql(u8, command, "lite")) return lite_runtime.runFromIterator(init, "antfly", args);
    if (std.mem.eql(u8, command, "sql")) return lite_runtime.runSqlFromIterator(init, args);
    return standalone_runtime.runFromIterator(init, "antfly", args);
}

fn cliEntry(context: *const bridge.Context) callconv(.c) c_int {
    return runtimeEntry(context, "cli", runCli);
}

fn dataEntry(context: *const bridge.Context) callconv(.c) c_int {
    return runtimeEntry(context, "data", runData);
}

fn graphMetricMaintenanceEntry(context: *const bridge.Context) callconv(.c) c_int {
    return runtimeEntry(context, "__graph-metric-maintenance", runGraphMetricMaintenance);
}

fn haEntry(context: *const bridge.Context) callconv(.c) c_int {
    return runtimeEntry(context, "ha", runHa);
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

fn exportInternal(comptime function: anytype, comptime name: []const u8) void {
    @export(function, .{ .name = name, .visibility = .hidden });
}

comptime {
    if (owns_api_kernel) {
        exportInternal(&api_kernel_exports.create, "antfly_api_kernel_create");
        exportInternal(&api_kernel_exports.destroy, "antfly_api_kernel_destroy");
        exportInternal(&api_kernel_exports.requestStats, "antfly_api_kernel_request_stats");
        exportInternal(&api_kernel_exports.setProvider, "antfly_api_kernel_set_provider");
        exportInternal(&api_kernel_exports.setHAExecutor, "antfly_api_kernel_set_ha_executor");
        exportInternal(&api_kernel_exports.executor, "antfly_api_kernel_executor");
        exportInternal(&api_kernel_exports.streamingExecutor, "antfly_api_kernel_streaming_executor");
        exportInternal(&api_kernel_exports.attachRuntimeRestoreStore, "antfly_api_kernel_attach_runtime_restore_store");
        exportInternal(&api_kernel_exports.attachReplicatedRestoreStore, "antfly_api_kernel_attach_replicated_restore_store");
        exportInternal(&api_kernel_exports.resumeRestoreJobs, "antfly_api_kernel_resume_restore_jobs");
        exportInternal(&api_kernel_exports.pollRestoreJobs, "antfly_api_kernel_poll_restore_jobs");
        exportInternal(&api_kernel_exports.prepareRestoreLeadership, "antfly_api_kernel_prepare_restore_leadership");
        exportInternal(&api_kernel_exports.scheduleSessionMaintenance, "antfly_api_kernel_schedule_session_maintenance");
        exportInternal(&api_kernel_exports.storageMaintenanceActive, "antfly_api_kernel_storage_maintenance_active");
        exportInternal(&api_kernel_exports.handle, "antfly_api_kernel_handle");
        exportInternal(&api_kernel_exports.handleInternal, "antfly_api_kernel_handle_internal");
        exportInternal(&api_kernel_exports.handlerCreate, "antfly_api_kernel_handler_create");
        exportInternal(&api_kernel_exports.handlerInit, "antfly_api_kernel_handler_init");
        exportInternal(&api_kernel_exports.handlerStats, "antfly_api_kernel_handler_stats");
        exportInternal(&api_kernel_exports.handlerRegisterRoutes, "antfly_api_kernel_handler_register_routes");
        exportInternal(&api_kernel_exports.handlerDestroy, "antfly_api_kernel_handler_destroy");
    }
    if (owns_application_runtime) {
        if (unit_options.unit == .distributed) _ = storage_kernel_exports;
        exportInternal(&dataEntry, "antfly_runtime_data");
        exportInternal(&graphMetricMaintenanceEntry, "antfly_runtime_graph_metric_maintenance");
        exportInternal(&haEntry, "antfly_runtime_ha");
        exportInternal(&metadataEntry, "antfly_runtime_metadata");
        exportInternal(&serverlessEntry, "antfly_runtime_serverless");
        exportInternal(&standaloneEntry, "antfly_runtime_standalone");
        exportInternal(&restore_staging_exports.create, "antfly_restore_staging_create");
        exportInternal(&restore_staging_exports.destroy, "antfly_restore_staging_destroy");
    }
    if (unit_options.unit == .inference) {
        exportInternal(&inferenceEntry, "antfly_runtime_inference");
        exportInternal(&standaloneInferenceCreate, "antfly_standalone_inference_create");
        exportInternal(&standaloneInferenceConfigure, "antfly_standalone_inference_configure");
        exportInternal(&standaloneInferenceProvider, "antfly_standalone_inference_provider");
        exportInternal(&standaloneInferenceRegisterRoutes, "antfly_standalone_inference_register_routes");
        exportInternal(&standaloneInferenceDestroy, "antfly_standalone_inference_destroy");
    }
    if (unit_options.unit == .cli) exportInternal(&cliEntry, "antfly_runtime_cli");
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
