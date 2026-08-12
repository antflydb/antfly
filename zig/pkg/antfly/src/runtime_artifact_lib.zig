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
const restore_staging_exports = if (unit_options.unit == .distributed)
    @import("standalone/restore_staging_exports.zig")
else
    struct {};
const api_kernel_exports = if (unit_options.unit == .api_kernel)
    @import("api/kernel_exports.zig")
else
    struct {};
const storage_kernel_exports = if (unit_options.unit == .distributed)
    @import("capi/db.zig")
else
    struct {};

const cli_runtime = if (unit_options.unit == .cli) @import("cli_runtime.zig") else struct {};
// Local HA administration owns storage handles and seed lifecycle artifacts.
// Keep it with the distributed/storage unit so the small remote-client CLI
// archive does not code-generate a second copy of the HA storage closure.
const ha_runtime = if (unit_options.unit == .distributed) @import("cmd/ha.zig") else struct {};
const data_runtime = if (unit_options.unit == .distributed) @import("data/runtime.zig") else struct {};
const metadata_runtime = if (unit_options.unit == .distributed) @import("metadata/runtime.zig") else struct {};
const serverless_runtime = if (unit_options.unit == .distributed) @import("cmd/serverless.zig") else struct {};
const inference_runtime = if (unit_options.unit == .inference) @import("inference_runtime/runtime.zig") else struct {};
// Standalone adds about 35 seconds when co-generated with the server roles but
// costs 6 minutes and 8 GiB as a separate ARM64 Linux unit. Keep it co-located
// until the shared storage kernel removes that duplicated LLVM work.
const standalone_runtime = if (unit_options.unit == .distributed) @import("standalone/runtime.zig") else struct {};
// Lite's non-server commands share storage types with standalone, while
// `lite serve` directly enters that runtime. Co-locating Lite and the server
// roles gives them one storage type identity and one LLVM unit.
const lite_runtime = if (unit_options.unit == .distributed)
    @import("cmd/lite.zig")
else
    struct {};
const standalone_inference_host = if (unit_options.unit == .inference)
    @import("standalone/inference_host.zig")
else
    struct {};

// The embedded CAPI imports the distributed compilation root as its focused
// storage facade so every shared file has one Zig module/type identity. The
// user-manager adapter likewise imports its storage types through this root.
pub const aggregation = @import("search/aggregation.zig");
pub const backup_codec = @import("storage/backup_codec.zig");
pub const db = @import("storage/db/mod.zig");
pub const geo = @import("search/geo.zig");
pub const graph = @import("graph/graph.zig");
pub const graph_pattern = @import("graph/pattern.zig");
pub const graph_query = @import("graph/query.zig");
pub const hbc = @import("storage/hbc_adapter.zig");
pub const lite = @import("storage/lite/mod.zig");
pub const lsm_backend = @import("storage/lsm_backend/mod.zig");
pub const paths = @import("graph/paths.zig");
pub const platform_clock = @import("antfly_platform").clock;
pub const platform_time = @import("antfly_platform").time;
pub const portable_backup = @import("storage/portable_backup.zig");
pub const public_api = @import("api/mod.zig");
pub const raft = @import("raft/mod.zig");
pub const storage_backend_erased = @import("storage/backend_erased.zig");
pub const transactions = @import("storage/transactions.zig");
pub const traversal = @import("graph/traversal.zig");

fn runtimeEntry(
    context: *const bridge.Context,
    comptime role_name: []const u8,
    comptime run: fn (std.process.Init, []const u8, *std.process.Args.Iterator) anyerror!void,
) c_int {
    if (!context.valid()) {
        std.debug.print("antfly {s}: invalid runtime process ABI context\n", .{role_name});
        return 1;
    }
    var process = RuntimeProcess.init(context) catch |err| {
        std.debug.print("antfly {s}: failed to initialize runtime process context (error.{s})\n", .{ role_name, @errorName(err) });
        return 1;
    };
    defer process.deinit();
    var args = std.process.Args.Iterator.initAllocator(process.processArgs(), process.alloc) catch |err| {
        std.debug.print("antfly {s}: failed to initialize runtime arguments (error.{s})\n", .{ role_name, @errorName(err) });
        return 1;
    };
    defer args.deinit();
    _ = args.next(); // synthetic argv[0], owned by this runtime unit
    const command = context.command.slice();

    run(process.processInit(), command, &args) catch |err| {
        if (@errorReturnTrace()) |trace| std.debug.dumpErrorReturnTrace(trace);
        const message = switch (err) {
            error.FileNotFound => "required file was not found; check the configured path",
            error.AddressInUse => "listen address is already in use",
            error.InvalidCharacter, error.InvalidArguments => "invalid command-line value; run with --help",
            else => "startup failed; see the preceding diagnostic for details",
        };
        std.debug.print("antfly {s}: {s} (error.{s})\n", .{ role_name, message, @errorName(err) });
        return 1;
    };
    return 0;
}

const RuntimeProcess = struct {
    alloc: std.mem.Allocator,
    arena: std.heap.ArenaAllocator,
    io_impl: std.Io.Threaded,
    process_environ: std.process.Environ,
    environ_map: std.process.Environ.Map,
    argument_storage: [][:0]u8,
    argument_ptrs: [][*:0]const u8,
    preopens: std.process.Preopens,

    fn init(context: *const bridge.Context) !RuntimeProcess {
        const alloc = runtimeAllocator();
        const input_arguments = context.arguments() orelse return error.InvalidArgument;
        const argument_storage = try alloc.alloc([:0]u8, input_arguments.len + 1);
        errdefer alloc.free(argument_storage);
        var initialized_arguments: usize = 0;
        errdefer for (argument_storage[0..initialized_arguments]) |argument| alloc.free(argument);
        argument_storage[0] = try alloc.dupeZ(u8, "antfly-runtime");
        initialized_arguments = 1;
        for (input_arguments, 1..) |argument, index| {
            argument_storage[index] = try alloc.dupeZ(u8, argument.slice());
            initialized_arguments += 1;
        }
        const argument_ptrs = try alloc.alloc([*:0]const u8, argument_storage.len);
        errdefer alloc.free(argument_ptrs);
        for (argument_storage, argument_ptrs) |argument, *pointer| pointer.* = argument.ptr;

        var environ_map = std.process.Environ.Map.init(alloc);
        errdefer environ_map.deinit();
        for (context.environment() orelse return error.InvalidArgument) |entry| {
            if (!std.process.Environ.Map.validateKeyForPut(entry.name.slice()) or
                std.mem.indexOfScalar(u8, entry.value.slice(), 0) != null)
                return error.InvalidArgument;
            try environ_map.put(entry.name.slice(), entry.value.slice());
        }

        var arena = std.heap.ArenaAllocator.init(alloc);
        errdefer arena.deinit();
        const preopens = try std.process.Preopens.init(arena.allocator());
        const process_environ: std.process.Environ = switch (builtin.os.tag) {
            .windows, .wasi => @compileError("partitioned Antfly runtime process ABI currently requires a POSIX host"),
            else => .{ .block = try environ_map.createPosixBlock(alloc, .{}) },
        };
        errdefer process_environ.block.deinit(alloc);
        const io_impl = std.Io.Threaded.init(alloc, .{ .environ = process_environ });

        return .{
            .alloc = alloc,
            .arena = arena,
            .io_impl = io_impl,
            .process_environ = process_environ,
            .environ_map = environ_map,
            .argument_storage = argument_storage,
            .argument_ptrs = argument_ptrs,
            .preopens = preopens,
        };
    }

    fn deinit(self: *RuntimeProcess) void {
        self.io_impl.deinit();
        self.process_environ.block.deinit(self.alloc);
        self.environ_map.deinit();
        self.arena.deinit();
        for (self.argument_storage) |argument| self.alloc.free(argument);
        self.alloc.free(self.argument_storage);
        self.alloc.free(self.argument_ptrs);
        self.* = undefined;
    }

    fn processArgs(self: *const RuntimeProcess) std.process.Args {
        switch (builtin.os.tag) {
            .windows => @compileError("partitioned Antfly runtime process ABI does not yet support Windows"),
            .wasi => @compileError("partitioned Antfly runtime process ABI does not support WASI"),
            else => return .{ .vector = self.argument_ptrs },
        }
    }

    fn processInit(self: *RuntimeProcess) std.process.Init {
        return .{
            .minimal = .{ .args = self.processArgs(), .environ = self.process_environ },
            .arena = &self.arena,
            .gpa = self.alloc,
            .io = self.io_impl.io(),
            .environ_map = &self.environ_map,
            .preopens = self.preopens,
        };
    }
};

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
    switch (unit_options.unit) {
        .api_kernel => {
            exportInternal(&api_kernel_exports.getFunctionTable, "antfly_api_kernel_get_function_table");
        },
        .distributed => {
            // Importing the C ABI implementation makes its `pub export`
            // declarations roots of this PIC archive. The executable and both
            // C ABI library names link this exact compiled artifact.
            _ = storage_kernel_exports;
            exportInternal(&dataEntry, "antfly_runtime_data");
            exportInternal(&haEntry, "antfly_runtime_ha");
            exportInternal(&metadataEntry, "antfly_runtime_metadata");
            exportInternal(&serverlessEntry, "antfly_runtime_serverless");
            exportInternal(&standaloneEntry, "antfly_runtime_standalone");
            exportInternal(&restore_staging_exports.create, "antfly_restore_staging_create");
            exportInternal(&restore_staging_exports.destroy, "antfly_restore_staging_destroy");
        },
        .inference => {
            exportInternal(&standaloneInferenceGetFunctionTable, "antfly_standalone_inference_get_function_table");
            exportInternal(&inferenceEntry, "antfly_runtime_inference");
        },
        .cli => {
            exportInternal(&cliEntry, "antfly_runtime_cli");
        },
    }
}

fn standaloneInferenceCreate(context: *const standalone_inference_bridge.CreateContext) callconv(.c) standalone_inference_bridge.Status {
    if (!standalone_inference_bridge.validContext(
        standalone_inference_bridge.CreateContext,
        context.abi_version,
        context.struct_size,
    ))
        return standalone_inference_bridge.statusFromError(error.UnsupportedVersion);
    context.out_handle.* = standalone_inference_host.linkedInferenceCreate(context) catch |err| {
        return reportStandaloneInferenceFailure("create", err);
    };
    return .ok;
}

fn standaloneInferenceConfigure(context: *const standalone_inference_bridge.ConfigureContext) callconv(.c) standalone_inference_bridge.Status {
    if (!standalone_inference_bridge.validContext(
        standalone_inference_bridge.ConfigureContext,
        context.abi_version,
        context.struct_size,
    ))
        return standalone_inference_bridge.statusFromError(error.UnsupportedVersion);
    standalone_inference_host.linkedInferenceConfigure(context) catch |err| {
        return reportStandaloneInferenceFailure("configure", err);
    };
    return .ok;
}

fn standaloneInferenceInvokeProvider(context: *const standalone_inference_bridge.ProviderInvokeContext) callconv(.c) standalone_inference_bridge.Status {
    if (!standalone_inference_bridge.validContext(
        standalone_inference_bridge.ProviderInvokeContext,
        context.abi_version,
        context.struct_size,
    ))
        return standalone_inference_bridge.statusFromError(error.UnsupportedVersion);
    standalone_inference_host.linkedInferenceInvokeProvider(context) catch |err| {
        return standalone_inference_bridge.statusFromError(err);
    };
    return .ok;
}

fn standaloneInferenceDestroyProviderResponse(handle: *anyopaque) callconv(.c) void {
    standalone_inference_host.linkedInferenceDestroyProviderResponse(handle);
}

fn standaloneInferenceRouteManifest(context: *const standalone_inference_bridge.RouteManifestContext) callconv(.c) standalone_inference_bridge.Status {
    if (!standalone_inference_bridge.validContext(
        standalone_inference_bridge.RouteManifestContext,
        context.abi_version,
        context.struct_size,
    ))
        return standalone_inference_bridge.statusFromError(error.UnsupportedVersion);
    standalone_inference_host.linkedInferenceRouteManifest(context) catch |err| {
        return reportStandaloneInferenceFailure("route_manifest", err);
    };
    return .ok;
}

fn standaloneInferenceHandleHttp(context: *const standalone_inference_bridge.HttpHandleContext) callconv(.c) standalone_inference_bridge.Status {
    if (!standalone_inference_bridge.validContext(
        standalone_inference_bridge.HttpHandleContext,
        context.abi_version,
        context.struct_size,
    ))
        return standalone_inference_bridge.statusFromError(error.UnsupportedVersion);
    standalone_inference_host.linkedInferenceHandleHttp(context) catch |err| {
        return reportStandaloneInferenceFailure("handle_http", err);
    };
    return .ok;
}

fn standaloneInferenceDestroyHttpResponse(handle: *anyopaque) callconv(.c) void {
    standalone_inference_host.linkedInferenceDestroyHttpResponse(handle);
}

fn standaloneInferenceDestroy(handle: *anyopaque) callconv(.c) void {
    standalone_inference_host.linkedInferenceDestroy(handle);
}

const standalone_inference_function_table: standalone_inference_bridge.FunctionTable = .{
    .abi_version = standalone_inference_bridge.abi_version,
    .struct_size = @sizeOf(standalone_inference_bridge.FunctionTable),
    .capabilities = standalone_inference_bridge.Capability.provider |
        standalone_inference_bridge.Capability.route_manifest |
        standalone_inference_bridge.Capability.resource_budget,
    .create = &standaloneInferenceCreate,
    .configure = &standaloneInferenceConfigure,
    .invoke_provider = &standaloneInferenceInvokeProvider,
    .destroy_provider_response = &standaloneInferenceDestroyProviderResponse,
    .route_manifest = &standaloneInferenceRouteManifest,
    .handle_http = &standaloneInferenceHandleHttp,
    .destroy_http_response = &standaloneInferenceDestroyHttpResponse,
    .destroy = &standaloneInferenceDestroy,
};

fn standaloneInferenceGetFunctionTable() callconv(.c) *const standalone_inference_bridge.FunctionTable {
    return &standalone_inference_function_table;
}

fn reportStandaloneInferenceFailure(comptime operation: []const u8, err: anyerror) standalone_inference_bridge.Status {
    std.log.err("standalone inference bridge failed operation={s} err={}", .{ operation, err });
    return standalone_inference_bridge.statusFromError(err);
}

fn runtimeAllocator() std.mem.Allocator {
    const fallback = if (!builtin.single_threaded) std.heap.smp_allocator else std.heap.page_allocator;
    return platform.allocator.processAllocator(fallback);
}
