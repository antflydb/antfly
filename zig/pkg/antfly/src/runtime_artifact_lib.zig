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
pub const platform_sync = platform.sync;
const bridge = @import("runtime_bridge.zig");
const private_error_diagnostics = @import("runtime_private_error_diagnostics.zig");
const unit_options = @import("runtime_library_options");
const owns_storage_kernel = unit_options.unit == .storage_kernel or unit_options.unit == .data_pic_probe or
    unit_options.unit == .storage_runtime_pic_probe or unit_options.unit == .application_pic_probe or
    (unit_options.unit == .distributed and !unit_options.storage_kernel_experiment);
const standalone_inference_bridge = @import("standalone/inference_bridge.zig");
const restore_staging_exports = if ((unit_options.unit == .distributed and !unit_options.storage_kernel_experiment) or
    unit_options.unit == .storage_kernel or unit_options.unit == .storage_runtime_pic_probe or
    unit_options.unit == .application_pic_probe)
    @import("standalone/restore_staging_exports.zig")
else
    struct {};
const api_kernel_exports = if (unit_options.unit == .api_kernel or unit_options.unit == .control_api_probe or
    (unit_options.unit == .distributed and unit_options.storage_kernel_experiment))
    @import("api/kernel_exports.zig")
else
    struct {};
pub const kernel_wal_owner = if (owns_storage_kernel) @import("storage/kernel_wal_owner.zig") else struct {};
const storage_kernel_exports = if (owns_storage_kernel)
    @import("capi/db.zig")
else
    struct {};
const enrichment_compute_exports = if (unit_options.unit == .enrichment_compute)
    @import("storage/enrichment_compute_provider.zig")
else
    struct {};
const local_query_exports = if (owns_storage_kernel or unit_options.unit == .local_query)
    @import("storage/local_query_provider.zig")
else
    struct {};

const cli_runtime = if (unit_options.unit == .cli or unit_options.unit == .serverless or unit_options.unit == .control_probe or
    unit_options.unit == .cli_pic_probe) @import("cli_runtime.zig") else struct {};
// Local HA administration owns storage handles and seed lifecycle artifacts.
// Keep it with the distributed/storage unit so the small remote-client CLI
// archive does not code-generate a second copy of the HA storage closure.
const ha_runtime = if (unit_options.unit == .distributed or unit_options.unit == .application_pic_probe or
    unit_options.unit == .control_api_probe) @import("cmd/ha.zig") else struct {};
const data_runtime = if (unit_options.unit == .distributed or unit_options.unit == .data_pic_probe or
    unit_options.unit == .storage_runtime_pic_probe or unit_options.unit == .application_pic_probe or
    unit_options.unit == .control_api_probe) @import("data/runtime.zig") else struct {};
const metadata_runtime = if (unit_options.unit == .distributed or unit_options.unit == .control_probe or
    unit_options.unit == .application_pic_probe or unit_options.unit == .control_api_probe)
    @import("metadata/runtime.zig")
else
    struct {};
const serverless_runtime = if (unit_options.unit == .serverless or unit_options.unit == .application_pic_probe)
    @import("cmd/serverless.zig")
else
    struct {};
const inference_runtime = if (unit_options.unit == .inference) @import("inference_runtime/runtime.zig") else struct {};
// Standalone adds about 35 seconds when co-generated with the server roles but
// costs 6 minutes and 8 GiB as a separate ARM64 Linux unit. Keep it co-located
// until the shared storage kernel removes that duplicated LLVM work.
const standalone_runtime = if (unit_options.unit == .distributed or unit_options.unit == .storage_runtime_pic_probe or
    unit_options.unit == .application_pic_probe) @import("standalone/runtime.zig") else struct {};
// Lite's non-server commands share storage types with standalone, while
// `lite serve` directly enters that runtime. Co-locating Lite and the server
// roles gives them one storage type identity and one LLVM unit.
const lite_runtime = if ((unit_options.unit == .distributed and !unit_options.storage_kernel_experiment) or
    unit_options.unit == .storage_kernel or unit_options.unit == .storage_runtime_pic_probe or
    unit_options.unit == .application_pic_probe)
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
pub const common_config = @import("common/config.zig");
pub const common_secrets = @import("common/secrets.zig");
pub const data_snapshot = @import("data/storage/shard_state_store.zig");
pub const data_raft_apply = @import("data/storage/raft_apply_store.zig");
pub const data_raft_projection_wire = @import("storage/data_raft_projection_wire.zig");
pub const db = @import("storage/db/selected_root.zig").db;
pub const geo = @import("search/geo.zig");
pub const graph = @import("graph/graph.zig");
pub const graph_pattern = @import("graph/pattern.zig");
pub const graph_query = @import("graph/query.zig");
pub const ha_seed_activation = @import("storage/ha/seed_activation.zig");
pub const hbc = @import("storage/hbc_adapter.zig");
pub const lite = @import("storage/lite/mod.zig");
pub const lsm_backend = @import("storage/lsm_backend/mod.zig");
pub const metadata_raft_apply = @import("metadata/storage/raft_apply_store.zig");
pub const metadata_table_manager = @import("metadata/table_manager.zig");
pub const metadata_table_provisioner = @import("metadata/table_provisioner.zig");
pub const paths = @import("graph/paths.zig");
pub const platform_clock = @import("antfly_platform").clock;
pub const platform_time = @import("antfly_platform").time;
pub const portable_backup = @import("storage/portable_backup.zig");
pub const restore_state_contract = @import("storage/restore_state_contract.zig");
pub const public_api = @import("api/mod.zig");
pub const raft = @import("raft/mod.zig");
pub const raft_catalog = @import("raft/catalog.zig");
pub const shard = @import("storage/shard.zig");
pub const storage_backend = @import("storage/backend_types.zig");
pub const storage_backend_erased = @import("storage/backend_erased.zig");
pub const storage_maintenance = @import("storage/maintenance.zig");
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
    if (std.mem.eql(u8, command, "lite") and !unit_options.storage_kernel_experiment)
        return lite_runtime.runFromIterator(init, "antfly", args);
    return standalone_runtime.runFromIterator(init, "antfly", args);
}

fn runLite(init: std.process.Init, _: []const u8, args: *std.process.Args.Iterator) !void {
    return lite_runtime.runFromIterator(init, "antfly", args);
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

fn liteEntry(context: *const bridge.Context) callconv(.c) c_int {
    return runtimeEntry(context, "lite", runLite);
}

fn standaloneLiteEntry(context: *const bridge.LiteServeContext) callconv(.c) c_int {
    const init: *const std.process.Init = @ptrCast(@alignCast(context.init));
    const extra_args = init.gpa.alloc([]const u8, context.extra_args_len) catch return 1;
    defer init.gpa.free(extra_args);
    if (context.extra_args_len > 0) {
        const encoded = context.extra_args.?[0..context.extra_args_len];
        for (encoded, 0..) |arg, i| extra_args[i] = arg.slice();
    }
    standalone_runtime.runLite(
        init.*,
        context.path.slice(),
        context.host.slice(),
        context.port,
        context.fsync != 0,
        extra_args,
    ) catch |err| {
        std.debug.print("antfly lite serve: startup failed err={s}\n", .{@errorName(err)});
        return 1;
    };
    return 0;
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
            if (!unit_options.storage_kernel_experiment) _ = storage_kernel_exports;
            exportInternal(&dataEntry, "antfly_runtime_data");
            exportInternal(&haEntry, "antfly_runtime_ha");
            exportInternal(&metadataEntry, "antfly_runtime_metadata");
            exportInternal(&standaloneEntry, "antfly_runtime_standalone");
            if (unit_options.storage_kernel_experiment) {
                exportInternal(&api_kernel_exports.getFunctionTable, "antfly_api_kernel_get_function_table");
                exportInternal(&standaloneLiteEntry, "antfly_runtime_standalone_lite");
            } else {
                exportInternal(&liteEntry, "antfly_runtime_lite");
                exportInternal(&restore_staging_exports.create, "antfly_restore_staging_create");
                exportInternal(&restore_staging_exports.destroy, "antfly_restore_staging_destroy");
                exportInternal(&storage_kernel_exports.storageOwnerContextDestroy, "antfly_storage_context_destroy");
                exportInternal(&storage_kernel_exports.storageOwnerClose, "antfly_storage_owner_close");
            }
        },
        .data_pic_probe => {
            _ = storage_kernel_exports;
            exportInternal(&dataEntry, "antfly_runtime_data");
        },
        .storage_runtime_pic_probe => {
            _ = storage_kernel_exports;
            exportInternal(&dataEntry, "antfly_runtime_data");
            exportInternal(&standaloneEntry, "antfly_runtime_standalone");
            exportInternal(&restore_staging_exports.create, "antfly_restore_staging_create");
            exportInternal(&restore_staging_exports.destroy, "antfly_restore_staging_destroy");
        },
        .application_pic_probe => {
            _ = storage_kernel_exports;
            exportInternal(&dataEntry, "antfly_runtime_data");
            exportInternal(&haEntry, "antfly_runtime_ha");
            exportInternal(&metadataEntry, "antfly_runtime_metadata");
            exportInternal(&standaloneEntry, "antfly_runtime_standalone");
            exportInternal(&restore_staging_exports.create, "antfly_restore_staging_create");
            exportInternal(&restore_staging_exports.destroy, "antfly_restore_staging_destroy");
        },
        .control_probe => {
            exportInternal(&cliEntry, "antfly_runtime_cli");
            exportInternal(&metadataEntry, "antfly_runtime_metadata");
        },
        .cli_pic_probe => exportInternal(&cliEntry, "antfly_runtime_cli"),
        .control_api_probe => {
            exportInternal(&api_kernel_exports.getFunctionTable, "antfly_api_kernel_get_function_table");
            exportInternal(&dataEntry, "antfly_runtime_data");
            exportInternal(&haEntry, "antfly_runtime_ha");
            exportInternal(&metadataEntry, "antfly_runtime_metadata");
        },
        .serverless => {
            exportInternal(&cliEntry, "antfly_runtime_cli");
            exportInternal(&serverlessEntry, "antfly_runtime_serverless");
        },
        .enrichment_compute => {
            exportInternal(&enrichment_compute_exports.extractStream, "antfly_enrichment_extract_stream");
            exportInternal(&enrichment_compute_exports.renderPdfPagePng, "antfly_enrichment_render_pdf_page_png");
            exportInternal(&enrichment_compute_exports.bufferDestroy, "antfly_enrichment_buffer_destroy");
        },
        .local_query => {
            exportInternal(&local_query_exports.execute, "antfly_local_query_execute");
            exportInternal(&local_query_exports.bufferDestroy, "antfly_local_query_buffer_destroy");
        },
        .storage_kernel => {
            // The kernel owns physical DB and local-query compilation plus
            // the C API. Product-mode orchestration stays in the distributed
            // control unit and reaches these implementations through opaque
            // owner and restore-staging entry points.
            _ = storage_kernel_exports;
            exportInternal(&liteEntry, "antfly_runtime_lite");
            exportInternal(&restore_staging_exports.create, "antfly_restore_staging_create");
            exportInternal(&restore_staging_exports.destroy, "antfly_restore_staging_destroy");
            exportInternal(&storage_kernel_exports.storageOwnerContextCreate, "antfly_storage_context_create");
            exportInternal(&storage_kernel_exports.storageOwnerContextDestroy, "antfly_storage_context_destroy");
            exportInternal(&storage_kernel_exports.storageContextAttachInferenceProvider, "antfly_storage_context_attach_inference_provider");
            exportInternal(&storage_kernel_exports.storageOwnerContextMetrics, "antfly_storage_context_metrics");
            exportInternal(&storage_kernel_exports.storageContextSystemStoreOpen, "antfly_storage_context_system_store_open");
            exportInternal(&storage_kernel_exports.storageSystemStoreClose, "antfly_storage_system_store_close");
            exportInternal(&storage_kernel_exports.storageSystemStoreSync, "antfly_storage_system_store_sync");
            exportInternal(&storage_kernel_exports.storageSystemStoreBeginRead, "antfly_storage_system_store_begin_read");
            exportInternal(&storage_kernel_exports.storageSystemStoreBeginCurrentScan, "antfly_storage_system_store_begin_current_scan");
            exportInternal(&storage_kernel_exports.storageSystemStoreBeginWrite, "antfly_storage_system_store_begin_write");
            exportInternal(&storage_kernel_exports.storageSystemReadGet, "antfly_storage_system_read_get");
            exportInternal(&storage_kernel_exports.storageSystemReadOpenCursor, "antfly_storage_system_read_open_cursor");
            exportInternal(&storage_kernel_exports.storageSystemReadAbort, "antfly_storage_system_read_abort");
            exportInternal(&storage_kernel_exports.storageSystemCurrentScanOpenCursor, "antfly_storage_system_current_scan_open_cursor");
            exportInternal(&storage_kernel_exports.storageSystemCurrentScanAbort, "antfly_storage_system_current_scan_abort");
            exportInternal(&storage_kernel_exports.storageSystemWriteGet, "antfly_storage_system_write_get");
            exportInternal(&storage_kernel_exports.storageSystemWritePut, "antfly_storage_system_write_put");
            exportInternal(&storage_kernel_exports.storageSystemWriteDelete, "antfly_storage_system_write_delete");
            exportInternal(&storage_kernel_exports.storageSystemWriteCommit, "antfly_storage_system_write_commit");
            exportInternal(&storage_kernel_exports.storageSystemWriteAbort, "antfly_storage_system_write_abort");
            exportInternal(&storage_kernel_exports.storageSystemCursorMove, "antfly_storage_system_cursor_move");
            exportInternal(&storage_kernel_exports.storageSystemCursorClose, "antfly_storage_system_cursor_close");
            exportInternal(&storage_kernel_exports.storageContextLiteAdoptionProbe, "antfly_storage_context_lite_adoption_probe");
            exportInternal(&storage_kernel_exports.storageContextLiteAdoptAndVerify, "antfly_storage_context_lite_adopt_and_verify");
            exportInternal(&storage_kernel_exports.storageContextLiteMarkStandalone, "antfly_storage_context_lite_mark_standalone");
            exportInternal(&storage_kernel_exports.storageContextMaintenanceStatus, "antfly_storage_context_maintenance_status");
            exportInternal(&storage_kernel_exports.storageContextMaintenanceRun, "antfly_storage_context_maintenance_run");
            exportInternal(&storage_kernel_exports.metadataApplyStoreOpen, "antfly_metadata_apply_store_open");
            exportInternal(&storage_kernel_exports.metadataApplyStoreClose, "antfly_metadata_apply_store_close");
            exportInternal(&storage_kernel_exports.metadataApplyStoreApplyBatch, "antfly_metadata_apply_store_apply_batch");
            exportInternal(&storage_kernel_exports.metadataApplyStoreBuildSnapshot, "antfly_metadata_apply_store_build_snapshot");
            exportInternal(&storage_kernel_exports.metadataApplyStoreInstallSnapshot, "antfly_metadata_apply_store_install_snapshot");
            exportInternal(&storage_kernel_exports.metadataApplyStorePrepareSnapshot, "antfly_metadata_apply_store_prepare_snapshot");
            exportInternal(&storage_kernel_exports.metadataApplyPreparedSnapshotMaterialize, "antfly_metadata_apply_prepared_snapshot_materialize");
            exportInternal(&storage_kernel_exports.metadataApplyPreparedSnapshotCancel, "antfly_metadata_apply_prepared_snapshot_cancel");
            exportInternal(&storage_kernel_exports.metadataApplyPreparedSnapshotDestroy, "antfly_metadata_apply_prepared_snapshot_destroy");
            exportInternal(&storage_kernel_exports.metadataApplyStoreProjection, "antfly_metadata_apply_store_projection");
            exportInternal(&storage_kernel_exports.metadataApplyStoreAddListeners, "antfly_metadata_apply_store_add_listeners");
            exportInternal(&storage_kernel_exports.metadataReconcileReplicaRoot, "antfly_metadata_reconcile_replica_root");
            exportInternal(&storage_kernel_exports.dataApplyStoreOpen, "antfly_data_apply_store_open");
            exportInternal(&storage_kernel_exports.dataApplyStoreClose, "antfly_data_apply_store_close");
            exportInternal(&storage_kernel_exports.dataApplyStoreApplyBatch, "antfly_data_apply_store_apply_batch");
            exportInternal(&storage_kernel_exports.dataApplyStoreBuildSnapshot, "antfly_data_apply_store_build_snapshot");
            exportInternal(&storage_kernel_exports.dataApplyStoreInstallSnapshot, "antfly_data_apply_store_install_snapshot");
            exportInternal(&storage_kernel_exports.dataApplyStorePrepareSnapshot, "antfly_data_apply_store_prepare_snapshot");
            exportInternal(&storage_kernel_exports.dataApplyPreparedSnapshotMaterialize, "antfly_data_apply_prepared_snapshot_materialize");
            exportInternal(&storage_kernel_exports.dataApplyPreparedSnapshotCancel, "antfly_data_apply_prepared_snapshot_cancel");
            exportInternal(&storage_kernel_exports.dataApplyPreparedSnapshotDestroy, "antfly_data_apply_prepared_snapshot_destroy");
            exportInternal(&storage_kernel_exports.dataApplyStoreLatest, "antfly_data_apply_store_latest");
            exportInternal(&storage_kernel_exports.dataApplyStoreLatestForTransition, "antfly_data_apply_store_latest_for_transition");
            exportInternal(&storage_kernel_exports.dataApplyStoreRaftBatchProtocolVersion, "antfly_data_apply_store_raft_batch_protocol_version");
            exportInternal(&storage_kernel_exports.dataApplyStoreProjection, "antfly_data_apply_store_projection");
            exportInternal(&storage_kernel_exports.dataApplyStoreReconcileOwner, "antfly_data_apply_store_reconcile_owner");
            exportInternal(&storage_kernel_exports.dataApplyStoreRetainGroups, "antfly_data_apply_store_retain_groups");
            exportInternal(&storage_kernel_exports.dataApplyStoreBeginGroupTransition, "antfly_data_apply_store_begin_group_transition");
            exportInternal(&storage_kernel_exports.dataApplyStoreCommitGroupTransition, "antfly_data_apply_store_commit_group_transition");
            exportInternal(&storage_kernel_exports.dataApplyStoreAbortGroupTransition, "antfly_data_apply_store_abort_group_transition");
            exportInternal(&storage_kernel_exports.dataApplyStoreDestroyGroupTransition, "antfly_data_apply_store_destroy_group_transition");
            exportInternal(&storage_kernel_exports.storageOwnerLocalTransition, "antfly_storage_owner_local_transition");
            exportInternal(&storage_kernel_exports.storageOwnerOpen, "antfly_storage_owner_open");
            exportInternal(&storage_kernel_exports.storageOwnerClose, "antfly_storage_owner_close");
            exportInternal(&storage_kernel_exports.storageHASeedActivateJson, "antfly_storage_ha_seed_activate_json");
            exportInternal(&storage_kernel_exports.storageHASeedValidateJson, "antfly_storage_ha_seed_validate_json");
            exportInternal(&storage_kernel_exports.storageHASeedPruneJson, "antfly_storage_ha_seed_prune_json");
            exportInternal(&storage_kernel_exports.storageOwnerConfigure, "antfly_storage_owner_configure");
            exportInternal(&storage_kernel_exports.storageOwnerReconcile, "antfly_storage_owner_reconcile");
            exportInternal(&storage_kernel_exports.storageOwnerPreflightWriteAdmission, "antfly_storage_owner_preflight_write_admission");
            exportInternal(&storage_kernel_exports.storageOwnerFindMedianKey, "antfly_storage_owner_find_median_key");
            exportInternal(&storage_kernel_exports.storageOwnerBulkBegin, "antfly_storage_owner_bulk_begin");
            exportInternal(&storage_kernel_exports.storageOwnerBulkFinish, "antfly_storage_owner_bulk_finish");
            exportInternal(&storage_kernel_exports.storageOwnerBulkAbort, "antfly_storage_owner_bulk_abort");
            exportInternal(&storage_kernel_exports.storageOwnerBatchJson, "antfly_storage_owner_batch_json");
            exportInternal(&storage_kernel_exports.storageOwnerReplicatedBatchJson, "antfly_storage_owner_replicated_batch_json");
            exportInternal(&storage_kernel_exports.storageOwnerTransactionStatus, "antfly_storage_owner_transaction_status");
            exportInternal(&storage_kernel_exports.storageOwnerWaitForSync, "antfly_storage_owner_wait_for_sync");
            exportInternal(&storage_kernel_exports.storageOwnerApplyHAReplicationRecord, "antfly_storage_owner_apply_ha_replication_record");
            exportInternal(&storage_kernel_exports.storageOwnerBackupJson, "antfly_storage_owner_backup_json");
            exportInternal(&storage_kernel_exports.storageSnapshotPrepare, "antfly_storage_snapshot_prepare");
            exportInternal(&storage_kernel_exports.storageRestorePrepare, "antfly_storage_restore_prepare");
            exportInternal(&storage_kernel_exports.storageRestoreReconcile, "antfly_storage_restore_reconcile");
            exportInternal(&storage_kernel_exports.storageRestoreApplyBootstrap, "antfly_storage_restore_apply_bootstrap");
            exportInternal(&storage_kernel_exports.storageOwnerRestoreRepair, "antfly_storage_owner_restore_repair");
            exportInternal(&storage_kernel_exports.storageSnapshotPromote, "antfly_storage_snapshot_promote");
            exportInternal(&storage_kernel_exports.storageSnapshotPublishPrepared, "antfly_storage_snapshot_publish_prepared");
            exportInternal(&storage_kernel_exports.storageSnapshotCommit, "antfly_storage_snapshot_commit");
            exportInternal(&storage_kernel_exports.storageSnapshotRollback, "antfly_storage_snapshot_rollback");
            exportInternal(&storage_kernel_exports.storageSnapshotDestroy, "antfly_storage_snapshot_destroy");
            exportInternal(&storage_kernel_exports.storageOwnerQueryJson, "antfly_storage_owner_query_json");
            exportInternal(&storage_kernel_exports.storageOwnerLookupJson, "antfly_storage_owner_lookup_json");
            exportInternal(&storage_kernel_exports.storageOwnerScanNdjson, "antfly_storage_owner_scan_ndjson");
            exportInternal(&storage_kernel_exports.storageOwnerPreflightJson, "antfly_storage_owner_preflight_json");
            exportInternal(&storage_kernel_exports.storageOwnerTextStatsJson, "antfly_storage_owner_text_stats_json");
            exportInternal(&storage_kernel_exports.storageOwnerAlgebraicPartialsJson, "antfly_storage_owner_algebraic_partials_json");
            exportInternal(&storage_kernel_exports.storageAggregateJson, "antfly_storage_aggregate_json");
            exportInternal(&storage_kernel_exports.storageOwnerGraphExpandJson, "antfly_storage_owner_graph_expand_json");
            exportInternal(&storage_kernel_exports.storageOwnerGraphHydrateJson, "antfly_storage_owner_graph_hydrate_json");
            exportInternal(&storage_kernel_exports.storageOwnerGraphEdgesJson, "antfly_storage_owner_graph_edges_json");
            exportInternal(&storage_kernel_exports.storageOwnerDocumentArtifactManifestJson, "antfly_storage_owner_document_artifact_manifest_json");
            exportInternal(&storage_kernel_exports.storageOwnerDocumentArtifactManifestsJson, "antfly_storage_owner_document_artifact_manifests_json");
            exportInternal(&storage_kernel_exports.storageOwnerArtifactOperationJson, "antfly_storage_owner_artifact_operation_json");
            exportInternal(&storage_kernel_exports.storageOwnerRuntimeStatusJson, "antfly_storage_owner_runtime_status_json");
            exportInternal(&storage_kernel_exports.storageOwnerObservedDynamicFieldCapabilitySetsJson, "antfly_storage_owner_observed_dynamic_field_capability_sets_json");
            exportInternal(&storage_kernel_exports.storageOwnerRestoreStateJson, "antfly_storage_owner_restore_state_json");
            exportInternal(&storage_kernel_exports.storageOwnerTextMemoryJson, "antfly_storage_owner_text_memory_json");
            exportInternal(&storage_kernel_exports.storageOwnerMaintenance, "antfly_storage_owner_maintenance");
            exportInternal(&storage_kernel_exports.storageWalOpen, "antfly_storage_wal_open");
            exportInternal(&storage_kernel_exports.storageWalClose, "antfly_storage_wal_close");
            exportInternal(&storage_kernel_exports.storageWalAppend, "antfly_storage_wal_append");
            exportInternal(&storage_kernel_exports.storageWalSync, "antfly_storage_wal_sync");
            exportInternal(&storage_kernel_exports.storageWalTruncatePrefix, "antfly_storage_wal_truncate_prefix");
            exportInternal(&storage_kernel_exports.storageWalTruncateSuffix, "antfly_storage_wal_truncate_suffix");
            exportInternal(&storage_kernel_exports.storageWalIterate, "antfly_storage_wal_iterate");
            exportInternal(&storage_kernel_exports.storageWalRead, "antfly_storage_wal_read");
            exportInternal(&storage_kernel_exports.storageWalStatsSnapshot, "antfly_storage_wal_stats_snapshot");
            exportInternal(&storage_kernel_exports.storageWalLastLsn, "antfly_storage_wal_last_lsn");
            exportInternal(&storage_kernel_exports.storageOwnerBufferDestroy, "antfly_storage_owner_buffer_destroy");
            exportInternal(&local_query_exports.execute, "antfly_local_query_execute");
            exportInternal(&local_query_exports.bufferDestroy, "antfly_local_query_buffer_destroy");
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

const inference_provider_operation_slots = 13;
var inference_private_failure_counts = [_]std.atomic.Value(u64){std.atomic.Value(u64).init(0)} ** inference_provider_operation_slots;

// Private inference errors are normalized at this archive boundary, so this is
// the only place their original identity is available. Keep a fixed-size set
// of per-operation/error/model counters: one noisy model must not consume the
// first diagnostic for a different model, while model names supplied by a
// client must never grow process memory or produce unbounded first-error logs.
var inference_private_failure_diagnostics = [_]private_error_diagnostics.Diagnostic{.{}} ** private_error_diagnostics.slots_count;

fn shouldLogInferencePrivateFailure(count: u64) bool {
    // Keep the first few failures for diagnosis, then retain logarithmic
    // visibility without allowing a bad model to amplify logs per request.
    return count <= 4 or std.math.isPowerOfTwo(count);
}

fn standaloneInferenceInvokeProvider(context: *const standalone_inference_bridge.ProviderInvokeContext) callconv(.c) standalone_inference_bridge.Status {
    if (!standalone_inference_bridge.validContext(
        standalone_inference_bridge.ProviderInvokeContext,
        context.abi_version,
        context.struct_size,
    ))
        return standalone_inference_bridge.statusFromError(error.UnsupportedVersion);
    standalone_inference_host.linkedInferenceInvokeProvider(context) catch |err| {
        // Stable errors retain their exact identity at the caller, which owns
        // the request correlation and can log them once with table context.
        // Only private inference-unit errors need an owner-side diagnostic
        // before they are normalized to the stable provider failure.
        if (standalone_inference_bridge.errorHasStableDetail(err))
            return standalone_inference_bridge.statusFromError(err);
        const provider_operation = std.enums.fromInt(
            standalone_inference_bridge.ProviderOperation,
            context.operation,
        );
        const operation_slot: usize = if (context.operation > 0 and context.operation < inference_provider_operation_slots)
            @intCast(context.operation)
        else
            0;
        const diagnostic_fingerprint = private_error_diagnostics.fingerprint(
            context.operation,
            err,
            context.request_json.slice(),
        );
        if (private_error_diagnostics.note(
            &inference_private_failure_diagnostics,
            diagnostic_fingerprint,
        )) |failure_count| {
            if (shouldLogInferencePrivateFailure(failure_count)) {
                std.log.err("standalone inference bridge failed operation=invoke_provider provider_operation={s} request_bytes={d} has_deadline={} diagnostic_fingerprint={x} observed_diagnostic_failures={d} err={}", .{
                    if (provider_operation) |value| @tagName(value) else "unknown",
                    context.request_json.len,
                    context.has_deadline != 0,
                    diagnostic_fingerprint,
                    failure_count,
                    err,
                });
            }
        } else {
            // Once the bounded fingerprint table is full, retain logarithmic
            // aggregate visibility without allocating or logging once per new
            // client-controlled model name.
            const failure_count = inference_private_failure_counts[operation_slot].fetchAdd(1, .monotonic) +% 1;
            if (shouldLogInferencePrivateFailure(failure_count)) {
                std.log.err("standalone inference bridge failed operation=invoke_provider provider_operation={s} request_bytes={d} has_deadline={} diagnostic_table_saturated=true observed_overflow_failures={d} err={}", .{
                    if (provider_operation) |value| @tagName(value) else "unknown",
                    context.request_json.len,
                    context.has_deadline != 0,
                    failure_count,
                    err,
                });
            }
        }
        return standalone_inference_bridge.statusFromErrorWithFallback(err, error.InferenceProviderFailure);
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

fn standaloneInferenceTryAcquireRequest(handle: *anyopaque) callconv(.c) u8 {
    return @intFromBool(standalone_inference_host.linkedInferenceTryAcquireRequest(handle));
}

fn standaloneInferenceReleaseRequest(handle: *anyopaque) callconv(.c) void {
    standalone_inference_host.linkedInferenceReleaseRequest(handle);
}

fn standaloneInferenceRequestAdmissionStats(
    handle: *anyopaque,
    out: *standalone_inference_bridge.RequestAdmissionStats,
) callconv(.c) void {
    out.* = standalone_inference_host.linkedInferenceRequestAdmissionStats(handle);
}

fn standaloneInferenceDestroy(handle: *anyopaque) callconv(.c) void {
    standalone_inference_host.linkedInferenceDestroy(handle);
}

const standalone_inference_function_table: standalone_inference_bridge.FunctionTable = .{
    .abi_version = standalone_inference_bridge.abi_version,
    .struct_size = @sizeOf(standalone_inference_bridge.FunctionTable),
    .capabilities = standalone_inference_bridge.Capability.provider |
        standalone_inference_bridge.Capability.route_manifest |
        standalone_inference_bridge.Capability.resource_budget |
        standalone_inference_bridge.Capability.request_admission,
    .create = &standaloneInferenceCreate,
    .configure = &standaloneInferenceConfigure,
    .invoke_provider = &standaloneInferenceInvokeProvider,
    .destroy_provider_response = &standaloneInferenceDestroyProviderResponse,
    .route_manifest = &standaloneInferenceRouteManifest,
    .handle_http = &standaloneInferenceHandleHttp,
    .destroy_http_response = &standaloneInferenceDestroyHttpResponse,
    .try_acquire_request = &standaloneInferenceTryAcquireRequest,
    .release_request = &standaloneInferenceReleaseRequest,
    .request_admission_stats = &standaloneInferenceRequestAdmissionStats,
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
