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
const owns_storage_kernel = unit_options.unit == .storage_kernel or unit_options.unit == .data_pic_probe or
    unit_options.unit == .storage_runtime_pic_probe or unit_options.unit == .application_pic_probe or
    (unit_options.unit == .distributed and !unit_options.storage_kernel_experiment);
const standalone_inference_bridge = @import("standalone/inference_bridge.zig");
const restore_staging_exports = if (unit_options.unit == .distributed or unit_options.unit == .storage_runtime_pic_probe or
    unit_options.unit == .application_pic_probe)
    @import("standalone/restore_staging_exports.zig")
else
    struct {};
const api_kernel_exports = if (unit_options.unit == .api_kernel)
    @import("api/kernel_exports.zig")
else
    struct {};
const storage_kernel_exports = if (owns_storage_kernel)
    @import("capi/db.zig")
else
    struct {};

const cli_runtime = if (unit_options.unit == .cli or unit_options.unit == .control_probe or
    unit_options.unit == .cli_pic_probe)
    @import("cli_runtime.zig")
else
    struct {};
const ha_runtime = if (unit_options.unit == .distributed or unit_options.unit == .application_pic_probe)
    @import("cmd/ha.zig")
else
    struct {};
const data_runtime = if (unit_options.unit == .distributed or unit_options.unit == .data_pic_probe or
    unit_options.unit == .storage_runtime_pic_probe or unit_options.unit == .application_pic_probe)
    @import("data/runtime.zig")
else
    struct {};
const metadata_runtime = if (unit_options.unit == .distributed or unit_options.unit == .control_probe or
    unit_options.unit == .application_pic_probe)
    @import("metadata/runtime.zig")
else
    struct {};
// Serverless owns a complete local query/maintenance runtime. Co-generate it
// with the application/storage owner so API protocol handling does not emit a
// second copy of physical query and storage implementation.
const serverless_runtime = if (unit_options.unit == .distributed or unit_options.unit == .application_pic_probe)
    @import("cmd/serverless.zig")
else
    struct {};
const inference_runtime = if (unit_options.unit == .inference) @import("inference_runtime/runtime.zig") else struct {};
// Standalone adds about 35 seconds when co-generated with the server roles but
// costs 6 minutes and 8 GiB as a separate ARM64 Linux unit. Keep it co-located
// until the shared storage kernel removes that duplicated LLVM work.
const standalone_runtime = if (unit_options.unit == .distributed or unit_options.unit == .storage_runtime_pic_probe or
    unit_options.unit == .application_pic_probe)
    @import("standalone/runtime.zig")
else
    struct {};
// Lite's non-server commands share storage types with standalone, while
// `lite serve` directly enters that runtime. Co-locating Lite and the server
// roles gives them one storage type identity and one LLVM unit.
const lite_runtime = if (unit_options.unit == .distributed or unit_options.unit == .storage_runtime_pic_probe or
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
pub const data_snapshot = @import("data/storage/shard_state_store.zig");
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
pub const platform_sync = @import("antfly_platform").sync;
pub const platform_time = @import("antfly_platform").time;
pub const portable_backup = @import("storage/portable_backup.zig");
pub const public_api = @import("api/mod.zig");
pub const raft = @import("raft/mod.zig");
pub const storage_backend = @import("storage/backend_types.zig");
pub const storage_backend_erased = @import("storage/backend_erased.zig");
pub const transactions = @import("storage/transactions.zig");
pub const traversal = @import("graph/traversal.zig");

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

fn runHa(
    init: std.process.Init,
    _: []const u8,
    args: *std.process.Args.Iterator,
) !void {
    return ha_runtime.runFromIterator(init, "antfly", args);
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

fn haEntry(context: *const bridge.Context) callconv(.c) c_int {
    return runtimeEntry(context, "ha", runHa);
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

fn exportInternal(comptime function: anytype, comptime name: []const u8) void {
    @export(function, .{ .name = name, .visibility = .hidden });
}

comptime {
    switch (unit_options.unit) {
        .api_kernel => {
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
        },
        .distributed => {
            // Importing the C ABI implementation makes its `pub export`
            // declarations roots of this PIC archive. The executable and both
            // C ABI library names link this exact compiled artifact.
            if (!unit_options.storage_kernel_experiment) _ = storage_kernel_exports;
            exportInternal(&dataEntry, "antfly_runtime_data");
            exportInternal(&haEntry, "antfly_runtime_ha");
            exportInternal(&metadataEntry, "antfly_runtime_metadata");
            exportInternal(&serverlessEntry, "antfly_runtime_serverless");
            exportInternal(&standaloneEntry, "antfly_runtime_standalone");
            exportInternal(&restore_staging_exports.create, "antfly_restore_staging_create");
            exportInternal(&restore_staging_exports.destroy, "antfly_restore_staging_destroy");
        },
        .data_pic_probe => {
            // Measurement-only mirror of the production archive: retain the
            // data entry point and the same CAPI roots, but none of the CLI,
            // metadata, standalone, Lite, or restore-staging entry points.
            _ = storage_kernel_exports;
            exportInternal(&dataEntry, "antfly_runtime_data");
        },
        .storage_runtime_pic_probe => {
            // Candidate storage-owning unit: keep the runtime paths that must
            // share the physical storage implementation, while excluding the
            // remote/control-only CLI and metadata roots.
            _ = storage_kernel_exports;
            exportInternal(&dataEntry, "antfly_runtime_data");
            exportInternal(&standaloneEntry, "antfly_runtime_standalone");
            exportInternal(&restore_staging_exports.create, "antfly_restore_staging_create");
            exportInternal(&restore_staging_exports.destroy, "antfly_restore_staging_destroy");
        },
        .application_pic_probe => {
            // Candidate application/storage unit after removing only CLI.
            // Metadata is intentionally co-generated because prior data plus
            // metadata measurements showed near-zero marginal LLVM cost.
            _ = storage_kernel_exports;
            exportInternal(&dataEntry, "antfly_runtime_data");
            exportInternal(&haEntry, "antfly_runtime_ha");
            exportInternal(&metadataEntry, "antfly_runtime_metadata");
            exportInternal(&serverlessEntry, "antfly_runtime_serverless");
            exportInternal(&standaloneEntry, "antfly_runtime_standalone");
            exportInternal(&restore_staging_exports.create, "antfly_restore_staging_create");
            exportInternal(&restore_staging_exports.destroy, "antfly_restore_staging_destroy");
        },
        .control_probe => {
            exportInternal(&cliEntry, "antfly_runtime_cli");
            exportInternal(&metadataEntry, "antfly_runtime_metadata");
        },
        .cli_pic_probe => {
            exportInternal(&cliEntry, "antfly_runtime_cli");
        },
        .cli => {
            exportInternal(&cliEntry, "antfly_runtime_cli");
        },
        .storage_kernel => {
            // The experiment gives the existing coarse CAPI DB implementation
            // its own PIC compilation unit. Consumer runtimes continue to own
            // storage directly until representative operations migrate across
            // this ABI in later phases.
            _ = storage_kernel_exports;
            exportInternal(&storage_kernel_exports.storageOwnerContextCreate, "antfly_storage_context_create");
            exportInternal(&storage_kernel_exports.storageOwnerContextDestroy, "antfly_storage_context_destroy");
            exportInternal(&storage_kernel_exports.storageOwnerOpen, "antfly_storage_owner_open");
            exportInternal(&storage_kernel_exports.storageOwnerClose, "antfly_storage_owner_close");
            exportInternal(&storage_kernel_exports.storageOwnerConfigure, "antfly_storage_owner_configure");
            exportInternal(&storage_kernel_exports.storageOwnerReconcile, "antfly_storage_owner_reconcile");
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
            exportInternal(&storage_kernel_exports.storageOwnerGraphExpandJson, "antfly_storage_owner_graph_expand_json");
            exportInternal(&storage_kernel_exports.storageOwnerGraphHydrateJson, "antfly_storage_owner_graph_hydrate_json");
            exportInternal(&storage_kernel_exports.storageOwnerGraphEdgesJson, "antfly_storage_owner_graph_edges_json");
            exportInternal(&storage_kernel_exports.storageOwnerDocumentArtifactManifestJson, "antfly_storage_owner_document_artifact_manifest_json");
            exportInternal(&storage_kernel_exports.storageOwnerDocumentArtifactManifestsJson, "antfly_storage_owner_document_artifact_manifests_json");
            exportInternal(&storage_kernel_exports.storageOwnerArtifactOperationJson, "antfly_storage_owner_artifact_operation_json");
            exportInternal(&storage_kernel_exports.storageOwnerRuntimeStatusJson, "antfly_storage_owner_runtime_status_json");
            exportInternal(&storage_kernel_exports.storageOwnerMaintenance, "antfly_storage_owner_maintenance");
            exportInternal(&storage_kernel_exports.storageOwnerBufferDestroy, "antfly_storage_owner_buffer_destroy");
        },
        .inference => {
            exportInternal(&inferenceEntry, "antfly_runtime_inference");
            exportInternal(&standaloneInferenceCreate, "antfly_standalone_inference_create");
            exportInternal(&standaloneInferenceConfigure, "antfly_standalone_inference_configure");
            exportInternal(&standaloneInferenceProvider, "antfly_standalone_inference_provider");
            exportInternal(&standaloneInferenceRegisterRoutes, "antfly_standalone_inference_register_routes");
            exportInternal(&standaloneInferenceDestroy, "antfly_standalone_inference_destroy");
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
