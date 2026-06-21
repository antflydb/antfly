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

const std = @import("std");
const builtin = @import("builtin");
const platform_sync = @import("antfly_platform").sync;
const db_mod = @import("../db/db.zig");
const db_core = @import("../db/core.zig");
const db_types = @import("../db/types.zig");
const backend_erased = @import("../backend_erased.zig");
const bridge = @import("bridge.zig");
const capabilities = @import("capabilities.zig");
const docstore = @import("docstore.zig");
const index_storage = @import("index_storage.zig");

const Allocator = std.mem.Allocator;
const native_index_base_path = "__antfly_lite";

pub const native = @import("native.zig");
pub const CheckReport = native.CheckReport;
pub const StableSnapshotReport = native.StableSnapshotReport;
pub const VacuumReport = native.VacuumReport;

pub const Profile = capabilities.Profile;
pub const supported_inference_modes = capabilities.supported_inference_modes;
pub const Capabilities = capabilities.Capabilities;
pub const InferenceStatus = capabilities.InferenceStatus;
pub const InferenceOpenOptions = capabilities.InferenceOpenOptions;
pub const capabilitiesForProfile = capabilities.capabilitiesForProfile;
pub const inferenceStatusForProfile = capabilities.inferenceStatusForProfile;
pub const inferenceStatusForProfileWithOptions = capabilities.inferenceStatusForProfileWithOptions;

pub const EngineKind = enum {
    /// Internal LSM bridge for explicit storage-engine development.
    bridge_lsm_container,

    /// Native v1 `.aflite` file engine. It owns real Lite pages and checkpoint
    /// roots and is the public default for newly-created Lite files.
    native_single_file,
};

pub const EngineSelection = enum {
    /// Open and create public Lite files as native v1.
    auto,
    bridge_lsm_container,
    native_single_file,
};

pub const OpenOptions = struct {
    engine: EngineSelection = .auto,
    read_only: bool = false,
    no_sync: bool = false,
};

pub fn isAflitePath(path: []const u8) bool {
    return std.mem.endsWith(u8, path, ".aflite");
}

pub const StorageStatus = struct {
    format: []const u8 = "aflite",
    engine: []const u8,
    format_version: ?u32 = null,
    page_size: ?u32 = null,
    active_checkpoint: ?u8 = null,
    checkpoint_sequence: ?u64 = null,
    page_count: ?u64 = null,
};

pub fn Status(comptime Stats: type) type {
    return struct {
        storage: StorageStatus,
        stats: Stats,
        pending_work: db_core.PendingWorkStats,
        inference: InferenceStatus,
        capabilities: Capabilities,

        pub fn deinit(self: *@This(), allocator: Allocator) void {
            if (Stats == db_types.DBStats) {
                db_types.freeDBStats(allocator, self.stats);
            }
            self.* = undefined;
        }
    };
}

pub const FullStatus = Status(db_types.DBStats);

pub fn checkFile(allocator: Allocator, path: []const u8) !CheckReport {
    if (!isAflitePath(path)) return error.InvalidArgument;
    return try native.checkFile(allocator, path);
}

pub const Handle = struct {
    allocator: Allocator,
    engine: EngineKind,
    bridge_storage: ?*bridge.ContainerStorage = null,
    native_docstore: ?*docstore.Store = null,
    native_index_storage: ?*index_storage.Store = null,
    native_runtime_store: ?*backend_erased.Store = null,

    pub fn open(allocator: Allocator, path: []const u8, opts: OpenOptions) !Handle {
        if (!isAflitePath(path)) return error.InvalidArgument;

        const engine = switch (opts.engine) {
            .auto => EngineKind.native_single_file,
            .bridge_lsm_container => EngineKind.bridge_lsm_container,
            .native_single_file => EngineKind.native_single_file,
        };

        return switch (engine) {
            .bridge_lsm_container => try openBridgeLsmContainer(allocator, path, opts),
            .native_single_file => try openNativeSingleFile(allocator, path, opts),
        };
    }

    pub fn create(allocator: Allocator, path: []const u8, exclusive: bool) !Handle {
        if (!isAflitePath(path)) return error.InvalidArgument;
        return try createNativeSingleFile(allocator, path, exclusive);
    }

    pub fn deinit(self: *Handle) void {
        switch (self.engine) {
            .bridge_lsm_container => {
                if (self.bridge_storage) |storage| {
                    storage.deinit();
                    self.allocator.destroy(storage);
                    self.bridge_storage = null;
                }
            },
            .native_single_file => {
                if (self.native_index_storage) |storage| {
                    self.allocator.destroy(storage);
                    self.native_index_storage = null;
                }
                if (self.native_runtime_store) |runtime_store| {
                    runtime_store.deinit();
                    self.allocator.destroy(runtime_store);
                    self.native_runtime_store = null;
                }
                if (self.native_docstore) |store| {
                    store.close();
                    self.allocator.destroy(store);
                    self.native_docstore = null;
                }
            },
        }
        self.* = undefined;
    }

    pub fn configureDbOpenOptions(self: *Handle, opts: *db_mod.OpenOptions) !void {
        switch (self.engine) {
            .bridge_lsm_container => {
                const storage = self.bridge_storage.?.storage();
                opts.storage = storage;
                opts.index_backends.text_lsm_storage = storage;
                opts.index_backends.dense_lsm_storage = storage;
                opts.index_backends.sparse_lsm_storage = storage;
                opts.index_backends.graph_lsm_storage = storage;
                opts.external_derived_checkpoints = false;
            },
            .native_single_file => {
                opts.primary_backend = .{ .mem = .{} };
                opts.primary_runtime_store = self.native_runtime_store.?;
                const storage = self.native_index_storage.?.storage();
                opts.index_backends.text_main_backend = .lsm;
                opts.index_backends.dense_storage_backend = .lsm;
                opts.index_backends.sparse_backend = .lsm;
                opts.index_backends.graph_reverse_backend = .lsm;
                opts.index_backends.text_lsm_storage = storage;
                opts.index_backends.dense_lsm_storage = storage;
                opts.index_backends.sparse_lsm_storage = storage;
                opts.index_backends.graph_lsm_storage = storage;
                opts.index_backends.text_main_lsm_options.storage = storage;
                opts.index_backends.text_wal_lsm_options.storage = storage;
                opts.index_backends.dense_lsm_options.storage = storage;
                opts.index_backends.sparse_lsm_options.storage = storage;
                opts.index_backends.graph_reverse_lsm_options.storage = storage;
                opts.index_base_path = native_index_base_path;
                opts.index_open_parallelism = 1;
                opts.external_derived_checkpoints = false;
            },
        }
    }

    pub fn storageStatus(self: *Handle) StorageStatus {
        return switch (self.engine) {
            .native_single_file => blk: {
                const file = &self.native_docstore.?.file;
                const checkpoint = file.activeCheckpoint();
                break :blk .{
                    .engine = @tagName(self.engine),
                    .format_version = native.format_version,
                    .page_size = file.header.page_size,
                    .active_checkpoint = file.header.active_checkpoint,
                    .checkpoint_sequence = checkpoint.commit_sequence,
                    .page_count = checkpoint.page_count,
                };
            },
            .bridge_lsm_container => .{
                .format = "aflite-internal",
                .engine = @tagName(self.engine),
            },
        };
    }

    pub fn fullStatus(self: *Handle, allocator: Allocator, db: *db_mod.DB, profile: Profile) !FullStatus {
        const stats_value = try db.stats(allocator);
        errdefer db_types.freeDBStats(allocator, stats_value);

        return .{
            .storage = self.storageStatus(),
            .stats = stats_value,
            .pending_work = db.pendingWorkStats(),
            .inference = inferenceStatusForProfile(profile),
            .capabilities = capabilitiesForProfile(profile),
        };
    }

    pub fn check(self: *Handle) !CheckReport {
        return switch (self.engine) {
            .bridge_lsm_container => toCheckReport(try self.bridge_storage.?.check()),
            .native_single_file => try self.native_docstore.?.file.check(),
        };
    }

    pub fn vacuum(self: *Handle) !VacuumReport {
        return switch (self.engine) {
            .bridge_lsm_container => toVacuumReport(try self.bridge_storage.?.vacuum()),
            .native_single_file => try nativeVacuumReport(self),
        };
    }

    pub fn copyStableSnapshot(self: *Handle, dest_path: []const u8, replace: bool) !StableSnapshotReport {
        if (!isAflitePath(dest_path)) return error.InvalidArgument;
        return switch (self.engine) {
            .bridge_lsm_container => error.UnsupportedLiteSnapshot,
            .native_single_file => try nativeStableSnapshot(self, dest_path, replace),
        };
    }
};

fn openBridgeLsmContainer(allocator: Allocator, path: []const u8, opts: OpenOptions) !Handle {
    var storage = try allocator.create(bridge.ContainerStorage);
    errdefer allocator.destroy(storage);

    storage.* = try bridge.ContainerStorage.openWithOptions(allocator, path, .{
        .read_only = opts.read_only,
    });
    errdefer storage.deinit();

    return .{
        .allocator = allocator,
        .engine = .bridge_lsm_container,
        .bridge_storage = storage,
    };
}

fn openNativeSingleFile(allocator: Allocator, path: []const u8, opts: OpenOptions) !Handle {
    var initial_store = try docstore.Store.openWithOptions(allocator, path, .{
        .read_only = opts.read_only,
        .no_sync = opts.no_sync,
    });
    errdefer initial_store.close();
    return try initNativeSingleFile(allocator, &initial_store);
}

fn createNativeSingleFile(allocator: Allocator, path: []const u8, exclusive: bool) !Handle {
    var initial_store = try docstore.Store.create(allocator, path, exclusive);
    errdefer initial_store.close();
    return try initNativeSingleFile(allocator, &initial_store);
}

fn initNativeSingleFile(allocator: Allocator, initial_store: *docstore.Store) !Handle {
    const store = try allocator.create(docstore.Store);
    errdefer allocator.destroy(store);

    store.* = initial_store.*;
    initial_store.* = undefined;
    errdefer store.close();

    const native_index_storage = try allocator.create(index_storage.Store);
    errdefer allocator.destroy(native_index_storage);
    native_index_storage.* = index_storage.Store.init(allocator, store);

    const runtime_store = try allocator.create(backend_erased.Store);
    errdefer allocator.destroy(runtime_store);

    runtime_store.* = try store.runtimeStore(allocator);
    errdefer runtime_store.deinit();

    return .{
        .allocator = allocator,
        .engine = .native_single_file,
        .native_docstore = store,
        .native_index_storage = native_index_storage,
        .native_runtime_store = runtime_store,
    };
}

fn toCheckReport(report: bridge.ContainerStorage.CheckReport) CheckReport {
    return .{
        .valid = report.valid,
        .file_size = report.file_size,
        .valid_prefix_size = report.valid_prefix_size,
        .tail_bytes = report.tail_bytes,
        .record_count = report.record_count,
        .live_file_count = report.live_file_count,
        .live_bytes = report.live_bytes,
        .compact_size = report.compact_size,
        .reclaimable_bytes = report.reclaimable_bytes,
        .issue = report.issue,
    };
}

fn toVacuumReport(report: bridge.ContainerStorage.VacuumReport) VacuumReport {
    return .{
        .before_size = report.before_size,
        .after_size = report.after_size,
        .reclaimed_bytes = report.reclaimed_bytes,
        .live_file_count = report.live_file_count,
        .live_bytes = report.live_bytes,
    };
}

fn nativeVacuumReport(handle: *Handle) !VacuumReport {
    const report = try handle.native_docstore.?.vacuum();
    return .{
        .before_size = report.before_size,
        .after_size = report.after_size,
        .reclaimed_bytes = report.reclaimed_bytes,
        .live_file_count = report.live_file_count,
        .live_bytes = report.live_bytes,
    };
}

fn nativeStableSnapshot(handle: *Handle, dest_path: []const u8, replace: bool) !StableSnapshotReport {
    const store = handle.native_docstore.?;
    platform_sync.lockYielding(&store.mutex);
    defer store.mutex.unlock();
    return try store.file.copyStableSnapshotToPath(dest_path, replace);
}

fn testPath(allocator: Allocator, tmp: std.testing.TmpDir, name: []const u8) ![]u8 {
    return try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/{s}", .{ tmp.sub_path, name });
}

test "lite backend capabilities distinguish native and hosted profiles" {
    const native_caps = capabilitiesForProfile(.native);
    try std.testing.expect(!native_caps.hosted_profile);
    try std.testing.expect(!native_caps.manual_maintenance);
    try std.testing.expect(native_caps.generated_enrichment_planning);
    try std.testing.expect(native_caps.dense_vector_search);
    try std.testing.expect(native_caps.sparse_vector_search);
    try std.testing.expectEqualStrings("caller_supplied_or_disabled", native_caps.inference_mode);
    try std.testing.expect(!native_caps.inference_required);
    try std.testing.expect(native_caps.no_inference_configured_ok);
    try std.testing.expect(native_caps.caller_supplied_artifacts);
    try std.testing.expect(native_caps.caller_supplied_embeddings);
    try std.testing.expect(!native_caps.local_inference_runtime);
    try std.testing.expect(native_caps.text_search);
    try std.testing.expect(native_caps.hybrid_search);
    try std.testing.expect(native_caps.graph_search);
    try std.testing.expect(!native_caps.distributed_shard_ownership);
    try std.testing.expect(!native_caps.raft_replication);
    try std.testing.expect(!native_caps.cluster_placement);
    try std.testing.expect(!native_caps.cross_node_joins);
    try std.testing.expect(!native_caps.remote_shard_fanout);
    try std.testing.expect(!native_caps.distributed_transaction_coordination);
    try std.testing.expect(!native_caps.cluster_heartbeat_status_aggregation);
    try std.testing.expect(!native_caps.server_side_autoscaling);
    try std.testing.expect(!native_caps.kubernetes_operator);
    try std.testing.expect(!native_caps.object_storage_primary);

    const hosted_caps = capabilitiesForProfile(.hosted);
    try std.testing.expect(hosted_caps.hosted_profile);
    try std.testing.expect(hosted_caps.manual_maintenance);
    try std.testing.expect(!hosted_caps.background_enrichment_runtime);
    try std.testing.expect(!hosted_caps.ttl_cleanup_runtime);
    try std.testing.expect(!hosted_caps.transaction_recovery_runtime);
}

test "lite backend capabilities contract is stable" {
    const expected_fields = [_][]const u8{
        "freestanding_build",
        "hosted_profile",
        "manual_maintenance",
        "background_enrichment_runtime",
        "ttl_cleanup_runtime",
        "transaction_recovery_runtime",
        "local_template_rendering",
        "remote_template_rendering",
        "remote_template_host_callbacks",
        "inference_mode",
        "supported_inference_modes",
        "available_inference_modes",
        "inference_required",
        "no_inference_configured_ok",
        "caller_supplied_artifacts",
        "caller_supplied_embeddings",
        "remote_inference_providers",
        "local_inference_runtime",
        "generated_enrichment_planning",
        "text_search",
        "dense_vector_search",
        "sparse_vector_search",
        "hybrid_search",
        "graph_search",
        "distributed_shard_ownership",
        "raft_replication",
        "cluster_placement",
        "cross_node_joins",
        "remote_shard_fanout",
        "distributed_transaction_coordination",
        "cluster_heartbeat_status_aggregation",
        "server_side_autoscaling",
        "kubernetes_operator",
        "object_storage_primary",
    };

    const fields = @typeInfo(Capabilities).@"struct".fields;
    try std.testing.expectEqual(expected_fields.len, fields.len);
    inline for (fields, 0..) |field, i| {
        try std.testing.expectEqualStrings(expected_fields[i], field.name);
    }

    const allocator = std.testing.allocator;
    const freestanding = builtin.os.tag == .freestanding;
    const native_json = try std.json.Stringify.valueAlloc(allocator, capabilitiesForProfile(.native), .{});
    defer allocator.free(native_json);
    const native_available_modes =
        if (freestanding)
            "[\"caller_supplied_artifacts\",\"disabled_deferred\"]"
        else
            "[\"caller_supplied_artifacts\",\"remote_provider\",\"disabled_deferred\"]";
    const supported_modes_json = "[\"caller_supplied_artifacts\",\"remote_provider\",\"local_embedded\",\"manual_maintenance\",\"disabled_deferred\"]";
    const expected_native = try std.fmt.allocPrint(allocator, "{{\"freestanding_build\":{},\"hosted_profile\":false,\"manual_maintenance\":false,\"background_enrichment_runtime\":{},\"ttl_cleanup_runtime\":{},\"transaction_recovery_runtime\":{},\"local_template_rendering\":true,\"remote_template_rendering\":{},\"remote_template_host_callbacks\":{},\"inference_mode\":\"caller_supplied_or_disabled\",\"supported_inference_modes\":{s},\"available_inference_modes\":{s},\"inference_required\":false,\"no_inference_configured_ok\":true,\"caller_supplied_artifacts\":true,\"caller_supplied_embeddings\":true,\"remote_inference_providers\":{},\"local_inference_runtime\":false,\"generated_enrichment_planning\":true,\"text_search\":true,\"dense_vector_search\":true,\"sparse_vector_search\":true,\"hybrid_search\":true,\"graph_search\":true,\"distributed_shard_ownership\":false,\"raft_replication\":false,\"cluster_placement\":false,\"cross_node_joins\":false,\"remote_shard_fanout\":false,\"distributed_transaction_coordination\":false,\"cluster_heartbeat_status_aggregation\":false,\"server_side_autoscaling\":false,\"kubernetes_operator\":false,\"object_storage_primary\":false}}", .{
        freestanding,
        !freestanding,
        !freestanding,
        !freestanding,
        !freestanding,
        freestanding,
        supported_modes_json,
        native_available_modes,
        !freestanding,
    });
    defer allocator.free(expected_native);
    try std.testing.expectEqualStrings(expected_native, native_json);

    const hosted_json = try std.json.Stringify.valueAlloc(allocator, capabilitiesForProfile(.hosted), .{});
    defer allocator.free(hosted_json);
    const hosted_available_modes =
        if (freestanding)
            "[\"caller_supplied_artifacts\",\"manual_maintenance\",\"disabled_deferred\"]"
        else
            "[\"caller_supplied_artifacts\",\"remote_provider\",\"manual_maintenance\",\"disabled_deferred\"]";
    const expected_hosted = try std.fmt.allocPrint(allocator, "{{\"freestanding_build\":{},\"hosted_profile\":true,\"manual_maintenance\":true,\"background_enrichment_runtime\":false,\"ttl_cleanup_runtime\":false,\"transaction_recovery_runtime\":false,\"local_template_rendering\":true,\"remote_template_rendering\":{},\"remote_template_host_callbacks\":{},\"inference_mode\":\"caller_supplied_or_disabled\",\"supported_inference_modes\":{s},\"available_inference_modes\":{s},\"inference_required\":false,\"no_inference_configured_ok\":true,\"caller_supplied_artifacts\":true,\"caller_supplied_embeddings\":true,\"remote_inference_providers\":{},\"local_inference_runtime\":false,\"generated_enrichment_planning\":true,\"text_search\":true,\"dense_vector_search\":true,\"sparse_vector_search\":true,\"hybrid_search\":true,\"graph_search\":true,\"distributed_shard_ownership\":false,\"raft_replication\":false,\"cluster_placement\":false,\"cross_node_joins\":false,\"remote_shard_fanout\":false,\"distributed_transaction_coordination\":false,\"cluster_heartbeat_status_aggregation\":false,\"server_side_autoscaling\":false,\"kubernetes_operator\":false,\"object_storage_primary\":false}}", .{
        freestanding,
        !freestanding,
        freestanding,
        supported_modes_json,
        hosted_available_modes,
        !freestanding,
    });
    defer allocator.free(expected_hosted);
    try std.testing.expectEqualStrings(expected_hosted, hosted_json);
}

test "lite backend inference status reports disabled as clean state" {
    const expected_fields = [_][]const u8{
        "mode",
        "available_modes",
        "configured",
        "remote_provider_configured",
        "local_runtime_configured",
        "local_runtime_available",
        "caller_supplied_artifacts",
        "no_inference_configured_ok",
    };

    const fields = @typeInfo(InferenceStatus).@"struct".fields;
    try std.testing.expectEqual(expected_fields.len, fields.len);
    inline for (fields, 0..) |field, i| {
        try std.testing.expectEqualStrings(expected_fields[i], field.name);
    }

    const status = inferenceStatusForProfile(.native);
    try std.testing.expectEqualStrings("caller_supplied_or_disabled", status.mode);
    try std.testing.expect(std.mem.eql(u8, status.available_modes[0], "caller_supplied_artifacts"));
    try std.testing.expect(std.mem.eql(u8, status.available_modes[status.available_modes.len - 1], "disabled_deferred"));
    try std.testing.expect(!status.configured);
    try std.testing.expect(!status.remote_provider_configured);
    try std.testing.expect(!status.local_runtime_configured);
    try std.testing.expect(!status.local_runtime_available);
    try std.testing.expect(status.caller_supplied_artifacts);
    try std.testing.expect(status.no_inference_configured_ok);

    const hosted = inferenceStatusForProfile(.hosted);
    try std.testing.expectEqualStrings("caller_supplied_or_disabled", hosted.mode);
    var hosted_has_manual = false;
    for (hosted.available_modes) |mode| {
        if (std.mem.eql(u8, mode, "manual_maintenance")) hosted_has_manual = true;
    }
    try std.testing.expect(hosted_has_manual);
    try std.testing.expect(!hosted.configured);
    try std.testing.expect(hosted.no_inference_configured_ok);
}

test "lite backend inference status reflects explicit remote provider configuration" {
    const remote = inferenceStatusForProfileWithOptions(.native, .{ .remote_provider_configured = true });
    try std.testing.expectEqualStrings("remote_provider", remote.mode);
    try std.testing.expect(remote.configured);
    try std.testing.expect(remote.remote_provider_configured);
    try std.testing.expect(!remote.local_runtime_configured);
    try std.testing.expect(!remote.local_runtime_available);
    try std.testing.expect(remote.caller_supplied_artifacts);
    try std.testing.expect(remote.no_inference_configured_ok);

    const local_requested = inferenceStatusForProfileWithOptions(.native, .{ .local_runtime_configured = true });
    try std.testing.expectEqualStrings("caller_supplied_or_disabled", local_requested.mode);
    try std.testing.expect(!local_requested.configured);
    try std.testing.expect(!local_requested.local_runtime_configured);
    try std.testing.expect(!local_requested.local_runtime_available);
}

test "lite backend native engine creates and checks aflite file" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-backend.aflite");
    defer allocator.free(path);

    var handle = try Handle.open(allocator, path, .{ .engine = .native_single_file });
    defer handle.deinit();

    try handle.native_docstore.?.file.putDocument("doc:1", "value");

    const report = try handle.check();
    try std.testing.expect(report.valid);
    try std.testing.expectEqual(@as(u64, 1), report.record_count);

    var db_opts = db_mod.OpenOptions{};
    try handle.configureDbOpenOptions(&db_opts);
    try std.testing.expect(db_opts.primary_runtime_store != null);
    try std.testing.expect(db_opts.primary_backend == .mem);
    try std.testing.expectEqual(@as(@TypeOf(db_opts.index_backends.text_main_backend), .lsm), db_opts.index_backends.text_main_backend);
    try std.testing.expectEqual(@as(@TypeOf(db_opts.index_backends.dense_storage_backend), .lsm), db_opts.index_backends.dense_storage_backend);
    try std.testing.expectEqual(@as(@TypeOf(db_opts.index_backends.sparse_backend), .lsm), db_opts.index_backends.sparse_backend);
    try std.testing.expectEqual(@as(@TypeOf(db_opts.index_backends.graph_reverse_backend), .lsm), db_opts.index_backends.graph_reverse_backend);
    try std.testing.expect(db_opts.index_backends.text_lsm_storage != null);
    try std.testing.expect(db_opts.index_backends.dense_lsm_storage != null);
    try std.testing.expect(db_opts.index_backends.sparse_lsm_storage != null);
    try std.testing.expect(db_opts.index_backends.graph_lsm_storage != null);
    try std.testing.expectEqualStrings(native_index_base_path, db_opts.index_base_path.?);
    try std.testing.expectEqual(@as(?usize, 1), db_opts.index_open_parallelism);

    const vacuumed = try handle.vacuum();
    try std.testing.expectEqual(report.file_size, vacuumed.before_size);
    try std.testing.expectEqual(report.file_size, vacuumed.after_size);
    try std.testing.expectEqual(@as(u64, 0), vacuumed.reclaimed_bytes);
}

test "lite backend propagates no_sync to native engine" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-backend-no-sync.aflite");
    defer allocator.free(path);

    {
        var handle = try Handle.open(allocator, path, .{
            .engine = .native_single_file,
            .no_sync = true,
        });
        defer handle.deinit();

        try std.testing.expect(handle.native_docstore.?.file.no_sync);
        try handle.native_docstore.?.file.putDocument("doc:no-sync", "value");
    }

    {
        var handle = try Handle.open(allocator, path, .{
            .engine = .native_single_file,
            .read_only = true,
            .no_sync = true,
        });
        defer handle.deinit();

        try std.testing.expect(handle.native_docstore.?.file.no_sync);
        const value = (try handle.native_docstore.?.file.getDocumentAlloc(allocator, "doc:no-sync")) orelse return error.MissingNativeLiteDocument;
        defer allocator.free(value);
        try std.testing.expectEqualStrings("value", value);
    }
}

test "lite backend reports native storage status from active checkpoint" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-status.aflite");
    defer allocator.free(path);

    var handle = try Handle.open(allocator, path, .{ .engine = .native_single_file });
    defer handle.deinit();

    try handle.native_docstore.?.file.putDocument("doc:status", "value");

    const status = handle.storageStatus();
    const checkpoint = handle.native_docstore.?.file.activeCheckpoint();
    try std.testing.expectEqualStrings("aflite", status.format);
    try std.testing.expectEqualStrings("native_single_file", status.engine);
    try std.testing.expectEqual(native.format_version, status.format_version.?);
    try std.testing.expectEqual(native.default_page_size, status.page_size.?);
    try std.testing.expectEqual(handle.native_docstore.?.file.header.active_checkpoint, status.active_checkpoint.?);
    try std.testing.expectEqual(checkpoint.commit_sequence, status.checkpoint_sequence.?);
    try std.testing.expectEqual(checkpoint.page_count, status.page_count.?);
}

test "lite backend native stable snapshot uses open handle checkpoint" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-backend-snapshot.aflite");
    defer allocator.free(path);
    const snapshot_path = try testPath(allocator, tmp, "native-backend-snapshot-copy.aflite");
    defer allocator.free(snapshot_path);

    var handle = try Handle.open(allocator, path, .{ .engine = .native_single_file });
    defer handle.deinit();

    try handle.native_docstore.?.file.putDocument("doc:1", "value");
    const snapshot_size = handle.native_docstore.?.file.activeCheckpoint().page_count *
        @as(u64, handle.native_docstore.?.file.header.page_size);
    try handle.native_docstore.?.file.file.writePositionalAll(
        handle.native_docstore.?.file.io_impl.io(),
        "tail",
        snapshot_size,
    );

    const source_report = try handle.check();
    try std.testing.expect(!source_report.valid);
    try std.testing.expectEqualStrings("tail_bytes", source_report.issue.?);

    const copied = try handle.copyStableSnapshot(snapshot_path, false);
    try std.testing.expectEqual(snapshot_size, copied.snapshot_size);
    try std.testing.expectEqual(@as(u64, 4), copied.tail_bytes);

    const snapshot_report = try checkFile(allocator, snapshot_path);
    try std.testing.expect(snapshot_report.valid);
    try std.testing.expectEqual(@as(u64, 0), snapshot_report.tail_bytes);

    var snapshot = try native.NativeFile.open(allocator, snapshot_path, true);
    defer snapshot.close();
    const value = (try snapshot.getDocumentAlloc(allocator, "doc:1")).?;
    defer allocator.free(value);
    try std.testing.expectEqualStrings("value", value);
}

test "lite backend auto creates new aflite files with native engine" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "auto-native.aflite");
    defer allocator.free(path);

    var handle = try Handle.open(allocator, path, .{});
    defer handle.deinit();

    try std.testing.expectEqual(EngineKind.native_single_file, handle.engine);
    try handle.native_docstore.?.file.putDocument("doc:auto", "native");

    const report = try handle.check();
    try std.testing.expect(report.valid);
    try std.testing.expectEqual(@as(u64, 1), report.record_count);
}

test "lite backend rejects non-aflite paths" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "not-lite.db");
    defer allocator.free(path);
    const snapshot_path = try testPath(allocator, tmp, "not-lite-copy.db");
    defer allocator.free(snapshot_path);
    const lite_path = try testPath(allocator, tmp, "valid-source.aflite");
    defer allocator.free(lite_path);

    try std.testing.expect(!isAflitePath(path));
    try std.testing.expectError(error.InvalidArgument, Handle.open(allocator, path, .{}));
    try std.testing.expectError(error.InvalidArgument, checkFile(allocator, path));

    var handle = try Handle.open(allocator, lite_path, .{ .engine = .native_single_file });
    defer handle.deinit();
    try std.testing.expectError(error.InvalidArgument, handle.copyStableSnapshot(snapshot_path, false));
}

test "lite backend auto rejects internal bridge aflite files" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "auto-bridge.aflite");
    defer allocator.free(path);

    {
        var handle = try Handle.open(allocator, path, .{ .engine = .bridge_lsm_container });
        defer handle.deinit();
        try std.testing.expectEqual(EngineKind.bridge_lsm_container, handle.engine);
    }

    try std.testing.expectError(error.TruncatedNativeHeader, Handle.open(allocator, path, .{}));
}

test "lite backend native engine can back db primary documents" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-db.aflite");
    defer allocator.free(path);

    {
        var handle = try Handle.open(allocator, path, .{ .engine = .native_single_file });
        defer handle.deinit();

        var db_opts = db_mod.OpenOptions{
            .open_mode = .writer_no_replay,
            .start_index_workers = false,
            .start_optional_runtimes = false,
            .ttl_cleanup = .{ .enabled = false },
        };
        try handle.configureDbOpenOptions(&db_opts);

        var db = try db_mod.DB.open(allocator, path, db_opts);
        defer db.close();

        try db.batch(.{
            .writes = &.{.{
                .key = "doc:lite-native",
                .value = "{\"name\":\"native\"}",
            }},
            .sync_level = .write,
        });

        const value = try db.get(allocator, "doc:lite-native") orelse return error.MissingNativeLiteDocument;
        defer allocator.free(value);
        try std.testing.expectEqualStrings("{\"name\":\"native\"}", value);
    }

    {
        var handle = try Handle.open(allocator, path, .{
            .engine = .native_single_file,
            .read_only = true,
        });
        defer handle.deinit();

        var db_opts = db_mod.OpenOptions{
            .open_mode = .query_readonly,
            .start_index_workers = false,
            .start_optional_runtimes = false,
            .ttl_cleanup = .{ .enabled = false },
        };
        try handle.configureDbOpenOptions(&db_opts);

        var db = try db_mod.DB.open(allocator, path, db_opts);
        defer db.close();

        const value = try db.get(allocator, "doc:lite-native") orelse return error.MissingNativeLiteDocument;
        defer allocator.free(value);
        try std.testing.expectEqualStrings("{\"name\":\"native\"}", value);
    }
}

test "lite backend native read-only open requires an existing file" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-missing.aflite");
    defer allocator.free(path);

    try std.testing.expectError(error.FileNotFound, Handle.open(allocator, path, .{
        .engine = .native_single_file,
        .read_only = true,
    }));
}
