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
const antfly = @import("../cli_root.zig");
const kernel_owner_client = @import("../storage/kernel_owner_client.zig");
const seed_activation_contract = @import("../storage/hot_standby/seed_activation_contract.zig");
const storage_source_options = @import("storage_source_options");
const Sha256 = std.crypto.hash.sha2.Sha256;

const admin_api = antfly.admin;
const ha = antfly.hot_standby;
const ha_validation = ha.validation;
const http_common = antfly.common.http.http_common;
const control_only_storage_sources = storage_source_options.control_only;

var test_path_counter: u64 = 0;

/// Command output goes to stdout in production. Under the test runner stdout
/// is the build server's IPC channel, so tests must never write to it: output
/// is captured by `test_output_sink` when a test installs one and discarded
/// otherwise.
var test_output_sink: ?*std.Io.Writer = null;

fn emit(io: std.Io, bytes: []const u8) void {
    emitOrFail(io, bytes) catch {};
}

fn emitOrFail(io: std.Io, bytes: []const u8) !void {
    if (builtin.is_test) {
        const sink = test_output_sink orelse return;
        return sink.writeAll(bytes);
    }
    try std.Io.File.stdout().writeStreamingAll(io, bytes);
}

const LocalOptions = struct {
    remote_url: ?[]const u8 = null,
    remote_token_env: ?[]const u8 = null,
    primary_log: ?[]const u8 = null,
    primary_slots: ?[]const u8 = null,
    primary_node_id: ?[]const u8 = null,
    standby_log: ?[]const u8 = null,
    standby_progress: ?[]const u8 = null,
    standby_node_id: ?[]const u8 = null,
    fence_wal: ?[]const u8 = null,
    former_primary_log: ?[]const u8 = null,
    data_dir: ?[]const u8 = null,
    config_path: ?[]const u8 = null,
    identity: IdentityOptions = .{},

    fn wantsPrimary(self: LocalOptions) bool {
        return self.primary_log != null or self.primary_slots != null;
    }

    fn hasLocalHandles(self: LocalOptions) bool {
        return self.wantsPrimary() or self.wantsStandby() or
            self.fence_wal != null or self.former_primary_log != null;
    }

    fn wantsStandby(self: LocalOptions) bool {
        return self.standby_log != null or self.standby_progress != null;
    }

    fn primaryIdentity(self: LocalOptions) !ha.standby.Identity {
        if (self.primary_log == null) return error.PrimaryLogMissing;
        if (self.primary_slots == null) return error.PrimarySlotsMissing;
        return try self.identity.finish();
    }

    fn standbyIdentity(self: LocalOptions) !ha.standby.Identity {
        if (self.standby_log == null) return error.StandbyLogMissing;
        if (self.standby_progress == null) return error.StandbyProgressMissing;
        return try self.identity.finish();
    }
};

const IdentityOptions = struct {
    cluster_id: ?u64 = null,
    shard_id: ?u64 = null,
    table_id: ?u64 = null,
    timeline_id: ?u64 = null,
    epoch: ?u64 = null,

    fn finish(self: IdentityOptions) !ha.standby.Identity {
        return .{
            .cluster_id = self.cluster_id orelse return error.ClusterIdMissing,
            .shard_id = self.shard_id orelse 0,
            .table_id = self.table_id orelse 0,
            .timeline_id = self.timeline_id orelse return error.TimelineIdMissing,
            .epoch = self.epoch orelse return error.EpochMissing,
        };
    }
};

const ParsedArgs = struct {
    options: LocalOptions,
    command_args: []const []const u8,

    fn deinit(self: *ParsedArgs, alloc: std.mem.Allocator) void {
        alloc.free(self.command_args);
        self.* = undefined;
    }
};

const RemoteOptions = struct {
    bearer_token: ?[]const u8 = null,
};

pub fn run(init: std.process.Init) !void {
    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, init.gpa);
    defer args.deinit();

    const argv0 = args.next() orelse "antfly";
    return try runFromIterator(init, argv0, &args);
}

pub fn runFromIterator(init: std.process.Init, argv0: []const u8, args: *std.process.Args.Iterator) !void {
    const first = args.next() orelse {
        printUsage(argv0);
        return;
    };
    if (std.mem.eql(u8, first, "--help") or std.mem.eql(u8, first, "-h") or std.mem.eql(u8, first, "help")) {
        printUsage(argv0);
        return;
    }

    var all_args = std.ArrayListUnmanaged([]const u8).empty;
    defer all_args.deinit(init.gpa);
    try all_args.append(init.gpa, first);
    while (args.next()) |arg| try all_args.append(init.gpa, arg);

    try runArgv(init.gpa, init.io, all_args.items);
}

pub fn runArgv(alloc: std.mem.Allocator, io: std.Io, argv: []const []const u8) !void {
    if (argv.len > 0 and std.mem.eql(u8, argv[0], "artifact")) {
        try runArtifactArgv(alloc, io, argv[1..]);
        return;
    }
    var parsed = try parseLocalArgs(alloc, argv);
    defer parsed.deinit(alloc);
    if (parsed.command_args.len == 0) {
        printUsage("antfly");
        return error.HaCommandMissing;
    }
    if (std.mem.eql(u8, parsed.command_args[0], "artifact")) {
        if (parsed.options.remote_url != null or parsed.options.remote_token_env != null or
            parsed.options.data_dir != null or parsed.options.hasLocalHandles())
        {
            return error.SeedArtifactCannotUseAdminHandles;
        }
        try runArtifactArgv(alloc, io, parsed.command_args[1..]);
        return;
    }

    // Target resolution order: explicit flags, then environment, then the
    // config file, then whatever --data-dir finds on disk.
    try applyEnvironmentDefaults(&parsed.options, process_env);
    var loaded_config = try applyConfigDefaults(alloc, io, &parsed.options);
    defer if (loaded_config) |*cfg| cfg.deinit();
    try applyEnvironmentDefaults(&parsed.options, process_env);

    // Derived strings (data-dir paths) live for the rest of this call.
    var derived_arena = std.heap.ArenaAllocator.init(alloc);
    defer derived_arena.deinit();
    try applyDataDir(derived_arena.allocator(), io, &parsed.options);

    if (parsed.options.remote_url) |remote_url| {
        if (parsed.options.wantsPrimary() or parsed.options.wantsStandby() or
            parsed.options.fence_wal != null or parsed.options.former_primary_log != null)
        {
            return error.HaRemoteCannotUseLocalHandles;
        }
        const bearer_token = try resolveRemoteBearerToken(alloc, parsed.options);
        defer if (bearer_token) |token| alloc.free(token);
        var executor = antfly.common.http.StdHttpExecutor.init(alloc, .{});
        defer executor.deinit();
        if (std.mem.eql(u8, parsed.command_args[0], "switchover")) {
            try runSwitchover(alloc, io, remote_url, parsed.command_args[1..], executor.executor(), .{
                .bearer_token = bearer_token,
            });
            return;
        }
        try runRemoteArgvWithOptions(alloc, io, remote_url, parsed.command_args, executor.executor(), .{
            .bearer_token = bearer_token,
        });
        return;
    }
    if (parsed.options.remote_token_env != null) return error.HaTokenEnvRequiresRemote;
    if (std.mem.eql(u8, parsed.command_args[0], "switchover")) return error.SwitchoverRequiresAdminApi;

    var plan = try ha.admin_cli.parse(alloc, parsed.command_args);
    defer plan.deinit(alloc);

    var primary: ?ha.primary.Primary = null;
    defer if (primary) |*handle| handle.close();

    var standby: ?ha.standby.Standby = null;
    defer if (standby) |*handle| handle.close();

    var fence_store: ?ha.fencing.Store = null;
    defer if (fence_store) |*handle| handle.close();

    var former_primary_log: ?ha.replication_log.ReplicationLog = null;
    defer if (former_primary_log) |*handle| handle.close();

    if (parsed.options.wantsPrimary()) {
        const identity = try parsed.options.primaryIdentity();
        const primary_log = try zPath(alloc, parsed.options.primary_log.?);
        defer alloc.free(primary_log);
        const primary_slots = try zPath(alloc, parsed.options.primary_slots.?);
        defer alloc.free(primary_slots);
        primary = try ha.primary.Primary.open(
            alloc,
            primary_log.ptr,
            primary_slots.ptr,
            identity,
            .{},
        );
    }
    if (parsed.options.wantsStandby()) {
        const identity = try parsed.options.standbyIdentity();
        const standby_log = try zPath(alloc, parsed.options.standby_log.?);
        defer alloc.free(standby_log);
        const standby_progress = try zPath(alloc, parsed.options.standby_progress.?);
        defer alloc.free(standby_progress);
        standby = try ha.standby.Standby.open(
            alloc,
            standby_log.ptr,
            standby_progress.ptr,
            identity,
            .{},
        );
    }
    if (parsed.options.fence_wal) |path| {
        const fence_wal = try zPath(alloc, path);
        defer alloc.free(fence_wal);
        fence_store = try ha.fencing.Store.open(alloc, fence_wal.ptr, .{});
    }
    if (parsed.options.former_primary_log) |path| {
        const former_log_path = try zPath(alloc, path);
        defer alloc.free(former_log_path);
        former_primary_log = try ha.replication_log.ReplicationLog.open(former_log_path.ptr, .{});
    }

    var rendered = try ha.admin_exec.executeAndRenderAlloc(alloc, .{
        .primary = if (primary) |*handle| handle else null,
        .primary_node_id = parsed.options.primary_node_id,
        .standby = if (standby) |*handle| handle else null,
        .standby_node_id = parsed.options.standby_node_id,
        .fence_store = if (fence_store) |*handle| handle else null,
        .former_primary_log = if (former_primary_log) |*handle| handle else null,
    }, plan);
    defer rendered.deinit(alloc);

    emit(io, rendered.body);
    emit(io, "\n");
}

const ArtifactAction = enum { publish, restore, verify, activate, prune, gc_source, gc_target, delete_prefix };

const ArtifactFlag = enum {
    location,
    generation,
    slot,
    manifest,
    content_root,
    capture_receipt,
    capture_receipt_sha256,
    staging_root,
    target_root,
    capture_root,
    slot_activation_receipt,
    ha_cluster_id,
    ha_shard_id,
    ha_table_id,
    ha_timeline_id,
    ha_epoch,
    minimum_checkpoint_lsn,
    topology_id,
    topology_generation,
    node_id,
    target_pvc_name,
    target_pvc_uid,
    target_local_node_id,
    target_replica_id,
    retain_generations,
    protect_generation,
    operation_id,
    retry_token,
    instance_id,
    prefix_sha256,
    credentials_secret_name,
    delete_all,
    request_sha256,
};

const PrefixCleanupOptions = struct {
    operation_id: ?[]const u8 = null,
    retry_token: ?[]const u8 = null,
    instance_id: ?[]const u8 = null,
    topology_id: ?[]const u8 = null,
    topology_generation: ?u64 = null,
    prefix_sha256: ?[]const u8 = null,
    credentials_secret_name: ?[]const u8 = null,
    delete_all: bool = false,
    request_sha256: ?[]const u8 = null,

    fn finish(self: PrefixCleanupOptions, location: ?[]const u8) !ha.seed_prefix_cleanup.Request {
        return .{
            .version = ha.seed_prefix_cleanup.request_version,
            .kind = ha.seed_prefix_cleanup.request_kind,
            .operation_id = self.operation_id orelse return error.SeedPrefixCleanupOperationIdMissing,
            .retry_token = self.retry_token orelse return error.SeedPrefixCleanupRetryTokenMissing,
            .instance_id = self.instance_id orelse return error.SeedPrefixCleanupInstanceIdMissing,
            .topology_id = self.topology_id orelse return error.TopologyIdMissing,
            .topology_generation = self.topology_generation orelse return error.TopologyGenerationMissing,
            .location = location orelse return error.SeedLocationMissing,
            .prefix_sha256 = self.prefix_sha256 orelse return error.SeedPrefixCleanupPrefixDigestMissing,
            .credentials_secret_name = self.credentials_secret_name orelse return error.SeedPrefixCleanupCredentialsSecretMissing,
            .delete_all = self.delete_all,
            .request_sha256 = self.request_sha256 orelse return error.SeedPrefixCleanupRequestDigestMissing,
        };
    }
};

const ActivationBindingOptions = struct {
    topology_id: ?[]const u8 = null,
    topology_generation: ?u64 = null,
    node_id: ?[]const u8 = null,
    target_pvc_name: ?[]const u8 = null,
    target_pvc_uid: ?[]const u8 = null,

    fn requested(self: ActivationBindingOptions) bool {
        return self.topology_id != null or self.topology_generation != null or self.node_id != null or
            self.target_pvc_name != null or self.target_pvc_uid != null;
    }

    fn finish(self: ActivationBindingOptions) !?seed_activation_contract.ActivationBinding {
        if (!self.requested()) return null;
        return .{
            .topology_id = self.topology_id orelse return error.TopologyIdMissing,
            .topology_generation = self.topology_generation orelse return error.TopologyGenerationMissing,
            .node_id = self.node_id orelse return error.NodeIdMissing,
            .target_pvc_name = self.target_pvc_name orelse return error.TargetPVCNameMissing,
            .target_pvc_uid = self.target_pvc_uid orelse return error.TargetPVCUIDMissing,
        };
    }
};

const ArtifactOptions = struct {
    action: ArtifactAction,
    location: ?[]const u8 = null,
    generation: ?[]const u8 = null,
    slot_name: ?[]const u8 = null,
    manifest_path: ?[]const u8 = null,
    content_root: ?[]const u8 = null,
    capture_receipt_path: ?[]const u8 = null,
    capture_receipt_sha256: ?[]const u8 = null,
    staging_root: ?[]const u8 = null,
    target_root: ?[]const u8 = null,
    capture_root: ?[]const u8 = null,
    slot_activation_receipt_path: ?[]const u8 = null,
    identity: IdentityOptions = .{},
    binding: ActivationBindingOptions = .{},
    minimum_checkpoint_lsn: u64 = 0,
    target_local_node_id: ?u64 = null,
    target_replica_id: ?u64 = null,
    retain_generations: usize = 2,
    protected_generations: []const []const u8 = &.{},
    owns_protected_generations: bool = false,
    cleanup: PrefixCleanupOptions = .{},

    fn deinit(self: *ArtifactOptions, alloc: std.mem.Allocator) void {
        if (self.owns_protected_generations) alloc.free(self.protected_generations);
        self.* = undefined;
    }
};

fn runArtifactArgv(alloc: std.mem.Allocator, io: std.Io, argv: []const []const u8) !void {
    var options = try parseArtifactArgs(alloc, argv);
    defer options.deinit(alloc);

    switch (options.action) {
        .publish => {
            const generation = options.generation orelse return error.SeedGenerationMissing;
            const slot_name = options.slot_name orelse return error.SeedSlotMissing;
            const location = options.location orelse return error.SeedLocationMissing;
            const manifest_path = options.manifest_path orelse return error.SeedManifestMissing;
            const content_root = options.content_root orelse return error.SeedContentRootMissing;
            const capture_receipt_path = options.capture_receipt_path orelse return error.CaptureReceiptMissing;
            const capture_receipt_sha256 = try requireCaptureReceiptDigest(options.capture_receipt_sha256);
            const binding = (try options.binding.finish()) orelse return error.SeedArtifactBindingMissing;
            const manifest_bytes = try readArtifactFileAlloc(alloc, manifest_path, (ha.seed_artifact.Limits{}).max_manifest_bytes);
            defer alloc.free(manifest_bytes);
            const capture_receipt_json = try readArtifactFileAlloc(alloc, capture_receipt_path, (ha.seed_artifact.Limits{}).max_receipt_bytes);
            defer alloc.free(capture_receipt_json);
            var opened = try antfly.serverless.object_store_support.OpenedObjectStore.initRemoteUri(alloc, location, "antfly-ha-seeds");
            defer opened.deinit();
            var result = try ha.seed_artifact.publish(alloc, .{
                .client = &opened.client,
                .bucket = opened.bucket,
                .prefix = opened.prefix,
            }, .{
                .generation = generation,
                .slot_name = slot_name,
                .manifest_bytes = manifest_bytes,
                .content_root = content_root,
                .capture_receipt_json = capture_receipt_json,
                .capture_receipt_sha256 = capture_receipt_sha256,
                .binding = binding,
            });
            defer result.deinit(alloc);
            try writeArtifactResult(io, result.receipt_json);
        },
        .restore => {
            const generation = options.generation orelse return error.SeedGenerationMissing;
            const slot_name = options.slot_name orelse return error.SeedSlotMissing;
            const location = options.location orelse return error.SeedLocationMissing;
            const staging_root = options.staging_root orelse return error.SeedStagingRootMissing;
            const capture_receipt_sha256 = try requireCaptureReceiptDigest(options.capture_receipt_sha256);
            const binding = (try options.binding.finish()) orelse return error.SeedArtifactBindingMissing;
            var opened = try antfly.serverless.object_store_support.OpenedObjectStore.initRemoteUri(alloc, location, "antfly-ha-seeds");
            defer opened.deinit();
            var result = try ha.seed_artifact.restoreToStaging(alloc, .{
                .client = &opened.client,
                .bucket = opened.bucket,
                .prefix = opened.prefix,
            }, .{
                .expected = .{
                    .generation = generation,
                    .slot_name = slot_name,
                    .identity = try options.identity.finish(),
                    .minimum_checkpoint_lsn = options.minimum_checkpoint_lsn,
                    .binding = binding,
                    .capture_receipt_sha256 = capture_receipt_sha256,
                },
                .staging_root = staging_root,
            });
            defer result.deinit(alloc);
            try writeArtifactResult(io, result.receipt_json);
        },
        .verify => {
            const generation = options.generation orelse return error.SeedGenerationMissing;
            const slot_name = options.slot_name orelse return error.SeedSlotMissing;
            const staging_root = options.staging_root orelse return error.SeedStagingRootMissing;
            const capture_receipt_sha256 = try requireCaptureReceiptDigest(options.capture_receipt_sha256);
            const binding = (try options.binding.finish()) orelse return error.SeedArtifactBindingMissing;
            try ha.seed_artifact.verifyStaged(alloc, staging_root, .{
                .generation = generation,
                .slot_name = slot_name,
                .identity = try options.identity.finish(),
                .minimum_checkpoint_lsn = options.minimum_checkpoint_lsn,
                .binding = binding,
                .capture_receipt_sha256 = capture_receipt_sha256,
            }, .{});
            emit(io, "{\"verified\":true}\n");
        },
        .activate => {
            const generation = options.generation orelse return error.SeedGenerationMissing;
            const slot_name = options.slot_name orelse return error.SeedSlotMissing;
            const staging_root = options.staging_root orelse return error.SeedStagingRootMissing;
            const target_root = options.target_root orelse return error.SeedActivationTargetMissing;
            const capture_receipt_sha256 = try requireCaptureReceiptDigest(options.capture_receipt_sha256);
            const binding = (try options.binding.finish()) orelse return error.SeedArtifactBindingMissing;
            const target_local_node_id = options.target_local_node_id orelse return error.TargetLocalNodeIdMissing;
            const target_replica_id = options.target_replica_id orelse return error.TargetReplicaIdMissing;
            if (target_local_node_id == 0) return error.InvalidTargetLocalNodeId;
            if (target_replica_id == 0) return error.InvalidTargetReplicaId;
            const request = seed_activation_contract.ActivateRequest{
                .staging_root = staging_root,
                .target_root = target_root,
                .expected = .{
                    .generation = generation,
                    .slot_name = slot_name,
                    .identity = try options.identity.finish(),
                    .minimum_checkpoint_lsn = options.minimum_checkpoint_lsn,
                    .binding = binding,
                    .capture_receipt_sha256 = capture_receipt_sha256,
                },
                .binding = binding,
                .materialization = .{
                    .target_local_node_id = target_local_node_id,
                    .target_replica_id = target_replica_id,
                },
                .pod_uid = try resolveHAPodUID(),
            };
            if (comptime control_only_storage_sources) {
                const request_json = try std.json.Stringify.valueAlloc(alloc, request, .{});
                defer alloc.free(request_json);
                var response = try kernel_owner_client.haSeedActivate(request_json);
                defer response.deinit();
                try writeArtifactResult(io, response.bytes());
            } else {
                var result = try ha.seed_activation.activate(alloc, request);
                defer result.deinit(alloc);
                try writeArtifactResult(io, result.active_receipt_json);
            }
        },
        .prune => {
            const generation = options.generation orelse return error.SeedGenerationMissing;
            const slot_name = options.slot_name orelse return error.SeedSlotMissing;
            const location = options.location orelse return error.SeedLocationMissing;
            var opened = try antfly.serverless.object_store_support.OpenedObjectStore.initRemoteUri(alloc, location, "antfly-ha-seeds");
            defer opened.deinit();
            var result = try ha.seed_artifact.prune(alloc, .{
                .client = &opened.client,
                .bucket = opened.bucket,
                .prefix = opened.prefix,
            }, .{
                .slot_name = slot_name,
                .current_generation = generation,
                .retain_generations = options.retain_generations,
            });
            defer result.deinit(alloc);
            try writeArtifactResult(io, result.result_json);
        },
        .gc_source => {
            const generation = options.generation orelse return error.SeedGenerationMissing;
            const slot_name = options.slot_name orelse return error.SeedSlotMissing;
            const location = options.location orelse return error.SeedLocationMissing;
            const capture_root = options.capture_root orelse return error.SeedCaptureRootMissing;
            var opened = try antfly.serverless.object_store_support.OpenedObjectStore.initRemoteUri(alloc, location, "antfly-ha-seeds");
            defer opened.deinit();
            var result = try ha.seed_capture.prunePublishedGenerations(alloc, .{
                .store = .{
                    .client = &opened.client,
                    .bucket = opened.bucket,
                    .prefix = opened.prefix,
                },
                .capture_root = capture_root,
                .generation = generation,
                .slot_name = slot_name,
                .protected_generations = options.protected_generations,
                .retain_generations = options.retain_generations,
            });
            defer result.deinit(alloc);
            try writeArtifactResult(io, result.result_json);
        },
        .gc_target => {
            const target_root = options.target_root orelse return error.SeedActivationTargetMissing;
            const receipt_path = options.slot_activation_receipt_path orelse
                return error.SeedActivationCheckpointMissing;
            const request = seed_activation_contract.ActivatedGenerationGCRequest{
                .target_root = target_root,
                .slot_activation_receipt_path = receipt_path,
                .protected_generations = options.protected_generations,
                .retain_generations = options.retain_generations,
            };
            if (comptime control_only_storage_sources) {
                const request_json = try std.json.Stringify.valueAlloc(alloc, request, .{});
                defer alloc.free(request_json);
                var response = try kernel_owner_client.haSeedPruneActivatedGenerations(request_json);
                defer response.deinit();
                try writeArtifactResult(io, response.bytes());
            } else {
                var result = try ha.seed_activation.pruneActivatedGenerations(alloc, request);
                defer result.deinit(alloc);
                try writeArtifactResult(io, result.result_json);
            }
        },
        .delete_prefix => {
            const request = try options.cleanup.finish(options.location);
            // Validate every controller-bound field and digest before opening a
            // remote client, so malformed cleanup authority has no side effect.
            try ha.seed_prefix_cleanup.validateRequestAuthority(alloc, request);
            var opened = try antfly.serverless.object_store_support.OpenedObjectStore.initExistingS3RemoteUri(alloc, request.location);
            defer opened.deinit();
            var result = try ha.seed_prefix_cleanup.deleteAll(alloc, .{
                .client = &opened.client,
                .bucket = opened.bucket,
                .prefix = ha.seed_prefix_cleanup.exactObjectPrefix(request),
            }, request, .{});
            defer result.deinit(alloc);
            try writeArtifactResult(io, result.receipt_json);
        },
    }
}

fn parseArtifactArgs(alloc: std.mem.Allocator, argv: []const []const u8) !ArtifactOptions {
    if (argv.len == 0) return error.SeedArtifactActionMissing;
    var options = ArtifactOptions{ .action = if (std.mem.eql(u8, argv[0], "publish"))
        .publish
    else if (std.mem.eql(u8, argv[0], "restore"))
        .restore
    else if (std.mem.eql(u8, argv[0], "verify"))
        .verify
    else if (std.mem.eql(u8, argv[0], "activate"))
        .activate
    else if (std.mem.eql(u8, argv[0], "prune"))
        .prune
    else if (std.mem.eql(u8, argv[0], "gc-source"))
        .gc_source
    else if (std.mem.eql(u8, argv[0], "gc-target"))
        .gc_target
    else if (std.mem.eql(u8, argv[0], "delete-prefix"))
        .delete_prefix
    else
        return error.InvalidSeedArtifactAction };
    var protected_generations = std.ArrayListUnmanaged([]const u8).empty;
    errdefer protected_generations.deinit(alloc);
    var seen_flags = std.EnumSet(ArtifactFlag).initEmpty();
    var idx: usize = 1;
    while (idx < argv.len) {
        const raw_flag = argv[idx];
        idx += 1;
        const flag = artifactFlag(raw_flag) orelse return error.InvalidSeedArtifactFlag;
        if (!artifactFlagAllowed(options.action, flag)) return error.SeedArtifactFlagNotAllowedForAction;
        if (flag != .protect_generation) {
            if (seen_flags.contains(flag)) return error.DuplicateSeedArtifactFlag;
            seen_flags.insert(flag);
        }
        switch (flag) {
            .location => options.location = try artifactValue(argv, &idx),
            .generation => options.generation = try artifactValue(argv, &idx),
            .slot => options.slot_name = try artifactValue(argv, &idx),
            .manifest => options.manifest_path = try absoluteArtifactPath(try artifactValue(argv, &idx)),
            .content_root => options.content_root = try absoluteArtifactPath(try artifactValue(argv, &idx)),
            .capture_receipt => options.capture_receipt_path = try absoluteArtifactPath(try artifactValue(argv, &idx)),
            .capture_receipt_sha256 => options.capture_receipt_sha256 = try artifactValue(argv, &idx),
            .staging_root => options.staging_root = try absoluteArtifactPath(try artifactValue(argv, &idx)),
            .target_root => options.target_root = try absoluteArtifactPath(try artifactValue(argv, &idx)),
            .capture_root => options.capture_root = try absoluteArtifactPath(try artifactValue(argv, &idx)),
            .slot_activation_receipt => options.slot_activation_receipt_path = try absoluteArtifactPath(try artifactValue(argv, &idx)),
            .ha_cluster_id => options.identity.cluster_id = try parseU64(try artifactValue(argv, &idx)),
            .ha_shard_id => options.identity.shard_id = try parseU64(try artifactValue(argv, &idx)),
            .ha_table_id => options.identity.table_id = try parseU64(try artifactValue(argv, &idx)),
            .ha_timeline_id => options.identity.timeline_id = try parseU64(try artifactValue(argv, &idx)),
            .ha_epoch => options.identity.epoch = try parseU64(try artifactValue(argv, &idx)),
            .minimum_checkpoint_lsn => options.minimum_checkpoint_lsn = try parseU64(try artifactValue(argv, &idx)),
            .topology_id => {
                const raw = try artifactValue(argv, &idx);
                if (options.action == .delete_prefix)
                    options.cleanup.topology_id = raw
                else
                    options.binding.topology_id = raw;
            },
            .topology_generation => {
                const raw = try parseU64(try artifactValue(argv, &idx));
                if (options.action == .delete_prefix)
                    options.cleanup.topology_generation = raw
                else
                    options.binding.topology_generation = raw;
            },
            .node_id => options.binding.node_id = try artifactValue(argv, &idx),
            .target_pvc_name => options.binding.target_pvc_name = try artifactValue(argv, &idx),
            .target_pvc_uid => options.binding.target_pvc_uid = try artifactValue(argv, &idx),
            .target_local_node_id => options.target_local_node_id = try parseU64(try artifactValue(argv, &idx)),
            .target_replica_id => options.target_replica_id = try parseU64(try artifactValue(argv, &idx)),
            .retain_generations => {
                options.retain_generations = std.math.cast(usize, try parseU64(try artifactValue(argv, &idx))) orelse return error.InvalidSeedRetention;
                if (options.retain_generations == 0) return error.InvalidSeedRetention;
            },
            .protect_generation => {
                const generation = try artifactValue(argv, &idx);
                if (!ha_validation.isIdentifier(generation)) return error.InvalidProtectedSeedGeneration;
                if (protected_generations.items.len >= 256) return error.TooManyProtectedSeedGenerations;
                for (protected_generations.items) |previous| {
                    if (std.mem.eql(u8, previous, generation)) return error.DuplicateProtectedSeedGeneration;
                }
                try protected_generations.append(alloc, generation);
            },
            .operation_id => options.cleanup.operation_id = try artifactValue(argv, &idx),
            .retry_token => options.cleanup.retry_token = try artifactValue(argv, &idx),
            .instance_id => options.cleanup.instance_id = try artifactValue(argv, &idx),
            .prefix_sha256 => options.cleanup.prefix_sha256 = try artifactValue(argv, &idx),
            .credentials_secret_name => options.cleanup.credentials_secret_name = try artifactValue(argv, &idx),
            .delete_all => options.cleanup.delete_all = true,
            .request_sha256 => options.cleanup.request_sha256 = try artifactValue(argv, &idx),
        }
    }
    options.protected_generations = try protected_generations.toOwnedSlice(alloc);
    options.owns_protected_generations = true;
    return options;
}

fn artifactFlag(raw: []const u8) ?ArtifactFlag {
    const names = std.StaticStringMap(ArtifactFlag).initComptime(.{
        .{ "--location", .location },
        .{ "--generation", .generation },
        .{ "--slot", .slot },
        .{ "--manifest", .manifest },
        .{ "--content-root", .content_root },
        .{ "--capture-receipt", .capture_receipt },
        .{ "--capture-receipt-sha256", .capture_receipt_sha256 },
        .{ "--staging-root", .staging_root },
        .{ "--target-root", .target_root },
        .{ "--capture-root", .capture_root },
        .{ "--slot-activation-receipt", .slot_activation_receipt },
        .{ "--ha-cluster-id", .ha_cluster_id },
        .{ "--ha-shard-id", .ha_shard_id },
        .{ "--ha-table-id", .ha_table_id },
        .{ "--ha-timeline-id", .ha_timeline_id },
        .{ "--ha-epoch", .ha_epoch },
        .{ "--minimum-checkpoint-lsn", .minimum_checkpoint_lsn },
        .{ "--topology-id", .topology_id },
        .{ "--topology-generation", .topology_generation },
        .{ "--node-id", .node_id },
        .{ "--target-pvc-name", .target_pvc_name },
        .{ "--target-pvc-uid", .target_pvc_uid },
        .{ "--target-local-node-id", .target_local_node_id },
        .{ "--target-replica-id", .target_replica_id },
        .{ "--retain-generations", .retain_generations },
        .{ "--protect-generation", .protect_generation },
        .{ "--operation-id", .operation_id },
        .{ "--retry-token", .retry_token },
        .{ "--instance-id", .instance_id },
        .{ "--prefix-sha256", .prefix_sha256 },
        .{ "--credentials-secret-name", .credentials_secret_name },
        .{ "--delete-all", .delete_all },
        .{ "--request-sha256", .request_sha256 },
    });
    return names.get(raw);
}

fn artifactFlagAllowed(action: ArtifactAction, flag: ArtifactFlag) bool {
    return switch (flag) {
        .location => action == .publish or action == .restore or action == .prune or action == .gc_source or action == .delete_prefix,
        .generation, .slot => action != .gc_target and action != .delete_prefix,
        .manifest, .content_root => action == .publish,
        .capture_receipt => action == .publish,
        .capture_receipt_sha256 => action == .publish or action == .restore or action == .verify or action == .activate,
        .staging_root => action == .restore or action == .verify or action == .activate,
        .target_root => action == .activate or action == .gc_target,
        .capture_root => action == .gc_source,
        .slot_activation_receipt => action == .gc_target,
        .ha_cluster_id, .ha_shard_id, .ha_table_id, .ha_timeline_id, .ha_epoch, .minimum_checkpoint_lsn => action == .restore or action == .verify or action == .activate,
        .topology_id, .topology_generation => action == .publish or action == .restore or action == .verify or action == .activate or action == .delete_prefix,
        .node_id, .target_pvc_name, .target_pvc_uid => action == .publish or action == .restore or action == .verify or action == .activate,
        .target_local_node_id, .target_replica_id => action == .activate,
        .retain_generations => action == .prune or action == .gc_source or action == .gc_target,
        .protect_generation => action == .gc_source or action == .gc_target,
        .operation_id, .retry_token, .instance_id, .prefix_sha256, .credentials_secret_name, .delete_all, .request_sha256 => action == .delete_prefix,
    };
}

fn artifactValue(argv: []const []const u8, idx: *usize) ![]const u8 {
    if (idx.* >= argv.len) return error.FlagValueMissing;
    const out = argv[idx.*];
    idx.* += 1;
    return out;
}

fn absoluteArtifactPath(path: []const u8) ![]const u8 {
    if (!ha_validation.isAbsoluteNormalizedPath(path)) return error.InvalidSeedArtifactPath;
    return path;
}

fn requireCaptureReceiptDigest(raw_digest: ?[]const u8) ![]const u8 {
    const digest = raw_digest orelse return error.CaptureReceiptDigestMissing;
    if (digest.len != Sha256.digest_length * 2) return error.InvalidCaptureReceiptDigest;
    for (digest) |byte| {
        if ((byte < '0' or byte > '9') and (byte < 'a' or byte > 'f'))
            return error.InvalidCaptureReceiptDigest;
    }
    return digest;
}

fn readArtifactFileAlloc(alloc: std.mem.Allocator, path: []const u8, max_bytes: usize) ![]u8 {
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    return try std.Io.Dir.cwd().readFileAlloc(io_impl.io(), path, alloc, .limited(max_bytes));
}

fn writeArtifactResult(io: std.Io, body: []const u8) !void {
    emitOrFail(io, body) catch return error.SeedArtifactOutputFailed;
    emitOrFail(io, "\n") catch return error.SeedArtifactOutputFailed;
}

fn runRemoteArgv(
    alloc: std.mem.Allocator,
    io: std.Io,
    remote_url: []const u8,
    command_args: []const []const u8,
    executor: http_common.RequestExecutor,
) !void {
    return try runRemoteArgvWithOptions(alloc, io, remote_url, command_args, executor, .{});
}

fn runRemoteArgvWithOptions(
    alloc: std.mem.Allocator,
    io: std.Io,
    remote_url: []const u8,
    command_args: []const []const u8,
    executor: http_common.RequestExecutor,
    remote_options: RemoteOptions,
) !void {
    var plan = try ha.admin_cli.parse(alloc, command_args);
    defer plan.deinit(alloc);

    var client = ha.http_client.Client.initWithOptions(alloc, executor, .{
        .bearer_token = remote_options.bearer_token,
    });
    if (try executeTypedRemote(alloc, io, &client, remote_url, plan)) return;
    if (!remoteCommandEndpointAllowed(plan.command, plan.output)) return error.HaRemoteTypedAdminRequired;

    var rendered = try client.executeCommand(remote_url, command_args);
    defer rendered.deinit(alloc);
    writeRemoteBody(io, rendered.body);
}

fn executeTypedRemote(
    alloc: std.mem.Allocator,
    io: std.Io,
    client: *ha.http_client.Client,
    remote_url: []const u8,
    plan: ha.admin_cli.Plan,
) !bool {
    if (plan.output == .prometheus and !typedRemotePrometheusAllowed(plan.command)) return false;

    switch (plan.command) {
        .slot => |command| switch (command.action) {
            .create => {
                var out = try client.createReplicationSlot(
                    remote_url,
                    command.request.slot_name,
                    command.request.initial_lsn,
                );
                defer out.deinit(alloc);
                try writeTypedRemoteBody(alloc, io, plan.output, out.body);
                return true;
            },
            .pause => {
                var out = try client.pauseReplicationSlot(remote_url, command.request.slot_name);
                defer out.deinit(alloc);
                try writeTypedRemoteBody(alloc, io, plan.output, out.body);
                return true;
            },
            .@"resume" => {
                var out = try client.resumeReplicationSlot(remote_url, command.request.slot_name);
                defer out.deinit(alloc);
                try writeTypedRemoteBody(alloc, io, plan.output, out.body);
                return true;
            },
            .drop => {
                var out = try client.dropReplicationSlot(remote_url, command.request.slot_name);
                defer out.deinit(alloc);
                try writeTypedRemoteBody(alloc, io, plan.output, out.body);
                return true;
            },
        },
        .slot_list => |command| {
            if (isDefaultRetentionPolicy(command.retention_policy)) {
                var out = try client.listReplicationSlots(remote_url);
                defer out.deinit(alloc);
                try writeTypedRemoteBody(alloc, io, plan.output, out.body);
            } else {
                var out = try client.getPrimaryStatus(remote_url, primaryStatusOptionsFromRetention(command.retention_policy));
                defer out.deinit(alloc);
                try writeTypedRemoteBody(alloc, io, plan.output, out.body);
            }
            return true;
        },
        .seed => |command| switch (command) {
            .begin => |request| {
                var out = try client.beginBaseBackup(remote_url, .{
                    .slot_name = request.slot_name,
                    .manifest_id = request.manifest_id,
                });
                defer out.deinit(alloc);
                try writeTypedRemoteBody(alloc, io, plan.output, out.body);
                return true;
            },
            .finish => |request| {
                var out = try client.finishBaseBackup(remote_url, .{
                    .manifest_path = request.manifest_path,
                });
                defer out.deinit(alloc);
                try writeTypedRemoteBody(alloc, io, plan.output, out.body);
                return true;
            },
            .bootstrap => |request| {
                var out = try client.bootstrapStandby(remote_url, .{
                    .manifest_path = request.manifest_path,
                    .content_root = if (request.content_root) |root| .{ .value = root } else .absent,
                });
                defer out.deinit(alloc);
                try writeTypedRemoteBody(alloc, io, plan.output, out.body);
                return true;
            },
        },
        .primary_status => |command| {
            if (command.view != .status and plan.output != .prometheus) return false;
            var options = primaryStatusOptionsFromRetention(command.retention_policy);
            options.sync_policy = command.sync_policy;
            var out = try client.getPrimaryStatus(remote_url, options);
            defer out.deinit(alloc);
            try writePrimaryStatusRemote(alloc, io, plan.output, out.body, out.parsed.value.snapshot);
            return true;
        },
        .standby_status => |command| {
            if (command.view != .status and plan.output != .prometheus) return false;
            var out = try client.getStandbyStatus(remote_url, command.upstream_lsn);
            defer out.deinit(alloc);
            try writeStandbyStatusRemote(alloc, io, plan.output, out.body, out.parsed.value.snapshot);
            return true;
        },
        .commit_check => |command| {
            var out = try client.checkCommit(remote_url, .{
                .target_lsn = command.target_lsn,
                .sync_policy = try syncPolicyOpenApi(command.policy),
            });
            defer out.deinit(alloc);
            try writeTypedRemoteBody(alloc, io, plan.output, out.body);
            return true;
        },
        .commit_append => |command| {
            var out = try client.appendCommit(remote_url, .{
                .payload = command.append.payload,
                .kind = try recordKindName(command.append.kind),
                .payload_codec = try payloadCodecName(command.append.payload_codec),
                .shard_id = if (command.append.shard_id) |raw| raw else null,
                .table_id = if (command.append.table_id) |raw| raw else null,
                .commit_timestamp_ns = command.append.commit_timestamp_ns,
                .sync_policy = try syncPolicyOpenApi(command.policy),
            });
            defer out.deinit(alloc);
            try writeTypedRemoteBody(alloc, io, plan.output, out.body);
            return true;
        },
        .read_check => |request| {
            var out = try client.checkRead(remote_url, .{
                .consistency = @tagName(request.consistency),
                .required_lsn = if (request.required_lsn) |raw| .{ .value = raw } else .absent,
                .required_metadata_lsn = if (request.required_metadata_lsn) |raw| .{ .value = raw } else .absent,
                .metadata_applied_lsn = if (request.metadata_applied_lsn) |raw| .{ .value = raw } else .absent,
            });
            defer out.deinit(alloc);
            try writeTypedRemoteBody(alloc, io, plan.output, out.body);
            return true;
        },
        .write_check => |command| {
            var out = try client.checkWrite(remote_url, .{
                .role = @tagName(command.role),
                .expected_identity = if (command.request.expected_identity) |identity| try adminIdentity(identity) else null,
            });
            defer out.deinit(alloc);
            try writeTypedRemoteBody(alloc, io, plan.output, out.body);
            return true;
        },
        .owner_job_check => |command| {
            var out = try client.checkOwnerJob(remote_url, .{
                .role = @tagName(command.role),
                .kind = @tagName(command.request.kind),
                .expected_identity = if (command.request.expected_identity) |identity| try adminIdentity(identity) else null,
            });
            defer out.deinit(alloc);
            try writeTypedRemoteBody(alloc, io, plan.output, out.body);
            return true;
        },
        .fence_acquire => |request| {
            var out = try client.acquireFence(remote_url, try fenceRequestOpenApi(request));
            defer out.deinit(alloc);
            try writeTypedRemoteBody(alloc, io, plan.output, out.body);
            return true;
        },
        .fence_current => {
            var out = try client.currentFence(remote_url);
            defer out.deinit(alloc);
            try writeTypedRemoteBody(alloc, io, plan.output, out.body);
            return true;
        },
        .promote_assess => |command| {
            var out = try client.assessPromotion(remote_url, .{
                .required_lsn = if (command.check.required_lsn) |raw| raw else null,
                .fencing_confirmed = command.check.fencing_confirmed,
                .force = command.check.force,
                .use_current_fence = command.use_current_fence,
            });
            defer out.deinit(alloc);
            try writePromotionAssessRemote(alloc, io, plan.output, out.body, out.parsed.value.assessment);
            return true;
        },
        .promote_current_fence => {
            var out = try client.promoteWithCurrentFence(remote_url);
            defer out.deinit(alloc);
            try writeTypedRemoteBody(alloc, io, plan.output, out.body);
            return true;
        },
        .promote => |command| {
            var out = try client.promote(remote_url, try fenceRequestOpenApi(command.fence));
            defer out.deinit(alloc);
            try writeTypedRemoteBody(alloc, io, plan.output, out.body);
            return true;
        },
        .rejoin_assess => |command| {
            var out = try client.assessRejoin(remote_url, try rejoinRequestOpenApi(command));
            defer out.deinit(alloc);
            try writeTypedRemoteBody(alloc, io, plan.output, out.body);
            return true;
        },
        .rejoin_rewind => |command| {
            var out = try client.rewindRejoin(remote_url, try rejoinRequestOpenApi(command));
            defer out.deinit(alloc);
            try writeTypedRemoteBody(alloc, io, plan.output, out.body);
            return true;
        },
        .rejoin_reseed => |command| {
            var out = try client.reseedRejoin(remote_url, try rejoinRequestOpenApi(command));
            defer out.deinit(alloc);
            try writeTypedRemoteBody(alloc, io, plan.output, out.body);
            return true;
        },
        .standby_upstream => |command| {
            var out = try client.setStandbyUpstream(remote_url, try standbyUpstreamRequestOpenApi(command));
            defer out.deinit(alloc);
            try writeTypedRemoteBody(alloc, io, plan.output, out.body);
            return true;
        },
        .identify_system,
        .start_replication,
        .stream_once,
        .standby_status_update,
        .operator_plan,
        => return false,
    }
}

fn isDefaultRetentionPolicy(policy: ha.slot_store.RetentionPolicy) bool {
    return policy.max_lag_lsn == 0 and
        policy.max_retained_bytes == 0 and
        policy.max_retained_age_ns == 0;
}

fn primaryStatusOptionsFromRetention(policy: ha.slot_store.RetentionPolicy) ha.http_client.PrimaryStatusOptions {
    return .{
        .max_lag_lsn = if (policy.max_lag_lsn == 0) null else policy.max_lag_lsn,
        .max_retained_bytes = if (policy.max_retained_bytes == 0) null else policy.max_retained_bytes,
        .max_retained_age_ns = if (policy.max_retained_age_ns == 0) null else policy.max_retained_age_ns,
    };
}

fn typedRemotePrometheusAllowed(command: ha.admin_cli.Command) bool {
    return switch (command) {
        .primary_status,
        .standby_status,
        .promote_assess,
        => true,
        else => false,
    };
}

fn remoteCommandEndpointAllowed(command: ha.admin_cli.Command, output: ha.admin_cli.OutputFormat) bool {
    if (output == .prometheus) return false;
    return switch (command) {
        .identify_system,
        .start_replication,
        .stream_once,
        .standby_status_update,
        => true,
        else => false,
    };
}

/// Planned primary change. Composed entirely from typed admin routes; see
/// `zig/HOT_STANDBY.md` ("switchover") for the sequence and its safety
/// argument. The old primary is fenced before the boundary is read, so no
/// write can land after the LSN the standby is required to reach.
const SwitchoverOptions = struct {
    to: []const u8,
    new_upstream_url: ?[]const u8 = null,
    followers: []const Follower = &.{},
    generation: ?u64 = null,
    max_lag_lsn: ?u64 = null,
    wait_seconds: u64 = 60,
    retained_from_lsn: u64 = 0,
    reason: []const u8 = "planned switchover",
    dry_run: bool = false,
    output: ha.admin_cli.OutputFormat = .table,

    const Follower = struct {
        slot_name: []const u8,
        url: []const u8,
    };

    fn deinit(self: *SwitchoverOptions, alloc: std.mem.Allocator) void {
        alloc.free(self.followers);
        self.* = undefined;
    }

    fn newUpstreamUrl(self: SwitchoverOptions) []const u8 {
        return self.new_upstream_url orelse self.to;
    }
};

fn parseSwitchoverArgs(alloc: std.mem.Allocator, argv: []const []const u8) !SwitchoverOptions {
    var to: ?[]const u8 = null;
    var options = SwitchoverOptions{ .to = "" };
    var followers = std.ArrayListUnmanaged(SwitchoverOptions.Follower).empty;
    errdefer followers.deinit(alloc);

    var idx: usize = 0;
    while (idx < argv.len) {
        const arg = argv[idx];
        idx += 1;
        if (std.mem.eql(u8, arg, "--to")) {
            to = try validateHAAdminURL(try value(argv, &idx, arg));
        } else if (std.mem.eql(u8, arg, "--new-upstream-url")) {
            options.new_upstream_url = try validateHAAdminURL(try value(argv, &idx, arg));
        } else if (std.mem.eql(u8, arg, "--follower")) {
            const raw = try value(argv, &idx, arg);
            const split = std.mem.indexOfScalar(u8, raw, '=') orelse return error.SwitchoverFollowerInvalid;
            const slot_name = raw[0..split];
            if (!ha_validation.isIdentifier(slot_name)) return error.SwitchoverFollowerInvalid;
            try followers.append(alloc, .{
                .slot_name = slot_name,
                .url = try validateHAAdminURL(raw[split + 1 ..]),
            });
        } else if (std.mem.eql(u8, arg, "--generation")) {
            const generation = try parseU64(try value(argv, &idx, arg));
            if (generation == 0) return error.SwitchoverGenerationInvalid;
            options.generation = generation;
        } else if (std.mem.eql(u8, arg, "--max-lag-lsn")) {
            options.max_lag_lsn = try parseU64(try value(argv, &idx, arg));
        } else if (std.mem.eql(u8, arg, "--wait-seconds")) {
            options.wait_seconds = try parseU64(try value(argv, &idx, arg));
        } else if (std.mem.eql(u8, arg, "--retained-from-lsn")) {
            options.retained_from_lsn = try parseU64(try value(argv, &idx, arg));
        } else if (std.mem.eql(u8, arg, "--reason")) {
            options.reason = try value(argv, &idx, arg);
        } else if (std.mem.eql(u8, arg, "--dry-run")) {
            options.dry_run = true;
        } else if (std.mem.eql(u8, arg, "--json")) {
            options.output = .json;
        } else if (std.mem.eql(u8, arg, "--table")) {
            options.output = .table;
        } else {
            return error.UnknownSwitchoverOption;
        }
    }
    options.to = to orelse return error.SwitchoverTargetMissing;
    options.followers = try followers.toOwnedSlice(alloc);
    return options;
}

fn identityEql(a: admin_api.openapi.StandbyIdentity, b: admin_api.openapi.StandbyIdentity) bool {
    return a.cluster_id == b.cluster_id and a.shard_id == b.shard_id and a.table_id == b.table_id and
        a.timeline_id == b.timeline_id and a.epoch == b.epoch;
}

fn reportSwitchoverStep(alloc: std.mem.Allocator, io: std.Io, output: ha.admin_cli.OutputFormat, step: []const u8, body: ?[]const u8) !void {
    switch (output) {
        .json => {
            const line = try std.fmt.allocPrint(alloc, "{{\"step\":\"{s}\",\"response\":{s}}}\n", .{ step, body orelse "null" });
            defer alloc.free(line);
            emit(io, line);
        },
        else => {
            const line = try std.fmt.allocPrint(alloc, "switchover {s}: ok\n", .{step});
            defer alloc.free(line);
            emit(io, line);
        },
    }
}

fn reportSwitchoverFailure(alloc: std.mem.Allocator, io: std.Io, output: ha.admin_cli.OutputFormat, step: []const u8, err: anyerror) void {
    const line = switch (output) {
        .json => std.fmt.allocPrint(alloc, "{{\"step\":\"{s}\",\"error\":\"{s}\"}}\n", .{ step, @errorName(err) }) catch return,
        else => std.fmt.allocPrint(alloc, "switchover {s}: failed: {s}\n", .{ step, @errorName(err) }) catch return,
    };
    defer alloc.free(line);
    emit(io, line);
}

fn runSwitchover(
    alloc: std.mem.Allocator,
    io: std.Io,
    primary_url: []const u8,
    argv: []const []const u8,
    executor: http_common.RequestExecutor,
    remote_options: RemoteOptions,
) !void {
    var options = try parseSwitchoverArgs(alloc, argv);
    defer options.deinit(alloc);
    var client = ha.http_client.Client.initWithOptions(alloc, executor, .{
        .bearer_token = remote_options.bearer_token,
    });

    // 1. Preflight: read both nodes, write nothing.
    var primary_out = try client.getPrimaryStatus(primary_url, .{});
    defer primary_out.deinit(alloc);
    const primary = primary_out.parsed.value.snapshot;
    if (!std.mem.eql(u8, primary.role, "primary")) return error.SwitchoverSourceNotPrimary;
    const boundary = primary.current_lsn;
    if (boundary == 0) return error.SwitchoverPrimaryHasNoRecords;

    var standby_out = try client.getStandbyStatus(options.to, boundary);
    defer standby_out.deinit(alloc);
    const standby = standby_out.parsed.value.snapshot;
    if (!std.mem.eql(u8, standby.role, "standby")) return error.SwitchoverTargetNotStandby;
    if (!identityEql(primary.identity, standby.identity)) return error.SwitchoverIdentityMismatch;
    if (options.max_lag_lsn) |max_lag| {
        const applied = standby.applied_lsn;
        if (boundary > applied and boundary - applied > max_lag) return error.SwitchoverStandbyLagging;
    }
    try reportSwitchoverStep(alloc, io, options.output, "preflight-primary", primary_out.body);
    try reportSwitchoverStep(alloc, io, options.output, "preflight-standby", standby_out.body);
    if (options.dry_run) return;

    const old_identity = primary.identity;
    var fence_request = admin_api.openapi.FenceAcquireRequest{
        .identity = old_identity,
        .old_primary_id = primary.node_id,
        .promoted_node_id = standby.node_id,
        .new_timeline_id = try std.math.add(u64, old_identity.timeline_id, 1),
        .new_epoch = try std.math.add(u64, old_identity.epoch, 1),
        .generation = options.generation,
        .required_lsn = primary.current_lsn,
        .observed_lsn = primary.current_lsn,
        .force = false,
        .reason = options.reason,
    };

    // 2. Fence the old primary first; its write gate fails closed from here on.
    var primary_fence = try client.acquireFence(primary_url, fence_request);
    defer primary_fence.deinit(alloc);
    const generation = primary_fence.parsed.value.receipt.generation;
    try reportSwitchoverStep(alloc, io, options.output, "fence-primary", primary_fence.body);

    // 3. Read the final boundary and wait for the standby to reach it.
    var fenced_out = try client.getPrimaryStatus(primary_url, .{});
    defer fenced_out.deinit(alloc);
    const final_lsn_raw = fenced_out.parsed.value.snapshot.current_lsn;
    const final_lsn = final_lsn_raw;
    if (final_lsn < boundary) return error.SwitchoverBoundaryRegressed;
    try waitForStandbyBoundary(alloc, io, &client, options.to, final_lsn, options.wait_seconds);
    try reportSwitchoverStep(alloc, io, options.output, "boundary", fenced_out.body);

    // 4. Fence the standby with the same generation, assess, promote.
    fence_request.generation = generation;
    fence_request.required_lsn = final_lsn_raw;
    fence_request.observed_lsn = final_lsn_raw;
    var standby_fence = try client.acquireFence(options.to, fence_request);
    defer standby_fence.deinit(alloc);
    try reportSwitchoverStep(alloc, io, options.output, "fence-standby", standby_fence.body);

    var assess = try client.assessPromotion(options.to, .{
        .required_lsn = final_lsn_raw,
        .fencing_confirmed = true,
        .force = false,
        .use_current_fence = true,
    });
    defer assess.deinit(alloc);
    const assessment = assess.parsed.value.assessment;
    try reportSwitchoverStep(alloc, io, options.output, "assess", assess.body);
    if (!assessment.can_promote or !std.mem.eql(u8, assessment.mode, "safe")) return error.SwitchoverPromotionNotSafe;

    var promoted = try client.promoteWithCurrentFence(options.to);
    defer promoted.deinit(alloc);
    try reportSwitchoverStep(alloc, io, options.output, "promote", promoted.body);

    // 5. Rejoin the old primary; 6. repoint followers. Both run after the
    // role has moved, so a failure here is reported but does not undo the
    // promotion; the caller re-runs the individual verb.
    var incomplete = false;
    rejoinFormerPrimary(alloc, io, &client, primary_url, options, primary.node_id, old_identity, final_lsn_raw) catch |err| {
        incomplete = true;
        reportSwitchoverFailure(alloc, io, options.output, "rejoin", err);
    };
    for (options.followers) |follower| {
        followNewPrimary(alloc, io, &client, options, follower, primary.slots, old_identity, final_lsn) catch |err| {
            incomplete = true;
            reportSwitchoverFailure(alloc, io, options.output, follower.slot_name, err);
        };
    }
    if (incomplete) return error.SwitchoverIncomplete;
}

fn waitForStandbyBoundary(
    alloc: std.mem.Allocator,
    io: std.Io,
    client: *ha.http_client.Client,
    standby_url: []const u8,
    final_lsn: u64,
    wait_seconds: u64,
) !void {
    const poll_interval_ms: u64 = 250;
    var remaining = @max(wait_seconds, 1) * (1000 / poll_interval_ms);
    while (true) {
        var out = try client.getStandbyStatus(standby_url, final_lsn);
        defer out.deinit(alloc);
        const snapshot = out.parsed.value.snapshot;
        const received = snapshot.received_lsn;
        const applied = snapshot.applied_lsn;
        const safe_read = snapshot.safe_read_lsn;
        if (received >= final_lsn and applied >= final_lsn and safe_read >= final_lsn) return;
        if (remaining == 0) return error.SwitchoverStandbyCatchUpTimeout;
        remaining -= 1;
        try io.sleep(.fromMilliseconds(poll_interval_ms), .awake);
    }
}

fn rejoinFormerPrimary(
    alloc: std.mem.Allocator,
    io: std.Io,
    client: *ha.http_client.Client,
    primary_url: []const u8,
    options: SwitchoverOptions,
    former_node_id: []const u8,
    old_identity: admin_api.openapi.StandbyIdentity,
    final_lsn_raw: u64,
) !void {
    var fence = try client.currentFence(options.to);
    defer fence.deinit(alloc);
    if (!fence.parsed.value.held) return error.SwitchoverFenceMissing;
    const receipt = fence.parsed.value.receipt orelse return error.SwitchoverFenceMissing;

    const request = admin_api.openapi.RejoinAssessRequest{
        .node_id = former_node_id,
        .identity = old_identity,
        .last_lsn = final_lsn_raw,
        .retained_from_lsn = options.retained_from_lsn,
        .allow_rewind_after_forced_promotion = false,
        .receipt = receipt,
    };
    var assessed = try client.assessRejoin(primary_url, request);
    defer assessed.deinit(alloc);
    try reportSwitchoverStep(alloc, io, options.output, "rejoin-assess", assessed.body);
    const action = assessed.parsed.value.assessment.action;
    if (std.mem.eql(u8, action, "rewind")) {
        // The old primary still owns its replication log while it runs in the
        // primary role, and a log cannot be opened twice in one process, so
        // `rejoin rewind` executes after the node restarts as a standby with
        // `former_primary_log` configured. Report it as pending rather than
        // attempting it here and failing on the lock.
        try reportSwitchoverStep(alloc, io, options.output, "rejoin-rewind-pending", assessed.body);
    } else if (std.mem.eql(u8, action, "reseed")) {
        var reseeded = try client.reseedRejoin(options.to, request);
        defer reseeded.deinit(alloc);
        try reportSwitchoverStep(alloc, io, options.output, "rejoin-reseed", reseeded.body);
    } else if (!std.mem.eql(u8, action, "already_current")) {
        return error.SwitchoverRejoinRejected;
    }
}

fn followNewPrimary(
    alloc: std.mem.Allocator,
    io: std.Io,
    client: *ha.http_client.Client,
    options: SwitchoverOptions,
    follower: SwitchoverOptions.Follower,
    old_slots: []const admin_api.openapi.StandbySlotSnapshot,
    old_identity: admin_api.openapi.StandbyIdentity,
    final_lsn: u64,
) !void {
    var initial_lsn = final_lsn;
    for (old_slots) |slot| {
        if (std.mem.eql(u8, slot.name, follower.slot_name)) {
            initial_lsn = slot.received_lsn;
            break;
        }
    }
    // The new primary needs a slot for the follower; an existing slot is fine.
    if (client.createReplicationSlot(options.to, follower.slot_name, initial_lsn)) |*created| {
        var owned = created.*;
        defer owned.deinit(alloc);
        try reportSwitchoverStep(alloc, io, options.output, follower.slot_name, owned.body);
    } else |err| switch (err) {
        error.HaCommandConflict => {},
        else => return err,
    }
    var repointed = try client.setStandbyUpstream(follower.url, .{
        .identity = old_identity,
        .upstream_url = options.newUpstreamUrl(),
        .slot_name = follower.slot_name,
        .reason = options.reason,
    });
    defer repointed.deinit(alloc);
    try reportSwitchoverStep(alloc, io, options.output, follower.slot_name, repointed.body);
}

fn writeRemoteBody(io: std.Io, body: []const u8) void {
    emit(io, body);
    emit(io, "\n");
}

fn writeTypedRemoteBody(
    alloc: std.mem.Allocator,
    io: std.Io,
    output: ha.admin_cli.OutputFormat,
    body: []const u8,
) !void {
    switch (output) {
        .json => writeRemoteBody(io, body),
        .table => {
            const table = try renderJsonTableAlloc(alloc, body);
            defer alloc.free(table);
            writeRemoteBody(io, table);
        },
        .prometheus => unreachable,
    }
}

fn writePrimaryStatusRemote(
    alloc: std.mem.Allocator,
    io: std.Io,
    output: ha.admin_cli.OutputFormat,
    body: []const u8,
    snapshot: admin_api.HAPrimarySnapshot,
) !void {
    switch (output) {
        .json, .table => try writeTypedRemoteBody(alloc, io, output, body),
        .prometheus => {
            var metrics = try primaryMetricsFromAdminSnapshot(alloc, snapshot);
            defer metrics.deinit(alloc);
            const rendered = try ha.metrics.renderPrimaryPrometheusAlloc(alloc, metrics);
            defer alloc.free(rendered);
            writeRemoteBody(io, rendered);
        },
    }
}

fn writeStandbyStatusRemote(
    alloc: std.mem.Allocator,
    io: std.Io,
    output: ha.admin_cli.OutputFormat,
    body: []const u8,
    snapshot: admin_api.HAStandbySnapshot,
) !void {
    switch (output) {
        .json, .table => try writeTypedRemoteBody(alloc, io, output, body),
        .prometheus => {
            const metrics = try standbyMetricsFromAdminSnapshot(snapshot);
            const rendered = try ha.metrics.renderStandbyPrometheusAlloc(alloc, metrics);
            defer alloc.free(rendered);
            writeRemoteBody(io, rendered);
        },
    }
}

fn writePromotionAssessRemote(
    alloc: std.mem.Allocator,
    io: std.Io,
    output: ha.admin_cli.OutputFormat,
    body: []const u8,
    assessment: admin_api.HAPromotionAssessment,
) !void {
    switch (output) {
        .json, .table => try writeTypedRemoteBody(alloc, io, output, body),
        .prometheus => {
            const metrics = try promotionMetricsFromAdminAssessment(assessment);
            const rendered = try ha.metrics.renderPromotionPrometheusAlloc(alloc, metrics);
            defer alloc.free(rendered);
            writeRemoteBody(io, rendered);
        },
    }
}

fn primaryMetricsFromAdminSnapshot(alloc: std.mem.Allocator, snapshot: admin_api.HAPrimarySnapshot) !ha.metrics.PrimaryMetrics {
    const slots = try alloc.alloc(ha.metrics.SlotMetrics, snapshot.slots.len);
    errdefer alloc.free(slots);

    var filled: usize = 0;
    errdefer for (slots[0..filled]) |slot| alloc.free(slot.name);

    var active_slots: u64 = 0;
    var reseed_required_slots: u64 = 0;
    var max_write_lag_lsn: u64 = 0;
    var max_apply_lag_lsn: u64 = 0;
    var max_safe_read_lag_lsn: u64 = 0;
    var max_retention_lag_lsn: u64 = 0;

    for (snapshot.slots, 0..) |slot, idx| {
        const received_lsn = slot.received_lsn;
        const applied_lsn = slot.applied_lsn;
        const safe_read_lsn = slot.safe_read_lsn;
        const restart_lsn = slot.restart_lsn;
        const write_lag_lsn = slot.write_lag_lsn;
        const apply_lag_lsn = slot.apply_lag_lsn;
        const safe_read_lag_lsn = slot.safe_read_lag_lsn;
        const retention_lag_lsn = slot.retention_lag_lsn;
        const status_code = @intFromEnum(try slotStatusCodeFromAdmin(slot.status));

        if (slot.active) active_slots += 1;
        if (slot.reseed_required) reseed_required_slots += 1;
        max_write_lag_lsn = @max(max_write_lag_lsn, write_lag_lsn);
        max_apply_lag_lsn = @max(max_apply_lag_lsn, apply_lag_lsn);
        max_safe_read_lag_lsn = @max(max_safe_read_lag_lsn, safe_read_lag_lsn);
        max_retention_lag_lsn = @max(max_retention_lag_lsn, retention_lag_lsn);

        slots[idx] = .{
            .name = try alloc.dupe(u8, slot.name),
            .active = boolGauge(slot.active),
            .reseed_required = boolGauge(slot.reseed_required),
            .received_lsn = received_lsn,
            .applied_lsn = applied_lsn,
            .safe_read_lsn = safe_read_lsn,
            .restart_lsn = restart_lsn,
            .write_lag_lsn = write_lag_lsn,
            .apply_lag_lsn = apply_lag_lsn,
            .safe_read_lag_lsn = safe_read_lag_lsn,
            .retention_lag_lsn = retention_lag_lsn,
            .status_code = status_code,
            .last_error = boolGauge(slot.last_error.valueOrNull() != null),
        };
        filled += 1;
    }

    const durability = snapshot.durability;
    const durability_status_code = if (durability) |decision|
        @intFromEnum(try durabilityStatusCodeFromAdmin(decision.status))
    else
        @intFromEnum(ha.metrics.DurabilityStatusCode.not_configured);
    const durability_satisfied = if (durability) |decision|
        boolGauge(std.mem.eql(u8, decision.status, "satisfied"))
    else
        0;
    const durability_degraded = if (durability) |decision|
        boolGauge(!std.mem.eql(u8, decision.status, "satisfied"))
    else
        0;

    return .{
        .current_lsn = snapshot.current_lsn,
        .slot_count = @intCast(snapshot.slots.len),
        .active_slots = active_slots,
        .reseed_required_slots = reseed_required_slots,
        .max_write_lag_lsn = max_write_lag_lsn,
        .max_apply_lag_lsn = max_apply_lag_lsn,
        .max_safe_read_lag_lsn = max_safe_read_lag_lsn,
        .max_retention_lag_lsn = max_retention_lag_lsn,
        .retention_oldest_restart_lsn = snapshot.retention.oldest_restart_lsn,
        .retention_retained_lsn_count = snapshot.retention.retained_lsn_count,
        .retention_retained_byte_count = snapshot.retention.retained_byte_count,
        .retention_retained_age_ns = snapshot.retention.retained_age_ns,
        .retention_active_slots = snapshot.retention.active_slots,
        .retention_reseed_recommended = snapshot.retention.reseed_recommended,
        .durability_configured = boolGauge(durability != null),
        .durability_satisfied = durability_satisfied,
        .durability_degraded = durability_degraded,
        .durability_status_code = durability_status_code,
        .durability_target_lsn = if (durability) |decision| decision.target_lsn else 0,
        .durability_progress_lsn = if (durability) |decision| decision.progress_lsn else 0,
        .durability_missing_lsn_count = if (durability) |decision| decision.missing_lsn_count else 0,
        .durability_required_count = if (durability) |decision| decision.required_count else 0,
        .durability_satisfied_count = if (durability) |decision| decision.satisfied_count else 0,
        .durability_candidate_count = if (durability) |decision| decision.candidate_count else 0,
        .slots = slots,
    };
}

fn standbyMetricsFromAdminSnapshot(snapshot: admin_api.HAStandbySnapshot) !ha.metrics.StandbyMetrics {
    return .{
        .received_lsn = snapshot.received_lsn,
        .applied_lsn = snapshot.applied_lsn,
        .safe_read_lsn = snapshot.safe_read_lsn,
        .upstream_configured = boolGauge(snapshot.upstream_lsn.valueOrNull() != null),
        .write_lag_lsn = if (snapshot.write_lag_lsn.valueOrNull()) |raw| raw else 0,
        .receive_lag_lsn = if (snapshot.receive_lag_lsn.valueOrNull()) |raw| raw else 0,
        .apply_lag_lsn = if (snapshot.apply_lag_lsn.valueOrNull()) |raw| raw else 0,
        .unapplied_lsn_count = snapshot.unapplied_lsn_count,
        .caught_up_to_received = boolGauge(snapshot.caught_up_to_received),
        .can_serve_safe_reads = boolGauge(snapshot.can_serve_safe_reads),
    };
}

fn promotionMetricsFromAdminAssessment(assessment: admin_api.HAPromotionAssessment) !ha.metrics.PromotionMetrics {
    return .{
        .required_lsn = assessment.required_lsn,
        .received_lsn = assessment.received_lsn,
        .applied_lsn = assessment.applied_lsn,
        .has_required_lsn = boolGauge(assessment.has_required_lsn),
        .caught_up_to_received = boolGauge(assessment.caught_up_to_received),
        .fencing_confirmed = boolGauge(assessment.fencing_confirmed),
        .force = boolGauge(assessment.force),
        .data_loss_possible = boolGauge(assessment.data_loss_possible),
        .safe = boolGauge(assessment.safe),
        .requires_fencing = boolGauge(assessment.requires_fencing),
        .requires_force = boolGauge(assessment.requires_force),
        .can_promote = boolGauge(assessment.can_promote),
    };
}

fn slotStatusCodeFromAdmin(raw: []const u8) !ha.metrics.SlotStatusCode {
    if (std.mem.eql(u8, raw, "healthy")) return .healthy;
    if (std.mem.eql(u8, raw, "lagging")) return .lagging;
    if (std.mem.eql(u8, raw, "reseed_required")) return .reseed_required;
    return error.InvalidHaCommand;
}

fn durabilityStatusCodeFromAdmin(raw: []const u8) !ha.metrics.DurabilityStatusCode {
    if (std.mem.eql(u8, raw, "satisfied")) return .satisfied;
    if (std.mem.eql(u8, raw, "would_block")) return .would_block;
    if (std.mem.eql(u8, raw, "fail_closed")) return .fail_closed;
    if (std.mem.eql(u8, raw, "degraded_to_async")) return .degraded_to_async;
    return error.InvalidHaCommand;
}

fn boolGauge(enabled: bool) u64 {
    return if (enabled) 1 else 0;
}

fn renderJsonTableAlloc(alloc: std.mem.Allocator, body: []const u8) ![]u8 {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, body, .{});
    defer parsed.deinit();

    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try appendJsonTableValue(alloc, &out, "", parsed.value);
    return try out.toOwnedSlice(alloc);
}

fn appendJsonTableValue(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    path: []const u8,
    json_value: std.json.Value,
) !void {
    switch (json_value) {
        .object => |object| {
            var iter = object.iterator();
            while (iter.next()) |entry| {
                const next_path = if (path.len == 0)
                    try alloc.dupe(u8, entry.key_ptr.*)
                else
                    try std.fmt.allocPrint(alloc, "{s}.{s}", .{ path, entry.key_ptr.* });
                defer alloc.free(next_path);
                try appendJsonTableValue(alloc, out, next_path, entry.value_ptr.*);
            }
        },
        .array => |array| {
            for (array.items, 0..) |item, idx| {
                const next_path = try std.fmt.allocPrint(alloc, "{s}[{d}]", .{ path, idx });
                defer alloc.free(next_path);
                try appendJsonTableValue(alloc, out, next_path, item);
            }
            if (array.items.len == 0) try appendJsonTableLine(alloc, out, path, "[]");
        },
        .string => |text| try appendJsonTableLine(alloc, out, path, text),
        .number_string => |text| try appendJsonTableLine(alloc, out, path, text),
        .integer => |number| try appendJsonTableLineFmt(alloc, out, path, "{d}", .{number}),
        .float => |number| try appendJsonTableLineFmt(alloc, out, path, "{d}", .{number}),
        .bool => |flag| try appendJsonTableLine(alloc, out, path, if (flag) "true" else "false"),
        .null => try appendJsonTableLine(alloc, out, path, "null"),
    }
}

fn appendJsonTableLine(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    key: []const u8,
    value_text: []const u8,
) !void {
    try out.appendSlice(alloc, key);
    try out.append(alloc, '=');
    try out.appendSlice(alloc, value_text);
    try out.append(alloc, '\n');
}

fn appendJsonTableLineFmt(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    key: []const u8,
    comptime fmt: []const u8,
    args: anytype,
) !void {
    try out.appendSlice(alloc, key);
    try out.append(alloc, '=');
    const rendered = try std.fmt.allocPrint(alloc, fmt, args);
    defer alloc.free(rendered);
    try out.appendSlice(alloc, rendered);
    try out.append(alloc, '\n');
}

fn syncPolicyOpenApi(policy: ha.primary.SyncPolicy) !admin_api.openapi.StandbySyncPolicy {
    return .{
        .mode = @tagName(policy.mode),
        .selection = @tagName(policy.selection),
        .required = policy.required,
        .standby_names = policy.standby_names,
        .failure_policy = @tagName(policy.failure_policy),
    };
}

fn adminIdentity(identity: ha.standby.Identity) !admin_api.openapi.StandbyIdentity {
    return .{
        .cluster_id = identity.cluster_id,
        .shard_id = identity.shard_id,
        .table_id = identity.table_id,
        .timeline_id = identity.timeline_id,
        .epoch = identity.epoch,
    };
}

fn fenceRequestOpenApi(request: ha.fencing.FenceRequest) !admin_api.openapi.FenceAcquireRequest {
    return .{
        .identity = try adminIdentity(request.identity),
        .old_primary_id = request.old_primary_id,
        .promoted_node_id = request.promoted_node_id,
        .new_timeline_id = request.new_timeline_id,
        .new_epoch = request.new_epoch,
        .generation = if (request.generation == 0) null else request.generation,
        .required_lsn = request.required_lsn,
        .observed_lsn = request.observed_lsn,
        .force = request.force,
        .reason = request.reason,
    };
}

fn fenceReceiptOpenApi(receipt: ha.fencing.Receipt) !admin_api.openapi.StandbyFenceReceipt {
    return .{
        .identity = try adminIdentity(receipt.identity),
        .old_primary_id = receipt.old_primary_id,
        .promoted_node_id = receipt.promoted_node_id,
        .parent_timeline_id = receipt.parent_timeline_id,
        .parent_epoch = receipt.parent_epoch,
        .new_timeline_id = receipt.new_timeline_id,
        .new_epoch = receipt.new_epoch,
        .required_lsn = receipt.required_lsn,
        .observed_lsn = receipt.observed_lsn,
        .generation = receipt.generation,
        .forced = receipt.forced,
        .token = receipt.token,
        .reason = receipt.reason,
    };
}

fn rejoinRequestOpenApi(command: ha.admin_cli.RejoinAssessCommand) !admin_api.openapi.RejoinAssessRequest {
    return .{
        .node_id = command.former.node_id,
        .identity = try adminIdentity(command.former.identity),
        .last_lsn = command.former.last_lsn,
        .retained_from_lsn = command.policy.retained_from_lsn,
        .allow_rewind_after_forced_promotion = command.policy.allow_rewind_after_forced_promotion,
        .receipt = if (command.receipt) |receipt| try fenceReceiptOpenApi(receipt) else null,
    };
}

fn standbyUpstreamRequestOpenApi(command: ha.admin_cli.StandbyUpstreamCommand) !admin_api.openapi.StandbyUpstreamRequest {
    return .{
        .identity = try adminIdentity(command.identity),
        .upstream_url = command.upstream_url,
        .slot_name = command.slot_name,
        .reason = if (command.reason.len == 0) null else command.reason,
    };
}

fn recordKindName(kind: ha.replication_record.RecordKind) ![]const u8 {
    return switch (kind) {
        .batch_mutation => "batch_mutation",
        .metadata_mutation => "metadata_mutation",
        .derived_effect => "derived_effect",
        .backup_start => "backup_start",
        .backup_end => "backup_end",
        .checkpoint => "checkpoint",
        .manifest => "manifest",
        .truncate => "truncate",
        .timeline_switch => "timeline_switch",
        _ => error.InvalidHaCommand,
    };
}

fn payloadCodecName(codec: ha.replication_record.PayloadCodec) ![]const u8 {
    return switch (codec) {
        .raw => "raw",
        .json => "json",
        .binary => "binary",
        _ => error.InvalidHaCommand,
    };
}

fn zPath(alloc: std.mem.Allocator, path: []const u8) ![:0]u8 {
    return try alloc.dupeZ(u8, path);
}

fn flagMatches(arg: []const u8, spellings: []const []const u8) bool {
    for (spellings) |spelling| {
        if (std.mem.eql(u8, arg, spelling)) return true;
    }
    return false;
}

/// Local options come first; the first token that is not a recognized local
/// option starts the admin command. A literal `--` is accepted and skipped for
/// compatibility with older scripts but is not required. The `--ha-*` spellings
/// are compatibility aliases for the unprefixed flags.
fn parseLocalArgs(alloc: std.mem.Allocator, argv: []const []const u8) !ParsedArgs {
    var options = LocalOptions{};
    var command_start: usize = 0;

    while (command_start < argv.len) {
        const arg = argv[command_start];
        if (std.mem.eql(u8, arg, "--")) {
            command_start += 1;
            break;
        } else if (flagMatches(arg, &.{ "--admin-url", "--ha-url" })) {
            command_start += 1;
            options.remote_url = try validateHAAdminURL(try value(argv, &command_start, arg));
        } else if (flagMatches(arg, &.{ "--admin-token-env", "--ha-token-env" })) {
            command_start += 1;
            options.remote_token_env = try validateHAAdminTokenEnvName(try value(argv, &command_start, arg));
        } else if (flagMatches(arg, &.{ "--admin-token", "--admin-token-file", "--ha-token", "--ha-token-file", "--token" })) {
            return error.HAAdminRawTokenFlagUnsupported;
        } else if (std.mem.eql(u8, arg, "--data-dir")) {
            command_start += 1;
            options.data_dir = try validateHAPath(try value(argv, &command_start, arg), .data_dir);
        } else if (std.mem.eql(u8, arg, "--config")) {
            command_start += 1;
            options.config_path = try validateConfigPath(try value(argv, &command_start, arg));
        } else if (std.mem.eql(u8, arg, "--primary-log")) {
            command_start += 1;
            options.primary_log = try validateHAPath(try value(argv, &command_start, arg), .primary_log);
        } else if (std.mem.eql(u8, arg, "--primary-slots")) {
            command_start += 1;
            options.primary_slots = try validateHAPath(try value(argv, &command_start, arg), .primary_slots);
        } else if (std.mem.eql(u8, arg, "--primary-node-id")) {
            command_start += 1;
            options.primary_node_id = try validateHANodeID(try value(argv, &command_start, arg), .primary);
        } else if (std.mem.eql(u8, arg, "--standby-log")) {
            command_start += 1;
            options.standby_log = try validateHAPath(try value(argv, &command_start, arg), .standby_log);
        } else if (std.mem.eql(u8, arg, "--standby-progress")) {
            command_start += 1;
            options.standby_progress = try validateHAPath(try value(argv, &command_start, arg), .standby_progress);
        } else if (std.mem.eql(u8, arg, "--standby-node-id")) {
            command_start += 1;
            options.standby_node_id = try validateHANodeID(try value(argv, &command_start, arg), .standby);
        } else if (std.mem.eql(u8, arg, "--fence-wal")) {
            command_start += 1;
            options.fence_wal = try validateHAPath(try value(argv, &command_start, arg), .fence_wal);
        } else if (std.mem.eql(u8, arg, "--former-primary-log")) {
            command_start += 1;
            options.former_primary_log = try validateHAPath(try value(argv, &command_start, arg), .former_primary_log);
        } else if (flagMatches(arg, &.{ "--cluster-id", "--ha-cluster-id" })) {
            command_start += 1;
            options.identity.cluster_id = try parseU64(try value(argv, &command_start, arg));
        } else if (flagMatches(arg, &.{ "--shard-id", "--ha-shard-id" })) {
            command_start += 1;
            options.identity.shard_id = try parseU64(try value(argv, &command_start, arg));
        } else if (flagMatches(arg, &.{ "--table-id", "--ha-table-id" })) {
            command_start += 1;
            options.identity.table_id = try parseU64(try value(argv, &command_start, arg));
        } else if (flagMatches(arg, &.{ "--timeline-id", "--ha-timeline-id" })) {
            command_start += 1;
            options.identity.timeline_id = try parseU64(try value(argv, &command_start, arg));
        } else if (flagMatches(arg, &.{ "--epoch", "--ha-epoch" })) {
            command_start += 1;
            options.identity.epoch = try parseU64(try value(argv, &command_start, arg));
        } else {
            break;
        }
    }

    const command_args = try alloc.dupe([]const u8, argv[command_start..]);
    return .{
        .options = options,
        .command_args = command_args,
    };
}

/// Canonical layout of hot-standby state under a standalone data directory.
/// `--data-dir` derivation (see `applyDataDir`) prefers this tree per role
/// and falls back to `legacy_data_dir_layout` when the canonical files for
/// that role are absent. The Kubernetes operator still provisions the
/// legacy `ha/` tree this release; `antfly standalone` accepts explicit
/// paths through its `--ha-*` flags regardless of which tree they live in.
pub const data_dir_layout = struct {
    pub const primary_log = "standby/primary.wal";
    pub const primary_slots = "standby/slots";
    pub const standby_log = "standby/log.wal";
    pub const standby_progress = "standby/progress.wal";
    pub const fence_wal = "standby/fence.wal";
};

/// Pre-0.3 data directory layout. `--data-dir` still recognises it, per
/// role, when the canonical files under `data_dir_layout` are absent.
pub const legacy_data_dir_layout = struct {
    pub const primary_log = "ha/primary.wal";
    pub const primary_slots = "ha/slots";
    pub const standby_log = "ha/standby.wal";
    pub const standby_progress = "ha/standby-progress.wal";
    pub const fence_wal = "ha/fence.wal";
};

fn pathExists(io: std.Io, path: []const u8) bool {
    std.Io.Dir.cwd().access(io, path, .{}) catch return false;
    return true;
}

fn joinDataDirPath(alloc: std.mem.Allocator, data_dir: []const u8, relative: []const u8) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{s}/{s}", .{ data_dir, relative });
}

/// Resolves `--data-dir` into local handles and identity. Per role (primary,
/// standby, fence), state that already exists under the canonical
/// `<data-dir>/standby/` tree is preferred; when the canonical files for a
/// role are absent, the legacy `<data-dir>/ha/` tree is used if its files
/// exist for that role. If both trees exist for a role, the canonical tree
/// wins and no error is raised. This never creates files. Explicit path and
/// identity flags take precedence over derived values. Identity is read from
/// the standby progress WAL when a standby is present, otherwise from the
/// newest primary replication record.
fn applyDataDir(alloc: std.mem.Allocator, io: std.Io, options: *LocalOptions) !void {
    const data_dir = options.data_dir orelse return;
    if (options.remote_url != null or options.remote_token_env != null) return error.HaRemoteCannotUseLocalHandles;

    var found_any = options.hasLocalHandles();

    if (options.primary_log == null and options.primary_slots == null) {
        const primary_log = try joinDataDirPath(alloc, data_dir, data_dir_layout.primary_log);
        const primary_slots = try joinDataDirPath(alloc, data_dir, data_dir_layout.primary_slots);
        if (pathExists(io, primary_log) and pathExists(io, primary_slots)) {
            options.primary_log = primary_log;
            options.primary_slots = primary_slots;
            found_any = true;
        } else {
            const legacy_primary_log = try joinDataDirPath(alloc, data_dir, legacy_data_dir_layout.primary_log);
            const legacy_primary_slots = try joinDataDirPath(alloc, data_dir, legacy_data_dir_layout.primary_slots);
            if (pathExists(io, legacy_primary_log) and pathExists(io, legacy_primary_slots)) {
                options.primary_log = legacy_primary_log;
                options.primary_slots = legacy_primary_slots;
                found_any = true;
            }
        }
    }
    if (options.standby_log == null and options.standby_progress == null) {
        const standby_log = try joinDataDirPath(alloc, data_dir, data_dir_layout.standby_log);
        const standby_progress = try joinDataDirPath(alloc, data_dir, data_dir_layout.standby_progress);
        if (pathExists(io, standby_log) and pathExists(io, standby_progress)) {
            options.standby_log = standby_log;
            options.standby_progress = standby_progress;
            found_any = true;
        } else {
            const legacy_standby_log = try joinDataDirPath(alloc, data_dir, legacy_data_dir_layout.standby_log);
            const legacy_standby_progress = try joinDataDirPath(alloc, data_dir, legacy_data_dir_layout.standby_progress);
            if (pathExists(io, legacy_standby_log) and pathExists(io, legacy_standby_progress)) {
                options.standby_log = legacy_standby_log;
                options.standby_progress = legacy_standby_progress;
                found_any = true;
            }
        }
    }
    if (options.fence_wal == null) {
        const fence_wal = try joinDataDirPath(alloc, data_dir, data_dir_layout.fence_wal);
        if (pathExists(io, fence_wal)) {
            options.fence_wal = fence_wal;
            found_any = true;
        } else {
            const legacy_fence_wal = try joinDataDirPath(alloc, data_dir, legacy_data_dir_layout.fence_wal);
            if (pathExists(io, legacy_fence_wal)) {
                options.fence_wal = legacy_fence_wal;
                found_any = true;
            }
        }
    }
    if (!found_any) return error.HADataDirNoHAState;

    const needs_identity = options.identity.cluster_id == null or
        options.identity.shard_id == null or options.identity.table_id == null or
        options.identity.timeline_id == null or options.identity.epoch == null;
    if (!needs_identity) return;
    if (!options.wantsPrimary() and !options.wantsStandby()) return;

    var derived: ?ha.standby.Identity = null;
    if (options.wantsStandby()) {
        const progress_path = try zPath(alloc, options.standby_progress orelse return error.StandbyProgressMissing);
        derived = try ha.standby.readPersistedIdentity(alloc, progress_path.ptr, .{});
    }
    if (derived == null and options.wantsPrimary()) {
        const log_path = try zPath(alloc, options.primary_log orelse return error.PrimaryLogMissing);
        derived = try ha.primary.readPersistedIdentity(alloc, log_path.ptr, .{});
    }
    const identity = derived orelse return error.HADataDirIdentityUnavailable;
    if (options.identity.cluster_id == null) options.identity.cluster_id = identity.cluster_id;
    if (options.identity.shard_id == null) options.identity.shard_id = identity.shard_id;
    if (options.identity.table_id == null) options.identity.table_id = identity.table_id;
    if (options.identity.timeline_id == null) options.identity.timeline_id = identity.timeline_id;
    if (options.identity.epoch == null) options.identity.epoch = identity.epoch;
}

fn validateConfigPath(path: []const u8) ![]const u8 {
    return switch (ha_validation.classifyHAString(path)) {
        .ok => path,
        .missing => error.HAConfigPathMissing,
        .padded => error.HAConfigPathInvalid,
    };
}

/// Applies the `ha` section of the server config file named by `--config`.
/// When no target was chosen yet, `ha.admin.url` makes the command remote
/// (the node is running and owns its files); otherwise the section's paths
/// and identity become the local handles. `ha.admin.token_env` supplies the
/// token variable for any remote target that lacks one. The loaded config is
/// returned so the borrowed strings stay valid for the rest of the command.
fn applyConfigDefaults(alloc: std.mem.Allocator, io: std.Io, options: *LocalOptions) !?antfly.common.config.Config {
    const path = options.config_path orelse return null;
    var cfg = try antfly.common.config.loadFromPathWithSecretsForDeploymentWithIo(alloc, io, path, null, .standalone);
    errdefer cfg.deinit();
    const ha_cfg = cfg.ha orelse return cfg;

    const no_target = options.remote_url == null and options.data_dir == null and !options.hasLocalHandles();
    if (no_target) {
        if (ha_cfg.admin_url) |url| {
            options.remote_url = try validateHAAdminURL(url);
        } else {
            if (ha_cfg.primary_log) |v| options.primary_log = try validateHAPath(v, .primary_log);
            if (ha_cfg.primary_slots) |v| options.primary_slots = try validateHAPath(v, .primary_slots);
            if (ha_cfg.primary_node_id) |v| options.primary_node_id = try validateHANodeID(v, .primary);
            if (ha_cfg.standby_log) |v| options.standby_log = try validateHAPath(v, .standby_log);
            if (ha_cfg.standby_progress) |v| options.standby_progress = try validateHAPath(v, .standby_progress);
            if (ha_cfg.standby_node_id) |v| options.standby_node_id = try validateHANodeID(v, .standby);
            if (ha_cfg.fence_wal) |v| options.fence_wal = try validateHAPath(v, .fence_wal);
            if (ha_cfg.former_primary_log) |v| options.former_primary_log = try validateHAPath(v, .former_primary_log);
        }
    }
    if (options.remote_url != null and options.remote_token_env == null) {
        if (ha_cfg.admin_token_env) |name| options.remote_token_env = try validateHAAdminTokenEnvName(name);
    }
    if (options.hasLocalHandles()) {
        if (options.identity.cluster_id == null) options.identity.cluster_id = ha_cfg.cluster_id;
        if (options.identity.shard_id == null) options.identity.shard_id = ha_cfg.shard_id;
        if (options.identity.table_id == null) options.identity.table_id = ha_cfg.table_id;
        if (options.identity.timeline_id == null) options.identity.timeline_id = ha_cfg.timeline_id;
        if (options.identity.epoch == null) options.identity.epoch = ha_cfg.epoch;
    }
    return cfg;
}

/// Environment lookup seam so defaults can be tested without touching the
/// process environment.
const EnvLookup = struct {
    ctx: ?*const anyopaque = null,
    getFn: *const fn (ctx: ?*const anyopaque, name: [:0]const u8) ?[]const u8,

    fn get(self: EnvLookup, name: [:0]const u8) ?[]const u8 {
        return self.getFn(self.ctx, name);
    }
};

fn processEnvGet(_: ?*const anyopaque, name: [:0]const u8) ?[]const u8 {
    const raw = std.c.getenv(name.ptr) orelse return null;
    return std.mem.span(raw);
}

const process_env = EnvLookup{ .getFn = processEnvGet };

pub const default_admin_url_env = "ANTFLY_STANDBY_ADMIN_URL";
pub const default_admin_token_env = "ANTFLY_STANDBY_ADMIN_TOKEN";
/// Deprecated spellings, still honored as a fallback for one minor release.
pub const legacy_admin_url_env = "ANTFLY_HA_ADMIN_URL";
pub const legacy_admin_token_env = "ANTFLY_HA_ADMIN_TOKEN";

fn nonEmptyEnv(env: EnvLookup, name: [:0]const u8) ?[]const u8 {
    const raw = env.get(name) orelse return null;
    const trimmed = std.mem.trim(u8, raw, " \t\r\n");
    return if (trimmed.len > 0) trimmed else null;
}

/// Fills in the admin URL from `ANTFLY_STANDBY_ADMIN_URL` (falling back to
/// the deprecated `ANTFLY_HA_ADMIN_URL`) when no target was given, and the
/// token environment variable name from whichever of
/// `ANTFLY_STANDBY_ADMIN_TOKEN` / `ANTFLY_HA_ADMIN_TOKEN` is set when a
/// remote target is in use. Local handles and `--data-dir` always win over
/// the environment, so a configured default URL never turns a local-file
/// command into an HTTP call.
fn applyEnvironmentDefaults(options: *LocalOptions, env: EnvLookup) !void {
    if (options.remote_url == null and options.data_dir == null and !options.hasLocalHandles()) {
        const raw = nonEmptyEnv(env, default_admin_url_env) orelse nonEmptyEnv(env, legacy_admin_url_env);
        if (raw) |trimmed| options.remote_url = try validateHAAdminURL(trimmed);
    }
    if (options.remote_url != null and options.remote_token_env == null) {
        if (nonEmptyEnv(env, default_admin_token_env) != null) {
            options.remote_token_env = default_admin_token_env;
        } else if (nonEmptyEnv(env, legacy_admin_token_env) != null) {
            options.remote_token_env = legacy_admin_token_env;
        }
    }
}

fn value(argv: []const []const u8, idx: *usize, flag: []const u8) ![]const u8 {
    if (idx.* >= argv.len) {
        if (std.mem.eql(u8, flag, "--primary-log")) return error.PrimaryLogMissing;
        if (std.mem.eql(u8, flag, "--primary-slots")) return error.PrimarySlotsMissing;
        if (std.mem.eql(u8, flag, "--standby-log")) return error.StandbyLogMissing;
        if (std.mem.eql(u8, flag, "--standby-progress")) return error.StandbyProgressMissing;
        if (std.mem.eql(u8, flag, "--fence-wal")) return error.FenceWalMissing;
        return error.FlagValueMissing;
    }
    const out = argv[idx.*];
    idx.* += 1;
    return out;
}

fn resolveRemoteBearerToken(alloc: std.mem.Allocator, options: LocalOptions) !?[]u8 {
    const env_var = try validateHAAdminTokenEnvName(options.remote_token_env orelse return null);

    const env_var_z = try alloc.dupeZ(u8, env_var);
    defer alloc.free(env_var_z);

    const raw_token_z = std.c.getenv(env_var_z.ptr) orelse return error.HAAdminTokenMissing;
    const token = std.mem.trim(u8, std.mem.span(raw_token_z), " \t\r\n");
    if (token.len == 0) return error.HAAdminTokenMissing;
    return try alloc.dupe(u8, token);
}

fn resolveHAPodUID() !?[]const u8 {
    const raw_z = std.c.getenv("ANTFLY_POD_UID") orelse return null;
    const pod_uid = std.mem.trim(u8, std.mem.span(raw_z), " \t\r\n");
    if (!ha_validation.isIdentifier(pod_uid)) return error.HAPodUIDInvalid;
    return pod_uid;
}

const HAPathField = enum {
    primary_log,
    primary_slots,
    standby_log,
    standby_progress,
    fence_wal,
    former_primary_log,
    data_dir,
};

fn validateHAPath(path: []const u8, field: HAPathField) ![]const u8 {
    switch (ha_validation.classifyHAString(path)) {
        .ok => {},
        .missing => return switch (field) {
            .primary_log => error.PrimaryLogMissing,
            .primary_slots => error.PrimarySlotsMissing,
            .standby_log => error.StandbyLogMissing,
            .standby_progress => error.StandbyProgressMissing,
            .fence_wal => error.FenceWalMissing,
            .former_primary_log => error.FormerPrimaryLogMissing,
            .data_dir => error.HADataDirMissing,
        },
        .padded => return haPathInvalidError(field),
    }
    if (!ha_validation.isAbsoluteNormalizedPath(path)) return haPathInvalidError(field);
    return path;
}

fn haPathInvalidError(field: HAPathField) anyerror {
    return switch (field) {
        .primary_log => error.HAPrimaryLogInvalid,
        .primary_slots => error.HAPrimarySlotsInvalid,
        .standby_log => error.HAStandbyLogInvalid,
        .standby_progress => error.HAStandbyProgressInvalid,
        .fence_wal => error.HAFenceWalInvalid,
        .former_primary_log => error.HAFormerPrimaryLogInvalid,
        .data_dir => error.HADataDirInvalid,
    };
}

const HANodeIDField = enum {
    primary,
    standby,
};

fn validateHANodeID(node_id: []const u8, field: HANodeIDField) ![]const u8 {
    switch (ha_validation.classifyHAString(node_id)) {
        .ok => {},
        .missing, .padded => return haNodeIDInvalidError(field),
    }
    if (!ha_validation.isIdentifier(node_id)) return haNodeIDInvalidError(field);
    return node_id;
}

fn haNodeIDInvalidError(field: HANodeIDField) anyerror {
    return switch (field) {
        .primary => error.HAPrimaryNodeIdInvalid,
        .standby => error.HAStandbyNodeIdInvalid,
    };
}

fn validateHAAdminURL(raw_url: []const u8) ![]const u8 {
    switch (ha_validation.classifyHAString(raw_url)) {
        .ok => {},
        .missing => return error.HAAdminURLMissing,
        .padded => return error.HAAdminURLInvalid,
    }
    if (!ha_validation.isHTTPURLWithHostNoHiddenWhitespace(raw_url)) return error.HAAdminURLInvalid;
    return raw_url;
}

fn validateHAAdminTokenEnvName(raw_env_var: []const u8) ![]const u8 {
    switch (ha_validation.classifyHAString(raw_env_var)) {
        .ok => {},
        .missing => return error.HAAdminTokenEnvMissing,
        .padded => return error.HAAdminTokenEnvInvalid,
    }
    if (!ha_validation.isEnvVarName(raw_env_var)) return error.HAAdminTokenEnvInvalid;
    return raw_env_var;
}

fn parseU64(raw: []const u8) !u64 {
    return try std.fmt.parseInt(u64, raw, 10);
}

fn printUsage(argv0: []const u8) void {
    std.debug.print(
        \\usage: {s} standby [target options] <command> [command options]
        \\
        \\Manage hot-standby replication for a standalone Antfly node.
        \\`antfly ha` is a deprecated alias for this command and will be
        \\removed in a future minor release.
        \\
        \\Target (pick one):
        \\  --data-dir DIR            Open the node's local HA state under DIR/standby/
        \\                            (primary.wal, slots, log.wal, progress.wal,
        \\                            fence.wal), falling back to a legacy DIR/ha/
        \\                            tree (primary.wal, slots, standby.wal,
        \\                            standby-progress.wal, fence.wal) when present.
        \\                            Paths and identity are read from the files
        \\                            that exist.
        \\  --admin-url URL           Send the command to a running node's HA admin
        \\                            endpoint. Defaults to $ANTFLY_STANDBY_ADMIN_URL,
        \\                            falling back to $ANTFLY_HA_ADMIN_URL.
        \\  --admin-token-env NAME    Environment variable holding the admin bearer
        \\                            token. Defaults to ANTFLY_STANDBY_ADMIN_TOKEN
        \\                            (falling back to ANTFLY_HA_ADMIN_TOKEN) when
        \\                            one of those variables is set. Raw token
        \\                            flags are refused.
        \\  --config PATH             Read the server config file's `hot_standby`
        \\                            section (the deprecated `ha` section is still
        \\                            read): `hot_standby.admin.url` makes the
        \\                            command remote, else its paths and identity
        \\                            become local handles.
        \\
        \\Explicit local handles (override --data-dir, or use without it):
        \\  --primary-log PATH  --primary-slots PATH  --primary-node-id NODE
        \\  --standby-log PATH  --standby-progress PATH  --standby-node-id NODE
        \\  --fence-wal PATH    --former-primary-log PATH
        \\  --cluster-id N --shard-id N --table-id N --timeline-id N --epoch N
        \\                            Identity of the local logs; only needed when
        \\                            it cannot be read from the files themselves.
        \\
        \\The --ha-* spellings (--ha-url, --ha-token-env, --ha-cluster-id, ...) are
        \\accepted for compatibility. A literal "--" before the command is optional.
        \\
        \\commands:
        \\  status primary|standby    Replication status of this node
        \\  slot list|create|drop|pause|resume
        \\  seed begin|finish|bootstrap
        \\  fence current|acquire
        \\  promote [assess] [--current-fence]
        \\  rejoin assess|rewind|reseed
        \\  follow --upstream-url URL --slot NAME --cluster-id N --timeline-id N --epoch N
        \\                            Repoint this standby at another primary (remote only)
        \\  switchover --to URL [--follower SLOT=URL ...] [--generation N]
        \\             [--wait-seconds N] [--max-lag-lsn N] [--dry-run] [--json]
        \\                            Planned primary change: fence, wait, promote,
        \\                            rejoin, follow (remote only; target is the primary)
        \\  stream once, commit append|check, read check, write check, owner-job check
        \\  artifact publish|restore|verify|activate|prune|gc-source|gc-target
        \\
        \\examples:
        \\  {s} standby --data-dir /var/lib/antfly status primary
        \\  {s} standby --admin-url http://127.0.0.1:8081 status standby
        \\  {s} standby --admin-url http://127.0.0.1:8081 promote assess --current-fence
        \\  {s} standby --admin-url http://primary:8080 switchover --to http://standby-a:8080 --follower standby-b=http://standby-b:8080
        \\  {s} standby artifact publish --location s3://ha-seeds/cluster-a --generation seed-standby-a-42 --slot standby-a --manifest /source/manifest.afha --content-root /source/content
        \\
    , .{ argv0, argv0, argv0, argv0, argv0, argv0 });
}

test "standby cmd fence request preserves unsigned counters and omitted generation" {
    const high: u64 = @as(u64, 1) << 63;
    var request: ha.fencing.FenceRequest = .{
        .identity = testIdentity(),
        .old_primary_id = "primary",
        .promoted_node_id = "standby",
        .new_timeline_id = high + 1,
        .new_epoch = high + 2,
        .generation = 0,
        .required_lsn = high + 3,
        .observed_lsn = high + 4,
    };
    request.identity.cluster_id = high;
    const allocated = try fenceRequestOpenApi(request);
    try std.testing.expect(allocated.generation == null);
    try std.testing.expectEqual(high, allocated.identity.cluster_id);
    try std.testing.expectEqual(high + 1, allocated.new_timeline_id);
    try std.testing.expectEqual(high + 2, allocated.new_epoch);
    try std.testing.expectEqual(high + 3, allocated.required_lsn);
    try std.testing.expectEqual(high + 4, allocated.observed_lsn);
    request.generation = std.math.maxInt(u64);
    const exact = try fenceRequestOpenApi(request);
    try std.testing.expectEqual(std.math.maxInt(u64), exact.generation.?);
}

test "standby cmd parses local handles before admin command" {
    const alloc = std.testing.allocator;
    var parsed = try parseLocalArgs(alloc, &.{
        "--primary-log",     "/tmp/p.wal",
        "--primary-slots",   "/tmp/slots.wal",
        "--primary-node-id", "primary-a",
        "--ha-cluster-id",   "10",
        "--ha-shard-id",     "20",
        "--ha-table-id",     "30",
        "--ha-timeline-id",  "1",
        "--ha-epoch",        "2",
        "--",                "--table",
        "slot",              "list",
    });
    defer parsed.deinit(alloc);

    try std.testing.expectEqualStrings("/tmp/p.wal", parsed.options.primary_log.?);
    try std.testing.expectEqualStrings("primary-a", parsed.options.primary_node_id.?);
    try std.testing.expectEqual(@as(u64, 10), parsed.options.identity.cluster_id.?);
    try std.testing.expectEqual(@as(usize, 3), parsed.command_args.len);
    try std.testing.expectEqualStrings("--table", parsed.command_args[0]);
    try std.testing.expectEqualStrings("slot", parsed.command_args[1]);
    try std.testing.expectEqualStrings("list", parsed.command_args[2]);

    const identity = try parsed.options.primaryIdentity();
    try std.testing.expectEqual(@as(u64, 30), identity.table_id);
}

test "standby cmd local handles default shard and table identity to whole instance" {
    const alloc = std.testing.allocator;
    var parsed = try parseLocalArgs(alloc, &.{
        "--primary-log",    "/tmp/p.wal",
        "--primary-slots",  "/tmp/slots.wal",
        "--ha-cluster-id",  "10",
        "--ha-timeline-id", "1",
        "--ha-epoch",       "2",
        "--",               "slot",
        "list",
    });
    defer parsed.deinit(alloc);

    const primary_identity = try parsed.options.primaryIdentity();
    try std.testing.expectEqual(@as(u64, 10), primary_identity.cluster_id);
    try std.testing.expectEqual(@as(u64, 0), primary_identity.shard_id);
    try std.testing.expectEqual(@as(u64, 0), primary_identity.table_id);
    try std.testing.expectEqual(@as(u64, 1), primary_identity.timeline_id);
    try std.testing.expectEqual(@as(u64, 2), primary_identity.epoch);

    parsed.options.primary_log = null;
    parsed.options.primary_slots = null;
    parsed.options.standby_log = "/tmp/standby.wal";
    parsed.options.standby_progress = "/tmp/progress.wal";
    const standby_identity = try parsed.options.standbyIdentity();
    try std.testing.expectEqual(@as(u64, 10), standby_identity.cluster_id);
    try std.testing.expectEqual(@as(u64, 0), standby_identity.shard_id);
    try std.testing.expectEqual(@as(u64, 0), standby_identity.table_id);
    try std.testing.expectEqual(@as(u64, 1), standby_identity.timeline_id);
    try std.testing.expectEqual(@as(u64, 2), standby_identity.epoch);
}

test "standby cmd artifact parses offline seed activation target and identity" {
    const alloc = std.testing.allocator;
    var options = try parseArtifactArgs(alloc, &.{
        "activate",
        "--generation",
        "seed-standby-a-10",
        "--slot",
        "standby-a",
        "--staging-root",
        "/target/.antfly-ha/staging",
        "--target-root",
        "/target",
        "--ha-cluster-id",
        "100",
        "--ha-shard-id",
        "0",
        "--ha-table-id",
        "0",
        "--ha-timeline-id",
        "4",
        "--ha-epoch",
        "6",
        "--minimum-checkpoint-lsn",
        "10",
        "--topology-id",
        "topology-a",
        "--topology-generation",
        "3",
        "--node-id",
        "standby-a",
        "--target-pvc-name",
        "standby-a-data",
        "--target-pvc-uid",
        "pvc-uid-1",
        "--target-local-node-id",
        "77",
        "--target-replica-id",
        "9",
    });
    defer options.deinit(alloc);
    try std.testing.expectEqual(ArtifactAction.activate, options.action);
    try std.testing.expectEqualStrings("/target/.antfly-ha/staging", options.staging_root.?);
    try std.testing.expectEqualStrings("/target", options.target_root.?);
    try std.testing.expectEqual(@as(u64, 100), options.identity.cluster_id.?);
    try std.testing.expectEqual(@as(u64, 10), options.minimum_checkpoint_lsn);
    try std.testing.expectEqualStrings("topology-a", options.binding.topology_id.?);
    try std.testing.expectEqual(@as(u64, 3), options.binding.topology_generation.?);
    try std.testing.expectEqualStrings("pvc-uid-1", options.binding.target_pvc_uid.?);
    try std.testing.expect(@hasField(ArtifactOptions, "target_local_node_id"));
    try std.testing.expect(@hasField(ArtifactOptions, "target_replica_id"));
    try std.testing.expectEqual(@as(u64, 77), options.target_local_node_id.?);
    try std.testing.expectEqual(@as(u64, 9), options.target_replica_id.?);
}

test "standby cmd artifact accepts portable publish and restore topology pvc binding" {
    const alloc = std.testing.allocator;
    const binding_args = [_][]const u8{
        "--topology-id",         "topology-a",
        "--topology-generation", "9",
        "--node-id",             "primary-a",
        "--target-pvc-name",     "standby-a-data",
        "--target-pvc-uid",      "pvc-uid-9",
    };
    var publish_argv = std.ArrayListUnmanaged([]const u8).empty;
    defer publish_argv.deinit(alloc);
    try publish_argv.appendSlice(alloc, &.{
        "publish",        "--location",      "s3://ha-seeds/topology-a",
        "--generation",   "seed-9",          "--slot",
        "standby-a",      "--manifest",      "/source/manifest.afha",
        "--content-root", "/source/content",
    });
    try publish_argv.appendSlice(alloc, &binding_args);
    var publish = try parseArtifactArgs(alloc, publish_argv.items);
    defer publish.deinit(alloc);
    const publish_binding = (try publish.binding.finish()) orelse return error.TestExpectedEqual;
    try std.testing.expectEqualStrings("topology-a", publish_binding.topology_id);
    try std.testing.expectEqual(@as(u64, 9), publish_binding.topology_generation);
    try std.testing.expectEqualStrings("pvc-uid-9", publish_binding.target_pvc_uid);

    var restore_argv = std.ArrayListUnmanaged([]const u8).empty;
    defer restore_argv.deinit(alloc);
    try restore_argv.appendSlice(alloc, &.{
        "restore",         "--location",     "s3://ha-seeds/topology-a",
        "--generation",    "seed-9",         "--slot",
        "standby-a",       "--staging-root", "/target/staging",
        "--ha-cluster-id", "1",              "--ha-timeline-id",
        "1",               "--ha-epoch",     "1",
    });
    try restore_argv.appendSlice(alloc, &binding_args);
    var restore = try parseArtifactArgs(alloc, restore_argv.items);
    defer restore.deinit(alloc);
    try std.testing.expect((try restore.binding.finish()) != null);
}

test "standby cmd artifact requires capture receipt digest chain flags" {
    const alloc = std.testing.allocator;
    var options = try parseArtifactArgs(alloc, &.{
        "publish",
        "--location",
        "s3://ha-seeds/topology-a",
        "--generation",
        "seed-9",
        "--slot",
        "standby-a",
        "--manifest",
        "/source/manifest.afha",
        "--content-root",
        "/source/content",
        "--capture-receipt",
        "/source/COMPLETE.json",
        "--capture-receipt-sha256",
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "--topology-id",
        "topology-a",
        "--topology-generation",
        "9",
        "--node-id",
        "primary-a",
        "--target-pvc-name",
        "standby-a-data",
        "--target-pvc-uid",
        "pvc-uid-9",
    });
    defer options.deinit(alloc);
    try std.testing.expect(@hasField(ArtifactOptions, "capture_receipt_path"));
    try std.testing.expect(@hasField(ArtifactOptions, "capture_receipt_sha256"));
    try std.testing.expectEqualStrings("/source/COMPLETE.json", options.capture_receipt_path.?);
    try std.testing.expectEqualStrings(
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        options.capture_receipt_sha256.?,
    );
    try std.testing.expectEqualStrings(
        options.capture_receipt_sha256.?,
        try requireCaptureReceiptDigest(options.capture_receipt_sha256),
    );
}

test "standby cmd artifact parses lifecycle-gated source and target generation gc actions" {
    const alloc = std.testing.allocator;
    var source = try parseArtifactArgs(alloc, &.{
        "gc-source",
        "--location",
        "s3://ha-seeds/cluster-a",
        "--generation",
        "seed-standby-a-42",
        "--slot",
        "standby-a",
        "--capture-root",
        "/source/.antfly-ha/captures",
        "--retain-generations",
        "2",
        "--protect-generation",
        "seed-standby-a-41",
        "--protect-generation",
        "seed-standby-a-40",
    });
    defer source.deinit(alloc);
    try std.testing.expectEqual(ArtifactAction.gc_source, source.action);
    try std.testing.expectEqualStrings("/source/.antfly-ha/captures", source.capture_root.?);
    try std.testing.expectEqual(@as(usize, 2), source.protected_generations.len);

    var target = try parseArtifactArgs(alloc, &.{
        "gc-target",
        "--target-root",
        "/target",
        "--slot-activation-receipt",
        "/checkpoint/seeded-slot-activation.json",
        "--retain-generations",
        "2",
        "--protect-generation",
        "seed-standby-a-41",
    });
    defer target.deinit(alloc);
    try std.testing.expectEqual(ArtifactAction.gc_target, target.action);
    try std.testing.expectEqualStrings("/checkpoint/seeded-slot-activation.json", target.slot_activation_receipt_path.?);
    try std.testing.expectEqual(@as(usize, 1), target.protected_generations.len);
}

test "standby cmd artifact parses exact controller-authorized seed prefix deletion" {
    const alloc = std.testing.allocator;
    var options = try parseArtifactArgs(alloc, &.{
        "delete-prefix",
        "--location",
        "s3://ha-bucket/instances/instance-a/ha-seeds/",
        "--operation-id",
        "ha-seed-delete-0123456789abcdef0123456789abcdef",
        "--retry-token",
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "--instance-id",
        "instance-a",
        "--topology-id",
        "topology-a",
        "--topology-generation",
        "7",
        "--prefix-sha256",
        "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        "--credentials-secret-name",
        "instance-a-ha-seed-store",
        "--delete-all",
        "--request-sha256",
        "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
    });
    defer options.deinit(alloc);
    try std.testing.expectEqual(ArtifactAction.delete_prefix, options.action);
    try std.testing.expectEqualStrings("instance-a", options.cleanup.instance_id.?);
    try std.testing.expectEqualStrings("topology-a", options.cleanup.topology_id.?);
    try std.testing.expectEqual(@as(u64, 7), options.cleanup.topology_generation.?);
    try std.testing.expect(options.cleanup.delete_all);

    try std.testing.expectError(error.SeedArtifactFlagNotAllowedForAction, parseArtifactArgs(alloc, &.{
        "delete-prefix",
        "--generation",
        "seed-a",
    }));
    try std.testing.expectError(error.DuplicateSeedArtifactFlag, parseArtifactArgs(alloc, &.{
        "delete-prefix",
        "--delete-all",
        "--delete-all",
    }));
}

test "standby cmd artifact rejects flags that the selected action would ignore" {
    const alloc = std.testing.allocator;

    try std.testing.expectError(error.SeedArtifactFlagNotAllowedForAction, parseArtifactArgs(alloc, &.{
        "prune",
        "--location",
        "s3://ha-seeds/cluster-a",
        "--generation",
        "seed-standby-a-42",
        "--slot",
        "standby-a",
        "--protect-generation",
        "seed-standby-a-41",
    }));
    try std.testing.expectError(error.SeedArtifactFlagNotAllowedForAction, parseArtifactArgs(alloc, &.{
        "gc-source",
        "--target-root",
        "/target",
    }));
    try std.testing.expectError(error.SeedArtifactFlagNotAllowedForAction, parseArtifactArgs(alloc, &.{
        "gc-target",
        "--generation",
        "seed-standby-a-42",
    }));
    try std.testing.expectError(error.SeedArtifactFlagNotAllowedForAction, parseArtifactArgs(alloc, &.{
        "publish",
        "--retain-generations",
        "2",
    }));
}

test "standby cmd artifact rejects duplicate single-valued flags" {
    const alloc = std.testing.allocator;

    try std.testing.expectError(error.DuplicateSeedArtifactFlag, parseArtifactArgs(alloc, &.{
        "gc-source",
        "--generation",
        "seed-standby-a-42",
        "--generation",
        "seed-standby-a-43",
    }));
    try std.testing.expectError(error.DuplicateProtectedSeedGeneration, parseArtifactArgs(alloc, &.{
        "gc-target",
        "--protect-generation",
        "seed-standby-a-41",
        "--protect-generation",
        "seed-standby-a-41",
    }));
}

test "standby cmd parses remote admin URL before command" {
    const alloc = std.testing.allocator;
    var parsed = try parseLocalArgs(alloc, &.{
        "--ha-url",       "http://127.0.0.1:8081",
        "--ha-token-env", "ANTFLY_HA_ADMIN_TOKEN",
        "--",             "--table",
        "status",         "primary",
    });
    defer parsed.deinit(alloc);

    try std.testing.expectEqualStrings("http://127.0.0.1:8081", parsed.options.remote_url.?);
    try std.testing.expectEqualStrings("ANTFLY_HA_ADMIN_TOKEN", parsed.options.remote_token_env.?);
    try std.testing.expectEqual(@as(usize, 3), parsed.command_args.len);
    try std.testing.expectEqualStrings("--table", parsed.command_args[0]);
    try std.testing.expectEqualStrings("status", parsed.command_args[1]);
    try std.testing.expectEqualStrings("primary", parsed.command_args[2]);
}

test "standby cmd validates remote bearer token env name" {
    const alloc = std.testing.allocator;

    try std.testing.expect((try resolveRemoteBearerToken(alloc, .{})) == null);
    try std.testing.expectError(error.HAAdminTokenEnvMissing, parseLocalArgs(alloc, &.{ "--ha-url", "http://127.0.0.1:8081", "--ha-token-env", " \t ", "--", "status", "primary" }));
    try std.testing.expectError(error.HAAdminTokenEnvInvalid, parseLocalArgs(alloc, &.{ "--ha-url", "http://127.0.0.1:8081", "--ha-token-env", " ANTFLY_HA_ADMIN_TOKEN", "--", "status", "primary" }));
    try std.testing.expectError(error.HAAdminTokenEnvInvalid, resolveRemoteBearerToken(alloc, .{
        .remote_token_env = "bad-token-env",
    }));
    try std.testing.expectError(error.HAAdminTokenEnvInvalid, resolveRemoteBearerToken(alloc, .{
        .remote_token_env = "9TOKEN",
    }));
    try std.testing.expectError(error.HAAdminTokenMissing, resolveRemoteBearerToken(alloc, .{
        .remote_token_env = "ANTFLY_HA_ADMIN_TOKEN_SHOULD_NOT_EXIST",
    }));
}

test "standby cmd classifies HA strings before field-specific validation" {
    try std.testing.expectEqual(ha_validation.HAStringValidation.missing, ha_validation.classifyHAString(null));
    try std.testing.expectEqual(ha_validation.HAStringValidation.missing, ha_validation.classifyHAString(""));
    try std.testing.expectEqual(ha_validation.HAStringValidation.missing, ha_validation.classifyHAString(" \t\r\n"));
    try std.testing.expectEqual(ha_validation.HAStringValidation.padded, ha_validation.classifyHAString(" primary-a"));
    try std.testing.expectEqual(ha_validation.HAStringValidation.padded, ha_validation.classifyHAString("primary-a\n"));
    try std.testing.expectEqual(ha_validation.HAStringValidation.ok, ha_validation.classifyHAString("primary-a"));
}

test "standby cmd rejects padded or invalid HA local option strings" {
    const alloc = std.testing.allocator;

    try std.testing.expectError(error.HAAdminURLInvalid, parseLocalArgs(alloc, &.{ "--ha-url", " http://127.0.0.1:8081", "--", "status", "primary" }));
    try std.testing.expectError(error.HAAdminURLInvalid, parseLocalArgs(alloc, &.{ "--ha-url", "http://127.0.0.1:8081/\tadmin", "--", "status", "primary" }));
    try std.testing.expectError(error.HAAdminURLInvalid, parseLocalArgs(alloc, &.{ "--ha-url", "http://127.0.0.1:8081/a b", "--", "status", "primary" }));
    try std.testing.expectError(error.HAAdminURLInvalid, parseLocalArgs(alloc, &.{ "--ha-url", "not-a-url", "--", "status", "primary" }));
    try std.testing.expectError(error.HAAdminURLInvalid, parseLocalArgs(alloc, &.{ "--ha-url", "ftp://127.0.0.1:8081", "--", "status", "primary" }));
    try std.testing.expectError(error.HAPrimaryLogInvalid, parseLocalArgs(alloc, &.{ "--primary-log", " p.wal", "--", "slot", "list" }));
    try std.testing.expectError(error.HAPrimaryLogInvalid, parseLocalArgs(alloc, &.{ "--primary-log", "p.wal", "--", "slot", "list" }));
    try std.testing.expectError(error.HAPrimarySlotsInvalid, parseLocalArgs(alloc, &.{ "--primary-slots", "slots//wal", "--", "slot", "list" }));
    try std.testing.expectError(error.HAStandbyLogInvalid, parseLocalArgs(alloc, &.{ "--standby-log", "../standby.wal", "--", "status", "standby" }));
    try std.testing.expectError(error.HAStandbyProgressInvalid, parseLocalArgs(alloc, &.{ "--standby-progress", "progress.wal\n", "--", "status", "standby" }));
    try std.testing.expectError(error.HAFenceWalInvalid, parseLocalArgs(alloc, &.{ "--fence-wal", ".", "--", "fence", "current" }));
    try std.testing.expectError(error.HAFormerPrimaryLogInvalid, parseLocalArgs(alloc, &.{ "--former-primary-log", "former/../primary.wal", "--", "rejoin", "assess" }));
    try std.testing.expectError(error.HAPrimaryNodeIdInvalid, parseLocalArgs(alloc, &.{ "--primary-node-id", "primary a", "--", "status", "primary" }));
    try std.testing.expectError(error.HAStandbyNodeIdInvalid, parseLocalArgs(alloc, &.{ "--standby-node-id", " standby-a", "--", "status", "standby" }));
}

test "standby cmd rejects raw bearer token argv flags" {
    const alloc = std.testing.allocator;

    try std.testing.expectError(error.HAAdminRawTokenFlagUnsupported, parseLocalArgs(alloc, &.{
        "--ha-url",   "http://127.0.0.1:8081",
        "--ha-token", "secret-token",
        "--",         "status",
        "primary",
    }));
    try std.testing.expectError(error.HAAdminRawTokenFlagUnsupported, parseLocalArgs(alloc, &.{
        "--ha-url", "http://127.0.0.1:8081",
        "--token",  "secret-token",
        "--",       "status",
        "primary",
    }));
    try std.testing.expectError(error.HAAdminRawTokenFlagUnsupported, parseLocalArgs(alloc, &.{
        "--ha-url",        "http://127.0.0.1:8081",
        "--ha-token-file", "/run/secrets/ha-token",
        "--",              "status",
        "primary",
    }));
}

test "standby cmd remote commands prefer typed admin routes" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "remote-typed");
    defer paths.deinit(alloc);

    var primary = try ha.primary.Primary.open(alloc, paths.primary_log.ptr, paths.primary_slots.ptr, testIdentity(), .{});
    defer primary.close();
    var standby = try ha.standby.Standby.open(alloc, paths.standby_log.ptr, paths.standby_progress.ptr, testIdentity(), .{});
    defer standby.close();
    var fence_store = try ha.fencing.Store.open(alloc, paths.fence_wal.ptr, .{});
    defer fence_store.close();

    var server = ha.http_admin.Server.init(alloc, .{
        .primary = &primary,
        .primary_node_id = "primary-a",
        .standby = &standby,
        .standby_node_id = "standby-a",
        .fence_store = &fence_store,
    });
    defer server.deinit();
    var recorder = RecordingExecutor.init(alloc, server.executor());
    defer recorder.deinit();

    try runRemoteArgv(alloc, std.testing.io, "http://ha-admin.test", &.{
        "slot",
        "create",
        "standby-json",
        "--initial-lsn",
        "0",
    }, recorder.executor());

    try expectTypedRoute(&recorder, .POST, admin_api.routes.ha_replication_slots);

    try runRemoteArgv(alloc, std.testing.io, "http://ha-admin.test", &.{
        "--table",
        "slot",
        "create",
        "standby-table",
        "--initial-lsn",
        "0",
    }, recorder.executor());

    try expectTypedRoute(&recorder, .POST, admin_api.routes.ha_replication_slots);

    try runRemoteArgv(alloc, std.testing.io, "http://ha-admin.test", &.{
        "slot",
        "pause",
        "standby-table",
    }, recorder.executor());

    try expectTypedRoute(&recorder, .PUT, admin_api.routes.ha_replication_slot_prefix ++ "standby-table" ++ admin_api.routes.ha_replication_slot_pause_suffix);

    try runRemoteArgv(alloc, std.testing.io, "http://ha-admin.test", &.{
        "slot",
        "resume",
        "standby-table",
    }, recorder.executor());

    try expectTypedRoute(&recorder, .PUT, admin_api.routes.ha_replication_slot_prefix ++ "standby-table" ++ admin_api.routes.ha_replication_slot_resume_suffix);

    try runRemoteArgv(alloc, std.testing.io, "http://ha-admin.test", &.{
        "slot",
        "drop",
        "standby-table",
    }, recorder.executor());

    try expectTypedRoute(&recorder, .DELETE, admin_api.routes.ha_replication_slot_prefix ++ "standby-table");

    try runRemoteArgv(alloc, std.testing.io, "http://ha-admin.test", &.{
        "seed",
        "begin",
        "--slot",
        "standby-seed",
        "--manifest-id",
        "base-standby-json-1",
    }, recorder.executor());

    try expectTypedRoute(&recorder, .POST, admin_api.routes.ha_base_backups);

    try std.testing.expectEqual(@as(u64, 2), try primary.append(.{ .payload = "during-copy" }));
    const manifest_path = try writeSeedManifestFiles(alloc, paths.backup_root, testIdentity(), "base-standby-json-1", 1, 2);
    defer alloc.free(manifest_path);

    try runRemoteArgv(alloc, std.testing.io, "http://ha-admin.test", &.{
        "seed",
        "finish",
        "--manifest",
        manifest_path,
    }, recorder.executor());

    try expectTypedRoute(&recorder, .POST, admin_api.routes.ha_base_backups_finish);

    try runRemoteArgv(alloc, std.testing.io, "http://ha-admin.test", &.{
        "seed",
        "bootstrap",
        "--manifest",
        manifest_path,
        "--content-root",
        paths.backup_root,
    }, recorder.executor());

    try expectTypedRoute(&recorder, .POST, admin_api.routes.ha_standby_bootstrap);
    // Production bootstrap keeps the seed slot in the seeding lifecycle until
    // the target has durably published its activation receipt. Model that
    // explicit transition before asking the standby to consume later WAL.
    try primary.activateSeededSlot("standby-seed", testIdentity().timeline_id, 2, 2, 2);

    try runRemoteArgv(alloc, std.testing.io, "http://ha-admin.test", &.{
        "status",
        "primary",
        "--max-lag-lsn",
        "4",
        "--max-retained-bytes",
        "4096",
        "--max-retained-age-ns",
        "1000000",
    }, recorder.executor());

    try expectTypedRoute(&recorder, .GET, admin_api.routes.ha_primary_status);
    try expectContains(recorder.last_uri.?, "max_lag_lsn=4");
    try expectContains(recorder.last_uri.?, "max_retained_bytes=4096");
    try expectContains(recorder.last_uri.?, "max_retained_age_ns=1000000");

    try runRemoteArgv(alloc, std.testing.io, "http://ha-admin.test", &.{
        "--prometheus",
        "status",
        "primary",
        "--view",
        "metrics",
    }, recorder.executor());

    try expectTypedRoute(&recorder, .GET, admin_api.routes.ha_primary_status);

    try runRemoteArgv(alloc, std.testing.io, "http://ha-admin.test", &.{
        "slot",
        "list",
        "--max-retained-bytes",
        "8192",
        "--max-retained-age-ns",
        "2000000",
    }, recorder.executor());

    try expectTypedRoute(&recorder, .GET, admin_api.routes.ha_primary_status);
    try expectContains(recorder.last_uri.?, "max_retained_bytes=8192");
    try expectContains(recorder.last_uri.?, "max_retained_age_ns=2000000");

    try runRemoteArgv(alloc, std.testing.io, "http://ha-admin.test", &.{
        "status",
        "standby",
        "--upstream-lsn",
        "4",
    }, recorder.executor());

    try expectTypedRoute(&recorder, .GET, admin_api.routes.ha_standby_status);

    try runRemoteArgv(alloc, std.testing.io, "http://ha-admin.test", &.{
        "--prometheus",
        "status",
        "standby",
        "--view",
        "metrics",
        "--upstream-lsn",
        "4",
    }, recorder.executor());

    try expectTypedRoute(&recorder, .GET, admin_api.routes.ha_standby_status);

    try runRemoteArgv(alloc, std.testing.io, "http://ha-admin.test", &.{
        "commit",
        "append",
        "--payload",
        "remote-cli-record",
        "--sync-mode",
        "async",
    }, recorder.executor());

    try expectTypedRoute(&recorder, .POST, admin_api.routes.ha_commit_append);

    // Fence acquisition upgrades the caller's stale observation to the former
    // primary's live durable tail. Catch the standby up before promotion so the
    // integration test proves that stronger boundary is honored end to end.
    try runRemoteArgv(alloc, std.testing.io, "http://ha-admin.test", &.{
        "stream",
        "once",
        "--slot",
        "standby-seed",
    }, recorder.executor());

    try runRemoteArgv(alloc, std.testing.io, "http://ha-admin.test", &.{
        "commit",
        "check",
        "--target-lsn",
        "1",
        "--sync-mode",
        "async",
    }, recorder.executor());

    try expectTypedRoute(&recorder, .POST, admin_api.routes.ha_commit_check);

    try runRemoteArgv(alloc, std.testing.io, "http://ha-admin.test", &.{
        "read",
        "check",
        "--at-least-lsn",
        "0",
    }, recorder.executor());

    try expectTypedRoute(&recorder, .POST, admin_api.routes.ha_read_check);

    try runRemoteArgv(alloc, std.testing.io, "http://ha-admin.test", &.{
        "write",
        "check",
        "--role",
        "primary",
    }, recorder.executor());

    try expectTypedRoute(&recorder, .POST, admin_api.routes.ha_write_check);

    try runRemoteArgv(alloc, std.testing.io, "http://ha-admin.test", &.{
        "owner-job",
        "check",
        "--role",
        "primary",
        "--kind",
        "retention-advance",
    }, recorder.executor());

    try expectTypedRoute(&recorder, .POST, admin_api.routes.ha_owner_job_check);

    try runRemoteArgv(alloc, std.testing.io, "http://ha-admin.test", &.{
        "fence",
        "acquire",
        "--cluster-id",
        "10",
        "--shard-id",
        "20",
        "--table-id",
        "30",
        "--timeline-id",
        "1",
        "--epoch",
        "2",
        "--old-primary-id",
        "primary-a",
        "--promoted-node-id",
        "standby-a",
        "--new-timeline-id",
        "2",
        "--new-epoch",
        "3",
        "--generation",
        "1",
        "--required-lsn",
        "1",
        "--observed-lsn",
        "1",
        "--reason",
        "operator-approved",
    }, recorder.executor());

    try expectTypedRoute(&recorder, .POST, admin_api.routes.ha_fence);

    try runRemoteArgv(alloc, std.testing.io, "http://ha-admin.test", &.{
        "fence",
        "current",
    }, recorder.executor());

    try expectTypedRoute(&recorder, .GET, admin_api.routes.ha_fence_current);

    try runRemoteArgv(alloc, std.testing.io, "http://ha-admin.test", &.{
        "promote",
        "assess",
        "--required-lsn",
        "0",
        "--fencing-confirmed",
    }, recorder.executor());

    try expectTypedRoute(&recorder, .POST, admin_api.routes.ha_promotion_assess);

    try runRemoteArgv(alloc, std.testing.io, "http://ha-admin.test", &.{
        "promote",
        "--current-fence",
    }, recorder.executor());

    try expectTypedRoute(&recorder, .POST, admin_api.routes.ha_promotion_current_fence);

    try runRemoteArgv(alloc, std.testing.io, "http://ha-admin.test", &.{
        "--prometheus",
        "promote",
        "assess",
        "--required-lsn",
        "0",
        "--fencing-confirmed",
    }, recorder.executor());

    try expectTypedRoute(&recorder, .POST, admin_api.routes.ha_promotion_assess);

    try runRemoteArgv(alloc, std.testing.io, "http://ha-admin.test", &.{
        "rejoin",              "assess",
        "--node-id",           "primary-a",
        "--cluster-id",        "10",
        "--shard-id",          "20",
        "--table-id",          "30",
        "--timeline-id",       "1",
        "--epoch",             "2",
        "--last-lsn",          "12",
        "--retained-from-lsn", "8",
    }, recorder.executor());

    try expectTypedRoute(&recorder, .POST, admin_api.routes.ha_rejoin_assess);

    // Route selection is under test; this server has no matching former-primary log.
    try std.testing.expectError(error.HaCommandConflict, runRemoteArgv(alloc, std.testing.io, "http://ha-admin.test", &.{
        "rejoin",                     "rewind",
        "--node-id",                  "primary-a",
        "--cluster-id",               "10",
        "--shard-id",                 "20",
        "--table-id",                 "30",
        "--timeline-id",              "1",
        "--epoch",                    "2",
        "--last-lsn",                 "4",
        "--retained-from-lsn",        "0",
        "--fence-old-primary-id",     "primary-a",
        "--fence-promoted-node-id",   "standby-a",
        "--fence-parent-timeline-id", "1",
        "--fence-parent-epoch",       "2",
        "--fence-new-timeline-id",    "2",
        "--fence-new-epoch",          "3",
        "--fence-required-lsn",       "1",
        "--fence-observed-lsn",       "1",
        "--fence-generation",         "1",
        "--fence-token",              "ha-fence:10:20:30:2:3:1:standby-a",
    }, recorder.executor()));

    try expectTypedRoute(&recorder, .POST, admin_api.routes.ha_rejoin_rewind);

    try runRemoteArgv(alloc, std.testing.io, "http://ha-admin.test", &.{
        "slot",
        "create",
        "primary-a",
        "--initial-lsn",
        "0",
    }, recorder.executor());

    try std.testing.expectError(error.HaCommandConflict, runRemoteArgv(alloc, std.testing.io, "http://ha-admin.test", &.{
        "rejoin",                     "reseed",
        "--node-id",                  "primary-a",
        "--cluster-id",               "10",
        "--shard-id",                 "20",
        "--table-id",                 "30",
        "--timeline-id",              "1",
        "--epoch",                    "2",
        "--last-lsn",                 "4",
        "--retained-from-lsn",        "2",
        "--fence-old-primary-id",     "primary-a",
        "--fence-promoted-node-id",   "standby-a",
        "--fence-parent-timeline-id", "1",
        "--fence-parent-epoch",       "2",
        "--fence-new-timeline-id",    "2",
        "--fence-new-epoch",          "3",
        "--fence-required-lsn",       "1",
        "--fence-observed-lsn",       "1",
        "--fence-generation",         "1",
        "--fence-token",              "ha-fence:10:20:30:2:3:1:standby-a",
    }, recorder.executor()));

    try expectTypedRoute(&recorder, .POST, admin_api.routes.ha_rejoin_reseed);
}

test "standby cmd remote sends bearer token to authenticated admin route" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "remote-auth");
    defer paths.deinit(alloc);

    var primary = try ha.primary.Primary.open(alloc, paths.primary_log.ptr, paths.primary_slots.ptr, testIdentity(), .{});
    defer primary.close();

    var server = ha.http_admin.Server.initWithOptions(alloc, .{
        .primary = &primary,
        .primary_node_id = "primary-a",
    }, .{
        .bearer_token = "secret-token",
    });
    defer server.deinit();
    var recorder = RecordingExecutor.init(alloc, server.executor());
    defer recorder.deinit();

    try std.testing.expectError(error.HaAdminUnauthorized, runRemoteArgvWithOptions(alloc, std.testing.io, "http://ha-admin.test", &.{
        "status",
        "primary",
    }, recorder.executor(), .{}));
    try expectTypedRoute(&recorder, .GET, admin_api.routes.ha_primary_status);
    try std.testing.expect(recorder.last_authorization == null);

    try runRemoteArgvWithOptions(alloc, std.testing.io, "http://ha-admin.test", &.{
        "status",
        "primary",
    }, recorder.executor(), .{
        .bearer_token = "secret-token",
    });

    try expectTypedRoute(&recorder, .GET, admin_api.routes.ha_primary_status);
    try std.testing.expectEqualStrings("Bearer secret-token", recorder.last_authorization.?);
}

test "standby cmd remote direct promotion uses typed admin route" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "remote-direct-promote");
    defer paths.deinit(alloc);

    var standby = try ha.standby.Standby.open(alloc, paths.standby_log.ptr, paths.standby_progress.ptr, testIdentity(), .{});
    defer standby.close();
    var fence_store = try ha.fencing.Store.open(alloc, paths.fence_wal.ptr, .{});
    defer fence_store.close();

    var server = ha.http_admin.Server.init(alloc, .{
        .standby = &standby,
        .standby_node_id = "standby-a",
        .fence_store = &fence_store,
    });
    defer server.deinit();
    var recorder = RecordingExecutor.init(alloc, server.executor());
    defer recorder.deinit();

    try runRemoteArgv(alloc, std.testing.io, "http://ha-admin.test", &.{
        "promote",
        "--cluster-id",
        "10",
        "--shard-id",
        "20",
        "--table-id",
        "30",
        "--timeline-id",
        "1",
        "--epoch",
        "2",
        "--old-primary-id",
        "primary-a",
        "--promoted-node-id",
        "standby-a",
        "--new-timeline-id",
        "2",
        "--new-epoch",
        "3",
        "--generation",
        "1",
        "--required-lsn",
        "1",
        "--observed-lsn",
        "0",
        "--force",
        "--reason",
        "operator-approved",
    }, recorder.executor());

    try expectTypedRoute(&recorder, .POST, admin_api.routes.ha_promotion);
}

test "standby cmd remote rejects legacy command fallback for production admin operations" {
    const alloc = std.testing.allocator;
    var executor = RejectingExecutor{};

    try std.testing.expectError(error.HaRemoteTypedAdminRequired, runRemoteArgv(alloc, std.testing.io, "http://ha-admin.test", &.{
        "operator",
        "plan",
        "--standby",
        "standby-a",
    }, executor.executor()));
    try std.testing.expect(!executor.called);

    try std.testing.expectError(error.HaRemoteTypedAdminRequired, runRemoteArgv(alloc, std.testing.io, "http://ha-admin.test", &.{
        "--prometheus",
        "slot",
        "create",
        "standby-a",
    }, executor.executor()));
    try std.testing.expect(!executor.called);
}

test "standby cmd remote keeps command endpoint for replication compatibility operations" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "remote-compat-command");
    defer paths.deinit(alloc);

    var primary = try ha.primary.Primary.open(alloc, paths.primary_log.ptr, paths.primary_slots.ptr, testIdentity(), .{});
    defer primary.close();

    var server = ha.http_admin.Server.init(alloc, .{
        .primary = &primary,
    });
    defer server.deinit();
    var recorder = RecordingExecutor.init(alloc, server.executor());
    defer recorder.deinit();

    try runRemoteArgv(alloc, std.testing.io, "http://ha-admin.test", &.{
        "identify",
    }, recorder.executor());

    try std.testing.expectEqual(http_common.Method.POST, recorder.last_method.?);
    try expectContains(recorder.last_uri.?, ha.http_admin.Routes.command);
}

test "standby cmd renders typed JSON responses as dotted table fields" {
    const alloc = std.testing.allocator;
    const table = try renderJsonTableAlloc(alloc,
        \\{"schema_version":1,"slot":{"slot_name":"standby-a","active":true,"restart_lsn":4},"empty":[]}
    );
    defer alloc.free(table);

    try expectContains(table, "schema_version=1\n");
    try expectContains(table, "slot.slot_name=standby-a\n");
    try expectContains(table, "slot.active=true\n");
    try expectContains(table, "slot.restart_lsn=4\n");
    try expectContains(table, "empty=[]\n");
}

test "standby cmd keeps promotion identity flags in admin command" {
    const alloc = std.testing.allocator;
    var parsed = try parseLocalArgs(alloc, &.{
        "--fence-wal", "/tmp/fence.wal",
        "promote",     "--cluster-id",
        "10",          "--shard-id",
        "20",          "--table-id",
        "30",
    });
    defer parsed.deinit(alloc);

    try std.testing.expectEqualStrings("/tmp/fence.wal", parsed.options.fence_wal.?);
    try std.testing.expectEqualStrings("promote", parsed.command_args[0]);
    try std.testing.expectEqualStrings("--cluster-id", parsed.command_args[1]);
}

test "standby cmd streams local primary WAL into durable standby state" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "stream-command");
    defer paths.deinit(alloc);

    try runArgv(alloc, std.testing.io, &.{
        "--primary-log",    paths.primary_log,
        "--primary-slots",  paths.primary_slots,
        "--ha-cluster-id",  "10",
        "--ha-shard-id",    "20",
        "--ha-table-id",    "30",
        "--ha-timeline-id", "1",
        "--ha-epoch",       "2",
        "--",               "--table",
        "slot",             "create",
        "standby-cli",      "--initial-lsn",
        "0",
    });

    try runArgv(alloc, std.testing.io, &.{
        "--primary-log",    paths.primary_log,
        "--primary-slots",  paths.primary_slots,
        "--ha-cluster-id",  "10",
        "--ha-shard-id",    "20",
        "--ha-table-id",    "30",
        "--ha-timeline-id", "1",
        "--ha-epoch",       "2",
        "--",               "--table",
        "commit",           "append",
        "--payload",        "one",
        "--sync-mode",      "async",
    });

    try runArgv(alloc, std.testing.io, &.{
        "--primary-log",      paths.primary_log,
        "--primary-slots",    paths.primary_slots,
        "--standby-log",      paths.standby_log,
        "--standby-progress", paths.standby_progress,
        "--ha-cluster-id",    "10",
        "--ha-shard-id",      "20",
        "--ha-table-id",      "30",
        "--ha-timeline-id",   "1",
        "--ha-epoch",         "2",
        "--",                 "--table",
        "stream",             "once",
        "--slot",             "standby-cli",
    });

    {
        var primary = try ha.primary.Primary.open(alloc, paths.primary_log.ptr, paths.primary_slots.ptr, testIdentity(), .{});
        defer primary.close();
        const slot = primary.slot("standby-cli") orelse return error.TestExpectedEqual;
        try std.testing.expectEqual(@as(u64, 1), slot.timeline_id);
        try std.testing.expectEqual(@as(u64, 1), slot.restart_lsn);
        try std.testing.expectEqual(@as(u64, 1), slot.received_lsn);
        try std.testing.expectEqual(@as(u64, 1), slot.applied_lsn);
        try std.testing.expect(slot.active);
    }

    {
        var standby = try ha.standby.Standby.open(alloc, paths.standby_log.ptr, paths.standby_progress.ptr, testIdentity(), .{});
        defer standby.close();
        try std.testing.expectEqual(@as(u64, 1), standby.currentProgress().received_lsn);
        try std.testing.expectEqual(@as(u64, 1), standby.currentProgress().applied_lsn);
        try std.testing.expectEqual(@as(u64, 1), standby.currentProgress().safe_read_lsn);
    }
}

test "standby cmd accepts unprefixed target and identity flag spellings" {
    const alloc = std.testing.allocator;

    var remote = try parseLocalArgs(alloc, &.{
        "--admin-url",       "http://127.0.0.1:8081",
        "--admin-token-env", "ANTFLY_HA_ADMIN_TOKEN",
        "status",            "primary",
    });
    defer remote.deinit(alloc);
    try std.testing.expectEqualStrings("http://127.0.0.1:8081", remote.options.remote_url.?);
    try std.testing.expectEqualStrings("ANTFLY_HA_ADMIN_TOKEN", remote.options.remote_token_env.?);
    try std.testing.expectEqual(@as(usize, 2), remote.command_args.len);
    try std.testing.expectEqualStrings("status", remote.command_args[0]);

    var local = try parseLocalArgs(alloc, &.{
        "--primary-log",   "/var/lib/antfly/standby/primary.wal",
        "--primary-slots", "/var/lib/antfly/standby/slots",
        "--cluster-id",    "10",
        "--shard-id",      "20",
        "--table-id",      "30",
        "--timeline-id",   "1",
        "--epoch",         "2",
        "slot",            "list",
    });
    defer local.deinit(alloc);
    const identity = try local.options.primaryIdentity();
    try std.testing.expectEqual(@as(u64, 10), identity.cluster_id);
    try std.testing.expectEqual(@as(u64, 20), identity.shard_id);
    try std.testing.expectEqual(@as(u64, 30), identity.table_id);
    try std.testing.expectEqual(@as(u64, 1), identity.timeline_id);
    try std.testing.expectEqual(@as(u64, 2), identity.epoch);
    try std.testing.expectEqual(@as(usize, 2), local.command_args.len);
    try std.testing.expectEqualStrings("slot", local.command_args[0]);
    try std.testing.expectEqualStrings("list", local.command_args[1]);
}

test "standby cmd keeps admin command flags after the verb regardless of separator" {
    const alloc = std.testing.allocator;

    var with_separator = try parseLocalArgs(alloc, &.{ "--fence-wal", "/tmp/fence.wal", "--", "fence", "acquire", "--epoch", "3" });
    defer with_separator.deinit(alloc);
    var without_separator = try parseLocalArgs(alloc, &.{ "--fence-wal", "/tmp/fence.wal", "fence", "acquire", "--epoch", "3" });
    defer without_separator.deinit(alloc);

    for ([_]ParsedArgs{ with_separator, without_separator }) |parsed| {
        try std.testing.expectEqualStrings("/tmp/fence.wal", parsed.options.fence_wal.?);
        try std.testing.expect(parsed.options.identity.epoch == null);
        try std.testing.expectEqual(@as(usize, 4), parsed.command_args.len);
        try std.testing.expectEqualStrings("fence", parsed.command_args[0]);
        try std.testing.expectEqualStrings("--epoch", parsed.command_args[2]);
    }
}

test "standby cmd rejects unprefixed raw token flags" {
    const alloc = std.testing.allocator;

    try std.testing.expectError(error.HAAdminRawTokenFlagUnsupported, parseLocalArgs(alloc, &.{
        "--admin-url",   "http://127.0.0.1:8081",
        "--admin-token", "secret-token",
        "status",        "primary",
    }));
    try std.testing.expectError(error.HAAdminRawTokenFlagUnsupported, parseLocalArgs(alloc, &.{
        "--admin-url",        "http://127.0.0.1:8081",
        "--admin-token-file", "/run/secrets/ha-token",
        "status",             "primary",
    }));
}

test "standby cmd parses and validates the data dir target" {
    const alloc = std.testing.allocator;

    var parsed = try parseLocalArgs(alloc, &.{ "--data-dir", "/var/lib/antfly", "status", "primary" });
    defer parsed.deinit(alloc);
    try std.testing.expectEqualStrings("/var/lib/antfly", parsed.options.data_dir.?);
    try std.testing.expect(!parsed.options.hasLocalHandles());

    try std.testing.expectError(error.HADataDirInvalid, parseLocalArgs(alloc, &.{ "--data-dir", "relative/dir", "status", "primary" }));
    try std.testing.expectError(error.HADataDirInvalid, parseLocalArgs(alloc, &.{ "--data-dir", "/var/lib/antfly/../antfly", "status", "primary" }));
    try std.testing.expectError(error.HADataDirMissing, parseLocalArgs(alloc, &.{ "--data-dir", " ", "status", "primary" }));
}

const TestEnv = struct {
    url: ?[]const u8 = null,
    token: ?[]const u8 = null,
    legacy_url: ?[]const u8 = null,
    legacy_token: ?[]const u8 = null,

    fn lookup(self: *const TestEnv) EnvLookup {
        return .{ .ctx = @ptrCast(self), .getFn = get };
    }

    fn get(ctx: ?*const anyopaque, name: [:0]const u8) ?[]const u8 {
        const self: *const TestEnv = @ptrCast(@alignCast(ctx.?));
        if (std.mem.eql(u8, name, default_admin_url_env)) return self.url;
        if (std.mem.eql(u8, name, default_admin_token_env)) return self.token;
        if (std.mem.eql(u8, name, legacy_admin_url_env)) return self.legacy_url;
        if (std.mem.eql(u8, name, legacy_admin_token_env)) return self.legacy_token;
        return null;
    }
};

test "standby cmd defaults admin url and token env from the environment" {
    const env = TestEnv{ .url = "http://127.0.0.1:8081", .token = "secret" };

    var bare = LocalOptions{};
    try applyEnvironmentDefaults(&bare, env.lookup());
    try std.testing.expectEqualStrings("http://127.0.0.1:8081", bare.remote_url.?);
    try std.testing.expectEqualStrings(default_admin_token_env, bare.remote_token_env.?);

    var explicit_env_name = LocalOptions{ .remote_url = "http://127.0.0.1:9000", .remote_token_env = "OTHER_TOKEN" };
    try applyEnvironmentDefaults(&explicit_env_name, env.lookup());
    try std.testing.expectEqualStrings("http://127.0.0.1:9000", explicit_env_name.remote_url.?);
    try std.testing.expectEqualStrings("OTHER_TOKEN", explicit_env_name.remote_token_env.?);

    var local = LocalOptions{ .primary_log = "/var/lib/antfly/standby/primary.wal" };
    try applyEnvironmentDefaults(&local, env.lookup());
    try std.testing.expect(local.remote_url == null);
    try std.testing.expect(local.remote_token_env == null);

    var data_dir = LocalOptions{ .data_dir = "/var/lib/antfly" };
    try applyEnvironmentDefaults(&data_dir, env.lookup());
    try std.testing.expect(data_dir.remote_url == null);

    const no_token = TestEnv{ .url = "http://127.0.0.1:8081", .token = " " };
    var url_only = LocalOptions{};
    try applyEnvironmentDefaults(&url_only, no_token.lookup());
    try std.testing.expectEqualStrings("http://127.0.0.1:8081", url_only.remote_url.?);
    try std.testing.expect(url_only.remote_token_env == null);

    const bad_url = TestEnv{ .url = "not-a-url" };
    var invalid = LocalOptions{};
    try std.testing.expectError(error.HAAdminURLInvalid, applyEnvironmentDefaults(&invalid, bad_url.lookup()));

    // The deprecated ANTFLY_HA_* names still work when the new names are unset.
    const legacy_only = TestEnv{ .legacy_url = "http://127.0.0.1:9090", .legacy_token = "legacy-secret" };
    var legacy = LocalOptions{};
    try applyEnvironmentDefaults(&legacy, legacy_only.lookup());
    try std.testing.expectEqualStrings("http://127.0.0.1:9090", legacy.remote_url.?);
    try std.testing.expectEqualStrings(legacy_admin_token_env, legacy.remote_token_env.?);

    // The new ANTFLY_STANDBY_* names win when both spellings are present.
    const both = TestEnv{
        .url = "http://127.0.0.1:8081",
        .token = "secret",
        .legacy_url = "http://127.0.0.1:9090",
        .legacy_token = "legacy-secret",
    };
    var preferred = LocalOptions{};
    try applyEnvironmentDefaults(&preferred, both.lookup());
    try std.testing.expectEqualStrings("http://127.0.0.1:8081", preferred.remote_url.?);
    try std.testing.expectEqualStrings(default_admin_token_env, preferred.remote_token_env.?);
}

test "standby cmd applies the config file ha section as a target" {
    const alloc = std.testing.allocator;
    const io = std.testing.io;
    const nonce = @atomicRmw(u64, &test_path_counter, .Add, 1, .seq_cst);
    const remote_path = try allocPrintPath(alloc, "config", "remote.json", nonce);
    defer alloc.free(remote_path);
    const local_path = try allocPrintPath(alloc, "config", "local.json", nonce);
    defer alloc.free(local_path);
    defer std.Io.Dir.cwd().deleteFile(io, remote_path) catch {};
    defer std.Io.Dir.cwd().deleteFile(io, local_path) catch {};
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = remote_path, .data =
        \\{"ha":{"admin":{"url":"http://127.0.0.1:8081","token_env":"ANTFLY_HA_ADMIN_TOKEN"},
        \\ "primary":{"log":"/var/lib/antfly/standby/primary.wal","slots":"/var/lib/antfly/standby/slots"}}}
    });
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = local_path, .data =
        \\{"ha":{"identity":{"cluster_id":10,"shard_id":20,"table_id":30,"timeline_id":1,"epoch":2},
        \\ "primary":{"log":"/var/lib/antfly/standby/primary.wal","slots":"/var/lib/antfly/standby/slots","node_id":"primary-a"},
        \\ "fence_wal":"/var/lib/antfly/standby/fence.wal"}}
    });

    // admin.url wins over local paths when nothing else picked a target.
    {
        var options = LocalOptions{ .config_path = remote_path };
        var cfg = (try applyConfigDefaults(alloc, io, &options)).?;
        defer cfg.deinit();
        try std.testing.expectEqualStrings("http://127.0.0.1:8081", options.remote_url.?);
        try std.testing.expectEqualStrings("ANTFLY_HA_ADMIN_TOKEN", options.remote_token_env.?);
        try std.testing.expect(!options.hasLocalHandles());
    }
    // Without admin.url the section's paths and identity become local handles.
    {
        var options = LocalOptions{ .config_path = local_path };
        var cfg = (try applyConfigDefaults(alloc, io, &options)).?;
        defer cfg.deinit();
        try std.testing.expect(options.remote_url == null);
        try std.testing.expectEqualStrings("/var/lib/antfly/standby/primary.wal", options.primary_log.?);
        try std.testing.expectEqualStrings("primary-a", options.primary_node_id.?);
        try std.testing.expectEqualStrings("/var/lib/antfly/standby/fence.wal", options.fence_wal.?);
        const identity = try options.primaryIdentity();
        try std.testing.expectEqual(@as(u64, 10), identity.cluster_id);
        try std.testing.expectEqual(@as(u64, 20), identity.shard_id);
        try std.testing.expectEqual(@as(u64, 2), identity.epoch);
    }
    // An explicit target keeps the config out of the way, except for the token env name.
    {
        var options = LocalOptions{ .config_path = remote_path, .remote_url = "http://10.0.0.5:8081" };
        var cfg = (try applyConfigDefaults(alloc, io, &options)).?;
        defer cfg.deinit();
        try std.testing.expectEqualStrings("http://10.0.0.5:8081", options.remote_url.?);
        try std.testing.expectEqualStrings("ANTFLY_HA_ADMIN_TOKEN", options.remote_token_env.?);
    }
    {
        var options = LocalOptions{ .config_path = remote_path, .data_dir = "/var/lib/antfly" };
        var cfg = (try applyConfigDefaults(alloc, io, &options)).?;
        defer cfg.deinit();
        try std.testing.expect(options.remote_url == null);
        try std.testing.expect(!options.hasLocalHandles());
    }
    // Explicit identity flags win over the section.
    {
        var options = LocalOptions{ .config_path = local_path, .identity = .{ .epoch = 7 } };
        var cfg = (try applyConfigDefaults(alloc, io, &options)).?;
        defer cfg.deinit();
        try std.testing.expectEqual(@as(u64, 7), options.identity.epoch.?);
        try std.testing.expectEqual(@as(u64, 10), options.identity.cluster_id.?);
    }

    try std.testing.expectError(error.HAConfigPathMissing, parseLocalArgs(alloc, &.{ "--config", " ", "status", "primary" }));
    try std.testing.expectError(error.HAConfigPathInvalid, parseLocalArgs(alloc, &.{ "--config", " cfg.json", "status", "primary" }));
}

test "standby cmd derives local handles and identity from the data dir" {
    const alloc = std.testing.allocator;
    const io = std.testing.io;
    const nonce = @atomicRmw(u64, &test_path_counter, .Add, 1, .seq_cst);
    const data_dir = try allocPrintPath(alloc, "data-dir", "root", nonce);
    defer alloc.free(data_dir);
    std.Io.Dir.cwd().deleteTree(io, data_dir) catch {};
    defer std.Io.Dir.cwd().deleteTree(io, data_dir) catch {};
    const ha_dir = try std.fmt.allocPrint(alloc, "{s}/ha", .{data_dir});
    defer alloc.free(ha_dir);
    try std.Io.Dir.cwd().createDirPath(io, ha_dir);

    // An empty data dir has no HA state to open.
    {
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        var empty = LocalOptions{ .data_dir = data_dir };
        try std.testing.expectError(error.HADataDirNoHAState, applyDataDir(arena.allocator(), io, &empty));
    }

    // Provision a primary the explicit way, with one durable record so the
    // identity is recoverable from the log.
    const primary_log = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ data_dir, data_dir_layout.primary_log });
    defer alloc.free(primary_log);
    const primary_slots = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ data_dir, data_dir_layout.primary_slots });
    defer alloc.free(primary_slots);
    try runArgv(alloc, io, &.{
        "--primary-log",   primary_log,
        "--primary-slots", primary_slots,
        "--cluster-id",    "10",
        "--shard-id",      "20",
        "--table-id",      "30",
        "--timeline-id",   "1",
        "--epoch",         "2",
        "--table",         "commit",
        "append",          "--payload",
        "one",             "--sync-mode",
        "async",
    });

    {
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        var options = LocalOptions{ .data_dir = data_dir };
        try applyDataDir(arena.allocator(), io, &options);
        try std.testing.expectEqualStrings(primary_log, options.primary_log.?);
        try std.testing.expectEqualStrings(primary_slots, options.primary_slots.?);
        try std.testing.expect(options.standby_log == null);
        try std.testing.expect(options.fence_wal == null);
        const identity = try options.primaryIdentity();
        try std.testing.expectEqual(@as(u64, 10), identity.cluster_id);
        try std.testing.expectEqual(@as(u64, 20), identity.shard_id);
        try std.testing.expectEqual(@as(u64, 30), identity.table_id);
        try std.testing.expectEqual(@as(u64, 1), identity.timeline_id);
        try std.testing.expectEqual(@as(u64, 2), identity.epoch);
    }

    // Explicit identity flags win over the derived identity.
    {
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        var options = LocalOptions{ .data_dir = data_dir, .identity = .{ .cluster_id = 10, .timeline_id = 1, .epoch = 7 } };
        try applyDataDir(arena.allocator(), io, &options);
        try std.testing.expectEqual(@as(u64, 7), options.identity.epoch.?);
        try std.testing.expectEqual(@as(u64, 20), options.identity.shard_id.?);
    }

    // A legacy `ha/` tree (pre-0.3) is used when the canonical `standby/`
    // tree is absent.
    {
        const legacy_dir = try allocPrintPath(alloc, "data-dir", "legacy", nonce);
        defer alloc.free(legacy_dir);
        std.Io.Dir.cwd().deleteTree(io, legacy_dir) catch {};
        defer std.Io.Dir.cwd().deleteTree(io, legacy_dir) catch {};
        const legacy_primary_log = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ legacy_dir, legacy_data_dir_layout.primary_log });
        defer alloc.free(legacy_primary_log);
        const legacy_primary_slots = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ legacy_dir, legacy_data_dir_layout.primary_slots });
        defer alloc.free(legacy_primary_slots);
        try runArgv(alloc, io, &.{
            "--primary-log",   legacy_primary_log,
            "--primary-slots", legacy_primary_slots,
            "--cluster-id",    "11",
            "--shard-id",      "21",
            "--table-id",      "31",
            "--timeline-id",   "1",
            "--epoch",         "2",
            "--table",         "commit",
            "append",          "--payload",
            "one",             "--sync-mode",
            "async",
        });

        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        var options = LocalOptions{ .data_dir = legacy_dir };
        try applyDataDir(arena.allocator(), io, &options);
        try std.testing.expectEqualStrings(legacy_primary_log, options.primary_log.?);
        try std.testing.expectEqualStrings(legacy_primary_slots, options.primary_slots.?);
        const identity = try options.primaryIdentity();
        try std.testing.expectEqual(@as(u64, 11), identity.cluster_id);
    }

    // When both the canonical `standby/` tree and a legacy `ha/` tree exist,
    // the canonical tree wins and no error is raised.
    {
        const both_dir = try allocPrintPath(alloc, "data-dir", "both", nonce);
        defer alloc.free(both_dir);
        std.Io.Dir.cwd().deleteTree(io, both_dir) catch {};
        defer std.Io.Dir.cwd().deleteTree(io, both_dir) catch {};
        const canonical_primary_log = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ both_dir, data_dir_layout.primary_log });
        defer alloc.free(canonical_primary_log);
        const canonical_primary_slots = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ both_dir, data_dir_layout.primary_slots });
        defer alloc.free(canonical_primary_slots);
        try runArgv(alloc, io, &.{
            "--primary-log",   canonical_primary_log,
            "--primary-slots", canonical_primary_slots,
            "--cluster-id",    "12",
            "--shard-id",      "22",
            "--table-id",      "32",
            "--timeline-id",   "1",
            "--epoch",         "2",
            "--table",         "commit",
            "append",          "--payload",
            "one",             "--sync-mode",
            "async",
        });
        const legacy_primary_log = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ both_dir, legacy_data_dir_layout.primary_log });
        defer alloc.free(legacy_primary_log);
        const legacy_primary_slots = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ both_dir, legacy_data_dir_layout.primary_slots });
        defer alloc.free(legacy_primary_slots);
        try runArgv(alloc, io, &.{
            "--primary-log",   legacy_primary_log,
            "--primary-slots", legacy_primary_slots,
            "--cluster-id",    "13",
            "--shard-id",      "23",
            "--table-id",      "33",
            "--timeline-id",   "1",
            "--epoch",         "2",
            "--table",         "commit",
            "append",          "--payload",
            "one",             "--sync-mode",
            "async",
        });

        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        var options = LocalOptions{ .data_dir = both_dir };
        try applyDataDir(arena.allocator(), io, &options);
        try std.testing.expectEqualStrings(canonical_primary_log, options.primary_log.?);
        try std.testing.expectEqualStrings(canonical_primary_slots, options.primary_slots.?);
        const identity = try options.primaryIdentity();
        try std.testing.expectEqual(@as(u64, 12), identity.cluster_id);
    }

    // The whole command works end to end with just the data dir.
    {
        var captured: std.Io.Writer.Allocating = .init(alloc);
        defer captured.deinit();
        test_output_sink = &captured.writer;
        defer test_output_sink = null;
        try runArgv(alloc, io, &.{ "--data-dir", data_dir, "--table", "status", "primary" });
        try std.testing.expect(std.mem.indexOf(u8, captured.written(), "cluster_id") != null);
    }

    // A remote target and a data dir are mutually exclusive.
    {
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        var options = LocalOptions{ .data_dir = data_dir, .remote_url = "http://127.0.0.1:8081" };
        try std.testing.expectError(error.HaRemoteCannotUseLocalHandles, applyDataDir(arena.allocator(), io, &options));
    }
}

test "standby cmd parses switchover options" {
    const alloc = std.testing.allocator;
    var options = try parseSwitchoverArgs(alloc, &.{
        "--to",           "http://standby-a:8080",
        "--follower",     "standby-b=http://standby-b:8080",
        "--follower",     "standby-c=http://standby-c:8080",
        "--generation",   "7",
        "--max-lag-lsn",  "10",
        "--wait-seconds", "5",
        "--reason",       "maintenance",
        "--json",
    });
    defer options.deinit(alloc);
    try std.testing.expectEqualStrings("http://standby-a:8080", options.to);
    try std.testing.expectEqualStrings("http://standby-a:8080", options.newUpstreamUrl());
    try std.testing.expectEqual(@as(usize, 2), options.followers.len);
    try std.testing.expectEqualStrings("standby-c", options.followers[1].slot_name);
    try std.testing.expectEqualStrings("http://standby-c:8080", options.followers[1].url);
    try std.testing.expectEqual(@as(u64, 7), options.generation.?);
    try std.testing.expectEqual(@as(u64, 10), options.max_lag_lsn.?);
    try std.testing.expectEqual(@as(u64, 5), options.wait_seconds);
    try std.testing.expectEqualStrings("maintenance", options.reason);
    try std.testing.expectEqual(ha.admin_cli.OutputFormat.json, options.output);

    try std.testing.expectError(error.SwitchoverTargetMissing, parseSwitchoverArgs(alloc, &.{"--dry-run"}));
    try std.testing.expectError(error.SwitchoverFollowerInvalid, parseSwitchoverArgs(alloc, &.{ "--to", "http://s:1", "--follower", "http://no-slot:8080" }));
    try std.testing.expectError(error.SwitchoverGenerationInvalid, parseSwitchoverArgs(alloc, &.{ "--to", "http://s:1", "--generation", "0" }));
    try std.testing.expectError(error.UnknownSwitchoverOption, parseSwitchoverArgs(alloc, &.{ "--to", "http://s:1", "--force" }));
    try std.testing.expectError(error.HAAdminURLInvalid, parseSwitchoverArgs(alloc, &.{ "--to", "standby-a:8080" }));
}

test "standby cmd switchover requires a remote target" {
    const alloc = std.testing.allocator;
    try std.testing.expectError(error.SwitchoverRequiresAdminApi, runArgv(alloc, std.testing.io, &.{
        "--fence-wal", "/tmp/fence.wal", "switchover", "--to", "http://standby:8080",
    }));
}

const HostRoutingExecutor = struct {
    routes: []const Route,

    const Route = struct {
        prefix: []const u8,
        executor: http_common.RequestExecutor,
    };

    fn executor(self: *HostRoutingExecutor) http_common.RequestExecutor {
        return .{ .ptr = self, .vtable = &.{ .execute = execute } };
    }

    fn execute(ptr: *anyopaque, alloc: std.mem.Allocator, req: http_common.HttpRequest) !http_common.HttpResponse {
        const self: *HostRoutingExecutor = @ptrCast(@alignCast(ptr));
        for (self.routes) |route| {
            if (std.mem.startsWith(u8, req.uri, route.prefix)) return try route.executor.execute(alloc, req);
        }
        return error.TestUnexpectedHost;
    }
};

const FakeFollowerHook = struct {
    upstream_url: ?[]u8 = null,
    slot_name: ?[]u8 = null,
    alloc: std.mem.Allocator,

    fn hook(self: *FakeFollowerHook) ha.http_admin.Server.StandbyUpstreamHook {
        return .{ .ptr = self, .run_fn = FakeFollowerHook.run };
    }

    fn deinit(self: *FakeFollowerHook) void {
        if (self.upstream_url) |url| self.alloc.free(url);
        if (self.slot_name) |slot| self.alloc.free(slot);
    }

    fn run(ptr: *anyopaque, _: std.mem.Allocator, upstream_url: []const u8, slot_name: []const u8, expected: ha.standby.Identity) anyerror!ha.http_admin.Server.StandbyUpstreamResult {
        const self: *FakeFollowerHook = @ptrCast(@alignCast(ptr));
        self.upstream_url = try self.alloc.dupe(u8, upstream_url);
        self.slot_name = try self.alloc.dupe(u8, slot_name);
        return .{ .identity = expected, .previous = null, .changed = true };
    }
};

test "standby cmd switchover fences promotes rejoins and repoints followers" {
    const alloc = std.testing.allocator;
    const io = std.testing.io;
    const primary_paths = try testPaths(alloc, "switchover-primary");
    defer primary_paths.deinit(alloc);
    const standby_paths = try testPaths(alloc, "switchover-standby");
    defer standby_paths.deinit(alloc);
    const follower_paths = try testPaths(alloc, "switchover-follower");
    defer follower_paths.deinit(alloc);

    // Seed a primary with one durable record, a slot for the standby, and
    // stream it so the standby is exactly caught up.
    try runArgv(alloc, io, &.{
        "--primary-log",   primary_paths.primary_log,
        "--primary-slots", primary_paths.primary_slots,
        "--cluster-id",    "10",
        "--shard-id",      "20",
        "--table-id",      "30",
        "--timeline-id",   "1",
        "--epoch",         "2",
        "--table",         "slot",
        "create",          "standby-a",
        "--initial-lsn",   "0",
    });
    try runArgv(alloc, io, &.{
        "--primary-log",   primary_paths.primary_log,
        "--primary-slots", primary_paths.primary_slots,
        "--cluster-id",    "10",
        "--shard-id",      "20",
        "--table-id",      "30",
        "--timeline-id",   "1",
        "--epoch",         "2",
        "--table",         "commit",
        "append",          "--payload",
        "one",             "--sync-mode",
        "async",
    });
    try runArgv(alloc, io, &.{
        "--primary-log",      primary_paths.primary_log,
        "--primary-slots",    primary_paths.primary_slots,
        "--standby-log",      standby_paths.standby_log,
        "--standby-progress", standby_paths.standby_progress,
        "--cluster-id",       "10",
        "--shard-id",         "20",
        "--table-id",         "30",
        "--timeline-id",      "1",
        "--epoch",            "2",
        "--table",            "stream",
        "once",               "--slot",
        "standby-a",
    });

    var primary = try ha.primary.Primary.open(alloc, primary_paths.primary_log.ptr, primary_paths.primary_slots.ptr, testIdentity(), .{});
    defer primary.close();
    var primary_fence = try ha.fencing.Store.open(alloc, primary_paths.fence_wal.ptr, .{});
    defer primary_fence.close();
    var primary_server = ha.http_admin.Server.init(alloc, .{
        .primary = &primary,
        .primary_node_id = "primary-a",
        .fence_store = &primary_fence,
    });
    defer primary_server.deinit();

    var standby = try ha.standby.Standby.open(alloc, standby_paths.standby_log.ptr, standby_paths.standby_progress.ptr, testIdentity(), .{});
    defer standby.close();
    var standby_fence = try ha.fencing.Store.open(alloc, standby_paths.fence_wal.ptr, .{});
    defer standby_fence.close();
    var standby_server = ha.http_admin.Server.init(alloc, .{
        .standby = &standby,
        .standby_node_id = "standby-a",
        .fence_store = &standby_fence,
    });
    defer standby_server.deinit();

    var follower = try ha.standby.Standby.open(alloc, follower_paths.standby_log.ptr, follower_paths.standby_progress.ptr, testIdentity(), .{});
    defer follower.close();
    var follower_hook = FakeFollowerHook{ .alloc = alloc };
    defer follower_hook.deinit();
    var follower_server = ha.http_admin.Server.initWithOptions(alloc, .{
        .standby = &follower,
        .standby_node_id = "standby-b",
    }, .{ .standby_upstream = follower_hook.hook() });
    defer follower_server.deinit();

    var router = HostRoutingExecutor{ .routes = &.{
        .{ .prefix = "http://primary.test", .executor = primary_server.executor() },
        .{ .prefix = "http://standby.test", .executor = standby_server.executor() },
        .{ .prefix = "http://follower.test", .executor = follower_server.executor() },
    } };
    var recorder = RecordingExecutor.init(alloc, router.executor());
    defer recorder.deinit();

    var captured: std.Io.Writer.Allocating = .init(alloc);
    defer captured.deinit();
    test_output_sink = &captured.writer;
    defer test_output_sink = null;

    try runSwitchover(alloc, io, "http://primary.test", &.{
        "--to",           "http://standby.test",
        "--follower",     "standby-b=http://follower.test",
        "--wait-seconds", "5",
        "--json",
    }, recorder.executor(), .{});

    // Both nodes hold one fence with the allocated generation and the new timeline.
    const primary_receipt = primary_fence.currentBorrowed() orelse return error.TestExpectedEqual;
    const standby_receipt = standby_fence.currentBorrowed() orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u64, 1), primary_receipt.generation);
    try std.testing.expectEqual(@as(u64, 1), standby_receipt.generation);
    try std.testing.expectEqual(@as(u64, 2), standby_receipt.new_timeline_id);
    try std.testing.expectEqual(@as(u64, 3), standby_receipt.new_epoch);
    try std.testing.expectEqual(@as(u64, 1), standby_receipt.required_lsn);

    // The follower was repointed at the new primary with its own slot.
    try std.testing.expectEqualStrings("http://standby.test", follower_hook.upstream_url.?);
    try std.testing.expectEqualStrings("standby-b", follower_hook.slot_name.?);

    // Every step reported a receipt, in order.
    const out = captured.written();
    const steps = [_][]const u8{ "preflight-primary", "preflight-standby", "fence-primary", "boundary", "fence-standby", "assess", "promote", "rejoin-assess", "rejoin-rewind-pending", "standby-b" };
    var cursor: usize = 0;
    for (steps) |step| {
        const needle = try std.fmt.allocPrint(alloc, "{{\"step\":\"{s}\"", .{step});
        defer alloc.free(needle);
        const at = std.mem.indexOfPos(u8, out, cursor, needle) orelse {
            std.debug.print("missing step {s} in:\n{s}\n", .{ step, out });
            return error.TestExpectedEqual;
        };
        cursor = at + needle.len;
    }
    try std.testing.expect(std.mem.indexOf(u8, out, "\"error\"") == null);
}

test "standby cmd compiles" {
    _ = run;
    _ = runFromIterator;
    _ = runArgv;
}

const TestPaths = struct {
    primary_log: [:0]u8,
    primary_slots: [:0]u8,
    standby_log: [:0]u8,
    standby_progress: [:0]u8,
    fence_wal: [:0]u8,
    backup_root: [:0]u8,

    fn deinit(self: TestPaths, alloc: std.mem.Allocator) void {
        alloc.free(self.primary_log);
        alloc.free(self.primary_slots);
        alloc.free(self.standby_log);
        alloc.free(self.standby_progress);
        alloc.free(self.fence_wal);
        alloc.free(self.backup_root);
    }
};

fn testPaths(alloc: std.mem.Allocator, comptime name: []const u8) !TestPaths {
    const nonce = @atomicRmw(u64, &test_path_counter, .Add, 1, .seq_cst);
    const primary_log = try allocPrintPath(alloc, name, "primary-log", nonce);
    defer alloc.free(primary_log);
    const primary_slots = try allocPrintPath(alloc, name, "primary-slots", nonce);
    defer alloc.free(primary_slots);
    const standby_log = try allocPrintPath(alloc, name, "standby-log", nonce);
    defer alloc.free(standby_log);
    const standby_progress = try allocPrintPath(alloc, name, "standby-progress", nonce);
    defer alloc.free(standby_progress);
    const fence_wal = try allocPrintPath(alloc, name, "fence-wal", nonce);
    defer alloc.free(fence_wal);
    const backup_root = try allocPrintPath(alloc, name, "backup-root", nonce);
    defer alloc.free(backup_root);

    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), primary_log) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), primary_slots) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), standby_log) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), standby_progress) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), fence_wal) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), backup_root) catch {};

    return .{
        .primary_log = try alloc.dupeZ(u8, primary_log),
        .primary_slots = try alloc.dupeZ(u8, primary_slots),
        .standby_log = try alloc.dupeZ(u8, standby_log),
        .standby_progress = try alloc.dupeZ(u8, standby_progress),
        .fence_wal = try alloc.dupeZ(u8, fence_wal),
        .backup_root = try alloc.dupeZ(u8, backup_root),
    };
}

fn allocPrintPath(alloc: std.mem.Allocator, comptime name: []const u8, comptime part: []const u8, nonce: u64) ![]u8 {
    return try std.fmt.allocPrint(
        alloc,
        "/tmp/antfly-ha-cmd-" ++ name ++ "-" ++ part ++ "-{d}-{d}",
        .{ std.testing.random_seed, nonce },
    );
}

fn testIdentity() ha.standby.Identity {
    return .{
        .cluster_id = 10,
        .shard_id = 20,
        .table_id = 30,
        .timeline_id = 1,
        .epoch = 2,
    };
}

fn seedFiles() [2]ha.backup_manifest.FileEntry {
    return .{
        .{ .path = "manifest", .kind = .manifest, .size_bytes = 8, .crc32 = ha.backup_manifest.crc32("manifest") },
        .{ .path = "sst/0001", .kind = .sstable, .size_bytes = 7, .crc32 = ha.backup_manifest.crc32("sstable") },
    };
}

fn writeSeedManifestFiles(
    alloc: std.mem.Allocator,
    backup_root: []const u8,
    identity: ha.standby.Identity,
    manifest_id: []const u8,
    backup_lsn: u64,
    checkpoint_lsn: u64,
) ![]u8 {
    const files = seedFiles();
    const encoded_manifest = try ha.backup_manifest.encodeAlloc(alloc, .{
        .identity = identity,
        .manifest_id = manifest_id,
        .backup_lsn = backup_lsn,
        .checkpoint_lsn = checkpoint_lsn,
        .files = &files,
    });
    defer alloc.free(encoded_manifest);

    const manifest_path = try std.fs.path.join(alloc, &.{ backup_root, "backup.afha" });
    errdefer alloc.free(manifest_path);
    const manifest_file_path = try std.fs.path.join(alloc, &.{ backup_root, "manifest" });
    defer alloc.free(manifest_file_path);
    const sstable_path = try std.fs.path.join(alloc, &.{ backup_root, "sst/0001" });
    defer alloc.free(sstable_path);

    try writeTestFile(manifest_path, encoded_manifest);
    try writeTestFile(manifest_file_path, "manifest");
    try writeTestFile(sstable_path, "sstable");
    return manifest_path;
}

fn writeTestFile(path: []const u8, bytes: []const u8) !void {
    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
    defer io_impl.deinit();
    if (std.fs.path.dirname(path)) |parent| try std.Io.Dir.cwd().createDirPath(io_impl.io(), parent);
    try std.Io.Dir.cwd().writeFile(io_impl.io(), .{
        .sub_path = path,
        .data = bytes,
    });
}

const RecordingExecutor = struct {
    alloc: std.mem.Allocator,
    inner: http_common.RequestExecutor,
    last_method: ?http_common.Method = null,
    last_uri: ?[]u8 = null,
    last_authorization: ?[]u8 = null,

    fn init(alloc: std.mem.Allocator, inner: http_common.RequestExecutor) RecordingExecutor {
        return .{
            .alloc = alloc,
            .inner = inner,
        };
    }

    fn deinit(self: *RecordingExecutor) void {
        if (self.last_uri) |uri| self.alloc.free(uri);
        if (self.last_authorization) |authorization| self.alloc.free(authorization);
        self.* = undefined;
    }

    fn executor(self: *RecordingExecutor) http_common.RequestExecutor {
        return .{
            .ptr = self,
            .vtable = &.{
                .execute = execute,
            },
        };
    }

    fn execute(ptr: *anyopaque, alloc: std.mem.Allocator, req: http_common.HttpRequest) !http_common.HttpResponse {
        const self: *RecordingExecutor = @ptrCast(@alignCast(ptr));
        if (self.last_uri) |uri| {
            self.alloc.free(uri);
            self.last_uri = null;
        }
        if (self.last_authorization) |authorization| {
            self.alloc.free(authorization);
            self.last_authorization = null;
        }
        self.last_uri = try self.alloc.dupe(u8, req.uri);
        self.last_authorization = if (req.authorization) |authorization|
            try self.alloc.dupe(u8, authorization)
        else
            null;
        self.last_method = req.method;
        return try self.inner.execute(alloc, req);
    }
};

const RejectingExecutor = struct {
    called: bool = false,

    fn executor(self: *RejectingExecutor) http_common.RequestExecutor {
        return .{
            .ptr = self,
            .vtable = &.{
                .execute = execute,
            },
        };
    }

    fn execute(ptr: *anyopaque, alloc: std.mem.Allocator, req: http_common.HttpRequest) !http_common.HttpResponse {
        const self: *RejectingExecutor = @ptrCast(@alignCast(ptr));
        _ = alloc;
        _ = req;
        self.called = true;
        return error.TestUnexpectedRemoteRequest;
    }
};

fn expectContains(haystack: []const u8, needle: []const u8) !void {
    if (std.mem.indexOf(u8, haystack, needle) == null) {
        std.debug.print("expected to find '{s}' in '{s}'\n", .{ needle, haystack });
        return error.TestExpectedSubstring;
    }
}

fn expectTypedRoute(recorder: *const RecordingExecutor, method: http_common.Method, path: []const u8) !void {
    try std.testing.expectEqual(method, recorder.last_method.?);
    try expectContains(recorder.last_uri.?, path);
    try std.testing.expect(std.mem.indexOf(u8, recorder.last_uri.?, ha.http_admin.Routes.command) == null);
}
