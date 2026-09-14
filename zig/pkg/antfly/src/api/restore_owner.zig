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

//! Private owner controls for the shared restore job. Materialization is local
//! and disposable; every target mutation goes through the normal Raft batcher.
const std = @import("std");
const db = @import("../storage/db/mod.zig");
const staging = @import("../storage/db/restore_staging.zig");
const metadata_staging = @import("../metadata/restore_staging.zig");
const generation = @import("../storage/db/generation_lifecycle.zig");
const backups = @import("backups.zig");
const operation = @import("operation.zig");
const native_backup = @import("../storage/db/native_backup.zig");
var test_fail_after_source_stage_rename = false;
var test_fail_after_source_publication = false;

pub const Source = struct {
    location: []const u8,
    connection: []const u8 = "",
    artifact: metadata_staging.SourceArtifact,
};
pub const Request = struct {
    scope: staging.Scope,
    action: enum { begin, import_page, status, validate, publish, cancel },
    source: ?Source = null,
    max_rows: u16 = 128,
    pub fn jsonStringify(self: @This(), jw: anytype) @TypeOf(jw.*).Error!void {
        try @import("../storage/db/relational_integrity_json.zig").write(self, jw);
    }
    pub fn validate(self: Request, group_id: u64) !void {
        try self.scope.validate();
        if (self.scope.target_namespace.shard_id != group_id or self.max_rows == 0 or self.max_rows > 128) return error.InvalidRestoreStagingCommand;
        if (self.action == .import_page) {
            const source = self.source orelse return error.RestoreSourceProofMissing;
            if (source.location.len == 0 or source.location.len > 4096 or source.connection.len > 256 or
                source.artifact.target_group_id != group_id or !source.artifact.source_namespace.eql(self.scope.source_namespace) or
                !std.mem.eql(u8, &source.artifact.artifact_sha256, &self.scope.source_artifact_digest)) return error.RestoreStagingScopeChanged;
            try backups.validateArtifactRelativePath(source.artifact.snapshot_path);
        }
    }
};
pub const Response = struct {
    phase: staging.Phase,
    rows: u64,
    receipt: staging.Digest,
};
pub const Port = struct {
    ptr: *anyopaque,
    execute_fn: *const fn (*anyopaque, std.mem.Allocator, []const u8, u64, Request, operation.RequestContext) anyerror!Response,
    pub fn execute(self: Port, alloc: std.mem.Allocator, table_name: []const u8, group_id: u64, request: Request, context: operation.RequestContext) !Response {
        try context.ensureActive();
        try request.validate(group_id);
        return self.execute_fn(self.ptr, alloc, table_name, group_id, request, context);
    }
};
pub const Proposer = struct {
    ptr: *anyopaque,
    propose: *const fn (*anyopaque, db.types.BatchRequest, operation.RequestContext) anyerror!void,
};
pub const Environment = struct {
    io: std.Io,
    runtime: *db.background_runtime.BackendRuntime,
    location_options: backups.OpenOptions,
    /// Exact owner-derived path; never accepted from the request body.
    cache_path: []const u8,
    source_byte_budget: usize = @import("restore_materialization.zig").chunk_bytes,
    proposer: Proposer,
};

fn snapshotRecord(source: Source) backups.ShardSnapshot {
    return .{
        .group_id = source.artifact.source_namespace.shard_id,
        .range_id = source.artifact.source_namespace.range_id,
        .start_key = "",
        .snapshot_path = source.artifact.snapshot_path,
        .artifact_size_bytes = source.artifact.artifact_size_bytes,
        .artifact_sha256 = "", // Caller owns the hexadecimal digest bytes.
        .native_manifest_size_bytes = source.artifact.native_manifest_size_bytes,
        .native_manifest_sha256 = source.artifact.native_manifest_sha256,
    };
}

/// Build a source decoder once per scope, using the existing generation
/// staging/publish protocol. Replays open the same immutable decoder; they do
/// not download or hash the corpus for each 128-row import page.
fn ensureSource(alloc: std.mem.Allocator, env: Environment, input: Request, context: operation.RequestContext) !bool {
    const source = input.source.?;
    const marker = try std.fmt.allocPrint(alloc, "{s}/restore-source.scope", .{env.cache_path});
    defer alloc.free(marker);
    var transition = try generation.beginProcessExclusiveWithRuntimeAndIo(env.cache_path, env.runtime, env.io);
    defer transition.deinit();
    const existing = native_backup.readFileAlloc(alloc, env.io, marker, 64) catch |err| switch (err) {
        error.FileNotFound => null,
        else => return err,
    };
    if (existing) |bytes| {
        defer alloc.free(bytes);
        if (!std.mem.eql(u8, bytes, &input.scope.digest())) return error.RestoreStagingScopeChanged;
        // A previous receipt may have failed after rename. Reconcile the
        // publisher's durable marker before treating the decoder as available.
        try transition.completeDurableAdoption();
        return true;
    }
    try context.ensureActive();
    const materialization = @import("restore_materialization.zig");
    const durable_stage = try materialization.stagePath(alloc, env.cache_path, input.scope);
    defer alloc.free(durable_stage);
    const completed_work = try std.fmt.allocPrint(alloc, "{s}.materializing", .{env.cache_path});
    defer alloc.free(completed_work);
    const completed_files = try std.fmt.allocPrint(alloc, "{s}/files", .{completed_work});
    defer alloc.free(completed_files);
    if (try materialization.hasScope(alloc, env.io, durable_stage, input.scope) or try materialization.hasScope(alloc, env.io, completed_files, input.scope)) {
        try materialization.installDurableTree(alloc, env.io, completed_work, durable_stage, input.scope);
        var candidate = try transition.adoptDurableStaging(durable_stage);
        defer candidate.deinit();
        if (@import("builtin").is_test and test_fail_after_source_publication) {
            test_fail_after_source_publication = false;
            _ = try candidate.publishPrepared();
            candidate.abandonForCrashForTest();
            return error.InjectedSourcePublicationFailure;
        }
        if (try candidate.publish() != .durable) return error.RestoreSourceDurabilityUncertain;
        return true;
    }
    var options = env.location_options;
    options.connection = if (source.connection.len == 0) null else source.connection;
    options.required_capability = "restore.read";
    var location = try backups.openBackupLocationWithOptions(alloc, source.location, options);
    defer location.deinit(alloc);
    if (source.artifact.format == .portable and source.artifact.cohort_seal != null) {
        const work_path = try std.fmt.allocPrint(alloc, "{s}.materializing", .{env.cache_path});
        defer alloc.free(work_path);
        if (!try materialization.stepPortableWithBudget(alloc, env.io, &location, source.artifact, input.scope, work_path, context.cancellation, env.source_byte_budget)) return false;
        const files = try std.fmt.allocPrint(alloc, "{s}/files", .{work_path});
        defer alloc.free(files);
        var decoder = try db.DB.open(alloc, files, .{ .backend_runtime = env.runtime, .identity_namespace = input.scope.source_namespace, .prefer_existing_identity_namespace = false, .primary_backend = .{ .lsm = .{} }, .open_mode = .query_readonly, .primary_only_readonly = true, .start_index_workers = false, .start_optional_runtimes = false });
        decoder.close();
        const portable_marker = try std.fmt.allocPrint(alloc, "{s}/restore-source.scope", .{files});
        defer alloc.free(portable_marker);
        _ = try native_backup.writeFileDurable(env.io, portable_marker, &input.scope.digest());
        try @import("../common/fs_paths.zig").syncDirPortable(env.io, files);
        try context.ensureActive();
        try materialization.installDurableTree(alloc, env.io, work_path, durable_stage, input.scope);
        if (@import("builtin").is_test and test_fail_after_source_stage_rename) {
            test_fail_after_source_stage_rename = false;
            return error.InjectedSourceStageRenameFailure;
        }
        var candidate = try transition.adoptDurableStaging(durable_stage);
        defer candidate.deinit();
        if (try candidate.publish() != .durable) return error.RestoreSourceDurabilityUncertain;
        return true;
    }
    if (source.artifact.format == .native and source.artifact.native_manifest_size_bytes != 0) {
        const work_path = try std.fmt.allocPrint(alloc, "{s}.materializing", .{env.cache_path});
        defer alloc.free(work_path);
        var manifest = (try materialization.stepWithBudget(alloc, env.io, &location, source.artifact, input.scope, work_path, context.cancellation, env.source_byte_budget)) orelse return false;
        defer manifest.deinit();
        const files = try std.fmt.allocPrint(alloc, "{s}/files", .{work_path});
        defer alloc.free(files);
        if (!try materialization.hasScope(alloc, env.io, files, input.scope)) {
            var decoder = try db.DB.open(alloc, files, .{ .backend_runtime = env.runtime, .identity_namespace = input.scope.source_namespace, .prefer_existing_identity_namespace = false, .primary_backend = .{ .lsm = .{} }, .open_mode = .query_readonly, .primary_only_readonly = true, .start_index_workers = false, .start_optional_runtimes = false });
            decoder.close();
            const candidate_marker = try std.fmt.allocPrint(alloc, "{s}/restore-source.scope", .{files});
            defer alloc.free(candidate_marker);
            _ = try native_backup.writeFileDurable(env.io, candidate_marker, &input.scope.digest());
            try @import("../common/fs_paths.zig").syncDirPortable(env.io, files);
        }
        try context.ensureActive();
        try materialization.installDurableTree(alloc, env.io, work_path, durable_stage, input.scope);
        if (@import("builtin").is_test and test_fail_after_source_stage_rename) {
            test_fail_after_source_stage_rename = false;
            return error.InjectedSourceStageRenameFailure;
        }
        var candidate = try transition.adoptDurableStaging(durable_stage);
        defer candidate.deinit();
        if (try candidate.publish() != .durable) return error.RestoreSourceDurabilityUncertain;
        return true;
    }
    const artifact_path = try std.fmt.allocPrint(alloc, "{s}.artifact", .{env.cache_path});
    defer alloc.free(artifact_path);
    var artifact_transition = try generation.beginProcessExclusiveWithRuntimeAndIo(artifact_path, env.runtime, env.io);
    defer artifact_transition.deinit();
    var artifact = try artifact_transition.beginStaging();
    defer artifact.deinit();
    // A generation is always a directory. Portable artifacts are files inside
    // it, not replacements for its root (which would fail with IsDir forever).
    const portable_path = if (source.artifact.format == .portable) try std.fmt.allocPrint(alloc, "{s}/source.afb", .{artifact.path()}) else null;
    defer if (portable_path) |path| alloc.free(path);
    const source_path = portable_path orelse artifact.path();
    const digest_hex = std.fmt.bytesToHex(source.artifact.artifact_sha256, .lower);
    switch (source.artifact.format) {
        .native => try backups.copyDirectoryFromLocationUsingIoWithCancellation(alloc, env.io, &location, source.artifact.snapshot_path, artifact.path(), context.cancellation),
        .portable => try backups.copyFileFromLocationVerifiedUsingIo(alloc, env.io, &location, source.artifact.snapshot_path, source_path, source.artifact.artifact_size_bytes, &digest_hex, context.cancellation),
    }
    var shard = snapshotRecord(source);
    shard.artifact_sha256 = &digest_hex;
    // Portable copy already verifies the immutable full-file digest while
    // streaming. Native directories require the separate tree inventory.
    if (source.artifact.format == .native) try backups.verifyShardArtifactIntegrityWithCancellation(alloc, env.io, .native, source_path, &shard, context.cancellation);
    if (source.artifact.cohort_seal) |seal| {
        if (source.artifact.format == .native) _ = try @import("../storage/db/native_backup_seal.zig").exportedBytes(alloc, env.io, artifact.path(), seal);
    }
    try context.ensureActive();
    var candidate = try transition.beginStaging();
    defer candidate.deinit();
    const opts: db.OpenOptions = .{
        .backend_runtime = env.runtime,
        .identity_namespace = input.scope.source_namespace,
        .start_index_workers = false,
        .start_optional_runtimes = false,
        .staged_generation = &candidate,
    };
    switch (source.artifact.format) {
        .native => {
            var decoder = try db.DB.openVerifiedRestoreSourceWithCancellation(&candidate, alloc, artifact.path(), candidate.path(), opts, input.scope.source_namespace, context.cancellation);
            decoder.close();
        },
        .portable => {
            var decoder = try db.DB.open(alloc, candidate.path(), opts);
            defer decoder.close();
            const file = try std.Io.Dir.cwd().openFile(env.io, source_path, .{});
            defer file.close(env.io);
            const stat = try file.stat(env.io);
            if (source.artifact.cohort_seal) |seal| {
                try decoder.importCohortPortableFileIntoUnpublishedEmpty(alloc, env.io, file, stat.size, .{ .seal = seal, .namespace = input.scope.source_namespace }, context.cancellation);
            } else try decoder.importPortableFileIntoUnpublishedEmpty(alloc, env.io, file, stat.size, input.scope.source_namespace);
        },
    }
    const candidate_marker = try std.fmt.allocPrint(alloc, "{s}/restore-source.scope", .{candidate.path()});
    defer alloc.free(candidate_marker);
    _ = try native_backup.writeFileDurable(env.io, candidate_marker, &input.scope.digest());
    try candidate.seal();
    try context.ensureActive();
    const result = try candidate.publish();
    if (result != .durable) return error.RestoreSourceDurabilityUncertain;
    return true;
}

/// Reclaim corpus files through the existing durable retired-generation GC.
/// Keep only an exact scope tombstone; a lost response makes this O(1) on retry.
fn releaseSource(alloc: std.mem.Allocator, env: Environment, scope: staging.Scope) !void {
    try releaseSourceAt(alloc, env, scope);
    var work = env;
    const work_path = try std.fmt.allocPrint(alloc, "{s}.materializing", .{env.cache_path});
    defer alloc.free(work_path);
    work.cache_path = work_path;
    try releaseSourceAt(alloc, work, scope);
}

fn releaseSourceAt(alloc: std.mem.Allocator, env: Environment, scope: staging.Scope) !void {
    var transition = try generation.beginProcessExclusiveWithRuntimeAndIo(env.cache_path, env.runtime, env.io);
    defer transition.deinit();
    const marker = try std.fmt.allocPrint(alloc, "{s}/restore-source.released", .{env.cache_path});
    defer alloc.free(marker);
    const existing = native_backup.readFileAlloc(alloc, env.io, marker, 64) catch |err| switch (err) {
        error.FileNotFound => null,
        else => return err,
    };
    if (existing) |bytes| {
        defer alloc.free(bytes);
        if (!std.mem.eql(u8, bytes, &scope.digest())) return error.RestoreStagingScopeChanged;
        return;
    }
    var candidate = try transition.beginStaging();
    defer candidate.deinit();
    try @import("../common/fs_paths.zig").createDirPathPortable(env.io, candidate.path());
    const candidate_marker = try std.fmt.allocPrint(alloc, "{s}/restore-source.released", .{candidate.path()});
    defer alloc.free(candidate_marker);
    _ = try native_backup.writeFileDurable(env.io, candidate_marker, &scope.digest());
    try candidate.seal();
    if (try candidate.publish() != .durable) return error.RestoreSourceDurabilityUncertain;
}

/// Caller holds an exact resident owner lease and a ReadIndex barrier. The
/// persisted reservation has already authenticated the entire source scope.
/// Publication also requires the metadata job's committed publication proof.
pub fn executeResident(alloc: std.mem.Allocator, target: *db.DB, env: Environment, input: Request, context: operation.RequestContext) !Response {
    try context.ensureActive();
    try input.validate(input.scope.target_namespace.shard_id);
    if (input.source) |source| {
        if (!std.mem.allEqual(u8, &input.scope.source_descriptor_digest, 0) and !std.mem.eql(u8, &try source.artifact.digest(alloc), &input.scope.source_descriptor_digest)) return error.RestoreStagingScopeChanged;
    }
    var before = (try target.restoreStagingStatus(alloc)) orelse return error.RestoreStagingScopeChanged;
    defer before.deinit();
    if (!std.mem.eql(u8, &before.value.scope.digest(), &input.scope.digest())) return error.RestoreStagingScopeChanged;
    if (input.action == .status) return .{ .phase = before.value.phase, .rows = before.value.rows, .receipt = before.value.receipt() };
    const desired: ?staging.Phase = switch (input.action) {
        .validate => .validated,
        .publish => .published,
        .cancel => .canceled,
        else => null,
    };
    if (desired) |phase| if (before.value.phase == phase) {
        if (phase == .published or phase == .canceled) try releaseSource(alloc, env, input.scope);
        return .{ .phase = phase, .rows = before.value.rows, .receipt = before.value.receipt() };
    };
    if (before.value.phase == .canceled or before.value.phase == .published) return error.RestoreStagingScopeChanged;
    switch (input.action) {
        .begin => try env.proposer.propose(env.proposer.ptr, .{ .restore_staging = .{ .begin = input.scope } }, context),
        .import_page => {
            if (!try ensureSource(alloc, env, input, context)) return .{ .phase = before.value.phase, .rows = before.value.rows, .receipt = before.value.receipt() };
            var decoder = try db.DB.open(alloc, env.cache_path, .{ .backend_runtime = env.runtime, .identity_namespace = input.scope.source_namespace, .open_mode = .query_readonly, .primary_only_readonly = true, .start_index_workers = false, .start_optional_runtimes = false });
            defer decoder.close();
            var page = try target.prepareRestoreStagingPage(alloc, input.scope, &decoder, input.max_rows, context.cancellation);
            defer page.deinit();
            if (page.batch) |batch| try env.proposer.propose(env.proposer.ptr, batch, context);
        },
        .validate => {
            if (try target.prepareRestoreStagingIndexesStep(alloc, input.scope.digest())) {
                try context.ensureActive();
                try env.proposer.propose(env.proposer.ptr, .{ .restore_staging = .{ .finish = .{ .scope = input.scope.digest(), .phase = .validated } } }, context);
            }
        },
        .publish, .cancel => try env.proposer.propose(env.proposer.ptr, .{ .restore_staging = .{ .finish = .{ .scope = input.scope.digest(), .phase = desired.? } } }, context),
        .status => unreachable,
    }
    var after = (try target.restoreStagingStatus(alloc)) orelse return error.RestoreStagingScopeChanged;
    defer after.deinit();
    if (after.value.phase == .published or after.value.phase == .canceled) try releaseSource(alloc, env, input.scope);
    return .{ .phase = after.value.phase, .rows = after.value.rows, .receipt = after.value.receipt() };
}

test "restore owner verified decoder is reused across pages and terminal cleanup revokes import" {
    try testVerifiedDecoder(false);
}

test "restore owner verified decoder portable resumes and preserves vector projections" {
    try testVerifiedDecoder(true);
}

fn testVerifiedDecoder(comptime portable: bool) !void {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(std.testing.io, ".", alloc);
    defer alloc.free(root);
    const source_path = try std.fmt.allocPrint(alloc, "{s}/source", .{root});
    defer alloc.free(source_path);
    const target_path = try std.fmt.allocPrint(alloc, "{s}/target", .{root});
    defer alloc.free(target_path);
    const cache_path = try std.fmt.allocPrint(alloc, "{s}/decoder", .{root});
    defer alloc.free(cache_path);
    var runtime = try db.background_runtime.BackendRuntime.init(alloc, .{ .backend = .manual, .filesystem_io = std.testing.io });
    defer runtime.deinit();
    const source_namespace: @import("../storage/db/doc_identity.zig").Namespace = .{ .table_id = 11, .shard_id = 101, .range_id = 101 };
    const target_namespace: @import("../storage/db/doc_identity.zig").Namespace = .{ .table_id = 12, .shard_id = 102, .range_id = 102 };
    var source = try db.DB.open(alloc, source_path, .{ .backend_runtime = &runtime, .identity_namespace = source_namespace, .primary_backend = .{ .lsm = .{} }, .start_optional_runtimes = false, .start_index_workers = false });
    defer source.close();
    const row_schema =
        \\{"version":1,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"v":{"type":"integer"},"embedding":{"type":"array","items":{"type":"number"}}},"additionalProperties":false}}}}
    ;
    const dense_index: db.types.IndexConfig = .{ .name = "restore_dense", .kind = .dense_vector, .config_json = "{\"field\":\"embedding\",\"dims\":2,\"metric\":\"l2_squared\"}" };
    try source.setSchemaJson(alloc, row_schema);
    try source.addIndex(dense_index);
    try source.batch(.{ .sync_level = .full_index, .writes = &.{ .{ .key = "a", .value = "{\"v\":1,\"embedding\":[1,0]}" }, .{ .key = "b", .value = "{\"v\":2,\"embedding\":[0,1]}" }, .{ .key = "c", .value = "{\"v\":3,\"embedding\":[1,1]}" } } });
    const source_identity = try source.relationalTopologyIdentity();
    const source_fence: @import("../storage/db/relational_integrity_topology.zig").Fence = .{ .role = .backup_snapshot, .transition_id = 900, .attempt = 1, .peer_group_id = 101, .owner_group_id = 101, .admission_epoch = source_identity.next_epoch, .namespace = source_identity.namespace, .catalog_digest = source_identity.catalog_digest };
    try source.applyRelationalTopologyControl(.{ .fence = source_fence, .action = .begin }, null);
    const seal = try source.sealBackupCohort("source-pin", source_fence, .none);
    try source.applyRelationalTopologyControl(.{ .fence = source_fence, .action = .release }, null);
    const artifact_path = if (portable) try std.fmt.allocPrint(alloc, "{s}/source.afb", .{root}) else try std.fmt.allocPrint(alloc, "{s}.snapshots/source-image", .{source_path});
    defer alloc.free(artifact_path);
    if (portable) {
        const file = try std.Io.Dir.cwd().createFile(std.testing.io, artifact_path, .{});
        defer file.close(std.testing.io);
        var buffer: [64 * 1024]u8 = undefined;
        var writer = file.writer(std.testing.io, &buffer);
        try source.exportBackupCohortPortable(seal, &writer.interface, .{}, .none);
        try writer.interface.flush();
        try file.sync(std.testing.io);
    } else _ = try source.exportBackupCohort(seal, "source-image", .none);
    var integrity = try backups.artifactIntegrityAlloc(alloc, std.testing.io, if (portable) .portable else .native, artifact_path);
    defer integrity.deinit(alloc);
    var artifact_digest: [32]u8 = undefined;
    _ = try std.fmt.hexToBytes(&artifact_digest, integrity.sha256);
    const target_options: db.OpenOptions = .{ .backend_runtime = &runtime, .identity_namespace = target_namespace, .primary_backend = .{ .lsm = .{} }, .start_optional_runtimes = false, .start_index_workers = false };
    var target = try db.DB.open(alloc, target_path, target_options);
    var target_open = true;
    defer if (target_open) target.close();
    try target.setSchemaJson(alloc, row_schema);
    try target.addIndex(dense_index);
    const schema = try @import("../storage/schema.zig").serializeSchema(alloc, target.core.schema orelse .{});
    defer alloc.free(schema);
    var scope: staging.Scope = .{ .plan_id = @splat(1), .plan_digest = @splat(2), .source_artifact_digest = artifact_digest, .source_namespace = source_namespace, .target_namespace = target_namespace, .target_schema_digest = staging.digest(schema) };
    const Apply = struct {
        target: *db.DB,
        index: u64 = 0,
        fn propose(ptr: *anyopaque, request: db.types.BatchRequest, context: operation.RequestContext) !void {
            try context.ensureActive();
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.index += 1;
            var batch = request;
            batch.restore_staging_scope = scopeDigest(self.target);
            try self.target.batchRaftReplicatedApply(batch, .{ .index = self.index, .term = 1 });
        }
        fn scopeDigest(target_db: *db.DB) staging.Digest {
            var progress = (target_db.restoreStagingStatus(std.testing.allocator) catch unreachable).?;
            defer progress.deinit();
            return progress.value.scope.digest();
        }
    };
    var apply: Apply = .{ .target = &target };
    const env: Environment = .{ .io = std.testing.io, .runtime = &runtime, .location_options = .{ .filesystem_io = std.testing.io }, .cache_path = cache_path, .proposer = .{ .ptr = &apply, .propose = Apply.propose } };
    var artifact: metadata_staging.SourceArtifact = .{ .target_group_id = 102, .source_namespace = source_namespace, .format = if (portable) .portable else .native, .snapshot_path = if (portable) "source.afb" else "source.snapshots/source-image", .artifact_size_bytes = integrity.size_bytes, .artifact_sha256 = artifact_digest, .cohort_seal = seal };
    if (!portable) {
        var manifest_integrity = try backups.nativeGenerationManifestIntegrityAllocWithCancellation(alloc, std.testing.io, artifact_path, .none);
        defer manifest_integrity.deinit(alloc);
        artifact.native_manifest_size_bytes = manifest_integrity.size_bytes;
        artifact.native_manifest_sha256 = try alloc.dupe(u8, manifest_integrity.sha256);
    }
    defer if (!portable) alloc.free(artifact.native_manifest_sha256);
    scope.source_descriptor_digest = try artifact.digest(alloc);
    try target.reserveRestoreStagingScoped(alloc, scope);
    const location_uri = try std.fmt.allocPrint(alloc, "file://{s}", .{root});
    defer alloc.free(location_uri);
    const source_request: Source = .{ .location = location_uri, .artifact = artifact };
    _ = try executeResident(alloc, &target, env, .{ .scope = scope, .action = .begin }, .{});
    var bounded_env = env;
    bounded_env.source_byte_budget = 1;
    const pending = try executeResident(alloc, &target, bounded_env, .{ .scope = scope, .action = .import_page, .source = source_request, .max_rows = 1 }, .{});
    try std.testing.expectEqual(staging.Phase.importing, pending.phase);
    try std.testing.expectEqual(@as(u64, 0), pending.rows);
    // Both the source-copy SHA prefix and native owner admission survive a
    // worker/root restart without repeating the remote prefix transfer.
    target.close();
    target_open = false;
    target = try db.DB.open(alloc, target_path, target_options);
    target_open = true;
    const checkpoint_path = try std.fmt.allocPrint(alloc, "{s}.materializing/progress", .{cache_path});
    defer alloc.free(checkpoint_path);
    const checkpoint = try native_backup.readFileAlloc(alloc, std.testing.io, checkpoint_path, 4096);
    defer alloc.free(checkpoint);
    checkpoint[checkpoint.len - 1] ^= 1;
    _ = try native_backup.writeFileDurable(std.testing.io, checkpoint_path, checkpoint);
    try std.testing.expectError(error.InvalidRestoreSourceCheckpoint, executeResident(alloc, &target, env, .{ .scope = scope, .action = .import_page, .source = source_request }, .{}));
    checkpoint[checkpoint.len - 1] ^= 1;
    _ = try native_backup.writeFileDurable(std.testing.io, checkpoint_path, checkpoint);
    var canceled = std.atomic.Value(bool).init(true);
    try std.testing.expectError(error.Canceled, executeResident(alloc, &target, env, .{ .scope = scope, .action = .import_page, .source = source_request }, .{ .cancellation = .fromAtomic(&canceled) }));
    var changed_proof = source_request;
    changed_proof.artifact.artifact_sha256[0] ^= 1;
    try std.testing.expectError(error.RestoreStagingScopeChanged, executeResident(alloc, &target, env, .{ .scope = scope, .action = .import_page, .source = changed_proof }, .{}));
    test_fail_after_source_stage_rename = true;
    defer test_fail_after_source_stage_rename = false;
    for (0..200) |_| {
        const pending_source = executeResident(alloc, &target, env, .{ .scope = scope, .action = .import_page, .source = source_request, .max_rows = 1 }, .{}) catch |err| {
            try std.testing.expectEqual(error.InjectedSourceStageRenameFailure, err);
            break;
        };
        try std.testing.expectEqual(@as(u64, 0), pending_source.rows);
    } else return error.SourceMaterializationDidNotConverge;
    // Restart/retry after the directory move no longer needs the repository,
    // the verified file copy loop, or a corpus-wide metadata installation pass.
    var staged_source = source_request;
    staged_source.location = "/does-not-exist/restore-owner-test";
    test_fail_after_source_publication = true;
    defer test_fail_after_source_publication = false;
    try std.testing.expectError(error.InjectedSourcePublicationFailure, executeResident(alloc, &target, env, .{ .scope = scope, .action = .import_page, .source = staged_source, .max_rows = 1 }, .{}));
    const first = try executeResident(alloc, &target, env, .{ .scope = scope, .action = .import_page, .source = staged_source, .max_rows = 1 }, .{});
    try std.testing.expectEqual(@as(u64, 1), first.rows);
    // An unavailable repository after page one must not trigger another read.
    var unavailable = source_request;
    unavailable.location = "/does-not-exist/restore-owner-test";
    var last = first;
    for (0..5) |_| {
        last = try executeResident(alloc, &target, env, .{ .scope = scope, .action = .import_page, .source = unavailable, .max_rows = 1 }, .{});
        if (last.phase == .imported) break;
    }
    try std.testing.expectEqual(staging.Phase.imported, last.phase);
    try std.testing.expectEqual(@as(u64, 3), last.rows);
    _ = try executeResident(alloc, &target, env, .{ .scope = scope, .action = .validate }, .{});
    const published = try executeResident(alloc, &target, env, .{ .scope = scope, .action = .publish }, .{});
    try std.testing.expectEqual(staging.Phase.published, published.phase);
    const repeated = try executeResident(alloc, &target, env, .{ .scope = scope, .action = .publish }, .{});
    try std.testing.expectEqualSlices(u8, &published.receipt, &repeated.receipt);
    try std.testing.expectError(error.RestoreStagingScopeChanged, executeResident(alloc, &target, env, .{ .scope = scope, .action = .import_page, .source = source_request }, .{}));
    const row = (try target.get(alloc, "b")) orelse return error.TestUnexpectedResult;
    defer alloc.free(row);
    var parsed_row = try std.json.parseFromSlice(std.json.Value, alloc, row, .{});
    defer parsed_row.deinit();
    try std.testing.expectEqual(@as(i64, 2), parsed_row.value.object.get("v").?.integer);
    try std.testing.expectEqual(@as(usize, 2), parsed_row.value.object.get("embedding").?.array.items.len);
    try std.testing.expect(target.core.index_manager.denseIndex("restore_dense") != null);
    var nearest = try target.search(alloc, .{ .index_name = "restore_dense", .limit = 1, .include_stored = false, .query = .{ .dense_knn = .{ .vector = &.{ 0, 1 }, .k = 1 } } });
    defer nearest.deinit();
    try std.testing.expectEqual(@as(usize, 1), nearest.hits.len);
    try std.testing.expectEqualStrings("b", nearest.hits[0].id);
}
