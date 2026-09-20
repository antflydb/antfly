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
const db = @import("db/mod.zig");
const staging = @import("db/restore_staging.zig");
const metadata_staging = @import("../metadata/restore_staging.zig");
const generation = @import("db/generation_lifecycle.zig");
const backups = @import("../api/backups.zig");
const operation = @import("../api/operation.zig");
const native_backup = @import("db/native_backup.zig");
var test_fail_after_source_stage_rename = false;
var test_fail_after_source_publication = false;

pub const Source = @import("../api/restore_owner_contract.zig").Source;
pub const Request = @import("../api/restore_owner_contract.zig").Request;
pub const Response = @import("../api/restore_owner_contract.zig").Response;
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
    source_byte_budget: usize = @import("../api/restore_materialization.zig").chunk_bytes,
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
fn ensureSource(alloc: std.mem.Allocator, env: Environment, input: Request, owner_range: db.types.ByteRange, context: operation.RequestContext, source_next_offset: *u64, program: ?*const @import("db/relational_rewrite_program.zig").ProgramSet) !bool {
    const source = input.source.?;
    source_next_offset.* = if (source.peer_descriptor) |descriptor| descriptor.total_bytes else 0;
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
    const materialization = @import("../api/restore_materialization.zig");
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
    var optional_location = if (source.peer_descriptor == null) try backups.openBackupLocationWithOptions(alloc, source.location, options) else null;
    defer if (optional_location) |*location| location.deinit(alloc);
    if (source.artifact.format == .portable and (source.artifact.cohort_seal != null or source.artifact.rewrite != null)) {
        const work_path = try std.fmt.allocPrint(alloc, "{s}.materializing", .{env.cache_path});
        defer alloc.free(work_path);
        const complete = if (source.peer_descriptor != null) peer: {
            const result = try @import("../api/restore_peer_materialization.zig").step(alloc, env.io, source, input.scope, owner_range, work_path, input.source_chunk, context.cancellation, env.source_byte_budget);
            source_next_offset.* = result.next_offset;
            break :peer result.complete;
        } else try materialization.stepPortableWithBudget(alloc, env.io, &optional_location.?, source.artifact, input.scope, owner_range, work_path, context.cancellation, env.source_byte_budget);
        if (!complete) return false;
        const files = try std.fmt.allocPrint(alloc, "{s}/files", .{work_path});
        defer alloc.free(files);
        var decoder = try db.DB.open(alloc, files, .{ .backend_runtime = env.runtime, .identity_namespace = input.scope.source_namespace, .prefer_existing_identity_namespace = false, .primary_backend = .{ .lsm = .{} }, .open_mode = .query_readonly, .primary_only_readonly = true, .start_index_workers = false, .start_optional_runtimes = false });
        {
            defer decoder.close();
            if (program) |compiled| {
                var scratch = std.heap.ArenaAllocator.init(alloc);
                defer scratch.deinit();
                var read = try decoder.core.store.beginReadTxn();
                defer read.abort();
                const manifest = try @import("db/relational_rewrite_manifest.zig").read(scratch.allocator(), &read, context.cancellation);
                try compiled.requireSourceManifest(alloc, manifest, try read.get("\x00\x00__metadata__:schema_json"));
            }
        }
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
    const location = &optional_location.?;
    if (source.artifact.format == .native and source.artifact.native_manifest_size_bytes != 0) {
        const work_path = try std.fmt.allocPrint(alloc, "{s}.materializing", .{env.cache_path});
        defer alloc.free(work_path);
        var manifest = (try materialization.stepWithBudget(alloc, env.io, location, source.artifact, input.scope, work_path, context.cancellation, env.source_byte_budget)) orelse return false;
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
        .native => try backups.copyDirectoryFromLocationUsingIoWithCancellation(alloc, env.io, location, source.artifact.snapshot_path, artifact.path(), context.cancellation),
        .portable => try backups.copyFileFromLocationVerifiedUsingIo(alloc, env.io, location, source.artifact.snapshot_path, source_path, source.artifact.artifact_size_bytes, &digest_hex, context.cancellation),
    }
    var shard = snapshotRecord(source);
    shard.artifact_sha256 = &digest_hex;
    // Portable copy already verifies the immutable full-file digest while
    // streaming. Native directories require the separate tree inventory.
    if (source.artifact.format == .native) try backups.verifyShardArtifactIntegrityWithCancellation(alloc, env.io, .native, source_path, &shard, context.cancellation);
    if (source.artifact.cohort_seal) |seal| {
        if (source.artifact.format == .native) _ = try @import("db/native_backup_seal.zig").exportedBytes(alloc, env.io, artifact.path(), seal);
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
            try materialization.bindPortableDecoderRange(alloc, decoder.core.store, owner_range);
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
    // Metadata/owner receipts survive restart; physical index coverage belongs
    // to this replica. Recheck it before acknowledging recovered readiness.
    // Cancellation must remain available even when a projection is broken.
    if ((input.action == .status or input.action == .validate or input.action == .publish) and
        (before.value.phase == .validated or before.value.phase == .published))
    {
        if (!try target.prepareRestoreStagingIndexesStep(alloc, input.scope.digest())) return error.RestoreValidationPending;
    }
    if (input.action == .status) {
        const resume_offset = if (before.value.rewrite) |progress| blk: {
            if (!progress.snapshot_complete or progress.final_cut != null) break :blk 0;
            var transition = try generation.beginProcessExclusiveWithRuntimeAndIo(env.cache_path, env.runtime, env.io);
            defer transition.deinit();
            break :blk try @import("rewrite_tail_spool.zig").resumeOffset(alloc, env.io, env.cache_path, input.scope, progress.sequence);
        } else 0;
        return .{ .phase = before.value.phase, .rows = before.value.rows, .receipt = before.value.receipt(), .rewrite = before.value.rewrite, .tail_next = resume_offset };
    }
    const desired: ?staging.Phase = switch (input.action) {
        .validate => .validated,
        .publish => .published,
        .cancel => .canceled,
        else => null,
    };
    if (desired) |phase| if (before.value.phase == phase) {
        target.rewrite_program_cache.evict(env.io);
        target.rewrite_tail_cache.mutex.lockUncancelable(env.io);
        target.rewrite_tail_cache.clear();
        target.rewrite_tail_cache.mutex.unlock(env.io);
        if (phase == .published or phase == .canceled) try releaseSource(alloc, env, input.scope);
        return .{ .phase = phase, .rows = before.value.rows, .receipt = before.value.receipt() };
    };
    if (before.value.phase == .canceled or before.value.phase == .published) return error.RestoreStagingScopeChanged;
    if (input.action == .begin and before.value.phase != .reserved) {
        // The caller already obtained a ReadIndex barrier and an exact owner
        // lease. This matching durable scope proves admission, even when its
        // original reply was lost. Re-proposing begin would only append another
        // Raft entry/outbox record for every bounded snapshot page.
        if (!input.scope.target_namespace.eql(target.core.identity_namespace)) return error.RestoreStagingScopeChanged;
        return .{ .phase = before.value.phase, .rows = before.value.rows, .receipt = before.value.receipt(), .rewrite = before.value.rewrite };
    }
    var tail_next: u32 = 0;
    var source_next_offset: u64 = 0;
    switch (input.action) {
        .begin => try env.proposer.propose(env.proposer.ptr, .{ .restore_staging = .{ .begin = input.scope } }, context),
        .import_page => import: {
            if (input.rewrite_finish) |receipt| {
                try receipt.validate(input.scope.rewrite.?);
                var page = try @import("db/relational_rewrite_staging.zig").prepareFinish(target, alloc, input.scope, receipt.cut);
                defer page.deinit();
                if (page.batch) |batch| try env.proposer.propose(env.proposer.ptr, batch, context);
                target.rewrite_program_cache.evict(env.io);
                break :import;
            }
            // Compile once outside generation/frame locks; the lease protects
            // immutable programs across source certification and either page path.
            var program_lease = if (input.rewrite) |intent| try target.rewrite_program_cache.acquire(env.io, target.alloc, target.core.index_manager.resource_manager, input.scope, intent, context) else null;
            defer if (program_lease) |*lease| lease.deinit();
            const program = if (program_lease) |lease| lease.program() else null;
            try context.ensureActive();
            if (input.rewrite_tail) |chunk| {
                const progress = before.value.rewrite orelse return error.InvalidRestoreStagingCommand;
                if (!progress.snapshot_complete or progress.final_cut != null) return error.RestoreStagingInProgress;
                if (chunk.sequence <= progress.sequence) break :import;
                // Reuse the same source-generation lock/terminal GC root.
                // No target apply lock is held during bounded chunk fsync.
                var transition = try generation.beginProcessExclusiveWithRuntimeAndIo(env.cache_path, env.runtime, env.io);
                defer transition.deinit();
                const cache = &target.rewrite_tail_cache;
                try cache.mutex.lock(env.io);
                defer cache.mutex.unlock(env.io);
                const assembled = try @import("rewrite_tail_spool.zig").receive(alloc, env.io, env.cache_path, input.scope, progress.sequence, chunk, cache, target.alloc, target.core.index_manager.resource_manager);
                tail_next = assembled.next;
                if (assembled.frame) |frame| {
                    var page = try @import("db/relational_rewrite_staging.zig").prepareTailVerified(target, alloc, input.scope, frame, program.?, input.max_rows, context.cancellation);
                    defer page.deinit();
                    // The owned batch no longer needs compiled schemas. Do not
                    // pin program memory through a potentially slow proposal.
                    if (program_lease) |*lease| lease.deinit();
                    program_lease = null;
                    if (page.batch) |batch| {
                        try env.proposer.propose(env.proposer.ptr, batch, context);
                        var advanced = try staging.Progress.decode(alloc, batch.restore_staging.?.rewrite_page.next);
                        defer advanced.deinit();
                        if (advanced.value.rewrite.?.sequence == chunk.sequence) cache.clear();
                    }
                }
                break :import;
            }
            if (!try ensureSource(alloc, env, input, target.core.byteRange(), context, &source_next_offset, program)) return .{ .phase = before.value.phase, .rows = before.value.rows, .receipt = before.value.receipt(), .rewrite = before.value.rewrite, .source_next_offset = source_next_offset };
            var decoder = try db.DB.open(alloc, env.cache_path, .{ .backend_runtime = env.runtime, .identity_namespace = input.scope.source_namespace, .open_mode = .query_readonly, .primary_only_readonly = true, .start_index_workers = false, .start_optional_runtimes = false });
            defer decoder.close();
            var page = if (program) |compiled| try target.prepareRewriteStagingPage(alloc, input.scope, &decoder, input.max_rows, context.cancellation, compiled) else try target.prepareRestoreStagingPage(alloc, input.scope, &decoder, input.max_rows, context.cancellation);
            defer page.deinit();
            if (program_lease) |*lease| lease.deinit();
            program_lease = null;
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
    if (after.value.phase == .published or after.value.phase == .canceled) {
        target.rewrite_program_cache.evict(env.io);
        target.rewrite_tail_cache.mutex.lockUncancelable(env.io);
        target.rewrite_tail_cache.clear();
        target.rewrite_tail_cache.mutex.unlock(env.io);
        try releaseSource(alloc, env, input.scope);
    }
    return .{ .phase = after.value.phase, .rows = after.value.rows, .receipt = after.value.receipt(), .rewrite = after.value.rewrite, .tail_next = tail_next, .source_next_offset = source_next_offset };
}

test "restore owner verified decoder rewrite history compiles once across production tail pages" {
    const alloc = std.testing.allocator;
    const io = std.testing.io;
    const rewrite = @import("db/relational_rewrite_contract.zig");
    const ProgramSet = @import("db/relational_rewrite_program.zig").ProgramSet;
    const keys = @import("internal_keys.zig");
    var directory = std.testing.tmpDir(.{});
    defer directory.cleanup();
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const root = try directory.dir.realPathFileAlloc(io, ".", a);
    var runtime = try db.background_runtime.BackendRuntime.init(alloc, .{ .backend = .manual, .filesystem_io = io });
    defer runtime.deinit();
    const source_ns: @import("db/doc_identity_namespace.zig").Namespace = .{ .table_id = 11, .shard_id = 12, .range_id = 12 };
    const target_ns: @import("db/doc_identity_namespace.zig").Namespace = .{ .table_id = 21, .shard_id = 22, .range_id = 22 };
    var definitions: [65][]const u8 = undefined;
    for (&definitions, 1..) |*definition, version| definition.* = try std.fmt.allocPrint(a, "{{\"version\":{d},\"storage_mode\":\"relational\",\"default_type\":\"row\",\"document_schemas\":{{\"row\":{{\"schema\":{{\"type\":\"object\",\"properties\":{{\"x\":{{\"type\":\"integer\"}}}},\"additionalProperties\":false}}}}}}}}", .{version});
    var reference = try ProgramSet.init(alloc, definitions[0..64], definitions[64], .{});
    defer reference.deinit();
    const intent: rewrite.Intent = .{ .source_schemas = definitions[0..64], .target_schema = definitions[64], .program_digest = reference.identity };
    const scope: staging.Scope = .{
        .plan_id = @splat(1),
        .plan_digest = @splat(2),
        .source_artifact_digest = @splat(3),
        .source_namespace = source_ns,
        .target_namespace = target_ns,
        .target_schema_digest = reference.target_runtime_digest,
        .rewrite = .{ .program_digest = reference.identity, .retained_pin = @splat(6), .snapshot_certificate = @splat(7), .retained_epoch = 1, .retained_start = 0, .source_applied_index = 1 },
    };
    const source_path = try std.fmt.allocPrint(a, "{s}/source", .{root});
    var source = try db.DB.open(alloc, source_path, .{ .backend_runtime = &runtime, .identity_namespace = source_ns, .primary_backend = .{ .lsm = .{} }, .start_optional_runtimes = false, .start_index_workers = false });
    defer source.close();
    try source.setSchemaJson(alloc, definitions[63]);
    var rows: [256]db.types.BatchWrite = undefined;
    for (&rows, 0..) |*row, i| row.* = .{ .key = try std.fmt.allocPrint(a, "row:{d:0>8}", .{i}), .value = "{\"x\":7}" };
    try source.batchRaftReplicatedApply(.{ .timestamp_ns = 42, .writes = &rows }, .{ .term = 1, .index = 1 });
    var frame = std.ArrayList(u8).empty;
    defer frame.deinit(alloc);
    var header: [16]u8 = undefined;
    @memcpy(header[0..4], "REF3");
    std.mem.writeInt(u64, header[4..12], 1, .little);
    std.mem.writeInt(u32, header[12..16], rows.len, .little);
    try frame.appendSlice(alloc, &header);
    {
        var read = try source.core.store.beginReadTxn();
        defer read.abort();
        for (rows) |row| {
            const key = try keys.relationalRowKeyAlloc(a, row.key);
            const value = try read.get(key);
            std.mem.writeInt(u32, header[0..4], @intCast(key.len), .little);
            std.mem.writeInt(u32, header[4..8], @intCast(value.len), .little);
            std.mem.writeInt(u64, header[8..16], 42, .little);
            try frame.appendSlice(alloc, &header);
            try frame.appendSlice(alloc, key);
            try frame.appendSlice(alloc, value);
        }
    }
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(frame.items, &digest, .{});
    try frame.appendSlice(alloc, &digest);
    std.crypto.hash.sha2.Sha256.hash(frame.items, &digest, .{});
    const target_path = try std.fmt.allocPrint(a, "{s}/target", .{root});
    var target = try db.DB.open(alloc, target_path, .{ .backend_runtime = &runtime, .identity_namespace = target_ns, .primary_backend = .{ .lsm = .{} }, .start_optional_runtimes = false, .start_index_workers = false });
    defer target.close();
    try target.setSchemaJson(alloc, definitions[64]);
    try target.reserveRestoreStagingScoped(alloc, scope);
    const Apply = struct {
        target: *db.DB,
        scope: [32]u8,
        index: u64 = 0,
        lose_reply: bool = false,
        evict_on_index: u64,
        fn propose(ptr: *anyopaque, request: db.types.BatchRequest, context: operation.RequestContext) !void {
            try context.ensureActive();
            const self: *@This() = @ptrCast(@alignCast(ptr));
            var batch = request;
            batch.restore_staging_scope = self.scope;
            self.index += 1;
            if (self.index == self.evict_on_index) {
                // The page must own all batch values and release its program
                // lease before proposal. Evict before apply to prove both.
                try std.testing.expectEqual(@as(usize, 1), self.target.rewrite_program_cache.entry.?.refs.load(.acquire));
                self.target.rewrite_program_cache.evict(std.testing.io);
            }
            try self.target.batchRaftReplicatedApply(batch, .{ .term = 1, .index = self.index });
            if (self.lose_reply) {
                self.lose_reply = false;
                return error.InjectedReplyLoss;
            }
        }
    };
    var apply: Apply = .{ .target = &target, .scope = scope.digest(), .evict_on_index = 1 + rows.len / 8 };
    const env: Environment = .{ .io = io, .runtime = &runtime, .location_options = .{}, .cache_path = try std.fmt.allocPrint(a, "{s}/decoder", .{root}), .proposer = .{ .ptr = &apply, .propose = Apply.propose } };
    apply.lose_reply = true;
    try std.testing.expectError(error.InjectedReplyLoss, executeResident(alloc, &target, env, .{ .scope = scope, .action = .begin }, .{}));
    try std.testing.expectEqual(@as(u64, 1), apply.index);
    const replayed_begin = try executeResident(alloc, &target, env, .{ .scope = scope, .action = .begin }, .{});
    try std.testing.expectEqual(staging.Phase.importing, replayed_begin.phase);
    try std.testing.expectEqual(@as(u64, 1), apply.index);
    var wrong_scope = scope;
    wrong_scope.plan_digest[0] ^= 1;
    try std.testing.expectError(error.RestoreStagingScopeChanged, executeResident(alloc, &target, env, .{ .scope = wrong_scope, .action = .begin }, .{}));
    try std.testing.expectEqual(@as(u64, 1), apply.index);
    // Seed the durable boundary after an empty snapshot; every measured tail
    // page below uses the production owner handler and real LSM apply path.
    {
        var status = (try target.restoreStagingStatus(alloc)).?;
        defer status.deinit();
        status.value.rewrite.?.snapshot_complete = true;
        const encoded = try status.value.encode(alloc);
        defer alloc.free(encoded);
        var txn = try target.core.store.beginWriteTxn();
        errdefer txn.abort();
        try txn.put(@import("db/restore_staging_contract.zig").key, encoded);
        try txn.commit();
    }
    var chunk: rewrite.TailChunk = .{ .pin = scope.rewrite.?.retained_pin, .sequence = 1, .frame_digest = digest, .total = @intCast(frame.items.len), .offset = 0, .data = frame.items[0..@min(frame.items.len, 64 * 1024)] };
    const started = std.Io.Clock.awake.now(io);
    var requests: usize = 0;
    while (true) {
        requests += 1;
        if (requests > 64) return error.TestUnexpectedResult;
        apply.lose_reply = requests == 3;
        const response = executeResident(alloc, &target, env, .{ .scope = scope, .action = .import_page, .rewrite = intent, .rewrite_tail = chunk, .max_rows = 8 }, .{}) catch |err| switch (err) {
            error.InjectedReplyLoss => continue,
            else => return err,
        };
        if (response.rewrite.?.sequence == 1) break;
        chunk.offset = if (response.tail_next == chunk.total) chunk.total - 1 else response.tail_next;
        chunk.data = frame.items[chunk.offset..@min(frame.items.len, chunk.offset + 64 * 1024)];
    }
    const elapsed = started.durationTo(std.Io.Clock.awake.now(io)).toNanoseconds();
    try std.testing.expectEqual(@as(u64, 1), target.rewrite_program_cache.compilations);
    try std.testing.expect(target.rewrite_program_cache.hits >= 31);
    try std.testing.expect(target.rewrite_program_cache.entry == null);
    var status = (try target.restoreStagingStatus(alloc)).?;
    defer status.deinit();
    try std.testing.expectEqual(@as(u64, rows.len), status.value.rows);
    const control_start = std.Io.Clock.awake.now(io);
    for (0..32) |_| {
        var control = try ProgramSet.initIntent(alloc, intent);
        control.deinit();
    }
    const control_ns = control_start.durationTo(std.Io.Clock.awake.now(io)).toNanoseconds();
    std.debug.print("\nrewrite production handler: 64 historical schemas, 256 effects / 32 pages, {} compilations / {} hits, {}ms including LSM apply; uncached 32 compilations alone {}ms\n", .{ target.rewrite_program_cache.compilations, target.rewrite_program_cache.hits, @divTrunc(elapsed, std.time.ns_per_ms), @divTrunc(control_ns, std.time.ns_per_ms) });
    _ = try executeResident(alloc, &target, env, .{ .scope = scope, .action = .cancel }, .{});
    try std.testing.expect(target.rewrite_program_cache.entry == null);
}

test "restore owner verified decoder is reused across pages and terminal cleanup revokes import" {
    try testVerifiedDecoder(false);
}

test "restore owner verified decoder portable resumes and preserves vector projections" {
    try testVerifiedDecoder(true);
}

test "restore owner verified decoder portable range binding is immutable and durable" {
    const alloc = std.testing.allocator;
    const range_state = @import("db/range_state.zig");
    const materialization = @import("../api/restore_materialization.zig");
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(std.testing.io, ".", alloc);
    defer alloc.free(root);
    const options: @import("lsm_backend.zig").Options = .{ .read_runtime = @import("lsm_backend/storage_io.zig").ReadRuntime.init(std.testing.io) };
    const expected: db.types.ByteRange = .{ .start = &.{ 0, 127, 255 }, .end = &.{ 1, 0, 128 } };
    {
        var backend = try @import("lsm_backend.zig").Backend.open(alloc, root, options);
        defer backend.close();
        var store = try @import("docstore.zig").DocStore.openRuntime(alloc, try backend.runtimeStore(alloc, .{ .name = "docs" }));
        defer store.close();
        try materialization.bindPortableDecoderRange(alloc, &store, expected);
        try materialization.bindPortableDecoderRange(alloc, &store, expected);
        try std.testing.expectError(error.RestoreStagingScopeChanged, materialization.bindPortableDecoderRange(alloc, &store, .{ .start = "", .end = "" }));
        try std.testing.expectError(error.RestoreStagingScopeChanged, materialization.bindPortableDecoderRange(alloc, &store, .{ .start = "z", .end = "a" }));
    }
    var reopened = try @import("lsm_backend.zig").Backend.open(alloc, root, options);
    defer reopened.close();
    var store = try @import("docstore.zig").DocStore.openRuntime(alloc, try reopened.runtimeStore(alloc, .{ .name = "docs" }));
    defer store.close();
    const range = try range_state.loadRange(alloc, &store);
    defer range_state.freeRange(alloc, range);
    try std.testing.expectEqualSlices(u8, expected.start, range.start);
    try std.testing.expectEqualSlices(u8, expected.end, range.end);
}

test "restore owner verified decoder peer rewrite certificate chunks survive reopen corruption cancellation and publication" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const root = try tmp.dir.realPathFileAlloc(std.testing.io, ".", a);
    const source_path = try std.fmt.allocPrint(a, "{s}/source", .{root});
    const target_path = try std.fmt.allocPrint(a, "{s}/target", .{root});
    const cache_path = try std.fmt.allocPrint(a, "{s}/decoder", .{root});
    var runtime = try db.background_runtime.BackendRuntime.init(alloc, .{ .backend = .manual, .filesystem_io = std.testing.io });
    defer runtime.deinit();
    const source_ns: @import("db/doc_identity_namespace.zig").Namespace = .{ .table_id = 11, .shard_id = 12, .range_id = 12 };
    const target_ns: @import("db/doc_identity_namespace.zig").Namespace = .{ .table_id = 21, .shard_id = 22, .range_id = 22 };
    const source_options: db.OpenOptions = .{ .backend_runtime = &runtime, .identity_namespace = source_ns, .primary_backend = .{ .lsm = .{} }, .start_optional_runtimes = false, .start_index_workers = false };
    const target_options: db.OpenOptions = .{ .backend_runtime = &runtime, .identity_namespace = target_ns, .primary_backend = .{ .lsm = .{} }, .start_optional_runtimes = false, .start_index_workers = false };
    var source = try db.DB.open(alloc, source_path, source_options);
    defer source.close();
    const schema_json = "{\"version\":1,\"storage_mode\":\"document\"}";
    try source.setSchemaJson(alloc, schema_json);
    const padding = try a.alloc(u8, 3 * 1024 * 1024);
    var random = std.Random.DefaultPrng.init(71942);
    for (padding) |*byte| byte.* = 'a' + random.random().uintLessThan(u8, 26);
    const document = try std.fmt.allocPrint(a, "{{\"padding\":\"{s}\"}}", .{padding});
    try source.batchRaftReplicatedApply(.{ .writes = &.{.{ .key = "a", .value = document }}, .timestamp_ns = 123 }, .{ .term = 1, .index = 1 });
    const identity = try source.relationalTopologyIdentity();
    const source_scope: @import("db/online_source_contract.zig").Scope = .{
        .fence = .{ .role = .rewrite_source, .transition_id = 1, .attempt = 1, .owner_group_id = 12, .peer_group_id = 22, .namespace = source_ns, .admission_epoch = identity.next_epoch, .catalog_digest = identity.catalog_digest },
        .receiver_namespace = target_ns,
        .consumer_epoch = 1,
        .copy_attempt = .{ .donor_term = 1, .sequence = 1 },
    };
    try source.batchRaftReplicatedApply(.{ .online_source = .{ .admit = .{ .scope = source_scope, .limit = @import("retained_effects.zig").default_limit } } }, .{ .term = 1, .index = 2 });
    const certificate = try source.prepareOnlineSourcePublication(source_scope, .none);
    try source.batchRaftReplicatedApply(.{ .online_source = .{ .publish_certificate = .{ .scope = source_scope, .certificate = certificate } } }, .{ .term = 1, .index = 3 });
    const transfer = @import("db/source_artifact_transfer.zig");
    const descriptor = try transfer.describe(&source, source_scope, .none);
    try std.testing.expect(descriptor.total_bytes > transfer.max_chunk_bytes);
    var program = try @import("db/relational_rewrite_staging.zig").ProgramSet.initDocumentPreservation(alloc, schema_json);
    defer program.deinit();
    const binding: @import("db/relational_rewrite_contract.zig").Binding = .{ .program_digest = program.identity, .retained_pin = source_scope.pin(), .snapshot_certificate = try certificate.digest(), .retained_epoch = 1, .retained_start = certificate.cut.retained_start, .source_applied_index = certificate.cut.applied_index, .source_scope = source_scope };
    const artifact: metadata_staging.SourceArtifact = .{ .target_group_id = 22, .source_namespace = source_ns, .format = .portable, .snapshot_path = "source.afb2", .artifact_size_bytes = descriptor.total_bytes, .artifact_sha256 = try certificate.digest(), .rewrite = binding };
    const scope: staging.Scope = .{ .plan_id = @splat(1), .plan_digest = @splat(2), .source_artifact_digest = artifact.artifact_sha256, .source_descriptor_digest = try artifact.digest(alloc), .source_namespace = source_ns, .target_namespace = target_ns, .target_schema_digest = program.target_runtime_digest, .rewrite = binding };
    var target = try db.DB.open(alloc, target_path, target_options);
    var target_open = true;
    defer if (target_open) target.close();
    try target.setSchemaJson(alloc, schema_json);
    try target.reserveRestoreStagingScoped(alloc, scope);
    const Apply = struct {
        target: *db.DB,
        scope_digest: [32]u8,
        index: u64 = 0,
        fn propose(ptr: *anyopaque, value: db.types.BatchRequest, context: operation.RequestContext) !void {
            try context.ensureActive();
            const self: *@This() = @ptrCast(@alignCast(ptr));
            var batch = value;
            batch.restore_staging_scope = self.scope_digest;
            self.index += 1;
            try self.target.batchRaftReplicatedApply(batch, .{ .term = 1, .index = self.index });
        }
    };
    var apply: Apply = .{ .target = &target, .scope_digest = scope.digest() };
    const env: Environment = .{ .io = std.testing.io, .runtime = &runtime, .location_options = .{}, .cache_path = cache_path, .source_byte_budget = transfer.max_chunk_bytes, .proposer = .{ .ptr = &apply, .propose = Apply.propose } };
    _ = try executeResident(alloc, &target, env, .{ .scope = scope, .action = .begin }, .{});
    var request: Request = .{ .scope = scope, .action = .import_page, .source = .{ .location = "", .artifact = artifact, .peer_descriptor = descriptor }, .rewrite = .{ .preserve_document = true, .source_schemas = &.{schema_json}, .target_schema = schema_json, .program_digest = program.identity } };
    var canceled = std.atomic.Value(bool).init(true);
    try std.testing.expectError(error.Canceled, executeResident(alloc, &target, env, request, .{ .cancellation = .fromAtomic(&canceled) }));
    var wrong = request;
    wrong.source.?.peer_descriptor.?.certificate.cut.applied_index += 1;
    try std.testing.expectError(error.RestoreStagingScopeChanged, executeResident(alloc, &target, env, wrong, .{}));
    var response = try executeResident(alloc, &target, env, request, .{});
    try std.testing.expectEqual(@as(u64, 1), target.rewrite_program_cache.compilations);
    try std.testing.expectEqual(@as(u64, 0), response.source_next_offset);
    var chunk_count: usize = 0;
    while (response.source_next_offset < descriptor.total_bytes) {
        var bytes = try transfer.read(&source, alloc, descriptor, response.source_next_offset, .none);
        defer bytes.deinit(alloc);
        const encoded = try alloc.alloc(u8, std.base64.standard.Encoder.calcSize(bytes.data.len));
        defer alloc.free(encoded);
        request.source_chunk = .{ .offset = bytes.offset, .data_base64 = std.base64.standard.Encoder.encode(encoded, bytes.data), .digest = bytes.digest };
        const status_read = request.statusRead();
        try status_read.validate(22);
        try std.testing.expect(status_read.source == null and status_read.source_chunk == null and status_read.rewrite == null and status_read.rewrite_tail == null and status_read.rewrite_finish == null);
        if (chunk_count == 0) {
            request.source_chunk.?.digest[0] ^= 1;
            try std.testing.expectError(error.SourceSnapshotCorrupt, executeResident(alloc, &target, env, request, .{}));
            request.source_chunk.?.digest[0] ^= 1;
        }
        const wire = try std.json.Stringify.valueAlloc(alloc, request, .{});
        defer alloc.free(wire);
        var decoded = try std.json.parseFromSlice(Request, alloc, wire, .{});
        defer decoded.deinit();
        response = try executeResident(alloc, &target, env, decoded.value, .{});
        try std.testing.expectEqual(bytes.offset + bytes.data.len, response.source_next_offset);
        const replay = try executeResident(alloc, &target, env, decoded.value, .{});
        try std.testing.expectEqual(@as(u64, 1), target.rewrite_program_cache.compilations);
        try std.testing.expectEqual(response.source_next_offset, replay.source_next_offset);
        try std.testing.expectEqual(@as(u64, 0), response.rows);
        if (chunk_count == 0) {
            target.close();
            target_open = false;
            target = try db.DB.open(alloc, target_path, target_options);
            target_open = true;
            request.source_chunk = null;
            const peer_receipt_path = try std.fmt.allocPrint(a, "{s}.materializing/peer.progress", .{cache_path});
            const peer_receipt = try native_backup.readFileAlloc(alloc, std.testing.io, peer_receipt_path, 121);
            defer alloc.free(peer_receipt);
            peer_receipt[peer_receipt.len - 1] ^= 1;
            _ = try native_backup.writeFileDurable(std.testing.io, peer_receipt_path, peer_receipt);
            try std.testing.expectError(error.InvalidRestoreSourceCheckpoint, executeResident(alloc, &target, env, request, .{}));
            peer_receipt[peer_receipt.len - 1] ^= 1;
            _ = try native_backup.writeFileDurable(std.testing.io, peer_receipt_path, peer_receipt);
            var empty_storage: [0]u8 = .{};
            var failing = std.heap.FixedBufferAllocator.init(&empty_storage);
            try std.testing.expectError(error.OutOfMemory, executeResident(failing.allocator(), &target, env, request, .{}));
            response = try executeResident(alloc, &target, env, request, .{});
            try std.testing.expectEqual(bytes.offset + bytes.data.len, response.source_next_offset);
        }
        chunk_count += 1;
    }
    request.source_chunk = null;
    test_fail_after_source_stage_rename = true;
    var saw_publication_crash = false;
    for (0..1000) |_| {
        response = executeResident(alloc, &target, env, request, .{}) catch |err| {
            if (err != error.InjectedSourceStageRenameFailure) return err;
            saw_publication_crash = true;
            target.close();
            target_open = false;
            target = try db.DB.open(alloc, target_path, target_options);
            target_open = true;
            continue;
        };
        try std.testing.expectEqual(@as(u64, 1), target.rewrite_program_cache.compilations);
        if (response.rewrite.?.snapshot_complete) break;
    } else return error.TestUnexpectedResult;
    try std.testing.expect(saw_publication_crash);
    const copied = (try target.get(alloc, "a")).?;
    defer alloc.free(copied);
    try std.testing.expectEqualSlices(u8, document, copied);
    try std.testing.expectEqual(descriptor.total_bytes, response.source_next_offset);
    _ = try executeResident(alloc, &target, env, .{ .scope = scope, .action = .cancel }, .{});
    try std.testing.expect(target.rewrite_program_cache.entry == null);
    try std.testing.expectError(error.RestoreStagingScopeChanged, executeResident(alloc, &target, env, request, .{}));
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
    const source_namespace: @import("db/doc_identity.zig").Namespace = .{ .table_id = 11, .shard_id = 101, .range_id = 101 };
    const target_namespace: @import("db/doc_identity.zig").Namespace = .{ .table_id = 12, .shard_id = 102, .range_id = 102 };
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
    const source_fence: @import("db/relational_integrity_topology.zig").Fence = .{ .role = .backup_snapshot, .transition_id = 900, .attempt = 1, .peer_group_id = 101, .owner_group_id = 101, .admission_epoch = source_identity.next_epoch, .namespace = source_identity.namespace, .catalog_digest = source_identity.catalog_digest };
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
    const schema = try @import("schema.zig").serializeSchema(alloc, target.core.schema orelse .{});
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
