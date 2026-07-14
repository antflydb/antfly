// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the License at https://www.antfly.io/licensing/ELv2-license.

//! Runtime-owned, point-in-time HA seed capture.
//!
//! The caller describes live source files or directory trees, never manifest
//! bytes. Capture takes the process-wide mutation barrier exclusively before it
//! creates the durable seeding slot and holds it through `backup_end`. Thus the
//! copied file set and the manifest checkpoint are one mutation boundary.
//!
//! Publication is fail closed and crash resumable:
//!
//! 1. `backup_start` reserves a non-streaming seeding slot and pins WAL.
//! 2. Files are copied into `.capturing-<generation>` and fsynced.
//! 3. `manifest.afha` and `SNAPSHOT.json` make the local snapshot resumable.
//! 4. The exact manifest is appended as `backup_end` (or an identical existing
//!    record is recovered after a crash).
//! 5. `COMPLETE.json` is fsynced and the directory is atomically renamed to
//!    `generations/<generation>`.
//!
//! A crash before `SNAPSHOT.json` cannot be resumed at the old checkpoint: the
//! generation is durably marked aborted and its seeding slot is dropped. A new
//! generation must be requested. No partial directory is ever published.

const std = @import("std");
const Allocator = std.mem.Allocator;
const Crc32 = std.hash.Crc32;
const Sha256 = std.crypto.hash.sha2.Sha256;
const fs_paths = @import("../../common/fs_paths.zig");
const backup_manifest = @import("backup_manifest.zig");
const lifecycle_receipt_ledger = @import("lifecycle_receipt_ledger.zig");
const local_generation_gc = @import("local_generation_gc.zig");
const mutation_barrier = @import("mutation_barrier.zig");
const object_storage = @import("../object_storage.zig");
const primary_mod = @import("primary.zig");
const replication_log = @import("replication_log.zig");
const seed_artifact = @import("seed_artifact.zig");
const standby_mod = @import("standby.zig");
const validation = @import("validation.zig");

pub const legacy_format_version: u16 = 1;
pub const format_version: u16 = 2;
pub const generations_dir_name = "generations";
pub const manifest_name = "manifest.afha";
pub const prepared_name = "PREPARED.json";
pub const snapshot_name = "SNAPSHOT.json";
pub const complete_name = "COMPLETE.json";

pub const Limits = struct {
    max_files: usize = 1_000_000,
    max_file_bytes: u64 = 8 * 1024 * 1024 * 1024,
    max_total_bytes: u64 = 64 * 1024 * 1024 * 1024,
    max_manifest_bytes: usize = 16 * 1024 * 1024,
    max_receipt_bytes: usize = 1024 * 1024,
};

pub const FileSource = struct {
    source_path: []const u8,
    artifact_path: []const u8,
    kind: backup_manifest.FileKind = .other,
};

pub const TreeSource = struct {
    source_root: []const u8,
    artifact_prefix: []const u8,
    kind: backup_manifest.FileKind = .other,
};

/// A tree is walked recursively while the exclusive mutation lease is held.
/// Symbolic links and other non-regular filesystem entries are rejected.
pub const Source = union(enum) {
    file: FileSource,
    tree: TreeSource,
};

pub const CaptureRequest = struct {
    primary: *primary_mod.Primary,
    barrier: *mutation_barrier.MutationBarrier,
    slot_name: []const u8,
    generation: []const u8,
    capture_root: []const u8,
    sources: []const Source,
    binding: ?seed_artifact.LifecycleBinding = null,
    pod_uid: ?[]const u8 = null,
    limits: Limits = .{},
};

pub const CaptureResult = struct {
    generation_root: []u8,
    content_root: []u8,
    manifest_path: []u8,
    manifest_bytes: []u8,
    receipt_json: []u8,
    backup_lsn: u64,
    checkpoint_lsn: u64,
    end_record_lsn: u64,
    already_captured: bool,

    pub fn deinit(self: *CaptureResult, alloc: Allocator) void {
        alloc.free(self.generation_root);
        alloc.free(self.content_root);
        alloc.free(self.manifest_path);
        alloc.free(self.manifest_bytes);
        alloc.free(self.receipt_json);
        self.* = undefined;
    }
};

pub const PublishedGenerationGCRequest = struct {
    store: seed_artifact.Store,
    capture_root: []const u8,
    generation: []const u8,
    slot_name: []const u8,
    protected_generations: []const []const u8 = &.{},
    retain_generations: usize = 2,
    artifact_limits: seed_artifact.Limits = .{},
    max_local_generations: usize = 10_000,
};

const PreparedReceipt = struct {
    format_version: u16,
    generation: []const u8,
    slot_name: []const u8,
    cluster_id: u64,
    shard_id: u64,
    table_id: u64,
    timeline_id: u64,
    epoch: u64,
    source_plan_sha256: []const u8,
    backup_lsn: u64,
    topology_id: []const u8 = "",
    topology_generation: u64 = 0,
    node_id: []const u8 = "",
    target_pvc_name: []const u8 = "",
    target_pvc_uid: []const u8 = "",
};

const SnapshotReceipt = struct {
    format_version: u16,
    generation: []const u8,
    slot_name: []const u8,
    cluster_id: u64,
    shard_id: u64,
    table_id: u64,
    timeline_id: u64,
    epoch: u64,
    source_plan_sha256: []const u8,
    manifest_id: []const u8,
    backup_lsn: u64,
    checkpoint_lsn: u64,
    manifest_sha256: []const u8,
    file_count: usize,
    total_bytes: u64,
    topology_id: []const u8 = "",
    topology_generation: u64 = 0,
    node_id: []const u8 = "",
    target_pvc_name: []const u8 = "",
    target_pvc_uid: []const u8 = "",
};

pub const CaptureReceipt = struct {
    format_version: u16,
    generation: []const u8,
    slot_name: []const u8,
    cluster_id: u64,
    shard_id: u64,
    table_id: u64,
    timeline_id: u64,
    epoch: u64,
    source_plan_sha256: []const u8,
    manifest_id: []const u8,
    backup_lsn: u64,
    checkpoint_lsn: u64,
    end_record_lsn: u64,
    manifest_sha256: []const u8,
    file_count: usize,
    total_bytes: u64,
    topology_id: []const u8 = "",
    topology_generation: u64 = 0,
    node_id: []const u8 = "",
    target_pvc_name: []const u8 = "",
    target_pvc_uid: []const u8 = "",

    pub fn identity(self: CaptureReceipt) standby_mod.Identity {
        return .{
            .cluster_id = self.cluster_id,
            .shard_id = self.shard_id,
            .table_id = self.table_id,
            .timeline_id = self.timeline_id,
            .epoch = self.epoch,
        };
    }
};

const AbortedReceipt = struct {
    format_version: u16,
    generation: []const u8,
    slot_name: []const u8,
    source_plan_sha256: []const u8,
    reason: []const u8 = "snapshot_not_durable",
};

const OwnedSource = struct {
    source_path: []u8,
    artifact_path: []u8,
    kind: backup_manifest.FileKind,

    fn deinit(self: *OwnedSource, alloc: Allocator) void {
        alloc.free(self.source_path);
        alloc.free(self.artifact_path);
        self.* = undefined;
    }
};

const FailureBoundary = enum {
    snapshot_durable,
    backup_end_durable,
    completion_durable,
};

const Hooks = struct {
    context: ?*anyopaque = null,
    before_exclusive: ?*const fn (?*anyopaque) void = null,
    after_exclusive: ?*const fn (?*anyopaque) void = null,
};

const CaptureOptions = struct {
    hooks: Hooks = .{},
    fail_after: ?FailureBoundary = null,
};

pub fn capture(alloc: Allocator, request: CaptureRequest) !CaptureResult {
    return captureWithOptions(alloc, request, .{});
}

/// Verifies the complete remote v2 artifact against the immutable local
/// capture receipt before granting local deletion authority. The source PVC
/// must be mounted read-write for this distinct post-publish action.
pub fn prunePublishedGenerations(
    alloc: Allocator,
    request: PublishedGenerationGCRequest,
) !local_generation_gc.PruneResult {
    if (!validAbsoluteRoot(request.capture_root) or std.mem.eql(u8, request.capture_root, "/"))
        return error.InvalidCaptureRoot;
    if (!validation.isIdentifier(request.generation)) return error.InvalidCaptureGeneration;
    if (!validation.isIdentifier(request.slot_name)) return error.InvalidSlotName;

    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    const generation_root = try std.fs.path.join(alloc, &.{ request.capture_root, generations_dir_name, request.generation });
    defer alloc.free(generation_root);
    const complete_path = try std.fs.path.join(alloc, &.{ generation_root, complete_name });
    defer alloc.free(complete_path);
    const local_json = readFileAlloc(io, alloc, complete_path, request.artifact_limits.max_receipt_bytes) catch |err| switch (err) {
        error.FileNotFound => return error.CaptureCompletionMissing,
        else => return err,
    };
    defer alloc.free(local_json);
    var local = std.json.parseFromSlice(CaptureReceipt, alloc, local_json, .{ .ignore_unknown_fields = false }) catch
        return error.InvalidCaptureReceipt;
    defer local.deinit();
    try validatePublishedGCReceipt(local.value, request);

    const manifest_path = try std.fs.path.join(alloc, &.{ generation_root, manifest_name });
    defer alloc.free(manifest_path);
    const manifest_bytes = try readFileAlloc(io, alloc, manifest_path, request.artifact_limits.max_manifest_bytes);
    defer alloc.free(manifest_bytes);
    try expectSha256(manifest_bytes, local.value.manifest_sha256, error.CaptureManifestDigestMismatch);
    const manifest = try backup_manifest.decodeAlloc(alloc, manifest_bytes);
    defer backup_manifest.freeDecoded(alloc, manifest);
    try validatePublishedGCManifest(manifest, local.value);

    var verified = try seed_artifact.verifyRemote(alloc, request.store, .{
        .generation = request.generation,
        .slot_name = request.slot_name,
        .identity = local.value.identity(),
        .minimum_checkpoint_lsn = local.value.checkpoint_lsn,
        .binding = captureReceiptBinding(local.value),
    }, request.artifact_limits);
    defer verified.deinit(alloc);
    var remote = std.json.parseFromSlice(seed_artifact.Receipt, alloc, verified.receipt_json, .{ .ignore_unknown_fields = false }) catch
        return error.InvalidArtifactReceipt;
    defer remote.deinit();
    try validateRemoteAgainstCapture(remote.value, local.value);

    try local_generation_gc.markEligible(alloc, .{
        .root = request.capture_root,
        .scope = .source_capture,
        .generation = request.generation,
        .slot_name = request.slot_name,
        .checkpoint_lsn = remote.value.checkpoint_lsn,
        .checkpoint_bytes = verified.receipt_json,
        .max_checkpoint_bytes = request.artifact_limits.max_receipt_bytes,
    });
    return try local_generation_gc.prune(alloc, .{
        .root = request.capture_root,
        .scope = .source_capture,
        .slot_name = request.slot_name,
        .current_generation = request.generation,
        .protected_generations = request.protected_generations,
        .retain_generations = request.retain_generations,
        .max_entries = request.max_local_generations,
    });
}

fn validatePublishedGCReceipt(receipt: CaptureReceipt, request: PublishedGenerationGCRequest) !void {
    if ((receipt.format_version != legacy_format_version and receipt.format_version != format_version) or
        !std.mem.eql(u8, receipt.generation, request.generation) or
        !std.mem.eql(u8, receipt.slot_name, request.slot_name) or
        !std.mem.eql(u8, receipt.manifest_id, request.generation)) return error.CaptureStateConflict;
    if (receipt.backup_lsn == 0 or receipt.checkpoint_lsn != receipt.backup_lsn or receipt.end_record_lsn <= receipt.checkpoint_lsn)
        return error.CaptureCheckpointMismatch;
    if (receipt.file_count == 0 or receipt.file_count > request.artifact_limits.max_files or
        receipt.total_bytes > request.artifact_limits.max_total_bytes or
        receipt.manifest_sha256.len != Sha256.digest_length * 2 or
        receipt.source_plan_sha256.len != Sha256.digest_length * 2)
        return error.InvalidCaptureReceipt;
    try standby_mod.validateIdentity(receipt.identity());
    if (captureReceiptBinding(receipt)) |binding| try validateCaptureBinding(binding);
}

fn validatePublishedGCManifest(manifest: backup_manifest.ManifestView, receipt: CaptureReceipt) !void {
    try expectIdentity(receipt.identity(), manifest.identity, error.CaptureIdentityMismatch);
    if (!std.mem.eql(u8, manifest.manifest_id, receipt.manifest_id) or
        manifest.backup_lsn != receipt.backup_lsn or
        manifest.checkpoint_lsn != receipt.checkpoint_lsn or
        manifest.files.len != receipt.file_count) return error.CaptureManifestMismatch;
    var total_bytes: u64 = 0;
    for (manifest.files) |file| total_bytes = try std.math.add(u64, total_bytes, file.size_bytes);
    if (total_bytes != receipt.total_bytes) return error.CaptureManifestMismatch;
}

fn validateRemoteAgainstCapture(remote: seed_artifact.Receipt, local: CaptureReceipt) !void {
    if (remote.format_version != seed_artifact.chunked_format_version and remote.format_version != seed_artifact.format_version)
        return error.LocalGCRequiresArtifactV2;
    if (!std.mem.eql(u8, remote.generation, local.generation) or
        !std.mem.eql(u8, remote.slot_name, local.slot_name) or
        !std.mem.eql(u8, remote.manifest_id, local.manifest_id) or
        remote.backup_lsn != local.backup_lsn or
        remote.checkpoint_lsn != local.checkpoint_lsn or
        !std.mem.eql(u8, remote.manifest_sha256, local.manifest_sha256) or
        remote.files.len != local.file_count or
        remote.total_bytes != local.total_bytes) return error.ArtifactCaptureMismatch;
    try expectIdentity(local.identity(), remote.identity(), error.ArtifactCaptureMismatch);
    if (captureReceiptBinding(local)) |binding| {
        if (remote.format_version != seed_artifact.format_version or
            !std.mem.eql(u8, binding.topology_id, remote.topology_id) or
            binding.topology_generation != remote.topology_generation or
            !std.mem.eql(u8, binding.node_id, remote.node_id) or
            !std.mem.eql(u8, binding.target_pvc_name, remote.target_pvc_name) or
            !std.mem.eql(u8, binding.target_pvc_uid, remote.target_pvc_uid)) return error.ArtifactCaptureMismatch;
    }
}

fn captureReceiptBinding(receipt: CaptureReceipt) ?seed_artifact.LifecycleBinding {
    if (receipt.format_version != format_version) return null;
    return .{
        .topology_id = receipt.topology_id,
        .topology_generation = receipt.topology_generation,
        .node_id = receipt.node_id,
        .target_pvc_name = receipt.target_pvc_name,
        .target_pvc_uid = receipt.target_pvc_uid,
    };
}

/// Capture while the caller already owns the exclusive mutation lease.
///
/// Runtime integration should use this form when it must lock its HA role/state
/// mutex to keep `request.primary` alive: acquire the barrier first, then the
/// role/state mutex, then call this function. That preserves the global lock
/// order `mutation barrier -> HA state -> database/catalog locks`.
pub fn captureWithExclusiveLease(
    alloc: Allocator,
    request: CaptureRequest,
    lease: *const mutation_barrier.MutationBarrier.ExclusiveLease,
) !CaptureResult {
    try validateRequest(request);
    if (lease.barrier != request.barrier) return error.WrongCaptureBarrier;
    return captureHeld(alloc, request, .{});
}

fn captureWithOptions(alloc: Allocator, request: CaptureRequest, options: CaptureOptions) !CaptureResult {
    try validateRequest(request);
    if (options.hooks.before_exclusive) |hook| hook(options.hooks.context);
    var lease = request.barrier.acquireExclusive();
    defer lease.release();
    if (options.hooks.after_exclusive) |hook| hook(options.hooks.context);

    return captureHeld(alloc, request, options);
}

fn captureHeld(alloc: Allocator, request: CaptureRequest, options: CaptureOptions) !CaptureResult {
    const plan_digest = sourcePlanDigest(request.sources);
    var plan_hex: [Sha256.digest_length * 2]u8 = undefined;
    encodeHex(&plan_hex, &plan_digest);

    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    const generations_root = try std.fs.path.join(alloc, &.{ request.capture_root, generations_dir_name });
    defer alloc.free(generations_root);
    const generation_root = try std.fs.path.join(alloc, &.{ generations_root, request.generation });
    defer alloc.free(generation_root);
    const staging_name = try std.fmt.allocPrint(alloc, ".capturing-{s}", .{request.generation});
    defer alloc.free(staging_name);
    const staging_root = try std.fs.path.join(alloc, &.{ request.capture_root, staging_name });
    defer alloc.free(staging_root);
    const aborted_name = try std.fmt.allocPrint(alloc, "ABORTED-{s}.json", .{request.generation});
    defer alloc.free(aborted_name);
    const aborted_path = try std.fs.path.join(alloc, &.{ request.capture_root, aborted_name });
    defer alloc.free(aborted_path);

    if (try directoryExists(io, generation_root)) {
        return try openPublished(alloc, io, request, generation_root, &plan_hex, true);
    }

    if (try fileExists(io, aborted_path)) {
        const aborted_json = try readFileAlloc(io, alloc, aborted_path, request.limits.max_receipt_bytes);
        defer alloc.free(aborted_json);
        try validateAbortedReceipt(alloc, aborted_json, request, &plan_hex);
        try dropAbortedSlotIfPresent(request);
        return error.CaptureGenerationAborted;
    }

    const staging_exists = try directoryExists(io, staging_root);
    const prior_slot = request.primary.slot(request.slot_name);
    if (!staging_exists and prior_slot != null and prior_slot.?.lifecycle == .seeding) {
        // `backup_start` was durable but not enough local state exists to know
        // which files belonged to that checkpoint.
        _ = try request.primary.beginBaseBackup(.{
            .slot_name = request.slot_name,
            .manifest_id = request.generation,
        });
        try abortIncomplete(alloc, io, request, staging_root, aborted_path, &plan_hex);
        return error.CaptureGenerationAborted;
    }

    if (staging_exists) {
        const prepared_path = try std.fs.path.join(alloc, &.{ staging_root, prepared_name });
        defer alloc.free(prepared_path);
        const prepared_json = readFileAlloc(io, alloc, prepared_path, request.limits.max_receipt_bytes) catch |err| switch (err) {
            error.FileNotFound => {
                try abortIncomplete(alloc, io, request, staging_root, aborted_path, &plan_hex);
                return error.CaptureGenerationAborted;
            },
            else => return err,
        };
        defer alloc.free(prepared_json);
        const prepared = try parsePrepared(alloc, prepared_json, request, &plan_hex);
        const retained = request.primary.slot(request.slot_name) orelse {
            try abortIncomplete(alloc, io, request, staging_root, aborted_path, &plan_hex);
            return error.CaptureGenerationAborted;
        };
        if (retained.lifecycle != .seeding or retained.active or retained.reseed_required) return error.CaptureSlotNotRetained;
        const started = try request.primary.beginBaseBackup(.{
            .slot_name = request.slot_name,
            .manifest_id = request.generation,
        });
        if (started.backup_lsn != prepared.backup_lsn) return error.CaptureCheckpointMismatch;

        const snapshot_path = try std.fs.path.join(alloc, &.{ staging_root, snapshot_name });
        defer alloc.free(snapshot_path);
        if (!try fileExists(io, snapshot_path)) {
            try abortIncomplete(alloc, io, request, staging_root, aborted_path, &plan_hex);
            return error.CaptureGenerationAborted;
        }
        return try finishDurableSnapshot(alloc, io, request, staging_root, generation_root, generations_root, &plan_hex, options);
    }

    const started = try request.primary.beginBaseBackup(.{
        .slot_name = request.slot_name,
        .manifest_id = request.generation,
    });
    // `backup_start` is a control record. With all mutations drained and new
    // mutations excluded, its LSN is the exact data/catalog checkpoint.
    if (request.primary.lastLsn() != started.backup_lsn) return error.CaptureCheckpointMismatch;

    try fs_paths.createDirPathPortable(io, request.capture_root);
    try fs_paths.createDirPathPortable(io, generations_root);
    try fs_paths.createDirPathPortable(io, staging_root);
    try fs_paths.syncDirPortable(io, request.capture_root);

    const binding = request.binding orelse seed_artifact.LifecycleBinding{};
    const prepared_json = try std.json.Stringify.valueAlloc(alloc, PreparedReceipt{
        .format_version = captureReceiptFormatVersion(request),
        .generation = request.generation,
        .slot_name = request.slot_name,
        .cluster_id = request.primary.identity.cluster_id,
        .shard_id = request.primary.identity.shard_id,
        .table_id = request.primary.identity.table_id,
        .timeline_id = request.primary.identity.timeline_id,
        .epoch = request.primary.identity.epoch,
        .source_plan_sha256 = &plan_hex,
        .backup_lsn = started.backup_lsn,
        .topology_id = binding.topology_id,
        .topology_generation = binding.topology_generation,
        .node_id = binding.node_id,
        .target_pvc_name = binding.target_pvc_name,
        .target_pvc_uid = binding.target_pvc_uid,
    }, .{});
    defer alloc.free(prepared_json);
    const prepared_path = try std.fs.path.join(alloc, &.{ staging_root, prepared_name });
    defer alloc.free(prepared_path);
    _ = try writeImmutableFile(io, alloc, prepared_path, prepared_json, error.CaptureStateConflict);

    _ = createSnapshot(alloc, io, request, staging_root, started.backup_lsn, &plan_hex) catch |err| {
        try abortIncomplete(alloc, io, request, staging_root, aborted_path, &plan_hex);
        return err;
    };
    return try finishDurableSnapshot(alloc, io, request, staging_root, generation_root, generations_root, &plan_hex, options);
}

fn createSnapshot(
    alloc: Allocator,
    io: std.Io,
    request: CaptureRequest,
    staging_root: []const u8,
    backup_lsn: u64,
    plan_hex: []const u8,
) !void {
    var sources = try collectSources(alloc, io, request);
    defer {
        for (sources.items) |*source| source.deinit(alloc);
        sources.deinit(alloc);
    }
    if (sources.items.len == 0) return error.EmptyCapture;
    if (sources.items.len > request.limits.max_files) return error.TooManyCaptureFiles;
    std.mem.sort(OwnedSource, sources.items, {}, sourceLessThan);
    for (sources.items, 0..) |source, index| {
        try backup_manifest.validatePath(source.artifact_path);
        if (!validation.isNormalizedPath(source.artifact_path) or std.fs.path.isAbsolute(source.artifact_path)) return error.InvalidArtifactPath;
        if (index > 0 and std.mem.eql(u8, sources.items[index - 1].artifact_path, source.artifact_path)) return error.DuplicateArtifactPath;
    }

    const content_root = try std.fs.path.join(alloc, &.{ staging_root, "content" });
    defer alloc.free(content_root);
    try fs_paths.createDirPathPortable(io, content_root);

    const files = try alloc.alloc(backup_manifest.FileEntry, sources.items.len);
    defer alloc.free(files);
    var total_bytes: u64 = 0;
    for (sources.items, 0..) |source, index| {
        const destination = try std.fs.path.join(alloc, &.{ content_root, source.artifact_path });
        defer alloc.free(destination);
        const metadata = try copyAndChecksum(io, source.source_path, destination, content_root, request.limits.max_file_bytes);
        total_bytes = try std.math.add(u64, total_bytes, metadata.size_bytes);
        if (total_bytes > request.limits.max_total_bytes) return error.CaptureTooLarge;
        files[index] = .{
            .path = source.artifact_path,
            .kind = source.kind,
            .size_bytes = metadata.size_bytes,
            .crc32 = metadata.crc32,
        };
    }

    const manifest_bytes = try backup_manifest.encodeAlloc(alloc, .{
        .identity = request.primary.identity,
        .manifest_id = request.generation,
        .backup_lsn = backup_lsn,
        .checkpoint_lsn = backup_lsn,
        .files = files,
    });
    defer alloc.free(manifest_bytes);
    if (manifest_bytes.len > request.limits.max_manifest_bytes) return error.ManifestTooLarge;
    var manifest_digest: [Sha256.digest_length]u8 = undefined;
    Sha256.hash(manifest_bytes, &manifest_digest, .{});
    var manifest_hex: [Sha256.digest_length * 2]u8 = undefined;
    encodeHex(&manifest_hex, &manifest_digest);

    const manifest_path = try std.fs.path.join(alloc, &.{ staging_root, manifest_name });
    defer alloc.free(manifest_path);
    _ = try writeImmutableFile(io, alloc, manifest_path, manifest_bytes, error.CaptureManifestConflict);
    const binding = request.binding orelse seed_artifact.LifecycleBinding{};
    const snapshot_json = try std.json.Stringify.valueAlloc(alloc, SnapshotReceipt{
        .format_version = captureReceiptFormatVersion(request),
        .generation = request.generation,
        .slot_name = request.slot_name,
        .cluster_id = request.primary.identity.cluster_id,
        .shard_id = request.primary.identity.shard_id,
        .table_id = request.primary.identity.table_id,
        .timeline_id = request.primary.identity.timeline_id,
        .epoch = request.primary.identity.epoch,
        .source_plan_sha256 = plan_hex,
        .manifest_id = request.generation,
        .backup_lsn = backup_lsn,
        .checkpoint_lsn = backup_lsn,
        .manifest_sha256 = &manifest_hex,
        .file_count = files.len,
        .total_bytes = total_bytes,
        .topology_id = binding.topology_id,
        .topology_generation = binding.topology_generation,
        .node_id = binding.node_id,
        .target_pvc_name = binding.target_pvc_name,
        .target_pvc_uid = binding.target_pvc_uid,
    }, .{});
    defer alloc.free(snapshot_json);
    const snapshot_path = try std.fs.path.join(alloc, &.{ staging_root, snapshot_name });
    defer alloc.free(snapshot_path);
    _ = try writeImmutableFile(io, alloc, snapshot_path, snapshot_json, error.CaptureStateConflict);
    try fs_paths.syncDirPortable(io, content_root);
    try fs_paths.syncDirPortable(io, staging_root);
}

fn finishDurableSnapshot(
    alloc: Allocator,
    io: std.Io,
    request: CaptureRequest,
    staging_root: []const u8,
    generation_root: []const u8,
    generations_root: []const u8,
    plan_hex: []const u8,
    options: CaptureOptions,
) !CaptureResult {
    const snapshot = try verifySnapshot(alloc, io, request, staging_root, plan_hex);
    defer alloc.free(snapshot.manifest_bytes);
    try failAt(options, .snapshot_durable);

    const end_lsn = try ensureBackupEnd(
        alloc,
        request.primary,
        snapshot.manifest_bytes,
        request.generation,
        snapshot.backup_lsn,
    );
    try failAt(options, .backup_end_durable);

    const binding = request.binding orelse seed_artifact.LifecycleBinding{};
    const complete_json = try std.json.Stringify.valueAlloc(alloc, CaptureReceipt{
        .format_version = captureReceiptFormatVersion(request),
        .generation = request.generation,
        .slot_name = request.slot_name,
        .cluster_id = request.primary.identity.cluster_id,
        .shard_id = request.primary.identity.shard_id,
        .table_id = request.primary.identity.table_id,
        .timeline_id = request.primary.identity.timeline_id,
        .epoch = request.primary.identity.epoch,
        .source_plan_sha256 = plan_hex,
        .manifest_id = request.generation,
        .backup_lsn = snapshot.backup_lsn,
        .checkpoint_lsn = snapshot.checkpoint_lsn,
        .end_record_lsn = end_lsn,
        .manifest_sha256 = &snapshot.manifest_sha256,
        .file_count = snapshot.file_count,
        .total_bytes = snapshot.total_bytes,
        .topology_id = binding.topology_id,
        .topology_generation = binding.topology_generation,
        .node_id = binding.node_id,
        .target_pvc_name = binding.target_pvc_name,
        .target_pvc_uid = binding.target_pvc_uid,
    }, .{});
    defer alloc.free(complete_json);
    const complete_path = try std.fs.path.join(alloc, &.{ staging_root, complete_name });
    defer alloc.free(complete_path);
    _ = try writeImmutableFile(io, alloc, complete_path, complete_json, error.CaptureStateConflict);
    try fs_paths.syncDirPortable(io, staging_root);
    try failAt(options, .completion_durable);

    std.Io.Dir.rename(std.Io.Dir.cwd(), staging_root, std.Io.Dir.cwd(), generation_root, io) catch |err| {
        if (try directoryExists(io, generation_root)) {
            const published = try openPublished(alloc, io, request, generation_root, plan_hex, true);
            std.Io.Dir.cwd().deleteTree(io, staging_root) catch {};
            return published;
        }
        return err;
    };
    try fs_paths.syncDirPortable(io, generations_root);
    return try openPublished(alloc, io, request, generation_root, plan_hex, false);
}

const VerifiedSnapshot = struct {
    manifest_bytes: []u8,
    manifest_sha256: [Sha256.digest_length * 2]u8,
    backup_lsn: u64,
    checkpoint_lsn: u64,
    file_count: usize,
    total_bytes: u64,
};

fn verifySnapshot(alloc: Allocator, io: std.Io, request: CaptureRequest, root: []const u8, plan_hex: []const u8) !VerifiedSnapshot {
    const snapshot_path = try std.fs.path.join(alloc, &.{ root, snapshot_name });
    defer alloc.free(snapshot_path);
    const raw = try readFileAlloc(io, alloc, snapshot_path, request.limits.max_receipt_bytes);
    defer alloc.free(raw);
    var parsed = std.json.parseFromSlice(SnapshotReceipt, alloc, raw, .{ .ignore_unknown_fields = false }) catch return error.InvalidCaptureReceipt;
    defer parsed.deinit();
    try validateSnapshotReceipt(parsed.value, request, plan_hex);
    if (parsed.value.manifest_sha256.len != Sha256.digest_length * 2) return error.InvalidCaptureReceipt;
    var manifest_sha256: [Sha256.digest_length * 2]u8 = undefined;
    @memcpy(&manifest_sha256, parsed.value.manifest_sha256);

    const manifest_path = try std.fs.path.join(alloc, &.{ root, manifest_name });
    defer alloc.free(manifest_path);
    const manifest_bytes = try readFileAlloc(io, alloc, manifest_path, request.limits.max_manifest_bytes);
    errdefer alloc.free(manifest_bytes);
    try expectSha256(manifest_bytes, parsed.value.manifest_sha256, error.CaptureManifestDigestMismatch);
    const content_root = try std.fs.path.join(alloc, &.{ root, "content" });
    defer alloc.free(content_root);
    try verifyManifestFiles(
        alloc,
        io,
        manifest_bytes,
        content_root,
        request,
        parsed.value.backup_lsn,
        parsed.value.checkpoint_lsn,
        parsed.value.file_count,
        parsed.value.total_bytes,
    );
    return .{
        .manifest_bytes = manifest_bytes,
        .manifest_sha256 = manifest_sha256,
        .backup_lsn = parsed.value.backup_lsn,
        .checkpoint_lsn = parsed.value.checkpoint_lsn,
        .file_count = parsed.value.file_count,
        .total_bytes = parsed.value.total_bytes,
    };
}

fn openPublished(
    alloc: Allocator,
    io: std.Io,
    request: CaptureRequest,
    generation_root: []const u8,
    plan_hex: []const u8,
    already_captured: bool,
) !CaptureResult {
    const complete_path = try std.fs.path.join(alloc, &.{ generation_root, complete_name });
    defer alloc.free(complete_path);
    const complete_json = try readFileAlloc(io, alloc, complete_path, request.limits.max_receipt_bytes);
    errdefer alloc.free(complete_json);
    var parsed = std.json.parseFromSlice(CaptureReceipt, alloc, complete_json, .{ .ignore_unknown_fields = false }) catch return error.InvalidCaptureReceipt;
    defer parsed.deinit();
    try validateCompleteReceipt(parsed.value, request, plan_hex);

    const manifest_path = try std.fs.path.join(alloc, &.{ generation_root, manifest_name });
    errdefer alloc.free(manifest_path);
    const manifest_bytes = try readFileAlloc(io, alloc, manifest_path, request.limits.max_manifest_bytes);
    errdefer alloc.free(manifest_bytes);
    try expectSha256(manifest_bytes, parsed.value.manifest_sha256, error.CaptureManifestDigestMismatch);
    const content_root = try std.fs.path.join(alloc, &.{ generation_root, "content" });
    errdefer alloc.free(content_root);
    try verifyManifestFiles(
        alloc,
        io,
        manifest_bytes,
        content_root,
        request,
        parsed.value.backup_lsn,
        parsed.value.checkpoint_lsn,
        parsed.value.file_count,
        parsed.value.total_bytes,
    );
    try verifyBackupEndAt(request.primary, alloc, parsed.value.end_record_lsn, manifest_bytes);
    try validateRetainedSlot(request, parsed.value.backup_lsn);
    if (request.binding != null) {
        var ledger = try lifecycle_receipt_ledger.Ledger.open(alloc, request.capture_root, .{});
        defer ledger.close();
        _ = try ledger.recordCapture(complete_json, .{ .pod_uid = request.pod_uid });
    }

    return .{
        .generation_root = try alloc.dupe(u8, generation_root),
        .content_root = content_root,
        .manifest_path = manifest_path,
        .manifest_bytes = manifest_bytes,
        .receipt_json = complete_json,
        .backup_lsn = parsed.value.backup_lsn,
        .checkpoint_lsn = parsed.value.checkpoint_lsn,
        .end_record_lsn = parsed.value.end_record_lsn,
        .already_captured = already_captured,
    };
}

fn collectSources(alloc: Allocator, io: std.Io, request: CaptureRequest) !std.ArrayListUnmanaged(OwnedSource) {
    var out = std.ArrayListUnmanaged(OwnedSource).empty;
    errdefer {
        for (out.items) |*source| source.deinit(alloc);
        out.deinit(alloc);
    }
    for (request.sources) |source| switch (source) {
        .file => |file| {
            const stat = try std.Io.Dir.cwd().statFile(io, file.source_path, .{ .follow_symlinks = false });
            if (stat.kind != .file) return error.UnsupportedCaptureSource;
            try out.append(alloc, .{
                .source_path = try alloc.dupe(u8, file.source_path),
                .artifact_path = try alloc.dupe(u8, file.artifact_path),
                .kind = file.kind,
            });
        },
        .tree => |tree| {
            var dir = try std.Io.Dir.cwd().openDir(io, tree.source_root, .{ .iterate = true });
            defer dir.close(io);
            var walker = try dir.walk(alloc);
            defer walker.deinit();
            while (try walker.next(io)) |entry| {
                if (entry.kind == .directory) continue;
                if (entry.kind != .file) return error.UnsupportedCaptureSource;
                const source_path = try std.fs.path.join(alloc, &.{ tree.source_root, entry.path });
                errdefer alloc.free(source_path);
                const artifact_path = if (tree.artifact_prefix.len == 0)
                    try alloc.dupe(u8, entry.path)
                else
                    try std.fs.path.join(alloc, &.{ tree.artifact_prefix, entry.path });
                errdefer alloc.free(artifact_path);
                try out.append(alloc, .{
                    .source_path = source_path,
                    .artifact_path = artifact_path,
                    .kind = tree.kind,
                });
            }
        },
    };
    return out;
}

const CopiedMetadata = struct { size_bytes: u64, crc32: u32 };

fn copyAndChecksum(io: std.Io, source: []const u8, destination: []const u8, sync_root: []const u8, max_bytes: u64) !CopiedMetadata {
    const stat = try std.Io.Dir.cwd().statFile(io, source, .{ .follow_symlinks = false });
    if (stat.kind != .file) return error.UnsupportedCaptureSource;
    if (stat.size > max_bytes) return error.CaptureFileTooLarge;
    try std.Io.Dir.copyFile(std.Io.Dir.cwd(), source, std.Io.Dir.cwd(), destination, io, .{
        .make_path = true,
        .replace = false,
    });
    try fs_paths.syncFileAndParentPortable(io, destination);
    try syncParents(io, destination, sync_root);
    return try checksumFile(io, destination, max_bytes);
}

fn checksumFile(io: std.Io, path: []const u8, max_bytes: u64) !CopiedMetadata {
    var file = try std.Io.Dir.cwd().openFile(io, path, .{});
    defer file.close(io);
    var reader = file.reader(io, &.{});
    var buffer: [64 * 1024]u8 = undefined;
    var crc = Crc32.init();
    var size: u64 = 0;
    while (true) {
        const read = try reader.interface.readSliceShort(&buffer);
        if (read == 0) break;
        size = try std.math.add(u64, size, read);
        if (size > max_bytes) return error.CaptureFileTooLarge;
        crc.update(buffer[0..read]);
    }
    return .{ .size_bytes = size, .crc32 = crc.final() };
}

fn verifyManifestFiles(
    alloc: Allocator,
    io: std.Io,
    manifest_bytes: []const u8,
    content_root: []const u8,
    request: CaptureRequest,
    expected_backup_lsn: u64,
    expected_checkpoint_lsn: u64,
    expected_count: usize,
    expected_total: u64,
) !void {
    const manifest = try backup_manifest.decodeAlloc(alloc, manifest_bytes);
    defer backup_manifest.freeDecoded(alloc, manifest);
    try expectIdentity(request.primary.identity, manifest.identity, error.CaptureIdentityMismatch);
    if (!std.mem.eql(u8, manifest.manifest_id, request.generation)) return error.CaptureGenerationMismatch;
    if (manifest.backup_lsn != expected_backup_lsn or manifest.checkpoint_lsn != expected_checkpoint_lsn or
        manifest.backup_lsn == 0 or manifest.checkpoint_lsn != manifest.backup_lsn) return error.CaptureCheckpointMismatch;
    if (manifest.files.len != expected_count) return error.CaptureFileSetMismatch;

    var total: u64 = 0;
    for (manifest.files) |entry| {
        const path = try std.fs.path.join(alloc, &.{ content_root, entry.path });
        defer alloc.free(path);
        const actual = try checksumFile(io, path, request.limits.max_file_bytes);
        if (actual.size_bytes != entry.size_bytes) return error.CaptureFileSizeMismatch;
        if (actual.crc32 != entry.crc32) return error.CaptureFileChecksumMismatch;
        total = try std.math.add(u64, total, actual.size_bytes);
    }
    if (total != expected_total or total > request.limits.max_total_bytes) return error.CaptureSizeMismatch;

    var dir = try std.Io.Dir.cwd().openDir(io, content_root, .{ .iterate = true });
    defer dir.close(io);
    var walker = try dir.walk(alloc);
    defer walker.deinit();
    var seen: usize = 0;
    while (try walker.next(io)) |entry| {
        if (entry.kind == .directory) continue;
        if (entry.kind != .file) return error.UnsupportedCaptureSource;
        if (manifest.fileIndex(entry.path) == null) return error.CaptureFileSetMismatch;
        seen += 1;
    }
    if (seen != manifest.files.len) return error.CaptureFileSetMismatch;
}

fn ensureBackupEnd(
    alloc: Allocator,
    primary: *primary_mod.Primary,
    manifest_bytes: []const u8,
    manifest_id: []const u8,
    backup_lsn: u64,
) !u64 {
    const entries = try primary.log.iterateFrom(alloc, backup_lsn + 1);
    defer replication_log.freeEntries(alloc, entries);
    for (entries) |entry| {
        if (entry.record.kind != .backup_end or entry.record.payload_codec != .binary) continue;
        if (std.mem.eql(u8, entry.record.payload, manifest_bytes)) return entry.wal_lsn;
        const candidate = backup_manifest.decodeAlloc(alloc, entry.record.payload) catch continue;
        defer backup_manifest.freeDecoded(alloc, candidate);
        if (candidate.backup_lsn == backup_lsn and std.mem.eql(u8, candidate.manifest_id, manifest_id)) {
            return error.CaptureManifestConflict;
        }
    }
    const manifest = try backup_manifest.decodeAlloc(alloc, manifest_bytes);
    defer backup_manifest.freeDecoded(alloc, manifest);
    const ended = try primary.endBaseBackup(.{
        .identity = manifest.identity,
        .manifest_id = manifest.manifest_id,
        .backup_lsn = manifest.backup_lsn,
        .checkpoint_lsn = manifest.checkpoint_lsn,
        .files = manifest.files,
        .flags = manifest.flags,
    });
    return ended.end_record_lsn;
}

fn verifyBackupEndAt(primary: *primary_mod.Primary, alloc: Allocator, lsn: u64, manifest_bytes: []const u8) !void {
    var entry = (try primary.log.entryAt(alloc, lsn)) orelse return error.CaptureBackupEndMissing;
    defer entry.deinit(alloc);
    if (entry.record.kind != .backup_end or entry.record.payload_codec != .binary or
        !std.mem.eql(u8, entry.record.payload, manifest_bytes)) return error.CaptureBackupEndMismatch;
}

fn validateRetainedSlot(request: CaptureRequest, backup_lsn: u64) !void {
    const slot = request.primary.slot(request.slot_name) orelse return error.CaptureSlotMissing;
    if (slot.lifecycle != .seeding or slot.active or slot.reseed_required or
        slot.timeline_id != request.primary.identity.timeline_id or slot.restart_lsn != backup_lsn)
        return error.CaptureSlotNotRetained;
}

fn captureReceiptFormatVersion(request: CaptureRequest) u16 {
    return if (request.binding != null) format_version else legacy_format_version;
}

fn validateCaptureBinding(binding: seed_artifact.LifecycleBinding) !void {
    if (!validation.isIdentifier(binding.topology_id) or binding.topology_generation == 0 or
        !validation.isIdentifier(binding.node_id) or !validation.isIdentifier(binding.target_pvc_name) or
        !validation.isIdentifier(binding.target_pvc_uid)) return error.InvalidCaptureBinding;
}

fn expectCaptureBinding(
    expected: ?seed_artifact.LifecycleBinding,
    topology_id: []const u8,
    topology_generation: u64,
    node_id: []const u8,
    target_pvc_name: []const u8,
    target_pvc_uid: []const u8,
) !void {
    const binding = expected orelse {
        if (topology_id.len != 0 or topology_generation != 0 or node_id.len != 0 or
            target_pvc_name.len != 0 or target_pvc_uid.len != 0) return error.CaptureBindingMismatch;
        return;
    };
    if (!std.mem.eql(u8, binding.topology_id, topology_id) or
        binding.topology_generation != topology_generation or
        !std.mem.eql(u8, binding.node_id, node_id) or
        !std.mem.eql(u8, binding.target_pvc_name, target_pvc_name) or
        !std.mem.eql(u8, binding.target_pvc_uid, target_pvc_uid)) return error.CaptureBindingMismatch;
}

fn validateRequest(request: CaptureRequest) !void {
    if (!validation.isIdentifier(request.generation)) return error.InvalidCaptureGeneration;
    if (!validation.isIdentifier(request.slot_name)) return error.InvalidSlotName;
    if (!validAbsoluteRoot(request.capture_root) or std.mem.eql(u8, request.capture_root, "/")) return error.InvalidCaptureRoot;
    if (request.sources.len == 0) return error.EmptyCapture;
    if (request.binding) |binding| try validateCaptureBinding(binding);
    if (request.pod_uid) |pod_uid| if (!validation.isIdentifier(pod_uid)) return error.InvalidCapturePodUID;
    try standby_mod.validateIdentity(request.primary.identity);
    for (request.sources) |source| switch (source) {
        .file => |file| {
            if (!validation.isAbsoluteNormalizedPath(file.source_path)) return error.InvalidCaptureSource;
            if (!validArtifactPath(file.artifact_path)) return error.InvalidArtifactPath;
            if (pathContains(request.capture_root, file.source_path)) return error.OverlappingCapturePaths;
        },
        .tree => |tree| {
            if (!validAbsoluteRoot(tree.source_root)) return error.InvalidCaptureSource;
            if (tree.artifact_prefix.len > 0 and !validArtifactPath(tree.artifact_prefix)) return error.InvalidArtifactPath;
            if (pathsOverlap(request.capture_root, tree.source_root)) return error.OverlappingCapturePaths;
        },
    };
}

fn validAbsoluteRoot(path: []const u8) bool {
    return path.len > 1 and validation.isAbsoluteNormalizedPath(path) and path[path.len - 1] != std.fs.path.sep;
}

fn validArtifactPath(path: []const u8) bool {
    if (std.fs.path.isAbsolute(path) or !validation.isNormalizedPath(path)) return false;
    backup_manifest.validatePath(path) catch return false;
    return true;
}

fn pathsOverlap(a: []const u8, b: []const u8) bool {
    return pathContains(a, b) or pathContains(b, a);
}

fn pathContains(parent: []const u8, child: []const u8) bool {
    if (std.mem.eql(u8, parent, child)) return true;
    return std.mem.startsWith(u8, child, parent) and child.len > parent.len and child[parent.len] == std.fs.path.sep;
}

fn sourcePlanDigest(sources: []const Source) [Sha256.digest_length]u8 {
    var hash = Sha256.init(.{});
    for (sources) |source| switch (source) {
        .file => |file| {
            hash.update("file\x00");
            hashFramed(&hash, file.source_path);
            hashFramed(&hash, file.artifact_path);
            hashInt(&hash, @intFromEnum(file.kind));
        },
        .tree => |tree| {
            hash.update("tree\x00");
            hashFramed(&hash, tree.source_root);
            hashFramed(&hash, tree.artifact_prefix);
            hashInt(&hash, @intFromEnum(tree.kind));
        },
    };
    var out: [Sha256.digest_length]u8 = undefined;
    hash.final(&out);
    return out;
}

fn hashFramed(hash: *Sha256, bytes: []const u8) void {
    var length: [8]u8 = undefined;
    std.mem.writeInt(u64, &length, bytes.len, .little);
    hash.update(&length);
    hash.update(bytes);
}

fn hashInt(hash: *Sha256, value: u16) void {
    var encoded: [2]u8 = undefined;
    std.mem.writeInt(u16, &encoded, value, .little);
    hash.update(&encoded);
}

fn parsePrepared(alloc: Allocator, raw: []const u8, request: CaptureRequest, plan_hex: []const u8) !PreparedReceipt {
    var parsed = std.json.parseFromSlice(PreparedReceipt, alloc, raw, .{ .ignore_unknown_fields = false }) catch return error.InvalidCaptureReceipt;
    defer parsed.deinit();
    const value = parsed.value;
    if (value.format_version != captureReceiptFormatVersion(request) or
        !std.mem.eql(u8, value.generation, request.generation) or
        !std.mem.eql(u8, value.slot_name, request.slot_name) or
        !std.mem.eql(u8, value.source_plan_sha256, plan_hex)) return error.CaptureStateConflict;
    try expectCaptureBinding(request.binding, value.topology_id, value.topology_generation, value.node_id, value.target_pvc_name, value.target_pvc_uid);
    try expectReceiptIdentity(request.primary.identity, value.cluster_id, value.shard_id, value.table_id, value.timeline_id, value.epoch);
    return .{
        .format_version = value.format_version,
        .generation = request.generation,
        .slot_name = request.slot_name,
        .cluster_id = value.cluster_id,
        .shard_id = value.shard_id,
        .table_id = value.table_id,
        .timeline_id = value.timeline_id,
        .epoch = value.epoch,
        .source_plan_sha256 = plan_hex,
        .backup_lsn = value.backup_lsn,
        .topology_id = value.topology_id,
        .topology_generation = value.topology_generation,
        .node_id = value.node_id,
        .target_pvc_name = value.target_pvc_name,
        .target_pvc_uid = value.target_pvc_uid,
    };
}

fn validateSnapshotReceipt(value: SnapshotReceipt, request: CaptureRequest, plan_hex: []const u8) !void {
    if (value.format_version != captureReceiptFormatVersion(request) or
        !std.mem.eql(u8, value.generation, request.generation) or
        !std.mem.eql(u8, value.slot_name, request.slot_name) or
        !std.mem.eql(u8, value.source_plan_sha256, plan_hex) or
        !std.mem.eql(u8, value.manifest_id, request.generation)) return error.CaptureStateConflict;
    try expectCaptureBinding(request.binding, value.topology_id, value.topology_generation, value.node_id, value.target_pvc_name, value.target_pvc_uid);
    if (value.backup_lsn == 0 or value.checkpoint_lsn != value.backup_lsn) return error.CaptureCheckpointMismatch;
    if (value.file_count == 0 or value.file_count > request.limits.max_files or value.total_bytes > request.limits.max_total_bytes)
        return error.InvalidCaptureReceipt;
    try expectReceiptIdentity(request.primary.identity, value.cluster_id, value.shard_id, value.table_id, value.timeline_id, value.epoch);
}

fn validateCompleteReceipt(value: CaptureReceipt, request: CaptureRequest, plan_hex: []const u8) !void {
    if (value.format_version != captureReceiptFormatVersion(request) or
        !std.mem.eql(u8, value.generation, request.generation) or
        !std.mem.eql(u8, value.slot_name, request.slot_name) or
        !std.mem.eql(u8, value.source_plan_sha256, plan_hex) or
        !std.mem.eql(u8, value.manifest_id, request.generation)) return error.CaptureStateConflict;
    try expectCaptureBinding(request.binding, value.topology_id, value.topology_generation, value.node_id, value.target_pvc_name, value.target_pvc_uid);
    if (value.backup_lsn == 0 or value.checkpoint_lsn != value.backup_lsn or value.end_record_lsn <= value.backup_lsn)
        return error.CaptureCheckpointMismatch;
    if (value.file_count == 0 or value.file_count > request.limits.max_files or value.total_bytes > request.limits.max_total_bytes)
        return error.InvalidCaptureReceipt;
    try expectIdentity(request.primary.identity, value.identity(), error.CaptureIdentityMismatch);
}

fn validateAbortedReceipt(alloc: Allocator, raw: []const u8, request: CaptureRequest, plan_hex: []const u8) !void {
    var parsed = std.json.parseFromSlice(AbortedReceipt, alloc, raw, .{ .ignore_unknown_fields = false }) catch return error.InvalidCaptureReceipt;
    defer parsed.deinit();
    const value = parsed.value;
    if (value.format_version != captureReceiptFormatVersion(request) or
        !std.mem.eql(u8, value.generation, request.generation) or
        !std.mem.eql(u8, value.slot_name, request.slot_name) or
        !std.mem.eql(u8, value.source_plan_sha256, plan_hex) or
        !std.mem.eql(u8, value.reason, "snapshot_not_durable")) return error.CaptureStateConflict;
}

fn expectReceiptIdentity(expected: standby_mod.Identity, cluster_id: u64, shard_id: u64, table_id: u64, timeline_id: u64, epoch: u64) !void {
    try expectIdentity(expected, .{
        .cluster_id = cluster_id,
        .shard_id = shard_id,
        .table_id = table_id,
        .timeline_id = timeline_id,
        .epoch = epoch,
    }, error.CaptureIdentityMismatch);
}

fn expectIdentity(expected: standby_mod.Identity, actual: standby_mod.Identity, mismatch: anyerror) !void {
    if (expected.cluster_id != actual.cluster_id or expected.shard_id != actual.shard_id or
        expected.table_id != actual.table_id or expected.timeline_id != actual.timeline_id or expected.epoch != actual.epoch)
        return mismatch;
}

fn abortIncomplete(
    alloc: Allocator,
    io: std.Io,
    request: CaptureRequest,
    staging_root: []const u8,
    aborted_path: []const u8,
    plan_hex: []const u8,
) !void {
    try std.Io.Dir.cwd().deleteTree(io, staging_root);
    try fs_paths.createDirPathPortable(io, request.capture_root);
    const body = try std.json.Stringify.valueAlloc(alloc, AbortedReceipt{
        .format_version = captureReceiptFormatVersion(request),
        .generation = request.generation,
        .slot_name = request.slot_name,
        .source_plan_sha256 = plan_hex,
    }, .{});
    defer alloc.free(body);
    _ = try writeImmutableFile(io, alloc, aborted_path, body, error.CaptureStateConflict);
    try fs_paths.syncDirPortable(io, request.capture_root);
    try dropAbortedSlotIfPresent(request);
}

fn dropAbortedSlotIfPresent(request: CaptureRequest) !void {
    const slot = request.primary.slot(request.slot_name) orelse return;
    if (slot.lifecycle != .seeding or slot.active or slot.reseed_required) return error.CaptureSlotNotRetained;
    // Confirm that the retained backup_start belongs to this generation. A
    // stale ABORTED marker must never drop a newer capture using the same slot.
    _ = try request.primary.beginBaseBackup(.{
        .slot_name = request.slot_name,
        .manifest_id = request.generation,
    });
    try request.primary.dropSlot(request.slot_name);
}

fn syncParents(io: std.Io, path: []const u8, root: []const u8) !void {
    var parent = std.fs.path.dirname(path) orelse return error.InvalidCapturePath;
    while (!std.mem.eql(u8, parent, root)) {
        if (!pathContains(root, parent)) return error.InvalidCapturePath;
        try fs_paths.syncDirPortable(io, parent);
        parent = std.fs.path.dirname(parent) orelse return error.InvalidCapturePath;
    }
    try fs_paths.syncDirPortable(io, root);
}

fn writeImmutableFile(io: std.Io, alloc: Allocator, path: []const u8, body: []const u8, conflict: anyerror) !bool {
    var atomic_file = try std.Io.Dir.cwd().createFileAtomic(io, path, .{ .make_path = false, .replace = false });
    defer atomic_file.deinit(io);
    try atomic_file.file.writeStreamingAll(io, body);
    try atomic_file.file.sync(io);
    atomic_file.link(io) catch |err| switch (err) {
        error.PathAlreadyExists => {
            const existing = readFileAlloc(io, alloc, path, @max(body.len +| 1, 1024 * 1024)) catch |read_err| switch (read_err) {
                error.StreamTooLong => return conflict,
                else => return read_err,
            };
            defer alloc.free(existing);
            if (!std.mem.eql(u8, existing, body)) return conflict;
            return false;
        },
        else => return err,
    };
    try fs_paths.syncDirPortable(io, std.fs.path.dirname(path) orelse return error.InvalidCapturePath);
    return true;
}

fn readFileAlloc(io: std.Io, alloc: Allocator, path: []const u8, max_bytes: usize) ![]u8 {
    return try std.Io.Dir.cwd().readFileAlloc(io, path, alloc, .limited(max_bytes));
}

fn directoryExists(io: std.Io, path: []const u8) !bool {
    var dir = std.Io.Dir.cwd().openDir(io, path, .{}) catch |err| switch (err) {
        error.FileNotFound => return false,
        error.NotDir => return error.InvalidCaptureState,
        else => return err,
    };
    defer dir.close(io);
    return true;
}

fn fileExists(io: std.Io, path: []const u8) !bool {
    std.Io.Dir.cwd().access(io, path, .{}) catch |err| switch (err) {
        error.FileNotFound => return false,
        else => return err,
    };
    return true;
}

fn expectSha256(body: []const u8, expected_hex: []const u8, mismatch: anyerror) !void {
    if (expected_hex.len != Sha256.digest_length * 2) return mismatch;
    var digest: [Sha256.digest_length]u8 = undefined;
    Sha256.hash(body, &digest, .{});
    var encoded: [Sha256.digest_length * 2]u8 = undefined;
    encodeHex(&encoded, &digest);
    if (!std.mem.eql(u8, &encoded, expected_hex)) return mismatch;
}

fn encodeHex(out: []u8, bytes: []const u8) void {
    std.debug.assert(out.len == bytes.len * 2);
    for (bytes, 0..) |byte, index| {
        out[index * 2] = std.fmt.digitToChar(byte >> 4, .lower);
        out[index * 2 + 1] = std.fmt.digitToChar(byte & 0x0f, .lower);
    }
}

fn sourceLessThan(_: void, a: OwnedSource, b: OwnedSource) bool {
    return std.mem.order(u8, a.artifact_path, b.artifact_path) == .lt;
}

fn failAt(options: CaptureOptions, boundary: FailureBoundary) !void {
    if (options.fail_after == boundary) return error.InjectedCaptureFailure;
}

fn testIdentity() standby_mod.Identity {
    return .{ .cluster_id = 10, .shard_id = 20, .table_id = 30, .timeline_id = 2, .epoch = 4 };
}

const TestPaths = struct {
    log: [:0]u8,
    slots: [:0]u8,

    fn deinit(self: TestPaths, alloc: Allocator) void {
        alloc.free(self.log);
        alloc.free(self.slots);
    }
};

fn testPrimaryPaths(alloc: Allocator, root: []const u8, label: []const u8) !TestPaths {
    const log_raw = try std.fmt.allocPrint(alloc, "{s}/{s}.wal", .{ root, label });
    defer alloc.free(log_raw);
    const slots_raw = try std.fmt.allocPrint(alloc, "{s}/{s}.slots", .{ root, label });
    defer alloc.free(slots_raw);
    return .{
        .log = try alloc.dupeZ(u8, log_raw),
        .slots = try alloc.dupeZ(u8, slots_raw),
    };
}

fn writeTestFile(path: []const u8, body: []const u8) !void {
    if (std.fs.path.dirname(path)) |parent| try fs_paths.createDirPathPortable(std.testing.io, parent);
    var file = try std.Io.Dir.cwd().createFile(std.testing.io, path, .{ .truncate = true });
    defer file.close(std.testing.io);
    try file.writeStreamingAll(std.testing.io, body);
    try file.sync(std.testing.io);
}

const BarrierProbe = struct {
    before: std.atomic.Value(bool) = .init(false),
    exclusive: std.atomic.Value(bool) = .init(false),
    allow_copy: std.atomic.Value(bool) = .init(false),

    fn beforeHook(raw: ?*anyopaque) void {
        const self: *BarrierProbe = @ptrCast(@alignCast(raw.?));
        self.before.store(true, .release);
    }

    fn afterHook(raw: ?*anyopaque) void {
        const self: *BarrierProbe = @ptrCast(@alignCast(raw.?));
        self.exclusive.store(true, .release);
        while (!self.allow_copy.load(.acquire)) std.Thread.yield() catch {};
    }
};

const CaptureWorker = struct {
    alloc: Allocator,
    request: CaptureRequest,
    options: CaptureOptions,
    result: ?CaptureResult = null,
    capture_error: ?anyerror = null,

    fn run(self: *CaptureWorker) void {
        self.result = captureWithOptions(self.alloc, self.request, self.options) catch |err| {
            self.capture_error = err;
            return;
        };
    }
};

fn waitFor(flag: *const std.atomic.Value(bool)) !void {
    var attempts: usize = 0;
    while (!flag.load(.acquire)) : (attempts += 1) {
        if (attempts > 1_000_000) return error.TestTimedOut;
        std.Thread.yield() catch {};
    }
}

test "storage.ha seed capture waits for in-flight mutation and excludes post-checkpoint state" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(std.testing.io, ".", alloc);
    defer alloc.free(root);
    const paths = try testPrimaryPaths(alloc, root, "capture-barrier");
    defer paths.deinit(alloc);
    var primary = try primary_mod.Primary.open(alloc, paths.log.ptr, paths.slots.ptr, testIdentity(), .{});
    defer primary.close();
    var barrier: mutation_barrier.MutationBarrier = .{};

    const source_root = try std.fs.path.join(alloc, &.{ root, "live" });
    defer alloc.free(source_root);
    const source_path = try std.fs.path.join(alloc, &.{ source_root, "catalog/state" });
    defer alloc.free(source_path);
    const capture_root = try std.fs.path.join(alloc, &.{ root, "captures" });
    defer alloc.free(capture_root);
    try writeTestFile(source_path, "before");

    var mutation = barrier.acquireShared();
    try writeTestFile(source_path, "committed");
    const committed_lsn = try primary.append(.{ .payload = "committed" });

    const sources = [_]Source{.{ .tree = .{
        .source_root = source_root,
        .artifact_prefix = "database",
        .kind = .metadata,
    } }};
    var probe = BarrierProbe{};
    var worker = CaptureWorker{
        .alloc = alloc,
        .request = .{
            .primary = &primary,
            .barrier = &barrier,
            .slot_name = "standby-a",
            .generation = "gen-barrier",
            .capture_root = capture_root,
            .sources = &sources,
        },
        .options = .{ .hooks = .{
            .context = &probe,
            .before_exclusive = BarrierProbe.beforeHook,
            .after_exclusive = BarrierProbe.afterHook,
        } },
    };
    const thread = try std.Thread.spawn(.{}, CaptureWorker.run, .{&worker});
    try waitFor(&probe.before);
    try std.testing.expect(!probe.exclusive.load(.acquire));
    mutation.release();

    try waitFor(&probe.exclusive);
    try std.testing.expect(barrier.tryAcquireShared() == null);
    probe.allow_copy.store(true, .release);
    thread.join();
    if (worker.capture_error) |err| return err;
    var result = worker.result orelse return error.TestExpectedEqual;
    defer result.deinit(alloc);

    var post = barrier.acquireShared();
    try writeTestFile(source_path, "post-checkpoint");
    const post_lsn = try primary.append(.{ .payload = "post-checkpoint" });
    post.release();

    const captured_path = try std.fs.path.join(alloc, &.{ result.content_root, "database/catalog/state" });
    defer alloc.free(captured_path);
    const captured = try readFileAlloc(std.testing.io, alloc, captured_path, 128);
    defer alloc.free(captured);
    try std.testing.expectEqualStrings("committed", captured);
    const manifest = try backup_manifest.decodeAlloc(alloc, result.manifest_bytes);
    defer backup_manifest.freeDecoded(alloc, manifest);
    try std.testing.expectEqual(committed_lsn + 1, manifest.checkpoint_lsn);
    try std.testing.expectEqual(manifest.checkpoint_lsn + 1, result.end_record_lsn);
    try std.testing.expectEqual(result.end_record_lsn + 1, post_lsn);
}

test "storage.ha seed capture resumes every durable local crash boundary without duplicate backup end" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(std.testing.io, ".", alloc);
    defer alloc.free(root);
    const source_path = try std.fs.path.join(alloc, &.{ root, "live/catalog" });
    defer alloc.free(source_path);
    try writeTestFile(source_path, "catalog-v1");
    const sources = [_]Source{.{ .file = .{
        .source_path = source_path,
        .artifact_path = "catalog/manifest",
        .kind = .manifest,
    } }};

    inline for (.{
        FailureBoundary.snapshot_durable,
        FailureBoundary.backup_end_durable,
        FailureBoundary.completion_durable,
    }, 0..) |boundary, index| {
        const label = try std.fmt.allocPrint(alloc, "capture-crash-{d}", .{index});
        defer alloc.free(label);
        const paths = try testPrimaryPaths(alloc, root, label);
        defer paths.deinit(alloc);
        var primary = try primary_mod.Primary.open(alloc, paths.log.ptr, paths.slots.ptr, testIdentity(), .{});
        defer primary.close();
        var barrier: mutation_barrier.MutationBarrier = .{};
        const capture_name = try std.fmt.allocPrint(alloc, "captures-{d}", .{index});
        defer alloc.free(capture_name);
        const capture_root = try std.fs.path.join(alloc, &.{ root, capture_name });
        defer alloc.free(capture_root);
        const generation = try std.fmt.allocPrint(alloc, "gen-{d}", .{index});
        defer alloc.free(generation);
        const request = CaptureRequest{
            .primary = &primary,
            .barrier = &barrier,
            .slot_name = "standby-a",
            .generation = generation,
            .capture_root = capture_root,
            .sources = &sources,
        };

        try std.testing.expectError(error.InjectedCaptureFailure, captureWithOptions(alloc, request, .{ .fail_after = boundary }));
        var recovered = try capture(alloc, request);
        defer recovered.deinit(alloc);
        try std.testing.expect(!recovered.already_captured);
        try std.testing.expectEqual(@as(u64, 2), recovered.end_record_lsn);
        try std.testing.expectEqual(@as(u64, 2), primary.lastLsn());

        var repeated = try capture(alloc, request);
        defer repeated.deinit(alloc);
        try std.testing.expect(repeated.already_captured);
        try std.testing.expectEqual(recovered.end_record_lsn, repeated.end_record_lsn);
        try std.testing.expectEqual(@as(u64, 2), primary.lastLsn());
    }
}

test "storage.ha seed capture burns a generation whose local snapshot was incomplete" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(std.testing.io, ".", alloc);
    defer alloc.free(root);
    const paths = try testPrimaryPaths(alloc, root, "capture-abort");
    defer paths.deinit(alloc);
    var primary = try primary_mod.Primary.open(alloc, paths.log.ptr, paths.slots.ptr, testIdentity(), .{});
    defer primary.close();
    var barrier: mutation_barrier.MutationBarrier = .{};
    const missing = try std.fs.path.join(alloc, &.{ root, "missing" });
    defer alloc.free(missing);
    const capture_root = try std.fs.path.join(alloc, &.{ root, "captures-abort" });
    defer alloc.free(capture_root);
    const sources = [_]Source{.{ .file = .{
        .source_path = missing,
        .artifact_path = "catalog/manifest",
    } }};
    const request = CaptureRequest{
        .primary = &primary,
        .barrier = &barrier,
        .slot_name = "standby-a",
        .generation = "gen-aborted",
        .capture_root = capture_root,
        .sources = &sources,
    };
    try std.testing.expectError(error.FileNotFound, capture(alloc, request));
    try std.testing.expect(primary.slot("standby-a") == null);
    try std.testing.expectError(error.CaptureGenerationAborted, capture(alloc, request));
}

test "storage.ha seed capture gc requires a remotely verified v2 COMPLETE checkpoint" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(std.testing.io, ".", alloc);
    defer alloc.free(root);
    const paths = try testPrimaryPaths(alloc, root, "capture-publish-gc");
    defer paths.deinit(alloc);
    var primary = try primary_mod.Primary.open(alloc, paths.log.ptr, paths.slots.ptr, testIdentity(), .{});
    defer primary.close();
    var barrier: mutation_barrier.MutationBarrier = .{};
    const source_path = try std.fs.path.join(alloc, &.{ root, "live/catalog" });
    defer alloc.free(source_path);
    try writeTestFile(source_path, "catalog-v1");
    const capture_root = try std.fs.path.join(alloc, &.{ root, "captures" });
    defer alloc.free(capture_root);
    const sources = [_]Source{.{ .file = .{
        .source_path = source_path,
        .artifact_path = "catalog/manifest",
        .kind = .manifest,
    } }};
    var captured = try capture(alloc, .{
        .primary = &primary,
        .barrier = &barrier,
        .slot_name = "standby-a",
        .generation = "gen-published",
        .capture_root = capture_root,
        .sources = &sources,
    });
    defer captured.deinit(alloc);

    var memory = object_storage.MemoryObjectStorage.init(alloc);
    defer memory.deinit();
    var client = memory.client();
    try client.makeBucket("ha-seeds");
    const store = seed_artifact.Store{ .client = &client, .bucket = "ha-seeds" };
    try std.testing.expectError(error.IncompleteSeedArtifact, prunePublishedGenerations(alloc, .{
        .store = store,
        .capture_root = capture_root,
        .generation = "gen-published",
        .slot_name = "standby-a",
        .retain_generations = 1,
    }));
    const marker_path = try std.fs.path.join(alloc, &.{ captured.generation_root, local_generation_gc.marker_name });
    defer alloc.free(marker_path);
    try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().access(std.testing.io, marker_path, .{}));

    var published = try seed_artifact.publish(alloc, store, .{
        .generation = "gen-published",
        .slot_name = "standby-a",
        .manifest_bytes = captured.manifest_bytes,
        .content_root = captured.content_root,
        .limits = .{ .max_chunk_bytes = 4 },
    });
    defer published.deinit(alloc);
    var pruned = try prunePublishedGenerations(alloc, .{
        .store = store,
        .capture_root = capture_root,
        .generation = "gen-published",
        .slot_name = "standby-a",
        .retain_generations = 1,
        .artifact_limits = .{ .max_chunk_bytes = 4 },
    });
    defer pruned.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 0), pruned.deleted_generations);
    try std.Io.Dir.cwd().access(std.testing.io, marker_path, .{});
}

test "storage.ha seed capture v2 binds COMPLETE and journals publication before success" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(std.testing.io, ".", alloc);
    defer alloc.free(root);
    const paths = try testPrimaryPaths(alloc, root, "capture-bound");
    defer paths.deinit(alloc);
    var primary = try primary_mod.Primary.open(alloc, paths.log.ptr, paths.slots.ptr, testIdentity(), .{});
    defer primary.close();
    var barrier: mutation_barrier.MutationBarrier = .{};
    const source_path = try std.fs.path.join(alloc, &.{ root, "live/catalog" });
    defer alloc.free(source_path);
    try writeTestFile(source_path, "catalog-v2");
    const capture_root = try std.fs.path.join(alloc, &.{ root, "captures" });
    defer alloc.free(capture_root);
    const sources = [_]Source{.{ .file = .{
        .source_path = source_path,
        .artifact_path = "catalog/manifest",
        .kind = .manifest,
    } }};
    const binding = seed_artifact.LifecycleBinding{
        .topology_id = "topology-a",
        .topology_generation = 9,
        .node_id = "primary-a",
        .target_pvc_name = "standby-a-data",
        .target_pvc_uid = "pvc-uid-9",
    };
    const request = CaptureRequest{
        .primary = &primary,
        .barrier = &barrier,
        .slot_name = "standby-a",
        .generation = "capture-bound",
        .capture_root = capture_root,
        .sources = &sources,
        .binding = binding,
        .pod_uid = "pod-primary-1",
    };
    var captured = try capture(alloc, request);
    defer captured.deinit(alloc);
    var receipt = try std.json.parseFromSlice(CaptureReceipt, alloc, captured.receipt_json, .{ .ignore_unknown_fields = false });
    defer receipt.deinit();
    try std.testing.expectEqual(@as(u16, 2), receipt.value.format_version);
    try std.testing.expectEqualStrings(binding.topology_id, receipt.value.topology_id);
    try std.testing.expectEqual(binding.topology_generation, receipt.value.topology_generation);
    try std.testing.expectEqualStrings(binding.node_id, receipt.value.node_id);
    try std.testing.expectEqualStrings(binding.target_pvc_uid, receipt.value.target_pvc_uid);

    var ledger = try lifecycle_receipt_ledger.Ledger.open(alloc, capture_root, .{});
    var page = try ledger.readPage(alloc, .capture, .{ .limit = 10 }, .{ .authoritative_root = capture_root });
    try std.testing.expectEqual(@as(usize, 1), page.entries.len);
    try std.testing.expectEqualStrings(captured.receipt_json, page.entries[0].receipt_json);
    page.deinit(alloc);
    ledger.close();

    var changed_binding = binding;
    changed_binding.target_pvc_uid = "pvc-uid-other";
    var conflicting = request;
    conflicting.binding = changed_binding;
    try std.testing.expectError(error.CaptureBindingMismatch, capture(alloc, conflicting));

    var repeated = try capture(alloc, request);
    defer repeated.deinit(alloc);
    try std.testing.expect(repeated.already_captured);
    ledger = try lifecycle_receipt_ledger.Ledger.open(alloc, capture_root, .{});
    defer ledger.close();
    var after_retry = try ledger.readPage(alloc, .capture, .{ .limit = 10 }, .{ .authoritative_root = capture_root });
    defer after_retry.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), after_retry.entries.len);
}
