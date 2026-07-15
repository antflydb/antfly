// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the License at https://www.antfly.io/licensing/ELv2-license.

//! Crash-resumable, offline activation of a verified HA seed generation.
//!
//! Activation is deliberately a one-way publication protocol:
//!
//! 1. Verify the staged artifact and its manifest before touching the target.
//! 2. Copy it into an owned `.installing-*` directory on the target volume.
//! 3. Verify and fsync the copy, then atomically rename it to an immutable
//!    `generations/<generation>` directory.
//! 4. Materialize portable logical state into a separate mutable
//!    `live-generations/<generation>` runtime tree and publish that tree.
//! 5. Publish `ACTIVE.json` with an atomic no-replace operation.
//!
//! Until step 5 completes there is no active generation. A retry may rebuild
//! only an install directory carrying the exact expected activation receipt;
//! it never overwrites an already-published generation. The caller must keep
//! the Antfly runtime offline until activation succeeds and then open the
//! returned generation path as its data root.

const std = @import("std");
const Allocator = std.mem.Allocator;
const Sha256 = std.crypto.hash.sha2.Sha256;
const fs_paths = @import("../../common/fs_paths.zig");
const backup_manifest = @import("backup_manifest.zig");
const lifecycle_receipt_ledger = @import("lifecycle_receipt_ledger.zig");
const local_generation_gc = @import("local_generation_gc.zig");
const object_storage = @import("../object_storage.zig");
const seed_artifact = @import("seed_artifact.zig");
const seed_capture = @import("seed_capture.zig");
const seed_materialization = @import("seed_materialization.zig");
const standby_mod = @import("standby.zig");
const validation = @import("validation.zig");

pub const legacy_format_version: u16 = 1;
pub const format_version: u16 = 2;
pub const generations_dir_name = "generations";
pub const live_generations_dir_name = "live-generations";
pub const active_receipt_name = "ACTIVE.json";
pub const generation_receipt_name = ".antfly-ha-active-generation.json";

pub const ActivateRequest = struct {
    staging_root: []const u8,
    target_root: []const u8,
    expected: seed_artifact.ExpectedArtifact,
    binding: ?ActivationBinding = null,
    materialization: ?MaterializationTarget = null,
    pod_uid: ?[]const u8 = null,
    limits: seed_artifact.Limits = .{},
};

pub const MaterializationTarget = struct {
    target_local_node_id: u64,
    target_replica_id: u64 = 1,
};

pub const ActivationBinding = seed_artifact.LifecycleBinding;

pub const StartupExpectation = struct {
    target_root: []const u8,
    expected: seed_artifact.ExpectedArtifact,
    binding: ActivationBinding,
    manifest_sha256: ?[]const u8 = null,
    aggregate_sha256: ?[]const u8 = null,
    seed_receipt_sha256: ?[]const u8 = null,
    capture_receipt_sha256: ?[]const u8 = null,
    materialized_receipt_sha256: ?[]const u8 = null,
    materialized_aggregate_sha256: ?[]const u8 = null,
    target_local_node_id: ?u64 = null,
    target_replica_id: ?u64 = null,
    limits: seed_artifact.Limits = .{},
};

pub const ActivationResult = struct {
    /// Absolute path of the mutable, identity-validated generation that the
    /// runtime may open. The raw transport generation remains immutable.
    generation_path: []u8,
    active_receipt_json: []u8,
    already_active: bool,

    pub fn deinit(self: *ActivationResult, alloc: Allocator) void {
        alloc.free(self.generation_path);
        alloc.free(self.active_receipt_json);
        self.* = undefined;
    }
};

pub const ActivatedGenerationGCRequest = struct {
    target_root: []const u8,
    /// Durable controller-owned copy of HASeededSlotActivateResponse.
    slot_activation_receipt_path: []const u8,
    protected_generations: []const []const u8 = &.{},
    retain_generations: usize = 2,
    limits: seed_artifact.Limits = .{},
    max_local_generations: usize = 10_000,
};

pub const ActivationReceipt = struct {
    format_version: u16 = format_version,
    generation: []const u8,
    slot_name: []const u8,
    cluster_id: u64,
    shard_id: u64,
    table_id: u64,
    timeline_id: u64,
    epoch: u64,
    manifest_id: []const u8,
    backup_lsn: u64,
    checkpoint_lsn: u64,
    seed_receipt_sha256: []const u8,
    /// Digest of the exact runtime-owned capture COMPLETE bytes that
    /// authorized publication of the portable seed artifact.
    capture_receipt_sha256: []const u8 = "",
    manifest_sha256: []const u8,
    aggregate_sha256: []const u8,
    generation_path: []const u8,
    raw_generation_path: []const u8 = "",
    materialized_receipt_sha256: []const u8 = "",
    materialized_aggregate_sha256: []const u8 = "",
    target_local_node_id: u64 = 0,
    target_replica_id: u64 = 0,
    topology_id: []const u8 = "",
    topology_generation: u64 = 0,
    node_id: []const u8 = "",
    target_pvc_name: []const u8 = "",
    target_pvc_uid: []const u8 = "",

    pub fn identity(self: ActivationReceipt) standby_mod.Identity {
        return .{
            .cluster_id = self.cluster_id,
            .shard_id = self.shard_id,
            .table_id = self.table_id,
            .timeline_id = self.timeline_id,
            .epoch = self.epoch,
        };
    }
};

test "storage.ha activation schema preserves capture and seed receipt digest chain" {
    try std.testing.expectEqual(@as(u16, 2), format_version);
    try std.testing.expect(@hasField(ActivationReceipt, "capture_receipt_sha256"));
    try std.testing.expect(@hasField(StartupExpectation, "capture_receipt_sha256"));
    try std.testing.expect(@hasField(SeededSlotActivationCheckpoint, "capture_receipt_sha256"));
}

const FailureBoundary = enum {
    generation_copied,
    generation_published,
    live_generation_materialized,
    live_generation_published,
    active_published,
};

const ActivateOptions = struct {
    fail_after: ?FailureBoundary = null,
};

pub fn activate(alloc: Allocator, request: ActivateRequest) !ActivationResult {
    return activateWithOptions(alloc, request, .{});
}

const SeededSlotActivationAction = struct {
    action_id: []const u8,
    action_kind: []const u8,
    target: []const u8,
    state: []const u8,
    node_id: []const u8,
};

const SeededSlotActivationCheckpoint = struct {
    schema_version: i64,
    action: SeededSlotActivationAction,
    slot_name: []const u8,
    generation: []const u8,
    manifest_id: []const u8,
    timeline_id: i64,
    checkpoint_lsn: i64,
    seed_receipt_sha256: []const u8,
    capture_receipt_sha256: []const u8,
    manifest_sha256: []const u8,
    aggregate_sha256: []const u8,
};

const RawGenerationReceipt = struct {
    format_version: u16 = 1,
    generation: []const u8,
    slot_name: []const u8,
    seed_receipt_sha256: []const u8,
    capture_receipt_sha256: []const u8,
    manifest_sha256: []const u8,
    aggregate_sha256: []const u8,
    topology_id: []const u8,
    topology_generation: u64,
    node_id: []const u8,
    target_pvc_name: []const u8,
    target_pvc_uid: []const u8,
};

/// Grants target-volume deletion authority only after the runtime's immutable
/// ACTIVE evidence and the primary's durable seeded-slot activation response
/// are both present and agree field-for-field.
pub fn pruneActivatedGenerations(
    alloc: Allocator,
    request: ActivatedGenerationGCRequest,
) !local_generation_gc.PruneResult {
    if (!validAbsoluteRoot(request.target_root)) return error.InvalidActivationTarget;
    if (!validation.isAbsoluteNormalizedPath(request.slot_activation_receipt_path))
        return error.InvalidSeedActivationCheckpointPath;

    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    const checkpoint_stat = std.Io.Dir.cwd().statFile(io, request.slot_activation_receipt_path, .{ .follow_symlinks = false }) catch |err| switch (err) {
        error.FileNotFound => return error.SeedActivationCheckpointMissing,
        else => return err,
    };
    if (checkpoint_stat.kind != .file or checkpoint_stat.size > request.limits.max_receipt_bytes)
        return error.InvalidSeedActivationCheckpoint;
    const checkpoint_json = readFileAlloc(io, alloc, request.slot_activation_receipt_path, request.limits.max_receipt_bytes) catch
        return error.InvalidSeedActivationCheckpoint;
    defer alloc.free(checkpoint_json);
    var checkpoint = std.json.parseFromSlice(SeededSlotActivationCheckpoint, alloc, checkpoint_json, .{ .ignore_unknown_fields = false }) catch
        return error.InvalidSeedActivationCheckpoint;
    defer checkpoint.deinit();

    const active_path = try std.fs.path.join(alloc, &.{ request.target_root, active_receipt_name });
    defer alloc.free(active_path);
    const active_json = readFileAlloc(io, alloc, active_path, request.limits.max_receipt_bytes) catch |err| switch (err) {
        error.FileNotFound => return error.ActiveReceiptMissing,
        else => return err,
    };
    defer alloc.free(active_json);
    var active = std.json.parseFromSlice(ActivationReceipt, alloc, active_json, .{ .ignore_unknown_fields = false }) catch
        return error.InvalidActiveReceipt;
    defer active.deinit();
    try validateActiveForGC(alloc, active.value);
    try validateActivationCheckpoint(alloc, checkpoint.value, active.value);

    const active_binding = ActivationBinding{
        .topology_id = active.value.topology_id,
        .topology_generation = active.value.topology_generation,
        .node_id = active.value.node_id,
        .target_pvc_name = active.value.target_pvc_name,
        .target_pvc_uid = active.value.target_pvc_uid,
    };
    const active_request = ActivateRequest{
        .staging_root = "/unused",
        .target_root = request.target_root,
        .expected = .{
            .generation = active.value.generation,
            .slot_name = active.value.slot_name,
            .identity = active.value.identity(),
            .minimum_checkpoint_lsn = active.value.checkpoint_lsn,
            .binding = active_binding,
            .capture_receipt_sha256 = active.value.capture_receipt_sha256,
        },
        .binding = active_binding,
        .materialization = if (active.value.format_version == format_version) .{
            .target_local_node_id = active.value.target_local_node_id,
            .target_replica_id = active.value.target_replica_id,
        } else null,
        .limits = request.limits,
    };
    if (active.value.format_version == format_version) {
        const raw_root = try std.fs.path.join(alloc, &.{ request.target_root, active.value.raw_generation_path });
        defer alloc.free(raw_root);
        const live_root = try std.fs.path.join(alloc, &.{ request.target_root, active.value.generation_path });
        defer alloc.free(live_root);
        const raw_marker = try rawMarkerForActiveAlloc(alloc, active.value);
        defer alloc.free(raw_marker);
        try validatePublishedRawGeneration(alloc, raw_root, active_request, raw_marker);
        try seed_materialization.validateRuntimeIdentity(
            alloc,
            raw_root,
            live_root,
            active.value.generation,
            active.value.materialized_receipt_sha256,
        );
    } else {
        const generation_path = try std.fs.path.join(alloc, &.{ request.target_root, active.value.generation_path });
        defer alloc.free(generation_path);
        try validatePublishedGeneration(alloc, generation_path, active_request, active_json);
    }

    try local_generation_gc.markEligible(alloc, .{
        .root = request.target_root,
        .scope = .target_activation,
        .generation = active.value.generation,
        .slot_name = active.value.slot_name,
        .checkpoint_lsn = active.value.checkpoint_lsn,
        .checkpoint_bytes = checkpoint_json,
        .max_checkpoint_bytes = request.limits.max_receipt_bytes,
    });
    return try local_generation_gc.prune(alloc, .{
        .root = request.target_root,
        .scope = .target_activation,
        .slot_name = active.value.slot_name,
        .current_generation = active.value.generation,
        .protected_generations = request.protected_generations,
        .retain_generations = request.retain_generations,
        .max_entries = request.max_local_generations,
        .paired_generations_dir_name = if (active.value.format_version == format_version) live_generations_dir_name else null,
    });
}

fn validateActiveForGC(alloc: Allocator, receipt: ActivationReceipt) !void {
    if ((receipt.format_version != legacy_format_version and receipt.format_version != format_version) or
        !validation.isIdentifier(receipt.generation) or
        !validation.isIdentifier(receipt.slot_name) or
        receipt.manifest_id.len == 0 or
        receipt.backup_lsn == 0 or
        receipt.checkpoint_lsn < receipt.backup_lsn or
        !isCanonicalSha256(receipt.seed_receipt_sha256) or
        !isCanonicalSha256(receipt.capture_receipt_sha256) or
        !isCanonicalSha256(receipt.manifest_sha256) or
        !isCanonicalSha256(receipt.aggregate_sha256))
        return error.InvalidActiveReceipt;
    try standby_mod.validateIdentity(receipt.identity());
    const raw_expected_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ generations_dir_name, receipt.generation });
    defer alloc.free(raw_expected_path);
    if (receipt.format_version == format_version) {
        const live_expected_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ live_generations_dir_name, receipt.generation });
        defer alloc.free(live_expected_path);
        if (!std.mem.eql(u8, receipt.raw_generation_path, raw_expected_path) or
            !std.mem.eql(u8, receipt.generation_path, live_expected_path) or
            !isCanonicalSha256(receipt.materialized_receipt_sha256) or
            !isCanonicalSha256(receipt.materialized_aggregate_sha256) or
            receipt.target_local_node_id == 0 or receipt.target_replica_id == 0)
            return error.InvalidActiveReceipt;
        try validateBinding(.{
            .topology_id = receipt.topology_id,
            .topology_generation = receipt.topology_generation,
            .node_id = receipt.node_id,
            .target_pvc_name = receipt.target_pvc_name,
            .target_pvc_uid = receipt.target_pvc_uid,
        });
    } else if (!std.mem.eql(u8, receipt.generation_path, raw_expected_path)) {
        return error.InvalidActiveReceipt;
    }
}

fn validateActivationCheckpoint(
    alloc: Allocator,
    checkpoint: SeededSlotActivationCheckpoint,
    active: ActivationReceipt,
) !void {
    const expected_action_id = try std.fmt.allocPrint(alloc, "seeded_slot_activate:{s}", .{active.generation});
    defer alloc.free(expected_action_id);
    const state_valid = std.mem.eql(u8, checkpoint.action.state, "applied") or
        std.mem.eql(u8, checkpoint.action.state, "already_applied");
    if (checkpoint.schema_version != 1 or
        !std.mem.eql(u8, checkpoint.action.action_id, expected_action_id) or
        !std.mem.eql(u8, checkpoint.action.action_kind, "seeded_slot_activate") or
        !std.mem.eql(u8, checkpoint.action.target, active.generation) or
        !validation.isIdentifier(checkpoint.action.node_id) or
        !state_valid or
        !std.mem.eql(u8, checkpoint.slot_name, active.slot_name) or
        !std.mem.eql(u8, checkpoint.generation, active.generation) or
        !std.mem.eql(u8, checkpoint.manifest_id, active.manifest_id) or
        checkpoint.timeline_id <= 0 or @as(u64, @intCast(checkpoint.timeline_id)) != active.timeline_id or
        checkpoint.checkpoint_lsn <= 0 or @as(u64, @intCast(checkpoint.checkpoint_lsn)) != active.checkpoint_lsn or
        !std.mem.eql(u8, checkpoint.seed_receipt_sha256, active.seed_receipt_sha256) or
        !std.mem.eql(u8, checkpoint.capture_receipt_sha256, active.capture_receipt_sha256) or
        !std.mem.eql(u8, checkpoint.manifest_sha256, active.manifest_sha256) or
        !std.mem.eql(u8, checkpoint.aggregate_sha256, active.aggregate_sha256))
        return error.SeedActivationCheckpointMismatch;
}

fn activateMaterializedWithOptions(alloc: Allocator, request: ActivateRequest, options: ActivateOptions) !ActivationResult {
    try validateRequest(request);
    const target = request.materialization orelse return error.MaterializationTargetMissing;
    const binding = request.binding orelse return error.ActivationBindingMissing;
    if (target.target_local_node_id == 0 or target.target_replica_id == 0) return error.InvalidMaterializationTarget;

    // Raw transport validation is always complete before either raw or live
    // generation roots are created on the target PVC.
    try seed_artifact.verifyStaged(alloc, request.staging_root, request.expected, request.limits);
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    const staged_receipt_path = try std.fs.path.join(alloc, &.{ request.staging_root, seed_artifact.receipt_name });
    defer alloc.free(staged_receipt_path);
    const staged_receipt_json = try readFileAlloc(io, alloc, staged_receipt_path, request.limits.max_receipt_bytes);
    defer alloc.free(staged_receipt_json);
    var staged_receipt = std.json.parseFromSlice(seed_artifact.Receipt, alloc, staged_receipt_json, .{ .ignore_unknown_fields = false }) catch
        return error.InvalidArtifactReceipt;
    defer staged_receipt.deinit();
    if (staged_receipt.value.format_version != seed_artifact.format_version or
        !isCanonicalSha256(staged_receipt.value.capture_receipt_sha256)) return error.CaptureReceiptAuthorityMissing;

    const staged_manifest_path = try std.fs.path.join(alloc, &.{ request.staging_root, seed_artifact.staged_manifest_name });
    defer alloc.free(staged_manifest_path);
    const staged_manifest = try readFileAlloc(io, alloc, staged_manifest_path, request.limits.max_manifest_bytes);
    defer alloc.free(staged_manifest);
    try expectSha256(staged_manifest, staged_receipt.value.manifest_sha256, error.ManifestDigestMismatch);

    var seed_digest: [Sha256.digest_length]u8 = undefined;
    Sha256.hash(staged_receipt_json, &seed_digest, .{});
    var seed_receipt_sha256: [Sha256.digest_length * 2]u8 = undefined;
    encodeHex(&seed_receipt_sha256, &seed_digest);
    const capture_receipt_sha256 = staged_receipt.value.capture_receipt_sha256;
    const expected_capture = request.expected.capture_receipt_sha256 orelse return error.CaptureReceiptAuthorityMissing;
    if (!std.mem.eql(u8, capture_receipt_sha256, expected_capture)) return error.WrongCaptureReceiptDigest;

    const raw_relative_path = try std.fs.path.join(alloc, &.{ generations_dir_name, request.expected.generation });
    defer alloc.free(raw_relative_path);
    const live_relative_path = try std.fs.path.join(alloc, &.{ live_generations_dir_name, request.expected.generation });
    defer alloc.free(live_relative_path);
    const raw_root = try std.fs.path.join(alloc, &.{ request.target_root, raw_relative_path });
    defer alloc.free(raw_root);
    const live_root = try std.fs.path.join(alloc, &.{ request.target_root, live_relative_path });
    defer alloc.free(live_root);
    const active_path = try std.fs.path.join(alloc, &.{ request.target_root, active_receipt_name });
    defer alloc.free(active_path);

    if (readOptionalFileAlloc(io, alloc, active_path, request.limits.max_receipt_bytes)) |existing_active| {
        defer alloc.free(existing_active);
        try validateMaterializedActive(
            alloc,
            existing_active,
            request,
            &seed_receipt_sha256,
            capture_receipt_sha256,
            raw_relative_path,
            live_relative_path,
            raw_root,
            live_root,
        );
        try recordLifecycleReceipt(alloc, request, existing_active);
        return .{
            .generation_path = try alloc.dupe(u8, live_root),
            .active_receipt_json = try alloc.dupe(u8, existing_active),
            .already_active = true,
        };
    } else |err| switch (err) {
        error.FileNotFound => {},
        else => return err,
    }

    try inspectTargetRoot(io, request.target_root);
    const raw_generations_root = try std.fs.path.join(alloc, &.{ request.target_root, generations_dir_name });
    defer alloc.free(raw_generations_root);
    const live_generations_root = try std.fs.path.join(alloc, &.{ request.target_root, live_generations_dir_name });
    defer alloc.free(live_generations_root);
    try fs_paths.createDirPathPortable(io, request.target_root);
    try fs_paths.createDirPathPortable(io, raw_generations_root);
    try fs_paths.createDirPathPortable(io, live_generations_root);
    try fs_paths.syncDirPortable(io, request.target_root);

    const raw_installing_name = try std.fmt.allocPrint(alloc, ".installing-{s}", .{request.expected.generation});
    defer alloc.free(raw_installing_name);
    const raw_installing_root = try std.fs.path.join(alloc, &.{ raw_generations_root, raw_installing_name });
    defer alloc.free(raw_installing_root);
    try inspectGenerationsRoot(io, raw_generations_root, request.expected.generation, raw_installing_name);

    const raw_marker_json = try std.json.Stringify.valueAlloc(alloc, RawGenerationReceipt{
        .generation = request.expected.generation,
        .slot_name = request.expected.slot_name,
        .seed_receipt_sha256 = &seed_receipt_sha256,
        .capture_receipt_sha256 = capture_receipt_sha256,
        .manifest_sha256 = staged_receipt.value.manifest_sha256,
        .aggregate_sha256 = staged_receipt.value.aggregate_sha256,
        .topology_id = binding.topology_id,
        .topology_generation = binding.topology_generation,
        .node_id = binding.node_id,
        .target_pvc_name = binding.target_pvc_name,
        .target_pvc_uid = binding.target_pvc_uid,
    }, .{});
    defer alloc.free(raw_marker_json);

    if (try directoryExists(io, raw_root)) {
        try validatePublishedRawGeneration(alloc, raw_root, request, raw_marker_json);
    } else {
        try recoverInstallingDirectory(alloc, io, raw_installing_root, raw_marker_json, request.limits.max_receipt_bytes);
        try fs_paths.createDirPathPortable(io, raw_installing_root);
        const raw_marker_path = try std.fs.path.join(alloc, &.{ raw_installing_root, generation_receipt_name });
        defer alloc.free(raw_marker_path);
        _ = try writeImmutableFile(io, alloc, raw_marker_path, raw_marker_json, error.SeedGenerationConflict);
        for (staged_receipt.value.files) |file| {
            const source = try std.fs.path.join(alloc, &.{ request.staging_root, file.path });
            defer alloc.free(source);
            const destination = try std.fs.path.join(alloc, &.{ raw_installing_root, file.path });
            defer alloc.free(destination);
            try copyFileDurably(io, source, destination, raw_installing_root);
        }
        const installed_receipt_path = try std.fs.path.join(alloc, &.{ raw_installing_root, seed_artifact.receipt_name });
        defer alloc.free(installed_receipt_path);
        try copyFileDurably(io, staged_receipt_path, installed_receipt_path, raw_installing_root);
        const installed_manifest_path = try std.fs.path.join(alloc, &.{ raw_installing_root, seed_artifact.staged_manifest_name });
        defer alloc.free(installed_manifest_path);
        try copyFileDurably(io, staged_manifest_path, installed_manifest_path, raw_installing_root);
        try validatePublishedRawGeneration(alloc, raw_installing_root, request, raw_marker_json);
        try failAt(options, .generation_copied);
        try renameDirectoryNoReplace(io, raw_installing_root, raw_root, error.SeedTargetGenerationConflict);
        try fs_paths.syncDirPortable(io, raw_generations_root);
    }
    try failAt(options, .generation_published);

    const live_installing_name = try std.fmt.allocPrint(alloc, ".installing-{s}", .{request.expected.generation});
    defer alloc.free(live_installing_name);
    const live_installing_root = try std.fs.path.join(alloc, &.{ live_generations_root, live_installing_name });
    defer alloc.free(live_installing_root);
    try inspectGenerationsRoot(io, live_generations_root, request.expected.generation, live_installing_name);

    var evidence: seed_materialization.PublishedEvidence = undefined;
    var evidence_owned = false;
    defer if (evidence_owned) evidence.deinit(alloc);
    if (try directoryExists(io, live_root)) {
        evidence = try seed_materialization.loadPublishedEvidence(alloc, live_root, request.expected.generation);
        evidence_owned = true;
        try seed_materialization.validatePublishedBeforeRuntime(alloc, live_root, request.expected.generation, &evidence.receipt_sha256);
    } else {
        if (try directoryExists(io, live_installing_root)) std.Io.Dir.cwd().deleteTree(io, live_installing_root) catch |err| return err;
        const materialized = try seed_materialization.materialize(alloc, .{
            .raw_generation_root = raw_root,
            .live_installing_root = live_installing_root,
            .generation = request.expected.generation,
            .target_local_node_id = target.target_local_node_id,
            .target_replica_id = target.target_replica_id,
            .seed_receipt_sha256 = &seed_receipt_sha256,
            .capture_receipt_sha256 = capture_receipt_sha256,
            .raw_manifest_sha256 = staged_receipt.value.manifest_sha256,
            .raw_aggregate_sha256 = staged_receipt.value.aggregate_sha256,
        });
        evidence = .{
            .receipt_json = materialized.receipt_json,
            .receipt_sha256 = materialized.receipt_sha256,
            .aggregate_sha256 = materialized.aggregate_sha256,
        };
        evidence_owned = true;
        try seed_materialization.validatePublishedBeforeRuntime(alloc, live_installing_root, request.expected.generation, &evidence.receipt_sha256);
        try failAt(options, .live_generation_materialized);
        try renameDirectoryNoReplace(io, live_installing_root, live_root, error.LiveGenerationConflict);
        try fs_paths.syncDirPortable(io, live_generations_root);
    }
    try failAt(options, .live_generation_published);

    const activation_json = try std.json.Stringify.valueAlloc(alloc, ActivationReceipt{
        .generation = request.expected.generation,
        .slot_name = request.expected.slot_name,
        .cluster_id = staged_receipt.value.cluster_id,
        .shard_id = staged_receipt.value.shard_id,
        .table_id = staged_receipt.value.table_id,
        .timeline_id = staged_receipt.value.timeline_id,
        .epoch = staged_receipt.value.epoch,
        .manifest_id = staged_receipt.value.manifest_id,
        .backup_lsn = staged_receipt.value.backup_lsn,
        .checkpoint_lsn = staged_receipt.value.checkpoint_lsn,
        .seed_receipt_sha256 = &seed_receipt_sha256,
        .capture_receipt_sha256 = capture_receipt_sha256,
        .manifest_sha256 = staged_receipt.value.manifest_sha256,
        .aggregate_sha256 = staged_receipt.value.aggregate_sha256,
        .generation_path = live_relative_path,
        .raw_generation_path = raw_relative_path,
        .materialized_receipt_sha256 = &evidence.receipt_sha256,
        .materialized_aggregate_sha256 = &evidence.aggregate_sha256,
        .target_local_node_id = target.target_local_node_id,
        .target_replica_id = target.target_replica_id,
        .topology_id = binding.topology_id,
        .topology_generation = binding.topology_generation,
        .node_id = binding.node_id,
        .target_pvc_name = binding.target_pvc_name,
        .target_pvc_uid = binding.target_pvc_uid,
    }, .{});
    errdefer alloc.free(activation_json);
    const active_created = try writeImmutableFile(io, alloc, active_path, activation_json, error.ActiveGenerationConflict);
    try failAt(options, .active_published);
    try recordLifecycleReceipt(alloc, request, activation_json);
    return .{
        .generation_path = try alloc.dupe(u8, live_root),
        .active_receipt_json = activation_json,
        .already_active = !active_created,
    };
}

fn validatePublishedRawGeneration(alloc: Allocator, raw_root: []const u8, request: ActivateRequest, raw_marker_json: []const u8) !void {
    seed_artifact.verifyStaged(alloc, raw_root, request.expected, request.limits) catch return error.SeedGenerationConflict;
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const marker_path = try std.fs.path.join(alloc, &.{ raw_root, generation_receipt_name });
    defer alloc.free(marker_path);
    try verifyGenerationReceipt(io_impl.io(), alloc, marker_path, raw_marker_json, request.limits.max_receipt_bytes);
}

fn validateMaterializedActive(
    alloc: Allocator,
    raw: []const u8,
    request: ActivateRequest,
    seed_receipt_sha256: []const u8,
    capture_receipt_sha256: []const u8,
    raw_relative_path: []const u8,
    live_relative_path: []const u8,
    raw_root: []const u8,
    live_root: []const u8,
) !void {
    var parsed = std.json.parseFromSlice(ActivationReceipt, alloc, raw, .{ .ignore_unknown_fields = false }) catch
        return error.InvalidActiveReceipt;
    defer parsed.deinit();
    const receipt = parsed.value;
    const binding = request.binding orelse return error.ActivationBindingMissing;
    const target = request.materialization orelse return error.MaterializationTargetMissing;
    if (receipt.format_version != format_version or
        !std.mem.eql(u8, receipt.generation, request.expected.generation) or
        !std.mem.eql(u8, receipt.slot_name, request.expected.slot_name) or
        !std.mem.eql(u8, receipt.seed_receipt_sha256, seed_receipt_sha256) or
        !std.mem.eql(u8, receipt.capture_receipt_sha256, capture_receipt_sha256) or
        !std.mem.eql(u8, receipt.raw_generation_path, raw_relative_path) or
        !std.mem.eql(u8, receipt.generation_path, live_relative_path) or
        receipt.target_local_node_id != target.target_local_node_id or
        receipt.target_replica_id != target.target_replica_id or
        !isCanonicalSha256(receipt.materialized_receipt_sha256) or
        !isCanonicalSha256(receipt.materialized_aggregate_sha256)) return error.ActiveGenerationConflict;
    try expectIdentity(request.expected.identity, receipt.identity(), error.ActiveGenerationConflict);
    try expectBinding(binding, receipt, error.ActiveGenerationConflict);
    const raw_marker = try rawMarkerForActiveAlloc(alloc, receipt);
    defer alloc.free(raw_marker);
    try validatePublishedRawGeneration(alloc, raw_root, request, raw_marker);
    try seed_materialization.validateRuntimeIdentity(
        alloc,
        raw_root,
        live_root,
        request.expected.generation,
        receipt.materialized_receipt_sha256,
    );
}

fn rawMarkerForActiveAlloc(alloc: Allocator, receipt: ActivationReceipt) ![]u8 {
    return try std.json.Stringify.valueAlloc(alloc, RawGenerationReceipt{
        .generation = receipt.generation,
        .slot_name = receipt.slot_name,
        .seed_receipt_sha256 = receipt.seed_receipt_sha256,
        .capture_receipt_sha256 = receipt.capture_receipt_sha256,
        .manifest_sha256 = receipt.manifest_sha256,
        .aggregate_sha256 = receipt.aggregate_sha256,
        .topology_id = receipt.topology_id,
        .topology_generation = receipt.topology_generation,
        .node_id = receipt.node_id,
        .target_pvc_name = receipt.target_pvc_name,
        .target_pvc_uid = receipt.target_pvc_uid,
    }, .{});
}

fn renameDirectoryNoReplace(io: std.Io, source: []const u8, destination: []const u8, conflict: anyerror) !void {
    std.Io.Dir.rename(std.Io.Dir.cwd(), source, std.Io.Dir.cwd(), destination, io) catch |err| {
        if (try directoryExists(io, destination)) return conflict;
        return err;
    };
}

fn activateWithOptions(alloc: Allocator, request: ActivateRequest, options: ActivateOptions) !ActivationResult {
    if (request.materialization != null) return activateMaterializedWithOptions(alloc, request, options);
    try validateRequest(request);

    // This must precede every target-volume mutation. Besides validating all
    // content digests, it binds generation, slot, identity and LSN boundary.
    try seed_artifact.verifyStaged(alloc, request.staging_root, request.expected, request.limits);

    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    const staged_receipt_path = try std.fs.path.join(alloc, &.{ request.staging_root, seed_artifact.receipt_name });
    defer alloc.free(staged_receipt_path);
    const staged_receipt_json = try readFileAlloc(io, alloc, staged_receipt_path, request.limits.max_receipt_bytes);
    defer alloc.free(staged_receipt_json);
    var staged_receipt = std.json.parseFromSlice(seed_artifact.Receipt, alloc, staged_receipt_json, .{}) catch return error.InvalidArtifactReceipt;
    defer staged_receipt.deinit();

    const staged_manifest_path = try std.fs.path.join(alloc, &.{ request.staging_root, seed_artifact.staged_manifest_name });
    defer alloc.free(staged_manifest_path);
    const staged_manifest = try readFileAlloc(io, alloc, staged_manifest_path, request.limits.max_manifest_bytes);
    defer alloc.free(staged_manifest);
    try expectSha256(staged_manifest, staged_receipt.value.manifest_sha256, error.ManifestDigestMismatch);

    var seed_receipt_digest: [Sha256.digest_length]u8 = undefined;
    Sha256.hash(staged_receipt_json, &seed_receipt_digest, .{});
    var seed_receipt_hex: [Sha256.digest_length * 2]u8 = undefined;
    encodeHex(&seed_receipt_hex, &seed_receipt_digest);

    const capture_receipt_sha256 = staged_receipt.value.capture_receipt_sha256;
    if (request.binding != null) {
        if (staged_receipt.value.format_version != seed_artifact.format_version or
            !isCanonicalSha256(capture_receipt_sha256)) return error.CaptureReceiptAuthorityMissing;
        const expected_capture = request.expected.capture_receipt_sha256 orelse
            return error.CaptureReceiptAuthorityMissing;
        if (!isCanonicalSha256(expected_capture) or
            !std.mem.eql(u8, expected_capture, capture_receipt_sha256))
            return error.WrongCaptureReceiptDigest;
    } else if (capture_receipt_sha256.len != 0) {
        return error.UnexpectedCaptureReceiptAuthority;
    }

    const generation_relative_path = try std.fs.path.join(alloc, &.{ generations_dir_name, request.expected.generation });
    defer alloc.free(generation_relative_path);
    const binding = request.binding orelse ActivationBinding{};
    const activation_json = try std.json.Stringify.valueAlloc(alloc, ActivationReceipt{
        .format_version = legacy_format_version,
        .generation = request.expected.generation,
        .slot_name = request.expected.slot_name,
        .cluster_id = staged_receipt.value.cluster_id,
        .shard_id = staged_receipt.value.shard_id,
        .table_id = staged_receipt.value.table_id,
        .timeline_id = staged_receipt.value.timeline_id,
        .epoch = staged_receipt.value.epoch,
        .manifest_id = staged_receipt.value.manifest_id,
        .backup_lsn = staged_receipt.value.backup_lsn,
        .checkpoint_lsn = staged_receipt.value.checkpoint_lsn,
        .seed_receipt_sha256 = &seed_receipt_hex,
        .capture_receipt_sha256 = capture_receipt_sha256,
        .manifest_sha256 = staged_receipt.value.manifest_sha256,
        .aggregate_sha256 = staged_receipt.value.aggregate_sha256,
        .generation_path = generation_relative_path,
        .topology_id = binding.topology_id,
        .topology_generation = binding.topology_generation,
        .node_id = binding.node_id,
        .target_pvc_name = binding.target_pvc_name,
        .target_pvc_uid = binding.target_pvc_uid,
    }, .{});
    errdefer alloc.free(activation_json);

    const generations_root = try std.fs.path.join(alloc, &.{ request.target_root, generations_dir_name });
    defer alloc.free(generations_root);
    const generation_path = try std.fs.path.join(alloc, &.{ generations_root, request.expected.generation });
    errdefer alloc.free(generation_path);
    const installing_name = try std.fmt.allocPrint(alloc, ".installing-{s}", .{request.expected.generation});
    defer alloc.free(installing_name);
    const installing_path = try std.fs.path.join(alloc, &.{ generations_root, installing_name });
    defer alloc.free(installing_path);
    const active_path = try std.fs.path.join(alloc, &.{ request.target_root, active_receipt_name });
    defer alloc.free(active_path);

    try inspectTargetRoot(io, request.target_root);
    if (readOptionalFileAlloc(io, alloc, active_path, request.limits.max_receipt_bytes)) |existing_active| {
        defer alloc.free(existing_active);
        try validateActiveReceipt(
            alloc,
            existing_active,
            request.expected,
            request.binding,
            &seed_receipt_hex,
            capture_receipt_sha256,
            generation_relative_path,
        );
        try inspectGenerationsRoot(io, generations_root, request.expected.generation, installing_name);
        try validatePublishedGeneration(alloc, generation_path, request, activation_json);
        try recordLifecycleReceipt(alloc, request, existing_active);
        const active_receipt_copy = try alloc.dupe(u8, existing_active);
        alloc.free(activation_json);
        return .{
            .generation_path = generation_path,
            .active_receipt_json = active_receipt_copy,
            .already_active = true,
        };
    } else |err| switch (err) {
        error.FileNotFound => {},
        else => return err,
    }

    try fs_paths.createDirPathPortable(io, request.target_root);
    try fs_paths.createDirPathPortable(io, generations_root);
    try fs_paths.syncDirPortable(io, request.target_root);
    try inspectGenerationsRoot(io, generations_root, request.expected.generation, installing_name);

    if (try directoryExists(io, generation_path)) {
        try validatePublishedGeneration(alloc, generation_path, request, activation_json);
    } else {
        try recoverInstallingDirectory(alloc, io, installing_path, activation_json, request.limits.max_receipt_bytes);
        try fs_paths.createDirPathPortable(io, installing_path);

        const generation_receipt_path = try std.fs.path.join(alloc, &.{ installing_path, generation_receipt_name });
        defer alloc.free(generation_receipt_path);
        _ = try writeImmutableFile(io, alloc, generation_receipt_path, activation_json, error.SeedGenerationConflict);

        for (staged_receipt.value.files) |file| {
            const source_path = try std.fs.path.join(alloc, &.{ request.staging_root, file.path });
            defer alloc.free(source_path);
            const destination_path = try std.fs.path.join(alloc, &.{ installing_path, file.path });
            defer alloc.free(destination_path);
            try copyFileDurably(io, source_path, destination_path, installing_path);
        }
        const installed_receipt_path = try std.fs.path.join(alloc, &.{ installing_path, seed_artifact.receipt_name });
        defer alloc.free(installed_receipt_path);
        try copyFileDurably(io, staged_receipt_path, installed_receipt_path, installing_path);
        const installed_manifest_path = try std.fs.path.join(alloc, &.{ installing_path, seed_artifact.staged_manifest_name });
        defer alloc.free(installed_manifest_path);
        try copyFileDurably(io, staged_manifest_path, installed_manifest_path, installing_path);

        try seed_artifact.verifyStaged(alloc, installing_path, request.expected, request.limits);
        try verifyGenerationReceipt(io, alloc, generation_receipt_path, activation_json, request.limits.max_receipt_bytes);
        try fs_paths.syncDirPortable(io, installing_path);
        try failAt(options, .generation_copied);

        std.Io.Dir.rename(std.Io.Dir.cwd(), installing_path, std.Io.Dir.cwd(), generation_path, io) catch |err| {
            if (directoryExists(io, generation_path) catch false) {
                try validatePublishedGeneration(alloc, generation_path, request, activation_json);
                std.Io.Dir.cwd().deleteTree(io, installing_path) catch {};
            } else return err;
        };
        try fs_paths.syncDirPortable(io, generations_root);
    }

    try failAt(options, .generation_published);
    const active_created = try writeImmutableFile(io, alloc, active_path, activation_json, error.ActiveGenerationConflict);
    try failAt(options, .active_published);
    try recordLifecycleReceipt(alloc, request, activation_json);

    return .{
        .generation_path = generation_path,
        .active_receipt_json = activation_json,
        .already_active = !active_created,
    };
}

fn validateRequest(request: ActivateRequest) !void {
    if (!validation.isIdentifier(request.expected.generation)) return error.InvalidSeedGeneration;
    if (!validation.isIdentifier(request.expected.slot_name)) return error.InvalidSlotName;
    if (!validAbsoluteRoot(request.staging_root)) return error.InvalidStagingRoot;
    if (!validAbsoluteRoot(request.target_root)) return error.InvalidActivationTarget;
    if (pathsOverlap(request.staging_root, request.target_root)) return error.OverlappingActivationPaths;
    if (request.binding) |binding| {
        try validateBinding(binding);
        const expected_binding = request.expected.binding orelse return error.ArtifactBindingRequired;
        try expectArtifactBinding(binding, expected_binding, error.ActivationBindingMismatch);
        const capture_digest = request.expected.capture_receipt_sha256 orelse
            return error.CaptureReceiptAuthorityMissing;
        if (!isCanonicalSha256(capture_digest)) return error.InvalidCaptureReceiptDigest;
    } else if (request.expected.binding != null or request.expected.capture_receipt_sha256 != null) {
        return error.UnexpectedActivationBinding;
    }
    if (request.materialization) |target| {
        if (request.binding == null) return error.MaterializationRequiresBinding;
        if (target.target_local_node_id == 0 or target.target_replica_id == 0) return error.InvalidMaterializationTarget;
    }
    if (request.pod_uid) |pod_uid| if (!validation.isIdentifier(pod_uid)) return error.InvalidActivationPodUID;
}

fn validateBinding(binding: ActivationBinding) !void {
    if (!validation.isIdentifier(binding.topology_id)) return error.InvalidTopologyId;
    if (binding.topology_generation == 0) return error.InvalidTopologyGeneration;
    if (!validation.isIdentifier(binding.node_id)) return error.InvalidNodeId;
    if (!validation.isIdentifier(binding.target_pvc_name)) return error.InvalidTargetPVCName;
    if (!validation.isIdentifier(binding.target_pvc_uid)) return error.InvalidTargetPVCUID;
}

fn recordLifecycleReceipt(alloc: Allocator, request: ActivateRequest, receipt_json: []const u8) !void {
    // Legacy unbound activation remains readable for compatibility but cannot
    // be advertised as topology authority.
    if (request.binding == null) return;
    var ledger = try lifecycle_receipt_ledger.Ledger.open(alloc, request.target_root, .{});
    defer ledger.close();
    _ = try ledger.recordActivation(receipt_json, .{ .pod_uid = request.pod_uid });
}

fn validAbsoluteRoot(path: []const u8) bool {
    return path.len > 1 and
        validation.isAbsoluteNormalizedPath(path) and
        path[path.len - 1] != std.fs.path.sep;
}

fn pathsOverlap(a: []const u8, b: []const u8) bool {
    return pathContains(a, b) or pathContains(b, a);
}

fn pathContains(parent: []const u8, child: []const u8) bool {
    if (std.mem.eql(u8, parent, child)) return true;
    return std.mem.startsWith(u8, child, parent) and child.len > parent.len and child[parent.len] == std.fs.path.sep;
}

fn inspectTargetRoot(io: std.Io, target_root: []const u8) !void {
    var dir = std.Io.Dir.cwd().openDir(io, target_root, .{ .iterate = true }) catch |err| switch (err) {
        error.FileNotFound => return,
        error.NotDir => return error.UnsafeActivationTarget,
        else => return err,
    };
    defer dir.close(io);
    var iterator = dir.iterateAssumeFirstIteration();
    while (try iterator.next(io)) |entry| {
        if (std.mem.eql(u8, entry.name, generations_dir_name) and entry.kind == .directory) continue;
        if (std.mem.eql(u8, entry.name, live_generations_dir_name) and entry.kind == .directory) continue;
        if (std.mem.eql(u8, entry.name, active_receipt_name) and entry.kind == .file) continue;
        if (std.mem.eql(u8, entry.name, lifecycle_receipt_ledger.ledger_dir_name) and entry.kind == .directory) continue;
        return error.UnsafeActivationTarget;
    }
}

fn inspectGenerationsRoot(io: std.Io, generations_root: []const u8, generation: []const u8, installing_name: []const u8) !void {
    var dir = std.Io.Dir.cwd().openDir(io, generations_root, .{ .iterate = true }) catch |err| switch (err) {
        error.FileNotFound => return,
        error.NotDir => return error.UnsafeActivationTarget,
        else => return err,
    };
    defer dir.close(io);
    var iterator = dir.iterateAssumeFirstIteration();
    while (try iterator.next(io)) |entry| {
        if (entry.kind != .directory) return error.UnsafeActivationTarget;
        if (std.mem.eql(u8, entry.name, generation) or std.mem.eql(u8, entry.name, installing_name)) continue;
        return error.UnsafeActivationTarget;
    }
}

fn validateActiveReceipt(
    alloc: Allocator,
    raw: []const u8,
    expected: seed_artifact.ExpectedArtifact,
    expected_binding: ?ActivationBinding,
    seed_receipt_sha256: []const u8,
    capture_receipt_sha256: []const u8,
    generation_path: []const u8,
) !void {
    var parsed = std.json.parseFromSlice(ActivationReceipt, alloc, raw, .{}) catch return error.InvalidActiveReceipt;
    defer parsed.deinit();
    const receipt = parsed.value;
    if (receipt.format_version != legacy_format_version) return error.UnsupportedActivationVersion;
    if (!std.mem.eql(u8, receipt.generation, expected.generation) or
        !std.mem.eql(u8, receipt.slot_name, expected.slot_name) or
        !std.mem.eql(u8, receipt.seed_receipt_sha256, seed_receipt_sha256) or
        !std.mem.eql(u8, receipt.capture_receipt_sha256, capture_receipt_sha256) or
        !std.mem.eql(u8, receipt.generation_path, generation_path)) return error.ActiveGenerationConflict;
    try expectIdentity(expected.identity, receipt.identity(), error.ActiveGenerationConflict);
    if (receipt.checkpoint_lsn < expected.minimum_checkpoint_lsn) return error.ActiveGenerationConflict;
    if (expected_binding) |binding| try expectBinding(binding, receipt, error.ActiveGenerationConflict);
}

fn validatePublishedGeneration(alloc: Allocator, generation_path: []const u8, request: ActivateRequest, activation_json: []const u8) !void {
    seed_artifact.verifyStaged(alloc, generation_path, request.expected, request.limits) catch return error.SeedGenerationConflict;
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const marker_path = try std.fs.path.join(alloc, &.{ generation_path, generation_receipt_name });
    defer alloc.free(marker_path);
    try verifyGenerationReceipt(io_impl.io(), alloc, marker_path, activation_json, request.limits.max_receipt_bytes);
    verifyInstalledActivationEvidence(io_impl.io(), alloc, generation_path, activation_json, request.limits) catch return error.SeedGenerationConflict;
}

fn verifyInstalledActivationEvidence(io: std.Io, alloc: Allocator, generation_path: []const u8, activation_json: []const u8, limits: seed_artifact.Limits) !void {
    var active = std.json.parseFromSlice(ActivationReceipt, alloc, activation_json, .{}) catch return error.InvalidActiveReceipt;
    defer active.deinit();
    const receipt_path = try std.fs.path.join(alloc, &.{ generation_path, seed_artifact.receipt_name });
    defer alloc.free(receipt_path);
    const receipt_json = try readFileAlloc(io, alloc, receipt_path, limits.max_receipt_bytes);
    defer alloc.free(receipt_json);
    try expectSha256(receipt_json, active.value.seed_receipt_sha256, error.SeedReceiptDigestMismatch);
    var installed = std.json.parseFromSlice(seed_artifact.Receipt, alloc, receipt_json, .{}) catch return error.InvalidArtifactReceipt;
    defer installed.deinit();
    const receipt = installed.value;
    if (!std.mem.eql(u8, receipt.generation, active.value.generation) or
        !std.mem.eql(u8, receipt.slot_name, active.value.slot_name) or
        !std.mem.eql(u8, receipt.manifest_id, active.value.manifest_id) or
        receipt.backup_lsn != active.value.backup_lsn or
        receipt.checkpoint_lsn != active.value.checkpoint_lsn or
        !std.mem.eql(u8, receipt.capture_receipt_sha256, active.value.capture_receipt_sha256) or
        !std.mem.eql(u8, receipt.manifest_sha256, active.value.manifest_sha256) or
        !std.mem.eql(u8, receipt.aggregate_sha256, active.value.aggregate_sha256)) return error.ActivationReceiptMismatch;
    try expectIdentity(receipt.identity(), active.value.identity(), error.ActivationReceiptMismatch);
}

/// Revalidates all boot-critical evidence from the mounted target volume. This
/// performs no writes and must succeed before the runtime opens the generation.
pub fn validateActivatedGeneration(alloc: Allocator, expectation: StartupExpectation) !u64 {
    if (!validAbsoluteRoot(expectation.target_root)) return error.InvalidActivationTarget;
    if (!validation.isIdentifier(expectation.expected.generation)) return error.InvalidSeedGeneration;
    if (!validation.isIdentifier(expectation.expected.slot_name)) return error.InvalidSlotName;
    try validateBinding(expectation.binding);

    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    const active_path = try std.fs.path.join(alloc, &.{ expectation.target_root, active_receipt_name });
    defer alloc.free(active_path);
    const active_json = readFileAlloc(io, alloc, active_path, expectation.limits.max_receipt_bytes) catch |err| switch (err) {
        error.FileNotFound => return error.ActiveReceiptMissing,
        else => return err,
    };
    defer alloc.free(active_json);
    var active = std.json.parseFromSlice(ActivationReceipt, alloc, active_json, .{}) catch return error.InvalidActiveReceipt;
    defer active.deinit();
    const receipt = active.value;
    try expectIdentity(expectation.expected.identity, receipt.identity(), error.ActiveGenerationConflict);
    try expectBinding(expectation.binding, receipt, error.ActiveGenerationConflict);
    try expectOptionalDigest(expectation.manifest_sha256, receipt.manifest_sha256);
    try expectOptionalDigest(expectation.aggregate_sha256, receipt.aggregate_sha256);
    try expectOptionalDigest(expectation.seed_receipt_sha256, receipt.seed_receipt_sha256);
    try expectOptionalDigest(expectation.capture_receipt_sha256, receipt.capture_receipt_sha256);

    if (receipt.format_version == format_version) {
        const raw_relative_path = try std.fs.path.join(alloc, &.{ generations_dir_name, expectation.expected.generation });
        defer alloc.free(raw_relative_path);
        const live_relative_path = try std.fs.path.join(alloc, &.{ live_generations_dir_name, expectation.expected.generation });
        defer alloc.free(live_relative_path);
        if (!std.mem.eql(u8, receipt.generation, expectation.expected.generation) or
            !std.mem.eql(u8, receipt.slot_name, expectation.expected.slot_name) or
            !std.mem.eql(u8, receipt.raw_generation_path, raw_relative_path) or
            !std.mem.eql(u8, receipt.generation_path, live_relative_path) or
            receipt.checkpoint_lsn < expectation.expected.minimum_checkpoint_lsn or
            !isCanonicalSha256(receipt.materialized_receipt_sha256) or
            !isCanonicalSha256(receipt.materialized_aggregate_sha256) or
            receipt.target_local_node_id == 0 or receipt.target_replica_id == 0) return error.ActiveGenerationConflict;
        try expectOptionalDigest(expectation.materialized_receipt_sha256, receipt.materialized_receipt_sha256);
        try expectOptionalDigest(expectation.materialized_aggregate_sha256, receipt.materialized_aggregate_sha256);
        if (expectation.target_local_node_id) |node_id| if (node_id != receipt.target_local_node_id) return error.ActiveGenerationConflict;
        if (expectation.target_replica_id) |replica_id| if (replica_id != receipt.target_replica_id) return error.ActiveGenerationConflict;

        const raw_root = try std.fs.path.join(alloc, &.{ expectation.target_root, raw_relative_path });
        defer alloc.free(raw_root);
        const live_root = try std.fs.path.join(alloc, &.{ expectation.target_root, live_relative_path });
        defer alloc.free(live_root);
        const raw_marker = try rawMarkerForActiveAlloc(alloc, receipt);
        defer alloc.free(raw_marker);
        try validatePublishedRawGeneration(alloc, raw_root, .{
            .staging_root = "/unused",
            .target_root = expectation.target_root,
            .expected = expectation.expected,
            .binding = expectation.binding,
            .materialization = .{
                .target_local_node_id = receipt.target_local_node_id,
                .target_replica_id = receipt.target_replica_id,
            },
            .limits = expectation.limits,
        }, raw_marker);
        try seed_materialization.validateRuntimeIdentity(
            alloc,
            raw_root,
            live_root,
            expectation.expected.generation,
            receipt.materialized_receipt_sha256,
        );
        return receipt.checkpoint_lsn;
    }

    const generation_relative_path = try std.fs.path.join(alloc, &.{ generations_dir_name, expectation.expected.generation });
    defer alloc.free(generation_relative_path);
    if (receipt.format_version != legacy_format_version or
        !std.mem.eql(u8, receipt.generation, expectation.expected.generation) or
        !std.mem.eql(u8, receipt.slot_name, expectation.expected.slot_name) or
        !std.mem.eql(u8, receipt.generation_path, generation_relative_path) or
        receipt.checkpoint_lsn < expectation.expected.minimum_checkpoint_lsn) return error.ActiveGenerationConflict;
    const generation_path = try std.fs.path.join(alloc, &.{ expectation.target_root, generation_relative_path });
    defer alloc.free(generation_path);
    try validatePublishedGeneration(alloc, generation_path, .{
        .staging_root = "/unused",
        .target_root = expectation.target_root,
        .expected = expectation.expected,
        .binding = expectation.binding,
        .limits = expectation.limits,
    }, active_json);
    return receipt.checkpoint_lsn;
}

fn expectBinding(expected: ActivationBinding, actual: ActivationReceipt, mismatch: anyerror) !void {
    if (!std.mem.eql(u8, expected.topology_id, actual.topology_id) or
        expected.topology_generation != actual.topology_generation or
        !std.mem.eql(u8, expected.node_id, actual.node_id) or
        !std.mem.eql(u8, expected.target_pvc_name, actual.target_pvc_name) or
        !std.mem.eql(u8, expected.target_pvc_uid, actual.target_pvc_uid)) return mismatch;
}

fn expectArtifactBinding(expected: ActivationBinding, actual: seed_artifact.LifecycleBinding, mismatch: anyerror) !void {
    if (!std.mem.eql(u8, expected.topology_id, actual.topology_id) or
        expected.topology_generation != actual.topology_generation or
        !std.mem.eql(u8, expected.node_id, actual.node_id) or
        !std.mem.eql(u8, expected.target_pvc_name, actual.target_pvc_name) or
        !std.mem.eql(u8, expected.target_pvc_uid, actual.target_pvc_uid)) return mismatch;
}

fn expectOptionalDigest(expected: ?[]const u8, actual: []const u8) !void {
    if (expected) |digest| {
        if (!isCanonicalSha256(digest) or !std.mem.eql(u8, digest, actual)) return error.ActiveGenerationConflict;
    }
}

fn isCanonicalSha256(value: []const u8) bool {
    if (value.len != Sha256.digest_length * 2) return false;
    for (value) |byte| {
        if ((byte < '0' or byte > '9') and (byte < 'a' or byte > 'f')) return false;
    }
    return true;
}

fn verifyGenerationReceipt(io: std.Io, alloc: Allocator, marker_path: []const u8, expected: []const u8, max_bytes: usize) !void {
    const raw = readFileAlloc(io, alloc, marker_path, max_bytes) catch return error.SeedGenerationConflict;
    defer alloc.free(raw);
    if (!std.mem.eql(u8, raw, expected)) return error.SeedGenerationConflict;
}

fn recoverInstallingDirectory(alloc: Allocator, io: std.Io, installing_path: []const u8, activation_json: []const u8, max_bytes: usize) !void {
    const empty = blk: {
        var dir = std.Io.Dir.cwd().openDir(io, installing_path, .{ .iterate = true }) catch |err| switch (err) {
            error.FileNotFound => return,
            error.NotDir => return error.UnsafeActivationTarget,
            else => return err,
        };
        defer dir.close(io);
        var iterator = dir.iterateAssumeFirstIteration();
        break :blk try iterator.next(io) == null;
    };
    if (empty) {
        std.Io.Dir.cwd().deleteTree(io, installing_path) catch |err| return err;
        return;
    }

    const marker_path = try std.fs.path.join(alloc, &.{ installing_path, generation_receipt_name });
    defer alloc.free(marker_path);
    const marker = readFileAlloc(io, alloc, marker_path, max_bytes) catch return error.UnsafeActivationTarget;
    defer alloc.free(marker);
    if (!std.mem.eql(u8, marker, activation_json)) return error.SeedTargetGenerationConflict;
    std.Io.Dir.cwd().deleteTree(io, installing_path) catch |err| return err;
}

fn directoryExists(io: std.Io, path: []const u8) !bool {
    var dir = std.Io.Dir.cwd().openDir(io, path, .{}) catch |err| switch (err) {
        error.FileNotFound => return false,
        error.NotDir => return error.UnsafeActivationTarget,
        else => return err,
    };
    defer dir.close(io);
    return true;
}

fn copyFileDurably(io: std.Io, source_path: []const u8, destination_path: []const u8, sync_root: []const u8) !void {
    try std.Io.Dir.copyFile(std.Io.Dir.cwd(), source_path, std.Io.Dir.cwd(), destination_path, io, .{
        .make_path = true,
        .replace = false,
    });
    try fs_paths.syncFileAndParentPortable(io, destination_path);
    var parent = std.fs.path.dirname(destination_path) orelse return error.InvalidActivationPath;
    while (!std.mem.eql(u8, parent, sync_root)) {
        if (!pathContains(sync_root, parent)) return error.InvalidActivationPath;
        try fs_paths.syncDirPortable(io, parent);
        parent = std.fs.path.dirname(parent) orelse return error.InvalidActivationPath;
    }
    try fs_paths.syncDirPortable(io, sync_root);
}

/// Returns true only when this call published the path. Concurrent publication
/// of identical bytes is treated as an idempotent success; different bytes are
/// always a conflict.
fn writeImmutableFile(io: std.Io, alloc: Allocator, path: []const u8, body: []const u8, conflict: anyerror) !bool {
    var atomic_file = try std.Io.Dir.cwd().createFileAtomic(io, path, .{
        .make_path = false,
        .replace = false,
    });
    defer atomic_file.deinit(io);
    try atomic_file.file.writeStreamingAll(io, body);
    try atomic_file.file.sync(io);
    atomic_file.link(io) catch |err| switch (err) {
        error.PathAlreadyExists => {
            const existing = try readFileAlloc(io, alloc, path, body.len);
            defer alloc.free(existing);
            if (!std.mem.eql(u8, existing, body)) return conflict;
            return false;
        },
        else => return err,
    };
    const parent = std.fs.path.dirname(path) orelse return error.InvalidActivationPath;
    try fs_paths.syncDirPortable(io, parent);
    return true;
}

fn readFileAlloc(io: std.Io, alloc: Allocator, path: []const u8, max_bytes: usize) ![]u8 {
    return try std.Io.Dir.cwd().readFileAlloc(io, path, alloc, .limited(max_bytes));
}

fn readOptionalFileAlloc(io: std.Io, alloc: Allocator, path: []const u8, max_bytes: usize) ![]u8 {
    return readFileAlloc(io, alloc, path, max_bytes) catch |err| switch (err) {
        error.FileNotFound => error.FileNotFound,
        else => err,
    };
}

fn expectIdentity(expected: standby_mod.Identity, actual: standby_mod.Identity, mismatch: anyerror) !void {
    if (actual.cluster_id != expected.cluster_id or
        actual.shard_id != expected.shard_id or
        actual.table_id != expected.table_id or
        actual.timeline_id != expected.timeline_id or
        actual.epoch != expected.epoch) return mismatch;
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

fn failAt(options: ActivateOptions, boundary: FailureBoundary) !void {
    if (options.fail_after == boundary) return error.InjectedActivationFailure;
}

fn testIdentity() standby_mod.Identity {
    return .{ .cluster_id = 10, .shard_id = 20, .table_id = 30, .timeline_id = 2, .epoch = 4 };
}

const PreparedTestStaging = struct {
    root: []u8,
    capture_receipt_sha256: [Sha256.digest_length * 2]u8,
    bound: bool,
};

fn prepareTestStagingWithBinding(
    alloc: Allocator,
    root: []const u8,
    generation: []const u8,
    identity: standby_mod.Identity,
    body: []const u8,
    binding: ?ActivationBinding,
) !PreparedTestStaging {
    const source_root = try std.fs.path.join(alloc, &.{ root, "source", generation });
    defer alloc.free(source_root);
    const source_path = try std.fs.path.join(alloc, &.{ source_root, "data/catalog.txt" });
    defer alloc.free(source_path);
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    try fs_paths.createDirPathPortable(io_impl.io(), std.fs.path.dirname(source_path).?);
    {
        var file = try std.Io.Dir.cwd().createFile(io_impl.io(), source_path, .{ .truncate = true });
        defer file.close(io_impl.io());
        try file.writeStreamingAll(io_impl.io(), body);
    }

    const files = [_]backup_manifest.FileEntry{.{
        .path = "data/catalog.txt",
        .kind = .manifest,
        .size_bytes = body.len,
        .crc32 = backup_manifest.crc32(body),
    }};
    const manifest = try backup_manifest.encodeAlloc(alloc, .{
        .identity = identity,
        .manifest_id = generation,
        .backup_lsn = 8,
        .checkpoint_lsn = 11,
        .files = &files,
    });
    defer alloc.free(manifest);

    var capture_receipt_sha256: [Sha256.digest_length * 2]u8 = [_]u8{0} ** (Sha256.digest_length * 2);
    var capture_receipt_json: ?[]u8 = null;
    defer if (capture_receipt_json) |json| alloc.free(json);
    if (binding) |authority| {
        var manifest_digest: [Sha256.digest_length]u8 = undefined;
        Sha256.hash(manifest, &manifest_digest, .{});
        var manifest_sha256: [Sha256.digest_length * 2]u8 = undefined;
        encodeHex(&manifest_sha256, &manifest_digest);
        capture_receipt_json = try std.json.Stringify.valueAlloc(alloc, seed_capture.CaptureReceipt{
            .format_version = seed_capture.format_version,
            .generation = generation,
            .slot_name = "standby-a",
            .cluster_id = identity.cluster_id,
            .shard_id = identity.shard_id,
            .table_id = identity.table_id,
            .timeline_id = identity.timeline_id,
            .epoch = identity.epoch,
            .source_plan_sha256 = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            .manifest_id = generation,
            .backup_lsn = 8,
            .checkpoint_lsn = 11,
            .end_record_lsn = 12,
            .manifest_sha256 = &manifest_sha256,
            .file_count = files.len,
            .total_bytes = body.len,
            .topology_id = authority.topology_id,
            .topology_generation = authority.topology_generation,
            .node_id = authority.node_id,
            .target_pvc_name = authority.target_pvc_name,
            .target_pvc_uid = authority.target_pvc_uid,
        }, .{});
        var capture_digest: [Sha256.digest_length]u8 = undefined;
        Sha256.hash(capture_receipt_json.?, &capture_digest, .{});
        encodeHex(&capture_receipt_sha256, &capture_digest);
    }

    var memory = object_storage.MemoryObjectStorage.init(alloc);
    defer memory.deinit();
    var client = memory.client();
    try client.makeBucket("ha-seeds");
    const store = seed_artifact.Store{ .client = &client, .bucket = "ha-seeds" };
    var published = try seed_artifact.publish(alloc, store, .{
        .generation = generation,
        .slot_name = "standby-a",
        .manifest_bytes = manifest,
        .content_root = source_root,
        .capture_receipt_json = capture_receipt_json,
        .capture_receipt_sha256 = if (binding != null) &capture_receipt_sha256 else null,
        .binding = binding,
    });
    defer published.deinit(alloc);

    const staging_root = try std.fs.path.join(alloc, &.{ root, "staging", generation });
    errdefer alloc.free(staging_root);
    var restored = try seed_artifact.restoreToStaging(alloc, store, .{
        .expected = .{
            .generation = generation,
            .slot_name = "standby-a",
            .identity = identity,
            .minimum_checkpoint_lsn = 11,
            .binding = binding,
            .capture_receipt_sha256 = if (binding != null) &capture_receipt_sha256 else null,
        },
        .staging_root = staging_root,
    });
    restored.deinit(alloc);
    return .{
        .root = staging_root,
        .capture_receipt_sha256 = capture_receipt_sha256,
        .bound = binding != null,
    };
}

fn prepareTestStaging(alloc: Allocator, root: []const u8, generation: []const u8, identity: standby_mod.Identity, body: []const u8) ![]u8 {
    const prepared = try prepareTestStagingWithBinding(alloc, root, generation, identity, body, null);
    std.debug.assert(!prepared.bound);
    return prepared.root;
}

fn prepareMaterializedTestStaging(
    alloc: Allocator,
    root: []const u8,
    generation: []const u8,
    identity: standby_mod.Identity,
    binding: ActivationBinding,
) !PreparedTestStaging {
    const source_root = try std.fs.path.join(alloc, &.{ root, "materialized-source", generation });
    defer alloc.free(source_root);
    const source_snapshot_root = try std.fs.path.join(alloc, &.{ source_root, "replicas/group-1" });
    defer alloc.free(source_snapshot_root);
    const live_db_path = try std.fs.path.join(alloc, &.{ root, "materialized-primary", generation, "table-db" });
    defer alloc.free(live_db_path);
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    try fs_paths.createDirPathPortable(io, source_snapshot_root);

    {
        var db = try @import("../db/db.zig").DB.open(alloc, live_db_path, .{
            .identity_namespace = .{
                .table_id = identity.table_id,
                .shard_id = identity.shard_id,
                .range_id = 1,
            },
            .start_index_workers = false,
            .start_optional_runtimes = false,
        });
        defer db.close();
        try db.batch(.{ .writes = &.{.{ .key = "doc:seed", .value = "\"materialized-value\"" }} });
        _ = try db.snapshot("portable-seed");
    }
    const captured_snapshot_root = try std.fmt.allocPrint(alloc, "{s}.snapshots/portable-seed", .{live_db_path});
    defer alloc.free(captured_snapshot_root);
    const store_source = try std.fs.path.join(alloc, &.{ captured_snapshot_root, "store.bin" });
    defer alloc.free(store_source);
    const store_target = try std.fs.path.join(alloc, &.{ source_snapshot_root, "store.bin" });
    defer alloc.free(store_target);
    try copyFileDurably(io, store_source, store_target, source_snapshot_root);
    const journal_source = try std.fs.path.join(alloc, &.{ captured_snapshot_root, "change-journal.bin" });
    defer alloc.free(journal_source);
    const journal_target = try std.fs.path.join(alloc, &.{ source_snapshot_root, "change-journal.bin" });
    defer alloc.free(journal_target);
    try copyFileDurably(io, journal_source, journal_target, source_snapshot_root);

    const store_bytes = try readFileAlloc(io, alloc, store_target, 16 * 1024 * 1024);
    defer alloc.free(store_bytes);
    const journal_bytes = try readFileAlloc(io, alloc, journal_target, 16 * 1024 * 1024);
    defer alloc.free(journal_bytes);
    var store_digest: [Sha256.digest_length]u8 = undefined;
    Sha256.hash(store_bytes, &store_digest, .{});
    var store_sha256: [Sha256.digest_length * 2]u8 = undefined;
    encodeHex(&store_sha256, &store_digest);
    const topology_json = try std.json.Stringify.valueAlloc(alloc, seed_materialization.Topology{
        .generation = generation,
        .catalog = .{
            .epoch = 1,
            .tables = &.{.{
                .table_id = identity.table_id,
                .name = "docs",
                .desired_replica_count = 1,
            }},
            .ranges = &.{.{
                .group_id = 1,
                .range_id = 1,
                .table_id = identity.table_id,
                .start_key = "",
                .doc_identity_shard_id = identity.shard_id,
                .doc_identity_range_id = 1,
            }},
        },
        .replicas = &.{.{
            .group_id = 1,
            .table_id = identity.table_id,
            .table_name = "docs",
            .snapshot_path = "replicas/group-1",
            .logical_sha256 = &store_sha256,
            .identity_table_id = identity.table_id,
            .identity_shard_id = identity.shard_id,
            .identity_range_id = 1,
        }},
    }, .{});
    defer alloc.free(topology_json);
    const topology_path = try std.fs.path.join(alloc, &.{ source_root, seed_materialization.topology_name });
    defer alloc.free(topology_path);
    {
        var file = try std.Io.Dir.cwd().createFile(io, topology_path, .{ .truncate = true });
        defer file.close(io);
        try file.writeStreamingAll(io, topology_json);
        try file.sync(io);
    }
    try fs_paths.syncDirPortable(io, source_root);

    const files = [_]backup_manifest.FileEntry{
        .{ .path = seed_materialization.topology_name, .kind = .manifest, .size_bytes = topology_json.len, .crc32 = backup_manifest.crc32(topology_json) },
        .{ .path = "replicas/group-1/store.bin", .kind = .artifact, .size_bytes = store_bytes.len, .crc32 = backup_manifest.crc32(store_bytes) },
        .{ .path = "replicas/group-1/change-journal.bin", .kind = .artifact, .size_bytes = journal_bytes.len, .crc32 = backup_manifest.crc32(journal_bytes) },
    };
    const manifest = try backup_manifest.encodeAlloc(alloc, .{
        .identity = identity,
        .manifest_id = generation,
        .backup_lsn = 8,
        .checkpoint_lsn = 11,
        .files = &files,
    });
    defer alloc.free(manifest);
    var manifest_digest: [Sha256.digest_length]u8 = undefined;
    Sha256.hash(manifest, &manifest_digest, .{});
    var manifest_sha256: [Sha256.digest_length * 2]u8 = undefined;
    encodeHex(&manifest_sha256, &manifest_digest);
    const capture_receipt_json = try std.json.Stringify.valueAlloc(alloc, seed_capture.CaptureReceipt{
        .format_version = seed_capture.format_version,
        .generation = generation,
        .slot_name = "standby-a",
        .cluster_id = identity.cluster_id,
        .shard_id = identity.shard_id,
        .table_id = identity.table_id,
        .timeline_id = identity.timeline_id,
        .epoch = identity.epoch,
        .source_plan_sha256 = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        .manifest_id = generation,
        .backup_lsn = 8,
        .checkpoint_lsn = 11,
        .end_record_lsn = 12,
        .manifest_sha256 = &manifest_sha256,
        .file_count = files.len,
        .total_bytes = topology_json.len + store_bytes.len + journal_bytes.len,
        .topology_id = binding.topology_id,
        .topology_generation = binding.topology_generation,
        .node_id = binding.node_id,
        .target_pvc_name = binding.target_pvc_name,
        .target_pvc_uid = binding.target_pvc_uid,
    }, .{});
    defer alloc.free(capture_receipt_json);
    var capture_digest: [Sha256.digest_length]u8 = undefined;
    Sha256.hash(capture_receipt_json, &capture_digest, .{});
    var capture_receipt_sha256: [Sha256.digest_length * 2]u8 = undefined;
    encodeHex(&capture_receipt_sha256, &capture_digest);

    var memory = object_storage.MemoryObjectStorage.init(alloc);
    defer memory.deinit();
    var client = memory.client();
    try client.makeBucket("ha-seeds");
    const store = seed_artifact.Store{ .client = &client, .bucket = "ha-seeds" };
    var published = try seed_artifact.publish(alloc, store, .{
        .generation = generation,
        .slot_name = "standby-a",
        .manifest_bytes = manifest,
        .content_root = source_root,
        .capture_receipt_json = capture_receipt_json,
        .capture_receipt_sha256 = &capture_receipt_sha256,
        .binding = binding,
    });
    defer published.deinit(alloc);
    const staging_root = try std.fs.path.join(alloc, &.{ root, "materialized-staging", generation });
    errdefer alloc.free(staging_root);
    var restored = try seed_artifact.restoreToStaging(alloc, store, .{
        .expected = .{
            .generation = generation,
            .slot_name = "standby-a",
            .identity = identity,
            .minimum_checkpoint_lsn = 11,
            .binding = binding,
            .capture_receipt_sha256 = &capture_receipt_sha256,
        },
        .staging_root = staging_root,
    });
    restored.deinit(alloc);
    return .{ .root = staging_root, .capture_receipt_sha256 = capture_receipt_sha256, .bound = true };
}

fn expectPathMissing(io: std.Io, path: []const u8) !void {
    try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().access(io, path, .{}));
}

test "storage.ha seed activation publishes a verified immutable generation idempotently" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(std.testing.io, ".", alloc);
    defer alloc.free(root);
    const staging_root = try prepareTestStaging(alloc, root, "gen-0001", testIdentity(), "catalog-v1");
    defer alloc.free(staging_root);
    const target_root = try std.fs.path.join(alloc, &.{ root, "target" });
    defer alloc.free(target_root);
    const request = ActivateRequest{
        .staging_root = staging_root,
        .target_root = target_root,
        .expected = .{
            .generation = "gen-0001",
            .slot_name = "standby-a",
            .identity = testIdentity(),
            .minimum_checkpoint_lsn = 11,
        },
    };

    var activated = try activate(alloc, request);
    defer activated.deinit(alloc);
    try std.testing.expect(!activated.already_active);
    const installed_file = try std.fs.path.join(alloc, &.{ activated.generation_path, "data/catalog.txt" });
    defer alloc.free(installed_file);
    const installed = try readFileAlloc(std.testing.io, alloc, installed_file, 128);
    defer alloc.free(installed);
    try std.testing.expectEqualStrings("catalog-v1", installed);

    var retried = try activate(alloc, request);
    defer retried.deinit(alloc);
    try std.testing.expect(retried.already_active);
    try std.testing.expectEqualStrings(activated.generation_path, retried.generation_path);
    try std.testing.expectEqualStrings(activated.active_receipt_json, retried.active_receipt_json);
}

test "storage.ha seed activation gc requires the durable seeded-slot activation checkpoint" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(std.testing.io, ".", alloc);
    defer alloc.free(root);
    const binding = ActivationBinding{
        .topology_id = "topology-a",
        .topology_generation = 3,
        .node_id = "standby-a",
        .target_pvc_name = "standby-a-data",
        .target_pvc_uid = "pvc-uid-1",
    };
    const prepared = try prepareTestStagingWithBinding(alloc, root, "gen-gc", testIdentity(), "catalog-gc", binding);
    defer alloc.free(prepared.root);
    const target_root = try std.fs.path.join(alloc, &.{ root, "target-gc" });
    defer alloc.free(target_root);
    var activated = try activate(alloc, .{
        .staging_root = prepared.root,
        .target_root = target_root,
        .expected = .{
            .generation = "gen-gc",
            .slot_name = "standby-a",
            .identity = testIdentity(),
            .minimum_checkpoint_lsn = 11,
            .binding = binding,
            .capture_receipt_sha256 = &prepared.capture_receipt_sha256,
        },
        .binding = binding,
    });
    defer activated.deinit(alloc);
    var active = try std.json.parseFromSlice(ActivationReceipt, alloc, activated.active_receipt_json, .{});
    defer active.deinit();
    const checkpoint_path = try std.fs.path.join(alloc, &.{ root, "seeded-slot-activation.json" });
    defer alloc.free(checkpoint_path);
    try std.testing.expectError(error.SeedActivationCheckpointMissing, pruneActivatedGenerations(alloc, .{
        .target_root = target_root,
        .slot_activation_receipt_path = checkpoint_path,
        .retain_generations = 1,
    }));
    const marker_path = try std.fs.path.join(alloc, &.{ activated.generation_path, local_generation_gc.marker_name });
    defer alloc.free(marker_path);
    try expectPathMissing(std.testing.io, marker_path);

    const checkpoint_json = try std.json.Stringify.valueAlloc(alloc, .{
        .schema_version = @as(i64, 1),
        .action = .{
            .action_id = "seeded_slot_activate:gen-gc",
            .action_kind = "seeded_slot_activate",
            .target = "gen-gc",
            .state = "applied",
            .node_id = "primary-a",
        },
        .slot_name = "standby-a",
        .generation = "gen-gc",
        .manifest_id = active.value.manifest_id,
        .timeline_id = @as(i64, @intCast(active.value.timeline_id)),
        .checkpoint_lsn = @as(i64, @intCast(active.value.checkpoint_lsn)),
        .seed_receipt_sha256 = active.value.seed_receipt_sha256,
        .capture_receipt_sha256 = active.value.capture_receipt_sha256,
        .manifest_sha256 = active.value.manifest_sha256,
        .aggregate_sha256 = active.value.aggregate_sha256,
    }, .{});
    defer alloc.free(checkpoint_json);
    {
        var file = try std.Io.Dir.cwd().createFile(std.testing.io, checkpoint_path, .{ .truncate = true });
        defer file.close(std.testing.io);
        try file.writeStreamingAll(std.testing.io, checkpoint_json);
        try file.sync(std.testing.io);
    }
    try fs_paths.syncDirPortable(std.testing.io, root);

    var pruned = try pruneActivatedGenerations(alloc, .{
        .target_root = target_root,
        .slot_activation_receipt_path = checkpoint_path,
        .retain_generations = 1,
    });
    defer pruned.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 0), pruned.deleted_generations);
    try std.Io.Dir.cwd().access(std.testing.io, marker_path, .{});
}

test "storage.ha materialized activation gc deletes raw and live generations as one lifecycle pair" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(std.testing.io, ".", alloc);
    defer alloc.free(root);
    const binding = ActivationBinding{
        .topology_id = "topology-a",
        .topology_generation = 3,
        .node_id = "standby-a",
        .target_pvc_name = "standby-a-data",
        .target_pvc_uid = "pvc-uid-1",
    };
    const prepared = try prepareMaterializedTestStaging(alloc, root, "gen-z-current", testIdentity(), binding);
    defer alloc.free(prepared.root);
    const target_root = try std.fs.path.join(alloc, &.{ root, "target-paired-gc" });
    defer alloc.free(target_root);
    var activated = try activate(alloc, .{
        .staging_root = prepared.root,
        .target_root = target_root,
        .expected = .{
            .generation = "gen-z-current",
            .slot_name = "standby-a",
            .identity = testIdentity(),
            .minimum_checkpoint_lsn = 11,
            .binding = binding,
            .capture_receipt_sha256 = &prepared.capture_receipt_sha256,
        },
        .binding = binding,
        .materialization = .{ .target_local_node_id = 7, .target_replica_id = 1 },
    });
    defer activated.deinit(alloc);
    var active = try std.json.parseFromSlice(ActivationReceipt, alloc, activated.active_receipt_json, .{});
    defer active.deinit();

    const checkpoint_path = try std.fs.path.join(alloc, &.{ root, "paired-gc-seeded-slot-activation.json" });
    defer alloc.free(checkpoint_path);
    const checkpoint_json = try std.json.Stringify.valueAlloc(alloc, .{
        .schema_version = @as(i64, 1),
        .action = .{
            .action_id = "seeded_slot_activate:gen-z-current",
            .action_kind = "seeded_slot_activate",
            .target = "gen-z-current",
            .state = "applied",
            .node_id = "primary-a",
        },
        .slot_name = "standby-a",
        .generation = "gen-z-current",
        .manifest_id = active.value.manifest_id,
        .timeline_id = @as(i64, @intCast(active.value.timeline_id)),
        .checkpoint_lsn = @as(i64, @intCast(active.value.checkpoint_lsn)),
        .seed_receipt_sha256 = active.value.seed_receipt_sha256,
        .capture_receipt_sha256 = active.value.capture_receipt_sha256,
        .manifest_sha256 = active.value.manifest_sha256,
        .aggregate_sha256 = active.value.aggregate_sha256,
    }, .{});
    defer alloc.free(checkpoint_json);
    {
        var file = try std.Io.Dir.cwd().createFile(std.testing.io, checkpoint_path, .{ .truncate = true });
        defer file.close(std.testing.io);
        try file.writeStreamingAll(std.testing.io, checkpoint_json);
        try file.sync(std.testing.io);
    }

    const old_raw = try std.fs.path.join(alloc, &.{ target_root, generations_dir_name, "gen-a-old" });
    defer alloc.free(old_raw);
    const old_live = try std.fs.path.join(alloc, &.{ target_root, live_generations_dir_name, "gen-a-old" });
    defer alloc.free(old_live);
    try fs_paths.createDirPathPortable(std.testing.io, old_raw);
    try fs_paths.createDirPathPortable(std.testing.io, old_live);
    const old_raw_file = try std.fs.path.join(alloc, &.{ old_raw, "raw" });
    defer alloc.free(old_raw_file);
    const old_live_file = try std.fs.path.join(alloc, &.{ old_live, "live" });
    defer alloc.free(old_live_file);
    for ([_]struct { path: []const u8, body: []const u8 }{
        .{ .path = old_raw_file, .body = "immutable-transport" },
        .{ .path = old_live_file, .body = "mutable-runtime" },
    }) |entry| {
        var file = try std.Io.Dir.cwd().createFile(std.testing.io, entry.path, .{ .truncate = true });
        defer file.close(std.testing.io);
        try file.writeStreamingAll(std.testing.io, entry.body);
        try file.sync(std.testing.io);
    }
    try local_generation_gc.markEligible(alloc, .{
        .root = target_root,
        .scope = .target_activation,
        .generation = "gen-a-old",
        .slot_name = "standby-a",
        .checkpoint_lsn = 1,
        .checkpoint_bytes = "old-durable-seeded-slot-activation",
    });

    var pruned = try pruneActivatedGenerations(alloc, .{
        .target_root = target_root,
        .slot_activation_receipt_path = checkpoint_path,
        .retain_generations = 1,
    });
    defer pruned.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), pruned.deleted_generations);
    try expectPathMissing(std.testing.io, old_raw);
    try expectPathMissing(std.testing.io, old_live);
    const current_raw = try std.fs.path.join(alloc, &.{ target_root, generations_dir_name, "gen-z-current" });
    defer alloc.free(current_raw);
    const current_live = try std.fs.path.join(alloc, &.{ target_root, live_generations_dir_name, "gen-z-current" });
    defer alloc.free(current_live);
    try std.Io.Dir.cwd().access(std.testing.io, current_raw, .{});
    try std.Io.Dir.cwd().access(std.testing.io, current_live, .{});
}

test "storage.ha seed activation binds startup evidence and revalidates installed bytes on restart" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(std.testing.io, ".", alloc);
    defer alloc.free(root);
    const binding = ActivationBinding{
        .topology_id = "topology-a",
        .topology_generation = 3,
        .node_id = "standby-a",
        .target_pvc_name = "standby-a-data",
        .target_pvc_uid = "pvc-uid-1",
    };
    const prepared = try prepareTestStagingWithBinding(alloc, root, "gen-bound", testIdentity(), "catalog-bound", binding);
    defer alloc.free(prepared.root);
    const target_root = try std.fs.path.join(alloc, &.{ root, "target-bound" });
    defer alloc.free(target_root);
    const expected = seed_artifact.ExpectedArtifact{
        .generation = "gen-bound",
        .slot_name = "standby-a",
        .identity = testIdentity(),
        .minimum_checkpoint_lsn = 11,
        .binding = binding,
        .capture_receipt_sha256 = &prepared.capture_receipt_sha256,
    };

    var wrong_expected = expected;
    wrong_expected.capture_receipt_sha256 = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    try std.testing.expectError(error.WrongCaptureReceiptDigest, activate(alloc, .{
        .staging_root = prepared.root,
        .target_root = target_root,
        .expected = wrong_expected,
        .binding = binding,
        .pod_uid = "pod-activation-wrong-capture",
    }));
    try expectPathMissing(std.testing.io, target_root);

    var activated = try activate(alloc, .{
        .staging_root = prepared.root,
        .target_root = target_root,
        .expected = expected,
        .binding = binding,
        .pod_uid = "pod-activation-1",
    });
    defer activated.deinit(alloc);
    var receipt = try std.json.parseFromSlice(ActivationReceipt, alloc, activated.active_receipt_json, .{});
    defer receipt.deinit();
    try std.testing.expectEqualStrings("topology-a", receipt.value.topology_id);
    try std.testing.expectEqual(@as(u64, 3), receipt.value.topology_generation);
    try std.testing.expectEqualStrings("pvc-uid-1", receipt.value.target_pvc_uid);
    try std.testing.expectEqualStrings(&prepared.capture_receipt_sha256, receipt.value.capture_receipt_sha256);

    var ledger = try lifecycle_receipt_ledger.Ledger.open(alloc, target_root, .{});
    var page = try ledger.readPage(alloc, .activation, .{ .limit = 10 }, .{ .authoritative_root = target_root });
    try std.testing.expectEqual(@as(usize, 1), page.entries.len);
    try std.testing.expectEqualStrings(activated.active_receipt_json, page.entries[0].receipt_json);
    try std.testing.expectEqualStrings("pod-activation-1", page.entries[0].pod_uid.?);
    page.deinit(alloc);
    ledger.close();

    var repeated = try activate(alloc, .{
        .staging_root = prepared.root,
        .target_root = target_root,
        .expected = expected,
        .binding = binding,
        .pod_uid = "pod-activation-retry",
    });
    defer repeated.deinit(alloc);
    try std.testing.expect(repeated.already_active);
    ledger = try lifecycle_receipt_ledger.Ledger.open(alloc, target_root, .{});
    defer ledger.close();
    var after_retry = try ledger.readPage(alloc, .activation, .{ .limit = 10 }, .{ .authoritative_root = target_root });
    defer after_retry.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), after_retry.entries.len);

    const expectation = StartupExpectation{
        .target_root = target_root,
        .expected = expected,
        .binding = binding,
        .manifest_sha256 = receipt.value.manifest_sha256,
        .aggregate_sha256 = receipt.value.aggregate_sha256,
        .seed_receipt_sha256 = receipt.value.seed_receipt_sha256,
        .capture_receipt_sha256 = receipt.value.capture_receipt_sha256,
    };
    try std.testing.expectEqual(@as(u64, 11), try validateActivatedGeneration(alloc, expectation));
    try std.testing.expectEqual(@as(u64, 11), try validateActivatedGeneration(alloc, expectation));

    var stale = expectation;
    stale.binding.topology_generation = 2;
    try std.testing.expectError(error.ActiveGenerationConflict, validateActivatedGeneration(alloc, stale));

    var wrong_capture = expectation;
    wrong_capture.capture_receipt_sha256 = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    try std.testing.expectError(error.ActiveGenerationConflict, validateActivatedGeneration(alloc, wrong_capture));

    const installed_file = try std.fs.path.join(alloc, &.{ activated.generation_path, "data/catalog.txt" });
    defer alloc.free(installed_file);
    {
        var file = try std.Io.Dir.cwd().createFile(std.testing.io, installed_file, .{ .truncate = true });
        defer file.close(std.testing.io);
        try file.writeStreamingAll(std.testing.io, "tampered");
    }
    try std.testing.expectError(error.SeedGenerationConflict, validateActivatedGeneration(alloc, expectation));
}

test "storage.ha bound activation keeps immutable transport separate from mutable live runtime" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(std.testing.io, ".", alloc);
    defer alloc.free(root);
    const binding = ActivationBinding{
        .topology_id = "topology-a",
        .topology_generation = 3,
        .node_id = "standby-a",
        .target_pvc_name = "standby-a-data",
        .target_pvc_uid = "pvc-uid-1",
    };
    const prepared = try prepareMaterializedTestStaging(
        alloc,
        root,
        "gen-materialized",
        testIdentity(),
        binding,
    );
    defer alloc.free(prepared.root);
    const target_root = try std.fs.path.join(alloc, &.{ root, "target-materialized" });
    defer alloc.free(target_root);
    const expected = seed_artifact.ExpectedArtifact{
        .generation = "gen-materialized",
        .slot_name = "standby-a",
        .identity = testIdentity(),
        .minimum_checkpoint_lsn = 11,
        .binding = binding,
        .capture_receipt_sha256 = &prepared.capture_receipt_sha256,
    };

    var activated = try activate(alloc, .{
        .staging_root = prepared.root,
        .target_root = target_root,
        .expected = expected,
        .binding = binding,
        .materialization = .{ .target_local_node_id = 7, .target_replica_id = 1 },
        .pod_uid = "pod-materialize",
    });
    defer activated.deinit(alloc);

    const raw_generation_path = try std.fs.path.join(alloc, &.{ target_root, generations_dir_name, "gen-materialized" });
    defer alloc.free(raw_generation_path);
    const expected_live_path = try std.fs.path.join(alloc, &.{ target_root, "live-generations", "gen-materialized" });
    defer alloc.free(expected_live_path);
    try std.testing.expectEqualStrings(expected_live_path, activated.generation_path);

    const raw_catalog_path = try std.fs.path.join(alloc, &.{ raw_generation_path, seed_materialization.topology_name });
    defer alloc.free(raw_catalog_path);
    const raw_before = try readFileAlloc(std.testing.io, alloc, raw_catalog_path, 1024);
    defer alloc.free(raw_before);
    try std.testing.expect(std.mem.indexOf(u8, raw_before, "\"generation\":\"gen-materialized\"") != null);

    // Runtime-owned files must be allowed to evolve after ACTIVE publication.
    // Restart validates the immutable receipt chain and installed identity,
    // never by treating the mutable live tree as the raw transport artifact.
    const live_db_path = try std.fs.path.join(alloc, &.{ expected_live_path, "data/replicas/group-1/table-db" });
    defer alloc.free(live_db_path);
    {
        var db = try @import("../db/db.zig").DB.open(alloc, live_db_path, .{
            .identity_namespace = .{ .table_id = testIdentity().table_id, .shard_id = testIdentity().shard_id, .range_id = 1 },
            .start_index_workers = false,
            .start_optional_runtimes = false,
        });
        defer db.close();
        try db.batch(.{ .writes = &.{.{ .key = "doc:after-activation", .value = "\"runtime-write\"" }} });
    }
    try std.testing.expectEqual(@as(u64, 11), try validateActivatedGeneration(alloc, .{
        .target_root = target_root,
        .expected = expected,
        .binding = binding,
    }));
    const raw_after = try readFileAlloc(std.testing.io, alloc, raw_catalog_path, 1024);
    defer alloc.free(raw_after);
    try std.testing.expectEqualSlices(u8, raw_before, raw_after);
}

test "storage.ha materialized activation recovers every raw live and ACTIVE publication crash" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(std.testing.io, ".", alloc);
    defer alloc.free(root);
    const binding = ActivationBinding{
        .topology_id = "topology-a",
        .topology_generation = 3,
        .node_id = "standby-a",
        .target_pvc_name = "standby-a-data",
        .target_pvc_uid = "pvc-uid-1",
    };
    const prepared = try prepareMaterializedTestStaging(
        alloc,
        root,
        "gen-materialized-crash",
        testIdentity(),
        binding,
    );
    defer alloc.free(prepared.root);
    const expected = seed_artifact.ExpectedArtifact{
        .generation = "gen-materialized-crash",
        .slot_name = "standby-a",
        .identity = testIdentity(),
        .minimum_checkpoint_lsn = 11,
        .binding = binding,
        .capture_receipt_sha256 = &prepared.capture_receipt_sha256,
    };

    inline for (.{
        FailureBoundary.generation_copied,
        FailureBoundary.generation_published,
        FailureBoundary.live_generation_materialized,
        FailureBoundary.live_generation_published,
        FailureBoundary.active_published,
    }, 0..) |boundary, index| {
        const target_root = try std.fmt.allocPrint(alloc, "{s}/target-materialized-crash-{d}", .{ root, index });
        defer alloc.free(target_root);
        const request = ActivateRequest{
            .staging_root = prepared.root,
            .target_root = target_root,
            .expected = expected,
            .binding = binding,
            .materialization = .{ .target_local_node_id = 7, .target_replica_id = 1 },
            .pod_uid = "pod-materialized-crash",
        };
        try std.testing.expectError(
            error.InjectedActivationFailure,
            activateWithOptions(alloc, request, .{ .fail_after = boundary }),
        );

        const active_path = try std.fs.path.join(alloc, &.{ target_root, active_receipt_name });
        defer alloc.free(active_path);
        if (boundary == .active_published) {
            try std.Io.Dir.cwd().access(std.testing.io, active_path, .{});
        } else {
            try expectPathMissing(std.testing.io, active_path);
        }

        var recovered = try activate(alloc, request);
        defer recovered.deinit(alloc);
        try std.testing.expectEqual(boundary == .active_published, recovered.already_active);
        try std.testing.expectEqual(@as(u64, 11), try validateActivatedGeneration(alloc, .{
            .target_root = target_root,
            .expected = expected,
            .binding = binding,
            .target_local_node_id = 7,
            .target_replica_id = 1,
        }));
    }
}

test "storage.ha seed activation rejects unrelated nonempty targets without mutation" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(std.testing.io, ".", alloc);
    defer alloc.free(root);
    const staging_root = try prepareTestStaging(alloc, root, "gen-safe", testIdentity(), "seed-data");
    defer alloc.free(staging_root);
    const target_root = try std.fs.path.join(alloc, &.{ root, "occupied" });
    defer alloc.free(target_root);
    const unrelated = try std.fs.path.join(alloc, &.{ target_root, "primary.db" });
    defer alloc.free(unrelated);
    try fs_paths.createDirPathPortable(std.testing.io, target_root);
    {
        var file = try std.Io.Dir.cwd().createFile(std.testing.io, unrelated, .{ .truncate = true });
        defer file.close(std.testing.io);
        try file.writeStreamingAll(std.testing.io, "do-not-overwrite");
    }

    try std.testing.expectError(error.UnsafeActivationTarget, activate(alloc, .{
        .staging_root = staging_root,
        .target_root = target_root,
        .expected = .{
            .generation = "gen-safe",
            .slot_name = "standby-a",
            .identity = testIdentity(),
        },
    }));
    const preserved = try readFileAlloc(std.testing.io, alloc, unrelated, 128);
    defer alloc.free(preserved);
    try std.testing.expectEqualStrings("do-not-overwrite", preserved);
}

test "storage.ha seed activation rejects cross-identity and conflicting generations" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(std.testing.io, ".", alloc);
    defer alloc.free(root);
    const staging_a = try prepareTestStaging(alloc, root, "gen-a", testIdentity(), "generation-a");
    defer alloc.free(staging_a);
    const target_root = try std.fs.path.join(alloc, &.{ root, "target" });
    defer alloc.free(target_root);

    var wrong_identity = testIdentity();
    wrong_identity.cluster_id += 1;
    try std.testing.expectError(error.WrongCluster, activate(alloc, .{
        .staging_root = staging_a,
        .target_root = target_root,
        .expected = .{
            .generation = "gen-a",
            .slot_name = "standby-a",
            .identity = wrong_identity,
        },
    }));
    try expectPathMissing(std.testing.io, target_root);

    var first = try activate(alloc, .{
        .staging_root = staging_a,
        .target_root = target_root,
        .expected = .{
            .generation = "gen-a",
            .slot_name = "standby-a",
            .identity = testIdentity(),
        },
    });
    defer first.deinit(alloc);

    const staging_b = try prepareTestStaging(alloc, root, "gen-b", testIdentity(), "generation-b");
    defer alloc.free(staging_b);
    try std.testing.expectError(error.ActiveGenerationConflict, activate(alloc, .{
        .staging_root = staging_b,
        .target_root = target_root,
        .expected = .{
            .generation = "gen-b",
            .slot_name = "standby-a",
            .identity = testIdentity(),
        },
    }));
}

test "storage.ha seed activation recovers each publication crash boundary" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(std.testing.io, ".", alloc);
    defer alloc.free(root);
    const staging_root = try prepareTestStaging(alloc, root, "gen-crash", testIdentity(), "crash-safe");
    defer alloc.free(staging_root);

    inline for (.{
        FailureBoundary.generation_copied,
        FailureBoundary.generation_published,
        FailureBoundary.active_published,
    }, 0..) |boundary, index| {
        const target_name = try std.fmt.allocPrint(alloc, "target-{d}", .{index});
        defer alloc.free(target_name);
        const target_root = try std.fs.path.join(alloc, &.{ root, target_name });
        defer alloc.free(target_root);
        const request = ActivateRequest{
            .staging_root = staging_root,
            .target_root = target_root,
            .expected = .{
                .generation = "gen-crash",
                .slot_name = "standby-a",
                .identity = testIdentity(),
            },
        };
        try std.testing.expectError(error.InjectedActivationFailure, activateWithOptions(alloc, request, .{ .fail_after = boundary }));

        const active_path = try std.fs.path.join(alloc, &.{ target_root, active_receipt_name });
        defer alloc.free(active_path);
        if (boundary == .active_published) {
            try std.Io.Dir.cwd().access(std.testing.io, active_path, .{});
        } else {
            try expectPathMissing(std.testing.io, active_path);
        }

        var recovered = try activate(alloc, request);
        defer recovered.deinit(alloc);
        try std.testing.expect(boundary != .active_published or recovered.already_active);
        try seed_artifact.verifyStaged(alloc, recovered.generation_path, request.expected, .{});
    }
}
