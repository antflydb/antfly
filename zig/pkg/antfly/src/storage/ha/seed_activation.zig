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
//! 4. Publish `ACTIVE.json` with an atomic no-replace operation.
//!
//! Until step 4 completes there is no active generation. A retry may rebuild
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
const standby_mod = @import("standby.zig");
const validation = @import("validation.zig");

pub const format_version: u16 = 1;
pub const generations_dir_name = "generations";
pub const active_receipt_name = "ACTIVE.json";
pub const generation_receipt_name = ".antfly-ha-active-generation.json";

pub const ActivateRequest = struct {
    staging_root: []const u8,
    target_root: []const u8,
    expected: seed_artifact.ExpectedArtifact,
    binding: ?ActivationBinding = null,
    pod_uid: ?[]const u8 = null,
    limits: seed_artifact.Limits = .{},
};

pub const ActivationBinding = seed_artifact.LifecycleBinding;

pub const StartupExpectation = struct {
    target_root: []const u8,
    expected: seed_artifact.ExpectedArtifact,
    binding: ActivationBinding,
    manifest_sha256: ?[]const u8 = null,
    aggregate_sha256: ?[]const u8 = null,
    seed_receipt_sha256: ?[]const u8 = null,
    limits: seed_artifact.Limits = .{},
};

pub const ActivationResult = struct {
    /// Absolute path of the immutable generation that the runtime may open.
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
    manifest_sha256: []const u8,
    aggregate_sha256: []const u8,
    generation_path: []const u8,
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

const FailureBoundary = enum {
    generation_copied,
    generation_published,
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
    manifest_sha256: []const u8,
    aggregate_sha256: []const u8,
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

    const generation_path = try std.fs.path.join(alloc, &.{ request.target_root, active.value.generation_path });
    defer alloc.free(generation_path);
    try validatePublishedGeneration(alloc, generation_path, .{
        .staging_root = "/unused",
        .target_root = request.target_root,
        .expected = .{
            .generation = active.value.generation,
            .slot_name = active.value.slot_name,
            .identity = active.value.identity(),
            .minimum_checkpoint_lsn = active.value.checkpoint_lsn,
        },
        .limits = request.limits,
    }, active_json);

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
    });
}

fn validateActiveForGC(alloc: Allocator, receipt: ActivationReceipt) !void {
    if (receipt.format_version != format_version or
        !validation.isIdentifier(receipt.generation) or
        !validation.isIdentifier(receipt.slot_name) or
        receipt.manifest_id.len == 0 or
        receipt.backup_lsn == 0 or
        receipt.checkpoint_lsn < receipt.backup_lsn or
        receipt.seed_receipt_sha256.len != Sha256.digest_length * 2 or
        receipt.manifest_sha256.len != Sha256.digest_length * 2 or
        receipt.aggregate_sha256.len != Sha256.digest_length * 2)
        return error.InvalidActiveReceipt;
    try standby_mod.validateIdentity(receipt.identity());
    const expected_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ generations_dir_name, receipt.generation });
    defer alloc.free(expected_path);
    if (!std.mem.eql(u8, receipt.generation_path, expected_path)) return error.InvalidActiveReceipt;
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
        !std.mem.eql(u8, checkpoint.manifest_sha256, active.manifest_sha256) or
        !std.mem.eql(u8, checkpoint.aggregate_sha256, active.aggregate_sha256))
        return error.SeedActivationCheckpointMismatch;
}

fn activateWithOptions(alloc: Allocator, request: ActivateRequest, options: ActivateOptions) !ActivationResult {
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

    const generation_relative_path = try std.fs.path.join(alloc, &.{ generations_dir_name, request.expected.generation });
    defer alloc.free(generation_relative_path);
    const binding = request.binding orelse ActivationBinding{};
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
        .seed_receipt_sha256 = &seed_receipt_hex,
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
        try validateActiveReceipt(alloc, existing_active, request.expected, request.binding, &seed_receipt_hex, generation_relative_path);
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
    if (request.binding) |binding| try validateBinding(binding);
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
    generation_path: []const u8,
) !void {
    var parsed = std.json.parseFromSlice(ActivationReceipt, alloc, raw, .{}) catch return error.InvalidActiveReceipt;
    defer parsed.deinit();
    const receipt = parsed.value;
    if (receipt.format_version != format_version) return error.UnsupportedActivationVersion;
    if (!std.mem.eql(u8, receipt.generation, expected.generation) or
        !std.mem.eql(u8, receipt.slot_name, expected.slot_name) or
        !std.mem.eql(u8, receipt.seed_receipt_sha256, seed_receipt_sha256) or
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
    const generation_relative_path = try std.fs.path.join(alloc, &.{ generations_dir_name, expectation.expected.generation });
    defer alloc.free(generation_relative_path);
    if (receipt.format_version != format_version or
        !std.mem.eql(u8, receipt.generation, expectation.expected.generation) or
        !std.mem.eql(u8, receipt.slot_name, expectation.expected.slot_name) or
        !std.mem.eql(u8, receipt.generation_path, generation_relative_path) or
        receipt.checkpoint_lsn < expectation.expected.minimum_checkpoint_lsn) return error.ActiveGenerationConflict;
    try expectIdentity(expectation.expected.identity, receipt.identity(), error.ActiveGenerationConflict);
    try expectBinding(expectation.binding, receipt, error.ActiveGenerationConflict);
    try expectOptionalDigest(expectation.manifest_sha256, receipt.manifest_sha256);
    try expectOptionalDigest(expectation.aggregate_sha256, receipt.aggregate_sha256);
    try expectOptionalDigest(expectation.seed_receipt_sha256, receipt.seed_receipt_sha256);

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

fn expectOptionalDigest(expected: ?[]const u8, actual: []const u8) !void {
    if (expected) |digest| {
        if (digest.len != Sha256.digest_length * 2 or !std.mem.eql(u8, digest, actual)) return error.ActiveGenerationConflict;
    }
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

fn prepareTestStaging(alloc: Allocator, root: []const u8, generation: []const u8, identity: standby_mod.Identity, body: []const u8) ![]u8 {
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
        },
        .staging_root = staging_root,
    });
    restored.deinit(alloc);
    return staging_root;
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
    const staging_root = try prepareTestStaging(alloc, root, "gen-gc", testIdentity(), "catalog-gc");
    defer alloc.free(staging_root);
    const target_root = try std.fs.path.join(alloc, &.{ root, "target-gc" });
    defer alloc.free(target_root);
    var activated = try activate(alloc, .{
        .staging_root = staging_root,
        .target_root = target_root,
        .expected = .{
            .generation = "gen-gc",
            .slot_name = "standby-a",
            .identity = testIdentity(),
            .minimum_checkpoint_lsn = 11,
        },
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

test "storage.ha seed activation binds startup evidence and revalidates installed bytes on restart" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(std.testing.io, ".", alloc);
    defer alloc.free(root);
    const staging_root = try prepareTestStaging(alloc, root, "gen-bound", testIdentity(), "catalog-bound");
    defer alloc.free(staging_root);
    const target_root = try std.fs.path.join(alloc, &.{ root, "target-bound" });
    defer alloc.free(target_root);
    const binding = ActivationBinding{
        .topology_id = "topology-a",
        .topology_generation = 3,
        .node_id = "standby-a",
        .target_pvc_name = "standby-a-data",
        .target_pvc_uid = "pvc-uid-1",
    };
    const expected = seed_artifact.ExpectedArtifact{
        .generation = "gen-bound",
        .slot_name = "standby-a",
        .identity = testIdentity(),
        .minimum_checkpoint_lsn = 11,
    };

    var activated = try activate(alloc, .{
        .staging_root = staging_root,
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

    var ledger = try lifecycle_receipt_ledger.Ledger.open(alloc, target_root, .{});
    var page = try ledger.readPage(alloc, .activation, .{ .limit = 10 }, .{ .authoritative_root = target_root });
    try std.testing.expectEqual(@as(usize, 1), page.entries.len);
    try std.testing.expectEqualStrings(activated.active_receipt_json, page.entries[0].receipt_json);
    try std.testing.expectEqualStrings("pod-activation-1", page.entries[0].pod_uid.?);
    page.deinit(alloc);
    ledger.close();

    var repeated = try activate(alloc, .{
        .staging_root = staging_root,
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
    };
    try std.testing.expectEqual(@as(u64, 11), try validateActivatedGeneration(alloc, expectation));
    try std.testing.expectEqual(@as(u64, 11), try validateActivatedGeneration(alloc, expectation));

    var stale = expectation;
    stale.binding.topology_generation = 2;
    try std.testing.expectError(error.ActiveGenerationConflict, validateActivatedGeneration(alloc, stale));

    const installed_file = try std.fs.path.join(alloc, &.{ activated.generation_path, "data/catalog.txt" });
    defer alloc.free(installed_file);
    {
        var file = try std.Io.Dir.cwd().createFile(std.testing.io, installed_file, .{ .truncate = true });
        defer file.close(std.testing.io);
        try file.writeStreamingAll(std.testing.io, "tampered");
    }
    try std.testing.expectError(error.SeedGenerationConflict, validateActivatedGeneration(alloc, expectation));
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
