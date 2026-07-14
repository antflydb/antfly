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
        try validateActiveReceipt(alloc, existing_active, request.expected, &seed_receipt_hex, generation_relative_path);
        try inspectGenerationsRoot(io, generations_root, request.expected.generation, installing_name);
        try validatePublishedGeneration(alloc, generation_path, request, activation_json);
        alloc.free(activation_json);
        return .{
            .generation_path = generation_path,
            .active_receipt_json = try alloc.dupe(u8, existing_active),
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
            try copyFileDurably(io, source_path, destination_path);
        }
        const installed_receipt_path = try std.fs.path.join(alloc, &.{ installing_path, seed_artifact.receipt_name });
        defer alloc.free(installed_receipt_path);
        try copyFileDurably(io, staged_receipt_path, installed_receipt_path);
        const installed_manifest_path = try std.fs.path.join(alloc, &.{ installing_path, seed_artifact.staged_manifest_name });
        defer alloc.free(installed_manifest_path);
        try copyFileDurably(io, staged_manifest_path, installed_manifest_path);

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
}

fn validatePublishedGeneration(alloc: Allocator, generation_path: []const u8, request: ActivateRequest, activation_json: []const u8) !void {
    seed_artifact.verifyStaged(alloc, generation_path, request.expected, request.limits) catch return error.SeedGenerationConflict;
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const marker_path = try std.fs.path.join(alloc, &.{ generation_path, generation_receipt_name });
    defer alloc.free(marker_path);
    try verifyGenerationReceipt(io_impl.io(), alloc, marker_path, activation_json, request.limits.max_receipt_bytes);
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

fn copyFileDurably(io: std.Io, source_path: []const u8, destination_path: []const u8) !void {
    try std.Io.Dir.copyFile(std.Io.Dir.cwd(), source_path, std.Io.Dir.cwd(), destination_path, io, .{
        .make_path = true,
        .replace = false,
    });
    try fs_paths.syncFileAndParentPortable(io, destination_path);
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
