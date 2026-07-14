// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the License at https://www.antfly.io/licensing/ELv2-license.

//! Portable, immutable HA seed artifacts.
//!
//! A generation is invisible until `COMPLETE.json` is conditionally published.
//! Every reopenable file is bound to the framed HA backup manifest, a SHA-256
//! digest, and a generation aggregate. Uploads are safe to retry after process
//! or Job restarts: an existing object is accepted only when its bytes match.

const std = @import("std");
const Allocator = std.mem.Allocator;
const Sha256 = std.crypto.hash.sha2.Sha256;
const fs_paths = @import("../../common/fs_paths.zig");
const backup_manifest = @import("backup_manifest.zig");
const object_storage = @import("../object_storage.zig");
const standby_mod = @import("standby.zig");
const validation = @import("validation.zig");

pub const legacy_format_version: u16 = 1;
pub const format_version: u16 = 2;
pub const complete_name = "COMPLETE.json";
pub const manifest_name = "manifest.afha";
pub const receipt_name = ".antfly-ha-seed-receipt.json";
pub const staged_manifest_name = ".antfly-ha-seed-manifest.afha";
pub const staging_marker_name = ".antfly-ha-seed-staging.json";

pub const Limits = struct {
    max_manifest_bytes: usize = 16 * 1024 * 1024,
    max_receipt_bytes: usize = 16 * 1024 * 1024,
    max_file_bytes: usize = 8 * 1024 * 1024 * 1024,
    /// Maximum payload passed to any object-store put/get operation for file
    /// data. Manifest and receipt objects have their own independent bounds.
    max_chunk_bytes: usize = 8 * 1024 * 1024,
    max_total_bytes: u64 = 64 * 1024 * 1024 * 1024,
    max_files: usize = 1_000_000,
};

pub const Store = struct {
    client: *object_storage.ObjectStorage,
    bucket: []const u8,
    prefix: []const u8 = "",
};

pub const PublishRequest = struct {
    generation: []const u8,
    slot_name: []const u8,
    manifest_bytes: []const u8,
    content_root: []const u8,
    limits: Limits = .{},
};

pub const ExpectedArtifact = struct {
    generation: []const u8,
    slot_name: []const u8,
    identity: standby_mod.Identity,
    minimum_checkpoint_lsn: u64 = 0,
};

pub const RestoreRequest = struct {
    expected: ExpectedArtifact,
    staging_root: []const u8,
    limits: Limits = .{},
};

pub const PublishResult = struct {
    receipt_json: []u8,
    already_available: bool,

    pub fn deinit(self: *PublishResult, alloc: Allocator) void {
        alloc.free(self.receipt_json);
        self.* = undefined;
    }
};

pub const RestoreResult = struct {
    receipt_json: []u8,
    file_count: usize,
    total_bytes: u64,

    pub fn deinit(self: *RestoreResult, alloc: Allocator) void {
        alloc.free(self.receipt_json);
        self.* = undefined;
    }
};

pub const RemoteVerificationResult = struct {
    receipt_json: []u8,
    file_count: usize,
    total_bytes: u64,

    pub fn deinit(self: *RemoteVerificationResult, alloc: Allocator) void {
        alloc.free(self.receipt_json);
        self.* = undefined;
    }
};

pub const PruneRequest = struct {
    slot_name: []const u8,
    current_generation: []const u8,
    retain_generations: usize = 2,
    limits: Limits = .{},
};

pub const PruneResult = struct {
    result_json: []u8,
    deleted_generations: usize,

    pub fn deinit(self: *PruneResult, alloc: Allocator) void {
        alloc.free(self.result_json);
        self.* = undefined;
    }
};

pub const FileReceipt = struct {
    path: []const u8,
    size_bytes: u64,
    crc32: u32,
    sha256: []const u8,
    /// Absent only for legacy v1 generations, which stored one object per file.
    chunks: ?[]const ChunkReceipt = null,
};

pub const ChunkReceipt = struct {
    index: usize,
    size_bytes: u64,
    sha256: []const u8,
};

pub const Receipt = struct {
    format_version: u16,
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
    manifest_sha256: []const u8,
    aggregate_sha256: []const u8,
    total_bytes: u64,
    files: []const FileReceipt,

    pub fn identity(self: Receipt) standby_mod.Identity {
        return .{
            .cluster_id = self.cluster_id,
            .shard_id = self.shard_id,
            .table_id = self.table_id,
            .timeline_id = self.timeline_id,
            .epoch = self.epoch,
        };
    }
};

const StagingMarker = struct {
    format_version: u16 = format_version,
    generation: []const u8,
    slot_name: []const u8,
    cluster_id: u64,
    shard_id: u64,
    table_id: u64,
    timeline_id: u64,
    epoch: u64,
};

const StagingState = enum {
    prepared,
    already_complete,
};

const PruneReceipt = struct {
    // This is an action-result schema, not an artifact transport schema.
    format_version: u16 = legacy_format_version,
    slot_name: []const u8,
    current_generation: []const u8,
    retained_generations: usize,
    deleted_generations: usize,
};

const GenerationCandidate = struct {
    generation: []u8,
    checkpoint_lsn: u64,

    fn deinit(self: *GenerationCandidate, alloc: Allocator) void {
        alloc.free(self.generation);
        self.* = undefined;
    }
};

const OwnedFileReceipt = struct {
    path: []u8,
    sha256: []u8,
    size_bytes: u64,
    crc32: u32,
    chunks: []ChunkReceipt,

    fn view(self: OwnedFileReceipt) FileReceipt {
        return .{
            .path = self.path,
            .size_bytes = self.size_bytes,
            .crc32 = self.crc32,
            .sha256 = self.sha256,
            .chunks = self.chunks,
        };
    }

    fn deinit(self: *OwnedFileReceipt, alloc: Allocator) void {
        alloc.free(self.path);
        alloc.free(self.sha256);
        for (self.chunks) |chunk| alloc.free(chunk.sha256);
        alloc.free(self.chunks);
        self.* = undefined;
    }
};

const PublishOptions = struct {
    fail_before_complete: bool = false,
};

pub fn publish(alloc: Allocator, store: Store, request: PublishRequest) !PublishResult {
    return publishWithOptions(alloc, store, request, .{});
}

fn publishWithOptions(alloc: Allocator, store: Store, request: PublishRequest, options: PublishOptions) !PublishResult {
    try validateIdentifier(request.generation, error.InvalidSeedGeneration);
    try validateIdentifier(request.slot_name, error.InvalidSlotName);
    if (!validation.isNormalizedPath(request.content_root)) return error.InvalidContentRoot;
    if (request.manifest_bytes.len > request.limits.max_manifest_bytes) return error.ManifestTooLarge;
    if (request.limits.max_chunk_bytes == 0 or request.limits.max_chunk_bytes > request.limits.max_file_bytes)
        return error.InvalidArtifactChunkSize;

    const complete_key = try generationKeyAlloc(alloc, store.prefix, request.generation, complete_name);
    defer alloc.free(complete_key);
    if (getOptionalObject(alloc, store, complete_key, request.limits.max_receipt_bytes)) |existing| {
        defer alloc.free(existing);
        try validateExistingReceipt(alloc, existing, request.generation, request.slot_name, request.manifest_bytes);
        return .{ .receipt_json = try alloc.dupe(u8, existing), .already_available = true };
    } else |err| switch (err) {
        error.FileNotFound => {},
        else => return err,
    }

    const manifest = try backup_manifest.decodeAlloc(alloc, request.manifest_bytes);
    defer backup_manifest.freeDecoded(alloc, manifest);
    if (manifest.files.len > request.limits.max_files) return error.TooManyArtifactFiles;

    var total_bytes: u64 = 0;
    var owned_files = try alloc.alloc(OwnedFileReceipt, manifest.files.len);
    var owned_count: usize = 0;
    defer {
        for (owned_files[0..owned_count]) |*file| file.deinit(alloc);
        alloc.free(owned_files);
    }

    var aggregate = Sha256.init(.{});
    var manifest_digest: [Sha256.digest_length]u8 = undefined;
    Sha256.hash(request.manifest_bytes, &manifest_digest, .{});
    aggregate.update(&manifest_digest);
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    const chunk_buffer = try alloc.alloc(u8, request.limits.max_chunk_bytes);
    defer alloc.free(chunk_buffer);

    for (manifest.files, 0..) |file, idx| {
        if (file.size_bytes > request.limits.max_file_bytes) return error.ArtifactFileTooLarge;
        total_bytes = try std.math.add(u64, total_bytes, file.size_bytes);
        if (total_bytes > request.limits.max_total_bytes) return error.ArtifactTooLarge;

        const local_path = try std.fs.path.join(alloc, &.{ request.content_root, file.path });
        defer alloc.free(local_path);
        const stat = try std.Io.Dir.cwd().statFile(io, local_path, .{ .follow_symlinks = false });
        if (stat.kind != .file) return error.UnsupportedArtifactSource;
        if (stat.size != file.size_bytes) return error.ManifestFileSizeMismatch;
        var source = try std.Io.Dir.cwd().openFile(io, local_path, .{});
        defer source.close(io);
        var reader = source.reader(io, &.{});
        var file_sha = Sha256.init(.{});
        var file_crc = std.hash.Crc32.init();
        var streamed_bytes: u64 = 0;
        var chunk_receipts = std.ArrayListUnmanaged(ChunkReceipt).empty;
        errdefer {
            for (chunk_receipts.items) |chunk| alloc.free(chunk.sha256);
            chunk_receipts.deinit(alloc);
        }
        while (true) {
            const read = try readChunk(&reader.interface, chunk_buffer);
            if (read == 0) break;
            const body = chunk_buffer[0..read];
            streamed_bytes = try std.math.add(u64, streamed_bytes, read);
            if (streamed_bytes > file.size_bytes) return error.ManifestFileSizeMismatch;
            file_sha.update(body);
            file_crc.update(body);

            var chunk_digest: [Sha256.digest_length]u8 = undefined;
            Sha256.hash(body, &chunk_digest, .{});
            const chunk_index = chunk_receipts.items.len;
            const object_key = try generationChunkKeyAlloc(alloc, store.prefix, request.generation, idx, chunk_index);
            defer alloc.free(object_key);
            try putImmutable(alloc, store, object_key, body, "application/vnd.antfly.ha-chunk");
            const chunk_hex = try hexAlloc(alloc, &chunk_digest);
            errdefer alloc.free(chunk_hex);
            try chunk_receipts.append(alloc, .{
                .index = chunk_index,
                .size_bytes = read,
                .sha256 = chunk_hex,
            });
        }
        if (streamed_bytes != file.size_bytes) return error.ManifestFileSizeMismatch;
        if (file_crc.final() != file.crc32) return error.ManifestFileChecksumMismatch;
        var digest: [Sha256.digest_length]u8 = undefined;
        file_sha.final(&digest);
        aggregateFile(&aggregate, file.path, file.size_bytes, &digest);

        const chunks = try chunk_receipts.toOwnedSlice(alloc);
        errdefer {
            for (chunks) |chunk| alloc.free(chunk.sha256);
            alloc.free(chunks);
        }

        const owned_path = try alloc.dupe(u8, file.path);
        errdefer alloc.free(owned_path);
        const owned_sha = try hexAlloc(alloc, &digest);
        errdefer alloc.free(owned_sha);
        owned_files[idx] = .{
            .path = owned_path,
            .sha256 = owned_sha,
            .size_bytes = file.size_bytes,
            .crc32 = file.crc32,
            .chunks = chunks,
        };
        owned_count += 1;
    }

    const manifest_key = try generationKeyAlloc(alloc, store.prefix, request.generation, manifest_name);
    defer alloc.free(manifest_key);
    try putImmutable(alloc, store, manifest_key, request.manifest_bytes, "application/vnd.antfly.ha-manifest");

    var file_views = try alloc.alloc(FileReceipt, owned_files.len);
    defer alloc.free(file_views);
    for (owned_files, 0..) |file, idx| file_views[idx] = file.view();

    var aggregate_digest: [Sha256.digest_length]u8 = undefined;
    aggregate.final(&aggregate_digest);
    const manifest_hex = try hexAlloc(alloc, &manifest_digest);
    defer alloc.free(manifest_hex);
    const aggregate_hex = try hexAlloc(alloc, &aggregate_digest);
    defer alloc.free(aggregate_hex);

    const receipt_json = try std.json.Stringify.valueAlloc(alloc, Receipt{
        .format_version = format_version,
        .generation = request.generation,
        .slot_name = request.slot_name,
        .cluster_id = manifest.identity.cluster_id,
        .shard_id = manifest.identity.shard_id,
        .table_id = manifest.identity.table_id,
        .timeline_id = manifest.identity.timeline_id,
        .epoch = manifest.identity.epoch,
        .manifest_id = manifest.manifest_id,
        .backup_lsn = manifest.backup_lsn,
        .checkpoint_lsn = manifest.checkpoint_lsn,
        .manifest_sha256 = manifest_hex,
        .aggregate_sha256 = aggregate_hex,
        .total_bytes = total_bytes,
        .files = file_views,
    }, .{});
    errdefer alloc.free(receipt_json);
    if (receipt_json.len > request.limits.max_receipt_bytes) return error.ArtifactReceiptTooLarge;

    // This conditional write is the publication boundary. A partial upload has
    // no COMPLETE object and therefore can never be selected by a restore.
    if (options.fail_before_complete) return error.InjectedArtifactFailure;
    try putImmutable(alloc, store, complete_key, receipt_json, "application/json");
    return .{ .receipt_json = receipt_json, .already_available = false };
}

fn readChunk(reader: *std.Io.Reader, buffer: []u8) !usize {
    var used: usize = 0;
    while (used < buffer.len) {
        const read = try reader.readSliceShort(buffer[used..]);
        if (read == 0) break;
        used += read;
    }
    return used;
}

/// Re-reads and verifies the immutable remote publication without writing any
/// local staging state. This is the post-publish durability checkpoint used by
/// source-volume GC: COMPLETE, the manifest, every bounded data object, every
/// per-file digest, and the generation aggregate must all agree.
pub fn verifyRemote(
    alloc: Allocator,
    store: Store,
    expected: ExpectedArtifact,
    limits: Limits,
) !RemoteVerificationResult {
    try validateIdentifier(expected.generation, error.InvalidSeedGeneration);
    try validateIdentifier(expected.slot_name, error.InvalidSlotName);

    const complete_key = try generationKeyAlloc(alloc, store.prefix, expected.generation, complete_name);
    defer alloc.free(complete_key);
    const receipt_json = try getRequiredObject(alloc, store, complete_key, limits.max_receipt_bytes);
    errdefer alloc.free(receipt_json);
    var parsed = std.json.parseFromSlice(Receipt, alloc, receipt_json, .{ .ignore_unknown_fields = false }) catch
        return error.InvalidArtifactReceipt;
    defer parsed.deinit();
    const receipt = parsed.value;
    try validateReceipt(receipt, expected, limits);

    const manifest_key = try generationKeyAlloc(alloc, store.prefix, expected.generation, manifest_name);
    defer alloc.free(manifest_key);
    const manifest_bytes = try getRequiredObject(alloc, store, manifest_key, limits.max_manifest_bytes);
    defer alloc.free(manifest_bytes);
    try expectSha256(manifest_bytes, receipt.manifest_sha256, error.ManifestDigestMismatch);
    const manifest = try backup_manifest.decodeAlloc(alloc, manifest_bytes);
    defer backup_manifest.freeDecoded(alloc, manifest);
    try validateManifestAgainstReceipt(manifest, receipt);

    var aggregate = Sha256.init(.{});
    var manifest_digest: [Sha256.digest_length]u8 = undefined;
    Sha256.hash(manifest_bytes, &manifest_digest, .{});
    aggregate.update(&manifest_digest);
    var total_bytes: u64 = 0;
    for (receipt.files, 0..) |file, index| {
        const checked = try checksumRemoteArtifactFile(
            alloc,
            store,
            expected.generation,
            receipt.format_version,
            index,
            file,
            limits,
        );
        aggregateFile(&aggregate, file.path, checked.size_bytes, &checked.sha256);
        total_bytes = try std.math.add(u64, total_bytes, checked.size_bytes);
    }
    if (total_bytes != receipt.total_bytes) return error.ArtifactTotalSizeMismatch;
    var aggregate_digest: [Sha256.digest_length]u8 = undefined;
    aggregate.final(&aggregate_digest);
    var aggregate_hex: [Sha256.digest_length * 2]u8 = undefined;
    encodeHex(&aggregate_hex, &aggregate_digest);
    if (!std.mem.eql(u8, &aggregate_hex, receipt.aggregate_sha256))
        return error.ArtifactAggregateDigestMismatch;

    return .{
        .receipt_json = receipt_json,
        .file_count = receipt.files.len,
        .total_bytes = total_bytes,
    };
}

pub fn restoreToStaging(alloc: Allocator, store: Store, request: RestoreRequest) !RestoreResult {
    try validateIdentifier(request.expected.generation, error.InvalidSeedGeneration);
    try validateIdentifier(request.expected.slot_name, error.InvalidSlotName);
    if (!validation.isNormalizedPath(request.staging_root)) return error.InvalidStagingRoot;

    const complete_key = try generationKeyAlloc(alloc, store.prefix, request.expected.generation, complete_name);
    defer alloc.free(complete_key);
    const receipt_json = try getRequiredObject(alloc, store, complete_key, request.limits.max_receipt_bytes);
    errdefer alloc.free(receipt_json);

    var parsed = std.json.parseFromSlice(Receipt, alloc, receipt_json, .{ .ignore_unknown_fields = false }) catch return error.InvalidArtifactReceipt;
    defer parsed.deinit();
    const receipt = parsed.value;
    try validateReceipt(receipt, request.expected, request.limits);

    const manifest_key = try generationKeyAlloc(alloc, store.prefix, request.expected.generation, manifest_name);
    defer alloc.free(manifest_key);
    const manifest_bytes = try getRequiredObject(alloc, store, manifest_key, request.limits.max_manifest_bytes);
    defer alloc.free(manifest_bytes);
    try expectSha256(manifest_bytes, receipt.manifest_sha256, error.ManifestDigestMismatch);

    const manifest = try backup_manifest.decodeAlloc(alloc, manifest_bytes);
    defer backup_manifest.freeDecoded(alloc, manifest);
    try validateManifestAgainstReceipt(manifest, receipt);

    switch (try prepareStaging(alloc, request.staging_root, request.expected, receipt_json, request.limits)) {
        .already_complete => return .{
            .receipt_json = receipt_json,
            .file_count = receipt.files.len,
            .total_bytes = receipt.total_bytes,
        },
        .prepared => {},
    }

    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    errdefer std.Io.Dir.cwd().deleteTree(io, request.staging_root) catch {};

    var aggregate = Sha256.init(.{});
    var manifest_digest: [Sha256.digest_length]u8 = undefined;
    Sha256.hash(manifest_bytes, &manifest_digest, .{});
    aggregate.update(&manifest_digest);
    var total_bytes: u64 = 0;

    for (receipt.files, 0..) |file, idx| {
        const manifest_file = manifest.files[idx];
        if (manifest_file.size_bytes != file.size_bytes or manifest_file.crc32 != file.crc32) return error.ManifestReceiptMismatch;

        const destination = try std.fs.path.join(alloc, &.{ request.staging_root, file.path });
        defer alloc.free(destination);
        const restored = try restoreArtifactFile(alloc, store, request.expected.generation, receipt.format_version, idx, file, destination, request.limits);
        aggregateFile(&aggregate, file.path, restored.size_bytes, &restored.sha256);
        total_bytes = try std.math.add(u64, total_bytes, restored.size_bytes);
    }

    if (total_bytes != receipt.total_bytes) return error.ArtifactTotalSizeMismatch;
    var aggregate_digest: [Sha256.digest_length]u8 = undefined;
    aggregate.final(&aggregate_digest);
    const aggregate_hex = try hexAlloc(alloc, &aggregate_digest);
    defer alloc.free(aggregate_hex);
    if (!std.mem.eql(u8, aggregate_hex, receipt.aggregate_sha256)) return error.ArtifactAggregateDigestMismatch;

    const local_receipt = try std.fs.path.join(alloc, &.{ request.staging_root, receipt_name });
    defer alloc.free(local_receipt);
    try writeFileAtomically(alloc, local_receipt, receipt_json);
    const local_manifest = try std.fs.path.join(alloc, &.{ request.staging_root, staged_manifest_name });
    defer alloc.free(local_manifest);
    try writeFileAtomically(alloc, local_manifest, manifest_bytes);
    return .{
        .receipt_json = receipt_json,
        .file_count = receipt.files.len,
        .total_bytes = total_bytes,
    };
}

pub fn verifyStaged(alloc: Allocator, staging_root: []const u8, expected: ExpectedArtifact, limits: Limits) !void {
    const receipt_path = try std.fs.path.join(alloc, &.{ staging_root, receipt_name });
    defer alloc.free(receipt_path);
    const raw = try readFileAlloc(alloc, receipt_path, limits.max_receipt_bytes);
    defer alloc.free(raw);
    var parsed = std.json.parseFromSlice(Receipt, alloc, raw, .{}) catch return error.InvalidArtifactReceipt;
    defer parsed.deinit();
    try validateReceipt(parsed.value, expected, limits);

    var aggregate = Sha256.init(.{});
    // The manifest digest is already bound into the publish receipt. Seed the
    // aggregate from its decoded bytes so staged verification is self-contained.
    const manifest_digest = try decodeHexDigest(parsed.value.manifest_sha256);
    aggregate.update(&manifest_digest);
    var total_bytes: u64 = 0;
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    for (parsed.value.files) |file| {
        const path = try std.fs.path.join(alloc, &.{ staging_root, file.path });
        defer alloc.free(path);
        const checked = try checksumLocalFile(io_impl.io(), path, limits.max_file_bytes);
        if (checked.size_bytes != file.size_bytes) return error.ArtifactFileSizeMismatch;
        if (checked.crc32 != file.crc32) return error.ArtifactFileChecksumMismatch;
        var digest_hex: [Sha256.digest_length * 2]u8 = undefined;
        encodeHex(&digest_hex, &checked.sha256);
        if (!std.mem.eql(u8, &digest_hex, file.sha256)) return error.ArtifactFileDigestMismatch;
        aggregateFile(&aggregate, file.path, checked.size_bytes, &checked.sha256);
        total_bytes = try std.math.add(u64, total_bytes, checked.size_bytes);
    }
    if (total_bytes != parsed.value.total_bytes) return error.ArtifactTotalSizeMismatch;
    var digest: [Sha256.digest_length]u8 = undefined;
    aggregate.final(&digest);
    const hex = try hexAlloc(alloc, &digest);
    defer alloc.free(hex);
    if (!std.mem.eql(u8, hex, parsed.value.aggregate_sha256)) return error.ArtifactAggregateDigestMismatch;
}

/// Deletes only older COMPLETE generations for the requested slot. COMPLETE is
/// removed before generation members, so a crash can leave garbage but can
/// never leave a partially deleted generation selectable by restore.
pub fn prune(alloc: Allocator, store: Store, request: PruneRequest) !PruneResult {
    try validateIdentifier(request.slot_name, error.InvalidSlotName);
    try validateIdentifier(request.current_generation, error.InvalidSeedGeneration);
    if (request.retain_generations == 0) return error.InvalidSeedRetention;

    const generation_root = try generationRootPrefixAlloc(alloc, store.prefix);
    defer alloc.free(generation_root);
    const keys = try listAllKeysAlloc(alloc, store, generation_root);
    defer {
        for (keys) |key| alloc.free(key);
        alloc.free(keys);
    }

    var candidates = std.ArrayListUnmanaged(GenerationCandidate).empty;
    defer {
        for (candidates.items) |*candidate| candidate.deinit(alloc);
        candidates.deinit(alloc);
    }
    var found_current = false;
    for (keys) |key| {
        const generation = completeGenerationFromKey(generation_root, key) orelse continue;
        const raw = try getRequiredObject(alloc, store, key, request.limits.max_receipt_bytes);
        defer alloc.free(raw);
        var parsed = std.json.parseFromSlice(Receipt, alloc, raw, .{}) catch return error.InvalidArtifactReceipt;
        defer parsed.deinit();
        if (!std.mem.eql(u8, parsed.value.generation, generation)) return error.GenerationConflict;
        if (!std.mem.eql(u8, parsed.value.slot_name, request.slot_name)) continue;
        try candidates.append(alloc, .{
            .generation = try alloc.dupe(u8, generation),
            .checkpoint_lsn = parsed.value.checkpoint_lsn,
        });
        if (std.mem.eql(u8, generation, request.current_generation)) found_current = true;
    }
    if (!found_current) return error.CurrentSeedGenerationNotComplete;
    std.mem.sort(GenerationCandidate, candidates.items, {}, newerGeneration);

    var retained: usize = 1;
    var deleted: usize = 0;
    for (candidates.items) |candidate| {
        if (std.mem.eql(u8, candidate.generation, request.current_generation)) continue;
        if (retained < request.retain_generations) {
            retained += 1;
            continue;
        }

        const complete_key = try generationKeyAlloc(alloc, store.prefix, candidate.generation, complete_name);
        defer alloc.free(complete_key);
        deleteObjectIfPresent(store, complete_key) catch |err| return err;
        const member_prefix = try generationMemberPrefixAlloc(alloc, store.prefix, candidate.generation);
        defer alloc.free(member_prefix);
        for (keys) |key| {
            if (!std.mem.startsWith(u8, key, member_prefix) or std.mem.eql(u8, key, complete_key)) continue;
            deleteObjectIfPresent(store, key) catch |err| return err;
        }
        deleted += 1;
    }

    const result_json = try std.json.Stringify.valueAlloc(alloc, PruneReceipt{
        .slot_name = request.slot_name,
        .current_generation = request.current_generation,
        .retained_generations = retained,
        .deleted_generations = deleted,
    }, .{});
    return .{ .result_json = result_json, .deleted_generations = deleted };
}

fn prepareStaging(
    alloc: Allocator,
    staging_root: []const u8,
    expected: ExpectedArtifact,
    remote_receipt: []const u8,
    limits: Limits,
) !StagingState {
    const receipt_path = try std.fs.path.join(alloc, &.{ staging_root, receipt_name });
    defer alloc.free(receipt_path);
    if (readOptionalLocalFileAlloc(alloc, receipt_path, limits.max_receipt_bytes)) |local_receipt| {
        defer alloc.free(local_receipt);
        if (std.mem.eql(u8, local_receipt, remote_receipt)) {
            if (verifyStaged(alloc, staging_root, expected, limits)) |_| return .already_complete else |_| {}
        }
    } else |err| switch (err) {
        error.FileNotFound => {},
        else => return err,
    }

    const marker = StagingMarker{
        .generation = expected.generation,
        .slot_name = expected.slot_name,
        .cluster_id = expected.identity.cluster_id,
        .shard_id = expected.identity.shard_id,
        .table_id = expected.identity.table_id,
        .timeline_id = expected.identity.timeline_id,
        .epoch = expected.identity.epoch,
    };
    const marker_json = try std.json.Stringify.valueAlloc(alloc, marker, .{});
    defer alloc.free(marker_json);
    const marker_path = try std.fs.path.join(alloc, &.{ staging_root, staging_marker_name });
    defer alloc.free(marker_path);

    const empty_or_missing = try directoryEmptyOrMissing(alloc, staging_root);
    if (!empty_or_missing) {
        const existing_marker = readOptionalLocalFileAlloc(alloc, marker_path, limits.max_receipt_bytes) catch |err| switch (err) {
            error.FileNotFound => return error.UnsafeSeedTarget,
            else => return err,
        };
        defer alloc.free(existing_marker);
        if (!std.mem.eql(u8, marker_json, existing_marker)) return error.SeedTargetGenerationConflict;
    }

    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    try std.Io.Dir.cwd().deleteTree(io, staging_root);
    try std.Io.Dir.cwd().createDirPath(io, staging_root);
    try writeFileAtomically(alloc, marker_path, marker_json);
    return .prepared;
}

fn directoryEmptyOrMissing(alloc: Allocator, path: []const u8) !bool {
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    var dir = std.Io.Dir.cwd().openDir(io, path, .{ .iterate = true }) catch |err| switch (err) {
        error.FileNotFound => return true,
        error.NotDir => return error.UnsafeSeedTarget,
        else => return err,
    };
    defer dir.close(io);
    var iter = dir.iterateAssumeFirstIteration();
    return try iter.next(io) == null;
}

fn readOptionalLocalFileAlloc(alloc: Allocator, path: []const u8, max_bytes: usize) ![]u8 {
    return readFileAlloc(alloc, path, max_bytes) catch |err| switch (err) {
        error.FileNotFound, error.NotDir => error.FileNotFound,
        else => err,
    };
}

fn validateReceipt(receipt: Receipt, expected: ExpectedArtifact, limits: Limits) !void {
    if (receipt.format_version != legacy_format_version and receipt.format_version != format_version)
        return error.UnsupportedArtifactVersion;
    if (limits.max_chunk_bytes == 0 or limits.max_chunk_bytes > limits.max_file_bytes)
        return error.InvalidArtifactChunkSize;
    if (!std.mem.eql(u8, receipt.generation, expected.generation)) return error.WrongArtifactGeneration;
    if (!std.mem.eql(u8, receipt.slot_name, expected.slot_name)) return error.WrongArtifactSlot;
    try expectIdentity(expected.identity, receipt.identity());
    if (receipt.manifest_id.len == 0) return error.InvalidManifestId;
    if (receipt.backup_lsn == 0 or receipt.checkpoint_lsn < receipt.backup_lsn) return error.InvalidArtifactBoundary;
    if (receipt.checkpoint_lsn < expected.minimum_checkpoint_lsn) return error.StaleSeedArtifact;
    if (receipt.files.len == 0) return error.EmptyArtifact;
    if (receipt.files.len > limits.max_files) return error.TooManyArtifactFiles;
    if (receipt.total_bytes > limits.max_total_bytes) return error.ArtifactTooLarge;
    if (receipt.manifest_sha256.len != Sha256.digest_length * 2 or receipt.aggregate_sha256.len != Sha256.digest_length * 2) return error.InvalidArtifactDigest;
    var total: u64 = 0;
    for (receipt.files, 0..) |file, idx| {
        try backup_manifest.validatePath(file.path);
        if (file.size_bytes > limits.max_file_bytes) return error.ArtifactFileTooLarge;
        if (file.sha256.len != Sha256.digest_length * 2) return error.InvalidArtifactDigest;
        total = try std.math.add(u64, total, file.size_bytes);
        switch (receipt.format_version) {
            legacy_format_version => if (file.chunks != null) return error.InvalidArtifactChunks,
            format_version => {
                const chunks = file.chunks orelse return error.InvalidArtifactChunks;
                var chunk_total: u64 = 0;
                for (chunks, 0..) |chunk, chunk_index| {
                    if (chunk.index != chunk_index or chunk.size_bytes == 0 or
                        chunk.size_bytes > limits.max_chunk_bytes or
                        chunk.sha256.len != Sha256.digest_length * 2)
                        return error.InvalidArtifactChunks;
                    chunk_total = try std.math.add(u64, chunk_total, chunk.size_bytes);
                }
                if (chunk_total != file.size_bytes) return error.InvalidArtifactChunks;
            },
            else => unreachable,
        }
        for (receipt.files[0..idx]) |previous| {
            if (std.mem.eql(u8, previous.path, file.path)) return error.DuplicateArtifactPath;
        }
    }
    if (total != receipt.total_bytes) return error.ArtifactTotalSizeMismatch;
}

fn validateManifestAgainstReceipt(manifest: backup_manifest.ManifestView, receipt: Receipt) !void {
    try expectIdentity(receipt.identity(), manifest.identity);
    if (!std.mem.eql(u8, receipt.manifest_id, manifest.manifest_id)) return error.ManifestReceiptMismatch;
    if (receipt.backup_lsn != manifest.backup_lsn or receipt.checkpoint_lsn != manifest.checkpoint_lsn) return error.ManifestReceiptMismatch;
    if (receipt.files.len != manifest.files.len) return error.ManifestReceiptMismatch;
    for (receipt.files, manifest.files) |file, manifest_file| {
        if (!std.mem.eql(u8, file.path, manifest_file.path) or file.size_bytes != manifest_file.size_bytes or file.crc32 != manifest_file.crc32) return error.ManifestReceiptMismatch;
    }
}

fn validateExistingReceipt(alloc: Allocator, raw: []const u8, generation: []const u8, slot_name: []const u8, manifest_bytes: []const u8) !void {
    var parsed = std.json.parseFromSlice(Receipt, alloc, raw, .{}) catch return error.GenerationConflict;
    defer parsed.deinit();
    if ((parsed.value.format_version != legacy_format_version and parsed.value.format_version != format_version) or
        !std.mem.eql(u8, parsed.value.generation, generation) or !std.mem.eql(u8, parsed.value.slot_name, slot_name)) return error.GenerationConflict;
    var digest: [Sha256.digest_length]u8 = undefined;
    Sha256.hash(manifest_bytes, &digest, .{});
    const hex = try hexAlloc(alloc, &digest);
    defer alloc.free(hex);
    if (!std.mem.eql(u8, parsed.value.manifest_sha256, hex)) return error.GenerationConflict;
}

fn expectIdentity(expected: standby_mod.Identity, actual: standby_mod.Identity) !void {
    if (actual.cluster_id != expected.cluster_id) return error.WrongCluster;
    if (actual.shard_id != expected.shard_id) return error.WrongShard;
    if (actual.table_id != expected.table_id) return error.WrongTable;
    if (actual.timeline_id != expected.timeline_id) return error.WrongTimeline;
    if (actual.epoch != expected.epoch) return error.WrongEpoch;
}

fn validateIdentifier(value: []const u8, err: anyerror) !void {
    if (!validation.isIdentifier(value)) return err;
}

fn generationKeyAlloc(alloc: Allocator, prefix: []const u8, generation: []const u8, suffix: []const u8) ![]u8 {
    if (prefix.len == 0) return try std.fmt.allocPrint(alloc, "generations/{s}/{s}", .{ generation, suffix });
    return try std.fmt.allocPrint(alloc, "{s}/generations/{s}/{s}", .{ std.mem.trim(u8, prefix, "/"), generation, suffix });
}

fn generationRootPrefixAlloc(alloc: Allocator, prefix: []const u8) ![]u8 {
    if (prefix.len == 0) return try alloc.dupe(u8, "generations/");
    return try std.fmt.allocPrint(alloc, "{s}/generations/", .{std.mem.trim(u8, prefix, "/")});
}

fn generationMemberPrefixAlloc(alloc: Allocator, prefix: []const u8, generation: []const u8) ![]u8 {
    const root = try generationRootPrefixAlloc(alloc, prefix);
    defer alloc.free(root);
    return try std.fmt.allocPrint(alloc, "{s}{s}/", .{ root, generation });
}

fn completeGenerationFromKey(root: []const u8, key: []const u8) ?[]const u8 {
    if (!std.mem.startsWith(u8, key, root)) return null;
    const rest = key[root.len..];
    const suffix = "/" ++ complete_name;
    if (!std.mem.endsWith(u8, rest, suffix)) return null;
    const generation = rest[0 .. rest.len - suffix.len];
    if (std.mem.indexOfScalar(u8, generation, '/') != null or !validation.isIdentifier(generation)) return null;
    return generation;
}

fn newerGeneration(_: void, a: GenerationCandidate, b: GenerationCandidate) bool {
    if (a.checkpoint_lsn != b.checkpoint_lsn) return a.checkpoint_lsn > b.checkpoint_lsn;
    return std.mem.order(u8, a.generation, b.generation) == .gt;
}

fn listAllKeysAlloc(alloc: Allocator, store: Store, prefix: []const u8) ![][]u8 {
    var keys = std.ArrayListUnmanaged([]u8).empty;
    errdefer {
        for (keys.items) |key| alloc.free(key);
        keys.deinit(alloc);
    }
    var continuation: ?[]u8 = null;
    defer if (continuation) |value| alloc.free(value);
    while (true) {
        var result = try store.client.listObjects(store.bucket, .{
            .prefix = prefix,
            .recursive = true,
            .continuation_token = continuation,
            .max_keys = 1000,
        });
        defer result.deinit(alloc);
        for (result.entries) |entry| try keys.append(alloc, try alloc.dupe(u8, entry.key));
        const next = if (result.next_continuation_token) |value| try alloc.dupe(u8, value) else null;
        if (continuation) |value| alloc.free(value);
        continuation = next;
        if (continuation == null) break;
    }
    return try keys.toOwnedSlice(alloc);
}

fn deleteObjectIfPresent(store: Store, key: []const u8) !void {
    store.client.deleteObject(store.bucket, key, .{}) catch |err| switch (err) {
        error.FileNotFound, error.NoSuchKey, error.ObjectNotFound => return,
        else => return err,
    };
}

fn generationFileKeyAlloc(alloc: Allocator, prefix: []const u8, generation: []const u8, path: []const u8) ![]u8 {
    try backup_manifest.validatePath(path);
    const suffix = try std.fmt.allocPrint(alloc, "files/{s}", .{path});
    defer alloc.free(suffix);
    return try generationKeyAlloc(alloc, prefix, generation, suffix);
}

fn generationChunkKeyAlloc(
    alloc: Allocator,
    prefix: []const u8,
    generation: []const u8,
    file_index: usize,
    chunk_index: usize,
) ![]u8 {
    const suffix = try std.fmt.allocPrint(alloc, "chunks/{d:0>8}/{d:0>8}", .{ file_index, chunk_index });
    defer alloc.free(suffix);
    return try generationKeyAlloc(alloc, prefix, generation, suffix);
}

fn putImmutable(alloc: Allocator, store: Store, key: []const u8, body: []const u8, content_type: []const u8) !void {
    var result = store.client.putObject(store.bucket, key, body, .{
        .content_type = content_type,
        .if_none_match = true,
    }) catch |err| switch (err) {
        error.PreconditionFailed => {
            const existing = try getRequiredObject(alloc, store, key, body.len);
            defer alloc.free(existing);
            if (!std.mem.eql(u8, existing, body)) return error.GenerationConflict;
            return;
        },
        else => return err,
    };
    result.deinit(alloc);
}

fn getOptionalObject(alloc: Allocator, store: Store, key: []const u8, max_bytes: usize) ![]u8 {
    const probe_len = std.math.add(usize, max_bytes, 1) catch return error.ArtifactObjectTooLarge;
    var result = store.client.getObject(store.bucket, key, .{
        .range = .{ .offset = 0, .length = @intCast(probe_len) },
    }) catch |err| switch (err) {
        error.NoSuchKey, error.ObjectNotFound, error.FileNotFound => return error.FileNotFound,
        else => return err,
    };
    defer result.deinit(store.client.allocator);
    if (result.body.len > max_bytes) return error.ArtifactObjectTooLarge;
    return try alloc.dupe(u8, result.body);
}

fn getRequiredObject(alloc: Allocator, store: Store, key: []const u8, max_bytes: usize) ![]u8 {
    return getOptionalObject(alloc, store, key, max_bytes) catch |err| switch (err) {
        error.FileNotFound => error.IncompleteSeedArtifact,
        else => err,
    };
}

const RestoredFile = struct {
    size_bytes: u64,
    crc32: u32,
    sha256: [Sha256.digest_length]u8,
};

fn checksumRemoteArtifactFile(
    alloc: Allocator,
    store: Store,
    generation: []const u8,
    receipt_version: u16,
    file_index: usize,
    receipt: FileReceipt,
    limits: Limits,
) !RestoredFile {
    var sha = Sha256.init(.{});
    var crc = std.hash.Crc32.init();
    var read_bytes: u64 = 0;
    switch (receipt_version) {
        legacy_format_version => {
            const object_key = try generationFileKeyAlloc(alloc, store.prefix, generation, receipt.path);
            defer alloc.free(object_key);
            if (receipt.size_bytes == 0) {
                var metadata = store.client.statObject(store.bucket, object_key) catch |err| switch (err) {
                    error.NoSuchKey, error.ObjectNotFound, error.FileNotFound => return error.IncompleteSeedArtifact,
                    else => return err,
                };
                defer metadata.deinit(store.client.allocator);
                if (metadata.content_length != 0) return error.ArtifactFileSizeMismatch;
            } else {
                while (read_bytes < receipt.size_bytes) {
                    const remaining = receipt.size_bytes - read_bytes;
                    const requested: usize = @intCast(@min(remaining, limits.max_chunk_bytes));
                    var object = try getRequiredObjectResult(store, object_key, .{
                        .range = .{ .offset = read_bytes, .length = @intCast(requested) },
                    });
                    defer object.deinit(store.client.allocator);
                    if (object.body.len != requested) return error.ArtifactFileSizeMismatch;
                    try absorbVerifiedBytes(&sha, &crc, &read_bytes, object.body, receipt.size_bytes);
                }
            }
        },
        format_version => {
            const chunks = receipt.chunks orelse return error.InvalidArtifactChunks;
            for (chunks, 0..) |chunk, index| {
                if (chunk.index != index) return error.InvalidArtifactChunks;
                const object_key = try generationChunkKeyAlloc(alloc, store.prefix, generation, file_index, index);
                defer alloc.free(object_key);
                const probe_len = std.math.add(u64, chunk.size_bytes, 1) catch return error.ArtifactChunkSizeMismatch;
                var object = try getRequiredObjectResult(store, object_key, .{
                    .range = .{ .offset = 0, .length = probe_len },
                });
                defer object.deinit(store.client.allocator);
                if (object.body.len != chunk.size_bytes or object.body.len > limits.max_chunk_bytes)
                    return error.ArtifactChunkSizeMismatch;
                try expectSha256(object.body, chunk.sha256, error.ArtifactChunkDigestMismatch);
                try absorbVerifiedBytes(&sha, &crc, &read_bytes, object.body, receipt.size_bytes);
            }
        },
        else => return error.UnsupportedArtifactVersion,
    }
    if (read_bytes != receipt.size_bytes) return error.ArtifactFileSizeMismatch;
    if (crc.final() != receipt.crc32) return error.ArtifactFileChecksumMismatch;
    var digest: [Sha256.digest_length]u8 = undefined;
    sha.final(&digest);
    var digest_hex: [Sha256.digest_length * 2]u8 = undefined;
    encodeHex(&digest_hex, &digest);
    if (!std.mem.eql(u8, &digest_hex, receipt.sha256)) return error.ArtifactFileDigestMismatch;
    return .{ .size_bytes = read_bytes, .crc32 = receipt.crc32, .sha256 = digest };
}

fn absorbVerifiedBytes(
    sha: *Sha256,
    crc: *std.hash.Crc32,
    read_bytes: *u64,
    body: []const u8,
    expected_size: u64,
) !void {
    read_bytes.* = try std.math.add(u64, read_bytes.*, body.len);
    if (read_bytes.* > expected_size) return error.ArtifactFileSizeMismatch;
    sha.update(body);
    crc.update(body);
}

fn checksumLocalFile(io: std.Io, path: []const u8, max_bytes: usize) !RestoredFile {
    const stat = try std.Io.Dir.cwd().statFile(io, path, .{ .follow_symlinks = false });
    if (stat.kind != .file) return error.UnsupportedArtifactSource;
    if (stat.size > max_bytes) return error.ArtifactFileTooLarge;
    var file = try std.Io.Dir.cwd().openFile(io, path, .{});
    defer file.close(io);
    var reader = file.reader(io, &.{});
    var buffer: [64 * 1024]u8 = undefined;
    var sha = Sha256.init(.{});
    var crc = std.hash.Crc32.init();
    var size: u64 = 0;
    while (true) {
        const read = try reader.interface.readSliceShort(&buffer);
        if (read == 0) break;
        size = try std.math.add(u64, size, read);
        if (size > max_bytes) return error.ArtifactFileTooLarge;
        sha.update(buffer[0..read]);
        crc.update(buffer[0..read]);
    }
    var digest: [Sha256.digest_length]u8 = undefined;
    sha.final(&digest);
    return .{ .size_bytes = size, .crc32 = crc.final(), .sha256 = digest };
}

fn restoreArtifactFile(
    alloc: Allocator,
    store: Store,
    generation: []const u8,
    receipt_version: u16,
    file_index: usize,
    receipt: FileReceipt,
    destination: []const u8,
    limits: Limits,
) !RestoredFile {
    const parent = std.fs.path.dirname(destination) orelse return error.InvalidArtifactPath;
    const temp = try std.fmt.allocPrint(alloc, "{s}.tmp", .{destination});
    defer alloc.free(temp);
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    try std.Io.Dir.cwd().createDirPath(io, parent);
    errdefer std.Io.Dir.cwd().deleteFile(io, temp) catch {};

    var sha = Sha256.init(.{});
    var crc = std.hash.Crc32.init();
    var written: u64 = 0;
    {
        var file = try std.Io.Dir.cwd().createFile(io, temp, .{ .truncate = true });
        defer file.close(io);
        var write_buffer: [64 * 1024]u8 = undefined;
        var writer = file.writer(io, &write_buffer);

        switch (receipt_version) {
            legacy_format_version => {
                const object_key = try generationFileKeyAlloc(alloc, store.prefix, generation, receipt.path);
                defer alloc.free(object_key);
                if (receipt.size_bytes == 0) {
                    var metadata = store.client.statObject(store.bucket, object_key) catch |err| switch (err) {
                        error.NoSuchKey, error.ObjectNotFound, error.FileNotFound => return error.IncompleteSeedArtifact,
                        else => return err,
                    };
                    defer metadata.deinit(store.client.allocator);
                    if (metadata.content_length != 0) return error.ArtifactFileSizeMismatch;
                } else {
                    while (written < receipt.size_bytes) {
                        const remaining = receipt.size_bytes - written;
                        const requested: usize = @intCast(@min(remaining, limits.max_chunk_bytes));
                        var object = try getRequiredObjectResult(store, object_key, .{
                            .range = .{ .offset = written, .length = @intCast(requested) },
                        });
                        defer object.deinit(store.client.allocator);
                        if (object.body.len != requested) return error.ArtifactFileSizeMismatch;
                        try appendRestoredBytes(&writer.interface, &sha, &crc, &written, object.body, receipt.size_bytes);
                    }
                }
            },
            format_version => {
                const chunks = receipt.chunks orelse return error.InvalidArtifactChunks;
                for (chunks, 0..) |chunk, index| {
                    if (chunk.index != index) return error.InvalidArtifactChunks;
                    const object_key = try generationChunkKeyAlloc(alloc, store.prefix, generation, file_index, index);
                    defer alloc.free(object_key);
                    const probe_len = std.math.add(u64, chunk.size_bytes, 1) catch return error.ArtifactChunkSizeMismatch;
                    var object = try getRequiredObjectResult(store, object_key, .{
                        .range = .{ .offset = 0, .length = probe_len },
                    });
                    defer object.deinit(store.client.allocator);
                    if (object.body.len != chunk.size_bytes or object.body.len > limits.max_chunk_bytes)
                        return error.ArtifactChunkSizeMismatch;
                    try expectSha256(object.body, chunk.sha256, error.ArtifactChunkDigestMismatch);
                    try appendRestoredBytes(&writer.interface, &sha, &crc, &written, object.body, receipt.size_bytes);
                }
            },
            else => return error.UnsupportedArtifactVersion,
        }

        if (written != receipt.size_bytes) return error.ArtifactFileSizeMismatch;
        if (crc.final() != receipt.crc32) return error.ArtifactFileChecksumMismatch;
        try writer.end();
        try file.sync(io);
    }
    var digest: [Sha256.digest_length]u8 = undefined;
    sha.final(&digest);
    var digest_hex: [Sha256.digest_length * 2]u8 = undefined;
    encodeHex(&digest_hex, &digest);
    if (!std.mem.eql(u8, &digest_hex, receipt.sha256)) return error.ArtifactFileDigestMismatch;
    try std.Io.Dir.rename(std.Io.Dir.cwd(), temp, std.Io.Dir.cwd(), destination, io);
    try fs_paths.syncDirPortable(io, parent);
    return .{ .size_bytes = written, .crc32 = receipt.crc32, .sha256 = digest };
}

fn appendRestoredBytes(
    writer: *std.Io.Writer,
    sha: *Sha256,
    crc: *std.hash.Crc32,
    written: *u64,
    body: []const u8,
    expected_size: u64,
) !void {
    written.* = try std.math.add(u64, written.*, body.len);
    if (written.* > expected_size) return error.ArtifactFileSizeMismatch;
    sha.update(body);
    crc.update(body);
    try writer.writeAll(body);
}

fn getRequiredObjectResult(store: Store, key: []const u8, options: object_storage.GetOptions) !object_storage.GetResult {
    return store.client.getObject(store.bucket, key, options) catch |err| switch (err) {
        error.NoSuchKey, error.ObjectNotFound, error.FileNotFound => error.IncompleteSeedArtifact,
        else => err,
    };
}

fn readFileAlloc(alloc: Allocator, path: []const u8, max_bytes: usize) ![]u8 {
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    return try std.Io.Dir.cwd().readFileAlloc(io_impl.io(), path, alloc, .limited(max_bytes));
}

fn writeFileAtomically(alloc: Allocator, path: []const u8, body: []const u8) !void {
    const parent = std.fs.path.dirname(path) orelse return error.InvalidArtifactPath;
    const temp = try std.fmt.allocPrint(alloc, "{s}.tmp", .{path});
    defer alloc.free(temp);
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    try std.Io.Dir.cwd().createDirPath(io, parent);
    {
        var file = try std.Io.Dir.cwd().createFile(io, temp, .{ .truncate = true });
        defer file.close(io);
        var buffer: [64 * 1024]u8 = undefined;
        var writer = file.writer(io, &buffer);
        try writer.interface.writeAll(body);
        try writer.end();
        try file.sync(io);
    }
    try std.Io.Dir.rename(std.Io.Dir.cwd(), temp, std.Io.Dir.cwd(), path, io);
    try fs_paths.syncDirPortable(io, parent);
}

fn expectSha256(body: []const u8, expected_hex: []const u8, err: anyerror) !void {
    var digest: [Sha256.digest_length]u8 = undefined;
    Sha256.hash(body, &digest, .{});
    var encoded: [Sha256.digest_length * 2]u8 = undefined;
    encodeHex(&encoded, &digest);
    if (!std.mem.eql(u8, &encoded, expected_hex)) return err;
}

fn aggregateFile(hash: *Sha256, path: []const u8, size: u64, digest: *const [Sha256.digest_length]u8) void {
    hash.update(path);
    const separator = [_]u8{0};
    hash.update(&separator);
    var size_bytes: [8]u8 = undefined;
    std.mem.writeInt(u64, &size_bytes, size, .little);
    hash.update(&size_bytes);
    hash.update(digest);
}

fn hexAlloc(alloc: Allocator, bytes: []const u8) ![]u8 {
    const out = try alloc.alloc(u8, bytes.len * 2);
    encodeHex(out, bytes);
    return out;
}

fn encodeHex(out: []u8, bytes: []const u8) void {
    std.debug.assert(out.len == bytes.len * 2);
    for (bytes, 0..) |byte, idx| {
        out[idx * 2] = std.fmt.digitToChar(byte >> 4, .lower);
        out[idx * 2 + 1] = std.fmt.digitToChar(byte & 0x0f, .lower);
    }
}

fn decodeHexDigest(hex: []const u8) ![Sha256.digest_length]u8 {
    if (hex.len != Sha256.digest_length * 2) return error.InvalidArtifactDigest;
    var out: [Sha256.digest_length]u8 = undefined;
    for (&out, 0..) |*byte, idx| {
        const hi = std.fmt.charToDigit(hex[idx * 2], 16) catch return error.InvalidArtifactDigest;
        const lo = std.fmt.charToDigit(hex[idx * 2 + 1], 16) catch return error.InvalidArtifactDigest;
        byte.* = (hi << 4) | lo;
    }
    return out;
}

fn testIdentity() standby_mod.Identity {
    return .{ .cluster_id = 10, .shard_id = 20, .table_id = 30, .timeline_id = 2, .epoch = 4 };
}

fn writeTestFile(alloc: Allocator, path: []const u8, body: []const u8) !void {
    try writeFileAtomically(alloc, path, body);
}

test "storage.ha seed artifact publishes last and restores verified staging" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(std.testing.io, ".", alloc);
    defer alloc.free(root);
    const content_root = try std.fs.path.join(alloc, &.{ root, "source" });
    defer alloc.free(content_root);
    const staging_root = try std.fs.path.join(alloc, &.{ root, "staging" });
    defer alloc.free(staging_root);
    const first_path = try std.fs.path.join(alloc, &.{ content_root, "catalog/manifest" });
    defer alloc.free(first_path);
    const second_path = try std.fs.path.join(alloc, &.{ content_root, "tables/1/sst-1" });
    defer alloc.free(second_path);
    try writeTestFile(alloc, first_path, "catalog-v1");
    try writeTestFile(alloc, second_path, "immutable-sstable");

    const files = [_]backup_manifest.FileEntry{
        .{ .path = "catalog/manifest", .kind = .manifest, .size_bytes = 10, .crc32 = backup_manifest.crc32("catalog-v1") },
        .{ .path = "tables/1/sst-1", .kind = .sstable, .size_bytes = 17, .crc32 = backup_manifest.crc32("immutable-sstable") },
    };
    const manifest_bytes = try backup_manifest.encodeAlloc(alloc, .{
        .identity = testIdentity(),
        .manifest_id = "seed-a",
        .backup_lsn = 8,
        .checkpoint_lsn = 11,
        .files = &files,
    });
    defer alloc.free(manifest_bytes);

    var memory = object_storage.MemoryObjectStorage.init(alloc);
    defer memory.deinit();
    var client = memory.client();
    try client.makeBucket("ha-seeds");
    const store = Store{ .client = &client, .bucket = "ha-seeds", .prefix = "cluster-a" };

    var published = try publish(alloc, store, .{
        .generation = "gen-0001",
        .slot_name = "standby-a",
        .manifest_bytes = manifest_bytes,
        .content_root = content_root,
    });
    defer published.deinit(alloc);
    try std.testing.expect(!published.already_available);

    var verified_remote = try verifyRemote(alloc, store, .{
        .generation = "gen-0001",
        .slot_name = "standby-a",
        .identity = testIdentity(),
        .minimum_checkpoint_lsn = 11,
    }, .{});
    defer verified_remote.deinit(alloc);
    try std.testing.expectEqualStrings(published.receipt_json, verified_remote.receipt_json);
    try std.testing.expectEqual(@as(usize, 2), verified_remote.file_count);
    try std.testing.expectEqual(@as(u64, 27), verified_remote.total_bytes);

    var repeated = try publish(alloc, store, .{
        .generation = "gen-0001",
        .slot_name = "standby-a",
        .manifest_bytes = manifest_bytes,
        .content_root = content_root,
    });
    defer repeated.deinit(alloc);
    try std.testing.expect(repeated.already_available);
    try std.testing.expectEqualStrings(published.receipt_json, repeated.receipt_json);

    var restored = try restoreToStaging(alloc, store, .{
        .expected = .{
            .generation = "gen-0001",
            .slot_name = "standby-a",
            .identity = testIdentity(),
            .minimum_checkpoint_lsn = 10,
        },
        .staging_root = staging_root,
    });
    defer restored.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), restored.file_count);
    try std.testing.expectEqual(@as(u64, 27), restored.total_bytes);
    try verifyStaged(alloc, staging_root, .{
        .generation = "gen-0001",
        .slot_name = "standby-a",
        .identity = testIdentity(),
        .minimum_checkpoint_lsn = 11,
    }, .{});

    var restored_again = try restoreToStaging(alloc, store, .{
        .expected = .{
            .generation = "gen-0001",
            .slot_name = "standby-a",
            .identity = testIdentity(),
            .minimum_checkpoint_lsn = 11,
        },
        .staging_root = staging_root,
    });
    defer restored_again.deinit(alloc);
    try std.testing.expectEqualStrings(restored.receipt_json, restored_again.receipt_json);
}

test "storage.ha seed artifact v2 chunks every data object within the configured bound" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(std.testing.io, ".", alloc);
    defer alloc.free(root);
    const content_root = try std.fs.path.join(alloc, &.{ root, "source" });
    defer alloc.free(content_root);
    const source_path = try std.fs.path.join(alloc, &.{ content_root, "data/catalog" });
    defer alloc.free(source_path);
    try writeTestFile(alloc, source_path, "0123456789abcdefg");

    const files = [_]backup_manifest.FileEntry{.{
        .path = "data/catalog",
        .kind = .manifest,
        .size_bytes = 17,
        .crc32 = backup_manifest.crc32("0123456789abcdefg"),
    }};
    const manifest_bytes = try backup_manifest.encodeAlloc(alloc, .{
        .identity = testIdentity(),
        .manifest_id = "seed-v2-chunks",
        .backup_lsn = 8,
        .checkpoint_lsn = 11,
        .files = &files,
    });
    defer alloc.free(manifest_bytes);

    var memory = object_storage.MemoryObjectStorage.init(alloc);
    defer memory.deinit();
    var client = memory.client();
    try client.makeBucket("ha-seeds");
    const store = Store{ .client = &client, .bucket = "ha-seeds" };
    const limits = Limits{ .max_chunk_bytes = 4 };
    var published = try publish(alloc, store, .{
        .generation = "gen-v2-chunks",
        .slot_name = "standby-a",
        .manifest_bytes = manifest_bytes,
        .content_root = content_root,
        .limits = limits,
    });
    defer published.deinit(alloc);

    var parsed = try std.json.parseFromSlice(Receipt, alloc, published.receipt_json, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(u16, 2), parsed.value.format_version);
    const chunks = parsed.value.files[0].chunks orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(usize, 5), chunks.len);
    for (chunks, 0..) |chunk, index| {
        try std.testing.expect(chunk.size_bytes > 0 and chunk.size_bytes <= limits.max_chunk_bytes);
        try std.testing.expectEqual(index, chunk.index);
        const key = try generationChunkKeyAlloc(alloc, store.prefix, "gen-v2-chunks", 0, index);
        defer alloc.free(key);
        const body = try getRequiredObject(alloc, store, key, limits.max_chunk_bytes);
        defer alloc.free(body);
        try std.testing.expectEqual(chunk.size_bytes, body.len);
    }

    const legacy_key = try generationFileKeyAlloc(alloc, store.prefix, "gen-v2-chunks", "data/catalog");
    defer alloc.free(legacy_key);
    try std.testing.expectError(error.FileNotFound, getOptionalObject(alloc, store, legacy_key, 64));

    const staging_root = try std.fs.path.join(alloc, &.{ root, "staging" });
    defer alloc.free(staging_root);
    var restored = try restoreToStaging(alloc, store, .{
        .expected = .{
            .generation = "gen-v2-chunks",
            .slot_name = "standby-a",
            .identity = testIdentity(),
            .minimum_checkpoint_lsn = 11,
        },
        .staging_root = staging_root,
        .limits = limits,
    });
    defer restored.deinit(alloc);
    const restored_path = try std.fs.path.join(alloc, &.{ staging_root, "data/catalog" });
    defer alloc.free(restored_path);
    const restored_body = try readFileAlloc(alloc, restored_path, 32);
    defer alloc.free(restored_body);
    try std.testing.expectEqualStrings("0123456789abcdefg", restored_body);
}

test "storage.ha seed artifact does not publish COMPLETE before all v2 chunks are durable" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(std.testing.io, ".", alloc);
    defer alloc.free(root);
    const content_root = try std.fs.path.join(alloc, &.{ root, "source" });
    defer alloc.free(content_root);
    const source_path = try std.fs.path.join(alloc, &.{ content_root, "data/catalog" });
    defer alloc.free(source_path);
    try writeTestFile(alloc, source_path, "chunked-publication");
    const files = [_]backup_manifest.FileEntry{.{
        .path = "data/catalog",
        .kind = .manifest,
        .size_bytes = 19,
        .crc32 = backup_manifest.crc32("chunked-publication"),
    }};
    const manifest_bytes = try backup_manifest.encodeAlloc(alloc, .{
        .identity = testIdentity(),
        .manifest_id = "seed-v2-crash",
        .backup_lsn = 8,
        .checkpoint_lsn = 11,
        .files = &files,
    });
    defer alloc.free(manifest_bytes);
    var memory = object_storage.MemoryObjectStorage.init(alloc);
    defer memory.deinit();
    var client = memory.client();
    try client.makeBucket("ha-seeds");
    const store = Store{ .client = &client, .bucket = "ha-seeds" };

    try std.testing.expectError(error.InjectedArtifactFailure, publishWithOptions(alloc, store, .{
        .generation = "gen-v2-crash",
        .slot_name = "standby-a",
        .manifest_bytes = manifest_bytes,
        .content_root = content_root,
        .limits = .{ .max_chunk_bytes = 4 },
    }, .{ .fail_before_complete = true }));

    const complete_key = try generationKeyAlloc(alloc, store.prefix, "gen-v2-crash", complete_name);
    defer alloc.free(complete_key);
    try std.testing.expectError(error.FileNotFound, getOptionalObject(alloc, store, complete_key, 4096));
    const staging_root = try std.fs.path.join(alloc, &.{ root, "must-not-restore" });
    defer alloc.free(staging_root);
    try std.testing.expectError(error.IncompleteSeedArtifact, restoreToStaging(alloc, store, .{
        .expected = .{
            .generation = "gen-v2-crash",
            .slot_name = "standby-a",
            .identity = testIdentity(),
        },
        .staging_root = staging_root,
    }));
}

test "storage.ha seed artifact restores a legacy v1 single-object generation" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(std.testing.io, ".", alloc);
    defer alloc.free(root);
    const staging_root = try std.fs.path.join(alloc, &.{ root, "legacy-staging" });
    defer alloc.free(staging_root);
    const body = "legacy-v1";
    const files = [_]backup_manifest.FileEntry{.{
        .path = "data/catalog",
        .kind = .manifest,
        .size_bytes = body.len,
        .crc32 = backup_manifest.crc32(body),
    }};
    const manifest_bytes = try backup_manifest.encodeAlloc(alloc, .{
        .identity = testIdentity(),
        .manifest_id = "seed-v1-compat",
        .backup_lsn = 8,
        .checkpoint_lsn = 11,
        .files = &files,
    });
    defer alloc.free(manifest_bytes);

    var manifest_digest: [Sha256.digest_length]u8 = undefined;
    Sha256.hash(manifest_bytes, &manifest_digest, .{});
    var file_digest: [Sha256.digest_length]u8 = undefined;
    Sha256.hash(body, &file_digest, .{});
    var aggregate = Sha256.init(.{});
    aggregate.update(&manifest_digest);
    aggregateFile(&aggregate, files[0].path, files[0].size_bytes, &file_digest);
    var aggregate_digest: [Sha256.digest_length]u8 = undefined;
    aggregate.final(&aggregate_digest);
    const manifest_hex = try hexAlloc(alloc, &manifest_digest);
    defer alloc.free(manifest_hex);
    const file_hex = try hexAlloc(alloc, &file_digest);
    defer alloc.free(file_hex);
    const aggregate_hex = try hexAlloc(alloc, &aggregate_digest);
    defer alloc.free(aggregate_hex);
    const receipt_files = [_]FileReceipt{.{
        .path = files[0].path,
        .size_bytes = files[0].size_bytes,
        .crc32 = files[0].crc32,
        .sha256 = file_hex,
    }};
    const receipt_json = try std.json.Stringify.valueAlloc(alloc, Receipt{
        .format_version = 1,
        .generation = "gen-v1-compat",
        .slot_name = "standby-a",
        .cluster_id = testIdentity().cluster_id,
        .shard_id = testIdentity().shard_id,
        .table_id = testIdentity().table_id,
        .timeline_id = testIdentity().timeline_id,
        .epoch = testIdentity().epoch,
        .manifest_id = "seed-v1-compat",
        .backup_lsn = 8,
        .checkpoint_lsn = 11,
        .manifest_sha256 = manifest_hex,
        .aggregate_sha256 = aggregate_hex,
        .total_bytes = body.len,
        .files = &receipt_files,
    }, .{});
    defer alloc.free(receipt_json);

    var memory = object_storage.MemoryObjectStorage.init(alloc);
    defer memory.deinit();
    var client = memory.client();
    try client.makeBucket("ha-seeds");
    const store = Store{ .client = &client, .bucket = "ha-seeds" };
    const file_key = try generationFileKeyAlloc(alloc, store.prefix, "gen-v1-compat", files[0].path);
    defer alloc.free(file_key);
    const manifest_key = try generationKeyAlloc(alloc, store.prefix, "gen-v1-compat", manifest_name);
    defer alloc.free(manifest_key);
    const complete_key = try generationKeyAlloc(alloc, store.prefix, "gen-v1-compat", complete_name);
    defer alloc.free(complete_key);
    try putImmutable(alloc, store, file_key, body, "application/octet-stream");
    try putImmutable(alloc, store, manifest_key, manifest_bytes, "application/vnd.antfly.ha-manifest");
    try putImmutable(alloc, store, complete_key, receipt_json, "application/json");

    var verified_remote = try verifyRemote(alloc, store, .{
        .generation = "gen-v1-compat",
        .slot_name = "standby-a",
        .identity = testIdentity(),
        .minimum_checkpoint_lsn = 11,
    }, .{ .max_chunk_bytes = 4 });
    defer verified_remote.deinit(alloc);
    try std.testing.expectEqualStrings(receipt_json, verified_remote.receipt_json);

    var restored = try restoreToStaging(alloc, store, .{
        .expected = .{
            .generation = "gen-v1-compat",
            .slot_name = "standby-a",
            .identity = testIdentity(),
            .minimum_checkpoint_lsn = 11,
        },
        .staging_root = staging_root,
    });
    defer restored.deinit(alloc);
    const restored_path = try std.fs.path.join(alloc, &.{ staging_root, files[0].path });
    defer alloc.free(restored_path);
    const restored_body = try readFileAlloc(alloc, restored_path, 64);
    defer alloc.free(restored_body);
    try std.testing.expectEqualStrings(body, restored_body);
}

test "storage.ha seed artifact rejects incomplete stale and cross-cluster generations" {
    const alloc = std.testing.allocator;
    var memory = object_storage.MemoryObjectStorage.init(alloc);
    defer memory.deinit();
    var client = memory.client();
    try client.makeBucket("ha-seeds");
    const store = Store{ .client = &client, .bucket = "ha-seeds" };
    const expected = ExpectedArtifact{
        .generation = "missing-generation",
        .slot_name = "standby-a",
        .identity = testIdentity(),
        .minimum_checkpoint_lsn = 100,
    };
    try std.testing.expectError(error.IncompleteSeedArtifact, restoreToStaging(alloc, store, .{
        .expected = expected,
        .staging_root = ".zig-cache/tmp/missing-seed-staging",
    }));
}

test "storage.ha seed artifact refuses a non-empty unowned target" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(std.testing.io, ".", alloc);
    defer alloc.free(root);
    const content_root = try std.fs.path.join(alloc, &.{ root, "source" });
    defer alloc.free(content_root);
    const staging_root = try std.fs.path.join(alloc, &.{ root, "occupied" });
    defer alloc.free(staging_root);
    const source_path = try std.fs.path.join(alloc, &.{ content_root, "catalog/manifest" });
    defer alloc.free(source_path);
    const existing_path = try std.fs.path.join(alloc, &.{ staging_root, "do-not-overwrite" });
    defer alloc.free(existing_path);
    try writeTestFile(alloc, source_path, "catalog-v1");
    try writeTestFile(alloc, existing_path, "primary-data");

    const files = [_]backup_manifest.FileEntry{
        .{ .path = "catalog/manifest", .kind = .manifest, .size_bytes = 10, .crc32 = backup_manifest.crc32("catalog-v1") },
    };
    const manifest_bytes = try backup_manifest.encodeAlloc(alloc, .{
        .identity = testIdentity(),
        .manifest_id = "seed-safe-target",
        .backup_lsn = 8,
        .checkpoint_lsn = 11,
        .files = &files,
    });
    defer alloc.free(manifest_bytes);

    var memory = object_storage.MemoryObjectStorage.init(alloc);
    defer memory.deinit();
    var client = memory.client();
    try client.makeBucket("ha-seeds");
    const store = Store{ .client = &client, .bucket = "ha-seeds" };
    var published = try publish(alloc, store, .{
        .generation = "gen-safe-target",
        .slot_name = "standby-a",
        .manifest_bytes = manifest_bytes,
        .content_root = content_root,
    });
    defer published.deinit(alloc);

    try std.testing.expectError(error.UnsafeSeedTarget, restoreToStaging(alloc, store, .{
        .expected = .{
            .generation = "gen-safe-target",
            .slot_name = "standby-a",
            .identity = testIdentity(),
        },
        .staging_root = staging_root,
    }));
    const preserved = try readFileAlloc(alloc, existing_path, 64);
    defer alloc.free(preserved);
    try std.testing.expectEqualStrings("primary-data", preserved);
}

test "storage.ha seed artifact prunes older complete generations publish marker first" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(std.testing.io, ".", alloc);
    defer alloc.free(root);
    const content_root = try std.fs.path.join(alloc, &.{ root, "source" });
    defer alloc.free(content_root);
    const source_path = try std.fs.path.join(alloc, &.{ content_root, "catalog/manifest" });
    defer alloc.free(source_path);
    try writeTestFile(alloc, source_path, "catalog-v1");
    const files = [_]backup_manifest.FileEntry{
        .{ .path = "catalog/manifest", .kind = .manifest, .size_bytes = 10, .crc32 = backup_manifest.crc32("catalog-v1") },
    };
    const manifest_bytes = try backup_manifest.encodeAlloc(alloc, .{
        .identity = testIdentity(),
        .manifest_id = "seed-prune",
        .backup_lsn = 8,
        .checkpoint_lsn = 11,
        .files = &files,
    });
    defer alloc.free(manifest_bytes);

    var memory = object_storage.MemoryObjectStorage.init(alloc);
    defer memory.deinit();
    var client = memory.client();
    try client.makeBucket("ha-seeds");
    const store = Store{ .client = &client, .bucket = "ha-seeds", .prefix = "cluster-a" };
    for ([_][]const u8{ "gen-0001", "gen-0002", "gen-0003" }) |generation| {
        var published = try publish(alloc, store, .{
            .generation = generation,
            .slot_name = "standby-a",
            .manifest_bytes = manifest_bytes,
            .content_root = content_root,
        });
        published.deinit(alloc);
    }

    var pruned = try prune(alloc, store, .{
        .slot_name = "standby-a",
        .current_generation = "gen-0003",
        .retain_generations = 2,
    });
    defer pruned.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), pruned.deleted_generations);

    const old_complete = try generationKeyAlloc(alloc, store.prefix, "gen-0001", complete_name);
    defer alloc.free(old_complete);
    try std.testing.expectError(error.FileNotFound, getOptionalObject(alloc, store, old_complete, 1024));
    const retained_complete = try generationKeyAlloc(alloc, store.prefix, "gen-0002", complete_name);
    defer alloc.free(retained_complete);
    const retained = try getOptionalObject(alloc, store, retained_complete, 16 * 1024);
    defer alloc.free(retained);
    try std.testing.expect(retained.len > 0);
}
