// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0.

//! Canonical backup repository contract.
//!
//! Backends expose three immutable namespaces:
//!   refs/<backup-id>              mutable atomic pointer
//!   manifests/<sha256>            immutable canonical JSON
//!   blobs/sha256/<sha256>         immutable bytes
//!
//! Every manifest contains the complete materialized inventory for that
//! snapshot. A parent is lineage/GC information only; restore never needs to
//! replay a chain. Backend implementations may be filesystem or object-store
//! based, but must use conditional ref publication and immutable puts.

const std = @import("std");
const bundle = @import("backup_bundle.zig");

pub const manifest_schema_version: u32 = 1;
pub const ref_schema_version: u32 = 1;
pub const lease_schema_version: u32 = 1;
pub const max_blobs: usize = 1_000_000;
pub const max_shards: usize = 65_536;
pub const max_ref_name_bytes: usize = 128;
pub const max_manifest_bytes: usize = 64 * 1024 * 1024;

pub const BlobRef = struct {
    sha256: []const u8,
    logical_size_bytes: u64,
    stored_size_bytes: u64,
    compression: bundle.Compression = .none,
    encryption_key_id: ?[]const u8 = null,
    role: []const u8,
};

pub const Shard = struct {
    group_id: u64,
    start_key_base64: []const u8,
    end_key_base64: ?[]const u8 = null,
    blob_sha256: []const []const u8,
    capture_revision: u64,
    checkpoint_revision: u64,
};

pub const Manifest = struct {
    schema_version: u32 = manifest_schema_version,
    backup_id: []const u8,
    table_id: u64,
    table_name: []const u8,
    catalog_sha256: []const u8,
    representation: bundle.Representation,
    mode: bundle.SnapshotMode = .full,
    parent_manifest_sha256: ?[]const u8 = null,
    created_at_unix_ns: i128,
    compatibility: bundle.Compatibility = .{},
    compression: bundle.Compression = .none,
    encryption: ?bundle.Encryption = null,
    shards: []const Shard,
    /// Complete, digest-sorted materialized inventory for this snapshot.
    blobs: []const BlobRef,
};

pub const Ref = struct {
    schema_version: u32 = ref_schema_version,
    backup_id: []const u8,
    manifest_sha256: []const u8,
    generation: u64,
    updated_at_unix_ns: i128,
};

pub const RefPublication = struct {
    next: Ref,
    /// Null means create-only. Otherwise the backend must compare the current
    /// digest and generation atomically before replacing the ref.
    expected_manifest_sha256: ?[]const u8 = null,
    expected_generation: ?u64 = null,
};

pub const Lease = struct {
    schema_version: u32 = lease_schema_version,
    lease_id: []const u8,
    manifest_sha256: []const u8,
    expires_at_unix_ns: i128,
};

/// Storage adapters implement this contract for local filesystems and object
/// stores. Immutable puts must be create-if-absent (an existing byte-identical
/// object is success); refs must use the backend's atomic compare-and-swap or
/// conditional-write primitive. No backup is visible until `publish_ref`.
pub const Backend = struct {
    context: *anyopaque,
    put_immutable: *const fn (context: *anyopaque, path: []const u8, bytes: []const u8) anyerror!void,
    read_alloc: *const fn (context: *anyopaque, alloc: std.mem.Allocator, path: []const u8, limit: usize) anyerror![]u8,
    contains: *const fn (context: *anyopaque, path: []const u8) anyerror!bool,
    publish_ref: *const fn (
        context: *anyopaque,
        path: []const u8,
        encoded: []const u8,
        expected_manifest_sha256: ?[]const u8,
        expected_generation: ?u64,
    ) anyerror!void,
    delete_if_older_than: *const fn (context: *anyopaque, path: []const u8, cutoff_unix_ns: i128) anyerror!bool,
};

pub fn refPathAlloc(alloc: std.mem.Allocator, backup_id: []const u8) ![]u8 {
    try validateBackupId(backup_id);
    return try std.fmt.allocPrint(alloc, "refs/{s}", .{backup_id});
}

pub fn manifestPathAlloc(alloc: std.mem.Allocator, sha256: []const u8) ![]u8 {
    try bundle.validateSha256(sha256);
    return try std.fmt.allocPrint(alloc, "manifests/{s}", .{sha256});
}

pub fn blobPathAlloc(alloc: std.mem.Allocator, sha256: []const u8) ![]u8 {
    try bundle.validateSha256(sha256);
    return try std.fmt.allocPrint(alloc, "blobs/sha256/{s}", .{sha256});
}

pub fn leasePathAlloc(alloc: std.mem.Allocator, lease_id: []const u8) ![]u8 {
    try validateBackupId(lease_id);
    return try std.fmt.allocPrint(alloc, "leases/{s}", .{lease_id});
}

pub fn encodeManifestCanonicalAlloc(alloc: std.mem.Allocator, manifest: Manifest) ![]u8 {
    try validateManifest(manifest);
    const encoded = try std.json.Stringify.valueAlloc(alloc, manifest, .{});
    errdefer alloc.free(encoded);
    try validateManifestEncodedSize(encoded.len);
    return encoded;
}

pub const ParsedManifest = std.json.Parsed(Manifest);

pub fn parseManifestCanonical(alloc: std.mem.Allocator, encoded: []const u8) !ParsedManifest {
    try validateManifestEncodedSize(encoded.len);
    var parsed = std.json.parseFromSlice(Manifest, alloc, encoded, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = false,
    }) catch return error.InvalidBackupManifest;
    errdefer parsed.deinit();
    try validateManifest(parsed.value);
    const canonical = try encodeManifestCanonicalAlloc(alloc, parsed.value);
    defer alloc.free(canonical);
    if (!std.mem.eql(u8, canonical, encoded)) return error.NonCanonicalBackupManifest;
    return parsed;
}

fn validateManifestEncodedSize(encoded_len: usize) !void {
    if (encoded_len == 0 or encoded_len > max_manifest_bytes)
        return error.BackupManifestTooLarge;
}

pub fn manifestDigestHexAlloc(alloc: std.mem.Allocator, encoded: []const u8) ![]u8 {
    var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(encoded, &digest, .{});
    const hex = std.fmt.bytesToHex(digest, .lower);
    return try alloc.dupe(u8, &hex);
}

/// Publishes a fully uploaded snapshot. This function intentionally performs
/// the mutable ref write last; a crash before that point leaves only harmless
/// unreachable immutable objects for grace-period GC.
pub fn publishSnapshot(
    alloc: std.mem.Allocator,
    backend: Backend,
    manifest: Manifest,
    publication: RefPublication,
) ![]u8 {
    try validateManifest(manifest);
    const encoded_manifest = try encodeManifestCanonicalAlloc(alloc, manifest);
    defer alloc.free(encoded_manifest);
    const digest = try manifestDigestHexAlloc(alloc, encoded_manifest);
    errdefer alloc.free(digest);
    if (!std.mem.eql(u8, publication.next.manifest_sha256, digest) or
        !std.mem.eql(u8, publication.next.backup_id, manifest.backup_id))
        return error.InvalidBackupRef;
    for (manifest.blobs) |blob| {
        const path = try blobPathAlloc(alloc, blob.sha256);
        defer alloc.free(path);
        if (!try backend.contains(backend.context, path)) return error.BackupBlobMissing;
    }
    const manifest_path = try manifestPathAlloc(alloc, digest);
    defer alloc.free(manifest_path);
    try backend.put_immutable(backend.context, manifest_path, encoded_manifest);

    const ref_path = try refPathAlloc(alloc, publication.next.backup_id);
    defer alloc.free(ref_path);
    const encoded_ref = try encodeRefCanonicalAlloc(alloc, publication.next);
    defer alloc.free(encoded_ref);
    try backend.publish_ref(
        backend.context,
        ref_path,
        encoded_ref,
        publication.expected_manifest_sha256,
        publication.expected_generation,
    );
    return digest;
}

pub fn validateManifest(manifest: Manifest) !void {
    if (manifest.schema_version != manifest_schema_version) return error.UnsupportedBackupManifestVersion;
    try validateBackupId(manifest.backup_id);
    if (manifest.table_id == 0 or manifest.table_name.len == 0) return error.InvalidBackupManifest;
    try bundle.validateSha256(manifest.catalog_sha256);
    switch (manifest.mode) {
        .full => if (manifest.parent_manifest_sha256 != null) return error.InvalidBackupManifest,
        .delta => try bundle.validateSha256(manifest.parent_manifest_sha256 orelse return error.InvalidBackupManifest),
    }
    if (manifest.blobs.len == 0 or manifest.blobs.len > max_blobs or
        manifest.shards.len == 0 or manifest.shards.len > max_shards)
        return error.InvalidBackupManifest;

    var previous_digest: ?[]const u8 = null;
    for (manifest.blobs) |blob| {
        try bundle.validateSha256(blob.sha256);
        if (blob.role.len == 0)
            return error.InvalidBackupManifest;
        if (previous_digest) |previous| {
            if (std.mem.order(u8, previous, blob.sha256) != .lt)
                return error.NonCanonicalBackupManifest;
        }
        previous_digest = blob.sha256;
    }
    if (!containsBlob(manifest.blobs, manifest.catalog_sha256))
        return error.IncompleteBackupInventory;

    var previous_group_id: u64 = 0;
    for (manifest.shards, 0..) |shard, shard_index| {
        if (shard.group_id == 0 or (shard_index > 0 and shard.group_id <= previous_group_id) or
            shard.blob_sha256.len == 0 or shard.checkpoint_revision > shard.capture_revision)
            return error.NonCanonicalBackupManifest;
        previous_group_id = shard.group_id;
        var previous_shard_digest: ?[]const u8 = null;
        for (shard.blob_sha256) |digest| {
            try bundle.validateSha256(digest);
            if (previous_shard_digest) |previous| {
                if (std.mem.order(u8, previous, digest) != .lt)
                    return error.NonCanonicalBackupManifest;
            }
            if (!containsBlob(manifest.blobs, digest)) return error.IncompleteBackupInventory;
            previous_shard_digest = digest;
        }
    }
}

/// Validates lineage invariants and returns indices of current-snapshot blobs
/// absent from the complete parent inventory. Capture can upload only these
/// objects, while the child manifest remains independently restorable.
pub fn incrementalBlobIndicesAlloc(
    alloc: std.mem.Allocator,
    child_manifest_sha256: []const u8,
    child: Manifest,
    parent_manifest_sha256: []const u8,
    parent: Manifest,
) ![]usize {
    try bundle.validateSha256(child_manifest_sha256);
    try bundle.validateSha256(parent_manifest_sha256);
    try validateManifest(child);
    try validateManifest(parent);
    if (child.mode != .delta or child.parent_manifest_sha256 == null or
        !std.mem.eql(u8, child.parent_manifest_sha256.?, parent_manifest_sha256) or
        child.table_id != parent.table_id or
        !std.mem.eql(u8, child.table_name, parent.table_name) or
        child.representation != parent.representation or
        std.mem.eql(u8, child_manifest_sha256, parent_manifest_sha256))
        return error.InvalidBackupLineage;

    var changed = std.ArrayListUnmanaged(usize).empty;
    errdefer changed.deinit(alloc);
    var parent_index: usize = 0;
    for (child.blobs, 0..) |blob, child_index| {
        while (parent_index < parent.blobs.len and
            std.mem.order(u8, parent.blobs[parent_index].sha256, blob.sha256) == .lt)
            parent_index += 1;
        if (parent_index == parent.blobs.len or
            !std.mem.eql(u8, parent.blobs[parent_index].sha256, blob.sha256))
            try changed.append(alloc, child_index);
    }
    return try changed.toOwnedSlice(alloc);
}

fn containsBlob(blobs: []const BlobRef, digest: []const u8) bool {
    var low: usize = 0;
    var high: usize = blobs.len;
    while (low < high) {
        const mid = low + (high - low) / 2;
        switch (std.mem.order(u8, blobs[mid].sha256, digest)) {
            .lt => low = mid + 1,
            .gt => high = mid,
            .eq => return true,
        }
    }
    return false;
}

pub fn validateRef(ref: Ref) !void {
    if (ref.schema_version != ref_schema_version or ref.generation == 0)
        return error.InvalidBackupRef;
    try validateBackupId(ref.backup_id);
    try bundle.validateSha256(ref.manifest_sha256);
}

pub fn encodeRefCanonicalAlloc(alloc: std.mem.Allocator, ref: Ref) ![]u8 {
    try validateRef(ref);
    return try std.json.Stringify.valueAlloc(alloc, ref, .{});
}

pub fn validateLease(lease: Lease, now_unix_ns: i128) !void {
    if (lease.schema_version != lease_schema_version or lease.expires_at_unix_ns <= now_unix_ns)
        return error.InvalidBackupLease;
    try validateBackupId(lease.lease_id);
    try bundle.validateSha256(lease.manifest_sha256);
}

pub fn encodeLeaseCanonicalAlloc(alloc: std.mem.Allocator, lease: Lease, now_unix_ns: i128) ![]u8 {
    try validateLease(lease, now_unix_ns);
    return try std.json.Stringify.valueAlloc(alloc, lease, .{});
}

pub fn validateRefPublication(publication: RefPublication, current: ?Ref) !void {
    try validateRef(publication.next);
    if (current) |existing| {
        try validateRef(existing);
        const expected_digest = publication.expected_manifest_sha256 orelse
            return error.BackupRefConflict;
        const expected_generation = publication.expected_generation orelse
            return error.BackupRefConflict;
        if (!std.mem.eql(u8, expected_digest, existing.manifest_sha256) or
            expected_generation != existing.generation or
            publication.next.generation != existing.generation + 1)
            return error.BackupRefConflict;
    } else if (publication.expected_manifest_sha256 != null or
        publication.expected_generation != null or publication.next.generation != 1)
    {
        return error.BackupRefConflict;
    }
}

pub fn validateBackupId(value: []const u8) !void {
    if (value.len == 0 or value.len > max_ref_name_bytes or
        std.mem.eql(u8, value, ".") or std.mem.eql(u8, value, ".."))
        return error.InvalidBackupId;
    for (value) |char| {
        if (!std.ascii.isAlphanumeric(char) and char != '-' and char != '_' and char != '.')
            return error.InvalidBackupId;
    }
}

/// In-memory result used by backend GC implementations after walking refs,
/// active leases, and parent links. Deletion still requires a backend-specific
/// grace-period check against object creation time.
pub const Reachability = struct {
    manifests: std.StringHashMapUnmanaged(void) = .empty,
    blobs: std.StringHashMapUnmanaged(void) = .empty,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        var manifest_it = self.manifests.keyIterator();
        while (manifest_it.next()) |key| alloc.free(key.*);
        var blob_it = self.blobs.keyIterator();
        while (blob_it.next()) |key| alloc.free(key.*);
        self.manifests.deinit(alloc);
        self.blobs.deinit(alloc);
        self.* = .{};
    }

    pub fn markManifest(self: *@This(), alloc: std.mem.Allocator, digest: []const u8) !bool {
        try bundle.validateSha256(digest);
        if (self.manifests.contains(digest)) return false;
        const owned = try alloc.dupe(u8, digest);
        errdefer alloc.free(owned);
        try self.manifests.put(alloc, owned, {});
        return true;
    }

    pub fn markSnapshot(self: *@This(), alloc: std.mem.Allocator, digest: []const u8, manifest: Manifest) !void {
        _ = try self.markManifest(alloc, digest);
        try validateManifest(manifest);
        for (manifest.blobs) |blob| {
            if (self.blobs.contains(blob.sha256)) continue;
            const owned = try alloc.dupe(u8, blob.sha256);
            errdefer alloc.free(owned);
            try self.blobs.put(alloc, owned, {});
        }
    }
};

/// Marks complete inventories reachable from live refs and unexpired restore
/// leases, following parent links only for lineage retention. A cycle is safe:
/// `markManifest` provides the traversal visited set.
pub fn buildReachability(
    alloc: std.mem.Allocator,
    backend: Backend,
    refs: []const Ref,
    leases: []const Lease,
    now_unix_ns: i128,
) !Reachability {
    var result: Reachability = .{};
    errdefer result.deinit(alloc);
    var pending = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (pending.items) |digest| alloc.free(digest);
        pending.deinit(alloc);
    }
    for (refs) |ref| {
        try validateRef(ref);
        try pending.append(alloc, try alloc.dupe(u8, ref.manifest_sha256));
    }
    for (leases) |lease| {
        validateLease(lease, now_unix_ns) catch |err| switch (err) {
            error.InvalidBackupLease => continue,
            else => return err,
        };
        try pending.append(alloc, try alloc.dupe(u8, lease.manifest_sha256));
    }
    while (pending.pop()) |owned_digest| {
        defer alloc.free(owned_digest);
        if (!try result.markManifest(alloc, owned_digest)) continue;
        const path = try manifestPathAlloc(alloc, owned_digest);
        defer alloc.free(path);
        const encoded = try backend.read_alloc(backend.context, alloc, path, max_manifest_bytes);
        defer alloc.free(encoded);
        var parsed = try parseManifestCanonical(alloc, encoded);
        defer parsed.deinit();
        for (parsed.value.blobs) |blob| {
            if (result.blobs.contains(blob.sha256)) continue;
            const owned_blob = try alloc.dupe(u8, blob.sha256);
            errdefer alloc.free(owned_blob);
            try result.blobs.put(alloc, owned_blob, {});
        }
        if (parsed.value.parent_manifest_sha256) |parent|
            try pending.append(alloc, try alloc.dupe(u8, parent));
    }
    return result;
}

pub const GarbageCandidate = struct {
    kind: enum { manifest, blob },
    sha256: []const u8,
};

/// Sweep is deliberately a separate, grace-gated phase. Backends enumerate
/// immutable candidates and provide their own creation-time conditional
/// delete, preventing a racing uploader from being removed between mark and
/// ref publication.
pub fn sweepUnreachable(
    alloc: std.mem.Allocator,
    backend: Backend,
    reachable: *const Reachability,
    candidates: []const GarbageCandidate,
    grace_cutoff_unix_ns: i128,
) !usize {
    var deleted: usize = 0;
    for (candidates) |candidate| {
        try bundle.validateSha256(candidate.sha256);
        const retained = switch (candidate.kind) {
            .manifest => reachable.manifests.contains(candidate.sha256),
            .blob => reachable.blobs.contains(candidate.sha256),
        };
        if (retained) continue;
        const path = switch (candidate.kind) {
            .manifest => try manifestPathAlloc(alloc, candidate.sha256),
            .blob => try blobPathAlloc(alloc, candidate.sha256),
        };
        defer alloc.free(path);
        if (try backend.delete_if_older_than(backend.context, path, grace_cutoff_unix_ns))
            deleted += 1;
    }
    return deleted;
}

test "repository manifest is a complete canonical materialized inventory" {
    const digest_a = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    const digest_b = "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789";
    const manifest: Manifest = .{
        .backup_id = "quickstart-2026",
        .table_id = 7,
        .table_name = "wikipedia",
        .catalog_sha256 = digest_a,
        .representation = .native,
        .mode = .delta,
        .parent_manifest_sha256 = digest_b,
        .created_at_unix_ns = 1,
        .shards = &.{.{
            .group_id = 9,
            .start_key_base64 = "",
            .blob_sha256 = &.{ digest_a, digest_b },
            .capture_revision = 42,
            .checkpoint_revision = 42,
        }},
        .blobs = &.{
            .{ .sha256 = digest_a, .logical_size_bytes = 10, .stored_size_bytes = 10, .role = "primary" },
            .{ .sha256 = digest_b, .logical_size_bytes = 20, .stored_size_bytes = 20, .role = "dense_projection" },
        },
    };
    try validateManifest(manifest);
    const alloc = std.testing.allocator;
    const encoded = try encodeManifestCanonicalAlloc(alloc, manifest);
    defer alloc.free(encoded);
    const digest = try manifestDigestHexAlloc(alloc, encoded);
    defer alloc.free(digest);
    try bundle.validateSha256(digest);
}

test "repository manifest parsing is bounded before allocation" {
    try validateManifestEncodedSize(max_manifest_bytes);
    try std.testing.expectError(
        error.BackupManifestTooLarge,
        validateManifestEncodedSize(max_manifest_bytes + 1),
    );
    try std.testing.expectError(error.BackupManifestTooLarge, validateManifestEncodedSize(0));
}

test "repository ref publication is compare and swap" {
    const old_digest = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    const new_digest = "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789";
    const current: Ref = .{
        .backup_id = "daily",
        .manifest_sha256 = old_digest,
        .generation = 4,
        .updated_at_unix_ns = 1,
    };
    try validateRefPublication(.{
        .next = .{ .backup_id = "daily", .manifest_sha256 = new_digest, .generation = 5, .updated_at_unix_ns = 2 },
        .expected_manifest_sha256 = old_digest,
        .expected_generation = 4,
    }, current);
    try std.testing.expectError(error.BackupRefConflict, validateRefPublication(.{
        .next = .{ .backup_id = "daily", .manifest_sha256 = new_digest, .generation = 5, .updated_at_unix_ns = 2 },
        .expected_manifest_sha256 = new_digest,
        .expected_generation = 4,
    }, current));
}

test "incremental plan uploads only blobs absent from complete parent" {
    const digest_a = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    const digest_b = "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789";
    const digest_child = "1111111111111111111111111111111111111111111111111111111111111111";
    const parent: Manifest = .{
        .backup_id = "daily",
        .table_id = 7,
        .table_name = "docs",
        .catalog_sha256 = digest_a,
        .representation = .native,
        .created_at_unix_ns = 1,
        .shards = &.{.{
            .group_id = 9,
            .start_key_base64 = "",
            .blob_sha256 = &.{digest_a},
            .capture_revision = 41,
            .checkpoint_revision = 41,
        }},
        .blobs = &.{.{ .sha256 = digest_a, .logical_size_bytes = 10, .stored_size_bytes = 10, .role = "catalog" }},
    };
    const child: Manifest = .{
        .backup_id = "daily",
        .table_id = 7,
        .table_name = "docs",
        .catalog_sha256 = digest_a,
        .representation = .native,
        .mode = .delta,
        .parent_manifest_sha256 = digest_b,
        .created_at_unix_ns = 2,
        .shards = &.{.{
            .group_id = 9,
            .start_key_base64 = "",
            .blob_sha256 = &.{ digest_a, digest_child },
            .capture_revision = 42,
            .checkpoint_revision = 42,
        }},
        .blobs = &.{
            .{ .sha256 = digest_a, .logical_size_bytes = 10, .stored_size_bytes = 10, .role = "catalog" },
            .{ .sha256 = digest_child, .logical_size_bytes = 20, .stored_size_bytes = 20, .role = "primary" },
        },
    };
    const changed = try incrementalBlobIndicesAlloc(std.testing.allocator, digest_child, child, digest_b, parent);
    defer std.testing.allocator.free(changed);
    try std.testing.expectEqualSlices(usize, &.{1}, changed);
}
