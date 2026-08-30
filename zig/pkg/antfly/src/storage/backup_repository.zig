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
const fs_paths = @import("../common/fs_paths.zig");

const Sha256 = std.crypto.hash.sha2.Sha256;
const io_buffer_bytes: usize = 256 * 1024;

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
};

/// A logical snapshot object is independently named and points at immutable
/// content. Keeping path/role outside BlobRef is what permits one physical
/// blob to back multiple files without losing the information needed to
/// reconstruct the captured generation.
pub const ObjectRef = struct {
    logical_path: []const u8,
    role: []const u8,
    blob_sha256: []const u8,
    logical_size_bytes: u64,
};

pub const Shard = struct {
    group_id: u64,
    start_key_base64: []const u8,
    end_key_base64: ?[]const u8 = null,
    /// Canonical, path-sorted logical object membership. Global catalog
    /// objects need not belong to a shard.
    object_paths: []const []const u8,
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
    /// Complete, path-sorted logical inventory for this snapshot.
    objects: []const ObjectRef,
    /// Complete, digest-sorted unique physical inventory for this snapshot.
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
    /// Stream a verified local file into an immutable repository object. The
    /// backend must use create-if-absent semantics; an existing object is
    /// success only when its bytes match the declared digest and size.
    put_blob_from_file: *const fn (
        context: *anyopaque,
        io: std.Io,
        path: []const u8,
        source_path: []const u8,
        logical_size_bytes: u64,
        sha256: []const u8,
    ) anyerror!void,
    /// Stream one immutable repository blob into a new local file. Restore
    /// revalidates size and digest before exposing the staging generation.
    materialize_blob_to_file: *const fn (
        context: *anyopaque,
        io: std.Io,
        path: []const u8,
        destination_path: []const u8,
        logical_size_bytes: u64,
        sha256: []const u8,
    ) anyerror!void,
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
pub const ParsedRef = std.json.Parsed(Ref);

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

pub fn parseRefCanonical(alloc: std.mem.Allocator, encoded: []const u8) !ParsedRef {
    if (encoded.len == 0 or encoded.len > 64 * 1024) return error.InvalidBackupRef;
    var parsed = std.json.parseFromSlice(Ref, alloc, encoded, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = false,
    }) catch return error.InvalidBackupRef;
    errdefer parsed.deinit();
    try validateRef(parsed.value);
    const canonical = try encodeRefCanonicalAlloc(alloc, parsed.value);
    defer alloc.free(canonical);
    if (!std.mem.eql(u8, canonical, encoded)) return error.NonCanonicalBackupRef;
    return parsed;
}

pub const ResolvedSnapshot = struct {
    ref: ParsedRef,
    manifest: ParsedManifest,

    pub fn deinit(self: *@This()) void {
        self.manifest.deinit();
        self.ref.deinit();
        self.* = undefined;
    }
};

/// Resolves a mutable ref once, then reads the immutable manifest it names.
/// Callers retain that digest for the whole operation; a later ref update can
/// never splice a different snapshot into an in-flight restore.
pub fn resolveSnapshot(
    alloc: std.mem.Allocator,
    backend: Backend,
    backup_id: []const u8,
) !ResolvedSnapshot {
    const ref_path = try refPathAlloc(alloc, backup_id);
    defer alloc.free(ref_path);
    const encoded_ref = try backend.read_alloc(backend.context, alloc, ref_path, 64 * 1024);
    defer alloc.free(encoded_ref);
    var parsed_ref = try parseRefCanonical(alloc, encoded_ref);
    errdefer parsed_ref.deinit();
    if (!std.mem.eql(u8, parsed_ref.value.backup_id, backup_id)) return error.InvalidBackupRef;
    const manifest_path = try manifestPathAlloc(alloc, parsed_ref.value.manifest_sha256);
    defer alloc.free(manifest_path);
    const encoded_manifest = try backend.read_alloc(
        backend.context,
        alloc,
        manifest_path,
        max_manifest_bytes,
    );
    defer alloc.free(encoded_manifest);
    var parsed_manifest = try parseManifestCanonical(alloc, encoded_manifest);
    errdefer parsed_manifest.deinit();
    if (!std.mem.eql(u8, parsed_manifest.value.backup_id, backup_id))
        return error.InvalidBackupRef;
    const actual_digest = try manifestDigestHexAlloc(alloc, encoded_manifest);
    defer alloc.free(actual_digest);
    if (!std.mem.eql(u8, actual_digest, parsed_ref.value.manifest_sha256))
        return error.BackupArtifactIntegrityMismatch;
    return .{ .ref = parsed_ref, .manifest = parsed_manifest };
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

/// Uploads each unique blob from a captured local generation. Object paths are
/// the authoritative mapping back to source files; duplicate objects select
/// one representative and therefore upload their shared content only once.
pub fn uploadSnapshotBlobsFromDirectory(
    alloc: std.mem.Allocator,
    io: std.Io,
    backend: Backend,
    manifest: Manifest,
    source_root: []const u8,
) !void {
    try validateManifest(manifest);
    if (manifest.encryption != null) return error.UnsupportedBackupEncryption;
    for (manifest.blobs, 0..) |_, blob_index|
        try uploadSnapshotBlobFromDirectory(alloc, io, backend, manifest, source_root, blob_index);
}

/// Streams only content absent from the exact complete parent inventory. The
/// child manifest remains complete and independently restorable; this narrows
/// capture I/O without turning restore into parent-chain replay.
pub fn uploadIncrementalSnapshotBlobsFromDirectory(
    alloc: std.mem.Allocator,
    io: std.Io,
    backend: Backend,
    child: Manifest,
    parent_manifest_sha256: []const u8,
    parent: Manifest,
    source_root: []const u8,
) !void {
    const changed = try incrementalBlobIndicesAlloc(alloc, child, parent_manifest_sha256, parent);
    defer alloc.free(changed);
    for (changed) |blob_index|
        try uploadSnapshotBlobFromDirectory(alloc, io, backend, child, source_root, blob_index);
}

fn uploadSnapshotBlobFromDirectory(
    alloc: std.mem.Allocator,
    io: std.Io,
    backend: Backend,
    manifest: Manifest,
    source_root: []const u8,
    blob_index: usize,
) !void {
    if (blob_index >= manifest.blobs.len) return error.InvalidBackupManifest;
    const blob = manifest.blobs[blob_index];
    if (blob.compression != .none or blob.encryption_key_id != null)
        return error.UnsupportedBackupCompression;
    const object = findObjectForBlob(manifest.objects, blob.sha256) orelse
        return error.IncompleteBackupInventory;
    const source_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ source_root, object.logical_path });
    defer alloc.free(source_path);
    try verifyFileDigest(io, source_path, blob.logical_size_bytes, blob.sha256);
    const repository_path = try blobPathAlloc(alloc, blob.sha256);
    defer alloc.free(repository_path);
    try backend.put_blob_from_file(
        backend.context,
        io,
        repository_path,
        source_path,
        blob.logical_size_bytes,
        blob.sha256,
    );
}

/// Materializes one complete immutable manifest into a caller-owned staging
/// generation. Every unique blob is downloaded once into a private cache,
/// verified, then copied to all logical object paths. The destination is never
/// partially exposed and is removed on failure.
pub fn materializeSnapshotToStagingDirectory(
    alloc: std.mem.Allocator,
    io: std.Io,
    backend: Backend,
    manifest: Manifest,
    staging_root: []const u8,
) !void {
    try validateManifest(manifest);
    if (manifest.encryption != null) return error.UnsupportedBackupEncryption;
    if (pathExists(io, staging_root)) return error.PathAlreadyExists;
    const blob_root = try std.fmt.allocPrint(alloc, "{s}.repository-blobs", .{staging_root});
    defer alloc.free(blob_root);
    if (pathExists(io, blob_root)) return error.PathAlreadyExists;
    try fs_paths.createDirPathPortable(io, staging_root);
    errdefer std.Io.Dir.cwd().deleteTree(io, staging_root) catch {};
    try fs_paths.createDirPathPortable(io, blob_root);
    defer std.Io.Dir.cwd().deleteTree(io, blob_root) catch {};

    for (manifest.blobs) |blob| {
        if (blob.compression != .none or blob.encryption_key_id != null)
            return error.UnsupportedBackupCompression;
        const repository_path = try blobPathAlloc(alloc, blob.sha256);
        defer alloc.free(repository_path);
        const cached_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ blob_root, blob.sha256 });
        defer alloc.free(cached_path);
        try backend.materialize_blob_to_file(
            backend.context,
            io,
            repository_path,
            cached_path,
            blob.logical_size_bytes,
            blob.sha256,
        );
        try verifyFileDigest(io, cached_path, blob.logical_size_bytes, blob.sha256);
    }
    for (manifest.objects) |object| {
        const cached_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ blob_root, object.blob_sha256 });
        defer alloc.free(cached_path);
        const destination = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ staging_root, object.logical_path });
        defer alloc.free(destination);
        if (std.fs.path.dirname(destination)) |parent| try fs_paths.createDirPathPortable(io, parent);
        try copyFileBounded(io, cached_path, destination, object.logical_size_bytes);
    }
    try fs_paths.syncDirPortable(io, staging_root);
}

fn findObjectForBlob(objects: []const ObjectRef, digest: []const u8) ?*const ObjectRef {
    for (objects) |*object| {
        if (std.mem.eql(u8, object.blob_sha256, digest)) return object;
    }
    return null;
}

fn verifyFileDigest(io: std.Io, path: []const u8, expected_size: u64, expected_hex: []const u8) !void {
    var file = if (std.fs.path.isAbsolute(path))
        try std.Io.Dir.openFileAbsolute(io, path, .{})
    else
        try std.Io.Dir.cwd().openFile(io, path, .{});
    defer file.close(io);
    const stat = try file.stat(io);
    if (stat.size != expected_size) return error.BackupArtifactIntegrityMismatch;
    var hasher = Sha256.init(.{});
    var buffer: [io_buffer_bytes]u8 = undefined;
    var offset: u64 = 0;
    while (offset < stat.size) {
        const wanted: usize = @intCast(@min(@as(u64, buffer.len), stat.size - offset));
        const read = try file.readPositionalAll(io, buffer[0..wanted], offset);
        if (read != wanted) return error.BackupArtifactIntegrityMismatch;
        hasher.update(buffer[0..wanted]);
        offset += wanted;
    }
    var digest: [Sha256.digest_length]u8 = undefined;
    hasher.final(&digest);
    const actual_hex = std.fmt.bytesToHex(digest, .lower);
    if (!std.mem.eql(u8, &actual_hex, expected_hex)) return error.BackupArtifactIntegrityMismatch;
}

fn copyFileBounded(io: std.Io, source_path: []const u8, destination_path: []const u8, size: u64) !void {
    var source = if (std.fs.path.isAbsolute(source_path))
        try std.Io.Dir.openFileAbsolute(io, source_path, .{})
    else
        try std.Io.Dir.cwd().openFile(io, source_path, .{});
    defer source.close(io);
    var destination = try fs_paths.createFilePortable(io, destination_path, .{ .truncate = true, .exclusive = true });
    defer destination.close(io);
    var buffer: [io_buffer_bytes]u8 = undefined;
    var offset: u64 = 0;
    while (offset < size) {
        const wanted: usize = @intCast(@min(@as(u64, buffer.len), size - offset));
        const read = try source.readPositionalAll(io, buffer[0..wanted], offset);
        if (read != wanted) return error.BackupArtifactIntegrityMismatch;
        try destination.writePositionalAll(io, buffer[0..wanted], offset);
        offset += wanted;
    }
    try destination.sync(io);
}

fn pathExists(io: std.Io, path: []const u8) bool {
    var dir = if (std.fs.path.isAbsolute(path))
        std.Io.Dir.openDirAbsolute(io, path, .{}) catch return false
    else
        std.Io.Dir.cwd().openDir(io, path, .{}) catch return false;
    dir.close(io);
    return true;
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
    if (manifest.objects.len == 0 or manifest.objects.len > max_blobs or
        manifest.blobs.len == 0 or manifest.blobs.len > max_blobs or
        manifest.shards.len == 0 or manifest.shards.len > max_shards)
        return error.InvalidBackupManifest;

    var previous_digest: ?[]const u8 = null;
    for (manifest.blobs) |blob| {
        try bundle.validateSha256(blob.sha256);
        if (blob.compression == .none and blob.logical_size_bytes != blob.stored_size_bytes)
            return error.InvalidBackupManifest;
        if ((blob.encryption_key_id == null) != (manifest.encryption == null))
            return error.InvalidBackupManifest;
        if (previous_digest) |previous| {
            if (std.mem.order(u8, previous, blob.sha256) != .lt)
                return error.NonCanonicalBackupManifest;
        }
        previous_digest = blob.sha256;
    }
    if (!containsBlob(manifest.blobs, manifest.catalog_sha256))
        return error.IncompleteBackupInventory;

    var previous_path: ?[]const u8 = null;
    var catalog_object_found = false;
    for (manifest.objects) |object| {
        try bundle.validateRelativePath(object.logical_path);
        if (object.role.len == 0 or object.role.len > 128)
            return error.InvalidBackupManifest;
        try bundle.validateSha256(object.blob_sha256);
        const blob = findBlob(manifest.blobs, object.blob_sha256) orelse
            return error.IncompleteBackupInventory;
        if (blob.logical_size_bytes != object.logical_size_bytes)
            return error.InvalidBackupManifest;
        if (previous_path) |previous| {
            if (std.mem.order(u8, previous, object.logical_path) != .lt)
                return error.NonCanonicalBackupManifest;
        }
        previous_path = object.logical_path;
        if (std.mem.eql(u8, object.role, "catalog") and
            std.mem.eql(u8, object.blob_sha256, manifest.catalog_sha256))
            catalog_object_found = true;
    }
    if (!catalog_object_found) return error.IncompleteBackupInventory;
    var previous_group_id: u64 = 0;
    for (manifest.shards, 0..) |shard, shard_index| {
        if (shard.group_id == 0 or (shard_index > 0 and shard.group_id <= previous_group_id) or
            shard.object_paths.len == 0 or shard.checkpoint_revision > shard.capture_revision)
            return error.NonCanonicalBackupManifest;
        previous_group_id = shard.group_id;
        var previous_shard_path: ?[]const u8 = null;
        for (shard.object_paths) |path| {
            try bundle.validateRelativePath(path);
            if (previous_shard_path) |previous| {
                if (std.mem.order(u8, previous, path) != .lt)
                    return error.NonCanonicalBackupManifest;
            }
            if (!containsObjectPath(manifest.objects, path)) return error.IncompleteBackupInventory;
            previous_shard_path = path;
        }
    }
}

/// Validates lineage invariants and returns indices of current-snapshot blobs
/// absent from the complete parent inventory. Capture can upload only these
/// objects, while the child manifest remains independently restorable.
pub fn incrementalBlobIndicesAlloc(
    alloc: std.mem.Allocator,
    child: Manifest,
    parent_manifest_sha256: []const u8,
    parent: Manifest,
) ![]usize {
    try bundle.validateSha256(parent_manifest_sha256);
    try validateManifest(child);
    try validateManifest(parent);
    const parent_encoded = try encodeManifestCanonicalAlloc(alloc, parent);
    defer alloc.free(parent_encoded);
    const actual_parent_sha256 = try manifestDigestHexAlloc(alloc, parent_encoded);
    defer alloc.free(actual_parent_sha256);
    const child_encoded = try encodeManifestCanonicalAlloc(alloc, child);
    defer alloc.free(child_encoded);
    const child_manifest_sha256 = try manifestDigestHexAlloc(alloc, child_encoded);
    defer alloc.free(child_manifest_sha256);
    if (!std.mem.eql(u8, actual_parent_sha256, parent_manifest_sha256) or
        child.mode != .delta or child.parent_manifest_sha256 == null or
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
    return findBlob(blobs, digest) != null;
}

fn findBlob(blobs: []const BlobRef, digest: []const u8) ?*const BlobRef {
    var low: usize = 0;
    var high: usize = blobs.len;
    while (low < high) {
        const mid = low + (high - low) / 2;
        switch (std.mem.order(u8, blobs[mid].sha256, digest)) {
            .lt => low = mid + 1,
            .gt => high = mid,
            .eq => return &blobs[mid],
        }
    }
    return null;
}

fn containsObjectPath(objects: []const ObjectRef, path: []const u8) bool {
    var low: usize = 0;
    var high: usize = objects.len;
    while (low < high) {
        const mid = low + (high - low) / 2;
        switch (std.mem.order(u8, objects[mid].logical_path, path)) {
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
            .object_paths = &.{"shards/9/dense/segment-1"},
            .capture_revision = 42,
            .checkpoint_revision = 42,
        }},
        .objects = &.{
            .{ .logical_path = "catalog/table.json", .role = "catalog", .blob_sha256 = digest_a, .logical_size_bytes = 10 },
            .{ .logical_path = "shards/9/dense/segment-1", .role = "dense_projection", .blob_sha256 = digest_b, .logical_size_bytes = 20 },
        },
        .blobs = &.{
            .{ .sha256 = digest_a, .logical_size_bytes = 10, .stored_size_bytes = 10 },
            .{ .sha256 = digest_b, .logical_size_bytes = 20, .stored_size_bytes = 20 },
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

test "repository inventory preserves logical paths while deduplicating bytes" {
    const digest = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    const manifest: Manifest = .{
        .backup_id = "deduplicated",
        .table_id = 7,
        .table_name = "docs",
        .catalog_sha256 = digest,
        .representation = .native,
        .created_at_unix_ns = 1,
        .shards = &.{.{
            .group_id = 9,
            .start_key_base64 = "",
            .object_paths = &.{ "shards/9/copy-a", "shards/9/copy-b" },
            .capture_revision = 1,
            .checkpoint_revision = 1,
        }},
        .objects = &.{
            .{ .logical_path = "catalog/table.json", .role = "catalog", .blob_sha256 = digest, .logical_size_bytes = 10 },
            .{ .logical_path = "shards/9/copy-a", .role = "native_file", .blob_sha256 = digest, .logical_size_bytes = 10 },
            .{ .logical_path = "shards/9/copy-b", .role = "native_file", .blob_sha256 = digest, .logical_size_bytes = 10 },
        },
        .blobs = &.{.{ .sha256 = digest, .logical_size_bytes = 10, .stored_size_bytes = 10 }},
    };
    try validateManifest(manifest);
    try std.testing.expectEqual(@as(usize, 3), manifest.objects.len);
    try std.testing.expectEqual(@as(usize, 1), manifest.blobs.len);

    var invalid = manifest;
    invalid.objects = &.{.{
        .logical_path = "shards/../escape",
        .role = "catalog",
        .blob_sha256 = digest,
        .logical_size_bytes = 10,
    }};
    try std.testing.expectError(error.InvalidBackupPath, validateManifest(invalid));
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
            .object_paths = &.{"catalog/table.json"},
            .capture_revision = 41,
            .checkpoint_revision = 41,
        }},
        .objects = &.{.{ .logical_path = "catalog/table.json", .role = "catalog", .blob_sha256 = digest_a, .logical_size_bytes = 10 }},
        .blobs = &.{.{ .sha256 = digest_a, .logical_size_bytes = 10, .stored_size_bytes = 10 }},
    };
    const parent_encoded = try encodeManifestCanonicalAlloc(std.testing.allocator, parent);
    defer std.testing.allocator.free(parent_encoded);
    const parent_sha256 = try manifestDigestHexAlloc(std.testing.allocator, parent_encoded);
    defer std.testing.allocator.free(parent_sha256);
    const child: Manifest = .{
        .backup_id = "daily",
        .table_id = 7,
        .table_name = "docs",
        .catalog_sha256 = digest_a,
        .representation = .native,
        .mode = .delta,
        .parent_manifest_sha256 = parent_sha256,
        .created_at_unix_ns = 2,
        .shards = &.{.{
            .group_id = 9,
            .start_key_base64 = "",
            .object_paths = &.{"shards/9/store.bin"},
            .capture_revision = 42,
            .checkpoint_revision = 42,
        }},
        .objects = &.{
            .{ .logical_path = "catalog/table.json", .role = "catalog", .blob_sha256 = digest_a, .logical_size_bytes = 10 },
            .{ .logical_path = "shards/9/store.bin", .role = "primary", .blob_sha256 = digest_child, .logical_size_bytes = 20 },
        },
        .blobs = &.{
            .{ .sha256 = digest_a, .logical_size_bytes = 10, .stored_size_bytes = 10 },
            .{ .sha256 = digest_child, .logical_size_bytes = 20, .stored_size_bytes = 20 },
        },
    };
    const changed = try incrementalBlobIndicesAlloc(std.testing.allocator, child, parent_sha256, parent);
    defer std.testing.allocator.free(changed);
    try std.testing.expectEqualSlices(usize, &.{1}, changed);
}

const TestFilesystemBackend = struct {
    alloc: std.mem.Allocator,
    io: std.Io,
    root: []const u8,
    put_blob_calls: usize = 0,
    materialize_blob_calls: usize = 0,

    fn backend(self: *@This()) Backend {
        return .{
            .context = self,
            .put_immutable = putImmutable,
            .read_alloc = readAlloc,
            .contains = contains,
            .publish_ref = publishRef,
            .delete_if_older_than = deleteIfOlderThan,
            .put_blob_from_file = putBlobFromFile,
            .materialize_blob_to_file = materializeBlobToFile,
        };
    }

    fn pathAlloc(self: *@This(), suffix: []const u8) ![]u8 {
        return try std.fmt.allocPrint(self.alloc, "{s}/{s}", .{ self.root, suffix });
    }

    fn putImmutable(context: *anyopaque, path: []const u8, bytes: []const u8) !void {
        const self: *@This() = @ptrCast(@alignCast(context));
        const absolute = try self.pathAlloc(path);
        defer self.alloc.free(absolute);
        const existing = std.Io.Dir.cwd().readFileAlloc(
            self.io,
            absolute,
            self.alloc,
            .limited(max_manifest_bytes),
        ) catch |err| switch (err) {
            error.FileNotFound => null,
            else => return err,
        };
        if (existing) |value| {
            defer self.alloc.free(value);
            if (!std.mem.eql(u8, value, bytes)) return error.ImmutableBackupObjectConflict;
            return;
        }
        if (std.fs.path.dirname(absolute)) |parent| try fs_paths.createDirPathPortable(self.io, parent);
        try writeTestFile(self.io, absolute, bytes);
    }

    fn readAlloc(context: *anyopaque, alloc: std.mem.Allocator, path: []const u8, limit: usize) ![]u8 {
        const self: *@This() = @ptrCast(@alignCast(context));
        const absolute = try self.pathAlloc(path);
        defer self.alloc.free(absolute);
        return try std.Io.Dir.cwd().readFileAlloc(self.io, absolute, alloc, .limited(limit));
    }

    fn contains(context: *anyopaque, path: []const u8) !bool {
        const self: *@This() = @ptrCast(@alignCast(context));
        const absolute = try self.pathAlloc(path);
        defer self.alloc.free(absolute);
        var file = std.Io.Dir.cwd().openFile(self.io, absolute, .{}) catch |err| switch (err) {
            error.FileNotFound => return false,
            else => return err,
        };
        file.close(self.io);
        return true;
    }

    fn publishRef(
        context: *anyopaque,
        path: []const u8,
        encoded: []const u8,
        expected_manifest_sha256: ?[]const u8,
        expected_generation: ?u64,
    ) !void {
        const self: *@This() = @ptrCast(@alignCast(context));
        var next = try parseRefCanonical(self.alloc, encoded);
        defer next.deinit();
        const absolute = try self.pathAlloc(path);
        defer self.alloc.free(absolute);
        const existing_bytes = std.Io.Dir.cwd().readFileAlloc(
            self.io,
            absolute,
            self.alloc,
            .limited(64 * 1024),
        ) catch |err| switch (err) {
            error.FileNotFound => null,
            else => return err,
        };
        defer if (existing_bytes) |value| self.alloc.free(value);
        var current: ?ParsedRef = if (existing_bytes) |value| try parseRefCanonical(self.alloc, value) else null;
        defer if (current) |*value| value.deinit();
        try validateRefPublication(.{
            .next = next.value,
            .expected_manifest_sha256 = expected_manifest_sha256,
            .expected_generation = expected_generation,
        }, if (current) |value| value.value else null);
        if (std.fs.path.dirname(absolute)) |parent| try fs_paths.createDirPathPortable(self.io, parent);
        try writeTestFile(self.io, absolute, encoded);
    }

    fn deleteIfOlderThan(_: *anyopaque, _: []const u8, _: i128) !bool {
        return false;
    }

    fn putBlobFromFile(
        context: *anyopaque,
        io: std.Io,
        path: []const u8,
        source_path: []const u8,
        logical_size_bytes: u64,
        sha256: []const u8,
    ) !void {
        const self: *@This() = @ptrCast(@alignCast(context));
        self.put_blob_calls += 1;
        const absolute = try self.pathAlloc(path);
        defer self.alloc.free(absolute);
        if (try contains(context, path)) {
            return try verifyFileDigest(io, absolute, logical_size_bytes, sha256);
        }
        if (std.fs.path.dirname(absolute)) |parent| try fs_paths.createDirPathPortable(io, parent);
        try copyFileBounded(io, source_path, absolute, logical_size_bytes);
        try verifyFileDigest(io, absolute, logical_size_bytes, sha256);
    }

    fn materializeBlobToFile(
        context: *anyopaque,
        io: std.Io,
        path: []const u8,
        destination_path: []const u8,
        logical_size_bytes: u64,
        sha256: []const u8,
    ) !void {
        const self: *@This() = @ptrCast(@alignCast(context));
        self.materialize_blob_calls += 1;
        const absolute = try self.pathAlloc(path);
        defer self.alloc.free(absolute);
        try verifyFileDigest(io, absolute, logical_size_bytes, sha256);
        try copyFileBounded(io, absolute, destination_path, logical_size_bytes);
    }
};

fn writeTestFile(io: std.Io, path: []const u8, bytes: []const u8) !void {
    if (std.fs.path.dirname(path)) |parent| try fs_paths.createDirPathPortable(io, parent);
    var file = try fs_paths.createFilePortable(io, path, .{ .truncate = true });
    defer file.close(io);
    try file.writePositionalAll(io, bytes, 0);
    try file.sync(io);
}

test "repository incremental upload streams only blobs absent from exact parent" {
    const alloc = std.testing.allocator;
    const io = std.testing.io;
    const catalog_bytes = "catalog-v1";
    const changed_bytes = "segment-v2";
    var catalog_digest_bytes: [Sha256.digest_length]u8 = undefined;
    var changed_digest_bytes: [Sha256.digest_length]u8 = undefined;
    Sha256.hash(catalog_bytes, &catalog_digest_bytes, .{});
    Sha256.hash(changed_bytes, &changed_digest_bytes, .{});
    const catalog_sha256 = std.fmt.bytesToHex(catalog_digest_bytes, .lower);
    const changed_sha256 = std.fmt.bytesToHex(changed_digest_bytes, .lower);

    const parent: Manifest = .{
        .backup_id = "incremental",
        .table_id = 7,
        .table_name = "docs",
        .catalog_sha256 = &catalog_sha256,
        .representation = .native,
        .created_at_unix_ns = 1,
        .shards = &.{.{
            .group_id = 9,
            .start_key_base64 = "",
            .object_paths = &.{"catalog/table.json"},
            .capture_revision = 1,
            .checkpoint_revision = 1,
        }},
        .objects = &.{.{
            .logical_path = "catalog/table.json",
            .role = "catalog",
            .blob_sha256 = &catalog_sha256,
            .logical_size_bytes = catalog_bytes.len,
        }},
        .blobs = &.{.{
            .sha256 = &catalog_sha256,
            .logical_size_bytes = catalog_bytes.len,
            .stored_size_bytes = catalog_bytes.len,
        }},
    };
    const parent_encoded = try encodeManifestCanonicalAlloc(alloc, parent);
    defer alloc.free(parent_encoded);
    const parent_sha256 = try manifestDigestHexAlloc(alloc, parent_encoded);
    defer alloc.free(parent_sha256);

    var child_blobs = [_]BlobRef{
        .{ .sha256 = &catalog_sha256, .logical_size_bytes = catalog_bytes.len, .stored_size_bytes = catalog_bytes.len },
        .{ .sha256 = &changed_sha256, .logical_size_bytes = changed_bytes.len, .stored_size_bytes = changed_bytes.len },
    };
    std.mem.sort(BlobRef, &child_blobs, {}, struct {
        fn lessThan(_: void, lhs: BlobRef, rhs: BlobRef) bool {
            return std.mem.order(u8, lhs.sha256, rhs.sha256) == .lt;
        }
    }.lessThan);
    const child: Manifest = .{
        .backup_id = "incremental",
        .table_id = 7,
        .table_name = "docs",
        .catalog_sha256 = &catalog_sha256,
        .representation = .native,
        .mode = .delta,
        .parent_manifest_sha256 = parent_sha256,
        .created_at_unix_ns = 2,
        .shards = &.{.{
            .group_id = 9,
            .start_key_base64 = "",
            .object_paths = &.{"shards/9/segment"},
            .capture_revision = 2,
            .checkpoint_revision = 2,
        }},
        .objects = &.{
            .{ .logical_path = "catalog/table.json", .role = "catalog", .blob_sha256 = &catalog_sha256, .logical_size_bytes = catalog_bytes.len },
            .{ .logical_path = "shards/9/segment", .role = "native_file", .blob_sha256 = &changed_sha256, .logical_size_bytes = changed_bytes.len },
        },
        .blobs = &child_blobs,
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const source_root = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/incremental-source", .{tmp.sub_path});
    defer alloc.free(source_root);
    const repository_root = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/incremental-repository", .{tmp.sub_path});
    defer alloc.free(repository_root);
    const catalog_path = try std.fmt.allocPrint(alloc, "{s}/catalog/table.json", .{source_root});
    defer alloc.free(catalog_path);
    const changed_path = try std.fmt.allocPrint(alloc, "{s}/shards/9/segment", .{source_root});
    defer alloc.free(changed_path);
    try writeTestFile(io, catalog_path, catalog_bytes);
    try writeTestFile(io, changed_path, changed_bytes);
    try fs_paths.createDirPathPortable(io, repository_root);

    var filesystem = TestFilesystemBackend{ .alloc = alloc, .io = io, .root = repository_root };
    const backend = filesystem.backend();
    try uploadSnapshotBlobsFromDirectory(alloc, io, backend, parent, source_root);
    try std.testing.expectEqual(@as(usize, 1), filesystem.put_blob_calls);
    filesystem.put_blob_calls = 0;
    try uploadIncrementalSnapshotBlobsFromDirectory(
        alloc,
        io,
        backend,
        child,
        parent_sha256,
        parent,
        source_root,
    );
    try std.testing.expectEqual(@as(usize, 1), filesystem.put_blob_calls);
    const changed_repository_path = try blobPathAlloc(alloc, &changed_sha256);
    defer alloc.free(changed_repository_path);
    try std.testing.expect(try backend.contains(backend.context, changed_repository_path));
}

test "repository publishes resolves and materializes one complete deduplicated snapshot" {
    const alloc = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const source_root = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/source", .{tmp.sub_path});
    defer alloc.free(source_root);
    const repository_root = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/repository", .{tmp.sub_path});
    defer alloc.free(repository_root);
    try fs_paths.createDirPathPortable(io, source_root);
    try fs_paths.createDirPathPortable(io, repository_root);
    for ([_][]const u8{ "catalog/table.json", "shards/9/copy-a", "shards/9/copy-b" }) |logical_path| {
        const path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ source_root, logical_path });
        defer alloc.free(path);
        try writeTestFile(io, path, "same-bytes");
    }
    var digest_bytes: [Sha256.digest_length]u8 = undefined;
    Sha256.hash("same-bytes", &digest_bytes, .{});
    const digest = std.fmt.bytesToHex(digest_bytes, .lower);
    const manifest: Manifest = .{
        .backup_id = "daily",
        .table_id = 7,
        .table_name = "docs",
        .catalog_sha256 = &digest,
        .representation = .native,
        .created_at_unix_ns = 1,
        .shards = &.{.{
            .group_id = 9,
            .start_key_base64 = "",
            .object_paths = &.{ "shards/9/copy-a", "shards/9/copy-b" },
            .capture_revision = 1,
            .checkpoint_revision = 1,
        }},
        .objects = &.{
            .{ .logical_path = "catalog/table.json", .role = "catalog", .blob_sha256 = &digest, .logical_size_bytes = 10 },
            .{ .logical_path = "shards/9/copy-a", .role = "native_file", .blob_sha256 = &digest, .logical_size_bytes = 10 },
            .{ .logical_path = "shards/9/copy-b", .role = "native_file", .blob_sha256 = &digest, .logical_size_bytes = 10 },
        },
        .blobs = &.{.{ .sha256 = &digest, .logical_size_bytes = 10, .stored_size_bytes = 10 }},
    };
    var filesystem = TestFilesystemBackend{ .alloc = alloc, .io = io, .root = repository_root };
    const backend = filesystem.backend();
    try uploadSnapshotBlobsFromDirectory(alloc, io, backend, manifest, source_root);
    try std.testing.expectEqual(@as(usize, 1), filesystem.put_blob_calls);

    const encoded = try encodeManifestCanonicalAlloc(alloc, manifest);
    defer alloc.free(encoded);
    const manifest_digest = try manifestDigestHexAlloc(alloc, encoded);
    defer alloc.free(manifest_digest);
    const published_digest = try publishSnapshot(alloc, backend, manifest, .{ .next = .{
        .backup_id = "daily",
        .manifest_sha256 = manifest_digest,
        .generation = 1,
        .updated_at_unix_ns = 2,
    } });
    defer alloc.free(published_digest);

    var resolved = try resolveSnapshot(alloc, backend, "daily");
    defer resolved.deinit();
    try std.testing.expectEqualStrings(manifest_digest, resolved.ref.value.manifest_sha256);
    const restore_root = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/restore", .{tmp.sub_path});
    defer alloc.free(restore_root);
    try materializeSnapshotToStagingDirectory(alloc, io, backend, resolved.manifest.value, restore_root);
    try std.testing.expectEqual(@as(usize, 1), filesystem.materialize_blob_calls);
    for (manifest.objects) |object| {
        const path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ restore_root, object.logical_path });
        defer alloc.free(path);
        const restored = try std.Io.Dir.cwd().readFileAlloc(io, path, alloc, .limited(32));
        defer alloc.free(restored);
        try std.testing.expectEqualStrings("same-bytes", restored);
    }
}
