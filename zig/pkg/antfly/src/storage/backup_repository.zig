// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0.

//! Canonical backup repository contract.
//!
//! Backends expose three canonical namespaces:
//!   refs/<backup-id>              mutable atomic pointer
//!   manifests/<sha256>            immutable canonical JSON
//!   blobs/sha256/<sha256>         immutable bytes
//! plus fenced publication leases and a repository epoch/coordinator used to
//! serialize visibility transitions with garbage-collection deletion.
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

pub const manifest_schema_version: u32 = 2;
pub const ref_schema_version: u32 = 1;
pub const lease_schema_version: u32 = 3;
pub const completion_seal_schema_version: u32 = 1;
pub const max_blobs: usize = 1_000_000;
pub const max_shards: usize = 65_536;
pub const max_ref_name_bytes: usize = 128;
pub const max_manifest_bytes: usize = 64 * 1024 * 1024;

pub const BlobRef = struct {
    /// Digest of the plaintext logical content used by ObjectRef mappings.
    content_sha256: []const u8,
    /// Digest of the exact stored representation and therefore the physical
    /// object key. It deliberately differs from the content digest once
    /// compression or encryption is introduced.
    storage_sha256: []const u8,
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
    content_sha256: []const u8,
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
    /// The exact committed base whose completion proof was used to omit blob
    /// uploads. GC retains this proof while the candidate is being produced.
    base_manifest_sha256: ?[]const u8 = null,
    /// Non-zero owner token used to fence activation, renewal, publication,
    /// and release. A stale process can never act on a successor's lease.
    fencing_token: u64,
    expires_at_unix_ns: i128,
};

/// Immutable proof that every physical object named by a manifest was
/// verified before the snapshot became eligible for ref publication or use
/// as an incremental base. A manifest by itself is only publication intent.
pub const CompletionSeal = struct {
    schema_version: u32 = completion_seal_schema_version,
    manifest_sha256: []const u8,
    blob_count: u64,
    representation: bundle.Representation,
};

pub const VerifiedBlobReceipt = struct {
    content_sha256: []const u8,
    storage_sha256: []const u8,
    logical_size_bytes: u64,
    publication_fence: u64,
    /// Backend-owned identity for the exact verified object generation. Local
    /// adapters derive this from file identity; remote adapters derive it from
    /// the provider's version/ETag after byte verification.
    storage_identity: u64,
};

pub const CommittedSnapshot = struct {
    manifest_sha256: []const u8,
    manifest: Manifest,
    seal: CompletionSeal,

    pub fn validate(self: @This(), alloc: std.mem.Allocator) !void {
        try bundle.validateSha256(self.manifest_sha256);
        try validateManifest(self.manifest);
        const encoded = try encodeManifestCanonicalAlloc(alloc, self.manifest);
        defer alloc.free(encoded);
        const actual = try manifestDigestHexAlloc(alloc, encoded);
        defer alloc.free(actual);
        if (!std.mem.eql(u8, actual, self.manifest_sha256))
            return error.BackupBaseMismatch;
        try validateCompletionSeal(self.seal);
        if (!std.mem.eql(u8, self.seal.manifest_sha256, self.manifest_sha256) or
            self.seal.blob_count != self.manifest.blobs.len or
            self.seal.representation != self.manifest.representation)
            return error.BackupBaseMismatch;
    }
};

pub const SnapshotPlan = struct {
    manifest: Manifest,
    base: ?CommittedSnapshot = null,

    pub fn validate(self: @This(), alloc: std.mem.Allocator) !void {
        try validateManifest(self.manifest);
        switch (self.manifest.mode) {
            .full => if (self.base != null) return error.InvalidBackupLineage,
            .delta => {
                const base = self.base orelse return error.InvalidBackupLineage;
                try base.validate(alloc);
                const declared = self.manifest.parent_manifest_sha256 orelse
                    return error.InvalidBackupLineage;
                if (!std.mem.eql(u8, declared, base.manifest_sha256) or
                    self.manifest.table_id != base.manifest.table_id or
                    !std.mem.eql(u8, self.manifest.table_name, base.manifest.table_name) or
                    self.manifest.representation != base.manifest.representation)
                    return error.InvalidBackupLineage;
            },
        }
    }
};

pub const PublicationSession = struct {
    lease_id: []u8,
    manifest_sha256: []u8,
    fencing_token: u64,
    activated_epoch: u64,
    expires_at_unix_ns: i128,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.lease_id);
        alloc.free(self.manifest_sha256);
        self.* = undefined;
    }
};

/// Content-addressed objects may only be swept when the backend can keep the
/// deletion fence valid for the entire deletion, including an arbitrarily
/// paused owner. A local non-expiring file lock has that property. Renewable
/// object-store coordinator leases do not; those repositories remain
/// append-only until their catalog supports version-targeted two-phase delete.
pub const GarbageCollector = struct {
    /// The implementation must keep its reachability fence valid across the
    /// entire operation. It may perform a local conditional delete under a
    /// non-expiring lock or a durable catalog tombstone plus exact-version
    /// delete; a renewable time lease alone is insufficient.
    delete_unreachable: *const fn (
        context: *anyopaque,
        path: []const u8,
        cutoff_unix_ns: i128,
        expected_epoch: u64,
    ) anyerror!bool,
};

/// Storage adapters implement this contract for local filesystems and object
/// stores. Immutable puts must be create-if-absent (an existing byte-identical
/// object is success); refs must use the backend's atomic compare-and-swap or
/// conditional-write primitive. No backup is visible until `publish_ref`.
pub const Backend = struct {
    context: *anyopaque,
    /// Null means the immutable store is intentionally append-only.
    garbage_collector: ?GarbageCollector,
    put_immutable: *const fn (context: *anyopaque, path: []const u8, bytes: []const u8) anyerror!void,
    read_alloc: *const fn (context: *anyopaque, alloc: std.mem.Allocator, path: []const u8, limit: usize) anyerror![]u8,
    contains: *const fn (context: *anyopaque, path: []const u8) anyerror!bool,
    /// Repository epoch changes whenever a lease or ref changes. GC marks at
    /// one stable epoch and every deletion rechecks it under the backend's
    /// repository coordinator fence.
    read_epoch: *const fn (context: *anyopaque) anyerror!u64,
    activate_lease: *const fn (
        context: *anyopaque,
        path: []const u8,
        encoded: []const u8,
        fencing_token: u64,
    ) anyerror!u64,
    renew_lease: *const fn (
        context: *anyopaque,
        path: []const u8,
        fencing_token: u64,
        manifest_sha256: []const u8,
        now_unix_ns: i128,
        expires_at_unix_ns: i128,
    ) anyerror!u64,
    finalize_publication: *const fn (
        context: *anyopaque,
        lease_path: []const u8,
        fencing_token: u64,
        manifest_sha256: []const u8,
        now_unix_ns: i128,
        ref_path: []const u8,
        encoded_ref: []const u8,
        expected_manifest_sha256: ?[]const u8,
        expected_generation: ?u64,
    ) anyerror!u64,
    release_lease: *const fn (
        context: *anyopaque,
        lease_path: []const u8,
        fencing_token: u64,
    ) anyerror!u64,
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
        publication_fence: u64,
    ) anyerror!VerifiedBlobReceipt,
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

pub fn completionSealPathAlloc(alloc: std.mem.Allocator, sha256: []const u8) ![]u8 {
    try bundle.validateSha256(sha256);
    return try std.fmt.allocPrint(alloc, "seals/{s}", .{sha256});
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
pub const ParsedLease = std.json.Parsed(Lease);
pub const ParsedCompletionSeal = std.json.Parsed(CompletionSeal);

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

pub fn parseLeaseCanonical(
    alloc: std.mem.Allocator,
    encoded: []const u8,
    now_unix_ns: i128,
) !ParsedLease {
    if (encoded.len == 0 or encoded.len > 64 * 1024) return error.InvalidBackupLease;
    var parsed = std.json.parseFromSlice(Lease, alloc, encoded, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = false,
    }) catch return error.InvalidBackupLease;
    errdefer parsed.deinit();
    try validateLease(parsed.value, now_unix_ns);
    const canonical = try encodeLeaseCanonicalAlloc(alloc, parsed.value, now_unix_ns);
    defer alloc.free(canonical);
    if (!std.mem.eql(u8, canonical, encoded)) return error.NonCanonicalBackupLease;
    return parsed;
}

pub fn validateCompletionSeal(seal: CompletionSeal) !void {
    if (seal.schema_version != completion_seal_schema_version)
        return error.InvalidBackupCompletionSeal;
    try bundle.validateSha256(seal.manifest_sha256);
}

pub fn encodeCompletionSealCanonicalAlloc(
    alloc: std.mem.Allocator,
    seal: CompletionSeal,
) ![]u8 {
    try validateCompletionSeal(seal);
    return try std.json.Stringify.valueAlloc(alloc, seal, .{});
}

pub fn parseCompletionSealCanonical(
    alloc: std.mem.Allocator,
    encoded: []const u8,
) !ParsedCompletionSeal {
    if (encoded.len == 0 or encoded.len > 64 * 1024)
        return error.InvalidBackupCompletionSeal;
    var parsed = std.json.parseFromSlice(CompletionSeal, alloc, encoded, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = false,
    }) catch return error.InvalidBackupCompletionSeal;
    errdefer parsed.deinit();
    try validateCompletionSeal(parsed.value);
    const canonical = try encodeCompletionSealCanonicalAlloc(alloc, parsed.value);
    defer alloc.free(canonical);
    if (!std.mem.eql(u8, canonical, encoded))
        return error.NonCanonicalBackupCompletionSeal;
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
    const seal_path = try completionSealPathAlloc(alloc, parsed_ref.value.manifest_sha256);
    defer alloc.free(seal_path);
    const encoded_seal = try backend.read_alloc(backend.context, alloc, seal_path, 64 * 1024);
    defer alloc.free(encoded_seal);
    var seal = try parseCompletionSealCanonical(alloc, encoded_seal);
    defer seal.deinit();
    if (!std.mem.eql(u8, seal.value.manifest_sha256, parsed_ref.value.manifest_sha256) or
        seal.value.blob_count != parsed_manifest.value.blobs.len or
        seal.value.representation != parsed_manifest.value.representation)
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

/// Activates a durable, repository-epoch-fenced publication lease and then
/// installs its immutable candidate manifest before any blob is trusted or
/// reused. Retention-expanding control mutations advance the repository epoch
/// before installing the new root. A crash may therefore leave a lease whose
/// manifest is not present; reachability must abort and retry in that state.
pub fn beginPublication(
    alloc: std.mem.Allocator,
    backend: Backend,
    plan: SnapshotPlan,
    lease_id: []const u8,
    fencing_token: u64,
    now_unix_ns: i128,
    expires_at_unix_ns: i128,
) !PublicationSession {
    if (fencing_token == 0) return error.InvalidBackupLease;
    try plan.validate(alloc);
    const owned_lease_id = try alloc.dupe(u8, lease_id);
    errdefer alloc.free(owned_lease_id);
    const encoded_manifest = try encodeManifestCanonicalAlloc(alloc, plan.manifest);
    defer alloc.free(encoded_manifest);
    const digest = try manifestDigestHexAlloc(alloc, encoded_manifest);
    errdefer alloc.free(digest);
    const lease: Lease = .{
        .lease_id = lease_id,
        .manifest_sha256 = digest,
        .base_manifest_sha256 = if (plan.base) |base| base.manifest_sha256 else null,
        .fencing_token = fencing_token,
        .expires_at_unix_ns = expires_at_unix_ns,
    };
    const encoded_lease = try encodeLeaseCanonicalAlloc(alloc, lease, now_unix_ns);
    defer alloc.free(encoded_lease);
    const lease_path = try leasePathAlloc(alloc, lease_id);
    defer alloc.free(lease_path);
    const activated_epoch = try backend.activate_lease(
        backend.context,
        lease_path,
        encoded_lease,
        fencing_token,
    );
    errdefer _ = backend.release_lease(backend.context, lease_path, fencing_token) catch 0;
    const manifest_path = try manifestPathAlloc(alloc, digest);
    defer alloc.free(manifest_path);
    try backend.put_immutable(backend.context, manifest_path, encoded_manifest);
    if (plan.base) |base| try verifyCommittedBaseStored(alloc, backend, base);
    return .{
        .lease_id = owned_lease_id,
        .manifest_sha256 = digest,
        .fencing_token = fencing_token,
        .activated_epoch = activated_epoch,
        .expires_at_unix_ns = expires_at_unix_ns,
    };
}

/// Renews a publication lease using the same manifest-bound owner fence. The
/// backend performs the read/compare/write while holding its repository
/// coordinator, so a delayed worker cannot extend a replacement lease.
pub fn renewPublication(
    alloc: std.mem.Allocator,
    backend: Backend,
    session: *PublicationSession,
    now_unix_ns: i128,
    expires_at_unix_ns: i128,
) !u64 {
    if (expires_at_unix_ns <= now_unix_ns or
        expires_at_unix_ns <= session.expires_at_unix_ns)
        return error.InvalidBackupLease;
    const lease_path = try leasePathAlloc(alloc, session.lease_id);
    defer alloc.free(lease_path);
    const epoch = try backend.renew_lease(
        backend.context,
        lease_path,
        session.fencing_token,
        session.manifest_sha256,
        now_unix_ns,
        expires_at_unix_ns,
    );
    session.expires_at_unix_ns = expires_at_unix_ns;
    return epoch;
}

/// Atomically publishes a receipt-complete snapshot and releases its lease.
/// Inherited blobs are proven by the exact base plus the candidate-manifest
/// lease; only newly required blobs need backend verification receipts.
pub fn finishPublication(
    alloc: std.mem.Allocator,
    backend: Backend,
    plan: SnapshotPlan,
    session: PublicationSession,
    receipts: []const VerifiedBlobReceipt,
    publication: RefPublication,
    now_unix_ns: i128,
) !u64 {
    try plan.validate(alloc);
    if (now_unix_ns >= session.expires_at_unix_ns) return error.BackupPublicationLeaseExpired;
    const encoded_manifest = try encodeManifestCanonicalAlloc(alloc, plan.manifest);
    defer alloc.free(encoded_manifest);
    const digest = try manifestDigestHexAlloc(alloc, encoded_manifest);
    defer alloc.free(digest);
    if (!std.mem.eql(u8, digest, session.manifest_sha256) or
        !std.mem.eql(u8, publication.next.manifest_sha256, digest) or
        !std.mem.eql(u8, publication.next.backup_id, plan.manifest.backup_id))
        return error.InvalidBackupRef;
    const required = try requiredBlobIndicesAssumeValidatedAlloc(alloc, plan);
    defer alloc.free(required);
    if (receipts.len != required.len) return error.IncompleteBackupInventory;
    for (required, receipts) |blob_index, receipt| {
        const blob = plan.manifest.blobs[blob_index];
        if (!std.mem.eql(u8, receipt.content_sha256, blob.content_sha256) or
            !std.mem.eql(u8, receipt.storage_sha256, blob.storage_sha256) or
            receipt.logical_size_bytes != blob.logical_size_bytes or
            receipt.publication_fence != session.fencing_token or
            receipt.storage_identity == 0)
            return error.BackupArtifactIntegrityMismatch;
    }

    const seal: CompletionSeal = .{
        .manifest_sha256 = digest,
        .blob_count = @intCast(plan.manifest.blobs.len),
        .representation = plan.manifest.representation,
    };
    const encoded_seal = try encodeCompletionSealCanonicalAlloc(alloc, seal);
    defer alloc.free(encoded_seal);
    const seal_path = try completionSealPathAlloc(alloc, digest);
    defer alloc.free(seal_path);
    try backend.put_immutable(backend.context, seal_path, encoded_seal);

    const ref_path = try refPathAlloc(alloc, publication.next.backup_id);
    defer alloc.free(ref_path);
    const encoded_ref = try encodeRefCanonicalAlloc(alloc, publication.next);
    defer alloc.free(encoded_ref);
    const lease_path = try leasePathAlloc(alloc, session.lease_id);
    defer alloc.free(lease_path);
    return try backend.finalize_publication(
        backend.context,
        lease_path,
        session.fencing_token,
        digest,
        now_unix_ns,
        ref_path,
        encoded_ref,
        publication.expected_manifest_sha256,
        publication.expected_generation,
    );
}

pub fn abortPublication(
    alloc: std.mem.Allocator,
    backend: Backend,
    session: PublicationSession,
) !u64 {
    const lease_path = try leasePathAlloc(alloc, session.lease_id);
    defer alloc.free(lease_path);
    return try backend.release_lease(backend.context, lease_path, session.fencing_token);
}

/// Uploads each unique blob from a captured local generation. Object paths are
/// the authoritative mapping back to source files; duplicate objects select
/// one representative and therefore upload their shared content only once.
pub fn uploadSnapshotBlobsFromDirectory(
    alloc: std.mem.Allocator,
    io: std.Io,
    backend: Backend,
    plan: SnapshotPlan,
    session: PublicationSession,
    source_root: []const u8,
) ![]VerifiedBlobReceipt {
    try plan.validate(alloc);
    if (plan.manifest.encryption != null) return error.UnsupportedBackupEncryption;
    const required = try requiredBlobIndicesAssumeValidatedAlloc(alloc, plan);
    defer alloc.free(required);
    const receipts = try alloc.alloc(VerifiedBlobReceipt, required.len);
    errdefer alloc.free(receipts);
    for (required, 0..) |blob_index, receipt_index|
        receipts[receipt_index] = try uploadSnapshotBlobFromDirectory(
            alloc,
            io,
            backend,
            plan.manifest,
            source_root,
            blob_index,
            session.fencing_token,
        );
    return receipts;
}

fn uploadSnapshotBlobFromDirectory(
    alloc: std.mem.Allocator,
    io: std.Io,
    backend: Backend,
    manifest: Manifest,
    source_root: []const u8,
    blob_index: usize,
    publication_fence: u64,
) !VerifiedBlobReceipt {
    if (blob_index >= manifest.blobs.len) return error.InvalidBackupManifest;
    const blob = manifest.blobs[blob_index];
    if (blob.compression != .none or blob.encryption_key_id != null)
        return error.UnsupportedBackupCompression;
    const object = findObjectForBlob(manifest.objects, blob.content_sha256) orelse
        return error.IncompleteBackupInventory;
    const source_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ source_root, object.logical_path });
    defer alloc.free(source_path);
    const repository_path = try blobPathAlloc(alloc, blob.storage_sha256);
    defer alloc.free(repository_path);
    return try backend.put_blob_from_file(
        backend.context,
        io,
        repository_path,
        source_path,
        blob.logical_size_bytes,
        blob.content_sha256,
        publication_fence,
    );
}

fn verifyCommittedBaseStored(
    alloc: std.mem.Allocator,
    backend: Backend,
    base: CommittedSnapshot,
) !void {
    const path = try manifestPathAlloc(alloc, base.manifest_sha256);
    defer alloc.free(path);
    const stored = backend.read_alloc(
        backend.context,
        alloc,
        path,
        max_manifest_bytes,
    ) catch |err| switch (err) {
        error.FileNotFound => return error.BackupBaseRequired,
        else => return err,
    };
    defer alloc.free(stored);
    const expected = try encodeManifestCanonicalAlloc(alloc, base.manifest);
    defer alloc.free(expected);
    if (!std.mem.eql(u8, stored, expected)) return error.BackupBaseMismatch;
    const seal_path = try completionSealPathAlloc(alloc, base.manifest_sha256);
    defer alloc.free(seal_path);
    const stored_seal = backend.read_alloc(
        backend.context,
        alloc,
        seal_path,
        64 * 1024,
    ) catch |err| switch (err) {
        error.FileNotFound => return error.BackupBaseRequired,
        else => return err,
    };
    defer alloc.free(stored_seal);
    var parsed_seal = try parseCompletionSealCanonical(alloc, stored_seal);
    defer parsed_seal.deinit();
    if (parsed_seal.value.schema_version != base.seal.schema_version or
        !std.mem.eql(u8, parsed_seal.value.manifest_sha256, base.seal.manifest_sha256) or
        parsed_seal.value.blob_count != base.seal.blob_count or
        parsed_seal.value.representation != base.seal.representation)
        return error.BackupBaseMismatch;
}

fn requiredBlobIndicesAssumeValidatedAlloc(alloc: std.mem.Allocator, plan: SnapshotPlan) ![]usize {
    if (plan.base) |base| return try changedBlobIndicesAlloc(alloc, plan.manifest.blobs, base.manifest.blobs);
    const indices = try alloc.alloc(usize, plan.manifest.blobs.len);
    for (indices, 0..) |*index, i| index.* = i;
    return indices;
}

fn changedBlobIndicesAlloc(
    alloc: std.mem.Allocator,
    child_blobs: []const BlobRef,
    parent_blobs: []const BlobRef,
) ![]usize {
    var changed = std.ArrayListUnmanaged(usize).empty;
    errdefer changed.deinit(alloc);
    var parent_index: usize = 0;
    for (child_blobs, 0..) |blob, child_index| {
        while (parent_index < parent_blobs.len and
            std.mem.order(u8, parent_blobs[parent_index].content_sha256, blob.content_sha256) == .lt)
            parent_index += 1;
        if (parent_index == parent_blobs.len or
            !blobStorageIdentityEqual(parent_blobs[parent_index], blob))
            try changed.append(alloc, child_index);
    }
    return try changed.toOwnedSlice(alloc);
}

fn blobStorageIdentityEqual(lhs: BlobRef, rhs: BlobRef) bool {
    return std.mem.eql(u8, lhs.content_sha256, rhs.content_sha256) and
        std.mem.eql(u8, lhs.storage_sha256, rhs.storage_sha256) and
        lhs.logical_size_bytes == rhs.logical_size_bytes and
        lhs.stored_size_bytes == rhs.stored_size_bytes and
        lhs.compression == rhs.compression and
        optionalBytesEqual(lhs.encryption_key_id, rhs.encryption_key_id);
}

fn optionalBytesEqual(lhs: ?[]const u8, rhs: ?[]const u8) bool {
    if (lhs == null or rhs == null) return lhs == null and rhs == null;
    return std.mem.eql(u8, lhs.?, rhs.?);
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
        const repository_path = try blobPathAlloc(alloc, blob.storage_sha256);
        defer alloc.free(repository_path);
        const cached_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ blob_root, blob.content_sha256 });
        defer alloc.free(cached_path);
        try backend.materialize_blob_to_file(
            backend.context,
            io,
            repository_path,
            cached_path,
            blob.logical_size_bytes,
            blob.content_sha256,
        );
        try verifyFileDigest(io, cached_path, blob.logical_size_bytes, blob.content_sha256);
    }
    for (manifest.objects) |object| {
        const cached_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ blob_root, object.content_sha256 });
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
        if (std.mem.eql(u8, object.content_sha256, digest)) return object;
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
        try bundle.validateSha256(blob.content_sha256);
        try bundle.validateSha256(blob.storage_sha256);
        if (blob.compression == .none and blob.logical_size_bytes != blob.stored_size_bytes)
            return error.InvalidBackupManifest;
        if (blob.compression == .none and blob.encryption_key_id == null and
            !std.mem.eql(u8, blob.content_sha256, blob.storage_sha256))
            return error.InvalidBackupManifest;
        if (blob.compression != manifest.compression)
            return error.InvalidBackupManifest;
        if ((blob.encryption_key_id != null) != (manifest.encryption != null))
            return error.InvalidBackupManifest;
        if (manifest.encryption) |encryption| {
            if (!std.mem.eql(u8, blob.encryption_key_id.?, encryption.key_id))
                return error.InvalidBackupManifest;
        }
        if (previous_digest) |previous| {
            if (std.mem.order(u8, previous, blob.content_sha256) != .lt)
                return error.NonCanonicalBackupManifest;
        }
        previous_digest = blob.content_sha256;
    }
    if (!containsBlob(manifest.blobs, manifest.catalog_sha256))
        return error.IncompleteBackupInventory;

    var previous_path: ?[]const u8 = null;
    var catalog_object_found = false;
    for (manifest.objects) |object| {
        try bundle.validateRelativePath(object.logical_path);
        if (object.role.len == 0 or object.role.len > 128)
            return error.InvalidBackupManifest;
        try bundle.validateSha256(object.content_sha256);
        const blob = findBlob(manifest.blobs, object.content_sha256) orelse
            return error.IncompleteBackupInventory;
        if (blob.logical_size_bytes != object.logical_size_bytes)
            return error.InvalidBackupManifest;
        if (previous_path) |previous| {
            if (std.mem.order(u8, previous, object.logical_path) != .lt)
                return error.NonCanonicalBackupManifest;
        }
        previous_path = object.logical_path;
        if (std.mem.eql(u8, object.role, "catalog") and
            std.mem.eql(u8, object.content_sha256, manifest.catalog_sha256))
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

    return try changedBlobIndicesAlloc(alloc, child.blobs, parent.blobs);
}

fn containsBlob(blobs: []const BlobRef, digest: []const u8) bool {
    return findBlob(blobs, digest) != null;
}

fn findBlob(blobs: []const BlobRef, digest: []const u8) ?*const BlobRef {
    var low: usize = 0;
    var high: usize = blobs.len;
    while (low < high) {
        const mid = low + (high - low) / 2;
        switch (std.mem.order(u8, blobs[mid].content_sha256, digest)) {
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
    if (lease.schema_version != lease_schema_version or lease.fencing_token == 0 or
        lease.expires_at_unix_ns <= now_unix_ns)
        return error.InvalidBackupLease;
    try validateBackupId(lease.lease_id);
    try bundle.validateSha256(lease.manifest_sha256);
    if (lease.base_manifest_sha256) |digest| {
        try bundle.validateSha256(digest);
        if (std.mem.eql(u8, digest, lease.manifest_sha256))
            return error.InvalidBackupLease;
    }
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

/// In-memory result used by backend GC implementations after walking refs and
/// active leases. Deletion still requires a backend-specific
/// grace-period check against object creation time.
pub const Reachability = struct {
    repository_epoch: u64 = 0,
    manifests: std.StringHashMapUnmanaged(void) = .empty,
    seals: std.StringHashMapUnmanaged(void) = .empty,
    blobs: std.StringHashMapUnmanaged(void) = .empty,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        var manifest_it = self.manifests.keyIterator();
        while (manifest_it.next()) |key| alloc.free(key.*);
        var blob_it = self.blobs.keyIterator();
        while (blob_it.next()) |key| alloc.free(key.*);
        var seal_it = self.seals.keyIterator();
        while (seal_it.next()) |key| alloc.free(key.*);
        self.manifests.deinit(alloc);
        self.seals.deinit(alloc);
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
        if (!self.seals.contains(digest)) {
            const owned_seal = try alloc.dupe(u8, digest);
            errdefer alloc.free(owned_seal);
            try self.seals.put(alloc, owned_seal, {});
        }
        try validateManifest(manifest);
        for (manifest.blobs) |blob| {
            if (self.blobs.contains(blob.storage_sha256)) continue;
            const owned = try alloc.dupe(u8, blob.storage_sha256);
            errdefer alloc.free(owned);
            try self.blobs.put(alloc, owned, {});
        }
    }
};

fn appendOwnedDigest(
    alloc: std.mem.Allocator,
    list: *std.ArrayListUnmanaged([]u8),
    digest: []const u8,
) !void {
    const owned = try alloc.dupe(u8, digest);
    errdefer alloc.free(owned);
    try list.append(alloc, owned);
}

/// Marks complete inventories reachable from live refs and unexpired leases.
/// Parent links are informational lineage only: because each manifest is a
/// complete materialized inventory, following ancestry would retain an
/// unbounded chain. An active delta lease separately roots its exact base
/// manifest and completion proof while base validation is in flight. The
/// caller reads
/// `expected_epoch` before enumerating refs and leases; the checks bracketing
/// traversal reject any namespace transition that raced that enumeration.
pub fn buildReachability(
    alloc: std.mem.Allocator,
    backend: Backend,
    refs: []const Ref,
    leases: []const Lease,
    now_unix_ns: i128,
    expected_epoch: u64,
) !Reachability {
    if (try backend.read_epoch(backend.context) != expected_epoch)
        return error.BackupRepositoryEpochChanged;
    var result: Reachability = .{ .repository_epoch = expected_epoch };
    errdefer result.deinit(alloc);
    var pending = std.ArrayListUnmanaged([]u8).empty;
    var base_proofs = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (pending.items) |digest| alloc.free(digest);
        pending.deinit(alloc);
        for (base_proofs.items) |digest| alloc.free(digest);
        base_proofs.deinit(alloc);
    }
    for (refs) |ref| {
        try validateRef(ref);
        try appendOwnedDigest(alloc, &pending, ref.manifest_sha256);
    }
    for (leases) |lease| {
        validateLease(lease, now_unix_ns) catch |err| switch (err) {
            error.InvalidBackupLease => continue,
            else => return err,
        };
        try appendOwnedDigest(alloc, &pending, lease.manifest_sha256);
        if (lease.base_manifest_sha256) |base_digest|
            try appendOwnedDigest(alloc, &base_proofs, base_digest);
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
        try result.markSnapshot(alloc, owned_digest, parsed.value);
    }
    for (base_proofs.items) |base_digest| {
        _ = try result.markManifest(alloc, base_digest);
        if (!result.seals.contains(base_digest)) {
            const owned_seal = try alloc.dupe(u8, base_digest);
            errdefer alloc.free(owned_seal);
            try result.seals.put(alloc, owned_seal, {});
        }
    }
    if (try backend.read_epoch(backend.context) != expected_epoch)
        return error.BackupRepositoryEpochChanged;
    return result;
}

pub const GarbageCandidate = struct {
    kind: enum { manifest, seal, blob },
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
    const collector = backend.garbage_collector orelse
        return error.BackupRepositoryGcUnsupported;
    var deleted: usize = 0;
    for (candidates) |candidate| {
        try bundle.validateSha256(candidate.sha256);
        const retained = switch (candidate.kind) {
            .manifest => reachable.manifests.contains(candidate.sha256),
            .seal => reachable.seals.contains(candidate.sha256),
            .blob => reachable.blobs.contains(candidate.sha256),
        };
        if (retained) continue;
        const path = switch (candidate.kind) {
            .manifest => try manifestPathAlloc(alloc, candidate.sha256),
            .seal => try completionSealPathAlloc(alloc, candidate.sha256),
            .blob => try blobPathAlloc(alloc, candidate.sha256),
        };
        defer alloc.free(path);
        if (try collector.delete_unreachable(
            backend.context,
            path,
            grace_cutoff_unix_ns,
            reachable.repository_epoch,
        ))
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
            .{ .logical_path = "catalog/table.json", .role = "catalog", .content_sha256 = digest_a, .logical_size_bytes = 10 },
            .{ .logical_path = "shards/9/dense/segment-1", .role = "dense_projection", .content_sha256 = digest_b, .logical_size_bytes = 20 },
        },
        .blobs = &.{
            .{ .content_sha256 = digest_a, .storage_sha256 = digest_a, .logical_size_bytes = 10, .stored_size_bytes = 10 },
            .{ .content_sha256 = digest_b, .storage_sha256 = digest_b, .logical_size_bytes = 20, .stored_size_bytes = 20 },
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
            .{ .logical_path = "catalog/table.json", .role = "catalog", .content_sha256 = digest, .logical_size_bytes = 10 },
            .{ .logical_path = "shards/9/copy-a", .role = "native_file", .content_sha256 = digest, .logical_size_bytes = 10 },
            .{ .logical_path = "shards/9/copy-b", .role = "native_file", .content_sha256 = digest, .logical_size_bytes = 10 },
        },
        .blobs = &.{.{ .content_sha256 = digest, .storage_sha256 = digest, .logical_size_bytes = 10, .stored_size_bytes = 10 }},
    };
    try validateManifest(manifest);
    try std.testing.expectEqual(@as(usize, 3), manifest.objects.len);
    try std.testing.expectEqual(@as(usize, 1), manifest.blobs.len);

    var invalid = manifest;
    invalid.objects = &.{.{
        .logical_path = "shards/../escape",
        .role = "catalog",
        .content_sha256 = digest,
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
        .objects = &.{.{ .logical_path = "catalog/table.json", .role = "catalog", .content_sha256 = digest_a, .logical_size_bytes = 10 }},
        .blobs = &.{.{ .content_sha256 = digest_a, .storage_sha256 = digest_a, .logical_size_bytes = 10, .stored_size_bytes = 10 }},
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
            .{ .logical_path = "catalog/table.json", .role = "catalog", .content_sha256 = digest_a, .logical_size_bytes = 10 },
            .{ .logical_path = "shards/9/store.bin", .role = "primary", .content_sha256 = digest_child, .logical_size_bytes = 20 },
        },
        .blobs = &.{
            .{ .content_sha256 = digest_a, .storage_sha256 = digest_a, .logical_size_bytes = 10, .stored_size_bytes = 10 },
            .{ .content_sha256 = digest_child, .storage_sha256 = digest_child, .logical_size_bytes = 20, .stored_size_bytes = 20 },
        },
    };
    const changed = try incrementalBlobIndicesAlloc(std.testing.allocator, child, parent_sha256, parent);
    defer std.testing.allocator.free(changed);
    try std.testing.expectEqualSlices(usize, &.{1}, changed);

    const reencoded = [_]BlobRef{.{
        .content_sha256 = digest_a,
        .storage_sha256 = digest_child,
        .logical_size_bytes = 10,
        .stored_size_bytes = 8,
        .compression = .zstd,
    }};
    const representation_changed = try changedBlobIndicesAlloc(
        std.testing.allocator,
        &reencoded,
        parent.blobs,
    );
    defer std.testing.allocator.free(representation_changed);
    try std.testing.expectEqualSlices(usize, &.{0}, representation_changed);
}

const TestFilesystemBackend = struct {
    alloc: std.mem.Allocator,
    io: std.Io,
    root: []const u8,
    put_blob_calls: usize = 0,
    materialize_blob_calls: usize = 0,
    contains_calls: usize = 0,
    epoch: u64 = 1,
    active_lease_fence: u64 = 0,
    require_active_lease_for_manifest: bool = false,
    fail_next_lease_write: bool = false,
    delete_calls: usize = 0,

    fn backend(self: *@This()) Backend {
        return .{
            .context = self,
            .garbage_collector = .{ .delete_unreachable = deleteIfOlderThan },
            .put_immutable = putImmutable,
            .read_alloc = readAlloc,
            .contains = contains,
            .read_epoch = readEpoch,
            .activate_lease = activateLease,
            .renew_lease = renewLease,
            .finalize_publication = finalizePublication,
            .release_lease = releaseLease,
            .put_blob_from_file = putBlobFromFile,
            .materialize_blob_to_file = materializeBlobToFile,
        };
    }

    fn pathAlloc(self: *@This(), suffix: []const u8) ![]u8 {
        return try std.fmt.allocPrint(self.alloc, "{s}/{s}", .{ self.root, suffix });
    }

    fn putImmutable(context: *anyopaque, path: []const u8, bytes: []const u8) !void {
        const self: *@This() = @ptrCast(@alignCast(context));
        if (self.require_active_lease_for_manifest and
            std.mem.startsWith(u8, path, "manifests/") and
            self.active_lease_fence == 0)
            return error.BackupManifestPublishedWithoutLease;
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
        self.contains_calls += 1;
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

    fn readEpoch(context: *anyopaque) !u64 {
        const self: *@This() = @ptrCast(@alignCast(context));
        return self.epoch;
    }

    fn activateLease(
        context: *anyopaque,
        path: []const u8,
        encoded: []const u8,
        fencing_token: u64,
    ) !u64 {
        const self: *@This() = @ptrCast(@alignCast(context));
        if (fencing_token == 0 or self.active_lease_fence != 0)
            return error.BackupRepositoryBusy;
        // Retention expansion is ordered epoch first. Failure after this
        // point is conservative: an in-flight mark restarts even though the
        // new root was not installed.
        self.epoch +|= 1;
        if (self.fail_next_lease_write) {
            self.fail_next_lease_write = false;
            return error.InjectedLeaseWriteFailure;
        }
        const absolute = try self.pathAlloc(path);
        defer self.alloc.free(absolute);
        if (std.fs.path.dirname(absolute)) |parent| try fs_paths.createDirPathPortable(self.io, parent);
        try writeTestFile(self.io, absolute, encoded);
        self.active_lease_fence = fencing_token;
        return self.epoch;
    }

    fn finalizePublication(
        context: *anyopaque,
        lease_path: []const u8,
        fencing_token: u64,
        manifest_sha256: []const u8,
        now_unix_ns: i128,
        ref_path: []const u8,
        encoded_ref: []const u8,
        expected_manifest_sha256: ?[]const u8,
        expected_generation: ?u64,
    ) !u64 {
        const self: *@This() = @ptrCast(@alignCast(context));
        var next_ref = try parseRefCanonical(self.alloc, encoded_ref);
        defer next_ref.deinit();
        if (!std.mem.eql(u8, next_ref.value.manifest_sha256, manifest_sha256))
            return error.BackupPublicationFenced;
        if (self.active_lease_fence != fencing_token) return error.BackupPublicationFenced;
        const absolute_lease = try self.pathAlloc(lease_path);
        defer self.alloc.free(absolute_lease);
        const encoded_lease = try std.Io.Dir.cwd().readFileAlloc(
            self.io,
            absolute_lease,
            self.alloc,
            .limited(64 * 1024),
        );
        defer self.alloc.free(encoded_lease);
        var parsed = try std.json.parseFromSlice(Lease, self.alloc, encoded_lease, .{
            .allocate = .alloc_always,
            .ignore_unknown_fields = false,
        });
        defer parsed.deinit();
        try validateLease(parsed.value, now_unix_ns);
        if (parsed.value.fencing_token != fencing_token or
            !std.mem.eql(u8, parsed.value.manifest_sha256, manifest_sha256))
            return error.BackupPublicationFenced;
        // Bump before the visible ref transition so a crash can only cause a
        // conservative GC restart, never stale deletion.
        self.epoch +|= 1;
        try publishRef(
            context,
            ref_path,
            encoded_ref,
            expected_manifest_sha256,
            expected_generation,
        );
        std.Io.Dir.cwd().deleteFile(self.io, absolute_lease) catch {};
        self.active_lease_fence = 0;
        return self.epoch;
    }

    fn renewLease(
        context: *anyopaque,
        lease_path: []const u8,
        fencing_token: u64,
        manifest_sha256: []const u8,
        now_unix_ns: i128,
        expires_at_unix_ns: i128,
    ) !u64 {
        const self: *@This() = @ptrCast(@alignCast(context));
        if (self.active_lease_fence != fencing_token) return error.BackupPublicationFenced;
        const absolute = try self.pathAlloc(lease_path);
        defer self.alloc.free(absolute);
        const encoded = try std.Io.Dir.cwd().readFileAlloc(
            self.io,
            absolute,
            self.alloc,
            .limited(64 * 1024),
        );
        defer self.alloc.free(encoded);
        var parsed = try parseLeaseCanonical(self.alloc, encoded, now_unix_ns);
        defer parsed.deinit();
        if (parsed.value.fencing_token != fencing_token or
            !std.mem.eql(u8, parsed.value.manifest_sha256, manifest_sha256))
            return error.BackupPublicationFenced;
        if (expires_at_unix_ns <= parsed.value.expires_at_unix_ns)
            return error.InvalidBackupLease;
        const renewed: Lease = .{
            .lease_id = parsed.value.lease_id,
            .manifest_sha256 = parsed.value.manifest_sha256,
            .base_manifest_sha256 = parsed.value.base_manifest_sha256,
            .fencing_token = fencing_token,
            .expires_at_unix_ns = expires_at_unix_ns,
        };
        const renewed_encoded = try encodeLeaseCanonicalAlloc(self.alloc, renewed, now_unix_ns);
        defer self.alloc.free(renewed_encoded);
        // A longer lease expands retention and must invalidate an older mark
        // before the extension can become observable.
        self.epoch +|= 1;
        if (self.fail_next_lease_write) {
            self.fail_next_lease_write = false;
            return error.InjectedLeaseWriteFailure;
        }
        try writeTestFile(self.io, absolute, renewed_encoded);
        return self.epoch;
    }

    fn releaseLease(context: *anyopaque, lease_path: []const u8, fencing_token: u64) !u64 {
        const self: *@This() = @ptrCast(@alignCast(context));
        if (self.active_lease_fence != fencing_token) return error.BackupPublicationFenced;
        self.epoch +|= 1;
        const absolute = try self.pathAlloc(lease_path);
        defer self.alloc.free(absolute);
        std.Io.Dir.cwd().deleteFile(self.io, absolute) catch |err| switch (err) {
            error.FileNotFound => {},
            else => return err,
        };
        self.active_lease_fence = 0;
        return self.epoch;
    }

    fn deleteIfOlderThan(context: *anyopaque, _: []const u8, _: i128, expected_epoch: u64) !bool {
        const self: *@This() = @ptrCast(@alignCast(context));
        self.delete_calls += 1;
        if (self.epoch != expected_epoch) return error.BackupRepositoryEpochChanged;
        return false;
    }

    fn putBlobFromFile(
        context: *anyopaque,
        io: std.Io,
        path: []const u8,
        source_path: []const u8,
        logical_size_bytes: u64,
        sha256: []const u8,
        publication_fence: u64,
    ) !VerifiedBlobReceipt {
        const self: *@This() = @ptrCast(@alignCast(context));
        self.put_blob_calls += 1;
        const absolute = try self.pathAlloc(path);
        defer self.alloc.free(absolute);
        if (try contains(context, path)) {
            try verifyFileDigest(io, absolute, logical_size_bytes, sha256);
        } else {
            if (std.fs.path.dirname(absolute)) |parent| try fs_paths.createDirPathPortable(io, parent);
            try copyFileBounded(io, source_path, absolute, logical_size_bytes);
            try verifyFileDigest(io, absolute, logical_size_bytes, sha256);
        }
        var identity = std.hash.Wyhash.hash(logical_size_bytes, sha256);
        if (identity == 0) identity = 1;
        return .{
            .content_sha256 = sha256,
            .storage_sha256 = sha256,
            .logical_size_bytes = logical_size_bytes,
            .publication_fence = publication_fence,
            .storage_identity = identity,
        };
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
            .content_sha256 = &catalog_sha256,
            .logical_size_bytes = catalog_bytes.len,
        }},
        .blobs = &.{.{
            .content_sha256 = &catalog_sha256,
            .storage_sha256 = &catalog_sha256,
            .logical_size_bytes = catalog_bytes.len,
            .stored_size_bytes = catalog_bytes.len,
        }},
    };
    const parent_encoded = try encodeManifestCanonicalAlloc(alloc, parent);
    defer alloc.free(parent_encoded);
    const parent_sha256 = try manifestDigestHexAlloc(alloc, parent_encoded);
    defer alloc.free(parent_sha256);

    var child_blobs = [_]BlobRef{
        .{ .content_sha256 = &catalog_sha256, .storage_sha256 = &catalog_sha256, .logical_size_bytes = catalog_bytes.len, .stored_size_bytes = catalog_bytes.len },
        .{ .content_sha256 = &changed_sha256, .storage_sha256 = &changed_sha256, .logical_size_bytes = changed_bytes.len, .stored_size_bytes = changed_bytes.len },
    };
    std.mem.sort(BlobRef, &child_blobs, {}, struct {
        fn lessThan(_: void, lhs: BlobRef, rhs: BlobRef) bool {
            return std.mem.order(u8, lhs.content_sha256, rhs.content_sha256) == .lt;
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
            .{ .logical_path = "catalog/table.json", .role = "catalog", .content_sha256 = &catalog_sha256, .logical_size_bytes = catalog_bytes.len },
            .{ .logical_path = "shards/9/segment", .role = "native_file", .content_sha256 = &changed_sha256, .logical_size_bytes = changed_bytes.len },
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
    const parent_seal: CompletionSeal = .{
        .manifest_sha256 = parent_sha256,
        .blob_count = parent.blobs.len,
        .representation = parent.representation,
    };
    const child_plan: SnapshotPlan = .{
        .manifest = child,
        .base = .{ .manifest_sha256 = parent_sha256, .manifest = parent, .seal = parent_seal },
    };
    try std.testing.expectError(
        error.BackupBaseRequired,
        beginPublication(alloc, backend, child_plan, "orphan-child", 10, 0, 100),
    );
    const parent_plan: SnapshotPlan = .{ .manifest = parent };
    var parent_session = try beginPublication(alloc, backend, parent_plan, "parent-upload", 11, 0, 100);
    defer parent_session.deinit(alloc);
    const parent_receipts = try uploadSnapshotBlobsFromDirectory(
        alloc,
        io,
        backend,
        parent_plan,
        parent_session,
        source_root,
    );
    defer alloc.free(parent_receipts);
    try std.testing.expectEqual(@as(usize, 1), filesystem.put_blob_calls);
    // A fully uploaded but aborted candidate still has no completion seal and
    // therefore cannot be used to omit inherited blobs.
    _ = try abortPublication(alloc, backend, parent_session);
    try std.testing.expectError(
        error.BackupBaseRequired,
        beginPublication(alloc, backend, child_plan, "unsealed-child", 13, 0, 100),
    );
    var committed_parent_session = try beginPublication(
        alloc,
        backend,
        parent_plan,
        "parent-commit",
        14,
        0,
        100,
    );
    defer committed_parent_session.deinit(alloc);
    const committed_parent_receipts = try uploadSnapshotBlobsFromDirectory(
        alloc,
        io,
        backend,
        parent_plan,
        committed_parent_session,
        source_root,
    );
    defer alloc.free(committed_parent_receipts);
    _ = try finishPublication(alloc, backend, parent_plan, committed_parent_session, committed_parent_receipts, .{ .next = .{
        .backup_id = "incremental",
        .manifest_sha256 = committed_parent_session.manifest_sha256,
        .generation = 1,
        .updated_at_unix_ns = 1,
    } }, 1);
    filesystem.put_blob_calls = 0;
    var child_session = try beginPublication(alloc, backend, child_plan, "child-upload", 12, 0, 100);
    defer child_session.deinit(alloc);
    const child_receipts = try uploadSnapshotBlobsFromDirectory(
        alloc,
        io,
        backend,
        child_plan,
        child_session,
        source_root,
    );
    defer alloc.free(child_receipts);
    try std.testing.expectEqual(@as(usize, 1), filesystem.put_blob_calls);
    const contains_before_finalize = filesystem.contains_calls;
    _ = try finishPublication(alloc, backend, child_plan, child_session, child_receipts, .{ .next = .{
        .backup_id = "incremental",
        .manifest_sha256 = child_session.manifest_sha256,
        .generation = 2,
        .updated_at_unix_ns = 2,
    }, .expected_manifest_sha256 = committed_parent_session.manifest_sha256, .expected_generation = 1 }, 2);
    // Publication consumes the verified receipts. It must not issue one
    // existence request per blob in the materialized child inventory.
    try std.testing.expectEqual(contains_before_finalize, filesystem.contains_calls);
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
            .{ .logical_path = "catalog/table.json", .role = "catalog", .content_sha256 = &digest, .logical_size_bytes = 10 },
            .{ .logical_path = "shards/9/copy-a", .role = "native_file", .content_sha256 = &digest, .logical_size_bytes = 10 },
            .{ .logical_path = "shards/9/copy-b", .role = "native_file", .content_sha256 = &digest, .logical_size_bytes = 10 },
        },
        .blobs = &.{.{ .content_sha256 = &digest, .storage_sha256 = &digest, .logical_size_bytes = 10, .stored_size_bytes = 10 }},
    };
    var filesystem = TestFilesystemBackend{
        .alloc = alloc,
        .io = io,
        .root = repository_root,
        .require_active_lease_for_manifest = true,
    };
    const backend = filesystem.backend();
    const plan: SnapshotPlan = .{ .manifest = manifest };
    const epoch_before_failed_activation = filesystem.epoch;
    filesystem.fail_next_lease_write = true;
    try std.testing.expectError(
        error.InjectedLeaseWriteFailure,
        beginPublication(alloc, backend, plan, "failed-activation", 20, 0, 100),
    );
    // A failed retention-expanding write still invalidates a concurrent mark,
    // and beginPublication never exposes the manifest before that transition.
    try std.testing.expectEqual(epoch_before_failed_activation + 1, filesystem.epoch);
    try std.testing.expectEqual(@as(u64, 0), filesystem.active_lease_fence);
    var session = try beginPublication(alloc, backend, plan, "daily-publication", 21, 0, 100);
    defer session.deinit(alloc);
    const receipts = try uploadSnapshotBlobsFromDirectory(
        alloc,
        io,
        backend,
        plan,
        session,
        source_root,
    );
    defer alloc.free(receipts);
    try std.testing.expectEqual(@as(usize, 1), filesystem.put_blob_calls);
    try std.testing.expectError(
        error.InvalidBackupLease,
        renewPublication(alloc, backend, &session, 1, 100),
    );
    const renewal_epoch = try renewPublication(alloc, backend, &session, 1, 200);
    try std.testing.expectEqual(@as(i128, 200), session.expires_at_unix_ns);
    try std.testing.expect(renewal_epoch > session.activated_epoch);
    const epoch_before_failed_renewal = filesystem.epoch;
    filesystem.fail_next_lease_write = true;
    try std.testing.expectError(
        error.InjectedLeaseWriteFailure,
        renewPublication(alloc, backend, &session, 2, 300),
    );
    // The epoch is durable before the extension is attempted, while the
    // caller's session and stored lease retain their prior expiry on failure.
    try std.testing.expectEqual(epoch_before_failed_renewal + 1, filesystem.epoch);
    try std.testing.expectEqual(@as(i128, 200), session.expires_at_unix_ns);

    const lease_path = try leasePathAlloc(alloc, session.lease_id);
    defer alloc.free(lease_path);
    const ref_path = try refPathAlloc(alloc, "daily");
    defer alloc.free(ref_path);
    const encoded_ref = try encodeRefCanonicalAlloc(alloc, .{
        .backup_id = "daily",
        .manifest_sha256 = session.manifest_sha256,
        .generation = 1,
        .updated_at_unix_ns = 2,
    });
    defer alloc.free(encoded_ref);
    try std.testing.expectError(error.BackupPublicationFenced, backend.finalize_publication(
        backend.context,
        lease_path,
        session.fencing_token,
        "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        2,
        ref_path,
        encoded_ref,
        null,
        null,
    ));
    const wrong_ref = try encodeRefCanonicalAlloc(alloc, .{
        .backup_id = "daily",
        .manifest_sha256 = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        .generation = 1,
        .updated_at_unix_ns = 2,
    });
    defer alloc.free(wrong_ref);
    try std.testing.expectError(error.BackupPublicationFenced, backend.finalize_publication(
        backend.context,
        lease_path,
        session.fencing_token,
        session.manifest_sha256,
        2,
        ref_path,
        wrong_ref,
        null,
        null,
    ));

    var forged_receipt = receipts[0];
    forged_receipt.publication_fence += 1;
    try std.testing.expectError(
        error.BackupArtifactIntegrityMismatch,
        finishPublication(alloc, backend, plan, session, &.{forged_receipt}, .{ .next = .{
            .backup_id = "daily",
            .manifest_sha256 = session.manifest_sha256,
            .generation = 1,
            .updated_at_unix_ns = 2,
        } }, 2),
    );

    const contains_before_finalize = filesystem.contains_calls;
    const publication_epoch = try finishPublication(alloc, backend, plan, session, receipts, .{ .next = .{
        .backup_id = "daily",
        .manifest_sha256 = session.manifest_sha256,
        .generation = 1,
        .updated_at_unix_ns = 2,
    } }, 2);
    try std.testing.expect(publication_epoch > session.activated_epoch);
    // Finalization proves changed blobs from receipts and the active manifest
    // lease; it must not regress to one remote HEAD per inventory entry.
    try std.testing.expectEqual(contains_before_finalize, filesystem.contains_calls);

    var resolved = try resolveSnapshot(alloc, backend, "daily");
    defer resolved.deinit();
    try std.testing.expectEqualStrings(session.manifest_sha256, resolved.ref.value.manifest_sha256);
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

test "repository epoch fences GC and active publication leases retain candidates" {
    const alloc = std.testing.allocator;
    const io = std.testing.io;
    const digest = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    const manifest: Manifest = .{
        .backup_id = "gc-race",
        .table_id = 7,
        .table_name = "docs",
        .catalog_sha256 = digest,
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
            .content_sha256 = digest,
            .logical_size_bytes = 10,
        }},
        .blobs = &.{.{
            .content_sha256 = digest,
            .storage_sha256 = digest,
            .logical_size_bytes = 10,
            .stored_size_bytes = 10,
        }},
    };
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const repository_root = try std.fmt.allocPrint(
        alloc,
        ".zig-cache/tmp/{s}/gc-repository",
        .{tmp.sub_path},
    );
    defer alloc.free(repository_root);
    try fs_paths.createDirPathPortable(io, repository_root);
    var filesystem = TestFilesystemBackend{ .alloc = alloc, .io = io, .root = repository_root };
    const backend = filesystem.backend();
    const old_epoch = try backend.read_epoch(backend.context);
    const parent_encoded = try encodeManifestCanonicalAlloc(alloc, manifest);
    defer alloc.free(parent_encoded);
    const parent_digest = try manifestDigestHexAlloc(alloc, parent_encoded);
    defer alloc.free(parent_digest);
    const parent_path = try manifestPathAlloc(alloc, parent_digest);
    defer alloc.free(parent_path);
    try backend.put_immutable(backend.context, parent_path, parent_encoded);
    var lineage_child = manifest;
    lineage_child.mode = .delta;
    lineage_child.parent_manifest_sha256 = parent_digest;
    lineage_child.created_at_unix_ns = 2;
    const child_encoded = try encodeManifestCanonicalAlloc(alloc, lineage_child);
    defer alloc.free(child_encoded);
    const child_digest = try manifestDigestHexAlloc(alloc, child_encoded);
    defer alloc.free(child_digest);
    const child_path = try manifestPathAlloc(alloc, child_digest);
    defer alloc.free(child_path);
    try backend.put_immutable(backend.context, child_path, child_encoded);
    var lineage_reachability = try buildReachability(
        alloc,
        backend,
        &.{.{
            .backup_id = "gc-race",
            .manifest_sha256 = child_digest,
            .generation = 1,
            .updated_at_unix_ns = 2,
        }},
        &.{},
        2,
        old_epoch,
    );
    defer lineage_reachability.deinit(alloc);
    try std.testing.expect(lineage_reachability.manifests.contains(child_digest));
    try std.testing.expect(lineage_reachability.seals.contains(child_digest));
    try std.testing.expect(!lineage_reachability.manifests.contains(parent_digest));
    try std.testing.expect(lineage_reachability.blobs.contains(digest));

    var stale_reachability: Reachability = .{ .repository_epoch = old_epoch };
    defer stale_reachability.deinit(alloc);

    const plan: SnapshotPlan = .{ .manifest = manifest };
    var session = try beginPublication(alloc, backend, plan, "gc-publication", 31, 0, 100);
    defer session.deinit(alloc);

    const lease: Lease = .{
        .lease_id = session.lease_id,
        .manifest_sha256 = session.manifest_sha256,
        .fencing_token = session.fencing_token,
        .expires_at_unix_ns = session.expires_at_unix_ns,
    };
    var current_reachability = try buildReachability(
        alloc,
        backend,
        &.{},
        &.{lease},
        1,
        session.activated_epoch,
    );
    defer current_reachability.deinit(alloc);
    try std.testing.expect(current_reachability.manifests.contains(session.manifest_sha256));
    try std.testing.expect(current_reachability.blobs.contains(digest));

    try std.testing.expectError(
        error.BackupRepositoryEpochChanged,
        sweepUnreachable(alloc, backend, &stale_reachability, &.{.{
            .kind = .blob,
            .sha256 = digest,
        }}, 100),
    );
    var append_only_backend = backend;
    append_only_backend.garbage_collector = null;
    const delete_calls_before_unsupported_sweep = filesystem.delete_calls;
    try std.testing.expectError(
        error.BackupRepositoryGcUnsupported,
        sweepUnreachable(alloc, append_only_backend, &current_reachability, &.{.{
            .kind = .blob,
            .sha256 = digest,
        }}, 100),
    );
    try std.testing.expectEqual(delete_calls_before_unsupported_sweep, filesystem.delete_calls);
    _ = try abortPublication(alloc, backend, session);
}

test "repository reachability fails closed while an active lease manifest is missing" {
    const alloc = std.testing.allocator;
    const io = std.testing.io;
    const missing_digest = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const repository_root = try std.fmt.allocPrint(
        alloc,
        ".zig-cache/tmp/{s}/missing-manifest-repository",
        .{tmp.sub_path},
    );
    defer alloc.free(repository_root);
    try fs_paths.createDirPathPortable(io, repository_root);
    var filesystem = TestFilesystemBackend{ .alloc = alloc, .io = io, .root = repository_root };
    const backend = filesystem.backend();
    const lease: Lease = .{
        .lease_id = "missing-manifest",
        .manifest_sha256 = missing_digest,
        .fencing_token = 41,
        .expires_at_unix_ns = 100,
    };
    const encoded_lease = try encodeLeaseCanonicalAlloc(alloc, lease, 0);
    defer alloc.free(encoded_lease);
    const lease_path = try leasePathAlloc(alloc, lease.lease_id);
    defer alloc.free(lease_path);
    const epoch = try backend.activate_lease(
        backend.context,
        lease_path,
        encoded_lease,
        lease.fencing_token,
    );

    try std.testing.expectError(
        error.FileNotFound,
        buildReachability(alloc, backend, &.{}, &.{lease}, 1, epoch),
    );
    // Callers cannot obtain a partial mark and therefore cannot enter sweep.
    try std.testing.expectEqual(@as(usize, 0), filesystem.delete_calls);
    _ = try backend.release_lease(backend.context, lease_path, lease.fencing_token);
}
