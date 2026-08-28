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

//! Versioned, self-contained native DB snapshot generations.
//!
//! The table-level backup manifest treats each shard snapshot as one opaque
//! tree. This manifest lives inside that tree and binds the primary snapshot,
//! generated index files, projection checkpoints, and repair metadata into one
//! generation. Legacy snapshots have no manifest and intentionally retain the
//! deferred runtime-repair restore path.

const std = @import("std");
const fs_paths = @import("../../common/fs_paths.zig");
const CancellationToken = @import("../../common/cancellation.zig").CancellationToken;

const Allocator = std.mem.Allocator;
const Io = std.Io;
const Sha256 = std.crypto.hash.sha2.Sha256;

pub const format_version: u32 = 3;
const minimum_supported_format_version: u32 = 1;
pub const manifest_file_name = "native-generation.json";
const indexes_directory_name = "indexes";
const applied_checkpoint_file_name = "derived_apply.checkpoint";
const repair_checkpoint_file_name = "index_repair.checkpoint";
const primary_lsm_directory_name = "primary-lsm";
const max_manifest_bytes: usize = 16 * 1024 * 1024;
const max_artifacts: usize = 1_000_000;

pub const Projection = struct {
    name: []const u8,
    kind: []const u8,
    config_hash: u64,
    coverage_generation: u64,
    checkpoint_generation: u64,
    applied_sequence: u64,
    target_sequence: u64,
    artifact_format: []const u8 = "legacy-managed-index-tree",
    artifact_version: u32 = 0,
    /// Physical backend and codec are part of the native compatibility
    /// contract. Native is deliberately not source-portable; a mismatch is a
    /// per-projection repair condition, never permission to trust watermarks.
    backend_id: []const u8 = "unknown",
    codec_version: u32 = 0,
    artifact_state: ProjectionArtifactState = .complete,
    repair_reason: []const u8 = "",
};

pub const ProjectionArtifactState = enum {
    complete,
    repair_required,
};

pub const Primary = struct {
    /// The format/version pair is the restore compatibility contract.
    /// `source_backend` is diagnostic for logical images and must identify the
    /// owning physical codec for backend-native checkpoints.
    artifact_format: []const u8 = "antfly-kv-stream",
    artifact_version: u32 = 2,
    source_backend: []const u8 = "unknown",
};

pub const ArtifactRole = enum {
    legacy,
    primary,
    projection,
    metadata,
};

pub const Artifact = struct {
    path: []const u8,
    size_bytes: u64,
    sha256: []const u8,
    role: ArtifactRole = .legacy,
    projection_name: []const u8 = "",
};

pub const Manifest = struct {
    format_version: u32 = format_version,
    capture_target_sequence: u64,
    artifacts: []const Artifact,
    projections: []const Projection,
    primary: Primary = .{},
};

pub const InvalidProjectionReason = enum {
    missing_artifact,
    integrity_mismatch,
    backend_mismatch,
    config_mismatch,
    coverage_mismatch,
    checkpoint_mismatch,
    capture_repair_required,
    unreadable,
};

pub const InvalidProjection = struct {
    name: []u8,
    reason: InvalidProjectionReason,
};

pub const LoadedManifest = struct {
    alloc: Allocator,
    parsed: std.json.Parsed(Manifest),
    invalid_projections: std.ArrayListUnmanaged(InvalidProjection) = .empty,

    pub fn deinit(self: *LoadedManifest) void {
        for (self.invalid_projections.items) |invalid| self.alloc.free(invalid.name);
        self.invalid_projections.deinit(self.alloc);
        self.parsed.deinit();
        self.* = undefined;
    }

    pub fn value(self: *const LoadedManifest) *const Manifest {
        return &self.parsed.value;
    }

    pub fn projectionInvalid(self: *const LoadedManifest, name: []const u8) bool {
        for (self.invalid_projections.items) |invalid| {
            if (std.mem.eql(u8, invalid.name, name)) return true;
        }
        return false;
    }

    pub fn invalidateProjection(
        self: *LoadedManifest,
        name: []const u8,
        reason: InvalidProjectionReason,
    ) !void {
        for (self.invalid_projections.items) |*invalid| {
            if (!std.mem.eql(u8, invalid.name, name)) continue;
            // Integrity/missing evidence is more actionable than a later
            // compatibility observation over the same projection.
            if (@intFromEnum(reason) < @intFromEnum(invalid.reason)) invalid.reason = reason;
            return;
        }
        try self.invalid_projections.append(self.alloc, .{
            .name = try self.alloc.dupe(u8, name),
            .reason = reason,
        });
    }

    pub fn discardInvalidProjectionArtifacts(self: *const LoadedManifest, io: Io, destination_root: []const u8) !void {
        for (self.invalid_projections.items) |invalid| {
            try validateProjectionName(invalid.name);
            const path = try std.fmt.allocPrint(self.alloc, "{s}/{s}/{s}", .{
                destination_root,
                indexes_directory_name,
                invalid.name,
            });
            defer self.alloc.free(path);
            try std.Io.Dir.cwd().deleteTree(io, path);
        }
        const indexes_path = try std.fmt.allocPrint(self.alloc, "{s}/{s}", .{ destination_root, indexes_directory_name });
        defer self.alloc.free(indexes_path);
        fs_paths.syncDirPortable(io, indexes_path) catch |err| switch (err) {
            error.FileNotFound => {},
            else => return err,
        };
    }
};

fn validateProjectionName(name: []const u8) !void {
    if (name.len == 0 or name.len > 255 or std.fs.path.isAbsolute(name) or
        std.mem.indexOfAny(u8, name, "/\\\x00") != null or
        std.mem.eql(u8, name, ".") or std.mem.eql(u8, name, ".."))
    {
        return error.InvalidNativeBackupManifest;
    }
}

const OwnedArtifact = struct {
    path: []u8,
    size_bytes: u64,
    sha256: []u8,
    role: ArtifactRole,
    projection_name: []const u8,

    fn deinit(self: *OwnedArtifact, alloc: Allocator) void {
        alloc.free(self.path);
        alloc.free(self.sha256);
    }
};

const PinnedArtifactFile = struct {
    relative_path: []u8,
    pinned_path: []u8,
    stat: std.Io.File.Stat,

    fn deinit(self: *PinnedArtifactFile, alloc: Allocator) void {
        alloc.free(self.relative_path);
        alloc.free(self.pinned_path);
        self.* = undefined;
    }
};

/// Exact generated generation selected under the capture fence. Open file
/// hardlinks preserve atomically replaced/unlinked artifacts after writers
/// resume without consuming one process descriptor per segment. Final inode,
/// size, and mtime checks reject any backend that mutates a pinned inode in
/// place, so a raced capture can never publish mixed bytes.
pub const PinnedGeneratedArtifacts = struct {
    alloc: Allocator,
    io: Io,
    pin_root: []u8,
    files: []PinnedArtifactFile,
    pin_present: bool = true,

    pub fn deinit(self: *PinnedGeneratedArtifacts) void {
        if (self.pin_present) std.Io.Dir.cwd().deleteTree(self.io, self.pin_root) catch {};
        for (self.files) |*file| file.deinit(self.alloc);
        self.alloc.free(self.files);
        self.alloc.free(self.pin_root);
        self.* = undefined;
    }

    pub fn materialize(
        self: *PinnedGeneratedArtifacts,
        snapshot_root: []const u8,
        cancellation: CancellationToken,
    ) !u64 {
        var total: u64 = 0;
        for (self.files) |*pinned| {
            try ensureActive(cancellation);
            const destination = try std.fmt.allocPrint(self.alloc, "{s}/{s}", .{ snapshot_root, pinned.relative_path });
            defer self.alloc.free(destination);
            total = std.math.add(
                u64,
                total,
                try copyPinnedFileDurable(self.io, pinned, destination, cancellation),
            ) catch return error.FileTooBig;
        }
        try std.Io.Dir.cwd().deleteTree(self.io, self.pin_root);
        self.pin_present = false;
        try fs_paths.syncDirPortable(self.io, snapshot_root);
        return total;
    }
};

pub fn pinGeneratedArtifacts(
    alloc: Allocator,
    io: Io,
    source_root: []const u8,
    pin_root: []const u8,
    cancellation: CancellationToken,
) !PinnedGeneratedArtifacts {
    return try pinGeneratedArtifactsForProjections(
        alloc,
        io,
        source_root,
        pin_root,
        null,
        cancellation,
    );
}

/// Pins only projection generations whose physical backend has an immutable
/// native checkpoint. Omitted projections remain represented in the manifest
/// as durable repair work; their live mutable files are never hardlinked.
pub fn pinGeneratedArtifactsForProjections(
    alloc: Allocator,
    io: Io,
    source_root: []const u8,
    pin_root: []const u8,
    projections: ?[]const Projection,
    cancellation: CancellationToken,
) !PinnedGeneratedArtifacts {
    try ensureActive(cancellation);
    try fs_paths.createDirPathPortable(io, pin_root);
    errdefer std.Io.Dir.cwd().deleteTree(io, pin_root) catch {};
    var files = std.ArrayListUnmanaged(PinnedArtifactFile).empty;
    errdefer {
        for (files.items) |*file| file.deinit(alloc);
        files.deinit(alloc);
    }

    const source_indexes = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ source_root, indexes_directory_name });
    defer alloc.free(source_indexes);
    if (try pathExists(io, source_indexes)) {
        var indexes = if (std.fs.path.isAbsolute(source_indexes))
            try std.Io.Dir.openDirAbsolute(io, source_indexes, .{ .iterate = true })
        else
            try std.Io.Dir.cwd().openDir(io, source_indexes, .{ .iterate = true });
        defer indexes.close(io);
        var walker = try indexes.walk(alloc);
        defer walker.deinit();
        while (try walker.next(io)) |entry| {
            try ensureActive(cancellation);
            switch (entry.kind) {
                .directory => {},
                .file => {
                    // A forced LSM checkpoint has already manifested all
                    // captured state into immutable runs. Segment payloads
                    // remain appendable after admission reopens and therefore
                    // must not be hardlinked into the generation. LSM recovery
                    // creates fresh WAL control/payload files from the pinned
                    // manifest and runs.
                    if (std.mem.endsWith(u8, entry.path, ".log")) continue;
                    if (projections) |inventory| {
                        const separator = std.mem.indexOfAny(u8, entry.path, "/\\");
                        const index_name = if (separator) |offset| entry.path[0..offset] else entry.path;
                        var include = false;
                        for (inventory) |projection| {
                            if (std.mem.eql(u8, projection.name, index_name)) {
                                include = projection.artifact_state == .complete;
                                break;
                            }
                        }
                        if (!include) continue;
                    }
                    const source = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ source_indexes, entry.path });
                    defer alloc.free(source);
                    const relative = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ indexes_directory_name, entry.path });
                    errdefer alloc.free(relative);
                    if (std.fs.path.sep != '/') {
                        for (relative) |*byte| if (byte.* == std.fs.path.sep) {
                            byte.* = '/';
                        };
                    }
                    const pinned_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ pin_root, relative });
                    errdefer alloc.free(pinned_path);
                    const stat = try pinArtifactFile(io, source, pinned_path);
                    try files.append(alloc, .{ .relative_path = relative, .pinned_path = pinned_path, .stat = stat });
                },
                else => return error.UnsupportedFileType,
            }
        }
    }

    inline for (.{ applied_checkpoint_file_name, repair_checkpoint_file_name }) |name| {
        try ensureActive(cancellation);
        const source = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ source_root, name });
        defer alloc.free(source);
        if (try pathExists(io, source)) {
            const relative = try alloc.dupe(u8, name);
            errdefer alloc.free(relative);
            const pinned_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ pin_root, relative });
            errdefer alloc.free(pinned_path);
            const stat = try pinArtifactFile(io, source, pinned_path);
            try files.append(alloc, .{ .relative_path = relative, .pinned_path = pinned_path, .stat = stat });
        }
    }

    std.mem.sort(PinnedArtifactFile, files.items, {}, struct {
        fn lessThan(_: void, lhs: PinnedArtifactFile, rhs: PinnedArtifactFile) bool {
            return std.mem.order(u8, lhs.relative_path, rhs.relative_path) == .lt;
        }
    }.lessThan);
    const owned_pin_root = try alloc.dupe(u8, pin_root);
    errdefer alloc.free(owned_pin_root);
    const owned_files = try files.toOwnedSlice(alloc);
    return .{
        .alloc = alloc,
        .io = io,
        .pin_root = owned_pin_root,
        .files = owned_files,
    };
}

pub fn capture(
    alloc: Allocator,
    io: Io,
    source_root: []const u8,
    snapshot_root: []const u8,
    capture_target_sequence: u64,
    projections: []const Projection,
) !u64 {
    var copied_bytes = try stageGeneratedArtifacts(alloc, io, source_root, snapshot_root);
    copied_bytes = std.math.add(u64, copied_bytes, try finalizeCapture(
        alloc,
        io,
        snapshot_root,
        capture_target_sequence,
        projections,
    )) catch return error.FileTooBig;
    return copied_bytes;
}

/// Compatibility helper for callers that do not manage a separate pin phase.
/// DB-native capture uses `pinGeneratedArtifacts` directly so materialization
/// can run after mutation admission reopens.
pub fn stageGeneratedArtifacts(
    alloc: Allocator,
    io: Io,
    source_root: []const u8,
    snapshot_root: []const u8,
) !u64 {
    return try stageGeneratedArtifactsWithCancellation(alloc, io, source_root, snapshot_root, .none);
}

pub fn stageGeneratedArtifactsWithCancellation(
    alloc: Allocator,
    io: Io,
    source_root: []const u8,
    snapshot_root: []const u8,
    cancellation: CancellationToken,
) !u64 {
    const pin_root = try std.fmt.allocPrint(alloc, "{s}/.generated-pin", .{snapshot_root});
    defer alloc.free(pin_root);
    var pinned = try pinGeneratedArtifacts(alloc, io, source_root, pin_root, cancellation);
    defer pinned.deinit();
    return try pinned.materialize(snapshot_root, cancellation);
}

/// Hashes an immutable staged generation and writes its manifest. Callers must
/// publish the directory only after this succeeds.
pub fn finalizeCapture(
    alloc: Allocator,
    io: Io,
    snapshot_root: []const u8,
    capture_target_sequence: u64,
    projections: []const Projection,
) !u64 {
    return try finalizeCaptureWithCancellation(
        alloc,
        io,
        snapshot_root,
        capture_target_sequence,
        projections,
        .none,
    );
}

pub fn finalizeCaptureWithCancellation(
    alloc: Allocator,
    io: Io,
    snapshot_root: []const u8,
    capture_target_sequence: u64,
    projections: []const Projection,
    cancellation: CancellationToken,
) !u64 {
    return try finalizeCaptureGenerationWithCancellation(
        alloc,
        io,
        snapshot_root,
        capture_target_sequence,
        projections,
        .{},
        cancellation,
    );
}

pub fn finalizeCaptureGenerationWithCancellation(
    alloc: Allocator,
    io: Io,
    snapshot_root: []const u8,
    capture_target_sequence: u64,
    projections: []const Projection,
    primary: Primary,
    cancellation: CancellationToken,
) !u64 {
    try ensureActive(cancellation);
    try validateProjectionInventory(format_version, capture_target_sequence, projections);
    try validatePrimary(format_version, primary);
    var artifacts = try collectArtifacts(alloc, io, snapshot_root, projections, cancellation);
    defer {
        for (artifacts.items) |*artifact| artifact.deinit(alloc);
        artifacts.deinit(alloc);
    }
    if (artifacts.items.len == 0) return error.NativeBackupGenerationEmpty;

    const manifest_artifacts = try alloc.alloc(Artifact, artifacts.items.len);
    defer alloc.free(manifest_artifacts);
    for (artifacts.items, 0..) |artifact, i| {
        manifest_artifacts[i] = .{
            .path = artifact.path,
            .size_bytes = artifact.size_bytes,
            .sha256 = artifact.sha256,
            .role = artifact.role,
            .projection_name = artifact.projection_name,
        };
    }
    const encoded = try std.json.Stringify.valueAlloc(alloc, Manifest{
        .capture_target_sequence = capture_target_sequence,
        .artifacts = manifest_artifacts,
        .projections = projections,
        .primary = primary,
    }, .{});
    defer alloc.free(encoded);
    try ensureActive(cancellation);
    if (encoded.len > max_manifest_bytes) return error.NativeBackupManifestTooLarge;

    const manifest_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ snapshot_root, manifest_file_name });
    defer alloc.free(manifest_path);
    const manifest_bytes = try writeFileDurable(io, manifest_path, encoded);
    try fs_paths.syncDirPortable(io, snapshot_root);
    return manifest_bytes;
}

/// Validates every declared artifact before copying generated state into the
/// unpublished destination generation. A null result identifies a legacy
/// native snapshot and leaves the destination untouched.
pub fn validateAndMaterialize(
    alloc: Allocator,
    io: Io,
    snapshot_root: []const u8,
    destination_root: []const u8,
) !?LoadedManifest {
    return try validateAndMaterializeWithCancellation(
        alloc,
        io,
        snapshot_root,
        destination_root,
        .none,
    );
}

pub fn validateAndMaterializeWithCancellation(
    alloc: Allocator,
    io: Io,
    snapshot_root: []const u8,
    destination_root: []const u8,
    cancellation: CancellationToken,
) !?LoadedManifest {
    try ensureActive(cancellation);
    const manifest_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ snapshot_root, manifest_file_name });
    defer alloc.free(manifest_path);
    const raw = readFileAlloc(alloc, io, manifest_path, max_manifest_bytes) catch |err| switch (err) {
        error.FileNotFound => return null,
        else => return err,
    };
    defer alloc.free(raw);

    const parsed = std.json.parseFromSlice(Manifest, alloc, raw, .{ .allocate = .alloc_always }) catch
        return error.InvalidNativeBackupManifest;
    var loaded = LoadedManifest{ .alloc = alloc, .parsed = parsed };
    errdefer loaded.deinit();
    const manifest = loaded.value();
    if (manifest.format_version < minimum_supported_format_version or
        manifest.format_version > format_version or
        manifest.artifacts.len == 0 or
        manifest.artifacts.len > max_artifacts)
    {
        return error.InvalidNativeBackupManifest;
    }
    try validateProjectionInventory(manifest.format_version, manifest.capture_target_sequence, manifest.projections);
    try validatePrimary(manifest.format_version, manifest.primary);
    if (manifest.format_version >= 3) {
        for (manifest.projections) |projection| {
            if (projection.artifact_state == .repair_required)
                try loaded.invalidateProjection(projection.name, .capture_repair_required);
        }
    }

    var previous_path: ?[]const u8 = null;
    for (manifest.artifacts) |artifact| {
        try ensureActive(cancellation);
        try validateRelativePath(artifact.path);
        if (std.mem.eql(u8, artifact.path, manifest_file_name) or
            artifact.sha256.len != Sha256.digest_length * 2)
        {
            return error.InvalidNativeBackupManifest;
        }
        if (previous_path) |previous| {
            if (std.mem.order(u8, previous, artifact.path) != .lt)
                return error.InvalidNativeBackupManifest;
        }
        previous_path = artifact.path;
        if (manifest.format_version >= 3)
            try validateArtifactOwnership(artifact, manifest.projections);

        const source = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ snapshot_root, artifact.path });
        defer alloc.free(source);
        const stat = statRegularFile(io, source) catch |err| switch (err) {
            error.FileNotFound => {
                if (try downgradeProjectionArtifactFailure(&loaded, artifact, .missing_artifact)) continue;
                return error.NativeBackupArtifactMissing;
            },
            else => return err,
        };
        if (stat.size != artifact.size_bytes) {
            if (try downgradeProjectionArtifactFailure(&loaded, artifact, .integrity_mismatch)) continue;
            return error.NativeBackupArtifactIntegrityMismatch;
        }
        var digest: [Sha256.digest_length]u8 = undefined;
        hashFile(io, source, stat, &digest, cancellation) catch |err| switch (err) {
            error.SourceFileChanged => {
                if (try downgradeProjectionArtifactFailure(&loaded, artifact, .integrity_mismatch)) continue;
                return err;
            },
            else => return err,
        };
        const actual = std.fmt.bytesToHex(digest, .lower);
        if (!std.mem.eql(u8, &actual, artifact.sha256)) {
            if (try downgradeProjectionArtifactFailure(&loaded, artifact, .integrity_mismatch)) continue;
            return error.NativeBackupArtifactIntegrityMismatch;
        }
    }

    if (manifest.format_version >= 3)
        try validateProjectionArtifactInventory(manifest);
    try validateCompleteInventory(alloc, io, snapshot_root, manifest, cancellation);
    try validatePrimaryInventory(manifest);
    for (manifest.artifacts) |artifact| {
        try ensureActive(cancellation);
        if (!isGeneratedArtifact(artifact.path)) continue;
        if (artifact.role == .projection and loaded.projectionInvalid(artifact.projection_name)) continue;
        const source = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ snapshot_root, artifact.path });
        defer alloc.free(source);
        const materialized_path = if (std.mem.startsWith(u8, artifact.path, primary_lsm_directory_name ++ "/"))
            artifact.path[(primary_lsm_directory_name ++ "/").len..]
        else
            artifact.path;
        if (materialized_path.len == 0) return error.InvalidNativeBackupManifest;
        const destination = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ destination_root, materialized_path });
        defer alloc.free(destination);
        _ = try copyFileDurableCancellable(io, source, destination, cancellation);
    }
    return loaded;
}

fn downgradeProjectionArtifactFailure(
    loaded: *LoadedManifest,
    artifact: Artifact,
    reason: InvalidProjectionReason,
) !bool {
    if (loaded.value().format_version < 3 or artifact.role != .projection or
        artifact.projection_name.len == 0)
    {
        return false;
    }
    try loaded.invalidateProjection(artifact.projection_name, reason);
    return true;
}

fn validateArtifactOwnership(artifact: Artifact, projections: []const Projection) !void {
    const expected = try artifactOwnership(artifact.path, projections);
    if (artifact.role != expected.role) return error.InvalidNativeBackupManifest;
    switch (artifact.role) {
        .projection => {
            if (artifact.projection_name.len == 0 or
                !std.mem.eql(u8, artifact.projection_name, expected.projection_name))
            {
                return error.InvalidNativeBackupManifest;
            }
        },
        .primary, .metadata => if (artifact.projection_name.len != 0)
            return error.InvalidNativeBackupManifest,
        .legacy => return error.InvalidNativeBackupManifest,
    }
}

fn validateProjectionArtifactInventory(manifest: *const Manifest) !void {
    for (manifest.projections) |projection| {
        var artifact_count: usize = 0;
        for (manifest.artifacts) |artifact| {
            if (artifact.role == .projection and
                std.mem.eql(u8, artifact.projection_name, projection.name))
            {
                artifact_count += 1;
            }
        }
        switch (projection.artifact_state) {
            .complete => if (!std.mem.eql(u8, projection.kind, "algebraic") and artifact_count == 0)
                return error.InvalidNativeBackupManifest,
            .repair_required => if (artifact_count != 0 or projection.repair_reason.len == 0)
                return error.InvalidNativeBackupManifest,
        }
    }
}

fn validateProjectionInventory(manifest_version: u32, capture_target_sequence: u64, projections: []const Projection) !void {
    if (projections.len > max_artifacts) return error.InvalidNativeBackupManifest;
    var previous_name: ?[]const u8 = null;
    for (projections) |projection| {
        if (manifest_version >= 3) try validateProjectionName(projection.name);
        if (projection.name.len == 0 or
            projection.kind.len == 0 or
            (manifest_version >= 2 and
                (!std.mem.eql(u8, projection.artifact_format, "antfly-managed-index-tree") or
                    projection.artifact_version != 1)) or
            (manifest_version >= 3 and
                (projection.backend_id.len == 0 or
                    std.mem.eql(u8, projection.backend_id, "unknown") or
                    projection.codec_version == 0 or
                    (projection.artifact_state == .complete and projection.repair_reason.len != 0) or
                    (projection.artifact_state == .repair_required and projection.repair_reason.len == 0))) or
            projection.target_sequence != capture_target_sequence or
            projection.applied_sequence != projection.target_sequence)
        {
            return error.InvalidNativeBackupManifest;
        }
        if (previous_name) |previous| {
            // Capture emits canonical order. Strict ordering simultaneously
            // rejects duplicate identities and makes a same-length inventory
            // a one-to-one mapping during DB-level config validation.
            if (std.mem.order(u8, previous, projection.name) != .lt)
                return error.InvalidNativeBackupManifest;
        }
        previous_name = projection.name;
    }
}

fn validatePrimary(manifest_version: u32, primary: Primary) !void {
    if (manifest_version < 2) return;
    const supported = (std.mem.eql(u8, primary.artifact_format, "antfly-kv-stream") and
        primary.artifact_version == 2) or
        (std.mem.eql(u8, primary.artifact_format, "antfly-lsm-checkpoint") and
            primary.artifact_version == 1);
    if (!supported or primary.source_backend.len == 0 or
        (std.mem.eql(u8, primary.artifact_format, "antfly-lsm-checkpoint") and
            !std.mem.eql(u8, primary.source_backend, "lsm")))
    {
        return error.InvalidNativeBackupManifest;
    }
}

fn validatePrimaryInventory(manifest: *const Manifest) !void {
    if (manifest.format_version < 2) return;
    const physical = std.mem.eql(u8, manifest.primary.artifact_format, "antfly-lsm-checkpoint");
    const has_store = manifestContainsArtifact(manifest, "store.bin");
    const has_lsm_manifest = manifestContainsArtifact(manifest, primary_lsm_directory_name ++ "/manifest.bin");
    if (physical) {
        if (has_store or !has_lsm_manifest) return error.InvalidNativeBackupManifest;
    } else if (!has_store or has_lsm_manifest) {
        return error.InvalidNativeBackupManifest;
    }

    for (manifest.artifacts) |artifact| {
        if (!std.mem.startsWith(u8, artifact.path, primary_lsm_directory_name ++ "/")) continue;
        if (!physical or !validLsmCheckpointArtifactPath(artifact.path))
            return error.InvalidNativeBackupManifest;
    }
}

fn validLsmCheckpointArtifactPath(path: []const u8) bool {
    const prefix = primary_lsm_directory_name ++ "/";
    const relative = path[prefix.len..];
    if (std.mem.eql(u8, relative, "manifest.bin")) return true;
    const runs_prefix = "runs/";
    if (!std.mem.startsWith(u8, relative, runs_prefix) or
        !std.mem.endsWith(u8, relative, ".tbl")) return false;
    const run_id = relative[runs_prefix.len .. relative.len - ".tbl".len];
    if (run_id.len == 0 or std.mem.indexOfScalar(u8, run_id, '/') != null) return false;
    _ = std.fmt.parseInt(u64, run_id, 10) catch return false;
    return true;
}

fn isGeneratedArtifact(path: []const u8) bool {
    return std.mem.startsWith(u8, path, indexes_directory_name ++ "/") or
        std.mem.startsWith(u8, path, primary_lsm_directory_name ++ "/") or
        std.mem.eql(u8, path, applied_checkpoint_file_name) or
        std.mem.eql(u8, path, repair_checkpoint_file_name);
}

fn collectArtifacts(
    alloc: Allocator,
    io: Io,
    root: []const u8,
    projections: []const Projection,
    cancellation: CancellationToken,
) !std.ArrayListUnmanaged(OwnedArtifact) {
    var result = std.ArrayListUnmanaged(OwnedArtifact).empty;
    errdefer {
        for (result.items) |*artifact| artifact.deinit(alloc);
        result.deinit(alloc);
    }
    var dir = if (std.fs.path.isAbsolute(root))
        try std.Io.Dir.openDirAbsolute(io, root, .{ .iterate = true })
    else
        try std.Io.Dir.cwd().openDir(io, root, .{ .iterate = true });
    defer dir.close(io);
    var walker = try dir.walk(alloc);
    defer walker.deinit();
    while (try walker.next(io)) |entry| {
        try ensureActive(cancellation);
        switch (entry.kind) {
            .directory => {},
            .file => {
                if (std.mem.eql(u8, entry.path, manifest_file_name)) continue;
                if (result.items.len == max_artifacts) return error.NativeBackupManifestTooLarge;
                try validateRelativePath(entry.path);
                const path = try alloc.dupe(u8, entry.path);
                errdefer alloc.free(path);
                const absolute = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ root, entry.path });
                defer alloc.free(absolute);
                const stat = try statRegularFile(io, absolute);
                var digest: [Sha256.digest_length]u8 = undefined;
                try hashFile(io, absolute, stat, &digest, cancellation);
                const hex = std.fmt.bytesToHex(digest, .lower);
                const sha256 = try alloc.dupe(u8, &hex);
                errdefer alloc.free(sha256);
                const ownership = try artifactOwnership(entry.path, projections);
                try result.append(alloc, .{
                    .path = path,
                    .size_bytes = stat.size,
                    .sha256 = sha256,
                    .role = ownership.role,
                    .projection_name = ownership.projection_name,
                });
            },
            else => return error.UnsupportedFileType,
        }
    }
    std.mem.sort(OwnedArtifact, result.items, {}, struct {
        fn lessThan(_: void, lhs: OwnedArtifact, rhs: OwnedArtifact) bool {
            return std.mem.order(u8, lhs.path, rhs.path) == .lt;
        }
    }.lessThan);
    return result;
}

const ArtifactOwnership = struct {
    role: ArtifactRole,
    projection_name: []const u8 = "",
};

fn artifactOwnership(path: []const u8, projections: []const Projection) !ArtifactOwnership {
    if (std.mem.eql(u8, path, "store.bin") or
        std.mem.eql(u8, path, "change-journal.bin") or
        std.mem.startsWith(u8, path, primary_lsm_directory_name ++ "/"))
    {
        return .{ .role = .primary };
    }
    if (std.mem.eql(u8, path, applied_checkpoint_file_name) or
        std.mem.eql(u8, path, repair_checkpoint_file_name))
    {
        return .{ .role = .metadata };
    }
    if (std.mem.startsWith(u8, path, indexes_directory_name ++ "/")) {
        const suffix = path[(indexes_directory_name ++ "/").len..];
        const separator = std.mem.indexOfScalar(u8, suffix, '/') orelse
            return error.InvalidNativeBackupArtifactPath;
        const index_name = suffix[0..separator];
        for (projections) |projection| {
            if (std.mem.eql(u8, projection.name, index_name))
                return .{ .role = .projection, .projection_name = projection.name };
        }
        return error.NativeBackupProjectionMismatch;
    }
    return error.InvalidNativeBackupManifest;
}

fn validateCompleteInventory(
    alloc: Allocator,
    io: Io,
    root: []const u8,
    manifest: *const Manifest,
    cancellation: CancellationToken,
) !void {
    var dir = if (std.fs.path.isAbsolute(root))
        try std.Io.Dir.openDirAbsolute(io, root, .{ .iterate = true })
    else
        try std.Io.Dir.cwd().openDir(io, root, .{ .iterate = true });
    defer dir.close(io);
    var walker = try dir.walk(alloc);
    defer walker.deinit();
    var files: usize = 0;
    while (try walker.next(io)) |entry| {
        try ensureActive(cancellation);
        switch (entry.kind) {
            .directory => {},
            .file => {
                if (std.mem.eql(u8, entry.path, manifest_file_name)) continue;
                files += 1;
                if (!manifestContainsArtifact(manifest, entry.path))
                    return error.InvalidNativeBackupManifest;
            },
            else => return error.UnsupportedFileType,
        }
    }
    if (manifest.format_version < 3 and files != manifest.artifacts.len)
        return error.InvalidNativeBackupManifest;
}

fn manifestContainsArtifact(manifest: *const Manifest, path: []const u8) bool {
    var low: usize = 0;
    var high = manifest.artifacts.len;
    while (low < high) {
        const mid = low + (high - low) / 2;
        switch (std.mem.order(u8, manifest.artifacts[mid].path, path)) {
            .lt => low = mid + 1,
            .gt => high = mid,
            .eq => return true,
        }
    }
    return false;
}

fn validateRelativePath(path: []const u8) !void {
    if (path.len == 0 or path.len > 4096 or std.fs.path.isAbsolute(path) or
        std.mem.indexOfScalar(u8, path, '\\') != null or
        std.mem.indexOfScalar(u8, path, 0) != null)
    {
        return error.InvalidNativeBackupArtifactPath;
    }
    var components = std.mem.splitScalar(u8, path, '/');
    while (components.next()) |component| {
        if (component.len == 0 or std.mem.eql(u8, component, ".") or std.mem.eql(u8, component, ".."))
            return error.InvalidNativeBackupArtifactPath;
    }
}

fn ensureActive(cancellation: CancellationToken) !void {
    if (cancellation.isCancelled()) return error.Canceled;
}

fn copyFileDurableCancellable(
    io: Io,
    source_path: []const u8,
    destination_path: []const u8,
    cancellation: CancellationToken,
) !u64 {
    if (std.fs.path.dirname(destination_path)) |parent| try fs_paths.createDirPathPortable(io, parent);
    var source = if (std.fs.path.isAbsolute(source_path))
        try std.Io.Dir.openFileAbsolute(io, source_path, .{})
    else
        try std.Io.Dir.cwd().openFile(io, source_path, .{});
    defer source.close(io);
    const initial = try source.stat(io);
    if (initial.kind != .file) return error.UnsupportedFileType;

    var destination = try fs_paths.createFilePortable(io, destination_path, .{ .truncate = true });
    defer destination.close(io);
    var writer_buffer: [64 * 1024]u8 = undefined;
    var writer = destination.writer(io, &writer_buffer);
    var buffer: [256 * 1024]u8 = undefined;
    var offset: u64 = 0;
    while (offset < initial.size) {
        try ensureActive(cancellation);
        const wanted: usize = @intCast(@min(initial.size - offset, buffer.len));
        const read = try source.readPositionalAll(io, buffer[0..wanted], offset);
        if (read != wanted) return error.SourceFileChanged;
        try writer.interface.writeAll(buffer[0..read]);
        offset += read;
    }
    try writer.end();
    try ensureActive(cancellation);
    const final = try source.stat(io);
    if (final.size != initial.size or !std.meta.eql(final.mtime, initial.mtime))
        return error.SourceFileChanged;
    try destination.sync(io);
    const parent = std.fs.path.dirname(destination_path) orelse if (std.fs.path.isAbsolute(destination_path)) "/" else ".";
    try fs_paths.syncDirPortable(io, parent);
    return initial.size;
}

fn copyPinnedFileDurable(
    io: Io,
    pinned: *const PinnedArtifactFile,
    destination_path: []const u8,
    cancellation: CancellationToken,
) !u64 {
    const current = try statRegularFile(io, pinned.pinned_path);
    if (current.inode != pinned.stat.inode or
        current.size != pinned.stat.size or
        !std.meta.eql(current.mtime, pinned.stat.mtime))
    {
        std.log.err("native backup pinned artifact changed before copy path={s}", .{pinned.relative_path});
        return error.SourceFileChanged;
    }
    return copyFileDurableCancellable(io, pinned.pinned_path, destination_path, cancellation) catch |err| {
        if (err == error.SourceFileChanged)
            std.log.err("native backup pinned artifact changed during copy path={s}", .{pinned.relative_path});
        return err;
    };
}

fn pinArtifactFile(io: Io, source_path: []const u8, pinned_path: []const u8) !std.Io.File.Stat {
    if (std.fs.path.dirname(pinned_path)) |parent|
        try fs_paths.createDirPathPortable(io, parent);
    const initial = try statRegularFile(io, source_path);
    try std.Io.Dir.hardLink(.cwd(), source_path, .cwd(), pinned_path, io, .{});
    const pinned = try statRegularFile(io, pinned_path);
    if (pinned.inode != initial.inode or
        pinned.size != initial.size or
        !std.meta.eql(pinned.mtime, initial.mtime))
    {
        return error.SourceFileChanged;
    }
    return pinned;
}

fn statRegularFile(io: Io, path: []const u8) !std.Io.File.Stat {
    var file = if (std.fs.path.isAbsolute(path))
        try std.Io.Dir.openFileAbsolute(io, path, .{})
    else
        try std.Io.Dir.cwd().openFile(io, path, .{});
    defer file.close(io);
    const stat = try file.stat(io);
    if (stat.kind != .file) return error.UnsupportedFileType;
    return stat;
}

fn hashFile(
    io: Io,
    path: []const u8,
    initial: std.Io.File.Stat,
    digest: *[Sha256.digest_length]u8,
    cancellation: CancellationToken,
) !void {
    var file = if (std.fs.path.isAbsolute(path))
        try std.Io.Dir.openFileAbsolute(io, path, .{})
    else
        try std.Io.Dir.cwd().openFile(io, path, .{});
    defer file.close(io);
    var hasher = Sha256.init(.{});
    var buffer: [256 * 1024]u8 = undefined;
    var offset: u64 = 0;
    while (offset < initial.size) {
        try ensureActive(cancellation);
        const wanted: usize = @intCast(@min(initial.size - offset, buffer.len));
        const read = try file.readPositionalAll(io, buffer[0..wanted], offset);
        if (read != wanted) return error.SourceFileChanged;
        hasher.update(buffer[0..read]);
        offset += read;
    }
    try ensureActive(cancellation);
    var extra: [1]u8 = undefined;
    if (try file.readPositionalAll(io, &extra, offset) != 0) return error.SourceFileChanged;
    const final = try file.stat(io);
    if (final.size != initial.size or !std.meta.eql(final.mtime, initial.mtime))
        return error.SourceFileChanged;
    hasher.final(digest);
}

fn pathExists(io: Io, path: []const u8) !bool {
    _ = std.Io.Dir.cwd().statFile(io, path, .{}) catch |err| switch (err) {
        error.FileNotFound => return false,
        else => return err,
    };
    return true;
}

fn readFileAlloc(alloc: Allocator, io: Io, path: []const u8, max_bytes: usize) ![]u8 {
    var file = if (std.fs.path.isAbsolute(path))
        try std.Io.Dir.openFileAbsolute(io, path, .{})
    else
        try std.Io.Dir.cwd().openFile(io, path, .{});
    defer file.close(io);
    const stat = try file.stat(io);
    var reader: std.Io.File.Reader = .initSize(file, io, &.{}, stat.size);
    return try reader.interface.allocRemaining(alloc, .limited(max_bytes));
}

fn writeFileDurable(io: Io, path: []const u8, body: []const u8) !u64 {
    var file = try fs_paths.createFilePortable(io, path, .{ .truncate = true });
    defer file.close(io);
    var buffer: [16 * 1024]u8 = undefined;
    var writer = file.writer(io, &buffer);
    try writer.interface.writeAll(body);
    try writer.end();
    try file.sync(io);
    const parent = std.fs.path.dirname(path) orelse if (std.fs.path.isAbsolute(path)) "/" else ".";
    try fs_paths.syncDirPortable(io, parent);
    return body.len;
}

test "native generation manifest captures validates and materializes generated artifacts" {
    const alloc = std.testing.allocator;
    var source_tmp = std.testing.tmpDir(.{});
    defer source_tmp.cleanup();
    var snapshot_tmp = std.testing.tmpDir(.{});
    defer snapshot_tmp.cleanup();
    var destination_tmp = std.testing.tmpDir(.{});
    defer destination_tmp.cleanup();

    const source = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}", .{source_tmp.sub_path});
    defer alloc.free(source);
    const snapshot = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}", .{snapshot_tmp.sub_path});
    defer alloc.free(snapshot);
    const destination = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}", .{destination_tmp.sub_path});
    defer alloc.free(destination);
    const index_file = try std.fmt.allocPrint(alloc, "{s}/indexes/dense/data.bin", .{source});
    defer alloc.free(index_file);
    try fs_paths.createDirPathPortable(std.testing.io, std.fs.path.dirname(index_file).?);
    _ = try writeFileDurable(std.testing.io, index_file, "dense-index");
    const store_file = try std.fmt.allocPrint(alloc, "{s}/store.bin", .{snapshot});
    defer alloc.free(store_file);
    _ = try writeFileDurable(std.testing.io, store_file, "primary");

    _ = try capture(alloc, std.testing.io, source, snapshot, 12, &.{.{
        .name = "dense",
        .kind = "dense_vector",
        .config_hash = 7,
        .coverage_generation = 3,
        .checkpoint_generation = 3,
        .applied_sequence = 12,
        .target_sequence = 12,
        .artifact_format = "antfly-managed-index-tree",
        .artifact_version = 1,
        .backend_id = "lsm",
        .codec_version = 1,
    }});
    var loaded = (try validateAndMaterialize(alloc, std.testing.io, snapshot, destination)).?;
    defer loaded.deinit();
    try std.testing.expectEqual(@as(u64, 12), loaded.value().capture_target_sequence);
    try std.testing.expectEqual(@as(usize, 1), loaded.value().projections.len);
    const restored = try std.fmt.allocPrint(alloc, "{s}/indexes/dense/data.bin", .{destination});
    defer alloc.free(restored);
    const restored_body = try readFileAlloc(alloc, std.testing.io, restored, 64);
    defer alloc.free(restored_body);
    try std.testing.expectEqualStrings("dense-index", restored_body);

    const snapshot_index = try std.fmt.allocPrint(alloc, "{s}/indexes/dense/data.bin", .{snapshot});
    defer alloc.free(snapshot_index);
    _ = try writeFileDurable(std.testing.io, snapshot_index, "corrupt");
    var corrupted = (try validateAndMaterialize(alloc, std.testing.io, snapshot, destination)).?;
    defer corrupted.deinit();
    try std.testing.expect(corrupted.projectionInvalid("dense"));
    try corrupted.discardInvalidProjectionArtifacts(std.testing.io, destination);
    try std.testing.expect(!try pathExists(std.testing.io, restored));
}

test "native generation projection inventory is complete and revision exact" {
    const first = Projection{
        .name = "dense_a",
        .kind = "dense_vector",
        .config_hash = 7,
        .coverage_generation = 3,
        .checkpoint_generation = 3,
        .applied_sequence = 12,
        .target_sequence = 12,
        .artifact_format = "antfly-managed-index-tree",
        .artifact_version = 1,
        .backend_id = "lsm",
        .codec_version = 1,
    };
    const second = Projection{
        .name = "dense_b",
        .kind = "dense_vector",
        .config_hash = 9,
        .coverage_generation = 4,
        .checkpoint_generation = 4,
        .applied_sequence = 12,
        .target_sequence = 12,
        .artifact_format = "antfly-managed-index-tree",
        .artifact_version = 1,
        .backend_id = "lsm",
        .codec_version = 1,
    };
    try validateProjectionInventory(format_version, 12, &.{ first, second });
    try std.testing.expectError(
        error.InvalidNativeBackupManifest,
        validateProjectionInventory(format_version, 12, &.{ first, first }),
    );

    var stale = second;
    stale.applied_sequence = 11;
    stale.target_sequence = 11;
    try std.testing.expectError(
        error.InvalidNativeBackupManifest,
        validateProjectionInventory(format_version, 12, &.{ first, stale }),
    );
}

test "native generation validator leaves legacy snapshot on repair path" {
    const alloc = std.testing.allocator;
    var snapshot_tmp = std.testing.tmpDir(.{});
    defer snapshot_tmp.cleanup();
    var destination_tmp = std.testing.tmpDir(.{});
    defer destination_tmp.cleanup();
    const snapshot = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}", .{snapshot_tmp.sub_path});
    defer alloc.free(snapshot);
    const destination = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}", .{destination_tmp.sub_path});
    defer alloc.free(destination);
    try std.testing.expect((try validateAndMaterialize(
        alloc,
        std.testing.io,
        snapshot,
        destination,
    )) == null);
}

test "native generation validation honors restore cancellation before materialization" {
    const alloc = std.testing.allocator;
    var snapshot_tmp = std.testing.tmpDir(.{});
    defer snapshot_tmp.cleanup();
    var destination_tmp = std.testing.tmpDir(.{});
    defer destination_tmp.cleanup();
    const snapshot = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}", .{snapshot_tmp.sub_path});
    defer alloc.free(snapshot);
    const destination = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}", .{destination_tmp.sub_path});
    defer alloc.free(destination);
    var canceled = std.atomic.Value(bool).init(true);
    try std.testing.expectError(
        error.Canceled,
        validateAndMaterializeWithCancellation(
            alloc,
            std.testing.io,
            snapshot,
            destination,
            CancellationToken.fromAtomic(&canceled),
        ),
    );
}

test "native generated pin rejects in-place artifact mutation" {
    const alloc = std.testing.allocator;
    var source_tmp = std.testing.tmpDir(.{});
    defer source_tmp.cleanup();
    var snapshot_tmp = std.testing.tmpDir(.{});
    defer snapshot_tmp.cleanup();
    const source = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}", .{source_tmp.sub_path});
    defer alloc.free(source);
    const snapshot = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}", .{snapshot_tmp.sub_path});
    defer alloc.free(snapshot);
    const source_file = try std.fmt.allocPrint(alloc, "{s}/indexes/dense/data.bin", .{source});
    defer alloc.free(source_file);
    try fs_paths.createDirPathPortable(std.testing.io, std.fs.path.dirname(source_file).?);
    _ = try writeFileDurable(std.testing.io, source_file, "stable");
    const pin_root = try std.fmt.allocPrint(alloc, "{s}/.generated-pin", .{snapshot});
    defer alloc.free(pin_root);
    var pinned = try pinGeneratedArtifacts(alloc, std.testing.io, source, pin_root, .none);
    defer pinned.deinit();

    _ = try writeFileDurable(std.testing.io, source_file, "mutated-in-place");
    try std.testing.expectError(
        error.SourceFileChanged,
        pinned.materialize(snapshot, .none),
    );
}
