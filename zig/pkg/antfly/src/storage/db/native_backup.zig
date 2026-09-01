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
const native_artifact_sink = @import("../native_artifact_sink.zig");
const lsm_backend = @import("../lsm_backend/mod.zig");

const Allocator = std.mem.Allocator;
const Io = std.Io;
const Sha256 = std.crypto.hash.sha2.Sha256;

/// Current native-generation contract. Versions 1 through 4 existed only on the
/// development branch and are deliberately not a compatibility surface.
/// v0.2.0 native backups have no generation manifest and use the legacy repair
/// path instead.
pub const format_version: u32 = 5;
pub const manifest_file_name = "native-generation.json";
const indexes_directory_name = "indexes";
const applied_checkpoint_file_name = "derived_apply.checkpoint";
const repair_checkpoint_file_name = "index_repair.checkpoint";
const primary_lsm_directory_name = "primary-lsm";
pub const max_manifest_bytes: usize = 16 * 1024 * 1024;
const max_artifacts: usize = 1_000_000;

pub const Projection = struct {
    name: []const u8,
    kind: []const u8,
    config_hash: u64,
    coverage_generation: u64,
    checkpoint_generation: u64,
    applied_sequence: u64,
    target_sequence: u64,
    artifact_format: []const u8,
    artifact_version: u32,
    /// Physical backend and codec are part of the native compatibility
    /// contract. Native is deliberately not source-portable; a mismatch is a
    /// per-projection repair condition, never permission to trust watermarks.
    backend_id: []const u8,
    codec_version: u32,
    artifact_state: ProjectionArtifactState,
    repair_reason: []const u8,
};

pub const ProjectionArtifactState = enum {
    complete,
    repair_required,
};

pub const Primary = struct {
    /// The format/version pair is the restore compatibility contract.
    /// `source_backend` is diagnostic for logical images and must identify the
    /// owning physical codec for backend-native checkpoints.
    artifact_format: []const u8,
    artifact_version: u32,
    source_backend: []const u8,
};

pub const ArtifactRole = enum {
    legacy,
    primary,
    projection,
    /// Table-wide exact-vector blocks are shared by every dense projection.
    /// They are an acceleration plane: loss or corruption never invalidates
    /// the independently complete posting generations or primary vectors.
    shared_acceleration,
    metadata,
};

pub const Artifact = struct {
    /// Stable path inside the portable snapshot generation.
    path: []const u8,
    /// Authenticated runtime-relative destination. Snapshot namespaces do not
    /// implicitly define the live database layout.
    install_path: []const u8,
    size_bytes: u64,
    sha256: []const u8,
    role: ArtifactRole,
    projection_name: []const u8,
};

pub const Manifest = struct {
    format_version: u32,
    capture_target_sequence: u64,
    artifacts: []const Artifact,
    projections: []const Projection,
    primary: Primary,
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
    shared_acceleration_invalid: bool = false,

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
        if (self.shared_acceleration_invalid) {
            const path = try std.fmt.allocPrint(self.alloc, "{s}/vector-blocks", .{destination_root});
            defer self.alloc.free(path);
            try std.Io.Dir.cwd().deleteTree(io, path);
            try fs_paths.syncDirPortable(io, destination_root);
        }
        const indexes_path = try std.fmt.allocPrint(self.alloc, "{s}/{s}", .{ destination_root, indexes_directory_name });
        defer self.alloc.free(indexes_path);
        fs_paths.syncDirPortable(io, indexes_path) catch |err| switch (err) {
            error.FileNotFound => {},
            else => return err,
        };
    }
};

/// One exact native-generation artifact selected while the DB capture fence is
/// held. Immutable files are hardlinked into a private pin tree; mutable WALs
/// acquire stable descriptor leases at their committed prefix; control records
/// are emitted from the already-decoded manifest. Corpus-byte copying therefore
/// occurs only after mutation admission reopens.
pub const ExplicitPinSpec = struct {
    relative_path: []const u8,
    source: union(enum) {
        immutable_file: []const u8,
        committed_prefix: struct {
            storage: lsm_backend.Storage,
            path: []const u8,
            bytes: u64,
        },
        bytes: []const u8,
    },
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
    install_path: []u8,
    size_bytes: u64,
    sha256: []u8,
    role: ArtifactRole,
    projection_name: []const u8,

    fn deinit(self: *OwnedArtifact, alloc: Allocator) void {
        alloc.free(self.path);
        alloc.free(self.install_path);
        alloc.free(self.sha256);
    }
};

/// Collects manifest identities at the durable write boundary. Checkpoint
/// backends stream SHA-256 through this sink while copying, eliminating the
/// former second walk and corpus-sized reread of the staged generation.
pub const ArtifactReceiptCollector = struct {
    alloc: Allocator,
    snapshot_root: []const u8,
    projections: []const Projection,
    artifacts: std.ArrayListUnmanaged(OwnedArtifact) = .empty,

    pub fn init(
        alloc: Allocator,
        snapshot_root: []const u8,
        projections: []const Projection,
    ) ArtifactReceiptCollector {
        return .{
            .alloc = alloc,
            .snapshot_root = snapshot_root,
            .projections = projections,
        };
    }

    pub fn deinit(self: *ArtifactReceiptCollector) void {
        for (self.artifacts.items) |*artifact| artifact.deinit(self.alloc);
        self.artifacts.deinit(self.alloc);
        self.* = undefined;
    }

    pub fn sink(self: *ArtifactReceiptCollector) native_artifact_sink.Sink {
        return .{ .ptr = self, .record_fn = record };
    }

    fn record(
        ptr: *anyopaque,
        absolute_path: []const u8,
        size_bytes: u64,
        digest: [Sha256.digest_length]u8,
    ) !void {
        const self: *ArtifactReceiptCollector = @ptrCast(@alignCast(ptr));
        if (absolute_path.len <= self.snapshot_root.len or
            !std.mem.eql(u8, absolute_path[0..self.snapshot_root.len], self.snapshot_root) or
            absolute_path[self.snapshot_root.len] != '/')
        {
            return error.InvalidNativeBackupArtifactPath;
        }
        const relative = absolute_path[self.snapshot_root.len + 1 ..];
        try validateRelativePath(relative);
        if (std.mem.eql(u8, relative, manifest_file_name)) return error.InvalidNativeBackupArtifactPath;
        if (self.artifacts.items.len == max_artifacts) return error.NativeBackupManifestTooLarge;
        const ownership = try artifactOwnership(relative, self.projections);
        const owned_path = try self.alloc.dupe(u8, relative);
        errdefer self.alloc.free(owned_path);
        const install_path = try artifactInstallPathAlloc(self.alloc, relative, ownership.role);
        errdefer self.alloc.free(install_path);
        const hex = std.fmt.bytesToHex(digest, .lower);
        const sha256 = try self.alloc.dupe(u8, &hex);
        errdefer self.alloc.free(sha256);
        try self.artifacts.append(self.alloc, .{
            .path = owned_path,
            .install_path = install_path,
            .size_bytes = size_bytes,
            .sha256 = sha256,
            .role = ownership.role,
            .projection_name = ownership.projection_name,
        });
    }
};

const PinnedArtifactFile = struct {
    const Source = union(enum) {
        pinned_file: struct {
            path: []u8,
            stat: std.Io.File.Stat,
        },
        leased_prefix: struct {
            reader: lsm_backend.storage_io.ColdSequentialReader,
            bytes: u64,
        },
    };

    relative_path: []u8,
    source: Source,

    fn deinit(self: *PinnedArtifactFile, alloc: Allocator) void {
        alloc.free(self.relative_path);
        switch (self.source) {
            .pinned_file => |file| alloc.free(file.path),
            .leased_prefix => |*prefix| prefix.reader.deinit(),
        }
        self.* = undefined;
    }
};

/// Exact generated generation selected under the capture fence. Hardlinks pin
/// immutable segments without consuming one descriptor each; stable reader
/// leases pin append-only WAL prefixes. Final inode, size, and mtime checks
/// reject a backend that mutates a hardlinked immutable inode in place.
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
        return try self.materializeWithSink(snapshot_root, cancellation, null);
    }

    pub fn materializeWithSink(
        self: *PinnedGeneratedArtifacts,
        snapshot_root: []const u8,
        cancellation: CancellationToken,
        sink: ?native_artifact_sink.Sink,
    ) !u64 {
        var total: u64 = 0;
        for (self.files) |*pinned| {
            try ensureActive(cancellation);
            const destination = try std.fmt.allocPrint(self.alloc, "{s}/{s}", .{ snapshot_root, pinned.relative_path });
            defer self.alloc.free(destination);
            const copied = switch (pinned.source) {
                .pinned_file => try copyPinnedFileDurable(self.io, pinned, destination, cancellation, sink),
                .leased_prefix => |*prefix| try copyLeasedPrefixDurable(
                    self.alloc,
                    self.io,
                    &prefix.reader,
                    prefix.bytes,
                    destination,
                    cancellation,
                    sink,
                ),
            };
            total = std.math.add(u64, total, copied) catch return error.FileTooBig;
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

/// Pins only the small generation checkpoints which are not owned by a
/// physical index backend. This operation is constant in corpus and segment
/// count and is safe to perform while the revision fence is held.
pub fn pinGeneratedCheckpointMetadata(
    alloc: Allocator,
    io: Io,
    source_root: []const u8,
    pin_root: []const u8,
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
    inline for (.{ applied_checkpoint_file_name, repair_checkpoint_file_name }) |name| {
        try ensureActive(cancellation);
        const source = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ source_root, name });
        defer alloc.free(source);
        if (try pathExists(io, source)) {
            const relative = try alloc.dupe(u8, name);
            const pinned_path = std.fmt.allocPrint(alloc, "{s}/{s}", .{ pin_root, relative }) catch |err| {
                alloc.free(relative);
                return err;
            };
            const stat = pinArtifactFile(io, source, pinned_path) catch |err| {
                alloc.free(pinned_path);
                alloc.free(relative);
                return err;
            };
            files.append(alloc, .{ .relative_path = relative, .source = .{ .pinned_file = .{ .path = pinned_path, .stat = stat } } }) catch |err| {
                alloc.free(pinned_path);
                alloc.free(relative);
                return err;
            };
        }
    }
    return try finishPinnedArtifacts(alloc, io, pin_root, &files);
}

fn finishPinnedArtifacts(
    alloc: Allocator,
    io: Io,
    pin_root: []const u8,
    files: *std.ArrayListUnmanaged(PinnedArtifactFile),
) !PinnedGeneratedArtifacts {
    std.mem.sort(PinnedArtifactFile, files.items, {}, struct {
        fn lessThan(_: void, lhs: PinnedArtifactFile, rhs: PinnedArtifactFile) bool {
            return std.mem.order(u8, lhs.relative_path, rhs.relative_path) == .lt;
        }
    }.lessThan);
    if (files.items.len > 1) {
        for (files.items[1..], files.items[0 .. files.items.len - 1]) |current, previous| {
            if (std.mem.eql(u8, current.relative_path, previous.relative_path))
                return error.InvalidNativeBackupArtifactPath;
        }
    }
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

/// Pins a manifest-derived file inventory without walking a live projection
/// directory. The operation is O(number of referenced immutable files) and
/// reads no WAL or corpus payload bytes.
pub fn pinExplicitArtifacts(
    alloc: Allocator,
    io: Io,
    pin_root: []const u8,
    specs: []const ExplicitPinSpec,
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
    try files.ensureTotalCapacity(alloc, specs.len);
    for (specs) |spec| {
        try ensureActive(cancellation);
        try validateRelativePath(spec.relative_path);
        const relative = try alloc.dupe(u8, spec.relative_path);
        errdefer alloc.free(relative);
        var pinned_path: ?[]u8 = null;
        errdefer if (pinned_path) |path| alloc.free(path);
        var leased_reader: ?lsm_backend.storage_io.ColdSequentialReader = null;
        errdefer if (leased_reader) |*reader| reader.deinit();
        const source: PinnedArtifactFile.Source = switch (spec.source) {
            .immutable_file => |source_path| blk: {
                const path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ pin_root, relative });
                pinned_path = path;
                break :blk .{ .pinned_file = .{ .path = path, .stat = try pinArtifactFile(io, source_path, path) } };
            },
            .committed_prefix => |prefix| blk: {
                const reader = try prefix.storage.beginStableSequentialRead(alloc, prefix.path);
                leased_reader = reader;
                break :blk .{ .leased_prefix = .{ .reader = reader, .bytes = prefix.bytes } };
            },
            .bytes => |body| blk: {
                const path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ pin_root, relative });
                pinned_path = path;
                if (std.fs.path.dirname(path)) |parent|
                    try fs_paths.createDirPathPortable(io, parent);
                _ = try writeFileDurable(io, path, body);
                break :blk .{ .pinned_file = .{ .path = path, .stat = try statRegularFile(io, path) } };
            },
        };
        pinned_path = null;
        leased_reader = null;
        files.appendAssumeCapacity(.{
            .relative_path = relative,
            .source = source,
        });
    }
    return try finishPinnedArtifacts(alloc, io, pin_root, &files);
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
    var included_projections = std.StringHashMapUnmanaged(void).empty;
    defer included_projections.deinit(alloc);
    if (projections) |inventory| {
        try included_projections.ensureTotalCapacity(alloc, @intCast(inventory.len));
        for (inventory) |projection| {
            if (projection.artifact_state != .complete) continue;
            const entry = included_projections.getOrPutAssumeCapacity(projection.name);
            if (entry.found_existing) return error.InvalidNativeBackupManifest;
        }
    }
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
                    if (projections != null) {
                        const separator = std.mem.indexOfAny(u8, entry.path, "/\\");
                        const index_name = if (separator) |offset| entry.path[0..offset] else entry.path;
                        if (!included_projections.contains(index_name)) continue;
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
                    try files.append(alloc, .{ .relative_path = relative, .source = .{ .pinned_file = .{ .path = pinned_path, .stat = stat } } });
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
            try files.append(alloc, .{ .relative_path = relative, .source = .{ .pinned_file = .{ .path = pinned_path, .stat = stat } } });
        }
    }

    return try finishPinnedArtifacts(alloc, io, pin_root, &files);
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
        .{
            .artifact_format = "antfly-kv-stream",
            .artifact_version = 2,
            .source_backend = "unknown",
        },
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
    try validateProjectionInventory(capture_target_sequence, projections);
    try validatePrimary(primary);
    var artifacts = try collectArtifacts(alloc, io, snapshot_root, projections, cancellation);
    defer {
        for (artifacts.items) |*artifact| artifact.deinit(alloc);
        artifacts.deinit(alloc);
    }
    return try writeManifestFromArtifacts(
        alloc,
        io,
        snapshot_root,
        capture_target_sequence,
        projections,
        primary,
        artifacts.items,
        cancellation,
    );
}

pub fn finalizeCaptureGenerationFromReceiptsWithCancellation(
    alloc: Allocator,
    io: Io,
    collector: *ArtifactReceiptCollector,
    capture_target_sequence: u64,
    primary: Primary,
    cancellation: CancellationToken,
) !u64 {
    try ensureActive(cancellation);
    try validateProjectionInventory(capture_target_sequence, collector.projections);
    try validatePrimary(primary);
    std.mem.sort(OwnedArtifact, collector.artifacts.items, {}, struct {
        fn lessThan(_: void, lhs: OwnedArtifact, rhs: OwnedArtifact) bool {
            return std.mem.order(u8, lhs.path, rhs.path) == .lt;
        }
    }.lessThan);
    if (collector.artifacts.items.len > 1) {
        for (collector.artifacts.items[1..], collector.artifacts.items[0 .. collector.artifacts.items.len - 1]) |current, previous| {
            if (std.mem.eql(u8, current.path, previous.path)) return error.InvalidNativeBackupManifest;
        }
    }
    return try writeManifestFromArtifacts(
        alloc,
        io,
        collector.snapshot_root,
        capture_target_sequence,
        collector.projections,
        primary,
        collector.artifacts.items,
        cancellation,
    );
}

fn writeManifestFromArtifacts(
    alloc: Allocator,
    io: Io,
    snapshot_root: []const u8,
    capture_target_sequence: u64,
    projections: []const Projection,
    primary: Primary,
    artifacts: []const OwnedArtifact,
    cancellation: CancellationToken,
) !u64 {
    if (artifacts.len == 0) return error.NativeBackupGenerationEmpty;

    const manifest_artifacts = try alloc.alloc(Artifact, artifacts.len);
    defer alloc.free(manifest_artifacts);
    for (artifacts, 0..) |artifact, i| {
        manifest_artifacts[i] = .{
            .path = artifact.path,
            .install_path = artifact.install_path,
            .size_bytes = artifact.size_bytes,
            .sha256 = artifact.sha256,
            .role = artifact.role,
            .projection_name = artifact.projection_name,
        };
    }
    const encoded = try std.json.Stringify.valueAlloc(alloc, Manifest{
        .format_version = format_version,
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

    var loaded = try parseManifestBytes(alloc, raw);
    errdefer loaded.deinit();
    const manifest = loaded.value();

    for (manifest.artifacts) |artifact| {
        try ensureActive(cancellation);
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

    try validateCompleteInventory(alloc, io, snapshot_root, manifest, cancellation);
    for (manifest.artifacts) |artifact| {
        try ensureActive(cancellation);
        if (!isGeneratedArtifact(artifact.path)) continue;
        if (artifact.role == .projection and loaded.projectionInvalid(artifact.projection_name)) continue;
        if (artifact.role == .shared_acceleration and loaded.shared_acceleration_invalid) continue;
        const source = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ snapshot_root, artifact.path });
        defer alloc.free(source);
        const destination = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ destination_root, artifact.install_path });
        defer alloc.free(destination);
        _ = try copyFileDurableCancellable(io, source, destination, cancellation);
    }
    return loaded;
}

/// Parses the one released generation schema and validates its complete
/// logical inventory before any artifact bytes are admitted. The caller may
/// then materialize exactly the declared files from a local or remote source.
pub fn parseManifestBytes(alloc: Allocator, raw: []const u8) !LoadedManifest {
    if (raw.len == 0 or raw.len > max_manifest_bytes)
        return error.InvalidNativeBackupManifest;
    const parsed = std.json.parseFromSlice(Manifest, alloc, raw, .{ .allocate = .alloc_always }) catch
        return error.InvalidNativeBackupManifest;
    var loaded = LoadedManifest{ .alloc = alloc, .parsed = parsed };
    errdefer loaded.deinit();
    const manifest = loaded.value();
    if (manifest.format_version != format_version or
        manifest.artifacts.len == 0 or
        manifest.artifacts.len > max_artifacts)
    {
        return error.InvalidNativeBackupManifest;
    }
    try validateProjectionInventory(manifest.capture_target_sequence, manifest.projections);
    try validatePrimary(manifest.primary);

    var previous_path: ?[]const u8 = null;
    var install_paths = std.StringHashMapUnmanaged(void).empty;
    defer install_paths.deinit(alloc);
    try install_paths.ensureTotalCapacity(alloc, @intCast(manifest.artifacts.len));
    for (manifest.artifacts) |artifact| {
        try validateRelativePath(artifact.path);
        try validateRelativePath(artifact.install_path);
        if (std.mem.eql(u8, artifact.path, manifest_file_name) or
            artifact.sha256.len != Sha256.digest_length * 2)
        {
            return error.InvalidNativeBackupManifest;
        }
        for (artifact.sha256) |byte| {
            if (!std.ascii.isDigit(byte) and !(byte >= 'a' and byte <= 'f'))
                return error.InvalidNativeBackupManifest;
        }
        if (previous_path) |previous| {
            if (std.mem.order(u8, previous, artifact.path) != .lt)
                return error.InvalidNativeBackupManifest;
        }
        previous_path = artifact.path;
        try validateArtifactOwnership(artifact, manifest.projections);
        const install_entry = install_paths.getOrPutAssumeCapacity(artifact.install_path);
        if (install_entry.found_existing) return error.InvalidNativeBackupManifest;
    }
    try validateProjectionArtifactInventory(alloc, manifest);
    try validatePrimaryInventory(manifest);
    for (manifest.projections) |projection| {
        if (projection.artifact_state == .repair_required)
            try loaded.invalidateProjection(projection.name, .capture_repair_required);
    }
    return loaded;
}

pub fn declaredGenerationBytes(manifest: *const Manifest, manifest_size_bytes: u64) !u64 {
    var total = manifest_size_bytes;
    for (manifest.artifacts) |artifact| {
        total = std.math.add(u64, total, artifact.size_bytes) catch
            return error.NativeBackupManifestTooLarge;
    }
    return total;
}

fn downgradeProjectionArtifactFailure(
    loaded: *LoadedManifest,
    artifact: Artifact,
    reason: InvalidProjectionReason,
) !bool {
    if (artifact.role == .shared_acceleration) {
        loaded.shared_acceleration_invalid = true;
        return true;
    }
    if (artifact.role != .projection or artifact.projection_name.len == 0) {
        return false;
    }
    try loaded.invalidateProjection(artifact.projection_name, reason);
    return true;
}

fn validateArtifactOwnership(artifact: Artifact, projections: []const Projection) !void {
    const expected = try artifactOwnership(artifact.path, projections);
    if (artifact.role != expected.role) return error.InvalidNativeBackupManifest;
    const expected_install_path = try artifactInstallPath(artifact.path, expected.role);
    if (!std.mem.eql(u8, artifact.install_path, expected_install_path))
        return error.InvalidNativeBackupManifest;
    switch (artifact.role) {
        .projection => {
            if (artifact.projection_name.len == 0 or
                !std.mem.eql(u8, artifact.projection_name, expected.projection_name))
            {
                return error.InvalidNativeBackupManifest;
            }
        },
        .primary, .shared_acceleration, .metadata => if (artifact.projection_name.len != 0)
            return error.InvalidNativeBackupManifest,
        .legacy => return error.InvalidNativeBackupManifest,
    }
}

fn validateProjectionArtifactInventory(alloc: Allocator, manifest: *const Manifest) !void {
    const artifact_counts = try alloc.alloc(usize, manifest.projections.len);
    defer alloc.free(artifact_counts);
    @memset(artifact_counts, 0);
    for (manifest.artifacts) |artifact| {
        if (artifact.role != .projection) continue;
        const projection_index = findProjectionIndex(manifest.projections, artifact.projection_name) orelse
            return error.InvalidNativeBackupManifest;
        artifact_counts[projection_index] += 1;
    }
    for (manifest.projections, artifact_counts) |projection, artifact_count| {
        switch (projection.artifact_state) {
            .complete => if (!std.mem.eql(u8, projection.kind, "algebraic") and artifact_count == 0)
                return error.InvalidNativeBackupManifest,
            .repair_required => if (artifact_count != 0 or projection.repair_reason.len == 0)
                return error.InvalidNativeBackupManifest,
        }
    }
}

fn validateProjectionInventory(capture_target_sequence: u64, projections: []const Projection) !void {
    if (projections.len > max_artifacts) return error.InvalidNativeBackupManifest;
    var previous_name: ?[]const u8 = null;
    for (projections) |projection| {
        try validateProjectionName(projection.name);
        if (projection.name.len == 0 or
            projection.kind.len == 0 or
            !std.mem.eql(u8, projection.artifact_format, "antfly-managed-index-tree") or
            projection.artifact_version != 1 or
            projection.backend_id.len == 0 or
            std.mem.eql(u8, projection.backend_id, "unknown") or
            projection.codec_version == 0 or
            (projection.artifact_state == .complete and projection.repair_reason.len != 0) or
            (projection.artifact_state == .repair_required and projection.repair_reason.len == 0) or
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

fn validatePrimary(primary: Primary) !void {
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
                const install_path = try artifactInstallPathAlloc(alloc, entry.path, ownership.role);
                errdefer alloc.free(install_path);
                try result.append(alloc, .{
                    .path = path,
                    .install_path = install_path,
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

fn artifactInstallPathAlloc(alloc: Allocator, snapshot_path: []const u8, role: ArtifactRole) ![]u8 {
    return try alloc.dupe(u8, try artifactInstallPath(snapshot_path, role));
}

fn artifactInstallPath(snapshot_path: []const u8, role: ArtifactRole) ![]const u8 {
    const install_path = switch (role) {
        .shared_acceleration => blk: {
            const prefix = indexes_directory_name ++ "/";
            if (!std.mem.startsWith(u8, snapshot_path, prefix))
                return error.InvalidNativeBackupArtifactPath;
            break :blk snapshot_path[prefix.len..];
        },
        .primary => if (std.mem.startsWith(u8, snapshot_path, primary_lsm_directory_name ++ "/"))
            snapshot_path[(primary_lsm_directory_name ++ "/").len..]
        else
            snapshot_path,
        .projection, .metadata => snapshot_path,
        .legacy => return error.InvalidNativeBackupManifest,
    };
    if (install_path.len == 0) return error.InvalidNativeBackupArtifactPath;
    try validateRelativePath(install_path);
    return install_path;
}

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
        if (std.mem.eql(u8, index_name, "vector-blocks"))
            return .{ .role = .shared_acceleration };
        const projection_index = findProjectionIndex(projections, index_name) orelse
            return error.NativeBackupProjectionMismatch;
        return .{ .role = .projection, .projection_name = projections[projection_index].name };
    }
    return error.InvalidNativeBackupManifest;
}

fn findProjectionIndex(projections: []const Projection, name: []const u8) ?usize {
    var low: usize = 0;
    var high = projections.len;
    while (low < high) {
        const mid = low + (high - low) / 2;
        switch (std.mem.order(u8, projections[mid].name, name)) {
            .lt => low = mid + 1,
            .gt => high = mid,
            .eq => return mid,
        }
    }
    return null;
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
    while (try walker.next(io)) |entry| {
        try ensureActive(cancellation);
        switch (entry.kind) {
            .directory => {},
            .file => {
                if (std.mem.eql(u8, entry.path, manifest_file_name)) continue;
                if (!manifestContainsArtifact(manifest, entry.path))
                    return error.InvalidNativeBackupManifest;
            },
            else => return error.UnsupportedFileType,
        }
    }
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
    return try copyFileDurableCancellableWithSink(io, source_path, destination_path, cancellation, null);
}

fn copyFileDurableCancellableWithSink(
    io: Io,
    source_path: []const u8,
    destination_path: []const u8,
    cancellation: CancellationToken,
    sink: ?native_artifact_sink.Sink,
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
    var hasher = Sha256.init(.{});
    var offset: u64 = 0;
    while (offset < initial.size) {
        try ensureActive(cancellation);
        const wanted: usize = @intCast(@min(initial.size - offset, buffer.len));
        const read = try source.readPositionalAll(io, buffer[0..wanted], offset);
        if (read != wanted) return error.SourceFileChanged;
        try writer.interface.writeAll(buffer[0..read]);
        hasher.update(buffer[0..read]);
        offset += read;
    }
    try writer.end();
    try ensureActive(cancellation);
    const final = try source.stat(io);
    if (final.size != initial.size or !std.meta.eql(final.mtime, initial.mtime))
        return error.SourceFileChanged;
    try destination.sync(io);
    if (sink) |active| {
        var digest: [Sha256.digest_length]u8 = undefined;
        hasher.final(&digest);
        try active.record(destination_path, initial.size, digest);
    }
    const parent = std.fs.path.dirname(destination_path) orelse if (std.fs.path.isAbsolute(destination_path)) "/" else ".";
    try fs_paths.syncDirPortable(io, parent);
    return initial.size;
}

fn copyPinnedFileDurable(
    io: Io,
    pinned: *const PinnedArtifactFile,
    destination_path: []const u8,
    cancellation: CancellationToken,
    sink: ?native_artifact_sink.Sink,
) !u64 {
    const file = switch (pinned.source) {
        .pinned_file => |value| value,
        .leased_prefix => unreachable,
    };
    const current = try statRegularFile(io, file.path);
    if (current.inode != file.stat.inode or
        current.size != file.stat.size or
        !std.meta.eql(current.mtime, file.stat.mtime))
    {
        std.log.err("native backup pinned artifact changed before copy path={s}", .{pinned.relative_path});
        return error.SourceFileChanged;
    }
    return copyFileDurableCancellableWithSink(io, file.path, destination_path, cancellation, sink) catch |err| {
        if (err == error.SourceFileChanged)
            std.log.err("native backup pinned artifact changed during copy path={s}", .{pinned.relative_path});
        return err;
    };
}

/// Copies an exact committed append-only prefix from a descriptor acquired
/// while mutation admission was fenced. The descriptor remains valid if
/// CURRENT rotates and unlinks the pathname after admission resumes; appends
/// beyond `prefix_bytes` are deliberately invisible to this generation.
fn copyLeasedPrefixDurable(
    alloc: Allocator,
    io: Io,
    reader: *lsm_backend.storage_io.ColdSequentialReader,
    prefix_bytes: u64,
    destination_path: []const u8,
    cancellation: CancellationToken,
    sink: ?native_artifact_sink.Sink,
) !u64 {
    if (std.fs.path.dirname(destination_path)) |parent|
        try fs_paths.createDirPathPortable(io, parent);
    var destination = try fs_paths.createFilePortable(io, destination_path, .{ .truncate = true });
    defer destination.close(io);
    var writer_buffer: [64 * 1024]u8 = undefined;
    var writer = destination.writer(io, &writer_buffer);
    var hasher = Sha256.init(.{});
    var offset: u64 = 0;
    while (offset < prefix_bytes) {
        try ensureActive(cancellation);
        const wanted: usize = @intCast(@min(prefix_bytes - offset, 256 * 1024));
        const bytes = try reader.readRangeAlloc(alloc, offset, wanted);
        defer alloc.free(bytes);
        if (bytes.len != wanted) return error.SourceFileChanged;
        try writer.interface.writeAll(bytes);
        hasher.update(bytes);
        offset += bytes.len;
    }
    try writer.end();
    try ensureActive(cancellation);
    try destination.sync(io);
    if (sink) |active| {
        var digest: [Sha256.digest_length]u8 = undefined;
        hasher.final(&digest);
        try active.record(destination_path, prefix_bytes, digest);
    }
    const parent = std.fs.path.dirname(destination_path) orelse if (std.fs.path.isAbsolute(destination_path)) "/" else ".";
    try fs_paths.syncDirPortable(io, parent);
    return prefix_bytes;
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

test "explicit native generation pin keeps immutable files and exact WAL prefix" {
    const alloc = std.testing.allocator;
    var native_storage = try lsm_backend.NativeStorage.init(alloc, .threaded);
    defer native_storage.deinit();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer alloc.free(root);
    const segment = try std.fmt.allocPrint(alloc, "{s}/live/segment.afps", .{root});
    defer alloc.free(segment);
    const wal = try std.fmt.allocPrint(alloc, "{s}/live/wal.afpw", .{root});
    defer alloc.free(wal);
    const pin_root = try std.fmt.allocPrint(alloc, "{s}/pin", .{root});
    defer alloc.free(pin_root);
    const snapshot = try std.fmt.allocPrint(alloc, "{s}/snapshot", .{root});
    defer alloc.free(snapshot);
    try fs_paths.createDirPathPortable(std.testing.io, std.fs.path.dirname(segment).?);
    _ = try writeFileDurable(std.testing.io, segment, "immutable-generation");
    _ = try writeFileDurable(std.testing.io, wal, "committed-uncommitted-tail");

    var pinned = try pinExplicitArtifacts(alloc, std.testing.io, pin_root, &.{
        .{
            .relative_path = "indexes/dense/posting-segments/CURRENT",
            .source = .{ .bytes = "manifest" },
        },
        .{
            .relative_path = "indexes/dense/posting-segments/segment-1.afps",
            .source = .{ .immutable_file = segment },
        },
        .{
            .relative_path = "indexes/dense/posting-segments/wal-1.afpw",
            .source = .{ .committed_prefix = .{
                .storage = native_storage.storage(),
                .path = wal,
                .bytes = "committed".len,
            } },
        },
    }, .none);
    defer pinned.deinit();

    // Atomic generation replacement may unlink and recreate the live path;
    // both the hardlinked immutable pin and leased WAL descriptor must retain
    // the selected inode while corpus copying runs outside admission.
    try std.Io.Dir.cwd().deleteFile(std.testing.io, segment);
    _ = try writeFileDurable(std.testing.io, segment, "replacement");
    try std.Io.Dir.cwd().deleteFile(std.testing.io, wal);
    _ = try writeFileDurable(std.testing.io, wal, "replacement-wal");
    _ = try pinned.materialize(snapshot, .none);

    const restored_segment = try std.fmt.allocPrint(alloc, "{s}/indexes/dense/posting-segments/segment-1.afps", .{snapshot});
    defer alloc.free(restored_segment);
    const restored_wal = try std.fmt.allocPrint(alloc, "{s}/indexes/dense/posting-segments/wal-1.afpw", .{snapshot});
    defer alloc.free(restored_wal);
    const segment_bytes = try readFileAlloc(alloc, std.testing.io, restored_segment, 64);
    defer alloc.free(segment_bytes);
    const wal_bytes = try readFileAlloc(alloc, std.testing.io, restored_wal, 64);
    defer alloc.free(wal_bytes);
    try std.testing.expectEqualStrings("immutable-generation", segment_bytes);
    try std.testing.expectEqualStrings("committed", wal_bytes);
}

test "shared vector acceleration corruption preserves native posting authority" {
    const alloc = std.testing.allocator;
    var source_tmp = std.testing.tmpDir(.{});
    defer source_tmp.cleanup();
    var snapshot_tmp = std.testing.tmpDir(.{});
    defer snapshot_tmp.cleanup();
    var destination_tmp = std.testing.tmpDir(.{});
    defer destination_tmp.cleanup();
    var valid_destination_tmp = std.testing.tmpDir(.{});
    defer valid_destination_tmp.cleanup();
    const source = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}", .{source_tmp.sub_path});
    defer alloc.free(source);
    const snapshot = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}", .{snapshot_tmp.sub_path});
    defer alloc.free(snapshot);
    const destination = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}", .{destination_tmp.sub_path});
    defer alloc.free(destination);
    const valid_destination = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}", .{valid_destination_tmp.sub_path});
    defer alloc.free(valid_destination);
    const posting = try std.fmt.allocPrint(alloc, "{s}/indexes/dense/posting-segments/CURRENT", .{source});
    defer alloc.free(posting);
    const vector_block = try std.fmt.allocPrint(alloc, "{s}/indexes/vector-blocks/block-1-0.afvb", .{source});
    defer alloc.free(vector_block);
    try fs_paths.createDirPathPortable(std.testing.io, std.fs.path.dirname(posting).?);
    try fs_paths.createDirPathPortable(std.testing.io, std.fs.path.dirname(vector_block).?);
    _ = try writeFileDurable(std.testing.io, posting, "posting-generation");
    _ = try writeFileDurable(std.testing.io, vector_block, "vector-acceleration");
    const store_file = try std.fmt.allocPrint(alloc, "{s}/store.bin", .{snapshot});
    defer alloc.free(store_file);
    _ = try writeFileDurable(std.testing.io, store_file, "primary");
    _ = try capture(alloc, std.testing.io, source, snapshot, 9, &.{.{
        .name = "dense",
        .kind = "dense_vector",
        .config_hash = 1,
        .coverage_generation = 1,
        .checkpoint_generation = 1,
        .applied_sequence = 9,
        .target_sequence = 9,
        .artifact_format = "antfly-managed-index-tree",
        .artifact_version = 1,
        .backend_id = "hbc-native-v1",
        .codec_version = 1,
        .artifact_state = .complete,
        .repair_reason = "",
    }});
    var valid_loaded = (try validateAndMaterialize(alloc, std.testing.io, snapshot, valid_destination)).?;
    defer valid_loaded.deinit();
    const installed_vector = try std.fmt.allocPrint(alloc, "{s}/vector-blocks/block-1-0.afvb", .{valid_destination});
    defer alloc.free(installed_vector);
    try std.testing.expect(try pathExists(std.testing.io, installed_vector));
    const wrongly_nested_vector = try std.fmt.allocPrint(alloc, "{s}/indexes/vector-blocks/block-1-0.afvb", .{valid_destination});
    defer alloc.free(wrongly_nested_vector);
    try std.testing.expect(!try pathExists(std.testing.io, wrongly_nested_vector));
    const snapshot_vector = try std.fmt.allocPrint(alloc, "{s}/indexes/vector-blocks/block-1-0.afvb", .{snapshot});
    defer alloc.free(snapshot_vector);
    _ = try writeFileDurable(std.testing.io, snapshot_vector, "corrupt");

    var loaded = (try validateAndMaterialize(alloc, std.testing.io, snapshot, destination)).?;
    defer loaded.deinit();
    try std.testing.expect(loaded.shared_acceleration_invalid);
    try std.testing.expect(!loaded.projectionInvalid("dense"));
    const restored_posting = try std.fmt.allocPrint(alloc, "{s}/indexes/dense/posting-segments/CURRENT", .{destination});
    defer alloc.free(restored_posting);
    const restored_vector = try std.fmt.allocPrint(alloc, "{s}/vector-blocks/block-1-0.afvb", .{destination});
    defer alloc.free(restored_vector);
    try std.testing.expect(try pathExists(std.testing.io, restored_posting));
    try std.testing.expect(!try pathExists(std.testing.io, restored_vector));
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
        .artifact_state = .complete,
        .repair_reason = "",
    }});
    const manifest_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ snapshot, manifest_file_name });
    defer alloc.free(manifest_path);
    const manifest_raw = try readFileAlloc(alloc, std.testing.io, manifest_path, max_manifest_bytes);
    defer alloc.free(manifest_raw);
    var parsed_current = try parseManifestBytes(alloc, manifest_raw);
    parsed_current.deinit();
    const unreleased = try alloc.dupe(u8, manifest_raw);
    defer alloc.free(unreleased);
    const version_offset = std.mem.indexOf(u8, unreleased, "\"format_version\":5") orelse
        return error.TestUnexpectedResult;
    unreleased[version_offset + "\"format_version\":".len] = '3';
    try std.testing.expectError(error.InvalidNativeBackupManifest, parseManifestBytes(alloc, unreleased));
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

test "native materialization receipts construct the manifest in one corpus pass" {
    const alloc = std.testing.allocator;
    var source_tmp = std.testing.tmpDir(.{});
    defer source_tmp.cleanup();
    var snapshot_tmp = std.testing.tmpDir(.{});
    defer snapshot_tmp.cleanup();
    const source = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}", .{source_tmp.sub_path});
    defer alloc.free(source);
    const snapshot = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}", .{snapshot_tmp.sub_path});
    defer alloc.free(snapshot);
    const index_source = try std.fmt.allocPrint(alloc, "{s}/indexes/dense/data.bin", .{source});
    defer alloc.free(index_source);
    try fs_paths.createDirPathPortable(std.testing.io, std.fs.path.dirname(index_source).?);
    _ = try writeFileDurable(std.testing.io, index_source, "dense-receipt");
    const primary_source = try std.fmt.allocPrint(alloc, "{s}/primary.bin", .{source});
    defer alloc.free(primary_source);
    _ = try writeFileDurable(std.testing.io, primary_source, "primary-receipt");
    const projection = Projection{
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
        .artifact_state = .complete,
        .repair_reason = "",
    };
    const projections = [_]Projection{projection};
    const pin_root = try std.fmt.allocPrint(alloc, "{s}/.pin", .{snapshot});
    defer alloc.free(pin_root);
    var pinned = try pinGeneratedArtifactsForProjections(
        alloc,
        std.testing.io,
        source,
        pin_root,
        &projections,
        .none,
    );
    defer pinned.deinit();
    var receipts = ArtifactReceiptCollector.init(alloc, snapshot, &projections);
    defer receipts.deinit();
    const sink_value = receipts.sink();
    _ = try pinned.materializeWithSink(snapshot, .none, sink_value);
    const primary_destination = try std.fmt.allocPrint(alloc, "{s}/store.bin", .{snapshot});
    defer alloc.free(primary_destination);
    _ = try copyFileDurableCancellableWithSink(
        std.testing.io,
        primary_source,
        primary_destination,
        .none,
        sink_value,
    );
    _ = try finalizeCaptureGenerationFromReceiptsWithCancellation(
        alloc,
        std.testing.io,
        &receipts,
        12,
        .{
            .artifact_format = "antfly-kv-stream",
            .artifact_version = 2,
            .source_backend = "lmdb",
        },
        .none,
    );

    const manifest_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ snapshot, manifest_file_name });
    defer alloc.free(manifest_path);
    const raw = try readFileAlloc(alloc, std.testing.io, manifest_path, max_manifest_bytes);
    defer alloc.free(raw);
    var loaded = try parseManifestBytes(alloc, raw);
    defer loaded.deinit();
    try std.testing.expectEqual(@as(usize, 2), loaded.value().artifacts.len);
    try std.testing.expectEqualStrings("indexes/dense/data.bin", loaded.value().artifacts[0].path);
    var expected_digest: [Sha256.digest_length]u8 = undefined;
    Sha256.hash("dense-receipt", &expected_digest, .{});
    const expected_hex = std.fmt.bytesToHex(expected_digest, .lower);
    try std.testing.expectEqualStrings(&expected_hex, loaded.value().artifacts[0].sha256);
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
        .artifact_state = .complete,
        .repair_reason = "",
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
        .artifact_state = .complete,
        .repair_reason = "",
    };
    try validateProjectionInventory(12, &.{ first, second });
    try std.testing.expectError(
        error.InvalidNativeBackupManifest,
        validateProjectionInventory(12, &.{ first, first }),
    );

    var stale = second;
    stale.applied_sequence = 11;
    stale.target_sequence = 11;
    try std.testing.expectError(
        error.InvalidNativeBackupManifest,
        validateProjectionInventory(12, &.{ first, stale }),
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
