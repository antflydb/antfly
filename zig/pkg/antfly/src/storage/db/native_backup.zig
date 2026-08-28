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

pub const format_version: u32 = 1;
pub const manifest_file_name = "native-generation.json";
const indexes_directory_name = "indexes";
const applied_checkpoint_file_name = "derived_apply.checkpoint";
const repair_checkpoint_file_name = "index_repair.checkpoint";
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
};

pub const Artifact = struct {
    path: []const u8,
    size_bytes: u64,
    sha256: []const u8,
};

pub const Manifest = struct {
    format_version: u32 = format_version,
    capture_target_sequence: u64,
    artifacts: []const Artifact,
    projections: []const Projection,
};

pub const LoadedManifest = struct {
    parsed: std.json.Parsed(Manifest),

    pub fn deinit(self: *LoadedManifest) void {
        self.parsed.deinit();
    }

    pub fn value(self: *const LoadedManifest) *const Manifest {
        return &self.parsed.value;
    }
};

const OwnedArtifact = struct {
    path: []u8,
    size_bytes: u64,
    sha256: []u8,

    fn deinit(self: *OwnedArtifact, alloc: Allocator) void {
        alloc.free(self.path);
        alloc.free(self.sha256);
    }
};

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

/// Copies the mutable generated portion while the caller owns the DB capture
/// fence. Manifest hashing is deliberately excluded so writes can resume as
/// soon as the revision-consistent files have been staged.
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
    try ensureActive(cancellation);
    var copied_bytes: u64 = 0;
    const source_indexes = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ source_root, indexes_directory_name });
    defer alloc.free(source_indexes);
    if (try pathExists(io, source_indexes)) {
        const snapshot_indexes = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ snapshot_root, indexes_directory_name });
        defer alloc.free(snapshot_indexes);
        copied_bytes = try copyDirectoryDurableCancellable(alloc, io, source_indexes, snapshot_indexes, cancellation);
    }

    inline for (.{ applied_checkpoint_file_name, repair_checkpoint_file_name }) |name| {
        try ensureActive(cancellation);
        const source = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ source_root, name });
        defer alloc.free(source);
        if (try pathExists(io, source)) {
            const destination = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ snapshot_root, name });
            defer alloc.free(destination);
            copied_bytes = std.math.add(u64, copied_bytes, try copyFileDurableCancellable(io, source, destination, cancellation)) catch
                return error.FileTooBig;
        }
    }

    return copied_bytes;
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
    try ensureActive(cancellation);
    try validateProjectionInventory(capture_target_sequence, projections);
    var artifacts = try collectArtifacts(alloc, io, snapshot_root, cancellation);
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
        };
    }
    const encoded = try std.json.Stringify.valueAlloc(alloc, Manifest{
        .capture_target_sequence = capture_target_sequence,
        .artifacts = manifest_artifacts,
        .projections = projections,
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
    const manifest_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ snapshot_root, manifest_file_name });
    defer alloc.free(manifest_path);
    const raw = readFileAlloc(alloc, io, manifest_path, max_manifest_bytes) catch |err| switch (err) {
        error.FileNotFound => return null,
        else => return err,
    };
    defer alloc.free(raw);

    var parsed = std.json.parseFromSlice(Manifest, alloc, raw, .{ .allocate = .alloc_always }) catch
        return error.InvalidNativeBackupManifest;
    errdefer parsed.deinit();
    const manifest = &parsed.value;
    if (manifest.format_version != format_version or
        manifest.artifacts.len == 0 or
        manifest.artifacts.len > max_artifacts)
    {
        return error.InvalidNativeBackupManifest;
    }
    try validateProjectionInventory(manifest.capture_target_sequence, manifest.projections);

    var previous_path: ?[]const u8 = null;
    for (manifest.artifacts) |artifact| {
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

        const source = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ snapshot_root, artifact.path });
        defer alloc.free(source);
        const stat = statRegularFile(io, source) catch |err| switch (err) {
            error.FileNotFound => return error.NativeBackupArtifactMissing,
            else => return err,
        };
        if (stat.size != artifact.size_bytes) return error.NativeBackupArtifactIntegrityMismatch;
        var digest: [Sha256.digest_length]u8 = undefined;
        try hashFile(io, source, stat, &digest, .none);
        const actual = std.fmt.bytesToHex(digest, .lower);
        if (!std.mem.eql(u8, &actual, artifact.sha256))
            return error.NativeBackupArtifactIntegrityMismatch;
    }

    try validateCompleteInventory(alloc, io, snapshot_root, manifest);
    for (manifest.artifacts) |artifact| {
        if (!isGeneratedArtifact(artifact.path)) continue;
        const source = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ snapshot_root, artifact.path });
        defer alloc.free(source);
        const destination = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ destination_root, artifact.path });
        defer alloc.free(destination);
        _ = try fs_paths.copyFileDurablePortable(io, source, destination);
    }
    return .{ .parsed = parsed };
}

fn validateProjectionInventory(capture_target_sequence: u64, projections: []const Projection) !void {
    if (projections.len > max_artifacts) return error.InvalidNativeBackupManifest;
    var previous_name: ?[]const u8 = null;
    for (projections) |projection| {
        if (projection.name.len == 0 or
            projection.kind.len == 0 or
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

fn isGeneratedArtifact(path: []const u8) bool {
    return std.mem.startsWith(u8, path, indexes_directory_name ++ "/") or
        std.mem.eql(u8, path, applied_checkpoint_file_name) or
        std.mem.eql(u8, path, repair_checkpoint_file_name);
}

fn collectArtifacts(alloc: Allocator, io: Io, root: []const u8, cancellation: CancellationToken) !std.ArrayListUnmanaged(OwnedArtifact) {
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
                try result.append(alloc, .{
                    .path = path,
                    .size_bytes = stat.size,
                    .sha256 = sha256,
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

fn validateCompleteInventory(alloc: Allocator, io: Io, root: []const u8, manifest: *const Manifest) !void {
    var dir = if (std.fs.path.isAbsolute(root))
        try std.Io.Dir.openDirAbsolute(io, root, .{ .iterate = true })
    else
        try std.Io.Dir.cwd().openDir(io, root, .{ .iterate = true });
    defer dir.close(io);
    var walker = try dir.walk(alloc);
    defer walker.deinit();
    var files: usize = 0;
    while (try walker.next(io)) |entry| switch (entry.kind) {
        .directory => {},
        .file => {
            if (std.mem.eql(u8, entry.path, manifest_file_name)) continue;
            files += 1;
            if (!manifestContainsArtifact(manifest, entry.path))
                return error.InvalidNativeBackupManifest;
        },
        else => return error.UnsupportedFileType,
    };
    if (files != manifest.artifacts.len) return error.InvalidNativeBackupManifest;
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

fn copyDirectoryDurableCancellable(
    alloc: Allocator,
    io: Io,
    source_path: []const u8,
    destination_path: []const u8,
    cancellation: CancellationToken,
) !u64 {
    try fs_paths.createDirPathPortable(io, destination_path);
    var source = if (std.fs.path.isAbsolute(source_path))
        try std.Io.Dir.openDirAbsolute(io, source_path, .{ .iterate = true })
    else
        try std.Io.Dir.cwd().openDir(io, source_path, .{ .iterate = true });
    defer source.close(io);

    var walker = try source.walk(alloc);
    defer walker.deinit();
    var total: u64 = 0;
    while (try walker.next(io)) |entry| {
        try ensureActive(cancellation);
        const destination = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ destination_path, entry.path });
        defer alloc.free(destination);
        switch (entry.kind) {
            .directory => try fs_paths.createDirPathPortable(io, destination),
            .file => {
                const source_file = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ source_path, entry.path });
                defer alloc.free(source_file);
                total = std.math.add(u64, total, try copyFileDurableCancellable(
                    io,
                    source_file,
                    destination,
                    cancellation,
                )) catch return error.FileTooBig;
            },
            else => return error.UnsupportedFileType,
        }
    }
    try ensureActive(cancellation);
    try fs_paths.syncDirPortable(io, destination_path);
    return total;
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
    try std.testing.expectError(
        error.NativeBackupArtifactIntegrityMismatch,
        validateAndMaterialize(alloc, std.testing.io, snapshot, destination),
    );
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
    };
    const second = Projection{
        .name = "dense_b",
        .kind = "dense_vector",
        .config_hash = 9,
        .coverage_generation = 4,
        .checkpoint_generation = 4,
        .applied_sequence = 12,
        .target_sequence = 12,
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
