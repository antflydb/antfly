// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

const std = @import("std");

const builtin = @import("builtin");

const Allocator = std.mem.Allocator;

const backup_codec = @import("../backup_codec.zig");
const backup_bundle = @import("../backup_bundle.zig");
const backup_bundle_io = @import("../backup_bundle_io.zig");
const backups_api = @import("../../api/backups.zig");
const connection = @import("portable_restore.zig").connection;

const db_mod = @import("portable_restore.zig").db_mod;

const db_types = @import("../db/types.zig");
const group_ids = @import("../../common/group_ids.zig");
const internal_keys = @import("../internal_keys.zig");
const portable_backup = @import("../portable_backup.zig");
const query_api = @import("../../api/query.zig");
const tables_api = @import("../../api/tables.zig");
const table_writes = @import("../../api/table_writes.zig");
const fs_paths = @import("portable_restore.zig").fs_paths;

const full_text_index_defaults = @import("../../common/full_text_index_defaults.zig");

pub const max_afb_file_bytes: usize = 64 * 1024 * 1024 * 1024;

pub const PreparationOptions = struct {
    max_input_bytes: u64 = max_afb_file_bytes,
    max_prepared_bytes: u64 = 128 * 1024 * 1024 * 1024,
};

const LiteDb = @import("portable_restore.zig").LiteDb;

const portable_generation_lease_magic = @import("portable_restore.zig").portable_generation_lease_magic;

pub const confirmPublishedFileDurability = @import("portable_restore.zig").confirmPublishedFileDurability;

pub const failNextPublishedFileDirectorySyncForTest = @import("portable_restore.zig").failNextPublishedFileDirectorySyncForTest;

pub const isImportTargetEmpty = @import("portable_restore.zig").isImportTargetEmpty;

pub const finalizeRestoredLiteDb = @import("portable_restore.zig").finalizeRestoredLiteDb;

pub const populateUnpublishedLiteDb = @import("portable_restore.zig").populateUnpublishedLiteDb;

pub const populateUnpublishedLiteDbFromPortableFile = @import("portable_restore.zig").populateUnpublishedLiteDbFromPortableFile;

pub const importPortableIntoLiteDb = @import("portable_restore.zig").importPortableIntoLiteDb;

pub const StagedRestore = struct {
    backup_id: []const u8,
    location: []const u8,
    snapshot_path: []const u8,
    table_name: []const u8,

    pub fn deinit(self: *StagedRestore, allocator: Allocator) void {
        allocator.free(self.backup_id);
        allocator.free(self.location);
        allocator.free(self.snapshot_path);
        allocator.free(self.table_name);
        self.* = undefined;
    }
};

const PortableManifestMetadata = struct {
    schema_json: []const u8,
    indexes_json: []const u8,

    pub fn deinit(self: *PortableManifestMetadata, allocator: Allocator) void {
        allocator.free(self.schema_json);
        allocator.free(self.indexes_json);
        self.* = undefined;
    }
};

pub const preflight = @import("backup_preflight.zig");

/// Always returns a versioned report for source failures. Partial counts are
/// explicitly incomplete, and no failed scan can produce a compatible verdict.
pub fn inspectInputJsonAlloc(allocator: Allocator, input_path: []const u8, destination_json: ?[]const u8, options: preflight.Options) ![]u8 {
    var report: preflight.Report = .{ .allocator = allocator };
    defer report.deinit();
    inspectInput(allocator, input_path, options, &report) catch |err| {
        if (err == error.OutOfMemory) return err;
        report.analysis_complete = false;
        report.failure_code = switch (err) {
            error.Canceled => "cancelled",
            error.BackupInputLimitExceeded, error.BackupExpansionLimitExceeded, error.PreflightLimitExceeded => "limit_exceeded",
            error.SourceFileChanged => "source_changed",
            error.WriterLocked => "source_busy",
            error.BackupBaseRequired => "base_required",
            else => "input_uninspectable",
        };
    };
    return report.jsonAlloc(destination_json);
}

fn inspectInput(allocator: Allocator, input_path: []const u8, options: preflight.Options, report: *preflight.Report) !void {
    var io_impl = std.Io.Threaded.init(allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    try options.cancellation.check();
    var source = try openInputFile(io, input_path);
    defer source.close(io);
    if (!try source.tryLock(io, .shared)) return error.WriterLocked;
    defer source.unlock(io);
    try checkInputLimit(io, source, options.max_input_bytes);
    const initial = try source.stat(io);
    report.source_bytes = initial.size;
    const digest = try preflightFileDigest(io, source, initial.size, options);
    var magic: [8]u8 = undefined;
    if (try source.readPositionalAll(io, &magic, 0) != magic.len) return error.InvalidMagic;
    if (std.mem.startsWith(u8, &magic, "AFLITE")) {
        report.format = "lite";
        report.format_version = magic[6];
        var lite = try LiteDb.open(allocator, input_path, .query_readonly);
        defer lite.close();
        // The reopened path must still identify the descriptor hashed above.
        var reopened = try openInputFile(io, input_path);
        defer reopened.close(io);
        if (!backups_api.localArtifactStatsEqual(initial, try reopened.stat(io))) return error.SourceFileChanged;
        try inspectStore(allocator, io, lite.db.core.store, options, report, null);
    } else if (backup_codec.isAfbFormat(&magic)) {
        var framing = backup_codec.FileReader.init(io, source, initial.size);
        const header = try framing.readHeader();
        if (header.table_count != 1) return error.InvalidBackupRequest;
        report.format_version = header.format_version;
        var native = false;
        if (header.format_version == backup_codec.format_version) {
            const max_block_bytes = std.math.cast(usize, options.max_expanded_bytes) orelse std.math.maxInt(usize);
            var root = try backup_bundle_io.readManifestFromFileLimited(allocator, io, source, initial.size, max_block_bytes);
            defer root.deinit();
            if (root.value.mode != .full) return error.BackupBaseRequired;
            try backup_bundle.validateReadablePayloadFeatures(root.value);
            // Native extraction can retain both blob and materialized copies;
            // reserve half the budget for that overlap and portable scanning.
            native = root.value.representation == .native;
            try backup_bundle.validatePreparationSizeLimit(root.value, if (native) options.max_expanded_bytes / 2 else options.max_expanded_bytes);
            if (native) {
                report.format = "native";
                const scratch = try temporaryPreparationPath(allocator, io, "antfly-inspect-", "");
                defer allocator.free(scratch);
                defer std.Io.Dir.cwd().deleteTree(io, scratch) catch {};
                try backup_bundle_io.extractNativeFileToStagingDirectoryCancellableLimited(allocator, io, source, initial.size, scratch, options.cancellation, max_block_bytes);
                var manifest = try backups_api.readManifest(allocator, scratch, root.value.backup_id);
                defer manifest.deinit(allocator);
                if (manifest.format != .native or !std.mem.eql(u8, manifest.table_name, root.value.table_name)) return error.BackupArtifactFormatMismatch;
                // The table manifest is executable metadata as well as the
                // per-shard catalogs. Inspect it independently before all shards.
                var inspector: preflight.Inspector = .{ .allocator = allocator, .report = report, .options = options };
                defer inspector.deinit();
                try inspector.indexCatalogJson(manifest.indexes_json);
                try inspector.replicationSourcesJson(manifest.replication_sources_json);
                for (manifest.shards) |shard| {
                    try options.cancellation.check();
                    const canonical_path = try backups_api.shardSnapshotRelPath(allocator, manifest.backup_id, shard.group_id);
                    defer allocator.free(canonical_path);
                    if (!std.mem.eql(u8, canonical_path, shard.snapshot_path)) return error.InvalidBackupManifest;
                    const path = try std.fmt.allocPrintSentinel(allocator, "{s}/{s}", .{ scratch, shard.snapshot_path }, 0);
                    defer allocator.free(path);
                    var store = try @import("../docstore.zig").DocStore.open(allocator, path, .{ .read_only = true });
                    defer store.close();
                    try inspectStore(allocator, io, &store, options, report, manifest.indexes_json);
                }
            }
        }
        if (!native) {
            report.format = "portable";
            try inspectPortable(allocator, io, source, options, report, null);
        }
    } else return error.InvalidMagic;
    if (!backups_api.localArtifactStatsEqual(initial, try source.stat(io))) return error.SourceFileChanged;
    const final_digest = try preflightFileDigest(io, source, initial.size, options);
    if (!std.mem.eql(u8, &digest, &final_digest)) return error.SourceFileChanged;
    // Path replacement is distinct from in-place descriptor mutation.
    var final_source = try openInputFile(io, input_path);
    defer final_source.close(io);
    if (!backups_api.localArtifactStatsEqual(initial, try final_source.stat(io))) return error.SourceFileChanged;
    report.source_sha256 = digest;
    report.scan_complete = true;
    report.counts_complete = report.analysis_complete;
}

fn preflightFileDigest(io: std.Io, source: std.Io.File, size: u64, options: preflight.Options) !preflight.Digest {
    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    var buffer: [64 * 1024]u8 = undefined;
    var offset: u64 = 0;
    while (offset < size) {
        try options.cancellation.check();
        const want: usize = @intCast(@min(buffer.len, size - offset));
        if (try source.readPositionalAll(io, buffer[0..want], offset) != want) return error.SourceFileChanged;
        hasher.update(buffer[0..want]);
        offset += want;
    }
    var digest: [32]u8 = undefined;
    hasher.final(&digest);
    return std.fmt.bytesToHex(digest, .lower);
}

fn inspectStore(allocator: Allocator, io: std.Io, store: *@import("../docstore.zig").DocStore, options: preflight.Options, report: *preflight.Report, catalog: ?[]const u8) !void {
    const scratch = try temporaryPortablePath(allocator, io);
    defer allocator.free(scratch);
    defer std.Io.Dir.cwd().deleteFile(io, scratch) catch {};
    var file = try fs_paths.createFilePortable(io, scratch, .{ .read = true, .exclusive = true, .permissions = .fromMode(0o600) });
    defer file.close(io);
    var buffer: [64 * 1024]u8 = undefined;
    var writer = file.writer(io, &buffer);
    var limited = LimitedWriter.init(&writer.interface, options.max_expanded_bytes / 2);
    portable_backup.exportPortableToWriterWithOptions(allocator, store, &limited.interface, .{ .cancellation = options.cancellation }) catch |err| {
        if (limited.exceeded) return error.BackupExpansionLimitExceeded;
        return err;
    };
    try writer.end();
    var normalized = options;
    normalized.max_input_bytes = options.max_expanded_bytes / 2;
    try inspectPortable(allocator, io, file, normalized, report, catalog);
}

fn inspectPortable(allocator: Allocator, io: std.Io, file: std.Io.File, options: preflight.Options, report: *preflight.Report, catalog: ?[]const u8) !void {
    var inspector: preflight.Inspector = .{ .allocator = allocator, .report = report, .options = options };
    defer inspector.deinit();
    if (catalog) |json| try inspector.indexCatalogJson(json);
    _ = try portable_backup.inspectPortableFileBlocks(allocator, io, file, .{ .metadata_first = true, .max_input_bytes = options.max_input_bytes, .max_logical_bytes = options.max_expanded_bytes, .cancellation = options.cancellation }, &inspector, preflight.Inspector.visit);
}

pub fn defaultBackupIdAlloc(allocator: Allocator, path: []const u8) ![]u8 {
    const base = std.fs.path.basename(path);
    const stem = if (std.mem.endsWith(u8, base, ".aflite"))
        base[0 .. base.len - ".aflite".len]
    else if (std.mem.endsWith(u8, base, ".afb2"))
        base[0 .. base.len - ".afb2".len]
    else if (std.mem.endsWith(u8, base, ".afb"))
        base[0 .. base.len - ".afb".len]
    else
        base;
    var out = try std.ArrayListUnmanaged(u8).initCapacity(allocator, "lite-".len + stem.len);
    errdefer out.deinit(allocator);
    try out.appendSlice(allocator, "lite-");
    for (stem) |byte| {
        try out.append(allocator, if (std.ascii.isAlphanumeric(byte) or byte == '-' or byte == '_') byte else '-');
    }
    return try out.toOwnedSlice(allocator);
}

pub fn stageInputRestoreBackup(
    allocator: Allocator,
    input_path: []const u8,
    table_name: []const u8,
    backup_id: []const u8,
    location_uri: []const u8,
) !StagedRestore {
    return stageInputRestoreBackupWithOptions(allocator, input_path, table_name, backup_id, location_uri, .{});
}

/// Prepares an isolated restore source; this is not destination/dependency
/// admission. Callers must keep the repository private until their preflight
/// and create-only target admission have completed.
pub fn stageInputRestoreBackupWithOptions(
    allocator: Allocator,
    input_path: []const u8,
    table_name: []const u8,
    backup_id: []const u8,
    location_uri: []const u8,
    options: PreparationOptions,
) !StagedRestore {
    var owned_backup_id: ?[]u8 = null;
    defer if (owned_backup_id) |value| allocator.free(value);
    const resolved_backup_id = if (backup_id.len > 0)
        backup_id
    else blk: {
        owned_backup_id = try defaultBackupIdAlloc(allocator, input_path);
        break :blk owned_backup_id.?;
    };
    var io_impl = std.Io.Threaded.init(allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    var source = try openInputFile(io, input_path);
    defer source.close(io);
    try checkInputLimit(io, source, options.max_input_bytes);
    var magic: [8]u8 = undefined;
    if (try source.readPositionalAll(io, &magic, 0) != magic.len) return error.InvalidMagic;
    if (std.mem.startsWith(u8, &magic, "AFLITE"))
        return stageAfliteRestoreBackupWithOptions(allocator, input_path, table_name, resolved_backup_id, location_uri, options);
    if (backup_codec.isAfbFormat(&magic))
        return stageAfbRestoreBackupWithOptions(allocator, input_path, table_name, backup_id, resolved_backup_id, location_uri, options);
    return error.InvalidMagic;
}

fn checkInputLimit(io: std.Io, source: std.Io.File, max_bytes: u64) !void {
    const stat = try source.stat(io);
    if (stat.kind != .file) return error.InvalidBackupRequest;
    if (stat.size > max_bytes) return error.BackupInputLimitExceeded;
}

fn openInputFile(io: std.Io, path: []const u8) !std.Io.File {
    return if (std.fs.path.isAbsolute(path))
        std.Io.Dir.openFileAbsolute(io, path, .{})
    else
        std.Io.Dir.cwd().openFile(io, path, .{});
}

pub fn stageAfliteRestoreBackup(
    allocator: Allocator,
    path: []const u8,
    table_name: []const u8,
    backup_id: []const u8,
    location_uri: []const u8,
) !StagedRestore {
    return stageAfliteRestoreBackupWithOptions(allocator, path, table_name, backup_id, location_uri, .{});
}

fn stageAfliteRestoreBackupWithOptions(
    allocator: Allocator,
    path: []const u8,
    table_name: []const u8,
    backup_id: []const u8,
    location_uri: []const u8,
    options: PreparationOptions,
) !StagedRestore {
    var io_impl = std.Io.Threaded.init(allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    var source = try openInputFile(io, path);
    defer source.close(io);
    try checkInputLimit(io, source, options.max_input_bytes);
    var lite = try LiteDb.open(allocator, path, .query_readonly);
    defer lite.close();

    const scratch = try temporaryPortablePath(allocator, io);
    defer allocator.free(scratch);
    defer std.Io.Dir.cwd().deleteFile(io, scratch) catch {};
    var output = try fs_paths.createFilePortable(io, scratch, .{ .read = true, .exclusive = true, .permissions = .fromMode(0o600) });
    defer output.close(io);
    var buffer: [64 * 1024]u8 = undefined;
    var writer = output.writer(io, &buffer);
    var limited = LimitedWriter.init(&writer.interface, options.max_prepared_bytes);
    portable_backup.exportPortableToWriter(allocator, lite.db.core.store, &limited.interface) catch |err| {
        if (limited.exceeded) return error.BackupExpansionLimitExceeded;
        return err;
    };
    try writer.end();
    try output.sync(io);
    // Read metadata from the exported snapshot, not a separate live DB read.
    // Both metadata and records must describe the same pinned read transaction.
    return stagePortableFile(allocator, io, output, scratch, table_name, backup_id, location_uri, options.max_prepared_bytes, options.max_prepared_bytes);
}

fn temporaryPortablePath(allocator: Allocator, io: std.Io) ![]u8 {
    return temporaryPreparationPath(allocator, io, "antfly-prepare-", ".afb");
}

fn temporaryPreparationPath(allocator: Allocator, io: std.Io, prefix: []const u8, suffix: []const u8) ![]u8 {
    var nonce: [16]u8 = undefined;
    try io.randomSecure(&nonce);
    const env = @import("antfly_platform").env;
    const directory = env.getenv("TMPDIR") orelse env.getenv("TEMP") orelse "/tmp";
    const name = try std.fmt.allocPrint(allocator, "{s}{s}{s}", .{ prefix, std.fmt.bytesToHex(nonce, .lower), suffix });
    defer allocator.free(name);
    return std.fs.path.join(allocator, &.{ directory, name });
}

/// No archive-sized buffer, and the cap is checked before forwarding bytes.
const LimitedWriter = struct {
    interface: std.Io.Writer,
    output: *std.Io.Writer,
    remaining: u64,
    exceeded: bool = false,

    fn init(output: *std.Io.Writer, limit: u64) LimitedWriter {
        return .{ .interface = .{ .vtable = &.{ .drain = drain }, .buffer = &.{} }, .output = output, .remaining = limit };
    }

    fn drain(writer: *std.Io.Writer, data: []const []const u8, splat: usize) std.Io.Writer.Error!usize {
        const self: *LimitedWriter = @fieldParentPtr("interface", writer);
        var count: u64 = 0;
        for (data[0 .. data.len - 1]) |part| {
            count = std.math.add(u64, count, part.len) catch return self.limitExceeded();
        }
        const repeated = std.math.mul(u64, data[data.len - 1].len, splat) catch return self.limitExceeded();
        count = std.math.add(u64, count, repeated) catch return self.limitExceeded();
        if (count > self.remaining) return self.limitExceeded();
        const written = try self.output.writeSplat(data, splat);
        self.remaining -= written;
        return written;
    }

    fn limitExceeded(self: *LimitedWriter) std.Io.Writer.Error {
        self.exceeded = true;
        return error.WriteFailed;
    }
};

pub fn stageAfbRestoreBackup(
    allocator: Allocator,
    path: []const u8,
    table_name: []const u8,
    requested_backup_id: []const u8,
    portable_backup_id: []const u8,
    location_uri: []const u8,
) !StagedRestore {
    return stageAfbRestoreBackupWithOptions(allocator, path, table_name, requested_backup_id, portable_backup_id, location_uri, .{});
}

fn stageAfbRestoreBackupWithOptions(
    allocator: Allocator,
    path: []const u8,
    table_name: []const u8,
    requested_backup_id: []const u8,
    portable_backup_id: []const u8,
    location_uri: []const u8,
    options: PreparationOptions,
) !StagedRestore {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    var source = if (std.fs.path.isAbsolute(path))
        try std.Io.Dir.openFileAbsolute(io, path, .{})
    else
        try std.Io.Dir.cwd().openFile(io, path, .{});
    defer source.close(io);
    // Pin the source before reading any admission metadata. Native extraction
    // rereads the manifest and must consume the inventory checked below.
    if (!try source.tryLock(io, .shared)) return error.WriterLocked;
    defer source.unlock(io);
    try checkInputLimit(io, source, options.max_input_bytes);
    const source_stat = try source.stat(io);
    var framing = backup_codec.FileReader.init(io, source, source_stat.size);
    const header = try framing.readHeader();
    if (header.table_count != 1) return error.InvalidBackupRequest;
    if (header.format_version != backup_codec.legacy_format_version and
        header.format_version != backup_codec.format_version)
        return error.BackupArtifactFormatMismatch;
    if (header.format_version == backup_codec.legacy_format_version)
        return try stagePortableFile(allocator, io, source, path, table_name, portable_backup_id, location_uri, options.max_input_bytes, options.max_prepared_bytes);
    var root_manifest = try backup_bundle_io.readManifestFromFile(allocator, io, source, source_stat.size);
    defer root_manifest.deinit();
    if (root_manifest.value.mode != .full) return error.BackupBaseRequired;
    try backup_bundle.validateReadablePayloadFeatures(root_manifest.value);
    try backup_bundle.validatePreparationSizeLimit(root_manifest.value, options.max_prepared_bytes);
    if (root_manifest.value.representation == .native) {
        return try stageNativeAfbRestoreBackup(
            allocator,
            io,
            source,
            source_stat,
            table_name,
            requested_backup_id,
            location_uri,
            root_manifest.value,
        );
    }

    return try stagePortableFile(allocator, io, source, path, table_name, portable_backup_id, location_uri, options.max_input_bytes, options.max_prepared_bytes);
}

fn stagePortableFile(
    allocator: Allocator,
    io: std.Io,
    source: std.Io.File,
    path: []const u8,
    table_name: []const u8,
    portable_backup_id: []const u8,
    location_uri: []const u8,
    max_input_bytes: u64,
    max_prepared_bytes: u64,
) !StagedRestore {
    try backups_api.validateBackupId(portable_backup_id);
    const initial_stat = try source.stat(io);
    if (initial_stat.size > max_prepared_bytes) return error.BackupExpansionLimitExceeded;
    const metadata = try portableFileMetadataAlloc(allocator, io, source, .{
        .max_input_bytes = max_input_bytes,
        .max_logical_bytes = max_prepared_bytes,
    });
    var metadata_owned = true;
    defer if (metadata_owned) {
        allocator.free(metadata.schema_json);
        allocator.free(metadata.indexes_json);
    };

    // Pin the verified file in private scratch before opening a remote location.
    // Copy + hash is bounded and rejects a replaced or changed input descriptor.
    const scratch = try temporaryPortablePath(allocator, io);
    defer allocator.free(scratch);
    defer std.Io.Dir.cwd().deleteFile(io, scratch) catch {};
    var scratch_file = try fs_paths.createFilePortable(io, scratch, .{ .exclusive = true, .permissions = .fromMode(0o600) });
    scratch_file.close(io);
    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    try backups_api.copyFileAndHashCancellable(io, path, scratch, initial_stat, &hasher, .none);
    if (!backups_api.localArtifactStatsEqual(try source.stat(io), initial_stat)) return error.SourceFileChanged;
    var digest: [32]u8 = undefined;
    hasher.final(&digest);
    const digest_hex = std.fmt.bytesToHex(digest, .lower);
    const integrity: backups_api.ArtifactIntegrity = .{
        .size_bytes = initial_stat.size,
        .sha256 = try allocator.dupe(u8, &digest_hex),
    };
    var integrity_owned = true;
    defer if (integrity_owned) allocator.free(integrity.sha256);
    const snapshot_path = try std.fmt.allocPrint(allocator, "{s}.afb", .{portable_backup_id});
    errdefer allocator.free(snapshot_path);
    const manifest = try manifestFromParts(allocator, table_name, portable_backup_id, snapshot_path, integrity, metadata.schema_json, metadata.indexes_json, "Restored from portable Antfly backup");
    metadata_owned = false;
    integrity_owned = false;
    defer freeManifest(allocator, manifest);
    var location = try backups_api.openBackupLocation(allocator, location_uri);
    defer location.deinit(allocator);
    switch (location) {
        .file => |root| {
            const destination = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ root, snapshot_path });
            defer allocator.free(destination);
            try backups_api.copyFileAbsoluteWithIoOptions(io, scratch, destination, .durable);
        },
        .remote => |*store| try store.writeFile(allocator, io, snapshot_path, scratch, "application/vnd.antfly.backup", .none),
    }
    try backups_api.writeManifestToLocation(allocator, &location, &manifest);
    return .{
        .backup_id = try allocator.dupe(u8, portable_backup_id),
        .location = try allocator.dupe(u8, location_uri),
        .snapshot_path = snapshot_path,
        .table_name = try allocator.dupe(u8, table_name),
    };
}

fn stageNativeAfbRestoreBackup(
    allocator: Allocator,
    io: std.Io,
    source: std.Io.File,
    initial_stat: std.Io.File.Stat,
    target_table_name: []const u8,
    requested_backup_id: []const u8,
    location_uri: []const u8,
    root_manifest: backup_bundle.Manifest,
) !StagedRestore {
    if (root_manifest.mode != .full or root_manifest.backup_id.len == 0 or root_manifest.table_name.len == 0)
        return error.BackupArtifactFormatMismatch;
    if (requested_backup_id.len > 0 and !std.mem.eql(u8, requested_backup_id, root_manifest.backup_id))
        return error.BackupIdentityMismatch;

    // The caller holds the shared lock and supplies the stat from before its
    // header/manifest reads. Also reject writers that ignore advisory locks.
    if (!backups_api.localArtifactStatsEqual(try source.stat(io), initial_stat)) return error.SourceFileChanged;
    const staging_root = try temporaryPreparationPath(allocator, io, "antfly-afb2-", "");
    defer allocator.free(staging_root);
    defer std.Io.Dir.cwd().deleteTree(io, staging_root) catch {};
    try backup_bundle_io.extractNativeFileToStagingDirectory(allocator, io, source, initial_stat.size, staging_root);

    var source_manifest = try backups_api.readManifest(allocator, staging_root, root_manifest.backup_id);
    defer source_manifest.deinit(allocator);
    if (source_manifest.format != .native or !std.mem.eql(u8, source_manifest.table_name, root_manifest.table_name))
        return error.BackupArtifactFormatMismatch;

    if (!backups_api.localArtifactStatsEqual(try source.stat(io), initial_stat)) return error.SourceFileChanged;
    var location = try backups_api.openBackupLocation(allocator, location_uri);
    defer location.deinit(allocator);
    for (source_manifest.shards) |shard| {
        const canonical_path = try backups_api.shardSnapshotRelPath(allocator, source_manifest.backup_id, shard.group_id);
        defer allocator.free(canonical_path);
        if (!std.mem.eql(u8, canonical_path, shard.snapshot_path)) return error.InvalidBackupManifest;
        const extracted_shard = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ staging_root, shard.snapshot_path });
        defer allocator.free(extracted_shard);
        try backups_api.copyDirectoryToLocation(allocator, &location, source_manifest.backup_id, shard.group_id, extracted_shard);
    }
    var target_manifest = try backups_api.deriveRestoreManifestForTargetTable(
        allocator,
        source_manifest,
        target_table_name,
    );
    defer target_manifest.deinit(allocator);
    try backups_api.writeManifestToLocation(allocator, &location, &target_manifest);

    return .{
        .backup_id = try allocator.dupe(u8, target_manifest.backup_id),
        .location = try allocator.dupe(u8, location_uri),
        .snapshot_path = try allocator.dupe(u8, target_manifest.shards[0].snapshot_path),
        .table_name = try allocator.dupe(u8, target_table_name),
    };
}

fn portableFileMetadataAlloc(allocator: Allocator, io: std.Io, file: std.Io.File, options: portable_backup.FileInspectionOptions) !PortableManifestMetadata {
    var schema_json = try allocator.dupe(u8, "{}");
    errdefer allocator.free(schema_json);
    var raw_indexes_json: ?[]u8 = null;
    defer if (raw_indexes_json) |value| allocator.free(value);
    var raw_enrichments_json: ?[]u8 = null;
    defer if (raw_enrichments_json) |value| allocator.free(value);

    const Visitor = struct {
        allocator: Allocator,
        schema_json: *[]u8,
        raw_indexes_json: *?[]u8,
        raw_enrichments_json: *?[]u8,

        fn visit(self: *@This(), block_type: backup_codec.BlockType, payload: []const u8) !void {
            if (block_type != .metadata_batch) return;
            const entries = try backup_codec.decodeKeyValueBatch(self.allocator, payload);
            defer freeKeyValueEntries(self.allocator, entries);
            for (entries) |entry| {
                if (std.mem.eql(u8, entry.key, "\x00\x00__metadata__:schema_json")) {
                    self.allocator.free(self.schema_json.*);
                    self.schema_json.* = try self.allocator.dupe(u8, entry.value);
                } else if (std.mem.eql(u8, entry.key, "\x00\x00__metadata__:indexes")) {
                    if (self.raw_indexes_json.*) |value| self.allocator.free(value);
                    self.raw_indexes_json.* = try self.allocator.dupe(u8, entry.value);
                } else if (std.mem.eql(u8, entry.key, "\x00\x00__metadata__:enrichments")) {
                    if (self.raw_enrichments_json.*) |value| self.allocator.free(value);
                    self.raw_enrichments_json.* = try self.allocator.dupe(u8, entry.value);
                }
            }
        }
    };
    var visitor: Visitor = .{
        .allocator = allocator,
        .schema_json = &schema_json,
        .raw_indexes_json = &raw_indexes_json,
        .raw_enrichments_json = &raw_enrichments_json,
    };
    _ = try portable_backup.inspectPortableFileBlocks(allocator, io, file, options, &visitor, Visitor.visit);

    const indexes_json = try tableIndexesJsonFromPortableMetadata(allocator, raw_indexes_json, raw_enrichments_json);
    errdefer allocator.free(indexes_json);

    return .{
        .schema_json = schema_json,
        .indexes_json = indexes_json,
    };
}

fn freeKeyValueEntries(allocator: Allocator, entries: []backup_codec.KeyValueEntry) void {
    for (entries) |entry| {
        allocator.free(entry.key);
        allocator.free(entry.value);
    }
    allocator.free(entries);
}

pub fn tableIndexesJsonFromPortableMetadata(
    allocator: Allocator,
    raw_indexes_json: ?[]const u8,
    raw_enrichments_json: ?[]const u8,
) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(allocator);

    try out.append(allocator, '{');
    var first = true;

    if (raw_indexes_json) |indexes_json| {
        if (std.mem.startsWith(u8, indexes_json, "AIDX")) {
            try appendBinaryIndexCatalogFields(allocator, &out, &first, indexes_json);
        } else {
            var parsed = try std.json.parseFromSlice(std.json.Value, allocator, indexes_json, .{});
            defer parsed.deinit();
            switch (parsed.value) {
                .object => |object| {
                    var it = object.iterator();
                    while (it.next()) |entry| {
                        if (raw_enrichments_json != null and std.mem.eql(u8, entry.key_ptr.*, "enrichments")) continue;
                        try appendJsonObjectField(allocator, &out, &first, entry.key_ptr.*, entry.value_ptr.*);
                    }
                },
                .array => |array| {
                    for (array.items) |item| {
                        const object = switch (item) {
                            .object => |object| object,
                            else => return error.InvalidBackupRequest,
                        };
                        const name_value = object.get("name") orelse return error.InvalidBackupRequest;
                        if (name_value != .string or name_value.string.len == 0) return error.InvalidBackupRequest;
                        try appendJsonObjectField(allocator, &out, &first, name_value.string, item);
                    }
                },
                else => return error.InvalidBackupRequest,
            }
        }
    }

    if (raw_enrichments_json) |enrichments_json| {
        var parsed = try std.json.parseFromSlice(std.json.Value, allocator, enrichments_json, .{});
        defer parsed.deinit();
        if (parsed.value != .array) return error.InvalidBackupRequest;
        try appendJsonObjectField(allocator, &out, &first, "enrichments", parsed.value);
    }

    try out.append(allocator, '}');
    return try out.toOwnedSlice(allocator);
}

fn appendBinaryIndexCatalogFields(
    allocator: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    data: []const u8,
) !void {
    if (data.len < 12 or !std.mem.eql(u8, data[0..4], "AIDX")) return error.InvalidBackupRequest;

    var pos: usize = 4;
    const version = try readCatalogU32(data, &pos);
    if (version != 1 and version != 2) return error.InvalidBackupRequest;
    const count = try readCatalogU32(data, &pos);

    for (0..count) |_| {
        const name = try readCatalogString(data, &pos);
        if (pos >= data.len) return error.InvalidBackupRequest;
        const kind_value = data[pos];
        pos += 1;
        const kind = indexKindFromCatalogByte(kind_value) orelse return error.InvalidBackupRequest;
        const config_json = try readCatalogString(data, &pos);
        const coverage_generation = if (version >= 2)
            try readCatalogU64(data, &pos)
        else
            internal_keys.derivedCoverageGeneration(config_json);

        if (!first.*) try out.append(allocator, ',');
        first.* = false;
        try appendJsonString(allocator, out, name);
        try out.appendSlice(allocator, ":{\"name\":");
        try appendJsonString(allocator, out, name);
        try out.appendSlice(allocator, ",\"kind\":");
        try appendJsonString(allocator, out, @tagName(kind));
        try out.appendSlice(allocator, ",\"config_json\":");
        try appendJsonString(allocator, out, config_json);
        try out.appendSlice(allocator, ",\"coverage_generation\":");
        try appendJsonU64(allocator, out, coverage_generation);
        try out.append(allocator, '}');
    }
}

fn indexKindFromCatalogByte(value: u8) ?db_types.IndexKind {
    return switch (value) {
        @backingInt(db_types.IndexKind.full_text) => .full_text,
        @backingInt(db_types.IndexKind.dense_vector) => .dense_vector,
        @backingInt(db_types.IndexKind.sparse_vector) => .sparse_vector,
        @backingInt(db_types.IndexKind.graph) => .graph,
        @backingInt(db_types.IndexKind.algebraic) => .algebraic,
        else => null,
    };
}

fn readCatalogU32(data: []const u8, pos: *usize) !u32 {
    if (pos.* + 4 > data.len) return error.InvalidBackupRequest;
    const value = std.mem.readInt(u32, data[pos.*..][0..4], .little);
    pos.* += 4;
    return value;
}

fn readCatalogU64(data: []const u8, pos: *usize) !u64 {
    if (pos.* + 8 > data.len) return error.InvalidBackupRequest;
    const value = std.mem.readInt(u64, data[pos.*..][0..8], .little);
    pos.* += 8;
    return value;
}

fn readCatalogString(data: []const u8, pos: *usize) ![]const u8 {
    const len = try readCatalogU32(data, pos);
    if (pos.* + len > data.len) return error.InvalidBackupRequest;
    const value = data[pos.* .. pos.* + len];
    pos.* += len;
    return value;
}

fn appendJsonObjectField(
    allocator: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    name: []const u8,
    value: std.json.Value,
) !void {
    if (!first.*) try out.append(allocator, ',');
    first.* = false;
    try appendJsonString(allocator, out, name);
    try out.append(allocator, ':');
    const encoded = try std.fmt.allocPrint(allocator, "{f}", .{std.json.fmt(value, .{})});
    defer allocator.free(encoded);
    try out.appendSlice(allocator, encoded);
}

fn manifestFromParts(
    allocator: Allocator,
    table_name: []const u8,
    backup_id: []const u8,
    snapshot_path: []const u8,
    integrity: backups_api.ArtifactIntegrity,
    schema_json: []const u8,
    indexes_json: []const u8,
    description: []const u8,
) !backups_api.TableBackupManifest {
    const owned_backup_id = try allocator.dupe(u8, backup_id);
    errdefer allocator.free(owned_backup_id);
    const owned_table_name = try allocator.dupe(u8, table_name);
    errdefer allocator.free(owned_table_name);
    const owned_description = try allocator.dupe(u8, description);
    errdefer allocator.free(owned_description);
    const read_schema_json = try allocator.dupe(u8, "");
    errdefer allocator.free(read_schema_json);
    const replication_sources_json = try allocator.dupe(u8, "[]");
    errdefer allocator.free(replication_sources_json);
    const owned_snapshot_path = try allocator.dupe(u8, snapshot_path);
    errdefer allocator.free(owned_snapshot_path);
    const start_key = try allocator.dupe(u8, "");
    errdefer allocator.free(start_key);
    const shard = try allocator.alloc(backups_api.ShardSnapshot, 1);
    shard[0] = .{
        .group_id = group_ids.dataGroupIdFromHash(1),
        .start_key = start_key,
        .end_key = null,
        .snapshot_path = owned_snapshot_path,
        .artifact_size_bytes = integrity.size_bytes,
        .artifact_sha256 = integrity.sha256,
    };
    return .{
        .format = .portable,
        .backup_id = owned_backup_id,
        .table_name = owned_table_name,
        .description = owned_description,
        .schema_json = schema_json,
        .read_schema_json = read_schema_json,
        .indexes_json = indexes_json,
        .replication_sources_json = replication_sources_json,
        .shards = shard,
    };
}

fn freeManifest(allocator: Allocator, manifest: backups_api.TableBackupManifest) void {
    var owned = manifest;
    owned.deinit(allocator);
}

fn appendJsonString(
    allocator: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    value: []const u8,
) !void {
    const escaped = try std.fmt.allocPrint(allocator, "{f}", .{std.json.fmt(value, .{})});
    defer allocator.free(escaped);
    try out.appendSlice(allocator, escaped);
}

fn appendJsonU64(
    allocator: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    value: u64,
) !void {
    const encoded = try std.fmt.allocPrint(allocator, "{d}", .{value});
    defer allocator.free(encoded);
    try out.appendSlice(allocator, encoded);
}

fn readFileAlloc(allocator: Allocator, path: []const u8, max_bytes: usize) ![]u8 {
    var io_impl = std.Io.Threaded.init(allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    if (!std.fs.path.isAbsolute(path)) {
        return try std.Io.Dir.cwd().readFileAlloc(io, path, allocator, .limited(max_bytes));
    }
    var file = try std.Io.Dir.openFileAbsolute(io, path, .{});
    defer file.close(io);
    const size = (try file.stat(io)).size;
    if (size > max_bytes or size > std.math.maxInt(usize)) return error.FileTooBig;
    var buf: [8192]u8 = undefined;
    var reader = file.reader(io, &buf);
    return try reader.interface.readAlloc(allocator, @intCast(size));
}

fn searchJson(allocator: Allocator, db: *db_mod.DB, body: []const u8) ![]u8 {
    var owned = try query_api.parsePublicQueryRequest(
        allocator,
        null,
        "docs",
        body,
    );
    defer owned.deinit(allocator);

    var result = try db.search(allocator, owned.req);
    defer result.deinit();

    var response = try query_api.encodeQueryResponses(
        allocator,
        "docs",
        owned.req,
        .{},
        result,
    );
    defer response.deinit(allocator);
    return try allocator.dupe(u8, response.json);
}

fn fileExists(io: std.Io, path: []const u8) bool {
    if (std.fs.path.isAbsolute(path)) {
        std.Io.Dir.accessAbsolute(io, path, .{}) catch return false;
    } else {
        std.Io.Dir.cwd().access(io, path, .{}) catch return false;
    }
    return true;
}

const deleteFileIfExists = @import("portable_restore.zig").deleteFileIfExists;

const scavengePortableRestoreGenerations = @import("portable_restore.zig").scavengePortableRestoreGenerations;

fn freeBackupShards(allocator: Allocator, shards: []const backups_api.ShardSnapshot) void {
    for (shards) |shard| shard.deinit(allocator);
    allocator.free(@constCast(shards));
}

/// Finds an index by name in a `listIndexes` result, order-independent. Every
/// fresh Lite database now also carries the default full-text index (see
/// `full_text_index_defaults.zig`), so tests assert specific names are
/// present instead of relying on a fixed slice order.
fn indexNamed(indexes: []const db_types.IndexConfig, name: []const u8) ?db_types.IndexConfig {
    for (indexes) |index| {
        if (std.mem.eql(u8, index.name, name)) return index;
    }
    return null;
}

test "lite restore staging writer close syncs unsynced batch before readonly reopen" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/restore-staging-close-sync.aflite", .{tmp.sub_path});
    defer allocator.free(path);

    {
        var lite = try LiteDb.create(allocator, path, true);
        defer lite.close();

        try lite.db.batch(.{
            .writes = &.{.{
                .key = "doc:restore-staging-close-sync",
                .value = "{\"title\":\"restore staging close persists\"}",
            }},
            .sync_level = .propose,
        });
    }

    {
        var reopened = try LiteDb.open(allocator, path, .query_readonly);
        defer reopened.close();

        var result = (try reopened.db.lookup(allocator, "doc:restore-staging-close-sync", .{})) orelse return error.MissingRestoredLiteDocument;
        defer result.deinit(allocator);
        try std.testing.expect(std.mem.indexOf(u8, result.json, "\"restore staging close persists\"") != null);
    }
}

test "lite portable generation scavenging reaps stale data and lease-only crashes" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const parent = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer allocator.free(parent);
    const target = try std.fmt.allocPrint(allocator, "{s}/target.aflite", .{parent});
    defer allocator.free(target);
    const stale_data = try std.fmt.allocPrint(allocator, "{s}.portable-restore-stale.aflite", .{target});
    defer allocator.free(stale_data);
    const stale_lock = try std.fmt.allocPrint(allocator, "{s}.lock", .{stale_data});
    defer allocator.free(stale_lock);
    const lease_only_data = try std.fmt.allocPrint(allocator, "{s}.portable-restore-lease-only.aflite", .{target});
    defer allocator.free(lease_only_data);
    const lease_only_lock = try std.fmt.allocPrint(allocator, "{s}.lock", .{lease_only_data});
    defer allocator.free(lease_only_lock);
    const active_data = try std.fmt.allocPrint(allocator, "{s}.portable-restore-active.aflite", .{target});
    defer allocator.free(active_data);
    const active_lock = try std.fmt.allocPrint(allocator, "{s}.lock", .{active_data});
    defer allocator.free(active_lock);

    const io = std.testing.io;
    {
        var data = try std.Io.Dir.cwd().createFile(io, stale_data, .{ .truncate = true });
        data.close(io);
        var lease = try std.Io.Dir.cwd().createFile(io, stale_lock, .{ .read = true, .truncate = true });
        try lease.writePositionalAll(io, portable_generation_lease_magic, 0);
        lease.close(io);
    }
    {
        var lease = try std.Io.Dir.cwd().createFile(io, lease_only_lock, .{ .read = true, .truncate = true });
        try lease.writePositionalAll(io, portable_generation_lease_magic, 0);
        lease.close(io);
    }
    var active_lease = try std.Io.Dir.cwd().createFile(io, active_lock, .{
        .read = true,
        .truncate = true,
        .lock = .exclusive,
        .lock_nonblocking = true,
    });
    defer active_lease.close(io);
    try active_lease.writePositionalAll(io, portable_generation_lease_magic, 0);

    try scavengePortableRestoreGenerations(allocator, io, target);
    try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().access(io, stale_data, .{}));
    try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().access(io, stale_lock, .{}));
    try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().access(io, lease_only_lock, .{}));
    try std.Io.Dir.cwd().access(io, active_lock, .{});
}

test "lite portable restore enforces staging budget and preserves target" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const source_path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/budget-source.aflite", .{tmp.sub_path});
    defer a.free(source_path);
    const target_path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/budget-target.aflite", .{tmp.sub_path});
    defer a.free(target_path);
    var portable = std.ArrayList(u8).empty;
    defer portable.deinit(a);
    {
        var source = try LiteDb.createWithOptions(a, source_path, true, .{ .fsync = false });
        defer source.close();
        const value = try a.alloc(u8, 1200 * 1024);
        defer a.free(value);
        @memset(value, 'x');
        @memcpy(value[0..9], "{\"text\":\"");
        @memcpy(value[value.len - 2 ..], "\"}");
        // Populate primary data directly: this is an admission test, so
        // constructing a full-text index for the large token adds no coverage.
        const key = try internal_keys.documentKeyAlloc(a, "large");
        defer a.free(key);
        try source.db.core.store.put(key, value);
        try portable_backup.exportPortable(a, source.db.core.store, &portable);
        try std.testing.expect(portable.items.len > 1024 * 1024);
    }
    var target = try LiteDb.createWithOptions(a, target_path, true, .{ .fsync = false, .reclamation = .{ .max_storage_bytes = 256 * 4096 } });
    defer target.close();
    const owner = target.backend.native_docstore.?;
    const before_handle = owner.file.file.handle;
    try std.testing.expectError(error.LiteStorageBudgetExceeded, importPortableIntoLiteDb(a, &target.db, &target.backend, portable.items));
    try std.testing.expect(try isImportTargetEmpty(a, &target.db));
    const status = try owner.reclamationStatus();
    // Idle retirement can publish concurrently; adoption must retain the
    // target inode and its empty logical contents on definite rejection.
    try std.testing.expectEqual(before_handle, owner.file.file.handle);
    try std.testing.expect(status.current_file_bytes <= 256 * 4096);
    try std.testing.expectEqual(@as(u64, 0), status.temporary_bytes);
    try std.testing.expect(!owner.generation_workspace_active);
    try std.testing.expect((try owner.checkWithCancel(null)).valid);
}

test "lite portable publication never reports a retryable failure after adoption" {
    @import("antfly_test_error_logs").expectErrorLogs(1);
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const source_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/post-adopt-source.aflite", .{tmp.sub_path});
    defer allocator.free(source_path);
    const target_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/post-adopt-target.aflite", .{tmp.sub_path});
    defer allocator.free(target_path);
    var portable = std.ArrayList(u8).empty;
    defer portable.deinit(allocator);

    {
        var source = try LiteDb.create(allocator, source_path, true);
        defer source.close();
        var index = try std.json.parseFromSlice(
            db_types.IndexConfig,
            allocator,
            "{\"name\":\"published_ft\",\"kind\":\"full_text\",\"config_json\":\"{}\"}",
            .{ .ignore_unknown_fields = true },
        );
        defer index.deinit();
        try source.db.addIndex(index.value);
        try source.db.batch(.{ .writes = &.{.{
            .key = "doc:published",
            .value = "{\"title\":\"published\"}",
        }} });
        try portable_backup.exportPortable(allocator, source.db.core.store, &portable);
    }

    var target = try LiteDb.create(allocator, target_path, true);
    defer target.close();
    db_mod.failNextPortableIndexActivationForTest();
    try importPortableIntoLiteDb(allocator, &target.db, &target.backend, portable.items);

    const degraded = target.db.pendingWorkStats();
    try std.testing.expect(degraded.portable_runtime_activation_pending);
    try std.testing.expectEqual(@as(u64, 1), degraded.portable_runtime_activation_attempts);
    try std.testing.expectError(error.PortableRuntimeActivationPending, target.db.listIndexes(allocator));
    try std.testing.expectError(error.PortableRuntimeActivationPending, target.db.batch(.{ .writes = &.{.{
        .key = "doc:blocked-until-runtime-ready",
        .value = "{\"title\":\"blocked\"}",
    }} }));

    try target.db.runUntilIdle();
    const recovered_runtime = target.db.pendingWorkStats();
    try std.testing.expect(!recovered_runtime.portable_runtime_activation_pending);
    try std.testing.expectEqual(@as(u64, 2), recovered_runtime.portable_runtime_activation_attempts);
    const indexes = try target.db.listIndexes(allocator);
    defer db_types.freeIndexConfigs(allocator, indexes);
    // The import wholesale-replaces target's catalog with source's, which
    // carries both source's explicit index and the default full-text index
    // `LiteDb.create` provisions on every fresh Lite database.
    try std.testing.expectEqual(@as(usize, 2), indexes.len);
    try std.testing.expect(indexNamed(indexes, "published_ft") != null);
    try std.testing.expect(indexNamed(indexes, full_text_index_defaults.default_full_text_index_name) != null);

    var restored = (try target.db.lookup(allocator, "doc:published", .{})) orelse return error.MissingPublishedDocument;
    defer restored.deinit(allocator);
    try std.testing.expect(std.mem.indexOf(u8, restored.json, "published") != null);
    try std.testing.expect(!(try isImportTargetEmpty(allocator, &target.db)));
}

test "lite portable publication reports durability unknown after adopting runtime state" {
    @import("antfly_test_error_logs").expectErrorLogs(1);
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const source_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/durability-source.aflite", .{tmp.sub_path});
    defer allocator.free(source_path);
    const target_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/durability-target.aflite", .{tmp.sub_path});
    defer allocator.free(target_path);
    var portable = std.ArrayList(u8).empty;
    defer portable.deinit(allocator);

    {
        var source = try LiteDb.create(allocator, source_path, true);
        defer source.close();
        var index = try std.json.parseFromSlice(
            db_types.IndexConfig,
            allocator,
            "{\"name\":\"durable_ft\",\"kind\":\"full_text\",\"config_json\":\"{}\"}",
            .{ .ignore_unknown_fields = true },
        );
        defer index.deinit();
        try source.db.addIndex(index.value);
        try source.db.batch(.{ .writes = &.{.{
            .key = "doc:published-unknown",
            .value = "{\"title\":\"published unknown\"}",
        }} });
        try portable_backup.exportPortable(allocator, source.db.core.store, &portable);
    }

    var target = try LiteDb.create(allocator, target_path, true);
    defer target.close();
    target.backend.native_docstore.?.file.failNextGenerationDirectorySyncForTest();
    try std.testing.expectError(
        error.DurabilityOutcomeUnknown,
        importPortableIntoLiteDb(allocator, &target.db, &target.backend, portable.items),
    );

    // The error is explicitly non-retryable: the live storage and runtime
    // both adopted the committed generation before it was returned.
    try std.testing.expectError(
        error.LiteImportTargetNotEmpty,
        importPortableIntoLiteDb(allocator, &target.db, &target.backend, portable.items),
    );
    var restored = (try target.db.lookup(allocator, "doc:published-unknown", .{})) orelse
        return error.MissingPublishedDocument;
    defer restored.deinit(allocator);
    const indexes = try target.db.listIndexes(allocator);
    defer db_types.freeIndexConfigs(allocator, indexes);
    // See the comment in the sibling test above: import replaces target's
    // catalog wholesale with source's, which includes source's own default
    // full-text index alongside its explicit one.
    try std.testing.expectEqual(@as(usize, 2), indexes.len);
    try std.testing.expect(indexNamed(indexes, "durable_ft") != null);
    try std.testing.expect(indexNamed(indexes, full_text_index_defaults.default_full_text_index_name) != null);
}

test "lite portable import emptiness rejects tombstones and durable transactions" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const tombstone_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/import-tombstone.aflite", .{tmp.sub_path});
    defer allocator.free(tombstone_path);
    var tombstone_db = try LiteDb.create(allocator, tombstone_path, true);
    defer tombstone_db.close();
    try tombstone_db.db.batch(.{ .writes = &.{.{
        .key = "doc:deleted",
        .value = "{\"title\":\"deleted\"}",
    }} });
    try tombstone_db.db.batch(.{ .deletes = &.{"doc:deleted"} });
    try std.testing.expectEqual(@as(u64, 0), try tombstone_db.db.primaryDocCount(allocator));
    try std.testing.expect(!(try isImportTargetEmpty(allocator, &tombstone_db.db)));

    const transaction_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/import-transaction.aflite", .{tmp.sub_path});
    defer allocator.free(transaction_path);
    var transaction_db = try LiteDb.create(allocator, transaction_path, true);
    defer transaction_db.close();
    const txn_id: db_types.TxnId = .{ 0x69, 0x6d, 0x70, 0x6f, 0x72, 0x74, 0x2d, 0x74, 0x61, 0x72, 0x67, 0x65, 0x74, 0, 0, 1 };
    _ = try transaction_db.db.beginTransactionWithId(txn_id, 8_000);
    try std.testing.expect(!(try isImportTargetEmpty(allocator, &transaction_db.db)));
}

test "lite restore staging preserves portable afb schema index and enrichment metadata" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var io_impl = std.Io.Threaded.init(allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    const src_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/portable-metadata-src.aflite", .{tmp.sub_path});
    defer allocator.free(src_path);
    const afb_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/portable-metadata.afb", .{tmp.sub_path});
    defer allocator.free(afb_path);
    const cwd_tmp = try std.Io.Dir.cwd().realPathFileAlloc(io, ".zig-cache/tmp", allocator);
    defer allocator.free(cwd_tmp);
    const backup_root = try std.fmt.allocPrint(allocator, "{s}/{s}/portable-metadata-backups", .{ cwd_tmp, tmp.sub_path });
    defer allocator.free(backup_root);
    const location = try std.fmt.allocPrint(allocator, "file://{s}", .{backup_root});
    defer allocator.free(location);

    const schema_json =
        \\{"version":0,"default_type":"doc","enforce_types":false,"document_schemas":{"doc":{"schema":{"type":"object","additionalProperties":true}}}}
    ;
    const enrichment_json = "{\"name\":\"body_chunks_v1\",\"kind\":\"chunk\",\"field\":\"body\",\"chunk_size\":128,\"chunk_overlap\":16}";
    const index_json = "{\"name\":\"ft_body\",\"kind\":\"full_text\",\"config_json\":\"{\\\"chunk_name\\\":\\\"body_chunks_v1\\\"}\"}";

    var portable = std.ArrayList(u8).empty;
    defer portable.deinit(allocator);
    {
        var source = try LiteDb.create(allocator, src_path, true);
        defer source.close();

        try source.db.setSchemaJson(allocator, schema_json);

        var enrichment = try std.json.parseFromSlice(db_types.EnrichmentConfig, allocator, enrichment_json, .{
            .ignore_unknown_fields = true,
        });
        defer enrichment.deinit();
        try source.db.addEnrichment(enrichment.value);

        var index = try std.json.parseFromSlice(db_types.IndexConfig, allocator, index_json, .{
            .ignore_unknown_fields = true,
        });
        defer index.deinit();
        try source.db.addIndex(index.value);

        try portable_backup.exportPortable(allocator, source.db.core.store, &portable);
    }

    {
        var file = try std.Io.Dir.cwd().createFile(io, afb_path, .{ .truncate = true });
        defer file.close(io);
        var buf: [8192]u8 = undefined;
        var writer = file.writer(io, &buf);
        try writer.interface.writeAll(portable.items);
        try writer.end();
        try file.sync(io);
    }

    var staged = try stageAfbRestoreBackup(allocator, afb_path, "docs", "portable-metadata-test", "portable-metadata-test", location);
    defer staged.deinit(allocator);

    var backup_location = try backups_api.openBackupLocation(allocator, location);
    defer backup_location.deinit(allocator);
    var manifest = try backups_api.readManifestFromLocation(allocator, &backup_location, "portable-metadata-test");
    defer manifest.deinit(allocator);

    try std.testing.expectEqualStrings(schema_json, manifest.schema_json);
    try std.testing.expect(std.mem.indexOf(u8, manifest.indexes_json, "\"ft_body\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest.indexes_json, "\"enrichments\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest.indexes_json, "\"body_chunks_v1\"") != null);

    var parsed_indexes = try std.json.parseFromSlice(std.json.Value, allocator, manifest.indexes_json, .{});
    defer parsed_indexes.deinit();
    try std.testing.expect(parsed_indexes.value == .object);
    const ft_body = parsed_indexes.value.object.get("ft_body") orelse return error.TestExpectedEqual;
    try std.testing.expect(ft_body == .object);
    try std.testing.expect(ft_body.object.get("coverage_generation") != null);
    const enrichments = parsed_indexes.value.object.get("enrichments") orelse return error.TestExpectedEqual;
    try std.testing.expect(enrichments == .array);
    try std.testing.expectEqual(@as(usize, 1), enrichments.array.items.len);
}

test "lite restore staging preflights afb before publishing staged files" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var io_impl = std.Io.Threaded.init(allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    const malformed_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/malformed.afb", .{tmp.sub_path});
    defer allocator.free(malformed_path);
    const cwd_tmp = try std.Io.Dir.cwd().realPathFileAlloc(io, ".zig-cache/tmp", allocator);
    defer allocator.free(cwd_tmp);
    const backup_root = try std.fmt.allocPrint(allocator, "{s}/{s}/malformed-staging-backups", .{ cwd_tmp, tmp.sub_path });
    defer allocator.free(backup_root);
    const location = try std.fmt.allocPrint(allocator, "file://{s}", .{backup_root});
    defer allocator.free(location);
    const staged_afb_path = try std.fmt.allocPrint(allocator, "{s}/bad-input.afb", .{backup_root});
    defer allocator.free(staged_afb_path);
    const staged_manifest_path = try backups_api.metadataPath(allocator, backup_root, "bad-input");
    defer allocator.free(staged_manifest_path);

    var malformed = std.ArrayList(u8).empty;
    defer malformed.deinit(allocator);
    try backup_codec.writeHeader(&malformed, allocator, .{
        .format_version = backup_codec.format_version,
        .flags = 0,
        .created_at_ns = 0,
        .backup_id = @as([16]u8, @splat(0)),
        .table_count = 1,
        .shard_count = 1,
    });
    const malformed_doc_payload = [_]u8{ 1, 0, 0, 0 };
    try backup_codec.writeBlock(&malformed, allocator, .document_batch, &malformed_doc_payload);
    {
        var file = try std.Io.Dir.cwd().createFile(io, malformed_path, .{ .truncate = true });
        defer file.close(io);
        try file.writePositionalAll(io, malformed.items, 0);
        try file.sync(io);
    }

    try std.testing.expectError(
        error.InvalidBackupManifest,
        stageAfbRestoreBackup(allocator, malformed_path, "docs", "bad-input", "bad-input", location),
    );
    try std.testing.expect(!fileExists(io, staged_afb_path));
    try std.testing.expect(!fileExists(io, staged_manifest_path));
}

test "lite restore staging expands a self-contained native AFB2 bundle" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var io_impl = std.Io.Threaded.init(allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    const cwd_tmp = try std.Io.Dir.cwd().realPathFileAlloc(io, ".zig-cache/tmp", allocator);
    defer allocator.free(cwd_tmp);
    const source_root = try std.fmt.allocPrint(allocator, "{s}/{s}/native-source", .{ cwd_tmp, tmp.sub_path });
    defer allocator.free(source_root);
    const target_root = try std.fmt.allocPrint(allocator, "{s}/{s}/native-target", .{ cwd_tmp, tmp.sub_path });
    defer allocator.free(target_root);
    const target_location = try std.fmt.allocPrint(allocator, "file://{s}", .{target_root});
    defer allocator.free(target_location);
    const bundle_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/native-table.afb2", .{tmp.sub_path});
    defer allocator.free(bundle_path);
    const backup_id = "native-bundle-test";
    const snapshot_path = try backups_api.shardSnapshotRelPath(allocator, backup_id, 9);
    defer allocator.free(snapshot_path);
    const source_shard = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ source_root, snapshot_path });
    defer allocator.free(source_shard);
    try @import("antfly_runtime_fs").fs_paths.createDirPathPortable(io, source_shard);
    const payload_path = try std.fmt.allocPrint(allocator, "{s}/primary.bin", .{source_shard});
    defer allocator.free(payload_path);
    {
        var payload = try std.Io.Dir.createFileAbsolute(io, payload_path, .{ .exclusive = true });
        defer payload.close(io);
        try payload.writePositionalAll(io, "native-generation-payload", 0);
        try payload.sync(io);
    }

    var shard = backups_api.ShardSnapshot{
        .group_id = 9,
        .start_key = "",
        .snapshot_path = snapshot_path,
    };
    try backups_api.populateShardArtifactIntegrity(allocator, io, .native, source_shard, &shard);
    defer allocator.free(@constCast(shard.artifact_sha256));
    const source_manifest = backups_api.TableBackupManifest{
        .format = .native,
        .backup_id = backup_id,
        .table_name = "source_docs",
        .description = "native bundle restore test",
        .schema_json = "{}",
        .read_schema_json = "",
        .indexes_json = "{}",
        .replication_sources_json = "[]",
        .shards = &.{shard},
    };
    try backups_api.writeManifest(allocator, source_root, &source_manifest);

    const metadata_path = try std.fmt.allocPrint(allocator, "{s}-metadata.json", .{backup_id});
    defer allocator.free(metadata_path);
    {
        var output = try std.Io.Dir.cwd().createFile(io, bundle_path, .{ .exclusive = true });
        defer output.close(io);
        var buffer: [64 * 1024]u8 = undefined;
        var writer = output.writer(io, &buffer);
        try backup_bundle_io.packNativePathsToWriter(
            allocator,
            io,
            source_root,
            &.{ metadata_path, snapshot_path },
            &writer.interface,
            .{ .backup_id = backup_id, .table_name = "source_docs" },
        );
        try writer.end();
        try output.sync(io);
    }

    try std.testing.expectError(error.BackupExpansionLimitExceeded, stageInputRestoreBackupWithOptions(
        allocator,
        bundle_path,
        "restored_docs",
        "",
        target_location,
        .{ .max_prepared_bytes = 1 },
    ));
    try std.testing.expect(!fileExists(io, target_root));
    var staged = try stageInputRestoreBackup(allocator, bundle_path, "restored_docs", "", target_location);
    defer staged.deinit(allocator);
    try std.testing.expectEqualStrings(backup_id, staged.backup_id);
    try std.testing.expectEqualStrings("restored_docs", staged.table_name);

    var target = try backups_api.readManifest(allocator, target_root, backup_id);
    defer target.deinit(allocator);
    try std.testing.expectEqual(backups_api.BackupFormat.native, target.format);
    try std.testing.expectEqualStrings("restored_docs", target.table_name);
    const restore_table = try backups_api.deriveRestoreTableRecord(allocator, "restored_docs", target_location, &target);
    defer @import("antfly_server_test_sources").local_test_sources.metadata_table_manager.freeTable(allocator, restore_table);
    try std.testing.expectEqualStrings("restored_docs", restore_table.name);
    const restored_payload = try std.fmt.allocPrint(allocator, "{s}/{s}/primary.bin", .{ target_root, snapshot_path });
    defer allocator.free(restored_payload);
    const bytes = try std.Io.Dir.cwd().readFileAlloc(io, restored_payload, allocator, .limited(128));
    defer allocator.free(bytes);
    try std.testing.expectEqualStrings("native-generation-payload", bytes);
}

test "lite restore staging rejects unreferenced native blob bytes before extraction" {
    const alloc = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(io, ".", alloc);
    defer alloc.free(root);
    const input = try std.fs.path.join(alloc, &.{ root, "unused-blob.afb2" });
    defer alloc.free(input);
    const location = try std.fmt.allocPrint(alloc, "file://{s}/never-created", .{root});
    defer alloc.free(location);
    const first = "0000000000000000000000000000000000000000000000000000000000000000";
    const second = "1111111111111111111111111111111111111111111111111111111111111111";
    const encoded = try backup_bundle.encodeManifestAlloc(alloc, .{
        .representation = .native,
        .backup_id = "test",
        .table_name = "docs",
        .objects = &.{.{ .logical_path = "test-metadata.json", .role = "manifest", .size_bytes = 16, .sha256 = first }},
        .blobs = &.{
            .{ .sha256 = first, .logical_size_bytes = 16, .stored_size_bytes = 16 },
            .{ .sha256 = second, .logical_size_bytes = 4 * 1024 * 1024, .stored_size_bytes = 4 * 1024 * 1024 },
        },
    });
    defer alloc.free(encoded);
    {
        var file = try tmp.dir.createFile(io, "unused-blob.afb2", .{});
        defer file.close(io);
        var buffer: [4096]u8 = undefined;
        var writer = file.writer(io, &buffer);
        try backup_codec.writeHeaderTo(&writer.interface, .{
            .format_version = backup_codec.format_version,
            .flags = 0,
            .created_at_ns = 0,
            .backup_id = @splat(0),
            .table_count = 1,
            .shard_count = 1,
        });
        try backup_codec.writeBlockTo(&writer.interface, .bundle_manifest, encoded);
        // No payload/footer: the declared byte budget must reject the source
        // before extraction attempts to read either or creates a repository.
        try writer.end();
    }
    try std.testing.expectError(error.BackupExpansionLimitExceeded, stageInputRestoreBackupWithOptions(
        alloc,
        input,
        "docs",
        "test",
        location,
        .{ .max_prepared_bytes = 1024 * 1024 },
    ));
    try std.testing.expectError(error.FileNotFound, tmp.dir.access(io, "never-created", .{}));
}

test "lite restore staging accepts aflite input for normal restore" {
    // Keep leak/safety checks without capturing every allocation/free stack.
    // Opt into tracing when diagnosing a failure.
    var no_stack_allocator: std.heap.SafeAllocator = .init(std.heap.page_allocator, .{ .stack_trace_frames = 0 });
    defer std.debug.assert(no_stack_allocator.deinit() == 0);
    const allocator = if (@import("antfly_platform").env.getenvBool("ANTFLY_TEST_ALLOCATOR_TRACES")) std.testing.allocator else no_stack_allocator.allocator();
    var work_profile = RestoreWorkProfile.init();

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var io_impl = std.Io.Threaded.init(allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    const src_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/normal-restore-src.aflite", .{tmp.sub_path});
    defer allocator.free(src_path);
    const cwd_tmp = try std.Io.Dir.cwd().realPathFileAlloc(io, ".zig-cache/tmp", allocator);
    defer allocator.free(cwd_tmp);
    const backup_root = try std.fmt.allocPrint(allocator, "{s}/{s}/normal-restore-backups", .{ cwd_tmp, tmp.sub_path });
    defer allocator.free(backup_root);
    const location = try std.fmt.allocPrint(allocator, "file://{s}", .{backup_root});
    defer allocator.free(location);

    const default_backup_id = try defaultBackupIdAlloc(allocator, src_path);
    defer allocator.free(default_backup_id);
    try std.testing.expectEqualStrings("lite-normal-restore-src", default_backup_id);

    const schema_json =
        \\{"version":0,"default_type":"doc","enforce_types":false,"document_schemas":{"doc":{"schema":{"type":"object","additionalProperties":true}}}}
    ;
    const enrichment_json = "{\"name\":\"restore_chunks_v1\",\"kind\":\"chunk\",\"field\":\"body\",\"chunk_size\":256,\"chunk_overlap\":32}";
    const index_jsons = [_][]const u8{
        "{\"name\":\"restore_ft_body\",\"kind\":\"full_text\",\"config_json\":\"{}\"}",
        "{\"name\":\"restore_dense\",\"kind\":\"dense_vector\",\"config_json\":\"{\\\"field\\\":\\\"embedding\\\",\\\"dims\\\":2,\\\"metric\\\":\\\"l2_squared\\\",\\\"external\\\":true}\"}",
        "{\"name\":\"restore_sparse\",\"kind\":\"sparse_vector\",\"config_json\":\"{\\\"field\\\":\\\"sparse_embedding\\\",\\\"external\\\":true}\"}",
        "{\"name\":\"restore_graph\",\"kind\":\"graph\",\"config_json\":\"{}\"}",
    };

    {
        var source = try LiteDb.create(allocator, src_path, true);
        defer source.close();

        try source.db.setSchemaJson(allocator, schema_json);

        var enrichment = try std.json.parseFromSlice(db_types.EnrichmentConfig, allocator, enrichment_json, .{
            .ignore_unknown_fields = true,
        });
        defer enrichment.deinit();
        try source.db.addEnrichment(enrichment.value);

        for (index_jsons) |index_json| {
            var index = try std.json.parseFromSlice(db_types.IndexConfig, allocator, index_json, .{
                .ignore_unknown_fields = true,
            });
            defer index.deinit();
            try source.db.addIndex(index.value);
        }

        work_profile.mark(&source.db, "source-index-initialization");
        try source.db.batch(.{
            .writes = &.{
                .{
                    .key = "doc:restore:a",
                    .value = "{\"title\":\"restore alpha\",\"body\":\"direct aflite restore hybrid alpha\",\"_embeddings\":{\"restore_dense\":[1,0],\"restore_sparse\":{\"indices\":[7,42],\"values\":[1.5,0.5]}},\"_edges\":{\"restore_graph\":{\"links\":[{\"target\":\"doc:restore:c\",\"weight\":1.0}]}}}",
                },
                .{
                    .key = "doc:restore:b",
                    .value = "{\"title\":\"restore beta\",\"body\":\"direct aflite restore hybrid beta\",\"_embeddings\":{\"restore_dense\":[0,1],\"restore_sparse\":{\"indices\":[99],\"values\":[2.0]}}}",
                },
                .{
                    .key = "doc:restore:c",
                    .value = "{\"title\":\"restore graph target\"}",
                },
            },
            .sync_level = .full_index,
        });
        work_profile.mark(&source.db, "source-batch");
        try source.db.runUntilIdle();
        work_profile.mark(&source.db, "source-idle");
        try expectIdleMaintenanceDoesNotPublish(&source.db);
    }

    work_profile.markTime("source-close");
    var staged = try stageInputRestoreBackup(allocator, src_path, "docs", default_backup_id, location);
    defer staged.deinit(allocator);
    work_profile.markTime("export");

    try std.testing.expectEqualStrings(default_backup_id, staged.backup_id);
    try std.testing.expectEqualStrings(location, staged.location);
    try std.testing.expectEqualStrings("docs", staged.table_name);
    try std.testing.expectEqualStrings("lite-normal-restore-src.afb", staged.snapshot_path);

    var backup_location = try backups_api.openBackupLocation(allocator, location);
    defer backup_location.deinit(allocator);
    var manifest = try backups_api.readManifestFromLocation(allocator, &backup_location, default_backup_id);
    defer manifest.deinit(allocator);

    try std.testing.expectEqualStrings(schema_json, manifest.schema_json);
    try std.testing.expectEqualStrings("docs", manifest.table_name);
    try std.testing.expectEqualStrings(default_backup_id, manifest.backup_id);
    try std.testing.expectEqualStrings(staged.snapshot_path, manifest.shards[0].snapshot_path);
    try std.testing.expect(std.mem.indexOf(u8, manifest.indexes_json, "\"restore_ft_body\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest.indexes_json, "\"restore_dense\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest.indexes_json, "\"restore_sparse\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest.indexes_json, "\"restore_graph\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest.indexes_json, "\"enrichments\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest.indexes_json, "\"restore_chunks_v1\"") != null);

    const afb_path = try std.fmt.allocPrint(allocator, "{s}/{s}.afb", .{ backup_root, default_backup_id });
    defer allocator.free(afb_path);
    const portable = try readFileAlloc(allocator, afb_path, max_afb_file_bytes);
    defer allocator.free(portable);
    try std.testing.expect(portable.len > 0);

    const restored_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/normal-restore-roundtrip.aflite", .{tmp.sub_path});
    defer allocator.free(restored_path);
    {
        var restored = try LiteDb.create(allocator, restored_path, true);
        defer restored.close();
        try populateUnpublishedLiteDb(allocator, &restored.db, portable);
        try expectIdleMaintenanceDoesNotPublish(&restored.db);

        const dense = try searchJson(
            allocator,
            &restored.db,
            "{\"embeddings\":{\"restore_dense\":[1,0]},\"indexes\":[\"restore_dense\"],\"limit\":1}",
        );
        defer allocator.free(dense);
        try std.testing.expect(std.mem.indexOf(u8, dense, "\"doc:restore:a\"") != null);

        const sparse = try searchJson(
            allocator,
            &restored.db,
            "{\"embeddings\":{\"restore_sparse\":{\"indices\":[7,42],\"values\":[1.5,0.5]}},\"indexes\":[\"restore_sparse\"],\"limit\":1}",
        );
        defer allocator.free(sparse);
        try std.testing.expect(std.mem.indexOf(u8, sparse, "\"doc:restore:a\"") != null);

        const graph = try searchJson(
            allocator,
            &restored.db,
            "{\"graph_queries\":{\"neighbors\":{\"index\":\"restore_graph\",\"traverse\":{\"start\":{\"keys\":[\"doc:restore:a\"]},\"edge_types\":[\"links\"],\"max_depth\":1}}},\"limit\":10}",
        );
        defer allocator.free(graph);
        try std.testing.expect(std.mem.indexOf(u8, graph, "\"doc:restore:c\"") != null);

        const hybrid = try searchJson(
            allocator,
            &restored.db,
            "{\"full_text_search\":{\"match\":{\"field\":\"body\",\"text\":\"hybrid alpha\"}},\"embeddings\":{\"restore_dense\":[1,0]},\"indexes\":[\"restore_dense\"],\"merge_config\":{\"strategy\":\"rrf\"},\"limit\":3}",
        );
        defer allocator.free(hybrid);
        try std.testing.expect(std.mem.indexOf(u8, hybrid, "\"doc:restore:a\"") != null);
    }
}

test "lite restore staging exports stable aflite data while writer has open transaction" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var io_impl = std.Io.Threaded.init(allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    const src_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/normal-restore-active-writer.aflite", .{tmp.sub_path});
    defer allocator.free(src_path);
    const restored_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/normal-restore-active-writer-restored.aflite", .{tmp.sub_path});
    defer allocator.free(restored_path);
    const cwd_tmp = try std.Io.Dir.cwd().realPathFileAlloc(io, ".zig-cache/tmp", allocator);
    defer allocator.free(cwd_tmp);
    const backup_root = try std.fmt.allocPrint(allocator, "{s}/{s}/normal-restore-active-writer-backups", .{ cwd_tmp, tmp.sub_path });
    defer allocator.free(backup_root);
    const location = try std.fmt.allocPrint(allocator, "file://{s}", .{backup_root});
    defer allocator.free(location);

    var source = try LiteDb.create(allocator, src_path, true);
    defer source.close();

    try source.db.batch(.{
        .writes = &.{.{
            .key = "doc:restore-committed",
            .value = "{\"title\":\"normal restore committed\"}",
        }},
        .sync_level = .write,
    });

    const txn_id: db_types.TxnId = .{ 0x6c, 0x69, 0x74, 0x65, 0x2d, 0x72, 0x65, 0x73, 0x74, 0x6f, 0x72, 0x65, 0x2d, 0, 0, 1 };
    _ = try source.db.beginTransactionWithId(txn_id, 7_000);
    try source.db.writeTransaction(txn_id, .{
        .writes = &.{.{
            .key = "doc:restore-pending",
            .value = "{\"title\":\"normal restore pending\"}",
        }},
    });

    var staged = try stageInputRestoreBackup(allocator, src_path, "docs", "active-writer", location);
    defer staged.deinit(allocator);
    try source.db.abortTransaction(txn_id, 7_001);

    try std.testing.expectEqualStrings("active-writer.afb", staged.snapshot_path);

    const afb_path = try std.fmt.allocPrint(allocator, "{s}/active-writer.afb", .{backup_root});
    defer allocator.free(afb_path);
    const portable = try readFileAlloc(allocator, afb_path, max_afb_file_bytes);
    defer allocator.free(portable);
    try portable_backup.validatePortable(allocator, portable);

    var restored = try LiteDb.create(allocator, restored_path, true);
    defer restored.close();
    try populateUnpublishedLiteDb(allocator, &restored.db, portable);

    var committed = (try restored.db.lookup(allocator, "doc:restore-committed", .{})) orelse return error.MissingRestoredCommittedDoc;
    defer committed.deinit(allocator);
    try std.testing.expect(std.mem.indexOf(u8, committed.json, "\"normal restore committed\"") != null);

    try std.testing.expectEqual(@as(?db_types.LookupResult, null), try restored.db.lookup(allocator, "doc:restore-pending", .{}));
}

test "lite portable backup roundtrips through normal table backup APIs" {
    // Keep leak/safety checks without capturing every allocation/free stack.
    // Opt into tracing when diagnosing a failure.
    var no_stack_allocator: std.heap.SafeAllocator = .init(std.heap.page_allocator, .{ .stack_trace_frames = 0 });
    defer std.debug.assert(no_stack_allocator.deinit() == 0);
    const allocator = if (@import("antfly_platform").env.getenvBool("ANTFLY_TEST_ALLOCATOR_TRACES")) std.testing.allocator else no_stack_allocator.allocator();
    var work_profile = RestoreWorkProfile.init();

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var io_impl = std.Io.Threaded.init(allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    const src_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/lite-normal-lite-src.aflite", .{tmp.sub_path});
    defer allocator.free(src_path);
    const normal_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/normal-middle-db", .{tmp.sub_path});
    defer allocator.free(normal_path);
    const restored_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/lite-normal-lite-restored.aflite", .{tmp.sub_path});
    defer allocator.free(restored_path);
    const cwd_tmp = try std.Io.Dir.cwd().realPathFileAlloc(io, ".zig-cache/tmp", allocator);
    defer allocator.free(cwd_tmp);
    const backup_root = try std.fmt.allocPrint(allocator, "{s}/{s}/lite-normal-lite-backups", .{ cwd_tmp, tmp.sub_path });
    defer allocator.free(backup_root);
    const location = try std.fmt.allocPrint(allocator, "file://{s}", .{backup_root});
    defer allocator.free(location);

    const schema_json: []const u8 =
        \\{"version":0,"default_type":"doc","enforce_types":false,"document_schemas":{"doc":{"schema":{"type":"object","additionalProperties":true}}}}
    ;
    const enrichment_json: []const u8 = "{\"name\":\"roundtrip_chunks_v1\",\"kind\":\"chunk\",\"field\":\"body\",\"chunk_size\":128,\"chunk_overlap\":16}";
    const index_jsons = [_][]const u8{
        "{\"name\":\"roundtrip_ft_body\",\"kind\":\"full_text\",\"config_json\":\"{}\"}",
        "{\"name\":\"roundtrip_dense\",\"kind\":\"dense_vector\",\"config_json\":\"{\\\"field\\\":\\\"embedding\\\",\\\"dims\\\":2,\\\"metric\\\":\\\"l2_squared\\\",\\\"external\\\":true}\"}",
        "{\"name\":\"roundtrip_sparse\",\"kind\":\"sparse_vector\",\"config_json\":\"{\\\"field\\\":\\\"sparse_embedding\\\",\\\"external\\\":true}\"}",
        "{\"name\":\"roundtrip_graph\",\"kind\":\"graph\",\"config_json\":\"{}\"}",
    };

    {
        var source = try LiteDb.create(allocator, src_path, true);
        defer source.close();

        try source.db.setSchemaJson(allocator, schema_json);

        var enrichment = try std.json.parseFromSlice(db_types.EnrichmentConfig, allocator, enrichment_json, .{
            .ignore_unknown_fields = true,
        });
        defer enrichment.deinit();
        try source.db.addEnrichment(enrichment.value);

        for (index_jsons) |index_json| {
            var index = try std.json.parseFromSlice(db_types.IndexConfig, allocator, index_json, .{
                .ignore_unknown_fields = true,
            });
            defer index.deinit();
            try source.db.addIndex(index.value);
        }

        work_profile.mark(&source.db, "source-index-initialization");
        try source.db.batch(.{
            .writes = &.{
                .{
                    .key = "doc:roundtrip:a",
                    .value = "{\"title\":\"normal hop alpha\",\"body\":\"lite normal lite hybrid alpha\",\"_embeddings\":{\"roundtrip_dense\":[1,0],\"roundtrip_sparse\":{\"indices\":[7,42],\"values\":[1.5,0.5]}},\"_edges\":{\"roundtrip_graph\":{\"links\":[{\"target\":\"doc:roundtrip:c\",\"weight\":1.0}]}}}",
                },
                .{
                    .key = "doc:roundtrip:b",
                    .value = "{\"title\":\"normal hop beta\",\"body\":\"lite normal lite hybrid beta\",\"_embeddings\":{\"roundtrip_dense\":[0,1],\"roundtrip_sparse\":{\"indices\":[99],\"values\":[2.0]}}}",
                },
                .{
                    .key = "doc:roundtrip:c",
                    .value = "{\"title\":\"normal hop graph target\"}",
                },
            },
            .sync_level = .full_index,
        });
        work_profile.mark(&source.db, "source-batch");
        try source.db.runUntilIdle();
        work_profile.mark(&source.db, "source-idle");
        try expectIdleMaintenanceDoesNotPublish(&source.db);
    }

    work_profile.markTime("source-close");
    var staged = try stageInputRestoreBackup(allocator, src_path, "docs", "lite-normal-lite-in", location);
    defer staged.deinit(allocator);
    work_profile.markTime("export");

    var backup_location = try backups_api.openBackupLocation(allocator, location);
    defer backup_location.deinit(allocator);
    var lite_manifest = try backups_api.readManifestFromLocation(allocator, &backup_location, "lite-normal-lite-in");
    defer lite_manifest.deinit(allocator);
    try std.testing.expectEqualStrings(schema_json, lite_manifest.schema_json);
    try std.testing.expect(std.mem.indexOf(u8, lite_manifest.indexes_json, "\"roundtrip_dense\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, lite_manifest.indexes_json, "\"roundtrip_chunks_v1\"") != null);

    var normal_db = try db_mod.DB.open(allocator, normal_path, .{});
    defer normal_db.close();
    var normal_source = table_writes.BoundTableWriteSource.init("docs", &normal_db);
    _ = try normal_source.source().restoreTable(allocator, "docs", .{
        .backup_root = backup_root,
        .manifest = &lite_manifest,
        .artifact_backup_id = lite_manifest.backup_id,
        .source_location = location,
    });
    _ = try normal_db.rebuildDenseIndexesForTargetCoverage(allocator);
    _ = try normal_db.rebuildSparseIndexesForTargetCoverage(allocator);
    try normal_db.rebuildGraphIndexesForTargetCoverage(allocator);
    _ = try normal_db.replayGeneratedEnrichmentsFromStoredDocs(allocator);
    try normal_db.runUntilIdle();

    {
        const dense = try searchJson(
            allocator,
            &normal_db,
            "{\"embeddings\":{\"roundtrip_dense\":[1,0]},\"indexes\":[\"roundtrip_dense\"],\"limit\":1}",
        );
        defer allocator.free(dense);
        try std.testing.expect(std.mem.indexOf(u8, dense, "\"doc:roundtrip:a\"") != null);
    }

    const normal_shards = (try normal_source.source().backupTable(allocator, "docs", .{
        .backup_root = backup_root,
        .backup_id = "lite-normal-lite-out",
        .format = .portable,
    })).?;
    defer freeBackupShards(allocator, normal_shards);
    try std.testing.expectEqual(@as(usize, 1), normal_shards.len);
    try std.testing.expectEqualStrings("lite-normal-lite-out.afb", normal_shards[0].snapshot_path);

    const table_schema_json = try allocator.dupe(u8, schema_json);
    defer allocator.free(table_schema_json);
    const table_indexes_json = try allocator.dupe(u8, lite_manifest.indexes_json);
    defer allocator.free(table_indexes_json);
    const table = tables_api.deriveTableRecord("docs", .{
        .schema_json = table_schema_json,
        .indexes_json = table_indexes_json,
    });
    var normal_manifest = try backups_api.createManifest(allocator, "lite-normal-lite-out", .portable, &table, normal_shards);
    defer normal_manifest.deinit(allocator);
    try std.testing.expectEqualStrings(schema_json, normal_manifest.schema_json);
    try std.testing.expect(std.mem.indexOf(u8, normal_manifest.indexes_json, "\"roundtrip_graph\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, normal_manifest.indexes_json, "\"roundtrip_chunks_v1\"") != null);

    const normal_afb_path = try std.fmt.allocPrint(allocator, "{s}/lite-normal-lite-out.afb", .{backup_root});
    defer allocator.free(normal_afb_path);
    const normal_portable = try readFileAlloc(allocator, normal_afb_path, max_afb_file_bytes);
    defer allocator.free(normal_portable);
    try portable_backup.validatePortable(allocator, normal_portable);

    {
        var restored = try LiteDb.create(allocator, restored_path, true);
        defer restored.close();

        try populateUnpublishedLiteDb(allocator, &restored.db, normal_portable);

        const schema = (try restored.db.getSchemaJson(allocator)) orelse return error.MissingLiteSchemaJson;
        defer allocator.free(schema);
        try std.testing.expectEqualStrings(schema_json, schema);

        const indexes = try restored.db.listIndexes(allocator);
        defer db_types.freeIndexConfigs(allocator, indexes);
        // `restored` starts with the default full-text index from
        // `LiteDb.create`, and `populateUnpublishedLiteDb` additively layers
        // the portable backup's 4 indexes on top of it.
        try std.testing.expectEqual(@as(usize, 5), indexes.len);
        try std.testing.expect(indexNamed(indexes, full_text_index_defaults.default_full_text_index_name) != null);

        const enrichments = try restored.db.listEnrichments(allocator);
        defer db_types.freeEnrichmentConfigs(allocator, enrichments);
        try std.testing.expectEqual(@as(usize, 1), enrichments.len);
        try std.testing.expectEqualStrings("roundtrip_chunks_v1", enrichments[0].name);

        const dense = try searchJson(
            allocator,
            &restored.db,
            "{\"embeddings\":{\"roundtrip_dense\":[1,0]},\"indexes\":[\"roundtrip_dense\"],\"limit\":1}",
        );
        defer allocator.free(dense);
        try std.testing.expect(std.mem.indexOf(u8, dense, "\"doc:roundtrip:a\"") != null);

        const sparse = try searchJson(
            allocator,
            &restored.db,
            "{\"embeddings\":{\"roundtrip_sparse\":{\"indices\":[7,42],\"values\":[1.5,0.5]}},\"indexes\":[\"roundtrip_sparse\"],\"limit\":1}",
        );
        defer allocator.free(sparse);
        try std.testing.expect(std.mem.indexOf(u8, sparse, "\"doc:roundtrip:a\"") != null);

        const graph = try searchJson(
            allocator,
            &restored.db,
            "{\"graph_queries\":{\"neighbors\":{\"index\":\"roundtrip_graph\",\"traverse\":{\"start\":{\"keys\":[\"doc:roundtrip:a\"]},\"edge_types\":[\"links\"],\"max_depth\":1}}},\"limit\":10}",
        );
        defer allocator.free(graph);
        try std.testing.expect(std.mem.indexOf(u8, graph, "\"doc:roundtrip:c\"") != null);

        const hybrid = try searchJson(
            allocator,
            &restored.db,
            "{\"full_text_search\":{\"match\":{\"field\":\"body\",\"text\":\"hybrid alpha\"}},\"embeddings\":{\"roundtrip_dense\":[1,0]},\"indexes\":[\"roundtrip_dense\"],\"merge_config\":{\"strategy\":\"rrf\"},\"limit\":3}",
        );
        defer allocator.free(hybrid);
        try std.testing.expect(std.mem.indexOf(u8, hybrid, "\"doc:roundtrip:a\"") != null);
    }
}

// Optional test diagnostics; the production build has no profiling work.
const RestoreWorkProfile = @import("portable_restore.zig").RestoreWorkProfile;

fn expectIdleMaintenanceDoesNotPublish(db: *db_mod.DB) !void {
    const before = db.core.index_manager.snapshotLsmWriteStats();
    // One probe must report quiescence; do not hide extra work in another drain.
    try std.testing.expect(!try db.runLsmMaintenanceStep());
    const after = db.core.index_manager.snapshotLsmWriteStats();
    try std.testing.expectEqual(before.manifest_writes, after.manifest_writes);
    try std.testing.expectEqual(before.manifest_bytes, after.manifest_bytes);
}

const InspectionCounter = struct {
    blocks: usize = 0,
    documents: u64 = 0,

    fn visit(self: *@This(), kind: backup_codec.BlockType, payload: []const u8) !void {
        self.blocks += 1;
        if (kind == .document_batch) self.documents += try backup_codec.documentBatchEntryCount(payload);
    }
};

test "lite restore staging inspector validates AFB1 and enforces both budgets before callbacks" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const fixture = @embedFile("../testdata/cross_backend_v1.afb");
    var file = try tmp.dir.createFile(io, "input.afb2", .{ .read = true });
    defer file.close(io);
    try file.writePositionalAll(io, fixture, 0);
    var counter: InspectionCounter = .{};
    const report = try portable_backup.inspectPortableFileBlocks(a, io, file, .{ .max_input_bytes = fixture.len }, &counter, InspectionCounter.visit);
    try std.testing.expectEqual(@as(u32, 1), report.format_version);
    try std.testing.expectEqual(@as(u64, fixture.len), report.size_bytes);
    try std.testing.expectEqual(@as(u64, 5), counter.documents);
    counter = .{};
    try std.testing.expectError(error.BackupInputLimitExceeded, portable_backup.inspectPortableFileBlocks(a, io, file, .{ .max_input_bytes = fixture.len - 1 }, &counter, InspectionCounter.visit));
    try std.testing.expectError(error.BackupExpansionLimitExceeded, portable_backup.inspectPortableFileBlocks(a, io, file, .{ .max_logical_bytes = report.logical_bytes - 1 }, &counter, InspectionCounter.visit));
    try std.testing.expectEqual(@as(usize, 0), counter.blocks);
    const exact = try portable_backup.inspectPortableFileBlocks(a, io, file, .{ .max_logical_bytes = report.logical_bytes }, &counter, InspectionCounter.visit);
    try std.testing.expectEqualSlices(u8, &report.fingerprint, &exact.fingerprint);
    counter = .{};
    try file.writePositionalAll(io, &.{fixture[fixture.len - 1] ^ 1}, fixture.len - 1);
    try std.testing.expectError(error.BlockCrcMismatch, portable_backup.inspectPortableFileBlocks(a, io, file, .{}, &counter, InspectionCounter.visit));
    try std.testing.expectEqual(@as(usize, 0), counter.blocks);
}

test "lite restore staging detects bytes and accepts afb2 without changing source" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const fixture = @embedFile("../testdata/cross_backend_v1.afb");
    const root = try tmp.dir.realPathFileAlloc(io, ".", a);
    defer a.free(root);
    for ([_][]const u8{ "snapshot.afb2", "renamed.aflite", "untrusted-name.data" }) |name| {
        const location = try std.fmt.allocPrint(a, "file://{s}/prepared/{s}", .{ root, name });
        defer a.free(location);
        try tmp.dir.writeFile(io, .{ .sub_path = name, .data = fixture });
        const path = try std.fs.path.join(a, &.{ root, name });
        defer a.free(path);
        var staged = try stageInputRestoreBackup(a, path, "docs", "portable-test", location);
        defer staged.deinit(a);
        try std.testing.expectEqualStrings("portable-test.afb", staged.snapshot_path);
        const original = try tmp.dir.readFileAlloc(io, name, a, .limited(fixture.len + 1));
        defer a.free(original);
        try std.testing.expectEqualSlices(u8, fixture, original);
        const prepared_path = try std.fmt.allocPrint(a, "prepared/{s}/portable-test.afb", .{name});
        defer a.free(prepared_path);
        const prepared = try tmp.dir.readFileAlloc(io, prepared_path, a, .limited(fixture.len + 1));
        defer a.free(prepared);
        try std.testing.expectEqualSlices(u8, fixture, prepared);
    }
    const id = try defaultBackupIdAlloc(a, "snapshot.afb2");
    defer a.free(id);
    try std.testing.expectEqualStrings("lite-snapshot", id);
}

test "lite restore staging rejects limits before creating a repository" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(io, ".", a);
    defer a.free(root);
    const path = try std.fs.path.join(a, &.{ root, "input.afb" });
    defer a.free(path);
    const location = try std.fmt.allocPrint(a, "file://{s}/never-created", .{root});
    defer a.free(location);
    const fixture = @embedFile("../testdata/cross_backend_v1.afb");
    try tmp.dir.writeFile(io, .{ .sub_path = "input.afb", .data = fixture });
    try std.testing.expectError(error.InvalidBackupId, stageInputRestoreBackupWithOptions(a, path, "docs", "../escape", location, .{}));
    try std.testing.expectError(error.BackupInputLimitExceeded, stageInputRestoreBackupWithOptions(a, path, "docs", "test", location, .{ .max_input_bytes = fixture.len - 1 }));
    try std.testing.expectError(error.BackupExpansionLimitExceeded, stageInputRestoreBackupWithOptions(a, path, "docs", "test", location, .{ .max_prepared_bytes = 1 }));
    try std.testing.expectError(error.FileNotFound, tmp.dir.access(io, "never-created", .{}));
    const lite_path = try std.fs.path.join(a, &.{ root, "input.aflite" });
    defer a.free(lite_path);
    {
        var db = try LiteDb.create(a, lite_path, true);
        defer db.close();
        try db.db.batch(.{ .writes = &.{.{ .key = "one", .value = "{\"text\":\"preserved\"}" }} });
    }
    try std.testing.expectError(error.BackupExpansionLimitExceeded, stageInputRestoreBackupWithOptions(a, lite_path, "docs", "test", location, .{ .max_prepared_bytes = 1 }));
    try std.testing.expectError(error.FileNotFound, tmp.dir.access(io, "never-created", .{}));
    var reopened = try LiteDb.open(a, lite_path, .query_readonly);
    defer reopened.close();
    var doc = (try reopened.db.lookup(a, "one", .{})) orelse return error.MissingDocument;
    defer doc.deinit(a);
    try std.testing.expect(std.mem.indexOf(u8, doc.json, "preserved") != null);
}

test "lite restore staging inspector bounds memory independently of archive length" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var file = try tmp.dir.createFile(io, "large.afb", .{ .read = true });
    defer file.close(io);
    const text = try a.alloc(u8, 256 * 1024);
    defer a.free(text);
    @memset(text, 'a');
    text[0] = '"';
    text[text.len - 1] = '"';
    const payload = try backup_codec.encodeDocumentBatch(a, &.{.{ .key = "doc", .value = text, .value_flags = 0, .timestamp_ns = 0 }});
    defer a.free(payload);
    var buffer: [64 * 1024]u8 = undefined;
    var writer = file.writer(io, &buffer);
    try backup_codec.writeHeaderTo(&writer.interface, .{
        .format_version = backup_codec.legacy_format_version,
        .flags = 0,
        .created_at_ns = 0,
        .backup_id = @splat(0),
        .table_count = 1,
        .shard_count = 1,
    });
    for (0..128) |_| try backup_codec.writeBlockTo(&writer.interface, .document_batch, payload);
    try writer.end();
    const memory = try a.alloc(u8, 2 * 1024 * 1024);
    defer a.free(memory);
    var bounded = std.heap.FixedBufferAllocator.init(memory);
    var counter: InspectionCounter = .{};
    const report = try portable_backup.inspectPortableFileBlocks(bounded.allocator(), io, file, .{}, &counter, InspectionCounter.visit);
    try std.testing.expect(report.size_bytes > 32 * 1024 * 1024);
    try std.testing.expectEqual(@as(u64, 128), counter.documents);
}

test "lite restore staging inspector rejects a source changed by its visitor" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var file = try tmp.dir.createFile(io, "input.afb", .{ .read = true });
    defer file.close(io);
    try file.writePositionalAll(io, @embedFile("../testdata/cross_backend_v1.afb"), 0);
    const Mutator = struct {
        file: std.Io.File,
        io: std.Io,
        fn visit(self: *@This(), _: backup_codec.BlockType, _: []const u8) !void {
            // Mutate bytes already consumed, without changing the file length.
            try self.file.writePositionalAll(self.io, "changed!", 16);
        }
    };
    var mutator: Mutator = .{ .file = file, .io = io };
    try std.testing.expectError(error.SourceFileChanged, portable_backup.inspectPortableFileBlocks(a, io, file, .{}, &mutator, Mutator.visit));
}

test "lite restore staging checks large u64 input sizes without allocating them" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var file = try tmp.dir.createFile(io, "large.afb", .{ .read = true });
    defer file.close(io);
    const large_size: u64 = 26 * 1024 * 1024 * 1024;
    // Sparse length exercises >16 GiB arithmetic; this is not a qualified
    // large-database restore fixture and contains no valid backup payload.
    try file.setLength(io, large_size);
    try checkInputLimit(io, file, max_afb_file_bytes);
    try std.testing.expectError(error.BackupInputLimitExceeded, checkInputLimit(io, file, large_size - 1));
    var counter: InspectionCounter = .{};
    try std.testing.expectError(error.InvalidMagic, portable_backup.inspectPortableFileBlocks(a, io, file, .{}, &counter, InspectionCounter.visit));
    try std.testing.expectEqual(@as(usize, 0), counter.blocks);
}

test "lite restore staging locks archives before parsing their header" {
    const alloc = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(io, ".", alloc);
    defer alloc.free(root);
    const input = try std.fs.path.join(alloc, &.{ root, "busy.afb" });
    defer alloc.free(input);
    const location = try std.fmt.allocPrint(alloc, "file://{s}/never-created", .{root});
    defer alloc.free(location);
    var writer = try tmp.dir.createFile(io, "busy.afb", .{ .read = true, .lock = .exclusive });
    defer writer.close(io);
    const fixture = @embedFile("../testdata/cross_backend_v1.afb");
    try writer.writePositionalAll(io, fixture, 0);
    // Model a writer halfway through replacing the header. No header parsing
    // or budget admission may happen while that writer owns the archive.
    try writer.writePositionalAll(io, &.{fixture[16] ^ 1}, 16);
    try std.testing.expectError(error.WriterLocked, stageInputRestoreBackup(alloc, input, "docs", "test", location));
    try std.testing.expectError(error.FileNotFound, tmp.dir.access(io, "never-created", .{}));
}

test "lite restore staging rejects same-size native source changes since admission" {
    const alloc = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var source = try tmp.dir.createFile(io, "changed.afb2", .{ .read = true });
    defer source.close(io);
    try source.writePositionalAll(io, "old archive", 0);
    // Make the generation change deterministic on coarse-timestamp filesystems.
    try source.setTimestamps(io, .{ .modify_timestamp = .{ .new = .fromNanoseconds(0) } });
    if (!try source.tryLock(io, .shared)) return error.WriterLocked;
    defer source.unlock(io);
    const admitted_stat = try source.stat(io);
    // Advisory locks cannot stop a noncooperating writer. Preserve the size
    // while replacing the generation the caller used for its admission.
    try source.writePositionalAll(io, "new archive", 0);
    const changed_stat = try source.stat(io);
    try std.testing.expectEqual(admitted_stat.size, changed_stat.size);
    try std.testing.expect(!backups_api.localArtifactStatsEqual(admitted_stat, changed_stat));
    const root = try tmp.dir.realPathFileAlloc(io, ".", alloc);
    defer alloc.free(root);
    const location = try std.fmt.allocPrint(alloc, "file://{s}/never-created", .{root});
    defer alloc.free(location);
    // Stale admission must be rejected before any payload decoding or output.
    try std.testing.expectError(error.SourceFileChanged, stageNativeAfbRestoreBackup(
        alloc,
        io,
        source,
        admitted_stat,
        "docs",
        "test",
        location,
        .{ .representation = .native, .backup_id = "test", .table_name = "docs" },
    ));
    try std.testing.expectError(error.FileNotFound, tmp.dir.access(io, "never-created", .{}));
}

test "lite restore staging scratch honors TMPDIR and TEMP" {
    if (comptime !builtin.link_libc or builtin.os.tag == .windows) return error.SkipZigTest;
    const Environment = struct {
        extern fn setenv(name: [*:0]const u8, value: [*:0]const u8, overwrite: c_int) c_int;
        extern fn unsetenv(name: [*:0]const u8) c_int;
    };
    const alloc = std.testing.allocator;
    const io = std.testing.io;
    const env = @import("antfly_platform").env;
    const saved_tmpdir = if (env.getenv("TMPDIR")) |value| try alloc.dupeSentinel(u8, value, 0) else null;
    defer if (saved_tmpdir) |value| alloc.free(value);
    const saved_temp = if (env.getenv("TEMP")) |value| try alloc.dupeSentinel(u8, value, 0) else null;
    defer if (saved_temp) |value| alloc.free(value);
    defer {
        if (saved_tmpdir) |value| {
            _ = Environment.setenv("TMPDIR", value, 1);
        } else {
            _ = Environment.unsetenv("TMPDIR");
        }
        if (saved_temp) |value| {
            _ = Environment.setenv("TEMP", value, 1);
        } else {
            _ = Environment.unsetenv("TEMP");
        }
    }
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const directory = try tmp.dir.realPathFileAlloc(io, ".", alloc);
    defer alloc.free(directory);
    const directory_z = try alloc.dupeSentinel(u8, directory, 0);
    defer alloc.free(directory_z);
    try std.testing.expectEqual(@as(c_int, 0), Environment.setenv("TMPDIR", directory_z, 1));
    try std.testing.expectEqual(@as(c_int, 0), Environment.setenv("TEMP", "/nonexistent-antfly-temp", 1));
    const portable = try temporaryPortablePath(alloc, io);
    defer alloc.free(portable);
    try std.testing.expectEqualStrings(directory, std.fs.path.dirname(portable).?);
    var file = try fs_paths.createFilePortable(io, portable, .{ .exclusive = true, .permissions = .fromMode(0o600) });
    file.close(io);
    try std.Io.Dir.cwd().deleteFile(io, portable);
    try std.testing.expectEqual(@as(c_int, 0), Environment.unsetenv("TMPDIR"));
    try std.testing.expectEqual(@as(c_int, 0), Environment.setenv("TEMP", directory_z, 1));
    const native = try temporaryPreparationPath(alloc, io, "antfly-afb2-", "");
    defer alloc.free(native);
    try std.testing.expectEqualStrings(directory, std.fs.path.dirname(native).?);
    try fs_paths.createDirPathPortable(io, native);
    try std.Io.Dir.cwd().deleteTree(io, native);
}

test "lite restore staging inspector refuses an active archive writer" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var writer = try tmp.dir.createFile(io, "busy.afb", .{ .read = true, .lock = .exclusive });
    defer writer.close(io);
    try writer.writePositionalAll(io, @embedFile("../testdata/cross_backend_v1.afb"), 0);
    var reader = try tmp.dir.openFile(io, "busy.afb", .{});
    defer reader.close(io);
    var counter: InspectionCounter = .{};
    try std.testing.expectError(error.WriterLocked, portable_backup.inspectPortableFileBlocks(a, io, reader, .{}, &counter, InspectionCounter.visit));
    try std.testing.expectEqual(@as(usize, 0), counter.blocks);
}

test "backup preflight Lite and portable inspect all records without changing source" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(io, ".", a);
    defer a.free(root);
    const lite_path = try std.fs.path.join(a, &.{ root, "source.aflite" });
    defer a.free(lite_path);
    const portable_path = try std.fs.path.join(a, &.{ root, "source.bin" });
    defer a.free(portable_path);
    {
        var lite = try LiteDb.createWithOptions(a, lite_path, true, .{ .fsync = false });
        defer lite.close();
        const key = try internal_keys.documentKeyAlloc(a, "last-document");
        defer a.free(key);
        try lite.db.core.store.put(key, "{\"body\":\"/private/incidental\"}");
        var file = try tmp.dir.createFile(io, "source.bin", .{});
        defer file.close(io);
        var buffer: [4096]u8 = undefined;
        var writer = file.writer(io, &buffer);
        try portable_backup.exportPortableToWriter(a, lite.db.core.store, &writer.interface);
        try writer.end();
    }
    for ([_][]const u8{ lite_path, portable_path }) |path| {
        var file = try openInputFile(io, path);
        defer file.close(io);
        const before = try file.stat(io);
        const digest = try preflightFileDigest(io, file, before.size, .{});
        const json = try inspectInputJsonAlloc(a, path, null, .{});
        defer a.free(json);
        var p = try std.json.parseFromSlice(std.json.Value, a, json, .{});
        defer p.deinit();
        try std.testing.expect(p.value.object.get("completeness").?.object.get("scan_complete").?.bool);
        try std.testing.expectEqual(@as(i64, 1), p.value.object.get("inventory").?.object.get("documents").?.integer);
        try std.testing.expectEqualStrings(&digest, p.value.object.get("source").?.object.get("sha256").?.string);
        try std.testing.expect(backups_api.localArtifactStatsEqual(before, try file.stat(io)));
        try std.testing.expectEqualStrings(&digest, &try preflightFileDigest(io, file, before.size, .{}));
        const limited = try inspectInputJsonAlloc(a, path, null, .{ .max_input_bytes = 1 });
        defer a.free(limited);
        try std.testing.expect(std.mem.indexOf(u8, limited, "limit_exceeded") != null);
        try std.testing.expect(std.mem.indexOf(u8, limited, "\"scan_complete\":false") != null);
    }
}

test "backup preflight Lite and portable preserve resolver and model media dependencies" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(io, ".", a);
    defer a.free(root);
    const lite_path = try std.fs.path.join(a, &.{ root, "dependencies.aflite" });
    defer a.free(lite_path);
    const portable_path = try std.fs.path.join(a, &.{ root, "dependencies.afb2" });
    defer a.free(portable_path);
    const policy =
        \\{"decider":{"provider":"antfly","model":"fixture","api_key":"private-key"},"template":"{{m.text}} / {{c.canonical_name}}","question":{"type":"choice","name":"same","instructions":"Are these the same entity?","choices":[{"value":"same"},{"value":"different"}]},"accept":{"choice":"same","min_probability":0.9}}
    ;
    const resolvers = @import("../db/catalog/resolver_catalog.zig");
    const cfg: resolvers.ResolverConfig = .{ .name = "concepts", .table = "entities", .source_artifact = "kg", .resolution_artifact = "resolved", .key_template = "{{_entity.text}}", .scorer_json = "{\"comparisons\":[]}", .adjudicate_json = policy };
    try cfg.validate();
    const resolver_json = try resolvers.serializeCatalog(a, &.{cfg});
    defer a.free(resolver_json);
    const producers = @import("../db/catalog/enrichment_catalog.zig");
    const enrichment_json = try producers.serializeCatalog(a, &.{.{ .name = "chunks", .kind = .chunk, .source_field = "body", .chunker_json = "{\"provider\":\"antfly\",\"embedding\":{\"model\":\"media-model\",\"dimensions\":3}}" }});
    defer a.free(enrichment_json);
    {
        var lite = try LiteDb.createWithOptions(a, lite_path, true, .{ .fsync = false });
        defer lite.close();
        try lite.db.core.store.put("\x00\x00__metadata__:resolvers", resolver_json);
        try lite.db.core.store.put("\x00\x00__metadata__:enrichments", enrichment_json);
        const key = try internal_keys.documentKeyAlloc(a, "clip");
        defer a.free(key);
        try lite.db.core.store.put(key, "{\"body\":{\"type\":\"source\",\"uri\":\"s3://bucket/clip.mp4\",\"mime_type\":\"video/mp4\",\"version\":\"immutable-v1\",\"version_kind\":\"s3_version_id\",\"length\":419430400}}");
        var file = try tmp.dir.createFile(io, "dependencies.afb2", .{});
        defer file.close(io);
        var buffer: [4096]u8 = undefined;
        var writer = file.writer(io, &buffer);
        try portable_backup.exportPortableToWriter(a, lite.db.core.store, &writer.interface);
        try writer.end();
    }
    for ([_][]const u8{ lite_path, portable_path }) |path| {
        const json = try inspectInputJsonAlloc(a, path, "{\"version\":1,\"configuration_id\":\"empty-destination\"}", .{});
        defer a.free(json);
        try std.testing.expect(std.mem.indexOf(u8, json, "\"scan_complete\":true") != null);
        try std.testing.expect(std.mem.indexOf(u8, json, "\"kind\":\"producer_model\"") != null);
        try std.testing.expect(std.mem.indexOf(u8, json, "\"kind\":\"model\"") != null);
        // The connection inventory contains both the frozen model credential
        // and the source bucket; neither raw value may escape into the report.
        try std.testing.expect(std.mem.indexOf(u8, json, "\"field\":\"body\"") != null);
        try std.testing.expect(std.mem.indexOf(u8, json, "\"verdict\":\"unknown\"") != null);
        try std.testing.expect(std.mem.indexOf(u8, json, "private-key") == null);
        try std.testing.expect(std.mem.indexOf(u8, json, "s3://bucket") == null);
    }
}

test "backup preflight native multishard scans later shards and table producers" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(io, ".", a);
    defer a.free(root);
    const bundle_path = try std.fs.path.join(a, &.{ root, "native.afb2" });
    defer a.free(bundle_path);
    var shards: [2]backups_api.ShardSnapshot = undefined;
    var initialized: usize = 0;
    defer for (shards[0..initialized]) |shard| {
        a.free(shard.snapshot_path);
        a.free(shard.artifact_sha256);
    };
    for (&shards, 0..) |*shard, i| {
        const path = try backups_api.shardSnapshotRelPath(a, "test", @intCast(i + 1));
        const absolute = try std.fmt.allocPrintSentinel(a, "{s}/{s}", .{ root, path }, 0);
        defer a.free(absolute);
        {
            var store = try @import("../docstore.zig").DocStore.open(a, absolute, .{});
            defer store.close();
            const key = try internal_keys.documentKeyAlloc(a, if (i == 0) "a" else "z");
            defer a.free(key);
            try store.put(key, if (i == 0) "{\"media\":\"https://example.org/a.pdf\"}" else "{\"media\":\"file:///private/z.pdf\"}");
        }
        shard.* = .{ .group_id = @intCast(i + 1), .start_key = if (i == 0) "" else "m", .end_key = if (i == 0) "m" else null, .snapshot_path = path };
        try backups_api.populateShardArtifactIntegrity(a, io, .native, absolute, shard);
        initialized += 1;
    }
    const manifest: backups_api.TableBackupManifest = .{
        .format = .native,
        .backup_id = "test",
        .table_name = "docs",
        .description = "",
        .schema_json = "{}",
        .read_schema_json = "",
        .replication_sources_json = "[{\"type\":\"postgres\",\"dsn\":\"${secret:replication.pg}\",\"postgres_table\":\"public.documents\"}]",
        .indexes_json = "{\"default\":{\"type\":\"embeddings\",\"field\":\"media\",\"dimension\":3,\"embedder\":{\"model\":\"mock\"}},\"enrichments\":[{\"name\":\"pdf\",\"kind\":\"asset\",\"field\":\"media\",\"producer\":{\"type\":\"reader\"}}]}",
        .shards = &shards,
    };
    try backups_api.writeManifest(a, root, &manifest);
    {
        var file = try tmp.dir.createFile(io, "native.afb2", .{});
        defer file.close(io);
        var buffer: [4096]u8 = undefined;
        var writer = file.writer(io, &buffer);
        try backup_bundle_io.packNativePathsToWriter(a, io, root, &.{ "test-metadata.json", shards[0].snapshot_path, shards[1].snapshot_path }, &writer.interface, .{ .backup_id = "test", .table_name = "docs" });
        try writer.end();
    }
    const json = try inspectInputJsonAlloc(a, bundle_path, null, .{});
    defer a.free(json);
    var parsed = try std.json.parseFromSlice(std.json.Value, a, json, .{});
    defer parsed.deinit();
    try std.testing.expect(parsed.value.object.get("completeness").?.object.get("scan_complete").?.bool);
    try std.testing.expectEqual(@as(i64, 2), parsed.value.object.get("inventory").?.object.get("documents").?.integer);
    try std.testing.expectEqual(@as(i64, 2), parsed.value.object.get("inventory").?.object.get("shards").?.integer);
    try std.testing.expect(std.mem.indexOf(u8, json, "source_local") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"kind\":\"model\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"kind\":\"connection\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "replication.pg") == null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"document_id\":\"z\"") != null);
}

test "backup preflight AFB1 corruption cancellation and missing capabilities cannot certify" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(io, ".", a);
    defer a.free(root);
    const path = try std.fs.path.join(a, &.{ root, "fixture.data" });
    defer a.free(path);
    const bytes = @embedFile("../testdata/cross_backend_v1.afb");
    var file = try tmp.dir.createFile(io, "fixture.data", .{ .read = true });
    defer file.close(io);
    try file.writePositionalAll(io, bytes, 0);
    const json = try inspectInputJsonAlloc(a, path, null, .{});
    defer a.free(json);
    var parsed = try std.json.parseFromSlice(std.json.Value, a, json, .{});
    defer parsed.deinit();
    try std.testing.expect(parsed.value.object.get("completeness").?.object.get("scan_complete").?.bool);
    try std.testing.expectEqual(@as(i64, 5), parsed.value.object.get("inventory").?.object.get("documents").?.integer);
    try std.testing.expectEqualStrings("unknown", parsed.value.object.get("compatibility").?.object.get("verdict").?.string);
    var cancelled = std.atomic.Value(bool).init(true);
    const stopped = try inspectInputJsonAlloc(a, path, null, .{ .cancellation = .fromAtomic(&cancelled) });
    defer a.free(stopped);
    try std.testing.expect(std.mem.indexOf(u8, stopped, "cancelled") != null);
    try file.writePositionalAll(io, &.{bytes[bytes.len - 1] ^ 1}, bytes.len - 1);
    const corrupt = try inspectInputJsonAlloc(a, path, "{\"version\":1,\"configuration_id\":\"test\"}", .{});
    defer a.free(corrupt);
    try std.testing.expect(std.mem.indexOf(u8, corrupt, "\"scan_complete\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, corrupt, "\"verdict\":\"compatible\"") == null);
}

test "backup preflight relational rows use archived schema for active media fields" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(io, ".", a);
    defer a.free(root);
    const source = try std.fmt.allocPrintSentinel(a, "{s}/lsm", .{root}, 0);
    defer a.free(source);
    const path = try std.fs.path.join(a, &.{ root, "relational.afb2" });
    defer a.free(path);
    const schema_json = "{\"version\":1,\"storage_mode\":\"relational\",\"default_type\":\"row\",\"document_schemas\":{\"row\":{\"schema\":{\"type\":\"object\",\"properties\":{\"media\":{\"type\":\"keyword\"}},\"required\":[\"media\"],\"additionalProperties\":false}}}}";
    var schema = try @import("../../schema/mod.zig").parseValidatedTableSchema(a, schema_json);
    defer schema.deinit(a);
    const runtime = try @import("../../schema/mod.zig").deriveRuntimeTableSchema(a, schema);
    defer @import("../schema.zig").freeSchema(a, runtime);
    const encoded = try @import("../schema.zig").serializeSchema(a, runtime);
    defer a.free(encoded);
    const row = try @import("../db/relational_store.zig").encodeValueForSchemaAlloc(a, "{\"media\":\"http://[0:0:0:0:0:0:0:1]:8888/source\"}", runtime);
    defer a.free(row);
    const key = try internal_keys.relationalRowKeyAlloc(a, "row:late");
    defer a.free(key);
    {
        var store = try @import("../docstore.zig").DocStore.open(a, source, .{});
        defer store.close();
        try store.putBatch(&.{
            .{ .key = key, .value = row },
            .{ .key = "\x00\x00__metadata__:schema", .value = encoded },
            .{ .key = "\x00\x00__metadata__:schema_v1", .value = encoded },
            .{ .key = "\x00\x00__metadata__:schema_json", .value = schema_json },
            .{ .key = "\x00\x00__metadata__:enrichments", .value = "[{\"name\":\"pdf\",\"kind\":\"asset\",\"source_field\":\"media\",\"producer_json\":\"{\\\"type\\\":\\\"reader\\\"}\"}]" },
        }, &.{});
        var file = try tmp.dir.createFile(io, "relational.afb2", .{});
        defer file.close(io);
        var buffer: [4096]u8 = undefined;
        var writer = file.writer(io, &buffer);
        try portable_backup.exportPortableToWriter(a, &store, &writer.interface);
        try writer.end();
    }
    const json = try inspectInputJsonAlloc(a, path, null, .{});
    defer a.free(json);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"scan_complete\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "source_local") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "row:late") != null);
}

test "backup preflight document decompression respects expanded byte limit" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    const compressed = "\x28\xb5\x2f\xfd\x04\x58\x94\x00\x00\x50\x7b\x22\x62\x6f\x64\x79\x22\x3a\x22\x61\x01\x00\xf3\xff\x39\x40\x02\x02\x00\x10\x61\x59\x00\x00\x61\x61\x61\x61\x61\x61\x61\x61\x61\x22\x7d\x14\x68\xf4\x0a";
    const decoded = try backup_codec.decompressZstd(a, compressed);
    defer a.free(decoded);
    try std.testing.expectEqual(@as(usize, 262155), decoded.len);
    var archive: std.ArrayListUnmanaged(u8) = .empty;
    defer archive.deinit(a);
    try backup_codec.writeHeader(&archive, a, .{ .format_version = backup_codec.legacy_format_version, .flags = 0, .created_at_ns = 0, .backup_id = @splat(0), .table_count = 1, .shard_count = 1 });
    const payload = try backup_codec.encodeDocumentBatch(a, &.{.{ .key = "doc", .value_flags = backup_codec.doc_value_flag_compressed, .value = compressed, .timestamp_ns = 0 }});
    defer a.free(payload);
    try backup_codec.writeBlock(&archive, a, .document_batch, payload);
    // This proves the input itself is accepted by the shared restore reader.
    try portable_backup.validatePortable(a, archive.items);
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var file = try tmp.dir.createFile(io, "compressed.afb", .{ .read = true });
    defer file.close(io);
    try file.writePositionalAll(io, archive.items, 0);
    const path = try tmp.dir.realPathFileAlloc(io, "compressed.afb", a);
    defer a.free(path);
    const json = try inspectInputJsonAlloc(a, path, "{\"version\":1,\"configuration_id\":\"empty\"}", .{ .max_expanded_bytes = 4096 });
    defer a.free(json);

    try std.testing.expect(std.mem.indexOf(u8, json, "limit_exceeded") != null);
    var counter: InspectionCounter = .{};
    try std.testing.expectError(error.BackupExpansionLimitExceeded, portable_backup.inspectPortableFileBlocks(a, io, file, .{ .max_logical_bytes = 4096 }, &counter, InspectionCounter.visit));
    try std.testing.expectEqual(@as(usize, 0), counter.blocks);
    const inspected = try portable_backup.inspectPortableFileBlocks(a, io, file, .{}, &counter, InspectionCounter.visit);
    try std.testing.expect(inspected.logical_bytes >= decoded.len);
    counter = .{};
    _ = try portable_backup.inspectPortableFileBlocks(a, io, file, .{ .max_logical_bytes = inspected.logical_bytes }, &counter, InspectionCounter.visit);
    try std.testing.expectEqual(@as(u64, 1), counter.documents);
    counter = .{};
    try std.testing.expectError(error.BackupExpansionLimitExceeded, portable_backup.inspectPortableFileBlocks(a, io, file, .{ .max_logical_bytes = inspected.logical_bytes - 1 }, &counter, InspectionCounter.visit));
    try std.testing.expectEqual(@as(usize, 0), counter.blocks);
}

test "backup preflight bounds outer expansion before allocation" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    // Stream-compressed DocumentBatch: one doc with {"body":"<32 MiB of a>"}.
    // The Zstd frame has no declared content size, so admission must bound
    // decoder output rather than relying on a frame-header size check.
    const compressed = "\x28\xb5\x2f\xfd\x04\x58\x14\x01\x00\xd0\x01\x00\x00\x00\x03\x00\x00\x00\x64\x6f\x63\x00\x0b\x00\x00\x02\x7b\x22\x62\x6f\x64\x79\x22\x3a\x22\x61\x01\x00\x8e\xff\xe7\x60\x0c\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x44\x00\x00\x00\x01\x00\xfd\xff\xcd\x0b\x10\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x44\x00\x00\x00\x01\x00\xfd\xff\xcd\x0b\x10\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x44\x00\x00\x00\x01\x00\xfd\xff\xcd\x0b\x10\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x02\x00\x10\x61\x85\x00\x00\x50\x22\x7d\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x5d\x0a\x10\xef\xb6\x4c\xc8";
    const decoded_size = 33554467;
    var archive: std.ArrayListUnmanaged(u8) = .empty;
    defer archive.deinit(a);
    try backup_codec.writeHeader(&archive, a, .{ .format_version = backup_codec.legacy_format_version, .flags = 0, .created_at_ns = 0, .backup_id = @splat(0), .table_count = 1, .shard_count = 1 });
    var envelope: [6]u8 = undefined;
    envelope[0] = @backingInt(backup_codec.BlockType.document_batch);
    envelope[1] = backup_codec.block_flag_compressed;
    std.mem.writeInt(u32, envelope[2..6], compressed.len, .little);
    const Crc = @import("antfly_hash").Crc32;
    var crc = Crc.init();
    crc.update(&envelope);
    crc.update(compressed);
    try archive.appendSlice(a, &envelope);
    try archive.appendSlice(a, compressed);
    var checksum: [4]u8 = undefined;
    std.mem.writeInt(u32, &checksum, crc.final(), .little);
    try archive.appendSlice(a, &checksum);
    try portable_backup.validatePortable(a, archive.items);
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var file = try tmp.dir.createFile(io, "outer.afb", .{ .read = true });
    defer file.close(io);
    try file.writePositionalAll(io, archive.items, 0);
    const memory = try a.alloc(u8, 16 * 1024 * 1024);
    defer a.free(memory);
    var bounded = std.heap.FixedBufferAllocator.init(memory);
    var counter: InspectionCounter = .{};
    try std.testing.expectError(error.BackupExpansionLimitExceeded, portable_backup.inspectPortableFileBlocks(bounded.allocator(), io, file, .{ .max_logical_bytes = 4096 }, &counter, InspectionCounter.visit));
    try std.testing.expectEqual(@as(usize, 0), counter.blocks);
    _ = try portable_backup.inspectPortableFileBlocks(a, io, file, .{ .max_logical_bytes = decoded_size }, &counter, InspectionCounter.visit);
    try std.testing.expectEqual(@as(u64, 1), counter.documents);
    counter = .{};
    try std.testing.expectError(error.BackupExpansionLimitExceeded, portable_backup.inspectPortableFileBlocks(a, io, file, .{ .max_logical_bytes = decoded_size - 1 }, &counter, InspectionCounter.visit));
    try std.testing.expectEqual(@as(usize, 0), counter.blocks);
    var slice = backup_codec.SliceReader.init(archive.items);
    _ = try slice.readHeader();
    try std.testing.expectError(error.BackupExpansionLimitExceeded, slice.readBlockLimited(a, 4096));
}
