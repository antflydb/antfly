// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0.

//! Streaming AFB2 native bundle packing and staged extraction.
//!
//! Native capture produces an immutable directory before this layer runs.
//! Packing hashes that generation, emits its complete manifest, then streams
//! each blob in bounded chunks. A second stat fences source mutation. Restore
//! writes only to a caller-owned staging directory and verifies every digest;
//! publication remains the restore owner's atomic generation swap.

const std = @import("std");
const backup_codec = @import("backup_codec.zig");
const bundle = @import("backup_bundle.zig");
const fs_paths = @import("../common/fs_paths.zig");

const Sha256 = std.crypto.hash.sha2.Sha256;
const io_buffer_bytes: usize = 256 * 1024;

const SourceFile = struct {
    logical_path: []u8,
    size_bytes: u64,
    sha256: [Sha256.digest_length]u8,
    stat: std.Io.File.Stat,

    fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.logical_path);
        self.* = undefined;
    }
};

const ExtractingBlob = struct {
    header: bundle.DecodedBlobHeader,
    file: std.Io.File,
    hasher: Sha256,
    written: u64,
};

const CountingOutput = struct {
    writer: *std.Io.Writer,
    offset: u64 = 0,

    fn header(self: *@This(), value: backup_codec.FileHeader) !void {
        try backup_codec.writeHeaderTo(self.writer, value);
        self.offset += backup_codec.header_size;
    }

    fn block(self: *@This(), kind: backup_codec.BlockType, payload: []const u8) !void {
        try backup_codec.writeBlockTo(self.writer, kind, payload);
        self.offset += backup_codec.block_envelope_overhead + payload.len;
    }
};

pub const PackOptions = struct {
    header_backup_id: [16]u8 = [_]u8{0} ** 16,
    backup_id: []const u8 = "",
    table_name: []const u8 = "",
    created_at_unix_ns: i64 = 0,
    storage_engine: []const u8 = "antfly-native",
    mode: bundle.SnapshotMode = .full,
    parent_manifest_sha256: ?[]const u8 = null,
    /// Digest-sorted complete base inventory. In delta mode, matching objects
    /// remain in the new complete manifest but their bytes are omitted.
    base_object_sha256: []const []const u8 = &.{},
};

pub fn readManifestFromFile(
    alloc: std.mem.Allocator,
    io: std.Io,
    source: std.Io.File,
    source_size: u64,
) !bundle.ParsedManifest {
    var reader = backup_codec.FileReader.init(io, source, source_size);
    const header = try reader.readHeader();
    if (header.format_version != backup_codec.format_version)
        return error.BackupArtifactFormatMismatch;
    const manifest_block = try reader.readBlock(alloc);
    defer alloc.free(manifest_block.payload);
    if (manifest_block.block_type != .bundle_manifest) return error.InvalidBackupManifest;
    return try bundle.parseManifest(alloc, manifest_block.payload);
}

pub fn packNativeDirectoryToWriter(
    alloc: std.mem.Allocator,
    io: std.Io,
    source_root: []const u8,
    sink: *std.Io.Writer,
    options: PackOptions,
) !void {
    return try packNativePathsToWriter(alloc, io, source_root, &.{}, sink, options);
}

/// Packs a complete native table generation from selected roots beneath a
/// backup repository directory. An empty selection means the whole directory;
/// otherwise only exact files and descendants of selected directories enter
/// the manifest, preventing unrelated backups from leaking into one bundle.
pub fn packNativePathsToWriter(
    alloc: std.mem.Allocator,
    io: std.Io,
    source_root: []const u8,
    selected_roots: []const []const u8,
    sink: *std.Io.Writer,
    options: PackOptions,
) !void {
    switch (options.mode) {
        .full => if (options.parent_manifest_sha256 != null or options.base_object_sha256.len != 0)
            return error.InvalidBackupManifest,
        .delta => {
            try bundle.validateSha256(options.parent_manifest_sha256 orelse return error.InvalidBackupManifest);
            var previous: ?[]const u8 = null;
            for (options.base_object_sha256) |digest| {
                try bundle.validateSha256(digest);
                if (previous) |value| if (std.mem.order(u8, value, digest) != .lt)
                    return error.NonCanonicalBackupManifest;
                previous = digest;
            }
        },
    }
    for (selected_roots, 0..) |path, index| {
        try bundle.validateRelativePath(path);
        for (selected_roots[0..index]) |previous| {
            if (std.mem.eql(u8, path, previous)) return error.InvalidBackupManifest;
        }
    }
    var files = try collectSourceFiles(alloc, io, source_root, selected_roots);
    defer {
        for (files.items) |*file| file.deinit(alloc);
        files.deinit(alloc);
    }
    if (files.items.len == 0) return error.BackupArtifactMissing;

    const objects = try alloc.alloc(bundle.ObjectDescriptor, files.items.len);
    defer alloc.free(objects);
    var digest_strings = try alloc.alloc([Sha256.digest_length * 2]u8, files.items.len);
    defer alloc.free(digest_strings);
    for (files.items, 0..) |file, index| {
        digest_strings[index] = std.fmt.bytesToHex(file.sha256, .lower);
        objects[index] = .{
            .logical_path = file.logical_path,
            .role = "native_file",
            .size_bytes = file.size_bytes,
            .sha256 = &digest_strings[index],
            .included = !digestInSortedSet(options.base_object_sha256, &digest_strings[index]),
        };
    }
    const manifest_bytes = try bundle.encodeManifestAlloc(alloc, .{
        .backup_id = options.backup_id,
        .table_name = options.table_name,
        .representation = .native,
        .mode = options.mode,
        .parent_manifest_sha256 = options.parent_manifest_sha256,
        .created_at_unix_ns = options.created_at_unix_ns,
        .compatibility = .{ .storage_engine = options.storage_engine },
        .objects = objects,
    });
    defer alloc.free(manifest_bytes);

    var out: CountingOutput = .{ .writer = sink };
    try out.header(.{
        .format_version = backup_codec.format_version,
        .flags = 0,
        .created_at_ns = options.created_at_unix_ns,
        .backup_id = options.header_backup_id,
        .table_count = 1,
        .shard_count = 1,
    });
    try out.block(.bundle_manifest, manifest_bytes);

    var footer_entries = std.ArrayListUnmanaged(bundle.FooterIndexEntry).empty;
    defer footer_entries.deinit(alloc);
    var chunk_buffer = try alloc.alloc(u8, bundle.native_chunk_target_bytes);
    defer alloc.free(chunk_buffer);

    for (files.items, 0..) |source, ordinal| {
        if (!objects[ordinal].included) continue;
        try footer_entries.append(alloc, .{
            .sha256 = source.sha256,
            .header_offset = out.offset,
            .stored_size_bytes = source.size_bytes,
        });
        const header_payload = try bundle.encodeBlobHeaderAlloc(alloc, .{
            .ordinal = @intCast(ordinal),
            .logical_path = source.logical_path,
            .role = "native_file",
            .logical_size_bytes = source.size_bytes,
            .stored_size_bytes = source.size_bytes,
            .sha256 = source.sha256,
        });
        defer alloc.free(header_payload);
        try out.block(.blob_header, header_payload);

        const absolute_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ source_root, source.logical_path });
        defer alloc.free(absolute_path);
        var file = if (std.fs.path.isAbsolute(absolute_path))
            try std.Io.Dir.openFileAbsolute(io, absolute_path, .{})
        else
            try std.Io.Dir.cwd().openFile(io, absolute_path, .{});
        defer file.close(io);
        var offset: u64 = 0;
        while (offset < source.size_bytes) {
            const wanted: usize = @intCast(@min(@as(u64, chunk_buffer.len), source.size_bytes - offset));
            const read = try file.readPositionalAll(io, chunk_buffer[0..wanted], offset);
            if (read != wanted) return error.SourceFileChanged;
            const chunk_payload = try bundle.encodeBlobChunkAlloc(alloc, .{
                .ordinal = @intCast(ordinal),
                .offset = offset,
                .bytes = chunk_buffer[0..wanted],
            });
            defer alloc.free(chunk_payload);
            try out.block(.blob_chunk, chunk_payload);
            offset += wanted;
        }
        const final_stat = try file.stat(io);
        if (!sameFileGeneration(source.stat, final_stat)) return error.SourceFileChanged;
    }

    const footer = try bundle.encodeFooterIndexAlloc(alloc, footer_entries.items);
    defer alloc.free(footer);
    try out.block(.footer_index, footer);
}

fn digestInSortedSet(sorted: []const []const u8, digest: []const u8) bool {
    var low: usize = 0;
    var high: usize = sorted.len;
    while (low < high) {
        const mid = low + (high - low) / 2;
        switch (std.mem.order(u8, sorted[mid], digest)) {
            .lt => low = mid + 1,
            .gt => high = mid,
            .eq => return true,
        }
    }
    return false;
}

fn collectSourceFiles(
    alloc: std.mem.Allocator,
    io: std.Io,
    root: []const u8,
    selected_roots: []const []const u8,
) !std.ArrayListUnmanaged(SourceFile) {
    var result = std.ArrayListUnmanaged(SourceFile).empty;
    errdefer {
        for (result.items) |*file| file.deinit(alloc);
        result.deinit(alloc);
    }
    var dir = if (std.fs.path.isAbsolute(root))
        try std.Io.Dir.openDirAbsolute(io, root, .{ .iterate = true, .follow_symlinks = false })
    else
        try std.Io.Dir.cwd().openDir(io, root, .{ .iterate = true, .follow_symlinks = false });
    defer dir.close(io);
    var walker = try dir.walk(alloc);
    defer walker.deinit();
    while (try walker.next(io)) |entry| {
        switch (entry.kind) {
            .directory => {},
            .file => {
                if (!pathSelected(entry.path, selected_roots)) continue;
                if (result.items.len >= bundle.max_objects) return error.BackupManifestTooLarge;
                try bundle.validateRelativePath(entry.path);
                const logical_path = try normalizedPathAlloc(alloc, entry.path);
                errdefer alloc.free(logical_path);
                const absolute_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ root, entry.path });
                defer alloc.free(absolute_path);
                var file = if (std.fs.path.isAbsolute(absolute_path))
                    try std.Io.Dir.openFileAbsolute(io, absolute_path, .{})
                else
                    try std.Io.Dir.cwd().openFile(io, absolute_path, .{});
                defer file.close(io);
                const stat = try file.stat(io);
                var hasher = Sha256.init(.{});
                var buffer: [io_buffer_bytes]u8 = undefined;
                var offset: u64 = 0;
                while (offset < stat.size) {
                    const wanted: usize = @intCast(@min(@as(u64, buffer.len), stat.size - offset));
                    const read = try file.readPositionalAll(io, buffer[0..wanted], offset);
                    if (read != wanted) return error.SourceFileChanged;
                    hasher.update(buffer[0..wanted]);
                    offset += wanted;
                }
                const final_stat = try file.stat(io);
                if (!sameFileGeneration(stat, final_stat)) return error.SourceFileChanged;
                var digest: [Sha256.digest_length]u8 = undefined;
                hasher.final(&digest);
                try result.append(alloc, .{
                    .logical_path = logical_path,
                    .size_bytes = stat.size,
                    .sha256 = digest,
                    .stat = stat,
                });
            },
            else => return error.UnsupportedFileType,
        }
    }
    std.mem.sort(SourceFile, result.items, {}, struct {
        fn lessThan(_: void, lhs: SourceFile, rhs: SourceFile) bool {
            return std.mem.order(u8, lhs.logical_path, rhs.logical_path) == .lt;
        }
    }.lessThan);
    return result;
}

fn pathSelected(path: []const u8, selected_roots: []const []const u8) bool {
    if (selected_roots.len == 0) return true;
    for (selected_roots) |root| {
        if (std.mem.eql(u8, path, root)) return true;
        if (path.len > root.len and std.mem.startsWith(u8, path, root) and
            (path[root.len] == '/' or path[root.len] == std.fs.path.sep))
            return true;
    }
    return false;
}

fn normalizedPathAlloc(alloc: std.mem.Allocator, path: []const u8) ![]u8 {
    const owned = try alloc.dupe(u8, path);
    if (std.fs.path.sep != '/') for (owned) |*char| if (char.* == std.fs.path.sep) {
        char.* = '/';
    };
    return owned;
}

fn sameFileGeneration(lhs: std.Io.File.Stat, rhs: std.Io.File.Stat) bool {
    return lhs.inode == rhs.inode and lhs.size == rhs.size and
        std.meta.eql(lhs.mtime, rhs.mtime) and std.meta.eql(lhs.ctime, rhs.ctime);
}

/// Extracts a self-contained native AFB2 bundle into a new staging directory.
/// The destination must not exist. On any failure it is removed recursively.
pub fn extractNativeFileToStagingDirectory(
    alloc: std.mem.Allocator,
    io: std.Io,
    source: std.Io.File,
    source_size: u64,
    staging_root: []const u8,
) !void {
    if (pathExists(io, staging_root)) return error.PathAlreadyExists;
    try fs_paths.createDirPathPortable(io, staging_root);
    errdefer std.Io.Dir.cwd().deleteTree(io, staging_root) catch {};

    var reader = backup_codec.FileReader.init(io, source, source_size);
    const header = try reader.readHeader();
    if (header.format_version != backup_codec.format_version) return error.BackupArtifactFormatMismatch;
    const manifest_block = try reader.readBlock(alloc);
    defer alloc.free(manifest_block.payload);
    if (manifest_block.block_type != .bundle_manifest) return error.InvalidBackupManifest;
    var parsed_manifest = try bundle.parseManifest(alloc, manifest_block.payload);
    defer parsed_manifest.deinit();
    if (parsed_manifest.value.representation != .native or parsed_manifest.value.mode != .full)
        return error.BackupArtifactFormatMismatch;

    var next_ordinal: u32 = 0;
    var footer_seen = false;
    var observed_footer = std.ArrayListUnmanaged(bundle.FooterIndexEntry).empty;
    defer observed_footer.deinit(alloc);
    var current: ?ExtractingBlob = null;
    defer if (current) |*active| {
        active.file.close(io);
        active.header.deinit(alloc);
    };

    while (reader.hasRemaining()) {
        const block_offset = reader.pos;
        const block = try reader.readBlock(alloc);
        defer alloc.free(block.payload);
        switch (block.block_type) {
            .blob_header => {
                try finishExtractedBlob(alloc, io, &current);
                var blob_header = try bundle.decodeBlobHeader(alloc, block.payload);
                errdefer blob_header.deinit(alloc);
                if (blob_header.ordinal != next_ordinal or blob_header.compression != .none)
                    return error.InvalidBackupManifest;
                if (@as(usize, next_ordinal) >= parsed_manifest.value.objects.len) return error.IncompleteBackupInventory;
                const descriptor = parsed_manifest.value.objects[next_ordinal];
                const expected_hex = std.fmt.bytesToHex(blob_header.sha256, .lower);
                if (!std.mem.eql(u8, descriptor.logical_path, blob_header.logical_path) or
                    !std.mem.eql(u8, descriptor.sha256, &expected_hex) or
                    descriptor.size_bytes != blob_header.logical_size_bytes or
                    blob_header.logical_size_bytes != blob_header.stored_size_bytes or
                    !std.mem.eql(u8, descriptor.role, blob_header.role))
                    return error.BackupArtifactIntegrityMismatch;
                try observed_footer.append(alloc, .{
                    .sha256 = blob_header.sha256,
                    .header_offset = block_offset,
                    .stored_size_bytes = blob_header.stored_size_bytes,
                });
                const destination = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ staging_root, blob_header.logical_path });
                defer alloc.free(destination);
                if (std.fs.path.dirname(destination)) |parent| try fs_paths.createDirPathPortable(io, parent);
                const file = try fs_paths.createFilePortable(io, destination, .{ .truncate = true, .exclusive = true });
                current = .{ .header = blob_header, .file = file, .hasher = Sha256.init(.{}), .written = 0 };
            },
            .blob_chunk => {
                const active = if (current) |*value| value else return error.InvalidBackupManifest;
                const chunk = try bundle.decodeBlobChunk(block.payload);
                if (chunk.ordinal != active.header.ordinal or chunk.offset != active.written or
                    chunk.bytes.len > active.header.stored_size_bytes -| active.written)
                    return error.InvalidNativeFileChunk;
                try active.file.writePositionalAll(io, chunk.bytes, active.written);
                active.hasher.update(chunk.bytes);
                active.written += chunk.bytes.len;
            },
            .footer_index => {
                try finishExtractedBlob(alloc, io, &current);
                const footer = try bundle.decodeFooterIndexAlloc(alloc, block.payload);
                defer alloc.free(footer);
                if (footer.len != observed_footer.items.len or footer.len != parsed_manifest.value.objects.len)
                    return error.InvalidBundleFooter;
                for (footer, observed_footer.items) |declared, observed| {
                    if (!std.meta.eql(declared, observed)) return error.InvalidBundleFooter;
                }
                footer_seen = true;
                if (reader.hasRemaining()) return error.InvalidBundleFooter;
            },
            else => return error.InvalidBackupManifest,
        }
        if (block.block_type == .blob_header) next_ordinal += 1;
    }
    try finishExtractedBlob(alloc, io, &current);
    if (!footer_seen or next_ordinal != parsed_manifest.value.objects.len)
        return error.IncompleteBackupInventory;
    try fs_paths.syncDirPortable(io, staging_root);
}

fn finishExtractedBlob(
    alloc: std.mem.Allocator,
    io: std.Io,
    current: *?ExtractingBlob,
) !void {
    if (current.*) |*active| {
        if (active.written != active.header.stored_size_bytes) return error.BackupArtifactIntegrityMismatch;
        var actual: [Sha256.digest_length]u8 = undefined;
        active.hasher.final(&actual);
        if (!std.crypto.timing_safe.eql(@TypeOf(actual), actual, active.header.sha256))
            return error.BackupArtifactIntegrityMismatch;
        try active.file.sync(io);
        active.file.close(io);
        active.header.deinit(alloc);
        current.* = null;
    }
}

fn pathExists(io: std.Io, path: []const u8) bool {
    var dir = if (std.fs.path.isAbsolute(path))
        std.Io.Dir.openDirAbsolute(io, path, .{}) catch return false
    else
        std.Io.Dir.cwd().openDir(io, path, .{}) catch return false;
    dir.close(io);
    return true;
}

test "AFB2 native directory round trips through staged extraction" {
    const alloc = std.testing.allocator;
    const io = std.testing.io;
    var source_tmp = std.testing.tmpDir(.{});
    defer source_tmp.cleanup();
    var output_tmp = std.testing.tmpDir(.{});
    defer output_tmp.cleanup();

    const source_root = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/native", .{source_tmp.sub_path});
    defer alloc.free(source_root);
    try fs_paths.createDirPathPortable(io, source_root);
    const nested = try std.fmt.allocPrint(alloc, "{s}/indexes/dense", .{source_root});
    defer alloc.free(nested);
    try fs_paths.createDirPathPortable(io, nested);
    const primary_path = try std.fmt.allocPrint(alloc, "{s}/store.bin", .{source_root});
    defer alloc.free(primary_path);
    const dense_path = try std.fmt.allocPrint(alloc, "{s}/segment.bin", .{nested});
    defer alloc.free(dense_path);
    try writeTestFile(io, primary_path, "primary-state");
    try writeTestFile(io, dense_path, "searchable-dense-generation");

    var archive: std.ArrayList(u8) = .empty;
    defer archive.deinit(alloc);
    var allocating = std.Io.Writer.Allocating.fromArrayList(alloc, &archive);
    try packNativeDirectoryToWriter(alloc, io, source_root, &allocating.writer, .{});
    archive = allocating.toArrayList();

    const archive_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/native.afb", .{output_tmp.sub_path});
    defer alloc.free(archive_path);
    try writeTestFile(io, archive_path, archive.items);
    var archive_file = try std.Io.Dir.cwd().openFile(io, archive_path, .{});
    defer archive_file.close(io);
    const restore_root = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/restored", .{output_tmp.sub_path});
    defer alloc.free(restore_root);
    try extractNativeFileToStagingDirectory(alloc, io, archive_file, archive.items.len, restore_root);

    const restored_dense_path = try std.fmt.allocPrint(alloc, "{s}/indexes/dense/segment.bin", .{restore_root});
    defer alloc.free(restored_dense_path);
    const restored = try std.Io.Dir.cwd().readFileAlloc(io, restored_dense_path, alloc, .limited(1024));
    defer alloc.free(restored);
    try std.testing.expectEqualStrings("searchable-dense-generation", restored);
}

fn writeTestFile(io: std.Io, path: []const u8, contents: []const u8) !void {
    var file = try fs_paths.createFilePortable(io, path, .{ .truncate = true });
    defer file.close(io);
    try file.writePositionalAll(io, contents, 0);
    try file.sync(io);
}
