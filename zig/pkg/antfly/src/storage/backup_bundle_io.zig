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

const SourceBlob = struct {
    sha256: [Sha256.digest_length]u8,
    size_bytes: u64,
    source_index: usize,
    included: bool,
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

fn readTrailerFromFile(io: std.Io, source: std.Io.File, source_size: u64) !bundle.Trailer {
    if (source_size < backup_codec.header_size + bundle.trailer_size)
        return error.InvalidBundleFooter;
    var encoded: [bundle.trailer_size]u8 = undefined;
    const offset = source_size - bundle.trailer_size;
    if (try source.readPositionalAll(io, &encoded, offset) != encoded.len)
        return error.InvalidBundleFooter;
    return try bundle.decodeTrailer(&encoded, source_size);
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
        };
    }
    var source_blobs = std.ArrayListUnmanaged(SourceBlob).empty;
    defer source_blobs.deinit(alloc);
    try source_blobs.ensureTotalCapacity(alloc, files.items.len);
    for (files.items, 0..) |file, source_index| {
        source_blobs.appendAssumeCapacity(.{
            .sha256 = file.sha256,
            .size_bytes = file.size_bytes,
            .source_index = source_index,
            .included = !digestInSortedSet(options.base_object_sha256, &digest_strings[source_index]),
        });
    }
    std.mem.sort(SourceBlob, source_blobs.items, {}, struct {
        fn lessThan(_: void, lhs: SourceBlob, rhs: SourceBlob) bool {
            return std.mem.order(u8, &lhs.sha256, &rhs.sha256) == .lt;
        }
    }.lessThan);
    var unique_len: usize = 0;
    for (source_blobs.items) |candidate| {
        if (unique_len > 0 and std.mem.eql(u8, &source_blobs.items[unique_len - 1].sha256, &candidate.sha256)) {
            if (source_blobs.items[unique_len - 1].size_bytes != candidate.size_bytes or
                source_blobs.items[unique_len - 1].included != candidate.included)
                return error.InvalidBackupManifest;
            continue;
        }
        source_blobs.items[unique_len] = candidate;
        unique_len += 1;
    }
    source_blobs.items.len = unique_len;

    const blobs = try alloc.alloc(bundle.BlobDescriptor, source_blobs.items.len);
    defer alloc.free(blobs);
    for (source_blobs.items, 0..) |blob, index| {
        blobs[index] = .{
            .sha256 = &digest_strings[blob.source_index],
            .logical_size_bytes = blob.size_bytes,
            .stored_size_bytes = blob.size_bytes,
            .included = blob.included,
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
        .blobs = blobs,
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

    for (source_blobs.items, 0..) |blob, ordinal| {
        if (!blob.included) continue;
        const source = files.items[blob.source_index];
        try footer_entries.append(alloc, .{
            .sha256 = blob.sha256,
            .header_offset = out.offset,
            .stored_size_bytes = blob.size_bytes,
        });
        const digest_hex = std.fmt.bytesToHex(blob.sha256, .lower);
        const blob_path = try std.fmt.allocPrint(alloc, "blobs/{s}", .{digest_hex});
        defer alloc.free(blob_path);
        const header_payload = try bundle.encodeBlobHeaderAlloc(alloc, .{
            .ordinal = @intCast(ordinal),
            .logical_path = blob_path,
            .role = "native_blob",
            .logical_size_bytes = blob.size_bytes,
            .stored_size_bytes = blob.size_bytes,
            .sha256 = blob.sha256,
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
        while (offset < blob.size_bytes) {
            const wanted: usize = @intCast(@min(@as(u64, chunk_buffer.len), blob.size_bytes - offset));
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
    const footer_offset = out.offset;
    try out.block(.footer_index, footer);
    const trailer = bundle.encodeTrailer(.{
        .footer_offset = footer_offset,
        .footer_payload_size = footer.len,
    });
    try sink.writeAll(&trailer);
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

/// Exact parent content provider used only for blobs omitted by an AFB2 delta.
/// Implementations may stream from a repository, another bundle, or a local
/// content cache, but must materialize precisely the requested digest.
pub const NativeBaseBlobSource = struct {
    manifest_sha256: []const u8,
    context: *anyopaque,
    materialize: *const fn (
        context: *anyopaque,
        alloc: std.mem.Allocator,
        io: std.Io,
        sha256: []const u8,
        logical_size_bytes: u64,
        destination_path: []const u8,
    ) anyerror!void,
};

/// Extracts a self-contained native AFB2 bundle into a new staging directory.
/// The destination must not exist. On any failure it is removed recursively.
pub fn extractNativeFileToStagingDirectory(
    alloc: std.mem.Allocator,
    io: std.Io,
    source: std.Io.File,
    source_size: u64,
    staging_root: []const u8,
) !void {
    return try extractNativeFileToStagingDirectoryWithBase(
        alloc,
        io,
        source,
        source_size,
        staging_root,
        null,
    );
}

/// Extracts full or delta native AFB2. A delta is admitted only with a source
/// pinned to the exact parent manifest digest declared by the bundle.
pub fn extractNativeFileToStagingDirectoryWithBase(
    alloc: std.mem.Allocator,
    io: std.Io,
    source: std.Io.File,
    source_size: u64,
    staging_root: []const u8,
    base: ?NativeBaseBlobSource,
) !void {
    if (pathExists(io, staging_root)) return error.PathAlreadyExists;
    const blob_root = try std.fmt.allocPrint(alloc, "{s}.afb2-blobs", .{staging_root});
    defer alloc.free(blob_root);
    if (pathExists(io, blob_root)) return error.PathAlreadyExists;
    try fs_paths.createDirPathPortable(io, staging_root);
    errdefer std.Io.Dir.cwd().deleteTree(io, staging_root) catch {};
    try fs_paths.createDirPathPortable(io, blob_root);
    defer std.Io.Dir.cwd().deleteTree(io, blob_root) catch {};

    const trailer = try readTrailerFromFile(io, source, source_size);
    var reader = backup_codec.FileReader.init(io, source, source_size - bundle.trailer_size);
    const header = try reader.readHeader();
    if (header.format_version != backup_codec.format_version) return error.BackupArtifactFormatMismatch;
    const manifest_block = try reader.readBlock(alloc);
    defer alloc.free(manifest_block.payload);
    if (manifest_block.block_type != .bundle_manifest) return error.InvalidBackupManifest;
    var parsed_manifest = try bundle.parseManifest(alloc, manifest_block.payload);
    defer parsed_manifest.deinit();
    try bundle.validateReadablePayloadFeatures(parsed_manifest.value);
    if (parsed_manifest.value.representation != .native)
        return error.BackupArtifactFormatMismatch;
    switch (parsed_manifest.value.mode) {
        .full => if (base != null) return error.UnexpectedBackupBase,
        .delta => {
            const required = parsed_manifest.value.parent_manifest_sha256.?;
            const supplied = base orelse return error.BackupBaseRequired;
            try bundle.validateSha256(supplied.manifest_sha256);
            if (!std.mem.eql(u8, required, supplied.manifest_sha256))
                return error.BackupBaseMismatch;
        },
    }

    var next_blob_index = nextIncludedBlobIndex(parsed_manifest.value.blobs, 0);
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
                const blob_index = next_blob_index orelse return error.IncompleteBackupInventory;
                const descriptor = parsed_manifest.value.blobs[blob_index];
                if (blob_header.ordinal != blob_index or blob_header.compression != descriptor.compression)
                    return error.InvalidBackupManifest;
                const expected_hex = std.fmt.bytesToHex(blob_header.sha256, .lower);
                const expected_blob_path = try std.fmt.allocPrint(alloc, "blobs/{s}", .{expected_hex});
                defer alloc.free(expected_blob_path);
                if (!std.mem.eql(u8, descriptor.sha256, &expected_hex) or
                    !std.mem.eql(u8, expected_blob_path, blob_header.logical_path) or
                    !std.mem.eql(u8, blob_header.role, "native_blob") or
                    descriptor.logical_size_bytes != blob_header.logical_size_bytes or
                    descriptor.stored_size_bytes != blob_header.stored_size_bytes)
                    return error.BackupArtifactIntegrityMismatch;
                try observed_footer.append(alloc, .{
                    .sha256 = blob_header.sha256,
                    .header_offset = block_offset,
                    .stored_size_bytes = blob_header.stored_size_bytes,
                });
                const destination = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ blob_root, descriptor.sha256 });
                defer alloc.free(destination);
                const file = try fs_paths.createFilePortable(io, destination, .{ .truncate = true, .exclusive = true });
                current = .{ .header = blob_header, .file = file, .hasher = Sha256.init(.{}), .written = 0 };
                next_blob_index = nextIncludedBlobIndex(parsed_manifest.value.blobs, blob_index + 1);
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
                if (block_offset != trailer.footer_offset or block.payload.len != trailer.footer_payload_size)
                    return error.InvalidBundleFooter;
                const footer = try bundle.decodeFooterIndexAlloc(alloc, block.payload);
                defer alloc.free(footer);
                if (footer.len != observed_footer.items.len or
                    footer.len != includedBlobCount(parsed_manifest.value.blobs))
                    return error.InvalidBundleFooter;
                for (footer, observed_footer.items) |declared, observed| {
                    if (!std.meta.eql(declared, observed)) return error.InvalidBundleFooter;
                }
                footer_seen = true;
                if (reader.hasRemaining()) return error.InvalidBundleFooter;
            },
            else => return error.InvalidBackupManifest,
        }
    }
    try finishExtractedBlob(alloc, io, &current);
    if (!footer_seen or next_blob_index != null)
        return error.IncompleteBackupInventory;

    for (parsed_manifest.value.blobs) |blob| {
        const blob_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ blob_root, blob.sha256 });
        defer alloc.free(blob_path);
        if (!blob.included) {
            const source_base = base orelse return error.BackupBaseRequired;
            try source_base.materialize(
                source_base.context,
                alloc,
                io,
                blob.sha256,
                blob.logical_size_bytes,
                blob_path,
            );
        }
        try verifyFileDigest(io, blob_path, blob.logical_size_bytes, blob.sha256);
    }
    for (parsed_manifest.value.objects) |object| {
        const blob_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ blob_root, object.sha256 });
        defer alloc.free(blob_path);
        const destination = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ staging_root, object.logical_path });
        defer alloc.free(destination);
        if (std.fs.path.dirname(destination)) |parent| try fs_paths.createDirPathPortable(io, parent);
        try copyFileBounded(io, blob_path, destination, object.size_bytes);
    }
    try fs_paths.syncDirPortable(io, staging_root);
}

fn nextIncludedBlobIndex(blobs: []const bundle.BlobDescriptor, start: usize) ?u32 {
    for (blobs[start..], start..) |blob, index| {
        if (blob.included) return @intCast(index);
    }
    return null;
}

fn includedBlobCount(blobs: []const bundle.BlobDescriptor) usize {
    var count: usize = 0;
    for (blobs) |blob| if (blob.included) {
        count += 1;
    };
    return count;
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

const TestBaseBlobContext = struct {
    root: []const u8,
    manifest: bundle.Manifest,
};

fn materializeTestBaseBlob(
    context_ptr: *anyopaque,
    alloc: std.mem.Allocator,
    io: std.Io,
    sha256: []const u8,
    logical_size_bytes: u64,
    destination_path: []const u8,
) !void {
    const context: *TestBaseBlobContext = @ptrCast(@alignCast(context_ptr));
    for (context.manifest.objects) |object| {
        if (!std.mem.eql(u8, object.sha256, sha256)) continue;
        if (object.size_bytes != logical_size_bytes) return error.BackupArtifactIntegrityMismatch;
        const source_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ context.root, object.logical_path });
        defer alloc.free(source_path);
        return try copyFileBounded(io, source_path, destination_path, logical_size_bytes);
    }
    return error.BackupBlobMissing;
}

test "AFB2 native delta requires and resolves the exact parent manifest" {
    const alloc = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const base_root = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/base", .{tmp.sub_path});
    defer alloc.free(base_root);
    const child_root = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/child", .{tmp.sub_path});
    defer alloc.free(child_root);
    try fs_paths.createDirPathPortable(io, base_root);
    try fs_paths.createDirPathPortable(io, child_root);
    const base_store = try std.fmt.allocPrint(alloc, "{s}/store.bin", .{base_root});
    defer alloc.free(base_store);
    const base_segment = try std.fmt.allocPrint(alloc, "{s}/segment.bin", .{base_root});
    defer alloc.free(base_segment);
    const child_store = try std.fmt.allocPrint(alloc, "{s}/store.bin", .{child_root});
    defer alloc.free(child_store);
    const child_segment = try std.fmt.allocPrint(alloc, "{s}/segment.bin", .{child_root});
    defer alloc.free(child_segment);
    try writeTestFile(io, base_store, "unchanged-primary");
    try writeTestFile(io, base_segment, "old-segment");
    try writeTestFile(io, child_store, "unchanged-primary");
    try writeTestFile(io, child_segment, "new-segment");

    var base_archive: std.ArrayList(u8) = .empty;
    defer base_archive.deinit(alloc);
    var base_writer = std.Io.Writer.Allocating.fromArrayList(alloc, &base_archive);
    try packNativeDirectoryToWriter(alloc, io, base_root, &base_writer.writer, .{});
    base_archive = base_writer.toArrayList();
    const base_archive_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/base.afb", .{tmp.sub_path});
    defer alloc.free(base_archive_path);
    try writeTestFile(io, base_archive_path, base_archive.items);
    var base_file = try std.Io.Dir.cwd().openFile(io, base_archive_path, .{});
    defer base_file.close(io);
    var base_manifest = try readManifestFromFile(alloc, io, base_file, base_archive.items.len);
    defer base_manifest.deinit();
    const base_manifest_bytes = try bundle.encodeManifestAlloc(alloc, base_manifest.value);
    defer alloc.free(base_manifest_bytes);
    var parent_digest_bytes: [Sha256.digest_length]u8 = undefined;
    Sha256.hash(base_manifest_bytes, &parent_digest_bytes, .{});
    const parent_digest = std.fmt.bytesToHex(parent_digest_bytes, .lower);

    const base_digests = try alloc.alloc([]const u8, base_manifest.value.blobs.len);
    defer alloc.free(base_digests);
    for (base_manifest.value.blobs, 0..) |blob, index| base_digests[index] = blob.sha256;
    var delta_archive: std.ArrayList(u8) = .empty;
    defer delta_archive.deinit(alloc);
    var delta_writer = std.Io.Writer.Allocating.fromArrayList(alloc, &delta_archive);
    try packNativeDirectoryToWriter(alloc, io, child_root, &delta_writer.writer, .{
        .mode = .delta,
        .parent_manifest_sha256 = &parent_digest,
        .base_object_sha256 = base_digests,
    });
    delta_archive = delta_writer.toArrayList();
    const delta_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/delta.afb", .{tmp.sub_path});
    defer alloc.free(delta_path);
    try writeTestFile(io, delta_path, delta_archive.items);
    var delta_file = try std.Io.Dir.cwd().openFile(io, delta_path, .{});
    defer delta_file.close(io);

    const missing_base_root = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/missing-base", .{tmp.sub_path});
    defer alloc.free(missing_base_root);
    try std.testing.expectError(
        error.BackupBaseRequired,
        extractNativeFileToStagingDirectory(alloc, io, delta_file, delta_archive.items.len, missing_base_root),
    );

    var base_context: TestBaseBlobContext = .{ .root = base_root, .manifest = base_manifest.value };
    const restored_root = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/restored-delta", .{tmp.sub_path});
    defer alloc.free(restored_root);
    try extractNativeFileToStagingDirectoryWithBase(
        alloc,
        io,
        delta_file,
        delta_archive.items.len,
        restored_root,
        .{
            .manifest_sha256 = &parent_digest,
            .context = &base_context,
            .materialize = materializeTestBaseBlob,
        },
    );
    const restored_store = try std.fmt.allocPrint(alloc, "{s}/store.bin", .{restored_root});
    defer alloc.free(restored_store);
    const restored_segment = try std.fmt.allocPrint(alloc, "{s}/segment.bin", .{restored_root});
    defer alloc.free(restored_segment);
    const store_bytes = try std.Io.Dir.cwd().readFileAlloc(io, restored_store, alloc, .limited(1024));
    defer alloc.free(store_bytes);
    const segment_bytes = try std.Io.Dir.cwd().readFileAlloc(io, restored_segment, alloc, .limited(1024));
    defer alloc.free(segment_bytes);
    try std.testing.expectEqualStrings("unchanged-primary", store_bytes);
    try std.testing.expectEqualStrings("new-segment", segment_bytes);
}

fn writeTestFile(io: std.Io, path: []const u8, contents: []const u8) !void {
    var file = try fs_paths.createFilePortable(io, path, .{ .truncate = true });
    defer file.close(io);
    try file.writePositionalAll(io, contents, 0);
    try file.sync(io);
}
