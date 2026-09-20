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

//! Durable, bounded source materialization. Only manifest-declared primary and
//! catalog files are needed to decode rows; fresh targets rebuild projections.
const std = @import("std");
const backups = @import("backups.zig");
const native = @import("../storage/db/native_backup.zig");
const fs = @import("../common/fs_paths.zig");
const staging = @import("../storage/db/restore_staging_contract.zig");
const Sha = std.crypto.hash.sha2.Sha256;
pub const chunk_bytes = 8 * 1024 * 1024;

const Checkpoint = struct {
    version: u8 = 1,
    scope: staging.Digest,
    file: usize = 0,
    offset: u64 = 0,
    hash_words: [8]u32 = Sha.init(.{}).s,
    hash_tail: [64]u8 = @splat(0),
    hash_tail_len: u8 = 0,
    pub fn jsonStringify(self: @This(), jw: anytype) @TypeOf(jw.*).Error!void {
        try @import("../storage/db/relational_integrity_json.zig").write(self, jw);
    }
    fn hash(self: Checkpoint) !Sha {
        if (self.version != 1 or self.hash_tail_len >= 64 or self.offset % 64 != self.hash_tail_len) return error.InvalidRestoreSourceCheckpoint;
        return .{ .s = self.hash_words, .buf = self.hash_tail, .buf_len = self.hash_tail_len, .total_len = self.offset };
    }
    fn setHash(self: *Checkpoint, value: Sha) void {
        self.hash_words = value.s;
        self.hash_tail = @splat(0);
        @memcpy(self.hash_tail[0..value.buf_len], value.buf[0..value.buf_len]);
        self.hash_tail_len = value.buf_len;
        self.offset = value.total_len;
    }
};

fn writeAtomic(alloc: std.mem.Allocator, io: std.Io, path: []const u8, bytes: []const u8) !void {
    const temp = try std.fmt.allocPrint(alloc, "{s}.next", .{path});
    defer alloc.free(temp);
    _ = try native.writeFileDurable(io, temp, bytes);
    try std.Io.Dir.rename(.cwd(), temp, .cwd(), path, io);
    try fs.syncDirPortable(io, std.fs.path.dirname(path).?);
}

fn save(alloc: std.mem.Allocator, io: std.Io, root: []const u8, checkpoint: Checkpoint) !void {
    const raw = try std.json.Stringify.valueAlloc(alloc, checkpoint, .{});
    defer alloc.free(raw);
    const encoded = try alloc.alloc(u8, raw.len + 32);
    defer alloc.free(encoded);
    @memcpy(encoded[0..raw.len], raw);
    @memcpy(encoded[raw.len..], &staging.digest(raw));
    const temp = try std.fmt.allocPrint(alloc, "{s}/progress.next", .{root});
    defer alloc.free(temp);
    const path = try std.fmt.allocPrint(alloc, "{s}/progress", .{root});
    defer alloc.free(path);
    _ = try native.writeFileDurable(io, temp, encoded);
    try std.Io.Dir.rename(.cwd(), temp, .cwd(), path, io);
    try fs.syncDirPortable(io, root);
}

/// Returns a parsed, authenticated manifest only once all decoder files are
/// durable. Every unsuccessful slice retains its verified byte/hash prefix.
pub fn step(alloc: std.mem.Allocator, io: std.Io, location: *backups.BackupLocation, source: @import("../metadata/restore_staging.zig").SourceArtifact, scope: staging.Scope, root: []const u8, cancellation: @import("../common/cancellation.zig").CancellationToken) !?native.LoadedManifest {
    return stepWithBudget(alloc, io, location, source, scope, root, cancellation, chunk_bytes);
}

/// Portable uses the identical durable SHA prefix as native objects, followed
/// by an atomic logical-object/row cursor inside a disposable LSM decoder.
pub fn stepPortableWithBudget(alloc: std.mem.Allocator, io: std.Io, location: *backups.BackupLocation, source: @import("../metadata/restore_staging.zig").SourceArtifact, scope: staging.Scope, owner_range: @import("../storage/docstore.zig").ByteRange, root: []const u8, cancellation: @import("../common/cancellation.zig").CancellationToken, byte_budget: usize) !bool {
    if (byte_budget == 0 or byte_budget > chunk_bytes or source.format != .portable or !std.mem.eql(u8, &try source.digest(alloc), &scope.source_descriptor_digest)) return error.RestoreSourceProofMissing;
    if ((source.cohort_seal == null) == (source.rewrite == null)) return error.RestoreSourceProofMissing;
    try cancellation.check();
    try fs.createDirPathPortable(io, root);
    const state_path = try std.fmt.allocPrint(alloc, "{s}/progress", .{root});
    defer alloc.free(state_path);
    const raw = native.readFileAlloc(alloc, io, state_path, 4096) catch |err| switch (err) {
        error.FileNotFound => null,
        else => return err,
    };
    defer if (raw) |bytes| alloc.free(bytes);
    var checkpoint: Checkpoint = .{ .scope = scope.digest() };
    if (raw) |bytes| {
        if (bytes.len < 32 or !std.mem.eql(u8, bytes[bytes.len - 32 ..], &staging.digest(bytes[0 .. bytes.len - 32]))) return error.InvalidRestoreSourceCheckpoint;
        var parsed = try std.json.parseFromSlice(Checkpoint, alloc, bytes[0 .. bytes.len - 32], .{});
        defer parsed.deinit();
        checkpoint = parsed.value;
        if (!std.mem.eql(u8, &checkpoint.scope, &scope.digest()) or checkpoint.file > 1 or checkpoint.offset > source.artifact_size_bytes) return error.InvalidRestoreSourceCheckpoint;
    }
    const artifact_path = try std.fmt.allocPrint(alloc, "{s}/source.afb", .{root});
    defer alloc.free(artifact_path);
    const artifact = try fs.createFilePortable(io, artifact_path, .{ .read = true, .truncate = false });
    defer artifact.close(io);
    if (checkpoint.file == 0) {
        if ((try artifact.stat(io)).size < checkpoint.offset) return error.InvalidRestoreSourceCheckpoint;
        var hash = try checkpoint.hash();
        const bytes = try backups.readFileRangeFromLocationUsingIo(alloc, io, location, source.snapshot_path, source.artifact_size_bytes, checkpoint.offset, byte_budget, cancellation);
        defer alloc.free(bytes);
        try artifact.writePositionalAll(io, bytes, checkpoint.offset);
        hash.update(bytes);
        checkpoint.setHash(hash);
        if (checkpoint.offset == source.artifact_size_bytes) {
            var digest: [32]u8 = undefined;
            hash.final(&digest);
            if (!std.mem.eql(u8, &digest, &source.artifact_sha256)) return error.BackupArtifactIntegrityMismatch;
            try artifact.setLength(io, source.artifact_size_bytes);
            checkpoint.file = 1;
        }
        try artifact.sync(io);
        try fs.syncDirPortable(io, root);
        try save(alloc, io, root, checkpoint);
        return false;
    }
    if (checkpoint.offset != source.artifact_size_bytes or (try artifact.stat(io)).size != source.artifact_size_bytes) return error.InvalidRestoreSourceCheckpoint;
    var finished_hash = try checkpoint.hash();
    var finished_digest: [32]u8 = undefined;
    finished_hash.final(&finished_digest);
    if (!std.mem.eql(u8, &finished_digest, &source.artifact_sha256)) return error.InvalidRestoreSourceCheckpoint;
    return stepPortableDecoder(alloc, io, artifact, source, scope, owner_range, root, cancellation);
}

/// Shared bounded logical import after either repository checksum verification
/// or the peer transport's exact immutable certificate verification.
pub fn stepPortableDecoder(alloc: std.mem.Allocator, io: std.Io, artifact: std.Io.File, source: @import("../metadata/restore_staging.zig").SourceArtifact, scope: staging.Scope, owner_range: @import("../storage/docstore.zig").ByteRange, root: []const u8, cancellation: @import("../common/cancellation.zig").CancellationToken) !bool {
    try cancellation.check();
    if (source.format != .portable or (source.cohort_seal == null) == (source.rewrite == null) or
        !std.mem.eql(u8, &try source.digest(alloc), &scope.source_descriptor_digest)) return error.RestoreSourceProofMissing;
    const files = try std.fmt.allocPrint(alloc, "{s}/files", .{root});
    defer alloc.free(files);
    // The importer synchronizes each page's rows AND restart checkpoint before
    // returning. This private decoder does not serve reads or acknowledge user
    // writes: commit-time full durability would sync the very same WAL twice
    // per page, burning the slice budget on redundant disk barriers.
    var options = @import("../storage/db/config.zig").portable_decoder_lsm_options_default;
    options.read_runtime = @import("../storage/lsm_backend/storage_io.zig").ReadRuntime.init(io);
    var backend = try @import("../storage/lsm_backend.zig").Backend.open(alloc, files, options);
    defer backend.close();
    var store = try @import("../storage/docstore.zig").DocStore.openRuntime(alloc, try backend.runtimeStore(alloc, .{ .name = "docs" }));
    defer store.close();
    const portable = @import("../storage/portable_backup.zig");
    // A page may only advance a manifest/layout phase, without importing a
    // row. Amortize decoder opens and coordinator RPCs across a bounded burst
    // of these durable steps. Each page still commits its restart checkpoint;
    // cancellation and either budget end the burst without losing progress.
    const started = std.Io.Clock.awake.now(io);
    var complete = false;
    for (0..8) |_| {
        try cancellation.check();
        complete = if (source.rewrite) |rewrite| try portable.importSourceCopyFilePage(alloc, &store, io, artifact, source.artifact_size_bytes, .{ .scope = rewrite.source_scope orelse return error.RestoreSourceProofMissing, .applied_index = rewrite.source_applied_index, .retained_start = rewrite.retained_start }, scope.digest(), 128, cancellation) else try portable.importCohortFilePage(alloc, &store, io, artifact, source.artifact_size_bytes, .{ .seal = source.cohort_seal.?, .namespace = scope.source_namespace }, scope.digest(), 128, cancellation);
        if (complete or started.durationTo(std.Io.Clock.awake.now(io)).toNanoseconds() >= 25 * std.time.ns_per_ms) break;
    }
    if (complete) try bindPortableDecoderRange(alloc, &store, owner_range);
    try fs.syncDirPortable(io, files);
    try fs.syncDirPortable(io, root);
    return complete;
}

/// Portable archives intentionally omit routing metadata. Only a private,
/// disposable decoder reconstructs it from the target owner's authenticated
/// immutable plan/bootstrap. Persist before decoder publication; a replay may
/// confirm the same range but must never replace a different bound range.
pub fn bindPortableDecoderRange(alloc: std.mem.Allocator, store: *@import("../storage/docstore.zig").DocStore, owner_range: @import("../storage/docstore.zig").ByteRange) !void {
    const range_state = @import("../storage/db/range_state.zig");
    if (owner_range.end.len != 0 and std.mem.order(u8, owner_range.start, owner_range.end) != .lt) return error.RestoreStagingScopeChanged;
    const encoded = try range_state.encodeRangeAlloc(alloc, owner_range);
    defer alloc.free(encoded);
    var txn = try store.beginWriteTxn();
    var txn_open = true;
    defer if (txn_open) txn.abort();
    const existing = txn.get(range_state.range_key) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
    if (existing) |bytes| {
        if (!std.mem.eql(u8, bytes, encoded)) return error.RestoreStagingScopeChanged;
        txn.abort();
        txn_open = false;
        try store.sync(true);
        return;
    }
    try txn.put(range_state.range_key, encoded);
    try txn.commit();
    txn_open = false;
    try store.sync(true);
}

pub fn stepWithBudget(alloc: std.mem.Allocator, io: std.Io, location: *backups.BackupLocation, source: @import("../metadata/restore_staging.zig").SourceArtifact, scope: staging.Scope, root: []const u8, cancellation: @import("../common/cancellation.zig").CancellationToken, byte_budget: usize) !?native.LoadedManifest {
    if (byte_budget == 0 or byte_budget > chunk_bytes) return error.InvalidBackupRange;
    try cancellation.check();
    if (source.native_manifest_size_bytes == 0 or source.native_manifest_sha256.len != 64 or !std.mem.eql(u8, &try source.digest(alloc), &scope.source_descriptor_digest)) return error.RestoreSourceProofMissing;
    try fs.createDirPathPortable(io, root);
    const manifest_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ root, native.manifest_file_name });
    defer alloc.free(manifest_path);
    const raw = native.readFileAlloc(alloc, io, manifest_path, native.max_manifest_bytes) catch |err| switch (err) {
        error.FileNotFound => blk: {
            const remote_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ source.snapshot_path, native.manifest_file_name });
            defer alloc.free(remote_path);
            const fetched = try backups.readFileFromLocationUsingIoLimited(alloc, io, location, remote_path, native.max_manifest_bytes);
            errdefer alloc.free(fetched);
            var digest: [32]u8 = undefined;
            Sha.hash(fetched, &digest, .{});
            if (fetched.len != source.native_manifest_size_bytes or !std.mem.eql(u8, &std.fmt.bytesToHex(digest, .lower), source.native_manifest_sha256)) return error.BackupArtifactIntegrityMismatch;
            try writeAtomic(alloc, io, manifest_path, fetched);
            break :blk fetched;
        },
        else => return err,
    };
    defer alloc.free(raw);
    var manifest_digest: [32]u8 = undefined;
    Sha.hash(raw, &manifest_digest, .{});
    if (raw.len != source.native_manifest_size_bytes or !std.mem.eql(u8, &std.fmt.bytesToHex(manifest_digest, .lower), source.native_manifest_sha256)) return error.BackupArtifactIntegrityMismatch;
    var loaded = try native.parseManifestBytes(alloc, raw);
    var keep = false;
    defer if (!keep) loaded.deinit();
    if (source.cohort_seal) |seal| {
        const receipt_path = try std.fmt.allocPrint(alloc, "{s}/backup-seal-receipt.json", .{root});
        defer alloc.free(receipt_path);
        const receipt = native.readFileAlloc(alloc, io, receipt_path, 4096) catch |err| switch (err) {
            error.FileNotFound => blk: {
                const remote = try std.fmt.allocPrint(alloc, "{s}/backup-seal-receipt.json", .{source.snapshot_path});
                defer alloc.free(remote);
                const value = try backups.readFileFromLocationUsingIoLimited(alloc, io, location, remote, 4096);
                errdefer alloc.free(value);
                try writeAtomic(alloc, io, receipt_path, value);
                break :blk value;
            },
            else => return err,
        };
        defer alloc.free(receipt);
        _ = try @import("../storage/db/native_backup_seal.zig").exportedBytes(alloc, io, root, seal);
    }
    // Physical LSM checkpoints are independently readable. Logical backups
    // use the shared portable/logical importer, not an invented physical codec.
    if (!std.mem.eql(u8, loaded.value().primary.artifact_format, "antfly-lsm-checkpoint")) return error.UnsupportedBackupFormat;
    const checkpoint_path = try std.fmt.allocPrint(alloc, "{s}/progress", .{root});
    defer alloc.free(checkpoint_path);
    const encoded = native.readFileAlloc(alloc, io, checkpoint_path, 4096) catch |err| switch (err) {
        error.FileNotFound => null,
        else => return err,
    };
    defer if (encoded) |bytes| alloc.free(bytes);
    var checkpoint: Checkpoint = .{ .scope = scope.digest() };
    if (encoded) |bytes| {
        if (bytes.len < 32 or !std.mem.eql(u8, bytes[bytes.len - 32 ..], &staging.digest(bytes[0 .. bytes.len - 32]))) return error.InvalidRestoreSourceCheckpoint;
        var parsed = try std.json.parseFromSlice(Checkpoint, alloc, bytes[0 .. bytes.len - 32], .{});
        defer parsed.deinit();
        checkpoint = parsed.value;
        if (!std.mem.eql(u8, &checkpoint.scope, &scope.digest()) or checkpoint.file > loaded.value().artifacts.len) return error.InvalidRestoreSourceCheckpoint;
    }
    _ = try checkpoint.hash();
    if (checkpoint.file == loaded.value().artifacts.len and checkpoint.offset != 0) return error.InvalidRestoreSourceCheckpoint;
    var remaining: usize = byte_budget;
    var visited: usize = 0;
    while (checkpoint.file < loaded.value().artifacts.len) {
        try cancellation.check();
        if (visited == 32) {
            try save(alloc, io, root, checkpoint);
            return null;
        }
        visited += 1;
        const artifact = loaded.value().artifacts[checkpoint.file];
        if (artifact.role == .projection or artifact.role == .shared_acceleration) {
            if (checkpoint.offset != 0) return error.InvalidRestoreSourceCheckpoint;
            checkpoint.file += 1;
            continue;
        }
        if (checkpoint.offset > artifact.size_bytes) return error.InvalidRestoreSourceCheckpoint;
        if (remaining == 0) {
            try save(alloc, io, root, checkpoint);
            return null;
        }
        const from = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ source.snapshot_path, artifact.path });
        defer alloc.free(from);
        const destination = try std.fmt.allocPrint(alloc, "{s}/files/{s}", .{ root, artifact.install_path });
        defer alloc.free(destination);
        try fs.createDirPathPortable(io, std.fs.path.dirname(destination).?);
        const bytes = try backups.readFileRangeFromLocationUsingIo(alloc, io, location, from, artifact.size_bytes, checkpoint.offset, remaining, cancellation);
        defer alloc.free(bytes);
        const first_chunk = checkpoint.offset == 0;
        var hash = try checkpoint.hash();
        const file = try fs.createFilePortable(io, destination, .{ .truncate = false });
        defer file.close(io);
        if ((try file.stat(io)).size < checkpoint.offset) return error.InvalidRestoreSourceCheckpoint;
        try file.writePositionalAll(io, bytes, checkpoint.offset);
        hash.update(bytes);
        checkpoint.setHash(hash);
        remaining -= bytes.len;
        if (checkpoint.offset == artifact.size_bytes) {
            var digest: [32]u8 = undefined;
            hash.final(&digest);
            if (!std.mem.eql(u8, &std.fmt.bytesToHex(digest, .lower), artifact.sha256)) return error.BackupArtifactIntegrityMismatch;
            try file.setLength(io, artifact.size_bytes);
            checkpoint.file += 1;
            checkpoint.setHash(Sha.init(.{}));
        }
        try file.sync(io);
        if (first_chunk) {
            // A durable progress record must never certify a file whose newly
            // created directory entry can disappear after a power loss.
            var parent = std.fs.path.dirname(destination).?;
            while (parent.len > root.len) {
                try fs.syncDirPortable(io, parent);
                parent = std.fs.path.dirname(parent) orelse return error.InvalidRestoreSourceCheckpoint;
            }
        }
        try save(alloc, io, root, checkpoint);
    }
    keep = true;
    return loaded;
}

/// A deterministic existing-generation sibling survives loss of the worker
/// between the directory rename and publication. The cache root is already
/// scope-specific; the full digest additionally binds the durable stage name.
pub fn stagePath(alloc: std.mem.Allocator, live: []const u8, scope: staging.Scope) ![]u8 {
    return std.fmt.allocPrint(alloc, "{s}.restore-stage-0-{s}", .{ live, std.fmt.bytesToHex(scope.digest(), .lower) });
}

pub fn hasScope(alloc: std.mem.Allocator, io: std.Io, root: []const u8, scope: staging.Scope) !bool {
    const path = try std.fmt.allocPrint(alloc, "{s}/restore-source.scope", .{root});
    defer alloc.free(path);
    const bytes = native.readFileAlloc(alloc, io, path, 64) catch |err| switch (err) {
        error.FileNotFound => return false,
        else => return err,
    };
    defer alloc.free(bytes);
    if (!std.mem.eql(u8, bytes, &scope.digest())) return error.RestoreStagingScopeChanged;
    return true;
}

/// All files and their parents were fsynced before progress advanced. The
/// decoder's scope marker certifies its one-time validation. Final installation
/// is therefore a directory rename, not O(number of files) hardlinks/resyncs.
pub fn installDurableTree(alloc: std.mem.Allocator, io: std.Io, root: []const u8, destination: []const u8, scope: staging.Scope) !void {
    if (try hasScope(alloc, io, destination, scope)) {
        try fs.syncDirPortable(io, root);
        try fs.syncDirPortable(io, std.fs.path.dirname(destination).?);
        return;
    }
    const files = try std.fmt.allocPrint(alloc, "{s}/files", .{root});
    defer alloc.free(files);
    if (!try hasScope(alloc, io, files, scope)) return error.InvalidRestoreSourceCheckpoint;
    try std.Io.Dir.rename(.cwd(), files, .cwd(), destination, io);
    try fs.syncDirPortable(io, root);
    try fs.syncDirPortable(io, std.fs.path.dirname(destination).?);
}
