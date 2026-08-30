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

const std = @import("std");
const builtin = @import("builtin");
const platform_sync = @import("antfly_platform").sync;
const platform_time = @import("antfly_platform").time;
const fs_paths = @import("../../common/fs_paths.zig");
const threaded_io_limits = @import("../../common/threaded_io_limits.zig");
const http_server = @import("../transport/http_server.zig");
const snapshot_transfer = @import("../transport/snapshot_transfer.zig");
const raft_engine = @import("raft_engine");

const MappedSnapshotOwner = struct {
    alloc: std.mem.Allocator,
    mapped: []align(std.heap.page_size_min) u8,

    fn release(ptr: *anyopaque) void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const alloc = self.alloc;
        std.posix.munmap(self.mapped);
        alloc.destroy(self);
    }
};

pub const SnapshotArtifactPolicy = struct {
    max_bytes: u64 = 4 * 1024 * 1024 * 1024,
    max_count: usize = 1024,
    staging_ttl_ns: i128 = 15 * std.time.ns_per_min,
    committed_ttl_ns: i128 = 24 * std.time.ns_per_hour,
    /// Sliding lease renewed by manifest and chunk reads. It fences committed
    /// TTL cleanup across the gaps between HTTP requests without keeping a
    /// file descriptor or worker alive for every transfer.
    active_fetch_lease_ns: i128 = 5 * std.time.ns_per_min,
};

pub const FileSnapshotStoreConfig = struct {
    root_dir: []const u8,
    max_snapshot_bytes: usize = 1 << 30,
    max_chunk_bytes: usize = snapshot_transfer.max_chunk_bytes,
    artifact_policy: SnapshotArtifactPolicy = .{},
};

pub const FileSnapshotStore = struct {
    const artifact_maintenance_interval_ns: u64 = std.time.ns_per_min;
    const FetchLease = struct {
        generation: snapshot_transfer.Generation,
        deadline_ns: u64,
    };

    const ArtifactHeader = struct {
        generation: snapshot_transfer.Generation,
        data_offset: u64,
        data_len: u64,
    };

    alloc: std.mem.Allocator,
    cfg: FileSnapshotStoreConfig,
    io_impl: std.Io.Threaded,
    root_dir: []u8,
    upload_locks: [1024]std.atomic.Mutex = [_]std.atomic.Mutex{.unlocked} ** 1024,
    artifact_lifecycle_mutex: std.atomic.Mutex = .unlocked,
    next_artifact_maintenance_ns: std.atomic.Value(u64) = .init(0),
    fetch_lease_mutex: std.atomic.Mutex = .unlocked,
    fetch_leases: std.StringHashMapUnmanaged(FetchLease) = .empty,

    pub fn init(alloc: std.mem.Allocator, cfg: FileSnapshotStoreConfig) !FileSnapshotStore {
        if (cfg.max_snapshot_bytes == 0 or cfg.max_chunk_bytes < snapshot_transfer.min_chunk_bytes or
            cfg.max_chunk_bytes > snapshot_transfer.max_chunk_bytes)
            return error.InvalidSnapshotTransferLimits;
        if (cfg.artifact_policy.max_bytes == 0 or cfg.artifact_policy.max_count == 0 or
            cfg.artifact_policy.active_fetch_lease_ns <= 0)
            return error.InvalidSnapshotArtifactPolicy;
        var self: FileSnapshotStore = .{
            .alloc = alloc,
            .cfg = cfg,
            .io_impl = threaded_io_limits.initService(alloc),
            .root_dir = try alloc.dupe(u8, cfg.root_dir),
        };
        errdefer {
            self.alloc.free(self.root_dir);
            self.io_impl.deinit();
        }
        self.cleanupExpiredArtifacts() catch |err| {
            std.log.warn("raft snapshot artifact startup cleanup deferred root={s} err={s}", .{
                self.root_dir,
                @errorName(err),
            });
        };
        self.deferArtifactMaintenance();
        return self;
    }

    pub fn deinit(self: *FileSnapshotStore) void {
        var lease_keys = self.fetch_leases.keyIterator();
        while (lease_keys.next()) |key| self.alloc.free(key.*);
        self.fetch_leases.deinit(self.alloc);
        self.alloc.free(self.root_dir);
        self.io_impl.deinit();
        self.* = undefined;
    }

    pub fn store(self: *FileSnapshotStore) http_server.SnapshotStore {
        return .{
            .ptr = self,
            .vtable = &.{
                .put_snapshot = putSnapshot,
                .get_snapshot = getSnapshot,
                .release_snapshot = releaseSnapshot,
                .begin_chunked_snapshot = beginChunkedSnapshot,
                .put_snapshot_chunk = putSnapshotChunk,
                .commit_chunked_snapshot = commitChunkedSnapshot,
                .get_snapshot_upload_manifest = getSnapshotUploadManifest,
                .get_snapshot_manifest = getSnapshotManifest,
                .get_snapshot_chunk = getSnapshotChunk,
                .abort_chunked_snapshot = abortChunkedSnapshot,
                .release_chunked_snapshot = releaseChunkedSnapshot,
                .max_chunk_bytes = maxChunkBytes,
            },
        };
    }

    fn maxChunkBytes(ptr: *anyopaque) usize {
        const self: *FileSnapshotStore = @ptrCast(@alignCast(ptr));
        return self.cfg.max_chunk_bytes;
    }

    fn putSnapshot(ptr: *anyopaque, alloc: std.mem.Allocator, snapshot_id: []const u8, body: []const u8) !void {
        _ = alloc;
        const self: *FileSnapshotStore = @ptrCast(@alignCast(ptr));
        try validateSnapshotId(snapshot_id);
        if (body.len > self.cfg.max_snapshot_bytes) return error.SnapshotTooLarge;
        try self.cleanupExpiredArtifacts();
        platform_sync.lockYielding(&self.artifact_lifecycle_mutex);
        defer self.artifact_lifecycle_mutex.unlock();
        const lock = self.uploadLock(snapshot_id);
        platform_sync.lockYielding(lock);
        defer lock.unlock();
        const staging_path = try self.transferPath(snapshot_id, ".snap.part");
        defer self.alloc.free(staging_path);
        const committed_path = try self.snapshotPath(snapshot_id);
        defer self.alloc.free(committed_path);

        const io_ctx = io(self);
        try fs_paths.createDirPathPortable(io_ctx, self.root_dir);
        if (self.committedSnapshotMatches(io_ctx, committed_path, body)) |matches| {
            // Snapshot identifiers are immutable generation keys. A sender may
            // legitimately retry after losing the successful response, so an
            // exact replay is success without consuming a second quota slot.
            // Reusing the identifier for different bytes is always a conflict.
            if (matches) return;
            return error.SnapshotGenerationMismatch;
        } else |err| switch (err) {
            error.FileNotFound => {},
            else => return err,
        }
        try self.admitArtifact(staging_path, body.len);
        var file = try fs_paths.createFilePortable(io_ctx, staging_path, .{ .truncate = true });
        var file_open = true;
        errdefer if (file_open) file.close(io_ctx);
        errdefer std.Io.Dir.cwd().deleteFile(io_ctx, staging_path) catch {};
        var write_buffer: [64 * 1024]u8 = undefined;
        var writer = file.writer(io_ctx, &write_buffer);
        try writer.interface.writeAll(body);
        try writer.end();
        try file.sync(io_ctx);
        file.close(io_ctx);
        file_open = false;
        // Rename is the publication point: readers see the previous complete
        // artifact or the new complete artifact, never a torn body.
        try std.Io.Dir.rename(std.Io.Dir.cwd(), staging_path, std.Io.Dir.cwd(), committed_path, io_ctx);
        try fs_paths.syncDirPortable(io_ctx, self.root_dir);
    }

    fn committedSnapshotMatches(
        self: *FileSnapshotStore,
        io_ctx: std.Io,
        path: []const u8,
        expected: []const u8,
    ) !bool {
        _ = self;
        var file = try std.Io.Dir.cwd().openFile(io_ctx, path, .{});
        defer file.close(io_ctx);
        if ((try file.stat(io_ctx)).size != expected.len) return false;
        var reader = file.reader(io_ctx, &.{});
        var buffer: [64 * 1024]u8 = undefined;
        var offset: usize = 0;
        while (offset < expected.len) {
            const read = try reader.interface.readSliceShort(&buffer);
            if (read == 0 or !std.mem.eql(u8, buffer[0..read], expected[offset .. offset + read]))
                return false;
            offset += read;
        }
        return (try reader.interface.readSliceShort(buffer[0..1])) == 0;
    }

    fn getSnapshot(ptr: *anyopaque, alloc: std.mem.Allocator, snapshot_id: []const u8) ![]u8 {
        const self: *FileSnapshotStore = @ptrCast(@alignCast(ptr));
        try validateSnapshotId(snapshot_id);
        self.maybeRunArtifactMaintenance();
        const lock = self.uploadLock(snapshot_id);
        platform_sync.lockYielding(lock);
        defer lock.unlock();
        const path = try self.snapshotPath(snapshot_id);
        defer self.alloc.free(path);

        return try std.Io.Dir.cwd().readFileAlloc(io(self), path, alloc, .limited(self.cfg.max_snapshot_bytes));
    }

    fn releaseSnapshot(ptr: *anyopaque, snapshot_id: []const u8) !void {
        const self: *FileSnapshotStore = @ptrCast(@alignCast(ptr));
        try validateSnapshotId(snapshot_id);
        const lock = self.uploadLock(snapshot_id);
        platform_sync.lockYielding(lock);
        defer lock.unlock();
        const path = try self.snapshotPath(snapshot_id);
        defer self.alloc.free(path);
        const deleted = if (std.Io.Dir.cwd().deleteFile(io(self), path))
            true
        else |err| switch (err) {
            error.FileNotFound => false,
            else => return err,
        };
        if (deleted) try fs_paths.syncDirPortable(io(self), self.root_dir);
    }

    fn beginChunkedSnapshot(
        ptr: *anyopaque,
        manifest: snapshot_transfer.Manifest,
        snapshot_id: []const u8,
    ) !void {
        const self: *FileSnapshotStore = @ptrCast(@alignCast(ptr));
        try validateSnapshotId(snapshot_id);
        if (manifest.data_len > self.cfg.max_snapshot_bytes) return error.SnapshotTooLarge;
        const encoded = try snapshot_transfer.encode(self.alloc, manifest);
        defer self.alloc.free(encoded);
        try self.cleanupExpiredArtifacts();
        platform_sync.lockYielding(&self.artifact_lifecycle_mutex);
        defer self.artifact_lifecycle_mutex.unlock();
        const lock = self.uploadLock(snapshot_id);
        platform_sync.lockYielding(lock);
        defer lock.unlock();
        const path = try self.transferPath(snapshot_id, ".v2.part");
        defer self.alloc.free(path);
        const committed_path = try self.transferPath(snapshot_id, ".v2");
        defer self.alloc.free(committed_path);
        try fs_paths.createDirPathPortable(io(self), self.root_dir);
        const total_len = std.math.add(u64, @sizeOf(u32) + encoded.len, manifest.data_len) catch
            return error.SnapshotTooLarge;

        // Snapshot IDs are immutable artifact identities. An exact begin
        // resumes a sparse staging file; replacement requires the owner to
        // conditionally abort/release the old generation first. This prevents
        // a delayed begin from rolling a newer upload back to an older
        // manifest after all later chunk requests have been fenced.
        if (self.readEncodedManifest(self.alloc, path)) |existing| {
            defer self.alloc.free(existing);
            if (std.mem.eql(u8, existing, encoded)) {
                var existing_file = try std.Io.Dir.cwd().openFile(io(self), path, .{});
                defer existing_file.close(io(self));
                // A crash can publish the header before setLength completes.
                // Only a fully shaped sparse artifact is resumable.
                if (try existing_file.length(io(self)) == total_len) return;
            } else return error.SnapshotGenerationMismatch;
        } else |err| switch (err) {
            error.FileNotFound, error.InvalidSnapshotManifest => {},
            else => return err,
        }
        if (self.readEncodedManifest(self.alloc, committed_path)) |existing| {
            defer self.alloc.free(existing);
            if (!std.mem.eql(u8, existing, encoded))
                return error.SnapshotGenerationMismatch;
            // A lost commit response is retried as begin/chunks/commit by the
            // sender. The immutable committed artifact already owns this
            // exact generation, so do not consume a second artifact's quota
            // merely to manufacture an identical staging file.
            return;
        } else |err| switch (err) {
            error.FileNotFound => {},
            // A corrupt committed artifact must not be silently replaced by a
            // request that only proved ownership of a different generation.
            error.InvalidSnapshotManifest => return err,
            else => return err,
        }
        try self.admitArtifact(path, total_len);

        var file = try fs_paths.createFilePortable(io(self), path, .{ .truncate = true });
        defer file.close(io(self));
        var encoded_len: [@sizeOf(u32)]u8 = undefined;
        std.mem.writeInt(u32, &encoded_len, @intCast(encoded.len), .little);
        try file.writePositionalAll(io(self), &encoded_len, 0);
        try file.writePositionalAll(io(self), encoded, encoded_len.len);
        try file.setLength(io(self), total_len);
        try file.sync(io(self));
        try fs_paths.syncDirPortable(io(self), self.root_dir);
    }

    fn putSnapshotChunk(
        ptr: *anyopaque,
        snapshot_id: []const u8,
        generation: snapshot_transfer.Generation,
        offset: u64,
        body: []const u8,
    ) !void {
        const self: *FileSnapshotStore = @ptrCast(@alignCast(ptr));
        try validateSnapshotId(snapshot_id);
        if (body.len == 0 or body.len > self.cfg.max_chunk_bytes) return error.InvalidSnapshotChunkLength;
        const lock = self.uploadLock(snapshot_id);
        platform_sync.lockYielding(lock);
        defer lock.unlock();
        const path = try self.transferPath(snapshot_id, ".v2.part");
        defer self.alloc.free(path);
        var committed = false;
        var file = std.Io.Dir.cwd().openFile(io(self), path, .{ .mode = .read_write }) catch |err| switch (err) {
            error.FileNotFound => blk: {
                const committed_path = try self.transferPath(snapshot_id, ".v2");
                defer self.alloc.free(committed_path);
                committed = true;
                break :blk try std.Io.Dir.cwd().openFile(io(self), committed_path, .{});
            },
            else => return err,
        };
        defer file.close(io(self));
        const header = try self.readArtifactHeader(&file);
        try requireGeneration(generation, header.generation);
        if (offset > header.data_len or body.len > header.data_len - offset)
            return error.InvalidSnapshotChunkRange;
        if (committed) {
            // Committed generations are immutable. An identical chunk is an
            // idempotent retry; a different byte proves the caller is not
            // retrying the generation described by its manifest.
            var compared: usize = 0;
            var buffer: [64 * 1024]u8 = undefined;
            while (compared < body.len) {
                const wanted = @min(buffer.len, body.len - compared);
                const read = try file.readPositionalAll(
                    io(self),
                    buffer[0..wanted],
                    header.data_offset + offset + compared,
                );
                if (read != wanted or !std.mem.eql(u8, buffer[0..read], body[compared..][0..read]))
                    return error.SnapshotGenerationMismatch;
                compared += read;
            }
            return;
        }
        try file.writePositionalAll(io(self), body, header.data_offset + offset);
    }

    fn commitChunkedSnapshot(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        snapshot_id: []const u8,
        generation: snapshot_transfer.Generation,
        materialize: bool,
    ) !?raft_engine.core.types.Snapshot {
        const self: *FileSnapshotStore = @ptrCast(@alignCast(ptr));
        try validateSnapshotId(snapshot_id);
        const lock = self.uploadLock(snapshot_id);
        platform_sync.lockYielding(lock);
        defer lock.unlock();
        const staging_path = try self.transferPath(snapshot_id, ".v2.part");
        defer self.alloc.free(staging_path);
        const committed_path = try self.transferPath(snapshot_id, ".v2");
        defer self.alloc.free(committed_path);
        var publish = true;
        const identified = self.readManifestWithGeneration(alloc, staging_path) catch |err| switch (err) {
            error.FileNotFound => blk: {
                publish = false;
                break :blk try self.readManifestWithGeneration(alloc, committed_path);
            },
            else => return err,
        };
        try requireGeneration(generation, identified.generation);
        var manifest = identified.manifest;
        errdefer manifest.deinit(alloc);
        const artifact_path = if (publish) staging_path else committed_path;
        const data_offset = try self.dataOffset(artifact_path);
        var file = try std.Io.Dir.cwd().openFile(io(self), artifact_path, .{ .mode = .read_write });
        var file_open = true;
        defer if (file_open) file.close(io(self));
        if (try file.length(io(self)) != data_offset + manifest.data_len)
            return error.SnapshotArtifactSizeMismatch;
        var hasher = std.crypto.hash.sha2.Sha256.init(.{});
        var buffer: [256 * 1024]u8 = undefined;
        var read_offset: u64 = 0;
        while (read_offset < manifest.data_len) {
            const wanted: usize = @intCast(@min(buffer.len, manifest.data_len - read_offset));
            const chunk = buffer[0..wanted];
            const read = try file.readPositionalAll(io(self), chunk, data_offset + read_offset);
            if (read != wanted) return error.SnapshotArtifactTruncated;
            hasher.update(chunk[0..read]);
            read_offset += read;
        }
        var actual_digest: [snapshot_transfer.digest_len]u8 = undefined;
        hasher.final(&actual_digest);
        if (!std.mem.eql(u8, &actual_digest, &manifest.digest)) return error.SnapshotChecksumMismatch;
        if (publish) try file.sync(io(self));
        file.close(io(self));
        file_open = false;
        if (publish) {
            // The platform rename is the publication point and replaces an
            // idempotent retry atomically; never unlink the last good artifact
            // before its replacement is durable.
            try std.Io.Dir.rename(std.Io.Dir.cwd(), staging_path, std.Io.Dir.cwd(), committed_path, io(self));
            try fs_paths.syncDirPortable(io(self), self.root_dir);
        }
        if (!materialize) {
            manifest.deinit(alloc);
            return null;
        }
        const snapshot_data_len = manifest.data_len;
        const metadata = manifest.metadata;
        manifest.metadata = .{};
        manifest.deinit(alloc);
        var snapshot: raft_engine.core.types.Snapshot = .{ .metadata = metadata };
        errdefer snapshot.metadata.deinit(alloc);
        const data_len = std.math.cast(usize, snapshot_data_len) orelse
            return error.SnapshotTooLarge;
        if (data_len == 0) return snapshot;

        if (comptime builtin.os.tag != .windows and builtin.os.tag != .wasi and
            builtin.os.tag != .freestanding)
        {
            var mapped_file = try std.Io.Dir.cwd().openFile(io(self), committed_path, .{});
            defer mapped_file.close(io(self));
            const file_len = std.math.cast(usize, try mapped_file.length(io(self))) orelse
                return error.SnapshotTooLarge;
            const mapped = try std.posix.mmap(
                null,
                file_len,
                .{ .READ = true },
                .{ .TYPE = .SHARED },
                mapped_file.handle,
                0,
            );
            errdefer std.posix.munmap(mapped);
            std.posix.madvise(mapped.ptr, mapped.len, std.posix.MADV.SEQUENTIAL) catch {};
            const owner = try alloc.create(MappedSnapshotOwner);
            errdefer alloc.destroy(owner);
            owner.* = .{ .alloc = alloc, .mapped = mapped };
            snapshot.data = mapped[@intCast(data_offset)..][0..data_len];
            try snapshot.shareExternalData(alloc, .{
                .ptr = owner,
                .release = MappedSnapshotOwner.release,
            }, null);
            return snapshot;
        }

        // Platforms without POSIX mappings keep the same ownership contract
        // with a bounded compatibility copy.
        snapshot.data = try alloc.alloc(u8, data_len);
        errdefer alloc.free(snapshot.data);
        var committed = try std.Io.Dir.cwd().openFile(io(self), committed_path, .{});
        defer committed.close(io(self));
        const read = try committed.readPositionalAll(io(self), snapshot.data, data_offset);
        if (read != data_len) return error.SnapshotArtifactTruncated;
        return snapshot;
    }

    fn getSnapshotManifest(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        snapshot_id: []const u8,
    ) !snapshot_transfer.Manifest {
        const self: *FileSnapshotStore = @ptrCast(@alignCast(ptr));
        try validateSnapshotId(snapshot_id);
        self.maybeRunArtifactMaintenance();
        const lock = self.uploadLock(snapshot_id);
        platform_sync.lockYielding(lock);
        defer lock.unlock();
        const committed_path = try self.transferPath(snapshot_id, ".v2");
        defer self.alloc.free(committed_path);
        const identified = try self.readManifestWithGeneration(alloc, committed_path);
        var manifest = identified.manifest;
        errdefer manifest.deinit(alloc);
        try self.renewFetchLease(snapshot_id, identified.generation);
        return manifest;
    }

    fn getSnapshotUploadManifest(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        snapshot_id: []const u8,
    ) !snapshot_transfer.Manifest {
        const self: *FileSnapshotStore = @ptrCast(@alignCast(ptr));
        try validateSnapshotId(snapshot_id);
        self.maybeRunArtifactMaintenance();
        const lock = self.uploadLock(snapshot_id);
        platform_sync.lockYielding(lock);
        defer lock.unlock();
        const staging_path = try self.transferPath(snapshot_id, ".v2.part");
        defer self.alloc.free(staging_path);
        return self.readManifest(alloc, staging_path) catch |err| switch (err) {
            error.FileNotFound => blk: {
                const committed_path = try self.transferPath(snapshot_id, ".v2");
                defer self.alloc.free(committed_path);
                break :blk try self.readManifest(alloc, committed_path);
            },
            else => return err,
        };
    }

    fn getSnapshotChunk(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        snapshot_id: []const u8,
        generation: snapshot_transfer.Generation,
        offset: u64,
        max_len: usize,
    ) ![]u8 {
        const self: *FileSnapshotStore = @ptrCast(@alignCast(ptr));
        try validateSnapshotId(snapshot_id);
        self.maybeRunArtifactMaintenance();
        if (max_len == 0 or max_len > self.cfg.max_chunk_bytes) return error.InvalidSnapshotChunkLength;
        const lock = self.uploadLock(snapshot_id);
        platform_sync.lockYielding(lock);
        defer lock.unlock();
        const path = try self.transferPath(snapshot_id, ".v2");
        defer self.alloc.free(path);
        var file = try std.Io.Dir.cwd().openFile(io(self), path, .{});
        defer file.close(io(self));
        const header = try self.readArtifactHeader(&file);
        try requireGeneration(generation, header.generation);
        try self.renewFetchLease(snapshot_id, generation);
        if (offset >= header.data_len) return try alloc.dupe(u8, &.{});
        const len: usize = @intCast(@min(max_len, header.data_len - offset));
        const chunk = try alloc.alloc(u8, len);
        errdefer alloc.free(chunk);
        const read = try file.readPositionalAll(io(self), chunk, header.data_offset + offset);
        if (read != len) return error.SnapshotArtifactTruncated;
        return chunk;
    }

    fn abortChunkedSnapshot(
        ptr: *anyopaque,
        snapshot_id: []const u8,
        generation: snapshot_transfer.Generation,
    ) !void {
        const self: *FileSnapshotStore = @ptrCast(@alignCast(ptr));
        try validateSnapshotId(snapshot_id);
        const lock = self.uploadLock(snapshot_id);
        platform_sync.lockYielding(lock);
        defer lock.unlock();
        const path = try self.transferPath(snapshot_id, ".v2.part");
        defer self.alloc.free(path);
        const current_generation = self.readGeneration(self.alloc, path) catch |err| switch (err) {
            // Abort owns staging only. A retry after commit (or after a
            // successful earlier abort) must not inspect or remove the
            // committed artifact with the same snapshot id.
            error.FileNotFound => return,
            else => return err,
        };
        try requireGeneration(generation, current_generation);
        const deleted = if (std.Io.Dir.cwd().deleteFile(io(self), path))
            true
        else |err| switch (err) {
            error.FileNotFound => false,
            else => return err,
        };
        if (deleted) try fs_paths.syncDirPortable(io(self), self.root_dir);
    }

    fn releaseChunkedSnapshot(
        ptr: *anyopaque,
        snapshot_id: []const u8,
        generation: snapshot_transfer.Generation,
    ) !void {
        const self: *FileSnapshotStore = @ptrCast(@alignCast(ptr));
        try validateSnapshotId(snapshot_id);
        const lock = self.uploadLock(snapshot_id);
        platform_sync.lockYielding(lock);
        defer lock.unlock();
        const path = try self.transferPath(snapshot_id, ".v2");
        defer self.alloc.free(path);
        const current_generation = try self.readGeneration(self.alloc, path);
        try requireGeneration(generation, current_generation);
        self.clearFetchLease(snapshot_id, generation);
        const deleted = if (std.Io.Dir.cwd().deleteFile(io(self), path))
            true
        else |err| switch (err) {
            error.FileNotFound => false,
            else => return err,
        };
        if (deleted) try fs_paths.syncDirPortable(io(self), self.root_dir);
    }

    const ArtifactUsage = struct {
        bytes: u64 = 0,
        count: usize = 0,
    };

    fn admitArtifact(self: *FileSnapshotStore, staging_path: []const u8, requested_bytes: u64) !void {
        var usage = try self.artifactUsage();
        const existing = std.Io.Dir.cwd().statFile(io(self), staging_path, .{}) catch |err| switch (err) {
            error.FileNotFound => null,
            else => return err,
        };
        if (existing) |stat| {
            usage.bytes -|= stat.size;
        } else {
            usage.count += 1;
        }
        if (usage.count > self.cfg.artifact_policy.max_count or
            usage.bytes > self.cfg.artifact_policy.max_bytes or
            requested_bytes > self.cfg.artifact_policy.max_bytes - usage.bytes)
        {
            return error.SnapshotArtifactQuotaExceeded;
        }
    }

    fn artifactUsage(self: *FileSnapshotStore) !ArtifactUsage {
        var dir = std.Io.Dir.cwd().openDir(io(self), self.root_dir, .{ .iterate = true }) catch |err| switch (err) {
            error.FileNotFound => return .{},
            else => return err,
        };
        defer dir.close(io(self));
        var usage: ArtifactUsage = .{};
        var iter = dir.iterateAssumeFirstIteration();
        while (try iter.next(io(self))) |entry| {
            if (entry.kind != .file or managedArtifact(entry.name) == null) continue;
            const stat = dir.statFile(io(self), entry.name, .{}) catch |err| switch (err) {
                error.FileNotFound => continue,
                else => return err,
            };
            usage.bytes +|= stat.size;
            usage.count +|= 1;
        }
        return usage;
    }

    fn cleanupExpiredArtifacts(self: *FileSnapshotStore) !void {
        platform_sync.lockYielding(&self.artifact_lifecycle_mutex);
        defer self.artifact_lifecycle_mutex.unlock();
        var dir = std.Io.Dir.cwd().openDir(io(self), self.root_dir, .{ .iterate = true }) catch |err| switch (err) {
            error.FileNotFound => return,
            else => return err,
        };
        defer dir.close(io(self));
        const realtime_now_ns = std.Io.Timestamp.now(io(self), .real).toNanoseconds();
        const monotonic_now_ns = platform_time.monotonicNs();
        var deleted = false;
        var iter = dir.iterateAssumeFirstIteration();
        while (try iter.next(io(self))) |entry| {
            if (entry.kind != .file) continue;
            const artifact = managedArtifact(entry.name) orelse continue;
            const stat = dir.statFile(io(self), entry.name, .{}) catch |err| switch (err) {
                error.FileNotFound => continue,
                else => return err,
            };
            const ttl_ns = if (artifact.staging)
                self.cfg.artifact_policy.staging_ttl_ns
            else
                self.cfg.artifact_policy.committed_ttl_ns;
            if (ttl_ns <= 0 or realtime_now_ns -| stat.mtime.toNanoseconds() < ttl_ns) continue;
            const lock = self.uploadLock(artifact.snapshot_id);
            if (!lock.tryLock()) continue;
            const removed = removed: {
                defer lock.unlock();
                if (!artifact.staging) {
                    const artifact_path = try self.transferPath(artifact.snapshot_id, ".v2");
                    defer self.alloc.free(artifact_path);
                    const generation = self.readGeneration(self.alloc, artifact_path) catch |err| switch (err) {
                        error.FileNotFound, error.InvalidSnapshotManifest => null,
                        else => return err,
                    };
                    if (generation) |value| {
                        if (self.fetchLeaseActive(artifact.snapshot_id, value, monotonic_now_ns))
                            break :removed false;
                    }
                }
                const current = dir.statFile(io(self), entry.name, .{}) catch |err| switch (err) {
                    error.FileNotFound => break :removed false,
                    else => return err,
                };
                if (realtime_now_ns -| current.mtime.toNanoseconds() < ttl_ns) break :removed false;
                dir.deleteFile(io(self), entry.name) catch |err| switch (err) {
                    error.FileNotFound => break :removed false,
                    else => return err,
                };
                break :removed true;
            };
            deleted = deleted or removed;
        }
        if (deleted) try fs_paths.syncDirPortable(io(self), self.root_dir);
    }

    fn renewFetchLease(
        self: *FileSnapshotStore,
        snapshot_id: []const u8,
        generation: snapshot_transfer.Generation,
    ) !void {
        const lease_ns = std.math.cast(u64, self.cfg.artifact_policy.active_fetch_lease_ns) orelse
            std.math.maxInt(u64);
        const deadline = platform_time.monotonicNs() +| lease_ns;
        platform_sync.lockYielding(&self.fetch_lease_mutex);
        defer self.fetch_lease_mutex.unlock();
        if (self.fetch_leases.getPtr(snapshot_id)) |existing| {
            existing.* = .{ .generation = generation, .deadline_ns = deadline };
            return;
        }
        const owned_id = try self.alloc.dupe(u8, snapshot_id);
        errdefer self.alloc.free(owned_id);
        try self.fetch_leases.putNoClobber(self.alloc, owned_id, .{
            .generation = generation,
            .deadline_ns = deadline,
        });
    }

    fn clearFetchLease(
        self: *FileSnapshotStore,
        snapshot_id: []const u8,
        generation: snapshot_transfer.Generation,
    ) void {
        platform_sync.lockYielding(&self.fetch_lease_mutex);
        defer self.fetch_lease_mutex.unlock();
        const lease = self.fetch_leases.get(snapshot_id) orelse return;
        if (!std.mem.eql(u8, &lease.generation, &generation)) return;
        if (self.fetch_leases.fetchRemove(snapshot_id)) |removed| self.alloc.free(removed.key);
    }

    fn fetchLeaseActive(
        self: *FileSnapshotStore,
        snapshot_id: []const u8,
        generation: snapshot_transfer.Generation,
        now_ns: u64,
    ) bool {
        platform_sync.lockYielding(&self.fetch_lease_mutex);
        defer self.fetch_lease_mutex.unlock();
        const lease = self.fetch_leases.get(snapshot_id) orelse return false;
        if (!std.mem.eql(u8, &lease.generation, &generation)) return false;
        if (lease.deadline_ns > now_ns) return true;
        if (self.fetch_leases.fetchRemove(snapshot_id)) |removed| self.alloc.free(removed.key);
        return false;
    }

    fn deferArtifactMaintenance(self: *FileSnapshotStore) void {
        const now_ns = platform_time.monotonicNs();
        self.next_artifact_maintenance_ns.store(now_ns +| artifact_maintenance_interval_ns, .release);
    }

    /// Rate-limited opportunistic sweeping avoids a store-owned maintenance
    /// thread while guaranteeing the first transfer after an idle interval
    /// reclaims expired artifacts. Only one request performs the directory
    /// scan; chunk traffic remains on its O(1) striped-lock path.
    fn maybeRunArtifactMaintenance(self: *FileSnapshotStore) void {
        const now_ns = platform_time.monotonicNs();
        const previous = self.next_artifact_maintenance_ns.load(.acquire);
        if (previous != 0 and now_ns < previous) return;
        if (self.next_artifact_maintenance_ns.cmpxchgStrong(
            previous,
            now_ns +| artifact_maintenance_interval_ns,
            .acq_rel,
            .acquire,
        ) != null) return;
        self.cleanupExpiredArtifacts() catch |err| {
            std.log.warn("raft snapshot artifact maintenance deferred root={s} err={s}", .{
                self.root_dir,
                @errorName(err),
            });
        };
    }

    const ManagedArtifact = struct { snapshot_id: []const u8, staging: bool };

    fn managedArtifact(name: []const u8) ?ManagedArtifact {
        if (std.mem.endsWith(u8, name, ".v2.part")) {
            const id = name[0 .. name.len - ".v2.part".len];
            if (validateSnapshotId(id)) |_| return .{ .snapshot_id = id, .staging = true } else |_| return null;
        }
        if (std.mem.endsWith(u8, name, ".v2")) {
            const id = name[0 .. name.len - ".v2".len];
            if (validateSnapshotId(id)) |_| return .{ .snapshot_id = id, .staging = false } else |_| return null;
        }
        if (std.mem.endsWith(u8, name, ".snap.part")) {
            const id = name[0 .. name.len - ".snap.part".len];
            if (validateSnapshotId(id)) |_| return .{ .snapshot_id = id, .staging = true } else |_| return null;
        }
        if (std.mem.endsWith(u8, name, ".snap")) {
            const id = name[0 .. name.len - ".snap".len];
            if (validateSnapshotId(id)) |_| return .{ .snapshot_id = id, .staging = false } else |_| return null;
        }
        return null;
    }

    fn readManifest(self: *FileSnapshotStore, alloc: std.mem.Allocator, path: []const u8) !snapshot_transfer.Manifest {
        const encoded = try self.readEncodedManifest(alloc, path);
        defer alloc.free(encoded);
        return try snapshot_transfer.decode(alloc, encoded);
    }

    const ManifestWithGeneration = struct {
        manifest: snapshot_transfer.Manifest,
        generation: snapshot_transfer.Generation,
    };

    fn readManifestWithGeneration(
        self: *FileSnapshotStore,
        alloc: std.mem.Allocator,
        path: []const u8,
    ) !ManifestWithGeneration {
        const encoded = try self.readEncodedManifest(alloc, path);
        defer alloc.free(encoded);
        const generation = try snapshot_transfer.generationFromEncodedManifest(encoded);
        return .{
            .manifest = try snapshot_transfer.decode(alloc, encoded),
            .generation = generation,
        };
    }

    fn readGeneration(
        self: *FileSnapshotStore,
        alloc: std.mem.Allocator,
        path: []const u8,
    ) !snapshot_transfer.Generation {
        const encoded = try self.readEncodedManifest(alloc, path);
        defer alloc.free(encoded);
        // Decode before trusting the checksum bytes as an artifact identity.
        var manifest = try snapshot_transfer.decode(alloc, encoded);
        defer manifest.deinit(alloc);
        return try snapshot_transfer.generationFromEncodedManifest(encoded);
    }

    /// Read the immutable artifact framing and generation with one file open
    /// and a fixed amount of I/O. Chunk requests do not need to allocate or
    /// decode the full manifest; begin/commit remain the validation points for
    /// its checksum and semantic fields.
    fn readArtifactHeader(self: *FileSnapshotStore, file: *std.Io.File) !ArtifactHeader {
        var encoded_len: [@sizeOf(u32)]u8 = undefined;
        if (try file.readPositionalAll(io(self), &encoded_len, 0) != encoded_len.len)
            return error.InvalidSnapshotManifest;
        const manifest_len: u64 = std.mem.readInt(u32, &encoded_len, .little);
        if (manifest_len < snapshot_transfer.digest_len or
            manifest_len > snapshot_transfer.max_manifest_bytes)
            return error.InvalidSnapshotManifest;
        const data_offset = std.math.add(u64, encoded_len.len, manifest_len) catch
            return error.InvalidSnapshotManifest;
        const file_len = try file.length(io(self));
        if (file_len < data_offset) return error.InvalidSnapshotManifest;
        var generation: snapshot_transfer.Generation = undefined;
        const generation_offset = data_offset - snapshot_transfer.digest_len;
        if (try file.readPositionalAll(io(self), &generation, generation_offset) != generation.len)
            return error.InvalidSnapshotManifest;
        return .{
            .generation = generation,
            .data_offset = data_offset,
            .data_len = file_len - data_offset,
        };
    }

    fn requireGeneration(
        expected: snapshot_transfer.Generation,
        actual: snapshot_transfer.Generation,
    ) !void {
        if (!std.mem.eql(u8, &expected, &actual))
            return error.SnapshotGenerationMismatch;
    }

    fn readEncodedManifest(self: *FileSnapshotStore, alloc: std.mem.Allocator, path: []const u8) ![]u8 {
        var file = try std.Io.Dir.cwd().openFile(io(self), path, .{});
        defer file.close(io(self));
        var encoded_len: [@sizeOf(u32)]u8 = undefined;
        if (try file.readPositionalAll(io(self), &encoded_len, 0) != encoded_len.len)
            return error.InvalidSnapshotManifest;
        const len: usize = std.mem.readInt(u32, &encoded_len, .little);
        if (len == 0 or len > snapshot_transfer.max_manifest_bytes) return error.InvalidSnapshotManifest;
        const encoded = try alloc.alloc(u8, len);
        errdefer alloc.free(encoded);
        if (try file.readPositionalAll(io(self), encoded, encoded_len.len) != len)
            return error.InvalidSnapshotManifest;
        return encoded;
    }

    fn dataOffset(self: *FileSnapshotStore, path: []const u8) !u64 {
        const encoded = try self.readEncodedManifest(self.alloc, path);
        defer self.alloc.free(encoded);
        return @sizeOf(u32) + encoded.len;
    }

    fn transferPath(self: *const FileSnapshotStore, snapshot_id: []const u8, suffix: []const u8) ![]u8 {
        return try std.fmt.allocPrint(self.alloc, "{s}/{s}{s}", .{ self.root_dir, snapshot_id, suffix });
    }

    fn uploadLock(self: *FileSnapshotStore, snapshot_id: []const u8) *std.atomic.Mutex {
        return &self.upload_locks[std.hash.Wyhash.hash(0, snapshot_id) % self.upload_locks.len];
    }

    fn io(self: *FileSnapshotStore) std.Io {
        return self.io_impl.io();
    }

    fn snapshotPath(self: *const FileSnapshotStore, snapshot_id: []const u8) ![]u8 {
        return try std.fmt.allocPrint(self.alloc, "{s}/{s}.snap", .{ self.root_dir, snapshot_id });
    }

    fn validateSnapshotId(snapshot_id: []const u8) !void {
        // Leave room for transfer suffixes under the portable 255-byte path
        // component ceiling and bound attacker-controlled path allocations.
        if (snapshot_id.len == 0 or snapshot_id.len > 192) return error.InvalidSnapshotId;
        if (std.mem.indexOf(u8, snapshot_id, "..") != null) return error.InvalidSnapshotId;
        for (snapshot_id) |c| {
            switch (c) {
                '/', '\\', 0 => return error.InvalidSnapshotId,
                else => {},
            }
        }
    }
};

fn testManifestGeneration(manifest: snapshot_transfer.Manifest) !snapshot_transfer.Generation {
    const encoded = try snapshot_transfer.encode(std.testing.allocator, manifest);
    defer std.testing.allocator.free(encoded);
    return try snapshot_transfer.generationFromEncodedManifest(encoded);
}

test "file snapshot store persists snapshot bodies" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root_dir = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/raft-snaps", .{tmp.sub_path});
    defer std.testing.allocator.free(root_dir);

    var store = try FileSnapshotStore.init(std.testing.allocator, .{ .root_dir = root_dir });
    defer store.deinit();

    try store.store().putSnapshot(std.testing.allocator, "snap-1", "snapshot-body");
    const body = try store.store().getSnapshot(std.testing.allocator, "snap-1");
    defer std.testing.allocator.free(body);
    try std.testing.expectEqualStrings("snapshot-body", body);
    try std.testing.expectEqual(@as(usize, 1), (try store.artifactUsage()).count);
    const iface = store.store();
    try iface.vtable.release_snapshot.?(iface.ptr, "snap-1");
    try std.testing.expectEqual(@as(usize, 0), (try store.artifactUsage()).count);
    try std.testing.expectError(
        error.FileNotFound,
        iface.getSnapshot(std.testing.allocator, "snap-1"),
    );
}

test "file snapshot store rejects invalid snapshot ids" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root_dir = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/raft-snaps", .{tmp.sub_path});
    defer std.testing.allocator.free(root_dir);

    var store = try FileSnapshotStore.init(std.testing.allocator, .{ .root_dir = root_dir });
    defer store.deinit();

    try std.testing.expectError(error.InvalidSnapshotId, store.store().putSnapshot(std.testing.allocator, "../bad", "x"));
}

test "file snapshot store rejects oversized uploads before persistence" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root_dir = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/raft-snaps", .{tmp.sub_path});
    defer std.testing.allocator.free(root_dir);

    var store = try FileSnapshotStore.init(std.testing.allocator, .{
        .root_dir = root_dir,
        .max_snapshot_bytes = 4,
    });
    defer store.deinit();

    try std.testing.expectError(
        error.SnapshotTooLarge,
        store.store().putSnapshot(std.testing.allocator, "snap-large", "12345"),
    );
}

test "legacy snapshot artifacts share quota and expiry lifecycle" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root_dir = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/raft-snaps-legacy-policy", .{tmp.sub_path});
    defer std.testing.allocator.free(root_dir);
    var store = try FileSnapshotStore.init(std.testing.allocator, .{
        .root_dir = root_dir,
        .artifact_policy = .{ .max_bytes = 4, .committed_ttl_ns = std.time.ns_per_ms },
    });
    defer store.deinit();
    const iface = store.store();
    try std.testing.expectError(
        error.SnapshotArtifactQuotaExceeded,
        iface.putSnapshot(std.testing.allocator, "over-quota", "12345"),
    );
    try iface.putSnapshot(std.testing.allocator, "expires", "1234");
    try store.io().sleep(.fromMilliseconds(2), .awake);
    store.next_artifact_maintenance_ns.store(0, .release);
    try std.testing.expectError(
        error.FileNotFound,
        iface.getSnapshot(std.testing.allocator, "expires"),
    );
    try std.testing.expectEqual(@as(usize, 0), (try store.artifactUsage()).count);
}

test "file snapshot store resumes chunks and atomically verifies commit" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root_dir = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/raft-snaps-v2", .{tmp.sub_path});
    defer std.testing.allocator.free(root_dir);
    var store = try FileSnapshotStore.init(std.testing.allocator, .{
        .root_dir = root_dir,
        .max_chunk_bytes = snapshot_transfer.min_chunk_bytes,
    });
    defer store.deinit();
    var voters = [_]u64{ 1, 2 };
    const body = "snapshot-data";
    const manifest: snapshot_transfer.Manifest = .{
        .group_id = 91,
        .from = 1,
        .to = 2,
        .request_term = 4,
        .metadata = .{ .index = 12, .term = 3, .conf_state = .{ .voters = &voters } },
        .data_len = body.len,
        .digest = snapshot_transfer.digest(body),
    };
    const iface = store.store();
    const generation = try testManifestGeneration(manifest);
    try iface.vtable.begin_chunked_snapshot.?(iface.ptr, manifest, "snap-v2");
    try iface.vtable.put_snapshot_chunk.?(iface.ptr, "snap-v2", generation, 0, body[0..4]);
    // Idempotent begin retains already-published chunks for retry/resume.
    try iface.vtable.begin_chunked_snapshot.?(iface.ptr, manifest, "snap-v2");
    try iface.vtable.put_snapshot_chunk.?(iface.ptr, "snap-v2", generation, 4, body[4..8]);
    try iface.vtable.put_snapshot_chunk.?(iface.ptr, "snap-v2", generation, 8, body[8..12]);
    try iface.vtable.put_snapshot_chunk.?(iface.ptr, "snap-v2", generation, 12, body[12..]);
    var snapshot = (try iface.vtable.commit_chunked_snapshot.?(
        iface.ptr,
        std.testing.allocator,
        "snap-v2",
        generation,
        true,
    )).?;
    defer snapshot.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings(body, snapshot.data);
    try std.testing.expectEqual(@as(u64, 12), snapshot.metadata.index);

    // Artifact-only publication must not allocate a full Snapshot merely to
    // make the committed chunks available to a later fetch.
    var replacement = manifest;
    replacement.metadata.index = 13;
    const replacement_generation = try testManifestGeneration(replacement);
    try std.testing.expectError(
        error.SnapshotGenerationMismatch,
        iface.vtable.begin_chunked_snapshot.?(iface.ptr, replacement, "snap-v2"),
    );
    try iface.vtable.release_chunked_snapshot.?(iface.ptr, "snap-v2", generation);
    try iface.vtable.begin_chunked_snapshot.?(iface.ptr, replacement, "snap-v2");
    var upload_manifest = try iface.vtable.get_snapshot_upload_manifest.?(
        iface.ptr,
        std.testing.allocator,
        "snap-v2",
    );
    defer upload_manifest.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u64, 13), upload_manifest.metadata.index);
    var offset: usize = 0;
    while (offset < body.len) {
        const end = @min(body.len, offset + 4);
        try iface.vtable.put_snapshot_chunk.?(iface.ptr, "snap-v2", replacement_generation, offset, body[offset..end]);
        offset = end;
    }
    try std.testing.expect((try iface.vtable.commit_chunked_snapshot.?(
        iface.ptr,
        std.testing.allocator,
        "snap-v2",
        replacement_generation,
        false,
    )) == null);
    // Abort is scoped to the unpublished staging generation. Retrying it
    // after commit is a no-op and must leave the committed artifact intact.
    try iface.vtable.abort_chunked_snapshot.?(iface.ptr, "snap-v2", replacement_generation);
    try iface.vtable.abort_chunked_snapshot.?(iface.ptr, "snap-v2", replacement_generation);
    const tail = try iface.vtable.get_snapshot_chunk.?(iface.ptr, std.testing.allocator, "snap-v2", replacement_generation, 5, 4);
    defer std.testing.allocator.free(tail);
    try std.testing.expectEqualStrings(body[5..9], tail);
    var retried = (try iface.vtable.commit_chunked_snapshot.?(
        iface.ptr,
        std.testing.allocator,
        "snap-v2",
        replacement_generation,
        true,
    )).?;
    defer retried.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u64, 13), retried.metadata.index);
    try iface.vtable.release_chunked_snapshot.?(iface.ptr, "snap-v2", replacement_generation);
    try std.testing.expectError(
        error.FileNotFound,
        iface.vtable.get_snapshot_manifest.?(iface.ptr, std.testing.allocator, "snap-v2"),
    );
}

test "legacy snapshot retry is idempotent under a full artifact quota" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root_dir = try std.fmt.allocPrint(
        std.testing.allocator,
        ".zig-cache/tmp/{s}/raft-snaps-legacy-retry",
        .{tmp.sub_path},
    );
    defer std.testing.allocator.free(root_dir);
    const body = "immutable-legacy-snapshot";
    var store = try FileSnapshotStore.init(std.testing.allocator, .{
        .root_dir = root_dir,
        .artifact_policy = .{ .max_bytes = body.len, .max_count = 1 },
    });
    defer store.deinit();
    const iface = store.store();

    try iface.putSnapshot(std.testing.allocator, "legacy-retry", body);
    try iface.putSnapshot(std.testing.allocator, "legacy-retry", body);
    try std.testing.expectError(
        error.SnapshotGenerationMismatch,
        iface.putSnapshot(std.testing.allocator, "legacy-retry", "different-legacy-bytes"),
    );

    const persisted = try iface.getSnapshot(std.testing.allocator, "legacy-retry");
    defer std.testing.allocator.free(persisted);
    try std.testing.expectEqualStrings(body, persisted);
    const usage = try store.artifactUsage();
    try std.testing.expectEqual(@as(usize, 1), usage.count);
    try std.testing.expectEqual(@as(u64, body.len), usage.bytes);
}

test "committed chunked snapshot protocol retry reuses one artifact quota" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root_dir = try std.fmt.allocPrint(
        std.testing.allocator,
        ".zig-cache/tmp/{s}/raft-snaps-committed-retry",
        .{tmp.sub_path},
    );
    defer std.testing.allocator.free(root_dir);
    const body = "immutable-snapshot-data";
    const manifest: snapshot_transfer.Manifest = .{
        .group_id = 92,
        .from = 1,
        .to = 2,
        .request_term = 4,
        .metadata = .{ .index = 18, .term = 5 },
        .data_len = body.len,
        .digest = snapshot_transfer.digest(body),
    };
    const encoded = try snapshot_transfer.encode(std.testing.allocator, manifest);
    defer std.testing.allocator.free(encoded);
    const artifact_bytes = @sizeOf(u32) + encoded.len + body.len;
    var store = try FileSnapshotStore.init(std.testing.allocator, .{
        .root_dir = root_dir,
        .artifact_policy = .{ .max_bytes = artifact_bytes, .max_count = 1 },
    });
    defer store.deinit();
    const iface = store.store();
    const generation = try testManifestGeneration(manifest);

    try iface.vtable.begin_chunked_snapshot.?(iface.ptr, manifest, "retry");
    try iface.vtable.put_snapshot_chunk.?(iface.ptr, "retry", generation, 0, body);
    try std.testing.expect((try iface.vtable.commit_chunked_snapshot.?(
        iface.ptr,
        std.testing.allocator,
        "retry",
        generation,
        false,
    )) == null);
    try std.testing.expectEqual(@as(usize, 1), (try store.artifactUsage()).count);

    // Simulate loss of the commit response: the sender repeats the complete
    // protocol, including chunks, under a quota that cannot hold staging and
    // committed copies simultaneously.
    try iface.vtable.begin_chunked_snapshot.?(iface.ptr, manifest, "retry");
    try iface.vtable.put_snapshot_chunk.?(iface.ptr, "retry", generation, 0, body);
    try std.testing.expectError(
        error.SnapshotGenerationMismatch,
        iface.vtable.put_snapshot_chunk.?(iface.ptr, "retry", generation, 0, "different"),
    );
    try std.testing.expect((try iface.vtable.commit_chunked_snapshot.?(
        iface.ptr,
        std.testing.allocator,
        "retry",
        generation,
        false,
    )) == null);
    const usage = try store.artifactUsage();
    try std.testing.expectEqual(@as(usize, 1), usage.count);
    try std.testing.expectEqual(@as(u64, artifact_bytes), usage.bytes);
}

test "chunked snapshot generations fence stale upload and release requests" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root_dir = try std.fmt.allocPrint(
        std.testing.allocator,
        ".zig-cache/tmp/{s}/raft-snaps-generation-fence",
        .{tmp.sub_path},
    );
    defer std.testing.allocator.free(root_dir);
    var store = try FileSnapshotStore.init(std.testing.allocator, .{ .root_dir = root_dir });
    defer store.deinit();

    const body = "current-generation";
    const first: snapshot_transfer.Manifest = .{
        .group_id = 94,
        .from = 0,
        .to = 2,
        .request_term = 0,
        .metadata = .{ .index = 14, .term = 4 },
        .data_len = body.len,
        .digest = snapshot_transfer.digest(body),
    };
    var replacement = first;
    replacement.metadata.index = 15;
    const first_generation = try testManifestGeneration(first);
    const replacement_generation = try testManifestGeneration(replacement);
    const iface = store.store();

    try iface.vtable.begin_chunked_snapshot.?(iface.ptr, first, "reused-id");
    try std.testing.expectError(
        error.SnapshotGenerationMismatch,
        iface.vtable.begin_chunked_snapshot.?(iface.ptr, replacement, "reused-id"),
    );
    try iface.vtable.abort_chunked_snapshot.?(
        iface.ptr,
        "reused-id",
        first_generation,
    );
    try iface.vtable.begin_chunked_snapshot.?(iface.ptr, replacement, "reused-id");
    try std.testing.expectError(
        error.SnapshotGenerationMismatch,
        iface.vtable.put_snapshot_chunk.?(
            iface.ptr,
            "reused-id",
            first_generation,
            0,
            body,
        ),
    );
    try std.testing.expectError(
        error.SnapshotGenerationMismatch,
        iface.vtable.abort_chunked_snapshot.?(
            iface.ptr,
            "reused-id",
            first_generation,
        ),
    );
    try std.testing.expectError(
        error.SnapshotGenerationMismatch,
        iface.vtable.commit_chunked_snapshot.?(
            iface.ptr,
            std.testing.allocator,
            "reused-id",
            first_generation,
            false,
        ),
    );

    try iface.vtable.put_snapshot_chunk.?(
        iface.ptr,
        "reused-id",
        replacement_generation,
        0,
        body,
    );
    try std.testing.expect((try iface.vtable.commit_chunked_snapshot.?(
        iface.ptr,
        std.testing.allocator,
        "reused-id",
        replacement_generation,
        false,
    )) == null);
    try std.testing.expectError(
        error.SnapshotGenerationMismatch,
        iface.vtable.release_chunked_snapshot.?(
            iface.ptr,
            "reused-id",
            first_generation,
        ),
    );
    const fetched = try iface.vtable.get_snapshot_chunk.?(
        iface.ptr,
        std.testing.allocator,
        "reused-id",
        replacement_generation,
        0,
        body.len,
    );
    defer std.testing.allocator.free(fetched);
    try std.testing.expectEqualStrings(body, fetched);
}

test "active chunked fetch lease fences committed artifact expiry" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root_dir = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/raft-snaps-fetch-lease", .{tmp.sub_path});
    defer std.testing.allocator.free(root_dir);
    var store = try FileSnapshotStore.init(std.testing.allocator, .{
        .root_dir = root_dir,
        .artifact_policy = .{
            .committed_ttl_ns = std.time.ns_per_ms,
            .active_fetch_lease_ns = 20 * std.time.ns_per_ms,
        },
    });
    defer store.deinit();
    const body = "lease-protected";
    const manifest: snapshot_transfer.Manifest = .{
        .group_id = 93,
        .from = 0,
        .to = 2,
        .request_term = 0,
        .metadata = .{ .index = 14, .term = 4 },
        .data_len = body.len,
        .digest = snapshot_transfer.digest(body),
    };
    const iface = store.store();
    const generation = try testManifestGeneration(manifest);
    try iface.vtable.begin_chunked_snapshot.?(iface.ptr, manifest, "leased");
    try iface.vtable.put_snapshot_chunk.?(iface.ptr, "leased", generation, 0, body);
    try std.testing.expect((try iface.vtable.commit_chunked_snapshot.?(
        iface.ptr,
        std.testing.allocator,
        "leased",
        generation,
        false,
    )) == null);

    var fetched_manifest = try iface.vtable.get_snapshot_manifest.?(
        iface.ptr,
        std.testing.allocator,
        "leased",
    );
    fetched_manifest.deinit(std.testing.allocator);
    try store.io().sleep(.fromMilliseconds(2), .awake);
    store.next_artifact_maintenance_ns.store(0, .release);
    const chunk = try iface.vtable.get_snapshot_chunk.?(
        iface.ptr,
        std.testing.allocator,
        "leased",
        generation,
        0,
        body.len,
    );
    defer std.testing.allocator.free(chunk);
    try std.testing.expectEqualStrings(body, chunk);

    // An abandoned fetch eventually becomes reclaimable again.
    try store.io().sleep(.fromMilliseconds(25), .awake);
    store.next_artifact_maintenance_ns.store(0, .release);
    try std.testing.expectError(
        error.FileNotFound,
        iface.vtable.get_snapshot_manifest.?(iface.ptr, std.testing.allocator, "leased"),
    );
}

test "file snapshot store enforces logical artifact quota before sparse allocation" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root_dir = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/raft-snaps-quota", .{tmp.sub_path});
    defer std.testing.allocator.free(root_dir);
    var store = try FileSnapshotStore.init(std.testing.allocator, .{
        .root_dir = root_dir,
        .max_snapshot_bytes = 1024,
        .artifact_policy = .{ .max_bytes = 32 },
    });
    defer store.deinit();
    const body = "snapshot-data";
    const manifest: snapshot_transfer.Manifest = .{
        .group_id = 91,
        .from = 1,
        .to = 2,
        .request_term = 4,
        .metadata = .{ .index = 12, .term = 3 },
        .data_len = body.len,
        .digest = snapshot_transfer.digest(body),
    };
    const iface = store.store();
    try std.testing.expectError(
        error.SnapshotArtifactQuotaExceeded,
        iface.vtable.begin_chunked_snapshot.?(iface.ptr, manifest, "over-quota"),
    );
    try std.testing.expectEqual(@as(usize, 0), (try store.artifactUsage()).count);
}

test "file snapshot store expires abandoned staging artifacts" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root_dir = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/raft-snaps-expiry", .{tmp.sub_path});
    defer std.testing.allocator.free(root_dir);
    var store = try FileSnapshotStore.init(std.testing.allocator, .{ .root_dir = root_dir });
    defer store.deinit();
    const body = "snapshot-data";
    const manifest: snapshot_transfer.Manifest = .{
        .group_id = 91,
        .from = 1,
        .to = 2,
        .request_term = 4,
        .metadata = .{ .index = 12, .term = 3 },
        .data_len = body.len,
        .digest = snapshot_transfer.digest(body),
    };
    const iface = store.store();
    try iface.vtable.begin_chunked_snapshot.?(iface.ptr, manifest, "abandoned");
    store.cfg.artifact_policy.staging_ttl_ns = std.time.ns_per_ms;
    try store.io().sleep(.fromMilliseconds(2), .awake);
    try store.cleanupExpiredArtifacts();
    const path = try store.transferPath("abandoned", ".v2.part");
    defer std.testing.allocator.free(path);
    try std.testing.expectError(
        error.FileNotFound,
        std.Io.Dir.cwd().statFile(store.io(), path, .{}),
    );
}
