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
const platform_sync = @import("antfly_platform").sync;
const fs_paths = @import("../../common/fs_paths.zig");
const threaded_io_limits = @import("../../common/threaded_io_limits.zig");
const http_server = @import("../transport/http_server.zig");
const snapshot_transfer = @import("../transport/snapshot_transfer.zig");

pub const FileSnapshotStoreConfig = struct {
    root_dir: []const u8,
    max_snapshot_bytes: usize = 1 << 30,
    max_chunk_bytes: usize = 4 * 1024 * 1024,
};

pub const FileSnapshotStore = struct {
    alloc: std.mem.Allocator,
    cfg: FileSnapshotStoreConfig,
    io_impl: std.Io.Threaded,
    root_dir: []u8,
    upload_locks: [64]std.atomic.Mutex = [_]std.atomic.Mutex{.unlocked} ** 64,

    pub fn init(alloc: std.mem.Allocator, cfg: FileSnapshotStoreConfig) !FileSnapshotStore {
        return .{
            .alloc = alloc,
            .cfg = cfg,
            .io_impl = threaded_io_limits.initService(alloc),
            .root_dir = try alloc.dupe(u8, cfg.root_dir),
        };
    }

    pub fn deinit(self: *FileSnapshotStore) void {
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
                .begin_chunked_snapshot = beginChunkedSnapshot,
                .put_snapshot_chunk = putSnapshotChunk,
                .commit_chunked_snapshot = commitChunkedSnapshot,
                .get_snapshot_manifest = getSnapshotManifest,
                .get_snapshot_chunk = getSnapshotChunk,
                .abort_chunked_snapshot = abortChunkedSnapshot,
            },
        };
    }

    fn putSnapshot(ptr: *anyopaque, alloc: std.mem.Allocator, snapshot_id: []const u8, body: []const u8) !void {
        _ = alloc;
        const self: *FileSnapshotStore = @ptrCast(@alignCast(ptr));
        try validateSnapshotId(snapshot_id);
        if (body.len > self.cfg.max_snapshot_bytes) return error.SnapshotTooLarge;
        const path = try self.snapshotPath(snapshot_id);
        defer self.alloc.free(path);

        const io_ctx = self.io_impl.io();
        try fs_paths.createDirPathPortable(io_ctx, self.root_dir);
        try std.Io.Dir.cwd().writeFile(io_ctx, .{
            .sub_path = path,
            .data = body,
        });
    }

    fn getSnapshot(ptr: *anyopaque, alloc: std.mem.Allocator, snapshot_id: []const u8) ![]u8 {
        const self: *FileSnapshotStore = @ptrCast(@alignCast(ptr));
        try validateSnapshotId(snapshot_id);
        const path = try self.snapshotPath(snapshot_id);
        defer self.alloc.free(path);

        return try std.Io.Dir.cwd().readFileAlloc(io(self), path, alloc, .limited(self.cfg.max_snapshot_bytes));
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
        const lock = self.uploadLock(snapshot_id);
        platform_sync.lockYielding(lock);
        defer lock.unlock();
        const path = try self.transferPath(snapshot_id, ".v2.part");
        defer self.alloc.free(path);
        try fs_paths.createDirPathPortable(io(self), self.root_dir);
        const total_len = std.math.add(u64, @sizeOf(u32) + encoded.len, manifest.data_len) catch
            return error.SnapshotTooLarge;

        // A repeated begin for the same manifest resumes the sparse staging
        // file. A different manifest with the same deterministic id starts a
        // clean incarnation and cannot inherit old chunks.
        if (self.readEncodedManifest(self.alloc, path)) |existing| {
            defer self.alloc.free(existing);
            if (std.mem.eql(u8, existing, encoded)) {
                var existing_file = try std.Io.Dir.cwd().openFile(io(self), path, .{});
                defer existing_file.close(io(self));
                // A crash can publish the header before setLength completes.
                // Only a fully shaped sparse artifact is resumable.
                if (try existing_file.length(io(self)) == total_len) return;
            }
        } else |err| switch (err) {
            error.FileNotFound, error.InvalidSnapshotManifest => {},
            else => return err,
        }

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
        var manifest = try self.readManifest(self.alloc, path);
        defer manifest.deinit(self.alloc);
        if (offset > manifest.data_len or body.len > manifest.data_len - offset)
            return error.InvalidSnapshotChunkRange;
        const data_offset = try self.dataOffset(path);
        var file = try std.Io.Dir.cwd().openFile(io(self), path, .{ .mode = .read_write });
        defer file.close(io(self));
        try file.writePositionalAll(io(self), body, data_offset + offset);
    }

    fn commitChunkedSnapshot(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        snapshot_id: []const u8,
        materialize: bool,
    ) !?@import("raft_engine").core.types.Snapshot {
        const self: *FileSnapshotStore = @ptrCast(@alignCast(ptr));
        try validateSnapshotId(snapshot_id);
        const lock = self.uploadLock(snapshot_id);
        platform_sync.lockYielding(lock);
        defer lock.unlock();
        const staging_path = try self.transferPath(snapshot_id, ".v2.part");
        defer self.alloc.free(staging_path);
        var manifest = try self.readManifest(alloc, staging_path);
        errdefer manifest.deinit(alloc);
        const data_offset = try self.dataOffset(staging_path);
        var file = try std.Io.Dir.cwd().openFile(io(self), staging_path, .{ .mode = .read_write });
        var file_open = true;
        defer if (file_open) file.close(io(self));
        if (try file.length(io(self)) != data_offset + manifest.data_len)
            return error.SnapshotArtifactSizeMismatch;
        var hasher = std.crypto.hash.sha2.Sha256.init(.{});
        var buffer: [256 * 1024]u8 = undefined;
        var read_offset: u64 = 0;
        while (read_offset < manifest.data_len) {
            const wanted: usize = @intCast(@min(buffer.len, manifest.data_len - read_offset));
            const read = try file.readPositionalAll(io(self), buffer[0..wanted], data_offset + read_offset);
            if (read != wanted) return error.SnapshotArtifactTruncated;
            hasher.update(buffer[0..read]);
            read_offset += read;
        }
        var actual_digest: [snapshot_transfer.digest_len]u8 = undefined;
        hasher.final(&actual_digest);
        if (!std.mem.eql(u8, &actual_digest, &manifest.digest)) return error.SnapshotChecksumMismatch;
        try file.sync(io(self));
        file.close(io(self));
        file_open = false;
        const committed_path = try self.transferPath(snapshot_id, ".v2");
        defer self.alloc.free(committed_path);
        // The platform rename is the publication point and replaces an
        // idempotent retry atomically; never unlink the last good artifact
        // before its replacement is durable.
        try std.Io.Dir.rename(std.Io.Dir.cwd(), staging_path, std.Io.Dir.cwd(), committed_path, io(self));
        try fs_paths.syncDirPortable(io(self), self.root_dir);
        if (!materialize) {
            manifest.deinit(alloc);
            return null;
        }

        const data_len = std.math.cast(usize, manifest.data_len) orelse return error.SnapshotTooLarge;
        const data = try alloc.alloc(u8, data_len);
        errdefer alloc.free(data);
        var committed = try std.Io.Dir.cwd().openFile(io(self), committed_path, .{});
        defer committed.close(io(self));
        const read = try committed.readPositionalAll(io(self), data, data_offset);
        if (read != data.len) return error.SnapshotArtifactTruncated;
        const metadata = manifest.metadata;
        manifest.metadata = .{};
        manifest.deinit(alloc);
        return .{ .metadata = metadata, .data = data };
    }

    fn getSnapshotManifest(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        snapshot_id: []const u8,
    ) !snapshot_transfer.Manifest {
        const self: *FileSnapshotStore = @ptrCast(@alignCast(ptr));
        try validateSnapshotId(snapshot_id);
        const lock = self.uploadLock(snapshot_id);
        platform_sync.lockYielding(lock);
        defer lock.unlock();
        const committed_path = try self.transferPath(snapshot_id, ".v2");
        defer self.alloc.free(committed_path);
        return self.readManifest(alloc, committed_path) catch |err| switch (err) {
            error.FileNotFound => blk: {
                const staging_path = try self.transferPath(snapshot_id, ".v2.part");
                defer self.alloc.free(staging_path);
                break :blk try self.readManifest(alloc, staging_path);
            },
            else => return err,
        };
    }

    fn getSnapshotChunk(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        snapshot_id: []const u8,
        offset: u64,
        max_len: usize,
    ) ![]u8 {
        const self: *FileSnapshotStore = @ptrCast(@alignCast(ptr));
        try validateSnapshotId(snapshot_id);
        if (max_len == 0 or max_len > self.cfg.max_chunk_bytes) return error.InvalidSnapshotChunkLength;
        const lock = self.uploadLock(snapshot_id);
        platform_sync.lockYielding(lock);
        defer lock.unlock();
        const path = try self.transferPath(snapshot_id, ".v2");
        defer self.alloc.free(path);
        var manifest = try self.readManifest(alloc, path);
        defer manifest.deinit(alloc);
        if (offset >= manifest.data_len) return try alloc.dupe(u8, &.{});
        const len: usize = @intCast(@min(max_len, manifest.data_len - offset));
        const chunk = try alloc.alloc(u8, len);
        errdefer alloc.free(chunk);
        var file = try std.Io.Dir.cwd().openFile(io(self), path, .{});
        defer file.close(io(self));
        const read = try file.readPositionalAll(io(self), chunk, try self.dataOffset(path) + offset);
        if (read != len) return error.SnapshotArtifactTruncated;
        return chunk;
    }

    fn abortChunkedSnapshot(ptr: *anyopaque, snapshot_id: []const u8) !void {
        const self: *FileSnapshotStore = @ptrCast(@alignCast(ptr));
        try validateSnapshotId(snapshot_id);
        const lock = self.uploadLock(snapshot_id);
        platform_sync.lockYielding(lock);
        defer lock.unlock();
        const path = try self.transferPath(snapshot_id, ".v2.part");
        defer self.alloc.free(path);
        std.Io.Dir.cwd().deleteFile(io(self), path) catch |err| switch (err) {
            error.FileNotFound => {},
            else => return err,
        };
        try fs_paths.syncDirPortable(io(self), self.root_dir);
    }

    fn readManifest(self: *FileSnapshotStore, alloc: std.mem.Allocator, path: []const u8) !snapshot_transfer.Manifest {
        const encoded = try self.readEncodedManifest(alloc, path);
        defer alloc.free(encoded);
        return try snapshot_transfer.decode(alloc, encoded);
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

test "file snapshot store resumes chunks and atomically verifies commit" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root_dir = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/raft-snaps-v2", .{tmp.sub_path});
    defer std.testing.allocator.free(root_dir);
    var store = try FileSnapshotStore.init(std.testing.allocator, .{
        .root_dir = root_dir,
        .max_chunk_bytes = 4,
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
    try iface.vtable.begin_chunked_snapshot.?(iface.ptr, manifest, "snap-v2");
    try iface.vtable.put_snapshot_chunk.?(iface.ptr, "snap-v2", 0, body[0..4]);
    // Idempotent begin retains already-published chunks for retry/resume.
    try iface.vtable.begin_chunked_snapshot.?(iface.ptr, manifest, "snap-v2");
    try iface.vtable.put_snapshot_chunk.?(iface.ptr, "snap-v2", 4, body[4..8]);
    try iface.vtable.put_snapshot_chunk.?(iface.ptr, "snap-v2", 8, body[8..12]);
    try iface.vtable.put_snapshot_chunk.?(iface.ptr, "snap-v2", 12, body[12..]);
    var snapshot = (try iface.vtable.commit_chunked_snapshot.?(
        iface.ptr,
        std.testing.allocator,
        "snap-v2",
        true,
    )).?;
    defer snapshot.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings(body, snapshot.data);
    try std.testing.expectEqual(@as(u64, 12), snapshot.metadata.index);

    // Artifact-only publication must not allocate a full Snapshot merely to
    // make the committed chunks available to a later fetch.
    try iface.vtable.begin_chunked_snapshot.?(iface.ptr, manifest, "snap-v2");
    var offset: usize = 0;
    while (offset < body.len) {
        const end = @min(body.len, offset + 4);
        try iface.vtable.put_snapshot_chunk.?(iface.ptr, "snap-v2", offset, body[offset..end]);
        offset = end;
    }
    try std.testing.expect((try iface.vtable.commit_chunked_snapshot.?(
        iface.ptr,
        std.testing.allocator,
        "snap-v2",
        false,
    )) == null);
    const tail = try iface.vtable.get_snapshot_chunk.?(iface.ptr, std.testing.allocator, "snap-v2", 5, 4);
    defer std.testing.allocator.free(tail);
    try std.testing.expectEqualStrings(body[5..9], tail);
}
