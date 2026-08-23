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
const Allocator = std.mem.Allocator;
const fs_paths = @import("../../common/fs_paths.zig");
const artifact_store = @import("store.zig");
const CancellationToken = @import("../../common/cancellation.zig").CancellationToken;

pub const FsStore = struct {
    const verified_file_cache_limit: usize = 4096;

    const VerifiedFile = struct {
        inode: std.Io.File.INode,
        byte_len: u64,
        mtime_ns: i128,
    };

    alloc: Allocator,
    root_dir: []u8,
    verified_mu: std.atomic.Mutex = .unlocked,
    verified_files: std.StringHashMapUnmanaged(VerifiedFile) = .empty,

    pub fn init(alloc: Allocator, root_dir: []const u8) !FsStore {
        var io_impl = threadedIo();
        defer io_impl.deinit();
        try fs_paths.createDirPathPortable(io_impl.io(), root_dir);
        return .{
            .alloc = alloc,
            .root_dir = try alloc.dupe(u8, root_dir),
        };
    }

    pub fn deinit(self: *FsStore) void {
        lockAtomic(&self.verified_mu);
        var it = self.verified_files.keyIterator();
        while (it.next()) |key| self.alloc.free(key.*);
        self.verified_files.deinit(self.alloc);
        self.verified_mu.unlock();
        self.alloc.free(self.root_dir);
        self.* = undefined;
    }

    pub fn artifactStore(self: *FsStore) artifact_store.ArtifactStore {
        return .{
            .allocator = self.alloc,
            .ptr = self,
            .vtable = &vtable,
        };
    }

    pub fn put(self: *FsStore, alloc: Allocator, contents: []const u8) !artifact_store.ArtifactMetadata {
        const checksum = try sha256StringAlloc(alloc, contents);
        errdefer alloc.free(checksum);
        const artifact_id = try makeArtifactIdAlloc(alloc, checksum);
        errdefer alloc.free(artifact_id);

        const path = try pathForArtifactAlloc(self.alloc, self.root_dir, checksum);
        defer self.alloc.free(path);

        const existing_valid = if (fileExists(path)) blk: {
            verifyPathContent(path, @intCast(contents.len), checksum, .none) catch |err| switch (err) {
                error.ArtifactIntegrityMismatch, error.FileNotFound => break :blk false,
                else => return err,
            };
            break :blk true;
        } else false;
        if (!existing_valid) {
            try ensureParentDir(path);
            try writeFileAtomically(path, contents);
        }

        return .{
            .artifact_id = artifact_id,
            .byte_len = @intCast(contents.len),
            .checksum = checksum,
        };
    }

    pub fn getAlloc(self: *FsStore, alloc: Allocator, artifact_id: []const u8) ![]u8 {
        return try self.getAllocWithCancellation(alloc, artifact_id, .none);
    }

    pub fn getAllocWithCancellation(
        self: *FsStore,
        alloc: Allocator,
        artifact_id: []const u8,
        cancellation: CancellationToken,
    ) ![]u8 {
        try cancellation.check();
        const checksum = try artifact_store.sha256ChecksumFromArtifactId(artifact_id);
        const path = try pathForArtifactAlloc(self.alloc, self.root_dir, checksum);
        defer self.alloc.free(path);
        return try readFileAllocWithCancellation(alloc, path, cancellation);
    }

    pub fn getRangeAlloc(self: *FsStore, alloc: Allocator, artifact_id: []const u8, offset: u64, len: usize) ![]u8 {
        return try self.getRangeAllocWithCancellation(alloc, artifact_id, offset, len, .none);
    }

    pub fn getRangeAllocWithCancellation(
        self: *FsStore,
        alloc: Allocator,
        artifact_id: []const u8,
        offset: u64,
        len: usize,
        cancellation: CancellationToken,
    ) ![]u8 {
        try cancellation.check();
        const checksum = try artifact_store.sha256ChecksumFromArtifactId(artifact_id);
        const path = try pathForArtifactAlloc(self.alloc, self.root_dir, checksum);
        defer self.alloc.free(path);
        return try readFileRangeAllocWithCancellation(alloc, path, offset, len, cancellation);
    }

    pub fn stat(self: *FsStore, alloc: Allocator, artifact_id: []const u8) !artifact_store.ArtifactMetadata {
        return try self.statWithCancellation(alloc, artifact_id, .none);
    }

    pub fn statWithCancellation(self: *FsStore, alloc: Allocator, artifact_id: []const u8, cancellation: CancellationToken) !artifact_store.ArtifactMetadata {
        try cancellation.check();
        const checksum_value = try artifact_store.sha256ChecksumFromArtifactId(artifact_id);
        const checksum = try alloc.dupe(u8, checksum_value);
        errdefer alloc.free(checksum);
        const artifact_id_copy = try alloc.dupe(u8, artifact_id);
        errdefer alloc.free(artifact_id_copy);
        const path = try pathForArtifactAlloc(self.alloc, self.root_dir, checksum);
        defer self.alloc.free(path);

        var io_impl = threadedIo();
        defer io_impl.deinit();
        const file_stat = try std.Io.Dir.cwd().statFile(io_impl.io(), path, .{});
        try cancellation.check();

        return .{
            .artifact_id = artifact_id_copy,
            .byte_len = @intCast(file_stat.size),
            .checksum = checksum,
        };
    }

    pub fn verifyContent(
        self: *FsStore,
        _: Allocator,
        artifact_id: []const u8,
        expected_byte_len: u64,
        expected_checksum: []const u8,
        cancellation: CancellationToken,
    ) !void {
        const checksum = try artifact_store.sha256ChecksumFromArtifactId(artifact_id);
        if (!std.mem.eql(u8, checksum, expected_checksum)) return error.ArtifactIntegrityMismatch;
        const path = try pathForArtifactAlloc(self.alloc, self.root_dir, checksum);
        defer self.alloc.free(path);
        var io_impl = threadedIo();
        defer io_impl.deinit();
        const before = try std.Io.Dir.cwd().statFile(io_impl.io(), path, .{});
        if (before.size != expected_byte_len) return error.ArtifactIntegrityMismatch;
        const verified = VerifiedFile{
            .inode = before.inode,
            .byte_len = before.size,
            .mtime_ns = before.mtime.toNanoseconds(),
        };
        lockAtomic(&self.verified_mu);
        if (self.verified_files.get(artifact_id)) |cached| {
            if (std.meta.eql(cached, verified)) {
                self.verified_mu.unlock();
                return;
            }
        }
        self.verified_mu.unlock();

        try verifyPathContent(path, expected_byte_len, expected_checksum, cancellation);
        const after = try std.Io.Dir.cwd().statFile(io_impl.io(), path, .{});
        if (after.inode != before.inode or after.size != before.size or !std.meta.eql(after.mtime, before.mtime)) return error.ArtifactIntegrityMismatch;
        const owned_id = try self.alloc.dupe(u8, artifact_id);
        errdefer self.alloc.free(owned_id);
        lockAtomic(&self.verified_mu);
        defer self.verified_mu.unlock();
        if (!self.verified_files.contains(artifact_id) and self.verified_files.count() >= verified_file_cache_limit) {
            var iterator = self.verified_files.keyIterator();
            if (iterator.next()) |victim| {
                const removed = self.verified_files.fetchRemove(victim.*).?;
                self.alloc.free(removed.key);
            }
        }
        const gop = try self.verified_files.getOrPut(self.alloc, owned_id);
        if (gop.found_existing) self.alloc.free(owned_id) else gop.key_ptr.* = owned_id;
        gop.value_ptr.* = verified;
    }

    pub fn delete(self: *FsStore, artifact_id: []const u8) !void {
        const checksum = try artifact_store.sha256ChecksumFromArtifactId(artifact_id);
        const path = try pathForArtifactAlloc(self.alloc, self.root_dir, checksum);
        defer self.alloc.free(path);
        try deleteFile(path);
        lockAtomic(&self.verified_mu);
        defer self.verified_mu.unlock();
        if (self.verified_files.fetchRemove(artifact_id)) |removed| self.alloc.free(removed.key);
    }

    const vtable: artifact_store.ArtifactStore.VTable = .{
        .deinit = erasedDeinit,
        .put = erasedPut,
        .get_alloc = erasedGetAlloc,
        .get_alloc_with_cancellation = erasedGetAllocWithCancellation,
        .get_range_alloc = erasedGetRangeAlloc,
        .get_range_alloc_with_cancellation = erasedGetRangeAllocWithCancellation,
        .stat = erasedStat,
        .stat_with_cancellation = erasedStatWithCancellation,
        .verify_content = erasedVerifyContent,
        .delete = erasedDelete,
    };

    fn erasedDeinit(_: Allocator, ptr: *anyopaque) void {
        const self: *FsStore = @ptrCast(@alignCast(ptr));
        self.deinit();
    }

    fn erasedPut(ptr: *anyopaque, alloc: Allocator, contents: []const u8) !artifact_store.ArtifactMetadata {
        const self: *FsStore = @ptrCast(@alignCast(ptr));
        return try self.put(alloc, contents);
    }

    fn erasedGetAlloc(ptr: *anyopaque, alloc: Allocator, artifact_id: []const u8) ![]u8 {
        const self: *FsStore = @ptrCast(@alignCast(ptr));
        return try self.getAlloc(alloc, artifact_id);
    }

    fn erasedGetAllocWithCancellation(ptr: *anyopaque, alloc: Allocator, artifact_id: []const u8, cancellation: CancellationToken) ![]u8 {
        const self: *FsStore = @ptrCast(@alignCast(ptr));
        return try self.getAllocWithCancellation(alloc, artifact_id, cancellation);
    }

    fn erasedGetRangeAlloc(ptr: *anyopaque, alloc: Allocator, artifact_id: []const u8, offset: u64, len: usize) ![]u8 {
        const self: *FsStore = @ptrCast(@alignCast(ptr));
        return try self.getRangeAlloc(alloc, artifact_id, offset, len);
    }

    fn erasedGetRangeAllocWithCancellation(ptr: *anyopaque, alloc: Allocator, artifact_id: []const u8, offset: u64, len: usize, cancellation: CancellationToken) ![]u8 {
        const self: *FsStore = @ptrCast(@alignCast(ptr));
        return try self.getRangeAllocWithCancellation(alloc, artifact_id, offset, len, cancellation);
    }

    fn erasedStat(ptr: *anyopaque, alloc: Allocator, artifact_id: []const u8) !artifact_store.ArtifactMetadata {
        const self: *FsStore = @ptrCast(@alignCast(ptr));
        return try self.stat(alloc, artifact_id);
    }

    fn erasedStatWithCancellation(ptr: *anyopaque, alloc: Allocator, artifact_id: []const u8, cancellation: CancellationToken) !artifact_store.ArtifactMetadata {
        const self: *FsStore = @ptrCast(@alignCast(ptr));
        return try self.statWithCancellation(alloc, artifact_id, cancellation);
    }

    fn erasedVerifyContent(ptr: *anyopaque, alloc: Allocator, artifact_id: []const u8, expected_byte_len: u64, expected_checksum: []const u8, cancellation: CancellationToken) !void {
        const self: *FsStore = @ptrCast(@alignCast(ptr));
        try self.verifyContent(alloc, artifact_id, expected_byte_len, expected_checksum, cancellation);
    }

    fn erasedDelete(ptr: *anyopaque, artifact_id: []const u8) !void {
        const self: *FsStore = @ptrCast(@alignCast(ptr));
        try self.delete(artifact_id);
    }
};

fn verifyPathContent(path: []const u8, expected_byte_len: u64, expected_checksum: []const u8, cancellation: CancellationToken) !void {
    try cancellation.check();
    var io_impl = threadedIo();
    defer io_impl.deinit();
    var file = if (std.fs.path.isAbsolute(path))
        try std.Io.Dir.openFileAbsolute(io_impl.io(), path, .{})
    else
        try std.Io.Dir.cwd().openFile(io_impl.io(), path, .{});
    defer file.close(io_impl.io());
    var reader = file.reader(io_impl.io(), &.{});
    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    var buffer: [1024 * 1024]u8 = undefined;
    var total: u64 = 0;
    while (true) {
        try cancellation.check();
        const read = try reader.interface.readSliceShort(&buffer);
        if (read == 0) break;
        total = std.math.add(u64, total, read) catch return error.ArtifactIntegrityMismatch;
        if (total > expected_byte_len) return error.ArtifactIntegrityMismatch;
        hasher.update(buffer[0..read]);
    }
    if (total != expected_byte_len) return error.ArtifactIntegrityMismatch;
    var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
    hasher.final(&digest);
    const actual = std.fmt.bytesToHex(digest, .lower);
    if (!std.mem.eql(u8, &actual, expected_checksum)) return error.ArtifactIntegrityMismatch;
    try cancellation.check();
}

fn threadedIo() std.Io.Threaded {
    return std.Io.Threaded.init(std.heap.page_allocator, .{});
}

fn lockAtomic(mutex: *std.atomic.Mutex) void {
    while (!mutex.tryLock()) std.atomic.spinLoopHint();
}

fn fileExists(path: []const u8) bool {
    var io_impl = threadedIo();
    defer io_impl.deinit();
    _ = std.Io.Dir.cwd().statFile(io_impl.io(), path, .{}) catch return false;
    return true;
}

fn readFileAllocWithCancellation(alloc: Allocator, path: []const u8, cancellation: CancellationToken) ![]u8 {
    try cancellation.check();
    var io_impl = threadedIo();
    defer io_impl.deinit();
    const io = io_impl.io();
    const file = if (std.fs.path.isAbsolute(path))
        try std.Io.Dir.openFileAbsolute(io, path, .{})
    else
        try std.Io.Dir.cwd().openFile(io, path, .{});
    defer file.close(io);
    const stat = try file.stat(io);
    const output_len = std.math.cast(usize, stat.size) orelse return error.ArtifactTooLarge;
    const out = try alloc.alloc(u8, output_len);
    errdefer alloc.free(out);

    const cancellation_chunk_bytes = 1024 * 1024;
    var copied: usize = 0;
    while (copied < out.len) {
        try cancellation.check();
        const chunk_len = @min(cancellation_chunk_bytes, out.len - copied);
        const chunk = out[copied..][0..chunk_len];
        if (try file.readPositionalAll(io, chunk, copied) != chunk.len) return error.ShortArtifactRead;
        copied += chunk.len;
    }
    try cancellation.check();
    return out;
}

fn readFileRangeAllocWithCancellation(
    alloc: Allocator,
    path: []const u8,
    offset: u64,
    len: usize,
    cancellation: CancellationToken,
) ![]u8 {
    var io_impl = threadedIo();
    defer io_impl.deinit();
    const io = io_impl.io();
    const file = if (std.fs.path.isAbsolute(path))
        try std.Io.Dir.openFileAbsolute(io, path, .{})
    else
        try std.Io.Dir.cwd().openFile(io, path, .{});
    defer file.close(io);
    const stat = try file.stat(io);
    if (offset > stat.size) return error.InvalidRange;
    const available = stat.size - offset;
    const output_len: usize = @intCast(@min(available, len));
    const out = try alloc.alloc(u8, output_len);
    errdefer alloc.free(out);

    const cancellation_chunk_bytes = 1024 * 1024;
    var copied: usize = 0;
    while (copied < out.len) {
        try cancellation.check();
        const chunk_len = @min(cancellation_chunk_bytes, out.len - copied);
        const chunk = out[copied..][0..chunk_len];
        if (try file.readPositionalAll(io, chunk, offset + copied) != chunk.len) return error.ShortArtifactRead;
        copied += chunk.len;
    }
    try cancellation.check();
    return out;
}

fn deleteFile(path: []const u8) !void {
    var io_impl = threadedIo();
    defer io_impl.deinit();
    try std.Io.Dir.cwd().deleteFile(io_impl.io(), path);
}

fn ensureParentDir(path: []const u8) !void {
    const parent = std.fs.path.dirname(path) orelse return;
    var io_impl = threadedIo();
    defer io_impl.deinit();
    try fs_paths.createDirPathPortable(io_impl.io(), parent);
}

fn writeFileAtomically(path: []const u8, contents: []const u8) !void {
    const tmp_path = try std.fmt.allocPrint(std.heap.page_allocator, "{s}.tmp-{d}", .{ path, test_nonce.fetchAdd(1, .monotonic) });
    defer std.heap.page_allocator.free(tmp_path);

    var io_impl = threadedIo();
    defer io_impl.deinit();
    const io = io_impl.io();

    {
        var file = try std.Io.Dir.createFileAbsolute(io, tmp_path, .{ .truncate = true });
        defer file.close(io);

        var buf: [4096]u8 = undefined;
        var writer = file.writer(io, &buf);
        try writer.interface.writeAll(contents);
        try writer.end();
    }

    if (std.fs.path.isAbsolute(path)) {
        std.Io.Dir.renameAbsolute(tmp_path, path, io) catch |err| {
            std.Io.Dir.deleteFileAbsolute(io, tmp_path) catch {};
            return err;
        };
    } else {
        std.Io.Dir.rename(std.Io.Dir.cwd(), tmp_path, std.Io.Dir.cwd(), path, io) catch |err| {
            std.Io.Dir.deleteFileAbsolute(io, tmp_path) catch {};
            return err;
        };
    }
}

fn sha256StringAlloc(alloc: Allocator, contents: []const u8) ![]u8 {
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(contents, &digest, .{});

    const out = try alloc.alloc(u8, 64);
    for (digest, 0..) |byte, idx| {
        out[idx * 2] = hexNibble(byte >> 4);
        out[idx * 2 + 1] = hexNibble(byte & 0x0f);
    }
    return out;
}

fn makeArtifactIdAlloc(alloc: Allocator, checksum: []const u8) ![]u8 {
    return try std.fmt.allocPrint(alloc, "sha256:{s}", .{checksum});
}

fn pathForArtifactAlloc(alloc: Allocator, root_dir: []const u8, checksum: []const u8) ![]u8 {
    try artifact_store.validateSha256Checksum(checksum);
    return try std.fs.path.join(alloc, &.{ root_dir, "sha256", checksum[0..2], checksum[2..] });
}

fn hexNibble(v: u8) u8 {
    return if (v < 10) '0' + v else 'a' + (v - 10);
}

var test_nonce: std.atomic.Value(u64) = .init(0);

fn nowNs() u64 {
    var io_impl = threadedIo();
    defer io_impl.deinit();
    const now = std.Io.Timestamp.now(io_impl.io(), .awake);
    return @intCast(now.toNanoseconds());
}

fn tmpPath(buf: []u8, label: []const u8) [*:0]const u8 {
    const nonce = test_nonce.fetchAdd(1, .monotonic);
    const slice = std.fmt.bufPrint(buf, "/tmp/antfly-serverless-artifacts-{s}-{d}-{d}\x00", .{
        label,
        nowNs(),
        nonce,
    }) catch unreachable;
    return @ptrCast(slice.ptr);
}

fn cleanupTmp(path: [*:0]const u8) void {
    var io_impl = threadedIo();
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), std.mem.span(path)) catch {};
}

test "fs artifact store put/get/stat are content-addressed" {
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf, "put-get");
    defer cleanupTmp(path);

    var store = try FsStore.init(std.testing.allocator, std.mem.span(path));
    defer store.deinit();

    var meta_a = try store.put(std.testing.allocator, "hello world");
    defer meta_a.deinit(std.testing.allocator);
    var meta_b = try store.put(std.testing.allocator, "hello world");
    defer meta_b.deinit(std.testing.allocator);

    try std.testing.expectEqualStrings(meta_a.artifact_id, meta_b.artifact_id);
    try std.testing.expectEqualStrings(meta_a.checksum, meta_b.checksum);
    try std.testing.expectEqual(@as(u64, 11), meta_a.byte_len);

    const full = try store.getAlloc(std.testing.allocator, meta_a.artifact_id);
    defer std.testing.allocator.free(full);
    try std.testing.expectEqualStrings("hello world", full);

    var stat = try store.stat(std.testing.allocator, meta_a.artifact_id);
    defer stat.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u64, 11), stat.byte_len);
}

test "fs artifact store getRangeAlloc returns requested slice" {
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf, "range");
    defer cleanupTmp(path);

    var store = try FsStore.init(std.testing.allocator, std.mem.span(path));
    defer store.deinit();

    var meta = try store.put(std.testing.allocator, "abcdef");
    defer meta.deinit(std.testing.allocator);

    const mid = try store.getRangeAlloc(std.testing.allocator, meta.artifact_id, 2, 3);
    defer std.testing.allocator.free(mid);
    try std.testing.expectEqualStrings("cde", mid);
}

test "serverless fs artifact store detects and repairs same-length content corruption" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const root = tmpPath(&path_buf, "repair-corruption");
    defer cleanupTmp(root);

    var store = try FsStore.init(alloc, std.mem.span(root));
    defer store.deinit();
    var meta = try store.put(alloc, "alpha");
    defer meta.deinit(alloc);
    const path = try pathForArtifactAlloc(alloc, store.root_dir, meta.checksum);
    defer alloc.free(path);
    try writeFileAtomically(path, "omega");

    var iface = store.artifactStore();
    try std.testing.expectError(error.ArtifactIntegrityMismatch, iface.verifyContentWithCancellationUsingAllocator(
        alloc,
        meta.artifact_id,
        meta.byte_len,
        meta.checksum,
        .none,
    ));
    var repaired = try store.put(alloc, "alpha");
    defer repaired.deinit(alloc);
    try iface.verifyContentWithCancellationUsingAllocator(alloc, repaired.artifact_id, repaired.byte_len, repaired.checksum, .none);
    const payload = try store.getAlloc(alloc, repaired.artifact_id);
    defer alloc.free(payload);
    try std.testing.expectEqualStrings("alpha", payload);
}

test "fs artifact store rejects malformed content addresses before lookup" {
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf, "invalid-id");
    defer cleanupTmp(path);

    var store = try FsStore.init(std.testing.allocator, std.mem.span(path));
    defer store.deinit();
    try std.testing.expectError(error.InvalidArtifactId, store.getAlloc(std.testing.allocator, "sha256:abcd"));
}

test "fs artifact store erased interface works" {
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf, "erased");
    defer cleanupTmp(path);

    var fs = try FsStore.init(std.testing.allocator, std.mem.span(path));
    var runtime = fs.artifactStore();
    defer runtime.deinit();

    var meta = try runtime.put("payload");
    defer meta.deinit(std.testing.allocator);

    const got = try runtime.getAlloc(meta.artifact_id);
    defer std.testing.allocator.free(got);
    try std.testing.expectEqualStrings("payload", got);
}

test "fs artifact store delete removes unreachable artifact" {
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf, "delete");
    defer cleanupTmp(path);

    var fs = try FsStore.init(std.testing.allocator, std.mem.span(path));
    var runtime = fs.artifactStore();
    defer runtime.deinit();

    var meta = try runtime.put("payload");
    defer meta.deinit(std.testing.allocator);
    try runtime.delete(meta.artifact_id);
    try std.testing.expectError(error.FileNotFound, runtime.getAlloc(meta.artifact_id));
}
