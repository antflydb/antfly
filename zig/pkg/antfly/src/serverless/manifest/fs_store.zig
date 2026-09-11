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
const Allocator = std.mem.Allocator;
const fs_paths = @import("../../common/fs_paths.zig");
const manifest_types = @import("types.zig");
const manifest_codec = @import("codec.zig");
const manifest_store = @import("store.zig");
const CancellationToken = @import("../../common/cancellation.zig").CancellationToken;

pub const FsStore = struct {
    alloc: Allocator,
    root_dir: []u8,
    durable_dirs: @import("objectstore").durable_directory.Cache = .{},
    mutex: std.atomic.Mutex = .unlocked,

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
        self.durable_dirs.deinit(self.alloc);
        self.alloc.free(self.root_dir);
        self.* = undefined;
    }

    pub fn manifestStore(self: *FsStore) manifest_store.ManifestStore {
        return .{
            .allocator = self.alloc,
            .ptr = self,
            .vtable = &vtable,
        };
    }

    pub fn put(self: *FsStore, manifest: manifest_types.Manifest) !void {
        var lock_io = threadedIo();
        defer lock_io.deinit();
        var mutation_lock = try self.lockManifestMutations(lock_io.io(), manifest.namespace);
        defer mutation_lock.close(lock_io.io());
        defer mutation_lock.unlock(lock_io.io());
        const path = try manifestPathAlloc(self.alloc, self.root_dir, manifest.namespace, manifest.version);
        defer self.alloc.free(path);

        const encoded = try manifest_codec.encodeAlloc(self.alloc, manifest);
        defer self.alloc.free(encoded);

        if (fileExists(path)) {
            const existing = try readFileAlloc(self.alloc, path);
            defer self.alloc.free(existing);
            if (!std.mem.eql(u8, existing, encoded)) return error.ManifestVersionAlreadyExists;
            var sync_io = threadedIo();
            defer sync_io.deinit();
            try self.durable_dirs.ensure(self.alloc, sync_io.io(), std.fs.path.dirname(path) orelse ".");
            try fs_paths.syncDirPortable(sync_io.io(), std.fs.path.dirname(path) orelse ".");
            return;
        }

        var directory_io = threadedIo();
        defer directory_io.deinit();
        try self.durable_dirs.ensure(self.alloc, directory_io.io(), std.fs.path.dirname(path) orelse ".");
        writeFileAtomically(path, encoded, manifest.publication_fencing_token) catch |err| switch (err) {
            error.PathAlreadyExists => {
                const winner = try readFileAlloc(self.alloc, path);
                defer self.alloc.free(winner);
                if (!std.mem.eql(u8, winner, encoded)) return error.ManifestVersionAlreadyExists;
                try fs_paths.syncDirPortable(directory_io.io(), std.fs.path.dirname(path) orelse ".");
            },
            else => return err,
        };
    }

    pub fn getAlloc(self: *FsStore, alloc: Allocator, namespace: []const u8, version: u64) !manifest_types.Manifest {
        const path = try manifestPathAlloc(alloc, self.root_dir, namespace, version);
        defer alloc.free(path);
        const raw = try readFileAlloc(alloc, path);
        defer alloc.free(raw);
        return try manifest_codec.decodeAlloc(alloc, raw);
    }

    pub fn setHead(self: *FsStore, namespace: []const u8, version: u64) !void {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        try self.setHeadUnlocked(namespace, version);
    }

    fn setHeadUnlocked(self: *FsStore, namespace: []const u8, version: u64) !void {
        var progress = try @import("../catalog/fs_progress_store.zig").FsProgressStore.init(self.alloc, self.root_dir);
        defer progress.deinit();
        const current = progress.getHead(namespace) catch |err| switch (err) {
            error.FileNotFound => null,
            else => return err,
        };
        if (!try progress.compareAndSwapHead(namespace, current, version)) return error.HeadChanged;
    }

    pub fn getHead(self: *FsStore, namespace: []const u8) !u64 {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        return try self.getHeadUnlocked(namespace);
    }

    fn getHeadUnlocked(self: *FsStore, namespace: []const u8) !u64 {
        var progress = try @import("../catalog/fs_progress_store.zig").FsProgressStore.init(self.alloc, self.root_dir);
        defer progress.deinit();
        return progress.getHead(namespace);
    }

    pub fn compareAndSwapHead(self: *FsStore, namespace: []const u8, expected: ?u64, version: u64) !bool {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();

        const manifest_path = try manifestPathAlloc(self.alloc, self.root_dir, namespace, version);
        defer self.alloc.free(manifest_path);
        if (!fileExists(manifest_path)) return error.ManifestVersionNotFound;

        var progress = try @import("../catalog/fs_progress_store.zig").FsProgressStore.init(self.alloc, self.root_dir);
        defer progress.deinit();
        return progress.compareAndSwapHead(namespace, expected, version);
    }

    pub fn listVersionsAlloc(self: *FsStore, alloc: Allocator, namespace: []const u8) ![]u64 {
        const path = try std.fs.path.join(alloc, &.{ self.root_dir, namespace, "manifests" });
        defer alloc.free(path);
        var io_impl = threadedIo();
        defer io_impl.deinit();
        const io = io_impl.io();
        var dir = std.Io.Dir.cwd().openDir(io, path, .{ .iterate = true }) catch |err| switch (err) {
            error.FileNotFound => return alloc.alloc(u64, 0),
            else => return err,
        };
        defer dir.close(io);
        var versions = std.ArrayListUnmanaged(u64).empty;
        errdefer versions.deinit(alloc);
        var entries = dir.iterate();
        while (try entries.next(io)) |entry| {
            if (entry.kind != .file or !std.mem.endsWith(u8, entry.name, ".bin")) continue;
            const stem = entry.name[0 .. entry.name.len - 4];
            if (stem.len == 0 or stem[0] == '0') continue;
            if (std.mem.indexOfNone(u8, stem, "0123456789") != null) continue;
            const version = std.fmt.parseInt(u64, stem, 10) catch continue;
            try versions.append(alloc, version);
        }
        std.mem.sort(u64, versions.items, {}, std.sort.asc(u64));
        return try versions.toOwnedSlice(alloc);
    }

    pub fn deleteVersion(self: *FsStore, namespace: []const u8, version: u64) !void {
        const current_head = self.getHead(namespace) catch |err| switch (err) {
            error.FileNotFound => null,
            else => return err,
        };
        if (current_head != null and current_head.? == version) return error.CannotDeleteHead;

        const path = try manifestPathAlloc(self.alloc, self.root_dir, namespace, version);
        defer self.alloc.free(path);
        try deleteFile(path);
    }

    fn lockManifestMutations(self: *FsStore, io: std.Io, namespace: []const u8) !std.Io.File {
        const path = try std.fs.path.join(self.alloc, &.{ self.root_dir, namespace, "MANIFEST_MUTATIONS.lock" });
        defer self.alloc.free(path);
        try self.durable_dirs.ensure(self.alloc, io, std.fs.path.dirname(path).?);
        var file = try std.Io.Dir.cwd().createFile(io, path, .{ .truncate = false, .read = true });
        errdefer file.close(io);
        try file.lock(io, .exclusive);
        return file;
    }

    pub fn deleteRetiredCandidate(self: *FsStore, namespace: []const u8, version: u64, cutoff: u64) !bool {
        if (cutoff == 0) return error.InvalidPublicationFence;
        var io_impl = threadedIo();
        defer io_impl.deinit();
        const io = io_impl.io();
        var mutation_lock = try self.lockManifestMutations(io, namespace);
        defer mutation_lock.close(io);
        defer mutation_lock.unlock(io);
        var current = self.getAlloc(self.alloc, namespace, version) catch |err| switch (err) {
            error.FileNotFound => return false,
            else => return err,
        };
        defer current.deinit(self.alloc);
        if (current.publication_fencing_token == 0 or current.publication_fencing_token >= cutoff) return false;
        try self.deleteVersion(namespace, version);
        return true;
    }

    fn cleanupRetiredTemporaries(self: *FsStore, namespace: []const u8, cutoff: u64, cancellation: CancellationToken) !void {
        const path = try std.fs.path.join(self.alloc, &.{ self.root_dir, namespace, "manifests" });
        defer self.alloc.free(path);
        var io_impl = threadedIo();
        defer io_impl.deinit();
        const io = io_impl.io();
        var dir = std.Io.Dir.cwd().openDir(io, path, .{ .iterate = true }) catch |err| switch (err) {
            error.FileNotFound => return,
            else => return err,
        };
        defer dir.close(io);
        var changed = false;
        var entries = dir.iterate();
        while (try entries.next(io)) |entry| {
            try cancellation.check();
            if (entry.kind != .file) continue;
            const token = temporaryPublicationToken(entry.name) orelse continue;
            if (token == 0 or token >= cutoff) continue;
            dir.deleteFile(io, entry.name) catch |err| switch (err) {
                error.FileNotFound => continue,
                else => return err,
            };
            changed = true;
        }
        if (changed) try fs_paths.syncDirPortable(io, path);
    }

    const vtable: manifest_store.ManifestStore.VTable = .{
        .deinit = erasedDeinit,
        .put = erasedPut,
        .get_alloc = erasedGetAlloc,
        .set_head = erasedSetHead,
        .get_head = erasedGetHead,
        .compare_and_swap_head = erasedCompareAndSwapHead,
        .list_versions_alloc = erasedListVersionsAlloc,
        .delete_version = erasedDeleteVersion,
        .delete_retired_candidate = erasedDeleteRetiredCandidate,
        .cleanup_retired_temporaries = erasedCleanupRetiredTemporaries,
    };

    fn erasedDeinit(_: Allocator, ptr: *anyopaque) void {
        const self: *FsStore = @ptrCast(@alignCast(ptr));
        self.deinit();
    }

    fn erasedPut(ptr: *anyopaque, manifest: manifest_types.Manifest) !void {
        const self: *FsStore = @ptrCast(@alignCast(ptr));
        try self.put(manifest);
    }

    fn erasedGetAlloc(ptr: *anyopaque, alloc: Allocator, namespace: []const u8, version: u64) !manifest_types.Manifest {
        const self: *FsStore = @ptrCast(@alignCast(ptr));
        return try self.getAlloc(alloc, namespace, version);
    }

    fn erasedSetHead(ptr: *anyopaque, namespace: []const u8, version: u64) !void {
        const self: *FsStore = @ptrCast(@alignCast(ptr));
        try self.setHead(namespace, version);
    }

    fn erasedGetHead(ptr: *anyopaque, namespace: []const u8) !u64 {
        const self: *FsStore = @ptrCast(@alignCast(ptr));
        return try self.getHead(namespace);
    }

    fn erasedCompareAndSwapHead(ptr: *anyopaque, namespace: []const u8, expected: ?u64, version: u64) !bool {
        const self: *FsStore = @ptrCast(@alignCast(ptr));
        return try self.compareAndSwapHead(namespace, expected, version);
    }

    fn erasedListVersionsAlloc(ptr: *anyopaque, alloc: Allocator, namespace: []const u8) ![]u64 {
        const self: *FsStore = @ptrCast(@alignCast(ptr));
        return try self.listVersionsAlloc(alloc, namespace);
    }

    fn erasedDeleteVersion(ptr: *anyopaque, namespace: []const u8, version: u64) !void {
        const self: *FsStore = @ptrCast(@alignCast(ptr));
        try self.deleteVersion(namespace, version);
    }

    fn erasedDeleteRetiredCandidate(ptr: *anyopaque, namespace: []const u8, version: u64, cutoff: u64) !bool {
        const self: *FsStore = @ptrCast(@alignCast(ptr));
        return self.deleteRetiredCandidate(namespace, version, cutoff);
    }

    fn erasedCleanupRetiredTemporaries(ptr: *anyopaque, namespace: []const u8, cutoff: u64, cancellation: CancellationToken) !void {
        const self: *FsStore = @ptrCast(@alignCast(ptr));
        try self.cleanupRetiredTemporaries(namespace, cutoff, cancellation);
    }
};

fn threadedIo() std.Io.Threaded {
    return std.Io.Threaded.init(std.heap.page_allocator, .{});
}

fn fileExists(path: []const u8) bool {
    var io_impl = threadedIo();
    defer io_impl.deinit();
    _ = std.Io.Dir.cwd().statFile(io_impl.io(), path, .{}) catch return false;
    return true;
}

fn lockAtomic(mutex: *std.atomic.Mutex) void {
    platform_sync.lockYielding(mutex);
}

fn readFileAlloc(alloc: Allocator, path: []const u8) ![]u8 {
    var io_impl = threadedIo();
    defer io_impl.deinit();
    return try std.Io.Dir.cwd().readFileAlloc(io_impl.io(), path, alloc, .limited(std.math.maxInt(usize)));
}

fn deleteFile(path: []const u8) !void {
    var io_impl = threadedIo();
    defer io_impl.deinit();
    try std.Io.Dir.cwd().deleteFile(io_impl.io(), path);
    try fs_paths.syncDirPortable(io_impl.io(), std.fs.path.dirname(path) orelse ".");
}

fn writeFileAtomically(path: []const u8, contents: []const u8, publication_token: u64) !void {
    var io_impl = threadedIo();
    defer io_impl.deinit();
    const io = io_impl.io();
    var nonce: [16]u8 = undefined;
    io.random(&nonce);
    const tmp_path = try std.fmt.allocPrint(std.heap.page_allocator, "{s}.tmp-{x:0>16}-{s}", .{ path, publication_token, std.fmt.bytesToHex(&nonce, .lower) });
    defer std.heap.page_allocator.free(tmp_path);
    var owns_temp = false;
    defer if (owns_temp) {
        std.Io.Dir.cwd().deleteFile(io, tmp_path) catch {};
    };

    {
        var file = try std.Io.Dir.cwd().createFile(io, tmp_path, .{ .exclusive = true });
        owns_temp = true;
        defer file.close(io);

        var buf: [4096]u8 = undefined;
        var writer = file.writer(io, &buf);
        try writer.interface.writeAll(contents);
        try writer.end();
        try file.sync(io);
    }

    try std.Io.Dir.renamePreserve(std.Io.Dir.cwd(), tmp_path, std.Io.Dir.cwd(), path, io);
    owns_temp = false;
    try fs_paths.syncDirPortable(io, std.fs.path.dirname(path) orelse ".");
}

fn temporaryPublicationToken(name: []const u8) ?u64 {
    const separator = std.mem.indexOf(u8, name, ".bin.tmp-") orelse return null;
    const version = name[0..separator];
    if (version.len == 0 or version[0] == '0' or std.mem.indexOfNone(u8, version, "0123456789") != null) return null;
    _ = std.fmt.parseInt(u64, version, 10) catch return null;
    const suffix = name[separator + ".bin.tmp-".len ..];
    if (suffix.len != 16 + 1 + 32 or suffix[16] != '-') return null;
    if (std.mem.indexOfNone(u8, suffix[0..16], "0123456789abcdef") != null or
        std.mem.indexOfNone(u8, suffix[17..], "0123456789abcdef") != null) return null;
    return std.fmt.parseInt(u64, suffix[0..16], 16) catch return null;
}

fn manifestPathAlloc(alloc: Allocator, root_dir: []const u8, namespace: []const u8, version: u64) ![]u8 {
    const file_name = try std.fmt.allocPrint(alloc, "{d}.bin", .{version});
    defer alloc.free(file_name);
    return try std.fs.path.join(alloc, &.{ root_dir, namespace, "manifests", file_name });
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
    const slice = std.fmt.bufPrint(buf, "/tmp/antfly-serverless-manifests-{s}-{d}-{d}\x00", .{
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

fn sampleManifest(alloc: Allocator, namespace: []const u8, version: u64, artifact_id: []const u8) !manifest_types.Manifest {
    var artifacts = try alloc.alloc(manifest_types.ArtifactRef, 1);
    errdefer alloc.free(artifacts);
    artifacts[0] = .{
        .kind = .text_segment,
        .artifact_id = try alloc.dupe(u8, artifact_id),
        .byte_len = 7,
        .checksum = try alloc.dupe(u8, "sha256:test"),
    };
    return .{
        .namespace = try alloc.dupe(u8, namespace),
        .version = version,
        .built_at_ns = 10 + version,
        .wal_start_lsn = 100,
        .wal_end_lsn = 110 + version,
        .stats = .{ .document_count = 1, .text_segment_count = 1, .vector_segment_count = 0 },
        .artifacts = artifacts,
    };
}

test "serverless retention filesystem manifest cleanup fences temporary ownership" {
    const a = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf, "retired-temporaries");
    defer cleanupTmp(path);
    var store = try FsStore.init(a, std.mem.span(path));
    defer store.deinit();
    var manifest = try sampleManifest(a, "docs", 1, "sha256:abc");
    defer manifest.deinit(a);
    manifest.publication_fencing_token = 1;
    try store.put(manifest);
    const directory = try std.fs.path.join(a, &.{ std.mem.span(path), "docs", "manifests" });
    defer a.free(directory);
    var io_impl = threadedIo();
    defer io_impl.deinit();
    const io = io_impl.io();
    var dir = try std.Io.Dir.cwd().openDir(io, directory, .{});
    defer dir.close(io);
    const names = [_][]const u8{
        "2.bin.tmp-0000000000000001-0123456789abcdef0123456789abcdef",
        "2.bin.tmp-0000000000000002-0123456789abcdef0123456789abcdef",
        "2.bin.tmp-0000000000000000-0123456789abcdef0123456789abcdef",
        "2.bin.tmp-user-owned",
    };
    for (names) |name| {
        var file = try dir.createFile(io, name, .{ .exclusive = true });
        file.close(io);
    }
    var capability = store.manifestStore();
    try capability.cleanupRetiredTemporaries("docs", 2, .{});
    try std.testing.expectError(error.FileNotFound, dir.statFile(io, names[0], .{}));
    for (names[1..]) |name| _ = try dir.statFile(io, name, .{});
    _ = try dir.statFile(io, "1.bin", .{});
    // A fenced writer can finish a delayed upload after a sweep. The next
    // inventory must rediscover it without risking any current attempt.
    var late = try dir.createFile(io, names[0], .{ .exclusive = true });
    late.close(io);
    try capability.cleanupRetiredTemporaries("docs", 2, .{});
    try std.testing.expectError(error.FileNotFound, dir.statFile(io, names[0], .{}));
    try std.testing.expectEqual(@as(?u64, null), temporaryPublicationToken("01.bin.tmp-0000000000000001-0123456789abcdef0123456789abcdef"));
}

test "serverless fs manifest store put/get/head round-trips" {
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf, "roundtrip");
    defer cleanupTmp(path);

    var store = try FsStore.init(std.testing.allocator, std.mem.span(path));
    defer store.deinit();

    var manifest = try sampleManifest(std.testing.allocator, "docs", 1, "sha256:abc");
    defer manifest.deinit(std.testing.allocator);

    try store.put(manifest);
    try store.setHead("docs", 1);

    try std.testing.expectEqual(@as(u64, 1), try store.getHead("docs"));

    var loaded = try store.getAlloc(std.testing.allocator, "docs", 1);
    defer loaded.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("docs", loaded.namespace);
    try std.testing.expectEqual(@as(u64, 1), loaded.version);
    try std.testing.expectEqualStrings("sha256:abc", loaded.artifacts[0].artifact_id);
}

test "serverless fs manifest store rejects mismatched overwrite of immutable version" {
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf, "overwrite");
    defer cleanupTmp(path);

    var store = try FsStore.init(std.testing.allocator, std.mem.span(path));
    defer store.deinit();

    var first = try sampleManifest(std.testing.allocator, "docs", 2, "sha256:one");
    defer first.deinit(std.testing.allocator);
    var second = try sampleManifest(std.testing.allocator, "docs", 2, "sha256:two");
    defer second.deinit(std.testing.allocator);

    try store.put(first);
    try std.testing.expectError(error.ManifestVersionAlreadyExists, store.put(second));
}

test "serverless fs manifest store compareAndSwapHead enforces expected version" {
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf, "cas");
    defer cleanupTmp(path);

    var store = try FsStore.init(std.testing.allocator, std.mem.span(path));
    defer store.deinit();

    var manifest_v1 = try sampleManifest(std.testing.allocator, "docs", 1, "sha256:one");
    defer manifest_v1.deinit(std.testing.allocator);
    var manifest_v2 = try sampleManifest(std.testing.allocator, "docs", 2, "sha256:two");
    defer manifest_v2.deinit(std.testing.allocator);
    try store.put(manifest_v1);
    try store.put(manifest_v2);

    try std.testing.expect(try store.compareAndSwapHead("docs", null, 1));
    try std.testing.expectEqual(@as(u64, 1), try store.getHead("docs"));
    try std.testing.expect(!(try store.compareAndSwapHead("docs", null, 2)));
    try std.testing.expect(!(try store.compareAndSwapHead("docs", 7, 2)));
    try std.testing.expect(try store.compareAndSwapHead("docs", 1, 2));
    try std.testing.expectEqual(@as(u64, 2), try store.getHead("docs"));
}

test "serverless fs manifest store candidate deletion protects recreated bootstrap and normal versions" {
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf, "candidate-recreation");
    defer cleanupTmp(path);
    var impl = try FsStore.init(std.testing.allocator, std.mem.span(path));
    defer impl.deinit();
    var store = impl.manifestStore();
    try manifest_store.testRetiredCandidateRecreation(&store);
}

test "serverless fs manifest store lists and prunes non-head versions" {
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf, "list-delete");
    defer cleanupTmp(path);

    var store = try FsStore.init(std.testing.allocator, std.mem.span(path));
    defer store.deinit();

    var manifest_v1 = try sampleManifest(std.testing.allocator, "docs", 1, "sha256:one");
    defer manifest_v1.deinit(std.testing.allocator);
    var manifest_v2 = try sampleManifest(std.testing.allocator, "docs", 2, "sha256:two");
    defer manifest_v2.deinit(std.testing.allocator);
    var manifest_v3 = try sampleManifest(std.testing.allocator, "docs", 3, "sha256:three");
    defer manifest_v3.deinit(std.testing.allocator);
    try store.put(manifest_v1);
    try store.put(manifest_v2);
    try store.put(manifest_v3);
    try store.setHead("docs", 3);

    {
        const versions = try store.listVersionsAlloc(std.testing.allocator, "docs");
        defer std.testing.allocator.free(versions);
        try std.testing.expectEqualSlices(u64, &.{ 1, 2, 3 }, versions);
    }

    try store.deleteVersion("docs", 1);
    {
        const versions = try store.listVersionsAlloc(std.testing.allocator, "docs");
        defer std.testing.allocator.free(versions);
        try std.testing.expectEqualSlices(u64, &.{ 2, 3 }, versions);
    }
    try std.testing.expectError(error.CannotDeleteHead, store.deleteVersion("docs", 3));
}

test "serverless fs manifest store compareAndSwapHead is serialized across threads" {
    const alloc = std.heap.page_allocator;

    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf, "cas-threads");
    defer cleanupTmp(path);

    var store = try FsStore.init(alloc, std.mem.span(path));
    defer store.deinit();

    var manifest_v1 = try sampleManifest(alloc, "docs", 1, "sha256:one");
    defer manifest_v1.deinit(alloc);
    var manifest_v2 = try sampleManifest(alloc, "docs", 2, "sha256:two");
    defer manifest_v2.deinit(alloc);
    var manifest_v3 = try sampleManifest(alloc, "docs", 3, "sha256:three");
    defer manifest_v3.deinit(alloc);
    try store.put(manifest_v1);
    try store.put(manifest_v2);
    try store.put(manifest_v3);
    try store.setHead("docs", 1);

    const Worker = struct {
        store: *FsStore,
        target: u64,
        result: bool = false,

        fn run(self: *@This()) void {
            self.result = self.store.compareAndSwapHead("docs", 1, self.target) catch false;
        }
    };

    var worker_a = Worker{ .store = &store, .target = 2 };
    var worker_b = Worker{ .store = &store, .target = 3 };
    var thread_a = try std.testing.io.concurrent(Worker.run, .{&worker_a});
    defer thread_a.await(std.testing.io);
    var thread_b = try std.testing.io.concurrent(Worker.run, .{&worker_b});
    thread_a.await(std.testing.io);
    thread_b.await(std.testing.io);

    try std.testing.expect(worker_a.result != worker_b.result);
    const head = try store.getHead("docs");
    try std.testing.expect(head == 2 or head == 3);
}
