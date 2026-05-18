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
const manifest_types = @import("types.zig");
const manifest_codec = @import("codec.zig");
const manifest_store = @import("store.zig");

pub const FsStore = struct {
    alloc: Allocator,
    root_dir: []u8,
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
        const path = try manifestPathAlloc(self.alloc, self.root_dir, manifest.namespace, manifest.version);
        defer self.alloc.free(path);

        const encoded = try manifest_codec.encodeAlloc(self.alloc, manifest);
        defer self.alloc.free(encoded);

        if (fileExists(path)) {
            const existing = try readFileAlloc(self.alloc, path);
            defer self.alloc.free(existing);
            if (!std.mem.eql(u8, existing, encoded)) return error.ManifestVersionAlreadyExists;
            return;
        }

        try ensureParentDir(path);
        try writeFileAtomically(path, encoded);
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
        const path = try headPathAlloc(self.alloc, self.root_dir, namespace);
        defer self.alloc.free(path);
        try ensureParentDir(path);
        const payload = try std.fmt.allocPrint(self.alloc, "{d}", .{version});
        defer self.alloc.free(payload);
        try writeFileAtomically(path, payload);
    }

    pub fn getHead(self: *FsStore, namespace: []const u8) !u64 {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        return try self.getHeadUnlocked(namespace);
    }

    fn getHeadUnlocked(self: *FsStore, namespace: []const u8) !u64 {
        const path = try headPathAlloc(self.alloc, self.root_dir, namespace);
        defer self.alloc.free(path);
        const raw = try readFileAlloc(self.alloc, path);
        defer self.alloc.free(raw);
        return try std.fmt.parseInt(u64, std.mem.trim(u8, raw, " \t\r\n"), 10);
    }

    pub fn compareAndSwapHead(self: *FsStore, namespace: []const u8, expected: ?u64, version: u64) !bool {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();

        const manifest_path = try manifestPathAlloc(self.alloc, self.root_dir, namespace, version);
        defer self.alloc.free(manifest_path);
        if (!fileExists(manifest_path)) return error.ManifestVersionNotFound;

        const current = self.getHeadUnlocked(namespace) catch |err| switch (err) {
            error.FileNotFound => null,
            else => return err,
        };
        if (current != expected) return false;
        try self.setHeadUnlocked(namespace, version);
        return true;
    }

    pub fn listVersionsAlloc(self: *FsStore, alloc: Allocator, namespace: []const u8) ![]u64 {
        const head = self.getHead(namespace) catch |err| switch (err) {
            error.FileNotFound => return try alloc.alloc(u64, 0),
            else => return err,
        };

        var versions = std.ArrayListUnmanaged(u64).empty;
        errdefer versions.deinit(alloc);

        var version: u64 = 1;
        while (version <= head) : (version += 1) {
            const path = try manifestPathAlloc(alloc, self.root_dir, namespace, version);
            defer alloc.free(path);
            if (fileExists(path)) try versions.append(alloc, version);
        }
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

    const vtable: manifest_store.ManifestStore.VTable = .{
        .deinit = erasedDeinit,
        .put = erasedPut,
        .get_alloc = erasedGetAlloc,
        .set_head = erasedSetHead,
        .get_head = erasedGetHead,
        .compare_and_swap_head = erasedCompareAndSwapHead,
        .list_versions_alloc = erasedListVersionsAlloc,
        .delete_version = erasedDeleteVersion,
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
    while (!mutex.tryLock()) std.atomic.spinLoopHint();
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

fn manifestPathAlloc(alloc: Allocator, root_dir: []const u8, namespace: []const u8, version: u64) ![]u8 {
    const file_name = try std.fmt.allocPrint(alloc, "{d}.bin", .{version});
    defer alloc.free(file_name);
    return try std.fs.path.join(alloc, &.{ root_dir, namespace, "manifests", file_name });
}

fn headPathAlloc(alloc: Allocator, root_dir: []const u8, namespace: []const u8) ![]u8 {
    return try std.fs.path.join(alloc, &.{ root_dir, namespace, "HEAD" });
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

test "fs manifest store put/get/head round-trips" {
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

test "fs manifest store rejects mismatched overwrite of immutable version" {
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

test "fs manifest store compareAndSwapHead enforces expected version" {
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

test "fs manifest store lists and prunes non-head versions" {
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

test "fs manifest store compareAndSwapHead is serialized across threads" {
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
    const thread_a = try std.Thread.spawn(.{}, Worker.run, .{&worker_a});
    const thread_b = try std.Thread.spawn(.{}, Worker.run, .{&worker_b});
    thread_a.join();
    thread_b.join();

    try std.testing.expect(worker_a.result != worker_b.result);
    const head = try store.getHead("docs");
    try std.testing.expect(head == 2 or head == 3);
}
