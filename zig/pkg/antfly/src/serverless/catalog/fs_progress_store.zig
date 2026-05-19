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
const catalog_types = @import("types.zig");
const progress_store = @import("progress_store.zig");

pub const FsProgressStore = struct {
    alloc: Allocator,
    root_dir: []u8,
    mutex: std.atomic.Mutex = .unlocked,

    pub fn init(alloc: Allocator, root_dir: []const u8) !FsProgressStore {
        var io_impl = threadedIo();
        defer io_impl.deinit();
        try fs_paths.createDirPathPortable(io_impl.io(), root_dir);
        return .{
            .alloc = alloc,
            .root_dir = try alloc.dupe(u8, root_dir),
        };
    }

    pub fn deinit(self: *FsProgressStore) void {
        self.alloc.free(self.root_dir);
        self.* = undefined;
    }

    pub fn progressStore(self: *FsProgressStore) progress_store.ProgressStore {
        return .{
            .allocator = self.alloc,
            .ptr = self,
            .vtable = &vtable,
        };
    }

    pub fn getHead(self: *FsProgressStore, namespace: []const u8) !u64 {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        const path = try headPathAlloc(self.alloc, self.root_dir, namespace);
        defer self.alloc.free(path);
        const raw = try readFileAlloc(self.alloc, path);
        defer self.alloc.free(raw);
        return try std.fmt.parseInt(u64, std.mem.trim(u8, raw, " \t\r\n"), 10);
    }

    pub fn compareAndSwapHead(self: *FsProgressStore, namespace: []const u8, expected: ?u64, version: u64) !bool {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();

        const current = self.readOptionalU64Unlocked(namespace, .head) catch |err| switch (err) {
            error.FileNotFound => null,
            else => return err,
        };
        if (current != expected) return false;
        try self.writeU64Unlocked(namespace, .head, version);
        return true;
    }

    pub fn getGcWatermark(self: *FsProgressStore, namespace: []const u8) !?u64 {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        return self.readOptionalU64Unlocked(namespace, .gc_watermark) catch |err| switch (err) {
            error.FileNotFound => null,
            else => return err,
        };
    }

    pub fn compareAndSwapGcWatermark(self: *FsProgressStore, namespace: []const u8, expected: ?u64, watermark: u64) !bool {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();

        const current = self.readOptionalU64Unlocked(namespace, .gc_watermark) catch |err| switch (err) {
            error.FileNotFound => null,
            else => return err,
        };
        if (current != expected) return false;
        if (current != null and watermark < current.?) return false;
        try self.writeU64Unlocked(namespace, .gc_watermark, watermark);
        return true;
    }

    pub fn getEnrichmentHeadVersion(self: *FsProgressStore, namespace: []const u8) !?u64 {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        return self.readOptionalU64Unlocked(namespace, .enrichment_head_version) catch |err| switch (err) {
            error.FileNotFound => null,
            else => return err,
        };
    }

    pub fn compareAndSwapEnrichmentHeadVersion(self: *FsProgressStore, namespace: []const u8, expected: ?u64, head_version: u64) !bool {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();

        const current = self.readOptionalU64Unlocked(namespace, .enrichment_head_version) catch |err| switch (err) {
            error.FileNotFound => null,
            else => return err,
        };
        if (current != expected) return false;
        try self.writeU64Unlocked(namespace, .enrichment_head_version, head_version);
        return true;
    }

    pub fn getEnrichmentStage(self: *FsProgressStore, namespace: []const u8) !?u64 {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        return self.readOptionalU64Unlocked(namespace, .enrichment_stage) catch |err| switch (err) {
            error.FileNotFound => null,
            else => return err,
        };
    }

    pub fn compareAndSwapEnrichmentStage(self: *FsProgressStore, namespace: []const u8, expected: ?u64, stage: u64) !bool {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();

        const current = self.readOptionalU64Unlocked(namespace, .enrichment_stage) catch |err| switch (err) {
            error.FileNotFound => null,
            else => return err,
        };
        if (current != expected) return false;
        try self.writeU64Unlocked(namespace, .enrichment_stage, stage);
        return true;
    }

    pub fn getEnrichmentDocOffset(self: *FsProgressStore, namespace: []const u8) !?u64 {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        return self.readOptionalU64Unlocked(namespace, .enrichment_doc_offset) catch |err| switch (err) {
            error.FileNotFound => null,
            else => return err,
        };
    }

    pub fn compareAndSwapEnrichmentDocOffset(self: *FsProgressStore, namespace: []const u8, expected: ?u64, doc_offset: u64) !bool {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();

        const current = self.readOptionalU64Unlocked(namespace, .enrichment_doc_offset) catch |err| switch (err) {
            error.FileNotFound => null,
            else => return err,
        };
        if (current != expected) return false;
        try self.writeU64Unlocked(namespace, .enrichment_doc_offset, doc_offset);
        return true;
    }

    pub fn getEnrichmentStageHeadVersion(self: *FsProgressStore, namespace: []const u8, stage: catalog_types.EnrichmentStage) !?u64 {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        return self.readOptionalStageU64Unlocked(namespace, stage, "HEAD_VERSION") catch |err| switch (err) {
            error.FileNotFound => null,
            else => return err,
        };
    }

    pub fn compareAndSwapEnrichmentStageHeadVersion(
        self: *FsProgressStore,
        namespace: []const u8,
        stage: catalog_types.EnrichmentStage,
        expected: ?u64,
        head_version: u64,
    ) !bool {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();

        const current = self.readOptionalStageU64Unlocked(namespace, stage, "HEAD_VERSION") catch |err| switch (err) {
            error.FileNotFound => null,
            else => return err,
        };
        if (current != expected) return false;
        try self.writeStageU64Unlocked(namespace, stage, "HEAD_VERSION", head_version);
        return true;
    }

    pub fn getEnrichmentStageDocOffset(self: *FsProgressStore, namespace: []const u8, stage: catalog_types.EnrichmentStage) !?u64 {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        return self.readOptionalStageU64Unlocked(namespace, stage, "DOC_OFFSET") catch |err| switch (err) {
            error.FileNotFound => null,
            else => return err,
        };
    }

    pub fn compareAndSwapEnrichmentStageDocOffset(
        self: *FsProgressStore,
        namespace: []const u8,
        stage: catalog_types.EnrichmentStage,
        expected: ?u64,
        doc_offset: u64,
    ) !bool {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();

        const current = self.readOptionalStageU64Unlocked(namespace, stage, "DOC_OFFSET") catch |err| switch (err) {
            error.FileNotFound => null,
            else => return err,
        };
        if (current != expected) return false;
        try self.writeStageU64Unlocked(namespace, stage, "DOC_OFFSET", doc_offset);
        return true;
    }

    const Kind = enum {
        head,
        gc_watermark,
        enrichment_head_version,
        enrichment_stage,
        enrichment_doc_offset,
    };

    fn readOptionalU64Unlocked(self: *FsProgressStore, namespace: []const u8, kind: Kind) !u64 {
        const path = try pathForAlloc(self.alloc, self.root_dir, namespace, kind);
        defer self.alloc.free(path);
        const raw = try readFileAlloc(self.alloc, path);
        defer self.alloc.free(raw);
        return try std.fmt.parseInt(u64, std.mem.trim(u8, raw, " \t\r\n"), 10);
    }

    fn writeU64Unlocked(self: *FsProgressStore, namespace: []const u8, kind: Kind, value: u64) !void {
        const path = try pathForAlloc(self.alloc, self.root_dir, namespace, kind);
        defer self.alloc.free(path);
        try ensureParentDir(path);
        const payload = try std.fmt.allocPrint(self.alloc, "{d}", .{value});
        defer self.alloc.free(payload);
        try writeFileAtomically(path, payload);
    }

    fn readOptionalStageU64Unlocked(self: *FsProgressStore, namespace: []const u8, stage: catalog_types.EnrichmentStage, leaf: []const u8) !u64 {
        const path = try stagePathAlloc(self.alloc, self.root_dir, namespace, stage, leaf);
        defer self.alloc.free(path);
        const raw = try readFileAlloc(self.alloc, path);
        defer self.alloc.free(raw);
        return try std.fmt.parseInt(u64, std.mem.trim(u8, raw, " \t\r\n"), 10);
    }

    fn writeStageU64Unlocked(self: *FsProgressStore, namespace: []const u8, stage: catalog_types.EnrichmentStage, leaf: []const u8, value: u64) !void {
        const path = try stagePathAlloc(self.alloc, self.root_dir, namespace, stage, leaf);
        defer self.alloc.free(path);
        try ensureParentDir(path);
        const payload = try std.fmt.allocPrint(self.alloc, "{d}", .{value});
        defer self.alloc.free(payload);
        try writeFileAtomically(path, payload);
    }

    const vtable: progress_store.ProgressStore.VTable = .{
        .deinit = erasedDeinit,
        .get_head = erasedGetHead,
        .compare_and_swap_head = erasedCompareAndSwapHead,
        .get_gc_watermark = erasedGetGcWatermark,
        .compare_and_swap_gc_watermark = erasedCompareAndSwapGcWatermark,
        .get_enrichment_head_version = erasedGetEnrichmentHeadVersion,
        .compare_and_swap_enrichment_head_version = erasedCompareAndSwapEnrichmentHeadVersion,
        .get_enrichment_stage = erasedGetEnrichmentStage,
        .compare_and_swap_enrichment_stage = erasedCompareAndSwapEnrichmentStage,
        .get_enrichment_doc_offset = erasedGetEnrichmentDocOffset,
        .compare_and_swap_enrichment_doc_offset = erasedCompareAndSwapEnrichmentDocOffset,
        .get_enrichment_stage_head_version = erasedGetEnrichmentStageHeadVersion,
        .compare_and_swap_enrichment_stage_head_version = erasedCompareAndSwapEnrichmentStageHeadVersion,
        .get_enrichment_stage_doc_offset = erasedGetEnrichmentStageDocOffset,
        .compare_and_swap_enrichment_stage_doc_offset = erasedCompareAndSwapEnrichmentStageDocOffset,
    };

    fn erasedDeinit(_: Allocator, ptr: *anyopaque) void {
        const self: *FsProgressStore = @ptrCast(@alignCast(ptr));
        self.deinit();
    }

    fn erasedGetHead(ptr: *anyopaque, namespace: []const u8) !u64 {
        const self: *FsProgressStore = @ptrCast(@alignCast(ptr));
        return try self.getHead(namespace);
    }

    fn erasedCompareAndSwapHead(ptr: *anyopaque, namespace: []const u8, expected: ?u64, version: u64) !bool {
        const self: *FsProgressStore = @ptrCast(@alignCast(ptr));
        return try self.compareAndSwapHead(namespace, expected, version);
    }

    fn erasedGetGcWatermark(ptr: *anyopaque, namespace: []const u8) !?u64 {
        const self: *FsProgressStore = @ptrCast(@alignCast(ptr));
        return try self.getGcWatermark(namespace);
    }

    fn erasedCompareAndSwapGcWatermark(ptr: *anyopaque, namespace: []const u8, expected: ?u64, watermark: u64) !bool {
        const self: *FsProgressStore = @ptrCast(@alignCast(ptr));
        return try self.compareAndSwapGcWatermark(namespace, expected, watermark);
    }

    fn erasedGetEnrichmentHeadVersion(ptr: *anyopaque, namespace: []const u8) !?u64 {
        const self: *FsProgressStore = @ptrCast(@alignCast(ptr));
        return try self.getEnrichmentHeadVersion(namespace);
    }

    fn erasedCompareAndSwapEnrichmentHeadVersion(ptr: *anyopaque, namespace: []const u8, expected: ?u64, head_version: u64) !bool {
        const self: *FsProgressStore = @ptrCast(@alignCast(ptr));
        return try self.compareAndSwapEnrichmentHeadVersion(namespace, expected, head_version);
    }

    fn erasedGetEnrichmentStage(ptr: *anyopaque, namespace: []const u8) !?u64 {
        const self: *FsProgressStore = @ptrCast(@alignCast(ptr));
        return try self.getEnrichmentStage(namespace);
    }

    fn erasedCompareAndSwapEnrichmentStage(ptr: *anyopaque, namespace: []const u8, expected: ?u64, stage: u64) !bool {
        const self: *FsProgressStore = @ptrCast(@alignCast(ptr));
        return try self.compareAndSwapEnrichmentStage(namespace, expected, stage);
    }

    fn erasedGetEnrichmentDocOffset(ptr: *anyopaque, namespace: []const u8) !?u64 {
        const self: *FsProgressStore = @ptrCast(@alignCast(ptr));
        return try self.getEnrichmentDocOffset(namespace);
    }

    fn erasedCompareAndSwapEnrichmentDocOffset(ptr: *anyopaque, namespace: []const u8, expected: ?u64, doc_offset: u64) !bool {
        const self: *FsProgressStore = @ptrCast(@alignCast(ptr));
        return try self.compareAndSwapEnrichmentDocOffset(namespace, expected, doc_offset);
    }

    fn erasedGetEnrichmentStageHeadVersion(ptr: *anyopaque, namespace: []const u8, stage_id: u8) !?u64 {
        const self: *FsProgressStore = @ptrCast(@alignCast(ptr));
        return try self.getEnrichmentStageHeadVersion(namespace, @enumFromInt(stage_id));
    }

    fn erasedCompareAndSwapEnrichmentStageHeadVersion(
        ptr: *anyopaque,
        namespace: []const u8,
        stage_id: u8,
        expected: ?u64,
        head_version: u64,
    ) !bool {
        const self: *FsProgressStore = @ptrCast(@alignCast(ptr));
        return try self.compareAndSwapEnrichmentStageHeadVersion(namespace, @enumFromInt(stage_id), expected, head_version);
    }

    fn erasedGetEnrichmentStageDocOffset(ptr: *anyopaque, namespace: []const u8, stage_id: u8) !?u64 {
        const self: *FsProgressStore = @ptrCast(@alignCast(ptr));
        return try self.getEnrichmentStageDocOffset(namespace, @enumFromInt(stage_id));
    }

    fn erasedCompareAndSwapEnrichmentStageDocOffset(
        ptr: *anyopaque,
        namespace: []const u8,
        stage_id: u8,
        expected: ?u64,
        doc_offset: u64,
    ) !bool {
        const self: *FsProgressStore = @ptrCast(@alignCast(ptr));
        return try self.compareAndSwapEnrichmentStageDocOffset(namespace, @enumFromInt(stage_id), expected, doc_offset);
    }
};

fn threadedIo() std.Io.Threaded {
    return std.Io.Threaded.init(std.heap.page_allocator, .{});
}

fn lockAtomic(mutex: *std.atomic.Mutex) void {
    while (!mutex.tryLock()) std.atomic.spinLoopHint();
}

fn readFileAlloc(alloc: Allocator, path: []const u8) ![]u8 {
    var io_impl = threadedIo();
    defer io_impl.deinit();
    return try std.Io.Dir.cwd().readFileAlloc(io_impl.io(), path, alloc, .limited(std.math.maxInt(usize)));
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

fn pathForAlloc(alloc: Allocator, root_dir: []const u8, namespace: []const u8, kind: FsProgressStore.Kind) ![]u8 {
    return switch (kind) {
        .head => try headPathAlloc(alloc, root_dir, namespace),
        .gc_watermark => try std.fs.path.join(alloc, &.{ root_dir, namespace, "GC_WATERMARK" }),
        .enrichment_head_version => try std.fs.path.join(alloc, &.{ root_dir, namespace, "ENRICHMENT_HEAD_VERSION" }),
        .enrichment_stage => try std.fs.path.join(alloc, &.{ root_dir, namespace, "ENRICHMENT_STAGE" }),
        .enrichment_doc_offset => try std.fs.path.join(alloc, &.{ root_dir, namespace, "ENRICHMENT_DOC_OFFSET" }),
    };
}

fn stageLeaf(stage: catalog_types.EnrichmentStage) []const u8 {
    return switch (stage) {
        .lexical_sparse => "LEXICAL_SPARSE",
        .chunk_preview => "CHUNK_PREVIEW",
        .chunk_embeddings => "CHUNK_EMBEDDINGS",
        .rerank_terms => "RERANK_TERMS",
    };
}

fn stagePathAlloc(alloc: Allocator, root_dir: []const u8, namespace: []const u8, stage: catalog_types.EnrichmentStage, leaf: []const u8) ![]u8 {
    return try std.fs.path.join(alloc, &.{ root_dir, namespace, "ENRICHMENT", stageLeaf(stage), leaf });
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
    const slice = std.fmt.bufPrint(buf, "/tmp/antfly-serverless-progress-{s}-{d}-{d}\x00", .{
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

test "fs progress store manages head and gc watermark with CAS" {
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf, "cas");
    defer cleanupTmp(path);

    var fs = try FsProgressStore.init(std.testing.allocator, std.mem.span(path));
    defer fs.deinit();

    try std.testing.expect(try fs.compareAndSwapHead("docs", null, 1));
    try std.testing.expectEqual(@as(u64, 1), try fs.getHead("docs"));
    try std.testing.expect(!(try fs.compareAndSwapHead("docs", null, 2)));
    try std.testing.expect(try fs.compareAndSwapHead("docs", 1, 2));

    try std.testing.expectEqual(@as(?u64, null), try fs.getGcWatermark("docs"));
    try std.testing.expect(try fs.compareAndSwapGcWatermark("docs", null, 10));
    try std.testing.expectEqual(@as(?u64, 10), try fs.getGcWatermark("docs"));
    try std.testing.expect(!(try fs.compareAndSwapGcWatermark("docs", null, 11)));
    try std.testing.expect(!(try fs.compareAndSwapGcWatermark("docs", 10, 9)));
    try std.testing.expect(try fs.compareAndSwapGcWatermark("docs", 10, 12));
    try std.testing.expectEqual(@as(?u64, 12), try fs.getGcWatermark("docs"));

    try std.testing.expectEqual(@as(?u64, null), try fs.getEnrichmentHeadVersion("docs"));
    try std.testing.expect(try fs.compareAndSwapEnrichmentHeadVersion("docs", null, 3));
    try std.testing.expectEqual(@as(?u64, 3), try fs.getEnrichmentHeadVersion("docs"));
    try std.testing.expectEqual(@as(?u64, null), try fs.getEnrichmentStage("docs"));
    try std.testing.expect(try fs.compareAndSwapEnrichmentStage("docs", null, 1));
    try std.testing.expectEqual(@as(?u64, 1), try fs.getEnrichmentStage("docs"));
    try std.testing.expect(try fs.compareAndSwapEnrichmentDocOffset("docs", null, 42));
    try std.testing.expectEqual(@as(?u64, 42), try fs.getEnrichmentDocOffset("docs"));
    try std.testing.expect(try fs.compareAndSwapEnrichmentStageHeadVersion("docs", .chunk_embeddings, null, 7));
    try std.testing.expectEqual(@as(?u64, 7), try fs.getEnrichmentStageHeadVersion("docs", .chunk_embeddings));
    try std.testing.expect(try fs.compareAndSwapEnrichmentStageDocOffset("docs", .chunk_embeddings, null, 99));
    try std.testing.expectEqual(@as(?u64, 99), try fs.getEnrichmentStageDocOffset("docs", .chunk_embeddings));
}

test "fs progress store allows exactly one concurrent head CAS winner" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf, "cas-race");
    defer cleanupTmp(path);

    var fs = try FsProgressStore.init(alloc, std.mem.span(path));
    defer fs.deinit();
    try std.testing.expect(try fs.compareAndSwapHead("docs", null, 1));

    const RaceState = struct {
        store: *FsProgressStore,
        winner_count: std.atomic.Value(u32) = .init(0),

        fn race(self: *@This()) void {
            const published = self.store.compareAndSwapHead("docs", 1, 2) catch false;
            if (published) _ = self.winner_count.fetchAdd(1, .monotonic);
        }
    };

    var state = RaceState{ .store = &fs };
    const thread_a = try std.Thread.spawn(.{}, RaceState.race, .{&state});
    const thread_b = try std.Thread.spawn(.{}, RaceState.race, .{&state});
    thread_a.join();
    thread_b.join();

    try std.testing.expectEqual(@as(u32, 1), state.winner_count.load(.monotonic));
    try std.testing.expectEqual(@as(u64, 2), try fs.getHead("docs"));
}
