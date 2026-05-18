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
const objectstore = @import("objectstore");
const catalog_types = @import("types.zig");
const progress_store = @import("progress_store.zig");
const remote_uri = @import("../remote_uri.zig");

pub const ObjectProgressStore = struct {
    alloc: std.mem.Allocator,
    client: objectstore.Client,
    fs_client: ?*objectstore.FilesystemClient = null,
    gcs_client: ?*objectstore.Gcs.JsonApiClient = null,
    s3_client: ?*objectstore.S3.Client = null,
    owns_client: bool = true,
    bucket: []u8,
    prefix: []u8,
    mutex: std.atomic.Mutex = .unlocked,

    pub fn initRemoteUri(alloc: std.mem.Allocator, uri: []const u8) !ObjectProgressStore {
        var parsed = try remote_uri.parseAlloc(alloc, uri);
        defer switch (parsed) {
            .file => |value| alloc.free(value),
            .gcs => |*value| value.deinit(alloc),
            .s3 => |*value| value.deinit(alloc),
        };

        return switch (parsed) {
            .file => |path| blk: {
                const file_uri = try std.fmt.allocPrint(alloc, "file://{s}", .{path});
                defer alloc.free(file_uri);
                break :blk try initFileUri(alloc, file_uri);
            },
            .gcs => |value| try initGcsUri(alloc, value.bucket, value.prefix),
            .s3 => |value| try initS3Uri(alloc, value.bucket, value.prefix),
        };
    }

    pub fn initFileUri(alloc: std.mem.Allocator, uri: []const u8) !ObjectProgressStore {
        const path = try remote_uri.filePathFromUriAlloc(alloc, uri);
        defer alloc.free(path);
        const fs = try alloc.create(objectstore.FilesystemClient);
        errdefer alloc.destroy(fs);
        fs.* = try objectstore.FilesystemClient.init(alloc, path);

        var owned_client = fs.client();
        if (!(try owned_client.bucketExists("serverless-progress"))) try owned_client.makeBucket("serverless-progress");
        return .{
            .alloc = alloc,
            .client = owned_client,
            .fs_client = fs,
            .bucket = try alloc.dupe(u8, "serverless-progress"),
            .prefix = try alloc.dupe(u8, ""),
        };
    }

    pub fn initGcsUri(alloc: std.mem.Allocator, bucket: []const u8, prefix: []const u8) !ObjectProgressStore {
        const gcs = try alloc.create(objectstore.Gcs.JsonApiClient);
        errdefer alloc.destroy(gcs);
        const cfg = try objectstore.Gcs.jsonApiClientConfigFromEnvAlloc(alloc);
        gcs.* = try objectstore.Gcs.JsonApiClient.init(alloc, cfg);

        var owned_client = gcs.client();
        if (!(try owned_client.bucketExists(bucket))) try owned_client.makeBucket(bucket);
        return .{
            .alloc = alloc,
            .client = owned_client,
            .gcs_client = gcs,
            .bucket = try alloc.dupe(u8, bucket),
            .prefix = try alloc.dupe(u8, prefix),
        };
    }

    pub fn initS3Uri(alloc: std.mem.Allocator, bucket: []const u8, prefix: []const u8) !ObjectProgressStore {
        const s3 = try alloc.create(objectstore.S3.Client);
        errdefer alloc.destroy(s3);
        const cfg = try objectstore.S3.fromEnvAlloc(alloc, null, true, null, null, null, null, .path);
        s3.* = try objectstore.S3.Client.init(alloc, cfg);

        var owned_client = s3.client();
        if (!(try owned_client.bucketExists(bucket))) try owned_client.makeBucket(bucket);
        return .{
            .alloc = alloc,
            .client = owned_client,
            .s3_client = s3,
            .bucket = try alloc.dupe(u8, bucket),
            .prefix = try alloc.dupe(u8, prefix),
        };
    }

    pub fn initWithClient(alloc: std.mem.Allocator, client: objectstore.Client, bucket: []const u8, prefix: []const u8) !ObjectProgressStore {
        var owned_client = client;
        if (!(try owned_client.bucketExists(bucket))) try owned_client.makeBucket(bucket);
        return .{
            .alloc = alloc,
            .client = owned_client,
            .owns_client = false,
            .bucket = try alloc.dupe(u8, bucket),
            .prefix = try alloc.dupe(u8, prefix),
        };
    }

    pub fn deinit(self: *ObjectProgressStore) void {
        if (self.owns_client) self.client.deinit();
        if (self.fs_client) |fs| self.alloc.destroy(fs);
        if (self.gcs_client) |gcs| self.alloc.destroy(gcs);
        if (self.s3_client) |s3| self.alloc.destroy(s3);
        self.alloc.free(self.bucket);
        self.alloc.free(self.prefix);
        self.* = undefined;
    }

    pub fn progressStore(self: *ObjectProgressStore) progress_store.ProgressStore {
        return .{
            .allocator = self.alloc,
            .ptr = self,
            .vtable = &vtable,
        };
    }

    pub fn getHead(self: *ObjectProgressStore, namespace: []const u8) !u64 {
        const key = try keyAlloc(self.alloc, self.prefix, namespace, "HEAD");
        defer self.alloc.free(key);
        return try self.readValue(key);
    }

    pub fn compareAndSwapHead(self: *ObjectProgressStore, namespace: []const u8, expected: ?u64, version: u64) !bool {
        const key = try keyAlloc(self.alloc, self.prefix, namespace, "HEAD");
        defer self.alloc.free(key);
        return try self.compareAndSwap(key, expected, version);
    }

    pub fn getGcWatermark(self: *ObjectProgressStore, namespace: []const u8) !?u64 {
        const key = try keyAlloc(self.alloc, self.prefix, namespace, "GC_WATERMARK");
        defer self.alloc.free(key);
        return self.tryReadValue(key);
    }

    pub fn compareAndSwapGcWatermark(self: *ObjectProgressStore, namespace: []const u8, expected: ?u64, watermark: u64) !bool {
        const key = try keyAlloc(self.alloc, self.prefix, namespace, "GC_WATERMARK");
        defer self.alloc.free(key);
        if (expected) |current| {
            if (watermark < current) return false;
        }
        return try self.compareAndSwap(key, expected, watermark);
    }

    pub fn getEnrichmentHeadVersion(self: *ObjectProgressStore, namespace: []const u8) !?u64 {
        const key = try keyAlloc(self.alloc, self.prefix, namespace, "ENRICHMENT_HEAD_VERSION");
        defer self.alloc.free(key);
        return self.tryReadValue(key);
    }

    pub fn compareAndSwapEnrichmentHeadVersion(self: *ObjectProgressStore, namespace: []const u8, expected: ?u64, head_version: u64) !bool {
        const key = try keyAlloc(self.alloc, self.prefix, namespace, "ENRICHMENT_HEAD_VERSION");
        defer self.alloc.free(key);
        return try self.compareAndSwap(key, expected, head_version);
    }

    pub fn getEnrichmentStage(self: *ObjectProgressStore, namespace: []const u8) !?u64 {
        const key = try keyAlloc(self.alloc, self.prefix, namespace, "ENRICHMENT_STAGE");
        defer self.alloc.free(key);
        return self.tryReadValue(key);
    }

    pub fn compareAndSwapEnrichmentStage(self: *ObjectProgressStore, namespace: []const u8, expected: ?u64, stage: u64) !bool {
        const key = try keyAlloc(self.alloc, self.prefix, namespace, "ENRICHMENT_STAGE");
        defer self.alloc.free(key);
        return try self.compareAndSwap(key, expected, stage);
    }

    pub fn getEnrichmentDocOffset(self: *ObjectProgressStore, namespace: []const u8) !?u64 {
        const key = try keyAlloc(self.alloc, self.prefix, namespace, "ENRICHMENT_DOC_OFFSET");
        defer self.alloc.free(key);
        return self.tryReadValue(key);
    }

    pub fn compareAndSwapEnrichmentDocOffset(self: *ObjectProgressStore, namespace: []const u8, expected: ?u64, doc_offset: u64) !bool {
        const key = try keyAlloc(self.alloc, self.prefix, namespace, "ENRICHMENT_DOC_OFFSET");
        defer self.alloc.free(key);
        return try self.compareAndSwap(key, expected, doc_offset);
    }

    pub fn getEnrichmentStageHeadVersion(self: *ObjectProgressStore, namespace: []const u8, stage: catalog_types.EnrichmentStage) !?u64 {
        const key = try enrichmentStageKeyAlloc(self.alloc, self.prefix, namespace, stage, "HEAD_VERSION");
        defer self.alloc.free(key);
        return self.tryReadValue(key);
    }

    pub fn compareAndSwapEnrichmentStageHeadVersion(
        self: *ObjectProgressStore,
        namespace: []const u8,
        stage: catalog_types.EnrichmentStage,
        expected: ?u64,
        head_version: u64,
    ) !bool {
        const key = try enrichmentStageKeyAlloc(self.alloc, self.prefix, namespace, stage, "HEAD_VERSION");
        defer self.alloc.free(key);
        return try self.compareAndSwap(key, expected, head_version);
    }

    pub fn getEnrichmentStageDocOffset(self: *ObjectProgressStore, namespace: []const u8, stage: catalog_types.EnrichmentStage) !?u64 {
        const key = try enrichmentStageKeyAlloc(self.alloc, self.prefix, namespace, stage, "DOC_OFFSET");
        defer self.alloc.free(key);
        return self.tryReadValue(key);
    }

    pub fn compareAndSwapEnrichmentStageDocOffset(
        self: *ObjectProgressStore,
        namespace: []const u8,
        stage: catalog_types.EnrichmentStage,
        expected: ?u64,
        doc_offset: u64,
    ) !bool {
        const key = try enrichmentStageKeyAlloc(self.alloc, self.prefix, namespace, stage, "DOC_OFFSET");
        defer self.alloc.free(key);
        return try self.compareAndSwap(key, expected, doc_offset);
    }

    fn compareAndSwap(self: *ObjectProgressStore, key: []const u8, expected: ?u64, value: u64) !bool {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();

        const current = try self.tryReadCurrent(self.alloc, key);
        defer if (current) |*entry| if (entry.etag) |etag| self.alloc.free(etag);
        if ((if (current) |entry| entry.value else null) != expected) return false;

        const payload = try std.fmt.allocPrint(self.alloc, "{d}", .{value});
        defer self.alloc.free(payload);

        var result = self.client.putObject(self.bucket, key, payload, .{
            .content_type = "text/plain",
            .if_none_match = current == null,
            .if_match_etag = if (current) |entry| entry.etag else null,
        }) catch |err| switch (err) {
            error.PreconditionFailed => return false,
            else => return err,
        };
        defer result.deinit(self.alloc);
        return true;
    }

    fn readValue(self: *ObjectProgressStore, key: []const u8) !u64 {
        const maybe = try self.tryReadValue(key);
        return maybe orelse error.FileNotFound;
    }

    fn tryReadValue(self: *ObjectProgressStore, key: []const u8) !?u64 {
        const current = try self.tryReadCurrent(self.alloc, key);
        defer if (current) |*entry| if (entry.etag) |etag| self.alloc.free(etag);
        return if (current) |entry| entry.value else null;
    }

    const CurrentValue = struct {
        value: u64,
        etag: ?[]u8,
    };

    fn tryReadCurrent(self: *ObjectProgressStore, alloc: std.mem.Allocator, key: []const u8) !?CurrentValue {
        var result = self.client.getObject(self.bucket, key, .{}) catch |err| switch (err) {
            error.FileNotFound => return null,
            else => return err,
        };
        defer result.deinit(alloc);
        return .{
            .value = try std.fmt.parseInt(u64, std.mem.trim(u8, result.body, " \t\r\n"), 10),
            .etag = if (result.metadata.etag) |value| try alloc.dupe(u8, value) else null,
        };
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

    fn erasedDeinit(_: std.mem.Allocator, ptr: *anyopaque) void {
        const self: *ObjectProgressStore = @ptrCast(@alignCast(ptr));
        self.deinit();
    }

    fn erasedGetHead(ptr: *anyopaque, namespace: []const u8) !u64 {
        const self: *ObjectProgressStore = @ptrCast(@alignCast(ptr));
        return try self.getHead(namespace);
    }

    fn erasedCompareAndSwapHead(ptr: *anyopaque, namespace: []const u8, expected: ?u64, version: u64) !bool {
        const self: *ObjectProgressStore = @ptrCast(@alignCast(ptr));
        return try self.compareAndSwapHead(namespace, expected, version);
    }

    fn erasedGetGcWatermark(ptr: *anyopaque, namespace: []const u8) !?u64 {
        const self: *ObjectProgressStore = @ptrCast(@alignCast(ptr));
        return try self.getGcWatermark(namespace);
    }

    fn erasedCompareAndSwapGcWatermark(ptr: *anyopaque, namespace: []const u8, expected: ?u64, watermark: u64) !bool {
        const self: *ObjectProgressStore = @ptrCast(@alignCast(ptr));
        return try self.compareAndSwapGcWatermark(namespace, expected, watermark);
    }

    fn erasedGetEnrichmentHeadVersion(ptr: *anyopaque, namespace: []const u8) !?u64 {
        const self: *ObjectProgressStore = @ptrCast(@alignCast(ptr));
        return try self.getEnrichmentHeadVersion(namespace);
    }

    fn erasedCompareAndSwapEnrichmentHeadVersion(ptr: *anyopaque, namespace: []const u8, expected: ?u64, head_version: u64) !bool {
        const self: *ObjectProgressStore = @ptrCast(@alignCast(ptr));
        return try self.compareAndSwapEnrichmentHeadVersion(namespace, expected, head_version);
    }

    fn erasedGetEnrichmentStage(ptr: *anyopaque, namespace: []const u8) !?u64 {
        const self: *ObjectProgressStore = @ptrCast(@alignCast(ptr));
        return try self.getEnrichmentStage(namespace);
    }

    fn erasedCompareAndSwapEnrichmentStage(ptr: *anyopaque, namespace: []const u8, expected: ?u64, stage: u64) !bool {
        const self: *ObjectProgressStore = @ptrCast(@alignCast(ptr));
        return try self.compareAndSwapEnrichmentStage(namespace, expected, stage);
    }

    fn erasedGetEnrichmentDocOffset(ptr: *anyopaque, namespace: []const u8) !?u64 {
        const self: *ObjectProgressStore = @ptrCast(@alignCast(ptr));
        return try self.getEnrichmentDocOffset(namespace);
    }

    fn erasedCompareAndSwapEnrichmentDocOffset(ptr: *anyopaque, namespace: []const u8, expected: ?u64, doc_offset: u64) !bool {
        const self: *ObjectProgressStore = @ptrCast(@alignCast(ptr));
        return try self.compareAndSwapEnrichmentDocOffset(namespace, expected, doc_offset);
    }

    fn erasedGetEnrichmentStageHeadVersion(ptr: *anyopaque, namespace: []const u8, stage_id: u8) !?u64 {
        const self: *ObjectProgressStore = @ptrCast(@alignCast(ptr));
        return try self.getEnrichmentStageHeadVersion(namespace, @enumFromInt(stage_id));
    }

    fn erasedCompareAndSwapEnrichmentStageHeadVersion(
        ptr: *anyopaque,
        namespace: []const u8,
        stage_id: u8,
        expected: ?u64,
        head_version: u64,
    ) !bool {
        const self: *ObjectProgressStore = @ptrCast(@alignCast(ptr));
        return try self.compareAndSwapEnrichmentStageHeadVersion(namespace, @enumFromInt(stage_id), expected, head_version);
    }

    fn erasedGetEnrichmentStageDocOffset(ptr: *anyopaque, namespace: []const u8, stage_id: u8) !?u64 {
        const self: *ObjectProgressStore = @ptrCast(@alignCast(ptr));
        return try self.getEnrichmentStageDocOffset(namespace, @enumFromInt(stage_id));
    }

    fn erasedCompareAndSwapEnrichmentStageDocOffset(
        ptr: *anyopaque,
        namespace: []const u8,
        stage_id: u8,
        expected: ?u64,
        doc_offset: u64,
    ) !bool {
        const self: *ObjectProgressStore = @ptrCast(@alignCast(ptr));
        return try self.compareAndSwapEnrichmentStageDocOffset(namespace, @enumFromInt(stage_id), expected, doc_offset);
    }
};

fn keyAlloc(alloc: std.mem.Allocator, prefix: []const u8, namespace: []const u8, suffix: []const u8) ![]u8 {
    if (prefix.len == 0) return try std.fmt.allocPrint(alloc, "{s}/{s}", .{ namespace, suffix });
    return try std.fmt.allocPrint(alloc, "{s}/{s}/{s}", .{ prefix, namespace, suffix });
}

fn stageLeaf(stage: catalog_types.EnrichmentStage) []const u8 {
    return switch (stage) {
        .lexical_sparse => "LEXICAL_SPARSE",
        .chunk_preview => "CHUNK_PREVIEW",
        .chunk_embeddings => "CHUNK_EMBEDDINGS",
        .rerank_terms => "RERANK_TERMS",
    };
}

fn enrichmentStageKeyAlloc(
    alloc: std.mem.Allocator,
    prefix: []const u8,
    namespace: []const u8,
    stage: catalog_types.EnrichmentStage,
    suffix: []const u8,
) ![]u8 {
    if (prefix.len == 0) return try std.fmt.allocPrint(alloc, "{s}/ENRICHMENT/{s}/{s}", .{ namespace, stageLeaf(stage), suffix });
    return try std.fmt.allocPrint(alloc, "{s}/{s}/ENRICHMENT/{s}/{s}", .{ prefix, namespace, stageLeaf(stage), suffix });
}

fn lockAtomic(mutex: *std.atomic.Mutex) void {
    while (!mutex.tryLock()) std.atomic.spinLoopHint();
}

test "objectstore-backed progress store supports cas over file uri" {
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf, "progress");
    defer cleanupTmp(path);

    const uri = try std.fmt.allocPrint(std.testing.allocator, "file://{s}", .{std.mem.span(path)});
    defer std.testing.allocator.free(uri);

    var impl = try ObjectProgressStore.initFileUri(std.testing.allocator, uri);
    var store = impl.progressStore();
    defer store.deinit();

    try std.testing.expect(try store.compareAndSwapHead("docs", null, 1));
    try std.testing.expectEqual(@as(u64, 1), try store.getHead("docs"));
    try std.testing.expect(try store.compareAndSwapGcWatermark("docs", null, 10));
    try std.testing.expect(try store.compareAndSwapEnrichmentHeadVersion("docs", null, 1));
    try std.testing.expect(try store.compareAndSwapEnrichmentStage("docs", null, 2));
    try std.testing.expect(try store.compareAndSwapEnrichmentDocOffset("docs", null, 3));
    try std.testing.expect(try store.compareAndSwapEnrichmentStageHeadVersion("docs", .chunk_embeddings, null, 4));
    try std.testing.expect(try store.compareAndSwapEnrichmentStageDocOffset("docs", .chunk_embeddings, null, 5));
}

var test_nonce: std.atomic.Value(u64) = .init(0);

fn threadedIo() std.Io.Threaded {
    return std.Io.Threaded.init(std.heap.page_allocator, .{});
}

fn nowNs() u64 {
    var io_impl = threadedIo();
    defer io_impl.deinit();
    const now = std.Io.Timestamp.now(io_impl.io(), .awake);
    return @intCast(now.toNanoseconds());
}

fn tmpPath(buf: []u8, label: []const u8) [*:0]const u8 {
    const nonce = test_nonce.fetchAdd(1, .monotonic);
    const slice = std.fmt.bufPrint(buf, "/tmp/antfly-serverless-object-progress-{s}-{d}-{d}\x00", .{ label, nowNs(), nonce }) catch unreachable;
    return @ptrCast(slice.ptr);
}

fn cleanupTmp(path: [*:0]const u8) void {
    var io_impl = threadedIo();
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), std.mem.span(path)) catch {};
}
