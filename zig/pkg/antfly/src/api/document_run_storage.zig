// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Document run payloads use the existing artifact backend and scoped upload
//! inventory. A separate durable job namespace owns leases and reachability.
const std = @import("std");
const runs = @import("antfly_readers").document_runs;
const jobs = @import("research_jobs.zig");
const artifacts = @import("../serverless/artifacts/store.zig");
const Cancellation = @import("antfly_cancellation").CancellationToken;
const A = std.mem.Allocator;

pub const Context = struct {
    metadata: *jobs.Store,
    artifacts: artifacts.ArtifactStore,
    io: std.Io,
    cancellation: Cancellation = .{},
    now_fn: *const fn () u64 = jobs.nowMillis,

    pub fn interface(self: *Context) !runs.Store {
        if (self.metadata.cfg.namespace != .documents or !self.metadata.durable()) return error.DocumentStorageNotConfigured;
        return .{ .ptr = self, .load_fn = load, .cas_fn = cas, .nonce_fn = nonce, .put_fn = put, .get_fn = get };
    }
    fn nonce(raw: *anyopaque, bytes: []u8) void {
        const self: *Context = @ptrCast(@alignCast(raw));
        self.io.random(bytes);
    }
    fn load(raw: *anyopaque, a: A, id: []const u8, owner: []const u8) !?runs.Record {
        const self: *Context = @ptrCast(@alignCast(raw));
        try self.cancellation.check();
        const record = (try self.metadata.load(a, id, owner)) orelse return null;
        return try std.json.parseFromSliceLeaky(runs.Record, a, record.request, .{ .allocate = .alloc_always, .duplicate_field_behavior = .@"error" });
    }
    fn cas(raw: *anyopaque, expected: ?runs.Version, next: runs.Record) !bool {
        const self: *Context = @ptrCast(@alignCast(raw));
        try self.cancellation.check();
        return self.metadata.compareDocumentRun(expected, next, self.now_fn);
    }
    pub fn domain(run_id: []const u8, owner: []const u8) [32]u8 {
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update("antfly-document-run-v1");
        for ([_][]const u8{ owner, run_id }) |part| {
            var length: [8]u8 = undefined;
            std.mem.writeInt(u64, &length, @intCast(part.len), .big);
            hash.update(&length);
            hash.update(part);
        }
        return hash.finalResult();
    }
    fn put(raw: *anyopaque, a: A, scope: runs.Scope, bytes: []const u8) !runs.Ref {
        const self: *Context = @ptrCast(@alignCast(raw));
        try self.cancellation.check();
        var scratch = std.heap.ArenaAllocator.init(a);
        defer scratch.deinit();
        const record = (try load(self, scratch.allocator(), scope.run_id, scope.owner)) orelse return error.DocumentRunNotFound;
        if (record.retired or (record.retention_until_ms != 0 and self.now_fn() >= record.retention_until_ms)) return error.DocumentRunExpired;
        if (record.state != .running or record.attempt != scope.attempt or !std.mem.eql(u8, &record.nonce, &scope.nonce) or self.now_fn() >= record.lease_until_ms) return error.DocumentRunLeaseLost;
        var upload = artifacts.UploadScope{ .domain = domain(scope.run_id, scope.owner), .attempt = undefined };
        std.mem.writeInt(u64, upload.attempt[0..8], scope.attempt, .big);
        @memcpy(upload.attempt[8..16], &scope.nonce);
        try upload.validate();
        var backend = self.artifacts;
        backend.allocator = a;
        backend.upload_scope = upload;
        var metadata = try backend.putWithCancellation(bytes, self.cancellation);
        defer metadata.deinit(a);
        var digest: [32]u8 = undefined;
        _ = try std.fmt.hexToBytes(&digest, metadata.checksum);
        return .{ .id = try a.dupe(u8, metadata.artifact_id), .bytes = std.math.cast(usize, metadata.byte_len) orelse return error.DocumentOutputTooLarge, .sha256 = digest };
    }
    pub const Collection = struct { deleted: usize, more: bool };

    /// Called by the maintenance owner, with a bounded payload deletion budget.
    /// Persist the tombstone before touching any artifact. Never delete the
    /// metadata marker: a stale uploader may still finish after this pass.
    pub fn collectExpired(self: *Context, a: A, id: []const u8, owner: []const u8, limit: usize) !Collection {
        if (limit == 0 or limit > 65536) return error.InvalidArtifactEnumerationLimit;
        const store = try self.interface();
        const record = try store.retire(a, id, owner, self.now_fn());
        if (record.attempt == 0) return .{ .deleted = 0, .more = false };
        const cutoff = std.math.add(u64, record.attempt, 1) catch return error.InvalidDocumentCheckpoint;
        const Collector = struct {
            context: *Context,
            domain: [32]u8,
            cutoff: u64,
            deleted: usize = 0,
            fn visit(raw: *anyopaque, scope: artifacts.UploadScope, artifact_id: []const u8) !void {
                const collector: *@This() = @ptrCast(@alignCast(raw));
                if (!std.mem.eql(u8, &scope.domain, &collector.domain) or scope.fencingToken() >= collector.cutoff) return error.InvalidArtifactUploadScope;
                try collector.context.artifacts.deleteWithCancellation(artifact_id, collector.context.cancellation);
                collector.deleted += 1;
            }
        };
        var collector: Collector = .{ .context = self, .domain = domain(id, owner), .cutoff = cutoff };
        var more = false;
        self.artifacts.visitScopedUploads(collector.domain, .{
            .ptr = &collector,
            .visit = Collector.visit,
            .fencing_cutoff = cutoff,
            .max_entries = limit,
            .cleanup_staging = true,
        }, self.cancellation) catch |err| switch (err) {
            error.ArtifactEnumerationPaused => more = true,
            else => return err,
        };
        if (!more) {
            try self.artifacts.cleanupRetiredScopedTemporaries(collector.domain, cutoff, self.cancellation);
            try self.artifacts.reclaimRetiredScopedInventory(collector.domain, 0, cutoff, self.cancellation);
        }
        return .{ .deleted = collector.deleted, .more = more };
    }
    fn get(raw: *anyopaque, a: A, ref: runs.Ref, max_bytes: usize) ![]u8 {
        const self: *Context = @ptrCast(@alignCast(raw));
        if (ref.bytes > max_bytes) return error.DocumentOutputTooLarge;
        var backend = self.artifacts;
        const checksum = std.fmt.bytesToHex(ref.sha256, .lower);
        return backend.getVerifiedAllocWithCancellationUsingAllocator(a, ref.id, ref.bytes, &checksum, self.cancellation);
    }
};

test "document artifact run checkpoints survive closing and reopening the existing stores" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const metadata_path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/document-jobs", .{tmp.sub_path});
    defer a.free(metadata_path);
    const artifact_path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/artifacts", .{tmp.sub_path});
    defer a.free(artifact_path);
    var io_impl = std.Io.Threaded.init(a, .{});
    defer io_impl.deinit();
    const Clock = struct {
        var value: u64 = 1;
        fn now() u64 {
            return value;
        }
    };
    Clock.value = jobs.nowMillis();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const work = arena.allocator();
    const identity: runs.Identity = .{ .request = @splat(1), .sources = @splat(2), .definition = @splat(3), .models = @splat(4), .runtime = @splat(5) };
    var old_lease: runs.Lease = undefined;
    {
        var metadata = jobs.Store.init(a, .{ .namespace = .documents });
        defer metadata.deinit();
        const opened = try a.create(jobs.OpenedStore);
        opened.* = try jobs.OpenedStore.open(a, metadata_path);
        try metadata.attachOpenedStore(opened);
        var files = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, artifact_path);
        defer files.deinit();
        var context = Context{ .metadata = &metadata, .artifacts = files.artifactStore(), .io = io_impl.io(), .now_fn = Clock.now };
        const store = try context.interface();
        try store.create(.{ .run_id = "document-run", .owner = "alice", .identity = identity, .limits = .{ .calls = 10, .tokens = 100, .output_bytes = 1024 }, .inputs = &.{} });
        const original = try store.load(work, "document-run", "alice");
        var drifted = original;
        drifted.revision += 1;
        drifted.identity.runtime = @splat(0);
        try std.testing.expectError(error.InvalidDocumentCheckpoint, metadata.compareDocumentRun(@as(?runs.Version, original.version()), drifted, Clock.now));
        drifted = original;
        drifted.revision += 1;
        drifted.limits.calls += 1;
        try std.testing.expectError(error.InvalidDocumentCheckpoint, metadata.compareDocumentRun(@as(?runs.Version, original.version()), drifted, Clock.now));
        old_lease = try store.claim(work, "document-run", "alice", identity, Clock.value, 10);
        try std.testing.expect(try store.reserve(work, old_lease, @splat(8), .{ .calls = 1, .tokens = 10, .output_bytes = 64 }, Clock.value));
        _ = try store.complete(work, old_lease, @splat(8), .{ .calls = 1, .tokens = 7, .output_bytes = 4 }, "page", Clock.value);
        // Simulate a crash with another dispatch acknowledged, but no output.
        try std.testing.expect(try store.reserve(work, old_lease, @splat(9), .{ .calls = 1, .tokens = 10, .output_bytes = 64 }, Clock.value));
    }
    Clock.value += 12;
    var metadata = jobs.Store.init(a, .{ .namespace = .documents });
    defer metadata.deinit();
    const opened = try a.create(jobs.OpenedStore);
    opened.* = try jobs.OpenedStore.open(a, metadata_path);
    try metadata.attachOpenedStore(opened);
    var files = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, artifact_path);
    defer files.deinit();
    var context = Context{ .metadata = &metadata, .artifacts = files.artifactStore(), .io = io_impl.io(), .now_fn = Clock.now };
    const store = try context.interface();
    try std.testing.expectError(error.DocumentRunNotFound, store.load(work, "document-run", "bob"));
    const record = try store.load(work, "document-run", "alice");
    try std.testing.expectEqual(@as(u64, 2), record.used.calls);
    try std.testing.expectEqual(@as(u64, 17), record.used.tokens);
    var index = try runs.Index.init(a, store, record, 100, 16384);
    defer index.deinit();
    try std.testing.expectEqualStrings("page", try store.read(work, index.get(@splat(8)).?, 4));
    const lease = try store.claim(work, "document-run", "alice", identity, Clock.value, 10);
    try std.testing.expect(!std.mem.eql(u8, &old_lease.nonce, &lease.nonce));
    try std.testing.expectError(error.DocumentRunLeaseLost, store.complete(work, old_lease, @splat(9), .{}, "", Clock.value));
    try std.testing.expect(try store.reserve(work, lease, @splat(9), .{ .calls = 1, .tokens = 10, .output_bytes = 64 }, Clock.value));
    const result = try store.complete(work, lease, @splat(9), .{ .calls = 1, .tokens = 6, .output_bytes = 4 }, "done", Clock.value);
    try store.finish(work, lease, result, Clock.value);
    try std.testing.expectEqual(@as(u64, 3), (try store.load(work, "document-run", "alice")).used.calls);
    try store.create(.{ .run_id = "slow-metadata-transition", .owner = "alice", .identity = identity, .limits = .{ .calls = 1 }, .inputs = &.{} });
    const slow = try store.claim(work, "slow-metadata-transition", "alice", identity, Clock.value, 10);
    const prepared_at = Clock.value;
    Clock.value += 20;
    // An admission prepared while the lease was live cannot commit after it
    // expires, even if no replacement worker has claimed the run yet.
    try std.testing.expectError(error.DocumentRunLeaseLost, store.reserve(work, slow, @splat(11), .{ .calls = 1 }, prepared_at));
    try std.testing.expectEqual(@as(u64, 0), (try store.load(work, "slow-metadata-transition", "alice")).used.calls);
}

test "document artifact run retention collects bounded namespaces after restart and catches late uploads" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const metadata_path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/document-jobs", .{tmp.sub_path});
    defer a.free(metadata_path);
    const artifact_path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/artifacts", .{tmp.sub_path});
    defer a.free(artifact_path);
    var io_impl = std.Io.Threaded.init(a, .{});
    defer io_impl.deinit();
    const Clock = struct {
        var value: u64 = 1;
        fn now() u64 {
            return value;
        }
    };
    Clock.value = jobs.nowMillis();
    const expires = Clock.value + 10;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const work = arena.allocator();
    const identity: runs.Identity = .{ .request = @splat(1), .sources = @splat(2), .definition = @splat(3), .models = @splat(4), .runtime = @splat(5) };
    var upload: artifacts.UploadScope = undefined;
    var accepted: runs.Ref = undefined;
    var other: runs.Ref = undefined;
    {
        var metadata = jobs.Store.init(a, .{ .namespace = .documents, .retention_ms = 0 });
        defer metadata.deinit();
        const opened = try a.create(jobs.OpenedStore);
        opened.* = try jobs.OpenedStore.open(a, metadata_path);
        try metadata.attachOpenedStore(opened);
        var files = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, artifact_path);
        defer files.deinit();
        var context = Context{ .metadata = &metadata, .artifacts = files.artifactStore(), .io = io_impl.io(), .now_fn = Clock.now };
        const store = try context.interface();
        try store.create(.{ .run_id = "expiring", .owner = "alice", .identity = identity, .retention_until_ms = expires, .limits = .{ .calls = 10, .output_bytes = 1024 }, .inputs = &.{} });
        try store.create(.{ .run_id = "keep", .owner = "bob", .identity = identity, .retention_until_ms = expires + 1000, .limits = .{ .calls = 10, .output_bytes = 1024 }, .inputs = &.{} });
        const lease = try store.claim(work, "expiring", "alice", identity, Clock.value, 1000);
        upload = .{ .domain = Context.domain(lease.run_id, lease.owner), .attempt = undefined };
        std.mem.writeInt(u64, upload.attempt[0..8], lease.attempt, .big);
        @memcpy(upload.attempt[8..], &lease.nonce);
        try std.testing.expect(try store.reserve(work, lease, @splat(1), .{ .calls = 1, .output_bytes = 100 }, Clock.value));
        accepted = try store.complete(work, lease, @splat(1), .{ .calls = 1, .output_bytes = 8 }, "accepted", Clock.value);
        try store.finish(work, lease, accepted, Clock.value);
        const keep = try store.claim(work, "keep", "bob", identity, Clock.value, 1000);
        try std.testing.expect(try store.reserve(work, keep, @splat(2), .{ .calls = 1, .output_bytes = 100 }, Clock.value));
        other = try store.complete(work, keep, @splat(2), .{ .calls = 1, .output_bytes = 8 }, "keep me!", Clock.value);
        try std.testing.expectError(error.DocumentRunRetentionActive, context.collectExpired(work, "expiring", "alice", 1));
        Clock.value = expires;
        try std.testing.expectError(error.DocumentRunNotFound, context.collectExpired(work, "expiring", "bob", 1));
        const first = try context.collectExpired(work, "expiring", "alice", 1);
        try std.testing.expectEqual(@as(usize, 1), first.deleted);
        try std.testing.expect(first.more);
        // Generic job cleanup must never forget the fenced artifact root.
        metadata.cleanupExpiredJobs();
        try std.testing.expectError(error.DocumentRunExpired, store.load(work, "expiring", "alice"));
    }
    {
        var metadata = jobs.Store.init(a, .{ .namespace = .documents, .retention_ms = 0 });
        defer metadata.deinit();
        const opened = try a.create(jobs.OpenedStore);
        opened.* = try jobs.OpenedStore.open(a, metadata_path);
        try metadata.attachOpenedStore(opened);
        var files = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, artifact_path);
        defer files.deinit();
        var context = Context{ .metadata = &metadata, .artifacts = files.artifactStore(), .io = io_impl.io(), .now_fn = Clock.now };
        const store = try context.interface();
        const rest = try context.collectExpired(work, "expiring", "alice", 1);
        try std.testing.expectEqual(@as(usize, 1), rest.deleted);
        try std.testing.expect(!rest.more);
        try std.testing.expectError(error.FileNotFound, store.read(work, accepted, 8));
        try std.testing.expectEqualStrings("keep me!", try store.read(work, other, 8));
        // Simulate an uploader already beyond its last authority check when
        // the owner expired. Tombstone retry still finds and collects it.
        var late = try context.artifacts.putScoped(upload, "late orphan", .none);
        defer late.deinit(a);
        try std.testing.expectEqual(@as(usize, 1), (try context.collectExpired(work, "expiring", "alice", 1)).deleted);
        try std.testing.expectError(error.DocumentRunExpired, store.claim(work, "expiring", "alice", identity, Clock.value, 1000));
    }
}
