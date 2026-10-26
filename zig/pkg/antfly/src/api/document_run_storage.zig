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
    leadership_term: u64 = 0,
    cancellation: Cancellation = .{},
    now_fn: *const fn () u64 = jobs.nowMillis,
    authority: ?struct { ptr: *anyopaque, check_fn: *const fn (*anyopaque) anyerror!void } = null,
    fn checkAuthority(self: *Context) !void {
        if (self.authority) |owner| try owner.check_fn(owner.ptr);
    }

    pub fn interface(self: *Context) !runs.Store {
        if (self.metadata.cfg.namespace != .documents or !self.metadata.durable()) return error.DocumentStorageNotConfigured;
        return .{ .ptr = self, .load_fn = load, .cas_fn = cas, .nonce_fn = nonce, .put_fn = put, .get_fn = get };
    }
    pub const Lifetime = struct { ptr: *anyopaque, release_fn: *const fn (*anyopaque) void };
    const Bound = struct {
        allocator: A,
        context: Context,
        id: []const u8,
        owner: []const u8,
        domain: [32]u8,
        lifetime: ?Lifetime,
        fn authorize(raw: *anyopaque, reference: runs.Ref) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const scope = (try artifacts.uploadScopeFromArtifactId(reference.id)) orelse return error.InvalidDocumentCheckpoint;
            if (!std.mem.eql(u8, &scope.domain, &self.domain)) return error.DocumentRunNotFound;
            var arena = std.heap.ArenaAllocator.init(self.allocator);
            defer arena.deinit();
            const record = try (try self.context.interface()).loadAt(arena.allocator(), self.id, self.owner, self.context.now_fn());
            if (scope.fencingToken() > record.attempt) return error.InvalidDocumentCheckpoint;
        }
        fn release(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const a = self.allocator;
            const parent = self.lifetime;
            a.free(self.id);
            a.free(self.owner);
            a.destroy(self);
            if (parent) |lifetime| lifetime.release_fn(lifetime.ptr);
        }
    };
    /// Transfer `lifetime` only on success. The caller's metadata/artifact
    /// owner must remain live until the last invocation/response releases it.
    /// Preserve cancellation only when its owner is protected by the transferred
    /// lifetime. A borrowed HTTP token may expire while reverse RPC is draining.
    pub fn bind(self: *Context, a: A, id: []const u8, owner: []const u8, lifetime: ?Lifetime) !*@import("antfly_readers").document_rpc.OwnedServer {
        const binding = try a.create(Bound);
        errdefer a.destroy(binding);
        const saved_id = try a.dupe(u8, id);
        errdefer a.free(saved_id);
        const saved_owner = try a.dupe(u8, owner);
        errdefer a.free(saved_owner);
        binding.* = .{ .allocator = a, .context = self.*, .id = saved_id, .owner = saved_owner, .domain = domain(id, owner), .lifetime = lifetime };
        if (lifetime == null or self.cancellation.ptr != lifetime.?.ptr) binding.context.cancellation = .none;
        return @import("antfly_readers").document_rpc.OwnedServer.create(a, .{
            .store = try binding.context.interface(),
            .id = id,
            .owner = owner,
            .authorization = .{ .ptr = binding, .reference_fn = Bound.authorize },
        }, .{ .ptr = binding, .release_fn = Bound.release });
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
        try self.checkAuthority();
        return self.metadata.compareDocumentRunInTerm(expected, next, self.now_fn, self.leadership_term);
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
        try self.checkAuthority();
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
        try self.checkAuthority();
        const store = try self.interface();
        const record = try store.retire(a, id, owner, self.now_fn());
        if (record.attempt == 0) {
            try self.metadata.deferDocumentCollection(id, owner, self.leadership_term, self.now_fn() +| 60_000);
            return .{ .deleted = 0, .more = false };
        }
        const cutoff = std.math.add(u64, record.attempt, 1) catch return error.InvalidDocumentCheckpoint;
        const Collector = struct {
            context: *Context,
            domain: [32]u8,
            cutoff: u64,
            deleted: usize = 0,
            fn visit(raw: *anyopaque, scope: artifacts.UploadScope, artifact_id: []const u8) !void {
                const collector: *@This() = @ptrCast(@alignCast(raw));
                if (!std.mem.eql(u8, &scope.domain, &collector.domain) or scope.fencingToken() >= collector.cutoff) return error.InvalidArtifactUploadScope;
                try collector.context.checkAuthority();
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
            try self.checkAuthority();
            try self.artifacts.cleanupRetiredScopedTemporaries(collector.domain, cutoff, self.cancellation);
            try self.artifacts.reclaimRetiredScopedInventory(collector.domain, 0, cutoff, self.cancellation);
            try self.metadata.deferDocumentCollection(id, owner, self.leadership_term, self.now_fn() +| 60_000);
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
        try store.create(.{ .run_id = "empty", .owner = "alice", .identity = identity, .retention_until_ms = expires, .limits = .{ .calls = 10, .output_bytes = 1024 }, .inputs = &.{} });
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
        try std.testing.expectEqual(@as(usize, 0), (try context.collectExpired(work, "empty", "alice", 1)).deleted);
        const empty = (try metadata.load(work, "empty", "alice")).?;
        try std.testing.expectEqual(Clock.value + 60_000, empty.maintenance_not_before_ms);
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

test "document artifact run native byte capability scopes reads and preserves checkpoint errors" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const metadata_path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/metadata", .{tmp.sub_path});
    defer a.free(metadata_path);
    const artifact_path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/artifacts", .{tmp.sub_path});
    defer a.free(artifact_path);
    var io_impl = std.Io.Threaded.init(a, .{});
    defer io_impl.deinit();
    var metadata = jobs.Store.init(a, .{ .namespace = .documents });
    defer metadata.deinit();
    const opened = try a.create(jobs.OpenedStore);
    opened.* = try jobs.OpenedStore.open(a, metadata_path);
    try metadata.attachOpenedStore(opened);
    var files = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, artifact_path);
    defer files.deinit();
    var context = Context{ .metadata = &metadata, .artifacts = files.artifactStore(), .io = io_impl.io() };
    var borrowed_cancelled: std.atomic.Value(bool) = .init(true);
    context.cancellation = Cancellation.fromAtomic(&borrowed_cancelled);
    const binding = try context.bind(a, "rpc-run", "alice", null);
    defer binding.view().release();
    var client = try @import("antfly_readers").document_rpc.Client.initBound(a, binding.view(), io_impl.io());
    defer client.deinit();
    const store = client.store();
    const identity: runs.Identity = .{ .request = @splat(1), .sources = @splat(2), .definition = @splat(3), .models = @splat(4), .runtime = @splat(5) };
    try store.create(.{ .run_id = "rpc-run", .owner = "alice", .identity = identity, .limits = .{ .calls = 3, .output_bytes = 1024 }, .inputs = &.{} });
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const work = arena.allocator();
    const now = jobs.nowMillis();
    const lease = try store.claim(work, "rpc-run", "alice", identity, now, 30000);
    try std.testing.expectError(error.DocumentRunNotFound, store.load(work, "rpc-run", "bob"));
    try std.testing.expect(try store.reserve(work, lease, @splat(8), .{ .calls = 1, .output_bytes = 100 }, now));
    const ref = try store.complete(work, lease, @splat(8), .{ .calls = 1, .output_bytes = 4 }, "done", now);
    try std.testing.expectEqualStrings("done", try store.read(work, ref, 100));
    var unrelated = try context.artifacts.putScoped(try artifacts.UploadScope.forPublication(@splat(9), 1, io_impl.io()), "other owner", .none);
    defer unrelated.deinit(a);
    var digest: [32]u8 = undefined;
    _ = try std.fmt.hexToBytes(&digest, unrelated.checksum);
    try std.testing.expectError(error.DocumentRunNotFound, store.read(work, .{ .id = unrelated.artifact_id, .bytes = 11, .sha256 = digest }, 100));
    try store.finish(work, lease, ref, now);
    try std.testing.expectError(error.DocumentRunTerminal, store.claim(work, "rpc-run", "alice", identity, now, 100));
    const Owner = struct {
        cancelled: bool = false,
        released: bool = false,
        fn cancelledFn(raw: *const anyopaque) bool {
            return @as(*const @This(), @ptrCast(@alignCast(raw))).cancelled;
        }
        fn release(raw: *anyopaque) void {
            @as(*@This(), @ptrCast(@alignCast(raw))).released = true;
        }
    };
    var owner: Owner = .{};
    context.cancellation = .{ .ptr = &owner, .is_cancelled_fn = Owner.cancelledFn };
    {
        const owned = try context.bind(a, "rpc-run", "alice", .{ .ptr = &owner, .release_fn = Owner.release });
        defer owned.view().release();
        var owned_client = try @import("antfly_readers").document_rpc.Client.initBound(a, owned.view(), io_impl.io());
        defer owned_client.deinit();
        _ = try owned_client.store().load(work, "rpc-run", "alice");
        owner.cancelled = true;
        try std.testing.expectError(error.Canceled, owned_client.store().load(work, "rpc-run", "alice"));
        try std.testing.expect(!owner.released);
    }
    try std.testing.expect(owner.released);
}

// The same transactional backend may be observed through independent API-owner
// mirrors. Fault switches model lost connectivity and an unknown commit receipt.
pub const SharedJobStateTestBackend = struct {
    store: *@import("antfly_local_sources").storage_docstore.DocStore,
    term: u64 = 1,
    partitioned: bool = false,
    lose_receipt: bool = false,
    fn get(raw: *anyopaque, a: A, key: []const u8) !?[]u8 {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (self.partitioned) return error.NotLeader;
        return self.store.get(a, key) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
    }
    fn scan(raw: *anyopaque, a: A, after: ?[]const u8) ![]u8 {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (self.partitioned) return error.NotLeader;
        const contract = @import("../metadata/job_state.zig");
        const page = try self.store.scanPrefixPage(a, "documents/", after, contract.max_page_rows);
        defer @import("antfly_local_sources").storage_docstore.DocStore.freeResults(a, page);
        const rows = try a.alloc(contract.Row, page.len);
        defer a.free(rows);
        for (page, rows) |entry, *row| row.* = .{ .key = entry.key, .value = entry.value };
        return std.json.Stringify.valueAlloc(a, contract.Page{ .rows = rows, .next = if (rows.len == contract.max_page_rows) rows[rows.len - 1].key else null }, .{});
    }
    fn cas(raw: *anyopaque, key: []const u8, expected: ?[]const u8, value: []const u8, term: u64) !bool {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (self.partitioned or self.term != term) return error.NotLeader;
        var txn = try self.store.beginWriteTxn();
        var committed = false;
        defer if (!committed) txn.abort();
        const old = txn.get(key) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        if (old) |bytes| {
            const hash = @import("../metadata/job_state.zig").digest(bytes);
            if (expected == null or !std.mem.eql(u8, &hash, expected.?)) return false;
        } else if (expected != null) return false;
        try txn.put(key, value);
        try txn.commit();
        committed = true;
        if (self.lose_receipt) {
            self.lose_receipt = false;
            return error.NotLeader;
        }
        return true;
    }
    pub fn capability(self: *@This()) @import("../metadata/job_state.zig").Persistence {
        return @import("../metadata/job_state.zig").Persistence.fromLocal(self, .{ .get = get, .scan = scan, .cas = cas });
    }
};

test "document artifact run shared owners recover checkpoints and fence partitioned superseded attempts" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/shared-jobs", .{tmp.sub_path});
    defer a.free(path);
    var backend_owner = try jobs.OpenedStore.open(a, path);
    defer backend_owner.deinit();
    var backend = SharedJobStateTestBackend{ .store = backend_owner.docstore };
    var first = jobs.Store.init(a, .{ .namespace = .documents });
    defer first.deinit();
    var second = jobs.Store.init(a, .{ .namespace = .documents });
    defer second.deinit();
    try first.attachReplicated(backend.capability());
    try second.attachReplicated(backend.capability());
    try first.prepareReplicatedLeadership(1);
    try second.prepareReplicatedLeadership(1);
    const artifact_path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/shared-artifacts", .{tmp.sub_path});
    defer a.free(artifact_path);
    var files = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, artifact_path);
    defer files.deinit();
    var io = std.Io.Threaded.init(a, .{});
    defer io.deinit();
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
    var one = Context{ .metadata = &first, .artifacts = files.artifactStore(), .io = io.io(), .now_fn = Clock.now, .leadership_term = 1 };
    var two = Context{ .metadata = &second, .artifacts = files.artifactStore(), .io = io.io(), .now_fn = Clock.now, .leadership_term = 2 };
    const s1 = try one.interface();
    const s2 = try two.interface();
    const identity: runs.Identity = .{ .request = @splat(1), .sources = @splat(2), .definition = @splat(3), .models = @splat(4), .runtime = @splat(5) };
    try s1.create(.{ .run_id = "shared", .owner = "alice", .identity = identity, .inputs = &.{}, .retention_until_ms = Clock.value + 100, .limits = .{ .calls = 10, .tokens = 100, .output_bytes = 1000 } });
    try std.testing.expectError(error.DocumentRunNotFound, s2.load(work, "shared", "bob"));
    const old = try s1.claim(work, "shared", "alice", identity, Clock.value, 30);
    try std.testing.expect(try s1.reserve(work, old, @splat(1), .{ .calls = 1, .tokens = 7, .output_bytes = 32 }, Clock.value));
    _ = try s1.complete(work, old, @splat(1), .{ .calls = 1, .tokens = 3, .output_bytes = 4 }, "page", Clock.value);
    try std.testing.expect(try s1.reserve(work, old, @splat(2), .{ .calls = 1, .tokens = 9 }, Clock.value));
    backend.term = 2;
    // A stale term cannot publish, even before the per-run lease expires.
    try std.testing.expectError(error.NotLeader, s1.renew(work, old, Clock.value, 30));
    backend.partitioned = true;
    try std.testing.expectError(error.NotLeader, s1.load(work, "shared", "alice"));
    backend.partitioned = false;
    try second.prepareReplicatedLeadership(2);
    Clock.value += 31;
    const replacement = try s2.claim(work, "shared", "alice", identity, Clock.value, 30);
    const record = try s2.load(work, "shared", "alice");
    try std.testing.expectEqual(@as(u64, 12), record.used.tokens);
    try std.testing.expectEqual(@as(u64, 9), record.uncertain.tokens);
    var index = try runs.Index.init(a, s2, record, 100, 16384);
    defer index.deinit();
    try std.testing.expectEqualStrings("page", try s2.read(work, index.get(@splat(1)).?, 4));
    try std.testing.expectError(error.DocumentRunLeaseLost, s1.complete(work, old, @splat(2), .{}, "late", Clock.value));
    // The commit may succeed while its receipt is lost. Fresh reads reconcile
    // that reservation; retry cannot authorize another physical dispatch.
    backend.lose_receipt = true;
    try std.testing.expectError(error.NotLeader, s2.reserve(work, replacement, @splat(3), .{ .calls = 1, .tokens = 5 }, Clock.value));
    const after = try s2.load(work, "shared", "alice");
    try std.testing.expectEqual(@as(u64, 17), after.used.tokens);
    try std.testing.expect(!try s2.reserve(work, replacement, @splat(3), .{ .calls = 1, .tokens = 5 }, Clock.value));
    try std.testing.expectEqual(@as(usize, 0), second.jobs.count());
    // Authoritative inventory is paged even when its history exceeds one pass.
    for (0..65) |number| {
        const id = try std.fmt.allocPrint(work, "paged-{d}", .{number});
        try s2.create(.{ .run_id = id, .owner = "alice", .identity = identity, .inputs = &.{}, .limits = .{} });
    }
    var cursor: usize = 0;
    var visited: usize = 0;
    for (0..66) |_| {
        _ = (try second.nextDocument(work, &cursor)) orelse return error.TestUnexpectedResult;
        visited += 1;
    }
    try std.testing.expectEqual(@as(usize, 66), visited);
}
