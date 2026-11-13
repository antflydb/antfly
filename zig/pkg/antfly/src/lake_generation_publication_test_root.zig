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

//! Compare bounded generation-map mutation policies with real encoded maps.
pub const antfly_sources = @import("source_owner_physical.zig");
const std = @import("std");
const local = @import("antfly_local_sources");
const objects = @import("objectstore");
const maps = @import("serverless/artifacts/cloud_generations.zig");
const stores = @import("serverless/artifacts/store.zig");
const A = std.mem.Allocator;

// Content-addressed fixture keys are immutable. Each accepted key has exactly
// one synthetic provider generation; reads must request that same generation.
const Provider = struct {
    memory: objects.MemoryClient,
    mu: std.atomic.Mutex = .unlocked,
    writes: usize = 0,
    reads: usize = 0,
    written_bytes: usize = 0,
    fn client(self: *@This()) objects.Client {
        return .{ .allocator = self.memory.alloc, .ptr = self, .vtable = &vtable };
    }
    fn lock(self: *@This()) void {
        while (!self.mu.tryLock()) std.atomic.spinLoopHint();
    }
    fn put(raw: *anyopaque, a: A, bucket: []const u8, key: []const u8, bytes: []const u8, opts: objects.PutOptions) !objects.PutResult {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.lock();
        defer self.mu.unlock();
        var client_ = self.memory.client();
        var result = try client_.putObject(bucket, key, bytes, opts);
        result.deinit(client_.allocator);
        self.writes += 1;
        self.written_bytes += bytes.len;
        return .{ .version_id = try a.dupe(u8, "1") };
    }
    fn get(raw: *anyopaque, a: A, bucket: []const u8, key: []const u8, opts: objects.GetOptions) !objects.GetResult {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.lock();
        defer self.mu.unlock();
        if (opts.version_id) |version| if (!std.mem.eql(u8, version, "1")) return error.SourceVersionChanged;
        var client_ = self.memory.client();
        client_.allocator = a;
        var unpinned = opts;
        unpinned.version_id = null;
        var result = try client_.getObject(bucket, key, unpinned);
        errdefer result.deinit(a);
        result.metadata.version_id = try a.dupe(u8, "1");
        self.reads += 1;
        return result;
    }
    fn noop(_: A, _: *anyopaque) void {}
    const vtable: objects.Client.VTable = .{ .deinit = noop, .bucket_exists = undefined, .make_bucket = undefined, .put_object = put, .get_object = get, .stat_object = undefined, .get_object_attributes = undefined, .delete_object = undefined, .list_objects = undefined };
};

test "external lake execution generation publication batching preserves all captured versions" {
    const a = std.testing.allocator;
    var scope: stores.UploadScope = .{ .domain = @splat(31), .attempt = @splat(5) };
    std.mem.writeInt(u64, scope.attempt[0..8], 1, .big);
    try scope.validate();
    var writes: [2]usize = undefined;
    for ([_]maps.BatchPolicy{ .{}, maps.BatchPolicy.bulk }, 0..) |policy, trial| {
        var provider: Provider = .{ .memory = .init(a) };
        defer provider.memory.deinit();
        var memory = provider.memory.client();
        try memory.makeBucket("bucket");
        var quota: local.sql_memory_budget = .{ .backing = a, .limit = 64 * 1024 * 1024 };
        var writer: maps.Writer = .{ .a = quota.allocator(), .reader = .{ .cache_allocator = quota.allocator(), .client = provider.client(), .bucket = "bucket", .prefix = "maps" }, .scope = scope, .batch_policy = policy, .complete = true };
        defer writer.deinit();
        const started = @import("antfly_platform").time.monotonicNs();
        for (0..16384) |ordinal| {
            var encoded: [8]u8 = undefined;
            std.mem.writeInt(u64, &encoded, ordinal, .big);
            var digest: [32]u8 = undefined;
            std.crypto.hash.sha2.Sha256.hash(&encoded, &digest, .{});
            const id = try scope.artifactId(&std.fmt.bytesToHex(digest, .lower));
            try writer.capture(&id, ordinal + 1, "payload-generation", .none);
            // Pending records must be queryable before their first flush.
            if (ordinal == 0) {
                var record = (try writer.lookup(a, &id, .none)).?;
                defer record.deinit(a);
                try std.testing.expectEqual(@as(u64, 1), record.bytes);
                try writer.capture(&id, 1, "payload-generation", .none);
            }
        }
        const root = (try writer.finish(.none)).?;
        defer maps.freeRef(quota.allocator(), root);
        const elapsed = @import("antfly_platform").time.monotonicNs() - started;
        writes[trial] = provider.writes;
        const build_reads = provider.reads;
        var reopened: maps.Reader = .{ .client = provider.client(), .bucket = "bucket", .prefix = "maps", .root = root, .complete = true, .remaining_bytes = 1024 * 1024 * 1024 };
        defer reopened.deinit();
        for (0..16384) |ordinal| {
            var encoded: [8]u8 = undefined;
            std.mem.writeInt(u64, &encoded, ordinal, .big);
            var digest: [32]u8 = undefined;
            std.crypto.hash.sha2.Sha256.hash(&encoded, &digest, .{});
            const id = try scope.artifactId(&std.fmt.bytesToHex(digest, .lower));
            var record = (try reopened.lookup(a, &id, .none)).?;
            defer record.deinit(a);
            try std.testing.expectEqual(@as(u64, ordinal + 1), record.bytes);
            try std.testing.expectEqualStrings("payload-generation", record.generation);
        }
        // A cold map read must obey the admitted owner even when the caller
        // can still allocate result records. Warm traversal refs obey it too.
        var encoded_zero: [8]u8 = @splat(0);
        var zero_digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(&encoded_zero, &zero_digest, .{});
        const zero_id = try scope.artifactId(&std.fmt.bytesToHex(zero_digest, .lower));
        var scratch_quota: local.sql_memory_budget = .{ .backing = a, .limit = 0 };
        var constrained: maps.Reader = .{ .cache_allocator = scratch_quota.allocator(), .client = provider.client(), .bucket = "bucket", .prefix = "maps", .root = root, .complete = true };
        try std.testing.expectError(error.OutOfMemory, constrained.lookup(a, &zero_id, .none));
        scratch_quota.limit = 8 * 1024 * 1024;
        var found = (try constrained.lookup(a, &zero_id, .none)).?;
        found.deinit(a);
        try std.testing.expect(scratch_quota.peak > 0);
        scratch_quota.limit = scratch_quota.live;
        try std.testing.expectError(error.OutOfMemory, constrained.lookup(a, &zero_id, .none));
        constrained.deinit();
        try std.testing.expectEqual(@as(usize, 0), scratch_quota.live);
        const saved_limit = quota.limit;
        quota.limit = quota.live;
        try std.testing.expectError(error.OutOfMemory, writer.lookup(a, &zero_id, .none));
        quota.limit = saved_limit;
        try std.testing.expect(writer.peak_pending_records <= policy.records);
        std.debug.print("BULK_GENERATION_PUBLICATION records=16384 batch_records={} puts={} reads={} written_bytes={} flushes={} pending_peak={} allocator_peak={} elapsed_ns={}\n", .{ policy.records, writes[trial], build_reads, provider.written_bytes, writer.flushes, writer.peak_pending_bytes, quota.peak, elapsed });
    }
    try std.testing.expect(writes[1] * 2 < writes[0]);
}

test "external lake global frontier dependency regressions" {
    _ = maps;
}

var observed_status: u16 = 0;
var observed_attempts: usize = 0;
fn observeHttp(_: ?*anyopaque, event: objects.Gcs.HttpDiagnostic) void {
    observed_status = event.status orelse 0;
    observed_attempts = event.attempts;
}

test "external lake execution generation publication HTTP status diagnostics preserve observers and conflicts" {
    const httpx = @import("httpx");
    const a = std.testing.allocator;
    const io = std.testing.io;
    const Observer = struct {
        before_count: usize = 0,
        after_count: usize = 0,
        fn before(raw: *anyopaque, _: httpx.AttemptObserver.Context) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.before_count += 1;
        }
        fn after(raw: *anyopaque, _: ?*const httpx.Response) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.after_count += 1;
        }
    };
    const Case = struct { status: u16, expected: anyerror, attempts: usize };
    for ([_]Case{ .{ .status = 503, .expected = error.RemoteUnavailable, .attempts = 2 }, .{ .status = 429, .expected = error.RateLimited, .attempts = 2 }, .{ .status = 412, .expected = error.PreconditionFailed, .attempts = 1 }, .{ .status = 403, .expected = error.AccessDenied, .attempts = 1 } }) |case| {
        observed_status = 0;
        observed_attempts = 0;
        var observer: Observer = .{};
        var server = try httpx.testing_mod.TestServer.start(a, io, &.{.{ .method = .POST, .path = "/upload/storage/v1/b/bucket/o", .respond = .{ .status = case.status } }});
        defer server.deinit();
        const Task = struct {
            fn serve(s: *httpx.testing_mod.TestServer, count: usize) std.Io.Cancelable!void {
                for (0..count) |_| s.handleOne() catch return;
            }
        };
        var group: std.Io.Group = .init;
        try group.concurrent(io, Task.serve, .{ &server, case.attempts });
        defer group.cancel(io);
        var provider = try objects.Gcs.JsonApiClient.init(a, .{
            .endpoint = try std.fmt.allocPrint(a, "{s}/storage/v1", .{server.baseUrl()}),
            .upload_endpoint = try std.fmt.allocPrint(a, "{s}/upload/storage/v1", .{server.baseUrl()}),
            .io = io,
            .request_timeout_ms = 3000,
            .diagnostics = .{ .report = observeHttp },
            .http_client_config = .{ .keep_alive = false, .retry_policy = .{ .max_retries = 1, .initial_delay_ms = 1 }, .attempt_observer = .{ .ptr = &observer, .before = Observer.before, .after = Observer.after } },
        });
        defer provider.deinit();
        var client = provider.client();
        try std.testing.expectError(case.expected, client.putObject("bucket", "object", "payload", .{ .if_none_match = true }));
        try group.await(io);
        try std.testing.expectEqual(case.status, observed_status);
        try std.testing.expectEqual(case.attempts, observed_attempts);
        try std.testing.expectEqual(case.attempts, observer.before_count);
        try std.testing.expectEqual(case.attempts, observer.after_count);
    }
}

test {
    _ = @import("api/lake_http_diagnostic_budget.zig");
}
