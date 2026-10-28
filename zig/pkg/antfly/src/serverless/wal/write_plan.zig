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

//! Native immutable WAL preparation from a retained metadata root. The root is
//! a hint only. Its exact dependencies and original WAL ETag are checked by
//! the final native transactional-store commit before any receipt is returned.
const std = @import("std");
const objects = @import("objectstore");
const tx = objects.transactional_store;
const A = std.mem.Allocator;
const T = objects.types;

pub const Input = struct {
    state: objects.immutable_log.State,
    metadata_namespace: []const u8,
    reservation_id: []const u8,
};
const Body = struct { __antfly_bytes: ?[]const u8 = null, __antfly_utf8: ?[]const u8 = null };
const EncodedRecord = struct { etag: []const u8, body: Body, contentType: ?[]const u8 = null };
const Record = struct { etag: []const u8, body: []const u8, content_type: ?[]const u8 = null };

pub const Plan = struct {
    arena: std.heap.ArenaAllocator,
    underlying: objects.Client,
    bucket: []const u8,
    keys: [3][]const u8,
    original: [3]?[]const u8,
    records: [3]?Record,
    original_etag: ?[]const u8,
    input: Input,
    io: std.Io,

    pub fn init(a: A, backing: objects.Client, bucket: []const u8, prefix: []const u8, namespace: []const u8, input: Input, io: std.Io) !Plan {
        var arena = std.heap.ArenaAllocator.init(a);
        errdefer arena.deinit();
        const m = arena.allocator();
        if (input.reservation_id.len == 0 or input.reservation_id.len > 64 or input.metadata_namespace.len == 0) return error.InvalidNativeWritePlan;
        var hash: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(namespace, &hash, .{});
        const keys = [3][]const u8{
            try std.fmt.allocPrint(m, "{s}/wal-v1/{s}/HEAD", .{ prefix, std.fmt.bytesToHex(hash, .lower) }),
            try std.fmt.allocPrint(m, "{s}/progress/{s}/HEAD", .{ prefix, namespace }),
            try std.fmt.allocPrint(m, "{s}/progress/{s}/MANIFEST_GC_FLOOR", .{ prefix, namespace }),
        };
        const encoded = try (tx.Context{ .alloc = a, .client = backing, .bucket = bucket, .prefix = prefix, .namespace = input.metadata_namespace, .io = io }).execute(.{
            .action = .read,
            .owner = input.state.owner,
            .state = input.state,
            .commands = &.{ .{ .kind = .get, .key = keys[0] }, .{ .kind = .get, .key = keys[1] }, .{ .kind = .get, .key = keys[2] } },
        });
        defer a.free(encoded);
        const rows = (try std.json.parseFromSlice(struct { results: []const []const struct { key: []const u8, value: []const u8 } }, m, encoded, .{ .allocate = .alloc_always })).value.results;
        if (rows.len != 3) return error.InvalidNativeWritePlan;
        var original: [3]?[]const u8 = @splat(null);
        var records: [3]?Record = @splat(null);
        for (rows, 0..) |items, i| {
            if (items.len > 1) return error.InvalidNativeWritePlan;
            if (items.len == 0) continue;
            if (!std.mem.eql(u8, keys[i], items[0].key)) return error.InvalidNativeWritePlan;
            original[i] = items[0].value;
            const value = (try std.json.parseFromSlice(EncodedRecord, m, items[0].value, .{ .allocate = .alloc_always })).value;
            if ((value.body.__antfly_bytes == null) == (value.body.__antfly_utf8 == null)) return error.InvalidNativeWritePlan;
            const body = value.body.__antfly_bytes orelse value.body.__antfly_utf8.?;
            if (body.len > 4096 or value.etag.len == 0 or value.etag.len > 128) return error.InvalidNativeWritePlan;
            records[i] = .{ .etag = value.etag, .body = body, .content_type = value.contentType };
        }
        return .{ .arena = arena, .underlying = backing, .bucket = bucket, .keys = keys, .original = original, .records = records, .original_etag = if (records[0]) |record| record.etag else null, .input = input, .io = io };
    }
    pub fn deinit(self: *Plan) void {
        self.arena.deinit();
    }
    pub fn client(self: *Plan, a: A) objects.Client {
        return .{ .allocator = a, .ptr = self, .vtable = &vtable };
    }
    pub fn finish(self: *Plan, a: A, watermark: []const u8, now_ms: u64) !tx.WalCommit {
        const record = self.records[0] orelse return error.InvalidNativeWritePlan;
        const value = try std.json.Stringify.valueAlloc(a, .{ .etag = record.etag, .body = .{ .__antfly_utf8 = record.body }, .contentType = record.content_type }, .{});
        errdefer a.free(value);
        const dependencies = try a.alloc(tx.WalDependency, 2);
        dependencies[0] = .{ .key = self.keys[1], .value = self.original[1], .comparison = .record_body };
        dependencies[1] = .{ .key = self.keys[2], .value = self.original[2], .comparison = .record_body };
        return .{ .wal_key = self.keys[0], .expected_etag = self.original_etag, .value = value, .dependencies = dependencies, .publication_key = "__antfly_maintenance/publication", .watermark = watermark, .now_ms = now_ms, .id = self.input.reservation_id };
    }
    fn selfFrom(raw: *anyopaque) *Plan {
        return @ptrCast(@alignCast(raw));
    }
    fn index(self: *Plan, key: []const u8) ?usize {
        for (self.keys, 0..) |name, i| if (std.mem.eql(u8, name, key)) return i;
        return null;
    }
    fn metadata(record: Record, a: A, bucket: []const u8, key: []const u8) !T.ObjectMetadata {
        const b = try a.dupe(u8, bucket);
        errdefer a.free(b);
        const k = try a.dupe(u8, key);
        errdefer a.free(k);
        const e = try a.dupe(u8, record.etag);
        errdefer a.free(e);
        return .{ .bucket = b, .key = k, .etag = e, .content_length = record.body.len, .content_type = if (record.content_type) |ct| try a.dupe(u8, ct) else null };
    }
    fn get(raw: *anyopaque, a: A, bucket: []const u8, key: []const u8, opts: T.GetOptions) !T.GetResult {
        const self = selfFrom(raw);
        if (self.index(key)) |i| {
            if (!std.mem.eql(u8, bucket, self.bucket) or opts.range != null or opts.version_id != null) return error.InvalidNativeWritePlan;
            const record = self.records[i] orelse return error.FileNotFound;
            if (opts.if_match_etag) |etag| if (!std.mem.eql(u8, etag, record.etag)) return error.PreconditionFailed;
            if (record.body.len > (opts.max_response_bytes orelse std.math.maxInt(usize))) return error.ResponseTooLarge;
            const body = try a.dupe(u8, record.body);
            errdefer a.free(body);
            return .{ .body = body, .metadata = try metadata(record, a, bucket, key) };
        }
        return self.underlying.vtable.get_object(self.underlying.ptr, a, bucket, key, opts);
    }
    fn put(raw: *anyopaque, a: A, bucket: []const u8, key: []const u8, body: []const u8, opts: T.PutOptions) !T.PutResult {
        const self = selfFrom(raw);
        if (self.index(key)) |i| {
            if (i != 0 or !std.mem.eql(u8, bucket, self.bucket) or opts.guard != null or body.len > 4096 or !std.unicode.utf8ValidateSlice(body) or (!opts.if_none_match and opts.if_match_etag == null)) return error.InvalidNativeWritePlan;
            const prior = self.records[0];
            if (opts.if_none_match and prior != null) return error.PreconditionFailed;
            if (opts.if_match_etag) |etag| if (prior == null or !std.mem.eql(u8, etag, prior.?.etag)) return error.PreconditionFailed;
            var nonce: [16]u8 = undefined;
            self.io.random(&nonce);
            const m = self.arena.allocator();
            const etag = try m.dupe(u8, &std.fmt.bytesToHex(nonce, .lower));
            self.records[0] = .{ .etag = etag, .body = try m.dupe(u8, body), .content_type = if (opts.content_type) |ct| try m.dupe(u8, ct) else null };
            return .{ .etag = try a.dupe(u8, etag) };
        }
        return self.underlying.vtable.put_object(self.underlying.ptr, a, bucket, key, body, opts);
    }
    fn stat(raw: *anyopaque, a: A, bucket: []const u8, key: []const u8) !T.ObjectMetadata {
        const self = selfFrom(raw);
        if (self.index(key)) |i| return metadata(self.records[i] orelse return error.FileNotFound, a, bucket, key);
        return self.underlying.vtable.stat_object(self.underlying.ptr, a, bucket, key);
    }
    fn many(raw: *anyopaque, a: A, bucket: []const u8, items: []const T.PutObjectRequest) ![]T.PutResult {
        const self = selfFrom(raw);
        for (items) |item| if (self.index(item.key) != null) return error.InvalidNativeWritePlan;
        var base = self.underlying;
        base.allocator = a;
        return base.putObjects(bucket, items);
    }
    fn attributes(raw: *anyopaque, a: A, bucket: []const u8, key: []const u8) !T.ObjectAttributes {
        const self = selfFrom(raw);
        if (self.index(key) != null) return error.ObjectOperationUnsupported;
        return self.underlying.vtable.get_object_attributes(self.underlying.ptr, a, bucket, key);
    }
    fn exists(raw: *anyopaque, bucket: []const u8, opts: T.BucketOptions) !bool {
        const self = selfFrom(raw);
        return self.underlying.vtable.bucket_exists(self.underlying.ptr, bucket, opts);
    }
    fn make(_: *anyopaque, _: []const u8, _: T.BucketOptions) !void {
        return error.ObjectOperationUnsupported;
    }
    fn delete(_: *anyopaque, _: []const u8, _: []const u8, _: T.DeleteOptions) !void {
        return error.ObjectOperationUnsupported;
    }
    fn list(_: *anyopaque, _: A, _: []const u8, _: T.ListOptions) !T.ListResult {
        return error.ObjectOperationUnsupported;
    }
    fn noop(_: A, _: *anyopaque) void {}
    const vtable = objects.Client.VTable{ .deinit = noop, .bucket_exists = exists, .make_bucket = make, .put_object = put, .put_objects = many, .get_object = get, .stat_object = stat, .get_object_attributes = attributes, .delete_object = delete, .list_objects = list };
};

test "serverless native write plan preserves semantic dependencies and final native append fencing" {
    const a = std.testing.allocator;
    var memory = objects.MemoryClient.init(a);
    defer memory.deinit();
    var client_ = memory.client();
    try client_.makeBucket("database");
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const m = arena.allocator();
    const context = tx.Context{ .alloc = m, .client = client_, .bucket = "database", .prefix = "test", .namespace = "metadata", .io = std.testing.io };
    const StateResult = struct { state: objects.immutable_log.State, applied: ?bool = null };
    const initial = try context.execute(.{ .action = .begin, .owner = @splat(1) });
    var state = (try std.json.parseFromSlice(StateResult, m, initial, .{ .ignore_unknown_fields = true })).value.state;
    var plan = try Plan.init(a, client_, "database", "test", "docs", .{ .state = state, .metadata_namespace = "metadata", .reservation_id = "first" }, std.testing.io);
    defer plan.deinit();
    var indexed = try @import("indexed_object_store.zig").IndexedObjectStore.init(a, plan.client(a), "database", "test");
    defer indexed.deinit();
    indexed.inline_tail_enabled = true;
    const batch = try indexed.appendGroupsIdempotent("docs", &.{.{ .timestamp = 1, .payloads = &.{"one"}, .id = "first" }});
    defer a.free(batch);
    const commit = try plan.finish(m, "1", 1000);
    try std.testing.expectError(error.FileNotFound, client_.getObject("database", commit.wal_key, .{}));
    const committed = try context.execute(.{ .action = .commit_wal, .owner = @splat(1), .state = state, .wal_commit = commit });
    const first = (try std.json.parseFromSlice(StateResult, m, committed, .{ .ignore_unknown_fields = true })).value;
    try std.testing.expect(first.applied.?);
    state = first.state;
    const header1 = try std.json.Stringify.valueAlloc(m, .{ .etag = "lease1", .body = .{ .__antfly_utf8 = "1" }, .contentType = "old" }, .{});
    const changed = try context.execute(.{ .action = .commit, .owner = @splat(1), .state = state, .commands = &.{.{ .kind = .put, .key = plan.keys[1], .value = header1 }} });
    state = (try std.json.parseFromSlice(StateResult, m, changed, .{ .ignore_unknown_fields = true })).value.state;
    var next = try Plan.init(a, client_, "database", "test", "docs", .{ .state = state, .metadata_namespace = "metadata", .reservation_id = "second" }, std.testing.io);
    defer next.deinit();
    var next_wal = try @import("indexed_object_store.zig").IndexedObjectStore.init(a, next.client(a), "database", "test");
    defer next_wal.deinit();
    const second_batch = try next_wal.appendGroupsIdempotent("docs", &.{.{ .timestamp = 2, .payloads = &.{"two"}, .id = "second" }});
    defer a.free(second_batch);
    const second = try next.finish(m, "2", 1001);
    const header2 = try std.json.Stringify.valueAlloc(m, .{ .etag = "lease2", .body = .{ .__antfly_utf8 = "1" }, .contentType = "renewed" }, .{});
    const renewed = try context.execute(.{ .action = .commit, .owner = @splat(1), .state = state, .commands = &.{.{ .kind = .put, .key = plan.keys[1], .value = header2 }} });
    state = (try std.json.parseFromSlice(StateResult, m, renewed, .{ .ignore_unknown_fields = true })).value.state;
    const accepted = try context.execute(.{ .action = .commit_wal, .owner = @splat(1), .state = state, .wal_commit = second });
    const result = (try std.json.parseFromSlice(StateResult, m, accepted, .{ .ignore_unknown_fields = true })).value;
    try std.testing.expect(result.applied.?);
    state = result.state;
    var stale_source = try Plan.init(a, client_, "database", "test", "docs", .{ .state = state, .metadata_namespace = "metadata", .reservation_id = "third" }, std.testing.io);
    defer stale_source.deinit();
    var third_wal = try @import("indexed_object_store.zig").IndexedObjectStore.init(a, stale_source.client(a), "database", "test");
    defer third_wal.deinit();
    const third_batch = try third_wal.appendGroupsIdempotent("docs", &.{.{ .timestamp = 3, .payloads = &.{"three"}, .id = "third" }});
    defer a.free(third_batch);
    const third = try stale_source.finish(m, "3", 1002);
    const changed_body = try std.json.Stringify.valueAlloc(m, .{ .etag = "publication2", .body = .{ .__antfly_utf8 = "2" } }, .{});
    const advanced = try context.execute(.{ .action = .commit, .owner = @splat(1), .state = state, .commands = &.{.{ .kind = .put, .key = plan.keys[1], .value = changed_body }} });
    state = (try std.json.parseFromSlice(StateResult, m, advanced, .{ .ignore_unknown_fields = true })).value.state;
    const rejected = try context.execute(.{ .action = .commit_wal, .owner = @splat(1), .state = state, .wal_commit = third });
    const rejected_result = (try std.json.parseFromSlice(StateResult, m, rejected, .{ .ignore_unknown_fields = true })).value;
    try std.testing.expect(!rejected_result.applied.?);
    state = rejected_result.state;
    var fenced_plan = try Plan.init(a, client_, "database", "test", "docs", .{ .state = state, .metadata_namespace = "metadata", .reservation_id = "fenced" }, std.testing.io);
    defer fenced_plan.deinit();
    var fenced_wal = try @import("indexed_object_store.zig").IndexedObjectStore.init(a, fenced_plan.client(a), "database", "test");
    defer fenced_wal.deinit();
    const fenced_batch = try fenced_wal.appendGroupsIdempotent("docs", &.{.{ .timestamp = 3, .payloads = &.{"fenced"}, .id = "fenced" }});
    defer a.free(fenced_batch);
    const replay = try fenced_plan.finish(m, "3", 1003);
    const taken = try context.execute(.{ .action = .begin, .owner = @splat(2), .state = state });
    _ = taken;
    try std.testing.expectError(error.ImmutableLogFenced, context.execute(.{ .action = .commit_wal, .owner = @splat(1), .state = state, .wal_commit = replay }));
}
