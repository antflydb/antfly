// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
const storage = @import("objectstore");
const types = @import("types.zig");
const managed = @import("managed.zig");
const metadata = @import("metadata.zig");
const rest = @import("rest.zig");
const a = std.testing.allocator;
const create_request = "{\"schema\":{\"type\":\"struct\",\"schema-id\":0,\"fields\":[{\"id\":1,\"name\":\"title\",\"type\":\"string\",\"required\":false}]}}";
const updates = "{\"requirements\":[{\"type\":\"assert-current-schema-id\",\"current-schema-id\":0},{\"type\":\"assert-ref-snapshot-id\",\"ref\":\"main\",\"snapshot-id\":null}],\"updates\":[{\"action\":\"add-snapshot\",\"snapshot\":{\"snapshot-id\":7,\"sequence-number\":1,\"timestamp-ms\":2,\"manifest-list\":\"gs://archive/hn/metadata/list.avro\",\"summary\":{\"operation\":\"append\"}}},{\"action\":\"set-snapshot-ref\",\"ref-name\":\"main\",\"snapshot-id\":7,\"type\":\"branch\"}]}";

test "lake catalog managed commits survive reopen and preserve replay after later heads" {
    var memory = storage.MemoryClient.init(a);
    defer memory.deinit();
    var catalog: managed.Managed = .{ .client = memory.client(), .bucket = "archive", .prefix = "hn", .source_uri = "gs://archive/hn" };
    var initial = try catalog.create(a, "create", create_request, 1);
    defer initial.deinit(a);
    const c: types.Commit = .{ .id = "one", .expected_metadata_location = initial.metadata_location, .body = updates, .timestamp_ms = 2 };
    var first = try catalog.commit(a, c);
    defer first.deinit(a);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const root = try metadata.parse(arena.allocator(), first.metadata_json);
    try std.testing.expectEqual(@as(i64, 7), try metadata.int(try metadata.get(root, "current-snapshot-id")));
    const next: types.Commit = .{ .id = "two", .expected_metadata_location = first.metadata_location, .body = "{\"requirements\":[],\"updates\":[{\"action\":\"set-properties\",\"updates\":{\"owner\":\"HN\"}}]}", .timestamp_ms = 3 };
    var second = try catalog.commit(a, next);
    defer second.deinit(a);
    var reopened = catalog;
    var replay = try reopened.commit(a, c);
    defer replay.deinit(a);
    try std.testing.expectEqualStrings(first.metadata_location, replay.metadata_location);
    try std.testing.expectEqual(types.Outcome.committed, try reopened.resolve(a, c.id, &types.commitHash(c)));
    var current = try reopened.load(a);
    defer current.deinit(a);
    try std.testing.expectEqualStrings(second.metadata_location, current.metadata_location);
    var stale = c;
    stale.id = "stale";
    try std.testing.expectError(error.LakeCommitConflict, reopened.commit(a, stale));
    var reused = c;
    reused.body = next.body;
    try std.testing.expectError(error.LakeCommitIdReused, reopened.commit(a, reused));
}

test "lake catalog managed resolves lost successful HEAD response and fences concurrent writers" {
    var memory = storage.MemoryClient.init(a);
    defer memory.deinit();
    var faults = storage.ScriptedFaultClient.init(a, memory.client());
    defer faults.deinit();
    var catalog: managed.Managed = .{ .client = faults.client(), .bucket = "archive", .prefix = "hn", .source_uri = "gs://archive/hn" };
    var initial = try catalog.create(a, "create", create_request, 1);
    defer initial.deinit(a);
    faults.put_filter = .{ .ptr = &faults, .matches = struct {
        fn matches(_: *anyopaque, _: []const u8, key: []const u8, _: []const u8) bool {
            return std.mem.endsWith(u8, key, "/head.json");
        }
    }.matches };
    faults.next_put = .{ .commit_then_fail = error.ConnectionResetByPeer };
    const c: types.Commit = .{ .id = "one", .expected_metadata_location = initial.metadata_location, .body = updates, .timestamp_ms = 2 };
    var first = try catalog.commit(a, c);
    defer first.deinit(a);
    try std.testing.expectEqual(types.Outcome.committed, try catalog.resolve(a, c.id, &types.commitHash(c)));
    var other = c;
    other.id = "other-writer";
    try std.testing.expectError(error.LakeCommitConflict, catalog.commit(a, other));
}

test "lake catalog managed validates requirements references and sequence numbers before publication" {
    const initial = try metadata.createAlloc(a, create_request, "uuid", 1, "gs://archive/hn");
    defer a.free(initial);
    for ([_][]const u8{
        "{\"requirements\":[{\"type\":\"assert-current-schema-id\",\"current-schema-id\":5}],\"updates\":[]}",
        "{\"requirements\":[{\"type\":\"made-up\"}],\"updates\":[]}",
        "{\"requirements\":[],\"updates\":[{\"action\":\"set-current-schema\",\"schema-id\":9}]}",
        "{\"requirements\":[],\"updates\":[{\"action\":\"set-snapshot-ref\",\"ref-name\":\"main\",\"snapshot-id\":999,\"type\":\"branch\"}]}",
    }, 0..) |body, i| {
        const c: types.Commit = .{ .id = "invalid", .expected_metadata_location = "metadata", .body = body, .timestamp_ms = 2 };
        const result = metadata.applyAlloc(a, initial, "metadata", c);
        switch (i) {
            0 => try std.testing.expectError(error.LakeCommitConflict, result),
            1 => try std.testing.expectError(error.UnsupportedLakeRequirement, result),
            else => try std.testing.expectError(error.InvalidLakeMetadata, result),
        }
    }
}

const FakeRest = struct {
    current: []u8,
    post_calls: usize = 0,
    lose_response: bool = false,
    fail_before_commit: bool = false,
    reject_commit: bool = false,
    idempotency: bool = false,
    path_seen: bool = false,
    fn transport(self: *FakeRest) rest.Transport {
        return .{ .ptr = self, .request_fn = request };
    }
    fn request(raw: *anyopaque, allocator: std.mem.Allocator, method: @import("httpx").Method, uri: []const u8, body: ?[]const u8, id: ?[]const u8, _: types.Context) !rest.Response {
        const self: *FakeRest = @ptrCast(@alignCast(raw));
        if (std.mem.endsWith(u8, uri, "/v1/config")) return .{ .status = 200, .body = try allocator.dupe(u8, if (self.idempotency) "{\"defaults\":{\"prefix\":\"tenant\"},\"overrides\":{},\"idempotency-key-lifetime\":\"PT30M\"}" else "{\"defaults\":{\"prefix\":\"tenant\"},\"overrides\":{}}") };
        self.path_seen = std.mem.endsWith(u8, uri, "/v1/tenant/namespaces/analytics%1Fpublic/tables/hn%2Fitems");
        if (method == .POST) {
            self.post_calls += 1;
            if (self.fail_before_commit) return error.ConnectionResetByPeer;
            if (self.reject_commit) return .{ .status = 409, .body = try allocator.dupe(u8, "{}") };
            try std.testing.expect((id != null) == self.idempotency);
            var arena = std.heap.ArenaAllocator.init(allocator);
            defer arena.deinit();
            const scratch = arena.allocator();
            var parsed = try metadata.parse(scratch, self.current);
            const request_body = try metadata.parse(scratch, body.?);
            const list = try metadata.get(request_body, "updates");
            const props = try metadata.get(list.array.items[list.array.items.len - 1], "updates");
            try parsed.object.put(scratch, "properties", props);
            const next = try std.json.Stringify.valueAlloc(allocator, parsed, .{});
            allocator.free(self.current);
            self.current = next;
            if (self.lose_response) return error.ConnectionResetByPeer;
        }
        const envelope = try std.fmt.allocPrint(allocator, "{{\"metadata-location\":\"gs://archive/hn/metadata/rest.metadata.json\",\"metadata\":{s}}}", .{self.current});
        return .{ .status = 200, .body = envelope };
    }
};

test "lake catalog REST negotiates paths delegates commits and resolves lost replies without replay" {
    var fake: FakeRest = .{ .current = try metadata.createAlloc(a, create_request, "uuid", 1, "gs://archive/hn"), .lose_response = true };
    defer a.free(fake.current);
    var journal = storage.MemoryClient.init(a);
    defer journal.deinit();
    var catalog: rest.Rest = .{ .config = .{ .type = .rest, .connection = "rest", .uri = "https://catalog.example", .namespace = &.{ "analytics", "public" }, .name = "hn/items" }, .transport = fake.transport(), .journal = .{ .client = journal.client(), .bucket = "state", .prefix = "hn" }, .now_ms = 2 };
    var current = try catalog.load(a);
    defer current.deinit(a);
    const c: types.Commit = .{ .id = "request", .expected_metadata_location = current.metadata_location, .body = "{\"requirements\":[],\"updates\":[]}", .timestamp_ms = 2 };
    var result = try catalog.commit(a, c);
    defer result.deinit(a);
    try std.testing.expect(fake.path_seen);
    try std.testing.expectEqual(@as(usize, 1), fake.post_calls);
    var restarted = catalog;
    var replay = try restarted.commit(a, c);
    defer replay.deinit(a);
    try std.testing.expectEqual(@as(usize, 1), fake.post_calls);
    var changed = c;
    changed.body = updates;
    try std.testing.expectError(error.LakeCommitIdReused, restarted.commit(a, changed));
}

test "lake catalog HTTP transport compiles with bounded cancellation and no automatic replay" {
    var io = std.Io.Threaded.init(a, .{});
    defer io.deinit();
    var client = @import("httpx").Client.init(a, io.io());
    defer client.deinit();
    var http: rest.HttpTransport = .{ .client = &client };
    _ = http.transport();
}

test "lake catalog REST does not replay ambiguous commits without a live negotiated window" {
    var fake: FakeRest = .{ .current = try metadata.createAlloc(a, create_request, "uuid", 1, "gs://archive/hn"), .fail_before_commit = true };
    defer a.free(fake.current);
    var journal = storage.MemoryClient.init(a);
    defer journal.deinit();
    var catalog: rest.Rest = .{ .config = .{ .type = .rest, .connection = "rest", .uri = "https://catalog.example", .namespace = &.{"hn"}, .name = "items" }, .transport = fake.transport(), .journal = .{ .client = journal.client(), .bucket = "state", .prefix = "hn" }, .now_ms = 2 };
    var initial = try catalog.load(a);
    defer initial.deinit(a);
    const c: types.Commit = .{ .id = "ambiguous", .expected_metadata_location = initial.metadata_location, .body = "{\"requirements\":[],\"updates\":[]}", .timestamp_ms = 2 };
    try std.testing.expectError(error.LakeCommitOutcomeUnknown, catalog.commit(a, c));
    fake.fail_before_commit = false;
    try std.testing.expectError(error.LakeCommitOutcomeUnknown, catalog.commit(a, c));
    try std.testing.expectEqual(@as(usize, 1), fake.post_calls);
    fake.idempotency = true;
    catalog.now_ms = 2 + 30 * 60 * 1000;
    try std.testing.expectError(error.LakeCommitOutcomeUnknown, catalog.commit(a, c));
    try std.testing.expectEqual(@as(usize, 1), fake.post_calls);
}

test "lake catalog REST receipts retain committed proof after metadata markers expire" {
    var fake: FakeRest = .{ .current = try metadata.createAlloc(a, create_request, "uuid", 1, "gs://archive/hn") };
    defer a.free(fake.current);
    var journal = storage.MemoryClient.init(a);
    defer journal.deinit();
    var catalog: rest.Rest = .{ .config = .{ .type = .rest, .connection = "rest", .uri = "https://catalog.example", .namespace = &.{"hn"}, .name = "items" }, .transport = fake.transport(), .journal = .{ .client = journal.client(), .bucket = "state", .prefix = "hn" }, .now_ms = 2 };
    var initial = try catalog.load(a);
    defer initial.deinit(a);
    const c: types.Commit = .{ .id = "receipt", .expected_metadata_location = initial.metadata_location, .body = "{\"requirements\":[],\"updates\":[]}", .timestamp_ms = 2 };
    var committed = try catalog.commit(a, c);
    defer committed.deinit(a);
    const without_markers = try a.dupe(u8, initial.metadata_json);
    a.free(fake.current);
    fake.current = without_markers;
    try std.testing.expectEqual(types.Outcome.committed, try catalog.resolve(a, c.id, &types.commitHash(c)));
    try std.testing.expectError(error.LakeCommitIdReused, catalog.resolve(a, c.id, "different-request"));
}

test "lake catalog REST preserves definitive rejection across restart without resending" {
    var fake: FakeRest = .{ .current = try metadata.createAlloc(a, create_request, "uuid", 1, "gs://archive/hn"), .reject_commit = true };
    defer a.free(fake.current);
    var journal = storage.MemoryClient.init(a);
    defer journal.deinit();
    var catalog: rest.Rest = .{ .config = .{ .type = .rest, .connection = "rest", .uri = "https://catalog.example", .namespace = &.{"hn"}, .name = "items" }, .transport = fake.transport(), .journal = .{ .client = journal.client(), .bucket = "state", .prefix = "hn" }, .now_ms = 2 };
    var initial = try catalog.load(a);
    defer initial.deinit(a);
    const c: types.Commit = .{ .id = "rejected", .expected_metadata_location = initial.metadata_location, .body = "{\"requirements\":[],\"updates\":[]}", .timestamp_ms = 2 };
    try std.testing.expectError(error.LakeCommitConflict, catalog.commit(a, c));
    try std.testing.expectEqual(types.Outcome.not_committed, try catalog.resolve(a, c.id, &types.commitHash(c)));
    fake.reject_commit = false;
    var restarted = catalog;
    try std.testing.expectError(error.LakeCommitConflict, restarted.commit(a, c));
    try std.testing.expectEqual(@as(usize, 1), fake.post_calls);
    var next = c;
    next.id = "rebased";
    var accepted = try restarted.commit(a, next);
    defer accepted.deinit(a);
    try std.testing.expectEqual(@as(usize, 2), fake.post_calls);
}

test "lake catalog retirement is a durable HEAD fence and prevents resurrection after reopen" {
    var memory = storage.MemoryClient.init(a);
    defer memory.deinit();
    var faults = storage.ScriptedFaultClient.init(a, memory.client());
    defer faults.deinit();
    var catalog: managed.Managed = .{ .client = faults.client(), .bucket = "archive", .prefix = "hn", .source_uri = "gs://archive/hn" };
    var initial = try catalog.create(a, "create", create_request, 1);
    defer initial.deinit(a);
    var parent = try catalog.load(a);
    defer parent.deinit(a);
    const dead = "gs://archive/hn/metadata/list.avro";
    const request: managed.Retirement = .{ .id = "retirement", .expected_metadata_location = parent.metadata_location, .expected_version = parent.version.?, .objects = &.{dead} };
    faults.put_filter = .{ .ptr = &faults, .matches = struct {
        fn matches(_: *anyopaque, _: []const u8, key: []const u8, _: []const u8) bool {
            return std.mem.endsWith(u8, key, "/head.json");
        }
    }.matches };
    faults.next_put = .{ .commit_then_fail = error.ConnectionResetByPeer };
    var retired = try catalog.retire(a, request);
    defer retired.deinit(a);
    try std.testing.expect(retired.retirement_root != null);
    var current = try catalog.load(a);
    defer current.deinit(a);
    try std.testing.expectEqualStrings(parent.metadata_location, current.metadata_location);
    try std.testing.expect(!std.mem.eql(u8, parent.version.?, current.version.?));
    var reopened = catalog;
    var replay = try reopened.retire(a, request);
    defer replay.deinit(a);
    try std.testing.expectEqual(retired.retirement_root.?, replay.retirement_root.?);
    const c: types.Commit = .{ .id = "resurrection", .expected_metadata_location = current.metadata_location, .body = updates, .timestamp_ms = 2 };
    try std.testing.expectError(error.LakeObjectRetired, reopened.commit(a, c));
    // Even a harmless metadata writer cannot erase the retirement set.
    const harmless: types.Commit = .{ .id = "properties", .expected_metadata_location = current.metadata_location, .body = "{\"requirements\":[],\"updates\":[{\"action\":\"set-properties\",\"updates\":{\"owner\":\"HN\"}}]}", .timestamp_ms = 3 };
    var changed = try reopened.commit(a, harmless);
    defer changed.deinit(a);
    var after = try reopened.load(a);
    defer after.deinit(a);
    try std.testing.expectEqual(retired.retirement_root.?, after.retirement_root.?);
    const stale: managed.Retirement = .{ .id = "stale-retirement", .expected_metadata_location = parent.metadata_location, .expected_version = parent.version.?, .objects = &.{"gs://archive/hn/data/other.parquet"} };
    try std.testing.expectError(error.LakeCommitConflict, reopened.retire(a, stale));
}

test "lake catalog retirement rejects live files and fences staged writers that predate GC" {
    var memory = storage.MemoryClient.init(a);
    defer memory.deinit();
    var faults = storage.ScriptedFaultClient.init(a, memory.client());
    defer faults.deinit();
    var catalog: managed.Managed = .{ .client = faults.client(), .bucket = "archive", .prefix = "hn", .source_uri = "gs://archive/hn" };
    var initial = try catalog.create(a, "create", create_request, 1);
    defer initial.deinit(a);
    var parent = try catalog.load(a);
    defer parent.deinit(a);
    const writer: types.Commit = .{ .id = "pending-writer", .expected_metadata_location = parent.metadata_location, .body = "{\"requirements\":[],\"updates\":[{\"action\":\"set-properties\",\"updates\":{\"writer\":\"pending\"}}]}", .timestamp_ms = 2 };
    faults.put_filter = .{ .ptr = &faults, .matches = struct {
        fn matches(_: *anyopaque, _: []const u8, key: []const u8, _: []const u8) bool {
            return std.mem.endsWith(u8, key, "/head.json");
        }
    }.matches };
    faults.next_put = .{ .fail_before = error.ConnectionResetByPeer };
    try std.testing.expectError(error.LakeCommitOutcomeUnknown, catalog.commit(a, writer));
    var proof = try catalog.retire(a, .{ .id = "GC", .expected_metadata_location = parent.metadata_location, .expected_version = parent.version.?, .objects = &.{"gs://archive/hn/data/dead.parquet"} });
    defer proof.deinit(a);
    try std.testing.expectError(error.LakeCommitConflict, catalog.commit(a, writer));

    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var current = try catalog.load(a);
    defer current.deinit(a);
    const row = try metadata.parse(scratch, "{\"title\":\"live\"}");
    const files: @import("row_commit.zig").Files = .{ .client = memory.client(), .bucket = "archive", .prefix = "hn", .uri = "gs://archive/hn" };
    const built = try @import("row_commit.zig").prepare(scratch, current, files, .{ .batch_id = "batch", .source = "source", .epoch = "epoch", .checkpoint = "one", .key_fields = &.{"title"}, .changes = &.{.{ .op = .upsert, .row = row }} }, 10, 3);
    var published = try catalog.commit(a, .{ .id = "publish", .expected_metadata_location = current.metadata_location, .body = built.body, .timestamp_ms = 3 });
    defer published.deinit(a);
    var live = try catalog.load(a);
    defer live.deinit(a);
    const root = try metadata.parse(scratch, live.metadata_json);
    const list_uri = try metadata.str(try metadata.get((try metadata.get(root, "snapshots")).array.items[0], "manifest-list"));
    const list_bytes = try @import("row_commit.zig").read(scratch, files, list_uri);
    const list = try @import("../iceberg_avro.zig").parseManifestListAlloc(scratch, list_bytes);
    const manifest_bytes = try @import("row_commit.zig").read(scratch, files, list.entries[0].manifest_path);
    const manifest = try @import("../iceberg_avro.zig").parseDataManifestAlloc(scratch, manifest_bytes);
    const data_uri = manifest.entries[0].file_path;
    try std.testing.expectError(error.LakeObjectStillReferenced, catalog.retire(a, .{ .id = "unsafe-GC", .expected_metadata_location = live.metadata_location, .expected_version = live.version.?, .objects = &.{data_uri} }));
    const snapshot_id = try metadata.int(try metadata.get((try metadata.get(root, "snapshots")).array.items[0], "snapshot-id"));
    const statistics_body = try std.json.Stringify.valueAlloc(scratch, .{ .requirements = .{}, .updates = .{.{ .action = "set-statistics", .statistics = .{ .@"snapshot-id" = snapshot_id, .@"statistics-path" = "gs://archive/hn/data/dead.parquet", .@"file-size-in-bytes" = @as(u64, 1), .@"file-footer-size-in-bytes" = @as(u64, 1), .@"blob-metadata" = .{} } }} }, .{});
    try std.testing.expectError(error.LakeObjectRetired, catalog.commit(a, .{ .id = "statistics-resurrection", .expected_metadata_location = live.metadata_location, .body = statistics_body, .timestamp_ms = 4 }));
}
