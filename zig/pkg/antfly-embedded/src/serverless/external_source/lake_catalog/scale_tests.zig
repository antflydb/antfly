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

//! Catalog cost as history grows: bounded metadata regressions plus an
//! opt-in commit throughput benchmark for memory, filesystem, S3 and GCS.
const std = @import("std");
const storage = @import("objectstore");
const platform = @import("antfly_platform");
const types = @import("types.zig");
const managed = @import("managed.zig");
const metadata = @import("metadata.zig");
const row_commit = @import("row_commit.zig");
const iceberg = @import("../iceberg_avro.zig");
const OpenedObjectStore = @import("../../object_store_support.zig").OpenedObjectStore;
const A = std.mem.Allocator;
const V = std.json.Value;

const schema_request = "{\"schema\":{\"type\":\"struct\",\"schema-id\":0,\"fields\":[{\"id\":1,\"name\":\"id\",\"type\":\"long\",\"required\":true},{\"id\":2,\"name\":\"body\",\"type\":\"string\",\"required\":false}]}}";

test "lake catalog metadata log keeps the newest previous versions" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var bytes: []const u8 = try metadata.createAlloc(a, "{\"schema\":{\"type\":\"struct\",\"schema-id\":0,\"fields\":[{\"id\":1,\"name\":\"id\",\"type\":\"long\",\"required\":true}]},\"properties\":{\"write.metadata.previous-versions-max\":\"3\"}}", "00000000-0000-0000-0000-000000000000", 1, "gs://archive/t");
    var location: []const u8 = "gs://archive/t/metadata/v0.metadata.json";
    for (1..10) |i| {
        const body = try std.fmt.allocPrint(a, "{{\"requirements\":[],\"updates\":[{{\"action\":\"set-properties\",\"updates\":{{\"n\":\"{d}\"}}}}]}}", .{i});
        bytes = try metadata.applyAlloc(a, bytes, location, .{ .id = try std.fmt.allocPrint(a, "c{d}", .{i}), .expected_metadata_location = location, .body = body, .timestamp_ms = @intCast(i + 1) });
        location = try std.fmt.allocPrint(a, "gs://archive/t/metadata/v{d}.metadata.json", .{i});
    }
    const root = try metadata.parse(a, bytes);
    const log = (try metadata.get(root, "metadata-log")).array.items;
    try std.testing.expectEqual(@as(usize, 3), log.len);
    // The newest entry is the location the final commit replaced.
    try std.testing.expectEqualStrings("gs://archive/t/metadata/v8.metadata.json", try metadata.str(try metadata.get(log[2], "metadata-file")));
    try std.testing.expectEqualStrings("gs://archive/t/metadata/v6.metadata.json", try metadata.str(try metadata.get(log[0], "metadata-file")));
    // Malformed bounds fail closed instead of silently disabling retention.
    var invalid = try metadata.parse(a, bytes);
    try invalid.object.getPtr("properties").?.object.put(a, metadata.previous_versions_max_property, .{ .string = "invalid" });
    try std.testing.expectError(error.InvalidLakeMetadata, metadata.previousVersionsMax(invalid));
}

/// One table driven by native row commits, as the WAL drain produces them.
const Table = struct {
    catalog: managed.Managed,
    files: row_commit.Files,
    current: types.Table,
    commits: usize = 0,
    fn init(a: A, client: storage.Client, bucket: []const u8, prefix: []const u8, source_uri: []const u8, files_uri: []const u8) !Table {
        const catalog: managed.Managed = .{ .client = client, .bucket = bucket, .prefix = prefix, .source_uri = source_uri };
        return .{ .catalog = catalog, .files = .{ .client = client, .bucket = bucket, .prefix = prefix, .uri = files_uri }, .current = try catalog.create(a, "create", schema_request, 1) };
    }
    fn deinit(self: *Table, a: A) void {
        self.current.deinit(a);
    }
    /// Commit one single-row CDC transaction whose checkpoint chains to the last.
    fn commitRow(self: *Table, a: A, timestamp_ms: i64) !void {
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const s = arena.allocator();
        const n = self.commits + 1;
        const row = try std.json.parseFromSliceLeaky(V, s, try std.fmt.allocPrint(s, "{{\"id\":{d},\"body\":\"row {d}\"}}", .{ n % 97, n }), .{});
        const batch: row_commit.Batch = .{
            .batch_id = try std.fmt.allocPrint(s, "b{d}", .{n}),
            .source = "bench",
            .epoch = "1",
            .checkpoint = try std.fmt.allocPrint(s, "{d}", .{n}),
            .expected_checkpoint = if (n == 1) null else try std.fmt.allocPrint(s, "{d}", .{n - 1}),
            .key_fields = &.{"id"},
            .changes = &.{.{ .op = .upsert, .row = row }},
        };
        const prepared = try row_commit.prepare(s, self.current, self.files, batch, n, timestamp_ms);
        const next = try self.catalog.commit(a, .{ .id = batch.batch_id, .expected_metadata_location = self.current.metadata_location, .body = prepared.body, .timestamp_ms = timestamp_ms });
        self.current.deinit(a);
        self.current = next;
        self.commits = n;
    }
    /// Expire all but the newest `keep` snapshots, as vacuum's catalog step does.
    fn expire(self: *Table, a: A, keep: usize, timestamp_ms: i64) !usize {
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const s = arena.allocator();
        const root = try metadata.parse(s, self.current.metadata_json);
        const snapshots = (try metadata.get(root, "snapshots")).array.items;
        if (snapshots.len <= keep) return 0;
        var ids: std.ArrayList(i64) = .empty;
        for (snapshots[0 .. snapshots.len - keep]) |snapshot| try ids.append(s, try metadata.int(try metadata.get(snapshot, "snapshot-id")));
        const body = try std.json.Stringify.valueAlloc(s, .{ .requirements = .{.{ .type = "assert-ref-snapshot-id", .ref = "main", .@"snapshot-id" = try metadata.get(root, "current-snapshot-id") }}, .updates = .{.{ .action = "remove-snapshots", .@"snapshot-ids" = ids.items }} }, .{});
        const next = try self.catalog.commit(a, .{ .id = try std.fmt.allocPrint(s, "expire-{d}", .{self.commits}), .expected_metadata_location = self.current.metadata_location, .body = body, .timestamp_ms = timestamp_ms });
        self.current.deinit(a);
        self.current = next;
        return ids.items.len;
    }
    fn counts(self: Table, a: A) !struct { snapshots: usize, metadata_log: usize } {
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const root = try metadata.parse(arena.allocator(), self.current.metadata_json);
        return .{ .snapshots = (try metadata.get(root, "snapshots")).array.items.len, .metadata_log = (try metadata.get(root, "metadata-log")).array.items.len };
    }
};

test "lake catalog row commits keep metadata bounded with snapshot expiry" {
    const alloc = std.testing.allocator;
    var memory = storage.MemoryClient.init(alloc);
    defer memory.deinit();
    var table = try Table.init(alloc, memory.client(), "archive", "events", "gs://archive/events", "gs://archive/events");
    defer table.deinit(alloc);
    var midpoint: usize = 0;
    for (1..241) |i| {
        try table.commitRow(alloc, @intCast(i + 1));
        if (i % 40 == 0) _ = try table.expire(alloc, 4, @intCast(i + 1));
        if (i == 120) midpoint = table.current.metadata_json.len;
    }
    const final = try table.counts(alloc);
    try std.testing.expect(final.snapshots <= 4);
    try std.testing.expectEqual(metadata.default_previous_versions_max, final.metadata_log);
    // Metadata size is a function of retained state, not total commit count.
    try std.testing.expect(table.current.metadata_json.len <= midpoint + midpoint / 4);
}

fn currentManifests(a: A, table: Table) !iceberg.ManifestList {
    const root = try metadata.parse(a, table.current.metadata_json);
    const current = try metadata.int(try metadata.get(root, "current-snapshot-id"));
    for ((try metadata.get(root, "snapshots")).array.items) |snapshot| {
        if (try metadata.int(try metadata.get(snapshot, "snapshot-id")) != current) continue;
        return iceberg.parseManifestListAlloc(a, try row_commit.read(a, table.files, try metadata.str(try metadata.get(snapshot, "manifest-list"))));
    }
    return error.InvalidLakeMetadata;
}

test "lake catalog row commits merge small manifests and keep every file sequence" {
    const alloc = std.testing.allocator;
    var memory = storage.MemoryClient.init(alloc);
    defer memory.deinit();
    var table = try Table.init(alloc, memory.client(), "archive", "events", "gs://archive/events", "gs://archive/events");
    defer table.deinit(alloc);
    const commits = 2 * row_commit.manifest_merge_min_count + 17;
    for (1..commits + 1) |i| try table.commitRow(alloc, @intCast(i + 1));
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const list = try currentManifests(a, table);
    // Without merging the list would hold one data and one delete manifest per commit.
    try std.testing.expect(list.entries.len <= 2 * (row_commit.manifest_merge_min_count + 1));
    var seen = try a.alloc(u2, commits + 1);
    @memset(seen, 0);
    var merged: usize = 0;
    for (list.entries) |entry| {
        if (std.mem.indexOf(u8, entry.manifest_path, "-merge-") != null) merged += 1;
        const manifest = try iceberg.parseDataManifestAlloc(a, try row_commit.read(a, table.files, entry.manifest_path));
        for (manifest.entries) |raw| {
            if (raw.status == .deleted) continue;
            const file = try @import("../iceberg_inventory.zig").resolveManifestEntry(raw, entry, 2);
            // Sequence N belongs to commit N: one data file and one equality delete.
            const sequence: usize = @intCast(file.data_sequence_number.?);
            try std.testing.expect(sequence >= 1 and sequence <= commits);
            try std.testing.expectEqual(file.data_sequence_number.?, file.file_sequence_number.?);
            seen[sequence] += 1;
            if (file.content == .equality_deletes) try std.testing.expectEqual(@as(i32, 1), file.equality_ids[0]);
        }
    }
    try std.testing.expect(merged >= 2);
    for (seen[1..]) |count| try std.testing.expectEqual(@as(u2, 2), count);
    // Merged data manifests stay below compaction's whole-manifest limit,
    // so small-file compaction can still select them.
    const selection = try @import("compaction.zig").selectResumable(a, table.current, table.files);
    var selected_merged = false;
    for (selection.manifests) |path| selected_merged = selected_merged or std.mem.indexOf(u8, path, "-merge-data") != null;
    try std.testing.expect(selected_merged);
}

fn percentile(sorted: []const u64, p: usize) u64 {
    if (sorted.len == 0) return 0;
    return sorted[@min(sorted.len - 1, (sorted.len * p) / 100)];
}

// Opt-in: ANTFLY_BENCH_LAKE_COMMITS=<n> runs n managed row commits and prints
// latency against history size. ANTFLY_BENCH_LAKE_URI=s3://bucket/prefix,
// gs://bucket/prefix or file:///dir targets a real store (default: memory).
// ANTFLY_BENCH_LAKE_EXPIRE_KEEP=<k> expires to k snapshots every 10% of the run.
test "lake catalog commit throughput benchmark" {
    const total = platform.env.getenvUsize("ANTFLY_BENCH_LAKE_COMMITS") orelse return error.SkipZigTest;
    if (total == 0) return error.SkipZigTest;
    const alloc = std.heap.smp_allocator;
    const keep = platform.env.getenvUsize("ANTFLY_BENCH_LAKE_EXPIRE_KEEP") orelse 0;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    var memory = storage.MemoryClient.init(alloc);
    defer memory.deinit();
    var opened: ?OpenedObjectStore = null;
    defer if (opened) |*value| value.deinit();
    const run_id = platform.time.realtimeNs();
    var table = if (platform.env.getenv("ANTFLY_BENCH_LAKE_URI")) |uri| remote: {
        opened = try OpenedObjectStore.initRemoteUriWithOptions(alloc, uri, "antfly-lake-bench", .{ .ensure_bucket = true });
        const store = opened.?;
        const base = std.mem.trim(u8, store.prefix, "/");
        const prefix = try std.fmt.allocPrint(a, "{s}{s}antfly-lake-commit-bench/{d}", .{ base, if (base.len == 0) "" else "/", run_id });
        const source_uri = if (store.fs_client != null) try std.fmt.allocPrint(a, "file://bench/{d}", .{run_id}) else try std.fmt.allocPrint(a, "{s}/antfly-lake-commit-bench/{d}", .{ std.mem.trimEnd(u8, uri, "/"), run_id });
        const files_uri = if (store.fs_client != null) try std.fmt.allocPrint(a, "object://{s}/{s}", .{ store.bucket, prefix }) else source_uri;
        break :remote try Table.init(alloc, store.client, store.bucket, prefix, source_uri, files_uri);
    } else try Table.init(alloc, memory.client(), "bench", "events", "gs://bench/events", "gs://bench/events");
    defer table.deinit(alloc);
    const bucket_size = @max(1, total / 10);
    var latencies: std.ArrayList(u64) = .empty;
    var bucket_ns: u64 = 0;
    std.debug.print("\nlake catalog commit benchmark: commits={d} expire_keep={d} target={s}\n", .{ total, keep, platform.env.getenv("ANTFLY_BENCH_LAKE_URI") orelse "memory" });
    std.debug.print("{s:>8} {s:>10} {s:>8} {s:>12} {s:>10} {s:>10} {s:>10}\n", .{ "commits", "snapshots", "md_log", "metadata_kib", "p50_ms", "p99_ms", "commit/s" });
    for (1..total + 1) |i| {
        const started = platform.time.monotonicNs();
        try table.commitRow(alloc, @intCast(i + 1));
        const elapsed = platform.time.monotonicNs() - started;
        try latencies.append(a, elapsed);
        bucket_ns += elapsed;
        if (i % bucket_size == 0 or i == total) {
            if (keep != 0) _ = try table.expire(alloc, keep, @intCast(i + 1));
            const sorted = try a.dupe(u64, latencies.items);
            std.mem.sort(u64, sorted, {}, std.sort.asc(u64));
            const state = try table.counts(alloc);
            const rate = if (bucket_ns == 0) 0 else @as(f64, @floatFromInt(latencies.items.len)) * std.time.ns_per_s / @as(f64, @floatFromInt(bucket_ns));
            std.debug.print("{d:>8} {d:>10} {d:>8} {d:>12.1} {d:>10.2} {d:>10.2} {d:>10.1}\n", .{ i, state.snapshots, state.metadata_log, @as(f64, @floatFromInt(table.current.metadata_json.len)) / 1024.0, @as(f64, @floatFromInt(percentile(sorted, 50))) / std.time.ns_per_ms, @as(f64, @floatFromInt(percentile(sorted, 99))) / std.time.ns_per_ms, rate });
            latencies.clearRetainingCapacity();
            bucket_ns = 0;
        }
    }
}

test "lake catalog metadata retention normalizes nonpositive values on every commit" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    for ([_][]const u8{ "0", "-1", "100001", "2147483647" }) |value| {
        const request = try std.fmt.allocPrint(a, "{{\"schema\":{{\"type\":\"struct\",\"schema-id\":0,\"fields\":[]}},\"properties\":{{\"write.metadata.previous-versions-max\":\"{s}\"}}}}", .{value});
        var bytes: []const u8 = try metadata.createAlloc(a, request, "00000000-0000-0000-0000-000000000000", 1, "gs://archive/t");
        for (0..3) |n| {
            const location = try std.fmt.allocPrint(a, "gs://archive/t/metadata/{d}.metadata.json", .{n});
            bytes = try metadata.applyAlloc(a, bytes, location, .{ .id = "update", .expected_metadata_location = location, .body = "{\"requirements\":[],\"updates\":[{\"action\":\"set-properties\",\"updates\":{\"owner\":\"new\"}}]}", .timestamp_ms = @intCast(n + 2) });
        }
        const root = try metadata.parse(a, bytes);
        const count = (try metadata.get(root, "metadata-log")).array.items.len;
        try std.testing.expectEqual(@as(usize, if (value[0] == '0' or value[0] == '-') 1 else 3), count);
    }
}

const AvroReads = struct {
    a: A,
    keys: std.StringHashMapUnmanaged(usize) = .empty,
    fn count(raw: *anyopaque, _: []const u8, key: []const u8) bool {
        const self: *AvroReads = @ptrCast(@alignCast(raw));
        if (std.mem.endsWith(u8, key, ".avro")) {
            const entry = self.keys.getOrPut(self.a, key) catch @panic("allocation failed");
            if (!entry.found_existing) {
                entry.key_ptr.* = self.a.dupe(u8, key) catch @panic("allocation failed");
                entry.value_ptr.* = 0;
            }
            entry.value_ptr.* += 1;
        }
        return false; // Observe every read without consuming the fault filter.
    }
};

fn awaitReferences(a: A, catalog: managed.Managed, objects: []const []const u8) !types.References {
    for (0..2048) |_| {
        var result = try catalog.references(a, objects);
        if (result.ready) return result;
        result.deinit(a);
    }
    return error.TestUnexpectedResult;
}

test "lake catalog reachability resumes cold after lost checkpoint ACK and reuses immutable evidence" {
    const alloc = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    var memory = storage.MemoryClient.init(alloc);
    defer memory.deinit();
    var faults = storage.ScriptedFaultClient.init(alloc, memory.client());
    defer faults.deinit();
    var table = try Table.init(alloc, faults.client(), "archive", "events", "gs://archive/events", "gs://archive/events");
    defer table.deinit(alloc);
    for (1..21) |n| try table.commitRow(alloc, @intCast(n + 1));
    const before = try metadata.parse(a, table.current.metadata_json);
    const old_list = try metadata.str(try metadata.get((try metadata.get(before, "snapshots")).array.items[0], "manifest-list"));
    var reads: AvroReads = .{ .a = a };
    faults.get_filter = .{ .ptr = &reads, .matches = AvroReads.count };
    faults.put_filter = .{ .ptr = &faults, .matches = struct {
        fn matches(_: *anyopaque, _: []const u8, key: []const u8, _: []const u8) bool {
            return std.mem.endsWith(u8, key, "/reachability/build.json");
        }
    }.matches };
    faults.next_put = .{ .commit_then_fail = error.ConnectionResetByPeer };
    try std.testing.expectError(error.ConnectionResetByPeer, table.catalog.references(alloc, &.{}));

    // Advance the metadata while bootstrap is partial. Completed roots and
    // inventories remain reusable, while the new snapshot must be proved too.
    faults.get_filter = null;
    try table.commitRow(alloc, 22);
    faults.get_filter = .{ .ptr = &reads, .matches = AvroReads.count };
    const dead = "gs://archive/events/data/unreferenced.parquet";
    var proof = try awaitReferences(alloc, table.catalog, &.{ old_list, dead });
    defer proof.deinit(alloc);
    try std.testing.expect(proof.referenced[0]);
    try std.testing.expect(!proof.referenced[1]);
    try std.testing.expectEqual(@as(usize, 63), reads.keys.count()); // 21 lists, 42 manifests.
    var counts = reads.keys.valueIterator();
    while (counts.next()) |count| try std.testing.expectEqual(@as(usize, 1), count.*);

    // Cold catalog handles, small retirements, and retirement-only HEAD
    // advancement never read the unchanged source manifests again.
    for (0..12) |n| {
        var current = try table.catalog.load(alloc);
        defer current.deinit(alloc);
        const uri = try std.fmt.allocPrint(a, "gs://archive/events/data/dead-{d}.parquet", .{n});
        var retired_table = try table.catalog.retire(alloc, .{ .id = try std.fmt.allocPrint(a, "gc-{d}", .{n}), .expected_metadata_location = current.metadata_location, .expected_version = current.version.?, .objects = &.{uri} });
        retired_table.deinit(alloc);
    }
    try std.testing.expectError(error.LakeObjectStillReferenced, table.catalog.retire(alloc, .{ .id = "unsafe", .expected_metadata_location = proof.table.metadata_location, .expected_version = blk: {
        const current = try table.catalog.load(a);
        break :blk current.version.?;
    }, .objects = &.{old_list} }));
    _ = try table.expire(alloc, 1, 23);
    var after = try awaitReferences(alloc, table.catalog, &.{old_list});
    defer after.deinit(alloc);
    try std.testing.expect(!after.referenced[0]);
    // Statistics are independent roots even without a snapshot graph change.
    const stats_uri = "gs://archive/events/statistics/summary.puffin";
    const root = try metadata.parse(a, table.current.metadata_json);
    const stats_body = try std.json.Stringify.valueAlloc(a, .{ .requirements = .{}, .updates = .{.{ .action = "set-statistics", .statistics = .{ .@"snapshot-id" = try metadata.int(try metadata.get(root, "current-snapshot-id")), .@"statistics-path" = stats_uri, .@"file-size-in-bytes" = 1, .@"file-footer-size-in-bytes" = 1, .@"blob-metadata" = .{} } }} }, .{});
    var stats = try table.catalog.commit(alloc, .{ .id = "statistics", .expected_metadata_location = table.current.metadata_location, .body = stats_body, .timestamp_ms = 24 });
    defer stats.deinit(alloc);
    var with_stats = try awaitReferences(alloc, table.catalog, &.{stats_uri});
    defer with_stats.deinit(alloc);
    try std.testing.expect(with_stats.referenced[0]);
    try std.testing.expectError(error.LakeObjectStillReferenced, table.catalog.retire(alloc, .{ .id = "unsafe-statistics", .expected_metadata_location = with_stats.table.metadata_location, .expected_version = with_stats.table.version.?, .objects = &.{stats_uri} }));
    // Root removal rebuilds the exact set from immutable inventories.
    counts = reads.keys.valueIterator();
    while (counts.next()) |count| try std.testing.expectEqual(@as(usize, 1), count.*);
}

test "lake catalog reachability bounds cold large-manifest turns and resumes inside inventory chunks" {
    const alloc = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    var memory = storage.MemoryClient.init(alloc);
    defer memory.deinit();
    var faults = storage.ScriptedFaultClient.init(alloc, memory.client());
    defer faults.deinit();
    var table = try Table.init(alloc, faults.client(), "archive", "events", "gs://archive/events", "gs://archive/events");
    defer table.deinit(alloc);
    const avro = @import("avro_writer.zig");
    const padding = try a.alloc(u8, 2048);
    @memset(padding, 'x');
    var entries: std.ArrayList(V) = .empty;
    for (0..1024) |n| {
        const uri = try std.fmt.allocPrint(a, "gs://archive/events/data/{s}-{d}.parquet", .{ padding, n });
        try entries.append(a, try std.json.parseFromSliceLeaky(V, a, try std.json.Stringify.valueAlloc(a, .{ .status = 1, .snapshot_id = 1, .sequence_number = 1, .file_sequence_number = 1, .data_file = .{ .content = 0, .file_path = uri, .file_format = "PARQUET", .partition = V{ .object = .empty }, .record_count = 1, .file_size_in_bytes = 1 } }, .{}), .{}));
    }
    const manifest = try avro.ocf(a, avro.entry_schema, entries.items, &.{ .{ .key = "partition-spec", .value = "[]" }, .{ .key = "format-version", .value = "2" }, .{ .key = "content", .value = "data" } });
    const manifest_uri = try row_commit.upload(a, table.files, "metadata/large-manifest.avro", manifest);
    const list_entry = try std.json.parseFromSliceLeaky(V, a, try std.json.Stringify.valueAlloc(a, .{ .manifest_path = manifest_uri, .manifest_length = manifest.len, .partition_spec_id = 0, .content = 0, .sequence_number = 1, .min_sequence_number = 1, .added_snapshot_id = 1, .added_files_count = 1024, .existing_files_count = 0, .deleted_files_count = 0, .added_rows_count = 1024, .existing_rows_count = 0, .deleted_rows_count = 0 }, .{}), .{});
    const list = try avro.ocf(a, avro.list_schema, &.{list_entry}, &.{.{ .key = "format-version", .value = "2" }});
    const list_uri = try row_commit.upload(a, table.files, "metadata/large-list.avro", list);
    const body = try std.json.Stringify.valueAlloc(a, .{ .requirements = .{}, .updates = .{
        .{ .action = "add-snapshot", .snapshot = .{ .@"snapshot-id" = 1, .@"sequence-number" = 1, .@"timestamp-ms" = 2, .@"manifest-list" = list_uri, .@"schema-id" = 0, .summary = .{ .operation = "append" } } },
        .{ .action = "set-snapshot-ref", .@"ref-name" = "main", .@"snapshot-id" = 1, .type = "branch" },
    } }, .{});
    var published = try table.catalog.commit(alloc, .{ .id = "large", .expected_metadata_location = table.current.metadata_location, .body = body, .timestamp_ms = 2 });
    defer published.deinit(alloc);
    const Counter = struct {
        avro: usize = 0,
        chunks: usize = 0,
        fn count(raw: *anyopaque, _: []const u8, key: []const u8) bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (std.mem.endsWith(u8, key, ".avro")) self.avro += 1;
            if (std.mem.indexOf(u8, key, "/reachability/chunks/") != null) self.chunks += 1;
            return false;
        }
    };
    var count: Counter = .{};
    faults.get_filter = .{ .ptr = &count, .matches = Counter.count };
    var constrained = table.catalog;
    constrained.context.deadline_ns = platform.time.monotonicNs() + std.time.ns_per_s;
    var yielded = try constrained.references(alloc, &.{manifest_uri});
    defer yielded.deinit(alloc);
    try std.testing.expect(!yielded.ready);
    try std.testing.expectEqual(@as(usize, 0), count.avro);
    var turns: usize = 0;
    while (turns < 64) : (turns += 1) {
        const writes = faults.put_attempts;
        count.chunks = 0;
        var result = try table.catalog.references(alloc, &.{manifest_uri});
        defer result.deinit(alloc);
        try std.testing.expect(faults.put_attempts - writes < 600);
        // One list page plus at most two pages around a file cursor boundary.
        try std.testing.expect(count.chunks <= 3);
        if (result.ready) {
            try std.testing.expect(result.referenced[0]);
            break;
        }
    }
    try std.testing.expect(turns >= 16 and turns < 64);
    try std.testing.expectEqual(@as(usize, 2), count.avro);
    const last = try metadata.str(try metadata.get(try metadata.get(entries.items[1023], "data_file"), "file_path"));
    var ready = try table.catalog.references(alloc, &.{ last, list_uri });
    defer ready.deinit(alloc);
    try std.testing.expect(ready.ready and ready.referenced[0] and ready.referenced[1]);
}

test "lake catalog retirement conflicts when authority advances during cold proof bootstrap" {
    const a = std.testing.allocator;
    var memory = storage.MemoryClient.init(a);
    defer memory.deinit();
    var faults = storage.ScriptedFaultClient.init(a, memory.client());
    defer faults.deinit();
    var table = try Table.init(a, faults.client(), "archive", "events", "gs://archive/events", "gs://archive/events");
    defer table.deinit(a);
    try table.commitRow(a, 2);
    var parent = try table.catalog.load(a);
    defer parent.deinit(a);
    const Advance = struct {
        a: A,
        catalog: managed.Managed,
        location: []const u8,
        advanced: bool = false,
        fn run(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            var next = self.catalog.commit(self.a, .{ .id = "concurrent", .expected_metadata_location = self.location, .body = "{\"requirements\":[],\"updates\":[{\"action\":\"set-properties\",\"updates\":{\"owner\":\"new\"}}]}", .timestamp_ms = 3 }) catch return;
            defer next.deinit(self.a);
            self.advanced = true;
        }
        fn matches(_: *anyopaque, _: []const u8, key: []const u8) bool {
            return std.mem.endsWith(u8, key, ".avro");
        }
    };
    var advance: Advance = .{ .a = a, .catalog = table.catalog, .location = parent.metadata_location };
    faults.get_filter = .{ .ptr = &advance, .matches = Advance.matches };
    faults.next_get = .{ .complete_then_callback = .{ .ptr = &advance, .run = Advance.run } };
    try std.testing.expectError(error.LakeCommitConflict, table.catalog.retire(a, .{ .id = "stale-gc", .expected_metadata_location = parent.metadata_location, .expected_version = parent.version.?, .objects = &.{"gs://archive/events/data/dead.parquet"} }));
    try std.testing.expect(advance.advanced);
    var current = try table.catalog.load(a);
    defer current.deinit(a);
    try std.testing.expect(current.retirement_root == null);
}
