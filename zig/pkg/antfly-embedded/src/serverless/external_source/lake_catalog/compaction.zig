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

//! Bounded whole-manifest rewrite. Input is delete-aware live rows from the
//! exact parent snapshot. Untouched manifests keep their original sequences;
//! equality deletes retire only with retained-sequence proofs; position deletes
//! retire only after all active data manifests are rewritten.
const std = @import("std");
const m = @import("metadata.zig");
const t = @import("types.zig");
const rows = @import("row_commit.zig");
const iceberg = @import("../iceberg_avro.zig");
const avro = @import("avro_writer.zig");
const parquet = @import("parquet_writer.zig");
const A = std.mem.Allocator;
const V = std.json.Value;
pub const Selection = struct {
    manifests: []const []const u8,
    files: []const []const u8,
    all_data: bool,
    rewrites_deletes: bool,
    input_rows: u64,
    input_bytes: u64,
};
fn json(a: A, value: anytype) !V {
    return std.json.parseFromSliceLeaky(V, a, try std.json.Stringify.valueAlloc(a, value, .{}), .{});
}
fn selected(paths: []const []const u8, path: []const u8) bool {
    for (paths) |candidate| if (std.mem.eql(u8, candidate, path)) return true;
    return false;
}
pub fn currentList(a: A, table: t.Table, files: rows.Files) !iceberg.ManifestList {
    const root = try m.parse(a, table.metadata_json);
    const parent = root.object.get("current-snapshot-id") orelse return .{ .entries = &.{} };
    if (parent == .null or try m.int(parent) == -1) return .{ .entries = &.{} };
    for ((try m.get(root, "snapshots")).array.items) |snapshot| {
        if (try m.int(try m.get(snapshot, "snapshot-id")) == try m.int(parent)) {
            return iceberg.parseManifestListAlloc(a, try rows.read(a, files, try m.str(try m.get(snapshot, "manifest-list"))));
        }
    }
    return error.InvalidLakeMetadata;
}
/// A pass admits whole small data manifests, never a prefix of a manifest.
/// Row/byte limits include physical rows before delete filtering.
pub fn select(a: A, table: t.Table, files: rows.Files, max_rows: u64, max_bytes: u64) !Selection {
    if (max_rows == 0 or max_rows > 16384 or max_bytes == 0 or max_bytes > 32 * 1024 * 1024) return error.InvalidLakeMaintenanceLimits;
    return selectBounded(a, table, files, max_rows, max_bytes, 32);
}
/// Select a whole manifest independently of the per-turn scan/output budget.
/// The durable coordinator rewrites its files over many bounded turns.
pub fn selectResumable(a: A, table: t.Table, files: rows.Files) !Selection {
    return selectBounded(a, table, files, std.math.maxInt(u64), std.math.maxInt(u64), 32);
}
fn selectBounded(a: A, table: t.Table, files: rows.Files, max_rows: u64, max_bytes: u64, max_manifests: usize) !Selection {
    const list = try currentList(a, table, files);
    var manifests: std.ArrayList([]const u8) = .empty;
    var paths: std.ArrayList([]const u8) = .empty;
    var count: u64 = 0;
    var size: u64 = 0;
    var all = true;
    var rewrites_deletes = false;
    var max_delete_sequence: i64 = 0;
    for (list.entries) |entry| if (entry.content == .deletes) {
        max_delete_sequence = @max(max_delete_sequence, entry.sequence_number orelse 0);
    };
    // Rewrite the oldest sequences first so bounded passes eventually retire
    // equality deletes even when the full archive cannot fit in one job.
    std.mem.sort(iceberg.ManifestListEntry, list.entries, {}, struct {
        fn less(_: void, l: iceberg.ManifestListEntry, r: iceberg.ManifestListEntry) bool {
            const ls = l.min_sequence_number orelse 0;
            const rs = r.min_sequence_number orelse 0;
            if (ls != rs) return ls < rs;
            return l.added_rows_count +| l.existing_rows_count < r.added_rows_count +| r.existing_rows_count;
        }
    }.less);
    for (list.entries) |entry| {
        try files.context.ensureActive();
        if (entry.content != .data) continue;
        // Large manifests remain immutable; another pass/operator can split
        // them. Never allocate an unbounded decoded manifest to select a job.
        if (entry.manifest_length > 4 * 1024 * 1024 or manifests.items.len == max_manifests) {
            all = false;
            continue;
        }
        const manifest = try iceberg.parseDataManifestAlloc(a, try rows.read(a, files, entry.manifest_path));
        var records: u64 = 0;
        var bytes: u64 = 0;
        for (manifest.entries) |file| if (file.status != .deleted) {
            if (file.content != .data) return error.InvalidIcebergDataManifest;
            records = try std.math.add(u64, records, file.record_count);
            bytes = try std.math.add(u64, bytes, file.file_size_in_bytes);
        };
        if (records > max_rows - count or bytes > max_bytes - size) {
            all = false;
            continue;
        }
        rewrites_deletes = rewrites_deletes or (entry.min_sequence_number orelse 0) < max_delete_sequence;
        try manifests.append(a, entry.manifest_path);
        for (manifest.entries) |file| if (file.status != .deleted) try paths.append(a, file.file_path);
        count += records;
        size += bytes;
    }
    if (all and paths.items.len == 0 and max_delete_sequence > 0) rewrites_deletes = true;
    return .{ .manifests = manifests.items, .files = paths.items, .all_data = all, .rewrites_deletes = rewrites_deletes, .input_rows = count, .input_bytes = size };
}
pub const Output = struct { uri: []const u8, rows: u64, bytes: u64 };
pub fn writeOutput(a: A, table: t.Table, files: rows.Files, operation: []const u8, part: u64, live: []const V) !Output {
    const root = try m.parse(a, table.metadata_json);
    const data = try parquet.encode(a, try rows.schema(a, root), live, files.context);
    const uri = try rows.upload(a, files, try std.fmt.allocPrint(a, "data/antfly-compact-{s}-{d}-{s}.parquet", .{ t.digestHex(operation), part, t.digestHex(data) }), data);
    return .{ .uri = uri, .rows = live.len, .bytes = data.len };
}
pub fn prepare(a: A, table: t.Table, files: rows.Files, selection: Selection, live: []const V, timestamp: i64) !rows.Prepared {
    if (live.len > selection.input_rows) return error.InvalidLakeCompaction;
    const operation = try std.json.Stringify.valueAlloc(a, .{ "compaction-v1", table.metadata_location, selection.manifests }, .{});
    const output: []const Output = if (live.len == 0) &.{} else &.{try writeOutput(a, table, files, operation, 0, live)};
    return prepareOutputs(a, table, files, selection, output, timestamp);
}
/// Final publication only occurs after the complete selected input was scanned.
/// Output pages are immutable; retries retain the original parent requirement.
pub fn prepareOutputs(a: A, table: t.Table, files: rows.Files, selection: Selection, outputs: []const Output, timestamp: i64) !rows.Prepared {
    if (selection.manifests.len == 0 and !selection.all_data) return error.InvalidLakeCompaction;
    var output_rows: u64 = 0;
    for (outputs) |output| output_rows = try std.math.add(u64, output_rows, output.rows);
    if (output_rows > selection.input_rows) return error.InvalidLakeCompaction;
    const root = try m.parse(a, table.metadata_json);
    const parent = try m.int(try m.get(root, "current-snapshot-id"));
    const previous_sequence = try m.int(try m.get(root, "last-sequence-number"));
    if (previous_sequence < 0 or previous_sequence == std.math.maxInt(i64)) return error.InvalidLakeMetadata;
    const sequence = previous_sequence + 1;
    const identity = try std.json.Stringify.valueAlloc(a, .{ "compaction-v1", table.metadata_location, selection.manifests }, .{});
    const digest = t.digestHex(identity);
    var hash: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(identity, &hash, .{});
    const snapshot: i64 = @intCast((std.mem.readInt(u64, hash[0..8], .little) & std.math.maxInt(i64)) | 1);
    const s = try rows.schema(a, root);
    var updates: std.ArrayList(V) = .empty;
    var manifests: std.ArrayList(V) = .empty;
    var spec_id: i32 = -1;
    var max_spec: i32 = 0;
    for ((try m.get(root, "partition-specs")).array.items) |spec| {
        const id: i32 = @intCast(try m.int(try m.get(spec, "spec-id")));
        max_spec = @max(max_spec, id);
        if ((try m.get(spec, "fields")).array.items.len == 0) spec_id = id;
    }
    if (spec_id < 0) {
        spec_id = std.math.add(i32, max_spec, 1) catch return error.InvalidLakeMetadata;
        try updates.append(a, try json(a, .{ .action = "add-spec", .spec = .{ .@"spec-id" = spec_id, .fields = @as([]const V, &.{}) } }));
    }
    const previous = try currentList(a, table, files);
    var min_retained_sequence = sequence;
    for (previous.entries) |entry| if (entry.content == .data and !selected(selection.manifests, entry.manifest_path)) {
        min_retained_sequence = @min(min_retained_sequence, entry.min_sequence_number orelse 0);
    };
    var removed: usize = 0;
    for (previous.entries) |entry| {
        if (selected(selection.manifests, entry.manifest_path)) {
            if (entry.content != .data) return error.InvalidLakeCompaction;
            removed += 1;
            continue;
        }
        if (entry.content == .deletes) {
            if (selection.all_data) continue;
            if (entry.manifest_length <= 4 * 1024 * 1024) {
                const deletes = try iceberg.parseDataManifestAlloc(a, try rows.read(a, files, entry.manifest_path));
                var obsolete = true;
                for (deletes.entries) |file| if (file.status != .deleted) {
                    const resolved = try @import("../iceberg_inventory.zig").resolveManifestEntry(file, entry, 2);
                    if (resolved.content != .equality_deletes or resolved.data_sequence_number.? > min_retained_sequence) obsolete = false;
                };
                if (obsolete) continue;
            }
        }
        // A false all_data claim could resurrect tombstoned archive rows.
        if (selection.all_data and entry.content == .data and entry.added_files_count +| entry.existing_files_count != 0) return error.InvalidLakeCompaction;
        try manifests.append(a, try rows.listEntry(a, entry));
    }
    if (removed != selection.manifests.len) return error.InvalidLakeCompaction;
    var part: usize = 0;
    while (part < outputs.len) {
        const end = @min(outputs.len, part + 128);
        var records: std.ArrayList(V) = .empty;
        var count: u64 = 0;
        for (outputs[part..end]) |output| {
            if (output.rows == 0 or output.bytes == 0) return error.InvalidLakeCompaction;
            count = try std.math.add(u64, count, output.rows);
            try records.append(a, try json(a, .{ .status = 1, .snapshot_id = snapshot, .sequence_number = sequence, .file_sequence_number = sequence, .data_file = .{ .content = 0, .file_path = output.uri, .file_format = "PARQUET", .partition = V{ .object = .empty }, .record_count = output.rows, .file_size_in_bytes = output.bytes } }));
        }
        const manifest = try avro.ocf(a, avro.entry_schema, records.items, &.{ .{ .key = "schema", .value = try std.json.Stringify.valueAlloc(a, s, .{}) }, .{ .key = "partition-spec", .value = "[]" }, .{ .key = "partition-spec-id", .value = try std.fmt.allocPrint(a, "{d}", .{spec_id}) }, .{ .key = "format-version", .value = "2" }, .{ .key = "content", .value = "data" } });
        const path = try rows.upload(a, files, try std.fmt.allocPrint(a, "metadata/antfly-compact-{s}-{d}.avro", .{ digest, part }), manifest);
        try manifests.append(a, try json(a, .{ .manifest_path = path, .manifest_length = manifest.len, .partition_spec_id = spec_id, .content = 0, .sequence_number = sequence, .min_sequence_number = sequence, .added_snapshot_id = snapshot, .added_files_count = records.items.len, .existing_files_count = 0, .deleted_files_count = 0, .added_rows_count = count, .existing_rows_count = 0, .deleted_rows_count = 0 }));
        part = end;
    }
    const list = try avro.ocf(a, avro.list_schema, manifests.items, &.{ .{ .key = "format-version", .value = "2" }, .{ .key = "snapshot-id", .value = try std.fmt.allocPrint(a, "{d}", .{snapshot}) }, .{ .key = "sequence-number", .value = try std.fmt.allocPrint(a, "{d}", .{sequence}) } });
    const path = try rows.upload(a, files, try std.fmt.allocPrint(a, "metadata/antfly-compact-{s}-list.avro", .{digest}), list);
    try updates.append(a, try json(a, .{ .action = "add-snapshot", .snapshot = .{ .@"snapshot-id" = snapshot, .@"parent-snapshot-id" = parent, .@"sequence-number" = sequence, .@"timestamp-ms" = timestamp, .@"manifest-list" = path, .@"schema-id" = try m.int(try m.get(root, "current-schema-id")), .summary = .{ .operation = "replace", .@"antfly.compaction" = &digest, .@"antfly.wal.coverage" = (try m.get(root, "properties")).object.get("antfly.wal.coverage") orelse V{ .string = "0" } } } }));
    try updates.append(a, try json(a, .{ .action = "set-snapshot-ref", .@"ref-name" = "main", .@"snapshot-id" = snapshot, .type = "branch" }));
    return .{ .snapshot_id = snapshot, .body = try std.json.Stringify.valueAlloc(a, .{ .requirements = .{ .{ .type = "assert-ref-snapshot-id", .ref = "main", .@"snapshot-id" = parent }, .{ .type = "assert-current-schema-id", .@"current-schema-id" = try m.int(try m.get(root, "current-schema-id")) } }, .updates = updates.items }, .{}) };
}

test "lake compaction bounded passes retain sequences and retire only obsolete equality deletes" {
    const objectstore = @import("objectstore");
    const alloc = std.testing.allocator;
    var memory = objectstore.MemoryClient.init(alloc);
    defer memory.deinit();
    var authority: @import("managed.zig").Managed = .{ .client = memory.client(), .bucket = "archive", .prefix = "hn", .source_uri = "s3://archive/hn" };
    var table = try authority.create(alloc, "init", "{\"schema\":{\"type\":\"struct\",\"schema-id\":0,\"fields\":[{\"id\":1,\"name\":\"id\",\"type\":\"long\",\"required\":true}]}}", 1);
    defer table.deinit(alloc);
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const files: rows.Files = .{ .client = memory.client(), .bucket = "archive", .prefix = "hn", .uri = "s3://archive/hn" };
    for (1..4) |n| {
        const id = try std.fmt.allocPrint(a, "b{d}", .{n});
        const image = try json(a, .{ .id = n });
        const batch: rows.Batch = .{ .batch_id = id, .source = "hook", .epoch = "1", .checkpoint = id, .expected_checkpoint = if (n == 1) null else try std.fmt.allocPrint(a, "b{d}", .{n - 1}), .key_fields = &.{"id"}, .changes = &.{.{ .op = .upsert, .row = image }} };
        const prepared = try rows.prepare(a, table, files, batch, n, @intCast(n + 1));
        const committed = try authority.commit(alloc, .{ .id = id, .expected_metadata_location = table.metadata_location, .body = prepared.body, .timestamp_ms = @intCast(n + 1) });
        table.deinit(alloc);
        table = committed;
    }
    for (1..3) |n| {
        const selection = try select(a, table, files, 1, 32 * 1024 * 1024);
        try std.testing.expect(!selection.all_data);
        try std.testing.expect(selection.rewrites_deletes);
        try std.testing.expectEqual(@as(u64, 1), selection.input_rows);
        const live = try json(a, .{ .id = n });
        const prepared = try prepare(a, table, files, selection, &.{live}, @intCast(n + 10));
        const committed = try authority.commit(alloc, .{ .id = try std.fmt.allocPrint(a, "compact{d}", .{n}), .expected_metadata_location = table.metadata_location, .body = prepared.body, .timestamp_ms = @intCast(n + 10) });
        table.deinit(alloc);
        table = committed;
        const list = try currentList(a, table, files);
        var deletes: usize = 0;
        for (list.entries) |entry| if (entry.content == .deletes) {
            deletes += 1;
            try std.testing.expectEqual(@as(?i64, 3), entry.sequence_number);
        };
        try std.testing.expectEqual(@as(usize, if (n == 1) 1 else 0), deletes);
        const root = try m.parse(a, table.metadata_json);
        try std.testing.expectEqualStrings("b3", try m.str(try m.get(try m.get(root, "properties"), "antfly.cdc.checkpoint")));
        try std.testing.expectEqualStrings("3", try m.str(try m.get(try m.get(root, "properties"), "antfly.wal.coverage")));
    }
}

test "lake compaction resumable selection and output manifests exceed turn limits safely" {
    const objectstore = @import("objectstore");
    const alloc = std.testing.allocator;
    var memory = objectstore.MemoryClient.init(alloc);
    defer memory.deinit();
    var authority: @import("managed.zig").Managed = .{ .client = memory.client(), .bucket = "archive", .prefix = "hn", .source_uri = "s3://archive/hn" };
    var table = try authority.create(alloc, "init", "{\"schema\":{\"type\":\"struct\",\"schema-id\":0,\"fields\":[{\"id\":1,\"name\":\"id\",\"type\":\"long\",\"required\":true}]}}", 1);
    defer table.deinit(alloc);
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const files: rows.Files = .{ .client = memory.client(), .bucket = "archive", .prefix = "hn", .uri = "s3://archive/hn" };
    const images = try a.alloc(V, 129);
    const changes = try a.alloc(rows.Batch.Change, images.len);
    for (images, changes, 0..) |*image, *change, n| {
        image.* = try json(a, .{ .id = n });
        change.* = .{ .op = .upsert, .row = image.* };
    }
    const prepared = try rows.prepare(a, table, files, .{ .batch_id = "initial", .source = "hook", .epoch = "1", .checkpoint = "1", .key_fields = &.{"id"}, .changes = changes }, 1, 2);
    const committed = try authority.commit(alloc, .{ .id = "initial", .expected_metadata_location = table.metadata_location, .body = prepared.body, .timestamp_ms = 2 });
    table.deinit(alloc);
    table = committed;
    const bounded = try select(a, table, files, 1, 1024 * 1024);
    try std.testing.expectEqual(@as(u64, 0), bounded.input_rows);
    const selection = try selectResumable(a, table, files);
    try std.testing.expectEqual(@as(u64, 129), selection.input_rows);
    const outputs = try a.alloc(Output, images.len);
    for (outputs, images, 0..) |*output, image, n| output.* = try writeOutput(a, table, files, "resumable", n, &.{image});
    const rewritten = try prepareOutputs(a, table, files, selection, outputs, 3);
    const next = try authority.commit(alloc, .{ .id = "resumable", .expected_metadata_location = table.metadata_location, .body = rewritten.body, .timestamp_ms = 3 });
    table.deinit(alloc);
    table = next;
    const list = try currentList(a, table, files);
    var total: u64 = 0;
    var data_manifests: usize = 0;
    for (list.entries) |entry| if (entry.content == .data) {
        data_manifests += 1;
        const manifest = try iceberg.parseDataManifestAlloc(a, try rows.read(a, files, entry.manifest_path));
        try std.testing.expect(manifest.entries.len <= 128);
        for (manifest.entries) |file| total += file.record_count;
    };
    try std.testing.expectEqual(@as(usize, 2), data_manifests);
    try std.testing.expectEqual(@as(u64, 129), total);
}
