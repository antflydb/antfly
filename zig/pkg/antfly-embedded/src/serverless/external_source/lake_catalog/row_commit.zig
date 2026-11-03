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

//! Immutable native row-delta construction. Data and equality deletes share
//! one sequence number, so deletes suppress older data without deleting the
//! replacement rows. Catalog CAS is the only publication authority.
const std = @import("std");
const objectstore = @import("objectstore");
const m = @import("metadata.zig");
const types = @import("types.zig");
const parquet = @import("parquet_writer.zig");
const avro = @import("avro_writer.zig");
const iceberg = @import("../iceberg_avro.zig");
const A = std.mem.Allocator;
const V = std.json.Value;
pub const Batch = struct {
    batch_id: []const u8,
    source: []const u8,
    epoch: []const u8,
    checkpoint: []const u8,
    expected_checkpoint: ?[]const u8 = null,
    key_fields: []const []const u8,
    changes: []const Change,
    pub const Change = struct { op: enum { upsert, delete }, row: V };
};
pub const Files = struct { client: objectstore.Client, bucket: []const u8, prefix: []const u8, uri: []const u8, context: types.Context = .{} };
pub const Prepared = struct { body: []const u8, snapshot_id: i64 };
fn object(a: A) V {
    _ = a;
    return .{ .object = .empty };
}
fn put(a: A, v: *V, key: []const u8, value: V) !void {
    try v.object.put(a, key, value);
}
fn integer(n: i64) V {
    return .{ .integer = n };
}
fn string(s: []const u8) V {
    return .{ .string = s };
}
fn array(a: A) V {
    return .{ .array = std.json.Array.init(a) };
}
fn append(v: *V, item: V) !void {
    try v.array.append(item);
}
fn json(a: A, v: anytype) !V {
    const bytes = try std.json.Stringify.valueAlloc(a, v, .{});
    return std.json.parseFromSliceLeaky(V, a, bytes, .{});
}
pub fn schema(a: A, metadata: V) !V {
    _ = a;
    const id = try m.int(try m.get(metadata, "current-schema-id"));
    for ((try m.get(metadata, "schemas")).array.items) |s| if (try m.int(try m.get(s, "schema-id")) == id) return s;
    return error.InvalidLakeMetadata;
}
pub const stable_key = @import("stable_key.zig");
const Rows = struct { data: []const V, keys: []const V, key_schema: V, ids: V };
fn normalize(a: A, s: V, batch: Batch, context: types.Context) !Rows {
    if (batch.batch_id.len == 0 or batch.batch_id.len > 256 or batch.source.len == 0 or batch.source.len > 256 or batch.epoch.len == 0 or batch.epoch.len > 256 or batch.checkpoint.len == 0 or batch.checkpoint.len > 1024 or batch.key_fields.len == 0 or batch.key_fields.len > 32 or batch.changes.len == 0 or batch.changes.len > parquet.max_rows) return error.InvalidLakeChangeBatch;
    if (batch.expected_checkpoint) |previous| {
        if (previous.len > 1024 or std.mem.eql(u8, previous, batch.checkpoint)) return error.InvalidLakeChangeBatch;
    }
    var fields = array(a);
    var ids = array(a);
    for (batch.key_fields, 0..) |name, index| {
        for (batch.key_fields[0..index]) |previous| if (std.mem.eql(u8, name, previous)) return error.InvalidLakeChangeBatch;
        var found = false;
        for ((try m.get(s, "fields")).array.items) |f| if (std.mem.eql(u8, try m.str(try m.get(f, "name")), name)) {
            const kind = try m.str(try m.get(f, "type"));
            _ = kind;
            try append(&fields, f);
            try append(&ids, try m.get(f, "id"));
            found = true;
            break;
        };
        if (!found) return error.InvalidLakeChangeBatch;
    }
    var key_schema = object(a);
    try put(a, &key_schema, "fields", fields);
    var seen: std.StringHashMap(usize) = .init(a);
    var data = array(a);
    var keys = array(a);
    // Keep only the last complete mutation for each key within the transaction.
    // Array slots preserve deterministic encoding across retries.
    for (batch.changes, 0..) |change, index| {
        try context.ensureActive();
        if (change.row != .object) return error.InvalidLakeRow;
        var key = object(a);
        var tuple = array(a);
        for (batch.key_fields, 0..) |name, key_index| {
            var v = change.row.object.get(name) orelse return error.InvalidLakeRow;
            const kind = try m.str(try m.get(fields.array.items[key_index], "type"));
            if (std.mem.eql(u8, kind, "float") or std.mem.eql(u8, kind, "double")) {
                var number: f64 = switch (v) {
                    .integer => @floatFromInt(v.integer),
                    .float => v.float,
                    else => return error.InvalidLakeRow,
                };
                if (std.mem.eql(u8, kind, "float")) number = @as(f64, @floatCast(@as(f32, @floatCast(number))));
                if (!std.math.isFinite(number)) return error.InvalidLakeRow;
                v = .{ .float = number };
                change.row.object.getPtr(name).?.* = v;
            }
            if (v == .null) return error.InvalidLakeRow;
            try put(a, &key, name, v);
            try append(&tuple, v);
        }
        if (change.op == .delete and change.row.object.count() != batch.key_fields.len) return error.InvalidLakeRow;
        const encoded_key = try stable_key.identity(a, batch.key_fields, change.row, &.{});
        try seen.put(encoded_key, index);
        try append(&keys, key);
    }
    var unique_keys = array(a);
    for (batch.changes, keys.array.items, 0..) |change, key, index| {
        var tuple = array(a);
        for (batch.key_fields) |name| try append(&tuple, key.object.get(name).?);
        const encoded_key = try stable_key.identity(a, batch.key_fields, change.row, &.{});
        if (seen.get(encoded_key).? != index) continue;
        try append(&unique_keys, key);
        if (change.op == .upsert) try append(&data, change.row);
    }
    // Validate all images, including shadowed mutations, in bounded columnar
    // batches. Encoding one complete footer per row makes admission quadratic
    // in schema width and retains excessive scratch in an attempt arena.
    var all_data = array(a);
    for (batch.changes) |change| if (change.op == .upsert) try append(&all_data, change.row);
    if (all_data.array.items.len != 0) {
        const checked = try parquet.encode(a, s, all_data.array.items, context);
        a.free(checked);
    }
    const checked_keys = try parquet.encode(a, key_schema, keys.array.items, context);
    a.free(checked_keys);
    return .{ .data = data.array.items, .keys = unique_keys.array.items, .key_schema = key_schema, .ids = ids };
}
pub fn validate(a: A, metadata_json: []const u8, batch: Batch, context: types.Context) !void {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const root = try m.parse(scratch, metadata_json);
    _ = try normalize(scratch, try schema(scratch, root), batch, context);
}

pub fn upload(a: A, files: Files, relative: []const u8, bytes: []const u8) ![]const u8 {
    try files.context.ensureActive();
    const key = try std.fmt.allocPrint(a, "{s}{s}{s}", .{ files.prefix, if (files.prefix.len == 0) "" else "/", relative });
    var client = files.client;
    var result = client.putObject(files.bucket, key, bytes, .{ .if_none_match = true, .cancellation = types.contextCancellation(&files.context) }) catch |err| switch (err) {
        error.PreconditionFailed, error.ObjectAlreadyExists => {
            var existing = try client.getObject(files.bucket, key, .{ .skip_metadata_probe = true, .cancellation = types.contextCancellation(&files.context) });
            defer existing.deinit(client.allocator);
            if (!std.mem.eql(u8, existing.body, bytes)) return error.LakeArtifactIdentityConflict;
            return recordOwnership(a, files, relative, bytes, existing.metadata.writeRevision());
        },
        else => return err,
    };
    defer result.deinit(client.allocator);
    return recordOwnership(a, files, relative, bytes, result.writeRevision());
}
fn recordOwnership(a: A, files: Files, relative: []const u8, bytes: []const u8, revision: ?objectstore.ObjectRevision) ![]const u8 {
    const uri = try std.fmt.allocPrint(a, "{s}/{s}", .{ std.mem.trimEnd(u8, files.uri, "/"), relative });
    const key = try std.fmt.allocPrint(a, "{s}{s}.antfly-owned/{s}.json", .{ files.prefix, if (files.prefix.len == 0) "" else "/", types.digestHex(uri) });
    const marker = try std.json.Stringify.valueAlloc(a, .{ .uri = uri, .sha256 = &types.digestHex(bytes), .revision = revision orelse return error.MissingObjectRevision, .owner = "antfly-native-lake-v2" }, .{});
    var client = files.client;
    var stored = client.putObject(files.bucket, key, marker, .{ .if_none_match = true, .cancellation = types.contextCancellation(&files.context) }) catch |err| switch (err) {
        error.PreconditionFailed, error.ObjectAlreadyExists => {
            var existing = try client.getObject(files.bucket, key, .{ .skip_metadata_probe = true, .cancellation = types.contextCancellation(&files.context) });
            defer existing.deinit(client.allocator);
            if (!std.mem.eql(u8, existing.body, marker)) return error.LakeArtifactIdentityConflict;
            return uri;
        },
        else => return err,
    };
    stored.deinit(client.allocator);
    return uri;
}
pub fn read(a: A, files: Files, uri: []const u8) ![]const u8 {
    return readLimited(a, files, uri, 64 * 1024 * 1024);
}
pub fn readLimited(a: A, files: Files, uri: []const u8, max_bytes: usize) ![]const u8 {
    const root = try std.fmt.allocPrint(a, "{s}/", .{std.mem.trimEnd(u8, files.uri, "/")});
    if (!std.mem.startsWith(u8, uri, root)) return error.LakeArtifactOutsideTable;
    const relative = uri[root.len..];
    if (std.mem.indexOf(u8, relative, "..") != null) return error.LakeArtifactOutsideTable;
    const key = try std.fmt.allocPrint(a, "{s}{s}{s}", .{ files.prefix, if (files.prefix.len == 0) "" else "/", relative });
    var client = files.client;
    var result = try client.getObject(files.bucket, key, .{ .cancellation = types.contextCancellation(&files.context), .max_response_bytes = max_bytes });
    defer result.deinit(client.allocator);
    return a.dupe(u8, result.body);
}
pub fn listEntry(a: A, entry: iceberg.ManifestListEntry) !V {
    return json(a, .{ .manifest_path = entry.manifest_path, .manifest_length = entry.manifest_length, .partition_spec_id = entry.partition_spec_id, .content = @backingInt(entry.content), .sequence_number = entry.sequence_number orelse 0, .min_sequence_number = entry.min_sequence_number orelse 0, .added_snapshot_id = entry.added_snapshot_id orelse 0, .added_files_count = entry.added_files_count, .existing_files_count = entry.existing_files_count, .deleted_files_count = entry.deleted_files_count, .added_rows_count = entry.added_rows_count, .existing_rows_count = entry.existing_rows_count, .deleted_rows_count = entry.deleted_rows_count });
}
/// Fast appends add two manifests per commit and carry every earlier manifest
/// forward. Without merging, commit cost and reader planning grow with the
/// number of commits. Once `manifest_merge_min_count` small Antfly-written
/// unpartitioned manifests of one content kind accumulate, they are rewritten
/// as one manifest whose entries keep their original snapshot and sequence
/// numbers with EXISTING status (Iceberg's commit.manifest-merge behavior).
pub const manifest_merge_min_count: usize = 100;
/// Merged manifests stop growing at this size and leave the merge pool. The
/// bound keeps them below compaction's 4 MiB whole-manifest admission limit.
pub const manifest_merge_target_bytes: u64 = 1024 * 1024;
const MergeContext = struct { spec_id: i32, spec_string: []const u8, schema_json: []const u8, snapshot_id: i64, sequence: i64, digest: []const u8 };
fn mergeable(entry: iceberg.ManifestListEntry, spec_id: i32) bool {
    if (entry.partition_spec_id != spec_id or entry.manifest_length >= manifest_merge_target_bytes) return false;
    if (std.mem.indexOf(u8, entry.manifest_path, "/metadata/antfly-") == null) return false;
    return std.mem.endsWith(u8, entry.manifest_path, if (entry.content == .deletes) "-delete.avro" else "-data.avro");
}
fn carryManifests(a: A, files: Files, previous: []const iceberg.ManifestListEntry, manifests: *V, context: MergeContext) !void {
    inline for (.{ iceberg.ManifestContent.data, iceberg.ManifestContent.deletes }) |kind| {
        var pool: std.ArrayList(iceberg.ManifestListEntry) = .empty;
        for (previous) |entry| if (entry.content == kind and mergeable(entry, context.spec_id)) try pool.append(a, entry);
        const merged = if (pool.items.len >= manifest_merge_min_count) try mergeManifests(a, files, pool.items, kind, context) else Merge.unsupported;
        for (previous) |entry| {
            if (entry.content != kind) continue;
            if (merged != .unsupported and mergeable(entry, context.spec_id)) continue;
            try append(manifests, try listEntry(a, entry));
        }
        if (merged == .merged) try append(manifests, merged.merged);
    }
}
const Merge = union(enum) { unsupported, empty, merged: V };
fn mergeManifests(a: A, files: Files, pool: []const iceberg.ManifestListEntry, comptime kind: iceberg.ManifestContent, context: MergeContext) !Merge {
    var records = array(a);
    var rows_count: u64 = 0;
    var min_sequence: i64 = std.math.maxInt(i64);
    for (pool) |manifest_entry| {
        try files.context.ensureActive();
        const manifest = try iceberg.parseDataManifestAlloc(a, try readLimited(a, files, manifest_entry.manifest_path, manifest_merge_target_bytes));
        for (manifest.entries) |raw| {
            if (raw.status == .deleted) continue;
            const file = try @import("../iceberg_inventory.zig").resolveManifestEntry(raw, manifest_entry, 2);
            // This writer never emits partitions or column bounds. Anything
            // richer stays in its original manifest rather than losing stats.
            if (file.partition_field_count != 0 or file.partition_values.len != 0 or file.lower_bounds.len != 0 or file.upper_bounds.len != 0) return .unsupported;
            if ((kind == .deletes) != (file.content != .data)) return .unsupported;
            var data_file = try json(a, .{ .content = @as(i32, @backingInt(file.content)), .file_path = file.file_path, .file_format = file.file_format, .partition = object(a), .record_count = file.record_count, .file_size_in_bytes = file.file_size_in_bytes });
            if (file.equality_ids.len != 0) try put(a, &data_file, "equality_ids", try json(a, file.equality_ids));
            const data_sequence = file.data_sequence_number orelse return .unsupported;
            try append(&records, try json(a, .{ .status = 0, .snapshot_id = file.snapshot_id orelse return .unsupported, .sequence_number = data_sequence, .file_sequence_number = file.file_sequence_number orelse return .unsupported, .data_file = data_file }));
            rows_count = try std.math.add(u64, rows_count, file.record_count);
            min_sequence = @min(min_sequence, data_sequence);
        }
    }
    if (records.array.items.len == 0) return .empty;
    const label = if (kind == .deletes) "delete" else "data";
    const manifest = try avro.ocf(a, avro.entry_schema, records.array.items, &.{ .{ .key = "schema", .value = context.schema_json }, .{ .key = "partition-spec", .value = "[]" }, .{ .key = "partition-spec-id", .value = context.spec_string }, .{ .key = "format-version", .value = "2" }, .{ .key = "content", .value = if (kind == .deletes) "deletes" else "data" } });
    const uri = try upload(a, files, try std.fmt.allocPrint(a, "metadata/antfly-{s}-merge-{s}.avro", .{ context.digest, label }), manifest);
    return .{ .merged = try json(a, .{ .manifest_path = uri, .manifest_length = manifest.len, .partition_spec_id = context.spec_id, .content = if (kind == .deletes) @as(i32, 1) else @as(i32, 0), .sequence_number = context.sequence, .min_sequence_number = min_sequence, .added_snapshot_id = context.snapshot_id, .added_files_count = 0, .existing_files_count = records.array.items.len, .deleted_files_count = 0, .added_rows_count = 0, .existing_rows_count = rows_count, .deleted_rows_count = 0 }) };
}
/// Allocations belong to a caller-owned attempt arena. The request identity
/// includes the pinned metadata location; a confirmed CAS conflict may build a
/// fresh attempt, but an ambiguous commit must resolve before any rebasing.
pub fn prepare(a: A, table: types.Table, files: Files, batch: Batch, wal_lsn: u64, timestamp_ms: i64) !Prepared {
    const root = try m.parse(a, table.metadata_json);
    if (try m.int(try m.get(root, "format-version")) != 2) return error.UnsupportedLakeFormatVersion;
    const s = try schema(a, root);
    const rows = try normalize(a, s, batch, files.context);
    const props = try m.get(root, "properties");
    const key_definition = try std.json.Stringify.valueAlloc(a, batch.key_fields, .{});
    if (props.object.get("antfly.cdc.source")) |owner| {
        if (!std.mem.eql(u8, try m.str(owner), batch.source) or !std.mem.eql(u8, try m.str(try m.get(props, "antfly.cdc.epoch")), batch.epoch) or !std.mem.eql(u8, try m.str(try m.get(props, "antfly.cdc.key-fields")), key_definition)) return error.LakeSourceConflict;
    }
    const current_checkpoint = if (props.object.get("antfly.cdc.checkpoint")) |v| try m.str(v) else null;
    if ((current_checkpoint == null) != (batch.expected_checkpoint == null) or (current_checkpoint != null and !std.mem.eql(u8, current_checkpoint.?, batch.expected_checkpoint.?))) return error.LakeCheckpointConflict;
    const last_seq = try m.int(try m.get(root, "last-sequence-number"));
    if (last_seq < 0 or last_seq == std.math.maxInt(i64)) return error.InvalidLakeMetadata;
    const seq = last_seq + 1;
    const attempt = try std.json.Stringify.valueAlloc(a, .{ batch.batch_id, table.metadata_location, wal_lsn }, .{});
    const digest = types.digestHex(attempt);
    var hash: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(attempt, &hash, .{});
    var snapshot_id: i64 = @intCast(std.mem.readInt(u64, hash[0..8], .little) & std.math.maxInt(i64));
    if (snapshot_id == 0) snapshot_id = 1;
    var updates = array(a);
    var requirements = array(a);
    var manifests = array(a);
    const parent = root.object.get("current-snapshot-id") orelse .null;
    try append(&requirements, try json(a, .{ .type = "assert-ref-snapshot-id", .ref = "main", .@"snapshot-id" = parent }));
    try append(&requirements, try json(a, .{ .type = "assert-current-schema-id", .@"current-schema-id" = try m.int(try m.get(root, "current-schema-id")) }));
    // Use an unpartitioned spec for global key deletes and their replacements;
    // existing partitioned manifests remain intact and keep their own specs.
    var spec_id: i32 = -1;
    var max_spec: i32 = 0;
    for ((try m.get(root, "partition-specs")).array.items) |spec| {
        const id: i32 = @intCast(try m.int(try m.get(spec, "spec-id")));
        max_spec = @max(max_spec, id);
        if ((try m.get(spec, "fields")).array.items.len == 0) spec_id = id;
    }
    if (spec_id < 0) {
        spec_id = max_spec + 1;
        try append(&updates, try json(a, .{ .action = "add-spec", .spec = .{ .@"spec-id" = spec_id, .fields = @as([]const V, &.{}) } }));
    }
    const schema_json = try std.json.Stringify.valueAlloc(a, s, .{});
    const spec_string = try std.fmt.allocPrint(a, "{d}", .{spec_id});
    if (parent != .null and try m.int(parent) != -1) {
        for ((try m.get(root, "snapshots")).array.items) |snapshot| if (try m.int(try m.get(snapshot, "snapshot-id")) == try m.int(parent)) {
            const bytes = try read(a, files, try m.str(try m.get(snapshot, "manifest-list")));
            var previous = try iceberg.parseManifestListAlloc(a, bytes);
            defer previous.deinit(a);
            try carryManifests(a, files, previous.entries, &manifests, .{ .spec_id = spec_id, .spec_string = spec_string, .schema_json = schema_json, .snapshot_id = snapshot_id, .sequence = seq, .digest = &digest });
            break;
        };
    }
    inline for (.{ false, true }) |deletes| {
        const group = if (deletes) rows.keys else rows.data;
        if (group.len != 0) {
            const data = try parquet.encode(a, if (deletes) rows.key_schema else s, group, files.context);
            const kind = if (deletes) "delete" else "data";
            const data_uri = try upload(a, files, try std.fmt.allocPrint(a, "data/antfly-{s}-{s}.parquet", .{ digest, kind }), data);
            var data_file = try json(a, .{ .content = if (deletes) @as(i32, 2) else @as(i32, 0), .file_path = data_uri, .file_format = "PARQUET", .partition = object(a), .record_count = group.len, .file_size_in_bytes = data.len });
            if (deletes) try put(a, &data_file, "equality_ids", rows.ids);
            const entry = try json(a, .{ .status = 1, .snapshot_id = snapshot_id, .sequence_number = seq, .file_sequence_number = seq, .data_file = data_file });
            const manifest = try avro.ocf(a, avro.entry_schema, &.{entry}, &.{ .{ .key = "schema", .value = schema_json }, .{ .key = "partition-spec", .value = "[]" }, .{ .key = "partition-spec-id", .value = spec_string }, .{ .key = "format-version", .value = "2" }, .{ .key = "content", .value = if (deletes) "deletes" else "data" } });
            const manifest_uri = try upload(a, files, try std.fmt.allocPrint(a, "metadata/antfly-{s}-{s}.avro", .{ digest, kind }), manifest);
            try append(&manifests, try json(a, .{ .manifest_path = manifest_uri, .manifest_length = manifest.len, .partition_spec_id = spec_id, .content = if (deletes) @as(i32, 1) else @as(i32, 0), .sequence_number = seq, .min_sequence_number = seq, .added_snapshot_id = snapshot_id, .added_files_count = 1, .existing_files_count = 0, .deleted_files_count = 0, .added_rows_count = group.len, .existing_rows_count = 0, .deleted_rows_count = 0 }));
        }
    }
    const manifest_list = try avro.ocf(a, avro.list_schema, manifests.array.items, &.{ .{ .key = "format-version", .value = "2" }, .{ .key = "snapshot-id", .value = try std.fmt.allocPrint(a, "{d}", .{snapshot_id}) }, .{ .key = "sequence-number", .value = try std.fmt.allocPrint(a, "{d}", .{seq}) } });
    const list_uri = try upload(a, files, try std.fmt.allocPrint(a, "metadata/antfly-{s}-list.avro", .{digest}), manifest_list);
    var snapshot = try json(a, .{ .@"snapshot-id" = snapshot_id, .@"sequence-number" = seq, .@"timestamp-ms" = timestamp_ms, .@"manifest-list" = list_uri, .@"schema-id" = try m.int(try m.get(root, "current-schema-id")), .summary = .{ .operation = "overwrite", .@"antfly.batch-id" = batch.batch_id, .@"antfly.wal.coverage" = try std.fmt.allocPrint(a, "{d}", .{wal_lsn}) } });
    if (parent != .null and try m.int(parent) != -1) try put(a, &snapshot, "parent-snapshot-id", parent);
    try append(&updates, try json(a, .{ .action = "add-snapshot", .snapshot = snapshot }));
    try append(&updates, try json(a, .{ .action = "set-snapshot-ref", .@"ref-name" = "main", .@"snapshot-id" = snapshot_id, .type = "branch" }));
    try append(&updates, try json(a, .{ .action = "set-properties", .updates = .{ .@"antfly.wal.coverage" = try std.fmt.allocPrint(a, "{d}", .{wal_lsn}), .@"antfly.cdc.source" = batch.source, .@"antfly.cdc.epoch" = batch.epoch, .@"antfly.cdc.checkpoint" = batch.checkpoint, .@"antfly.cdc.key-fields" = key_definition } }));
    return .{ .body = try std.json.Stringify.valueAlloc(a, .{ .requirements = requirements, .updates = updates }, .{}), .snapshot_id = snapshot_id };
}

test "lake native row commit builds real data delete and retained manifests with guarded checkpoints" {
    const alloc = std.testing.allocator;
    var memory = objectstore.MemoryClient.init(alloc);
    defer memory.deinit();
    var catalog: @import("managed.zig").Managed = .{ .client = memory.client(), .bucket = "archive", .prefix = "hn", .source_uri = "gs://archive/hn" };
    var initial = try catalog.create(alloc, "init", "{\"schema\":{\"type\":\"struct\",\"schema-id\":0,\"fields\":[{\"id\":1,\"name\":\"id\",\"type\":\"long\",\"required\":true},{\"id\":2,\"name\":\"body\",\"type\":\"string\",\"required\":false}]}}", 1);
    defer initial.deinit(alloc);
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const images = try std.json.parseFromSliceLeaky(V, a, "[{\"id\":1,\"body\":\"first\"},{\"id\":1,\"body\":null},{\"id\":2}]", .{});
    const batch: Batch = .{ .batch_id = "b1", .source = "hook", .epoch = "1", .checkpoint = "a", .key_fields = &.{"id"}, .changes = &.{ .{ .op = .upsert, .row = images.array.items[0] }, .{ .op = .upsert, .row = images.array.items[1] }, .{ .op = .delete, .row = images.array.items[2] } } };
    const files: Files = .{ .client = memory.client(), .bucket = "archive", .prefix = "hn", .uri = "gs://archive/hn" };
    const first = try prepare(a, initial, files, batch, 1, 2);
    var committed = try catalog.commit(alloc, .{ .id = "b1", .expected_metadata_location = initial.metadata_location, .body = first.body, .timestamp_ms = 2 });
    defer committed.deinit(alloc);
    const metadata = try m.parse(a, committed.metadata_json);
    const snapshot = (try m.get(metadata, "snapshots")).array.items[0];
    const bytes = try read(a, files, try m.str(try m.get(snapshot, "manifest-list")));
    var list = try iceberg.parseManifestListAlloc(alloc, bytes);
    defer list.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), list.entries.len);
    try std.testing.expectEqual(@as(u64, 1), list.entries[0].added_rows_count);
    try std.testing.expectEqual(@as(u64, 2), list.entries[1].added_rows_count);
    var delete_manifest = try iceberg.parseDataManifestAlloc(alloc, try read(a, files, list.entries[1].manifest_path));
    defer delete_manifest.deinit(alloc);
    try std.testing.expectEqual(@as(i32, 1), delete_manifest.entries[0].equality_ids[0]);
    try std.testing.expectEqual(@as(?i64, 1), delete_manifest.entries[0].data_sequence_number);
    var next = batch;
    next.batch_id = "b2";
    next.checkpoint = "b";
    try std.testing.expectError(error.LakeCheckpointConflict, prepare(a, committed, files, next, 2, 3));
    next.expected_checkpoint = "a";
    const second = try prepare(a, committed, files, next, 2, 3);
    var advanced = try catalog.commit(alloc, .{ .id = "b2", .expected_metadata_location = committed.metadata_location, .body = second.body, .timestamp_ms = 3 });
    defer advanced.deinit(alloc);
    const later = try m.parse(a, advanced.metadata_json);
    const later_snapshots = (try m.get(later, "snapshots")).array.items;
    var retained = try iceberg.parseManifestListAlloc(alloc, try read(a, files, try m.str(try m.get(later_snapshots[later_snapshots.len - 1], "manifest-list"))));
    defer retained.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 4), retained.entries.len);
}
