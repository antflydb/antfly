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
const bucket = @import("bucket.zig");
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
/// Read catalog-selected immutable Iceberg files. Mutable catalog heads use
/// revision-bearing catalog operations instead. A single bounded body GET is
/// atomic and sufficient here; none of the preflight metadata is consumed.
pub fn readLimited(a: A, files: Files, uri: []const u8, max_bytes: usize) ![]const u8 {
    try files.context.ensureActive();
    const root = try std.fmt.allocPrint(a, "{s}/", .{std.mem.trimEnd(u8, files.uri, "/")});
    if (!std.mem.startsWith(u8, uri, root)) return error.LakeArtifactOutsideTable;
    const relative = uri[root.len..];
    if (std.mem.indexOf(u8, relative, "..") != null) return error.LakeArtifactOutsideTable;
    const key = try std.fmt.allocPrint(a, "{s}{s}{s}", .{ files.prefix, if (files.prefix.len == 0) "" else "/", relative });
    var client = files.client;
    var result = client.getObject(files.bucket, key, .{ .skip_metadata_probe = true, .cancellation = types.contextCancellation(&files.context), .max_response_bytes = max_bytes }) catch |err| {
        try files.context.ensureActive();
        return err;
    };
    defer result.deinit(client.allocator);
    try files.context.ensureActive();
    if (result.body.len > max_bytes) return error.ResponseTooLarge;
    return a.dupe(u8, result.body);
}
pub fn listEntry(a: A, entry: iceberg.ManifestListEntry) !V {
    return json(a, .{ .manifest_path = entry.manifest_path, .manifest_length = entry.manifest_length, .partition_spec_id = entry.partition_spec_id, .content = @backingInt(entry.content), .sequence_number = entry.sequence_number orelse 0, .min_sequence_number = entry.min_sequence_number orelse 0, .added_snapshot_id = entry.added_snapshot_id orelse 0, .added_files_count = entry.added_files_count, .existing_files_count = entry.existing_files_count, .deleted_files_count = entry.deleted_files_count, .added_rows_count = entry.added_rows_count, .existing_rows_count = entry.existing_rows_count, .deleted_rows_count = entry.deleted_rows_count });
}
/// Fast appends add two manifests per commit and carry every earlier manifest
/// forward. Without merging, commit cost and reader planning grow with the
/// number of commits. Once `manifest_merge_min_count` small Antfly-written
/// unpartitioned manifests of one content kind accumulate, they are rewritten
/// as bounded manifests whose entries keep their original snapshot and sequence
/// numbers with EXISTING status (Iceberg's commit.manifest-merge behavior).
pub const manifest_merge_min_count: usize = 100;
/// Merged manifests stop growing at this size and leave the merge pool. The
/// bound keeps them below compaction's 4 MiB whole-manifest admission limit.
pub const manifest_merge_target_bytes: u64 = 1024 * 1024;
const MergeContext = struct { bucket_spec: ?bucket.Spec = null, entry_schema: []const u8 = avro.entry_schema, partition_spec: []const u8 = "[]", spec_id: i32, spec_string: []const u8, schema_json: []const u8, snapshot_id: i64, sequence: i64, digest: []const u8 };
fn mergeable(entry: iceberg.ManifestListEntry, spec_id: i32) bool {
    if (entry.partition_spec_id != spec_id or entry.manifest_length >= manifest_merge_target_bytes) return false;
    if (std.mem.indexOf(u8, entry.manifest_path, "/metadata/antfly-") == null) return false;
    return std.mem.endsWith(u8, entry.manifest_path, if (entry.content == .deletes) "-delete.avro" else "-data.avro");
}
fn carryManifests(a: A, files: Files, previous: []const iceberg.ManifestListEntry, manifests: *V, context: MergeContext) !void {
    inline for (.{ iceberg.ManifestContent.data, iceberg.ManifestContent.deletes }) |kind| {
        try carryManifestKind(a, files, previous, manifests, kind, context);
    }
}
fn carryManifestKind(a: A, files: Files, previous: []const iceberg.ManifestListEntry, manifests: *V, comptime kind: iceberg.ManifestContent, context: MergeContext) !void {
    var pool: std.ArrayList(iceberg.ManifestListEntry) = .empty;
    for (previous) |entry| if (entry.content == kind and mergeable(entry, context.spec_id)) try pool.append(a, entry);
    if (pool.items.len < manifest_merge_min_count) {
        for (previous) |entry| if (entry.content == kind) try append(manifests, try listEntry(a, entry));
        return;
    }
    for (previous) |entry| if (entry.content == kind and !mergeable(entry, context.spec_id)) try append(manifests, try listEntry(a, entry));
    try carryMergePool(a, files, pool.items, manifests, kind, context);
}
fn mergeMetadata(comptime kind: iceberg.ManifestContent, context: MergeContext) [5]avro.Metadata {
    return .{ .{ .key = "schema", .value = context.schema_json }, .{ .key = "partition-spec", .value = context.partition_spec }, .{ .key = "partition-spec-id", .value = context.spec_string }, .{ .key = "format-version", .value = "2" }, .{ .key = "content", .value = if (kind == .deletes) "deletes" else "data" } };
}
const MergeBatch = struct {
    arena: std.heap.ArenaAllocator,
    records: V,
    bytes: usize = 0,
    rows: u64 = 0,
    min_sequence: i64 = std.math.maxInt(i64),
    sources: usize = 0,
    original: ?iceberg.ManifestListEntry = null,

    fn init(a: A) MergeBatch {
        return .{ .arena = .init(a), .records = .{ .array = std.json.Array.init(a) } };
    }
    fn flush(self: *MergeBatch, a: A, files: Files, manifests: *V, comptime kind: iceberg.ManifestContent, context: MergeContext, header: usize) !void {
        if (self.sources == 0) return;
        const output_alloc = self.arena.allocator();
        // Leave filled outputs outside the merge pool. Header-heavy manifests
        // stay eligible while their record payload still has useful capacity.
        const capacity = manifest_merge_target_bytes - header - 36;
        const sealed = self.bytes >= capacity * 3 / 4;
        if (self.sources == 1 and !sealed) {
            try append(manifests, try listEntry(a, self.original.?));
        } else {
            const encoded = try avro.ocf(output_alloc, context.entry_schema, self.records.array.items, &mergeMetadata(kind, context));
            if (encoded.len > manifest_merge_target_bytes) return error.InvalidLakeManifest;
            const label = if (kind == .deletes) "delete" else "data";
            const uri = try upload(output_alloc, files, try std.fmt.allocPrint(output_alloc, "metadata/antfly-{s}-{s}-merge-{s}{s}.avro", .{ context.digest, types.digestHex(encoded), label, if (sealed) "-full" else "" }), encoded);
            try append(manifests, try json(a, .{ .manifest_path = uri, .manifest_length = encoded.len, .partition_spec_id = context.spec_id, .content = if (kind == .deletes) @as(i32, 1) else @as(i32, 0), .sequence_number = context.sequence, .min_sequence_number = self.min_sequence, .added_snapshot_id = context.snapshot_id, .added_files_count = 0, .existing_files_count = self.records.array.items.len, .deleted_files_count = 0, .added_rows_count = 0, .existing_rows_count = self.rows, .deleted_rows_count = 0 }));
        }
        _ = self.arena.reset(.retain_capacity);
        self.records = array(self.arena.allocator());
        self.bytes = 0;
        self.rows = 0;
        self.min_sequence = std.math.maxInt(i64);
        self.sources = 0;
        self.original = null;
    }
};
fn carryMergePool(a: A, files: Files, pool: []const iceberg.ManifestListEntry, manifests: *V, comptime kind: iceberg.ManifestContent, context: MergeContext) !void {
    if (pool.len == 0) return;
    const header_bytes = try avro.ocf(a, context.entry_schema, &.{}, &mergeMetadata(kind, context));
    const header = header_bytes.len;
    a.free(header_bytes);
    if (header + 36 >= manifest_merge_target_bytes) {
        for (pool) |entry| try append(manifests, try listEntry(a, entry));
        return;
    }
    const entry_schema = try std.json.parseFromSliceLeaky(V, a, context.entry_schema, .{});
    var batch = MergeBatch.init(a);
    defer batch.arena.deinit();
    var input = std.heap.ArenaAllocator.init(a);
    defer input.deinit();
    for (pool) |manifest_entry| {
        try files.context.ensureActive();
        // Reuse bounded decode scratch for one input at a time. Repeated schema
        // headers never consume the output bin's record budget.
        _ = input.reset(.retain_capacity);
        const scratch = input.allocator();
        const manifest = try iceberg.parseDataManifestAlloc(scratch, try readLimited(scratch, files, manifest_entry.manifest_path, manifest_merge_target_bytes));
        var records = array(scratch);
        var bytes: usize = 0;
        var rows: u64 = 0;
        var min_sequence: i64 = std.math.maxInt(i64);
        var supported = true;
        for (manifest.entries) |raw| {
            if (raw.status == .deleted) continue;
            const file = try @import("../iceberg_inventory.zig").resolveManifestEntry(raw, manifest_entry, 2);
            if ((if (context.bucket_spec != null) file.partition_field_count != 1 else file.partition_field_count != 0 or file.partition_values.len != 0) or file.lower_bounds.len != 0 or file.upper_bounds.len != 0 or (kind == .deletes) != (file.content != .data)) {
                supported = false;
                break;
            }
            var partition = object(scratch);
            if (context.bucket_spec) |spec| {
                var value: ?u32 = null;
                if (file.partition_values.len > 1) return error.InvalidLakeManifest;
                for (file.partition_values) |stored| {
                    if (!std.mem.eql(u8, stored.column_id, spec.name)) return error.InvalidLakeManifest;
                    value = try std.fmt.parseInt(u32, stored.string_value, 10);
                    if (value.? >= spec.count) return error.InvalidLakeManifest;
                }
                partition = try spec.partition(scratch, value);
            }
            var data_file = try json(scratch, .{ .content = @as(i32, @backingInt(file.content)), .file_path = file.file_path, .file_format = file.file_format, .partition = partition, .record_count = file.record_count, .file_size_in_bytes = file.file_size_in_bytes });
            if (file.equality_ids.len != 0) try put(scratch, &data_file, "equality_ids", try json(scratch, file.equality_ids));
            const sequence = file.data_sequence_number orelse return error.InvalidLakeManifest;
            const record = try json(scratch, .{ .status = 0, .snapshot_id = file.snapshot_id orelse return error.InvalidLakeManifest, .sequence_number = sequence, .file_sequence_number = file.file_sequence_number orelse return error.InvalidLakeManifest, .data_file = data_file });
            bytes += try avro.recordSize(entry_schema, record);
            try append(&records, record);
            rows = try std.math.add(u64, rows, file.record_count);
            min_sequence = @min(min_sequence, sequence);
        }
        if (!supported or try avro.ocfSize(header, records.array.items.len, bytes) > manifest_merge_target_bytes) {
            try append(manifests, try listEntry(a, manifest_entry));
            continue;
        }
        if (records.array.items.len == 0) continue;
        if (try avro.ocfSize(header, batch.records.array.items.len + records.array.items.len, batch.bytes + bytes) > manifest_merge_target_bytes) try batch.flush(a, files, manifests, kind, context, header);
        if (batch.sources == 0) batch.records = array(batch.arena.allocator());
        for (records.array.items) |record| try append(&batch.records, try json(batch.arena.allocator(), record));
        batch.bytes += bytes;
        batch.rows = try std.math.add(u64, batch.rows, rows);
        batch.min_sequence = @min(batch.min_sequence, min_sequence);
        batch.sources += 1;
        batch.original = manifest_entry;
    }
    try batch.flush(a, files, manifests, kind, context, header);
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
    // Equality deletes remain global across spec evolution and bucket moves.
    // Replacement data honors a supported default bucket spec.
    var spec_id: i32 = -1;
    var max_spec: i32 = 0;
    for ((try m.get(root, "partition-specs")).array.items) |spec| {
        const id: i32 = @intCast(try m.int(try m.get(spec, "spec-id")));
        max_spec = @max(max_spec, id);
        if ((try m.get(spec, "fields")).array.items.len == 0) spec_id = id;
    }
    if (spec_id < 0) {
        spec_id = std.math.add(i32, max_spec, 1) catch return error.InvalidLakeMetadata;
        try append(&updates, try json(a, .{ .action = "add-spec", .spec = .{ .@"spec-id" = spec_id, .fields = @as([]const V, &.{}) } }));
    }
    const data_bucket = try bucket.defaultSpec(root, s);
    const schema_json = try std.json.Stringify.valueAlloc(a, s, .{});
    const spec_string = try std.fmt.allocPrint(a, "{d}", .{spec_id});
    if (parent != .null and try m.int(parent) != -1) {
        for ((try m.get(root, "snapshots")).array.items) |snapshot| if (try m.int(try m.get(snapshot, "snapshot-id")) == try m.int(parent)) {
            const bytes = try read(a, files, try m.str(try m.get(snapshot, "manifest-list")));
            var previous = try iceberg.parseManifestListAlloc(a, bytes);
            defer previous.deinit(a);
            // Merge each known spec independently; carry unknown layouts once.
            for ((try m.get(root, "partition-specs")).array.items) |spec| {
                const id: i32 = @intCast(try m.int(try m.get(spec, "spec-id")));
                const b = try bucket.parse(spec, s);
                var matching: std.ArrayList(iceberg.ManifestListEntry) = .empty;
                for (previous.entries) |entry| if (entry.partition_spec_id == id) try matching.append(a, entry);
                if (id == spec_id or b != null) {
                    try carryManifests(a, files, matching.items, &manifests, .{ .spec_id = id, .spec_string = try std.fmt.allocPrint(a, "{d}", .{id}), .schema_json = schema_json, .snapshot_id = snapshot_id, .sequence = seq, .digest = &digest, .bucket_spec = b, .entry_schema = if (b) |v| try v.entrySchema(a) else avro.entry_schema, .partition_spec = if (b) |v| try std.json.Stringify.valueAlloc(a, v.fields, .{}) else "[]" });
                } else for (matching.items) |entry| try append(&manifests, try listEntry(a, entry));
            }
            break;
        };
    }
    inline for (.{ false, true }) |deletes| {
        const group = if (deletes) rows.keys else rows.data;
        if (group.len != 0) {
            const layout: ?bucket.Spec = if (deletes) null else data_bucket;
            const groups = if (layout) |spec| try bucket.group(a, spec, group) else &[_]bucket.Group{.{ .bucket = null, .rows = group }};
            var entries = array(a);
            const kind = if (deletes) "delete" else "data";
            for (groups, 0..) |part, ordinal| {
                const data = try parquet.encode(a, if (deletes) rows.key_schema else s, part.rows, files.context);
                const data_uri = try upload(a, files, try std.fmt.allocPrint(a, "data/antfly-{s}-{s}-{d}.parquet", .{ digest, kind, ordinal }), data);
                var data_file = try json(a, .{ .content = if (deletes) @as(i32, 2) else @as(i32, 0), .file_path = data_uri, .file_format = "PARQUET", .partition = if (layout) |spec| try spec.partition(a, part.bucket) else object(a), .record_count = part.rows.len, .file_size_in_bytes = data.len });
                if (deletes) try put(a, &data_file, "equality_ids", rows.ids);
                try append(&entries, try json(a, .{ .status = 1, .snapshot_id = snapshot_id, .sequence_number = seq, .file_sequence_number = seq, .data_file = data_file }));
            }
            const manifest = try avro.ocf(a, if (layout) |spec| try spec.entrySchema(a) else avro.entry_schema, entries.array.items, &.{ .{ .key = "schema", .value = schema_json }, .{ .key = "partition-spec", .value = if (layout) |spec| try std.json.Stringify.valueAlloc(a, spec.fields, .{}) else "[]" }, .{ .key = "partition-spec-id", .value = if (layout) |spec| try std.fmt.allocPrint(a, "{d}", .{spec.id}) else spec_string }, .{ .key = "format-version", .value = "2" }, .{ .key = "content", .value = if (deletes) "deletes" else "data" } });
            const manifest_uri = try upload(a, files, try std.fmt.allocPrint(a, "metadata/antfly-{s}-{s}.avro", .{ digest, kind }), manifest);
            try append(&manifests, try json(a, .{ .manifest_path = manifest_uri, .manifest_length = manifest.len, .partition_spec_id = if (layout) |spec| spec.id else spec_id, .content = if (deletes) @as(i32, 1) else @as(i32, 0), .sequence_number = seq, .min_sequence_number = seq, .added_snapshot_id = snapshot_id, .added_files_count = groups.len, .existing_files_count = 0, .deleted_files_count = 0, .added_rows_count = group.len, .existing_rows_count = 0, .deleted_rows_count = 0 }));
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

test "lake catalog manifest merge splits a large eligible pool into bounded outputs" {
    const alloc = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    var memory = objectstore.MemoryClient.init(alloc);
    defer memory.deinit();
    const files: Files = .{ .client = memory.client(), .bucket = "archive", .prefix = "events", .uri = "gs://archive/events" };
    const padding = try a.alloc(u8, 240);
    @memset(padding, 'x');
    var previous: std.ArrayList(iceberg.ManifestListEntry) = .empty;
    const rows_per_manifest = 256;
    for (0..manifest_merge_min_count) |i| {
        var scratch = std.heap.ArenaAllocator.init(alloc);
        defer scratch.deinit();
        const sa = scratch.allocator();
        var records = array(sa);
        for (0..rows_per_manifest) |j| {
            const path = try std.fmt.allocPrint(sa, "gs://archive/events/data/{s}-{d}-{d}.parquet", .{ padding, i, j });
            try append(&records, try json(sa, .{ .status = 1, .snapshot_id = i + 1, .sequence_number = i + 1, .file_sequence_number = i + 1, .data_file = .{ .content = 0, .file_path = path, .file_format = "PARQUET", .partition = object(sa), .record_count = 1, .file_size_in_bytes = 1 } }));
        }
        const bytes = try avro.ocf(sa, avro.entry_schema, records.array.items, &.{});
        const uri = try upload(a, files, try std.fmt.allocPrint(a, "metadata/antfly-input-{d}-data.avro", .{i}), bytes);
        try previous.append(a, .{ .manifest_path = try a.dupe(u8, uri), .manifest_length = bytes.len, .partition_spec_id = 0, .content = .data, .sequence_number = @intCast(i + 1), .min_sequence_number = @intCast(i + 1), .added_snapshot_id = @intCast(i + 1), .added_files_count = rows_per_manifest, .added_rows_count = rows_per_manifest });
    }
    var manifests = array(a);
    try carryManifests(a, files, previous.items, &manifests, .{ .spec_id = 0, .spec_string = "0", .schema_json = "{}", .snapshot_id = 101, .sequence = 101, .digest = "bounded" });
    try std.testing.expect(manifests.array.items.len > 1);
    try std.testing.expect(manifests.array.items.len < previous.items.len);
    var seen: std.StringHashMapUnmanaged(void) = .empty;
    var total: usize = 0;
    var sealed: usize = 0;
    for (manifests.array.items) |entry| {
        try std.testing.expect(try m.int(try m.get(entry, "manifest_length")) <= manifest_merge_target_bytes);
        var scratch = std.heap.ArenaAllocator.init(alloc);
        defer scratch.deinit();
        const sa = scratch.allocator();
        const path = try m.str(try m.get(entry, "manifest_path"));
        const bytes = try read(sa, files, path);
        if (std.mem.endsWith(u8, path, "-full.avro")) {
            sealed += 1;
            try std.testing.expect(!mergeable(.{ .manifest_path = try a.dupe(u8, path), .manifest_length = bytes.len }, 0));
        }
        const manifest = try iceberg.parseDataManifestAlloc(sa, bytes);
        for (manifest.entries) |file| {
            try std.testing.expectEqual(iceberg.ManifestEntryStatus.existing, file.status);
            try std.testing.expectEqual(file.snapshot_id, file.data_sequence_number);
            try std.testing.expectEqual(file.data_sequence_number, file.file_sequence_number);
            try std.testing.expect(!seen.contains(file.file_path));
            try seen.put(a, try a.dupe(u8, file.file_path), {});
            total += 1;
        }
    }
    try std.testing.expectEqual(manifest_merge_min_count * rows_per_manifest, total);
    try std.testing.expect(sealed > 0);
    // A wider evolved schema may grow the encoded header beyond the target.
    // Exact output validation retains inputs that cannot fit the new header.
    const schema_padding = try a.alloc(u8, @intCast(manifest_merge_target_bytes));
    @memset(schema_padding, ' ');
    var oversized = array(a);
    try carryMergePool(a, files, previous.items[0..2], &oversized, .data, .{ .spec_id = 0, .spec_string = "0", .schema_json = schema_padding, .snapshot_id = 101, .sequence = 101, .digest = "large-header" });
    try std.testing.expectEqual(@as(usize, 2), oversized.array.items.len);
    for (oversized.array.items, previous.items[0..2]) |entry, original| try std.testing.expectEqualStrings(original.manifest_path, try m.str(try m.get(entry, "manifest_path")));
}

test "lake catalog manifest merge deduplicates wide schema headers and preserves file sequences" {
    const alloc = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    var memory = objectstore.MemoryClient.init(alloc);
    defer memory.deinit();
    const files: Files = .{ .client = memory.client(), .bucket = "archive", .prefix = "events", .uri = "gs://archive/events" };
    const doc = try a.alloc(u8, 600 * 1024);
    @memset(doc, 'x');
    const schema_json = try std.json.Stringify.valueAlloc(a, .{ .type = "struct", .@"schema-id" = 0, .fields = .{.{ .id = 1, .name = "id", .type = "long", .required = true, .doc = doc }} }, .{});
    var previous: std.ArrayList(iceberg.ManifestListEntry) = .empty;
    for (0..manifest_merge_min_count) |i| {
        var scratch = std.heap.ArenaAllocator.init(alloc);
        defer scratch.deinit();
        const sa = scratch.allocator();
        const row = try json(sa, .{ .status = 1, .snapshot_id = i + 1, .sequence_number = i + 1, .file_sequence_number = i + 1, .data_file = .{ .content = 0, .file_path = try std.fmt.allocPrint(sa, "gs://archive/events/data/{d}.parquet", .{i}), .file_format = "PARQUET", .partition = object(sa), .record_count = 1, .file_size_in_bytes = 1 } });
        const bytes = try avro.ocf(sa, avro.entry_schema, &.{row}, &.{ .{ .key = "schema", .value = schema_json }, .{ .key = "partition-spec", .value = "[]" }, .{ .key = "partition-spec-id", .value = "0" }, .{ .key = "format-version", .value = "2" }, .{ .key = "content", .value = "data" } });
        const uri = try upload(a, files, try std.fmt.allocPrint(a, "metadata/antfly-wide-{d}-data.avro", .{i}), bytes);
        try previous.append(a, .{ .manifest_path = try a.dupe(u8, uri), .manifest_length = bytes.len, .partition_spec_id = 0, .content = .data, .sequence_number = @intCast(i + 1), .min_sequence_number = @intCast(i + 1), .added_snapshot_id = @intCast(i + 1), .added_files_count = 1, .added_rows_count = 1 });
    }
    var manifests = array(a);
    try carryManifests(a, files, previous.items, &manifests, .{ .spec_id = 0, .spec_string = "0", .schema_json = schema_json, .snapshot_id = 101, .sequence = 101, .digest = "wide" });
    try std.testing.expectEqual(@as(usize, 1), manifests.array.items.len);
    const entry = manifests.array.items[0];
    try std.testing.expect(try m.int(try m.get(entry, "manifest_length")) < manifest_merge_target_bytes);
    const output_path = try m.str(try m.get(entry, "manifest_path"));
    // A large header alone does not seal an otherwise mostly empty output.
    try std.testing.expect(mergeable(.{ .manifest_path = try a.dupe(u8, output_path), .manifest_length = @intCast(try m.int(try m.get(entry, "manifest_length"))) }, 0));
    const merged = try iceberg.parseDataManifestAlloc(a, try read(a, files, output_path));
    try std.testing.expectEqual(manifest_merge_min_count, merged.entries.len);
    for (merged.entries, 1..) |file, sequence| {
        try std.testing.expectEqual(iceberg.ManifestEntryStatus.existing, file.status);
        try std.testing.expectEqual(@as(?i64, @intCast(sequence)), file.data_sequence_number);
        try std.testing.expectEqual(file.data_sequence_number, file.file_sequence_number);
        try std.testing.expectEqual(file.snapshot_id, file.data_sequence_number);
    }
}

test "external lake immutable file reads use one bounded GCS request" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const gcs = objectstore.gcs;
    const State = struct {
        metadata_calls: usize = 0,
        body_calls: usize = 0,
        expire_during_read: bool = false,
        expired: bool = false,
        const payload = "immutable-avro";
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.expired) return error.DeadlineExceeded;
        }
        fn request(raw: ?*anyopaque, alloc: A, method: gcs.HttpMethod, url: []const u8, _: []const gcs.HeaderPair, _: ?[]const u8, _: ?[]const u8, max_response: ?usize, _: ?objectstore.CancellationToken) !gcs.TransportResponse {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            try std.testing.expectEqual(gcs.HttpMethod.GET, method);
            if (std.mem.indexOf(u8, url, "alt=media") != null) {
                self.body_calls += 1;
                if (self.expire_during_read) {
                    self.expired = true;
                    return error.Canceled;
                }
                try std.testing.expect(max_response != null);
                if (payload.len > max_response.?) return error.ResponseTooLarge;
                return .{ .status = 200, .body = try alloc.dupe(u8, payload), .etag = try alloc.dupe(u8, "etag") };
            }
            self.metadata_calls += 1;
            return .{ .status = 200, .body = try alloc.dupe(u8, "{\"bucket\":\"bucket\",\"name\":\"warehouse/metadata/antfly-list.avro\",\"etag\":\"etag\",\"generation\":\"7\",\"size\":\"14\"}") };
        }
    };
    var state: State = .{};
    const cfg = try gcs.jsonApiClientConfigWithBearerTokenAlloc(a, "test-token", null);
    var provider = gcs.JsonApiClient.initWithRequestFn(a, cfg, &state, State.request);
    var client = provider.client();
    defer client.deinit();
    var files: Files = .{ .client = client, .bucket = "bucket", .prefix = "warehouse", .uri = "gs://bucket/warehouse" };
    const uri = "gs://bucket/warehouse/metadata/antfly-list.avro";
    try std.testing.expectEqualStrings(State.payload, try readLimited(arena.allocator(), files, uri, 64));
    try std.testing.expectEqual(@as(usize, 1), state.body_calls);
    try std.testing.expectEqual(@as(usize, 0), state.metadata_calls);
    try std.testing.expectError(error.ResponseTooLarge, readLimited(arena.allocator(), files, uri, 3));
    try std.testing.expectError(error.LakeArtifactOutsideTable, readLimited(arena.allocator(), files, "gs://bucket/other/manifest.avro", 64));
    try std.testing.expectError(error.LakeArtifactOutsideTable, readLimited(arena.allocator(), files, "gs://bucket/warehouse/../manifest.avro", 64));
    files.context.checkpoint = .{ .ptr = &state, .check = State.check };
    state.expire_during_read = true;
    try std.testing.expectError(error.DeadlineExceeded, readLimited(arena.allocator(), files, uri, 64));
    state.expire_during_read = false;
    state.expired = false;
    files.context.checkpoint = null;
    const calls = state.body_calls;
    files.context.deadline_ns = 0;
    try std.testing.expectError(error.DeadlineExceeded, readLimited(arena.allocator(), files, uri, 64));
    try std.testing.expectEqual(calls, state.body_calls);
    try std.testing.expectEqual(@as(usize, 0), state.metadata_calls);
}
