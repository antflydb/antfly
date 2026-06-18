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

//! Iceberg table metadata JSON planning.
//!
//! This resolves the table metadata file to a pinned snapshot and manifest-list
//! URI. It intentionally stops before Avro manifest-list/manifest decoding.

const std = @import("std");
const Allocator = std.mem.Allocator;

pub const SnapshotRef = struct {
    snapshot_id: []u8,
    manifest_list_uri: []u8,
    timestamp_ms: i64 = 0,
    sequence_number: u64 = 0,
    schema_id: ?i64 = null,

    pub fn deinit(self: *SnapshotRef, alloc: Allocator) void {
        alloc.free(self.snapshot_id);
        alloc.free(self.manifest_list_uri);
        self.* = undefined;
    }

    pub fn validate(self: SnapshotRef) !void {
        if (self.snapshot_id.len == 0) return error.InvalidIcebergMetadata;
        if (self.manifest_list_uri.len == 0) return error.InvalidIcebergMetadata;
    }
};

pub const Plan = struct {
    metadata_uri: []u8,
    table_uuid: []u8,
    location: []u8,
    current_snapshot_id: []u8,
    schema_fingerprint: []u8,
    snapshots: []SnapshotRef,
    current_snapshot_index: usize,

    pub fn deinit(self: *Plan, alloc: Allocator) void {
        alloc.free(self.metadata_uri);
        alloc.free(self.table_uuid);
        alloc.free(self.location);
        alloc.free(self.current_snapshot_id);
        alloc.free(self.schema_fingerprint);
        for (self.snapshots) |*snapshot| snapshot.deinit(alloc);
        alloc.free(self.snapshots);
        self.* = undefined;
    }

    pub fn currentSnapshot(self: Plan) SnapshotRef {
        return self.snapshots[self.current_snapshot_index];
    }

    pub fn validate(self: Plan) !void {
        if (self.metadata_uri.len == 0) return error.InvalidIcebergMetadata;
        if (self.table_uuid.len == 0) return error.InvalidIcebergMetadata;
        if (self.location.len == 0) return error.InvalidIcebergMetadata;
        if (self.current_snapshot_id.len == 0) return error.InvalidIcebergMetadata;
        if (self.schema_fingerprint.len == 0) return error.InvalidIcebergMetadata;
        if (self.current_snapshot_index >= self.snapshots.len) return error.InvalidIcebergMetadata;
        for (self.snapshots, 0..) |snapshot, idx| {
            try snapshot.validate();
            for (self.snapshots[0..idx]) |previous| {
                if (std.mem.eql(u8, previous.snapshot_id, snapshot.snapshot_id)) return error.InvalidIcebergMetadata;
            }
        }
        if (!std.mem.eql(u8, self.current_snapshot_id, self.currentSnapshot().snapshot_id)) {
            return error.InvalidIcebergMetadata;
        }
    }
};

pub fn parseMetadataPlanAlloc(
    alloc: Allocator,
    metadata_uri: []const u8,
    metadata_json: []const u8,
    requested_snapshot_id: ?[]const u8,
) !Plan {
    if (metadata_uri.len == 0) return error.InvalidIcebergMetadata;

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, metadata_json, .{});
    defer parsed.deinit();
    const root = switch (parsed.value) {
        .object => |object| object,
        else => return error.InvalidIcebergMetadata,
    };

    const format_version = try requiredI64(root, "format-version");
    if (format_version != 1 and format_version != 2) return error.UnsupportedIcebergMetadataVersion;

    const table_uuid = try requiredString(root, "table-uuid");
    const location = try requiredString(root, "location");
    const current_snapshot_id_int = try requiredI64(root, "current-snapshot-id");
    if (current_snapshot_id_int < 0) return error.InvalidIcebergMetadata;
    const current_snapshot_id = try snapshotIdStringAlloc(alloc, current_snapshot_id_int);
    errdefer alloc.free(current_snapshot_id);
    if (requested_snapshot_id) |requested| {
        if (requested.len == 0) return error.InvalidIcebergMetadata;
        if (!std.mem.eql(u8, requested, current_snapshot_id)) return error.IcebergSnapshotMismatch;
    }

    const snapshots_value = root.get("snapshots") orelse return error.InvalidIcebergMetadata;
    const snapshots_array = switch (snapshots_value) {
        .array => |array| array,
        else => return error.InvalidIcebergMetadata,
    };
    if (snapshots_array.items.len == 0) return error.InvalidIcebergMetadata;

    const snapshots = try alloc.alloc(SnapshotRef, snapshots_array.items.len);
    errdefer alloc.free(snapshots);
    var initialized: usize = 0;
    errdefer {
        for (snapshots[0..initialized]) |*snapshot| snapshot.deinit(alloc);
    }

    var current_snapshot_index: ?usize = null;
    var current_schema_id: ?i64 = optionalI64(root, "current-schema-id") catch null;
    for (snapshots_array.items, 0..) |snapshot_value, idx| {
        const snapshot_object = switch (snapshot_value) {
            .object => |object| object,
            else => return error.InvalidIcebergMetadata,
        };
        const snapshot_id_int = try requiredI64(snapshot_object, "snapshot-id");
        if (snapshot_id_int < 0) return error.InvalidIcebergMetadata;
        const snapshot_id = try snapshotIdStringAlloc(alloc, snapshot_id_int);
        errdefer alloc.free(snapshot_id);
        const manifest_list = try requiredString(snapshot_object, "manifest-list");
        const manifest_list_uri = try alloc.dupe(u8, manifest_list);
        errdefer alloc.free(manifest_list_uri);
        const schema_id = optionalI64(snapshot_object, "schema-id") catch null;

        snapshots[idx] = .{
            .snapshot_id = snapshot_id,
            .manifest_list_uri = manifest_list_uri,
            .timestamp_ms = (try optionalI64(snapshot_object, "timestamp-ms")) orelse 0,
            .sequence_number = (try optionalU64(snapshot_object, "sequence-number")) orelse 0,
            .schema_id = schema_id,
        };
        initialized += 1;
        if (std.mem.eql(u8, snapshot_id, current_snapshot_id)) {
            current_snapshot_index = idx;
            if (current_schema_id == null) current_schema_id = schema_id;
        }
    }

    const got_current_index = current_snapshot_index orelse return error.InvalidIcebergMetadata;
    const got_schema_id = current_schema_id orelse return error.InvalidIcebergMetadata;
    const metadata_uri_copy = try alloc.dupe(u8, metadata_uri);
    errdefer alloc.free(metadata_uri_copy);
    const table_uuid_copy = try alloc.dupe(u8, table_uuid);
    errdefer alloc.free(table_uuid_copy);
    const location_copy = try alloc.dupe(u8, location);
    errdefer alloc.free(location_copy);
    const schema_fingerprint = try std.fmt.allocPrint(alloc, "iceberg-schema:{d}", .{got_schema_id});
    errdefer alloc.free(schema_fingerprint);

    var plan = Plan{
        .metadata_uri = metadata_uri_copy,
        .table_uuid = table_uuid_copy,
        .location = location_copy,
        .current_snapshot_id = current_snapshot_id,
        .schema_fingerprint = schema_fingerprint,
        .snapshots = snapshots,
        .current_snapshot_index = got_current_index,
    };
    errdefer plan.deinit(alloc);
    try plan.validate();
    return plan;
}

fn requiredString(object: std.json.ObjectMap, name: []const u8) ![]const u8 {
    const value = object.get(name) orelse return error.InvalidIcebergMetadata;
    return switch (value) {
        .string => |text| if (text.len == 0) error.InvalidIcebergMetadata else text,
        else => error.InvalidIcebergMetadata,
    };
}

fn requiredI64(object: std.json.ObjectMap, name: []const u8) !i64 {
    const value = object.get(name) orelse return error.InvalidIcebergMetadata;
    return jsonI64(value);
}

fn optionalI64(object: std.json.ObjectMap, name: []const u8) !?i64 {
    const value = object.get(name) orelse return null;
    return try jsonI64(value);
}

fn optionalU64(object: std.json.ObjectMap, name: []const u8) !?u64 {
    const value = object.get(name) orelse return null;
    return switch (value) {
        .integer => |int_value| std.math.cast(u64, int_value) orelse error.InvalidIcebergMetadata,
        else => error.InvalidIcebergMetadata,
    };
}

fn jsonI64(value: std.json.Value) !i64 {
    return switch (value) {
        .integer => |int_value| int_value,
        else => error.InvalidIcebergMetadata,
    };
}

fn snapshotIdStringAlloc(alloc: Allocator, snapshot_id: i64) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{d}", .{snapshot_id});
}

test "iceberg metadata plan resolves current snapshot manifest list" {
    const alloc = std.testing.allocator;
    const metadata_json =
        \\{
        \\  "format-version": 2,
        \\  "table-uuid": "uuid-events",
        \\  "location": "s3://bucket/warehouse/events",
        \\  "current-schema-id": 7,
        \\  "current-snapshot-id": 123,
        \\  "snapshots": [
        \\    {
        \\      "snapshot-id": 100,
        \\      "sequence-number": 10,
        \\      "timestamp-ms": 1700000000000,
        \\      "manifest-list": "s3://bucket/warehouse/events/metadata/snap-100.avro"
        \\    },
        \\    {
        \\      "snapshot-id": 123,
        \\      "sequence-number": 11,
        \\      "schema-id": 8,
        \\      "timestamp-ms": 1700000001000,
        \\      "manifest-list": "s3://bucket/warehouse/events/metadata/snap-123.avro"
        \\    }
        \\  ]
        \\}
    ;

    var plan = try parseMetadataPlanAlloc(
        alloc,
        "s3://bucket/warehouse/events/metadata/v2.metadata.json",
        metadata_json,
        "123",
    );
    defer plan.deinit(alloc);

    try std.testing.expectEqualStrings("uuid-events", plan.table_uuid);
    try std.testing.expectEqualStrings("123", plan.current_snapshot_id);
    try std.testing.expectEqualStrings("iceberg-schema:7", plan.schema_fingerprint);
    try std.testing.expectEqual(@as(usize, 1), plan.current_snapshot_index);
    try std.testing.expectEqualStrings(
        "s3://bucket/warehouse/events/metadata/snap-123.avro",
        plan.currentSnapshot().manifest_list_uri,
    );
}

test "iceberg metadata plan can use current snapshot schema id" {
    const alloc = std.testing.allocator;
    const metadata_json =
        \\{
        \\  "format-version": 2,
        \\  "table-uuid": "uuid-events",
        \\  "location": "s3://bucket/warehouse/events",
        \\  "current-snapshot-id": 123,
        \\  "snapshots": [
        \\    {
        \\      "snapshot-id": 123,
        \\      "schema-id": 8,
        \\      "manifest-list": "s3://bucket/warehouse/events/metadata/snap-123.avro"
        \\    }
        \\  ]
        \\}
    ;

    var plan = try parseMetadataPlanAlloc(
        alloc,
        "s3://bucket/warehouse/events/metadata/v2.metadata.json",
        metadata_json,
        null,
    );
    defer plan.deinit(alloc);

    try std.testing.expectEqualStrings("iceberg-schema:8", plan.schema_fingerprint);
}

test "iceberg metadata plan rejects mismatched and invalid snapshots" {
    const alloc = std.testing.allocator;
    const metadata_json =
        \\{
        \\  "format-version": 2,
        \\  "table-uuid": "uuid-events",
        \\  "location": "s3://bucket/warehouse/events",
        \\  "current-schema-id": 7,
        \\  "current-snapshot-id": 123,
        \\  "snapshots": [
        \\    {
        \\      "snapshot-id": 123,
        \\      "manifest-list": "s3://bucket/warehouse/events/metadata/snap-123.avro"
        \\    }
        \\  ]
        \\}
    ;

    try std.testing.expectError(error.IcebergSnapshotMismatch, parseMetadataPlanAlloc(
        alloc,
        "s3://bucket/warehouse/events/metadata/v2.metadata.json",
        metadata_json,
        "122",
    ));

    const missing_manifest =
        \\{
        \\  "format-version": 2,
        \\  "table-uuid": "uuid-events",
        \\  "location": "s3://bucket/warehouse/events",
        \\  "current-schema-id": 7,
        \\  "current-snapshot-id": 123,
        \\  "snapshots": [{ "snapshot-id": 123 }]
        \\}
    ;
    try std.testing.expectError(error.InvalidIcebergMetadata, parseMetadataPlanAlloc(
        alloc,
        "s3://bucket/warehouse/events/metadata/v2.metadata.json",
        missing_manifest,
        null,
    ));
}
