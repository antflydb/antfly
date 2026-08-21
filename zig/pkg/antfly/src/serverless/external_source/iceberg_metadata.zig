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

pub const SchemaField = struct {
    id: i32,
    name: []u8,

    pub fn deinit(self: *SchemaField, alloc: Allocator) void {
        alloc.free(self.name);
        self.* = undefined;
    }
};

pub const Plan = struct {
    metadata_uri: []u8,
    table_uuid: []u8,
    location: []u8,
    current_snapshot_id: []u8,
    schema_fingerprint: []u8,
    schema_fields: []SchemaField = &.{},
    snapshots: []SnapshotRef,
    current_snapshot_index: usize,

    pub fn deinit(self: *Plan, alloc: Allocator) void {
        alloc.free(self.metadata_uri);
        alloc.free(self.table_uuid);
        alloc.free(self.location);
        alloc.free(self.current_snapshot_id);
        alloc.free(self.schema_fingerprint);
        for (self.schema_fields) |*field| field.deinit(alloc);
        if (self.schema_fields.len > 0) alloc.free(self.schema_fields);
        for (self.snapshots) |*snapshot| snapshot.deinit(alloc);
        alloc.free(self.snapshots);
        self.* = undefined;
    }

    pub fn currentSnapshot(self: Plan) SnapshotRef {
        return self.snapshots[self.current_snapshot_index];
    }

    pub fn fieldNameForId(self: Plan, field_id: i32) ?[]const u8 {
        for (self.schema_fields) |field| {
            if (field.id == field_id) return field.name;
        }
        return null;
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
        for (self.schema_fields, 0..) |field, idx| {
            if (field.id < 0 or field.name.len == 0) return error.InvalidIcebergMetadata;
            for (self.schema_fields[0..idx]) |previous| {
                if (previous.id == field.id) return error.InvalidIcebergMetadata;
                if (std.mem.eql(u8, previous.name, field.name)) return error.InvalidIcebergMetadata;
            }
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
    var table_current_snapshot_id: ?[]u8 = try snapshotIdStringAlloc(alloc, current_snapshot_id_int);
    errdefer if (table_current_snapshot_id) |id| alloc.free(id);

    var target_is_table_current = true;
    var target_snapshot_id: ?[]u8 = if (requested_snapshot_id) |requested| blk: {
        if (requested.len == 0) return error.InvalidIcebergMetadata;
        target_is_table_current = std.mem.eql(u8, requested, table_current_snapshot_id.?);
        const requested_copy = try alloc.dupe(u8, requested);
        alloc.free(table_current_snapshot_id.?);
        table_current_snapshot_id = null;
        break :blk requested_copy;
    } else blk: {
        const current = table_current_snapshot_id.?;
        table_current_snapshot_id = null;
        break :blk current;
    };
    errdefer if (target_snapshot_id) |id| alloc.free(id);

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

    var target_snapshot_index: ?usize = null;
    const table_current_schema_id: ?i64 = optionalI64(root, "current-schema-id") catch null;
    var target_schema_id: ?i64 = null;
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
        if (std.mem.eql(u8, snapshot_id, target_snapshot_id.?)) {
            target_snapshot_index = idx;
            target_schema_id = schema_id;
        }
    }

    const got_target_index = target_snapshot_index orelse {
        if (requested_snapshot_id != null) return error.IcebergSnapshotMismatch;
        return error.InvalidIcebergMetadata;
    };
    const got_schema_id = target_schema_id orelse if (target_is_table_current)
        table_current_schema_id orelse return error.InvalidIcebergMetadata
    else
        return error.InvalidIcebergMetadata;
    const metadata_uri_copy = try alloc.dupe(u8, metadata_uri);
    errdefer alloc.free(metadata_uri_copy);
    const table_uuid_copy = try alloc.dupe(u8, table_uuid);
    errdefer alloc.free(table_uuid_copy);
    const location_copy = try alloc.dupe(u8, location);
    errdefer alloc.free(location_copy);
    const schema_fingerprint = try schemaFingerprintAlloc(alloc, root, got_schema_id);
    errdefer alloc.free(schema_fingerprint);
    const schema_fields = try schemaFieldsForIdAlloc(alloc, root, got_schema_id);
    const owned_target_snapshot_id = target_snapshot_id.?;
    target_snapshot_id = null;

    var plan = Plan{
        .metadata_uri = metadata_uri_copy,
        .table_uuid = table_uuid_copy,
        .location = location_copy,
        .current_snapshot_id = owned_target_snapshot_id,
        .schema_fingerprint = schema_fingerprint,
        .schema_fields = schema_fields,
        .snapshots = snapshots,
        .current_snapshot_index = got_target_index,
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

fn requiredBool(object: std.json.ObjectMap, name: []const u8) !bool {
    const value = object.get(name) orelse return error.InvalidIcebergMetadata;
    return switch (value) {
        .bool => |bool_value| bool_value,
        else => error.InvalidIcebergMetadata,
    };
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

fn schemaFingerprintAlloc(
    alloc: Allocator,
    root: std.json.ObjectMap,
    schema_id: i64,
) ![]u8 {
    if (root.get("schemas")) |schemas_value| {
        const schema_value = try schemaValueForId(schemas_value, schema_id);
        try validateSchemaDefinition(schema_value);
        var hasher = std.hash.Wyhash.init(0x1ce_b39_5c_a11_2026);
        try hashJsonCanonical(alloc, &hasher, schema_value);
        return try std.fmt.allocPrint(alloc, "iceberg-schema:{d}:hash={x}", .{ schema_id, hasher.final() });
    }
    return try std.fmt.allocPrint(alloc, "iceberg-schema:{d}", .{schema_id});
}

fn schemaValueForId(schemas_value: std.json.Value, schema_id: i64) !std.json.Value {
    const schemas = switch (schemas_value) {
        .array => |array| array,
        else => return error.InvalidIcebergMetadata,
    };
    if (schemas.items.len == 0) return error.InvalidIcebergMetadata;

    var found: ?std.json.Value = null;
    for (schemas.items) |schema_value| {
        const schema_object = switch (schema_value) {
            .object => |object| object,
            else => return error.InvalidIcebergMetadata,
        };
        const candidate_id = try requiredI64(schema_object, "schema-id");
        if (candidate_id < 0) return error.InvalidIcebergMetadata;
        if (candidate_id != schema_id) continue;
        if (found != null) return error.InvalidIcebergMetadata;
        found = schema_value;
    }
    return found orelse error.InvalidIcebergMetadata;
}

fn schemaFieldsForIdAlloc(
    alloc: Allocator,
    root: std.json.ObjectMap,
    schema_id: i64,
) ![]SchemaField {
    const schemas_value = root.get("schemas") orelse return &.{};
    const schema_value = try schemaValueForId(schemas_value, schema_id);
    try validateSchemaDefinition(schema_value);
    const schema_object = switch (schema_value) {
        .object => |object| object,
        else => return error.InvalidIcebergMetadata,
    };
    const fields_value = schema_object.get("fields") orelse return error.InvalidIcebergMetadata;
    const fields_array = switch (fields_value) {
        .array => |array| array,
        else => return error.InvalidIcebergMetadata,
    };

    const fields = try alloc.alloc(SchemaField, fields_array.items.len);
    errdefer alloc.free(fields);
    var initialized: usize = 0;
    errdefer deinitSchemaFieldItems(alloc, fields[0..initialized]);

    for (fields_array.items, 0..) |field_value, idx| {
        const field_object = switch (field_value) {
            .object => |object| object,
            else => return error.InvalidIcebergMetadata,
        };
        const field_id_i64 = try requiredI64(field_object, "id");
        const field_id = std.math.cast(i32, field_id_i64) orelse return error.InvalidIcebergMetadata;
        fields[idx] = .{
            .id = field_id,
            .name = try alloc.dupe(u8, try requiredString(field_object, "name")),
        };
        initialized += 1;
    }

    return fields;
}

fn deinitSchemaFields(alloc: Allocator, fields: []SchemaField) void {
    deinitSchemaFieldItems(alloc, fields);
    if (fields.len > 0) alloc.free(fields);
}

fn deinitSchemaFieldItems(alloc: Allocator, fields: []SchemaField) void {
    for (fields) |*field| field.deinit(alloc);
}

fn validateSchemaDefinition(schema_value: std.json.Value) !void {
    const schema_object = switch (schema_value) {
        .object => |object| object,
        else => return error.InvalidIcebergMetadata,
    };
    _ = try requiredI64(schema_object, "schema-id");
    const fields_value = schema_object.get("fields") orelse return error.InvalidIcebergMetadata;
    const fields = switch (fields_value) {
        .array => |array| array,
        else => return error.InvalidIcebergMetadata,
    };
    if (fields.items.len == 0) return error.InvalidIcebergMetadata;

    for (fields.items, 0..) |field_value, idx| {
        const field_object = switch (field_value) {
            .object => |object| object,
            else => return error.InvalidIcebergMetadata,
        };
        const field_id = try requiredI64(field_object, "id");
        if (field_id < 0) return error.InvalidIcebergMetadata;
        _ = try requiredString(field_object, "name");
        _ = try requiredBool(field_object, "required");
        try validateSupportedTopLevelFieldType(field_object.get("type") orelse return error.InvalidIcebergMetadata);
        for (fields.items[0..idx]) |previous_value| {
            const previous_object = switch (previous_value) {
                .object => |object| object,
                else => return error.InvalidIcebergMetadata,
            };
            if ((try requiredI64(previous_object, "id")) == field_id) return error.InvalidIcebergMetadata;
        }
    }
}

fn validateSupportedTopLevelFieldType(field_type: std.json.Value) !void {
    switch (field_type) {
        .string => |text| {
            if (text.len == 0) return error.InvalidIcebergMetadata;
        },
        .object => |object| {
            const kind = try requiredString(object, "type");
            if (std.mem.eql(u8, kind, "struct") or
                std.mem.eql(u8, kind, "list") or
                std.mem.eql(u8, kind, "map"))
            {
                return error.UnsupportedIcebergSchemaEvolution;
            }
            return error.InvalidIcebergMetadata;
        },
        else => return error.InvalidIcebergMetadata,
    }
}

fn hashJsonCanonical(
    alloc: Allocator,
    hasher: *std.hash.Wyhash,
    value: std.json.Value,
) !void {
    switch (value) {
        .null => hasher.update("n;"),
        .bool => |bool_value| hasher.update(if (bool_value) "b:true;" else "b:false;"),
        .integer => |int_value| {
            hasher.update("i:");
            try hashFmt(hasher, "{d}", .{int_value});
            hasher.update(";");
        },
        .float => |float_value| {
            hasher.update("f:");
            try hashFmt(hasher, "{d}", .{float_value});
            hasher.update(";");
        },
        .number_string => |number_text| {
            hasher.update("num:");
            hasher.update(number_text);
            hasher.update(";");
        },
        .string => |text| {
            hasher.update("s:");
            try hashFmt(hasher, "{d}:", .{text.len});
            hasher.update(text);
            hasher.update(";");
        },
        .array => |array| {
            hasher.update("a[");
            try hashFmt(hasher, "{d}", .{array.items.len});
            hasher.update(":");
            for (array.items) |item| try hashJsonCanonical(alloc, hasher, item);
            hasher.update("]");
        },
        .object => |object| {
            hasher.update("o{");
            var keys = std.ArrayListUnmanaged([]const u8).empty;
            defer keys.deinit(alloc);
            var it = object.iterator();
            while (it.next()) |entry| try keys.append(alloc, entry.key_ptr.*);
            std.mem.sort([]const u8, keys.items, {}, struct {
                fn lessThan(_: void, left: []const u8, right: []const u8) bool {
                    return std.mem.order(u8, left, right) == .lt;
                }
            }.lessThan);
            for (keys.items) |key| {
                hasher.update("k:");
                try hashFmt(hasher, "{d}:", .{key.len});
                hasher.update(key);
                hasher.update("=");
                try hashJsonCanonical(alloc, hasher, object.get(key) orelse return error.InvalidIcebergMetadata);
            }
            hasher.update("}");
        },
    }
}

fn hashFmt(hasher: *std.hash.Wyhash, comptime fmt: []const u8, args: anytype) !void {
    var buf: [128]u8 = undefined;
    const text = try std.fmt.bufPrint(&buf, fmt, args);
    hasher.update(text);
}

test "iceberg metadata plan resolves current snapshot manifest list" {
    const alloc = std.testing.allocator;
    const metadata_json =
        \\{
        \\  "format-version": 2,
        \\  "table-uuid": "uuid-events",
        \\  "location": "s3://bucket/warehouse/events",
        \\  "current-schema-id": 7,
        \\  "schemas": [
        \\    {
        \\      "schema-id": 7,
        \\      "fields": [
        \\        {"id": 1, "name": "tenant_id", "required": true, "type": "string"},
        \\        {"id": 2, "name": "amount", "required": false, "type": "long"}
        \\      ]
        \\    },
        \\    {
        \\      "schema-id": 8,
        \\      "fields": [
        \\        {"id": 1, "name": "tenant_id", "required": true, "type": "string"},
        \\        {"id": 2, "name": "amount", "required": false, "type": "double"}
        \\      ]
        \\    }
        \\  ],
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
    try std.testing.expect(std.mem.startsWith(u8, plan.schema_fingerprint, "iceberg-schema:8:hash="));
    try std.testing.expectEqual(@as(usize, 1), plan.current_snapshot_index);
    try std.testing.expectEqualStrings(
        "s3://bucket/warehouse/events/metadata/snap-123.avro",
        plan.currentSnapshot().manifest_list_uri,
    );
    try std.testing.expectEqual(@as(usize, 2), plan.schema_fields.len);
    try std.testing.expectEqualStrings("tenant_id", plan.fieldNameForId(1).?);
    try std.testing.expectEqualStrings("amount", plan.fieldNameForId(2).?);
    try std.testing.expect(plan.fieldNameForId(3) == null);
}

test "iceberg metadata plan resolves requested historical snapshot" {
    const alloc = std.testing.allocator;
    const metadata_json =
        \\{
        \\  "format-version": 2,
        \\  "table-uuid": "uuid-events",
        \\  "location": "s3://bucket/warehouse/events",
        \\  "current-schema-id": 8,
        \\  "schemas": [
        \\    {
        \\      "schema-id": 7,
        \\      "fields": [
        \\        {"id": 1, "name": "tenant_id", "required": true, "type": "string"},
        \\        {"id": 2, "name": "amount", "required": false, "type": "long"}
        \\      ]
        \\    },
        \\    {
        \\      "schema-id": 8,
        \\      "fields": [
        \\        {"id": 1, "name": "tenant_id", "required": true, "type": "string"},
        \\        {"id": 2, "name": "amount", "required": false, "type": "double"}
        \\      ]
        \\    }
        \\  ],
        \\  "current-snapshot-id": 123,
        \\  "snapshots": [
        \\    {
        \\      "snapshot-id": 100,
        \\      "sequence-number": 10,
        \\      "schema-id": 7,
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
        "100",
    );
    defer plan.deinit(alloc);

    try std.testing.expectEqualStrings("100", plan.current_snapshot_id);
    try std.testing.expectEqual(@as(usize, 0), plan.current_snapshot_index);
    try std.testing.expectEqualStrings(
        "s3://bucket/warehouse/events/metadata/snap-100.avro",
        plan.currentSnapshot().manifest_list_uri,
    );
    try std.testing.expect(std.mem.startsWith(u8, plan.schema_fingerprint, "iceberg-schema:7:hash="));
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
    try std.testing.expectEqual(@as(usize, 0), plan.schema_fields.len);
}

test "iceberg metadata plan requires schema id for requested historical snapshot" {
    const alloc = std.testing.allocator;
    const metadata_json =
        \\{
        \\  "format-version": 2,
        \\  "table-uuid": "uuid-events",
        \\  "location": "s3://bucket/warehouse/events",
        \\  "current-schema-id": 8,
        \\  "current-snapshot-id": 123,
        \\  "snapshots": [
        \\    {
        \\      "snapshot-id": 100,
        \\      "manifest-list": "s3://bucket/warehouse/events/metadata/snap-100.avro"
        \\    },
        \\    {
        \\      "snapshot-id": 123,
        \\      "manifest-list": "s3://bucket/warehouse/events/metadata/snap-123.avro"
        \\    }
        \\  ]
        \\}
    ;

    try std.testing.expectError(error.InvalidIcebergMetadata, parseMetadataPlanAlloc(
        alloc,
        "s3://bucket/warehouse/events/metadata/v2.metadata.json",
        metadata_json,
        "100",
    ));
}

test "iceberg metadata plan fingerprints and validates schema definitions when present" {
    const alloc = std.testing.allocator;
    const metadata_v1 =
        \\{
        \\  "format-version": 2,
        \\  "table-uuid": "uuid-events",
        \\  "location": "s3://bucket/warehouse/events",
        \\  "current-schema-id": 7,
        \\  "schemas": [
        \\    {
        \\      "schema-id": 7,
        \\      "fields": [
        \\        {"id": 1, "name": "tenant_id", "required": true, "type": "string"},
        \\        {"id": 2, "name": "amount", "required": false, "type": "long"}
        \\      ]
        \\    }
        \\  ],
        \\  "current-snapshot-id": 123,
        \\  "snapshots": [
        \\    {
        \\      "snapshot-id": 123,
        \\      "manifest-list": "s3://bucket/warehouse/events/metadata/snap-123.avro"
        \\    }
        \\  ]
        \\}
    ;
    const metadata_v2 =
        \\{
        \\  "format-version": 2,
        \\  "table-uuid": "uuid-events",
        \\  "location": "s3://bucket/warehouse/events",
        \\  "current-schema-id": 7,
        \\  "schemas": [
        \\    {
        \\      "schema-id": 7,
        \\      "fields": [
        \\        {"id": 1, "name": "tenant_id", "required": true, "type": "string"},
        \\        {"id": 2, "name": "amount", "required": false, "type": "double"}
        \\      ]
        \\    }
        \\  ],
        \\  "current-snapshot-id": 123,
        \\  "snapshots": [
        \\    {
        \\      "snapshot-id": 123,
        \\      "manifest-list": "s3://bucket/warehouse/events/metadata/snap-123.avro"
        \\    }
        \\  ]
        \\}
    ;

    var plan_v1 = try parseMetadataPlanAlloc(
        alloc,
        "s3://bucket/warehouse/events/metadata/v1.metadata.json",
        metadata_v1,
        null,
    );
    defer plan_v1.deinit(alloc);
    var plan_v2 = try parseMetadataPlanAlloc(
        alloc,
        "s3://bucket/warehouse/events/metadata/v2.metadata.json",
        metadata_v2,
        null,
    );
    defer plan_v2.deinit(alloc);

    try std.testing.expect(std.mem.startsWith(u8, plan_v1.schema_fingerprint, "iceberg-schema:7:hash="));
    try std.testing.expect(!std.mem.eql(u8, plan_v1.schema_fingerprint, plan_v2.schema_fingerprint));
}

test "iceberg metadata plan rejects missing or malformed schema definitions" {
    const alloc = std.testing.allocator;
    const missing_schema =
        \\{
        \\  "format-version": 2,
        \\  "table-uuid": "uuid-events",
        \\  "location": "s3://bucket/warehouse/events",
        \\  "current-schema-id": 7,
        \\  "schemas": [
        \\    {"schema-id": 8, "fields": [{"id": 1, "name": "tenant_id", "required": true, "type": "string"}]}
        \\  ],
        \\  "current-snapshot-id": 123,
        \\  "snapshots": [
        \\    {"snapshot-id": 123, "manifest-list": "s3://bucket/warehouse/events/metadata/snap-123.avro"}
        \\  ]
        \\}
    ;
    try std.testing.expectError(error.InvalidIcebergMetadata, parseMetadataPlanAlloc(
        alloc,
        "s3://bucket/warehouse/events/metadata/v2.metadata.json",
        missing_schema,
        null,
    ));

    const duplicate_field =
        \\{
        \\  "format-version": 2,
        \\  "table-uuid": "uuid-events",
        \\  "location": "s3://bucket/warehouse/events",
        \\  "current-schema-id": 7,
        \\  "schemas": [
        \\    {
        \\      "schema-id": 7,
        \\      "fields": [
        \\        {"id": 1, "name": "tenant_id", "required": true, "type": "string"},
        \\        {"id": 1, "name": "amount", "required": false, "type": "long"}
        \\      ]
        \\    }
        \\  ],
        \\  "current-snapshot-id": 123,
        \\  "snapshots": [
        \\    {"snapshot-id": 123, "manifest-list": "s3://bucket/warehouse/events/metadata/snap-123.avro"}
        \\  ]
        \\}
    ;
    try std.testing.expectError(error.InvalidIcebergMetadata, parseMetadataPlanAlloc(
        alloc,
        "s3://bucket/warehouse/events/metadata/v2.metadata.json",
        duplicate_field,
        null,
    ));

    const nested_field =
        \\{
        \\  "format-version": 2,
        \\  "table-uuid": "uuid-events",
        \\  "location": "s3://bucket/warehouse/events",
        \\  "current-schema-id": 7,
        \\  "schemas": [
        \\    {
        \\      "schema-id": 7,
        \\      "fields": [
        \\        {
        \\          "id": 1,
        \\          "name": "payload",
        \\          "required": false,
        \\          "type": {
        \\            "type": "struct",
        \\            "fields": [
        \\              {"id": 2, "name": "amount", "required": false, "type": "long"}
        \\            ]
        \\          }
        \\        }
        \\      ]
        \\    }
        \\  ],
        \\  "current-snapshot-id": 123,
        \\  "snapshots": [
        \\    {"snapshot-id": 123, "manifest-list": "s3://bucket/warehouse/events/metadata/snap-123.avro"}
        \\  ]
        \\}
    ;
    try std.testing.expectError(error.UnsupportedIcebergSchemaEvolution, parseMetadataPlanAlloc(
        alloc,
        "s3://bucket/warehouse/events/metadata/v2.metadata.json",
        nested_field,
        null,
    ));
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
