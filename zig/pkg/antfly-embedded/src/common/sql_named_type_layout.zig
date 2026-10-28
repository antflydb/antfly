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

//! Generated public type layouts and their schema-owned executable bindings.
//! Catalog ownership never enters value representation or its fingerprint.
const std = @import("std");
const wire = @import("antfly_schema_openapi");
const types = @import("../system_catalog/named_types.zig");
const values = @import("sql_enum_value.zig");
const A = std.mem.Allocator;
pub const max_types = 4096;
pub const max_registry_bytes = 16 * 1024 * 1024;

pub fn hexAlloc(a: A, bytes: []const u8) ![]u8 {
    const output = try a.alloc(u8, std.math.mul(usize, bytes.len, 2) catch return error.OutOfMemory);
    const alphabet = "0123456789abcdef";
    for (bytes, 0..) |byte, i| {
        output[2 * i] = alphabet[byte >> 4];
        output[2 * i + 1] = alphabet[byte & 15];
    }
    return output;
}
fn decodeHex(out: []u8, text: []const u8) !void {
    if (text.len != out.len * 2) return error.InvalidSchemaUpdateRequest;
    for (text) |byte| if (!std.ascii.isDigit(byte) and (byte < 'a' or byte > 'f')) return error.InvalidSchemaUpdateRequest;
    _ = std.fmt.hexToBytes(out, text) catch return error.InvalidSchemaUpdateRequest;
}

/// Region-owned adapter. Labels borrow the record, hexadecimal buffers and
/// the DTO directory belong to the caller's preparation region.
/// Shared public identity projection without serializing or copying labels.
pub fn identityWireLeaky(comptime T: type, a: A, identity: types.Identity) !T {
    try identity.validate();
    return .{ .id = identity.id, .namespace_id = identity.namespace_id, .generation = identity.generation, .origin_id = try hexAlloc(a, &identity.origin_id), .oid = identity.oid, .array_oid = identity.array_oid };
}
pub fn toWireLeaky(a: A, record: types.Record) !wire.SQLNamedTypeLayout {
    try record.validate(a);
    const labels = try a.alloc(wire.SQLNamedEnumLabel, record.definition.labels.len);
    for (record.definition.labels, labels) |source, *label| label.* = .{ .id = source.id, .text = source.text, .order_key = try hexAlloc(a, source.order_key) };
    return .{
        .kind = "enum",
        .identity = try identityWireLeaky(wire.SQLNamedTypeIdentity, a, record.identity),
        .labels = labels,
    };
}
/// The canonical snapshot is a catalog admission witness. No owner/name or
/// caller-controlled digest is included in this executable contract.
pub fn canonicalAlloc(a: A, record: types.Record) ![]u8 {
    var region = std.heap.ArenaAllocator.init(a);
    defer region.deinit();
    return std.json.Stringify.valueAlloc(a, try toWireLeaky(region.allocator(), record), .{});
}
pub fn digest(a: A, record: types.Record) ![32]u8 {
    const bytes = try canonicalAlloc(a, record);
    defer a.free(bytes);
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly:sql-named-layout:v1\x00");
    hash.update(bytes);
    var result: [32]u8 = undefined;
    hash.final(&result);
    return result;
}

pub const Guard = struct {
    id: u64,
    namespace_id: u64,
    generation: u32,
    digest: [32]u8,
};

/// Decode only canonical snapshots and their small identity headers. Catalog
/// apply compares these fixed-size witnesses through point lookups; it never
/// compiles another label directory under the serialized metadata transaction.
pub fn guardsAlloc(a: A, schema_json: []const u8) ![]Guard {
    // A decoded key can hide its spelling only behind a Unicode escape.
    // Preserve the ordinary-schema fast path without trusting substring
    // detection when escaped keys could declare a named-type directory.
    if (std.mem.indexOf(u8, schema_json, "named_types") == null and std.mem.indexOf(u8, schema_json, "\\u") == null) return a.alloc(Guard, 0);
    const Raw = @import("antfly-json").RawValue;
    const Layouts = struct {
        values: []const Raw,
        pub fn jsonParse(alloc: A, source: anytype, options: std.json.ParseOptions) !@This() {
            if (try source.next() != .array_begin) return error.UnexpectedToken;
            var list = std.ArrayList(Raw).empty;
            defer list.deinit(alloc);
            var total: usize = 0;
            while (try source.peekNextTokenType() != .array_end) {
                if (list.items.len == max_types) return error.UnexpectedToken;
                const raw = try Raw.jsonParse(alloc, source, options);
                total +|= raw.bytes.len;
                if (total > max_registry_bytes * 2 or raw.bytes.len > types.max_encoded_bytes * 2) return error.UnexpectedToken;
                try list.append(alloc, raw);
            }
            _ = try source.next();
            return .{ .values = try list.toOwnedSlice(alloc) };
        }
    };
    const Envelope = struct { named_types: ?Layouts = null };
    var parsed = std.json.parseFromSlice(Envelope, a, if (schema_json.len == 0) "{}" else schema_json, .{ .ignore_unknown_fields = true }) catch |err| return if (err == error.OutOfMemory) err else error.InvalidSchemaUpdateRequest;
    defer parsed.deinit();
    const layouts = if (parsed.value.named_types) |list| list.values else &.{};
    const guards = try a.alloc(Guard, layouts.len);
    errdefer a.free(guards);
    var previous: u64 = 0;
    for (layouts, guards) |layout, *guard| {
        var header = std.json.parseFromSlice(struct { identity: wire.SQLNamedTypeIdentity }, a, layout.bytes, .{ .ignore_unknown_fields = true }) catch |err| return if (err == error.OutOfMemory) err else error.InvalidSchemaUpdateRequest;
        defer header.deinit();
        const identity = header.value.identity;
        if (identity.id <= previous or identity.namespace_id == 0 or identity.generation == 0) return error.InvalidSchemaUpdateRequest;
        previous = identity.id;
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update("antfly:sql-named-layout:v1\x00");
        hash.update(layout.bytes);
        var result: [32]u8 = undefined;
        hash.final(&result);
        guard.* = .{ .id = identity.id, .namespace_id = identity.namespace_id, .generation = identity.generation, .digest = result };
    }
    return guards;
}

/// Both active writes and the retained read layout keep a type alive. A
/// schema migration cannot release its catalog dependency while historical
/// rows still need that decoder. Divergent executable generations require the
/// coordinated type-activation protocol, never an implicit snapshot upgrade.
pub fn guardsForDefinitionsAlloc(a: A, write: []const u8, read: []const u8) ![]Guard {
    const left = try guardsAlloc(a, write);
    defer a.free(left);
    const right = try guardsAlloc(a, read);
    defer a.free(right);
    var guards = std.ArrayList(Guard).empty;
    defer guards.deinit(a);
    var i: usize = 0;
    var j: usize = 0;
    while (i < left.len or j < right.len) {
        if (guards.items.len == max_types) return error.InvalidSchemaUpdateRequest;
        if (i < left.len and j < right.len and left[i].id == right[j].id) {
            if (!std.meta.eql(left[i], right[j])) return error.InvalidSchemaUpdateRequest;
            try guards.append(a, left[i]);
            i += 1;
            j += 1;
        } else if (j == right.len or (i < left.len and left[i].id < right[j].id)) {
            try guards.append(a, left[i]);
            i += 1;
        } else {
            try guards.append(a, right[j]);
            j += 1;
        }
    }
    return guards.toOwnedSlice(a);
}

/// The caller's DOM region owns both the replacement directory and its DTO
/// buffers. Normalize snapshots before creating a durable mutation witness.
pub fn canonicalizeSchemaLeaky(a: A, root: *std.json.Value, records: []const types.Record) !void {
    const layouts = try a.alloc(wire.SQLNamedTypeLayout, records.len);
    for (records, layouts) |record, *layout| layout.* = try toWireLeaky(a, record);
    const encoded = try std.json.Stringify.valueAlloc(a, layouts, .{});
    const value = try std.json.parseFromSliceLeaky(std.json.Value, a, encoded, .{ .parse_numbers = false });
    try root.object.put(a, "named_types", value);
}

fn fromWire(a: A, input: wire.SQLNamedTypeLayout) !types.Record {
    if (!std.mem.eql(u8, input.kind, "enum") or input.labels.len > types.max_labels) return error.InvalidSchemaUpdateRequest;
    var origin: [16]u8 = undefined;
    try decodeHex(&origin, input.identity.origin_id);
    const identity: types.Identity = .{ .id = input.identity.id, .namespace_id = input.identity.namespace_id, .generation = input.identity.generation, .origin_id = origin, .oid = input.identity.oid, .array_oid = input.identity.array_oid };
    identity.validate() catch return error.InvalidSchemaUpdateRequest;
    const labels = try a.alloc(types.Label, input.labels.len);
    var initialized: usize = 0;
    errdefer {
        for (labels[0..initialized]) |label| {
            a.free(label.text);
            a.free(label.order_key);
        }
        a.free(labels);
    }
    for (input.labels, labels) |source, *label| {
        if (source.text.len > types.max_label_bytes or source.order_key.len == 0 or source.order_key.len > types.max_order_bytes * 2 or source.order_key.len % 2 != 0) return error.InvalidSchemaUpdateRequest;
        const text = try a.dupe(u8, source.text);
        errdefer a.free(text);
        const rank = try a.alloc(u8, source.order_key.len / 2);
        errdefer a.free(rank);
        try decodeHex(rank, source.order_key);
        label.* = .{ .id = source.id, .text = text, .order_key = rank };
        initialized += 1;
    }
    const record: types.Record = .{ .identity = identity, .owner = .auth_disabled, .definition = .{ .labels = labels } };
    record.validate(a) catch |err| return if (err == error.OutOfMemory) err else error.InvalidSchemaUpdateRequest;
    return record;
}

pub const Registry = struct {
    records: []types.Record,
    views: []values.View,
    alloc: A,

    pub fn init(a: A, input: []const wire.SQLNamedTypeLayout) !Registry {
        if (input.len > max_types) return error.InvalidSchemaUpdateRequest;
        const records = try a.alloc(types.Record, input.len);
        var records_initialized: usize = 0;
        errdefer {
            for (records[0..records_initialized]) |*record| record.deinitOwned(a);
            a.free(records);
        }
        var origins = std.AutoHashMapUnmanaged([16]u8, void).empty;
        defer origins.deinit(a);
        var previous_id: u64 = 0;
        var total: usize = 0;
        for (input, records) |source, *record| {
            record.* = try fromWire(a, source);
            records_initialized += 1;
            if (record.identity.id <= previous_id or (try origins.getOrPut(a, record.identity.origin_id)).found_existing) return error.InvalidSchemaUpdateRequest;
            previous_id = record.identity.id;
            const encoded = record.encode(a) catch |err| return if (err == error.OutOfMemory) err else error.InvalidSchemaUpdateRequest;
            defer a.free(encoded);
            total = std.math.add(usize, total, encoded.len + 4) catch return error.InvalidSchemaUpdateRequest;
            if (total > max_registry_bytes) return error.InvalidSchemaUpdateRequest;
        }
        const views = try a.alloc(values.View, records.len);
        var views_initialized: usize = 0;
        errdefer {
            for (views[0..views_initialized]) |*view| view.deinit(a);
            a.free(views);
        }
        for (records, views) |record, *view| {
            view.* = try values.View.init(a, record);
            views_initialized += 1;
        }
        return .{ .alloc = a, .records = records, .views = views };
    }
    pub fn parseValue(a: A, input: std.json.Value) !Registry {
        if (input != .array or input.array.items.len > max_types) return error.InvalidSchemaUpdateRequest;
        var parsed = std.json.parseFromValue([]const wire.SQLNamedTypeLayout, a, input, .{ .allocate = .alloc_always }) catch |err| return if (err == error.OutOfMemory) err else error.InvalidSchemaUpdateRequest;
        defer parsed.deinit();
        return init(a, parsed.value);
    }
    pub fn deinit(self: *Registry) void {
        for (self.views) |*view| view.deinit(self.alloc);
        self.alloc.free(self.views);
        for (self.records) |*record| record.deinitOwned(self.alloc);
        self.alloc.free(self.records);
        self.* = undefined;
    }
    pub fn byId(self: Registry, id: u64) ?*const values.View {
        var lower: usize = 0;
        var upper = self.records.len;
        while (lower < upper) {
            const middle = lower + (upper - lower) / 2;
            const current = self.records[middle].identity.id;
            if (current == id) return &self.views[middle];
            if (current < id) lower = middle + 1 else upper = middle;
        }
        return null;
    }
    pub fn cloneRecords(self: Registry, a: A) ![]types.Record {
        const records = try a.alloc(types.Record, self.records.len);
        var initialized: usize = 0;
        errdefer {
            for (records[0..initialized]) |*record| record.deinitOwned(a);
            a.free(records);
        }
        for (self.records, records) |record, *owned| {
            owned.* = try record.cloneOwned(a);
            initialized += 1;
        }
        return records;
    }
};

pub fn freeRecords(a: A, records: []const types.Record) void {
    for (records) |record| {
        var owned = record;
        owned.deinitOwned(a);
    }
    a.free(records);
}

test "SQL named enum generated layouts normalize catalog ownership and bind canonical executable identity" {
    const a = std.testing.allocator;
    var labels = try types.Owned.create(a, &.{ "z", "a", "" });
    defer labels.deinit();
    var record: types.Record = .{ .identity = try types.Identity.init(7, 2, 1, @splat(3)), .definition = labels.definition, .owner = .auth_disabled };
    const original = try digest(a, record);
    record.owner = .{ .user = .{ .name = "type_owner", .instance_id = @splat(1) } };
    try std.testing.expectEqualDeep(original, try digest(a, record));
    const bytes = try canonicalAlloc(a, record);
    defer a.free(bytes);
    var parsed = try std.json.parseFromSlice(std.json.Value, a, bytes, .{});
    defer parsed.deinit();
    var region = std.heap.ArenaAllocator.init(a);
    defer region.deinit();
    const array: std.json.Value = .{ .array = std.json.Array.fromOwnedSlice(region.allocator(), try region.allocator().dupe(std.json.Value, &.{parsed.value})) };
    var registry = try Registry.parseValue(a, array);
    defer registry.deinit();
    try std.testing.expect(registry.records[0].owner == .auth_disabled);
    try std.testing.expectEqualStrings("", try registry.byId(7).?.output(try registry.byId(7).?.input("")));
    try std.testing.expect(registry.byId(8) == null);
    try std.testing.expectEqualDeep(original, try digest(a, registry.records[0]));
}

test "SQL named enum generated layout registries unwind all owned decode and directory allocations" {
    const Run = struct {
        fn run(a: A, input: []const wire.SQLNamedTypeLayout) !void {
            var registry = try Registry.init(a, input);
            defer registry.deinit();
            const copy = try registry.cloneRecords(a);
            defer freeRecords(a, copy);
            _ = try registry.byId(7).?.input("a");
        }
    };
    const a = std.testing.allocator;
    var region = std.heap.ArenaAllocator.init(a);
    defer region.deinit();
    var labels = try types.Owned.create(a, &.{ "z", "a" });
    defer labels.deinit();
    const record: types.Record = .{ .identity = try types.Identity.init(7, 2, 1, @splat(3)), .definition = labels.definition, .owner = .auth_disabled };
    const input = [_]wire.SQLNamedTypeLayout{try toWireLeaky(region.allocator(), record)};
    try @import("antfly_platform").allocator.checkAllAllocationFailures(a, Run.run, .{&input});
}

fn testSchemaAlloc(a: A, record: types.Record) ![]u8 {
    var region = std.heap.ArenaAllocator.init(a);
    defer region.deinit();
    const layouts = [_]wire.SQLNamedTypeLayout{try toWireLeaky(region.allocator(), record)};
    return std.json.Stringify.valueAlloc(a, .{
        .version = @as(u32, 1),
        .storage_mode = "relational",
        .default_type = "row",
        .enforce_types = true,
        .named_types = &layouts,
        .document_schemas = .{ .row = .{ .schema = .{
            .type = "object",
            .additionalProperties = false,
            .properties = .{
                .state = .{ .type = "sql_enum", .@"x-antfly-sql-type-id" = record.identity.id, .nullable = true },
                .previous = .{ .type = "sql_enum", .@"x-antfly-sql-type-id" = record.identity.id, .nullable = true },
            },
        } } },
    }, .{});
}

test "SQL named enum public schemas bind native preparation canonical restore and immutable validated epochs" {
    const a = std.testing.allocator;
    const api = @import("../schema/mod.zig");
    const native = @import("../storage/schema.zig");
    const mapper = @import("../storage/db/document_mapper.zig");
    const codec = @import("../storage/db/algebraic/relational_row_codec.zig");
    const epochs = @import("../storage/db/schema_registry.zig");
    var labels = try types.Owned.create(a, &.{ "z", "a", "" });
    defer labels.deinit();
    const record: types.Record = .{ .identity = try types.Identity.init(7, 2, 1, @splat(3)), .definition = labels.definition, .owner = .auth_disabled };
    const json = try testSchemaAlloc(a, record);
    defer a.free(json);
    const canonical = try api.parseSchemaUpdateRequest(a, json);
    defer a.free(canonical);
    var validator = try api.CompiledTableValidator.init(a, canonical);
    var validator_owned = true;
    defer if (validator_owned) validator.deinit(a);
    const table = try api.deriveRuntimeTableSchema(a, validator.schema);
    var table_owned = true;
    defer if (table_owned) native.freeSchema(a, table);
    try std.testing.expectEqual(@as(usize, 1), table.named_types.len);
    try std.testing.expectEqual(@as(u32, 39), native.requiredReaderVersion(table));
    const frame = try native.serializeSchema(a, table);
    defer a.free(frame);
    const cold = try native.deserializeSchema(a, frame);
    defer native.freeSchema(a, cold);
    try std.testing.expectEqualDeep(table.named_types[0].identity, cold.named_types[0].identity);
    try std.testing.expect(validator.schema.document_schemas[0].properties[0].enum_view == validator.schema.document_schemas[0].properties[1].enum_view);
    const epoch = try epochs.Epoch.createOwnedValidated(a, table, validator);
    table_owned = false;
    validator_owned = false;
    defer epoch.release();
    for ([_][]const u8{ "{\"state\":\"z\",\"previous\":null}", "{\"state\":\"\"}", "{}" }) |input| {
        var prepared = try mapper.PreparedRelationalWrite.init(a, "one", input, epoch.validator, epoch.schema, &epoch.physical_layout);
        defer prepared.deinit(a);
        try prepared.finalizeMetadata(1);
        var restored = try codec.validateCanonicalAndMaterializeOrdinalRootWithLayoutAlloc(a, prepared.packed_row, epoch.schema, &epoch.physical_layout);
        defer restored.deinit(a);
        try epoch.validator.?.validateTypedStoredRoot(a, &restored.root, try codec.ordinalRowViewTrusted(prepared.packed_row, epoch.schema, &epoch.physical_layout));
    }
    for ([_][]const u8{ "{\"state\":\"Z\"}", "{\"state\":1}", "{\"state\":true}" }) |input| {
        try std.testing.expectError(error.InvalidBatchRequest, mapper.PreparedRelationalWrite.init(a, "one", input, epoch.validator, epoch.schema, &epoch.physical_layout));
    }
}

test "SQL named enum public schema binding rejects forged references and nonroot contracts" {
    const a = std.testing.allocator;
    const api = @import("../schema/mod.zig");
    var labels = try types.Owned.create(a, &.{ "z", "a" });
    defer labels.deinit();
    const record: types.Record = .{ .identity = try types.Identity.init(7, 2, 1, @splat(3)), .definition = labels.definition, .owner = .auth_disabled };
    const json = try testSchemaAlloc(a, record);
    defer a.free(json);
    const Scenario = enum { missing_layout, missing_id, wrong_id, document_mode, nested, misplaced_builtin };
    for (std.enums.values(Scenario)) |scenario| {
        var region = std.heap.ArenaAllocator.init(a);
        defer region.deinit();
        const arena = region.allocator();
        var root = try std.json.parseFromSliceLeaky(std.json.Value, arena, json, .{});
        const row = root.object.getPtr("document_schemas").?.object.getPtr("row").?.object.getPtr("schema").?;
        const state = row.object.getPtr("properties").?.object.getPtr("state").?;
        switch (scenario) {
            .missing_layout => {
                _ = root.object.orderedRemove("named_types");
            },
            .missing_id => {
                _ = state.object.orderedRemove("x-antfly-sql-type-id");
            },
            .wrong_id => try state.object.put(arena, "x-antfly-sql-type-id", .{ .integer = 99 }),
            .document_mode => try root.object.put(arena, "storage_mode", .{ .string = "document" }),
            .nested => {
                var nested = std.json.ObjectMap.empty;
                try nested.put(arena, "hidden", state.*);
                state.* = .{ .object = .empty };
                try state.object.put(arena, "type", .{ .string = "object" });
                try state.object.put(arena, "properties", .{ .object = nested });
            },
            .misplaced_builtin => try state.object.put(arena, "x-antfly-sql-type", .{ .string = "text" }),
        }
        const bad = try std.json.Stringify.valueAlloc(arena, root, .{});
        try std.testing.expectError(error.InvalidSchemaUpdateRequest, api.parseValidatedTableSchema(a, bad));
    }
}

test "SQL named enum public schema compiler unwinds complete validator and schema allocations" {
    const Run = struct {
        fn run(a: A, json: []const u8) !void {
            var prepared = try @import("../schema/mod.zig").PreparedSchemaDefinition.init(a, json);
            defer prepared.deinit(a);
        }
    };
    const a = std.testing.allocator;
    var labels = try types.Owned.create(a, &.{ "z", "a", "" });
    defer labels.deinit();
    const record: types.Record = .{ .identity = try types.Identity.init(7, 2, 1, @splat(3)), .definition = labels.definition, .owner = .auth_disabled };
    const json = try testSchemaAlloc(a, record);
    defer a.free(json);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(a, Run.run, .{json});
}

test "SQL named enum catalog guards share canonical executable fingerprints without compiling directories" {
    const a = std.testing.allocator;
    var labels = try types.Owned.create(a, &.{ "z", "a", "" });
    defer labels.deinit();
    const record: types.Record = .{ .identity = try types.Identity.init(7, 2, 1, @splat(3)), .definition = labels.definition, .owner = .auth_disabled };
    const json = try testSchemaAlloc(a, record);
    defer a.free(json);
    const guards = try guardsAlloc(a, json);
    defer a.free(guards);
    try std.testing.expectEqual(@as(usize, 1), guards.len);
    try std.testing.expectEqual(@as(u64, 7), guards[0].id);
    try std.testing.expectEqualDeep(try digest(a, record), guards[0].digest);
    const none = try guardsAlloc(a, "{}");
    defer a.free(none);
    try std.testing.expectEqual(@as(usize, 0), none.len);
}

test "SQL named enum reference discovery binds only physical columns and ignores readonly assertions" {
    const api = @import("../schema/mod.zig");
    const a = std.testing.allocator;
    const source =
        \\{"storage_mode":"relational","default_type":"row","named_types":{"untrusted":true},"document_schemas":{"row":{"schema":{"type":"object","additionalProperties":false,"properties":{"a":{"type":"sql_enum","x-antfly-sql-type-id":9},"b":{"type":"sql_enum","x-antfly-sql-type-id":7},"c":{"type":"sql_enum","x-antfly-sql-type-id":9},"payload":{"type":"json","const":{"x-antfly-sql-type-id":100}}}}}}}
    ;
    var parsed = try std.json.parseFromSlice(std.json.Value, a, source, .{ .parse_numbers = false });
    defer parsed.deinit();
    const ids = try api.namedTypeReferencesAlloc(a, parsed.value);
    defer a.free(ids);
    try std.testing.expectEqualSlices(u64, &.{ 7, 9 }, ids);
    const Check = struct {
        fn run(alloc: A, value: std.json.Value) !void {
            const refs = try api.namedTypeReferencesAlloc(alloc, value);
            defer alloc.free(refs);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(a, Check.run, .{parsed.value});
    const invalid = [_][]const u8{
        \\{"storage_mode":"document","document_schemas":{"row":{"schema":{"type":"object","properties":{"a":{"type":"sql_enum","x-antfly-sql-type-id":9}}}}}}
        ,
        \\{"storage_mode":"relational","document_schemas":{"row":{"schema":{"type":"object","properties":{"a":{"type":"object","properties":{"nested":{"type":"sql_enum","x-antfly-sql-type-id":9}}}}}}}}
        ,
        \\{"storage_mode":"relational","document_schemas":{"row":{"schema":{"type":"object","properties":{"a":{"type":"sql_enum","x-antfly-sql-type-id":0}}}}}}
        ,
        \\{"storage_mode":"relational","document_schemas":{"row":{"schema":{"type":"object","properties":{"a":{"type":"sql_enum","x-antfly-sql-type-id":1,"x-antfly-sql-type":"text"}}}}}}
        ,
    };
    for (invalid) |bytes| {
        var bad = try std.json.parseFromSlice(std.json.Value, a, bytes, .{});
        defer bad.deinit();
        try std.testing.expectError(error.InvalidSchemaUpdateRequest, api.namedTypeReferencesAlloc(a, bad.value));
    }
}

test "SQL named enum escaped catalog guards and reordered public snapshots retain canonical authority" {
    const a = std.testing.allocator;
    var labels = try types.Owned.create(a, &.{ "z", "a", "" });
    defer labels.deinit();
    const record: types.Record = .{ .identity = try types.Identity.init(7, 2, 1, @splat(3)), .definition = labels.definition, .owner = .auth_disabled };
    const json = try testSchemaAlloc(a, record);
    defer a.free(json);
    const at = std.mem.indexOf(u8, json, "named_types").?;
    const escaped = try std.fmt.allocPrint(a, "{s}na\\u006ded_types{s}", .{ json[0..at], json[at + "named_types".len ..] });
    defer a.free(escaped);
    const guards = try guardsAlloc(a, escaped);
    defer a.free(guards);
    try std.testing.expectEqual(@as(usize, 1), guards.len);
    try std.testing.expectEqualDeep(try digest(a, record), guards[0].digest);
    var region = std.heap.ArenaAllocator.init(a);
    defer region.deinit();
    const scratch = region.allocator();
    var root = try std.json.parseFromSliceLeaky(std.json.Value, scratch, escaped, .{ .parse_numbers = false });
    const layout = &root.object.getPtr("named_types").?.array.items[0];
    // Change only object key order; JSON identity and the executable registry
    // remain identical. Admission must normalize before hashing the snapshot.
    const kind = layout.object.fetchOrderedRemove("kind").?.value;
    try layout.object.put(scratch, "kind", kind);
    const reordered = try std.json.Stringify.valueAlloc(scratch, root, .{});
    const normalized = try @import("../schema/mod.zig").parseSchemaUpdateRequest(a, reordered);
    defer a.free(normalized);
    const normalized_guards = try guardsAlloc(a, normalized);
    defer a.free(normalized_guards);
    try std.testing.expectEqualDeep(guards[0], normalized_guards[0]);
}

test "SQL named enum public FK admission shares exact type identity and immutable history" {
    const a = std.testing.allocator;
    var labels = try types.Owned.create(a, &.{ "z", "a" });
    defer labels.deinit();
    const record: types.Record = .{ .identity = try types.Identity.init(7, 2, 1, @splat(3)), .definition = labels.definition, .owner = .auth_disabled };
    var region = std.heap.ArenaAllocator.init(a);
    defer region.deinit();
    const scratch = region.allocator();
    const json = try testSchemaAlloc(scratch, record);
    var parent = try std.json.parseFromSliceLeaky(std.json.Value, scratch, json, .{});
    const unique = try std.json.parseFromSliceLeaky(std.json.Value, scratch, "[{\"name\":\"pk\",\"columns\":[\"state\"]}]", .{});
    try parent.object.put(scratch, "unique_constraints", unique);
    var child = try std.json.parseFromSliceLeaky(std.json.Value, scratch, json, .{});
    const foreign = try std.json.parseFromSliceLeaky(std.json.Value, scratch, "[{\"name\":\"fk\",\"child_columns\":[\"state\"],\"parent_table\":\"parents\",\"parent_columns\":[\"state\"]}]", .{});
    try child.object.put(scratch, "foreign_keys", foreign);
    const child_json = try std.json.Stringify.valueAlloc(scratch, child, .{});
    const parent_json = try std.json.Stringify.valueAlloc(scratch, parent, .{});
    try @import("../schema/relational_foreign_key_target.zig").validate(a, child_json, "parents", parent_json);
    const layout = &parent.object.getPtr("named_types").?.array.items[0];
    try layout.object.getPtr("identity").?.object.put(scratch, "origin_id", .{ .string = "08080808080808080808080808080808" });
    const impostor = try std.json.Stringify.valueAlloc(scratch, parent, .{});
    try std.testing.expectError(error.ForeignKeyTypeMismatch, @import("../schema/relational_foreign_key_target.zig").validate(a, child_json, "parents", impostor));
}

test "SQL named enum reader contracts and dependencies retain both write and historical layouts" {
    const a = std.testing.allocator;
    const api = @import("../schema/mod.zig");
    var labels = try types.Owned.create(a, &.{ "z", "a" });
    defer labels.deinit();
    const record: types.Record = .{ .identity = try types.Identity.init(7, 2, 1, @splat(3)), .definition = labels.definition, .owner = .auth_disabled };
    const json = try testSchemaAlloc(a, record);
    defer a.free(json);
    for ([_][2][]const u8{ .{ json, "" }, .{ json, json }, .{ "{}", json } }) |pair| {
        const guards = try guardsForDefinitionsAlloc(a, pair[0], pair[1]);
        defer a.free(guards);
        try std.testing.expectEqual(@as(usize, 1), guards.len);
        try std.testing.expectEqual(@as(u64, 7), guards[0].id);
        const requirements = (try api.namedTypeReaderRequirementsForDefinitions(a, pair[0], pair[1])).?;
        try std.testing.expectEqual(@as(u32, 39), requirements.semantic_version);
        try std.testing.expectEqual(@as(u32, 39), requirements.encoding_version);
    }
    try std.testing.expect(try api.namedTypeReaderRequirementsForDefinitions(a, "{}", "") == null);
    var other = record;
    other.identity = try types.Identity.init(9, 2, 1, @splat(4));
    const other_json = try testSchemaAlloc(a, other);
    defer a.free(other_json);
    const combined = try guardsForDefinitionsAlloc(a, other_json, json);
    defer a.free(combined);
    try std.testing.expectEqual(@as(usize, 2), combined.len);
    try std.testing.expectEqual(@as(u64, 7), combined[0].id);
    try std.testing.expectEqual(@as(u64, 9), combined[1].id);
    var changed = record;
    changed.identity.generation += 1;
    const changed_json = try testSchemaAlloc(a, changed);
    defer a.free(changed_json);
    try std.testing.expectError(error.InvalidSchemaUpdateRequest, guardsForDefinitionsAlloc(a, json, changed_json));
    const Check = struct {
        fn run(alloc: A, write: []const u8, read: []const u8) !void {
            const guards = try guardsForDefinitionsAlloc(alloc, write, read);
            defer alloc.free(guards);
            _ = try api.namedTypeReaderRequirementsForDefinitions(alloc, write, read);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(a, Check.run, .{ json, other_json });
}
