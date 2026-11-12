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

//! Validate the actual native extraction program before attesting readiness
//! or closure. Matching an advertised definition digest alone is insufficient.
const std = @import("std");
const local = @import("antfly_local_sources");
const t = local.retained_dataset_types;
const capture = local.storage_retained_series_capture;
const Projection = @typeInfo(@FieldType(t.Summary, "projection")).optional.child;
const Reference = @FieldType(Projection, "timestamp");
const public_schema = local.schema_mod;
const A = std.mem.Allocator;

fn optionalEqual(left: ?[]const u8, right: ?[]const u8) bool {
    if (left == null or right == null) return left == null and right == null;
    return std.mem.eql(u8, left.?, right.?);
}
fn property(properties: []const public_schema.DocumentProperty, name: []const u8) ?public_schema.DocumentProperty {
    for (properties) |p| if (std.mem.eql(u8, p.name, name)) return p;
    return null;
}
fn dimensionType(properties: []const public_schema.DocumentProperty, reference: Reference) !capture.DimensionType {
    var p = property(properties, reference.column) orelse return error.RetainedSeriesDefinitionMismatch;
    for (reference.path) |part| p = switch (part) {
        .key => |key| property(p.properties, key) orelse return error.RetainedSeriesDefinitionMismatch,
        .index => |index| if (index < p.prefix_items.len) p.prefix_items[index] else if (p.item) |item| item.* else return error.RetainedSeriesDefinitionMismatch,
    };
    const kind = public_schema.runtimeRelationalColumnType(p) orelse return error.RetainedSeriesDefinitionMismatch;
    return switch (kind) {
        .integer => .integer,
        .number => .number,
        .boolean => .boolean,
        .string => .string,
        .datetime => .datetime,
        else => error.RetainedSeriesDefinitionMismatch,
    };
}

/// Public field names denote literal top-level columns. Freeze that identity
/// before fingerprinting the definition; never reinterpret dots during capture.
pub fn publicProjection(a: A, timestamp: []const u8, dimensions: []const []const u8, measures: []const t.Measure, source_json: []const u8) !Projection {
    var schema = public_schema.parseValidatedTableSchema(a, source_json) catch return error.InvalidDatasetDefinition;
    defer schema.deinit(a);
    if (schema.document_schemas.len == 0) return error.InvalidDatasetDefinition;
    const dimension_refs = try a.alloc(Reference, dimensions.len);
    const measure_refs = try a.alloc(?Reference, measures.len);
    for (dimensions, dimension_refs) |name, *reference| reference.* = .{ .column = name };
    for (measures, measure_refs) |measure, *reference| reference.* = if (measure.field) |name| .{ .column = name } else null;
    for (schema.document_schemas, 0..) |document, document_index| {
        const time_property = property(document.properties, timestamp) orelse return error.InvalidDatasetDefinition;
        // Capture accepts the declared datetime representation and integer
        // Unix nanoseconds. Untyped strings/numbers cannot certify timestamps.
        const time_kind = public_schema.runtimeRelationalColumnType(time_property) orelse return error.InvalidDatasetDefinition;
        const datetime_string = time_kind == .string and time_property.format != null and std.mem.eql(u8, time_property.format.?, "date-time");
        if (time_kind != .datetime and time_kind != .integer and !datetime_string) return error.InvalidDatasetDefinition;
        for (dimension_refs) |reference| _ = dimensionType(document.properties, reference) catch return error.InvalidDatasetDefinition;
        for (measures, measure_refs) |measure, *reference| {
            if (reference.*) |*field| {
                const p = property(document.properties, field.column) orelse return error.InvalidDatasetDefinition;
                const kind = public_schema.runtimeRelationalColumnType(p) orelse return error.InvalidDatasetDefinition;
                // This generation's persisted numeric state and serving type
                // are binary64. Exact integer/decimal domains need a versioned
                // state and result contract, never an implicit lossy cast.
                if (measure.law != .count) switch (kind) {
                    .number => {},
                    .integer, .numeric => return error.UnsupportedRetainedMeasureType,
                    else => return error.InvalidDatasetDefinition,
                };
                const json = kind == .json;
                if (document_index != 0 and (field.result == .json) != json) return error.InvalidDatasetDefinition;
                field.result = if (json) .json else .scalar;
            } else if (measure.law != .count) return error.InvalidDatasetDefinition;
        }
    }
    var projection: Projection = .{ .source_schema = undefined, .timestamp = .{ .column = timestamp }, .dimensions = dimension_refs, .measures = measure_refs };
    std.crypto.hash.sha2.Sha256.hash(source_json, &projection.source_schema, .{});
    try projection.validate();
    return projection;
}

pub fn specification(a: A, definition: t.Definition, partition: t.Id, incarnation: u64) !local.storage_retained_series.Spec {
    try definition.validate();
    const summary = definition.summary orelse return error.InvalidDatasetDefinition;
    const laws = try a.alloc(t.Law, summary.measures.len);
    const flags = try a.alloc(bool, summary.measures.len);
    var nonnull_count = false;
    for (summary.measures, laws, flags) |measure, *law, *flag| {
        law.* = measure.law;
        flag.* = measure.law == .count and measure.field != null;
        nonnull_count = nonnull_count or flag.*;
    }
    const spec: local.storage_retained_series.Spec = .{ .id = definition.id, .generation = definition.generation, .definition = try definition.fingerprint(a), .source_partition = partition, .source_incarnation = incarnation, .interval_ns = summary.interval_ns, .origin_ns = try std.fmt.parseInt(i128, summary.origin_ns, 10), .mutable_for_ns = summary.mutable_for_ns, .retention_ns = definition.retention_ns, .laws = laws, .count_nonnull = if (nonnull_count) flags else &.{} };
    try spec.validate();
    return spec;
}

/// First representable aligned bucket in the existing checked i128 bin domain.
/// Widen intermediate calculations so extreme origins do not overflow while
/// choosing a boundary. This is a data-domain bound, not a historical claim.
pub fn minimumBucketStart(spec: local.storage_retained_series.Spec) !i128 {
    try spec.validate();
    const minimum: i256 = std.math.minInt(i128);
    const origin: i256 = spec.origin_ns;
    const interval: i256 = spec.interval_ns;
    const offset_floor = @max(minimum, minimum - origin);
    const offset = -@divFloor(-offset_floor, interval) * interval;
    const start = std.math.cast(i128, origin + offset) orelse return error.RetainedSeriesTimeOverflow;
    if (try spec.bucket(start) != start) return error.RetainedSeriesTimeOverflow;
    return start;
}

pub fn compile(a: A, definition: t.Definition, source_json: []const u8, partition: t.Id, incarnation: u64) !capture.Binding {
    const spec = try specification(a, definition, partition, incarnation);
    const summary = definition.summary.?;
    const inputs = try a.alloc(?[]const u8, summary.measures.len);
    for (summary.measures, inputs) |measure, *field| field.* = measure.field;
    const dimension_types = try a.alloc(capture.DimensionType, summary.dimensions.len);
    if (summary.dimensions.len != 0) {
        var schema = try public_schema.parseValidatedTableSchema(a, source_json);
        defer schema.deinit(a);
        if (schema.document_schemas.len == 0) return error.RetainedSeriesDefinitionMismatch;
        for (summary.dimensions, dimension_types, 0..) |name, *kind, i| {
            const reference: Reference = if (summary.projection) |projection| projection.dimensions[i] else .{ .column = name };
            kind.* = try dimensionType(schema.document_schemas[0].properties, reference);
            for (schema.document_schemas[1..]) |document| if (try dimensionType(document.properties, reference) != kind.*) return error.RetainedSeriesDefinitionMismatch;
        }
    }
    const binding: capture.Binding = .{ .spec = spec, .timestamp = summary.timestamp, .dimensions = summary.dimensions, .dimension_types = dimension_types, .fields = inputs, .compiled = summary.projection };
    try validate(a, definition, binding, source_json);
    return binding;
}

pub fn validate(a: A, definition: t.Definition, binding: capture.Binding, source_json: []const u8) !void {
    try definition.validate();
    try binding.validate();
    const summary = definition.summary orelse return error.RetainedSeriesDefinitionMismatch;
    const spec = binding.spec;
    if (!std.mem.eql(u8, &spec.id, &definition.id) or spec.generation != definition.generation or
        !std.mem.eql(u8, &spec.definition, &try definition.fingerprint(a)) or
        spec.interval_ns != summary.interval_ns or spec.mutable_for_ns != summary.mutable_for_ns or
        spec.retention_ns != definition.retention_ns or spec.origin_ns != try std.fmt.parseInt(i128, summary.origin_ns, 10) or
        !std.mem.eql(u8, binding.timestamp, summary.timestamp) or binding.dimensions.len != summary.dimensions.len or
        binding.fields.len != summary.measures.len) return error.RetainedSeriesDefinitionMismatch;
    for (binding.dimensions, summary.dimensions) |actual, expected| if (!std.mem.eql(u8, actual, expected)) return error.RetainedSeriesDefinitionMismatch;
    for (summary.measures, binding.fields, spec.laws, 0..) |measure, field, law, i| {
        if (law != measure.law or !optionalEqual(field, measure.field) or
            (law == .count and spec.countsNonNull(i) != (measure.field != null))) return error.RetainedSeriesDefinitionMismatch;
    }
    if ((summary.projection == null) != (binding.compiled == null)) return error.RetainedSeriesDefinitionMismatch;
    if (summary.projection) |expected| {
        var digest: t.Id = undefined;
        std.crypto.hash.sha2.Sha256.hash(source_json, &digest, .{});
        if (!std.mem.eql(u8, &digest, &expected.source_schema)) return error.RetainedSeriesDefinitionMismatch;
        const desired = try std.json.Stringify.valueAlloc(a, expected, .{});
        defer a.free(desired);
        const actual = try std.json.Stringify.valueAlloc(a, binding.compiled.?, .{});
        defer a.free(actual);
        if (!std.mem.eql(u8, desired, actual)) return error.RetainedSeriesDefinitionMismatch;
    }
    // Until the native producer has a compiled predicate program, an advertised
    // source predicate must never be certified as if it had been evaluated.
    var predicate = try std.json.parseFromSlice(std.json.Value, a, summary.source_filter_json, .{});
    defer predicate.deinit();
    if (predicate.value != .object or predicate.value.object.count() != 1) return error.RetainedSeriesDefinitionMismatch;
    const all = predicate.value.object.get("match_all") orelse return error.RetainedSeriesDefinitionMismatch;
    if (all != .object or all.object.count() != 0) return error.RetainedSeriesDefinitionMismatch;
    if (summary.dimensions.len == 0) return;
    var schema = public_schema.parseValidatedTableSchema(a, source_json) catch return error.RetainedSeriesDefinitionMismatch;
    defer schema.deinit(a);
    if (schema.document_schemas.len == 0) return error.RetainedSeriesDefinitionMismatch;
    for (summary.dimensions, binding.dimension_types, 0..) |name, actual, i| {
        const reference: Reference = if (summary.projection) |projection| projection.dimensions[i] else .{ .column = name };
        for (schema.document_schemas) |document| if (try dimensionType(document.properties, reference) != actual) return error.RetainedSeriesDefinitionMismatch;
    }
}

test "retained owner native controls reject mismatched extraction programs despite a matching advertised digest" {
    const a = std.testing.allocator;
    var schema: t.Id = undefined;
    std.crypto.hash.sha2.Sha256.hash("{}", &schema, .{});
    const def: t.Definition = .{ .id = @splat(1), .generation = 1, .kind = .summary, .schema = schema, .schema_json = "{}", .authorization = @splat(2), .retention_ns = 1000, .local_residence_ns = 100, .summary = .{ .source = @splat(3), .timestamp = "ts", .interval_ns = 10, .mutable_for_ns = 100, .measures = &.{.{ .name = "n", .law = .count }} } };
    const binding: capture.Binding = .{ .spec = .{ .id = def.id, .generation = 1, .definition = try def.fingerprint(a), .source_partition = @splat(4), .source_incarnation = 1, .interval_ns = 10, .mutable_for_ns = 100, .retention_ns = 1000, .laws = &.{.count} }, .timestamp = "ts", .fields = &.{null} };
    try validate(a, def, binding, "{}");
    var wrong = binding;
    wrong.timestamp = "other";
    try std.testing.expectError(error.RetainedSeriesDefinitionMismatch, validate(a, def, wrong, "{}"));
    wrong = binding;
    wrong.spec.interval_ns = 11;
    try std.testing.expectError(error.RetainedSeriesDefinitionMismatch, validate(a, def, wrong, "{}"));
    var filtered = def;
    filtered.summary.?.source_filter_json = "{\"match_none\":{}}";
    wrong = binding;
    wrong.spec.definition = try filtered.fingerprint(a);
    try std.testing.expectError(error.RetainedSeriesDefinitionMismatch, validate(a, filtered, wrong, "{}"));
}

test "retained owner native controls compile snapshot history without changing legacy definition fingerprints" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const temp = arena.allocator();
    var schema: t.Id = undefined;
    std.crypto.hash.sha2.Sha256.hash("{}", &schema, .{});
    const old: t.Definition = .{ .id = @splat(1), .generation = 1, .kind = .summary, .schema = schema, .schema_json = "{}", .authorization = @splat(2), .retention_ns = 1000, .local_residence_ns = 100, .summary = .{ .source = @splat(3), .timestamp = "ts", .interval_ns = 10, .mutable_for_ns = 100, .measures = &.{.{ .name = "n", .law = .count }} } };
    const old_json = try std.json.Stringify.valueAlloc(temp, old, .{});
    try std.testing.expect(std.mem.indexOf(u8, old_json, "initial_history") == null);
    var snapshot = old;
    snapshot.format = 2;
    snapshot.summary.?.initial_history = .retained_source_rows;
    const json = try std.json.Stringify.valueAlloc(temp, snapshot, .{});
    var parsed = try std.json.parseFromSlice(t.Definition, a, json, .{});
    defer parsed.deinit();
    try parsed.value.validate();
    try std.testing.expectEqual(t.InitialHistory.retained_source_rows, parsed.value.summary.?.initial_history);
    try std.testing.expect(!std.mem.eql(u8, &try old.fingerprint(temp), &try snapshot.fingerprint(temp)));
    const binding = try compile(temp, snapshot, "{}", @splat(4), 1);
    try std.testing.expectEqual(@as(usize, 0), binding.spec.count_nonnull.len);
    for ([_]i128{ 0, -1, std.math.minInt(i128), std.math.maxInt(i128) }) |origin| {
        var spec = binding.spec;
        spec.origin_ns = origin;
        const first = try minimumBucketStart(spec);
        try std.testing.expectEqual(first, try spec.bucket(first));
    }
    snapshot.format = 1;
    try std.testing.expectError(error.InvalidDatasetDefinition, snapshot.validate());
}

test "retained owner native controls public creation freezes literal fields and validates the full extraction schema" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const schema_json =
        \\{"default_type":"doc","document_schemas":{"doc":{"schema":{"type":"object","properties":{"ts":{"type":"datetime"},"a.b":{"type":"number"},"a":{"type":"object","properties":{"b":{"type":"number"}}},"doc":{"type":"object"},"bad_time":{"type":"boolean"}}}}}}
    ;
    const admin = @import("retained_dataset_admin.zig");
    const source = try local.system_catalog_retained_datasets.Source.fromTable(a, .{ .table_id = @as(u64, 7), .name = "raw", .schema_json = schema_json }, "11111111111111111111111111111111".*);
    var input: admin.Create = .{ .source = .{ .table = "raw" }, .timestamp = "ts", .dimensions = &.{"a.b"}, .interval_ns = "10", .mutable_for_ns = "100", .retention_ns = "1000", .measures = &.{ .{ .name = "sum_v", .law = .sum, .field = "a.b" }, .{ .name = "json_n", .law = .count, .field = "doc" }, .{ .name = "all_n", .law = .count } } };
    const descriptor = try admin.descriptor(a, std.testing.io, input, .{ .lineage = source, .schema_json = schema_json, .catalog_revision = 1 }, @splat(3));
    // Persist/reload before compiling: the schema-bound program must survive
    // restart and travel with the definition used for archive state identity.
    const bytes = try std.json.Stringify.valueAlloc(a, descriptor, .{});
    var decoded = try local.system_catalog_retained_datasets.parse(a, bytes);
    defer decoded.deinit();
    const binding = try compile(a, decoded.value.definition, schema_json, @splat(4), 1);
    try std.testing.expect(binding.compiled != null);
    for ([_][]const u8{
        "{\"ts\":\"2026-01-01T00:00:00Z\",\"a\":{\"b\":7},\"doc\":null}",
        "{\"ts\":\"2026-01-01T00:00:00Z\",\"a.b\":null,\"a\":{\"b\":7},\"doc\":null}",
    }) |json| {
        const value = try std.json.parseFromSliceLeaky(std.json.Value, a, json, .{});
        const row = try capture.normalizeValue(a, binding, value);
        try std.testing.expectEqualStrings("[null]", row.dimensions);
        try std.testing.expect(row.values[0] == null);
        try std.testing.expectEqual(@as(?f64, 1), row.values[1]);
    }
    const present = try std.json.parseFromSliceLeaky(std.json.Value, a, "{\"ts\":\"2026-01-01T00:00:00Z\",\"a.b\":3,\"a\":{\"b\":7}}", .{});
    const row = try capture.normalizeValue(a, binding, present);
    try std.testing.expectEqual(@as(?f64, 3), row.values[0]);
    try std.testing.expect(row.values[1] == null);
    const public_source: admin.Source = .{ .lineage = source, .schema_json = schema_json, .catalog_revision = 1 };
    input.timestamp = "absent";
    try std.testing.expectError(error.InvalidDatasetDefinition, admin.descriptor(a, std.testing.io, input, public_source, @splat(3)));
    input.timestamp = "bad_time";
    try std.testing.expectError(error.InvalidDatasetDefinition, admin.descriptor(a, std.testing.io, input, public_source, @splat(3)));
    input.timestamp = "ts";
    input.measures = &.{.{ .name = "n", .law = .count, .field = "absent" }};
    try std.testing.expectError(error.InvalidDatasetDefinition, admin.descriptor(a, std.testing.io, input, public_source, @splat(3)));
    input.measures = &.{.{ .name = "n", .law = .sum, .field = "doc" }};
    try std.testing.expectError(error.InvalidDatasetDefinition, admin.descriptor(a, std.testing.io, input, public_source, @splat(3)));
}

test "retained owner native controls numeric domains are explicit and never silently rounded" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    for ([_][]const u8{ "{\"type\":\"integer\"}", "{\"type\":\"number\",\"x-antfly-sql-type\":\"numeric\"}" }) |property_json| {
        const schema_json = try std.mem.concat(a, u8, &.{ "{\"storage_mode\":\"relational\",\"default_type\":\"row\",\"document_schemas\":{\"row\":{\"schema\":{\"type\":\"object\",\"properties\":{\"ts\":{\"type\":\"integer\"},\"v\":", property_json, "}}}}}" });
        for ([_]t.Law{ .sum, .avg, .min, .max }) |law| {
            try std.testing.expectError(error.UnsupportedRetainedMeasureType, publicProjection(a, "ts", &.{}, &.{.{ .name = "value", .field = "v", .law = law }}, schema_json));
        }
        _ = try publicProjection(a, "ts", &.{}, &.{.{ .name = "n", .field = "v", .law = .count }}, schema_json);
    }
}
