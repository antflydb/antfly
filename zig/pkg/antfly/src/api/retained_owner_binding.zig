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
