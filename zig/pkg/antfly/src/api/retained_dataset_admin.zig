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

//! Compile public retained-summary definitions from a qualified source cut.
//! Dataset, source and storage identities are assigned by trusted ingress.
const std = @import("std");
const local = @import("antfly_local_sources");
const domain = local.system_catalog_domain;
const retained = local.system_catalog_retained_datasets;
const dataset = local.retained_dataset_types;
const A = std.mem.Allocator;

pub const Create = struct {
    source: domain.Target,
    timestamp: []const u8,
    dimensions: []const []const u8 = &.{},
    interval_ns: []const u8,
    origin_ns: []const u8 = "0",
    mutable_for_ns: []const u8,
    retention_ns: []const u8,
    measures: []const dataset.Measure,
};
pub const Source = struct {
    lineage: retained.Source,
    schema_json: []const u8,
    catalog_revision: u64,
};

pub fn descriptor(a: A, io: std.Io, input: Create, source: Source, storage_identity: dataset.Id) !retained.Descriptor {
    try input.source.validate();
    const interval = try duration(input.interval_ns);
    const mutable = try duration(input.mutable_for_ns);
    const retention = try duration(input.retention_ns);
    if (input.origin_ns.len > 40) return error.InvalidDatasetDefinition;
    const origin = std.fmt.parseInt(i128, input.origin_ns, 10) catch return error.InvalidDatasetDefinition;
    var id: dataset.Id = undefined;
    io.random(&id);
    if (dataset.zero(id)) return error.InvalidDatasetDefinition;
    var authorization = std.crypto.hash.sha2.Sha256.init(.{});
    authorization.update("antfly/retained-dataset-authority/v1");
    authorization.update(&id);
    var schema: dataset.Id = undefined;
    std.crypto.hash.sha2.Sha256.hash("{}", &schema, .{});
    var definition: dataset.Definition = .{
        .format = 2,
        .id = id,
        .generation = 1,
        .kind = .summary,
        .schema = schema,
        .schema_json = "{}",
        .authorization = authorization.finalResult(),
        .retention_ns = retention,
        .local_residence_ns = retention,
        .summary = .{ .source = source.lineage.identity(), .timestamp = input.timestamp, .dimensions = input.dimensions, .interval_ns = interval, .origin_ns = try std.fmt.allocPrint(a, "{d}", .{origin}), .mutable_for_ns = mutable, .measures = input.measures, .initial_history = .retained_source_rows },
    };
    try requireNumericSources(a, input.measures, source.schema_json);
    for (input.dimensions) |name| if (reservedColumn(name)) return error.InvalidDatasetDefinition;
    for (input.measures) |measure| if (reservedColumn(measure.name)) return error.InvalidDatasetDefinition;
    const binding = try @import("retained_owner_binding.zig").compile(a, definition, source.schema_json, @splat(1), 1);
    _ = try binding.spec.bucket(@import("antfly_platform").time.realtimeNs());
    // Freeze the output schema independently of the raw table schema.
    const Dimension = struct { name: []const u8, type: local.storage_retained_series_capture.DimensionType };
    const dimensions = try a.alloc(Dimension, input.dimensions.len);
    for (dimensions, input.dimensions, binding.dimension_types) |*field, name, kind| field.* = .{ .name = name, .type = kind };
    definition.schema_json = try std.json.Stringify.valueAlloc(a, .{ .timestamp = "bucket_start_ns", .dimensions = dimensions, .measures = input.measures }, .{});
    std.crypto.hash.sha2.Sha256.hash(definition.schema_json, &definition.schema, .{});
    const location = @import("../serverless/datasets/configured_archive.zig").locationIdentity(storage_identity);
    const result: retained.Descriptor = .{ .definition = definition, .source = source.lineage, .catalog_location = location, .catalog_storage_identity = storage_identity };
    try result.validateCreation();
    return result;
}

fn duration(value: []const u8) !u64 {
    if (value.len == 0 or value.len > 20) return error.InvalidDatasetDefinition;
    for (value) |digit| if (!std.ascii.isDigit(digit)) return error.InvalidDatasetDefinition;
    return std.fmt.parseInt(u64, value, 10) catch error.InvalidDatasetDefinition;
}

fn reservedColumn(name: []const u8) bool {
    return std.mem.eql(u8, name, "bucket_start") or std.mem.eql(u8, name, "_id");
}

fn requireNumericSources(a: A, measures: []const dataset.Measure, schema_json: []const u8) !void {
    const typed = for (measures) |measure| {
        if (measure.law != .count) break true;
    } else false;
    if (!typed) return;
    const schema_api = local.schema_mod;
    var schema = schema_api.parseValidatedTableSchema(a, schema_json) catch return error.InvalidDatasetDefinition;
    defer schema.deinit(a);
    if (schema.document_schemas.len == 0) return error.InvalidDatasetDefinition;
    for (measures) |measure| {
        if (measure.law == .count) continue;
        const field = measure.field orelse return error.InvalidDatasetDefinition;
        for (schema.document_schemas) |document| {
            const property = for (document.properties) |candidate| {
                if (std.mem.eql(u8, candidate.name, field)) break candidate;
            } else return error.InvalidDatasetDefinition;
            // The native numeric state consumes binary64. Reject exact
            // integer/NUMERIC measures until their own state codec exists.
            if (schema_api.runtimeRelationalColumnType(property) != .number) return error.InvalidDatasetDefinition;
        }
    }
}
