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

//! Shared native schema normalization and source-document graph contributions.
const std = @import("std");
const cypher = @import("cypher");
const A = std.mem.Allocator;
pub fn normalize(self: anytype, a: A, _: cypher.catalog.Graph, node: *const cypher.value.Node, apply_defaults: bool) !*const cypher.value.Node {
    try cypher.value.validatePropertyValue(.{ .map = node.properties });
    var schema_view = self.core.acquireSchemaView();
    defer if (schema_view) |*view| view.release();
    // With no schema expressions the overlay already is the normalized
    // postimage. Avoid a JSON round trip and another retained entity per row.
    const expressions = if (schema_view) |view|
        if (view.validator()) |validator| validator.execution.expressions else null
    else
        null;
    const program = expressions orelse return node;
    if (program.bindings.len == 0 and program.modifier_ordinals.len == 0) return node;
    // Only the normalized property values survive this callback. Keep schema
    // expression and JSON decode buffers out of the statement's retained arena,
    // while preserving its quota/failure allocator boundary.
    var scratch = std.heap.ArenaAllocator.init(@import("antfly_platform").allocator.reclaimingScratch(a));
    defer scratch.deinit();
    const temporary = scratch.allocator();
    var document = try std.json.parseFromSliceLeaky(std.json.Value, temporary, try std.json.Stringify.valueAlloc(temporary, cypher.value.Value{ .map = node.properties }, .{}), .{ .parse_numbers = false });
    try program.applyJsonWithPolicy(temporary, &document, if (apply_defaults) .apply_to_absent else .preserve_absence);
    const value = try cypher.value.Value.fromJson(a, document);
    const result = try a.create(cypher.value.Node);
    result.* = .{ .identity = node.identity, .properties = try cypher.value.entityProperties(a, value.map) };
    // Membership is recomputed by the statement overlay after normalization,
    // under its work/cancellation meter and with the complete graph binding.
    return result;
}
pub fn relationships(self: anytype, a: A, graph: cypher.catalog.Graph, node: *const cypher.value.Node) ![]const *const cypher.value.Relationship {
    if (graph.relationships.len == 0) return &.{};
    var scratch = std.heap.ArenaAllocator.init(@import("antfly_platform").allocator.reclaimingScratch(a));
    defer scratch.deinit();
    const temporary = scratch.allocator();
    const json = try cypher.mutations.document(temporary, node);
    const document = try std.json.parseFromSliceLeaky(std.json.Value, temporary, json, .{ .parse_numbers = false });
    var extracted = try @import("document_mapper.zig").extractWriteFromParsed(temporary, node.identity.key, json, document);
    defer extracted.deinit(temporary);
    self.core.lockApplyShared();
    defer self.core.unlockApplyShared();
    try @import("graph_field_plan.zig").fromCatalog(self.core, temporary, node.identity.key, document, &extracted);
    var result: std.ArrayList(*const cypher.value.Relationship) = .empty;
    for (extracted.graph_writes) |edge| for (graph.relationships) |mapping| {
        const source_table = graph.label(mapping.source_label).?.table;
        if (!std.mem.eql(u8, source_table, node.identity.table) or !std.mem.eql(u8, edge.index_name, mapping.index) or !std.mem.eql(u8, edge.edge_type, mapping.stored_type)) continue;
        const target_table = graph.label(mapping.target_label).?.table;
        const metadata = if (edge.metadata_json.len == 0) cypher.value.Value{ .map = &.{} } else try cypher.value.Value.fromJson(temporary, try std.json.parseFromSliceLeaky(std.json.Value, temporary, edge.metadata_json, .{ .parse_numbers = false }));
        if (metadata != .map) return error.CypherInvalidRelationshipMetadata;
        const actual_source = metadata.property("source_table");
        const actual_target = metadata.property("target_table");
        if (actual_source != .null and (actual_source != .string or !std.mem.eql(u8, actual_source.string, source_table))) continue;
        if (actual_target != .null and (actual_target != .string or !std.mem.eql(u8, actual_target.string, target_table))) continue;
        var properties: std.ArrayList(cypher.value.Entry) = .empty;
        for (metadata.map) |entry| if (!std.mem.eql(u8, entry.name, "weight") and !std.mem.eql(u8, entry.name, "source_table") and !std.mem.eql(u8, entry.name, "target_table")) try properties.append(temporary, entry);
        try properties.append(temporary, .{ .name = "weight", .value = .{ .number = edge.weight } });
        const relationship: cypher.value.Relationship = .{
            .identity = .{ .table = source_table, .key = try std.json.Stringify.valueAlloc(temporary, .{ edge.source, edge.target, edge.edge_type, edge.edge_id, node.identity.key, "field" }, .{}) },
            .index = mapping.index,
            .type = mapping.name,
            .source = .{ .table = source_table, .key = edge.source },
            .target = .{ .table = target_table, .key = edge.target },
            .ownership = .field,
            .producer = node.identity,
            .properties = try cypher.value.entityProperties(temporary, properties.items),
        };
        try result.append(a, (try (cypher.value.Value{ .relationship = &relationship }).clone(a)).relationship);
    };
    return result.toOwnedSlice(a);
}
