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

const std = @import("std");
const Allocator = std.mem.Allocator;
const types = @import("types.zig");
const index_manager_mod = @import("catalog/index_manager.zig");
const graph_metadata_tables = @import("../../graph/metadata_tables.zig");
const graph_mod = @import("../../graph/graph.zig");
pub const GraphArtifactWritePage = struct {
    writes: []types.GraphEdgeWrite,
    next_item_offset: ?usize,
    item_count: usize,
};
pub fn freeGraphWriteFields(alloc: Allocator, write: types.GraphEdgeWrite) void {
    var owned = write;
    owned.deinit(alloc);
}

pub fn graphWritesFromArtifactParsedPageAlloc(
    alloc: Allocator,
    index_name: []const u8,
    doc_key: []const u8,
    artifact_value: std.json.Value,
    source: index_manager_mod.GraphArtifactSource,
    artifact_content_type: []const u8,
    doc_value: ?std.json.Value,
    item_offset: usize,
    item_limit: usize,
    output_byte_limit: usize,
    relation_limit: usize,
) !GraphArtifactWritePage {
    var values: [2]std.json.Value = undefined;
    var value_count: usize = 0;
    switch (source.format) {
        .extraction_relation => {
            if (source.path.len == 0 or std.mem.eql(u8, source.path, "$")) {
                values[0] = artifact_value;
                value_count = 1;
            } else if (selectGraphArtifactPath(artifact_value, source.path)) |selected| {
                values[0] = selected;
                value_count = 1;
            }
        },
        .extraction_graph => {
            if (source.path.len > 0) {
                if (selectGraphArtifactPath(artifact_value, source.path)) |selected| {
                    values[0] = selected;
                    value_count = 1;
                }
            } else if (artifact_value == .object) {
                if (artifact_value.object.get("relations")) |relations| {
                    values[value_count] = relations;
                    value_count += 1;
                }
                if (artifact_value.object.get("edges")) |edges| {
                    values[value_count] = edges;
                    value_count += 1;
                }
            }
        },
    }

    var item_count: usize = 0;
    for (values[0..value_count]) |value| {
        item_count = std.math.add(usize, item_count, if (value == .array) value.array.items.len else 1) catch
            return error.ResourceLimitExceeded;
        if (item_count > relation_limit) return error.ResourceLimitExceeded;
    }
    if (item_offset > item_count) return error.InvalidRestoreState;

    var writes = std.ArrayListUnmanaged(types.GraphEdgeWrite).empty;
    errdefer {
        for (writes.items) |write| freeGraphWriteFields(alloc, write);
        writes.deinit(alloc);
    }
    var value_base: usize = 0;
    var processed_until = item_offset;
    var output_bytes: usize = 0;
    outer: for (values[0..value_count]) |value| {
        const items = if (value == .array) value.array.items else @as([]const std.json.Value, &.{value});
        const value_end = std.math.add(usize, value_base, items.len) catch return error.ResourceLimitExceeded;
        if (item_offset >= value_end) {
            value_base = value_end;
            continue;
        }
        const local_start = if (item_offset > value_base) item_offset - value_base else 0;
        for (items[local_start..], local_start..) |item, local_ordinal| {
            if (processed_until - item_offset >= item_limit) break :outer;
            const ordinal = value_base + local_ordinal;

            const before = writes.items.len;
            try appendRelationItem(
                alloc,
                &writes,
                index_name,
                doc_key,
                doc_value,
                item,
                ordinal,
                source.mapping,
                source.artifact_name,
                artifact_content_type,
                artifact_value,
                relation_limit,
            );
            if (writes.items.len > before) {
                const write = writes.items[writes.items.len - 1];
                const write_bytes = write.retainedBytes();
                if (writes.items.len > 1 and output_bytes +| write_bytes > output_byte_limit) {
                    const removed = writes.pop().?;
                    freeGraphWriteFields(alloc, removed);
                    break :outer;
                }
                output_bytes +|= write_bytes;
            }
            processed_until = ordinal + 1;
        }
        value_base = value_end;
    }

    // A zero-output run still consumes raw relation items. This is essential
    // for malformed/filtered arrays to make deterministic cursor progress.
    if (processed_until == item_offset and item_offset < item_count) {
        processed_until = @min(item_count, item_offset + item_limit);
    }
    return .{
        .writes = try writes.toOwnedSlice(alloc),
        .next_item_offset = if (processed_until < item_count) processed_until else null,
        .item_count = item_count,
    };
}

pub fn selectGraphArtifactPath(root: std.json.Value, path: []const u8) ?std.json.Value {
    var trimmed = path;
    if (std.mem.startsWith(u8, trimmed, "$.")) trimmed = trimmed[2..];
    if (std.mem.endsWith(u8, trimmed, "[*]")) trimmed = trimmed[0 .. trimmed.len - 3];
    if (trimmed.len == 0) return root;

    var current = root;
    var parts = std.mem.splitScalar(u8, trimmed, '.');
    while (parts.next()) |part| {
        if (part.len == 0) return null;
        if (current != .object) return null;
        current = current.object.get(part) orelse return null;
    }
    return current;
}

pub fn appendRelationItem(
    alloc: Allocator,
    writes: *std.ArrayListUnmanaged(types.GraphEdgeWrite),
    index_name: []const u8,
    doc_key: []const u8,
    doc_value: ?std.json.Value,
    item: std.json.Value,
    item_index: usize,
    mapping: index_manager_mod.GraphArtifactMapping,
    artifact_name: []const u8,
    artifact_content_type: []const u8,
    artifact_value: std.json.Value,
    edge_limit: usize,
) !void {
    if (item != .object) return;
    const mapped_edge_type = if (mapping.edge_type_template.len > 0)
        try renderGraphArtifactTemplateAlloc(alloc, mapping.edge_type_template, doc_key, doc_value, item, item_index, artifact_name, artifact_content_type, artifact_value)
    else
        null;
    defer if (mapped_edge_type) |value| alloc.free(value);
    const edge_type = if (mapped_edge_type) |value|
        std.mem.trim(u8, value, &std.ascii.whitespace)
    else
        jsonStringField(item, "type") orelse jsonStringField(item, "edge_type") orelse jsonStringField(item, "relation") orelse return;
    if (edge_type.len == 0) return;

    const mapped_source = if (mapping.source_template.len > 0)
        try renderGraphArtifactTemplateAlloc(alloc, mapping.source_template, doc_key, doc_value, item, item_index, artifact_name, artifact_content_type, artifact_value)
    else
        null;
    defer if (mapped_source) |value| alloc.free(value);
    // Materialized edges are routed and retired with their OWNING document —
    // always the producer. The topological source may differ: a relation
    // whose source endpoint canonically resolves (an extraction entity with a
    // resolver-minted key, via the injected "_entities" map) starts from that
    // canonical node, giving true entity->entity / entity->event topology
    // (zig/AUTOSCHEMA.md). GraphEdgeWrite.owner carries the producer for
    // artifact-key routing when the two diverge. A source referencing an
    // extraction entity that has no canonical identity yet is dropped, like
    // the matching target rule, when a resolver targets this artifact (the
    // caller injected an "_entities" map, possibly empty): the resolution
    // replay re-renders it. With no resolver configured no canonical key
    // will ever arrive, so the source keeps the owning document instead of
    // losing the edge (zig/GRAPH.md's V1 extractor-only graph). A source
    // that matches no extraction entity keeps the legacy document source.
    var source_table: ?[]const u8 = null;
    const source_doc = blk: {
        if (mapped_source) |value| break :blk value;
        const source_value = item.object.get("source") orelse break :blk doc_key;
        if (resolveGraphEndpointEntity(source_value, artifact_value)) |entity| {
            const canonical = canonicalEntityDocumentId(entity) orelse {
                if (artifact_value == .object and artifact_value.object.get("_entities") != null) return;
                break :blk doc_key;
            };
            // The resolved SOURCE endpoint's home table must survive into
            // edge metadata like the target's: a backward traversal from
            // the target otherwise assigns the source an unqualified
            // identity and hydrates it against the wrong table.
            source_table = canonicalEntityTable(entity);
            break :blk canonical;
        }
        // A plain-string source matching no extraction entity is an external
        // node id; any other unresolvable shape (e.g. legacy inline endpoint
        // objects) keeps the owning document as the source, the historical
        // contract.
        break :blk switch (source_value) {
            .string => |external| if (external.len > 0) external else doc_key,
            else => doc_key,
        };
    };
    if (source_doc.len == 0) return error.InvalidGraphEdges;
    const mapped_id = if (mapping.edge_id_template.len > 0)
        try renderGraphArtifactTemplateAlloc(alloc, mapping.edge_id_template, doc_key, doc_value, item, item_index, artifact_name, artifact_content_type, artifact_value)
    else
        null;
    defer if (mapped_id) |value| alloc.free(value);
    const edge_id = mapped_id orelse "";
    if (mapped_id != null and edge_id.len == 0) return error.InvalidGraphEdges;

    const mapped_target = if (mapping.target_template.len > 0)
        try renderGraphArtifactTemplateAlloc(alloc, mapping.target_template, doc_key, doc_value, item, item_index, artifact_name, artifact_content_type, artifact_value)
    else
        null;
    defer if (mapped_target) |value| alloc.free(value);
    const target_doc = if (mapped_target) |value| blk: {
        const trimmed = std.mem.trim(u8, value, &std.ascii.whitespace);
        if (trimmed.len == 0) return;
        break :blk trimmed;
    } else blk: {
        const target_value = item.object.get("target") orelse return;
        break :blk jsonEndpointDocumentIdResolved(target_value, artifact_value) orelse return;
    };
    const target_table: ?[]const u8 = if (mapped_target != null) null else blk: {
        const target_value = item.object.get("target") orelse break :blk null;
        const entity = resolveGraphEndpointEntity(target_value, artifact_value) orelse break :blk null;
        break :blk canonicalEntityTable(entity);
    };
    if (writes.items.len >= edge_limit) return error.ResourceLimitExceeded;

    const weight = if (mapping.weight_template.len > 0) blk: {
        const rendered = try renderGraphArtifactTemplateAlloc(alloc, mapping.weight_template, doc_key, doc_value, item, item_index, artifact_name, artifact_content_type, artifact_value);
        defer alloc.free(rendered);
        const trimmed = std.mem.trim(u8, rendered, &std.ascii.whitespace);
        break :blk if (trimmed.len > 0) try std.fmt.parseFloat(f64, trimmed) else 1.0;
    } else jsonFloatField(item, "weight") orelse jsonFloatField(item, "confidence") orelse 1.0;
    graph_mod.validateEdgeWeight(weight) catch return error.InvalidGraphEdges;
    const metadata_json = if (mapping.metadata_template_json.len > 0) blk: {
        const rendered = try renderGraphArtifactMetadataTemplateAlloc(alloc, mapping.metadata_template_json, doc_key, doc_value, item, item_index, artifact_name, artifact_content_type, artifact_value);
        // A custom metadata template must not silently strip the resolved
        // endpoint's home-table tag: without it the node looks same-table to
        // traversal identity, admission, routing, and hydration. An explicit
        // target_table in the template wins.
        const table = target_table orelse break :blk rendered;
        defer alloc.free(rendered);
        break :blk try prependTargetTableToMetadataJsonAlloc(alloc, table, rendered);
    } else if (target_table) |table|
        // The same cross-table endpoint tag mention edges carry: traversal
        // and node admission route the resolved target to its home table.
        try prependTargetTableToItemMetadataAlloc(alloc, table, item)
    else
        try std.json.Stringify.valueAlloc(alloc, item, .{});
    var owned_metadata = metadata_json;
    errdefer alloc.free(owned_metadata);
    if (source_table) |table| {
        const tagged = try graph_metadata_tables.withTableAlloc(alloc, "source_table", table, owned_metadata, mapping.metadata_template_json.len > 0);
        alloc.free(owned_metadata);
        owned_metadata = tagged;
    }

    const owned_index_name = try alloc.dupe(u8, index_name);
    errdefer alloc.free(owned_index_name);
    const owned_source = try alloc.dupe(u8, source_doc);
    errdefer alloc.free(owned_source);
    const owned_target = try alloc.dupe(u8, target_doc);
    errdefer alloc.free(owned_target);
    const owned_edge_type = try alloc.dupe(u8, edge_type);
    errdefer alloc.free(owned_edge_type);
    const owned_id = try alloc.dupe(u8, edge_id);
    errdefer alloc.free(owned_id);
    const owner_document = if (edge_id.len > 0 and !std.mem.eql(u8, source_doc, doc_key)) try alloc.dupe(u8, doc_key) else "";
    errdefer if (owner_document.len > 0) alloc.free(owner_document);
    const owned_owner = if (edge_id.len == 0 and !std.mem.eql(u8, source_doc, doc_key)) try alloc.dupe(u8, doc_key) else "";
    errdefer if (owned_owner.len > 0) alloc.free(@constCast(owned_owner));
    try writes.append(alloc, .{
        .edge_id = owned_id,
        .owner_document = owner_document,
        .index_name = owned_index_name,
        .source = owned_source,
        .target = owned_target,
        .edge_type = owned_edge_type,
        .weight = weight,
        .created_at = 0,
        .updated_at = 0,
        .metadata_json = owned_metadata,
        .owner = owned_owner,
    });
}

fn renderGraphArtifactTemplateAlloc(
    alloc: Allocator,
    template_source: []const u8,
    doc_key: []const u8,
    doc_value: ?std.json.Value,
    item: std.json.Value,
    item_index: usize,
    artifact_name: []const u8,
    artifact_content_type: []const u8,
    artifact_value: std.json.Value,
) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    var pos: usize = 0;
    while (pos < template_source.len) {
        const start = std.mem.indexOfPos(u8, template_source, pos, "{{") orelse {
            try out.appendSlice(alloc, template_source[pos..]);
            break;
        };
        try out.appendSlice(alloc, template_source[pos..start]);
        const body_start = start + 2;
        const end = std.mem.indexOfPos(u8, template_source, body_start, "}}") orelse {
            try out.appendSlice(alloc, template_source[start..]);
            break;
        };
        const expr = std.mem.trim(u8, template_source[body_start..end], &std.ascii.whitespace);
        const rendered = try renderGraphArtifactExpressionAlloc(alloc, expr, doc_key, doc_value, item, item_index, artifact_name, artifact_content_type, artifact_value);
        defer alloc.free(rendered);
        try out.appendSlice(alloc, rendered);
        pos = end + 2;
    }
    return try out.toOwnedSlice(alloc);
}

fn renderGraphArtifactExpressionAlloc(
    alloc: Allocator,
    expr: []const u8,
    doc_key: []const u8,
    doc_value: ?std.json.Value,
    item: std.json.Value,
    item_index: usize,
    artifact_name: []const u8,
    artifact_content_type: []const u8,
    artifact_value: std.json.Value,
) ![]u8 {
    if (std.mem.startsWith(u8, expr, "default ")) {
        var parts = std.mem.tokenizeAny(u8, expr["default ".len..], &std.ascii.whitespace);
        const path = parts.next() orelse return try alloc.dupe(u8, "");
        const fallback = parts.next() orelse "";
        const value = graphTemplateValue(path, doc_key, doc_value, item, item_index, artifact_name, artifact_content_type, artifact_value);
        const text = if (value) |found| try graphJsonValueTextAlloc(alloc, found) else try alloc.dupe(u8, fallback);
        if (std.mem.trim(u8, text, &std.ascii.whitespace).len == 0 and fallback.len > 0) {
            alloc.free(text);
            return try alloc.dupe(u8, fallback);
        }
        return text;
    }
    if (graphTemplateValue(expr, doc_key, doc_value, item, item_index, artifact_name, artifact_content_type, artifact_value)) |value| {
        return try graphJsonValueTextAlloc(alloc, value);
    }
    return try alloc.dupe(u8, "");
}

fn graphTemplateValue(
    path: []const u8,
    doc_key: []const u8,
    doc_value: ?std.json.Value,
    item: std.json.Value,
    item_index: usize,
    artifact_name: []const u8,
    artifact_content_type: []const u8,
    artifact_value: std.json.Value,
) ?std.json.Value {
    if (std.mem.eql(u8, path, "_doc.key")) return .{ .string = doc_key };
    if (std.mem.startsWith(u8, path, "_doc.value.")) {
        const doc = doc_value orelse return null;
        return selectJsonDotPath(doc, path["_doc.value.".len..]);
    }
    if (std.mem.eql(u8, path, "_artifact.name")) return .{ .string = artifact_name };
    if (std.mem.eql(u8, path, "_artifact.content_type")) return .{ .string = artifact_content_type };
    if (std.mem.eql(u8, path, "_artifact.value")) return artifact_value;
    if (std.mem.startsWith(u8, path, "_artifact.value.")) return selectJsonDotPath(artifact_value, path["_artifact.value.".len..]);
    if (std.mem.eql(u8, path, "_item_index")) return .{ .integer = @intCast(item_index) };
    if (std.mem.eql(u8, path, "_item")) return item;
    if (std.mem.startsWith(u8, path, "_item.")) return selectGraphItemDotPath(item, path["_item.".len..], artifact_value);
    return null;
}

fn selectGraphItemDotPath(item: std.json.Value, path: []const u8, artifact_value: std.json.Value) ?std.json.Value {
    if (std.mem.eql(u8, path, "source") or std.mem.startsWith(u8, path, "source.")) {
        if (item != .object) return null;
        const endpoint = item.object.get("source") orelse return null;
        const selected = resolveGraphEndpointEntity(endpoint, artifact_value) orelse endpoint;
        if (std.mem.eql(u8, path, "source")) return selected;
        return selectJsonDotPath(selected, path["source.".len..]);
    }
    if (std.mem.eql(u8, path, "target") or std.mem.startsWith(u8, path, "target.")) {
        if (item != .object) return null;
        const endpoint = item.object.get("target") orelse return null;
        const selected = resolveGraphEndpointEntity(endpoint, artifact_value) orelse endpoint;
        if (std.mem.eql(u8, path, "target")) return selected;
        return selectJsonDotPath(selected, path["target.".len..]);
    }
    return selectJsonDotPath(item, path);
}

fn selectJsonDotPath(root: std.json.Value, path: []const u8) ?std.json.Value {
    var current = root;
    var parts = std.mem.splitScalar(u8, path, '.');
    while (parts.next()) |part| {
        if (part.len == 0) return null;
        if (current != .object) return null;
        current = current.object.get(part) orelse return null;
    }
    return current;
}

fn graphJsonValueTextAlloc(alloc: Allocator, value: std.json.Value) ![]u8 {
    return switch (value) {
        .null => try alloc.dupe(u8, ""),
        .bool => |b| try alloc.dupe(u8, if (b) "true" else "false"),
        .integer => |n| try std.fmt.allocPrint(alloc, "{d}", .{n}),
        .float => |n| try std.fmt.allocPrint(alloc, "{d}", .{n}),
        .number_string => |s| try alloc.dupe(u8, s),
        .string => |s| try alloc.dupe(u8, s),
        .array, .object => try std.json.Stringify.valueAlloc(alloc, value, .{}),
    };
}

fn renderGraphArtifactMetadataTemplateAlloc(
    alloc: Allocator,
    metadata_template_json: []const u8,
    doc_key: []const u8,
    doc_value: ?std.json.Value,
    item: std.json.Value,
    item_index: usize,
    artifact_name: []const u8,
    artifact_content_type: []const u8,
    artifact_value: std.json.Value,
) ![]u8 {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, metadata_template_json, .{ .parse_numbers = false });
    defer parsed.deinit();
    var rendered = try renderGraphArtifactMetadataValueAlloc(alloc, parsed.value, doc_key, doc_value, item, item_index, artifact_name, artifact_content_type, artifact_value);
    defer freeGraphRenderedJsonValue(alloc, &rendered);
    return try std.json.Stringify.valueAlloc(alloc, rendered, .{});
}

fn renderGraphArtifactMetadataValueAlloc(
    alloc: Allocator,
    value: std.json.Value,
    doc_key: []const u8,
    doc_value: ?std.json.Value,
    item: std.json.Value,
    item_index: usize,
    artifact_name: []const u8,
    artifact_content_type: []const u8,
    artifact_value: std.json.Value,
) !std.json.Value {
    return switch (value) {
        .string => |text| .{ .string = try renderGraphArtifactTemplateAlloc(alloc, text, doc_key, doc_value, item, item_index, artifact_name, artifact_content_type, artifact_value) },
        .array => |array| blk: {
            var out = std.json.Array.init(alloc);
            errdefer {
                for (out.items) |*child| freeGraphRenderedJsonValue(alloc, child);
                out.deinit();
            }
            try out.ensureUnusedCapacity(array.items.len);
            for (array.items) |child| out.appendAssumeCapacity(try renderGraphArtifactMetadataValueAlloc(alloc, child, doc_key, doc_value, item, item_index, artifact_name, artifact_content_type, artifact_value));
            break :blk .{ .array = out };
        },
        .object => |object| blk: {
            var out = std.json.ObjectMap.empty;
            errdefer {
                var owned = std.json.Value{ .object = out };
                freeGraphRenderedJsonValue(alloc, &owned);
            }
            try out.ensureUnusedCapacity(alloc, object.count());
            var it = object.iterator();
            while (it.next()) |entry| {
                const key = try alloc.dupe(u8, entry.key_ptr.*);
                errdefer alloc.free(key);
                const child = try renderGraphArtifactMetadataValueAlloc(alloc, entry.value_ptr.*, doc_key, doc_value, item, item_index, artifact_name, artifact_content_type, artifact_value);
                out.putAssumeCapacity(key, child);
            }
            break :blk .{ .object = out };
        },
        else => value,
    };
}

fn freeGraphRenderedJsonValue(alloc: Allocator, value: *std.json.Value) void {
    switch (value.*) {
        .string => |text| alloc.free(@constCast(text)),
        .array => |*array| {
            for (array.items) |*item| freeGraphRenderedJsonValue(alloc, item);
            array.deinit();
        },
        .object => |*object| {
            var it = object.iterator();
            while (it.next()) |entry| {
                alloc.free(@constCast(entry.key_ptr.*));
                freeGraphRenderedJsonValue(alloc, entry.value_ptr);
            }
            object.deinit(alloc);
        },
        else => {},
    }
    value.* = .null;
}

pub fn jsonEndpointDocumentId(value: std.json.Value) ?[]const u8 {
    return switch (value) {
        .string => value.string,
        .object => jsonStringField(value, "document_id") orelse jsonStringField(value, "doc_key") orelse jsonStringField(value, "key") orelse jsonStringField(value, "id") orelse jsonStringField(value, "local_id") orelse if (value.object.get("doc_ref")) |doc_ref| jsonEndpointDocumentId(doc_ref) else null,
        else => null,
    };
}

fn jsonEndpointDocumentIdResolved(value: std.json.Value, artifact_value: std.json.Value) ?[]const u8 {
    // A relation endpoint referencing an extraction entity (a plain string
    // like "e0" matching the artifact's entities, or {entity_id}/
    // {entity_index}) renders the entity's canonical identity — fields on
    // the entity entry itself or an injected "_entities" resolution map (see
    // injectGraphEndpointResolutions) — or nothing at all: before resolution
    // there is no durable node for a local mention, and rendering the local
    // id would strand an orphan edge that no later replay retires. The
    // resolution-artifact replay re-renders the artifact once canonical keys
    // exist. Endpoints matching no extraction entity keep the legacy
    // string-passthrough external-node behavior.
    if (resolveGraphEndpointEntity(value, artifact_value)) |entity| {
        return canonicalEntityDocumentId(entity);
    }
    return jsonEndpointDocumentId(value);
}

fn canonicalEntityDocumentId(entity: std.json.Value) ?[]const u8 {
    if (entity != .object) return null;
    if (jsonStringField(entity, "document_id") orelse jsonStringField(entity, "doc_key") orelse jsonStringField(entity, "key")) |id| return id;
    if (entity.object.get("doc_ref")) |doc_ref| return jsonEndpointDocumentId(doc_ref);
    return null;
}

fn canonicalEntityTable(entity: std.json.Value) ?[]const u8 {
    if (entity != .object) return null;
    if (jsonStringField(entity, "table")) |table| return table;
    if (entity.object.get("doc_ref")) |doc_ref| return jsonStringField(doc_ref, "table");
    return null;
}

fn prependTableTagToMetadataJsonAlloc(alloc: Allocator, comptime tag: []const u8, table: []const u8, metadata_json: []const u8) ![]u8 {
    return graph_metadata_tables.withTableAlloc(alloc, tag, table, metadata_json, true);
}

fn prependTargetTableToMetadataJsonAlloc(alloc: Allocator, target_table: []const u8, metadata_json: []const u8) ![]u8 {
    return try prependTableTagToMetadataJsonAlloc(alloc, "target_table", target_table, metadata_json);
}

fn prependTargetTableToItemMetadataAlloc(alloc: Allocator, target_table: []const u8, item: std.json.Value) ![]u8 {
    const item_json = try std.json.Stringify.valueAlloc(alloc, item, .{});
    defer alloc.free(item_json);
    return graph_metadata_tables.withTableAlloc(alloc, "target_table", target_table, item_json, false);
}

fn resolveGraphEndpointEntity(value: std.json.Value, artifact_value: std.json.Value) ?std.json.Value {
    switch (value) {
        .string => return findGraphArtifactEntity(artifact_value, value.string),
        .object => {
            if (jsonIntegerField(value, "entity_index")) |entity_index| return graphArtifactEntityAtIndex(artifact_value, entity_index);
            const entity_id = jsonStringField(value, "entity_id") orelse jsonStringField(value, "id") orelse jsonStringField(value, "local_id") orelse return null;
            return findGraphArtifactEntity(artifact_value, entity_id);
        },
        else => return null,
    }
}

pub fn findGraphArtifactEntity(artifact_value: std.json.Value, entity_id: []const u8) ?std.json.Value {
    if (artifact_value != .object) return null;
    // The injected "_entities" resolution map wins, but a mention it does not
    // cover (partial resolution) still matches the artifact's own entities so
    // the canonical-only endpoint rule can drop it instead of leaking its
    // local id as a node.
    if (artifact_value.object.get("_entities")) |resolved| {
        if (findGraphArtifactEntityIn(resolved, entity_id)) |entity| return entity;
    }
    const entities = artifact_value.object.get("entities") orelse return null;
    return findGraphArtifactEntityIn(entities, entity_id);
}

fn findGraphArtifactEntityIn(entities: std.json.Value, entity_id: []const u8) ?std.json.Value {
    return switch (entities) {
        .array => |array| blk: {
            for (array.items) |entity| {
                const id = jsonStringField(entity, "id") orelse jsonStringField(entity, "local_id") orelse continue;
                if (std.mem.eql(u8, id, entity_id)) break :blk entity;
            }
            break :blk null;
        },
        .object => entities.object.get(entity_id),
        else => null,
    };
}

fn graphArtifactEntityAtIndex(artifact_value: std.json.Value, entity_index: i64) ?std.json.Value {
    if (entity_index < 0 or artifact_value != .object) return null;
    const index: usize = @intCast(entity_index);
    const raw_entity: ?std.json.Value = blk: {
        const entities = artifact_value.object.get("entities") orelse break :blk null;
        if (entities != .array or index >= entities.array.items.len) break :blk null;
        break :blk entities.array.items[index];
    };
    if (artifact_value.object.get("_entities")) |resolved| {
        // The injected resolution map is keyed by mention local id. An
        // id-less extraction entity (GLiNER2.5's positional payloads) was
        // resolved under its decimal array position — the same identity
        // lib/resolver's parseExtractionEntities assigns it.
        var buf: [20]u8 = undefined;
        const positional_id = std.fmt.bufPrint(&buf, "{d}", .{index}) catch unreachable;
        const local_id = if (raw_entity) |entity|
            jsonStringField(entity, "id") orelse jsonStringField(entity, "local_id") orelse positional_id
        else
            positional_id;
        if (findGraphArtifactEntityIn(resolved, local_id)) |entity| return entity;
        if (resolved == .array and index < resolved.array.items.len) return resolved.array.items[index];
    }
    // The raw positional entity carries no canonical identity; the
    // canonical-only endpoint rule downstream drops it until resolution lands.
    return raw_entity;
}

pub fn jsonStringField(value: std.json.Value, field: []const u8) ?[]const u8 {
    if (value != .object) return null;
    const found = value.object.get(field) orelse return null;
    return if (found == .string) found.string else null;
}

fn jsonIntegerField(value: std.json.Value, field: []const u8) ?i64 {
    if (value != .object) return null;
    const found = value.object.get(field) orelse return null;
    return switch (found) {
        .integer => found.integer,
        .number_string => |text| std.fmt.parseInt(i64, text, 10) catch null,
        else => null,
    };
}

fn jsonFloatField(value: std.json.Value, field: []const u8) ?f64 {
    if (value != .object) return null;
    const found = value.object.get(field) orelse return null;
    return switch (found) {
        .float => found.float,
        .integer => @floatFromInt(found.integer),
        .number_string => |text| std.fmt.parseFloat(f64, text) catch null,
        else => null,
    };
}

/// Cache retains owned parsed values across durable pages. Replace only after
/// complete construction; a failed replacement leaves the prior cache valid.
pub const Cache = struct {
    artifact_key: []u8,
    index_name: []u8,
    artifact: std.json.Parsed(std.json.Value),
    document: ?std.json.Parsed(std.json.Value),
    pub fn init(alloc: Allocator, key: []const u8, index: []const u8, raw: []const u8, raw_doc: ?[]const u8) !Cache {
        var artifact = try std.json.parseFromSlice(std.json.Value, alloc, raw, .{ .parse_numbers = false });
        errdefer artifact.deinit();
        var document = if (raw_doc) |value| try std.json.parseFromSlice(std.json.Value, alloc, value, .{ .parse_numbers = false }) else null;
        errdefer if (document) |*value| value.deinit();
        const artifact_key = try alloc.dupe(u8, key);
        errdefer alloc.free(artifact_key);
        const index_name = try alloc.dupe(u8, index);
        return .{ .artifact_key = artifact_key, .index_name = index_name, .artifact = artifact, .document = document };
    }
    pub fn matches(self: Cache, key: []const u8, index: []const u8) bool {
        return std.mem.eql(u8, self.artifact_key, key) and std.mem.eql(u8, self.index_name, index);
    }
    pub fn deinit(self: *Cache, alloc: Allocator) void {
        self.artifact.deinit();
        if (self.document) |*document| document.deinit();
        alloc.free(self.artifact_key);
        alloc.free(self.index_name);
        self.* = undefined;
    }
};

test "graph restore parsed cache owns raw input across every allocation failure" {
    const F = struct {
        fn run(alloc: Allocator) !void {
            var cache = try Cache.init(alloc, "artifact", "graph", "{\"relations\":[{\"target\":\"b\"}]}", "{\"id\":\"a\"}");
            defer cache.deinit(alloc);
            try std.testing.expect(cache.matches("artifact", "graph"));
            try std.testing.expect(!cache.matches("artifact", "other"));
            try std.testing.expectEqualStrings("a", cache.document.?.value.object.get("id").?.string);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, F.run, .{});
}

test "graph restore page planner owns nested templates and resumes raw ordinals on OOM" {
    const F = struct {
        fn run(alloc: Allocator) !void {
            var cache = try Cache.init(alloc, "artifact", "graph", "[null,{\"target\":\"b\",\"type\":\"rel\"},{\"target\":\"c\",\"type\":\"rel\"}]", null);
            defer cache.deinit(alloc);
            const source: index_manager_mod.GraphArtifactSource = .{
                .artifact_name = @constCast("relations"),
                .mapping = .{ .metadata_template_json = @constCast("{\"nested\":[\"{{_doc.key}}\",{\"target\":\"{{_item.target}}\"}]}") },
            };
            const first = try graphWritesFromArtifactParsedPageAlloc(alloc, "graph", "a", cache.artifact.value, source, "application/json", null, 0, 2, 4096, 16);
            defer {
                for (first.writes) |write| freeGraphWriteFields(alloc, write);
                alloc.free(first.writes);
            }
            try std.testing.expectEqual(@as(?usize, 2), first.next_item_offset);
            try std.testing.expectEqual(@as(usize, 1), first.writes.len);
            try std.testing.expectEqualStrings("b", first.writes[0].target);
            const second = try graphWritesFromArtifactParsedPageAlloc(alloc, "graph", "a", cache.artifact.value, source, "application/json", null, first.next_item_offset.?, 2, 4096, 16);
            defer {
                for (second.writes) |write| freeGraphWriteFields(alloc, write);
                alloc.free(second.writes);
            }
            try std.testing.expectEqual(@as(?usize, null), second.next_item_offset);
            try std.testing.expectEqualStrings("c", second.writes[0].target);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, F.run, .{});
}
