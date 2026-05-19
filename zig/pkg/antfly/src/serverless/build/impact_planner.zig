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

const std = @import("std");
const catalog_types = @import("../catalog/types.zig");

pub const ArtifactFamily = enum {
    document_segment,
    full_text,
    dense_vector,
    sparse_vector,
    graph,
    chunk_preview,
    chunk_embeddings,
    rerank_terms,
};

pub const PlanInput = struct {
    before_schema_json: []const u8 = "",
    after_schema_json: []const u8 = "",
    before_read_schema_json: []const u8 = "",
    after_read_schema_json: []const u8 = "",
    before_indexes_json: []const u8 = "",
    after_indexes_json: []const u8 = "",
    before_policy: catalog_types.NamespacePolicy = .{},
    after_policy: catalog_types.NamespacePolicy = .{},
};

pub const ArtifactImpactPlan = struct {
    rebuild_document_segment: bool = false,
    rebuild_full_text: bool = false,
    rebuild_dense_vector: bool = false,
    rebuild_sparse_vector: bool = false,
    rebuild_graph: bool = false,
    republish_full_text_from_head: bool = false,
    republish_dense_vector_from_head: bool = false,
    republish_sparse_vector_from_head: bool = false,
    republish_graph_from_head: bool = false,
    rebuild_chunk_preview: bool = false,
    rebuild_chunk_embeddings: bool = false,
    rebuild_rerank_terms: bool = false,
    migration_state_changed: bool = false,

    pub fn any(self: ArtifactImpactPlan) bool {
        return self.rebuild_document_segment or
            self.rebuild_full_text or
            self.rebuild_dense_vector or
            self.rebuild_sparse_vector or
            self.rebuild_graph or
            self.rebuild_chunk_preview or
            self.rebuild_chunk_embeddings or
            self.rebuild_rerank_terms or
            self.migration_state_changed;
    }

    pub fn requiresHeadRepublish(self: ArtifactImpactPlan) bool {
        return self.migration_state_changed or
            self.republish_full_text_from_head or
            self.republish_dense_vector_from_head or
            self.republish_sparse_vector_from_head or
            self.republish_graph_from_head or
            self.rebuild_chunk_preview or
            self.rebuild_rerank_terms;
    }
};

pub fn planAlloc(alloc: std.mem.Allocator, input: PlanInput) !ArtifactImpactPlan {
    var before_indexes = try parseIndexConfigMapAlloc(alloc, input.before_indexes_json);
    defer before_indexes.deinit();
    var after_indexes = try parseIndexConfigMapAlloc(alloc, input.after_indexes_json);
    defer after_indexes.deinit();

    var plan: ArtifactImpactPlan = .{
        .migration_state_changed = !std.mem.eql(u8, input.before_read_schema_json, input.after_read_schema_json),
    };

    const schema_changed = !std.mem.eql(u8, input.before_schema_json, input.after_schema_json);
    if (schema_changed and familyPresent(before_indexes.value, after_indexes.value, .full_text)) {
        plan.rebuild_full_text = true;
        plan.republish_full_text_from_head = true;
    }

    if (try familyChanged(before_indexes.value, after_indexes.value, .full_text)) {
        plan.rebuild_full_text = true;
        plan.republish_full_text_from_head = true;
    }
    if (try familyChanged(before_indexes.value, after_indexes.value, .dense_vector)) {
        plan.rebuild_dense_vector = true;
        plan.republish_dense_vector_from_head = true;
    }
    if (try familyChanged(before_indexes.value, after_indexes.value, .sparse_vector)) {
        plan.rebuild_sparse_vector = true;
        plan.republish_sparse_vector_from_head = true;
    }
    if (try familyChanged(before_indexes.value, after_indexes.value, .graph)) {
        plan.rebuild_graph = true;
        plan.republish_graph_from_head = true;
    }

    const lexical_sparse_changed =
        input.before_policy.enrichment_enabled != input.after_policy.enrichment_enabled or
        input.before_policy.lexical_sparse_model_preference != input.after_policy.lexical_sparse_model_preference or
        input.before_policy.enrichment_pipeline_version != input.after_policy.enrichment_pipeline_version;
    if (lexical_sparse_changed) plan.rebuild_sparse_vector = true;

    const chunk_preview_changed =
        input.before_policy.chunk_preview_enabled != input.after_policy.chunk_preview_enabled or
        input.before_policy.chunk_preview_pipeline_version != input.after_policy.chunk_preview_pipeline_version;
    if (chunk_preview_changed) plan.rebuild_chunk_preview = true;

    const chunk_embeddings_changed =
        input.before_policy.chunk_embeddings_enabled != input.after_policy.chunk_embeddings_enabled or
        input.before_policy.chunk_embeddings_model_preference != input.after_policy.chunk_embeddings_model_preference or
        input.before_policy.chunk_embeddings_pipeline_version != input.after_policy.chunk_embeddings_pipeline_version;
    if (chunk_embeddings_changed) {
        plan.rebuild_chunk_embeddings = true;
    }

    const rerank_terms_changed =
        input.before_policy.rerank_terms_enabled != input.after_policy.rerank_terms_enabled or
        input.before_policy.rerank_terms_pipeline_version != input.after_policy.rerank_terms_pipeline_version;
    if (rerank_terms_changed) plan.rebuild_rerank_terms = true;

    return plan;
}

const ParsedIndexConfigMap = std.json.Parsed(std.json.Value);

fn parseIndexConfigMapAlloc(alloc: std.mem.Allocator, indexes_json: []const u8) !ParsedIndexConfigMap {
    const source = if (indexes_json.len == 0) "{}" else indexes_json;
    const parsed = try std.json.parseFromSlice(std.json.Value, alloc, source, .{});
    switch (parsed.value) {
        .object => {},
        else => return error.InvalidTableIndexMetadata,
    }
    return parsed;
}

fn familyPresent(before: std.json.Value, after: std.json.Value, family: ArtifactFamily) bool {
    return countFamilyEntries(before, family) > 0 or countFamilyEntries(after, family) > 0;
}

fn countFamilyEntries(root: std.json.Value, family: ArtifactFamily) usize {
    const object = switch (root) {
        .object => |value| value,
        else => return 0,
    };
    var count: usize = 0;
    var it = object.iterator();
    while (it.next()) |entry| {
        if (classifyIndexFamily(entry.value_ptr.*) == family) count += 1;
    }
    return count;
}

fn familyChanged(before: std.json.Value, after: std.json.Value, family: ArtifactFamily) !bool {
    const before_object = switch (before) {
        .object => |value| value,
        else => return error.InvalidTableIndexMetadata,
    };
    const after_object = switch (after) {
        .object => |value| value,
        else => return error.InvalidTableIndexMetadata,
    };

    var before_it = before_object.iterator();
    while (before_it.next()) |entry| {
        if (classifyIndexFamily(entry.value_ptr.*) != family) continue;
        const after_value = after_object.get(entry.key_ptr.*) orelse return true;
        if (classifyIndexFamily(after_value) != family) return true;
        if (!jsonValueEql(entry.value_ptr.*, after_value)) return true;
    }

    var after_it = after_object.iterator();
    while (after_it.next()) |entry| {
        if (classifyIndexFamily(entry.value_ptr.*) != family) continue;
        const before_value = before_object.get(entry.key_ptr.*) orelse return true;
        if (classifyIndexFamily(before_value) != family) return true;
    }

    return false;
}

fn classifyIndexFamily(value: std.json.Value) ?ArtifactFamily {
    const object = switch (value) {
        .object => |map| map,
        else => return null,
    };
    const type_value = object.get("type") orelse return null;
    if (type_value != .string) return null;
    if (std.mem.eql(u8, type_value.string, "full_text")) return .full_text;
    if (std.mem.eql(u8, type_value.string, "graph")) return .graph;
    if (!std.mem.eql(u8, type_value.string, "embeddings")) return null;

    const sparse = if (object.get("sparse")) |sparse_value|
        switch (sparse_value) {
            .bool => sparse_value.bool,
            else => return null,
        }
    else
        false;
    return if (sparse) .sparse_vector else .dense_vector;
}

fn jsonValueEql(lhs: std.json.Value, rhs: std.json.Value) bool {
    if (@intFromEnum(lhs) != @intFromEnum(rhs)) return false;
    return switch (lhs) {
        .null => true,
        .bool => |value| value == rhs.bool,
        .integer => |value| value == rhs.integer,
        .float => |value| value == rhs.float,
        .number_string => |value| std.mem.eql(u8, value, rhs.number_string),
        .string => |value| std.mem.eql(u8, value, rhs.string),
        .array => |items| blk: {
            if (items.items.len != rhs.array.items.len) break :blk false;
            for (items.items, rhs.array.items) |lhs_item, rhs_item| {
                if (!jsonValueEql(lhs_item, rhs_item)) break :blk false;
            }
            break :blk true;
        },
        .object => |object| blk: {
            if (object.count() != rhs.object.count()) break :blk false;
            var it = object.iterator();
            while (it.next()) |entry| {
                const other = rhs.object.get(entry.key_ptr.*) orelse break :blk false;
                if (!jsonValueEql(entry.value_ptr.*, other)) break :blk false;
            }
            break :blk true;
        },
    };
}

test "impact planner flags schema migration as full text rebuild only" {
    const plan = try planAlloc(std.testing.allocator, .{
        .before_schema_json = "{\"version\":0}",
        .after_schema_json = "{\"version\":1}",
        .before_read_schema_json = "",
        .after_read_schema_json = "{\"version\":0}",
        .before_indexes_json = "{\"full_text_index_v0\":{\"type\":\"full_text\"}}",
        .after_indexes_json = "{\"full_text_index_v0\":{\"type\":\"full_text\"},\"full_text_index_v1\":{\"type\":\"full_text\"}}",
    });
    try std.testing.expect(plan.rebuild_full_text);
    try std.testing.expect(plan.migration_state_changed);
    try std.testing.expect(!plan.rebuild_dense_vector);
    try std.testing.expect(!plan.rebuild_sparse_vector);
    try std.testing.expect(!plan.rebuild_graph);
}

test "impact planner flags added dense, sparse, and graph indexes independently" {
    const plan = try planAlloc(std.testing.allocator, .{
        .before_indexes_json = "{\"full_text_index_v0\":{\"type\":\"full_text\"}}",
        .after_indexes_json =
            "{\"full_text_index_v0\":{\"type\":\"full_text\"},\"dense_idx\":{\"type\":\"embeddings\",\"dimension\":3},\"sparse_idx\":{\"type\":\"embeddings\",\"sparse\":true},\"graph_idx\":{\"type\":\"graph\"}}",
    });
    try std.testing.expect(plan.rebuild_dense_vector);
    try std.testing.expect(plan.rebuild_sparse_vector);
    try std.testing.expect(plan.rebuild_graph);
    try std.testing.expect(plan.republish_dense_vector_from_head);
    try std.testing.expect(plan.republish_sparse_vector_from_head);
    try std.testing.expect(plan.republish_graph_from_head);
    try std.testing.expect(!plan.rebuild_full_text);
}

test "impact planner flags dense config updates without sparse rebuild" {
    const plan = try planAlloc(std.testing.allocator, .{
        .before_indexes_json = "{\"dense_idx\":{\"type\":\"embeddings\",\"dimension\":3}}",
        .after_indexes_json = "{\"dense_idx\":{\"type\":\"embeddings\",\"dimension\":4}}",
    });
    try std.testing.expect(plan.rebuild_dense_vector);
    try std.testing.expect(!plan.rebuild_sparse_vector);
}

test "impact planner flags enrichment family rebuilds from policy changes" {
    const before: catalog_types.NamespacePolicy = .{};
    var after = before;
    after.enrichment_enabled = true;
    after.chunk_preview_enabled = true;
    after.chunk_embeddings_enabled = true;
    after.rerank_terms_enabled = true;

    const plan = try planAlloc(std.testing.allocator, .{
        .before_policy = before,
        .after_policy = after,
    });
    try std.testing.expect(plan.rebuild_sparse_vector);
    try std.testing.expect(plan.rebuild_chunk_preview);
    try std.testing.expect(plan.rebuild_chunk_embeddings);
    try std.testing.expect(!plan.rebuild_dense_vector);
    try std.testing.expect(plan.rebuild_rerank_terms);
}

test "impact planner is stable for semantically equal index json with reordered object keys" {
    const plan = try planAlloc(std.testing.allocator, .{
        .before_indexes_json = "{\"dense_idx\":{\"type\":\"embeddings\",\"field\":\"body\",\"dimension\":3}}",
        .after_indexes_json = "{\"dense_idx\":{\"dimension\":3,\"field\":\"body\",\"type\":\"embeddings\"}}",
    });
    try std.testing.expect(!plan.any());
}
