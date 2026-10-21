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

//! Native operation contracts shared by policy and typed MCP discovery.
const std = @import("std");
const t = @import("types.zig");
const schema = @import("schema.zig");

pub const Action = enum { create, inspect, configure, observe, assertions, recall, get, history, correct, forget, outcome, operation, advance, list, entities, curate_entity, resolve_entity, evidence, stage_embeddings, configuration, explain, entity_candidates, propose_entity, entity_proposals, entity_proposal, review_entity, entity_view, episodes, episode, propose_relationship, relationship_proposal, review_relationship };
pub const Configure = struct { expected_revision: u64, config: t.Config };
pub const Page = struct { cursor: ?[]const u8 = null, limit: u16 = 64 };

pub fn mutates(action: Action) bool {
    return switch (action) {
        .inspect, .recall, .get, .history, .operation, .list, .entities, .resolve_entity, .evidence, .configuration, .explain, .entity_candidates, .entity_proposals, .entity_proposal, .entity_view, .episodes, .episode, .relationship_proposal => false,
        else => true,
    };
}

/// Space budgets bound newly admitted content and model-query input. Control
/// operations and structural reads retain the transport-wide hard bound, so a
/// small ingestion budget never locks out inspection, erasure or recovery.
pub fn usesInputBudget(action: Action) bool {
    return switch (action) {
        .observe, .assertions, .correct, .outcome, .curate_entity, .stage_embeddings, .recall, .propose_entity, .review_entity, .propose_relationship, .review_relationship => true,
        .create, .configure, .forget, .advance, .inspect, .get, .history, .operation, .list, .entities, .resolve_entity, .evidence, .configuration, .explain, .entity_candidates, .entity_proposals, .entity_proposal, .entity_view, .episodes, .episode, .relationship_proposal => false,
    };
}

pub fn Body(comptime action: Action) type {
    return switch (action) {
        .create => t.Config,
        .configure => Configure,
        .observe => t.ObservationRequest,
        .assertions => t.AssertionRequest,
        .recall, .list => t.Recall,
        .history, .entities => Page,
        .correct => t.Correction,
        .forget => t.Forget,
        .outcome => t.Outcome,
        .stage_embeddings => t.EmbeddingBatch,
        .curate_entity => t.EntityCorrection,
        .explain => t.provenance.ExplainRequest,
        .propose_relationship => t.RelationshipProposalRequest,
        .review_relationship => t.EntityVerdict,
        .entity_view => t.EntityViewRequest,
        .episodes, .episode => t.EpisodeRequest,
        .entity_candidates => t.EntityCandidateRequest,
        .propose_entity => t.EntityProposalRequest,
        .review_entity => t.EntityVerdict,
        .entity_proposals => Page,
        else => struct {},
    };
}

pub fn idRequired(action: Action) bool {
    return switch (action) {
        .get, .history, .operation, .resolve_entity, .evidence, .configuration, .entity_candidates, .entity_proposal, .review_entity, .entity_view, .episode, .relationship_proposal, .review_relationship => true,
        else => false,
    };
}

pub fn description(action: Action) []const u8 {
    return switch (action) {
        .propose_relationship => "Propose an evidence-grounded typed entity relationship for external review; does not publish an assertion.",
        .relationship_proposal => "Get an authorized retained relationship proposal and its verdict attribution.",
        .review_relationship => "Accept, reject or defer an external relationship judgment through native assertion policy with revision and configuration fences.",
        .entity_view => "Derive a canonical entity description from authorized current assertions, with exact dependency references and a content fingerprint.",
        .episodes => "Page through authorized evidence episodes grouped by thread, ordered by turn ordinal.",
        .episode => "Read an ordered evidence episode with grounded entity mentions and explicit pagination.",
        .entity_candidates => "Discover bounded name/description identity or typed relationship candidates with grounded views and immutable entity revisions; similarity is not authority to merge.",
        .propose_entity => "Queue an attributed external entity proposal with grounded evidence and a space revision fence. Requires administrator access.",
        .entity_proposals => "Page through retained entity review proposals. Requires administrator access.",
        .entity_proposal => "Get an entity proposal and its retained external verdict attribution. Requires administrator access.",
        .review_entity => "Submit an external accept/reject/defer verdict; apply through native curation policy atomically. Rejecting a merge may pin distinct identities. Requires administrator access.",
        .create => "Create a native memory space. Omit body for model-free defaults. Requires administrator access.",
        .inspect => "Inspect a space's effective configuration, capabilities, revision and pending configuration.",
        .configure => "Configure a space with an expected_revision fence. Requires administrator access; a rebuild may return a pending operation.",
        .observe => "Store immutable source evidence and optional grounded assertions/entities. Returns a durable operation receipt; managed processing may remain pending.",
        .assertions => "Store externally produced assertions and relationships grounded in existing evidence.",
        .recall => "Recall accessible evidence-backed memory using temporal filters, lexical/vector ranking and a bounded context budget.",
        .get => "Get an authoritative memory record by id with current authorization and retention applied.",
        .history => "Page through the authorized revision history of a memory id.",
        .correct => "Apply an explicit revision-fenced lifecycle or stability correction with provenance.",
        .forget => "Fence forgotten evidence or memories immediately and queue bounded physical cleanup.",
        .outcome => "Record immutable task utility feedback independently of assertion truth.",
        .operation => "Get a durable operation receipt by id, including readiness and retry state.",
        .advance => "Advance one operation by id. Omit id for one space maintenance quantum (administrator access). Configuration operations retain their administrator requirement.",
        .list => "Enumerate accessible memory with exact structural/temporal filters and explicit pagination.",
        .entities => "Page through accessible canonical entities and their provenance.",
        .curate_entity => "Merge, unmerge or suppress an entity through the shared resolver's curation policy.",
        .resolve_entity => "Resolve an entity id to its authorized canonical view.",
        .evidence => "Get an immutable source evidence version and original text by id with current authorization and retention applied.",
        .stage_embeddings => "Stage externally produced vectors for a pending embedding generation.",
        .configuration => "Get an immutable historical configuration by generation id.",
        .explain => "Explain a memory id or body.resource through a bounded provenance graph with authorization, retention and known_at applied.",
    };
}

/// Static tool schemas are generated once from the same native payload types
/// parsed by the service. The endpoint, rather than each tool name, owns v1.
pub fn mcpTools(a: std.mem.Allocator) !std.json.Value {
    @setEvalBranchQuota(100000);
    var tools: std.ArrayList(std.json.Value) = .empty;
    inline for (@typeInfo(Action).@"enum".field_names) |name| {
        const action = @field(Action, name);
        const Envelope = struct {
            space: []const u8,
            database: []const u8 = "default",
            namespace: []const u8 = "public",
        };
        var input = try schema.wire(Envelope, a);
        var props = &input.object.getPtr("properties").?.object;
        var required = &input.object.getPtr("required").?.array;
        if (comptime idRequired(action) or action == .advance or action == .explain) {
            const Id = struct { id: []const u8 };
            var id_schema = (try schema.wire(Id, a)).object.get("properties").?.object.get("id").?;
            try id_schema.object.put(a, "minLength", .{ .integer = 1 });
            try props.put(a, "id", id_schema);
            if (comptime idRequired(action)) try required.append(.{ .string = "id" });
        }
        if (comptime @typeInfo(Body(action)).@"struct".field_names.len > 0) {
            const body = try schema.wire(Body(action), a);
            try props.put(a, "body", body);
            if (body.object.get("required") != null) try required.append(.{ .string = "body" });
        }
        if (comptime mutates(action)) {
            var key_schema = (try schema.wire(struct { idempotency_key: []const u8 }, a)).object.get("properties").?.object.get("idempotency_key").?;
            try key_schema.object.put(a, "minLength", .{ .integer = 1 });
            try key_schema.object.put(a, "maxLength", .{ .integer = 256 });
            try props.put(a, "idempotency_key", key_schema);
            try required.append(.{ .string = "idempotency_key" });
        }
        var tool: std.json.ObjectMap = .empty;
        try tool.put(a, "action", .{ .string = name });
        try tool.put(a, "name", .{ .string = "memory_" ++ name });
        try tool.put(a, "description", .{ .string = description(action) });
        try tool.put(a, "input_schema_json", .{ .string = try std.json.Stringify.valueAlloc(a, input, .{}) });
        try tools.append(a, .{ .object = tool });
    }
    return .{ .array = std.array_list.Managed(std.json.Value).fromOwnedSlice(a, try tools.toOwnedSlice(a)) };
}

test "memory MCP contracts cover every native action with exact payload domains" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const tools = (try mcpTools(arena.allocator())).array.items;
    try std.testing.expectEqual(@typeInfo(Action).@"enum".field_names.len, tools.len);
    var configured = false;
    var recalled = false;
    for (tools) |tool| {
        const action = std.meta.stringToEnum(Action, tool.object.get("action").?.string).?;
        const input = try std.json.parseFromSliceLeaky(std.json.Value, arena.allocator(), tool.object.get("input_schema_json").?.string, .{ .parse_numbers = false });
        const props = input.object.get("properties").?.object;
        try std.testing.expect(!input.object.get("additionalProperties").?.bool);
        try std.testing.expectEqual(mutates(action), props.contains("idempotency_key"));
        if (action == .configure) {
            const revision = props.get("body").?.object.get("properties").?.object.get("expected_revision").?.object;
            try std.testing.expectEqualStrings("18446744073709551615", revision.get("maximum").?.number_string);
            configured = true;
        }
        if (action == .recall) {
            const provisional = props.get("body").?.object.get("properties").?.object.get("include_provisional").?.object;
            try std.testing.expect(provisional.get("default").?.bool);
            recalled = true;
        }
    }
    try std.testing.expect(configured and recalled);
}
