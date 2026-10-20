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

//! Replay bounds from the prepared request's canonical derived batch and all
//! accepted generated-effect families. No payload/vector copies or store reads.
//! The request must be the complete prepared request under its catalog/intent
//! witness; this key plan does not create that witness or fund recovery working RAM.
const std = @import("std");
const shape = @import("replay_record_shape.zig");
const inventory = @import("artifact_replay_inventory.zig");
const keys = @import("internal_keys.zig");
const derived = @import("db/derived/derived_types.zig");
const effects = @import("db/prepared_effect_types.zig");
const control = @import("../common/read_admission.zig");
const max_roots = @import("publication_backlog.zig").default_max_records;
const A = std.mem.Allocator;

pub const Plan = struct {
    allocator: A,
    base: shape.Record,
    /// Owned canonical physical document prefixes (not raw public IDs).
    roots: []const []const u8,
    pub fn deinit(self: *Plan) void {
        for (self.roots) |root| self.allocator.free(root);
        self.allocator.free(self.roots);
        self.* = undefined;
    }
};
const Builder = struct {
    allocator: A,
    original: ?control.Port,
    base: shape.Record = .{},
    roots: std.ArrayListUnmanaged([]const u8) = .empty,
    seen: std.StringHashMapUnmanaged(void) = .empty,
    fn deinit(self: *Builder) void {
        for (self.roots.items) |root| self.allocator.free(root);
        self.roots.deinit(self.allocator);
        self.seen.deinit(self.allocator);
    }
    fn checkpoint(self: *Builder) !void {
        if (self.original) |port| try port.checkpoint();
    }
    fn add(self: *Builder, comptime lane: []const u8, key: []const u8) !void {
        try self.checkpoint();
        try @field(self.base, lane).addKey(key.len);
    }
    fn physicalRoot(self: *Builder, physical: []const u8) !void {
        try self.checkpoint();
        const prefix = try inventory.docPrefix(physical);
        if (prefix.len >= physical.len) return error.InvalidPreparedEffects;
        if (self.seen.contains(prefix)) return;
        if (self.roots.items.len == max_roots) return error.Backpressured;
        const owned = try self.allocator.dupe(u8, prefix);
        self.roots.append(self.allocator, owned) catch |err| {
            self.allocator.free(owned);
            return err;
        };
        try self.seen.put(self.allocator, owned, {});
    }
    fn documentRoot(self: *Builder, logical: []const u8) !void {
        try self.checkpoint();
        const physical = try keys.documentKeyAlloc(self.allocator, logical);
        defer self.allocator.free(physical);
        try self.physicalRoot(physical);
    }
    fn document(self: *Builder, logical: []const u8) !void {
        try self.add("changed_doc_keys", logical);
        try self.documentRoot(logical);
    }
    fn endpoints(self: *Builder, owner: []const u8, source: []const u8, target: []const u8) !void {
        // Routing may wake either endpoint, including endpoints that lose their
        // old route. Overestimate aliases; canonical encoders deduplicate them.
        try self.add("changed_doc_keys", owner);
        try self.add("changed_doc_keys", source);
        try self.add("changed_doc_keys", target);
    }
    fn artifact(self: *Builder, physical: []const u8, deleted: bool) !void {
        try self.add("changed_artifact_keys", physical);
        if (deleted) try self.add("deleted_doc_keys", physical);
        try self.physicalRoot(physical);
        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        if (try keys.parseGraphEdgeArtifactKeyAlloc(arena.allocator(), physical)) |identity| {
            try self.endpoints(identity.doc_key, if (identity.logical_source.len == 0) identity.doc_key else identity.logical_source, identity.target_doc_key);
        }
    }
    fn graph(self: *Builder, value: anytype, deleted: bool) !void {
        try self.checkpoint();
        const owner = if (value.owner_document.len != 0) value.owner_document else if (value.owner.len != 0) value.owner else value.source;
        const physical = try keys.graphRelationshipArtifactKeyAlloc(self.allocator, owner, value.index_name, value.edge_type, value.target, value.source, value.edge_id);
        defer self.allocator.free(physical);
        // artifact() derives the same owner/source/target hints from the key.
        try self.artifact(physical, deleted);
    }
    fn request(self: *Builder, value: derived.DerivedBatch) !void {
        // Exhaustive coverage guard: new replay-bearing fields require an
        // explicit decision here instead of silently escaping the reservation.
        comptime assertFields(derived.DerivedBatch, &.{ "sequence", "documents", "deleted_keys", "overwritten_doc_keys", "changed_artifact_keys", "graph_doc_clears", "dense_embeddings", "sparse_embeddings", "generated_enrichment_refs", "graph_writes", "graph_deletes" });
        for (value.documents) |doc| {
            try self.documentRoot(doc.key);
            switch (doc.action) {
                .upsert => try self.add("changed_doc_keys", doc.key),
                .delete => try self.add("deleted_doc_keys", doc.key),
                .preserve_base_document => {},
            }
        }
        for (value.deleted_keys) |key| {
            // This lane mixes logical IDs and physical tombstones. A public
            // binary ID can look like a physical key: retain both roots rather
            // than selecting an interpretation and silently missing an owner.
            try self.add("deleted_doc_keys", key);
            try self.documentRoot(key);
            if (keys.isInternalUserKey(key)) if (keys.findComponentTerminator(key, 1)) |end| {
                if (end + 2 < key.len) try self.artifact(key, false);
            };
        }
        for (value.overwritten_doc_keys) |key| {
            try self.add("overwritten_doc_keys", key);
            try self.documentRoot(key);
        }
        for (value.changed_artifact_keys) |key| try self.artifact(key, false);
        for (value.graph_doc_clears) |clear| try self.document(clear.key);
        for (value.graph_writes) |write| try self.graph(write, false);
        for (value.graph_deletes) |removed| try self.graph(removed, true);
        for (value.dense_embeddings) |embedding| {
            try self.documentRoot(embedding.doc_key);
            if (embedding.parent_doc_key) |parent| try self.documentRoot(parent);
            if (embedding.artifact_key) |key| try self.artifact(key, false);
        }
        for (value.sparse_embeddings) |embedding| {
            try self.documentRoot(embedding.doc_key);
            if (embedding.artifact_key) |key| try self.artifact(key, false);
        }
        for (value.generated_enrichment_refs) |ref| try self.document(ref.doc_key);
    }
    fn accepted(self: *Builder, value: effects.PrecomputedGeneratedBatch) !void {
        comptime assertFields(effects.PrecomputedGeneratedBatch, &.{ "artifact_writes", "artifact_delete_keys", "documents", "dense_embeddings", "sparse_embeddings", "generated_enrichment_refs", "coverage_outcomes" });
        for (value.artifact_writes) |write| try self.artifact(write.key, false);
        for (value.artifact_delete_keys) |key| try self.artifact(key, true);
        try self.request(.{ .documents = value.documents, .dense_embeddings = value.dense_embeddings, .sparse_embeddings = value.sparse_embeddings, .generated_enrichment_refs = value.generated_enrichment_refs });
        // Coverage metadata is not itself a replay lane. It can refer to a
        // generated document outside the original request and needs its root.
        for (value.coverage_outcomes) |outcome| try self.documentRoot(outcome.doc_key);
    }
};
fn assertFields(comptime T: type, comptime names: []const []const u8) void {
    const actual = std.meta.fieldNames(T);
    if (actual.len != names.len) @compileError("prepared replay plan must classify every effect field");
    for (actual) |name| {
        var found = false;
        for (names) |expected| if (std.mem.eql(u8, name, expected)) {
            found = true;
        };
        if (!found) @compileError("unclassified prepared replay field: " ++ name);
    }
}
/// request is the actual canonical preparation output, including explicit
/// artifact tombstones and graph clears. Existing artifact retirement is added
/// separately from live known inventory by the durable credit owner.
pub fn derive(a: A, request: derived.DerivedBatch, accepted: effects.PrecomputedGeneratedBatch, original: ?control.Port) !Plan {
    var builder: Builder = .{ .allocator = a, .original = original };
    defer builder.deinit();
    try builder.checkpoint();
    try builder.request(request);
    try builder.accepted(accepted);
    _ = try builder.base.encodedBytes();
    try builder.checkpoint();
    const roots = try builder.roots.toOwnedSlice(a);
    return .{ .allocator = a, .base = builder.base, .roots = roots };
}

const Fixture = struct {
    fn run(a: A) !void {
        const types = @import("db/types.zig");
        const journal = @import("db/derived/change_journal.zig");
        const graph = try keys.graphRelationshipArtifactKeyAlloc(a, "owner\x00x", "g", "rel", "target\x00x", "source\x00x", "edge");
        defer a.free(graph);
        const asset = try keys.artifactNamedPrefixAlloc(a, "foreign", "asset", "result");
        defer a.free(asset);
        const vector = try keys.embeddingArtifactKeyForDocumentAlloc(a, "vector-owner", "v");
        defer a.free(vector);
        const edge: types.GraphEdgeDelete = .{ .index_name = "g", .edge_type = "rel", .source = "source\x00x", .target = "target\x00x", .owner_document = "owner\x00x", .edge_id = "edge" };
        const request: derived.DerivedBatch = .{
            .documents = &.{ .{ .key = "request" }, .{ .key = "graph-only-no-row", .action = .preserve_base_document, .targets = &.{.{ .kind = .graph, .index_name = "g" }} }, .{ .key = "deleted-action", .action = .delete } },
            .deleted_keys = &.{ "deleted", graph },
            .overwritten_doc_keys = &.{"request"},
            .changed_artifact_keys = &.{vector},
            .graph_doc_clears = &.{.{ .key = "clear", .index_names = &.{"g"} }},
            .graph_writes = &.{.{ .index_name = "g", .edge_type = "rel", .source = "source\x00x", .target = "target\x00x", .owner_document = "owner\x00x", .edge_id = "edge" }},
            .graph_deletes = &.{edge},
            .dense_embeddings = &.{.{ .index_name = "v", .doc_key = "vector", .parent_doc_key = "parent", .artifact_key = vector, .vector = &.{1} }},
            .sparse_embeddings = &.{.{ .index_name = "s", .doc_key = "sparse", .artifact_key = vector, .indices = &.{1}, .values = &.{2} }},
            .generated_enrichment_refs = &.{.{ .kind = .asset, .index_name = "asset", .artifact_name = "a", .embedding_name = "", .doc_key = "request-ref" }},
        };
        var writes = [_]types.BatchWrite{.{ .key = asset, .value = "large payloads are not inspected by this key plan" }};
        var outcomes = [_]effects.PrecomputedCoverageOutcome{.{ .index_name = @constCast("v"), .doc_key = @constCast("coverage-only"), .outcome = .produced }};
        const accepted: effects.PrecomputedGeneratedBatch = .{
            .artifact_writes = &writes,
            .artifact_delete_keys = &.{graph},
            .documents = &.{.{ .key = "generated" }},
            .dense_embeddings = request.dense_embeddings,
            .sparse_embeddings = request.sparse_embeddings,
            .generated_enrichment_refs = &.{.{ .kind = .asset, .index_name = "asset", .artifact_name = "a", .embedding_name = "", .doc_key = "generated-ref" }},
            .coverage_outcomes = &outcomes,
        };
        var plan = try derive(a, request, accepted, null);
        defer plan.deinit();
        // Compare with the actual canonical record builders, including absent
        // graph tombstones, all accepted families, and endpoint wake-ups.
        var left = try journal.recordFromDerivedBatch(a, request, 42);
        defer journal.deinitRecord(a, &left);
        var right = try journal.recordFromDerivedBatch(a, .{
            .documents = accepted.documents,
            .deleted_keys = accepted.artifact_delete_keys,
            .changed_artifact_keys = &.{ asset, graph },
            .dense_embeddings = accepted.dense_embeddings,
            .sparse_embeddings = accepted.sparse_embeddings,
            .generated_enrichment_refs = accepted.generated_enrichment_refs,
        }, 42);
        defer journal.deinitRecord(a, &right);
        const changed = try std.mem.concat(a, []const u8, &.{ left.changed_doc_keys, right.changed_doc_keys, &.{ "source\x00x", "target\x00x" } });
        defer a.free(changed);
        const deleted = try std.mem.concat(a, []const u8, &.{ left.deleted_doc_keys, right.deleted_doc_keys });
        defer a.free(deleted);
        const artifacts = try std.mem.concat(a, []const u8, &.{ left.changed_artifact_keys, right.changed_artifact_keys });
        defer a.free(artifacts);
        const actual = try journal.encodeRecord(a, .{ .sequence = 42, .changed_doc_keys = changed, .deleted_doc_keys = deleted, .overwritten_doc_keys = left.overwritten_doc_keys, .changed_artifact_keys = artifacts });
        defer a.free(actual);
        try std.testing.expect(actual.len <= try plan.base.encodedBytes());
        for ([_][]const u8{ "request", "deleted", "owner\x00x", "foreign", "vector-owner", "vector", "parent", "sparse", "generated", "request-ref", "generated-ref", "coverage-only", "deleted-action", "graph-only-no-row" }) |logical| {
            const physical = try keys.documentKeyAlloc(a, logical);
            defer a.free(physical);
            const prefix = try inventory.docPrefix(physical);
            var found = false;
            for (plan.roots) |root| if (std.mem.eql(u8, root, prefix)) {
                found = true;
            };
            try std.testing.expect(found);
        }
        for (plan.roots, 0..) |root, i| for (plan.roots[0..i]) |previous| try std.testing.expect(!std.mem.eql(u8, root, previous));
    }
};

test "prepared replay plan covers canonical request and all seven accepted effect families" {
    try Fixture.run(std.testing.allocator);
}

test "prepared replay plan owns every allocation failure without retaining input payloads" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}

test "prepared replay plan preserves original cancellation and refuses nonphysical artifacts" {
    const controls = @import("../common/read_admission.zig");
    const Reject = struct {
        fn check(_: *anyopaque) !void {
            return error.Canceled;
        }
    };
    var unused: u8 = 0;
    const source: controls.Payload = .{ .ptr = &unused, .vtable = &.{ .checkpoint = Reject.check } };
    try std.testing.expectError(error.Canceled, derive(std.testing.allocator, .{}, .{}, controls.Port.init(&source)));
    var writes = [_]@import("db/types.zig").BatchWrite{.{ .key = "not a physical artifact", .value = "{}" }};
    try std.testing.expectError(error.InvalidArgument, derive(std.testing.allocator, .{}, .{ .artifact_writes = &writes }, null));
}

test "prepared replay plan retains both interpretations of binary deletion IDs" {
    const a = std.testing.allocator;
    const ambiguous = try keys.embeddingArtifactKeyForDocumentAlloc(a, "owner", "vector");
    defer a.free(ambiguous);
    var plan = try derive(a, .{ .deleted_keys = &.{ambiguous} }, .{}, null);
    defer plan.deinit();
    const encoded_public_id = try keys.documentKeyAlloc(a, ambiguous);
    defer a.free(encoded_public_id);
    const physical_owner = try inventory.docPrefix(ambiguous);
    const logical_owner = try inventory.docPrefix(encoded_public_id);
    var physical_found = false;
    var logical_found = false;
    for (plan.roots) |root| {
        physical_found = physical_found or std.mem.eql(u8, root, physical_owner);
        logical_found = logical_found or std.mem.eql(u8, root, logical_owner);
    }
    try std.testing.expect(physical_found and logical_found);
    var only_logical = try derive(a, .{ .deleted_keys = &.{"\x01\x00\x00"} }, .{}, null);
    defer only_logical.deinit();
    try std.testing.expectEqual(@as(usize, 1), only_logical.roots.len);
}

test "prepared replay plan reserves document deletion without inventing preserved primary rows" {
    const a = std.testing.allocator;
    const journal = @import("db/derived/change_journal.zig");
    var plan = try derive(a, .{ .documents = &.{
        .{ .key = "upsert" },
        .{ .key = "retired", .action = .delete },
        .{ .key = "edges-only", .action = .preserve_base_document },
    } }, .{}, null);
    defer plan.deinit();
    const actual = try journal.encodeRecord(a, .{ .changed_doc_keys = &.{"upsert"}, .deleted_doc_keys = &.{"retired"} });
    defer a.free(actual);
    try std.testing.expectEqual(actual.len, try plan.base.encodedBytes());
    try std.testing.expectEqual(@as(usize, 3), plan.roots.len);
}
