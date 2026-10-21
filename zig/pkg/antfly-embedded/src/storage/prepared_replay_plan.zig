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
/// Logical primary keys from the authoritative stored intent snapshot. The
/// transaction owner filters private metadata intents before supplying these.
/// Possible overwrites reserve a lane even when the producer omitted it.
pub const PrimaryKeys = struct {
    writes: []const []const u8 = &.{},
    deletes: []const []const u8 = &.{},
};
const changed_bit: u8 = 1;
const deleted_bit: u8 = 2;
const overwritten_bit: u8 = 4;
const Required = struct { needed: u8 = 0, seen: u8 = 0 };

const Builder = struct {
    allocator: A,
    original: ?control.Port,
    base: shape.Record = .{},
    roots: std.ArrayListUnmanaged([]const u8) = .empty,
    seen: std.StringHashMapUnmanaged(void) = .empty,
    // Only authoritative input keys are retained here, borrowed through derive.
    // Temporary generated/physical key buffers never enter this map.
    required: std.StringHashMapUnmanaged(Required) = .empty,
    fn deinit(self: *Builder) void {
        for (self.roots.items) |root| self.allocator.free(root);
        self.roots.deinit(self.allocator);
        self.seen.deinit(self.allocator);
        self.required.deinit(self.allocator);
    }
    fn checkpoint(self: *Builder) !void {
        if (self.original) |port| try port.checkpoint();
    }
    fn add(self: *Builder, comptime lane: []const u8, key: []const u8) !void {
        try self.checkpoint();
        try @field(self.base, lane).addKey(key.len);
        const bit: u8 = comptime if (std.mem.eql(u8, lane, "changed_doc_keys")) changed_bit else if (std.mem.eql(u8, lane, "deleted_doc_keys")) deleted_bit else if (std.mem.eql(u8, lane, "overwritten_doc_keys")) overwritten_bit else 0;
        if (bit != 0 and self.required.count() != 0) if (self.required.getPtr(key)) |entry| {
            entry.seen |= bit;
        };
    }
    fn primaryKeys(self: *Builder, primary: PrimaryKeys) !void {
        for (primary.writes) |key| try self.requireKey(key, changed_bit | overwritten_bit);
        for (primary.deletes) |key| try self.requireKey(key, deleted_bit);
    }
    fn requireKey(self: *Builder, key: []const u8, lanes: u8) !void {
        try self.checkpoint();
        const result = try self.required.getOrPut(self.allocator, key);
        if (!result.found_existing) result.value_ptr.* = .{};
        result.value_ptr.needed |= lanes;
        try self.documentRoot(key);
    }
    fn completePrimaryFloor(self: *Builder) !void {
        var iterator = self.required.iterator();
        while (iterator.next()) |entry| {
            try self.checkpoint();
            const missing = entry.value_ptr.needed & ~entry.value_ptr.seen;
            if (missing & changed_bit != 0) try self.add("changed_doc_keys", entry.key_ptr.*);
            if (missing & deleted_bit != 0) try self.add("deleted_doc_keys", entry.key_ptr.*);
            if (missing & overwritten_bit != 0) try self.add("overwritten_doc_keys", entry.key_ptr.*);
        }
    }
    fn physicalRoot(self: *Builder, physical: []const u8) !void {
        try self.checkpoint();
        const prefix = try inventory.docPrefix(physical);
        if (prefix.len >= physical.len) return error.InvalidPreparedEffects;
        try self.includeRoot(prefix);
    }
    fn includeRoot(self: *Builder, prefix: []const u8) !void {
        try self.checkpoint();
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
    return deriveWithPrimaryKeys(a, .{}, request, accepted, original);
}

/// Add missing primary replay lanes and roots from exact stored inputs without
/// counting a key twice when the supplied canonical plan already covers it.
/// This floor does not certify complete auxiliary/generated-effect preparation.
pub fn deriveWithPrimaryKeys(a: A, primary: PrimaryKeys, request: derived.DerivedBatch, accepted: effects.PrecomputedGeneratedBatch, original: ?control.Port) !Plan {
    var builder: Builder = .{ .allocator = a, .original = original };
    defer builder.deinit();
    try builder.checkpoint();
    try builder.primaryKeys(primary);
    try builder.request(request);
    try builder.accepted(accepted);
    try builder.completePrimaryFloor();
    _ = try builder.base.encodedBytes();
    try builder.checkpoint();
    const roots = try builder.roots.toOwnedSlice(a);
    return .{ .allocator = a, .base = builder.base, .roots = roots };
}

/// Capture the complete assembled primary-store key footprint and canonical
/// replay shape. Values are never read or retained. The owning preparation
/// pipeline supplies its final physical writes/deletes, including auxiliary
/// effects which cannot be reconstructed from an input DerivedBatch alone.
pub fn deriveFromMutations(a: A, writes: anytype, deletes: []const []const u8, replay: []const u8, original: ?control.Port) !Plan {
    const inspected = try @import("db/derived/change_journal.zig").inspectBinaryRecord(replay, original);
    var builder: Builder = .{ .allocator = a, .original = original };
    defer builder.deinit();
    comptime assertFields(shape.Record, &.{ "changed_doc_keys", "deleted_doc_keys", "overwritten_doc_keys", "changed_artifact_keys" });
    inline for (.{ "changed_doc_keys", "deleted_doc_keys", "overwritten_doc_keys", "changed_artifact_keys" }, 0..) |lane, index| {
        @field(builder.base, lane) = .{ .count = inspected.key_counts[index], .key_bytes = inspected.key_bytes[index] };
    }
    // The canonical encoder omits empty lists. Reject alternate encodings
    // rather than deriving a scalar size smaller than the actual wire record.
    if (try builder.base.encodedBytes() != replay.len) return error.InvalidPreparedEffects;
    for (writes) |write| {
        try builder.checkpoint();
        if (keys.isInternalUserKey(write.key)) try builder.physicalRoot(write.key);
    }
    for (deletes) |key| {
        try builder.checkpoint();
        if (keys.isInternalUserKey(key)) try builder.physicalRoot(key);
    }
    try builder.checkpoint();
    const roots = try builder.roots.toOwnedSlice(a);
    return .{ .allocator = a, .base = builder.base, .roots = roots };
}

/// Cover both the owning assembler's complete batch and the authoritative
/// stored-input/accepted-effect floor. Logical no-ops may shrink the assembled
/// replay; they cannot drop the original transaction's protected roots.
/// Maxima are per lane, so overlap is not charged twice.
pub fn coverAssembly(a: A, writes: anytype, deletes: []const []const u8, replay: []const u8, floor: Plan, original: ?control.Port) !Plan {
    var assembled = try deriveFromMutations(a, writes, deletes, replay, original);
    defer assembled.deinit();
    var builder: Builder = .{ .allocator = a, .original = original };
    defer builder.deinit();
    inline for (comptime std.meta.fieldNames(shape.Record)) |lane| {
        const left = @field(assembled.base, lane);
        const right = @field(floor.base, lane);
        _ = try left.encodedBytes();
        _ = try right.encodedBytes();
        @field(builder.base, lane) = .{ .count = @max(left.count, right.count), .key_bytes = @max(left.key_bytes, right.key_bytes) };
    }
    _ = try builder.base.encodedBytes();
    for (assembled.roots) |root| try builder.includeRoot(root);
    for (floor.roots) |root| try builder.includeRoot(root);
    try builder.checkpoint();
    return .{ .allocator = a, .base = builder.base, .roots = try builder.roots.toOwnedSlice(a) };
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

test "prepared replay plan authoritative primary floor covers omitted write delete overwrite lanes and roots" {
    const a = std.testing.allocator;
    const primary: PrimaryKeys = .{ .writes = &.{ "written", "shared", "written" }, .deletes = &.{ "removed", "shared" } };
    var plan = try deriveWithPrimaryKeys(a, primary, .{}, .{}, null);
    defer plan.deinit();
    try std.testing.expectEqual(@as(u64, 2), plan.base.changed_doc_keys.count);
    try std.testing.expectEqual(@as(u64, 2), plan.base.deleted_doc_keys.count);
    try std.testing.expectEqual(@as(u64, 2), plan.base.overwritten_doc_keys.count);
    try std.testing.expectEqual(@as(usize, 3), plan.roots.len);
    const payload = try @import("db/derived/change_journal.zig").encodeRecord(a, .{
        .sequence = 1,
        .changed_doc_keys = &.{ "written", "shared" },
        .deleted_doc_keys = &.{ "removed", "shared" },
        .overwritten_doc_keys = &.{ "written", "shared" },
    });
    defer a.free(payload);
    try std.testing.expect(payload.len <= try plan.base.encodedBytes());
    for ([_][]const u8{ "written", "shared", "removed" }) |key| {
        const physical = try keys.documentKeyAlloc(a, key);
        defer a.free(physical);
        const prefix = try inventory.docPrefix(physical);
        var found = false;
        for (plan.roots) |root| if (std.mem.eql(u8, root, prefix)) {
            found = true;
        };
        try std.testing.expect(found);
    }
}

test "prepared replay plan authoritative primary floor reuses covered lanes without dropping supplied multiplicity" {
    const a = std.testing.allocator;
    const request: derived.DerivedBatch = .{
        .documents = &.{ .{ .key = "written" }, .{ .key = "written" }, .{ .key = "generated" } },
        .deleted_keys = &.{"removed"},
        .overwritten_doc_keys = &.{"written"},
    };
    var original = try derive(a, request, .{}, null);
    defer original.deinit();
    var covered = try deriveWithPrimaryKeys(a, .{ .writes = &.{"written"}, .deletes = &.{"removed"} }, request, .{}, null);
    defer covered.deinit();
    try std.testing.expectEqualDeep(original.base, covered.base);
    try std.testing.expectEqual(original.roots.len, covered.roots.len);
    // A preserved base document is not changed by the request alone. The
    // authoritative floor still reserves possible primary application lanes.
    var preserved = try deriveWithPrimaryKeys(a, .{ .writes = &.{"written"} }, .{ .documents = &.{.{ .key = "written", .action = .preserve_base_document }} }, .{}, null);
    defer preserved.deinit();
    try std.testing.expectEqual(@as(u64, 1), preserved.base.changed_doc_keys.count);
    try std.testing.expectEqual(@as(u64, 1), preserved.base.overwritten_doc_keys.count);
}

test "prepared replay plan authoritative primary floor unwinds all owned allocations and original checkpoints" {
    const Checks = struct {
        calls: usize = 0,
        fail_at: usize = std.math.maxInt(usize),
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            if (self.calls == self.fail_at) return error.DeadlineExceeded;
        }
        fn run(a: A, checks: ?*@This()) !void {
            var payload: control.Payload = .{ .ptr = checks orelse undefined, .vtable = &.{ .checkpoint = check } };
            var plan = try deriveWithPrimaryKeys(a, .{ .writes = &.{ "binary\x00key", "other" }, .deletes = &.{"removed"} }, .{ .documents = &.{.{ .key = "other" }} }, .{}, if (checks != null) control.Port.init(&payload) else null);
            defer plan.deinit();
            try std.testing.expectEqual(@as(usize, 3), plan.roots.len);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Checks.run, .{@as(?*Checks, null)});
    var baseline: Checks = .{};
    try Checks.run(std.testing.allocator, &baseline);
    for (1..baseline.calls + 1) |fail_at| {
        var checks: Checks = .{ .fail_at = fail_at };
        try std.testing.expectError(error.DeadlineExceeded, Checks.run(std.testing.allocator, &checks));
    }
}

test "prepared replay plan assembled mutations capture every physical root without copying values" {
    const a = std.testing.allocator;
    const row = try keys.documentKeyAlloc(a, "row");
    defer a.free(row);
    const artifact = try keys.embeddingArtifactKeyForDocumentAlloc(a, "row", "v");
    defer a.free(artifact);
    const foreign = try keys.documentKeyAlloc(a, "group-result");
    defer a.free(foreign);
    const payload = try @import("db/derived/change_journal.zig").encodeRecord(a, .{ .sequence = 1, .changed_doc_keys = &.{ "row", "group-result" } });
    defer a.free(payload);
    const large_value: [65536]u8 = @splat('v');
    const writes = [_]@import("docstore.zig").KVPair{
        .{ .key = row, .value = &large_value },                           .{ .key = artifact, .value = &large_value },
        .{ .key = "\x00\x00__metadata__:opaque", .value = &large_value },
    };
    var buffer: [4096]u8 = undefined;
    var bounded = std.heap.FixedBufferAllocator.init(&buffer);
    var plan = try deriveFromMutations(bounded.allocator(), &writes, &.{ foreign, row }, payload, null);
    defer plan.deinit();
    try std.testing.expectEqual(@as(usize, 2), plan.roots.len);
    try std.testing.expectEqual(@as(u64, payload.len), try plan.base.encodedBytes());
    try std.testing.expectEqual(@as(u64, 2), plan.base.changed_doc_keys.count);
}

test "prepared replay plan assembled mutations refuse malformed physical keys and noncanonical records" {
    const a = std.testing.allocator;
    const journal = @import("db/derived/change_journal.zig");
    const empty = try journal.encodeRecord(a, .{ .sequence = 1 });
    defer a.free(empty);
    const writes = [_]@import("docstore.zig").KVPair{.{ .key = "\x01unterminated", .value = "ignored" }};
    try std.testing.expectError(error.InvalidArgument, deriveFromMutations(a, &writes, &.{}, empty, null));
    // A zero-length list can be inspected, but is not the encoder's canonical
    // omission. Its four wire bytes must not disappear from a derived budget.
    var alternate: [20]u8 = undefined;
    try std.testing.expectEqual(@as(usize, 16), empty.len);
    @memcpy(alternate[0..16], empty);
    alternate[15] = 1;
    std.mem.writeInt(u32, alternate[16..20], 0, .little);
    const no_writes: []const @import("docstore.zig").KVPair = &.{};
    try std.testing.expectError(error.InvalidPreparedEffects, deriveFromMutations(a, no_writes, &.{}, &alternate, null));
}

test "prepared replay plan assembled mutations unwind allocations and every original checkpoint" {
    const Checks = struct {
        calls: usize = 0,
        fail_at: usize = std.math.maxInt(usize),
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            if (self.calls == self.fail_at) return error.DeadlineExceeded;
        }
        fn run(a: A, checks: ?*@This()) !void {
            const backing = std.testing.allocator;
            const row = try keys.documentKeyAlloc(backing, "row\x00binary");
            defer backing.free(row);
            const other = try keys.documentKeyAlloc(backing, "other");
            defer backing.free(other);
            const payload = try @import("db/derived/change_journal.zig").encodeRecord(backing, .{ .sequence = 1, .changed_doc_keys = &.{"row\x00binary"}, .deleted_doc_keys = &.{"other"} });
            defer backing.free(payload);
            const writes = [_]@import("docstore.zig").KVPair{.{ .key = row, .value = "not retained" }};
            var owner: control.Payload = .{ .ptr = checks orelse undefined, .vtable = &.{ .checkpoint = check } };
            var plan = try deriveFromMutations(a, &writes, &.{other}, payload, if (checks != null) control.Port.init(&owner) else null);
            defer plan.deinit();
            try std.testing.expectEqual(@as(usize, 2), plan.roots.len);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Checks.run, .{@as(?*Checks, null)});
    var baseline: Checks = .{};
    try Checks.run(std.testing.allocator, &baseline);
    for (1..baseline.calls + 1) |fail_at| {
        var checks: Checks = .{ .fail_at = fail_at };
        try std.testing.expectError(error.DeadlineExceeded, Checks.run(std.testing.allocator, &checks));
    }
}

test "prepared replay plan assembly cover preserves primary floors and unions auxiliary roots without duplicate lane charges" {
    const a = std.testing.allocator;
    const row = try keys.documentKeyAlloc(a, "row");
    defer a.free(row);
    const auxiliary = try keys.documentKeyAlloc(a, "group-result");
    defer a.free(auxiliary);
    var floor = try deriveWithPrimaryKeys(a, .{ .writes = &.{"row"}, .deletes = &.{"gone"} }, .{}, .{}, null);
    defer floor.deinit();
    const payload = try @import("db/derived/change_journal.zig").encodeRecord(a, .{ .sequence = 17, .changed_doc_keys = &.{ "row", "group-result" } });
    defer a.free(payload);
    const writes = [_]@import("docstore.zig").KVPair{ .{ .key = row, .value = "{}" }, .{ .key = auxiliary, .value = "{}" } };
    var covered = try coverAssembly(a, &writes, &.{}, payload, floor, null);
    defer covered.deinit();
    try std.testing.expectEqual(@as(usize, 3), covered.roots.len);
    try std.testing.expectEqual(@as(u64, 2), covered.base.changed_doc_keys.count);
    try std.testing.expectEqual(floor.base.deleted_doc_keys, covered.base.deleted_doc_keys);
    try std.testing.expectEqual(floor.base.overwritten_doc_keys, covered.base.overwritten_doc_keys);
    try std.testing.expect(try covered.base.encodedBytes() >= payload.len);
}

test "prepared replay plan assembly cover unwinds every allocation and original checkpoint" {
    const Checks = struct {
        calls: usize = 0,
        fail_at: usize = std.math.maxInt(usize),
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            if (self.calls == self.fail_at) return error.DeadlineExceeded;
        }
        fn run(a: A, checks: ?*@This()) !void {
            const backing = std.testing.allocator;
            const row = try keys.documentKeyAlloc(backing, "auxiliary");
            defer backing.free(row);
            var floor = try deriveWithPrimaryKeys(backing, .{ .writes = &.{"input"} }, .{}, .{}, null);
            defer floor.deinit();
            const replay = try @import("db/derived/change_journal.zig").encodeRecord(backing, .{ .changed_doc_keys = &.{"auxiliary"} });
            defer backing.free(replay);
            const writes = [_]@import("docstore.zig").KVPair{.{ .key = row, .value = "opaque" }};
            var source: control.Payload = .{ .ptr = checks orelse undefined, .vtable = &.{ .checkpoint = check } };
            var result = try coverAssembly(a, &writes, &.{}, replay, floor, if (checks != null) .init(&source) else null);
            defer result.deinit();
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Checks.run, .{@as(?*Checks, null)});
    var baseline: Checks = .{};
    try Checks.run(std.testing.allocator, &baseline);
    for (1..baseline.calls + 1) |stop| {
        var checks: Checks = .{ .fail_at = stop };
        try std.testing.expectError(error.DeadlineExceeded, Checks.run(std.testing.allocator, &checks));
    }
}
