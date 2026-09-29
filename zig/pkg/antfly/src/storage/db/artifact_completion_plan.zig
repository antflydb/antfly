// Copyright 2026 Antfly, Inc. SPDX-License-Identifier: Elastic-2.0
//! Immutable inventory of completion requirements, not a provider queue.
//! Projection, resolution and promotion requirements must not disappear just
//! because they have no generated-enrichment request. A node is a requirement,
//! never proof that its effects have been accepted or its scope set is closed.
const std = @import("std");
const inventory = @import("artifact_inventory.zig");
const publication = @import("artifact_publication.zig");
const types = @import("types.zig");
const requests = @import("enrichment/enrichment_types.zig");
const resolvers = @import("catalog/resolver_catalog.zig");
const enrichments = @import("catalog/enrichment_catalog.zig");

pub const Kind = enum { native_effects, generated, index_projection, resolution, promotion, unit_children };
pub const Scope = enum { document, inline_chunks, materialized_chunks, producer_defined, upstream_units, index_projection, resolver_mentions, promotion_decisions };
pub const Node = struct {
    id: publication.Digest,
    kind: Kind,
    scope: Scope,
    name: []const u8,
    artifact: []const u8 = "",
    embedding: []const u8 = "",
    upstream: []const u8 = "",
    target_table: []const u8 = "",
    generation: u64 = 0,
    definition: publication.Digest = @splat(0),
    index_kind: ?types.IndexKind = null,
    generated_kind: ?requests.GeneratedEnrichmentKind = null,
    resolver_source_kind: ?resolvers.ResolverSourceArtifactKind = null,
    /// Local ordinal into the SAME pinned plan; excluded from stable identity.
    template: ?u32 = null,
    /// Extraction-owned child, independent of the top-level provider queue.
    /// This local parent ordinal is excluded from stable requirement identity.
    parent_template: ?u32 = null,
    neighbor_context: bool = false,

    fn digest(self: Node) publication.Digest {
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("antfly:required-artifact-stream:v1:");
        hash.update(&.{ @intFromEnum(self.kind), @intFromEnum(self.scope) });
        hash.update(&.{ if (self.index_kind) |kind| @as(u8, @intFromEnum(kind)) + 1 else 0, if (self.generated_kind) |kind| @as(u8, @intFromEnum(kind)) + 1 else 0 });
        hash.update(&.{if (self.resolver_source_kind) |kind| @as(u8, @intFromEnum(kind)) + 1 else 0});
        hash.update(&.{@intFromBool(self.neighbor_context)});
        var number: [8]u8 = undefined;
        std.mem.writeInt(u64, &number, self.generation, .little);
        hash.update(&number);
        hash.update(&self.definition);
        for ([_][]const u8{ self.name, self.artifact, self.embedding, self.upstream, self.target_table }) |value| {
            std.mem.writeInt(u64, &number, value.len, .little);
            hash.update(&number);
            hash.update(value);
        }
        var result: publication.Digest = undefined;
        hash.final(&result);
        return result;
    }

    pub fn byteCost(self: Node) usize {
        return @sizeOf(Node) +| self.name.len +| self.artifact.len +| self.embedding.len +| self.upstream.len +| self.target_table.len;
    }

    pub fn requireValidIdentity(self: Node) !void {
        if (!std.mem.eql(u8, &self.id, &self.digest())) return error.ArtifactCatalogCorrupt;
    }
};

pub const Cursor = struct { plan: publication.Digest, next: u32 = 0 };
pub const Page = struct { nodes: []const Node, cursor: Cursor, at_end: bool };

pub const Plan = struct {
    arena: std.heap.ArenaAllocator,
    catalog: publication.Digest,
    digest: publication.Digest,
    nodes: []const Node,
    providers: []const u32,
    definitions: std.AutoHashMapUnmanaged(publication.Digest, u32),
    unit_children: std.StringHashMapUnmanaged(u32) = .empty,
    enrichment_kinds: std.StringHashMapUnmanaged(enrichments.EnrichmentType) = .empty,
    root_chunk_scopes: std.StringHashMapUnmanaged(RootChunkScope) = .empty,
    native_requirement: u32 = 0,
    has_projection_requirements: bool = false,

    pub fn deinit(self: *Plan) void {
        self.arena.deinit();
        self.* = undefined;
    }

    pub fn provider(self: *const Plan, template: usize) !*const Node {
        if (template >= self.providers.len) return error.InvalidBatchRequest;
        return &self.nodes[self.providers[template]];
    }

    pub fn nativeEffects(self: *const Plan) !*const Node {
        if (self.native_requirement >= self.nodes.len) return error.ArtifactCatalogCorrupt;
        const node = &self.nodes[self.native_requirement];
        if (node.kind != .native_effects or node.scope != .document) return error.ArtifactCatalogCorrupt;
        return node;
    }

    /// One definition hash and point lookup, independent of catalog size.
    /// The returned node retains the canonical template ordinal for an exact
    /// field comparison by authorization callers.
    pub fn providerFor(self: *const Plan, request: requests.GeneratedEnrichmentRequest) !*const Node {
        const definition = @import("artifact_producer_input.zig").definitionDigest(request);
        const index = self.definitions.get(definition) orelse return error.ArtifactCatalogDrift;
        return &self.nodes[index];
    }

    pub fn unitChild(self: *const Plan, name: []const u8) !*const Node {
        const index = self.unit_children.get(name) orelse {
            if (self.enrichment_kinds.contains(name)) return error.OnlineMergeArtifactTailsUnsupported;
            return error.ArtifactCatalogDrift;
        };
        return &self.nodes[index];
    }

    pub fn rootChunkScopeClosed(self: *const Plan, artifact: []const u8) !bool {
        return switch (self.root_chunk_scopes.get(artifact) orelse return error.ArtifactCatalogDrift) {
            .closed => true,
            .open => false,
            .ambiguous => error.ArtifactCatalogDrift,
        };
    }

    pub fn init(alloc: std.mem.Allocator, catalogs: inventory.Catalogs, templates: []const requests.GeneratedEnrichmentRequest) !Plan {
        var scratch = std.heap.ArenaAllocator.init(alloc);
        defer scratch.deinit();
        const a = scratch.allocator();
        const indexes = if (catalogs.indexes.len == 0) &.{} else try @import("catalog/index_manager.zig").deserializeCatalog(a, catalogs.indexes);
        const resolver_configs = if (catalogs.resolvers.len == 0) &.{} else try resolvers.deserializeCatalog(a, catalogs.resolvers);
        const enrichment_configs = if (catalogs.enrichments.len == 0) &.{} else try enrichments.deserializeCatalog(a, catalogs.enrichments);
        return compileWithChildren(alloc, catalogs.digest(), templates, indexes, resolver_configs, enrichment_configs);
    }

    /// Borrowed bounded pages; the caller retains the immutable plan. Catalog
    /// changes invalidate cursors, rather than silently shifting ordinals.
    pub fn page(self: *const Plan, after: ?Cursor) !Page {
        const first = if (after) |cursor| blk: {
            if (!std.mem.eql(u8, &cursor.plan, &self.digest)) return error.ArtifactCatalogDrift;
            if (cursor.next > self.nodes.len) return error.InvalidBatchRequest;
            break :blk cursor.next;
        } else 0;
        var end: usize = first;
        var bytes: usize = 0;
        while (end < self.nodes.len and end - first < 128) : (end += 1) {
            const node = self.nodes[end];
            const cost = node.byteCost();
            if (cost > publication.max_payload_bytes) return error.ResourceBudgetExceeded;
            if (end != first and bytes +| cost > 64 * 1024) break;
            bytes +|= cost;
        }
        return .{ .nodes = self.nodes[first..end], .cursor = .{ .plan = self.digest, .next = @intCast(end) }, .at_end = end == self.nodes.len };
    }
};

const RootChunkScope = enum { closed, open, ambiguous };

test "ordered artifact inventory compiles singleton and producer-defined scope once" {
    const Check = struct {
        fn run(alloc: std.mem.Allocator) !void {
            const templates = [_]requests.GeneratedEnrichmentRequest{
                .{ .kind = .asset, .index_name = "", .artifact_name = "copy", .doc_key = "", .source_field = "body" },
                .{ .kind = .asset, .index_name = "", .artifact_name = "generated", .doc_key = "", .source_field = "body", .producer_json = "{\"type\":\"generator\",\"config\":{\"model\":\"one\"}}" },
                .{ .kind = .asset, .index_name = "", .artifact_name = "units", .doc_key = "", .source_field = "body", .producer_json = "{\"type\":\"document_extraction\"}" },
                .{ .kind = .chunk_text, .index_name = "", .artifact_name = "chunks", .doc_key = "", .source_field = "body" },
                .{ .kind = .chunk_text, .index_name = "", .artifact_name = "unit-chunks", .doc_key = "", .source_field = "body", .upstream_artifact_name = "units" },
            };
            var plan = try compile(alloc, @splat(1), &templates, &.{}, &.{});
            defer plan.deinit();
            const scopes = [_]Scope{ .document, .document, .producer_defined, .document, .upstream_units };
            for (templates, scopes) |request, expected| try std.testing.expectEqual(expected, (try plan.providerFor(request)).scope);
            var changed = templates[1];
            changed.producer_json = templates[2].producer_json;
            try std.testing.expectError(error.ArtifactCatalogDrift, plan.providerFor(changed));
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Check.run, .{});
}

fn compile(alloc: std.mem.Allocator, catalog: publication.Digest, templates: []const requests.GeneratedEnrichmentRequest, indexes: []const types.IndexConfig, resolver_configs: []const resolvers.ResolverConfig) !Plan {
    return compileWithChildren(alloc, catalog, templates, indexes, resolver_configs, &.{});
}

fn compileWithChildren(alloc: std.mem.Allocator, catalog: publication.Digest, templates: []const requests.GeneratedEnrichmentRequest, indexes: []const types.IndexConfig, resolver_configs: []const resolvers.ResolverConfig, enrichment_configs: []const enrichments.EnrichmentConfig) !Plan {
    const count = 1 +| templates.len +| indexes.len +| resolver_configs.len *| 2 +| enrichment_configs.len;
    if (count > std.math.maxInt(u32)) return error.ResourceLimitExceeded;
    var arena = std.heap.ArenaAllocator.init(alloc);
    errdefer arena.deinit();
    const a = arena.allocator();
    const nodes = try a.alloc(Node, count);
    var root_chunk_scopes: std.StringHashMapUnmanaged(RootChunkScope) = .empty;
    for (templates) |request| {
        if (request.kind != .chunk_text) continue;
        if (root_chunk_scopes.getPtr(request.artifact_name)) |existing| {
            existing.* = .ambiguous;
        } else {
            try root_chunk_scopes.put(a, try a.dupe(u8, request.artifact_name), if (request.upstream_artifact_name.len == 0 and request.neighbor_context_json.len == 0) .closed else .open);
        }
    }
    var enrichment_kinds: std.StringHashMapUnmanaged(enrichments.EnrichmentType) = .empty;
    for (enrichment_configs) |config| {
        const name = try a.dupe(u8, config.name);
        const entry = try enrichment_kinds.getOrPut(a, name);
        if (entry.found_existing) return error.ArtifactCatalogCorrupt;
        entry.value_ptr.* = config.kind;
    }
    // Authored embeddings and surviving native artifacts are not represented
    // by provider templates. Even an empty catalog needs their exact-input
    // acceptance/retirement check; zero providers must not imply completion.
    nodes[0] = .{ .id = undefined, .kind = .native_effects, .scope = .document, .name = "native" };
    var next: usize = 1;
    for (templates, 0..) |request, ordinal| {
        nodes[next] = .{
            .id = undefined,
            .kind = .generated,
            .scope = switch (request.kind) {
                .asset => blk: {
                    // Scope is a catalog property, not something each row's
                    // completion/execution path must reparse from provider JSON.
                    var config = try @import("enrichment/asset_producer.zig").parseProducerConfig(alloc, request.producer_json);
                    defer config.deinit(alloc);
                    break :blk if (config.type == .document_extraction) .producer_defined else .document;
                },
                .chunk_text => if (request.upstream_artifact_name.len == 0) .document else .upstream_units,
                .dense_embedding, .sparse_embedding => switch (request.input_kind) {
                    .document => .document,
                    .inline_chunks => .inline_chunks,
                    .materialized_chunks => .materialized_chunks,
                },
            },
            .name = if (request.index_name.len != 0) request.index_name else request.artifact_name,
            .artifact = request.artifact_name,
            .embedding = request.embedding_name,
            .upstream = request.upstream_artifact_name,
            .generated_kind = request.kind,
            .definition = @import("artifact_producer_input.zig").definitionDigest(request),
            .template = @intCast(ordinal),
        };
        next += 1;
    }
    var parents: std.StringHashMapUnmanaged(?u32) = .empty;
    for (nodes[1..next]) |node| {
        if (node.scope != .producer_defined) continue;
        const entry = try parents.getOrPut(a, node.artifact);
        if (entry.found_existing) {
            if (!std.mem.eql(u8, &nodes[1 + entry.value_ptr.*.?].definition, &node.definition)) return error.ArtifactCatalogCorrupt;
        } else entry.value_ptr.* = node.template.?;
    }
    // Missing executable templates must not erase catalog-owned children.
    // Preserve their requirement with no dispatch ordinal; verification stays
    // pending until the immutable execution plan supplies the parent.
    for (enrichment_configs) |config| {
        if (config.kind != .asset or parents.contains(config.name)) continue;
        var producer = try @import("enrichment/asset_producer.zig").parseProducerConfig(alloc, config.producer_json);
        defer producer.deinit(alloc);
        if (producer.type == .document_extraction) try parents.put(a, config.name, null);
    }
    for (enrichment_configs) |config| {
        if (config.kind != .chunk or config.source_artifact_name.len == 0) continue;
        const parent = parents.get(config.source_artifact_name) orelse continue;
        // Compile the full child definition once, not once per row/page. It
        // includes chunker/execution settings even though the parent owns work.
        const encoded = try std.json.Stringify.valueAlloc(alloc, config, .{});
        defer alloc.free(encoded);
        var definition: publication.Digest = undefined;
        std.crypto.hash.Blake3.hash(encoded, &definition, .{});
        nodes[next] = .{ .id = undefined, .kind = .unit_children, .scope = .upstream_units, .name = config.name, .artifact = config.name, .upstream = config.source_artifact_name, .definition = definition, .generated_kind = .chunk_text, .parent_template = parent, .neighbor_context = config.neighbor_context_json.len != 0 };
        next += 1;
    }
    for (indexes) |index| {
        const generation = switch (index.kind) {
            .dense_vector, .sparse_vector => @import("../internal_keys.zig").derivedCoverageGenerationForConfig(index.coverage_generation, index.config_json),
            else => index.coverage_generation,
        };
        nodes[next] = .{ .id = undefined, .kind = .index_projection, .scope = .index_projection, .name = index.name, .generation = generation, .index_kind = index.kind };
        next += 1;
    }
    for (resolver_configs) |resolver| {
        nodes[next] = .{ .id = undefined, .kind = .resolution, .scope = .resolver_mentions, .name = resolver.name, .artifact = resolver.resolution_artifact, .upstream = resolver.source_artifact, .target_table = resolver.table, .resolver_source_kind = resolver.source_artifact_kind, .generation = resolver.config_generation };
        nodes[next + 1] = .{ .id = undefined, .kind = .promotion, .scope = .promotion_decisions, .name = resolver.name, .artifact = resolver.resolution_artifact, .upstream = resolver.resolution_artifact, .target_table = resolver.table, .resolver_source_kind = resolver.source_artifact_kind, .generation = resolver.config_generation };
        next += 2;
    }
    for (nodes[0..next]) |*node| {
        if (node.name.len == 0) return error.ArtifactCatalogCorrupt;
        node.name = try a.dupe(u8, node.name);
        node.artifact = try a.dupe(u8, node.artifact);
        node.embedding = try a.dupe(u8, node.embedding);
        node.upstream = try a.dupe(u8, node.upstream);
        node.target_table = try a.dupe(u8, node.target_table);
        node.id = node.digest();
    }
    std.mem.sort(Node, nodes[0..next], {}, struct {
        fn less(_: void, left: Node, right: Node) bool {
            return std.mem.order(u8, &left.id, &right.id) == .lt;
        }
    }.less);
    const providers = try a.alloc(u32, templates.len);
    var unique: usize = 0;
    for (nodes[0..next]) |node| {
        if (unique != 0 and std.mem.eql(u8, &nodes[unique - 1].id, &node.id)) {
            if (node.kind != .generated) return error.ArtifactCatalogCorrupt;
            // Identical providers can be routed to different consumers. Their
            // projections are independent nodes, but acceptance is shared.
            providers[node.template.?] = @intCast(unique - 1);
            nodes[unique - 1].template = @min(nodes[unique - 1].template.?, node.template.?);
            continue;
        }
        nodes[unique] = node;
        if (node.template) |template| providers[template] = @intCast(unique);
        unique += 1;
    }
    const requirements = nodes[0..unique];
    var definitions: std.AutoHashMapUnmanaged(publication.Digest, u32) = .empty;
    var unit_children: std.StringHashMapUnmanaged(u32) = .empty;
    for (requirements, 0..) |node, index| if (node.kind == .unit_children) {
        const entry = try unit_children.getOrPut(a, node.name);
        if (entry.found_existing) return error.ArtifactCatalogCorrupt;
        entry.value_ptr.* = @intCast(index);
    };
    for (requirements, 0..) |node, index| if (node.kind == .generated) {
        const entry = try definitions.getOrPut(a, node.definition);
        if (entry.found_existing) return error.ArtifactCatalogCorrupt;
        entry.value_ptr.* = @intCast(index);
    };
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly:artifact-completion-plan:v2:");
    hash.update(&catalog);
    var encoded_count: [8]u8 = undefined;
    std.mem.writeInt(u64, &encoded_count, requirements.len, .little);
    hash.update(&encoded_count);
    for (requirements) |node| hash.update(&node.id);
    const RootEntry = struct { name: []const u8, scope: RootChunkScope };
    const root_entries = try a.alloc(RootEntry, root_chunk_scopes.count());
    var root_iterator = root_chunk_scopes.iterator();
    for (root_entries) |*entry| {
        const next_entry = root_iterator.next() orelse return error.ArtifactCatalogCorrupt;
        entry.* = .{ .name = next_entry.key_ptr.*, .scope = next_entry.value_ptr.* };
    }
    std.mem.sort(RootEntry, root_entries, {}, struct {
        fn less(_: void, left: RootEntry, right: RootEntry) bool {
            return std.mem.order(u8, left.name, right.name) == .lt;
        }
    }.less);
    std.mem.writeInt(u64, &encoded_count, root_entries.len, .little);
    hash.update(&encoded_count);
    for (root_entries) |entry| {
        std.mem.writeInt(u64, &encoded_count, entry.name.len, .little);
        hash.update(&encoded_count);
        hash.update(entry.name);
        hash.update(&.{@intFromEnum(entry.scope)});
    }
    var digest: publication.Digest = undefined;
    hash.final(&digest);
    const native_requirement = for (requirements, 0..) |node, index| {
        if (node.kind == .native_effects) break @as(u32, @intCast(index));
    } else return error.ArtifactCatalogCorrupt;
    return .{ .arena = arena, .catalog = catalog, .digest = digest, .nodes = requirements, .providers = providers, .definitions = definitions, .unit_children = unit_children, .enrichment_kinds = enrichment_kinds, .root_chunk_scopes = root_chunk_scopes, .native_requirement = native_requirement, .has_projection_requirements = indexes.len != 0 };
}

test "ordered artifact inventory completion plan retains extraction-owned children without worker templates" {
    const Check = struct {
        fn run(alloc: std.mem.Allocator) !void {
            const parent: requests.GeneratedEnrichmentRequest = .{ .kind = .asset, .doc_key = "", .index_name = "", .artifact_name = "units", .source_field = "url", .producer_json = "{\"type\":\"document_extraction\"}" };
            const other: requests.GeneratedEnrichmentRequest = .{ .kind = .chunk_text, .doc_key = "", .index_name = "", .artifact_name = "ordinary", .source_field = "body" };
            const configs = [_]enrichments.EnrichmentConfig{
                .{ .kind = .asset, .name = "units", .source_field = "url", .producer_json = parent.producer_json },
                .{ .kind = .chunk, .name = "child", .source_field = "body", .source_artifact_name = "units", .chunk_size = 4 },
            };
            var plan = try compileWithChildren(alloc, @splat(1), &.{ parent, other }, &.{}, &.{}, &configs);
            defer plan.deinit();
            try std.testing.expectEqual(@as(usize, 4), plan.nodes.len);
            try std.testing.expectEqual(@as(usize, 2), plan.providers.len);
            const child = try plan.unitChild("child");
            try std.testing.expectEqual(Kind.unit_children, child.kind);
            try std.testing.expectEqual(Scope.upstream_units, child.scope);
            try std.testing.expectEqual(@as(?u32, 0), child.parent_template);
            try std.testing.expect(child.template == null);
            try std.testing.expectEqualStrings("units", child.upstream);
            try std.testing.expectEqual(Kind.generated, (try plan.provider(0)).kind);
            var reordered = try compileWithChildren(alloc, @splat(1), &.{ other, parent }, &.{}, &.{}, &.{ configs[1], configs[0] });
            defer reordered.deinit();
            try std.testing.expectEqualDeep(plan.digest, reordered.digest);
            try std.testing.expectEqualDeep(child.id, (try reordered.unitChild("child")).id);
            try std.testing.expectEqual(@as(?u32, 1), (try reordered.unitChild("child")).parent_template);
            var changed = configs;
            changed[1].chunk_size = 8;
            var reconfigured = try compileWithChildren(alloc, @splat(1), &.{parent}, &.{}, &.{}, &changed);
            defer reconfigured.deinit();
            try std.testing.expect(!std.meta.eql(child.id, (try reconfigured.unitChild("child")).id));
            var missing = try compileWithChildren(alloc, @splat(1), &.{}, &.{}, &.{}, &configs);
            defer missing.deinit();
            try std.testing.expectEqual(@as(usize, 2), missing.nodes.len);
            try std.testing.expect((try missing.unitChild("child")).parent_template == null);
            try std.testing.expectEqualDeep(child.id, (try missing.unitChild("child")).id);
            try std.testing.expectError(error.ArtifactCatalogDrift, missing.unitChild("other"));
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Check.run, .{});
}

test "ordered artifact inventory completion plan retains non-provider requirements and stable identities" {
    const Check = struct {
        fn run(alloc: std.mem.Allocator) !void {
            const templates = [_]requests.GeneratedEnrichmentRequest{
                .{ .kind = .chunk_text, .index_name = "", .artifact_name = "chunks", .doc_key = "", .source_field = "body" },
                .{ .kind = .dense_embedding, .index_name = "dense", .artifact_name = "chunks", .embedding_name = "model", .doc_key = "", .source_field = "body", .input_kind = .materialized_chunks },
            };
            const indexes = [_]types.IndexConfig{
                .{ .name = "text", .kind = .full_text, .config_json = "{}" },
                .{ .name = "dense", .kind = .dense_vector, .config_json = "{}", .coverage_generation = 3 },
                .{ .name = "sparse", .kind = .sparse_vector, .config_json = "{}" },
                .{ .name = "graph", .kind = .graph, .config_json = "{}" },
                .{ .name = "relational", .kind = .algebraic, .config_json = "{}" },
            };
            const configs = [_]resolvers.ResolverConfig{.{ .name = "entities", .table = "people", .source_artifact = "mentions", .resolution_artifact = "resolved", .key_template = "{{name}}" }};
            var plan = try compile(alloc, @splat(1), &templates, &indexes, &configs);
            defer plan.deinit();
            try std.testing.expectEqual(@as(usize, 10), plan.nodes.len);
            var counts = [_]usize{ 0, 0, 0, 0, 0 };
            for (plan.nodes) |node| {
                counts[@intFromEnum(node.kind)] += 1;
                if (node.template) |ordinal| try std.testing.expectEqual(templates[ordinal].kind, node.generated_kind.?);
                if (node.kind == .resolution) {
                    try std.testing.expectEqualStrings("resolved", node.artifact);
                    try std.testing.expectEqualStrings("mentions", node.upstream);
                    try std.testing.expectEqualStrings("people", node.target_table);
                    var changed = node;
                    changed.target_table = "other";
                    try std.testing.expect(!std.mem.eql(u8, &node.id, &changed.digest()));
                    changed = node;
                    changed.resolver_source_kind = .chunk;
                    try std.testing.expect(!std.mem.eql(u8, &node.id, &changed.digest()));
                }
            }
            try std.testing.expectEqualDeep([_]usize{ 1, 2, 5, 1, 1 }, counts);
            const reversed = [_]requests.GeneratedEnrichmentRequest{ templates[1], templates[0] };
            var reordered = try compile(alloc, @splat(1), &reversed, &indexes, &configs);
            defer reordered.deinit();
            for (plan.nodes, reordered.nodes) |left, right| try std.testing.expectEqualDeep(left.id, right.id);
            try std.testing.expectEqualDeep(plan.digest, reordered.digest);
            for (templates, 0..) |template, ordinal| {
                const node = try plan.provider(ordinal);
                try std.testing.expectEqual(template.kind, node.generated_kind.?);
                try std.testing.expectEqual(ordinal, node.template.?);
                try std.testing.expectEqualStrings(template.artifact_name, node.artifact);
            }
            try std.testing.expectError(error.InvalidBatchRequest, plan.provider(templates.len));
            const page = try plan.page(null);
            try std.testing.expect(page.at_end);
            try std.testing.expectEqual(@as(usize, 0), (try plan.page(page.cursor)).nodes.len);
            var stale = page.cursor;
            stale.plan[0] ^= 1;
            try std.testing.expectError(error.ArtifactCatalogDrift, plan.page(stale));
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Check.run, .{});
    const duplicate: requests.GeneratedEnrichmentRequest = .{ .kind = .asset, .index_name = "extract", .artifact_name = "extract", .doc_key = "", .source_field = "body" };
    var consumers = [_][]u8{@constCast("other-consumer")};
    var routed = duplicate;
    routed.consumer_indexes = &consumers;
    var shared = try compile(std.testing.allocator, @splat(1), &.{ duplicate, routed }, &.{}, &.{});
    defer shared.deinit();
    try std.testing.expectEqual(@as(usize, 2), shared.nodes.len);
    try std.testing.expect(try shared.provider(0) == try shared.provider(1));
    try std.testing.expect(try shared.providerFor(duplicate) == try shared.provider(0));
    var row_request = duplicate;
    row_request.doc_key = "different-row";
    row_request.sequence = 999;
    var consumer = [_][]u8{@constCast("different-consumer")};
    row_request.consumer_indexes = &consumer;
    try std.testing.expect(try shared.providerFor(row_request) == try shared.provider(0));
    var distinct = duplicate;
    distinct.source_field = "different-input";
    try std.testing.expectError(error.ArtifactCatalogDrift, shared.providerFor(distinct));
    var different = try compile(std.testing.allocator, @splat(1), &.{ duplicate, distinct }, &.{}, &.{});
    defer different.deinit();
    try std.testing.expectEqual(@as(usize, 3), different.nodes.len);
    try std.testing.expect(try different.providerFor(duplicate) != try different.providerFor(distinct));
    const index: types.IndexConfig = .{ .name = "index", .kind = .full_text, .config_json = "{}" };
    try std.testing.expectError(error.ArtifactCatalogCorrupt, compile(std.testing.allocator, @splat(1), &.{}, &.{ index, index }, &.{}));
}

test "ordered artifact inventory completion requirements page without catalog parsing or omitted tails" {
    const alloc = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    var indexes: [300]types.IndexConfig = undefined;
    for (&indexes, 0..) |*index, ordinal| index.* = .{ .name = try std.fmt.allocPrint(arena.allocator(), "index-{d}", .{ordinal}), .kind = .graph, .config_json = "{}" };
    var plan = try compile(alloc, @splat(2), &.{}, &indexes, &.{});
    defer plan.deinit();
    var cursor: ?Cursor = null;
    var count: usize = 0;
    var pages: usize = 0;
    while (true) {
        const page = try plan.page(cursor);
        try std.testing.expect(page.nodes.len != 0 and page.nodes.len <= 128);
        try std.testing.expectEqualSlices(Node, plan.nodes[count .. count + page.nodes.len], page.nodes);
        count += page.nodes.len;
        pages += 1;
        cursor = page.cursor;
        if (page.at_end) break;
    }
    try std.testing.expectEqual(@as(usize, 301), count);
    try std.testing.expectEqual(@as(usize, 3), pages);
    try std.testing.expectError(error.InvalidBatchRequest, plan.page(.{ .plan = plan.digest, .next = 302 }));
    var empty = try compile(alloc, @splat(3), &.{}, &.{}, &.{});
    defer empty.deinit();
    try std.testing.expectEqual(@as(usize, 1), empty.nodes.len);
    try std.testing.expectEqual(Kind.native_effects, empty.nodes[0].kind);
    // A changed requirement inventory also invalidates cursors, even when
    // persisted catalog bytes did not change (for example a policy upgrade).
    var same_catalog = try compile(alloc, plan.catalog, &.{}, &.{}, &.{});
    defer same_catalog.deinit();
    try std.testing.expectError(error.ArtifactCatalogDrift, same_catalog.page(cursor));
    const padding: [40 * 1024]u8 = @splat('x');
    var large_indexes: [3]types.IndexConfig = undefined;
    for (&large_indexes, 0..) |*index, ordinal| index.* = .{ .name = try std.fmt.allocPrint(arena.allocator(), "{s}{d}", .{ padding, ordinal }), .kind = .graph, .config_json = "{}" };
    var large = try compile(alloc, @splat(4), &.{}, &large_indexes, &.{});
    defer large.deinit();
    cursor = null;
    count = 0;
    pages = 0;
    while (true) {
        const page = try large.page(cursor);
        var bytes: usize = 0;
        for (page.nodes) |node| bytes += node.byteCost();
        try std.testing.expect(bytes <= 64 * 1024);
        count += page.nodes.len;
        pages += 1;
        cursor = page.cursor;
        if (page.at_end) break;
    }
    try std.testing.expectEqual(@as(usize, 4), count);
    try std.testing.expect(pages >= 3);
}
