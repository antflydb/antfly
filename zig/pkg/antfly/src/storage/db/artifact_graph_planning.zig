// Copyright 2026 Antfly, Inc. Licensed under the Elastic License 2.0.
//! Immutable graph planning context. Existing contender algorithms read one
//! snapshot through a read-only Store facade and collect guarded commands;
//! they never materialize worker-local graph state under producer authority.
const std = @import("std");
const erased = @import("../backend_erased.zig");
const snapshot_store = @import("../snapshot_store_view.zig");
const publication = @import("artifact_publication.zig");
const provenance = @import("artifact_producer_provenance.zig");
const inventory = @import("artifact_inventory.zig");
const manager = @import("catalog/index_manager.zig");
const resolvers = @import("catalog/resolver_catalog.zig");
const keys = @import("../internal_keys.zig");
const types = @import("types.zig");

pub const Catalog = struct {
    entries: []Entry,
    resolvers: struct { items: []resolvers.ResolverConfig },
    pub const Entry = struct { config: types.IndexConfig, artifact_sources: []manager.GraphArtifactSource, max_edges_per_document: u32 };

    pub fn graphIndexes(self: *const Catalog) []const Entry {
        return self.entries;
    }
    pub fn hasGraphIndexes(self: *const Catalog) bool {
        return self.entries.len != 0;
    }
    pub fn graphIndex(self: *Catalog, name: []const u8) ?*Entry {
        for (self.entries) |*entry| if (std.mem.eql(u8, name, entry.config.name)) return entry;
        return null;
    }
    pub fn graphArtifactSources(self: *const Catalog, name: []const u8) []const manager.GraphArtifactSource {
        for (self.entries) |entry| if (std.mem.eql(u8, name, entry.config.name)) return entry.artifact_sources;
        return &.{};
    }
    pub fn graphArtifactSourceForArtifact(self: *const Catalog, name: []const u8, artifact: []const u8) ?manager.GraphArtifactSource {
        for (self.graphArtifactSources(name)) |source| if (std.mem.eql(u8, source.artifact_name, artifact)) return source;
        return null;
    }
};

pub const Context = struct {
    owner: std.mem.Allocator,
    arena: std.heap.ArenaAllocator,
    alloc: std.mem.Allocator,
    owned: std.mem.Allocator,
    store: erased.Store,
    read: *erased.ReadTxn,
    catalog: Catalog,
    index_manager: *Catalog,
    base: publication.Command,
    source_index: u32,
    commands: std.ArrayList(publication.Command) = .empty,
    proof: provenance.Owned,

    /// expected_asset is the exact stored artifact read in this snapshot.
    /// Its accepted proof must still have current original inputs: a receipt
    /// from before a primary update cannot authorize a new graph projection.
    pub fn create(owner: std.mem.Allocator, read: *erased.ReadTxn, document: []const u8, artifact_name: []const u8, artifact_key: []const u8, expected_asset: ?[]const u8) !*Context {
        const self = try owner.create(Context);
        errdefer owner.destroy(self);
        self.arena = std.heap.ArenaAllocator.init(owner);
        errdefer self.arena.deinit();
        self.owner = owner;
        self.alloc = owner;
        self.owned = self.arena.allocator();
        self.read = read;
        self.store = snapshot_store.borrow(self.alloc, read);
        self.commands = .empty;
        self.proof = (try provenance.readCurrentForArtifact(self.owned, read, artifact_key, expected_asset)) orelse return error.ArtifactPublicationPending;
        errdefer self.proof.deinit();
        const proof = self.proof.proof;
        self.source_index = for (proof.sources, 0..) |source, index| {
            if (std.mem.eql(u8, source.document_key, document)) break @intCast(index);
        } else return error.InvalidBatchRequest;
        const catalogs = try inventory.catalogs(read);
        if (!std.mem.eql(u8, &catalogs.digest(), &proof.catalog_digest)) return error.ArtifactCatalogDrift;
        const configs: []const types.IndexConfig = if (catalogs.indexes.len == 0) &.{} else try manager.deserializeCatalog(self.owned, catalogs.indexes);
        var entries: std.ArrayList(Catalog.Entry) = .empty;
        for (configs) |config| {
            if (config.kind != .graph) continue;
            const parsed = try manager.parseGraphConfig(self.owned, config.config_json);
            const consumes = for (parsed.artifact_sources) |source| {
                if (std.mem.eql(u8, source.artifact_name, artifact_name)) break true;
            } else false;
            if (consumes) try entries.append(self.owned, .{ .config = config, .artifact_sources = parsed.artifact_sources, .max_edges_per_document = parsed.max_edges_per_document });
        }
        self.catalog = .{ .entries = entries.items, .resolvers = .{ .items = if (catalogs.resolvers.len == 0) &.{} else try resolvers.deserializeCatalog(self.owned, catalogs.resolvers) } };
        self.index_manager = &self.catalog;
        var guards: std.ArrayList(publication.ArtifactSource) = .empty;
        for (proof.artifact_sources) |input| if (!std.mem.eql(u8, input.key, artifact_key)) try guards.append(self.owned, input);
        try guards.append(self.owned, try self.guard(artifact_key));
        std.mem.sort(publication.ArtifactSource, guards.items, {}, guardLess);
        self.base = .{ .producer_kind = .graph, .namespace = proof.namespace, .authority_epoch = proof.authority_epoch, .catalog_digest = proof.catalog_digest, .producer_name = "", .producer_generation = 0, .producer_artifact_name = try self.owned.dupe(u8, artifact_name), .sources = proof.sources, .artifact_sources = guards.items, .mutations = &.{}, .publication_digest = @splat(0) };
        return self;
    }

    pub fn destroy(self: *Context) void {
        const owner = self.owner;
        self.proof.deinit();
        self.arena.deinit();
        owner.destroy(self);
    }

    pub fn getAlloc(self: *Context, key: []const u8) ![]u8 {
        var fork = try self.read.forkRead();
        defer fork.abort();
        return self.alloc.dupe(u8, try fork.get(key));
    }

    fn guard(self: *Context, key: []const u8) !publication.ArtifactSource {
        var fork = try self.read.forkRead();
        defer fork.abort();
        const value = fork.get(key) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        var digest: ?publication.Digest = null;
        if (value) |bytes| {
            var hash: publication.Digest = undefined;
            std.crypto.hash.sha2.Sha256.hash(bytes, &hash, .{});
            digest = hash;
        }
        return .{ .key = try self.owned.dupe(u8, key), .content_digest = digest, .input_position = try publication.artifactRevision(&fork, self.proof.proof.namespace, key), .source_index = self.source_index };
    }

    /// Fast-path accepted logical input before sampling mutable contender
    /// state. CAS preconditions intentionally do not change receipt identity.
    pub fn accepted(self: *Context, entry: Catalog.Entry) !bool {
        var command = self.base;
        command.producer_name = entry.config.name;
        command.producer_generation = entry.config.coverage_generation;
        return try publication.readReceipt(self.read, command, command.sources[self.source_index]) != null;
    }

    pub fn publishGraphEffects(self: *Context, writes: anytype, deletes: []const []const u8) !void {
        if (writes.len == 0 and deletes.len == 0) return error.InvalidBatchRequest;
        const first_key = if (writes.len != 0) writes[0].key else deletes[0];
        const entry = for (self.catalog.entries) |candidate| {
            if (matches(first_key, candidate.config.name)) break candidate;
        } else return error.InvalidBatchRequest;
        var command = self.base;
        command.producer_name = entry.config.name;
        command.producer_generation = entry.config.coverage_generation;
        var mutations: std.ArrayList(publication.Mutation) = .empty;
        var conditions: std.ArrayList(publication.ArtifactSource) = .empty;
        for (writes) |write| {
            try mutations.append(self.owned, .{ .family = .graph, .key = try self.owned.dupe(u8, write.key), .value = try self.owned.dupe(u8, write.value), .source_index = self.source_index });
            try conditions.append(self.owned, try self.guard(write.key));
        }
        for (deletes) |key| {
            try mutations.append(self.owned, .{ .family = .graph, .key = try self.owned.dupe(u8, key), .value = null, .source_index = self.source_index });
            try conditions.append(self.owned, try self.guard(key));
        }
        std.mem.sort(publication.Mutation, mutations.items, {}, struct {
            fn less(_: void, a: publication.Mutation, b: publication.Mutation) bool {
                return std.mem.order(u8, a.key, b.key) == .lt;
            }
        }.less);
        std.mem.sort(publication.ArtifactSource, conditions.items, {}, guardLess);
        command.mutations = mutations.items;
        command.mutation_preconditions = conditions.items;
        command.publication_digest = command.digest();
        try command.validate(self.alloc);
        try self.commands.append(self.owned, command);
    }
};

fn guardLess(_: void, a: publication.ArtifactSource, b: publication.ArtifactSource) bool {
    return std.mem.order(u8, a.key, b.key) == .lt;
}
fn matches(key: []const u8, name: []const u8) bool {
    return keys.matchesGraphEdgeIndexName(key, name) or keys.matchesGraphAssetStateIndexName(key, name) or keys.matchesGraphEdgeContenderIndexName(key, name) or keys.matchesGraphGlobalEdgeContenderIndexName(key, name);
}
