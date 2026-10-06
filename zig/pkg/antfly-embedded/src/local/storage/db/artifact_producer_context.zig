// Copyright 2026 Antfly, Inc. SPDX-License-Identifier: Elastic-2.0
//! Owned causal inputs shared by asynchronous producers. Provider execution
//! retains no storage snapshot; ordered publication revalidates every input.
const std = @import("std");
const publication = @import("artifact_publication.zig");

pub const Token = struct {
    arena: std.heap.ArenaAllocator,
    namespace: publication.Namespace,
    epoch: u64,
    catalog_digest: publication.Digest,
    producer_name: []const u8,
    producer_kind: @FieldType(publication.Command, "producer_kind"),
    producer_generation: u64,
    artifact_name: []const u8,
    source: publication.Source,
    inherited_sources: std.ArrayList(publication.Source) = .empty,
    producer_scope_key: []const u8 = "",
    reads: std.ArrayList(publication.ArtifactSource) = .empty,
    preconditions: std.ArrayList(publication.ArtifactSource) = .empty,
    read_key_bytes: usize = 0,

    pub fn sources(self: *const Token) []const publication.Source {
        return if (self.inherited_sources.items.len == 0) (&self.source)[0..1] else self.inherited_sources.items;
    }

    fn sourceIndex(self: *const Token, document: []const u8) !u32 {
        const values = self.sources();
        var lower: usize = 0;
        var upper = values.len;
        while (lower < upper) {
            const middle = lower + (upper - lower) / 2;
            switch (std.mem.order(u8, values[middle].document_key, document)) {
                .lt => lower = middle + 1,
                .gt => upper = middle,
                .eq => return @intCast(middle),
            }
        }
        return error.ArtifactCatalogCorrupt;
    }

    pub fn inheritSources(self: *Token, incoming: []const publication.Source) !void {
        const owned = self.arena.allocator();
        const previous = self.sources();
        var cursor: usize = 0;
        var added: usize = 0;
        var added_bytes: usize = 0;
        for (incoming, 0..) |source, index| {
            if (index != 0 and std.mem.order(u8, incoming[index - 1].document_key, source.document_key) != .lt) return error.ArtifactCatalogCorrupt;
            while (cursor < previous.len and std.mem.order(u8, previous[cursor].document_key, source.document_key) == .lt) : (cursor += 1) {}
            if (cursor < previous.len and std.mem.eql(u8, previous[cursor].document_key, source.document_key)) {
                const current = previous[cursor];
                if (current.exists != source.exists or current.timestamp != source.timestamp or
                    !std.meta.eql(current.content_digest, source.content_digest) or !std.meta.eql(current.input_position, source.input_position)) return error.EnrichmentSourceChanged;
            } else {
                added += 1;
                added_bytes +|= source.document_key.len;
            }
        }
        if (added == 0) return;
        if (added > publication.max_source_documents - previous.len or self.read_key_bytes +| added_bytes > 1024 * 1024) return error.ResourceBudgetExceeded;
        const merged = try owned.alloc(publication.Source, previous.len + added);
        const ordinals = try owned.alloc(u32, previous.len);
        var old_index: usize = 0;
        var new_index: usize = 0;
        var output: usize = 0;
        while (old_index < previous.len or new_index < incoming.len) {
            if (old_index < previous.len and (new_index == incoming.len or std.mem.order(u8, previous[old_index].document_key, incoming[new_index].document_key) != .gt)) {
                merged[output] = previous[old_index];
                ordinals[old_index] = @intCast(output);
                if (new_index < incoming.len and std.mem.eql(u8, previous[old_index].document_key, incoming[new_index].document_key)) new_index += 1;
                old_index += 1;
            } else {
                merged[output] = incoming[new_index];
                merged[output].document_key = try owned.dupe(u8, incoming[new_index].document_key);
                new_index += 1;
            }
            output += 1;
        }
        // Merge and remap once per proof, rather than shifting every guard
        // once for each inherited neighbor. Repeated proofs allocate nothing.
        for (self.reads.items) |*guard| guard.source_index = ordinals[guard.source_index];
        for (self.preconditions.items) |*guard| guard.source_index = ordinals[guard.source_index];
        self.inherited_sources = .{ .items = merged, .capacity = merged.len, .pointer_stability = .{} };
        self.read_key_bytes += added_bytes;
    }

    /// Retain the causal input set of an accepted upstream result, not just
    /// its current bytes. A neighbor update must invalidate downstream work
    /// even while the stale upstream artifact awaits regeneration.
    pub fn inheritProof(self: *Token, proof: @import("artifact_producer_provenance.zig").Proof) !void {
        try proof.validate();
        if (proof.authority_epoch != self.epoch or !std.mem.eql(u8, &proof.namespace, &self.namespace) or
            !std.mem.eql(u8, &proof.catalog_digest, &self.catalog_digest)) return error.ArtifactCatalogDrift;
        try self.inheritSources(proof.sources);
        for (proof.artifact_sources) |guard| {
            if (guard.source_index >= proof.sources.len) return error.ArtifactCatalogCorrupt;
            try self.observeDigest(false, guard.key, guard.content_digest, guard.input_position, try self.sourceIndex(proof.sources[guard.source_index].document_key));
        }
    }

    pub fn deinit(self: *Token) void {
        self.arena.deinit();
        self.* = undefined;
    }

    /// Current semantic acceptance, not a transport acknowledgement. Resolve
    /// the full retained causal proof before a duplicate provider invocation.
    pub fn accepted(self: *Token, alloc: std.mem.Allocator, txn: anytype) !bool {
        var proof = try self.acceptedProof(alloc, txn);
        if (proof) |*value| {
            defer value.deinit();
            return true;
        }
        return false;
    }

    /// Completion censuses need the full causal scope, not only a boolean.
    /// Returning ownership avoids resolving and decoding the same proof twice.
    pub fn acceptedProof(self: *Token, alloc: std.mem.Allocator, txn: anytype) !?@import("artifact_producer_provenance.zig").Accepted {
        const selector = try self.command(&.{});
        return @import("artifact_producer_provenance.zig").readCurrentForSource(alloc, txn, selector, self.source) catch |err| switch (err) {
            error.EnrichmentSourceChanged => return null,
            else => return err,
        };
    }

    /// Includes required absences (review overrides and sibling outputs).
    /// Seeing two versions of one input during computation forces a retry;
    /// publication must never bless a mixed-version provider result.
    pub fn observe(self: *Token, key: []const u8, raw: ?[]const u8, position: ?publication.Position) !void {
        return self.observeGuard(false, key, raw, position);
    }

    pub fn observePrecondition(self: *Token, key: []const u8, raw: ?[]const u8, position: ?publication.Position) !void {
        return self.observeGuard(true, key, raw, position);
    }

    fn observeGuard(self: *Token, precondition: bool, key: []const u8, raw: ?[]const u8, position: ?publication.Position) !void {
        var digest: ?publication.Digest = null;
        if (raw) |value| {
            var raw_digest: publication.Digest = undefined;
            std.crypto.hash.sha2.Sha256.hash(value, &raw_digest, .{});
            digest = raw_digest;
        }
        return self.observeDigest(precondition, key, digest, position, try self.sourceIndex(self.source.document_key));
    }

    fn observeDigest(self: *Token, precondition: bool, key: []const u8, digest: ?publication.Digest, position: ?publication.Position, source_index: u32) !void {
        const guards = if (precondition) &self.preconditions else &self.reads;
        var insertion: usize = guards.items.len;
        for (guards.items, 0..) |existing, i| {
            const order = std.mem.order(u8, existing.key, key);
            if (order == .lt) continue;
            if (order == .gt) {
                insertion = i;
                break;
            }
            if (existing.source_index != source_index or !std.meta.eql(existing.content_digest, digest) or !std.meta.eql(existing.input_position, position))
                return error.EnrichmentSourceChanged;
            return;
        }
        if (self.reads.items.len + self.preconditions.items.len >= publication.max_source_documents or key.len > 1024 * 1024 or
            self.read_key_bytes +| key.len > 1024 * 1024) return error.ResourceBudgetExceeded;
        const owned = self.arena.allocator();
        try guards.insert(owned, insertion, .{ .key = try owned.dupe(u8, key), .content_digest = digest, .input_position = position, .source_index = source_index });
        self.read_key_bytes += key.len;
    }

    pub fn command(self: *Token, mutations: []const publication.Mutation) !publication.Command {
        const effects = try self.arena.allocator().dupe(publication.Mutation, mutations);
        for (effects) |*effect| {
            if (effect.source_index != 0) return error.InvalidBatchRequest;
            effect.source_index = try self.sourceIndex(self.source.document_key);
        }
        var result: publication.Command = .{
            .namespace = self.namespace,
            .authority_epoch = self.epoch,
            .catalog_digest = self.catalog_digest,
            .producer_name = self.producer_name,
            .producer_kind = self.producer_kind,
            .producer_generation = self.producer_generation,
            .producer_artifact_name = self.artifact_name,
            .producer_scope_key = self.producer_scope_key,
            .sources = self.sources(),
            .artifact_sources = self.reads.items,
            .mutation_preconditions = self.preconditions.items,
            .mutations = effects,
            .publication_digest = @splat(0),
        };
        result.publication_digest = result.digest();
        return result;
    }

    pub fn validate(self: *const Token, alloc: std.mem.Allocator, txn: anytype) !void {
        try self.validateInputs(alloc, txn);
        try publication.validateArtifactSources(alloc, txn, self.namespace, self.sources(), self.preconditions.items);
    }

    pub fn validateInputs(self: *const Token, alloc: std.mem.Allocator, txn: anytype) !void {
        const current = (try publication.authority(txn)) orelse return error.ArtifactCatalogDrift;
        if (current.epoch != self.epoch or !std.mem.eql(u8, &current.namespace, &self.namespace) or
            !std.mem.eql(u8, &current.catalog_digest, &self.catalog_digest)) return error.ArtifactCatalogDrift;
        try publication.validateSources(alloc, txn, self.namespace, self.sources());
        try publication.validateArtifactSources(alloc, txn, self.namespace, self.sources(), self.reads.items);
    }
};
