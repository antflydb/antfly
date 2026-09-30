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

//! Receiver-local, bounded accumulation of ALL immutable plan requirements.
//! A verifier is trusted owner code, never a sender-supplied acceptance bit.
//! Ordered completion must prepare on each receiver and stage its own local
//! obligation revision; root identities and work revisions are not transferable.
const std = @import("std");
const publication = @import("artifact_publication.zig");
const obligations = @import("artifact_producer_obligations.zig");
const Observation = @import("artifact_stream_observation.zig").Observation;
const Plan = @import("artifact_completion_plan.zig").Plan;
const projection_epoch = @import("artifact_projection_epoch.zig");
const prefix = "\x00\x00__artifact_publication__:completion:";
pub const Key = [prefix.len + 80]u8;
const Encoded = [312]u8;

fn key(observation: Observation, root: u128) Key {
    var result: Key = undefined;
    @memcpy(result[0..prefix.len], prefix);
    @memcpy(result[prefix.len..][0..24], &observation.authority.namespace);
    std.mem.writeInt(u64, result[prefix.len + 24 ..][0..8], observation.authority.epoch, .big);
    std.mem.writeInt(u128, result[prefix.len + 32 ..][0..16], root, .big);
    @memcpy(result[prefix.len + 48 ..], &observation.document_digest);
    return result;
}

const State = struct {
    root: u128,
    plan: publication.Digest,
    total: u32,
    next: u32 = 0,
    work_revision: u64,
    work_position: ?publication.Position,
    observation: Observation,
    chain: publication.Digest = @splat(0),
    projection_epoch: ?u64 = null,
};

fn checksum(selected: *const Key, bytes: []const u8) publication.Digest {
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly:completion-progress:v1:");
    hash.update(selected);
    hash.update(bytes);
    var result: publication.Digest = undefined;
    hash.final(&result);
    return result;
}

fn encode(selected: *const Key, state: State) !Encoded {
    var raw: Encoded = @splat(0);
    @memcpy(raw[0..4], "ACP2");
    std.mem.writeInt(u128, raw[4..20], state.root, .little);
    @memcpy(raw[20..52], &state.plan);
    std.mem.writeInt(u32, raw[52..56], state.total, .little);
    std.mem.writeInt(u32, raw[56..60], state.next, .little);
    std.mem.writeInt(u64, raw[60..68], state.work_revision, .little);
    if (state.work_position) |position| @memcpy(raw[68..101], &try position.encode());
    @memcpy(raw[101..125], &state.observation.authority.namespace);
    std.mem.writeInt(u64, raw[125..133], state.observation.authority.epoch, .little);
    @memcpy(raw[133..165], &state.observation.authority.catalog_digest);
    @memcpy(raw[165..197], &state.observation.document_digest);
    if (state.observation.revision) |position| @memcpy(raw[197..230], &try position.encode());
    std.mem.writeInt(u64, raw[230..238], state.observation.validation_epoch, .little);
    raw[238] = @intFromBool(state.observation.foreign_inputs);
    @memcpy(raw[239..271], &state.chain);
    raw[271] = @intFromBool(state.projection_epoch != null);
    std.mem.writeInt(u64, raw[272..280], state.projection_epoch orelse 0, .little);
    @memcpy(raw[280..312], &checksum(selected, raw[0..280]));
    return raw;
}

fn decodePosition(raw: []const u8) !?publication.Position {
    return if (std.mem.allEqual(u8, raw, 0)) null else publication.Position.decode(raw) catch return error.ArtifactCatalogCorrupt;
}

fn decode(selected: *const Key, raw: []const u8) !State {
    if (raw.len != @sizeOf(Encoded) or !std.mem.eql(u8, raw[0..4], "ACP2") or raw[238] > 1 or raw[271] > 1 or
        (raw[271] == 0 and !std.mem.allEqual(u8, raw[272..280], 0)) or
        !std.mem.eql(u8, raw[280..312], &checksum(selected, raw[0..280]))) return error.ArtifactCatalogCorrupt;
    const state: State = .{
        .root = std.mem.readInt(u128, raw[4..20], .little),
        .plan = raw[20..52].*,
        .total = std.mem.readInt(u32, raw[52..56], .little),
        .next = std.mem.readInt(u32, raw[56..60], .little),
        .work_revision = std.mem.readInt(u64, raw[60..68], .little),
        .work_position = try decodePosition(raw[68..101]),
        .observation = .{ .authority = .{ .namespace = raw[101..125].*, .epoch = std.mem.readInt(u64, raw[125..133], .little), .catalog_digest = raw[133..165].* }, .document_digest = raw[165..197].*, .revision = try decodePosition(raw[197..230]), .validation_epoch = std.mem.readInt(u64, raw[230..238], .little), .foreign_inputs = raw[238] == 1 },
        .chain = raw[239..271].*,
        .projection_epoch = if (raw[271] == 1) std.mem.readInt(u64, raw[272..280], .little) else null,
    };
    // Only strict, nonempty prefixes are persisted. Terminal pages discharge
    // work and delete the checkpoint atomically; no stored EOF grants credit.
    if (state.root == 0 or state.total == 0 or state.next == 0 or state.next >= state.total or state.work_revision == 0 or
        state.observation.authority.epoch == 0 or
        !std.mem.eql(u8, selected, &key(state.observation, state.root))) return error.ArtifactCatalogCorrupt;
    const namespace = publication.namespaceFromBytes(state.observation.authority.namespace);
    if (state.work_position) |value| value.requireNamespace(namespace) catch return error.ArtifactCatalogCorrupt;
    if (state.observation.revision) |value| value.requireNamespace(namespace) catch return error.ArtifactCatalogCorrupt;
    return state;
}

fn load(txn: anytype, selected: *const Key) !?Encoded {
    const raw = txn.get(selected) catch |err| if (err == error.NotFound) return null else return err;
    _ = try decode(selected, raw);
    return raw[0..@sizeOf(Encoded)].*;
}

fn catalogStamp(txn: anytype) !?[40]u8 {
    const raw = txn.get(@import("artifact_inventory.zig").local_key) catch |err| if (err == error.NotFound) return null else return err;
    if (raw.len != 40) return error.ArtifactCatalogCorrupt;
    return raw[0..40].*;
}

pub const Limits = struct {
    visits: usize = 128,
    bytes: usize = 64 * 1024,
    time_budget_ns: ?u64 = 2 * std.time.ns_per_ms,
};

fn fingerprint(state: State) !publication.Digest {
    var portable = state;
    portable.root = 0;
    portable.work_revision = 0;
    portable.work_position = null;
    // Receivers can rebuild at different times. Each one fences its own
    // physical lifecycle without exporting that local counter as authority.
    if (portable.projection_epoch != null) portable.projection_epoch = 0;
    // Unrelated writes do not invalidate document-local evidence.
    if (!portable.observation.foreign_inputs) portable.observation.validation_epoch = 0;
    const raw = try encode(&key(portable.observation, 0), portable);
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly:completion-page:v1:");
    hash.update(&raw);
    var result: publication.Digest = undefined;
    hash.final(&result);
    return result;
}

/// Bridge from immutable requirements to the stream verifiers. Unsupported
/// scopes remain pending; dispatch success never substitutes for a witness.
/// Other requirement classes must acquire their own verifier before this can
/// drain a production document's complete plan.
pub const StreamVerifier = struct {
    plan: *const @import("catalog/index_manager.zig").IndexManager.WritePlanSnapshot,
    blocked_unit: ?*const @import("artifact_completion_plan.zig").Node = null,
    const streams = @import("artifact_stream_progress.zig");
    pub const Witness = struct {
        root: u128,
        requirement: publication.Digest,
        observation: Observation,
        value: union(enum) { native: @import("artifact_native_stream.zig").Closure, document: streams.DocumentClosure, extraction: streams.ExtractionClosure, chunks: streams.Closure, units: @import("artifact_unit_progress.zig").Closure, projection: @import("artifact_projection_certificate.zig").Closure },
        pub fn deinit(self: *@This()) void {
            switch (self.value) {
                inline else => |*value| value.deinit(),
            }
            self.* = undefined;
        }
        pub fn requireCurrent(self: @This(), txn: anytype, root: u128) !void {
            switch (self.value) {
                inline else => |value| try value.requireCurrent(txn, root),
            }
        }
    };

    pub fn verify(self: *StreamVerifier, alloc: std.mem.Allocator, txn: anytype, root: u128, document: []const u8, node: *const @import("artifact_completion_plan.zig").Node) !?Witness {
        self.blocked_unit = null;
        if (node.kind == .unit_children) {
            try node.requireValidIdentity();
            const completion = if (self.plan.completion_plan) |*value| value else return error.ArtifactCatalogDrift;
            const current = try completion.unitChild(node.name);
            if (!std.mem.eql(u8, &current.id, &node.id)) return error.ArtifactCatalogDrift;
            const ordinal = current.parent_template orelse return null;
            if (ordinal >= self.plan.generated_templates.len) return error.ArtifactCatalogDrift;
            var parent = self.plan.generated_templates[ordinal];
            parent.doc_key = document;
            const session = @import("artifact_chunk_publication.zig").unitVerificationSession(alloc, txn, parent, current.name, self.plan) catch |err| switch (err) {
                error.OnlineMergeArtifactTailsUnsupported, error.EnrichmentSourceChanged => return null,
                else => return err,
            };
            const closure = @import("artifact_unit_progress.zig").prepareClosure(alloc, root, session orelse return null) catch |err| switch (err) {
                error.ArtifactPublicationPending, error.EnrichmentSourceChanged => {
                    self.blocked_unit = current;
                    return null;
                },
                else => return err,
            };
            return .{ .root = root, .requirement = current.id, .observation = closure.observation, .value = .{ .units = closure } };
        }
        if (node.kind == .index_projection) {
            const closure = @import("artifact_projection_certificate.zig").prepareClosure(alloc, txn, root, document, node) catch |err| switch (err) {
                error.ArtifactPublicationPending, error.EnrichmentSourceChanged => return null,
                else => return err,
            };
            return .{ .root = root, .requirement = closure.requirement, .observation = closure.observation, .value = .{ .projection = closure } };
        }
        if (node.kind == .native_effects) {
            const closure = @import("artifact_native_stream.zig").prepareClosure(alloc, txn, root, document, self.plan) catch |err| switch (err) {
                error.ArtifactPublicationPending, error.EnrichmentSourceChanged => return null,
                else => return err,
            };
            return .{ .root = root, .requirement = closure.requirement, .observation = closure.observation, .value = .{ .native = closure } };
        }
        if (node.kind != .generated) return null;
        const ordinal = node.template orelse return error.ArtifactCatalogDrift;
        if (ordinal >= self.plan.generated_templates.len) return error.ArtifactCatalogDrift;
        var request = self.plan.generated_templates[ordinal];
        request.doc_key = document;
        switch (node.scope) {
            .producer_defined => {
                const closure = streams.prepareExtractionClosure(alloc, txn, root, request, self.plan) catch |err| switch (err) {
                    error.ArtifactPublicationPending, error.ArtifactCoverageBaselinePending, error.EnrichmentSourceChanged => return null,
                    else => return err,
                };
                return .{ .root = root, .requirement = closure.requirement, .observation = closure.observation, .value = .{ .extraction = closure } };
            },
            .document => {
                const closure = (if (request.kind == .asset or request.kind == .chunk_text)
                    streams.prepareEnrichmentClosure(alloc, txn, root, request, self.plan)
                else
                    streams.prepareDocumentClosure(alloc, txn, root, request, self.plan)) catch |err| switch (err) {
                    error.ArtifactPublicationPending, error.OnlineMergeArtifactTailsUnsupported, error.EnrichmentSourceChanged => return null,
                    else => return err,
                };
                return .{ .root = root, .requirement = closure.requirement, .observation = closure.observation, .value = .{ .document = closure } };
            },
            .materialized_chunks => {
                const closure = streams.prepareClosure(alloc, txn, root, request, self.plan) catch |err| switch (err) {
                    error.ArtifactPublicationPending, error.OnlineMergeArtifactTailsUnsupported, error.EnrichmentSourceChanged => return null,
                    else => return err,
                };
                return .{ .root = root, .requirement = closure.requirement, .observation = closure.observation, .value = .{ .chunks = closure } };
            },
            else => return null,
        }
    }
};

/// Witness supplies requirement, observation, root, requireCurrent and deinit.
/// Preparation performs expensive proof validation outside the writer; stage
/// repeats only bounded witness point fences and the local obligation CAS.
pub fn Prepared(comptime Witness: type) type {
    return struct {
        arena: std.heap.ArenaAllocator,
        document: []const u8,
        selected: Key,
        expected: ?Encoded,
        catalog_stamp: ?[40]u8,
        state: State,
        initial: State,
        limit_bytes: u32,
        encoded: Encoded,
        witnesses: []Witness,
        pub fn deinit(self: *@This()) void {
            for (self.witnesses) |*witness| witness.deinit();
            self.arena.deinit();
            self.* = undefined;
        }
        pub fn atEnd(self: *const @This()) bool {
            return self.state.next == self.state.total;
        }
        pub fn verified(self: *const @This()) usize {
            return self.witnesses.len;
        }
        pub fn command(self: *const @This()) !publication.Command {
            const authority = self.state.observation.authority;
            var result: publication.Command = .{ .mode = .complete_streams, .namespace = authority.namespace, .authority_epoch = authority.epoch, .catalog_digest = authority.catalog_digest, .producer_name = "", .producer_generation = 0, .sources = &.{}, .mutations = &.{}, .publication_digest = @splat(0), .completion = .{ .document_key = self.document, .visits = @intCast(self.verified()), .bytes = self.limit_bytes, .before = try fingerprint(self.initial), .after = try fingerprint(self.state) } };
            result.publication_digest = result.digest();
            return result;
        }
        /// Must run under the owner's ordered catalog/apply fence. All local
        /// evidence, checkpoint advance, and final discharge share this writer.
        pub fn stage(self: *const @This(), txn: anytype, root: u128, plan: *const Plan) !bool {
            if (root != self.state.root or root == 0) return error.DurableRootIncarnationUnavailable;
            if (!std.mem.eql(u8, &plan.digest, &self.state.plan) or plan.nodes.len != self.state.total) return error.ArtifactCatalogDrift;
            return self.stageCurrent(txn, root);
        }

        /// Ordered receiver preparation authenticates its pinned plan. The
        /// fixed catalog stamp binds that exact plan through this final writer.
        pub fn stageCurrent(self: *const @This(), txn: anytype, root: u128) !bool {
            if (root != self.state.root or root == 0) return error.DurableRootIncarnationUnavailable;
            // Every catalog mutation updates this fixed-width stamp in its
            // transaction. Recheck the prepared catalog without rehashing or
            // decoding its full definitions under the serialized writer.
            if (!std.meta.eql(self.catalog_stamp, try catalogStamp(txn))) return error.ArtifactCatalogDrift;
            if (self.state.projection_epoch) |epoch| try projection_epoch.requireCurrent(txn, epoch);
            try self.state.observation.requireCurrent(txn, self.document);
            const current = (try obligations.lookupWork(self.arena.child_allocator, txn, self.state.observation.authority, self.document)) orelse return error.EnrichmentSourceChanged;
            if (current.revision != self.state.work_revision or !std.meta.eql(current.position, self.state.work_position)) return error.EnrichmentSourceChanged;
            if (!std.meta.eql(try load(txn, &self.selected), self.expected)) return error.EnrichmentSourceChanged;
            const Probe = struct {
                parent: @TypeOf(txn),
                pub fn get(reader: *@This(), selected: []const u8) ![]const u8 {
                    return reader.parent.get(selected);
                }
            };
            var probe: Probe = .{ .parent = txn };
            for (self.witnesses) |witness| try witness.requireCurrent(&probe, root);
            if (self.atEnd()) {
                try obligations.complete(self.arena.child_allocator, txn, self.state.observation.authority, current);
                if (self.expected != null) try txn.delete(&self.selected);
                return true;
            }
            try txn.put(&self.selected, &self.encoded);
            return false;
        }
    };
}

/// Trusted verifier must visit the actual canonical node and return its owned
/// witness, or null while that scope is pending. No node can be skipped, and
/// zero provider templates cannot turn the mandatory native node into success.
const Start = struct { state: State, selected: Key, expected: ?Encoded };

fn start(alloc: std.mem.Allocator, txn: anytype, root: u128, plan: *const Plan, document: []const u8, limits: Limits) !?Start {
    if (root == 0) return error.DurableRootIncarnationUnavailable;
    if (document.len == 0 or document.len > obligations.max_cursor_bytes or plan.nodes.len == 0 or plan.nodes.len > std.math.maxInt(u32) or limits.visits == 0 or limits.visits > 128 or limits.bytes == 0 or limits.bytes > 64 * 1024) return error.InvalidBatchRequest;
    const observation = try Observation.capture(txn, document);
    const item = (try obligations.lookupWork(alloc, txn, observation.authority, document)) orelse return null;
    const selected = key(observation, root);
    const expected = try load(txn, &selected);
    var state: State = .{ .root = root, .plan = plan.digest, .total = @intCast(plan.nodes.len), .work_revision = item.revision, .work_position = item.position, .observation = observation, .projection_epoch = if (plan.has_projection_requirements) try projection_epoch.load(txn) else null };
    if (expected) |raw| {
        const previous = try decode(&selected, &raw);
        if (std.mem.eql(u8, &previous.plan, &plan.digest) and previous.total == plan.nodes.len and previous.work_revision == item.revision and std.meta.eql(previous.work_position, item.position) and std.meta.eql(previous.projection_epoch, state.projection_epoch)) {
            previous.observation.requireCurrent(txn, document) catch |err| switch (err) {
                error.EnrichmentSourceChanged => {},
                else => return err,
            };
            if (std.meta.eql(previous.observation.revision, observation.revision) and
                (!previous.observation.foreign_inputs or previous.observation.validation_epoch == observation.validation_epoch)) state = previous;
        }
    }
    return .{ .state = state, .selected = selected, .expected = expected };
}

pub fn prepare(alloc: std.mem.Allocator, txn: anytype, root: u128, plan: *const Plan, document: []const u8, verifier: anytype, limits: Limits) !?Prepared(@TypeOf(verifier.*).Witness) {
    const Witness = @TypeOf(verifier.*).Witness;
    const initial_state = (try start(alloc, txn, root, plan, document, limits)) orelse return null;
    var state = initial_state.state;
    const selected = initial_state.selected;
    const expected = initial_state.expected;
    const catalog_stamp = try catalogStamp(txn);
    var arena = std.heap.ArenaAllocator.init(alloc);
    errdefer arena.deinit();
    const owned = arena.allocator();
    const initial = state;
    var witnesses: std.ArrayList(Witness) = .empty;
    errdefer for (witnesses.items) |*witness| witness.deinit();
    const deadline = if (limits.time_budget_ns) |budget| @import("antfly_platform").time.monotonicNs() +| budget else std.math.maxInt(u64);
    var bytes: usize = 0;
    while (state.next < state.total) {
        const node = &plan.nodes[state.next];
        const cost = node.byteCost() +| document.len +| @sizeOf(Witness);
        if (cost > publication.max_payload_bytes) return error.ResourceBudgetExceeded;
        if (witnesses.items.len != 0 and (witnesses.items.len >= limits.visits or bytes +| cost > limits.bytes or @import("antfly_platform").time.monotonicNs() >= deadline)) break;
        var witness = (try verifier.verify(alloc, txn, root, document, node)) orelse break;
        errdefer witness.deinit();
        if (witness.root != root or !std.mem.eql(u8, &witness.requirement, &node.id)) return error.ArtifactCatalogDrift;
        try state.observation.merge(witness.observation);
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("antfly:completion-requirement:v1:");
        hash.update(&state.chain);
        hash.update(&node.id);
        hash.final(&state.chain);
        try witnesses.append(owned, witness);
        state.next += 1;
        bytes +|= cost;
    }
    if (witnesses.items.len == 0 and state.next != state.total) {
        arena.deinit();
        return null;
    }
    const name = try owned.dupe(u8, document);
    const encoded = try encode(&selected, state);
    return .{ .arena = arena, .document = name, .selected = selected, .expected = expected, .catalog_stamp = catalog_stamp, .state = state, .initial = initial, .limit_bytes = @intCast(limits.bytes), .encoded = encoded, .witnesses = witnesses.items };
}

pub fn discover(alloc: std.mem.Allocator, txn: anytype, root: u128, document: []const u8, snapshot: *const @import("catalog/index_manager.zig").IndexManager.WritePlanSnapshot, limits: Limits) !?Prepared(StreamVerifier.Witness) {
    const active = (try publication.authority(txn)) orelse return error.ArtifactCatalogDrift;
    const catalogs = try @import("artifact_inventory.zig").catalogs(txn);
    if (!snapshot.matchesArtifactInventory(catalogs) or !std.mem.eql(u8, &catalogs.digest(), &active.catalog_digest)) return error.ArtifactCatalogDrift;
    const plan = if (snapshot.completion_plan) |*value| value else return error.ArtifactCatalogDrift;
    var verifier: StreamVerifier = .{ .plan = snapshot };
    return prepare(alloc, txn, root, plan, document, &verifier, limits);
}

/// An owned, bounded scheduler proposal. Admission is not an acknowledgement:
/// only receiver-side apply can advance either durable checkpoint. The result
/// retains no read snapshot or catalog lease while waiting for queue capacity.
pub const NextControl = union(enum) {
    completion: Prepared(StreamVerifier.Witness),
    units: struct { page: @import("artifact_unit_progress.zig").Prepared, child: []const u8 },

    pub fn deinit(self: *NextControl) void {
        switch (self.*) {
            .completion => |*value| value.deinit(),
            .units => |*value| value.page.deinit(),
        }
        self.* = undefined;
    }
    pub fn command(self: *const NextControl) !publication.Command {
        return switch (self.*) {
            .completion => |*value| try value.command(),
            .units => |*value| value.page.command(value.child),
        };
    }
};

/// Shared by the completion scheduler and a producer's post-acceptance path.
/// Never dispatches a provider or skips an unaccepted child. Unsupported/missing
/// parent execution remains pending, rather than producing empty completion.
pub fn discoverUnitControl(alloc: std.mem.Allocator, txn: anytype, root: u128, document: []const u8, node: *const @import("artifact_completion_plan.zig").Node, snapshot: *const @import("catalog/index_manager.zig").IndexManager.WritePlanSnapshot, limits: @import("artifact_stream_census.zig").Limits) !?NextControl {
    try limits.validate();
    try node.requireValidIdentity();
    if (node.kind != .unit_children) return error.InvalidBatchRequest;
    const plan = if (snapshot.completion_plan) |*value| value else return error.ArtifactCatalogDrift;
    const current = try plan.unitChild(node.name);
    if (!std.mem.eql(u8, &node.id, &current.id)) return error.ArtifactCatalogDrift;
    const ordinal = current.parent_template orelse return null;
    if (ordinal >= snapshot.generated_templates.len) return error.ArtifactCatalogDrift;
    var parent = snapshot.generated_templates[ordinal];
    parent.doc_key = document;
    const session = @import("artifact_chunk_publication.zig").unitVerificationSession(alloc, txn, parent, current.name, snapshot) catch |err| switch (err) {
        error.OnlineMergeArtifactTailsUnsupported, error.EnrichmentSourceChanged => return null,
        else => return err,
    };
    var page = (@import("artifact_unit_progress.zig").prepare(alloc, root, session orelse return null, limits, null) catch |err| switch (err) {
        error.ArtifactPublicationPending, error.ArtifactCoverageBaselinePending, error.EnrichmentSourceChanged, error.OnlineMergeArtifactTailsUnsupported => return null,
        else => return err,
    }) orelse return null;
    errdefer page.deinit();
    const child = try page.arena.allocator().dupe(u8, current.name);
    return .{ .units = .{ .page = page, .child = child } };
}

/// Follow the durable completion prefix, not a fresh scan of the first catalog
/// page. Commit any verified prefix before advancing its first blocked unit
/// requirement; this preserves fairness for catalogs larger than one page.
pub fn discoverNextControl(alloc: std.mem.Allocator, txn: anytype, root: u128, document: []const u8, snapshot: *const @import("catalog/index_manager.zig").IndexManager.WritePlanSnapshot, limits: Limits) !?NextControl {
    var blocked: ?*const @import("artifact_completion_plan.zig").Node = null;
    return discoverNextControlOrBlocked(alloc, txn, root, document, snapshot, limits, &blocked);
}

fn discoverNextControlOrBlocked(alloc: std.mem.Allocator, txn: anytype, root: u128, document: []const u8, snapshot: *const @import("catalog/index_manager.zig").IndexManager.WritePlanSnapshot, limits: Limits, blocked_unit: *?*const @import("artifact_completion_plan.zig").Node) !?NextControl {
    blocked_unit.* = null;
    const active = (try publication.authority(txn)) orelse return error.ArtifactCatalogDrift;
    const catalogs = try @import("artifact_inventory.zig").catalogs(txn);
    if (!snapshot.matchesArtifactInventory(catalogs) or !std.mem.eql(u8, &catalogs.digest(), &active.catalog_digest)) return error.ArtifactCatalogDrift;
    const plan = if (snapshot.completion_plan) |*value| value else return error.ArtifactCatalogDrift;
    var verifier: StreamVerifier = .{ .plan = snapshot };
    if (try prepare(alloc, txn, root, plan, document, &verifier, limits)) |value| return .{ .completion = value };
    const blocked = verifier.blocked_unit orelse return null;
    blocked_unit.* = blocked;
    return discoverUnitControl(alloc, txn, root, document, blocked, snapshot, .{ .visits = limits.visits, .bytes = limits.bytes });
}

pub const NextAction = union(enum) {
    control: NextControl,
    jobs: @import("artifact_unit_dispatch.zig").Page,

    pub fn deinit(self: *NextAction) void {
        switch (self.*) {
            inline else => |*value| value.deinit(),
        }
        self.* = undefined;
    }
};

/// A missing child receipt schedules durable scoped work; it is not a control
/// command or completion claim. Both paths follow the same blocked catalog
/// prefix, so large catalogs do not restart discovery at their first child.
pub fn discoverNextAction(alloc: std.mem.Allocator, txn: anytype, root: u128, document: []const u8, snapshot: *const @import("catalog/index_manager.zig").IndexManager.WritePlanSnapshot, limits: Limits) !?NextAction {
    var blocked: ?*const @import("artifact_completion_plan.zig").Node = null;
    if (try discoverNextControlOrBlocked(alloc, txn, root, document, snapshot, limits, &blocked)) |control| return .{ .control = control };
    const child = blocked orelse return null;
    const page = @import("artifact_unit_jobs.zig").discover(alloc, txn, root, document, child.name, snapshot, .{ .visits = limits.visits, .bytes = limits.bytes }) catch |err| switch (err) {
        error.ArtifactPublicationPending, error.ArtifactCoverageBaselinePending, error.EnrichmentSourceChanged, error.OnlineMergeArtifactTailsUnsupported => return null,
        else => return err,
    };
    return .{ .jobs = page };
}

/// Refresh only projections in the next bounded completion page. The shared
/// resume calculation prevents large catalogs from starving requirements past
/// the first page. Cached coverage uses point probes; physical preparation
/// shares one sidecar lease and never runs inside a primary write transaction.
pub fn refreshProjections(alloc: std.mem.Allocator, store: anytype, manager: *@import("catalog/index_manager.zig").IndexManager, checkpoint_path: ?[]const u8, root: u128, document: []const u8, snapshot: *const @import("catalog/index_manager.zig").IndexManager.WritePlanSnapshot, limits: Limits) !void {
    return refreshProjectionsWithAdoption(alloc, store, manager, checkpoint_path, root, document, snapshot, limits, null);
}

/// Borrowed ordinals into the pinned plan. Admission happens after releasing
/// sidecar/physical leases, through the shared durable maintenance scheduler.
pub const AdoptionRequests = struct {
    ordinals: [128]u32 = undefined,
    count: usize = 0,
    authority: ?publication.Authority = null,

    fn add(self: *@This(), ordinal: u32) void {
        std.debug.assert(self.count < self.ordinals.len);
        self.ordinals[self.count] = ordinal;
        self.count += 1;
    }
};

pub fn refreshProjectionsWithAdoption(alloc: std.mem.Allocator, store: anytype, manager: *@import("catalog/index_manager.zig").IndexManager, checkpoint_path: ?[]const u8, root: u128, document: []const u8, snapshot: *const @import("catalog/index_manager.zig").IndexManager.WritePlanSnapshot, limits: Limits, adoption: ?*AdoptionRequests) !void {
    if (adoption) |requests| {
        requests.count = 0;
        requests.authority = null;
    }
    const certificates = @import("artifact_projection_certificate.zig");
    const plan = if (snapshot.completion_plan) |*value| value else return error.ArtifactCatalogDrift;
    if (!plan.has_projection_requirements) return;
    var pending: [128]u32 = undefined;
    var count: usize = 0;
    var authority: publication.Authority = undefined;
    var requires_baseline = false;
    {
        var read = try store.beginReadTxnWithBlockCacheAdmission(.transient);
        defer read.abort();
        const initial = (try start(alloc, &read, root, plan, document, limits)) orelse return;
        authority = initial.state.observation.authority;
        if (adoption) |requests| requests.authority = authority;
        const catalogs = try @import("artifact_inventory.zig").catalogs(&read);
        if (!snapshot.matchesArtifactInventory(catalogs) or !std.mem.eql(u8, &catalogs.digest(), &authority.catalog_digest)) return error.ArtifactCatalogDrift;
        const materialization = try publication.materializationState(&read, authority.namespace, document);
        const replay_sequence = if (materialization) |state| state.replay_sequence else null;
        requires_baseline = replay_sequence == null;
        const sequence = replay_sequence orelse (try @import("artifact_activation_boundary.zig").requireAuthority(&read, authority)).replay_sequence;
        var bytes: usize = 0;
        var visits: usize = 0;
        for (initial.state.next..plan.nodes.len) |ordinal| {
            const node = &plan.nodes[ordinal];
            const cost = node.byteCost() +| document.len +| @sizeOf(StreamVerifier.Witness);
            if (cost > publication.max_payload_bytes) return error.ResourceBudgetExceeded;
            if (visits != 0 and (visits >= limits.visits or bytes +| cost > limits.bytes)) break;
            visits += 1;
            bytes +|= cost;
            if (node.kind != .index_projection or node.index_kind != .full_text) continue;
            const cached = certificates.load(&read, &certificates.key(node.name)) catch |err| switch (err) {
                error.ArtifactCatalogCorrupt => null,
                else => return err,
            };
            if (cached) |value| {
                const current = blk: {
                    value.requireCurrent(&read, root, node.id, sequence) catch |err| switch (err) {
                        error.ArtifactPublicationPending, error.EnrichmentSourceChanged, error.ArtifactCatalogDrift => break :blk false,
                        else => return err,
                    };
                    if (replay_sequence == null) _ = value.requireBaseline(&read) catch |err| switch (err) {
                        error.ArtifactPublicationPending, error.EnrichmentSourceChanged, error.ArtifactCatalogDrift => break :blk false,
                        else => return err,
                    };
                    break :blk true;
                };
                if (current) continue;
            }
            pending[count] = @intCast(ordinal);
            count += 1;
        }
    }
    if (count == 0) return;
    var projection = (try @import("derived/apply_state.zig").tryAcquireProjectionSnapshot(alloc, manager.checkpointIo(), store, checkpoint_path)) orelse return;
    defer projection.deinit();
    if (!std.meta.eql(projection.authority, authority)) return error.ArtifactCatalogDrift;
    const deadline = if (limits.time_budget_ns) |budget| @import("antfly_platform").time.monotonicNs() +| budget else std.math.maxInt(u64);
    for (pending[0..count], 0..) |ordinal, visited| {
        if (visited != 0 and @import("antfly_platform").time.monotonicNs() >= deadline) break;
        const node = &plan.nodes[ordinal];
        var missing_seal = false;
        var guard = (try manager.tryValidateFullTextProjectionWithAdoptionHint(alloc, &projection, node.name, node.generation, &missing_seal)) orelse {
            if (missing_seal) if (adoption) |requests| requests.add(ordinal);
            continue;
        };
        defer guard.deinit();
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        const changed = try certificates.stageFullText(&txn, &guard, node);
        const certificate = (try certificates.load(&txn, &certificates.key(node.name))).?;
        const needs_adoption = requires_baseline and certificate.baseline == null;
        if (changed) try txn.commit() else txn.abort();
        if (needs_adoption) if (adoption) |requests| requests.add(ordinal);
    }
}

pub fn prepareCommand(alloc: std.mem.Allocator, txn: anytype, root: u128, command: publication.Command, snapshot: *const @import("catalog/index_manager.zig").IndexManager.WritePlanSnapshot) !Prepared(StreamVerifier.Witness) {
    try command.validate(alloc);
    if (command.mode != .complete_streams) return error.InvalidBatchRequest;
    const active = (try publication.authority(txn)) orelse return error.ArtifactCatalogDrift;
    if (active.epoch != command.authority_epoch or !std.mem.eql(u8, &active.namespace, &command.namespace) or !std.mem.eql(u8, &active.catalog_digest, &command.catalog_digest)) return error.ArtifactCatalogDrift;
    const claim = command.completion.?;
    var prepared = (try discover(alloc, txn, root, claim.document_key, snapshot, .{ .visits = claim.visits, .bytes = claim.bytes, .time_budget_ns = null })) orelse return error.ArtifactPublicationPending;
    errdefer prepared.deinit();
    const actual = (try prepared.command()).completion.?;
    if (actual.visits != claim.visits or !std.mem.eql(u8, &actual.before, &claim.before) or !std.mem.eql(u8, &actual.after, &claim.after)) return error.EnrichmentSourceChanged;
    return prepared;
}

pub fn collectObsoletePage(alloc: std.mem.Allocator, store: anytype, root: u128) !bool {
    if (root == 0) return true;
    var identity: [16]u8 = undefined;
    std.mem.writeInt(u128, &identity, root, .big);
    return obligations.collectObsoleteEpochPageForIdentity(alloc, store, prefix, 48, 48, &identity);
}

test "ordered artifact inventory projection completion reconstructs independent receiver evidence" {
    const alloc = std.testing.allocator;
    const db_mod = @import("db.zig");
    const certificates = @import("artifact_projection_certificate.zig");
    const native = @import("artifact_native_stream.zig");
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const source_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/projection-source", .{tmp.sub_path});
    defer alloc.free(source_path);
    const target_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/projection-target", .{tmp.sub_path});
    defer alloc.free(target_path);
    const options: db_mod.OpenOptions = .{ .identity_namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 3 }, .online_source_authority = .raft, .primary_backend = .{ .lsm = .{} }, .index_backends = .{ .text_main_backend = .lsm }, .start_index_workers = false, .start_optional_runtimes = false };
    var source = try db_mod.DB.open(alloc, source_path, options);
    defer source.close();
    var target = try db_mod.DB.open(alloc, target_path, options);
    defer target.close();
    for ([_]*db_mod.DB{ &source, &target }) |db| {
        try db.setSchemaJson(alloc, "{}");
        // Replicas share a catalog generation, not independently generated
        // local DDL identities. Their physical root identities still differ.
        try db.addIndex(.{ .name = "text", .kind = .full_text, .config_json = "{}", .coverage_generation = 7 });
    }
    var catalog = try source.artifactInventoryCommand(alloc);
    defer catalog.catalogs.deinit(alloc);
    catalog.binding.effect_protocol = 15;
    var activation: publication.Command = .{ .mode = .activate, .namespace = catalog.namespace, .authority_epoch = catalog.binding.epoch, .catalog_digest = catalog.binding.digest, .producer_name = "", .producer_generation = 0, .sources = &.{}, .mutations = &.{}, .publication_digest = @splat(0) };
    activation.publication_digest = activation.digest();
    for ([_]*db_mod.DB{ &source, &target }) |db| {
        try db.batchRaftReplicatedApply(.{ .artifact_catalog = catalog }, .{ .term = 1, .index = 1 });
        try db.batchRaftReplicatedApply(.{ .artifact_publication = activation }, .{ .term = 1, .index = 2 });
        try db.batchRaftReplicatedApply(.{ .writes = &.{.{ .key = "doc", .value = "{\"body\":\"independent replica\"}" }}, .sync_level = .full_index, .timestamp_ns = 100 }, .{ .term = 1, .index = 3 });
        try db.runUntilIdle();
        var pin = try db.core.index_manager.acquireWritePlanSnapshot();
        defer pin.release();
        for (0..16) |_| {
            if (try native.advance(alloc, db.core.store, db.root_incarnation, "doc", pin.plan()) == .closed) break;
        } else return error.TestExpectedNativeClosure;
    }
    try std.testing.expect(source.root_incarnation != target.root_incarnation);
    var pin = try source.core.index_manager.acquireWritePlanSnapshot();
    defer pin.release();
    try refreshProjections(alloc, source.core.store, source.core.index_manager, source.core.applied_sequence_checkpoint_path, source.root_incarnation, "doc", pin.plan(), .{ .time_budget_ns = null });
    var prepared = blk: {
        var read = try source.core.store.beginReadTxn();
        defer read.abort();
        break :blk (try discover(alloc, &read, source.root_incarnation, "doc", pin.plan(), .{ .time_budget_ns = null })).?;
    };
    defer prepared.deinit();
    try std.testing.expect(prepared.atEnd());
    {
        // Even copying a genuine leader certificate cannot authorize another
        // physical root. Ordered preparation must replace it using local data.
        var read = try source.core.store.beginReadTxn();
        defer read.abort();
        var txn = try target.core.store.beginWriteTxn();
        errdefer txn.abort();
        const selected = certificates.key("text");
        try txn.put(&selected, try read.get(&selected));
        var receiver_pin = try target.core.index_manager.acquireWritePlanSnapshot();
        defer receiver_pin.release();
        const node = for (receiver_pin.plan().completion_plan.?.nodes) |*candidate| {
            if (candidate.kind == .index_projection) break candidate;
        } else return error.TestExpectedProjectionRequirement;
        try std.testing.expectError(error.ArtifactPublicationPending, certificates.prepareClosure(alloc, &txn, target.root_incarnation, "doc", node));
        try txn.commit();
    }
    const command = try prepared.command();
    try target.batchRaftReplicatedApply(.{ .artifact_publication = command }, .{ .term = 1, .index = 4 });
    // Lost replies retry the same ordered entry without reopening obligations.
    try target.batchRaftReplicatedApply(.{ .artifact_publication = command }, .{ .term = 1, .index = 4 });
    var read = try target.core.store.beginReadTxn();
    defer read.abort();
    try std.testing.expectEqual(target.root_incarnation, (try certificates.load(&read, &certificates.key("text"))).?.root);
    try std.testing.expectEqual(@as(u64, 0), (try obligations.load(&read)).?.pending_documents);
}

test "ordered artifact inventory completion control verifies independent roots before atomic discharge" {
    const alloc = std.testing.allocator;
    const db_mod = @import("db.zig");
    const native = @import("artifact_native_stream.zig");
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const source_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/completion-source", .{tmp.sub_path});
    defer alloc.free(source_path);
    const target_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/completion-target", .{tmp.sub_path});
    defer alloc.free(target_path);
    const options: db_mod.OpenOptions = .{ .identity_namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 3 }, .online_source_authority = .raft, .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false };
    var source = try db_mod.DB.open(alloc, source_path, options);
    defer source.close();
    var target = try db_mod.DB.open(alloc, target_path, options);
    defer target.close();
    try std.testing.expect(source.root_incarnation != target.root_incarnation);
    try source.setSchemaJson(alloc, "{}");
    try target.setSchemaJson(alloc, "{}");
    var catalog = try source.artifactInventoryCommand(alloc);
    defer catalog.catalogs.deinit(alloc);
    catalog.binding.effect_protocol = 15;
    var activation: publication.Command = .{ .mode = .activate, .namespace = catalog.namespace, .authority_epoch = catalog.binding.epoch, .catalog_digest = catalog.binding.digest, .producer_name = "", .producer_generation = 0, .sources = &.{}, .mutations = &.{}, .publication_digest = @splat(0) };
    activation.publication_digest = activation.digest();
    for ([_]*db_mod.DB{ &source, &target }) |db| {
        try db.batchRaftReplicatedApply(.{ .artifact_catalog = catalog }, .{ .term = 1, .index = 1 });
        try db.batchRaftReplicatedApply(.{ .artifact_publication = activation }, .{ .term = 1, .index = 2 });
        try db.batchRaftReplicatedApply(.{ .writes = &.{.{ .key = "doc", .value = "{}" }}, .timestamp_ns = 100 }, .{ .term = 1, .index = 3 });
    }
    var pin = try source.core.index_manager.acquireWritePlanSnapshot();
    defer pin.release();
    try std.testing.expectEqual(.closed, try native.advance(alloc, source.core.store, source.root_incarnation, "doc", pin.plan()));
    var prepared = blk: {
        var read = try source.core.store.beginReadTxn();
        defer read.abort();
        break :blk (try discover(alloc, &read, source.root_incarnation, "doc", pin.plan(), .{})).?;
    };
    defer prepared.deinit();
    const command = try prepared.command();
    try command.validate(alloc);
    try std.testing.expect(prepared.atEnd());
    {
        var receiver_plan = try target.core.index_manager.acquireWritePlanSnapshot();
        defer receiver_plan.release();
        var read = try target.core.store.beginReadTxn();
        defer read.abort();
        // The sender's proof is not a completion certificate on another root.
        try std.testing.expectError(error.ArtifactPublicationPending, prepareCommand(alloc, &read, target.root_incarnation, command, receiver_plan.plan()));
    }
    {
        var txn = try source.core.store.beginWriteTxn();
        defer txn.abort();
        try std.testing.expectError(error.DurableRootIncarnationUnavailable, prepared.stageCurrent(&txn, target.root_incarnation));
        try std.testing.expect(try prepared.stageCurrent(&txn, source.root_incarnation));
        try std.testing.expectEqual(@as(u64, 0), (try obligations.load(&txn)).?.pending_documents);
    }
    {
        var read = try source.core.store.beginReadTxn();
        defer read.abort();
        try std.testing.expectEqual(@as(u64, 1), (try obligations.load(&read)).?.pending_documents);
    }
    {
        var receiver_plan = try target.core.index_manager.acquireWritePlanSnapshot();
        defer receiver_plan.release();
        try std.testing.expectEqual(.closed, try native.advance(alloc, target.core.store, target.root_incarnation, "doc", receiver_plan.plan()));
        // Receiver-local work revisions deliberately need not match the sender.
        var txn = try target.core.store.beginWriteTxn();
        errdefer txn.abort();
        const active = (try publication.authority(&txn)).?;
        const work = (try obligations.lookupWork(alloc, &txn, active, "doc")).?;
        _ = try obligations.mark(alloc, &txn, active, "doc", work.position);
        try txn.commit();
    }
    target.close();
    target = try db_mod.DB.open(alloc, target_path, options);
    {
        var receiver_plan = try target.core.index_manager.acquireWritePlanSnapshot();
        defer receiver_plan.release();
        var read = try target.core.store.beginReadTxn();
        defer read.abort();
        var receiver = try prepareCommand(alloc, &read, target.root_incarnation, command, receiver_plan.plan());
        defer receiver.deinit();
        try std.testing.expectEqualDeep(command, try receiver.command());
        const Harness = struct {
            fn run(a: std.mem.Allocator, txn: *@import("../docstore.zig").DocStore.Txn, root: u128, control: publication.Command, snapshot: *const @import("catalog/index_manager.zig").IndexManager.WritePlanSnapshot) !void {
                var value = try prepareCommand(a, txn, root, control, snapshot);
                defer value.deinit();
            }
        };
        try std.testing.checkAllAllocationFailures(alloc, Harness.run, .{ &read, target.root_incarnation, command, receiver_plan.plan() });
        var forged = command;
        forged.completion.?.after[0] ^= 1;
        forged.publication_digest = forged.digest();
        try std.testing.expectError(error.EnrichmentSourceChanged, prepareCommand(alloc, &read, target.root_incarnation, forged, receiver_plan.plan()));
    }
    for ([_]*db_mod.DB{ &source, &target }) |db| {
        try db.batchRaftReplicatedApply(.{ .artifact_publication = command }, .{ .term = 1, .index = 4 });
        // A lost completion reply must not double-decrement the work counter.
        try db.batchRaftReplicatedApply(.{ .artifact_publication = command }, .{ .term = 1, .index = 5 });
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        try std.testing.expectEqual(@as(u64, 0), (try obligations.load(&read)).?.pending_documents);
        try std.testing.expectEqual(null, try obligations.lookupWork(alloc, &read, (try publication.authority(&read)).?, "doc"));
    }
    // A new input reopens work. Old completion cannot discharge the new cut.
    try source.batchRaftReplicatedApply(.{ .writes = &.{.{ .key = "doc", .value = "{\"v\":2}" }}, .timestamp_ns = 101 }, .{ .term = 1, .index = 6 });
    try source.batchRaftReplicatedApply(.{ .artifact_publication = command }, .{ .term = 1, .index = 7 });
    {
        var read = try source.core.store.beginReadTxn();
        defer read.abort();
        try std.testing.expectEqual(@as(u64, 1), (try obligations.load(&read)).?.pending_documents);
    }
    const Queue = struct {
        bytes: ?[]u8 = null,
        fn enqueue(ptr: *anyopaque, _: publication.Namespace, bytes: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const owned = try std.testing.allocator.dupe(u8, bytes);
            if (self.bytes) |old| std.testing.allocator.free(old);
            self.bytes = owned;
        }
    };
    var queue: Queue = .{};
    defer if (queue.bytes) |bytes| alloc.free(bytes);
    source.artifact_publication_dispatcher = .{ .ptr = &queue, .enqueue = Queue.enqueue };
    defer source.artifact_publication_dispatcher = null;
    source.artifact_producer_work_retry_after_ns.store(0, .release);
    _ = try source.advanceArtifactProducerWorkPage();
    var decoded = try @import("artifact_publication_transport_codec.zig").decodeBorrowed(alloc, queue.bytes orelse return error.TestUnexpectedResult);
    defer decoded.deinit();
    try std.testing.expectEqual(.complete_streams, decoded.command.mode);
    try source.batchRaftReplicatedApply(.{ .artifact_publication = decoded.command }, .{ .term = 1, .index = 8 });
    var read = try source.core.store.beginReadTxn();
    defer read.abort();
    try std.testing.expectEqual(@as(u64, 0), (try obligations.load(&read)).?.pending_documents);
}

test "ordered artifact inventory completion leaves extraction-owned scope pending until certified" {
    const alloc = std.testing.allocator;
    const db_mod = @import("db.zig");
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/completion-pending-extraction", .{tmp.sub_path});
    defer alloc.free(path);
    var db = try db_mod.DB.open(alloc, path, .{ .identity_namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 4 }, .online_source_authority = .raft, .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false });
    defer db.close();
    try db.setSchemaJson(alloc, "{}");
    try db.addEnrichment(.{ .name = "units", .kind = .asset, .field = "url", .producer_json = "{\"type\":\"document_extraction\"}" });
    var catalog = try db.artifactInventoryCommand(alloc);
    defer catalog.catalogs.deinit(alloc);
    catalog.binding.effect_protocol = 15;
    try db.batchRaftReplicatedApply(.{ .artifact_catalog = catalog }, .{ .term = 1, .index = 1 });
    var activation: publication.Command = .{ .mode = .activate, .namespace = catalog.namespace, .authority_epoch = catalog.binding.epoch, .catalog_digest = catalog.binding.digest, .producer_name = "", .producer_generation = 0, .sources = &.{}, .mutations = &.{}, .publication_digest = @splat(0) };
    activation.publication_digest = activation.digest();
    try db.batchRaftReplicatedApply(.{ .artifact_publication = activation }, .{ .term = 1, .index = 2 });
    var pin = try db.core.index_manager.acquireWritePlanSnapshot();
    defer pin.release();
    const plan = if (pin.plan().completion_plan) |*value| value else return error.TestUnexpectedResult;
    const node = for (plan.nodes) |*candidate| {
        if (candidate.kind == .generated and candidate.scope == .producer_defined) break candidate;
    } else return error.TestUnexpectedResult;
    var read = try db.core.store.beginReadTxn();
    defer read.abort();
    var verifier: StreamVerifier = .{ .plan = pin.plan() };
    try std.testing.expectEqual(null, try verifier.verify(alloc, &read, db.root_incarnation, "doc", node));
}

test "ordered artifact inventory completion never skips an unverified index requirement" {
    const alloc = std.testing.allocator;
    const db_mod = @import("db.zig");
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/completion-pending-index", .{tmp.sub_path});
    defer alloc.free(path);
    var db = try db_mod.DB.open(alloc, path, .{ .identity_namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 3 }, .online_source_authority = .raft, .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false });
    defer db.close();
    try db.setSchemaJson(alloc, "{}");
    try db.addIndex(.{ .name = "text", .kind = .full_text, .config_json = "{}" });
    var catalog = try db.artifactInventoryCommand(alloc);
    defer catalog.catalogs.deinit(alloc);
    catalog.binding.effect_protocol = 15;
    try db.batchRaftReplicatedApply(.{ .artifact_catalog = catalog }, .{ .term = 1, .index = 1 });
    var activation: publication.Command = .{ .mode = .activate, .namespace = catalog.namespace, .authority_epoch = catalog.binding.epoch, .catalog_digest = catalog.binding.digest, .producer_name = "", .producer_generation = 0, .sources = &.{}, .mutations = &.{}, .publication_digest = @splat(0) };
    activation.publication_digest = activation.digest();
    try db.batchRaftReplicatedApply(.{ .artifact_publication = activation }, .{ .term = 1, .index = 2 });
    try db.batchRaftReplicatedApply(.{ .writes = &.{.{ .key = "doc", .value = "{\"body\":\"text\"}" }}, .timestamp_ns = 100 }, .{ .term = 1, .index = 3 });
    var pin = try db.core.index_manager.acquireWritePlanSnapshot();
    defer pin.release();
    try std.testing.expectEqual(.closed, try @import("artifact_native_stream.zig").advance(alloc, db.core.store, db.root_incarnation, "doc", pin.plan()));
    var stopped = false;
    for (0..3) |iteration| {
        var page = blk: {
            var read = try db.core.store.beginReadTxn();
            defer read.abort();
            break :blk (try discover(alloc, &read, db.root_incarnation, "doc", pin.plan(), .{})) orelse {
                stopped = true;
                break;
            };
        };
        defer page.deinit();
        try std.testing.expect(!page.atEnd());
        try db.batchRaftReplicatedApply(.{ .artifact_publication = try page.command() }, .{ .term = 1, .index = iteration + 4 });
    }
    try std.testing.expect(stopped);
    var read = try db.core.store.beginReadTxn();
    defer read.abort();
    try std.testing.expectEqual(@as(u64, 1), (try obligations.load(&read)).?.pending_documents);
}

test "ordered artifact inventory completion checkpoint authenticates only strict local prefixes" {
    const observation: Observation = .{ .authority = .{ .namespace = @splat(1), .epoch = 1, .catalog_digest = @splat(2) }, .document_digest = @splat(3), .revision = .{ .raft = .{ .term = 1, .index = 4 } }, .validation_epoch = 5 };
    const selected = key(observation, 41);
    const state: State = .{ .root = 41, .plan = @splat(6), .total = 3, .next = 1, .work_revision = 7, .work_position = observation.revision, .observation = observation, .chain = @splat(8) };
    var portable = state;
    portable.root = 42;
    portable.work_revision += 1;
    portable.work_position = null;
    portable.observation.validation_epoch += 1;
    try std.testing.expectEqualDeep(try fingerprint(state), try fingerprint(portable));
    portable.observation.foreign_inputs = true;
    try std.testing.expect(!std.mem.eql(u8, &try fingerprint(state), &try fingerprint(portable)));
    portable = state;
    portable.chain[0] ^= 1;
    try std.testing.expect(!std.mem.eql(u8, &try fingerprint(state), &try fingerprint(portable)));
    portable = state;
    portable.projection_epoch = 7;
    var rebuilt = portable;
    rebuilt.projection_epoch = 8;
    try std.testing.expectEqualDeep(try fingerprint(portable), try fingerprint(rebuilt));
    try std.testing.expect(!std.mem.eql(u8, &try fingerprint(state), &try fingerprint(portable)));
    const raw = try encode(&selected, state);
    try std.testing.expectEqualDeep(state, try decode(&selected, &raw));
    for (0..raw.len) |offset| {
        var changed = raw;
        changed[offset] ^= 1;
        try std.testing.expectError(error.ArtifactCatalogCorrupt, decode(&selected, &changed));
    }
    try std.testing.expectError(error.ArtifactCatalogCorrupt, decode(&key(observation, 42), &raw));
    var invalid = state;
    invalid.next = 0;
    try std.testing.expectError(error.ArtifactCatalogCorrupt, decode(&selected, &try encode(&selected, invalid)));
    invalid.next = invalid.total;
    try std.testing.expectError(error.ArtifactCatalogCorrupt, decode(&selected, &try encode(&selected, invalid)));
    invalid = state;
    invalid.work_position = .{ .native = .{ .namespace = .{ .table_id = 9, .shard_id = 9, .range_id = 9 }, .sequence = 1 } };
    try std.testing.expectError(error.ArtifactCatalogCorrupt, decode(&selected, &try encode(&selected, invalid)));
}

test "ordered artifact inventory completion checkpoint resumes all requirements and fences stale work" {
    const alloc = std.testing.allocator;
    const docstore = @import("../docstore.zig");
    const node_type = @import("artifact_completion_plan.zig").Node;
    // This tests the generic checkpoint engine, not producer activation. The
    // fixture verifier's private guard rows model already-verified witnesses.
    const Verifier = struct {
        calls: usize = 0,
        first: ?publication.Digest = null,
        const guard_prefix = "\x00\x00test_completion_guard:";
        fn guard(id: publication.Digest) [guard_prefix.len + 32]u8 {
            var result: [guard_prefix.len + 32]u8 = undefined;
            @memcpy(result[0..guard_prefix.len], guard_prefix);
            @memcpy(result[guard_prefix.len..], &id);
            return result;
        }
        const Witness = struct {
            root: u128,
            requirement: publication.Digest,
            observation: Observation,
            pub fn deinit(_: *@This()) void {}
            pub fn requireCurrent(self: @This(), txn: anytype, root: u128) !void {
                if (root != self.root) return error.DurableRootIncarnationUnavailable;
                try self.observation.requireCurrent(txn, "doc");
                const raw = txn.get(&guard(self.requirement)) catch |err| if (err == error.NotFound) return error.EnrichmentSourceChanged else return err;
                if (!std.mem.eql(u8, raw, &.{1})) return error.EnrichmentSourceChanged;
            }
        };
        pub fn verify(self: *@This(), _: std.mem.Allocator, txn: anytype, root: u128, document: []const u8, node: *const node_type) !?Witness {
            self.calls += 1;
            if (self.first == null) self.first = node.id;
            const raw = txn.get(&guard(node.id)) catch |err| if (err == error.NotFound) return null else return err;
            if (!std.mem.eql(u8, raw, &.{1})) return null;
            return .{ .root = root, .requirement = node.id, .observation = try Observation.capture(txn, document) };
        }
    };
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var path_buffer: [std.fs.max_path_bytes]u8 = undefined;
    const path_len = try tmp.dir.realPath(std.testing.io, &path_buffer);
    const path = try alloc.dupeZ(u8, path_buffer[0..path_len]);
    defer alloc.free(path);
    var store = try docstore.DocStore.open(alloc, path, .{});
    defer store.close();
    const authority: publication.Authority = .{ .namespace = @splat(7), .epoch = 1, .catalog_digest = @splat(3) };
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        try @import("../source_authority.zig").bind(&txn, .native, authority.namespace);
        try publication.stageAuthority(&txn, .{ .mode = .activate, .namespace = authority.namespace, .authority_epoch = 1, .catalog_digest = authority.catalog_digest, .producer_name = "", .producer_generation = 0, .sources = &.{}, .mutations = &.{}, .publication_digest = @splat(0) });
        try obligations.begin(alloc, &txn, authority);
        try @import("artifact_producer_validation.zig").begin(alloc, &txn, authority);
        try txn.commit();
    }
    const primary = try @import("../internal_keys.zig").documentKeyAlloc(alloc, "doc");
    defer alloc.free(primary);
    try store.put(primary, "{}");
    var nodes: [257]node_type = undefined;
    for (&nodes, 0..) |*node, ordinal| {
        var id: publication.Digest = @splat(0);
        std.mem.writeInt(u64, id[24..32], ordinal + 1, .big);
        node.* = .{ .id = id, .kind = @enumFromInt(ordinal % 5), .scope = .document, .name = "fixture" };
    }
    var plan: Plan = .{ .arena = std.heap.ArenaAllocator.init(alloc), .catalog = authority.catalog_digest, .digest = @splat(8), .nodes = &nodes, .providers = &.{}, .definitions = .empty, .has_projection_requirements = true };
    defer plan.deinit();
    var verifier: Verifier = .{};
    {
        var read = try store.beginReadTxn();
        defer read.abort();
        try std.testing.expect((try prepare(alloc, &read, 41, &plan, "doc", &verifier, .{})) == null);
        try std.testing.expectEqual(@as(u64, 1), (try obligations.load(&read)).?.pending_documents);
    }
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        for (nodes) |node| try txn.put(&Verifier.guard(node.id), &.{1});
        try txn.commit();
    }
    var first = blk: {
        var read = try store.beginReadTxn();
        defer read.abort();
        break :blk (try prepare(alloc, &read, 41, &plan, "doc", &verifier, .{ .visits = 17 })).?;
    };
    defer first.deinit();
    try std.testing.expect(first.verified() > 0 and first.verified() <= 17 and !first.atEnd());
    {
        var read = try store.beginReadTxn();
        defer read.abort();
        const Check = struct {
            fn run(a: std.mem.Allocator, txn: *@TypeOf(read), pinned: *const Plan, checker: *Verifier) !void {
                var page = (try prepare(a, txn, 41, pinned, "doc", checker, .{ .visits = 1, .bytes = 1 })).?;
                defer page.deinit();
                try std.testing.expectEqual(@as(usize, 1), page.verified());
            }
        };
        try std.testing.checkAllAllocationFailures(alloc, Check.run, .{ &read, &plan, &verifier });
    }
    {
        var txn = try store.beginWriteTxn();
        defer txn.abort();
        try std.testing.expectError(error.DurableRootIncarnationUnavailable, first.stage(&txn, 42, &plan));
        var changed_plan = plan;
        changed_plan.digest[0] ^= 1;
        try std.testing.expectError(error.ArtifactCatalogDrift, first.stage(&txn, 41, &changed_plan));
        try std.testing.expect(!try first.stage(&txn, 41, &plan));
    }
    {
        var txn = try store.beginWriteTxn();
        defer txn.abort();
        try txn.put(@import("artifact_inventory.zig").local_key, &@as([40]u8, @splat(0)));
        try std.testing.expectError(error.ArtifactCatalogDrift, first.stage(&txn, 41, &plan));
    }
    {
        var read = try store.beginReadTxn();
        defer read.abort();
        try std.testing.expect((try load(&read, &first.selected)) == null);
    }
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        try std.testing.expect(!try first.stage(&txn, 41, &plan));
        try txn.commit();
    }
    {
        var txn = try store.beginWriteTxn();
        defer txn.abort();
        try std.testing.expectError(error.EnrichmentSourceChanged, first.stage(&txn, 41, &plan));
    }
    store.close();
    store = try docstore.DocStore.open(alloc, path, .{});
    var resumed = blk: {
        var read = try store.beginReadTxn();
        defer read.abort();
        verifier.first = null;
        break :blk (try prepare(alloc, &read, 41, &plan, "doc", &verifier, .{ .visits = 17 })).?;
    };
    defer resumed.deinit();
    try std.testing.expectEqualDeep(nodes[first.state.next].id, verifier.first.?);
    {
        var txn = try store.beginWriteTxn();
        defer txn.abort();
        try txn.delete(&Verifier.guard(resumed.witnesses[0].requirement));
        try std.testing.expectError(error.EnrichmentSourceChanged, resumed.stage(&txn, 41, &plan));
    }
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        try projection_epoch.revoke(&txn);
        try txn.commit();
    }
    {
        var txn = try store.beginWriteTxn();
        defer txn.abort();
        try std.testing.expectError(error.EnrichmentSourceChanged, resumed.stage(&txn, 41, &plan));
    }
    {
        var read = try store.beginReadTxn();
        defer read.abort();
        verifier.first = null;
        var restarted = (try prepare(alloc, &read, 41, &plan, "doc", &verifier, .{ .visits = 1 })).?;
        defer restarted.deinit();
        try std.testing.expectEqualDeep(nodes[0].id, verifier.first.?);
        try std.testing.expectEqual(resumed.state.work_revision, restarted.state.work_revision);
        try std.testing.expectEqualDeep(resumed.state.observation, restarted.state.observation);
        try std.testing.expect(!std.meta.eql(resumed.state.projection_epoch, restarted.state.projection_epoch));
    }
    {
        // Dependency-only work changes no primary position, but must fence
        // both the prepared page and the persisted prefix from the old work.
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        try std.testing.expect(try obligations.mark(alloc, &txn, authority, "doc", resumed.state.work_position));
        try txn.commit();
    }
    {
        var txn = try store.beginWriteTxn();
        defer txn.abort();
        try std.testing.expectError(error.EnrichmentSourceChanged, resumed.stage(&txn, 41, &plan));
    }
    var finished = false;
    var verified: usize = 0;
    for (0..512) |_| {
        var page = blk: {
            var read = try store.beginReadTxn();
            defer read.abort();
            verifier.first = null;
            break :blk (try prepare(alloc, &read, 41, &plan, "doc", &verifier, .{ .visits = 17 })).?;
        };
        defer page.deinit();
        if (verified == 0) try std.testing.expectEqualDeep(nodes[0].id, verifier.first.?);
        {
            const command = try page.command();
            try command.validate(alloc);
            var read = try store.beginReadTxn();
            defer read.abort();
            var receiver = (try prepare(alloc, &read, 41, &plan, "doc", &verifier, .{ .visits = command.completion.?.visits, .bytes = command.completion.?.bytes, .time_budget_ns = null })).?;
            defer receiver.deinit();
            try std.testing.expectEqualDeep(command, try receiver.command());
        }
        verified += page.verified();
        if (page.atEnd()) {
            {
                var aborted = try store.beginWriteTxn();
                defer aborted.abort();
                try std.testing.expect(try page.stage(&aborted, 41, &plan));
            }
            var read = try store.beginReadTxn();
            defer read.abort();
            try std.testing.expectEqual(@as(u64, 1), (try obligations.load(&read)).?.pending_documents);
            try std.testing.expectEqualDeep(page.expected, try load(&read, &page.selected));
        }
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        finished = try page.stage(&txn, 41, &plan);
        try txn.commit();
        if (finished) break;
    }
    try std.testing.expect(finished);
    try std.testing.expectEqual(@as(usize, nodes.len), verified);
    {
        var read = try store.beginReadTxn();
        defer read.abort();
        try std.testing.expectEqual(@as(u64, 0), (try obligations.load(&read)).?.pending_documents);
        try std.testing.expect((try load(&read, &first.selected)) == null);
        // Lost replies never recreate work or decrement the counter twice.
        try std.testing.expect((try prepare(alloc, &read, 41, &plan, "doc", &verifier, .{})) == null);
    }
}
