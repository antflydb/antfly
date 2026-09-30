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

//! Authenticate complete chunk sets before entering serialized apply. A small
//! inventory fence at commit prevents a truncated replacement retiring only a
//! prefix of the old output. Provider payloads are never parsed under apply.
const std = @import("std");
const publication = @import("artifact_publication.zig");
const inventory = @import("artifact_inventory.zig");
const chunks = @import("artifact_chunk_manifest.zig");
const catalog = @import("catalog/enrichment_catalog.zig");
const text = @import("artifact_asset_publication.zig");
const ids = @import("artifact_ids.zig");
const extraction = @import("artifact_extraction_generation.zig");

/// One unit's owned input. Capturing never grants document completion; a
/// separate accepted upstream inventory must bound enumeration and retirement.
pub const UnitInput = struct {
    token: @import("artifact_producer_context.zig").Token,
    value: ?[]const u8,
    manifest_key: []const u8,
    previous: chunks.Manifest,

    pub fn deinit(self: *UnitInput) void {
        self.token.deinit();
        self.* = undefined;
    }
};

/// A bounded discovery page, not evidence that its child producers finished.
/// It owns every returned identity and the accepted upstream causal proof;
/// neither storage snapshots nor cursor buffers escape into provider work.
pub const UnitPage = ScopePage(extraction.Position);
pub const RetirementPage = ScopePage(RetirementPosition);

pub const VerifiedPage = struct {
    digest: publication.Digest,
    observation: @import("artifact_stream_observation.zig").Observation,
};

pub const PendingChildren = struct {
    units: []const []const u8,
    observation: @import("artifact_stream_observation.zig").Observation,
};

/// A discovery cursor only. Concurrent child writes can sort behind it, so a
/// caller must wrap/revalidate; scan-end must never discharge an obligation.
pub const RetirementPosition = struct {
    generation: publication.Digest,
    after_key: []const u8,

    pub fn encodeAlloc(self: @This(), alloc: std.mem.Allocator) ![]u8 {
        if (self.after_key.len > 1024 * 1024) return error.InvalidBatchRequest;
        const raw = try alloc.alloc(u8, 72 + self.after_key.len);
        @memcpy(raw[0..4], "AUR1");
        @memcpy(raw[4..36], &self.generation);
        std.mem.writeInt(u32, raw[36..40], @intCast(self.after_key.len), .little);
        @memcpy(raw[40 .. raw.len - 32], self.after_key);
        std.crypto.hash.sha2.Sha256.hash(raw[0 .. raw.len - 32], raw[raw.len - 32 ..][0..32], .{});
        return raw;
    }
    /// The key borrows raw; a page copies it before releasing the input.
    pub fn decode(raw: []const u8) !@This() {
        if (raw.len < 72 or raw.len > 72 + 1024 * 1024 or !std.mem.eql(u8, raw[0..4], "AUR1") or
            std.mem.readInt(u32, raw[36..40], .little) != raw.len - 72) return error.ArtifactCatalogCorrupt;
        var checksum: publication.Digest = undefined;
        std.crypto.hash.sha2.Sha256.hash(raw[0 .. raw.len - 32], &checksum, .{});
        if (!std.mem.eql(u8, &checksum, raw[raw.len - 32 ..])) return error.ArtifactCatalogCorrupt;
        return .{ .generation = raw[4..36].*, .after_key = raw[40 .. raw.len - 32] };
    }
};

const ParentCheck = struct { digest: publication.Digest, observation: @import("artifact_stream_observation.zig").Observation };

/// Borrowed accepted-head evidence shared by page verification and individual
/// job retirement. Keeping one verifier prevents weaker callback-only acks.
const ParentEvidence = struct {
    proof: *const @import("artifact_producer_provenance.zig").Proof,
    key: []const u8,
    value: []const u8,
    position: ?publication.Position,

    fn verify(self: ParentEvidence, alloc: std.mem.Allocator, txn: anytype, document: []const u8, producer: []const u8) !ParentCheck {
        if (producer.len == 0) return error.InvalidBatchRequest;
        const parent_effect = for (self.proof.effects) |effect| {
            if (std.mem.eql(u8, effect.key, self.key)) break effect;
        } else return error.ArtifactCatalogCorrupt;
        if (parent_effect.source_index >= self.proof.sources.len or
            !std.mem.eql(u8, self.proof.sources[parent_effect.source_index].document_key, document)) return error.InvalidBatchRequest;
        var observation = try @import("artifact_stream_observation.zig").Observation.capture(txn, document);
        try observation.observeProof(document, self.proof.*);
        const current = txn.get(self.key) catch |err| if (err == error.NotFound) return error.EnrichmentSourceChanged else return err;
        if (!std.mem.eql(u8, current, self.value) or
            !std.meta.eql(self.position, try publication.artifactRevision(txn, self.proof.namespace, self.key))) return error.EnrichmentSourceChanged;
        try self.proof.validateInputs(alloc, txn);
        var head_digest: publication.Digest = undefined;
        std.crypto.hash.sha2.Sha256.hash(self.value, &head_digest, .{});
        return .{ .digest = head_digest, .observation = observation };
    }

    fn child(self: ParentEvidence, alloc: std.mem.Allocator, txn: anytype, document: []const u8, producer: []const u8, unit: []const u8, head_digest: publication.Digest, require_empty: bool) !AcceptedResult {
        var accepted = try readAcceptedUnitResult(alloc, txn, document, producer, unit);
        errdefer accepted.proof.deinit();
        try self.proof.requireInheritedBy(accepted.proof.proof.inputCommand());
        const guard = for (accepted.proof.proof.artifact_sources) |candidate| {
            if (std.mem.eql(u8, candidate.key, self.key)) break candidate;
        } else return error.ArtifactPublicationPending;
        if (guard.source_index >= accepted.proof.proof.sources.len or
            !std.mem.eql(u8, accepted.proof.proof.sources[guard.source_index].document_key, document) or
            !std.meta.eql(guard.content_digest, @as(?publication.Digest, head_digest)) or
            !std.meta.eql(guard.input_position, self.position)) return error.EnrichmentSourceChanged;
        if (require_empty and accepted.count != 0) return error.ArtifactPublicationPending;
        return accepted;
    }
};

pub const JobResolution = struct {
    arena: std.heap.ArenaAllocator,
    kind: enum { accepted, obsolete },
    document: []const u8,
    head_key: []const u8,
    head_value: []const u8,
    head_position: ?publication.Position,
    observation: @import("artifact_stream_observation.zig").Observation,

    pub fn deinit(self: *JobResolution) void {
        self.arena.deinit();
        self.* = undefined;
    }
    pub fn requireCurrent(self: *const JobResolution, txn: anytype) !void {
        try self.observation.requireCurrent(txn, self.document);
        const current = txn.get(self.head_key) catch |err| if (err == error.NotFound) return error.EnrichmentSourceChanged else return err;
        if (!std.mem.eql(u8, current, self.head_value) or
            !std.meta.eql(self.head_position, try publication.artifactRevision(txn, self.observation.authority.namespace, self.head_key))) return error.EnrichmentSourceChanged;
    }
};

fn ScopePage(comptime Position: type) type {
    return struct {
        arena: std.heap.ArenaAllocator,
        proof: @import("artifact_producer_provenance.zig").Owned,
        head_key: []const u8,
        head_value: []const u8,
        head_position: ?publication.Position,
        units: []const []const u8,
        after: Position,
        visited: u32,
        at_end: bool,

        pub fn deinit(self: *@This()) void {
            self.proof.deinit();
            self.arena.deinit();
            self.* = undefined;
        }
        pub fn inherit(self: *const @This(), token: *@import("artifact_producer_context.zig").Token) !void {
            try token.inheritProof(self.proof.proof);
            try token.observe(self.head_key, self.head_value, self.head_position);
        }

        fn evidence(self: *const @This()) ParentEvidence {
            return .{ .proof = &self.proof.proof, .key = self.head_key, .value = self.head_value, .position = self.head_position };
        }
        fn requireParent(self: *const @This(), alloc: std.mem.Allocator, txn: anytype, document: []const u8, producer: []const u8) !ParentCheck {
            return self.evidence().verify(alloc, txn, document, producer);
        }

        fn requireChild(self: *const @This(), alloc: std.mem.Allocator, txn: anytype, document: []const u8, producer: []const u8, unit: []const u8, head_digest: publication.Digest) !AcceptedResult {
            return self.evidence().child(alloc, txn, document, producer, unit, head_digest, Position == RetirementPosition);
        }

        /// Select missing/stale child results for producer scheduling, without
        /// advancing verification or granting completion. The result borrows
        /// this owned page, not the read snapshot. A stale parent is an error;
        /// only stale CHILD evidence is eligible for regeneration.
        pub fn pendingChildren(self: *@This(), alloc: std.mem.Allocator, txn: anytype, document: []const u8, producer: []const u8) !PendingChildren {
            const parent = try self.requireParent(alloc, txn, document, producer);
            var observation = parent.observation;
            var pending: std.ArrayList([]const u8) = .empty;
            for (self.units) |unit| {
                var accepted = self.requireChild(alloc, txn, document, producer, unit, parent.digest) catch |err| switch (err) {
                    error.ArtifactPublicationPending, error.ArtifactCoverageBaselinePending, error.EnrichmentSourceChanged => {
                        try pending.append(self.arena.allocator(), unit);
                        continue;
                    },
                    else => return err,
                };
                defer accepted.proof.deinit();
                try observation.observeProof(document, accepted.proof.proof);
            }
            return .{ .units = pending.items, .observation = observation };
        }

        /// Verify a discovery page in the SAME snapshot used to discover it.
        /// This is a bounded prerequisite for checkpoint advancement, not a
        /// document closure. In particular an old, still-accepted child must
        /// not certify a newly selected parent directory.
        pub fn verifyChildren(self: *const @This(), alloc: std.mem.Allocator, txn: anytype, document: []const u8, producer: []const u8) !VerifiedPage {
            const parent = try self.requireParent(alloc, txn, document, producer);
            var observation = parent.observation;
            var hash = std.crypto.hash.sha2.Sha256.init(.{});
            hash.update("verified-unit-page-v1");
            hash.update(&parent.digest);
            hash.update(&.{@intFromBool(Position == RetirementPosition)});
            for ([_][]const u8{ document, producer }) |identity| {
                var length: [8]u8 = undefined;
                std.mem.writeInt(u64, &length, @intCast(identity.len), .little);
                hash.update(&length);
                hash.update(identity);
            }
            for (self.units) |unit| {
                var accepted = try self.requireChild(alloc, txn, document, producer, unit, parent.digest);
                defer accepted.proof.deinit();
                try observation.observeProof(document, accepted.proof.proof);
                var length: [8]u8 = undefined;
                std.mem.writeInt(u64, &length, @intCast(unit.len), .little);
                hash.update(&length);
                hash.update(unit);
                hash.update(&accepted.proof.proof.publication_digest);
            }
            return .{ .digest = hash.finalResult(), .observation = observation };
        }
    };
}

/// Amortize immutable catalog authorization over a bounded page of units.
/// The session borrows its snapshot; each returned input owns all its state.
pub fn UnitSession(comptime Txn: type) type {
    return UnitSessionImpl(Txn, true);
}

pub fn UnitVerificationSession(comptime Txn: type) type {
    return UnitSessionImpl(Txn, false);
}

fn UnitSessionImpl(comptime Txn: type, comptime capture_enabled: bool) type {
    return struct {
        const Session = @This();
        txn: Txn,
        request: @import("enrichment/enrichment_types.zig").GeneratedEnrichmentRequest,
        authority: publication.Authority,
        source: if (capture_enabled) publication.Source else void,

        const Directory = extraction.View(@typeInfo(Txn).pointer.child);
        const Acceptance = struct {
            view: Directory,
            proof: @import("artifact_producer_provenance.zig").Owned,
            position: ?publication.Position,
        };

        fn acceptedDirectory(self: @This(), alloc: std.mem.Allocator) !Acceptance {
            const scope = try @import("artifact_generation_scope.zig").extractionKeyAlloc(alloc, self.request.doc_key, self.request.upstream_artifact_name);
            defer alloc.free(scope);
            var view = (try Directory.open(alloc, self.txn, scope)) orelse return error.ArtifactPublicationPending;
            errdefer view.deinit();
            if (!std.meta.eql(view.plan.core.spec.authority, self.authority)) return error.ArtifactCatalogDrift;
            var proof = (try @import("artifact_producer_provenance.zig").readCurrentForArtifact(alloc, self.txn, view.plan.core.head_key, &view.plan.core.spec.encode())) orelse return error.ArtifactPublicationPending;
            errdefer proof.deinit();
            if (proof.proof.producer_kind != .enrichment or proof.proof.producer_scope_key.len != 0 or
                proof.proof.producer_generation != self.authority.epoch or
                !std.mem.eql(u8, proof.proof.producer_name, self.request.upstream_artifact_name) or
                !std.mem.eql(u8, proof.proof.producer_artifact_name, self.request.upstream_artifact_name) or
                !std.mem.eql(u8, &proof.proof.input_digest, &view.plan.core.spec.input_digest)) return error.ArtifactCatalogCorrupt;
            return .{ .view = view, .proof = proof, .position = try publication.artifactRevision(self.txn, self.authority.namespace, view.plan.core.head_key) };
        }

        /// A bounded worker page shares parent authorization and verification.
        /// The resolver borrows the read snapshot, never an external provider
        /// call; returned resolutions own their point-only writer guards.
        pub const JobResolver = struct {
            session: Session,
            accepted: Acceptance,
            verified: ParentCheck,
            encoded_head: @TypeOf(@as(@import("artifact_chunk_generation.zig").Spec, undefined).encode()),

            pub fn deinit(self: *@This()) void {
                self.accepted.proof.deinit();
                self.accepted.view.deinit();
                self.* = undefined;
            }

            /// A superseded generation retires scheduling only. Current absent
            /// units require an accepted EMPTY result, not mere absence.
            pub fn resolveJob(self: *@This(), alloc: std.mem.Allocator, generation: publication.Digest, parent_unit: []const u8) !?JobResolution {
                if (std.mem.allEqual(u8, &generation, 0)) return error.InvalidBatchRequest;
                var ref = (try ids.decodeArtifactRefAlloc(alloc, parent_unit)) orelse return error.InvalidBatchRequest;
                defer ref.deinit(alloc);
                if (ref.kind != .asset or ref.unit_id == null or ref.unit_id.?.len == 0 or ref.chunk_id != null or
                    !std.mem.eql(u8, ref.document_id, self.session.request.doc_key) or
                    !std.mem.eql(u8, ref.name, self.session.request.upstream_artifact_name)) return error.InvalidBatchRequest;
                const evidence: ParentEvidence = .{ .proof = &self.accepted.proof.proof, .key = self.accepted.view.plan.core.head_key, .value = &self.encoded_head, .position = self.accepted.position };
                var verified = self.verified;
                const obsolete = !std.mem.eql(u8, &generation, &self.accepted.view.plan.core.spec.id());
                if (!obsolete) {
                    const name = try extraction.unitNameAlloc(alloc, ref.unit_id.?);
                    defer alloc.free(name);
                    const live = try self.accepted.view.contains(alloc, name);
                    var child = evidence.child(alloc, self.session.txn, self.session.request.doc_key, self.session.request.artifact_name, parent_unit, verified.digest, !live) catch |err| switch (err) {
                        error.ArtifactPublicationPending, error.ArtifactCoverageBaselinePending, error.EnrichmentSourceChanged => return null,
                        else => return err,
                    };
                    defer child.proof.deinit();
                    try verified.observation.observeProof(self.session.request.doc_key, child.proof.proof);
                }
                var arena = std.heap.ArenaAllocator.init(alloc);
                errdefer arena.deinit();
                const document = try arena.allocator().dupe(u8, self.session.request.doc_key);
                const head_key = try arena.allocator().dupe(u8, evidence.key);
                const head_value = try arena.allocator().dupe(u8, evidence.value);
                return .{ .arena = arena, .kind = if (obsolete) .obsolete else .accepted, .document = document, .head_key = head_key, .head_value = head_value, .head_position = self.accepted.position, .observation = verified.observation };
            }
        };

        pub fn jobResolver(self: Session, alloc: std.mem.Allocator) !JobResolver {
            var accepted = try self.acceptedDirectory(alloc);
            errdefer accepted.view.deinit();
            errdefer accepted.proof.deinit();
            const encoded = accepted.view.plan.core.spec.encode();
            const evidence: ParentEvidence = .{ .proof = &accepted.proof.proof, .key = accepted.view.plan.core.head_key, .value = &encoded, .position = accepted.position };
            const verified = try evidence.verify(alloc, self.txn, self.request.doc_key, self.request.artifact_name);
            return .{ .session = self, .accepted = accepted, .verified = verified, .encoded_head = encoded };
        }

        pub fn resolveJob(self: Session, alloc: std.mem.Allocator, generation: publication.Digest, parent_unit: []const u8) !?JobResolution {
            var resolver = try self.jobResolver(alloc);
            defer resolver.deinit();
            return resolver.resolveJob(alloc, generation, parent_unit);
        }

        pub fn capture(self: @This(), alloc: std.mem.Allocator, parent_unit: []const u8) !UnitInput {
            if (!capture_enabled) @compileError("unit verification sessions cannot capture producer inputs");
            return captureUnit(alloc, self.txn, self.request, self.authority, self.source, parent_unit);
        }

        /// Durable jobs name an immutable parent generation, not whichever
        /// parent happens to be current when a delayed callback starts. Fence
        /// that identity before reading unit bodies or invoking any provider.
        /// capture() then authenticates the selected head and inherits its
        /// causal proof in this same pinned snapshot.
        pub fn captureGeneration(self: @This(), alloc: std.mem.Allocator, generation: publication.Digest, parent_unit: []const u8) !UnitInput {
            if (!capture_enabled) @compileError("unit verification sessions cannot capture producer inputs");
            if (std.mem.allEqual(u8, &generation, 0)) return error.InvalidBatchRequest;
            const key = try extraction.headKeyAlloc(alloc, self.request.doc_key, self.request.upstream_artifact_name);
            defer alloc.free(key);
            const raw = self.txn.get(key) catch |err| if (err == error.NotFound) return error.EnrichmentSourceChanged else return err;
            const spec = try @import("artifact_chunk_generation.zig").Spec.decode(raw);
            if (!std.mem.eql(u8, &generation, &spec.id())) return error.EnrichmentSourceChanged;
            return self.capture(alloc, parent_unit);
        }

        /// Enumerate only an accepted complete extraction directory. Metadata
        /// visits and owned key bytes are bounded independently of unit bodies;
        /// non-unit entries still advance the generation-bound continuation.
        pub fn unitPage(self: @This(), alloc: std.mem.Allocator, after: ?extraction.Position, max_visits: u32, max_bytes: usize) !UnitPage {
            if (max_visits == 0 or max_visits > 128 or max_bytes == 0 or max_bytes > 64 * 1024) return error.InvalidBatchRequest;
            var arena = std.heap.ArenaAllocator.init(alloc);
            errdefer arena.deinit();
            const owned = arena.allocator();
            var accepted = try self.acceptedDirectory(alloc);
            defer accepted.view.deinit();
            errdefer accepted.proof.deinit();
            var cursor = try accepted.view.resumeCursor(alloc, after);
            defer cursor.deinit();
            const head = try owned.dupe(u8, accepted.view.plan.core.head_key);
            const value = try owned.dupe(u8, &accepted.view.plan.core.spec.encode());
            var units: std.ArrayList([]const u8) = .empty;
            var position = cursor.position();
            var visited: u32 = 0;
            var bytes: usize = 0;
            while (visited < max_visits) {
                const descriptor = (try cursor.next()) orelse break;
                const is_unit = descriptor.name[0] == 1;
                if (is_unit and descriptor.name.len == 1) return error.ArtifactCatalogCorrupt;
                const key = if (is_unit) try @import("../internal_keys.zig").documentUnitArtifactKeyAlloc(alloc, self.request.doc_key, self.request.upstream_artifact_name, descriptor.name[1..]) else null;
                defer if (key) |name| alloc.free(name);
                const charged = std.math.add(usize, descriptor.name.len + 80, if (key) |name| name.len else 0) catch return error.ResourceBudgetExceeded;
                if (charged > publication.max_payload_bytes or (key != null and key.?.len > 1024 * 1024)) return error.ResourceBudgetExceeded;
                // A single bounded large identity must not starve forever.
                // Do not advance the saved position past a deferred member.
                if (visited != 0 and bytes +| charged > max_bytes) break;
                if (key) |name| try units.append(owned, try owned.dupe(u8, name));
                bytes += charged;
                visited += 1;
                position = cursor.position();
                if (bytes >= max_bytes) break;
            }
            return .{ .arena = arena, .proof = accepted.proof, .head_key = head, .head_value = value, .head_position = accepted.position, .units = units.items, .after = position, .visited = visited, .at_end = position.next_ordinal == accepted.view.plan.core.spec.output.count };
        }

        /// Discover previously materialized child scopes absent from the
        /// accepted current parent. This includes empty replacements; scanning
        /// only the current directory cannot find their obsolete children.
        pub fn retirementPage(self: @This(), alloc: std.mem.Allocator, after: ?RetirementPosition, max_visits: u32, max_bytes: usize) !RetirementPage {
            if (max_visits == 0 or max_visits > 128 or max_bytes == 0 or max_bytes > 64 * 1024) return error.InvalidBatchRequest;
            const keys = @import("../internal_keys.zig");
            var arena = std.heap.ArenaAllocator.init(alloc);
            errdefer arena.deinit();
            const owned = arena.allocator();
            var accepted = try self.acceptedDirectory(alloc);
            defer accepted.view.deinit();
            errdefer accepted.proof.deinit();
            const generation = accepted.view.plan.core.spec.id();
            if (after) |position| {
                if (!std.mem.eql(u8, &generation, &position.generation)) return error.EnrichmentSourceChanged;
            }
            const head = try owned.dupe(u8, accepted.view.plan.core.head_key);
            const value = try owned.dupe(u8, &accepted.view.plan.core.spec.encode());
            var last: std.ArrayListUnmanaged(u8) = .empty;
            if (after) |position| try last.appendSlice(owned, position.after_key);
            var cursor = try @import("artifact_unit_scope_cursor.zig").Cursor(@typeInfo(Txn).pointer.child).open(alloc, self.txn, self.request.doc_key, self.request.artifact_name, last.items);
            defer cursor.close();
            var units: std.ArrayList([]const u8) = .empty;
            var visited: u32 = 0;
            var bytes: usize = 0;
            var at_end = false;
            while (visited < max_visits) {
                const found = (try cursor.peek()) orelse {
                    at_end = true;
                    break;
                };
                const unit = try keys.decodeBodyAlloc(alloc, found.unit[0 .. found.unit.len - 2]);
                defer alloc.free(unit);
                const name = try extraction.unitNameAlloc(alloc, unit);
                defer alloc.free(name);
                const live = try accepted.view.contains(alloc, name);
                const key = if (!live) try keys.documentUnitArtifactKeyAlloc(alloc, self.request.doc_key, self.request.upstream_artifact_name, unit) else null;
                defer if (key) |selected| alloc.free(selected);
                const charge = found.bytes +| (if (key) |selected| selected.len else @as(usize, 0));
                if (key != null and key.?.len > 1024 * 1024) return error.ResourceBudgetExceeded;
                if (visited != 0 and bytes +| charge > max_bytes) break;
                if (key) |selected| try units.append(owned, try owned.dupe(u8, selected));
                last.clearRetainingCapacity();
                try last.appendSlice(owned, found.key);
                bytes +|= charge;
                visited += 1;
                if (visited == max_visits or bytes >= max_bytes) break;
                try cursor.advance();
            }
            return .{ .arena = arena, .proof = accepted.proof, .head_key = head, .head_value = value, .head_position = accepted.position, .units = units.items, .after = .{ .generation = generation, .after_key = last.items }, .visited = visited, .at_end = at_end };
        }
    };
}

pub fn authorizeUnitChild(alloc: std.mem.Allocator, txn: anytype, parent_request: @import("enrichment/enrichment_types.zig").GeneratedEnrichmentRequest, producer_name: []const u8, plan: *const @import("catalog/index_manager.zig").IndexManager.WritePlanSnapshot) !?publication.Authority {
    // Extraction currently owns its unit chunk execution. Authorize that
    // immutable template, then select its configured child, rather than invent
    // a top-level worker template or accept caller-provided chunker settings.
    const bound = (try @import("artifact_producer_input.zig").authorizeTemplate(alloc, txn, parent_request, plan)) orelse return null;
    if (parent_request.kind != .asset or bound.requirement.scope != .producer_defined or parent_request.neighbor_context_json.len != 0) return error.OnlineMergeArtifactTailsUnsupported;
    const completion = if (plan.completion_plan) |*value| value else return error.ArtifactCatalogDrift;
    const child = try completion.unitChild(producer_name);
    if (child.kind != .unit_children or child.scope != .upstream_units) return error.ArtifactCatalogDrift;
    if (child.parent_template != bound.requirement.template or !std.mem.eql(u8, child.upstream, parent_request.artifact_name)) return error.OnlineMergeArtifactTailsUnsupported;
    if (child.neighbor_context) return error.OnlineMergeArtifactTailsUnsupported;
    return bound.authority;
}

pub fn unitSession(alloc: std.mem.Allocator, txn: anytype, parent_request: @import("enrichment/enrichment_types.zig").GeneratedEnrichmentRequest, producer_name: []const u8, plan: *const @import("catalog/index_manager.zig").IndexManager.WritePlanSnapshot) !?UnitSession(@TypeOf(txn)) {
    const authority = (try authorizeUnitChild(alloc, txn, parent_request, producer_name, plan)) orelse return null;
    var scratch = std.heap.ArenaAllocator.init(alloc);
    defer scratch.deinit();
    // Only borrowed session identities escape scratch; child definition and
    // consumer authorization remain bound to the catalog digest at apply.
    const request: @import("enrichment/enrichment_types.zig").GeneratedEnrichmentRequest = .{ .kind = .chunk_text, .index_name = "", .artifact_name = producer_name, .doc_key = parent_request.doc_key, .source_field = "", .upstream_artifact_name = parent_request.artifact_name };
    // Hash/materialize primary identity once per pinned session, not once per
    // unit. Only fixed metadata and the caller's borrowed document key escape.
    var source = publication.capturePrimarySource(scratch.allocator(), txn, authority.namespace, parent_request.doc_key) catch |err| switch (err) {
        error.EnrichmentSourceChanged => try publication.capturePrimaryTombstoneSource(scratch.allocator(), txn, authority.namespace, parent_request.doc_key),
        else => return err,
    };
    source.document_key = parent_request.doc_key;
    return .{ .txn = txn, .request = request, .authority = authority, .source = source };
}

/// Metadata-only authorization for discovery and accepted-result verification.
/// The accepted directory and child receipts carry their own causal guards;
/// reading/hashing the primary body again adds no verification evidence. This
/// type deliberately cannot capture inputs or invoke a producer.
pub fn unitVerificationSession(alloc: std.mem.Allocator, txn: anytype, parent_request: @import("enrichment/enrichment_types.zig").GeneratedEnrichmentRequest, producer_name: []const u8, plan: *const @import("catalog/index_manager.zig").IndexManager.WritePlanSnapshot) !?UnitVerificationSession(@TypeOf(txn)) {
    const authority = (try authorizeUnitChild(alloc, txn, parent_request, producer_name, plan)) orelse return null;
    return .{ .txn = txn, .request = .{ .kind = .chunk_text, .index_name = "", .artifact_name = producer_name, .doc_key = parent_request.doc_key, .source_field = "", .upstream_artifact_name = parent_request.artifact_name }, .authority = authority, .source = {} };
}

fn captureUnit(alloc: std.mem.Allocator, txn: anytype, request: @import("enrichment/enrichment_types.zig").GeneratedEnrichmentRequest, authority: publication.Authority, source: publication.Source, parent_unit: []const u8) !UnitInput {
    var token: @import("artifact_producer_context.zig").Token = .{
        .arena = std.heap.ArenaAllocator.init(alloc),
        .namespace = authority.namespace,
        .epoch = authority.epoch,
        .catalog_digest = authority.catalog_digest,
        .producer_kind = .enrichment,
        .producer_name = undefined,
        .producer_generation = authority.epoch,
        .artifact_name = undefined,
        .source = undefined,
    };
    errdefer token.deinit();
    const owned = token.arena.allocator();
    var parent = (try ids.decodeArtifactRefAlloc(alloc, parent_unit)) orelse return error.InvalidBatchRequest;
    defer parent.deinit(alloc);
    if (parent.kind != .asset or parent.unit_id == null or parent.unit_id.?.len == 0 or parent.chunk_id != null or
        !std.mem.eql(u8, parent.document_id, request.doc_key) or !std.mem.eql(u8, parent.name, request.upstream_artifact_name)) return error.InvalidBatchRequest;
    const name = if (request.artifact_name.len != 0) request.artifact_name else request.index_name;
    token.producer_name = try owned.dupe(u8, name);
    token.artifact_name = token.producer_name;
    token.producer_scope_key = try owned.dupe(u8, parent_unit);
    token.source = source;
    token.source.document_key = try owned.dupe(u8, source.document_key);
    var input = try extraction.captureInput(alloc, txn, parent_unit);
    defer input.deinit();
    const raw = input.value;
    if (token.source.exists) {
        // Absence must be an accepted upstream deletion, not an incomplete
        // upload, missing replica data, or an arbitrarily invented unit ID.
        var proof = (try @import("artifact_producer_provenance.zig").readCurrentForArtifact(alloc, txn, input.proofKey(parent_unit), input.proofValue())) orelse return error.ArtifactPublicationPending;
        defer proof.deinit();
        try token.inheritProof(proof.proof);
    }
    try input.observe(&token, txn, parent_unit);
    const manifest = try chunks.scopedKeyAlloc(owned, request.doc_key, name, parent.unit_id.?);
    const previous_raw = txn.get(manifest) catch |err| if (err == error.NotFound) return error.ArtifactCoverageBaselinePending else return err;
    const previous = try chunks.Manifest.decode(previous_raw);
    try token.observePrecondition(manifest, previous_raw, try publication.artifactRevision(txn, authority.namespace, manifest));
    const value = if (token.source.exists and raw != null) try owned.dupe(u8, raw.?) else null;
    return .{ .token = token, .value = value, .manifest_key = manifest, .previous = previous };
}

/// A current accepted root manifest proves that this producer published its
/// complete root replacement, including an explicitly empty replacement.
/// It does NOT close unit producers or the document's dependency graph. The
/// caller must pin this snapshot and retain the proof's complete causal set
/// in its observation before publishing any enumeration result.
pub fn readAcceptedRoot(alloc: std.mem.Allocator, txn: anytype, document: []const u8, producer: []const u8) !@import("artifact_producer_provenance.zig").Owned {
    return (try readAcceptedScope(alloc, txn, document, producer, null, "")).proof;
}

/// A unit's complete replacement is evidence for that unit only. The caller
/// must enumerate a separately accepted upstream unit inventory before these
/// per-unit results can close an upstream-units requirement.
pub fn readAcceptedUnit(alloc: std.mem.Allocator, txn: anytype, document: []const u8, producer: []const u8, parent_unit: []const u8) !@import("artifact_producer_provenance.zig").Owned {
    return (try readAcceptedUnitResult(alloc, txn, document, producer, parent_unit)).proof;
}

const AcceptedResult = struct {
    proof: @import("artifact_producer_provenance.zig").Owned,
    count: u32,
};

fn readAcceptedUnitResult(alloc: std.mem.Allocator, txn: anytype, document: []const u8, producer: []const u8, parent_unit: []const u8) !AcceptedResult {
    var parent = (try ids.decodeArtifactRefAlloc(alloc, parent_unit)) orelse return error.InvalidBatchRequest;
    defer parent.deinit(alloc);
    if (parent.kind != .asset or parent.unit_id == null or parent.chunk_id != null or !std.mem.eql(u8, parent.document_id, document)) return error.InvalidBatchRequest;
    return readAcceptedScope(alloc, txn, document, producer, parent.unit_id.?, parent_unit);
}

fn readAcceptedScope(alloc: std.mem.Allocator, txn: anytype, document: []const u8, producer: []const u8, unit: ?[]const u8, parent_unit: []const u8) !AcceptedResult {
    const key = try chunks.scopedKeyAlloc(alloc, document, producer, unit);
    defer alloc.free(key);
    const internal_keys = @import("../internal_keys.zig");
    const generations = @import("artifact_chunk_generation.zig");
    const kind = internal_keys.findComponentTerminator(key, 1).? + 2;
    var scope_digest: publication.Digest = undefined;
    std.crypto.hash.sha2.Sha256.hash(key, &scope_digest, .{});
    key[kind] = internal_keys.producer_generation_head_kind;
    const head = txn.get(key) catch |err| if (err == error.NotFound) null else return err;
    var generation: ?generations.Spec = null;
    var count: u32 = 0;
    const raw = if (head) |value| blk: {
        generation = try generations.Spec.decode(value);
        count = generation.?.output.count;
        if (!std.mem.eql(u8, &generation.?.scope_digest, &scope_digest)) return error.ArtifactCatalogCorrupt;
        break :blk value;
    } else blk: {
        key[kind] = internal_keys.producer_stream_manifest_kind;
        const value = txn.get(key) catch |err| switch (err) {
            error.NotFound => return error.ArtifactPublicationPending,
            else => return err,
        };
        count = (try chunks.Manifest.decode(value)).count;
        break :blk value;
    };
    var accepted = (try @import("artifact_producer_provenance.zig").readCurrentForArtifact(alloc, txn, key, raw)) orelse return error.ArtifactPublicationPending;
    errdefer accepted.deinit();
    const proof = accepted.proof;
    // Artifact proof lookup authenticates authority, output revision/digest,
    // and every inherited input. Require the producer and scope identity too:
    // an output from another scope cannot certify this producer's boundary.
    if (proof.producer_kind != .enrichment or proof.producer_generation != proof.authority_epoch or
        !std.mem.eql(u8, proof.producer_scope_key, parent_unit) or !std.mem.eql(u8, proof.producer_name, producer) or
        !std.mem.eql(u8, proof.producer_artifact_name, producer)) return error.ArtifactCatalogCorrupt;
    const effect = for (proof.effects) |candidate| {
        if (std.mem.eql(u8, candidate.key, key)) break candidate;
    } else return error.ArtifactCatalogCorrupt;
    if (effect.family != .document_artifact or effect.value_digest == null or
        !std.mem.eql(u8, proof.sources[effect.source_index].document_key, document)) return error.ArtifactCatalogCorrupt;
    if (generation) |spec| {
        if (!std.meta.eql(spec.authority, publication.Authority{ .namespace = proof.namespace, .epoch = proof.authority_epoch, .catalog_digest = proof.catalog_digest }) or
            !std.mem.eql(u8, &spec.input_digest, &proof.input_digest)) return error.ArtifactCatalogCorrupt;
        key[kind] = internal_keys.producer_stream_manifest_kind;
        var selected = try generations.Plan.init(alloc, key, spec);
        defer selected.deinit();
        const state = selected.load(txn) catch |err| if (err == error.NotFound) return error.ArtifactCatalogCorrupt else return err;
        if (state.retiring or !std.meta.eql(state.progress, spec.output)) return error.ArtifactCatalogCorrupt;
    }
    return .{ .proof = accepted, .count = count };
}

pub const Fence = struct {
    key: []const u8,
    member_count: u32,
    next_count: u32,
    upstream_key: ?[]const u8 = null,
    upstream_certificate: ?@import("artifact_producer_provenance.zig").ArtifactCertificate = null,

    /// Resolve accepted unit provenance off-lock, including all inherited
    /// inputs. The final writer retains only one fixed-size reference fence.
    pub fn bind(self: *Fence, alloc: std.mem.Allocator, txn: anytype, command: publication.Command) !void {
        const selected = self.upstream_key orelse return;
        var input = try extraction.captureInput(alloc, txn, selected);
        defer input.deinit();
        const certificate = try @import("artifact_producer_provenance.zig").certifyInheritedArtifact(alloc, txn, input.proofKey(selected), input.proofValue(), command);
        if (self.next_count != 0 and input.value == null) return error.EnrichmentSourceChanged;
        self.upstream_certificate = certificate;
    }

    /// The normal publication precondition authenticates the prior manifest
    /// bytes/revision in this same transaction. This checks set completeness.
    pub fn requireCurrent(self: Fence, txn: anytype) !void {
        if (self.upstream_key != null) try (self.upstream_certificate orelse return error.ArtifactCoverageBaselinePending).requireCurrent(txn);
        const raw = txn.get(self.key) catch |err| {
            if (err == error.NotFound) return error.ArtifactCoverageBaselinePending;
            return err;
        };
        const previous = try chunks.Manifest.decode(raw);
        if (@max(previous.count, self.next_count) != self.member_count) return error.EnrichmentSourceChanged;
    }
};

pub fn prepare(alloc: std.mem.Allocator, command: publication.Command, catalogs: inventory.Catalogs) !publication.PreparedEffects {
    try command.validate(alloc);
    if (command.mode != .publish or command.producer_kind != .enrichment or
        command.producer_generation != command.authority_epoch or !std.mem.eql(u8, command.producer_name, command.producer_artifact_name)) return error.InvalidBatchRequest;
    var arena = std.heap.ArenaAllocator.init(alloc);
    errdefer arena.deinit();
    const owned = arena.allocator();
    const configs = try catalog.deserializeCatalog(owned, catalogs.enrichments);
    const config = for (configs) |value| {
        if (std.mem.eql(u8, value.name, command.producer_name)) break value;
    } else return error.EnrichmentSourceChanged;
    if (config.kind != .chunk) return error.InvalidBatchRequest;
    if (config.neighbor_context_json.len != 0) return error.OnlineMergeArtifactTailsUnsupported;
    const unit_source = if (config.source_artifact_name.len != 0) blk: {
        const upstream = for (configs) |candidate| {
            if (std.mem.eql(u8, candidate.name, config.source_artifact_name)) break candidate;
        } else return error.InvalidBatchRequest;
        if (upstream.kind != .asset) return error.InvalidBatchRequest;
        const producer = try @import("enrichment/asset_producer.zig").parseProducerConfig(owned, upstream.producer_json);
        break :blk producer.type == .document_extraction;
    } else false;
    if (unit_source != (command.producer_scope_key.len != 0)) return error.InvalidBatchRequest;
    var manifest_effect: ?publication.Mutation = null;
    var members: std.ArrayList(publication.Mutation) = .empty;
    for (command.mutations) |effect| {
        if (chunks.isKey(effect.key)) {
            if (manifest_effect != null or effect.family != .document_artifact or effect.value == null) return error.InvalidBatchRequest;
            manifest_effect = effect;
        } else try members.append(owned, effect);
    }
    const manifest = manifest_effect orelse return error.InvalidBatchRequest;
    const source = command.sources[manifest.source_index];
    const unit: ?[]const u8 = if (command.producer_scope_key.len != 0) blk: {
        const parent = (try ids.decodeArtifactRefAlloc(owned, command.producer_scope_key)) orelse return error.InvalidBatchRequest;
        if (parent.kind != .asset or parent.unit_id == null or parent.chunk_id != null or config.source_artifact_name.len == 0 or
            !std.mem.eql(u8, parent.document_id, source.document_key) or !std.mem.eql(u8, parent.name, config.source_artifact_name)) return error.InvalidBatchRequest;
        break :blk parent.unit_id.?;
    } else null;
    const key = try chunks.scopedKeyAlloc(owned, source.document_key, config.name, unit);
    if (!std.mem.eql(u8, key, manifest.key)) return error.InvalidBatchRequest;
    const next = chunks.Manifest.decode(manifest.value.?) catch return error.InvalidBatchRequest;
    var upstream_effect = manifest;
    if (next.count == 0) upstream_effect.value = null;
    if (unit != null) {
        // A selected immutable directory authenticates both presence and
        // absence. Legacy rows require BOTH head absence and exact row guards.
        const head = try extraction.headKeyAlloc(owned, source.document_key, config.source_artifact_name);
        const guarded_head = for (command.artifact_sources) |guard| {
            if (guard.source_index == manifest.source_index and std.mem.eql(u8, guard.key, head)) break guard;
        } else return error.InvalidBatchRequest;
        if (guarded_head.content_digest == null) {
            const guarded_unit = for (command.artifact_sources) |guard| {
                if (guard.source_index == manifest.source_index and std.mem.eql(u8, guard.key, command.producer_scope_key)) {
                    if (next.count != 0 and guard.content_digest == null) return error.InvalidBatchRequest;
                    break true;
                }
            } else false;
            if (!guarded_unit) return error.InvalidBatchRequest;
        }
    } else try text.requireUpstream(owned, command, config, upstream_effect, false);
    const guarded = for (command.mutation_preconditions) |condition| {
        if (condition.source_index == manifest.source_index and std.mem.eql(u8, condition.key, key) and condition.content_digest != null) break true;
    } else false;
    if (!guarded) return error.InvalidBatchRequest;
    // A synthetic old count verifies every declared ordinal/tail effect here;
    // the authenticated actual count is compared by Fence at commit.
    var declared_previous = chunks.Builder.init().finish();
    declared_previous.count = @intCast(members.items.len);
    try chunks.validateScopedReplacement(owned, source.document_key, config.name, unit, declared_previous, next, members.items);
    const text_members = try owned.alloc(bool, members.items.len);
    var scratch = std.heap.ArenaAllocator.init(alloc);
    defer scratch.deinit();
    for (members.items, text_members) |effect, *text_member| {
        text_member.* = false;
        if (effect.source_index != manifest.source_index) return error.InvalidBatchRequest;
        if (effect.value) |raw| {
            if (!scratch.reset(.retain_capacity)) return error.OutOfMemory;
            const temporary = scratch.allocator();
            var identity = (try ids.decodeArtifactRefAlloc(temporary, effect.key)) orelse return error.InvalidBatchRequest;
            defer identity.deinit(temporary);
            const parsed = std.json.parseFromSlice(std.json.Value, temporary, raw, .{}) catch |err| {
                if (err == error.OutOfMemory) return err;
                return error.InvalidBatchRequest;
            };
            defer parsed.deinit();
            if (parsed.value != .object) return error.InvalidBatchRequest;
            const object = parsed.value.object;
            try requireString(object, "_parent_doc_key", source.document_key);
            try requireString(object, "_artifact_name", config.name);
            try requireString(object, "_source_field", config.source_field);
            if (unit) |selected| {
                try requireString(object, "_parent_unit_id", selected);
                try requireString(object, "_parent_unit_key", command.producer_scope_key);
                try requireString(object, "_source_artifact_name", config.source_artifact_name);
            }
            const ordinal = object.get("_chunk_id") orelse return error.InvalidBatchRequest;
            if (ordinal != .integer or ordinal.integer != identity.chunk_id.?) return error.InvalidBatchRequest;
            const mime = object.get("_mime_type") orelse return error.InvalidBatchRequest;
            if (mime != .string or mime.string.len == 0) return error.InvalidBatchRequest;
            text_member.* = std.mem.eql(u8, mime.string, "text/plain");
            const payload = object.get(config.source_field) orelse (if (text_member.*) null else object.get("_data")) orelse return error.InvalidBatchRequest;
            if (payload != .string) return error.InvalidBatchRequest;
        }
    }
    if (!source.exists and next.count != 0) return error.InvalidBatchRequest;
    const upstream_key = if (unit != null and source.exists) try owned.dupe(u8, command.producer_scope_key) else null;
    var prepared = try text.prepareTextEffects(&arena, command, catalogs, config, configs, members.items, text_members, unit == null);
    prepared.chunk_fence = .{ .key = key, .member_count = @intCast(members.items.len), .next_count = next.count, .upstream_key = upstream_key };
    return prepared;
}

fn requireString(object: std.json.ObjectMap, field: []const u8, expected: []const u8) !void {
    const value = object.get(field) orelse return error.InvalidBatchRequest;
    if (value != .string or !std.mem.eql(u8, value.string, expected)) return error.InvalidBatchRequest;
}

test "ordered artifact inventory unit chunk replacement binds its exact parent and never closes document coverage" {
    const alloc = std.testing.allocator;
    const keys = @import("../internal_keys.zig");
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/unit-chunk-receiver", .{tmp.sub_path});
    defer alloc.free(path);
    var db = try @import("db.zig").DB.open(alloc, path, .{ .identity_namespace = .{ .table_id = 0x0101010101010101, .shard_id = 0x0101010101010101, .range_id = 0x0101010101010101 }, .online_source_authority = .raft, .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false });
    var db_open = true;
    defer if (db_open) db.close();
    try db.setSchemaJson(alloc, "{}");
    try db.addEnrichment(.{ .name = "units", .kind = .asset, .field = "url", .producer_json = "{\"type\":\"document_extraction\"}" });
    try db.addEnrichment(.{ .name = "chunks", .kind = .chunk, .field = "body", .source_artifact_name = "units", .chunk_size = 4 });
    try db.addIndex(.{ .name = "text", .kind = .full_text, .config_json = "{\"sources\":[{\"artifact\":\"chunks\"}]}" });
    try db.batchRaftReplicatedApply(.{ .writes = &.{.{ .key = "doc", .value = "{\"url\":\"input\"}" }}, .timestamp_ns = 1 }, .{ .term = 1, .index = 1 });
    var ordered = try db.artifactInventoryCommand(alloc);
    defer ordered.catalogs.deinit(alloc);
    ordered.binding.effect_protocol = 15;
    try db.batchRaftReplicatedApply(.{ .artifact_catalog = ordered }, .{ .term = 1, .index = 2 });
    var read = try db.core.store.beginReadTxn();
    var read_open = true;
    defer if (read_open) read.abort();
    const catalogs = try inventory.catalogs(&read);
    const unit_key = try keys.documentUnitArtifactKeyAlloc(alloc, "doc", "units", "page-1");
    defer alloc.free(unit_key);
    const member_key = try keys.documentUnitChunkArtifactKeyAlloc(alloc, "doc", "chunks", "page-1", 0);
    defer alloc.free(member_key);
    const manifest_key = try chunks.scopedKeyAlloc(alloc, "doc", "chunks", "page-1");
    defer alloc.free(manifest_key);
    const payload = try std.json.Stringify.valueAlloc(alloc, .{ ._parent_doc_key = "doc", ._parent_unit_key = unit_key, ._parent_unit_id = "page-1", ._artifact_name = "chunks", ._source_artifact_name = "units", ._source_field = "body", ._chunk_id = 0, ._mime_type = "text/plain", .body = "hello" }, .{});
    defer alloc.free(payload);
    var builder = chunks.Builder.init();
    try builder.append(0, payload);
    const manifest = builder.finish().encode();
    const effects = [_]publication.Mutation{
        .{ .family = .document_artifact, .key = member_key, .value = payload, .source_index = 0 },
        .{ .family = .document_artifact, .key = manifest_key, .value = &manifest, .source_index = 0 },
    };
    var source_arena = std.heap.ArenaAllocator.init(alloc);
    defer source_arena.deinit();
    const source = [_]publication.Source{try publication.capturePrimarySource(source_arena.allocator(), &read, @splat(1), "doc")};
    const extraction_head = try extraction.headKeyAlloc(alloc, "doc", "units");
    defer alloc.free(extraction_head);
    const inputs = [_]publication.ArtifactSource{
        .{ .key = unit_key, .content_digest = @splat(2), .input_position = null, .source_index = 0 },
        .{ .key = extraction_head, .content_digest = null, .input_position = null, .source_index = 0 },
    };
    const previous = [_]publication.ArtifactSource{.{ .key = manifest_key, .content_digest = @splat(3), .input_position = null, .source_index = 0 }};
    var command: publication.Command = .{ .namespace = @splat(1), .authority_epoch = ordered.binding.epoch, .catalog_digest = catalogs.digest(), .producer_kind = .enrichment, .producer_name = "chunks", .producer_generation = ordered.binding.epoch, .producer_artifact_name = "chunks", .producer_scope_key = unit_key, .sources = &source, .artifact_sources = &inputs, .mutation_preconditions = &previous, .mutations = &effects, .publication_digest = @splat(0) };
    command.publication_digest = command.digest();
    var prepared = try publication.prepareEffects(alloc, command, catalogs);
    defer prepared.deinit();
    try std.testing.expectEqual(@as(usize, 1), prepared.batch.documents.len);
    try std.testing.expectEqualStrings(member_key, prepared.batch.documents[0].key);
    try std.testing.expectEqual(@as(usize, 0), prepared.coverage.len);
    try std.testing.expectEqualStrings(manifest_key, prepared.chunk_fence.?.key);
    try std.testing.expectEqualStrings(unit_key, prepared.chunk_fence.?.upstream_key.?);
    // Raw unit bytes or an output-set manifest are not accepted provenance.
    try std.testing.expectError(error.ArtifactCoverageBaselinePending, prepared.chunk_fence.?.bind(alloc, &read, command));
    try std.testing.expectError(error.ArtifactCoverageBaselinePending, prepared.chunk_fence.?.requireCurrent(&read));
    const Check = struct {
        fn run(a: std.mem.Allocator, value: publication.Command, local: inventory.Catalogs) !void {
            var result = try publication.prepareEffects(a, value, local);
            defer result.deinit();
        }
    };
    try std.testing.checkAllAllocationFailures(alloc, Check.run, .{ command, catalogs });
    // An immutable extraction directory replaces the per-unit physical guard.
    // Structural validation must recognize that head without accepting a
    // sibling producer's head or unguarded head absence.
    var selected_head = inputs[1];
    selected_head.content_digest = @splat(4);
    var selected_command = command;
    selected_command.artifact_sources = (&selected_head)[0..1];
    selected_command.publication_digest = selected_command.digest();
    var head_prepared = try prepare(alloc, selected_command, catalogs);
    defer head_prepared.deinit();
    try std.testing.expectEqualStrings(unit_key, head_prepared.chunk_fence.?.upstream_key.?);
    try std.testing.checkAllAllocationFailures(alloc, Check.run, .{ selected_command, catalogs });
    const wrong_head = try extraction.headKeyAlloc(alloc, "doc", "other");
    defer alloc.free(wrong_head);
    selected_head.key = wrong_head;
    selected_command.publication_digest = selected_command.digest();
    try std.testing.expectError(error.InvalidBatchRequest, prepare(alloc, selected_command, catalogs));
    var changed = command;
    changed.producer_scope_key = "";
    changed.publication_digest = changed.digest();
    try std.testing.expectError(error.InvalidBatchRequest, prepare(alloc, changed, catalogs));
    changed = command;
    changed.artifact_sources = &.{};
    changed.publication_digest = changed.digest();
    try std.testing.expectError(error.InvalidBatchRequest, prepare(alloc, changed, catalogs));
    changed = command;
    changed.artifact_sources = inputs[0..1];
    changed.publication_digest = changed.digest();
    try std.testing.expectError(error.InvalidBatchRequest, prepare(alloc, changed, catalogs));
    changed = command;
    changed.artifact_sources = inputs[1..];
    changed.publication_digest = changed.digest();
    try std.testing.expectError(error.InvalidBatchRequest, prepare(alloc, changed, catalogs));
    const sibling = try keys.documentUnitArtifactKeyAlloc(alloc, "doc", "units", "page-2");
    defer alloc.free(sibling);
    changed = command;
    changed.producer_scope_key = sibling;
    changed.publication_digest = changed.digest();
    try std.testing.expectError(error.InvalidBatchRequest, prepare(alloc, changed, catalogs));

    // Supply an accepted upstream fixture to exercise the receiver fence and
    // scope-specific reader independently of the still-gated extraction writer.
    const provenance = @import("artifact_producer_provenance.zig");
    const parent_value = "unit payload";
    var parent = command;
    parent.producer_name = "units";
    parent.producer_artifact_name = "units";
    parent.producer_scope_key = "";
    parent.artifact_sources = &.{};
    parent.mutation_preconditions = &.{};
    const parent_effects = [_]publication.Mutation{.{ .family = .document_artifact, .key = unit_key, .value = parent_value, .source_index = 0 }};
    parent.mutations = &parent_effects;
    parent.publication_digest = parent.digest();
    var parent_proof = try provenance.fromCommand(alloc, parent);
    defer parent_proof.deinit();
    const parent_encoded = try provenance.encodeAlloc(alloc, parent_proof.proof);
    defer alloc.free(parent_encoded);
    const parent_position: publication.Position = .{ .raft = .{ .term = 1, .index = 3 } };
    {
        var txn = try db.core.store.beginWriteTxn();
        errdefer txn.abort();
        var activation = parent;
        activation.mode = .activate;
        try publication.stageAuthority(&txn, activation);
        try txn.put(unit_key, parent_value);
        try txn.put(manifest_key, &chunks.Builder.init().finish().encode());
        try publication.stageArtifactRevisions(&txn, parent, parent_position);
        try provenance.stage(&txn, parent, parent_encoded, parent_position);
        var marker: [16]u8 = undefined;
        std.mem.writeInt(u64, marker[0..8], 1, .little);
        std.mem.writeInt(u64, marker[8..16], 3, .little);
        try txn.put(&keys.raft_document_applied_entry_key, &marker);
        try txn.commit();
    }
    var actual_inputs = inputs;
    std.crypto.hash.sha2.Sha256.hash(parent_value, &actual_inputs[0].content_digest.?, .{});
    actual_inputs[0].input_position = parent_position;
    var actual = command;
    actual.artifact_sources = &actual_inputs;
    actual.publication_digest = actual.digest();
    var bound = try prepare(alloc, actual, catalogs);
    defer bound.deinit();
    {
        var plan = try db.core.index_manager.acquireWritePlanSnapshot();
        defer plan.release();
        var request = for (plan.plan().generated_templates) |candidate| {
            if (candidate.kind == .asset and std.mem.eql(u8, candidate.artifact_name, "units")) break candidate;
        } else return error.TestUnexpectedResult;
        request.doc_key = "doc";
        var current = try db.core.store.beginReadTxn();
        defer current.abort();
        const session = (try unitSession(alloc, &current, request, "chunks", plan.plan())).?;
        try std.testing.expectError(error.ArtifactCatalogDrift, unitSession(alloc, &current, request, "unknown", plan.plan()));
        try std.testing.expectError(error.OnlineMergeArtifactTailsUnsupported, unitSession(alloc, &current, request, "units", plan.plan()));
        var forged = request;
        forged.producer_json = "{}";
        try std.testing.expectError(error.ArtifactCatalogDrift, unitSession(alloc, &current, forged, "chunks", plan.plan()));
        var input = try session.capture(alloc, unit_key);
        defer input.deinit();
        try std.testing.expectEqualStrings(parent_value, input.value.?);
        try std.testing.expectEqualStrings(unit_key, input.token.producer_scope_key);
        try std.testing.expectEqualStrings(manifest_key, input.manifest_key);
        try std.testing.expectEqual(@as(u32, 0), input.previous.count);
        try parent_proof.proof.requireInheritedBy(try input.token.command(&effects));
        try std.testing.expectError(error.ArtifactPublicationPending, session.capture(alloc, sibling));
        try std.testing.expectError(error.InvalidBatchRequest, session.capture(alloc, member_key));
        const AllocationCheck = struct {
            fn run(a: std.mem.Allocator, capture_session: @TypeOf(session), scope: []const u8) !void {
                var selected = try capture_session.capture(a, scope);
                defer selected.deinit();
            }
            fn authorize(a: std.mem.Allocator, txn: @TypeOf(&current), parent_request: @TypeOf(request), snapshot: @TypeOf(plan.plan())) !void {
                _ = (try unitSession(a, txn, parent_request, "chunks", snapshot)).?;
            }
        };
        try std.testing.checkAllAllocationFailures(alloc, AllocationCheck.run, .{ session, unit_key });
        try std.testing.checkAllAllocationFailures(alloc, AllocationCheck.authorize, .{ &current, request, plan.plan() });
    }
    {
        var current = try db.core.store.beginReadTxn();
        defer current.abort();
        try bound.chunk_fence.?.bind(alloc, &current, actual);
        try bound.chunk_fence.?.requireCurrent(&current);
    }
    {
        var txn = try db.core.store.beginWriteTxn();
        defer txn.abort();
        const certificate = bound.chunk_fence.?.upstream_certificate.?;
        var replaced = certificate.value;
        replaced[0] ^= 1;
        try txn.put(&certificate.reference, &replaced);
        try std.testing.expectError(error.EnrichmentSourceChanged, bound.chunk_fence.?.requireCurrent(&txn));
    }
    var child_proof = try provenance.fromCommand(alloc, actual);
    defer child_proof.deinit();
    const child_encoded = try provenance.encodeAlloc(alloc, child_proof.proof);
    defer alloc.free(child_encoded);
    {
        var txn = try db.core.store.beginWriteTxn();
        errdefer txn.abort();
        try bound.chunk_fence.?.requireCurrent(&txn);
        for (actual.mutations) |effect| try txn.put(effect.key, effect.value.?);
        const position: publication.Position = .{ .raft = .{ .term = 1, .index = 4 } };
        try publication.stageArtifactRevisions(&txn, actual, position);
        try provenance.stage(&txn, actual, child_encoded, position);
        var marker: [16]u8 = undefined;
        std.mem.writeInt(u64, marker[0..8], 1, .little);
        std.mem.writeInt(u64, marker[8..16], 4, .little);
        try txn.put(&keys.raft_document_applied_entry_key, &marker);
        try txn.commit();
    }
    var current = try db.core.store.beginReadTxn();
    var current_open = true;
    defer if (current_open) current.abort();
    var accepted = try readAcceptedUnit(alloc, &current, "doc", "chunks", unit_key);
    defer accepted.deinit();
    try std.testing.expectEqualDeep(actual.publication_digest, accepted.proof.publication_digest);
    try std.testing.expectError(error.ArtifactPublicationPending, readAcceptedRoot(alloc, &current, "doc", "chunks"));
    try std.testing.expectError(error.ArtifactPublicationPending, readAcceptedUnit(alloc, &current, "doc", "chunks", sibling));

    // Provider input remains valid memory after its catalog/snapshot leases
    // end. A later accepted retirement invalidates the old token, but permits
    // an empty replacement with an exact old-tail precondition.
    var captured = blk: {
        var plan = try db.core.index_manager.acquireWritePlanSnapshot();
        defer plan.release();
        var request = for (plan.plan().generated_templates) |candidate| {
            if (candidate.kind == .asset and std.mem.eql(u8, candidate.artifact_name, "units")) break candidate;
        } else return error.TestUnexpectedResult;
        request.doc_key = "doc";
        var snapshot = try db.core.store.beginReadTxn();
        defer snapshot.abort();
        const session = (try unitSession(alloc, &snapshot, request, "chunks", plan.plan())).?;
        break :blk try session.capture(alloc, unit_key);
    };
    defer captured.deinit();
    try std.testing.expectEqualStrings(parent_value, captured.value.?);
    try std.testing.expectEqual(@as(u32, 1), captured.previous.count);
    var txn = try db.core.store.beginWriteTxn();
    var txn_open = true;
    defer if (txn_open) txn.abort();
    try txn.delete(unit_key);
    // Uncertified absence is not permission to publish an empty child set.
    const request: @import("enrichment/enrichment_types.zig").GeneratedEnrichmentRequest = .{ .kind = .chunk_text, .index_name = "", .artifact_name = "chunks", .doc_key = "doc", .source_field = "body", .upstream_artifact_name = "units" };
    const authority = (try publication.authority(&txn)).?;
    try std.testing.expectError(error.EnrichmentSourceChanged, captureUnit(alloc, &txn, request, authority, source[0], unit_key));
    var retired = parent;
    const retirement = [_]publication.Mutation{.{ .family = .document_artifact, .key = unit_key, .value = null, .source_index = 0 }};
    retired.mutations = &retirement;
    retired.publication_digest = retired.digest();
    var retired_proof = try provenance.fromCommand(alloc, retired);
    defer retired_proof.deinit();
    const retired_encoded = try provenance.encodeAlloc(alloc, retired_proof.proof);
    defer alloc.free(retired_encoded);
    const retirement_position: publication.Position = .{ .raft = .{ .term = 1, .index = 5 } };
    try publication.stageArtifactRevisions(&txn, retired, retirement_position);
    try provenance.stage(&txn, retired, retired_encoded, retirement_position);
    var marker: [16]u8 = undefined;
    std.mem.writeInt(u64, marker[0..8], 1, .little);
    std.mem.writeInt(u64, marker[8..16], 5, .little);
    try txn.put(&keys.raft_document_applied_entry_key, &marker);
    try std.testing.expectError(error.EnrichmentSourceChanged, captured.token.validateInputs(alloc, &txn));
    var absent = try captureUnit(alloc, &txn, request, authority, source[0], unit_key);
    defer absent.deinit();
    try std.testing.expect(absent.value == null);
    try std.testing.expectEqual(@as(u32, 1), absent.previous.count);
    const empty = chunks.Builder.init().finish().encode();
    const child_retirement = try absent.token.command(&.{
        .{ .family = .document_artifact, .key = member_key, .value = null, .source_index = 0 },
        .{ .family = .document_artifact, .key = manifest_key, .value = &empty, .source_index = 0 },
    });
    var prepared_retirement = try prepare(alloc, child_retirement, catalogs);
    defer prepared_retirement.deinit();
    try prepared_retirement.chunk_fence.?.bind(alloc, &txn, child_retirement);
    try prepared_retirement.chunk_fence.?.requireCurrent(&txn);
    try std.testing.expectEqual(@as(usize, 0), prepared_retirement.coverage.len);

    // Exercise consumers against an accepted generation fixture, without
    // granting generic mutation authority to heads or activating the writer.
    const generations = @import("artifact_chunk_generation.zig");
    const scope = try @import("artifact_generation_scope.zig").extractionKeyAlloc(alloc, "doc", "units");
    defer alloc.free(scope);
    const unit_name = try extraction.unitNameAlloc(alloc, "page-1");
    defer alloc.free(unit_name);
    const sibling_name = try extraction.unitNameAlloc(alloc, "page-2");
    defer alloc.free(sibling_name);
    const entries = [_]extraction.Entry{
        .{ .name = unit_name, .value = "new selected unit" },
        .{ .name = "root", .value = "metadata, not a unit" },
        .{ .name = sibling_name, .value = "second selected unit" },
    };
    var output = chunks.Builder.init();
    for (entries, 0..) |entry, ordinal| {
        const encoded = try extraction.encodeEntry(alloc, entry);
        defer alloc.free(encoded);
        try output.append(@intCast(ordinal), encoded);
    }
    var generation = try extraction.Plan.init(alloc, scope, try generations.Spec.init(authority, scope, parent.inputDigest(), output.finish(), 1));
    defer generation.deinit();
    _ = try generation.begin(&txn);
    var append = try extraction.PreparedAppend.init(alloc, &generation, try generation.core.load(&txn), &entries);
    defer append.deinit();
    _ = try append.stage(&generation, &txn);
    const Guard = struct {
        pub fn validate(_: @This(), _: anytype) !void {}
    };
    _ = try generation.publish(&txn, null, Guard{});
    const generation_session: UnitSession(@TypeOf(&txn)) = .{ .txn = &txn, .request = request, .authority = authority, .source = source[0] };
    try std.testing.expectError(error.ArtifactPublicationPending, generation_session.unitPage(alloc, null, 128, 64 * 1024));
    const AcceptedFixture = struct {
        fn stage(a: std.mem.Allocator, writer: *@import("../docstore.zig").DocStore.Txn, header: publication.Command, template: provenance.Proof, selected_plan: *const extraction.Plan, position: publication.Position) !void {
            const raw = selected_plan.core.spec.encode();
            const changes = [_]publication.Mutation{.{ .family = .document_artifact, .key = selected_plan.core.head_key, .value = &raw, .source_index = 0 }};
            var head_command = header;
            head_command.mutations = &changes;
            head_command.publication_digest = head_command.digest();
            var digest: publication.Digest = undefined;
            std.crypto.hash.sha2.Sha256.hash(&raw, &digest, .{});
            const proof_effects = [_]provenance.Effect{.{ .family = .document_artifact, .key = selected_plan.core.head_key, .value_digest = digest, .value_bytes = raw.len, .source_index = 0 }};
            var proof = template;
            proof.publication_digest = head_command.publication_digest;
            proof.effects = &proof_effects;
            const encoded = try provenance.encodeAlloc(a, proof);
            defer a.free(encoded);
            try publication.stageArtifactRevisions(writer, head_command, position);
            try provenance.stage(writer, head_command, encoded, position);
        }
    };
    try AcceptedFixture.stage(alloc, &txn, parent, parent_proof.proof, &generation, .{ .raft = .{ .term = 1, .index = 6 } });
    const MetadataOnly = struct {
        const StoreTxn = @import("../docstore.zig").DocStore.Txn;
        pub const CursorAdapter = StoreTxn.CursorAdapter;
        txn: *StoreTxn,
        forbidden: []const u8,
        pub fn get(self: *@This(), key: []const u8) ![]const u8 {
            try std.testing.expect(!std.mem.startsWith(u8, key, self.forbidden));
            return self.txn.get(key);
        }
        pub fn openPhysicalCursorAdapter(self: *@This()) !CursorAdapter {
            return self.txn.openPhysicalCursorAdapter();
        }
    };
    var metadata: MetadataOnly = .{ .txn = &txn, .forbidden = generation.core.row_prefix };
    const metadata_session: UnitSession(*MetadataOnly) = .{ .txn = &metadata, .request = request, .authority = authority, .source = source[0] };
    try std.testing.expectError(error.EnrichmentSourceChanged, metadata_session.captureGeneration(alloc, @splat(42), unit_key));
    var first_page = try metadata_session.unitPage(alloc, null, 1, 64 * 1024);
    defer first_page.deinit();
    try std.testing.expectEqual(@as(usize, 1), first_page.units.len);
    try std.testing.expectEqualStrings(unit_key, first_page.units[0]);
    try std.testing.expect(!first_page.at_end);
    var middle_page = try metadata_session.unitPage(alloc, first_page.after, 1, 64 * 1024);
    defer middle_page.deinit();
    try std.testing.expectEqual(@as(usize, 0), middle_page.units.len);
    try std.testing.expectEqual(@as(u32, 1), middle_page.visited);
    try std.testing.expect(!middle_page.at_end);
    var last_page = try metadata_session.unitPage(alloc, middle_page.after, 128, 64 * 1024);
    defer last_page.deinit();
    try std.testing.expectEqualStrings(sibling, last_page.units[0]);
    try std.testing.expect(last_page.at_end);
    var byte_limited = try metadata_session.unitPage(alloc, null, 128, 1);
    defer byte_limited.deinit();
    try std.testing.expectEqualDeep(first_page.after, byte_limited.after);
    const PageAllocationCheck = struct {
        fn run(a: std.mem.Allocator, session: @TypeOf(metadata_session)) !void {
            var page = try session.unitPage(a, null, 128, 64 * 1024);
            defer page.deinit();
            try std.testing.expectEqual(@as(usize, 2), page.units.len);
        }
    };
    try std.testing.checkAllAllocationFailures(alloc, PageAllocationCheck.run, .{metadata_session});
    const obsolete_id = "\x00obsolete\xff";
    const obsolete_manifest = try chunks.scopedKeyAlloc(alloc, "doc", "chunks", obsolete_id);
    defer alloc.free(obsolete_manifest);
    const obsolete_unit = try keys.documentUnitArtifactKeyAlloc(alloc, "doc", "units", obsolete_id);
    defer alloc.free(obsolete_unit);
    try txn.put(obsolete_manifest, &manifest);
    const other_child_manifest = try chunks.scopedKeyAlloc(alloc, "doc", "other-child", obsolete_id);
    defer alloc.free(other_child_manifest);
    try txn.put(other_child_manifest, &manifest);
    var obsolete = try metadata_session.retirementPage(alloc, null, 128, 64 * 1024);
    defer obsolete.deinit();
    try std.testing.expectEqual(@as(usize, 1), obsolete.units.len);
    try std.testing.expectEqualStrings(obsolete_unit, obsolete.units[0]);
    try std.testing.expectEqual(@as(u32, 2), obsolete.visited);
    try std.testing.expect(obsolete.at_end);
    var first_retirement = try metadata_session.retirementPage(alloc, null, 1, 64 * 1024);
    defer first_retirement.deinit();
    try std.testing.expectEqualStrings(obsolete_unit, first_retirement.units[0]);
    try std.testing.expect(!first_retirement.at_end);
    const cursor_bytes = try first_retirement.after.encodeAlloc(alloc);
    defer alloc.free(cursor_bytes);
    var next_retirement = try metadata_session.retirementPage(alloc, try RetirementPosition.decode(cursor_bytes), 128, 64 * 1024);
    defer next_retirement.deinit();
    try std.testing.expectEqual(@as(usize, 0), next_retirement.units.len);
    try std.testing.expectEqual(@as(u32, 1), next_retirement.visited);
    try std.testing.expect(next_retirement.at_end);
    var limited_retirement = try metadata_session.retirementPage(alloc, null, 128, 1);
    defer limited_retirement.deinit();
    try std.testing.expectEqualStrings(first_retirement.after.after_key, limited_retirement.after.after_key);
    var wrong_retirement = first_retirement.after;
    wrong_retirement.after_key = other_child_manifest;
    try std.testing.expectError(error.InvalidBatchRequest, metadata_session.retirementPage(alloc, wrong_retirement, 128, 64 * 1024));
    const RetirementAllocationCheck = struct {
        fn run(a: std.mem.Allocator, session: @TypeOf(metadata_session)) !void {
            var page = try session.retirementPage(a, null, 128, 64 * 1024);
            defer page.deinit();
            try std.testing.expectEqual(@as(usize, 1), page.units.len);
        }
    };
    try std.testing.checkAllAllocationFailures(alloc, RetirementAllocationCheck.run, .{metadata_session});
    var corrupt_inventory = manifest;
    corrupt_inventory[0] ^= 1;
    try txn.put(obsolete_manifest, &corrupt_inventory);
    try std.testing.expectError(error.ArtifactCatalogCorrupt, metadata_session.retirementPage(alloc, null, 128, 64 * 1024));
    try txn.put(obsolete_manifest, &manifest);
    // Discovery must wrap: an unrelated concurrent child inventory can sort
    // behind a resumed cursor. Ending this scan is not a closure certificate.
    const behind = try chunks.scopedKeyAlloc(alloc, "doc", "chunks", "\x00earlier");
    defer alloc.free(behind);
    try txn.put(behind, &manifest);
    var wrapped = try metadata_session.retirementPage(alloc, null, 128, 64 * 1024);
    defer wrapped.deinit();
    try std.testing.expectEqual(@as(usize, 2), wrapped.units.len);
    try std.testing.expectError(error.InvalidBatchRequest, generation_session.captureGeneration(alloc, @splat(0), unit_key));
    try std.testing.expectError(error.EnrichmentSourceChanged, generation_session.captureGeneration(alloc, @splat(42), unit_key));
    var selected_input = try generation_session.captureGeneration(alloc, generation.core.spec.id(), unit_key);
    defer selected_input.deinit();
    try std.testing.expectEqualStrings("new selected unit", selected_input.value.?);
    try first_page.inherit(&selected_input.token);
    try obsolete.inherit(&selected_input.token);
    const selected_output = try selected_input.token.command(&effects);
    var selected_effects = try prepare(alloc, selected_output, catalogs);
    defer selected_effects.deinit();
    try selected_effects.chunk_fence.?.bind(alloc, &txn, selected_output);
    try selected_effects.chunk_fence.?.requireCurrent(&txn);
    try selected_input.token.validateInputs(alloc, &txn);
    // A current child receipt from the previous parent is not completion of
    // this page. Publish the selected replacement and verify without reading
    // any child payloads; retirement still requires an accepted empty result.
    try @import("artifact_producer_validation.zig").begin(alloc, &txn, authority);
    try std.testing.expectError(error.EnrichmentSourceChanged, first_page.verifyChildren(alloc, &txn, "doc", "chunks"));
    var selected_proof = try provenance.fromCommand(alloc, selected_output);
    defer selected_proof.deinit();
    const selected_encoded = try provenance.encodeAlloc(alloc, selected_proof.proof);
    defer alloc.free(selected_encoded);
    for (selected_output.mutations) |effect| {
        if (effect.value) |value| try txn.put(effect.key, value) else try txn.delete(effect.key);
    }
    const selected_position: publication.Position = .{ .raft = .{ .term = 1, .index = 6 } };
    try publication.stageArtifactRevisions(&txn, selected_output, selected_position);
    try provenance.stage(&txn, selected_output, selected_encoded, selected_position);
    _ = try first_page.verifyChildren(alloc, &txn, "doc", "chunks");
    const VerificationAllocationCheck = struct {
        fn run(a: std.mem.Allocator, page: *const UnitPage, writer: @TypeOf(&txn)) !void {
            _ = try page.verifyChildren(a, writer, "doc", "chunks");
        }
    };
    try std.testing.checkAllAllocationFailures(alloc, VerificationAllocationCheck.run, .{ &first_page, &txn });
    // Borrow the page to isolate the stronger empty-output verification rule.
    // No deinit: first_page still owns these allocations.
    const nonempty_retirement: RetirementPage = .{ .arena = first_page.arena, .proof = first_page.proof, .head_key = first_page.head_key, .head_value = first_page.head_value, .head_position = first_page.head_position, .units = first_page.units, .after = .{ .generation = first_page.after.generation, .after_key = "" }, .visited = first_page.visited, .at_end = first_page.at_end };
    try std.testing.expectError(error.ArtifactPublicationPending, nonempty_retirement.verifyChildren(alloc, &txn, "doc", "chunks"));
    var empty_input = try generation_session.capture(alloc, unit_key);
    defer empty_input.deinit();
    const empty_manifest = chunks.Builder.init().finish().encode();
    const empty_effects = [_]publication.Mutation{
        .{ .family = .document_artifact, .key = member_key, .value = null, .source_index = 0 },
        .{ .family = .document_artifact, .key = manifest_key, .value = &empty_manifest, .source_index = 0 },
    };
    const empty_output = try empty_input.token.command(&empty_effects);
    var empty_prepared = try prepare(alloc, empty_output, catalogs);
    defer empty_prepared.deinit();
    try empty_prepared.chunk_fence.?.bind(alloc, &txn, empty_output);
    try empty_prepared.chunk_fence.?.requireCurrent(&txn);
    var empty_proof = try provenance.fromCommand(alloc, empty_output);
    defer empty_proof.deinit();
    const empty_encoded = try provenance.encodeAlloc(alloc, empty_proof.proof);
    defer alloc.free(empty_encoded);
    // Raw emptiness without an accepted receipt cannot advance a page.
    try txn.delete(member_key);
    try txn.put(manifest_key, &empty_manifest);
    try std.testing.expectError(error.EnrichmentSourceChanged, nonempty_retirement.verifyChildren(alloc, &txn, "doc", "chunks"));
    try publication.stageArtifactRevisions(&txn, empty_output, selected_position);
    try provenance.stage(&txn, empty_output, empty_encoded, selected_position);
    _ = try nonempty_retirement.verifyChildren(alloc, &txn, "doc", "chunks");
    const progress = @import("artifact_unit_progress.zig");
    try @import("artifact_producer_obligations.zig").begin(alloc, &txn, authority);
    var first_progress = (try progress.prepare(alloc, db.root_incarnation, generation_session, .{ .visits = 1 }, null)).?;
    defer first_progress.deinit();
    try std.testing.expectEqual(progress.Phase.desired, first_progress.record.progress.phase);
    try std.testing.expectEqual(@as(u32, 1), first_progress.record.progress.desired_ordinal);
    try std.testing.expectEqual(@as(u64, 1), first_progress.record.progress.verified_units);
    var accepted_job_resolution = (try metadata_session.resolveJob(alloc, generation.core.spec.id(), unit_key)).?;
    defer accepted_job_resolution.deinit();
    try std.testing.expectEqual(.accepted, accepted_job_resolution.kind);
    try accepted_job_resolution.requireCurrent(&txn);
    const dispatch = @import("artifact_unit_dispatch.zig");
    var dispatch_start = try dispatch.prepare(alloc, metadata_session, null, .{ .visits = 1 });
    defer dispatch_start.deinit();
    try std.testing.expectEqual(@as(usize, 0), dispatch_start.missing.len);
    try std.testing.expectEqual(@as(u32, 1), dispatch_start.after.ordinal);
    try dispatch_start.requireCurrent(&txn);
    var dispatch_metadata = try dispatch.prepare(alloc, generation_session, dispatch_start.after, .{ .visits = 1 });
    defer dispatch_metadata.deinit();
    try std.testing.expectEqual(@as(usize, 0), dispatch_metadata.missing.len);
    var dispatch_missing = try dispatch.prepare(alloc, generation_session, dispatch_metadata.after, .{ .visits = 1 });
    defer dispatch_missing.deinit();
    try std.testing.expectEqual(@as(usize, 1), dispatch_missing.missing.len);
    try std.testing.expectEqual(null, try generation_session.resolveJob(alloc, generation.core.spec.id(), dispatch_missing.missing[0]));
    try std.testing.expectEqual(dispatch.Phase.retiring, dispatch_missing.after.phase);
    try std.testing.expect(!dispatch_missing.at_end);
    var wrong_child_session = generation_session;
    wrong_child_session.request.artifact_name = "other-child";
    try std.testing.expectError(error.ArtifactCatalogDrift, dispatch.prepare(alloc, wrong_child_session, dispatch_start.after, .{}));
    const DispatchAllocationCheck = struct {
        fn run(a: std.mem.Allocator, session: @TypeOf(generation_session), after: dispatch.Cursor) !void {
            const raw = try after.encodeAlloc(a);
            defer a.free(raw);
            var selected = try dispatch.prepare(a, session, try dispatch.Cursor.decode(raw), .{ .visits = 1 });
            defer selected.deinit();
            try std.testing.expectEqual(@as(usize, 1), selected.missing.len);
        }
    };
    try std.testing.checkAllAllocationFailures(alloc, DispatchAllocationCheck.run, .{ generation_session, dispatch_metadata.after });
    const unit_jobs = @import("artifact_unit_jobs.zig");
    var first_admission = try unit_jobs.prepare(alloc, &txn, db.root_incarnation, &dispatch_start, .{});
    defer first_admission.deinit();
    try std.testing.expectError(error.DurableRootIncarnationUnavailable, first_admission.stage(&txn, db.root_incarnation + 1));
    try std.testing.expectEqual(.admitted, try first_admission.stage(&txn, db.root_incarnation));
    try std.testing.expectEqual(.duplicate, try first_admission.stage(&txn, db.root_incarnation));
    try std.testing.expectEqual(null, try unit_jobs.prepareDocumentTurn(&txn, db.root_incarnation, "doc"));
    var metadata_admission = try unit_jobs.prepare(alloc, &txn, db.root_incarnation, &dispatch_metadata, .{});
    defer metadata_admission.deinit();
    try std.testing.expectEqual(.admitted, try metadata_admission.stage(&txn, db.root_incarnation));
    // Outstanding work is bounded across pages. Refusal changes neither the
    // discovery position nor the job set, so the exact page remains retryable.
    var denied_admission = try unit_jobs.prepare(alloc, &txn, db.root_incarnation, &dispatch_missing, .{ .bytes = 1 });
    defer denied_admission.deinit();
    try std.testing.expectError(error.ResourceBudgetExceeded, denied_admission.stage(&txn, db.root_incarnation));
    try std.testing.expectError(error.NotFound, txn.get(&denied_admission.jobs[0].key));
    const before_admission = (try unit_jobs.load(&txn, first_admission.selected)).?;
    try std.testing.expectEqual(@as(u64, 2), before_admission.metadata.revision);
    try std.testing.expectEqual(@as(u64, 0), before_admission.metadata.jobs);
    try std.testing.expectEqualDeep(dispatch_metadata.after, before_admission.cursor);
    const JobAllocationCheck = struct {
        fn run(a: std.mem.Allocator, writer: @TypeOf(&txn), root: u128, page: *const dispatch.Page) !void {
            var admission = try unit_jobs.prepare(a, writer, root, page, .{});
            defer admission.deinit();
            for (admission.jobs) |job| _ = try unit_jobs.Job.decode(job.key, job.value);
        }
    };
    try std.testing.checkAllAllocationFailures(alloc, JobAllocationCheck.run, .{ &txn, db.root_incarnation, &dispatch_missing });
    var missing_admission = try unit_jobs.prepare(alloc, &txn, db.root_incarnation, &dispatch_missing, .{});
    defer missing_admission.deinit();
    try std.testing.expectEqual(.admitted, try missing_admission.stage(&txn, db.root_incarnation));
    try std.testing.expectEqual(.duplicate, try missing_admission.stage(&txn, db.root_incarnation));
    try std.testing.expectError(error.EnrichmentSourceChanged, first_admission.stage(&txn, db.root_incarnation));
    const admitted_job = try unit_jobs.Job.decode(missing_admission.jobs[0].key, try txn.get(&missing_admission.jobs[0].key));
    try std.testing.expectEqualStrings("doc", admitted_job.document);
    try std.testing.expectEqualStrings("chunks", admitted_job.child);
    try std.testing.expectEqualStrings(dispatch_missing.missing[0], admitted_job.unit);
    try std.testing.expectEqualDeep(dispatch_missing.after.generation, admitted_job.generation);
    try std.testing.expectEqual(@as(u64, 1), (try unit_jobs.load(&txn, missing_admission.selected)).?.metadata.jobs);
    {
        const document_turn = (try unit_jobs.prepareDocumentTurn(&txn, db.root_incarnation, "doc")).?;
        try std.testing.expectEqualDeep(missing_admission.selected, document_turn.selected.?);
        try std.testing.expectError(error.DurableRootIncarnationUnavailable, document_turn.stage(&txn, db.root_incarnation + 1));
        try document_turn.stage(&txn, db.root_incarnation);
        try document_turn.stage(&txn, db.root_incarnation);
        const wrap_turn = (try unit_jobs.prepareDocumentTurn(&txn, db.root_incarnation, "doc")).?;
        try std.testing.expectEqual(null, wrap_turn.selected);
        try wrap_turn.stage(&txn, db.root_incarnation);
        try std.testing.expectError(error.EnrichmentSourceChanged, document_turn.stage(&txn, db.root_incarnation));
        const repeated_turn = (try unit_jobs.prepareDocumentTurn(&txn, db.root_incarnation, "doc")).?;
        try std.testing.expectEqualDeep(document_turn.selected, repeated_turn.selected);
        try std.testing.expectEqual(null, try unit_jobs.prepareDocumentTurn(&txn, db.root_incarnation, "other-document"));
    }
    {
        var plan = try db.core.index_manager.acquireWritePlanSnapshot();
        defer plan.release();
        const control = first_progress.command("chunks");
        var verifier: @import("artifact_completion_progress.zig").StreamVerifier = .{ .plan = plan.plan() };
        const child_requirement = try plan.plan().completion_plan.?.unitChild("chunks");
        var discovered = try dispatch.discover(alloc, &txn, "doc", "chunks", plan.plan(), dispatch_metadata.after, .{ .visits = 1 });
        defer discovered.deinit();
        try std.testing.expectEqualDeep(dispatch_missing.after, discovered.after);
        try std.testing.expectEqualDeep(dispatch_missing.missing, discovered.missing);
        try discovered.requireCurrent(&txn);
        try std.testing.expect((try verifier.verify(alloc, &txn, db.root_incarnation, "doc", child_requirement)) == null);
        try std.testing.expect(verifier.blocked_unit == child_requirement);
        var scheduled = (try @import("artifact_completion_progress.zig").discoverUnitControl(alloc, &txn, db.root_incarnation, "doc", child_requirement, plan.plan(), .{ .visits = 1 })).?;
        defer scheduled.deinit();
        try std.testing.expectEqualDeep(control, try scheduled.command());
        // Discovery/queue admission is read-only. A refused or lost submission
        // must rediscover the identical page, not skip unaccepted work.
        var rescheduled = (try @import("artifact_completion_progress.zig").discoverUnitControl(alloc, &txn, db.root_incarnation, "doc", child_requirement, plan.plan(), .{ .visits = 1 })).?;
        defer rescheduled.deinit();
        try std.testing.expectEqualDeep(control, try rescheduled.command());
        try std.testing.expectError(error.ArtifactPublicationPending, progress.prepareClosure(alloc, db.root_incarnation, generation_session));
        const ControlAllocationCheck = struct {
            fn run(a: std.mem.Allocator, writer: @TypeOf(&txn), root: u128, requirement: @TypeOf(child_requirement), snapshot: @TypeOf(plan.plan())) !void {
                var next_control = (try @import("artifact_completion_progress.zig").discoverUnitControl(a, writer, root, "doc", requirement, snapshot, .{ .visits = 1 })).?;
                defer next_control.deinit();
                try (try next_control.command()).validate(a);
            }
        };
        try std.testing.checkAllAllocationFailures(alloc, ControlAllocationCheck.run, .{ &txn, db.root_incarnation, child_requirement, plan.plan() });
        var received = try progress.prepareCommand(alloc, &txn, db.root_incarnation, control, plan.plan());
        defer received.deinit();
        try std.testing.expectEqualDeep(first_progress.record, received.record);
        var forged = control;
        forged.census.?.after[0] ^= 1;
        forged.publication_digest = forged.digest();
        try std.testing.expectError(error.EnrichmentSourceChanged, progress.prepareCommand(alloc, &txn, db.root_incarnation, forged, plan.plan()));
        // Seed only the earlier completion prefix with a test verifier, then
        // exercise real scheduler selection at the blocked child. Production
        // parent publication remains gated; this fixture grants no parent
        // acceptance or child receipt to the real verifier.
        const completion_progress = @import("artifact_completion_progress.zig");
        const obligations = @import("artifact_producer_obligations.zig");
        _ = try obligations.mark(alloc, &txn, authority, "doc", null);
        const PrefixFixture = struct {
            const Observation = @import("artifact_stream_observation.zig").Observation;
            pub const Witness = struct {
                root: u128,
                requirement: publication.Digest,
                observation: Observation,
                pub fn deinit(_: *@This()) void {}
                pub fn requireCurrent(self: @This(), reader: anytype, root: u128) !void {
                    if (self.root != root) return error.DurableRootIncarnationUnavailable;
                    try self.observation.requireCurrent(reader, "doc");
                }
            };
            pub fn verify(_: *@This(), _: std.mem.Allocator, reader: anytype, root: u128, document: []const u8, node: *const @import("artifact_completion_plan.zig").Node) !?Witness {
                if (node.kind == .unit_children) return null;
                return .{ .root = root, .requirement = node.id, .observation = try Observation.capture(reader, document) };
            }
        };
        var fixture: PrefixFixture = .{};
        if (try completion_progress.prepare(alloc, &txn, db.root_incarnation, &plan.plan().completion_plan.?, "doc", &fixture, .{ .time_budget_ns = null })) |value| {
            var prefix_page = value;
            defer prefix_page.deinit();
            try std.testing.expect(!try prefix_page.stage(&txn, db.root_incarnation, &plan.plan().completion_plan.?));
        }
        var selected_control = (try completion_progress.discoverNextControl(alloc, &txn, db.root_incarnation, "doc", plan.plan(), .{ .visits = 1 })).?;
        defer selected_control.deinit();
        try std.testing.expectEqualDeep(control, try selected_control.command());
        // A larger verification page encounters an unfinished child instead
        // of admitting a prefix control. The scheduler must discover durable
        // jobs at its saved producer cursor, never certify that missing work.
        var selected_action = (try completion_progress.discoverNextAction(alloc, &txn, db.root_incarnation, "doc", plan.plan(), .{ .time_budget_ns = null })).?;
        defer selected_action.deinit();
        try std.testing.expect(selected_action == .jobs);
        try std.testing.expectEqualDeep(dispatch_missing.after, selected_action.jobs.before.?);
        try std.testing.expect(selected_action.jobs.missing.len != 0);
        try selected_action.jobs.requireCurrent(&txn);
        const ActionAllocationCheck = struct {
            fn run(a: std.mem.Allocator, reader: @TypeOf(&txn), root: u128, snapshot_plan: @TypeOf(plan.plan())) !void {
                var action = (try completion_progress.discoverNextAction(a, reader, root, "doc", snapshot_plan, .{ .time_budget_ns = null })).?;
                defer action.deinit();
                try std.testing.expect(action == .jobs);
            }
        };
        try std.testing.checkAllAllocationFailures(alloc, ActionAllocationCheck.run, .{ &txn, db.root_incarnation, plan.plan() });
        // Invalidate the synthetic prefix before the subsequent integration
        // checks, which continue using actual acceptance evidence only.
        _ = try obligations.mark(alloc, &txn, authority, "doc", null);
    }
    try std.testing.expectError(error.DurableRootIncarnationUnavailable, first_progress.stage(&txn, db.root_incarnation + 1));
    _ = try first_progress.stage(&txn, db.root_incarnation);
    try std.testing.expectError(error.EnrichmentSourceChanged, first_progress.stage(&txn, db.root_incarnation));
    var repeated = (try progress.prepare(alloc, db.root_incarnation, generation_session, .{ .visits = 1 }, first_progress.record.claim)).?;
    defer repeated.deinit();
    try std.testing.expect(repeated.duplicate);
    _ = try repeated.stage(&txn, db.root_incarnation);
    var metadata_progress = (try progress.prepare(alloc, db.root_incarnation, generation_session, .{ .visits = 1 }, null)).?;
    defer metadata_progress.deinit();
    try std.testing.expectEqual(@as(u32, 2), metadata_progress.record.progress.desired_ordinal);
    try std.testing.expectEqual(@as(u64, 1), metadata_progress.record.progress.verified_units);
    {
        var plan = try db.core.index_manager.acquireWritePlanSnapshot();
        defer plan.release();
        const requirement = try plan.plan().completion_plan.?.unitChild("chunks");
        var next_control = (try @import("artifact_completion_progress.zig").discoverUnitControl(alloc, &txn, db.root_incarnation, "doc", requirement, plan.plan(), .{ .visits = 1 })).?;
        defer next_control.deinit();
        try std.testing.expectEqualDeep(metadata_progress.command("chunks"), try next_control.command());
    }
    _ = try metadata_progress.stage(&txn, db.root_incarnation);
    try std.testing.expectError(error.ArtifactPublicationPending, progress.prepare(alloc, db.root_incarnation, generation_session, .{}, null));
    {
        var plan = try db.core.index_manager.acquireWritePlanSnapshot();
        defer plan.release();
        const requirement = try plan.plan().completion_plan.?.unitChild("chunks");
        try std.testing.expectEqual(null, try @import("artifact_completion_progress.zig").discoverUnitControl(alloc, &txn, db.root_incarnation, "doc", requirement, plan.plan(), .{}));
    }
    try std.testing.expectError(error.ArtifactPublicationPending, progress.prepareClosure(alloc, db.root_incarnation, generation_session));
    var empty_generation = try extraction.Plan.init(alloc, scope, try generations.Spec.init(authority, scope, parent.inputDigest(), chunks.Builder.init().finish(), 2));
    defer empty_generation.deinit();
    _ = try empty_generation.begin(&txn);
    _ = try empty_generation.publish(&txn, generation.core.spec.id(), Guard{});
    try AcceptedFixture.stage(alloc, &txn, parent, parent_proof.proof, &empty_generation, .{ .raft = .{ .term = 1, .index = 7 } });
    try std.testing.expectError(error.EnrichmentSourceChanged, generation_session.captureGeneration(alloc, generation.core.spec.id(), unit_key));
    {
        var plan = try db.core.index_manager.acquireWritePlanSnapshot();
        defer plan.release();
        var restarted = try unit_jobs.discover(alloc, &txn, db.root_incarnation, "doc", "chunks", plan.plan(), .{});
        defer restarted.deinit();
        try std.testing.expectEqual(null, restarted.before);
        try std.testing.expectEqualDeep(empty_generation.core.spec.id(), restarted.after.generation);
        try std.testing.expectEqual(@as(usize, 0), restarted.missing.len);
    }
    try std.testing.expectError(error.EnrichmentSourceChanged, selected_input.token.validateInputs(alloc, &txn));
    try std.testing.expectError(error.EnrichmentSourceChanged, accepted_job_resolution.requireCurrent(&txn));
    try std.testing.expectError(error.EnrichmentSourceChanged, selected_effects.chunk_fence.?.requireCurrent(&txn));
    try std.testing.expectError(error.EnrichmentSourceChanged, generation_session.unitPage(alloc, first_page.after, 128, 64 * 1024));
    try std.testing.expectError(error.EnrichmentSourceChanged, dispatch.prepare(alloc, generation_session, dispatch_start.after, .{}));
    try std.testing.expectError(error.EnrichmentSourceChanged, dispatch_start.requireCurrent(&txn));
    try std.testing.expectError(error.EnrichmentSourceChanged, first_page.pendingChildren(alloc, &txn, "doc", "chunks"));
    try std.testing.expectError(error.EnrichmentSourceChanged, generation_session.retirementPage(alloc, first_retirement.after, 128, 64 * 1024));
    var empty_page = try generation_session.unitPage(alloc, null, 128, 64 * 1024);
    defer empty_page.deinit();
    try std.testing.expect(empty_page.at_end);
    try std.testing.expectEqual(@as(u32, 0), empty_page.visited);
    try std.testing.expectEqual(@as(usize, 0), empty_page.units.len);
    try std.testing.expectError(error.InvalidBatchRequest, empty_page.verifyChildren(alloc, &txn, "another-document", "chunks"));
    try std.testing.expectError(error.InvalidBatchRequest, empty_page.verifyChildren(alloc, &txn, "doc", ""));
    const verified_empty = try empty_page.verifyChildren(alloc, &txn, "doc", "chunks");
    const verified_other = try empty_page.verifyChildren(alloc, &txn, "doc", "another-child");
    try std.testing.expect(!std.meta.eql(verified_empty.digest, verified_other.digest));
    var empty_retirements = try generation_session.retirementPage(alloc, null, 128, 64 * 1024);
    defer empty_retirements.deinit();
    try std.testing.expectEqual(@as(usize, 3), empty_retirements.units.len);
    try std.testing.expectEqual(@as(u32, 3), empty_retirements.visited);
    try std.testing.expect(empty_retirements.at_end);
    var dispatch_empty = try dispatch.prepare(alloc, generation_session, null, .{});
    defer dispatch_empty.deinit();
    try std.testing.expectEqual(dispatch.Phase.retiring, dispatch_empty.after.phase);
    try std.testing.expect(!dispatch_empty.at_end);
    var dispatch_retire = try dispatch.prepare(alloc, generation_session, dispatch_empty.after, .{});
    defer dispatch_retire.deinit();
    try std.testing.expectEqual(@as(usize, 3), dispatch_retire.missing.len);
    try std.testing.expect(dispatch_retire.at_end);
    try std.testing.expectError(error.EnrichmentSourceChanged, first_page.verifyChildren(alloc, &txn, "doc", "chunks"));

    // Publish accepted empty outputs for every obsolete scope, then reconcile
    // them page-by-page. The materialization cut is advanced explicitly in this
    // fixture; normal owner commits perform this in their physical write hook.
    const final_position: publication.Position = .{ .raft = .{ .term = 1, .index = 11 } };
    try AcceptedFixture.stage(alloc, &txn, parent, parent_proof.proof, &empty_generation, final_position);
    const materialization_key = publication.materializationRevisionKey(authority.namespace, "doc");
    try txn.put(&materialization_key, &try (publication.Materialization{ .position = .{ .raft = .{ .term = 1, .index = 10 } }, .replay_sequence = null }).encode());
    var reset_progress = (try progress.prepare(alloc, db.root_incarnation, generation_session, .{}, null)).?;
    defer reset_progress.deinit();
    try std.testing.expectEqual(progress.Phase.retiring, reset_progress.record.progress.phase);
    try std.testing.expectEqual(@as(u64, 0), reset_progress.record.progress.verified_units);
    _ = try reset_progress.stage(&txn, db.root_incarnation);
    try std.testing.expectError(error.ArtifactPublicationPending, progress.prepare(alloc, db.root_incarnation, generation_session, .{ .visits = 1 }, null));
    for (empty_retirements.units) |obsolete_scope| {
        var input = try generation_session.capture(alloc, obsolete_scope);
        defer input.deinit();
        try std.testing.expect(input.value == null);
        var ref = (try ids.decodeArtifactRefAlloc(alloc, obsolete_scope)).?;
        defer ref.deinit(alloc);
        const old_member = try keys.documentUnitChunkArtifactKeyAlloc(alloc, "doc", "chunks", ref.unit_id.?, 0);
        defer alloc.free(old_member);
        const retired_effects = [_]publication.Mutation{
            .{ .family = .document_artifact, .key = old_member, .value = null, .source_index = 0 },
            .{ .family = .document_artifact, .key = input.manifest_key, .value = &empty_manifest, .source_index = 0 },
        };
        const retired_output = try input.token.command(if (input.previous.count == 0) retired_effects[1..] else &retired_effects);
        var retired_prepared = try prepare(alloc, retired_output, catalogs);
        defer retired_prepared.deinit();
        try retired_prepared.chunk_fence.?.bind(alloc, &txn, retired_output);
        try retired_prepared.chunk_fence.?.requireCurrent(&txn);
        var proof = try provenance.fromCommand(alloc, retired_output);
        defer proof.deinit();
        const encoded = try provenance.encodeAlloc(alloc, proof.proof);
        defer alloc.free(encoded);
        for (retired_output.mutations) |effect| {
            if (effect.value) |value| try txn.put(effect.key, value) else try txn.delete(effect.key);
        }
        try publication.stageArtifactRevisions(&txn, retired_output, final_position);
        try provenance.stage(&txn, retired_output, encoded, final_position);
    }
    try txn.put(&materialization_key, &try (publication.Materialization{ .position = final_position, .replay_sequence = null }).encode());
    const ProgressAllocationCheck = struct {
        fn run(a: std.mem.Allocator, root: u128, session: @TypeOf(generation_session)) !void {
            var page = (try progress.prepare(a, root, session, .{ .visits = 1 }, null)).?;
            defer page.deinit();
        }
    };
    try std.testing.checkAllAllocationFailures(alloc, ProgressAllocationCheck.run, .{ db.root_incarnation, generation_session });
    var page_count: usize = 0;
    var terminal_command: ?publication.Command = null;
    while (try progress.prepare(alloc, db.root_incarnation, generation_session, .{ .visits = 1 }, null)) |value| {
        var page = value;
        defer page.deinit();
        _ = try page.stage(&txn, db.root_incarnation);
        page_count += 1;
        try std.testing.expect(page_count <= 5);
        if (page.record.progress.phase == .complete) {
            terminal_command = page.command("chunks");
            terminal_command.?.census.?.document_key = "doc";
            try std.testing.expectEqual(@as(u64, 3), page.record.progress.verified_units);
            var duplicate = (try progress.prepare(alloc, db.root_incarnation, generation_session, .{ .visits = 1 }, page.record.claim)).?;
            defer duplicate.deinit();
            try std.testing.expect(duplicate.duplicate);
            try std.testing.expect(try duplicate.stage(&txn, db.root_incarnation));
        }
    }
    try std.testing.expectEqual(@as(usize, 5), page_count);
    var closure = try progress.prepareClosure(alloc, db.root_incarnation, generation_session);
    defer closure.deinit();
    try closure.requireCurrent(&txn, db.root_incarnation);
    std.mem.writeInt(u64, marker[8..16], 11, .little);
    try txn.put(&keys.raft_document_applied_entry_key, &marker);
    try txn.commit();
    txn_open = false;
    current.abort();
    current_open = false;
    read.abort();
    read_open = false;
    db.close();
    db_open = false;
    db = try @import("db.zig").DB.open(alloc, path, .{ .identity_namespace = .{ .table_id = 0x0101010101010101, .shard_id = 0x0101010101010101, .range_id = 0x0101010101010101 }, .online_source_authority = .raft, .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false });
    db_open = true;
    // Lost-reply retry is applied through the real ordered control path after
    // restart, with no re-enumeration or second advancement of the prefix.
    try db.batchRaftReplicatedApply(.{ .artifact_publication = terminal_command.? }, .{ .term = 1, .index = 12 });
    {
        var reopened = try db.core.store.beginReadTxn();
        defer reopened.abort();
        // The job and its discovery cursor survive restart together even
        // though the parent generation has since changed. They are scheduling
        // state, not portable producer acceptance or document completion.
        const recovered_jobs = (try unit_jobs.load(&reopened, missing_admission.selected)).?;
        try std.testing.expectEqual(@as(u64, 1), recovered_jobs.metadata.jobs);
        try std.testing.expectEqualDeep(dispatch_missing.after, recovered_jobs.cursor);
        _ = try unit_jobs.Job.decode(missing_admission.jobs[0].key, try reopened.get(&missing_admission.jobs[0].key));
        try closure.requireCurrent(&reopened, db.root_incarnation);
        const restarted_session: UnitSession(@TypeOf(&reopened)) = .{ .txn = &reopened, .request = request, .authority = authority, .source = source[0] };
        var restarted = try progress.prepareClosure(alloc, db.root_incarnation, restarted_session);
        defer restarted.deinit();
        try restarted.requireCurrent(&reopened, db.root_incarnation);
        try std.testing.expectEqual(null, try progress.prepare(alloc, db.root_incarnation, restarted_session, .{}, null));
        try std.testing.expectError(error.DurableRootIncarnationUnavailable, restarted.requireCurrent(&reopened, db.root_incarnation + 1));
        var plan = try db.core.index_manager.acquireWritePlanSnapshot();
        defer plan.release();
        const child_requirement = try plan.plan().completion_plan.?.unitChild("chunks");
        var verifier: @import("artifact_completion_progress.zig").StreamVerifier = .{ .plan = plan.plan() };
        const primary_key = try keys.documentKeyAlloc(alloc, "doc");
        defer alloc.free(primary_key);
        const row_key = try keys.relationalRowKeyAlloc(alloc, "doc");
        defer alloc.free(row_key);
        const MetadataProbe = struct {
            reader: @TypeOf(&reopened),
            primary: []const u8,
            row: []const u8,
            pub fn get(self: *@This(), selected: []const u8) ![]const u8 {
                if (std.mem.eql(u8, selected, self.primary) or std.mem.eql(u8, selected, self.row)) return error.UnexpectedPrimaryBodyRead;
                return self.reader.get(selected);
            }
        };
        var closure_metadata: MetadataProbe = .{ .reader = &reopened, .primary = primary_key, .row = row_key };
        var witness = (try verifier.verify(alloc, &closure_metadata, db.root_incarnation, "doc", child_requirement)).?;
        defer witness.deinit();
        try std.testing.expect(witness.value == .units);
        try std.testing.expectEqualDeep(child_requirement.id, witness.requirement);
        try witness.requireCurrent(&closure_metadata, db.root_incarnation);
        try std.testing.expect((try @import("artifact_completion_progress.zig").discoverUnitControl(alloc, &reopened, db.root_incarnation, "doc", child_requirement, plan.plan(), .{})) == null);
        // A child's closure never substitutes for the extraction parent's
        // separate requirement or turns the entire document into completion.
        const parent_requirement = try plan.plan().completion_plan.?.provider(child_requirement.parent_template.?);
        var parent_witness = (try verifier.verify(alloc, &reopened, db.root_incarnation, "doc", parent_requirement)).?;
        defer parent_witness.deinit();
        try std.testing.expect(parent_witness.value == .extraction);
        try std.testing.expectEqualDeep(parent_requirement.id, parent_witness.requirement);
        try parent_witness.requireCurrent(&reopened, db.root_incarnation);
        const ExtractionAllocationCheck = struct {
            fn run(a: std.mem.Allocator, reader: @TypeOf(&reopened), root: u128, parent_request: @import("enrichment/enrichment_types.zig").GeneratedEnrichmentRequest, snapshot: @TypeOf(plan.plan())) !void {
                var prepared_parent = try @import("artifact_stream_progress.zig").prepareExtractionClosure(a, reader, root, parent_request, snapshot);
                defer prepared_parent.deinit();
                try prepared_parent.requireCurrent(reader, root);
            }
        };
        var parent_request = plan.plan().generated_templates[child_requirement.parent_template.?];
        parent_request.doc_key = "doc";
        try std.testing.checkAllAllocationFailures(alloc, ExtractionAllocationCheck.run, .{ &reopened, db.root_incarnation, parent_request, plan.plan() });
    }
    // A new child scope behind the terminal cursor invalidates the old closure
    // through the same physical mutation hook as normal generated writes.
    {
        var writer = try db.core.store.beginWriteTxn();
        errdefer writer.abort();
        const reopened_child_manifest = try chunks.scopedKeyAlloc(alloc, "doc", "chunks", "\x00behind");
        defer alloc.free(reopened_child_manifest);
        try writer.put(reopened_child_manifest, &empty_manifest);
        std.mem.writeInt(u64, marker[8..16], 13, .little);
        try writer.put(&keys.raft_document_applied_entry_key, &marker);
        try writer.commit();
    }
    {
        var changed_read = try db.core.store.beginReadTxn();
        defer changed_read.abort();
        try std.testing.expectError(error.EnrichmentSourceChanged, closure.requireCurrent(&changed_read, db.root_incarnation));
        var plan = try db.core.index_manager.acquireWritePlanSnapshot();
        defer plan.release();
        var verifier: @import("artifact_completion_progress.zig").StreamVerifier = .{ .plan = plan.plan() };
        try std.testing.expect((try verifier.verify(alloc, &changed_read, db.root_incarnation, "doc", try plan.plan().completion_plan.?.unitChild("chunks"))) == null);
    }
    {
        var writer = try db.core.store.beginWriteTxn();
        errdefer writer.abort();
        const head_only = try chunks.scopedKeyAlloc(alloc, "doc", "chunks", "\x00\x01head-only");
        defer alloc.free(head_only);
        const head_spec = try generations.Spec.init(authority, head_only, parent.inputDigest(), chunks.Builder.init().finish(), 1);
        head_only[keys.findComponentTerminator(head_only, 1).? + 2] = keys.producer_generation_head_kind;
        try writer.put(head_only, &head_spec.encode());
        const raw_only = try keys.documentUnitChunkArtifactKeyAlloc(alloc, "doc", "chunks", "\x00\x02raw-only", 1000000);
        defer alloc.free(raw_only);
        try writer.put(raw_only, "historical payload without an inventory");
        std.mem.writeInt(u64, marker[8..16], 14, .little);
        try writer.put(&keys.raft_document_applied_entry_key, &marker);
        try writer.commit();
    }
    {
        var discovery_read = try db.core.store.beginReadTxn();
        defer discovery_read.abort();
        const discovery_session: UnitSession(@TypeOf(&discovery_read)) = .{ .txn = &discovery_read, .request = request, .authority = authority, .source = source[0] };
        var head_only_page = try discovery_session.retirementPage(alloc, null, 1, 64 * 1024);
        defer head_only_page.deinit();
        const head_only_unit = try keys.documentUnitArtifactKeyAlloc(alloc, "doc", "units", "\x00\x01head-only");
        defer alloc.free(head_only_unit);
        try std.testing.expectEqualStrings(head_only_unit, head_only_page.units[0]);
        // A directory entry or raw tail is discovery, not accepted output.
        try std.testing.expectError(error.ArtifactPublicationPending, head_only_page.verifyChildren(alloc, &discovery_read, "doc", "chunks"));
        var raw_only_page = try discovery_session.retirementPage(alloc, head_only_page.after, 1, 64 * 1024);
        defer raw_only_page.deinit();
        const raw_only_unit = try keys.documentUnitArtifactKeyAlloc(alloc, "doc", "units", "\x00\x02raw-only");
        defer alloc.free(raw_only_unit);
        try std.testing.expectEqualStrings(raw_only_unit, raw_only_page.units[0]);
        try std.testing.expectError(error.ArtifactPublicationPending, raw_only_page.verifyChildren(alloc, &discovery_read, "doc", "chunks"));
        try std.testing.expectError(error.ArtifactCoverageBaselinePending, discovery_session.capture(alloc, raw_only_unit));
    }
    {
        const before_wakeup = db.core.store.lastReplaySequence(0);
        var cursor_page = blk: {
            var snapshot = try db.core.store.beginReadTxn();
            defer snapshot.abort();
            var plan = try db.core.index_manager.acquireWritePlanSnapshot();
            defer plan.release();
            break :blk try dispatch.discover(alloc, &snapshot, "doc", "chunks", plan.plan(), null, .{});
        };
        defer cursor_page.deinit();
        var cursor_admission = blk: {
            var snapshot = try db.core.store.beginReadTxn();
            defer snapshot.abort();
            break :blk try unit_jobs.prepare(alloc, &snapshot, db.root_incarnation, &cursor_page, .{});
        };
        defer cursor_admission.deinit();
        try std.testing.expectEqual(@as(usize, 0), cursor_admission.jobs.len);
        try std.testing.expectEqual(@as(u64, 0), try db.admitArtifactUnitJobs(&cursor_admission));
        try std.testing.expectEqual(before_wakeup, db.core.store.lastReplaySequence(0));
        var wake_page = blk: {
            var snapshot = try db.core.store.beginReadTxn();
            defer snapshot.abort();
            var plan = try db.core.index_manager.acquireWritePlanSnapshot();
            defer plan.release();
            break :blk try unit_jobs.discover(alloc, &snapshot, db.root_incarnation, "doc", "chunks", plan.plan(), .{});
        };
        defer wake_page.deinit();
        try std.testing.expect(wake_page.missing.len != 0);
        var wake_admission = blk: {
            var snapshot = try db.core.store.beginReadTxn();
            defer snapshot.abort();
            break :blk try unit_jobs.prepare(alloc, &snapshot, db.root_incarnation, &wake_page, .{ .bytes = 1 });
        };
        defer wake_admission.deinit();
        // Refusing scoped headroom aborts the enclosing replay transaction;
        // neither a wakeup nor any of its new jobs becomes visible.
        try std.testing.expectError(error.ResourceBudgetExceeded, db.admitArtifactUnitJobs(&wake_admission));
        try std.testing.expectEqual(before_wakeup, db.core.store.lastReplaySequence(0));
        {
            var snapshot = try db.core.store.beginReadTxn();
            defer snapshot.abort();
            try std.testing.expectError(error.NotFound, snapshot.get(&wake_admission.jobs[0].key));
            try std.testing.expectEqualDeep(cursor_page.after, (try unit_jobs.load(&snapshot, wake_admission.selected)).?.cursor);
        }
        wake_admission.limits = .{};
        const wake_sequence = try db.admitArtifactUnitJobs(&wake_admission);
        try std.testing.expect(wake_sequence > before_wakeup);
        var first_turn = blk: {
            var reader = try db.core.store.beginReadTxn();
            defer reader.abort();
            try std.testing.expectError(error.DurableRootIncarnationUnavailable, unit_jobs.prepareTurn(alloc, &reader, db.root_incarnation + 1, wake_admission.selected, .{}));
            break :blk try unit_jobs.prepareTurn(alloc, &reader, db.root_incarnation, wake_admission.selected, .{ .visits = 1 });
        };
        defer first_turn.deinit();
        try std.testing.expectEqual(@as(usize, 1), first_turn.page.items.len);
        // Simulate a failed callback. Advancing fairness must not delete the
        // job; it survives restart and remains eligible on the next sweep.
        const first_document_turn = blk: {
            var reader = try db.core.store.beginReadTxn();
            defer reader.abort();
            break :blk (try unit_jobs.prepareDocumentTurn(&reader, db.root_incarnation, "doc")).?;
        };
        try db.finishArtifactUnitWorkTurn(&first_document_turn, &first_turn);
        try db.finishArtifactUnitWorkTurn(&first_document_turn, &first_turn);
        const replay = @import("derived/replay_source.zig");
        {
            const groups = try replay.Source.fromPrimaryStore(db.core.store, null, null).collectEnrichmentDocumentGroups(alloc, before_wakeup);
            defer replay.freePendingDocumentGroups(alloc, groups);
            try std.testing.expectEqual(@as(usize, 1), groups.len);
            try std.testing.expectEqualStrings("doc", groups[0].doc_key);
            try std.testing.expectEqual(wake_sequence, groups[0].sequence);
        }
        db.close();
        db_open = false;
        db = try @import("db.zig").DB.open(alloc, path, .{ .identity_namespace = .{ .table_id = 0x0101010101010101, .shard_id = 0x0101010101010101, .range_id = 0x0101010101010101 }, .online_source_authority = .raft, .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false });
        db_open = true;
        var snapshot = try db.core.store.beginReadTxn();
        defer snapshot.abort();
        const recovered = (try unit_jobs.load(&snapshot, wake_admission.selected)).?;
        const recovered_document_turn = (try unit_jobs.prepareDocumentTurn(&snapshot, db.root_incarnation, "doc")).?;
        try std.testing.expectEqual(null, recovered_document_turn.selected);
        try std.testing.expectEqual(@as(u64, 3), recovered_document_turn.expected_revision);
        try db.finishArtifactUnitWorkTurn(&recovered_document_turn, null);
        var resumed_turn = try unit_jobs.prepareTurn(alloc, &snapshot, db.root_incarnation, wake_admission.selected, .{ .visits = 1 });
        defer resumed_turn.deinit();
        try std.testing.expectEqual(@as(u64, 1), resumed_turn.expected_revision);
        try std.testing.expectEqual(@as(usize, 1), resumed_turn.page.items.len);
        try std.testing.expect(!std.meta.eql(first_turn.page.items[0].key, resumed_turn.page.items[0].key));
        try db.advanceArtifactUnitWorkTurn(&resumed_turn);
        try std.testing.expectError(error.EnrichmentSourceChanged, db.advanceArtifactUnitWorkTurn(&first_turn));
        try std.testing.expectEqualDeep(wake_page.after, recovered.cursor);
        try std.testing.expectEqual(@as(u64, 1 + wake_page.missing.len), recovered.metadata.jobs);
        for (wake_admission.jobs) |job| _ = try unit_jobs.Job.decode(job.key, try snapshot.get(&job.key));
        try std.testing.expectEqual(wake_sequence, try replay.Source.fromPrimaryStore(db.core.store, null, null).latestMatchingSequence(alloc, before_wakeup, .enrichment));
        var work_cursor: ?unit_jobs.JobKey = null;
        var work_count: usize = 0;
        var old_generation_found = false;
        while (true) {
            var work_page = try unit_jobs.scan(alloc, &snapshot, wake_admission.selected, work_cursor, .{ .visits = 1 });
            defer work_page.deinit();
            try std.testing.expect(work_page.items.len <= 1);
            for (work_page.items) |item| {
                if (!std.mem.eql(u8, &item.job.generation, &wake_page.after.generation)) old_generation_found = true;
                work_count += 1;
            }
            if (work_page.at_end) break;
            try std.testing.expect(!std.meta.eql(work_cursor, work_page.after));
            work_cursor = work_page.after;
        }
        try std.testing.expect(old_generation_found);
        try std.testing.expectEqual(@as(usize, @intCast(recovered.metadata.jobs)), work_count);
        const WorkAllocationCheck = struct {
            fn run(a: std.mem.Allocator, reader: @TypeOf(&snapshot), selected_scope: unit_jobs.Scope) !void {
                var page = try unit_jobs.scan(a, reader, selected_scope, null, .{ .visits = 1 });
                defer page.deinit();
                try std.testing.expectEqual(@as(usize, 1), page.items.len);
            }
        };
        try std.testing.checkAllAllocationFailures(alloc, WorkAllocationCheck.run, .{ &snapshot, wake_admission.selected });
        const TurnAllocationCheck = struct {
            fn run(a: std.mem.Allocator, reader: @TypeOf(&snapshot), root: u128, selected_scope: unit_jobs.Scope) !void {
                var turn = try unit_jobs.prepareTurn(a, reader, root, selected_scope, .{ .visits = 1 });
                defer turn.deinit();
                try std.testing.expectEqual(@as(usize, 1), turn.page.items.len);
            }
        };
        try std.testing.checkAllAllocationFailures(alloc, TurnAllocationCheck.run, .{ &snapshot, db.root_incarnation, wake_admission.selected });
        var retirement_plan = try db.core.index_manager.acquireWritePlanSnapshot();
        defer retirement_plan.release();
        // Current jobs without accepted results retain their slots, regardless
        // of the successful wakeup or callback submission. Old generations may
        // release scheduling capacity only against a newly accepted head.
        var job_retirements = try unit_jobs.retirementSession(alloc, &snapshot, db.root_incarnation, "doc", "chunks", retirement_plan.plan());
        defer job_retirements.deinit();
        for (wake_admission.jobs) |job| try std.testing.expectEqual(null, try job_retirements.prepare(alloc, job.key));
        {
            var owned_page = try unit_jobs.scan(alloc, &snapshot, wake_admission.selected, null, .{ .visits = 1 });
            defer owned_page.deinit();
            var forged = owned_page.items[0];
            forged.job.document = "foreign";
            try std.testing.expectError(error.ArtifactCatalogCorrupt, job_retirements.preparePageItem(alloc, forged));
        }
        const JobRetirementAllocationCheck = struct {
            fn run(a: std.mem.Allocator, reader: @TypeOf(&snapshot), root: u128, key: unit_jobs.JobKey, plan: @TypeOf(retirement_plan.plan())) !void {
                var prepared_job_retirement = (try unit_jobs.prepareRetirement(a, reader, root, key, plan)).?;
                defer prepared_job_retirement.deinit();
                try std.testing.expectEqual(.obsolete, prepared_job_retirement.resolution.kind);
            }
        };
        try std.testing.checkAllAllocationFailures(alloc, JobRetirementAllocationCheck.run, .{ &snapshot, db.root_incarnation, missing_admission.jobs[0].key, retirement_plan.plan() });
        var obsolete_job = (try job_retirements.prepare(alloc, missing_admission.jobs[0].key)).?;
        defer obsolete_job.deinit();
        try std.testing.expectEqual(.obsolete, obsolete_job.resolution.kind);
        try std.testing.expect(try db.retireArtifactUnitJob(&obsolete_job));
        try std.testing.expect(!try db.retireArtifactUnitJob(&obsolete_job));
        // An already pinned reader retains the old job. New readers see the
        // deletion and released capacity together, with no cursor rewind.
        _ = try snapshot.get(&obsolete_job.key);
        var retired_read = try db.core.store.beginReadTxn();
        defer retired_read.abort();
        try std.testing.expectError(error.NotFound, retired_read.get(&obsolete_job.key));
        const after_retirement = (try unit_jobs.load(&retired_read, wake_admission.selected)).?;
        try std.testing.expectEqual(recovered.metadata.jobs - 1, after_retirement.metadata.jobs);
        try std.testing.expectEqual(recovered.metadata.bytes - obsolete_job.key.len - obsolete_job.bytes, after_retirement.metadata.bytes);
        try std.testing.expectEqual(recovered.metadata.revision, after_retirement.metadata.revision);
        try std.testing.expectEqualDeep(recovered.cursor, after_retirement.cursor);
        // A pinned old page is still safe to advance after receipt retirement;
        // scan resumes past a deleted key and wraps to retry retained failures.
        var wrapped_turn = false;
        for (0..work_count + 2) |_| {
            var turn = blk: {
                var reader = try db.core.store.beginReadTxn();
                defer reader.abort();
                break :blk try unit_jobs.prepareTurn(alloc, &reader, db.root_incarnation, wake_admission.selected, .{ .visits = 1 });
            };
            defer turn.deinit();
            try db.advanceArtifactUnitWorkTurn(&turn);
            if (turn.page.at_end) {
                wrapped_turn = true;
                break;
            }
        }
        try std.testing.expect(wrapped_turn);
        {
            var reader = try db.core.store.beginReadTxn();
            defer reader.abort();
            var turn = try unit_jobs.prepareTurn(alloc, &reader, db.root_incarnation, wake_admission.selected, .{ .visits = 1 });
            defer turn.deinit();
            var first = try unit_jobs.scan(alloc, &reader, wake_admission.selected, null, .{ .visits = 1 });
            defer first.deinit();
            try std.testing.expectEqualDeep(first.items, turn.page.items);
            const after_turns = (try unit_jobs.load(&reader, wake_admission.selected)).?;
            try std.testing.expectEqualDeep(after_retirement, after_turns);
        }
        // Replace the accepted parent once more so every remaining queued
        // generation is obsolete. The final retirement must remove the child
        // directory atomically, not leave a permanently runnable empty scope.
        var replacement_id: publication.Digest = undefined;
        {
            var replacement = try extraction.Plan.init(alloc, scope, try generations.Spec.init(authority, scope, parent.inputDigest(), chunks.Builder.init().finish(), 3));
            defer replacement.deinit();
            replacement_id = replacement.core.spec.id();
            var writer = try db.core.store.beginWriteTxn();
            errdefer writer.abort();
            _ = try replacement.begin(&writer);
            _ = try replacement.publish(&writer, empty_generation.core.spec.id(), Guard{});
            try AcceptedFixture.stage(alloc, &writer, parent, parent_proof.proof, &replacement, .{ .raft = .{ .term = 1, .index = 15 } });
            std.mem.writeInt(u64, marker[8..16], 15, .little);
            try writer.put(&keys.raft_document_applied_entry_key, &marker);
            try writer.commit();
        }
        // A lost retirement reply remains idempotent after a newer accepted
        // parent generation has invalidated the old receipt's proof.
        try std.testing.expect(!try db.retireArtifactUnitJob(&obsolete_job));
        const CaptureDispatch = struct {
            calls: usize = 0,
            publish_calls: usize = 0,
            encoded: ?[]u8 = null,
            fn enqueue(ptr: *anyopaque, _: publication.Namespace, bytes: []const u8) !void {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                var decoded = try @import("artifact_publication_transport_codec.zig").decodeBorrowed(std.testing.allocator, bytes);
                defer decoded.deinit();
                const copy = try std.testing.allocator.dupe(u8, bytes);
                if (self.encoded) |prior_bytes| std.testing.allocator.free(prior_bytes);
                self.encoded = copy;
                self.calls += 1;
                if (decoded.command.mode == .publish) self.publish_calls += 1;
            }
        };
        var dispatched = CaptureDispatch{};
        defer if (dispatched.encoded) |bytes| alloc.free(bytes);
        db.artifact_publication_dispatcher = .{ .ptr = &dispatched, .enqueue = CaptureDispatch.enqueue };
        try db.reconfigureEnrichmentRuntimePaused(.{ .enable_without_producers = true });
        const runtime = db.enrichment_runtime orelse return error.TestUnexpectedResult;
        const before_worker = blk: {
            var reader = try db.core.store.beginReadTxn();
            defer reader.abort();
            break :blk (try unit_jobs.prepareDocumentTurn(&reader, db.root_incarnation, "doc")).?.expected_revision;
        };
        {
            const original = runtime.artifact_publication_dispatcher;
            defer runtime.artifact_publication_dispatcher = original;
            runtime.artifact_publication_dispatcher = null;
            try std.testing.expectError(error.ArtifactPublicationPending, @import("enrichment/enrichment_runtime.zig").servicePendingArtifactUnitJobs(runtime, "doc", .{}));
        }
        {
            const original = runtime.artifact_unit_turn_commit;
            defer runtime.artifact_unit_turn_commit = original;
            runtime.artifact_unit_turn_commit = null;
            try std.testing.expectError(error.ArtifactPublicationPending, @import("enrichment/enrichment_runtime.zig").servicePendingArtifactUnitJobs(runtime, "doc", .{}));
        }
        {
            const original = runtime.artifact_unit_turn_commit;
            defer runtime.artifact_unit_turn_commit = original;
            const Refuse = struct {
                fn commit(_: *anyopaque, _: ?*const unit_jobs.DocumentTurn, _: ?*const unit_jobs.WorkTurn) !void {
                    return error.TestTurnCommitRefused;
                }
            };
            runtime.artifact_unit_turn_commit = .{ .ptr = &dispatched, .commit = Refuse.commit };
            try std.testing.expectError(error.TestTurnCommitRefused, @import("enrichment/enrichment_runtime.zig").servicePendingArtifactUnitJobs(runtime, "doc", .{}));
            var reader = try db.core.store.beginReadTxn();
            defer reader.abort();
            try std.testing.expectEqual(before_worker, (try unit_jobs.prepareDocumentTurn(&reader, db.root_incarnation, "doc")).?.expected_revision);
            _ = try reader.get(&wake_admission.jobs[0].key);
        }
        // The replay wake carries only the document. Obsolete generation jobs
        // are skipped without provider I/O, while the fair turn is durable and
        // the replay stays pending until receipt retirement removes the queue.
        try std.testing.expectError(error.ArtifactPublicationPending, @import("enrichment/enrichment_runtime.zig").servicePendingArtifactUnitJobs(runtime, "doc", .{}));
        try std.testing.expectEqual(@as(usize, 0), dispatched.calls);
        {
            var reader = try db.core.store.beginReadTxn();
            defer reader.abort();
            try std.testing.expectEqual(before_worker + 1, (try unit_jobs.prepareDocumentTurn(&reader, db.root_incarnation, "doc")).?.expected_revision);
        }
        var stale = blk: {
            var reader = try db.core.store.beginReadTxn();
            defer reader.abort();
            break :blk .{
                .work = try unit_jobs.prepareTurn(alloc, &reader, db.root_incarnation, wake_admission.selected, .{ .visits = 1 }),
                .retirement = (try unit_jobs.prepareRetirement(alloc, &reader, db.root_incarnation, wake_admission.jobs[0].key, retirement_plan.plan())).?,
            };
        };
        defer stale.work.deinit();
        defer stale.retirement.deinit();
        try db.advanceArtifactUnitWorkTurn(&stale.work);
        var next_turn = blk: {
            var reader = try db.core.store.beginReadTxn();
            defer reader.abort();
            break :blk try unit_jobs.prepareTurn(alloc, &reader, db.root_incarnation, wake_admission.selected, .{ .visits = 1 });
        };
        defer next_turn.deinit();
        try db.advanceArtifactUnitWorkTurn(&next_turn);
        try std.testing.expectError(error.EnrichmentSourceChanged, db.finishArtifactUnitWorkPage(null, &stale.work, &.{&stale.retirement}));
        {
            var checked = try db.core.store.beginReadTxn();
            defer checked.abort();
            _ = try checked.get(&wake_admission.jobs[0].key);
        }
        // The production maintenance pass selects one bounded child/page,
        // resolves accepted receipts, and commits fairness plus retirement.
        for (0..work_count + 4) |_| {
            if (!try db.advanceArtifactUnitReceiptPage(alloc, "doc", retirement_plan.plan())) break;
            var checked = try db.core.store.beginReadTxn();
            defer checked.abort();
            if ((try unit_jobs.load(&checked, wake_admission.selected)).?.metadata.jobs == 0) break;
        }
        for (wake_admission.jobs) |job| {
            var checked = try db.core.store.beginReadTxn();
            defer checked.abort();
            try std.testing.expectError(error.NotFound, checked.get(&job.key));
        }
        {
            var reader = try db.core.store.beginReadTxn();
            defer reader.abort();
            try std.testing.expectEqual(@as(u64, 0), (try unit_jobs.load(&reader, wake_admission.selected)).?.metadata.jobs);
            try std.testing.expectEqual(null, try unit_jobs.prepareDocumentTurn(&reader, db.root_incarnation, "doc"));
            // The old snapshot still retains its pre-wrap continuation.
            try std.testing.expectEqual(null, (try unit_jobs.prepareDocumentTurn(&snapshot, db.root_incarnation, "doc")).?.selected);
        }
        // A newly accepted generation after full retirement gives the replay
        // wake one current typed unit, rather than only obsolete jobs. The
        // old fair cursor may sort after the new key, so bounded wrap turns
        // must reach its callback without losing the job or crediting replay.
        const extracted: @import("enrichment/document_extraction.zig").Unit = .{
            .unit_id = @constCast("page-1"),
            .unit_type = @constCast("page"),
            .text = @constCast("hello world"),
            .method = @constCast("text"),
        };
        const fingerprint = try @import("enrichment/document_unit_fingerprint.zig").fingerprintAlloc(alloc, extracted);
        defer alloc.free(fingerprint);
        const typed = try @import("enrichment/document_unit_payload.zig").encodeAlloc(alloc, "doc", "units", extracted, fingerprint, "input", "text/plain", .{ .range_id = "unit-range-0" });
        defer alloc.free(typed);
        var fresh_output = chunks.Builder.init();
        const encoded_entry = try extraction.encodeEntry(alloc, .{ .name = unit_name, .value = typed });
        defer alloc.free(encoded_entry);
        try fresh_output.append(0, encoded_entry);
        {
            var fresh = try extraction.Plan.init(alloc, scope, try generations.Spec.init(authority, scope, parent.inputDigest(), fresh_output.finish(), 4));
            defer fresh.deinit();
            var writer = try db.core.store.beginWriteTxn();
            errdefer writer.abort();
            _ = try fresh.begin(&writer);
            const fresh_entries = [_]extraction.Entry{.{ .name = unit_name, .value = typed }};
            var fresh_append = try extraction.PreparedAppend.init(alloc, &fresh, try fresh.core.load(&writer), &fresh_entries);
            defer fresh_append.deinit();
            _ = try fresh_append.stage(&fresh, &writer);
            _ = try fresh.publish(&writer, replacement_id, Guard{});
            try AcceptedFixture.stage(alloc, &writer, parent, parent_proof.proof, &fresh, .{ .raft = .{ .term = 1, .index = 16 } });
            std.mem.writeInt(u64, marker[8..16], 16, .little);
            try writer.put(&keys.raft_document_applied_entry_key, &marker);
            try writer.commit();
        }
        var fresh_page = blk: {
            var reader = try db.core.store.beginReadTxn();
            defer reader.abort();
            var current_plan = try db.core.index_manager.acquireWritePlanSnapshot();
            defer current_plan.release();
            break :blk try unit_jobs.discover(alloc, &reader, db.root_incarnation, "doc", "chunks", current_plan.plan(), .{});
        };
        defer fresh_page.deinit();
        try std.testing.expectEqual(@as(usize, 1), fresh_page.missing.len);
        var fresh_admission = blk: {
            var reader = try db.core.store.beginReadTxn();
            defer reader.abort();
            break :blk try unit_jobs.prepare(alloc, &reader, db.root_incarnation, &fresh_page, .{});
        };
        defer fresh_admission.deinit();
        try std.testing.expectEqual(@as(usize, 1), fresh_admission.jobs.len);
        _ = try db.admitArtifactUnitJobs(&fresh_admission);
        const before_fresh = blk: {
            var reader = try db.core.store.beginReadTxn();
            defer reader.abort();
            break :blk (try unit_jobs.prepareDocumentTurn(&reader, db.root_incarnation, "doc")).?.expected_revision;
        };
        try std.testing.expect(try runtime.ownership.ensureLease(runtime.clock.nowRealtimeMs()));
        for (0..4) |_| {
            try std.testing.expectError(error.ArtifactPublicationPending, @import("enrichment/enrichment_runtime.zig").servicePendingArtifactUnitJobs(runtime, "doc", .{}));
            if (dispatched.calls != 0) break;
        }
        try std.testing.expect(dispatched.publish_calls != 0);
        {
            var reader = try db.core.store.beginReadTxn();
            defer reader.abort();
            try std.testing.expect((try unit_jobs.prepareDocumentTurn(&reader, db.root_incarnation, "doc")).?.expected_revision > before_fresh);
            _ = try reader.get(&fresh_admission.jobs[0].key);
        }
        var accepted_child = try @import("artifact_publication_transport_codec.zig").decodeBorrowed(alloc, dispatched.encoded.?);
        defer accepted_child.deinit();
        try std.testing.expectEqual(.publish, accepted_child.command.mode);
        try std.testing.expectEqualStrings("chunks", accepted_child.command.producer_name);
        try std.testing.expectEqualStrings(unit_key, accepted_child.command.producer_scope_key);
        try db.batchRaftReplicatedApply(.{ .artifact_publication = accepted_child.command }, .{ .term = 1, .index = 17 });
        const before_accepted_retry = dispatched.calls;
        for (0..3) |_| try std.testing.expectError(error.ArtifactPublicationPending, @import("enrichment/enrichment_runtime.zig").servicePendingArtifactUnitJobs(runtime, "doc", .{}));
        try std.testing.expectEqual(before_accepted_retry, dispatched.calls);
        {
            var reader = try db.core.store.beginReadTxn();
            defer reader.abort();
            _ = try reader.get(&fresh_admission.jobs[0].key);
        }
        for (0..4) |_| {
            if (!try db.advanceArtifactUnitReceiptPage(alloc, "doc", retirement_plan.plan())) break;
            var reader = try db.core.store.beginReadTxn();
            defer reader.abort();
            if ((try unit_jobs.prepareDocumentTurn(&reader, db.root_incarnation, "doc")) == null) break;
        }
        {
            var reader = try db.core.store.beginReadTxn();
            defer reader.abort();
            try std.testing.expectError(error.NotFound, reader.get(&fresh_admission.jobs[0].key));
            try std.testing.expectEqual(null, try unit_jobs.prepareDocumentTurn(&reader, db.root_incarnation, "doc"));
        }
        var extraction_witness = blk: {
            var reader = try db.core.store.beginReadTxn();
            defer reader.abort();
            const child_requirement = try retirement_plan.plan().completion_plan.?.unitChild("chunks");
            const parent_requirement = try retirement_plan.plan().completion_plan.?.provider(child_requirement.parent_template.?);
            var verifier: @import("artifact_completion_progress.zig").StreamVerifier = .{ .plan = retirement_plan.plan() };
            break :blk (try verifier.verify(alloc, &reader, db.root_incarnation, "doc", parent_requirement)).?;
        };
        defer extraction_witness.deinit();
        try std.testing.expect(extraction_witness.value == .extraction);
        {
            var reader = try db.core.store.beginReadTxn();
            defer reader.abort();
            try extraction_witness.requireCurrent(&reader, db.root_incarnation);
        }
        {
            var reader = try db.core.store.beginReadTxn();
            defer reader.abort();
            const head_key = extraction_witness.value.extraction.head_key;
            const changed_head = try alloc.dupe(u8, try reader.get(head_key));
            defer alloc.free(changed_head);
            changed_head[changed_head.len - 1] ^= 1;
            const HeadFault = struct {
                reader: @TypeOf(&reader),
                key: []const u8,
                bytes: []const u8,
                pub fn get(self: *@This(), selected: []const u8) ![]const u8 {
                    if (std.mem.eql(u8, selected, self.key)) return self.bytes;
                    return self.reader.get(selected);
                }
            };
            var fault: HeadFault = .{ .reader = &reader, .key = head_key, .bytes = changed_head };
            try std.testing.expectError(error.EnrichmentSourceChanged, extraction_witness.requireCurrent(&fault, db.root_incarnation));
            try extraction_witness.requireCurrent(&reader, db.root_incarnation);
        }
        // Private generation state is not a visible document mutation. It
        // still invalidates a prepared parent closure before final discharge.
        {
            var writer = try db.core.store.beginWriteTxn();
            errdefer writer.abort();
            const state_key = extraction_witness.value.extraction.state_key;
            const changed_state = try alloc.dupe(u8, try writer.get(state_key));
            defer alloc.free(changed_state);
            changed_state[changed_state.len - 1] ^= 1;
            try writer.put(state_key, changed_state);
            try writer.commit();
        }
        {
            var reader = try db.core.store.beginReadTxn();
            defer reader.abort();
            try std.testing.expectError(error.EnrichmentSourceChanged, extraction_witness.requireCurrent(&reader, db.root_incarnation));
        }
    }
}

test "ordered artifact inventory unit retirement discovery cursor rejects corrupt framing" {
    const alloc = std.testing.allocator;
    const original: RetirementPosition = .{ .generation = @splat(1), .after_key = "scope\x00\xff" };
    const bytes = try original.encodeAlloc(alloc);
    defer alloc.free(bytes);
    try std.testing.expectEqualDeep(original, try RetirementPosition.decode(bytes));
    for (0..bytes.len) |offset| {
        bytes[offset] ^= 1;
        try std.testing.expectError(error.ArtifactCatalogCorrupt, RetirementPosition.decode(bytes));
        bytes[offset] ^= 1;
        try std.testing.expectError(error.ArtifactCatalogCorrupt, RetirementPosition.decode(bytes[0..offset]));
    }
}
